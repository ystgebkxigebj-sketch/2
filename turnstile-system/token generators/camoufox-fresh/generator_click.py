#!/usr/bin/env python3
"""Camoufox Turnstile producer WITH a working click driver — repo 2's stealth arm.

WHY THIS FILE EXISTS
--------------------
`turnstile-system/token generators/camoufox-fresh/generator.py` (the file repo 2's
`gartic-camoufox-producers.yml` actually runs) has **no clicker of any kind**. It
was written before 2026-07-27, when Cloudflare began escalating every datacenter
challenge to interactive. Since then it renders a widget, waits, and mints
nothing — with `cf_errors: {}`, i.e. *no error at all*, because an escalated
widget is not an error, it is a widget waiting for a pointer. Run 30511497101 is
the proof: `nav status=200`, `tokens 0`, `cf_errors {}`, killed by a 90-second
stall watchdog.

The two older Camoufox generators in `tunnel system/camoufox-oracle/` are no help:
`generator_v2.py` has no clicker either, and `generator.py`'s clicker is gated on
`for frame in page.frames: if "turnstile" in frame.url` — **an escalated
`interaction-only` widget exposes no iframe at all**, so that clicker can never
fire. That false negative is what produced the (now corrected) memory "Camoufox
cannot mint on any IP".

THE FIX, AND THE RULE IT ENCODES
--------------------------------
**Never gate the click on iframe presence.** A non-interactive Turnstile widget
in `interaction-only` mode exposes no iframe at all, so an iframe gate can never
fire — that false negative is what produced the (now corrected) memory "Camoufox
cannot mint on any IP". The rect of every lane is logged on every transition and
on a heartbeat, so a zero-mint run says where it stopped instead of being
mysterious.

⚠️ 2026-07-31 — THE RECT GATE IS A NO-OP, AND WORSE THAN NOTHING. USE
`--click-gate escalation`.
------------------------------------------------------------------------
The rect gate was supposed to be that trigger. It is not one. `slot` is created
as a **fixed 300x70 div**, so `unionRect(slot)` clears `w>=20 && h>=20` from the
instant it is appended — the gate opens **~5 ms after build()** while Cloudflare
escalates (`before-interactive-callback`) at **1,481-2,225 ms**. The clicker
therefore fires blind, ~1.5 s BEFORE the challenge asks for interaction, and
then again every 3 s.

Those premature clicks **destroy the widget**. Across 9 instrumented arms,
`cf_errors.timeout` equalled the dead-generation count EXACTLY in all four
rect-gate arms (12/12, 10/10, 46/46, 9/9); each dead generation costs 122 s —
Cloudflare's interactive timeout — and they consumed **40.6% of all lane-time**.
Twelve failures cost more than two hundred successes.

Measured, one variable, 2 lanes, both arms: switching the trigger to the
escalation callback took rate **6.66 -> 9.33 tok/min (+40%)**,
`cf_errors.timeout` **12 -> 0**, dead generations **12 -> 0**, clicks/token
**2.79 -> 1.49**, build->token p50 **-19%**, with **acceptance unchanged at
100%**.

So: **the TRIGGER must be `before-interactive-callback`.** Keep the rect as a
secondary sanity check if you like (`--click-gate both`) — a widget whose rect
is genuinely `0x0` still cannot be clicked — but never as the trigger.

THE PRIMARY METRIC IS THE SOLVE PATH, NOT THE TOKEN COUNT
---------------------------------------------------------
Every token is tagged `solve=auto` or `solve=int`, using the SAME definition as
repo 1's Chromium producer (`before-interactive-callback` fired for this widget
generation) so the two arms are directly comparable. The hypothesis under test is
that acceptance tracks the solve path — that Camoufox's stealth stops Cloudflare
escalating at all, and that an unescalated challenge yields a token gartic will
take. A Camoufox token that had to be clicked is expected to be refused exactly
like a Chromium one, so `auto%` is the number that matters.

CLICK DELIVERY
--------------
Camoufox is Firefox-based: there is no CDP and no `Input.dispatchMouseEvent`.
Two drivers, tried in order, because a coordinate bug and a Cloudflare refusal
look identical from the outside and that ambiguity has already cost this project
multi-day misdiagnoses:

  1. **xdotool / XTEST** under Xvfb. Real X input; `isTrusted=true`; the driver
     repo 1 uses. Screen coordinates are derived from the X window geometry
     (`xdotool getwindowgeometry`) plus the page's *inner* size — deliberately
     NOT from `window.screenX`/`outerHeight`, which Camoufox SPOOFS as part of
     the fingerprint and which would silently aim the pointer at nothing.
  2. **Playwright `page.mouse`** — Camoufox's own humanized cursor path. Needs
     only page coordinates, so it cannot be defeated by a screen-geometry error.
     Used automatically once a lane has taken `--click-retry` xdotool clicks and
     is still unsolved.

Whichever driver solved a lane is logged, so "the clicker never fired" is never
again mistaken for "Cloudflare refused us".

WATCHDOGS ARE ≥20 MINUTES ON PURPOSE
------------------------------------
An escalated-and-clicked challenge can take minutes. The old 90-second stall
reload and 120-second periodic reload destroyed challenges that were still in
progress: a watchdog shorter than the work it supervises causes the failure it
watches for. Defaults here are 1500 s stall and no periodic reload.
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import contextlib
import json
import math
import os
import queue
import random
import re
import shutil
import subprocess
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

from camoufox.async_api import AsyncCamoufox
from camoufox import DefaultAddons

TARGET_URL = "https://gartic.io"
SITEKEY = "0x4AAAAAABBPKaIbNwnPEfSo"

# Prefs that remove work this workload never needs. Measured on the Oracle VM
# 2026-07-24 together with --no-ubo: -14% CPU per token, -29% peak RSS,
# acceptance unchanged (8/8 JOINED). Camoufox's anti-detection is compiled into
# the browser rather than delivered by prefs, which is why the fingerprint
# survives — but acceptance is re-verified per arm anyway, because a rate gain
# that costs acceptance is worthless.
#
# ⚠️ On an IDLE GitHub runner these convert to ZERO extra tokens: nothing there
# is waiting on CPU. They are for the Oracle VM, which runs at ~1% idle, and
# that is where the original win was measured.
LEAN_PREFS = {
    "media.rdd-process.enabled": False,
    "media.gpu-process-decoder": False,
    "media.autoplay.default": 5,
    "media.peerconnection.enabled": False,
    "media.webspeech.synth.enabled": False,
    "toolkit.telemetry.enabled": False,
    "toolkit.telemetry.unified": False,
    "datareporting.healthreport.uploadEnabled": False,
    "browser.safebrowsing.malware.enabled": False,
    "browser.safebrowsing.phishing.enabled": False,
    "browser.safebrowsing.downloads.enabled": False,
    "extensions.blocklist.enabled": False,
    "app.update.enabled": False,
    "browser.search.update": False,
    "network.dns.disablePrefetch": True,
    "network.prefetch-next": False,
    "browser.sessionstore.interval": 3600000,
    "browser.cache.disk.enable": False,
    "dom.ipc.processCount": 1,
    "dom.ipc.processPrelaunch.enabled": False,
}

# Camoufox v152 yields zero tokens on this sitekey (error 600010); v135 works.
# The workflow places that exact build by hand and passes --executable, because
# `camoufox fetch` installs v152.
DEFAULT_EXECUTABLE = ""
DEFAULT_FF_VERSION = 135

# ---------------------------------------------------------------------------
# In-page renderer. Camoufox isolates page.evaluate from the main world, so
# window.turnstile is invisible to Python and EVERYTHING that touches it has to
# live here and report back over console.log. That constraint is why the console
# protocol below exists rather than a nice evaluate() API.
#
#   GEOM iw=.. ih=.. ow=.. oh=.. dpr=..        page geometry, once per load
#   RECT lane=.. gen=.. w=.. h=.. x=.. y=..    widget rect (transition + heartbeat)
#   INTER lane=.. gen=..                       before-interactive fired (= escalated)
#   CLICKAT lane=.. gen=.. px=.. py=.. w=.. h=..   a click is wanted here (page coords)
#   TOK <lane> <gen> <auto|int> <token>        a token, tagged with its solve path
#   E:<code>                                   Cloudflare error-callback code
# ---------------------------------------------------------------------------
RENDERER_JS = r"""
(function () {
  var KEY = '__SITEKEY__';
  var LANES = __LANES__;
  var APPEARANCE = '__APPEARANCE__';
  var RESET_MS = __RESET_DELAY_MS__;
  var CLICK_MIN_W = __CLICK_MIN_W__;
  var CLICK_MIN_H = __CLICK_MIN_H__;
  var CLICK_GATE = '__CLICK_GATE__';   // rect | escalation | both
  var REARM_MS = __CLICK_REARM_MS__;   // retry spacing, non-rect gates only

  function log(s) { try { console.log(s); } catch (e) {} }

  document.body.innerHTML = '';
  var host = document.createElement('div');
  host.id = 'lanes';
  document.body.appendChild(host);

  function geom() {
    log('GEOM iw=' + window.innerWidth + ' ih=' + window.innerHeight +
        ' ow=' + window.outerWidth + ' oh=' + window.outerHeight +
        ' dpr=' + (window.devicePixelRatio || 1));
  }

  // ── LANE LAYOUT IS A GRID, NOT A ROW, AND THAT IS LOAD-BEARING ────────────
  // Each widget needs ~320px of width. The original layout put lane i at
  // left = 20 + i*320 on a single row, so on the standard 1280px window lane 4
  // landed at left=1300 — entirely outside the viewport. An off-screen widget
  // never lays out, its rect stays 0x0, the rect gate never opens and the
  // xdotool clicker never fires: Cloudflare then times out with err 300030 and
  // the arm mints ZERO. That is precisely the failure that produced the false
  // "Camoufox cannot mint on any IP" conclusion, and a single-row layout brings
  // it back the instant anyone raises --lanes past 3. Wrapping into rows keeps
  // every lane on screen at the SAME window size, which matters because the
  // window is part of the fingerprint and the fingerprint is what decides
  // acceptance — widening the window to fit more lanes would change the very
  // variable under test.
  //
  // For lanes <= 3 on a 1280px window this is byte-identical to the old layout
  // (cols() == 3, so col == i and row == 0), so the proven 2-lane behaviour is
  // untouched.
  var CELL_W = 320, CELL_H = 90, SLOT_W = 300, SLOT_H = 70, PAD = 20;
  function cols() {
    return Math.max(1, Math.floor((window.innerWidth - PAD) / CELL_W));
  }

  // Union of the slot's own rect and every descendant's. Cloudflare sometimes
  // keeps the container collapsed and renders the interactive challenge into a
  // child that is positioned over it; taking the union means the rect gate sees
  // that too instead of reading 0x0 forever.
  function unionRect(el) {
    var r = el.getBoundingClientRect();
    var x1 = r.left, y1 = r.top, x2 = r.right, y2 = r.bottom;
    var kids = el.querySelectorAll('*');
    for (var k = 0; k < kids.length; k++) {
      var kr = kids[k].getBoundingClientRect();
      if (kr.width <= 0 || kr.height <= 0) continue;
      if (kr.left < x1) x1 = kr.left;
      if (kr.top < y1) y1 = kr.top;
      if (kr.right > x2) x2 = kr.right;
      if (kr.bottom > y2) y2 = kr.bottom;
    }
    return { left: x1, top: y1, width: Math.max(0, x2 - x1), height: Math.max(0, y2 - y1) };
  }

  function lane(i) {
    var gen = 0, id = null, slot = null, sawInter = false, emitted = false;
    var lastW = -1, lastH = -1, ticks = 0;
    // CLICK GATE state. `wantClick` is set by before-interactive-callback and
    // is the only thing that opens a non-rect gate; `lastArm` spaces retries.
    var wantClick = false, lastArm = 0, nClickAt = 0;

    function build() {
      gen++;
      var myGen = gen;
      sawInter = false; emitted = false; lastW = -1; lastH = -1; ticks = 0;
      wantClick = false; lastArm = 0; nClickAt = 0;
      if (slot && slot.parentNode) slot.parentNode.removeChild(slot);
      slot = document.createElement('div');
      slot.id = 'lane_' + i + '_' + myGen;
      // The class stock Turnstile helpers look for. Our driver finds the widget
      // by rect and does not need it; it costs one line and keeps any future
      // off-the-shelf clicker able to locate us.
      slot.className = 'cf-turnstile';
      // Fixed and anchored to the TOP-left, filling rightwards then downwards:
      // a window shorter or narrower than expected still shows the early lanes,
      // whereas a bottom- or right-anchored widget can land outside the visible
      // area and then no pointer on earth can reach it.
      var c = cols();
      var col = i % c, row = Math.floor(i / c);
      var left = PAD + col * CELL_W, top = PAD + row * CELL_H;
      // Loud, not silent. A lane that cannot fit is a lane that will mint
      // nothing while every counter still looks healthy, so say so once per
      // build rather than letting it read as a Cloudflare refusal.
      if (top + SLOT_H > window.innerHeight) {
        log('E:lane_offscreen lane=' + i + ' top=' + top + ' ih=' + window.innerHeight);
      }
      slot.style.cssText = 'position:fixed;left:' + left + 'px;top:' + top +
                           'px;width:' + SLOT_W + 'px;height:' + SLOT_H +
                           'px;z-index:2147483647';
      host.appendChild(slot);
      try {
        id = window.turnstile.render(slot, {
          sitekey: KEY,
          action: 'join',                 // MANDATORY since 2026-07-23
          appearance: APPEARANCE,
          callback: function () { /* token is read via getResponse(); the
                                     callback has been observed firing EMPTY */ },
          'error-callback': function (c) { log('E:' + c); defer(myGen, 2500); },
          'expired-callback': function () { defer(myGen, 0); },
          'timeout-callback': function () { log('E:timeout'); defer(myGen, 0); },
          'before-interactive-callback': function () {
            // THIS is the "a click is wanted, now" signal. Cloudflare calls it
            // exactly when it escalates a widget to interactive.
            sawInter = true; wantClick = true;
            log('INTER lane=' + i + ' gen=' + myGen);
          },
          'after-interactive-callback': function () {
            log('AFTERINT lane=' + i + ' gen=' + myGen);
          }
        });
      } catch (e) {
        log('E:render:' + (e && e.message)); defer(myGen, 3000); return;
      }
      poll(0, myGen);
    }

    function read() {
      try { var t = window.turnstile.getResponse(id); if (t) return t; } catch (e) {}
      var el = slot && slot.querySelector('input[name="cf-turnstile-response"]');
      return (el && el.value) ? el.value : null;
    }

    function poll(n, myGen) {
      if (myGen !== gen) return;              // a stale chain must die, not recycle
      var t = read();
      if (t && !emitted) {
        emitted = true;
        log('TOK ' + i + ' ' + myGen + ' ' + (sawInter ? 'int' : 'auto') + ' ' + t);
        defer(myGen, RESET_MS);
        return;
      }
      if (slot) {
        var r = unionRect(slot);
        var w = Math.round(r.width), h = Math.round(r.height);
        ticks++;
        // Log on every transition, and on a heartbeat, so a run that mints
        // nothing still shows whether the widget ever escalated.
        if (w !== lastW || h !== lastH || ticks % 10 === 0) {
          log('RECT lane=' + i + ' gen=' + myGen + ' w=' + w + ' h=' + h +
              ' x=' + Math.round(r.left) + ' y=' + Math.round(r.top) +
              ' inter=' + (sawInter ? 1 : 0));
          lastW = w; lastH = h;
        }
        // ── THE CLICK GATE ──────────────────────────────────────────────
        // Never `if (iframe)` — an escalated interaction-only widget exposes
        // no iframe, which is exactly the bug this file was written to fix.
        // Which signal opens the gate is `--click-gate`:
        //
        //   rect       the historical behaviour, and a NO-OP. `slot` is a FIXED
        //              300x70 div, so unionRect(slot) is >= 300x70 from the
        //              instant it is appended — before render() has drawn
        //              anything and before Cloudflare has decided whether to
        //              escalate. Measured on the probe: the gate opens 5 ms
        //              after build() while before-interactive-callback fires at
        //              1481-2225 ms, so every widget is clicked ~1.5 s BEFORE a
        //              click is wanted, and again every --click-interval after.
        //   escalation before-interactive-callback fired, and only then. One
        //              click per escalation, REARM_MS the only retry.
        //   both       escalated AND laid out.
        //
        // Why it matters: across four rect-gate probe arms `cf_errors.timeout`
        // equalled the DEAD-GENERATION count exactly (12/12, 10/10, 46/46,
        // 9/9), a dead generation costs 122 s (Cloudflare's interactive
        // timeout), and dead generations ate 40.6% of all lane-time. Gating on
        // escalation instead measured 9.33 vs 6.66 tok/min (+40%) with
        // cf_errors.timeout = 0 and acceptance unchanged at 100%.
        var rectOK = (w >= CLICK_MIN_W && h >= CLICK_MIN_H);
        var fire = (CLICK_GATE === 'rect') ? rectOK
                 : (CLICK_GATE === 'escalation') ? wantClick
                 : (wantClick && rectOK);
        if (fire && CLICK_GATE !== 'rect') {
          // Re-arm, never spray: a click that has not been answered yet is not
          // evidence that another click is wanted, and clicking a challenge
          // that is mid-verification is how a solve gets thrown away and is
          // reported back to us as cf_errors.timeout.
          var tn = Date.now();
          if (nClickAt > 0 && (tn - lastArm) < REARM_MS) fire = false;
          else lastArm = tn;
        }
        if (fire) {
          nClickAt++;
          // The checkbox sits at the left end of a full-width widget; on a
          // small box aim at its centre instead.
          var ox = w >= 200 ? 30 : Math.round(w / 2);
          log('CLICKAT lane=' + i + ' gen=' + myGen +
              ' px=' + Math.round(r.left + ox) +
              ' py=' + Math.round(r.top + h / 2) +
              ' w=' + w + ' h=' + h);
        }
      }
      // ~10 minutes of polling before giving up on a generation. Cloudflare's
      // own interactive timeout is shorter, and its timeout-callback recycles
      // us; this is only the backstop for a widget that goes silent entirely.
      if (n > 600) { defer(myGen, 0); return; }
      setTimeout(function () { poll(n + 1, myGen); }, 1000);
    }

    // defer()/recycle() are the ONLY route back to build(). Do not add another
    // caller: five different callbacks can fire for one generation, and a
    // second entry point is how repo 1 grew a 2,577-widget fork bomb.
    function defer(myGen, ms) {
      if (myGen !== gen) return;
      setTimeout(function () { recycle(myGen); }, ms);
    }
    function recycle(myGen) {
      if (myGen !== gen) return;
      try { window.turnstile.remove(id); } catch (e) {}
      id = null;
      setTimeout(build, 400);
    }

    build();
  }

  function start() {
    geom();
    for (var i = 0; i < LANES; i++) {
      (function (idx) { setTimeout(function () { lane(idx); }, idx * 1500); })(i);
    }
  }

  if (window.turnstile && window.turnstile.render) {
    start();
  } else {
    var script = document.createElement('script');
    script.src = 'https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit';
    script.onload = start;
    script.onerror = function () { log('E:apiload'); };
    document.head.appendChild(script);
  }
})();
"""


def build_renderer(args) -> str:
    return (
        RENDERER_JS.replace("__SITEKEY__", SITEKEY)
        .replace("__LANES__", str(args.lanes))
        .replace("__APPEARANCE__", args.appearance)
        .replace("__RESET_DELAY_MS__", str(int(args.token_interval * 1000)))
        .replace("__CLICK_MIN_W__", str(args.click_min_w))
        .replace("__CLICK_MIN_H__", str(args.click_min_h))
        .replace("__CLICK_GATE__", args.click_gate)
        .replace("__CLICK_REARM_MS__", str(int(args.click_rearm_ms)))
    )


# ---------------------------------------------------------------------------
# Console line parsers
# ---------------------------------------------------------------------------
RE_GEOM = re.compile(r"^GEOM iw=(\d+) ih=(\d+) ow=(\d+) oh=(\d+) dpr=([\d.]+)")
RE_RECT = re.compile(r"^RECT lane=(\d+) gen=(\d+) w=(-?\d+) h=(-?\d+) x=(-?\d+) y=(-?\d+) inter=(\d)")
RE_CLICK = re.compile(r"^CLICKAT lane=(\d+) gen=(\d+) px=(-?\d+) py=(-?\d+) w=(\d+) h=(\d+)")
RE_TOK = re.compile(r"^TOK (\d+) (\d+) (auto|int) (\S+)$")


def parse_proxy(raw: str) -> dict:
    """``scheme://[user:pass@]host:port`` or ``host:port[:user:pass]``."""
    raw = raw.strip()
    if "://" in raw:
        parsed = urllib.parse.urlparse(raw)
        if not parsed.hostname or not parsed.port:
            raise SystemExit(f"PROXY url needs a host and port: {raw!r}")
        proxy = {"server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
        if parsed.username:
            proxy["username"] = urllib.parse.unquote(parsed.username)
            proxy["password"] = urllib.parse.unquote(parsed.password or "")
        return proxy
    parts = raw.split(":", 3)
    if len(parts) == 2:
        return {"server": f"http://{parts[0]}:{parts[1]}"}
    if len(parts) == 4:
        return {"server": f"http://{parts[0]}:{parts[1]}",
                "username": parts[2], "password": parts[3]}
    raise SystemExit("PROXY must be scheme://host:port or HOST:PORT[:USER:PASS]")


# ---------------------------------------------------------------------------
# X11 click driver
# ---------------------------------------------------------------------------
# CLICK COST PROFILES.
#
# The click is ~620 ms of wall time and ~92% of that is DELIBERATE SLEEP, not
# I/O: 16 waypoints x ~19 ms of inter-step pause + ~160 ms settle + ~100 ms hold.
# Note what it is NOT — the waypoints are already chained into ONE `xdotool`
# invocation (one process, one X connection), so "spawn a helper that holds a
# persistent X connection" cannot recover the 620 ms; at most it recovers the
# two EXTRA spawns that `mousedown`/`mouseup` cost.
#
# THE ACTUAL DEFECT, measured 2026-08-01 and much larger than the sleep budget.
# `xdotool mousemove --sync X Y` waits for a MotionNotify. A move to the position
# the pointer is ALREADY at generates none, so it blocks — until some OTHER
# thread moves the pointer, or the 25 s subprocess timeout. A cubic ease crawls
# sub-pixel at both ends, so consecutive waypoints round to the same integer
# constantly. Measured on the live VM lanes over 14,383 real clicks: p50 743 ms
# but 27-28% >= 1 s and 13-16% >= 5 s, p90 6.0-8.4 s, MEAN 2.0-2.2 s — i.e.
# 0.46-0.51 clicks/s, not the 1.6/s the p50 implies. It self-limits around 8.5 s
# rather than the timeout because a SIBLING LANE's motion releases the block,
# which is why a single lane is the worst case and why p50-based readings missed
# it entirely: the whole pathology lives in the tail.
#
# The three levers that are genuinely free (identical pointer positions,
# identical programmed timing, so an identical trace reaches Cloudflare):
#   * `dedupe` — skip a mousemove whose INTEGER position equals the previous
#                one, keeping its dwell. The page cannot observe a zero-length
#                move, so nothing is lost; only the impossible wait goes.
#   * `sync`   — drop `--sync` outright. The X server still processes the
#                motions in order at the same wall-clock times; only our
#                confirmation wait goes away.
#   * `batch`  — fold mousedown/hold/mouseup into the same invocation via
#                `xdotool click --delay <hold> 1`: 3 spawns -> 1. Worth ~35 ms,
#                and it shrinks the window in which another lane can move the
#                pointer between our final move and our mousedown.
# Everything below those DEGRADES THE MOTION and is a stealth trade-off, not a
# free saving. Measure acceptance, never rate alone.
#
# `base` is exactly the behaviour that shipped, so an unflagged run is unchanged.
CLICK_PROFILES: dict[str, dict] = {
    # the control: 12-20 waypoints, --sync on every one, 3 process spawns
    "base": dict(waypoints=(12, 20), step_ms=(8, 30), settle_ms=(90, 230),
                 hold_ms=(60, 140), approach_px=(80, 220), sync=True, batch=False,
                 dedupe=False),
    # THE CONSERVATIVE FIX. Byte-for-byte `base` — same --sync, same three
    # spawns, same everything — except that a mousemove to the pixel the pointer
    # already occupies is dropped and its dwell kept. Smallest possible diff from
    # production, and it retains the synchronisation semantics.
    "dedupe": dict(waypoints=(12, 20), step_ms=(8, 30), settle_ms=(90, 230),
                   hold_ms=(60, 140), approach_px=(80, 220), sync=True, batch=False,
                   dedupe=True),
    # IMPLEMENTATION-ONLY, the other way: no XSync at all, one spawn. Same
    # waypoint count, same jitter, same sleeps, same approach geometry.
    "impl": dict(waypoints=(12, 20), step_ms=(8, 30), settle_ms=(90, 230),
                 hold_ms=(60, 140), approach_px=(80, 220), sync=False, batch=True,
                 dedupe=False),
    # (A `batch` profile — --sync kept, 3 spawns folded into 1 — was measured and
    # REMOVED. Batching is worth ~35 ms, it is not the mechanism, and because it
    # keeps --sync it still emits ~0.3 zero-length moves per click: it looks like
    # a fix and is not one. check_click_profiles.py now fails any profile that
    # keeps --sync without deduping, so it cannot come back by accident.)
    # DEGRADED MOTION, mild: a third of the waypoints, a third of the dwell.
    "fast": dict(waypoints=(5, 8), step_ms=(4, 12), settle_ms=(45, 90),
                 hold_ms=(40, 80), approach_px=(60, 160), sync=False, batch=True,
                 dedupe=False),
    # DEGRADED MOTION, extreme: no approach at all — the pointer teleports to
    # the target, settles briefly and clicks. This is the cheapest click that is
    # still a click, and the one most likely to cost acceptance.
    "min": dict(waypoints=(0, 0), step_ms=(0, 0), settle_ms=(20, 40),
                hold_ms=(30, 50), approach_px=(0, 0), sync=False, batch=True,
                dedupe=False),
}


class XdoDriver:
    """Click through XTEST, addressing the browser window found on $DISPLAY.

    Screen coordinates come from the X server's own view of the window, never
    from `window.screenX`/`outerHeight`: Camoufox spoofs those as part of the
    fingerprint, and trusting them aims the pointer at empty space while every
    log line still looks healthy.
    """

    def __init__(self, display: str, profile: str = "base") -> None:
        self.display = display
        self.available = bool(shutil.which("xdotool")) and bool(display)
        self.win: tuple[int, int, int, int] | None = None   # x, y, w, h
        self.win_at = 0.0
        self.fail = 0
        self.profile_name = profile
        self.profile = CLICK_PROFILES[profile]
        # Per-click cost breakdown, refreshed by click(). `prog_ms` is the time
        # we ASKED for (the humanisation sleeps); the gap between it and the
        # measured wall time is everything X and the process table cost us, and
        # it is the only part a better implementation can ever recover.
        self.last: dict = {}

    def _run(self, *cmd: str, timeout: float = 10.0) -> str:
        env = dict(os.environ, DISPLAY=self.display)
        out = subprocess.run(cmd, capture_output=True, text=True,
                             timeout=timeout, env=env)
        return out.stdout or ""

    def window(self) -> tuple[int, int, int, int] | None:
        """Largest visible window on this display — Camoufox is the only client."""
        now = time.monotonic()
        if self.win and (now - self.win_at) < 30:
            return self.win
        try:
            ids = [w for w in self._run("xdotool", "search", "--onlyvisible",
                                        "--name", ".").split() if w.strip()]
            best = None
            for wid in ids:
                geo = self._run("xdotool", "getwindowgeometry", "--shell", wid)
                vals = dict(
                    line.split("=", 1) for line in geo.splitlines() if "=" in line
                )
                try:
                    x, y = int(vals["X"]), int(vals["Y"])
                    w, h = int(vals["WIDTH"]), int(vals["HEIGHT"])
                except (KeyError, ValueError):
                    continue
                if w < 200 or h < 200:
                    continue
                if best is None or w * h > best[2] * best[3]:
                    best = (x, y, w, h)
            self.win = best
            self.win_at = now
            return best
        except Exception as error:  # noqa: BLE001 — a missing tool is not fatal
            print(f"  [xdo] window lookup failed: {type(error).__name__}", flush=True)
            self.available = False
            return None

    def to_screen(self, px: int, py: int, geom: dict) -> tuple[int, int] | None:
        """Page point -> X screen point, using ONLY real X geometry + inner size.

        chromeTop = window height - rendered content height. padX splits the
        remaining horizontal border. Both are measured, not reported by a page
        that is actively lying about its screen.
        """
        win = self.window()
        if not win or not geom:
            return None
        wx, wy, ww, wh = win
        dpr = geom.get("dpr", 1.0) or 1.0
        iw = geom.get("iw", 0) * dpr
        ih = geom.get("ih", 0) * dpr
        if iw <= 0 or ih <= 0:
            return None
        chrome_top = wh - ih
        pad_x = max(0.0, (ww - iw) / 2.0)
        # A spoofed or stale inner size shows up here as an absurd border. Refuse
        # rather than click into the void — a wrong click and no click look the
        # same afterwards, and only one of them is diagnosable.
        if chrome_top < -4 or chrome_top > 400 or pad_x > 200:
            print(f"  [xdo] implausible chrome: win={ww}x{wh} inner={iw:.0f}x{ih:.0f} "
                  f"top={chrome_top:.0f} padx={pad_x:.0f} — declining xdotool",
                  flush=True)
            return None
        sx = int(round(wx + pad_x + px * dpr))
        sy = int(round(wy + max(0.0, chrome_top) + py * dpr))
        return sx, sy

    def click(self, sx: int, sy: int) -> bool:
        """Approach from a random angle, ease in, overshoot, correct, hold.

        Shape and cost come from the active CLICK_PROFILES entry; `base`
        reproduces the historical sequence exactly.
        """
        if not self.available:
            return False
        prof = self.profile
        try:
            steps: list[str] = []
            move = ["mousemove", "--sync"] if prof["sync"] else ["mousemove"]
            prog = 0.0                       # seconds of sleep we asked for
            dedupe = prof.get("dedupe", False)
            last_pt: tuple[int, int] | None = None
            skipped = 0
            lo_n, hi_n = prof["waypoints"]
            n = random.randint(lo_n, hi_n) if hi_n > 0 else 0
            if n > 0:
                ang = random.uniform(0, 6.283)
                dist = random.uniform(*prof["approach_px"])
                ox = sx + dist * math.cos(ang)
                oy = sy + dist * math.sin(ang)
                ox = min(max(ox, 5), 4000)
                oy = min(max(oy, 5), 4000)
                tx = sx + random.uniform(-4, 4)
                ty = sy + random.uniform(-3, 3)
                step_lo, step_hi = prof["step_ms"]
                for i in range(1, n + 1):
                    t = i / n
                    e = 4 * t * t * t if t < 0.5 else 1 - ((-2 * t + 2) ** 3) / 2
                    pause = random.uniform(step_lo, step_hi) / 1000.0
                    prog += pause
                    px = int(ox + (tx - ox) * e + random.uniform(-1.2, 1.2))
                    py = int(oy + (ty - oy) * e + random.uniform(-1.2, 1.2))
                    if dedupe and last_pt == (px, py):
                        # A move to the pixel the pointer already occupies is
                        # invisible to the page and impossible for --sync to
                        # observe. Keep the DWELL, drop the dead request.
                        steps += ["sleep", f"{pause:.3f}"]
                        skipped += 1
                        continue
                    last_pt = (px, py)
                    steps += move + [str(px), str(py), "sleep", f"{pause:.3f}"]
            settle = random.uniform(*prof["settle_ms"]) / 1000.0
            prog += settle
            if dedupe and last_pt == (sx, sy):
                steps += ["sleep", f"{settle:.3f}"]
                skipped += 1
            else:
                steps += move + [str(sx), str(sy), "sleep", f"{settle:.3f}"]
            hold = random.uniform(*prof["hold_ms"]) / 1000.0
            prog += hold
            started = time.monotonic()
            if prof["batch"]:
                # ONE process, one X connection, mousedown/hold/mouseup included.
                steps += ["click", "--delay", str(max(1, int(round(hold * 1000)))), "1"]
                self._run("xdotool", *steps, timeout=25)
                spawns = 1
            else:
                self._run("xdotool", *steps, timeout=25)
                self._run("xdotool", "mousedown", "1", timeout=5)
                time.sleep(hold)
                self._run("xdotool", "mouseup", "1", timeout=5)
                spawns = 3
            self.last = {
                "prof": self.profile_name,
                "prog_ms": round(prog * 1000, 1),
                "wall_ms": round((time.monotonic() - started) * 1000, 1),
                "wp": n,
                "spawns": spawns,
                # Zero-length moves dropped. On `base` this is the count that
                # WOULD have stalled; anything above 0 is a click production is
                # currently paying up to 25 s for.
                "nodup": skipped,
            }
            self.fail = 0
            return True
        except Exception as error:  # noqa: BLE001
            self.fail += 1
            print(f"  [xdo] click failed ({self.fail}): {type(error).__name__}", flush=True)
            if self.fail >= 5:
                self.available = False
                print("  [xdo] disabling xdotool after 5 failures; page.mouse only",
                      flush=True)
            return False


class Stats:
    def __init__(self) -> None:
        self.tokens = 0
        self.auto = 0
        self.inter = 0
        self.clicks = 0
        self.clicks_xdo = 0
        self.clicks_mouse = 0
        self.solved_after_click = 0
        # Clicks spent on generations that DID mint. Everything else was spent
        # on a generation that died — which is the cost the escalation gate
        # exists to remove, and it is invisible in any token-keyed metric.
        self.clicks_on_token = 0
        self.errors: dict[str, int] = {}
        self.rect_max: dict[int, tuple[int, int]] = {}
        self.escalations = 0
        self.started = time.monotonic()
        self.last_token_at = time.monotonic()
        self.posted = 0
        self.post_fail = 0
        # Tokens handed to the self-verifier instead of the relay. They are
        # DIVERTED, never duplicated: a token is single-use, so posting one and
        # then replaying it would hand icebot a spent token and manufacture a
        # code-5 that has nothing to do with acceptance.
        self.diverted = 0
        # First beacon early so a broken run is visible in ~3 minutes rather
        # than at the first slow interval; lives on Stats so it survives the
        # browser restarts that end each session.
        self.next_beacon = self.started + 180.0

    def rate_per_min(self) -> float:
        elapsed = time.monotonic() - self.started
        return (self.tokens / elapsed * 60.0) if elapsed > 0 else 0.0

    def auto_pct(self) -> float:
        return (self.auto / self.tokens * 100.0) if self.tokens else 0.0

    def note_error(self, code: str) -> None:
        self.errors[code] = self.errors.get(code, 0) + 1


# ---------------------------------------------------------------------------
# SELF-VERIFICATION — the runner measures its OWN acceptance
#
# WHY THE RUNNER HAS TO DO THIS ITSELF
# ------------------------------------
# The relay serves production from `SERVE_TRUSTED_LABELS=vm-`, so icebot never
# draws a GitHub token and the relay therefore learns nothing about a runner's
# acceptance — its GitHub exits read `wJ=0 wR=0` forever. An external sampler can
# be pointed at `/assign?src=<exit>`, but that is a human running a script on
# another box, which is exactly what is unavailable at 03:00 on hour four of a
# 300-minute run. A self-healing producer needs a feedback signal it owns.
#
# WHAT THE OLD `joinverify` DID WRONG (all four verified 2026-07-31)
# ------------------------------------------------------------------
#  1. It discovered with `/server/?check=1&v3=1&lang=19` and joined with
#     `42[1,{…,"idioma":19}]` — PUBLIC MATCHMAKING. On 2026-07-30 ~22:46Z gartic
#     began answering that path with `42["6",1]` for every language on every
#     exit, INCLUDING a residential control that specific-room joins accepted in
#     the same minute. code 1 is a room-state rejection that lands BEFORE the
#     token is read, so the verdict was decided before the token was examined.
#  2. The workflow scored it with `case "$V" in JOINED*) accepted++` — a
#     DENY-LIST. Measured just now with a live VM token against a full room:
#     `{"bucket":"ACCEPTED_roomfull","code":"3"}`. The Turnstile gate runs before
#     the capacity check and before the already-playing dedup check, so code 3
#     and code 4 PROVE acceptance — and that line scored both as refusals. A
#     100%-accepted arm could read 0%.
#  3. code 1, Cloudflare 1015 and every dial error landed in the DENOMINATOR.
#  4. Fixed nick `probe`, one un-rotatable target: gartic's per-(IP,room) re-join
#     lock (~45-60 s) then manufactures code 4, which (2) also counted as a loss.
#
# `verifier/main.go` (roomverify) fixes all four and emits one JSON line with a
# bucket from an allow-list. Only ACCEPTED_* and REFUSED_token may enter a ratio.
#
# THE ONE CAVEAT THAT REMAINS, STATED HONESTLY
# --------------------------------------------
# This verifies through the same tunnel that minted the token, seconds after it
# was minted. It therefore cannot see a *staleness* failure (icebot redeems
# tokens up to minutes old) and it is not a substitute for an off-box control on
# a different physical machine. What it CAN see — and what the ladder needs — is
# whether gartic's siteverify is refusing THIS producer's tokens right now.
# ---------------------------------------------------------------------------
ACCEPT_BUCKETS = frozenset(
    ("ACCEPTED_joined", "ACCEPTED_roomfull", "ACCEPTED_alreadyplaying")
)
REFUSE_BUCKET = "REFUSED_token"


class Ladder:
    """The escalation policy. Rungs change the BROWSER FINGERPRINT, never the
    network.

    Exit rotation is deliberately absent. It is on the do-not-re-run list (a
    network-wide cut is not per-runner, so rotating the exit has never once
    helped), and it would confound the one variable measured to move acceptance:
    on 2026-07-31 01:40-04:49Z, Camoufox scored 192 accepted / 0 refused across 3
    exits while a co-located Chromium on the SAME host, SAME ASN, SAME WARP
    egress and the SAME minutes scored 2/108 and 0/119.
    """

    # 0 is the baseline; escalation walks forward and wraps back to 1, because
    # if the fingerprint is the lever then cycling fingerprints is the correct
    # steady state, not sitting on a dead one.
    NAMES = ("baseline", "fp-regen", "fp-os-swap", "alt-build", "chromium")
    FAMILIES = ("windows", "macos", "linux")
    WINDOWS = ((1280, 1000), (1366, 900), (1600, 1000), (1440, 960))

    def __init__(self, args) -> None:
        self.args = args
        self.rung = 0
        self.family = 0
        self.geometry = 0
        self.wraps = 0
        self.entered_at = time.monotonic()
        self.restart = threading.Event()
        self.history: list[dict] = []
        # Per-rung allow-listed tallies, so the run's own log can answer "did
        # acceptance recover, and by how much" without external instrumentation.
        self.tally: dict[str, dict[str, int]] = collections.defaultdict(
            lambda: {"acc": 0, "ref": 0}
        )

    @property
    def name(self) -> str:
        return self.NAMES[self.rung]

    def key(self) -> str:
        return f"{self.rung}:{self.name}#{self.wraps}"

    def dwell_remaining(self) -> float:
        """Seconds this rung is still protected from escalation.

        Two reasons this exists. (1) Every escalation drops the browser, which
        costs 30-60 s of minting and destroys in-flight challenges — a ladder
        that re-rolls every four minutes would spend the run restarting.
        (2) gartic has GLOBAL, self-healing acceptance outages lasting ~18 to
        ≥60 minutes that no fingerprint can fix. The dwell grows with each full
        lap of the ladder, so a run that has already tried every rung twice
        slows down instead of thrashing against a cut it cannot influence.
        """
        budget = min(self.args.rung_min_dwell * (1 + self.wraps), 1800.0)
        return max(0.0, budget - (time.monotonic() - self.entered_at))

    def _next_rung(self) -> int:
        step = self.rung + 1
        while step < len(self.NAMES):
            if self.NAMES[step] == "alt-build" and not self.args.alt_executable:
                step += 1
                continue
            if self.NAMES[step] == "chromium" and not self.args.chromium_fallback:
                step += 1
                continue
            return step
        # Ladder exhausted: wrap to a fresh fingerprint rather than stop.
        self.wraps += 1
        return 1

    def escalate(self, reason: str, before: dict) -> None:
        old = self.key()
        held = time.monotonic() - self.entered_at
        self.rung = self._next_rung()
        if self.NAMES[self.rung] == "fp-os-swap":
            self.family = (self.family + 1) % len(self.FAMILIES)
            self.geometry = (self.geometry + 1) % len(self.WINDOWS)
        self.entered_at = time.monotonic()
        record = {
            "event": "escalate", "ts": time.time(), "reason": reason,
            "from": old, "to": self.key(), "held_s": round(held, 1),
            "before": before,
        }
        if self.NAMES[self.rung] == "fp-os-swap":
            record["forced_os"] = self.FAMILIES[self.family]
            record["window"] = "x".join(str(v) for v in self.WINDOWS[self.geometry])
        self.history.append(record)
        print("LADDER " + json.dumps(record, sort_keys=True), flush=True)
        # run_session polls this and returns, which drops the browser. Camoufox
        # regenerates its whole fingerprint on every launch, so a restart IS a
        # new fingerprint + a new process + a new profile directory; the rungs
        # differ in how much of that is left to chance.
        self.restart.set()

    def recovered(self, after: dict, seconds: float) -> None:
        record = {"event": "recovered", "ts": time.time(), "rung": self.key(),
                  "after": after, "recovery_s": round(seconds, 1)}
        self.history.append(record)
        print("LADDER " + json.dumps(record, sort_keys=True), flush=True)

    def launch_overrides(self) -> dict:
        """Fingerprint knobs for the CURRENT rung, applied at browser launch."""
        name = self.name
        if name in ("baseline", "fp-regen"):
            # Camoufox draws a brand-new fingerprint on every launch, so
            # fp-regen is a pure re-roll: same policy, new dice.
            return {}
        if name == "fp-os-swap":
            # Do not trust the dice twice. Force a DIFFERENT OS family and a
            # different window geometry so the second attempt is guaranteed to
            # look materially unlike the first, and flip humanize as well.
            return {
                "os": self.FAMILIES[self.family],
                "window": self.WINDOWS[self.geometry],
                "humanize": not self.args.humanize,
            }
        if name == "alt-build":
            return {"executable_path": self.args.alt_executable,
                    "ff_version": self.args.alt_ff_version}
        return {}


class Verifier(threading.Thread):
    """Diverts one freshly minted token every `--verify-every` seconds, redeems
    it against a ROTATING real room with a random nick, and drives the ladder."""

    def __init__(self, args, stats: Stats, ladder: Ladder) -> None:
        super().__init__(daemon=True, name="verifier")
        self.args = args
        self.stats = stats
        self.ladder = ladder
        self.want = threading.Event()
        self.slot: queue.Queue[str] = queue.Queue(maxsize=1)
        self.stop = threading.Event()
        self.rooms: list[str] = []
        self.rooms_at = 0.0
        self.rooms_fail = 0
        self.recent: collections.deque[str] = collections.deque(maxlen=args.verify_window)
        self.seen_rooms: collections.deque[str] = collections.deque(maxlen=12)
        self.buckets: dict[str, int] = collections.defaultdict(int)
        self.consecutive_refused = 0
        self.consecutive_accepted = 0
        self.collapsed_at = 0.0
        self.probing = False
        self.probe_start: dict = {}
        self.bins: dict[int, dict[str, int]] = collections.defaultdict(
            lambda: {"acc": 0, "ref": 0, "nov": 0}
        )

    # ── called from the asyncio loop ───────────────────────────────────────
    def offer(self, token: str) -> bool:
        """True when the verifier claimed this token — the caller must then NOT
        post it to the relay."""
        if not self.want.is_set():
            return False
        try:
            self.slot.put_nowait(token)
        except queue.Full:
            return False
        self.want.clear()
        return True

    def window_pct(self) -> tuple[float, int, int]:
        acc = sum(1 for b in self.recent if b in ACCEPT_BUCKETS)
        ref = sum(1 for b in self.recent if b == REFUSE_BUCKET)
        total = acc + ref
        return (100.0 * acc / total if total else 0.0), acc, total

    def snapshot(self) -> dict:
        pct, acc, total = self.window_pct()
        return {"acc": acc, "n": total, "pct": round(pct, 1),
                "streak_ref": self.consecutive_refused}

    # ── room roster ────────────────────────────────────────────────────────
    def _refresh_rooms(self) -> None:
        now = time.monotonic()
        if self.rooms and now - self.rooms_at < self.args.verify_room_ttl:
            return
        # gartic's /req/list answers 429 when hit hard; a failed refresh keeps
        # the previous roster and backs off rather than dialing a stale room
        # loop or, worse, falling back to `-lang N`.
        if now - self.rooms_at < 60 and self.rooms_fail:
            return
        def fetch(full: bool) -> list[str]:
            command = [self.args.verify_binary, "-list-rooms", self.args.verify_langs]
            if full:
                command.append("-full")
            if self.args.verify_proxy:
                command[1:1] = ["-proxy", self.args.verify_proxy]
            try:
                out = subprocess.run(command, capture_output=True, text=True, timeout=90)
                return [c.strip() for c in out.stdout.splitlines() if c.strip()]
            except Exception as error:  # noqa: BLE001
                print(f"  [verify] roster error {type(error).__name__}", flush=True)
                return []

        # PREFER FULL ROOMS. They answer code 3, which proves the token was
        # accepted, and the join adds nobody — so a 5-hour run measures its own
        # acceptance ~400 times without putting a single ghost player into
        # anyone's game. Fall back to the full listing only when there are too
        # few full rooms to rotate through.
        codes = fetch(True)
        if len(codes) < 8:
            time.sleep(3)  # /req/list answers 429 if the two calls are back to back
            codes = fetch(False)
            if codes:
                print(f"  [verify] too few full rooms; using the full listing",
                      flush=True)
        if codes:
            self.rooms = codes
            self.rooms_at = now
            self.rooms_fail = 0
            print(f"  [verify] roster {len(codes)} rooms", flush=True)
        else:
            self.rooms_fail += 1
            self.rooms_at = now
            print(f"  [verify] roster refresh FAILED ({self.rooms_fail}); "
                  f"keeping {len(self.rooms)}", flush=True)

    def _pick_room(self) -> str:
        # Rotate, and never revisit inside the last dozen dials: gartic holds a
        # per-(IP,room) re-join lock of ~45-60 s that manufactures code 4.
        pool = [r for r in self.rooms if r not in self.seen_rooms] or self.rooms
        room = random.choice(pool) if pool else ""
        if room:
            self.seen_rooms.append(room)
        return room

    # ── the loop ───────────────────────────────────────────────────────────
    def run(self) -> None:
        time.sleep(self.args.verify_start_delay)
        while not self.stop.is_set():
            self._refresh_rooms()
            room = self._pick_room()
            if not room:
                self.stop.wait(30)
                continue
            self.want.set()
            try:
                token = self.slot.get(timeout=self.args.verify_every * 3)
            except queue.Empty:
                self.want.clear()
                print("  [verify] no token offered — the producer is not "
                      "minting; issuance failure, not acceptance", flush=True)
                continue
            self.stats.diverted += 1
            nick = f"zz{random.randint(100000, 999999)}"
            command = [self.args.verify_binary, "-room", room, "-nick", nick,
                       "-timeout", "25"]
            if self.args.verify_proxy:
                command[1:1] = ["-proxy", self.args.verify_proxy]
            command.append(token)
            try:
                out = subprocess.run(command, capture_output=True, text=True, timeout=90)
                line = (out.stdout or "").strip().splitlines()
                payload = json.loads(line[-1]) if line else {}
            except Exception as error:  # noqa: BLE001
                payload = {"bucket": "NOVERDICT_other", "detail": type(error).__name__}
            self._record(payload, room)
            self.stop.wait(self.args.verify_every)

    def _record(self, payload: dict, room: str) -> None:
        bucket = payload.get("bucket", "NOVERDICT_other")
        self.buckets[bucket] += 1
        rung = self.ladder.key()
        minute = int((time.monotonic() - self.stats.started) // 600)

        if bucket in ACCEPT_BUCKETS:
            self.recent.append(bucket)
            self.consecutive_accepted += 1
            self.consecutive_refused = 0
            self.bins[minute]["acc"] += 1
            self.ladder.tally[rung]["acc"] += 1
        elif bucket == REFUSE_BUCKET:
            self.recent.append(bucket)
            self.consecutive_refused += 1
            self.consecutive_accepted = 0
            self.bins[minute]["ref"] += 1
            self.ladder.tally[rung]["ref"] += 1
        else:
            # NO VERDICT. Excluded from BOTH numerator and denominator, and it
            # must NOT touch the streaks: code 1 and cf1015 hit every arm at once
            # and would otherwise counterfeit the exact collapse the ladder
            # exists to react to.
            self.bins[minute]["nov"] += 1

        pct, acc, total = self.window_pct()
        print("VERIFY " + json.dumps(
            {"ts": time.time(), "bucket": bucket, "code": payload.get("code", ""),
             "room": room, "ms": payload.get("ms", 0), "rung": rung,
             "win_acc": acc, "win_n": total, "win_pct": round(pct, 1),
             "streak_ref": self.consecutive_refused,
             "detail": payload.get("detail", "")}, sort_keys=True), flush=True)

        self._drive_ladder()

    def _drive_ladder(self) -> None:
        if not self.args.ladder:
            return
        threshold = self.args.collapse_threshold

        if self.probing:
            tally = self.ladder.tally[self.ladder.key()]
            settled = tally["acc"] + tally["ref"]
            if self.consecutive_accepted >= self.args.recover_accepts:
                self.probing = False
                self.ladder.recovered(self.snapshot(),
                                      time.monotonic() - self.collapsed_at)
                return
            # A rung that has produced enough allow-listed verdicts and is still
            # mostly refused has failed. Escalate rather than sit on it.
            if (settled >= self.args.rung_probe and tally["acc"] * 2 < settled
                    and self.ladder.dwell_remaining() <= 0):
                self.ladder.escalate(
                    f"rung-failed acc={tally['acc']}/{settled}", self.snapshot())
                return

        if self.consecutive_refused >= threshold:
            held = self.ladder.dwell_remaining()
            if held > 0:
                print(f"  [rung] collapse signal held: {self.ladder.key()} has "
                      f"{held:.0f}s of dwell left", flush=True)
                return
            if not self.probing:
                self.collapsed_at = time.monotonic()
                self.probe_start = self.snapshot()
            self.probing = True
            self.consecutive_refused = 0
            self.ladder.escalate(
                f"{threshold}-consecutive-code5", self.probe_start or self.snapshot())


async def _block_irrelevant(route):
    request = route.request
    try:
        if request.resource_type == "document" or "challenges.cloudflare.com" in request.url:
            await route.continue_()
        else:
            await route.abort()
    except Exception:
        pass


def _post_to_relay(url: str, secret: str, token: str, label: str, proxy: str) -> str:
    """POST one token. Through the tunnel when a proxy is given, because the relay
    attributes a token to the TCP peer it arrived from — posting on the runner's
    own egress would file every WARP-minted token under GitHub's address and make
    `/assign?src=<exit>` score the wrong host."""
    body = json.dumps({"token": token, "label": label})
    if proxy:
        flag = "--socks5-hostname" if proxy.startswith("socks5") else "--proxy"
        target = proxy.split("://", 1)[-1] if proxy.startswith("socks5") else proxy
        try:
            out = subprocess.run(
                ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
                 flag, target, "--max-time", "20",
                 "-H", "Content-Type: application/json", "-H", f"X-Auth: {secret}",
                 "-X", "POST", "--data-binary", "@-", url],
                input=body, capture_output=True, text=True, timeout=30,
            )
            return (out.stdout or "").strip() or "curl-silent"
        except Exception as error:  # noqa: BLE001
            return f"ERR:{type(error).__name__}"
    request = urllib.request.Request(
        url, data=body.encode("utf-8"),
        headers={"Content-Type": "application/json", "X-Auth": secret},
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            response.read()
            return str(response.status)
    except Exception as error:  # noqa: BLE001
        return f"ERR:{type(error).__name__}"


@contextlib.asynccontextmanager
async def open_page(args, ladder: "Ladder | None"):
    """Yield a driveable page for the CURRENT rung.

    Factored out so every rung — including the Chromium control — runs through
    the identical renderer, click driver, console protocol and watchdogs. If the
    control had its own copy of the driving loop, a difference in the loop would
    be indistinguishable from a difference in the browser, which is precisely the
    confusion this experiment exists to remove.
    """
    proxy_raw = os.environ.get("PROXY", "").strip()

    if ladder is not None and ladder.name == "chromium":
        # RUNG 4 — the in-run control. Expected to be WORSE: on 2026-07-31 a
        # co-located Chromium on the same host, ASN, WARP exit and minutes scored
        # 2/108 and 0/119 against Camoufox's 192/0. It is kept precisely because
        # a ladder that only ever gets better is unfalsifiable — this rung is how
        # the run proves it is measuring something real.
        from playwright.async_api import async_playwright

        width, _, height = (args.window or "1280x1000").partition("x")
        chrome_args = [
            f"--window-size={width},{height}",
            "--window-position=0,0",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ]
        proxy = parse_proxy(proxy_raw) if proxy_raw else None
        print(f"[rung] {ladder.key()} launch chromium window={width}x{height} "
              f"proxy={'yes' if proxy else 'direct'}", flush=True)
        async with async_playwright() as driver:
            browser = await driver.chromium.launch(
                headless=args.headless, args=chrome_args, proxy=proxy)
            try:
                context = await browser.new_context(no_viewport=True)
                page = await context.new_page()
                yield page
            finally:
                try:
                    await browser.close()
                except Exception:  # noqa: BLE001
                    pass
        return

    launch: dict[str, object] = {
        "headless": args.headless,
        "disable_coop": True,
        "humanize": args.humanize,
        "os": ("windows", "macos", "linux")
        if args.fingerprint_os == "any"
        else args.fingerprint_os,
    }
    if args.executable:
        launch["executable_path"] = str(Path(args.executable))
        launch["ff_version"] = args.ff_version
        launch["i_know_what_im_doing"] = True
    if args.window:
        # Camoufox otherwise picks a RANDOM outer window size. A random small
        # window can leave lane 1 and beyond off the visible area, where no
        # pointer can reach them — and that is indistinguishable in the logs
        # from Cloudflare declining to escalate. Fix it.
        width, _, height = args.window.partition("x")
        launch["window"] = (int(width), int(height))
    if proxy_raw:
        launch["proxy"] = parse_proxy(proxy_raw)
        launch["geoip"] = True
    if args.geoip:
        launch["geoip"] = True
    if args.no_ubo:
        # uBlock Origin is Camoufox's ONLY default addon, and on the Oracle VM
        # it alone cost ~11% of a core. Dropping it is the single cheapest CPU
        # win available on a box that is CPU-bound.
        launch["exclude_addons"] = [DefaultAddons.UBO]
    if args.lean_prefs:
        launch["firefox_user_prefs"] = dict(LEAN_PREFS)

    # LADDER. Rung overrides are applied last so they win over the defaults, and
    # the effective launch is printed: a rung that silently did nothing is worse
    # than no ladder at all, because it looks like the fingerprint was tried.
    if ladder is not None:
        overrides = ladder.launch_overrides()
        for field, value in overrides.items():
            launch[field] = value
        if overrides.get("executable_path"):
            launch["i_know_what_im_doing"] = True
        print(f"[rung] {ladder.key()} launch os={launch.get('os')} "
              f"window={launch.get('window')} humanize={launch.get('humanize')} "
              f"exe={Path(str(launch.get('executable_path', ''))).name or 'default'}",
              flush=True)

    async with AsyncCamoufox(**launch) as browser:
        # no_viewport=True is required: Playwright >=1.61 sends an `isMobile`
        # field that v135's Juggler protocol rejects, killing new_context().
        context = await browser.new_context(no_viewport=True)
        page = await context.new_page()
        yield page


async def run_session(args, stats: Stats, out_handle, deadline: float | None,
                      xdo: XdoDriver, ladder: Ladder | None = None,
                      verifier: "Verifier | None" = None) -> None:
    renderer = build_renderer(args)
    geom: dict[str, float] = {}
    clicks_at: dict[tuple[int, int], int] = {}      # (lane, gen) -> click count
    last_click: dict[int, float] = {}               # lane -> monotonic
    loop = asyncio.get_running_loop()

    async with open_page(args, ladder) as page:
        await page.route("**/*", _block_irrelevant)

        def do_click(lane: int, gen: int, px: int, py: int) -> None:
            """Deliver one click. xdotool first, page.mouse once a lane has taken
            --click-retry XTEST clicks and is still unsolved."""
            key = (lane, gen)
            n = clicks_at.get(key, 0)
            driver = "xdo"
            if xdo.available and n < args.click_retry:
                screen = xdo.to_screen(px, py, geom)
                if screen:
                    ok = xdo.click(*screen)
                    if ok:
                        clicks_at[key] = n + 1
                        stats.clicks += 1
                        stats.clicks_xdo += 1
                        print(f"  [click] lane={lane} gen={gen} xdo screen={screen} "
                              f"page=({px},{py}) n={n + 1}", flush=True)
                        return
                driver = "mouse(xdo-declined)"
            asyncio.run_coroutine_threadsafe(
                page.mouse.click(px, py, delay=random.randint(60, 140)), loop
            )
            clicks_at[key] = n + 1
            stats.clicks += 1
            stats.clicks_mouse += 1
            print(f"  [click] lane={lane} gen={gen} {driver} page=({px},{py}) "
                  f"n={n + 1}", flush=True)

        def on_console(message):
            text = message.text

            match = RE_TOK.match(text)
            if match:
                lane, gen, solve, token = (int(match.group(1)), int(match.group(2)),
                                           match.group(3), match.group(4))
                mono = time.monotonic()
                gap = mono - stats.last_token_at
                stats.last_token_at = mono
                stats.tokens += 1
                clicked = clicks_at.pop((lane, gen), 0)
                if solve == "auto":
                    stats.auto += 1
                else:
                    stats.inter += 1
                stats.clicks_on_token += clicked
                if clicked:
                    stats.solved_after_click += 1
                # DIVERT, never duplicate. A token is single-use: posting it to
                # the relay AND replaying it here would hand icebot a spent token
                # and manufacture a code 5 that says nothing about acceptance.
                diverted = bool(verifier and verifier.offer(token))
                record = {
                    "ts": time.time(), "label": args.label, "token": token,
                    "len": len(token), "solve": solve, "clicks": clicked,
                    "lane": lane, "diverted": diverted,
                }
                out_handle.write(json.dumps(record) + "\n")
                out_handle.flush()
                if args.relay_url and not diverted:
                    # LIVE BEACON. GitHub publishes a job's log only when the job
                    # ENDS, so on a 300-minute run every number this producer
                    # prints — including the ladder — is invisible for five hours
                    # (`::notice::` annotations are materialised with the log and
                    # do not close that gap; measured 2026-07-31). The one thing
                    # that leaves the runner live is the token stream, so the
                    # current rung and the runner's own rolling acceptance ride
                    # along in the LABEL and show up on the relay's /stats/exits
                    # within seconds. The prefix is unchanged, so
                    # SERVE_TRUSTED_LABELS=vm- still keeps production off it.
                    beacon = args.label
                    if verifier and ladder:
                        pct, _, n = verifier.window_pct()
                        beacon = (f"{args.label}-r{ladder.rung}w{ladder.wraps}"
                                  f"-{int(pct)}of{n}")

                    def post(tok=token, lbl=beacon):
                        status = _post_to_relay(args.relay_url, args.auth_secret,
                                                tok, lbl, args.relay_proxy)
                        if status in ("200", "201", "204"):
                            stats.posted += 1
                        else:
                            stats.post_fail += 1
                            print(f"  [relay] {status}", flush=True)
                    loop.run_in_executor(None, post)
                print(
                    f"[{stats.tokens:3d}] +{gap:5.1f}s lane={lane} len={len(token)} "
                    f"solve={solve} clicks={clicked} "
                    f"rate={stats.rate_per_min():.1f}/min "
                    f"auto={stats.auto}/{stats.tokens} ({stats.auto_pct():.0f}%)",
                    flush=True,
                )
                return

            match = RE_CLICK.match(text)
            if match:
                lane, gen, px, py = (int(match.group(1)), int(match.group(2)),
                                     int(match.group(3)), int(match.group(4)))
                now = time.monotonic()
                if now - last_click.get(lane, 0.0) < args.click_interval:
                    return
                if clicks_at.get((lane, gen), 0) >= args.click_max:
                    return
                last_click[lane] = now
                loop.run_in_executor(None, do_click, lane, gen, px, py)
                return

            match = RE_RECT.match(text)
            if match:
                lane, w, h = int(match.group(1)), int(match.group(3)), int(match.group(4))
                prev = stats.rect_max.get(lane, (0, 0))
                if w * h > prev[0] * prev[1]:
                    stats.rect_max[lane] = (w, h)
                if args.log_rects:
                    print(f"  {text}", flush=True)
                return

            if text.startswith("INTER "):
                stats.escalations += 1
                print(f"  {text}", flush=True)
                return

            if text.startswith("E:"):
                code = text[2:]
                stats.note_error(code)
                print(f"  [cf-error] {code}", flush=True)
                return

            match = RE_GEOM.match(text)
            if match:
                geom.update({
                    "iw": int(match.group(1)), "ih": int(match.group(2)),
                    "ow": int(match.group(3)), "oh": int(match.group(4)),
                    "dpr": float(match.group(5)),
                })
                win = xdo.window() if xdo.available else None
                print(f"  [geom] page inner={geom['iw']}x{geom['ih']} "
                      f"outer={geom['ow']}x{geom['oh']} dpr={geom['dpr']} "
                      f"| xwindow={win}", flush=True)
                return

        page.on("console", on_console)

        try:
            await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60_000)
        except Exception as error:
            print(f"  [nav] {type(error).__name__}", flush=True)
        await asyncio.sleep(0.5)
        try:
            # gartic serves a Report-Only CSP; Playwright surfaces that as an
            # add_script_tag failure even though the script does execute.
            await page.add_script_tag(content=renderer)
        except Exception:
            pass

        session_end = time.monotonic() + args.browser_lifetime
        last_reload = time.monotonic()
        last_diag = time.monotonic()
        while True:
            if args.max_tokens and stats.tokens >= args.max_tokens:
                return
            now = time.monotonic()
            if now >= session_end or (deadline and now >= deadline):
                return
            # The ladder escalates by DROPPING THE BROWSER: Camoufox draws its
            # whole fingerprint at launch, so a new context in this process would
            # inherit the fingerprint that is being refused. Returning here is
            # what makes rung 1 a real re-roll.
            if ladder is not None and ladder.restart.is_set():
                ladder.restart.clear()
                print(f"  [rung] restarting browser for {ladder.key()}", flush=True)
                return

            # ── BEACON. GitHub publishes a job's log only when the JOB ENDS:
            # the REST logs endpoint answers `BlobNotFound` and `gh run view
            # --log` refuses outright ("logs will be available when it is
            # complete"). So on a 180-minute run every number this producer
            # prints is invisible for three hours, and the PRIMARY metric of
            # this experiment — the auto/int solve split — cannot be read until
            # it is over.
            #
            # ⚠️ MEASURED 2026-07-31: `::notice::` does NOT close that gap.
            # `GET /repos/O/R/check-runs/<job_id>/annotations` returned `[]`
            # 22 minutes into a run that had emitted two beacons — annotations
            # are materialised with the job, like the log. The lines are still
            # worth emitting (they are a compact, greppable summary in the final
            # log), but DO NOT plan a run around reading them live.
            #
            # What IS live from a runner: whatever the producer POSTs off-box.
            # Here that is the token stream itself, so the relay's
            # /stats/exits (minted, mintPerMin, queued) and the roomscore
            # sampler's jsonl are the only real-time instruments. If a future
            # run needs a live solve split, ship the counters to the relay —
            # not to the log, and not to an annotation.
            #
            # Cadence is deliberately slow: GitHub keeps ~10 annotations per
            # level per step, so a chatty beacon would evict its own history.
            if now >= stats.next_beacon:
                stats.next_beacon = now + args.beacon_every
                rects = ",".join(f"L{i}={w}x{h}" for i, (w, h)
                                 in sorted(stats.rect_max.items())) or "none"
                print(
                    f"::notice title=camoufox-solve::t={int(now - stats.started) // 60}min "
                    f"minted={stats.tokens} auto={stats.auto} int={stats.inter} "
                    f"auto_pct={stats.auto_pct():.0f} clicks={stats.clicks} "
                    f"(xdo={stats.clicks_xdo}/mouse={stats.clicks_mouse}) "
                    f"escalations={stats.escalations} maxrect={rects} "
                    f"errs={stats.errors or 'none'} "
                    f"posted={stats.posted}/{stats.posted + stats.post_fail}",
                    flush=True,
                )

            if now - last_diag >= args.diag_every:
                last_diag = now
                rects = " ".join(f"L{i}={w}x{h}" for i, (w, h)
                                 in sorted(stats.rect_max.items())) or "none"
                print(
                    f"[diag] t={int(now - stats.started)}s minted={stats.tokens} "
                    f"auto={stats.auto} int={stats.inter} ({stats.auto_pct():.0f}% auto) "
                    f"clicks={stats.clicks} (xdo={stats.clicks_xdo} "
                    f"mouse={stats.clicks_mouse}) escalations={stats.escalations} "
                    f"maxrect[{rects}] errs={stats.errors or '{}'} "
                    f"posted={stats.posted}/{stats.posted + stats.post_fail}"
                    + (f" | accept={verifier.snapshot()} rung={ladder.key()}"
                       if verifier and ladder else ""),
                    flush=True,
                )
                if stats.tokens == 0 and stats.clicks == 0 and stats.escalations:
                    print("[diag] ^ the widget ESCALATED but no click was ever "
                          "dispatched — the rect never reached the gate. This is a "
                          "CLICKER failure, not a Cloudflare verdict.", flush=True)

            # ≥20 minutes on purpose: a watchdog shorter than the work it
            # supervises causes the failure it watches for.
            stalled = (now - stats.last_token_at) > args.stall_timeout
            due = args.reload_interval and (now - last_reload) >= args.reload_interval
            if stalled or due:
                print(f"  [reload:{'stall' if stalled else 'periodic'}]", flush=True)
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=60_000)
                    await asyncio.sleep(0.5)
                    try:
                        await page.add_script_tag(content=renderer)
                    except Exception:
                        pass
                except Exception as error:
                    print(f"  [reload failed] {type(error).__name__}", flush=True)
                    return
                last_reload = time.monotonic()
                stats.last_token_at = time.monotonic()
            await asyncio.sleep(1)


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path("tokens.jsonl"))
    parser.add_argument("--label", default="camoufox-click")
    parser.add_argument("--duration", type=float, default=0)
    parser.add_argument("--max-tokens", type=int, default=0)
    parser.add_argument("--lanes", type=int, default=2,
                        help="concurrent Turnstile widgets on the page")
    parser.add_argument("--appearance", default="interaction-only",
                        choices=("interaction-only", "always", "execute"),
                        help="Turnstile widget mode. interaction-only is what "
                             "gartic's own client uses and is the ONLY mode in "
                             "which the rect transition cleanly separates an "
                             "auto-solved challenge (stays 0x0) from an "
                             "escalated one (~300x68) — which is the primary "
                             "measurement. Do not switch to `always` to make "
                             "the rect clickable; fix the gate instead.")
    parser.add_argument("--token-interval", type=float, default=0)
    parser.add_argument("--browser-lifetime", type=float, default=1800)
    parser.add_argument("--reload-interval", type=float, default=0,
                        help="0 = never. A periodic reload destroys challenges "
                             "that are still in progress.")
    parser.add_argument("--stall-timeout", type=float, default=1500,
                        help="reload if no token arrives within this many "
                             "seconds. MUST exceed the time one escalated, "
                             "clicked challenge can take — a 90 s value "
                             "pre-empted every slow solve this producer had.")
    parser.add_argument("--diag-every", type=float, default=60)
    parser.add_argument("--beacon-every", type=float, default=1200,
                        help="seconds between ::notice:: check-run annotations "
                             "— the ONLY channel that leaves a GitHub runner "
                             "before the job ends. Keep it slow: GitHub retains "
                             "roughly 10 annotations per level per step.")
    parser.add_argument("--log-rects", action="store_true",
                        help="echo every RECT line (very verbose; the [diag] "
                             "heartbeat already carries the max rect per lane)")
    # Click driver
    parser.add_argument("--click-interval", type=float, default=3.0,
                        help="minimum seconds between clicks on one lane")
    parser.add_argument("--click-retry", type=int, default=2,
                        help="xdotool attempts before a lane falls back to "
                             "Playwright's page.mouse")
    parser.add_argument("--click-max", type=int, default=8,
                        help="clicks per widget generation before giving up")
    parser.add_argument("--click-gate",
                        choices=("rect", "escalation", "both"), default="rect",
                        help="WHAT OPENS THE CLICK GATE. rect (default) is the "
                             "historical behaviour and a NO-OP — the slot is a "
                             "fixed 300x70 div, so the gate is open from "
                             "creation and the clicker fires ~1.5 s before "
                             "Cloudflare escalates, every --click-interval. "
                             "escalation clicks when "
                             "before-interactive-callback fires, the real "
                             "signal (measured +40% rate, cf_errors.timeout "
                             "12 -> 0). both = escalated AND laid out. The "
                             "default stays `rect` so uploading this file "
                             "changes nothing on its own.")
    parser.add_argument("--click-rearm-ms", type=int, default=8000,
                        help="non-rect gates: how long to wait before a RETRY "
                             "click on the same widget generation")
    parser.add_argument("--click-profile",
                        choices=tuple(CLICK_PROFILES), default="base",
                        help="HOW EXPENSIVE THE CLICK IS. `base` (default) is "
                             "the shipped 16-waypoint humanised motion, ~620 ms, "
                             "~92%% of it deliberate sleep. `impl`/`batch` keep "
                             "the pointer trace byte-identical and only remove "
                             "process spawns and XSync round-trips. `fast` and "
                             "`min` DEGRADE THE MOTION and are a stealth "
                             "trade-off: score acceptance, never rate alone.")
    parser.add_argument("--click-min-w", type=int, default=20)
    parser.add_argument("--click-min-h", type=int, default=20)
    # ── CPU arms. Default to the baseline value, so an arm that forgets a flag
    # is a BASELINE arm and not a silently half-applied one.
    #
    # ⚠️ THESE TWO EXIST BECAUSE THE VM COPY OF THIS FILE ONCE HAD THEM AND THIS
    # ONE DID NOT. A straight file swap then crash-looped VM lane 1 at `rc=2`
    # (argparse rejecting an unknown flag) while `systemctl` still read `active`
    # and the lane minted exactly zero — the silent-death shape. ONE generator
    # carries BOTH flag sets; `check_click_profiles.py` fails the build if
    # either disappears again.
    parser.add_argument("--no-ubo", action="store_true",
                        help="drop Camoufox's uBlock Origin addon (its only "
                             "default addon). ~11%% of a core on the Oracle VM. "
                             "On an idle runner it buys nothing; on a "
                             "CPU-bound box it is the cheapest win there is.")
    parser.add_argument("--lean-prefs", action="store_true",
                        help="Firefox prefs that cut media/RDD/telemetry/"
                             "safebrowsing/prefetch work and pin one content "
                             "process (-14%% CPU/token, -29%% RSS on the VM)")
    parser.add_argument("--display", default=os.environ.get("DISPLAY", ""),
                        help="X display for xdotool (empty = page.mouse only)")
    # Browser
    parser.add_argument("--humanize", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--headless", action=argparse.BooleanOptionalAction, default=False,
                        help="default False: XTEST needs a real window on an X "
                             "display, which is what Xvfb provides")
    parser.add_argument("--geoip", action="store_true")
    parser.add_argument("--window", default="1280x1000",
                        help="fixed outer window size WxH ('' = camoufox's "
                             "random one, which can hide a lane off-screen)")
    parser.add_argument("--fingerprint-os", choices=("any", "windows", "macos", "linux"),
                        default="any")
    parser.add_argument("--executable", default=DEFAULT_EXECUTABLE)
    parser.add_argument("--ff-version", type=int, default=DEFAULT_FF_VERSION)
    # Self-verification (STEP 1) — see the ACCEPT_BUCKETS block above
    parser.add_argument("--verify-binary", default="",
                        help="path to the compiled roomverify; empty disables "
                             "self-verification entirely")
    parser.add_argument("--verify-every", type=float, default=45,
                        help="seconds between self-verifications. Each one COSTS "
                             "ONE TOKEN, which is diverted from the relay rather "
                             "than duplicated. Tokens are ~74x oversupplied "
                             "(icebot joins ~1.6/min), so this is cheap.")
    parser.add_argument("--verify-start-delay", type=float, default=120,
                        help="wait this long before the first check so a slow "
                             "first mint is not reported as an issuance failure")
    parser.add_argument("--verify-proxy", default="",
                        help="redeem through this proxy (default: $PROXY, i.e. "
                             "the tunnel that minted the token — gartic's WAF "
                             "403s the discovery endpoint from some GitHub "
                             "addresses, which fails for reasons unrelated to "
                             "the token)")
    parser.add_argument("--verify-langs", default="19,2,1,0,5",
                        help="languages to pull the rotating room roster from")
    parser.add_argument("--verify-room-ttl", type=float, default=600,
                        help="seconds a room roster is reused; /req/list answers "
                             "429 when refreshed harder than this")
    parser.add_argument("--verify-window", type=int, default=40,
                        help="rolling window of ALLOW-LISTED verdicts "
                             "(ACCEPTED_* + REFUSED_token only)")
    # Ladder (STEP 2)
    parser.add_argument("--ladder", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--collapse-threshold", type=int, default=5,
                        help="consecutive REFUSED_token (code 5) that declare a "
                             "collapse. 5 was chosen from measurement: across "
                             "2471 verdicts in 74 healthy bins there was not one "
                             "refusal run of length >= 2, putting the "
                             "false-positive rate near 0.12%%/dial. code 1 and "
                             "cf1015 never touch this counter.")
    parser.add_argument("--recover-accepts", type=int, default=3,
                        help="consecutive ACCEPTED_* that declare a rung recovered")
    parser.add_argument("--rung-min-dwell", type=float, default=240,
                        help="seconds a rung is protected from escalation. "
                             "Multiplied by (1 + laps completed), capped at "
                             "1800: a run that has tried every rung twice is "
                             "probably inside one of gartic's global outages, "
                             "which no fingerprint can fix.")
    parser.add_argument("--rung-probe", type=int, default=8,
                        help="allow-listed verdicts a rung gets to prove itself "
                             "before the ladder escalates again")
    parser.add_argument("--alt-executable", default="",
                        help="RUNG 3: a second Camoufox build. EMPTY BY DEFAULT "
                             "and the rung is then skipped, deliberately: only "
                             "v135.0.1-beta.24 is verified to mint on this "
                             "sitekey (v152 yields zero, error 600010), so "
                             "shipping an unpinned second build would trade an "
                             "acceptance problem for an issuance one.")
    parser.add_argument("--alt-ff-version", type=int, default=DEFAULT_FF_VERSION)
    parser.add_argument("--chromium-fallback",
                        action=argparse.BooleanOptionalAction, default=True,
                        help="RUNG 4: fall back to Playwright Chromium. Expected "
                             "to be WORSE (2/108 and 0/119 vs Camoufox 192/0 on "
                             "the same host and minutes) and kept as the in-run "
                             "control that proves the ladder measures something")
    # Relay
    parser.add_argument("--relay-url", default="")
    parser.add_argument("--relay-proxy", default="",
                        help="POST tokens through this proxy so the relay "
                             "attributes them to the minting exit")
    args = parser.parse_args()

    args.auth_secret = os.environ.get("AUTH_SECRET", "").strip()
    if args.relay_url and not args.auth_secret:
        parser.error("--relay-url needs AUTH_SECRET in the environment")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    stats = Stats()
    deadline = time.monotonic() + args.duration if args.duration else None
    xdo = XdoDriver(args.display, profile=args.click_profile)

    ladder = Ladder(args)
    verifier = None
    if args.verify_binary and args.verify_every > 0:
        if not Path(args.verify_binary).exists():
            parser.error(f"--verify-binary {args.verify_binary!r} does not exist")
        # Resolve once. A relative path is re-interpreted per call, and on
        # Windows a relative path with forward slashes fails CreateProcess
        # outright — which surfaces as a bare FileNotFoundError inside the
        # roster refresh and reads exactly like "gartic is unreachable".
        args.verify_binary = str(Path(args.verify_binary).resolve())
        if not args.verify_proxy:
            args.verify_proxy = os.environ.get("PROXY", "").strip()
        verifier = Verifier(args, stats, ladder)
        verifier.start()
        print(f"[verify] every {args.verify_every:.0f}s via "
              f"{Path(args.verify_binary).name} proxy="
              f"{args.verify_proxy or 'direct'} langs={args.verify_langs} "
              f"collapse>={args.collapse_threshold} ladder={args.ladder}",
              flush=True)
    else:
        print("[verify] DISABLED — this run has no acceptance feedback and the "
              "ladder can never fire", flush=True)

    print(
        f"[config] label={args.label} lanes={args.lanes} "
        f"appearance={args.appearance} humanize={args.humanize} "
        f"headless={args.headless} display={args.display or 'none'} "
        f"xdotool={'yes' if xdo.available else 'NO — page.mouse only'} "
        f"click_profile={args.click_profile} "
        f"lifetime={args.browser_lifetime}s stall={args.stall_timeout}s "
        f"reload={args.reload_interval}s ff={args.ff_version} "
        f"proxy={'yes' if os.environ.get('PROXY', '').strip() else 'direct'} "
        f"relay={'yes' if args.relay_url else 'no'}",
        flush=True,
    )

    with args.out.open("a", encoding="utf-8") as handle:
        try:
            while True:
                if deadline and time.monotonic() >= deadline:
                    break
                if args.max_tokens and stats.tokens >= args.max_tokens:
                    break
                try:
                    await run_session(args, stats, handle, deadline, xdo,
                                      ladder, verifier)
                except Exception as error:
                    print(f"[session] {type(error).__name__}: {error}", flush=True)
                    # A rung whose browser will not launch at all must not trap
                    # the ladder: an alt build that is missing, or a Chromium
                    # that was never installed, would otherwise loop forever on
                    # a rung that can produce no verdicts.
                    if ladder.name in ("alt-build", "chromium"):
                        ladder.escalate(f"rung-launch-failed:"
                                        f"{type(error).__name__}",
                                        verifier.snapshot() if verifier else {})
                        ladder.restart.clear()
                    await asyncio.sleep(2)
                if not (deadline or args.max_tokens):
                    continue
        except KeyboardInterrupt:
            pass
        finally:
            if verifier:
                verifier.stop.set()

    elapsed = time.monotonic() - stats.started
    print(
        f"\n[summary] label={args.label} tokens={stats.tokens} "
        f"auto={stats.auto} int={stats.inter} auto_pct={stats.auto_pct():.1f} "
        f"clicks={stats.clicks} elapsed={elapsed:.0f}s "
        f"rate={stats.rate_per_min():.2f}/min cf_errors={stats.errors or '{}'}",
        flush=True,
    )
    print(
        "PRODUCER " + json.dumps({
            "label": args.label,
            "tokens": stats.tokens,
            "solve_auto": stats.auto,
            "solve_int": stats.inter,
            "auto_pct": round(stats.auto_pct(), 1),
            "clicks": stats.clicks,
            "clicks_xdotool": stats.clicks_xdo,
            "clicks_mouse": stats.clicks_mouse,
            "solved_after_click": stats.solved_after_click,
            # ── CLICK WASTE. The rect gate's whole cost shows up here: clicks
            # landing on generations that never minted, each one able to throw
            # away a solve and come back as cf_errors.timeout.
            "click_gate": args.click_gate,
            "click_profile": args.click_profile,
            "clicks_on_minting_generations": stats.clicks_on_token,
            "clicks_on_dead_generations": stats.clicks - stats.clicks_on_token,
            "clicks_wasted_pct": round(100.0 * (stats.clicks - stats.clicks_on_token)
                                       / stats.clicks, 1) if stats.clicks else -1.0,
            "clicks_per_token": round(stats.clicks / stats.tokens, 2)
                                if stats.tokens else -1.0,
            "clicks_per_escalation": round(stats.clicks / stats.escalations, 2)
                                     if stats.escalations else -1.0,
            "escalations": stats.escalations,
            "max_rect": {str(k): f"{w}x{h}" for k, (w, h) in stats.rect_max.items()},
            "elapsed_s": round(elapsed, 1),
            "rate_per_min": round(stats.rate_per_min(), 2),
            "cf_errors": stats.errors,
            "relay_posted": stats.posted,
            "relay_failed": stats.post_fail,
            "diverted_to_verify": stats.diverted,
        }, sort_keys=True),
        flush=True,
    )

    if verifier:
        pct, acc, total = verifier.window_pct()
        overall_acc = sum(verifier.buckets.get(b, 0) for b in ACCEPT_BUCKETS)
        overall_ref = verifier.buckets.get(REFUSE_BUCKET, 0)
        settled = overall_acc + overall_ref
        print(
            "ACCEPTANCE " + json.dumps({
                "settled": settled,
                "accepted": overall_acc,
                "refused_code5": overall_ref,
                # Allow-list only. Everything NOVERDICT_* is reported separately
                # and is in NEITHER half of this ratio.
                "accept_pct": round(100.0 * overall_acc / settled, 1) if settled else None,
                "window_pct": round(pct, 1), "window_n": total,
                "buckets": dict(sorted(verifier.buckets.items())),
                # Named `ladder_*` because the PRODUCER line above carries an
                # `escalations` counter of its own with a completely different
                # meaning — Cloudflare escalating a widget to interactive. The
                # two appear within three lines of each other in the log.
                "ladder_escalations": sum(1 for h in ladder.history
                                          if h["event"] == "escalate"),
                "ladder_recoveries": sum(1 for h in ladder.history
                                         if h["event"] == "recovered"),
                "final_rung": ladder.key(),
                "per_rung": {k: v for k, v in sorted(ladder.tally.items())},
            }, sort_keys=True),
            flush=True,
        )
        # 10-minute bins, never one pooled number: a pooled figure cannot show a
        # cut, and a cut is the whole thing this run is waiting for.
        print("--- acceptance by 10-min bin (acc/settled, noverdict excluded) ---",
              flush=True)
        for index in sorted(verifier.bins):
            row = verifier.bins[index]
            settled_bin = row["acc"] + row["ref"]
            share = f"{100.0 * row['acc'] / settled_bin:.0f}%" if settled_bin else "--"
            print(f"  t={index * 10:>3}-{index * 10 + 10:<3}min  "
                  f"{row['acc']}/{settled_bin}  {share:>4}  "
                  f"(noverdict {row['nov']})", flush=True)
        for entry in ladder.history:
            print("LADDER-HISTORY " + json.dumps(entry, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
