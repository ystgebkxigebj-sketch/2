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
**Gate the click on the widget's BOUNDING RECT, never on iframe presence.** A
non-interactive Turnstile widget in `interaction-only` mode occupies `0x0`. When
Cloudflare escalates it, the container grows to roughly `300x68` — that
transition is the *only* reliable signal that a click is wanted, and it is
observable without touching Cloudflare's DOM. The rect of every lane is logged
on every transition and on a heartbeat, so a zero-mint run says where it stopped
instead of being mysterious.

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
import json
import math
import os
import random
import re
import shutil
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

from camoufox.async_api import AsyncCamoufox

TARGET_URL = "https://gartic.io"
SITEKEY = "0x4AAAAAABBPKaIbNwnPEfSo"

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

    function build() {
      gen++;
      var myGen = gen;
      sawInter = false; emitted = false; lastW = -1; lastH = -1; ticks = 0;
      if (slot && slot.parentNode) slot.parentNode.removeChild(slot);
      slot = document.createElement('div');
      slot.id = 'lane_' + i + '_' + myGen;
      // The class stock Turnstile helpers look for. Our driver finds the widget
      // by rect and does not need it; it costs one line and keeps any future
      // off-the-shelf clicker able to locate us.
      slot.className = 'cf-turnstile';
      // Fixed at the TOP-left: a window shorter than expected still shows it,
      // whereas a bottom-anchored widget can land below the visible area and
      // then no pointer on earth can reach it.
      slot.style.cssText = 'position:fixed;left:' + (20 + i * 320) +
                           'px;top:20px;width:300px;height:70px;z-index:2147483647';
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
            sawInter = true; log('INTER lane=' + i + ' gen=' + myGen);
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
        // THE RECT GATE. Never `if (iframe)` — an escalated interaction-only
        // widget exposes no iframe, which is exactly the bug this file fixes.
        if (w >= CLICK_MIN_W && h >= CLICK_MIN_H) {
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
class XdoDriver:
    """Click through XTEST, addressing the browser window found on $DISPLAY.

    Screen coordinates come from the X server's own view of the window, never
    from `window.screenX`/`outerHeight`: Camoufox spoofs those as part of the
    fingerprint, and trusting them aims the pointer at empty space while every
    log line still looks healthy.
    """

    def __init__(self, display: str) -> None:
        self.display = display
        self.available = bool(shutil.which("xdotool")) and bool(display)
        self.win: tuple[int, int, int, int] | None = None   # x, y, w, h
        self.win_at = 0.0
        self.fail = 0

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
        """Approach from a random angle, ease in, overshoot, correct, hold."""
        if not self.available:
            return False
        try:
            steps: list[str] = []
            ang = random.uniform(0, 6.283)
            dist = random.uniform(80, 220)
            ox = sx + dist * math.cos(ang)
            oy = sy + dist * math.sin(ang)
            ox = min(max(ox, 5), 4000)
            oy = min(max(oy, 5), 4000)
            n = random.randint(12, 20)
            tx = sx + random.uniform(-4, 4)
            ty = sy + random.uniform(-3, 3)
            for i in range(1, n + 1):
                t = i / n
                e = 4 * t * t * t if t < 0.5 else 1 - ((-2 * t + 2) ** 3) / 2
                steps += ["mousemove", "--sync",
                          str(int(ox + (tx - ox) * e + random.uniform(-1.2, 1.2))),
                          str(int(oy + (ty - oy) * e + random.uniform(-1.2, 1.2))),
                          "sleep", f"{random.uniform(0.008, 0.03):.3f}"]
            steps += ["mousemove", "--sync", str(sx), str(sy),
                      "sleep", f"{random.uniform(0.09, 0.23):.3f}"]
            self._run("xdotool", *steps, timeout=25)
            self._run("xdotool", "mousedown", "1", timeout=5)
            time.sleep(random.uniform(0.06, 0.14))
            self._run("xdotool", "mouseup", "1", timeout=5)
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
        self.errors: dict[str, int] = {}
        self.rect_max: dict[int, tuple[int, int]] = {}
        self.escalations = 0
        self.started = time.monotonic()
        self.last_token_at = time.monotonic()
        self.posted = 0
        self.post_fail = 0
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


async def run_session(args, stats: Stats, out_handle, deadline: float | None,
                      xdo: XdoDriver) -> None:
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
    proxy_raw = os.environ.get("PROXY", "").strip()
    if proxy_raw:
        launch["proxy"] = parse_proxy(proxy_raw)
        launch["geoip"] = True
    if args.geoip:
        launch["geoip"] = True

    renderer = build_renderer(args)
    geom: dict[str, float] = {}
    clicks_at: dict[tuple[int, int], int] = {}      # (lane, gen) -> click count
    last_click: dict[int, float] = {}               # lane -> monotonic
    loop = asyncio.get_running_loop()

    async with AsyncCamoufox(**launch) as browser:
        # no_viewport=True is required: Playwright >=1.61 sends an `isMobile`
        # field that v135's Juggler protocol rejects, killing new_context().
        context = await browser.new_context(no_viewport=True)
        page = await context.new_page()
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
                if clicked:
                    stats.solved_after_click += 1
                record = {
                    "ts": time.time(), "label": args.label, "token": token,
                    "len": len(token), "solve": solve, "clicks": clicked,
                    "lane": lane,
                }
                out_handle.write(json.dumps(record) + "\n")
                out_handle.flush()
                if args.relay_url:
                    def post(tok=token):
                        status = _post_to_relay(args.relay_url, args.auth_secret,
                                                tok, args.label, args.relay_proxy)
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

            # ── LIVE BEACON. GitHub publishes a job's log only when the JOB
            # ENDS — the REST logs endpoint answers BlobNotFound and `gh run
            # view --log` refuses outright — so on a 180-minute run every number
            # this producer prints is invisible for three hours. `::notice::`
            # writes a check-run annotation, which IS readable while the job
            # runs (`gh api repos/O/R/check-runs/<job_id>/annotations`). That is
            # the only live channel out of a runner, and without it the PRIMARY
            # metric of this experiment — the auto/int solve split — cannot be
            # read until the run is over.
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
                    f"posted={stats.posted}/{stats.posted + stats.post_fail}",
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
    parser.add_argument("--click-min-w", type=int, default=20)
    parser.add_argument("--click-min-h", type=int, default=20)
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
    xdo = XdoDriver(args.display)

    print(
        f"[config] label={args.label} lanes={args.lanes} "
        f"appearance={args.appearance} humanize={args.humanize} "
        f"headless={args.headless} display={args.display or 'none'} "
        f"xdotool={'yes' if xdo.available else 'NO — page.mouse only'} "
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
                    await run_session(args, stats, handle, deadline, xdo)
                except Exception as error:
                    print(f"[session] {type(error).__name__}: {error}", flush=True)
                    await asyncio.sleep(2)
                if not (deadline or args.max_tokens):
                    continue
        except KeyboardInterrupt:
            pass

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
            "escalations": stats.escalations,
            "max_rect": {str(k): f"{w}x{h}" for k, (w, h) in stats.rect_max.items()},
            "elapsed_s": round(elapsed, 1),
            "rate_per_min": round(stats.rate_per_min(), 2),
            "cf_errors": stats.errors,
            "relay_posted": stats.posted,
            "relay_failed": stats.post_fail,
        }, sort_keys=True),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
