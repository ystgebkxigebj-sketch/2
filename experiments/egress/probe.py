#!/usr/bin/env python3
"""Egress experiment probe: can THIS machine mint gartic-accepted Turnstile tokens?

Self-contained on purpose. It duplicates the core of
``turnstile-system/token generators/camoufox-fresh/generator.py`` rather than
importing it, so an experiment can never perturb the production producer.

It answers three questions that must not be conflated:

  (a) Does Cloudflare even serve the challenge here?  -> tokens == 0 and
      cf_errors dominated by ``600010``.
  (b) Are tokens issued but rejected by gartic?       -> tokens > 0 and
      joins report ``REJECTED code=5``.
  (c) Are tokens issued AND accepted?                 -> ``JOINED``.

Tokens are single-use and expire in ~240 s, so shipping them off-box for
verification is unreliable. Instead every minted token is replayed *here*,
immediately, through gartic's real join handshake (the same protocol
``cmd/joindebug`` speaks). The verdict is printed inline.

The replay is delegated to the ``joinverify`` Go binary, because both in-process
options fail for reasons unrelated to the token: a Python ``websocket-client``
handshake to ``serverNN.gartic.io`` is answered with Cloudflare **403** on TLS
fingerprint alone (from the very IP where Go gets a clean 101), and opening the
socket from inside the Camoufox page tears down v135's Juggler connection.

Pass ``--verifier-proxy`` to send the check through the same tunnel the token
was minted over. That is not cosmetic: gartic's WAF serves a 403 block page to
the server-discovery endpoint from some GitHub runner addresses, which would
otherwise be misread as a rejected token.

Caveat when reading a negative: the join is made from wherever the verifier
egresses, so a ``REJECTED`` cannot by itself separate "bad token" from "join IP
refused". A ``JOINED`` is unambiguous. Use --emit-tokens to re-verify from a
trusted IP when that distinction matters.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

from camoufox.async_api import AsyncCamoufox

TARGET_URL = "https://gartic.io"
SITEKEY = "0x4AAAAAABBPKaIbNwnPEfSo"

# action:'join' is load-bearing — gartic validates the token's action server
# side, and a token minted without it fails every join with code 5.
RENDERER_JS = r"""
(function () {
  document.body.innerHTML = '<div id="ts_slot"></div>';
  var script = document.createElement('script');
  script.src = 'https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit';
  script.onload = function () {
    try {
      var widgetId = window.turnstile.render('#ts_slot', {
        sitekey: '__SITEKEY__',
        action: 'join',
        callback: function (token) {
          console.log('T:' + token);
          setTimeout(function () {
            try { window.turnstile.reset(widgetId); } catch (e) {}
          }, __RESET_DELAY_MS__);
        },
        'error-callback': function (code) {
          console.log('E:' + code);
          setTimeout(function () {
            try { window.turnstile.reset(widgetId); } catch (e) {}
          }, 2000);
        }
      });
    } catch (e) {
      console.log('E:render:' + String(e));
    }
  };
  script.onerror = function () { console.log('E:apiload'); };
  document.head.appendChild(script);
})();
"""


# --------------------------------------------------------------------------
# join verification — the only measurement that counts
# --------------------------------------------------------------------------

def verify_token(verifier: str, token: str, via_proxy: str = "") -> str:
    """Replay one token through gartic's join handshake via the Go helper.

    Returns "JOINED", "REJECTED code=N", "TIMEOUT" or "ERROR:...". The work is
    delegated to experiments/egress/joinverify because neither of the in-process
    options survives contact with reality: a Python websocket-client handshake
    to serverNN.gartic.io is refused by Cloudflare with 403 on TLS fingerprint
    alone, and opening the socket from inside the Camoufox page tears down
    v135's Juggler connection.
    """
    command = [verifier]
    if via_proxy:
        # gartic's WAF answers the server-discovery endpoint with a 403 block
        # page from some runner addresses. Checking over the same tunnel the
        # token was minted on removes that from the measurement.
        command += ["-proxy", via_proxy]
    try:
        completed = subprocess.run(
            command + [token], capture_output=True, text=True, timeout=60,
        )
    except subprocess.TimeoutExpired:
        return "ERROR:verifier-timeout"
    except OSError as error:
        return f"ERROR:verifier-spawn:{type(error).__name__}"
    output = (completed.stdout or "").strip().splitlines()
    return output[-1].strip() if output else "ERROR:verifier-silent"


# --------------------------------------------------------------------------
# proxy plumbing
# --------------------------------------------------------------------------

def parse_proxy(raw: str) -> dict | None:
    """Accept ``scheme://[user:pass@]host:port`` or ``host:port[:user:pass]``.

    The legacy colon form is what CAMOUFOX_PROXIES holds; the URL form is what
    the tunnel arms (WARP/Tor/VPN Gate) need in order to say ``socks5://``.
    """
    raw = raw.strip()
    if not raw:
        return None
    if "://" in raw:
        parsed = urllib.parse.urlparse(raw)
        proxy: dict[str, str] = {
            "server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
        }
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


class Stats:
    def __init__(self) -> None:
        self.tokens = 0
        self.verified = 0
        self.errors: dict[str, int] = {}
        self.verdicts: dict[str, int] = {}
        self.nav: dict[str, object] = {}
        self.started = time.monotonic()
        self.last_token_at = time.monotonic()
        # Sampling clock. Kept on Stats rather than in run_session because the
        # browser is restarted mid-run and the sampling cadence must survive it.
        self.last_verify_at = 0.0

    def rate_per_min(self) -> float:
        elapsed = time.monotonic() - self.started
        return (self.tokens / elapsed * 60.0) if elapsed > 0 else 0.0


async def _block_irrelevant(route):
    """Only the document, Cloudflare's challenge assets, and the join
    verifier's server-discovery call are needed; everything else costs
    bandwidth for nothing."""
    request = route.request
    try:
        if (request.resource_type == "document"
                or "challenges.cloudflare.com" in request.url
                or "/server/?check=" in request.url):
            await route.continue_()
        else:
            await route.abort()
    except Exception:
        pass


async def run_session(args, stats: Stats, deadline: float | None) -> None:
    launch: dict[str, object] = {
        "headless": True,
        "disable_coop": True,
        "humanize": True,
        "os": ("windows", "macos", "linux"),
    }
    if args.executable:
        launch["executable_path"] = str(Path(args.executable))
        launch["ff_version"] = args.ff_version
        launch["i_know_what_im_doing"] = True

    proxy = parse_proxy(os.environ.get("PROXY", ""))
    if proxy:
        launch["proxy"] = proxy
        # Without geoip the reported timezone contradicts the exit IP, which is
        # itself a 600010 trigger — so it is mandatory whenever we tunnel.
        launch["geoip"] = True
    if args.geoip:
        # A default-route tunnel (OpenVPN) has no PROXY to key off, but its exit
        # is just as foreign to the runner's UTC clock, so it needs geoip too.
        launch["geoip"] = True

    renderer = (RENDERER_JS.replace("__SITEKEY__", SITEKEY)
                .replace("__RESET_DELAY_MS__", str(int(args.token_interval * 1000))))
    loop = asyncio.get_running_loop()

    async with AsyncCamoufox(**launch) as browser:
        # no_viewport=True: playwright >= 1.61 sends an isMobile field that
        # v135's Juggler rejects, which kills new_context() outright.
        context = await browser.new_context(no_viewport=True)
        page = await context.new_page()
        await page.route("**/*", _block_irrelevant)

        pending: list[asyncio.Future] = []

        def on_console(message):
            text = message.text
            if text.startswith("T:"):
                token = text[2:]
                gap = time.monotonic() - stats.last_token_at
                stats.last_token_at = time.monotonic()
                stats.tokens += 1
                fingerprint = f"{token[:10]}..{token[-6:]}" if len(token) > 20 else "?"
                print(f"[{stats.tokens:3d}] +{gap:5.1f}s len={len(token)} "
                      f"fp={fingerprint} rate={stats.rate_per_min():.1f}/min",
                      flush=True)
                if args.emit_tokens:
                    print(f"TOKEN {token}", flush=True)
                due = (time.monotonic() - stats.last_verify_at) >= args.verify_interval
                if args.verifier and stats.verified < args.verify_count and due:
                    stats.last_verify_at = time.monotonic()
                    stats.verified += 1
                    index = stats.verified
                    at = time.monotonic() - stats.started

                    async def check(tok=token, idx=index, when=at):
                        verdict = await loop.run_in_executor(
                            None, verify_token, args.verifier, tok,
                            args.verifier_proxy)
                        key = (verdict.split(":")[0] if verdict.startswith("ERROR")
                               else verdict)
                        stats.verdicts[key] = stats.verdicts.get(key, 0) + 1
                        print(f"  VERIFY[{idx}] t=+{when:.0f}s {verdict}", flush=True)

                    pending.append(asyncio.ensure_future(check()))
            elif text.startswith("E:"):
                code = text[2:]
                stats.errors[code] = stats.errors.get(code, 0) + 1
                print(f"  [cf-error] {code}", flush=True)

        page.on("console", on_console)

        try:
            response = await page.goto(TARGET_URL, wait_until="domcontentloaded",
                                       timeout=45_000)
            # Whether the document is really gartic.io or a Cloudflare block page
            # decides how a 600010 should be read, so it must be recorded rather
            # than assumed.
            status = response.status if response else "none"
            title = (await page.title())[:60]
            length = await page.evaluate("() => document.documentElement.innerHTML.length")
            print(f"  [nav] status={status} url={page.url[:60]} "
                  f"title={title!r} html_len={length}", flush=True)
            stats.nav = {"status": status, "title": title, "html_len": length}
        except Exception as error:
            print(f"  [nav] FAILED {type(error).__name__}", flush=True)
            stats.nav = {"status": f"failed:{type(error).__name__}"}
        await asyncio.sleep(0.5)
        try:
            # gartic serves a Report-Only CSP; playwright surfaces that as an
            # add_script_tag failure even though the script does execute.
            await page.add_script_tag(content=renderer)
        except Exception:
            pass

        session_end = time.monotonic() + args.browser_lifetime
        last_reload = time.monotonic()
        try:
            while True:
                now = time.monotonic()
                if now >= session_end or (deadline and now >= deadline):
                    return
                if args.max_tokens and stats.tokens >= args.max_tokens:
                    return
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
        finally:
            page.remove_listener("console", on_console)
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", default="probe")
    parser.add_argument("--duration", type=float, default=300)
    parser.add_argument("--max-tokens", type=int, default=0)
    parser.add_argument("--token-interval", type=float, default=0)
    parser.add_argument("--browser-lifetime", type=float, default=300)
    parser.add_argument("--reload-interval", type=float, default=120)
    parser.add_argument("--stall-timeout", type=float, default=90)
    parser.add_argument("--executable", default="")
    parser.add_argument("--ff-version", type=int, default=135)
    parser.add_argument("--verify-count", type=int, default=3,
                        help="replay this many minted tokens through a real join")
    parser.add_argument("--verify-interval", type=float, default=0,
                        help="minimum seconds between sampled tokens. 0 (the "
                             "default) verifies the FIRST --verify-count tokens, "
                             "which all land in the opening seconds of a run and "
                             "therefore CANNOT detect a mid-run acceptance "
                             "collapse — this pipeline's known silent failure "
                             "mode. Set it to spread sampling over the run.")
    parser.add_argument("--verifier", default="",
                        help="path to the joinverify binary (empty = mint only)")
    parser.add_argument("--verifier-proxy", default="",
                        help="socks5://host:port to send join checks through")
    parser.add_argument("--result-file", default="",
                        help="also write the RESULT json here, so a lost stdout "
                             "tail cannot destroy the measurement")
    parser.add_argument("--geoip", action="store_true",
                        help="force geoip when egress is a default-route tunnel")
    parser.add_argument("--emit-tokens", action="store_true",
                        help="print full tokens (public log! only for off-box verification)")
    args = parser.parse_args()

    stats = Stats()
    deadline = time.monotonic() + args.duration if args.duration else None
    proxy_raw = os.environ.get("PROXY", "").strip()
    proxy_desc = proxy_raw.split("://")[0] + "://" if "://" in proxy_raw else (
        "http-connect" if proxy_raw else "direct")
    print(f"[config] label={args.label} proxy={proxy_desc} ff={args.ff_version} "
          f"lifetime={args.browser_lifetime}s verify={args.verify_count}", flush=True)

    while True:
        if deadline and time.monotonic() >= deadline:
            break
        if args.max_tokens and stats.tokens >= args.max_tokens:
            break
        try:
            await run_session(args, stats, deadline)
        except Exception as error:
            print(f"[session] {type(error).__name__}: {error}", flush=True)
            await asyncio.sleep(2)

    emit_result(args, stats, proxy_desc)
    return 0


def emit_result(args, stats: Stats, proxy_desc: str) -> None:
    """Report to stdout and, if asked, to a file.

    The file is not redundant: a CI step's trailing stdout has been observed to
    go missing when the probe is piped, and a measurement that cannot be read is
    not a measurement.
    """
    payload = json.dumps({
        "label": args.label,
        "proxy": proxy_desc,
        "tokens": stats.tokens,
        "elapsed_s": round(time.monotonic() - stats.started),
        "rate_per_min": round(stats.rate_per_min(), 2),
        "cf_errors": stats.errors,
        "join_verdicts": stats.verdicts,
        "nav": stats.nav,
    }, sort_keys=True)
    print("RESULT " + payload, flush=True)
    if args.result_file:
        try:
            Path(args.result_file).write_text(payload, encoding="utf-8")
        except OSError as error:
            print(f"[result-file] {type(error).__name__}", flush=True)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
