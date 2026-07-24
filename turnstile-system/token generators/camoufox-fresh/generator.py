#!/usr/bin/env python3
"""Camoufox Turnstile token generator for gartic.io — local, headless, no proxy.

Mints Cloudflare Turnstile tokens for gartic.io's sitekey and appends them to a
local JSONL file. Tokens are NEVER posted to the production relay; this tool is
for local experimentation and measurement only.

Two facts are load-bearing and must not be "simplified" away:

  1. ``action: 'join'`` MUST be passed to ``turnstile.render``. gartic validates
     the token's action server-side; without it every join fails with code 5.
  2. The page must genuinely be served from https://gartic.io. Turnstile checks
     the page origin against the sitekey's allowed domains, so a local or
     data: URL cannot host the widget.

Browser choice also matters: Camoufox v152 fails closed on this sitekey with a
continuous stream of Cloudflare error 600010 and yields zero tokens. v135 works.
Pass --executable/--ff-version accordingly (the defaults already point at v135).

Verify what this produces with verify.py, which replays each token through the
icebot joindebug tool and reports the JOINED/REJECTED ratio.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
import urllib.request
from pathlib import Path

from camoufox.async_api import AsyncCamoufox

TARGET_URL = "https://gartic.io"
SITEKEY = "0x4AAAAAABBPKaIbNwnPEfSo"

# Camoufox v152 yields zero tokens on this sitekey (error 600010); v135 works.
DEFAULT_EXECUTABLE = (
    r"D:\projects\gartic\oracle server\_diag\camoufox-token-loop"
    r"\browsers\v135.0.1-beta.24-win.x86_64\camoufox.exe"
)
DEFAULT_FF_VERSION = 135

# Rendered in the page's main world. Camoufox isolates page.evaluate from the
# main world, so window.turnstile is invisible to Python; the widget therefore
# has to drive its own reset from inside the callback.
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
          // Throttling happens here rather than Python-side because only the
          // main world can reach window.turnstile.
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


def build_renderer(reset_delay_ms: int) -> str:
    return RENDERER_JS.replace("__SITEKEY__", SITEKEY).replace(
        "__RESET_DELAY_MS__", str(reset_delay_ms)
    )


class Stats:
    """Counters shared across browser restarts for one run."""

    def __init__(self) -> None:
        self.tokens = 0
        self.errors: dict[str, int] = {}
        self.started = time.monotonic()
        self.last_token_at = time.monotonic()

    def rate_per_min(self) -> float:
        elapsed = time.monotonic() - self.started
        return (self.tokens / elapsed * 60.0) if elapsed > 0 else 0.0

    def note_error(self, code: str) -> None:
        self.errors[code] = self.errors.get(code, 0) + 1


async def _block_irrelevant(route):
    """Allow only the top-level document and Cloudflare's challenge assets.

    gartic's own JS/CSS/images are irrelevant to minting a token and cost real
    bandwidth, so everything else is aborted.
    """
    request = route.request
    try:
        if request.resource_type == "document" or "challenges.cloudflare.com" in request.url:
            await route.continue_()
        else:
            await route.abort()
    except Exception:
        pass


async def run_session(args, stats: Stats, out_handle, deadline: float | None) -> None:
    """Run one browser for up to --browser-lifetime seconds, then return.

    Returning closes the browser, which is the point of a short lifetime: it
    discards the Cloudflare reputation/state accumulated by that instance.
    """
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
    # PROXY is HOST:PORT[:USER:PASS] — an HTTP CONNECT proxy, which keeps the
    # page origin on gartic.io (a URL-rewriting proxy would not; see README).
    # geoip defaults ON whenever a proxy is used: Camoufox otherwise reports a
    # timezone/locale that contradicts the proxy's exit IP, and Cloudflare
    # refuses the challenge with 600010.
    proxy_raw = os.environ.get("PROXY", "").strip()
    if proxy_raw:
        parts = proxy_raw.split(":", 3)
        if len(parts) == 2:
            launch["proxy"] = {"server": f"http://{parts[0]}:{parts[1]}"}
        elif len(parts) == 4:
            launch["proxy"] = {
                "server": f"http://{parts[0]}:{parts[1]}",
                "username": parts[2],
                "password": parts[3],
            }
        else:
            raise SystemExit("PROXY must be HOST:PORT or HOST:PORT:USER:PASS")
        launch["geoip"] = True
    if args.geoip:
        launch["geoip"] = True

    renderer = build_renderer(int(args.token_interval * 1000))

    async with AsyncCamoufox(**launch) as browser:
        # no_viewport=True is required: Playwright >=1.61 sends an `isMobile`
        # field in Browser.setDefaultViewport that v135's Juggler protocol
        # rejects, which otherwise kills new_context() outright.
        context = await browser.new_context(no_viewport=True)
        page = await context.new_page()
        await page.route("**/*", _block_irrelevant)

        def on_console(message):
            text = message.text
            if text.startswith("T:"):
                token = text[2:]
                now = time.time()
                gap = time.monotonic() - stats.last_token_at
                stats.last_token_at = time.monotonic()
                stats.tokens += 1
                record = {
                    "ts": now,
                    "label": args.label,
                    "token": token,
                    "len": len(token),
                    "prefix": token[:2],
                }
                out_handle.write(json.dumps(record) + "\n")
                out_handle.flush()
                if args.relay_url:
                    asyncio.get_running_loop().run_in_executor(
                        None, _post_to_relay, args.relay_url, args.auth_secret, token
                    )
                print(
                    f"[{stats.tokens:3d}] +{gap:5.1f}s  len={len(token)}  "
                    f"rate={stats.rate_per_min():.1f}/min  errs={sum(stats.errors.values())}",
                    flush=True,
                )
            elif text.startswith("E:"):
                code = text[2:]
                stats.note_error(code)
                print(f"  [cf-error] {code}", flush=True)

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
        while True:
            if args.max_tokens and stats.tokens >= args.max_tokens:
                return
            now = time.monotonic()
            if now >= session_end:
                return
            if deadline and now >= deadline:
                return
            # Stall watchdog + periodic freshening both re-inject the renderer.
            stalled = (now - stats.last_token_at) > args.stall_timeout
            due = args.reload_interval and (now - last_reload) >= args.reload_interval
            if stalled or due:
                reason = "stall" if stalled else "periodic"
                print(f"  [reload:{reason}]", flush=True)
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


def _post_to_relay(url: str, secret: str, token: str) -> None:
    """Fire-and-forget POST of one token. Runs in an executor thread so a slow
    relay never stalls minting; failures are counted, not raised."""
    request = urllib.request.Request(
        url,
        data=json.dumps({"token": token}).encode("utf-8"),
        headers={"Content-Type": "application/json", "X-Auth": secret},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()
    except Exception as error:  # noqa: BLE001 - reported via the counter only
        print(f"[relay] {type(error).__name__}", flush=True)


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path("tokens.jsonl"),
                        help="JSONL file to append tokens to")
    parser.add_argument("--label", default="default",
                        help="tag written into each record (for A/B runs)")
    parser.add_argument("--duration", type=float, default=0,
                        help="seconds to run (0 = until --max-tokens or Ctrl-C)")
    parser.add_argument("--max-tokens", type=int, default=0, help="stop after N tokens (0 = no limit)")
    parser.add_argument("--token-interval", type=float, default=0,
                        help="seconds to wait after a token before re-arming the "
                             "widget; the throttle that caps tokens/min")
    parser.add_argument("--browser-lifetime", type=float, default=300,
                        help="seconds before the browser is fully restarted "
                             "(discards accumulated Cloudflare reputation)")
    parser.add_argument("--reload-interval", type=float, default=120,
                        help="seconds between page reloads (0 = never)")
    parser.add_argument("--stall-timeout", type=float, default=90,
                        help="reload if no token arrives within this many seconds")
    parser.add_argument("--humanize", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--headless", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--geoip", action="store_true")
    parser.add_argument("--fingerprint-os", choices=("any", "windows", "macos", "linux"),
                        default="any")
    parser.add_argument("--executable", default=DEFAULT_EXECUTABLE,
                        help="path to camoufox.exe (default: the v135 build)")
    parser.add_argument("--ff-version", type=int, default=DEFAULT_FF_VERSION)
    parser.add_argument("--relay-url", default="",
                        help="POST each token here as well as writing the JSONL "
                             "(needs AUTH_SECRET in the environment). Empty = "
                             "local capture only, which is the default so test "
                             "runs never pollute the live queue.")
    args = parser.parse_args()

    args.auth_secret = os.environ.get("AUTH_SECRET", "").strip()
    if args.relay_url and not args.auth_secret:
        parser.error("--relay-url needs AUTH_SECRET in the environment")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    stats = Stats()
    deadline = time.monotonic() + args.duration if args.duration else None

    print(
        f"[config] label={args.label} humanize={args.humanize} "
        f"lifetime={args.browser_lifetime}s reload={args.reload_interval}s "
        f"interval={args.token_interval}s ff={args.ff_version} "
        f"proxy={'yes' if os.environ.get('PROXY', '').strip() else 'direct'}",
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
                    await run_session(args, stats, handle, deadline)
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
        f"elapsed={elapsed:.0f}s rate={stats.rate_per_min():.2f}/min "
        f"cf_errors={stats.errors or '{}'}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
