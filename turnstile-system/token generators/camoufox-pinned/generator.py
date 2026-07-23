"""Pinned Camoufox 135 Turnstile producer for unattended Linux runners.

Tokens are sent directly to the configured relay. Logs contain only aggregate
counts, token length, and the 0./1. prefix; token values are never printed.
"""

from __future__ import annotations

import asyncio
import os
import signal
import time
from dataclasses import dataclass

import aiohttp
from camoufox.async_api import AsyncCamoufox


TARGET_URL = "https://gartic.io"
SITEKEY = "0x4AAAAAABBPKaIbNwnPEfSo"
DEFAULT_RELAY_URL = "https://mohanadino.duckdns.org:8443/add"


def env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def parse_proxy(raw: str) -> dict[str, str]:
    parts = raw.strip().split(":", 3)
    if len(parts) == 2:
        return {"server": f"http://{parts[0]}:{parts[1]}"}
    if len(parts) == 4:
        return {
            "server": f"http://{parts[0]}:{parts[1]}",
            "username": parts[2],
            "password": parts[3],
        }
    raise ValueError("PROXY must be HOST:PORT or HOST:PORT:USER:PASS")


@dataclass
class Stats:
    callbacks: int = 0
    posted: int = 0
    relay_errors: int = 0
    cf_errors: int = 0
    level_0: int = 0
    level_1: int = 0
    other: int = 0
    last_token_at: float = 0.0


class Generator:
    def __init__(self) -> None:
        self.relay_url = os.environ.get("RELAY_URL", "").strip() or DEFAULT_RELAY_URL
        self.auth_secret = os.environ.get("AUTH_SECRET", "").strip()
        proxy_raw = os.environ.get("PROXY", "").strip()
        require_proxy = os.environ.get("REQUIRE_PROXY", "0") == "1"
        if not self.auth_secret:
            raise ValueError("AUTH_SECRET is required")
        if require_proxy and not proxy_raw:
            raise ValueError("PROXY is required on GitHub-hosted runners")
        self.proxy = parse_proxy(proxy_raw) if proxy_raw else None
        self.tabs = env_int("NUM_TABS", 1)
        self.lanes = env_int("NUM_LANES", 1)
        self.stop_after_callbacks = env_int("STOP_AFTER_CALLBACKS", 0)
        self.executable = os.environ.get("CAMOUFOX_EXECUTABLE", "").strip()
        self.ff_version = env_int("CAMOUFOX_FF_VERSION", 0)
        self.lane_stagger_ms = env_int("LANE_STAGGER_MS", 500)
        self.browser_lifetime = env_int("BROWSER_LIFETIME_SECONDS", 50 * 60)
        self.startup_timeout = env_int("STARTUP_TOKEN_TIMEOUT_SECONDS", 120)
        self.stall_timeout = env_int("TOKEN_STALL_TIMEOUT_SECONDS", 180)
        self.runtime = env_int("MAX_RUNTIME_MINUTES", 330) * 60
        self.stats = Stats()
        self.stop = asyncio.Event()
        self.post_tasks: set[asyncio.Task] = set()
        self.last_error_log = 0.0

    def renderer_js(self) -> str:
        return r"""
(function () {
  document.body.innerHTML = '';
  for (var i = 0; i < __LANES__; i++) {
    var div = document.createElement('div');
    div.id = 'kutu_' + i;
    document.body.appendChild(div);
  }
  var script = document.createElement('script');
  script.src = 'https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit';
  script.onload = function () {
    for (var i = 0; i < __LANES__; i++) {
      (function (idx) {
        setTimeout(function () {
          try {
            var widgetId = window.turnstile.render('#kutu_' + idx, {
              sitekey: '__SITEKEY__',
              // gartic validates the token's action server-side (siteverify).
              // Its own client renders with action:'join'; tokens minted without
              // it are rejected at join time with event-6 code 5.
              action: 'join',
              callback: function (token) {
                console.log('T:' + token);
                window.turnstile.reset(widgetId);
              },
              'error-callback': function (error) {
                console.log('CF:err:' + error);
                try { window.turnstile.reset(widgetId); } catch (_) {}
              }
            });
          } catch (error) { console.log('CF:err:render:' + String(error)); }
        }, idx * __STAGGER__);
      })(i);
    }
  };
  script.onerror = function () { console.log('CF:err:apiload'); };
  document.head.appendChild(script);
})();
""".replace("__LANES__", str(self.lanes)).replace(
            "__STAGGER__", str(self.lane_stagger_ms)
        ).replace("__SITEKEY__", SITEKEY)

    async def post_token(self, session: aiohttp.ClientSession, token: str) -> None:
        try:
            async with session.post(
                self.relay_url,
                json={"token": token},
                headers={"X-Auth": self.auth_secret},
            ) as response:
                await response.read()
                if response.status >= 400:
                    raise RuntimeError(f"relay HTTP {response.status}")
                self.stats.posted += 1
        except Exception as error:
            self.stats.relay_errors += 1
            now = time.monotonic()
            if now - self.last_error_log >= 10:
                self.last_error_log = now
                print(f"[relay] {type(error).__name__}: {error}", flush=True)

    def schedule_post(self, session: aiohttp.ClientSession, token: str) -> None:
        task = asyncio.create_task(self.post_token(session, token))
        self.post_tasks.add(task)
        task.add_done_callback(self.post_tasks.discard)

    async def route_handler(self, route) -> None:
        request = route.request
        if request.resource_type == "document" or "challenges.cloudflare.com" in request.url:
            await route.continue_()
        else:
            await route.abort()

    async def setup_tab(self, page, tab_id: int, session: aiohttp.ClientSession) -> None:
        def on_console(message) -> None:
            text = message.text
            if text.startswith("T:"):
                token = text[2:]
                self.stats.callbacks += 1
                self.stats.last_token_at = time.monotonic()
                prefix = token[:2] if len(token) >= 2 and token[1] == "." else "other"
                if prefix == "0.":
                    self.stats.level_0 += 1
                elif prefix == "1.":
                    self.stats.level_1 += 1
                else:
                    self.stats.other += 1
                print(
                    f"[token] total={self.stats.callbacks} posted={self.stats.posted} "
                    f"prefix={prefix} len={len(token)} L1={self.stats.level_1} "
                    f"L0={self.stats.level_0} tab={tab_id}",
                    flush=True,
                )
                self.schedule_post(session, token)
                if self.stop_after_callbacks and self.stats.callbacks >= self.stop_after_callbacks:
                    self.stop.set()
            elif text.startswith("CF:err:"):
                self.stats.cf_errors += 1
                now = time.monotonic()
                if now - self.last_error_log >= 10:
                    self.last_error_log = now
                    print(f"[turnstile] {text}", flush=True)

        page.on("console", on_console)
        await page.route("**/*", self.route_handler)
        try:
            await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60_000)
        except Exception as error:
            print(f"[navigation] {type(error).__name__}", flush=True)
        await asyncio.sleep(0.5)
        try:
            await page.add_script_tag(content=self.renderer_js())
        except Exception:
            pass

        async def clicker() -> None:
            while not self.stop.is_set() and not page.is_closed():
                for frame in page.frames:
                    if "turnstile" not in frame.url:
                        continue
                    for selector in ("#challenge-stage", 'input[type="checkbox"]'):
                        try:
                            await frame.click(selector, timeout=500)
                        except Exception:
                            pass
                await asyncio.sleep(3)

        asyncio.create_task(clicker())

    async def browser_cycle(self, session: aiohttp.ClientSession, deadline: float) -> None:
        cycle_started = time.monotonic()
        callbacks_at_start = self.stats.callbacks
        print(
            f"[browser] launch headless=true proxy={'configured' if self.proxy else 'direct'} "
            f"tabs={self.tabs} lanes={self.lanes}",
            flush=True,
        )
        launch_options = dict(
            headless=True,
            disable_coop=True,
            humanize=False,
            proxy=self.proxy,
            geoip=True if self.proxy else None,
            os=("windows", "macos", "linux"),
            firefox_user_prefs={
                "fission.autostart": False,
                "dom.ipc.processCount": 4,
                "dom.ipc.processCount.webIsolated": 4,
            },
        )
        if self.executable:
            launch_options["executable_path"] = self.executable
        if self.ff_version:
            launch_options["ff_version"] = self.ff_version
            launch_options["i_know_what_im_doing"] = True
        async with AsyncCamoufox(**launch_options) as browser:
            for index in range(self.tabs):
                context = await browser.new_context()
                page = await context.new_page()
                await self.setup_tab(page, index + 1, session)
                if index < self.tabs - 1:
                    await asyncio.sleep(3)

            cycle_deadline = min(deadline, cycle_started + self.browser_lifetime)
            while not self.stop.is_set() and time.monotonic() < cycle_deadline:
                await asyncio.sleep(5)
                now = time.monotonic()
                if self.stats.callbacks == callbacks_at_start:
                    if now - cycle_started >= self.startup_timeout:
                        raise RuntimeError(
                            f"no callback in {self.startup_timeout}s; cf_errors={self.stats.cf_errors}"
                        )
                elif now - self.stats.last_token_at >= self.stall_timeout:
                    raise RuntimeError(f"token stream stalled for {self.stall_timeout}s")

    async def run(self) -> None:
        deadline = time.monotonic() + self.runtime
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while not self.stop.is_set() and time.monotonic() < deadline:
                try:
                    await self.browser_cycle(session, deadline)
                except Exception as error:
                    print(f"[browser] recycle after {type(error).__name__}: {error}", flush=True)
                    await asyncio.sleep(5)
            if self.post_tasks:
                await asyncio.gather(*self.post_tasks, return_exceptions=True)
        print(
            f"[summary] callbacks={self.stats.callbacks} posted={self.stats.posted} "
            f"cf_errors={self.stats.cf_errors} relay_errors={self.stats.relay_errors}",
            flush=True,
        )
        if self.stats.callbacks == 0 or self.stats.posted == 0:
            raise RuntimeError("generator ended without a relayed callback")


async def main() -> None:
    generator = Generator()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, generator.stop.set)
        except NotImplementedError:
            pass
    print(
        f"[config] runtime={generator.runtime}s tabs={generator.tabs} lanes={generator.lanes} "
        f"proxy={'configured' if generator.proxy else 'direct'} relay=configured",
        flush=True,
    )
    await generator.run()


if __name__ == "__main__":
    asyncio.run(main())
