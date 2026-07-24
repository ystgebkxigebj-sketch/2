"""Pinned Camoufox 135 Turnstile producer for unattended Linux runners.

Tokens are sent directly to the configured relay. Logs contain only aggregate
counts, token length, and the 0./1. prefix; token values are never printed.

Production is deliberately *rate limited*. Every accepted token costs roughly
0.75 MB of proxy bandwidth (measured: 84 tokens / 64.7 MB), and the Webshare
account behind CAMOUFOX_PROXIES is capped at 250 GB per month, so an unthrottled
producer (~2.35 tok/s) burns a whole month's quota in under a day. Two
independent brakes keep the fleet inside that budget:

  * a token bucket (TOKENS_PER_MINUTE refill, TOKEN_BURST capacity) that caps the
    long-run average while still allowing a short burst when a room deploy needs
    many joins at once;
  * relay backpressure (RELAY_QUEUE_HIGH / RELAY_QUEUE_LOW) that stops minting
    entirely while the relay already holds more tokens than it can spend before
    they expire, so we never pay for tokens that would expire unused.

Only browser traffic crosses the metered proxy; relay POSTs and /stats polls go
out over the runner's own network and cost the proxy budget nothing.
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

# Measured on the VM: 84 tokens for 64.7 MB of proxy traffic. That sample was
# taken at full speed, so it folds any time-based cost (idle widget chatter,
# browser cold starts) into the per-token figure. Re-measure with a slow run and
# override MB_PER_TOKEN if the throttled fleet overspends its budget.
DEFAULT_MB_PER_TOKEN = 0.75
MB_PER_TOKEN = float(os.environ.get("MB_PER_TOKEN", DEFAULT_MB_PER_TOKEN))


def env_int(name: str, default: int) -> int:
    # A workflow expression that evaluates to "" sets the variable to empty
    # rather than leaving it unset, so treat blank as absent instead of dying
    # on int("").
    raw = os.environ.get(name, "").strip()
    return int(raw) if raw else default


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    return float(raw) if raw else default


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


def tokens_per_minute_for(
    budget_gb: float, slots: int, mb_per_token: float = MB_PER_TOKEN
) -> float:
    """Per-slot mint rate that spends `budget_gb` over 30 days across `slots`.

    The operator only ever picks a number of gigabytes; every other pacing value
    is derived from it, so there is one place to be wrong instead of four.
    """
    slots = max(1, slots)
    minutes = 30 * 24 * 60
    return (budget_gb * 1024.0) / max(0.001, mb_per_token) / minutes / slots


def stats_url_for(relay_url: str) -> str:
    """Derive the relay's public /stats endpoint from its /add endpoint."""
    override = os.environ.get("RELAY_STATS_URL", "").strip()
    if override:
        return override
    base = relay_url.rstrip("/")
    if base.endswith("/add"):
        base = base[: -len("/add")]
    return f"{base}/stats"


class MintBudget:
    """Token bucket over *minted Turnstile tokens*, i.e. over proxy bytes.

    Credits refill at `per_minute` and cap at `burst`. Every observed callback
    spends one credit — including the free one each freshly rendered widget
    produces before it is ever reset — so credits may go briefly negative and the
    next mint simply waits longer. That keeps the long-run average equal to the
    refill rate no matter how often the browser is recycled.
    """

    def __init__(self, per_minute: float, burst: float, initial: float) -> None:
        self.rate = max(0.0, per_minute) / 60.0
        self.capacity = max(1.0, burst)
        self.credits = min(initial, self.capacity)
        self.updated = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        if self.rate > 0:
            self.credits = min(self.capacity, self.credits + (now - self.updated) * self.rate)
        self.updated = now

    def consume(self) -> None:
        self._refill()
        self.credits -= 1.0

    def wait_seconds(self) -> float:
        """How long until one whole credit is available (inf if refill is off)."""
        self._refill()
        if self.credits >= 1.0:
            return 0.0
        if self.rate <= 0:
            return float("inf")
        return (1.0 - self.credits) / self.rate


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
    throttled_seconds: float = 0.0
    backpressure_seconds: float = 0.0


class Generator:
    def __init__(self) -> None:
        self.relay_url = os.environ.get("RELAY_URL", "").strip() or DEFAULT_RELAY_URL
        self.stats_url = stats_url_for(self.relay_url)
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
        # A recycle costs a fresh page load and Turnstile bundle download. At the
        # old flat-out rate a 50-minute cycle amortised that over ~7000 tokens;
        # at a budgeted rate it would be over ~50, so recycle on tokens produced
        # with the clock only as a backstop against leaks.
        self.browser_lifetime = env_int("BROWSER_LIFETIME_SECONDS", 165 * 60)
        self.browser_max_tokens = env_int("BROWSER_MAX_TOKENS", 400)
        self.startup_timeout = env_int("STARTUP_TOKEN_TIMEOUT_SECONDS", 120)
        self.stall_timeout = env_int("TOKEN_STALL_TIMEOUT_SECONDS", 180)
        self.runtime = env_int("MAX_RUNTIME_MINUTES", 330) * 60

        # --- bandwidth brakes -------------------------------------------------
        # The operator sets a gigabyte allowance for the whole fleet and how many
        # slots share it; the per-slot rate falls out of that. The supervisor
        # reads BUDGET_SLOTS out of the producer workflow to size the fleet, so
        # the divisor here and the number of live producers cannot disagree.
        self.budget_gb = env_float("BUDGET_GB_PER_30D", 60.0)
        self.budget_slots = env_int("BUDGET_SLOTS", 2)
        self.tokens_per_minute = env_float(
            "TOKENS_PER_MINUTE", tokens_per_minute_for(self.budget_gb, self.budget_slots)
        )
        self.token_burst = env_float("TOKEN_BURST", 30.0)
        self.initial_credits = env_float("TOKEN_INITIAL_CREDITS", 5.0)
        self.budget = MintBudget(
            self.tokens_per_minute, self.token_burst, self.initial_credits
        )
        # Relay backpressure, expressed as a shelf to keep stocked rather than a
        # high/low watermark. A watermark cannot work here: tokens expire after
        # `usableTTL` (210 s), so merely *holding* N tokens costs N/210 tok/s of
        # continuous production — holding 40 would cost 11.4 tok/min, six times
        # the entire budget. The affordable shelf is single digits.
        self.shelf_target = env_int("RELAY_SHELF_TARGET", 4)
        # ...and when nothing has consumed a token for a while, an even smaller
        # one, because during idle hours every token minted expires unused.
        self.idle_shelf = env_int("RELAY_IDLE_SHELF", 1)
        self.demand_idle_seconds = env_float("DEMAND_IDLE_SECONDS", 600.0)
        self.queue_poll_seconds = env_float("RELAY_QUEUE_POLL_SECONDS", 5.0)
        self.queue_available: int | None = None
        self.relay_total_out: int | None = None
        self.last_demand_at = time.monotonic()
        self.backpressure = False
        # Turnstile errors are now budget-gated like tokens, so they no longer
        # trip the stall clock. This counts them instead.
        self.cf_error_limit = env_int("CF_ERROR_LIMIT", 10)
        self.errors_since_token = 0
        # Browser cycles that die before producing anything — a dead proxy, a
        # failed launch. The per-lane watchdogs live inside a cycle and never run
        # when the launch itself throws, so this is the only backstop.
        self.cycle_failure_limit = env_int("CYCLE_FAILURE_LIMIT", 5)
        self.cycle_failures = 0
        # True between arming a widget and its callback; keeps the clicker quiet
        # (and byte-free) while the producer is deliberately idle.
        self.solving = True

        self.stats = Stats()
        self.stop = asyncio.Event()
        self.post_tasks: set[asyncio.Task] = set()
        self.mint_tasks: set[asyncio.Task] = set()
        self.last_error_log = 0.0
        # When the pacing logic expects the next token. The stall watchdog is
        # measured from this instead of from the last token, so a deliberate
        # throttle or backpressure hold never looks like a broken browser.
        self.next_mint_estimate = time.monotonic()

    def renderer_js(self) -> str:
        return r"""
(function () {
  document.body.innerHTML = '';
  window.__tsWidgets = [];
  window.__tsReset = function (idx) {
    try {
      window.turnstile.reset(window.__tsWidgets[idx]);
      return true;
    } catch (error) {
      return false;
    }
  };
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
                // NOTE: no reset() here. The Python side resets the widget once
                // the bandwidth budget allows the next solve; resetting straight
                // away is what made this producer burn 250 GB in a day.
                console.log('T:' + idx + ':' + token);
              },
              'error-callback': function (error) {
                // Also budgeted: a failed challenge still cost proxy bytes, and
                // an unbudgeted retry here is exactly how an error storm on a
                // bad proxy would blow the monthly quota.
                console.log('CF:err:lane' + idx + ':' + error);
              }
            });
            window.__tsWidgets[idx] = widgetId;
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

    def current_shelf_target(self) -> int:
        """How many tokens the relay should be holding right now."""
        idle = (time.monotonic() - self.last_demand_at) >= self.demand_idle_seconds
        return self.idle_shelf if idle else self.shelf_target

    async def poll_relay_queue(self, session: aiohttp.ClientSession) -> None:
        """Track the relay's stock and whether anything is still consuming it.

        Runs on the runner's own network, not through the metered proxy, so this
        loop is free.

        Demand is read from `totalOut` (tokens handed to icebot), not from
        `waiters`: icebot calls /assign without a `wait` parameter, so the relay
        never records a waiter and an empty shelf looks identical to no demand.
        That is exactly why the idle shelf is 1 rather than 0 — a bot has to be
        able to take a token for us to see that anyone wants one.
        """
        if self.shelf_target <= 0:
            return
        while not self.stop.is_set():
            try:
                async with session.get(self.stats_url) as response:
                    payload = await response.json(content_type=None)
                available = int(payload.get("available", payload.get("queued", 0)))
                total_out = int(payload.get("totalOut", 0))
                if self.relay_total_out is not None and total_out > self.relay_total_out:
                    self.last_demand_at = time.monotonic()
                self.relay_total_out = total_out
                self.queue_available = available
                holding = available >= self.current_shelf_target()
                if holding != self.backpressure:
                    self.backpressure = holding
                    print(
                        f"[budget] relay available={available} "
                        f"target={self.current_shelf_target()} "
                        f"=> {'holding' if holding else 'minting'}",
                        flush=True,
                    )
            except Exception as error:
                # An unreadable relay must not wedge the producer: keep the last
                # known state and retry. The token bucket still caps spend, so
                # failing open here cannot blow the budget.
                now = time.monotonic()
                if now - self.last_error_log >= 30:
                    self.last_error_log = now
                    print(f"[budget] stats poll failed: {type(error).__name__}", flush=True)
            await asyncio.sleep(self.queue_poll_seconds)

    async def await_budget(self) -> bool:
        """Block until one token's worth of bandwidth budget is available."""
        started = time.monotonic()
        while not self.stop.is_set():
            if self.backpressure:
                self.next_mint_estimate = time.monotonic() + self.queue_poll_seconds
                await asyncio.sleep(min(self.queue_poll_seconds, 5.0))
                self.stats.backpressure_seconds += min(self.queue_poll_seconds, 5.0)
                continue
            wait = self.budget.wait_seconds()
            if wait <= 0:
                self.stats.throttled_seconds += time.monotonic() - started
                return True
            self.next_mint_estimate = time.monotonic() + wait
            await asyncio.sleep(min(wait, 5.0))
        return False

    async def mint_next(self, page, lane: int) -> None:
        """Wait out the budget, then arm the widget for its next solve."""
        if not await self.await_budget():
            return
        if page.is_closed() or self.stop.is_set():
            return
        try:
            self.solving = True
            ok = await page.evaluate("(idx) => window.__tsReset(idx)", lane)
            if not ok:
                print(f"[turnstile] reset lane={lane} rejected", flush=True)
        except Exception as error:
            print(f"[turnstile] reset lane={lane} failed: {type(error).__name__}", flush=True)

    def schedule_mint(self, page, lane: int) -> None:
        task = asyncio.create_task(self.mint_next(page, lane))
        self.mint_tasks.add(task)
        task.add_done_callback(self.mint_tasks.discard)

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
                lane_raw, _, token = text[2:].partition(":")
                try:
                    lane = int(lane_raw)
                except ValueError:
                    lane, token = 0, text[2:]
                self.stats.callbacks += 1
                self.stats.last_token_at = time.monotonic()
                self.errors_since_token = 0
                self.solving = False
                self.budget.consume()
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
                    f"L0={self.stats.level_0} tab={tab_id} "
                    f"credits={self.budget.credits:.1f} queue={self.queue_available} "
                    f"est_mb={self.stats.callbacks * MB_PER_TOKEN:.0f}",
                    flush=True,
                )
                self.schedule_post(session, token)
                if self.stop_after_callbacks and self.stats.callbacks >= self.stop_after_callbacks:
                    self.stop.set()
                    return
                self.schedule_mint(page, lane)
            elif text.startswith("CF:err:"):
                self.stats.cf_errors += 1
                now = time.monotonic()
                if now - self.last_error_log >= 10:
                    self.last_error_log = now
                    print(f"[turnstile] {text}", flush=True)
                detail = text[len("CF:err:"):]
                if detail.startswith("lane"):
                    lane_raw, _, _ = detail[len("lane"):].partition(":")
                    try:
                        lane = int(lane_raw)
                    except ValueError:
                        return
                    # Charge the failed attempt, then re-arm on the budget.
                    self.errors_since_token += 1
                    self.solving = False
                    self.budget.consume()
                    self.schedule_mint(page, lane)

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
                # Only poke the widget while a solve is actually in flight.
                # Clicking an idle, already-solved widget can kick off a fresh
                # challenge fetch, which would spend proxy bytes outside the
                # budget and mint tokens nobody asked for.
                if not self.solving:
                    await asyncio.sleep(1)
                    continue
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
                if self.stats.callbacks - callbacks_at_start >= self.browser_max_tokens:
                    print(
                        f"[browser] recycling after {self.browser_max_tokens} tokens",
                        flush=True,
                    )
                    return
                # Both watchdogs are measured against when a token was actually
                # *due*. Under a slow budget or a backpressure hold the browser is
                # healthy and idle by design, and recycling it would cost a page
                # load for nothing.
                due = max(self.stats.last_token_at, self.next_mint_estimate)
                if self.errors_since_token >= self.cf_error_limit:
                    # Budgeted retries can no longer be told apart from a healthy
                    # slow producer by the stall clock, so failing the run is what
                    # rotates us onto a different proxy: the successor dispatch
                    # picks a new index from GITHUB_RUN_NUMBER.
                    raise RuntimeError(
                        f"{self.errors_since_token} consecutive Turnstile errors; proxy likely dead"
                    )
                if self.stats.callbacks == callbacks_at_start:
                    # The first solve of a cycle is never budget-gated, so this
                    # clock stays honest even under a slow rate or a relay hold.
                    if now - cycle_started >= self.startup_timeout:
                        raise RuntimeError(
                            f"no callback in {self.startup_timeout}s; cf_errors={self.stats.cf_errors}"
                        )
                elif now - due >= self.stall_timeout:
                    raise RuntimeError(f"token stream stalled for {self.stall_timeout}s")

    async def run(self) -> None:
        deadline = time.monotonic() + self.runtime
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            poller = asyncio.create_task(self.poll_relay_queue(session))
            try:
                while not self.stop.is_set() and time.monotonic() < deadline:
                    before = self.stats.callbacks
                    try:
                        await self.browser_cycle(session, deadline)
                        if self.stats.callbacks > before:
                            self.cycle_failures = 0
                    except Exception as error:
                        self.cycle_failures = 0 if self.stats.callbacks > before else self.cycle_failures + 1
                        print(
                            f"[browser] recycle after {type(error).__name__}: {error} "
                            f"(consecutive={self.cycle_failures}/{self.cycle_failure_limit})",
                            flush=True,
                        )
                        # A run is pinned to ONE proxy for its whole life, so a
                        # cycle that never produces a token will never produce one:
                        # retrying here just spins. Failing the run is the only way
                        # to rotate proxies, because the successor dispatch picks a
                        # new index from GITHUB_RUN_NUMBER. Without this a dead
                        # proxy burned a slot for 25 minutes over 282 identical
                        # `InvalidProxy` relaunches on 2026-07-24.
                        if self.cycle_failures >= self.cycle_failure_limit:
                            raise RuntimeError(
                                f"{self.cycle_failures} consecutive failed browser cycles "
                                f"({type(error).__name__}); failing the run so the successor "
                                f"picks a different proxy"
                            ) from error
                        await asyncio.sleep(min(5 * self.cycle_failures, 60))
            finally:
                self.stop.set()
                poller.cancel()
                for task in list(self.mint_tasks):
                    task.cancel()
            if self.post_tasks:
                await asyncio.gather(*self.post_tasks, return_exceptions=True)
        print(
            f"[summary] callbacks={self.stats.callbacks} posted={self.stats.posted} "
            f"cf_errors={self.stats.cf_errors} relay_errors={self.stats.relay_errors} "
            f"est_mb={self.stats.callbacks * MB_PER_TOKEN:.0f} "
            f"throttled_s={self.stats.throttled_seconds:.0f} "
            f"backpressure_s={self.stats.backpressure_seconds:.0f}",
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
    per_slot_gb = generator.tokens_per_minute * 60 * 24 * 30 * MB_PER_TOKEN / 1024
    print(
        f"[config] runtime={generator.runtime}s tabs={generator.tabs} lanes={generator.lanes} "
        f"proxy={'configured' if generator.proxy else 'direct'} relay=configured "
        f"rate={generator.tokens_per_minute:.3f}/min burst={generator.token_burst:.0f} "
        f"shelf={generator.shelf_target}/idle={generator.idle_shelf} "
        f"mb_per_token={MB_PER_TOKEN} "
        f"budget={generator.budget_gb:.0f}GB/30d over {generator.budget_slots} slots "
        f"=> {per_slot_gb:.1f}GB/30d this slot",
        flush=True,
    )
    await generator.run()


if __name__ == "__main__":
    asyncio.run(main())
