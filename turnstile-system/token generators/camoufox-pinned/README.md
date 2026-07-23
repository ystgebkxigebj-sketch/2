# Pinned Camoufox 135 token generator

This package preserves the smallest configuration proven on 2026-07-22:
Camoufox/Firefox `135.0.1-beta.24`, headless, one browser, one tab, one
Turnstile lane, `disable_coop=True`, `humanize=False`, and four Firefox content
processes. The GitHub workflow additionally requires a proxy because hosted
runner datacenter IPs have previously returned Cloudflare error `600010`.

## Repository files

- `.github/workflows/gartic-camoufox-pinned.yml`
- `.github/workflows/camoufox-supervisor.yml`
- `turnstile-system/token generators/camoufox-pinned/generator.py`
- `turnstile-system/token generators/camoufox-pinned/requirements.txt`
- `turnstile-system/token generators/camoufox-pinned/workflow_master.py`

## Proxy bandwidth is the binding constraint — read this before scaling

An accepted Turnstile token costs **~0.75 MB** of proxy traffic (measured: 84
tokens / 64.7 MB). A Webshare account is capped at **250 GB/month**. An
unthrottled producer mints ~2.35 tok/s.

| rate | GB / 30 days |
|---|---|
| 1 token/min | 31.6 GB |
| 1 token/sec | 1,900 GB |
| one unthrottled slot (2.35 tok/s) | ~4,750 GB |

On **2026-07-23** the fleet ran unthrottled on account `rtorfcpq`, which was also
carrying the AFK public-room fill. Between them they burned the entire 250 GB in
about 22 hours and the account went dead mid-run — two slots stopped minting in
the same second. That is the failure this package now defends against.

The consequence to internalise: **no 250 GB plan funds even 1 token/sec.** The
throttle buys survival, not capacity.

### The one knob

`BUDGET_GB_PER_30D` in `.github/workflows/gartic-camoufox-pinned.yml` is the
slice of the account's 250 GB the whole fleet may spend. Everything else is
derived from it — the generator converts it to a per-slot mint rate, and the
supervisor reads `BUDGET_SLOTS` out of the same file to size the fleet, so the
number of producers can never exceed what the budget assumed.

| `BUDGET_GB_PER_30D` | fleet rate | tokens/day |
|---|---|---|
| 30 | 0.95 /min | 1,365 |
| **60 (default)** | **1.90 /min** | **2,731** |
| 120 | 3.79 /min | 5,461 |
| 250 (a whole account) | 7.90 /min | 11,378 |

Leave the rest alone unless you have a measurement:

- `TOKEN_BURST` (30) — credit that accrues while idle, so a room deploy is served
  fast. Bursting cannot overspend: emission over any window is `rate x time +
  burst`, and a full 30-token burst is 22 MB.
- `RELAY_SHELF_TARGET` (4) / `RELAY_IDLE_SHELF` (1) — how many tokens to keep on
  the relay's shelf. This is a shelf, not a watermark, because tokens expire
  after 210 s: *holding* N tokens costs `N/210` tok/s forever. Holding 40 would
  cost 11.4 tok/min — six times the whole default budget.
- `DEMAND_IDLE_SECONDS` (600) — after this long with no token consumed, drop to
  the idle shelf. Demand is detected from the relay's `totalOut`, not `waiters`,
  because icebot calls `/assign` without a `wait` parameter. That is also why the
  idle shelf is 1 and not 0: something has to be takeable for demand to be
  visible at all.
- `MB_PER_TOKEN` (0.75) — override if a slow run overspends its budget. The 0.75
  sample was taken at full speed, so it folds any time-based cost into the
  per-token figure. To measure the time-based part: mint one token, do **not**
  reset, and read `/proc/net/dev` over 300 idle seconds.

### What does not work

- **Byte-per-token optimisation.** The Playwright route handler already aborts
  everything except the page document and `challenges.cloudflare.com`, and the
  document is fetched once per browser cycle. ~0.75 MB is close to the raw cost
  of one solve on this sitekey.
- **Solving without a proxy.** Runner datacenter IPs get Cloudflare error
  `600010`, and tokens solved from a datacenter IP are rejected by gartic at join
  time (measured 0/88 from the Oracle VM). A residential-class exit is required
  for the solve to be *accepted*, not just to succeed.
- **The AFK public-room fill, on metered proxies, ever.** At ~3.3 tok/s it is
  ~6,270 GB/month — twenty-five Webshare accounts.

## Configure GitHub

Create these repository Actions secrets:

1. `CAMOUFOX_AUTH_SECRET`: relay `X-Auth` value.
2. `CAMOUFOX_PROXIES`: newline-separated `HOST:PORT:USER:PASS` proxies. Use
   health-checked residential/ISP exits. Do not commit this list.
   **All proxies from one Webshare account share a credential, so the number of
   distinct `USER` fields in this secret is the number of 250 GB quotas the
   fleet is spending.** The proxy-selection step prints that count as
   `accounts=N` — check it after any change.

Optionally create the Actions variable `CAMOUFOX_RELAY_URL`. If omitted, the
generator uses `https://mohanadino.duckdns.org:8443/add`.

The workflow selects and masks one proxy per slot/run, installs Python 3.9.25,
pins Camoufox 0.4.11 and Playwright 1.59.0, verifies the official Linux browser
asset against SHA-256
`61e1ec455e021720af38a5cc5ff7566121363cb5b82b72f24e381ba2676a4888`, and
runs headlessly for 330 minutes. It then dispatches the same numbered slot's
successor. A per-slot concurrency group prevents duplicates.

The supervisor workflow runs every five minutes and executes the same tested
master planner inside GitHub with `github.token`. It reads `BUDGET_SLOTS` out of
the producer workflow and uses it as `--target`, refills only missing slots,
counts queued jobs toward that ceiling, and needs no always-on PC or personal
access token. The per-slot self-chain remains the fast handoff path; the
supervisor is its recovery layer.

**After lowering `BUDGET_SLOTS`,** producer runs for the now-out-of-range slots
stay active and occupy the capacity the planner is allowed to use, so no refills
happen until they drain. `--retire-stale` clears them at one running run per
five-minute cycle; cancel them by hand in the Actions tab if you want the new
fleet size immediately.

## Bootstrap and supervise manually

Create a fine-grained GitHub token that can read and write Actions for the
repository, then expose it only in the master process environment:

```powershell
$env:GH_TOKEN = Read-Host -MaskInput "GitHub token"
python "turnstile-system/token generators/camoufox-pinned/workflow_master.py" `
  --repo OWNER/REPO --target 2
```

Start cautiously with one slot and inspect its log and relay counter:

```powershell
python "turnstile-system/token generators/camoufox-pinned/workflow_master.py" `
  --repo OWNER/REPO --target 1 --once
```

Then run a dry reconciliation before continuous mode:

```powershell
python "turnstile-system/token generators/camoufox-pinned/workflow_master.py" `
  --repo OWNER/REPO --target 2 --once --dry-run
```

Pass the same `--target` as `BUDGET_SLOTS`. More producers than the budget's
divisor multiplies proxy spend by the mismatch — the in-GitHub supervisor derives
it automatically, but a hand-run master does not.

Continuous mode polls every 60 seconds. It recognizes active runs by their
`Camoufox slot N` run title and dispatches only absent slots. It counts any
unnamed active run against the target, so it cannot knowingly exceed it. Keep
the master in `tmux`, a Windows Scheduled Task, or a supervised service only if
you want faster-than-five-minute external reconciliation.

To stop generation, disable `Camoufox Fleet Supervisor` first, then cancel the
producer runs. Otherwise the next scheduled supervisor pass intentionally
restores missing slots. A local continuous master stops with Ctrl+C.

GitHub Free permits 20 concurrent standard hosted jobs on paper; this account was
observed to admit **four** at a time, queueing the rest. Public repositories get
unlimited Actions minutes, so runner capacity has never been the limit here.
Proxy bandwidth is — size the fleet from `BUDGET_GB_PER_30D`, not from how many
jobs GitHub will run.
