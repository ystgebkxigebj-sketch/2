# Camoufox token-loop experiment — 2026-07-22

## Outcome

The smallest tested configuration reliably produced live Turnstile callbacks and
proved real Gartic platform-0 admission:

- official Camoufox/Firefox `135.0.1-beta.24`
- Camoufox Python package `0.4.11`
- headless mode
- one browser, one tab, one Turnstile lane
- `disable_coop=True`, `humanize=False`, four Firefox content processes
- direct residential Windows connection, and separately a health-checked proxy

No token, relay credential, or proxy credential is stored in this report or in
the deployable package.

## Controlled comparison

The browser build was the only changed variable in the decisive local A/B test.
Both sides used Windows 11, Python 3.13.14, Camoufox package 0.4.11,
Playwright 1.51.0, headless mode, a direct connection, and a 1×1×1 topology.

| Browser build | Trials | Turnstile callbacks | Cloudflare 600010 observations |
| --- | ---: | ---: | ---: |
| 152.0.4-beta.28 | 2 | 0 | 70 |
| 135.0.1-beta.24 | 5 | 5 | 0 |

The five direct v135 callbacks arrived in 8.010–9.650 seconds. Token lengths
were 645–666 characters and every token began with the expected `1.` prefix.
The first three exact callback-to-join loops produced Gartic event 5. Their
token ages at the end of the join attempt were 8595 ms, 1956 ms, and 2186 ms.

Two later direct joins returned Gartic code 4 after a valid callback. This was a
same-device/identity dedup result, not code 5 and not a Turnstile failure. The
test harness classifier was corrected so the evidence file records these as
`UNEXPECTED_CODE_4` rather than the earlier incorrect code-5 label.

A sixth v135 callback was generated through a health-checked proxy with GeoIP
enabled in 11.756 seconds. It also produced Gartic event 5, with token age
3782 ms at join completion. Across all v135 trials the callback rate was 6/6,
event-5 admissions were 4, Turnstile error 600010 occurrences were 0, and
Gartic code-5 rejections were 0.

## Relay and packaging verification

The packaged generator was run once with the pinned v135 executable, the
health-checked proxy, and a local mock relay. It produced one callback, posted
one token, and exited with zero Cloudflare and relay errors. The mock validated
only token length/prefix; token bodies were not logged.

The GitHub Actions package pins the production-like Linux matrix instead of the
local diagnostic matrix:

- Python `3.9.25`
- Camoufox `0.4.11`
- Playwright `1.59.0`
- official Linux Camoufox `135.0.1-beta.24`
- SHA-256 `61e1ec455e021720af38a5cc5ff7566121363cb5b82b72f24e381ba2676a4888`

The workflow was parsed, checked by actionlint 1.7.12 with no findings, and all
Python files compiled successfully. The workflow-master unit suite passes four
tests. A live GitHub-hosted run remains the final environment-specific check;
the workflow deliberately requires a proxy because hosted-runner datacenter IPs
have historically produced Cloudflare 600010.

## Reproduction artifacts

- Deployable package: `turnstile-system/token generators/camoufox-pinned/`
- Workflow: `.github/workflows/gartic-camoufox-pinned.yml`
- Local one-shot harness: `oracle server/_diag/camoufox-token-loop/trial.py`
- Redacted structured results: `oracle server/_diag/camoufox-token-loop/results.jsonl`
- Isolated local browser: `oracle server/_diag/camoufox-token-loop/browsers/v135.0.1-beta.24-win.x86_64/`

The existing global Camoufox v152 cache and the production Oracle generator
were not modified or restarted.
