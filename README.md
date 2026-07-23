# Pinned Camoufox 135 token generator

Headless Camoufox/Firefox `135.0.1-beta.24` producing Cloudflare Turnstile tokens
on GitHub-hosted runners and posting them to the relay.

**Full documentation lives in
[`turnstile-system/token generators/camoufox-pinned/README.md`](turnstile-system/token%20generators/camoufox-pinned/README.md).**
This file is deliberately a pointer — it used to be a byte-for-byte copy of that
README and the two drifted.

## Read this before changing the fleet size

An accepted token costs **~0.75 MB** of proxy bandwidth and a Webshare account is
capped at **250 GB/month**, so one unthrottled producer is ~4,750 GB/month — an
account per 38 hours. On 2026-07-23 an unthrottled 20-slot fleet did exactly
that.

The fleet is therefore sized by **bandwidth**, not by how many jobs GitHub will
run. One knob, in `.github/workflows/gartic-camoufox-pinned.yml`:

```yaml
BUDGET_GB_PER_30D: "60"   # what the whole fleet may spend in 30 days
BUDGET_SLOTS: "2"         # producers sharing it; the supervisor reads this
```

The generator derives its mint rate from those two, and the supervisor derives
`--target` from `BUDGET_SLOTS`, so the number of producers can never exceed what
the budget assumed.

## Repository files

- `.github/workflows/gartic-camoufox-pinned.yml` — the producer
- `.github/workflows/camoufox-supervisor.yml` — five-minute slot reconciler
- `turnstile-system/token generators/camoufox-pinned/generator.py`
- `turnstile-system/token generators/camoufox-pinned/workflow_master.py`
- `turnstile-system/token generators/camoufox-pinned/requirements.txt`
- `turnstile-system/token generators/camoufox-pinned/test_*.py`

## Actions secrets

1. `CAMOUFOX_AUTH_SECRET` — relay `X-Auth` value.
2. `CAMOUFOX_PROXIES` — newline-separated `HOST:PORT:USER:PASS`. All proxies in
   one Webshare account share a credential, so the number of distinct `USER`
   fields is the number of 250 GB quotas this fleet spends. The workflow logs
   that as `accounts=N`; it should be 1.

Optional Actions variable `CAMOUFOX_RELAY_URL` (defaults to
`https://mohanadino.duckdns.org:8443/add`).
