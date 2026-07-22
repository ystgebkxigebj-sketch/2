# Pinned Camoufox 135 token generator

This package preserves the smallest configuration proven on 2026-07-22:
Camoufox/Firefox `135.0.1-beta.24`, headless, one browser, one tab, one
Turnstile lane, `disable_coop=True`, `humanize=False`, and four Firefox content
processes. The GitHub workflow additionally requires a proxy because hosted
runner datacenter IPs have previously returned Cloudflare error `600010`.

## Repository files

- `.github/workflows/gartic-camoufox-pinned.yml`
- `turnstile-system/token generators/camoufox-pinned/generator.py`
- `turnstile-system/token generators/camoufox-pinned/requirements.txt`
- `turnstile-system/token generators/camoufox-pinned/workflow_master.py`

## Configure GitHub

Create these repository Actions secrets:

1. `CAMOUFOX_AUTH_SECRET`: relay `X-Auth` value.
2. `CAMOUFOX_PROXIES`: newline-separated `HOST:PORT:USER:PASS` proxies. Use
   health-checked residential/ISP exits. Do not commit this list.

Optionally create the Actions variable `CAMOUFOX_RELAY_URL`. If omitted, the
generator uses `https://mohanadino.duckdns.org:8443/add`.

The workflow selects and masks one proxy per slot/run, installs Python 3.9.25,
pins Camoufox 0.4.11 and Playwright 1.59.0, verifies the official Linux browser
asset against SHA-256
`61e1ec455e021720af38a5cc5ff7566121363cb5b82b72f24e381ba2676a4888`, and
runs headlessly for 330 minutes. It then dispatches the same numbered slot's
successor. A per-slot concurrency group prevents duplicates.

## Bootstrap and supervise

Create a fine-grained GitHub token that can read and write Actions for the
repository, then expose it only in the master process environment:

```powershell
$env:GH_TOKEN = Read-Host -MaskInput "GitHub token"
python "turnstile-system/token generators/camoufox-pinned/workflow_master.py" `
  --repo OWNER/REPO --target 20
```

Start cautiously with one slot and inspect its log and relay counter:

```powershell
python "turnstile-system/token generators/camoufox-pinned/workflow_master.py" `
  --repo OWNER/REPO --target 1 --once
```

Then run a dry reconciliation before continuous mode:

```powershell
python "turnstile-system/token generators/camoufox-pinned/workflow_master.py" `
  --repo OWNER/REPO --target 20 --once --dry-run
```

Continuous mode polls every 60 seconds. It recognizes active runs by their
`Camoufox slot N` run title and dispatches only absent slots. It counts any
unnamed active run against the target, so it cannot knowingly exceed 20. Keep
the master in `tmux`, a Windows Scheduled Task, or a supervised service.

Stop it with Ctrl+C. The producer runs already in GitHub continue and normally
self-chain; cancel those from the Actions page when you want generation to end.

GitHub Free currently permits 20 concurrent standard hosted jobs, but private
repositories also have a monthly Actions-minute allowance. Check the account's
plan and usage before bootstrapping 20 long-lived slots.
