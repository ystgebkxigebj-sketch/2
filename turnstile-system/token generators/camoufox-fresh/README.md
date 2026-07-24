# camoufox-fresh — local, proxy-free Turnstile producer

Built and measured 2026-07-24. Produces gartic.io Turnstile tokens from a
**residential** connection with **no proxy**, and sustains a higher rate than
either the Oracle VM or the GitHub fleet.

## Measured results

Sustained run, unthrottled, one browser / one tab / one lane:

| cumulative tokens | rate | acceptance |
|---|---|---|
| 259 | 46.5/min | 2/2 |
| 474 | 46.8/min | 2/2 |
| 662 | 45.1/min | 2/2 |
| 871 | 45.3/min | 2/2 |
| **1063** | **44.6/min** | **2/2** |

**1063 tokens over ~23 min, 10/10 accepted, 1 Cloudflare error total, no decay.**
Acceptance was measured by replaying tokens through `cmd/joindebug` for a real
`VERDICT: JOINED (event 5)` — not by counting callbacks.

### Versus the alternatives

| source | rate | proxy | bandwidth cost |
|---|---|---|---|
| **this, on a residential PC** | **~45/min** | **none** | **free** |
| Oracle VM (`camoufox-oracle`) | 35/min (rate-capped) | none | free (Oracle egress) |
| GitHub fleet (`camoufox-pinned`) | ~2/min (byte-budgeted) | **required** | ~0.75 MB/token |

## Winning configuration

```
headless=True, disable_coop=True, humanize=True,
os=("windows","macos","linux"),
executable_path=<v135.0.1-beta.24>, ff_version=135, i_know_what_im_doing=True
browser-lifetime 300s   # full restart; discards accumulated Cloudflare reputation
reload-interval 120s
token-interval 0        # unthrottled
1 browser / 1 tab / 1 lane
```

Run it:

```bash
python generator.py --out tokens.jsonl --duration 600
```

Feed the live relay (opt-in, so test runs never pollute the queue):

```bash
AUTH_SECRET=<relay secret> python generator.py \
  --relay-url https://mohanadino.duckdns.org:8443/add --duration 0
```

Verify tokens actually join — the only measurement that counts:

```bash
cd bots/booooooot/tokenServer
go run ./cmd/joindebug -lang 19 -platform 0 -tokenval "<TOKEN>" -nick probe
```

## Non-negotiables (each proven by measurement)

1. **`action: 'join'`** in `turnstile.render`. gartic validates it server-side;
   without it every join fails with event-6 code 5.
2. **Camoufox v135.0.1-beta.24 only.** `camoufox fetch` installs v152, which
   yields **zero** tokens on this sitekey — 70x error `600010`, measured on a
   residential line, so it is not an IP problem.
3. **playwright 1.61.0 with `no_viewport=True`.** playwright >= 1.61 sends an
   `isMobile` field in `Browser.setDefaultViewport` that v135's Juggler rejects,
   which kills `new_context()` outright.
4. **The page must be served from gartic.io.** See below.
5. `-platform 0` when joining with a token. platform 2 is rejected code 5 even
   with a perfectly valid token.

## What does NOT work, and why

**GitHub-hosted runners, proxyless.** Measured with *this exact config*:
`tokens=0 elapsed=479s cf_errors={'600010': 121}`. Cloudflare will not serve the
challenge to GitHub's IP ranges. `humanize`, the 5-minute restarts and v135 make
no difference — it is the runner IP. GitHub therefore **always** needs an HTTP
CONNECT proxy.

**CroxyProxy / Proxyium / any URL-rewriting proxy — cannot mint tokens, ever.**
Controlled test rendering gartic's sitekey from a non-gartic origin:

| origin | result |
|---|---|
| `gartic.io` | tokens |
| `example.com` | **`ERROR:110200`** (Cloudflare "domain not allowed") |

The sitekey is bound to gartic's domain. A rewriting proxy fetches the page and
re-serves it from *its own* host (`?__cpo=<base64>`), so the document origin is
the proxy's domain and Turnstile refuses before any IP question arises. This is
structural — being free or unlimited is irrelevant. (Those proxies remain useful
for **bot joins** via `__cpw.php`, where no sitekey is involved.)

**Running one IP too fast.** Cloudflare keeps issuing tokens with no errors and
clean logs, but gartic's siteverify then rejects them all with code 5. Measured
on the Oracle datacenter IP: ~52 tok/min collapsed to 0% acceptance within ~1.5h;
~35 tok/min was stable; a 10-minute idle restored it. **Residential is markedly
more tolerant** — 45/min held through 1063 tokens with no decay. Always measure
*acceptance*, never token count: a high rate at 0% acceptance looks perfectly
healthy from the producer side.

## Gotchas that cost real debugging time

- **Tokens are single-use.** Replaying one gives a misleading code 5.
- **Tokens expire at 240s** (`sourceTtlSec`); the relay stops serving at 210s. A
  standing queue of N tokens therefore requires `N / 3.5min` production *forever*
  — 400 standing needs 114/min.
- **CRLF.** `print()` on Windows writes `\r\n`; piping tokens through a file and
  reading with `read -r` appends a stray `\r` and corrupts every token. Use
  `tr -d '\r'`. This presents exactly like mass rejection.
- **stdin capture.** `joindebug` inside a `while read` loop eats the loop's
  stdin; redirect with `< /dev/null` or only the first iteration runs.
- Camoufox v135 sometimes throws on `Browser.close`; report results *before*
  teardown or the exception swallows them.

## Files

- `generator.py` — the producer (CLI-configurable for A/B work)
- `verify.py` — replays captured tokens through joindebug, reports the ratio
- `requirements.txt` — pins and the reasons for them
