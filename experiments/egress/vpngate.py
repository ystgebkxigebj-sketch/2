#!/usr/bin/env python3
"""Turn the VPN Gate public relay list into ready-to-use OpenVPN configs.

VPN Gate (vpngate.net) publishes a CSV of volunteer-run relays, each carrying
its whole .ovpn config inline as base64 — no account, no credentials, no API
key. Many exits are residential consumer lines (mostly JP/KR), which is the
property we actually want: the pipeline has measured that residential exits
tolerate far higher Turnstile solve rates than datacenter ones.

Usage: vpngate.py <csv> <outdir> [count]
Writes <outdir>/00-<country>-<host>.ovpn ... best candidates first.
"""

from __future__ import annotations

import base64
import csv
import io
import sys
from pathlib import Path


def main() -> int:
    source, outdir = Path(sys.argv[1]), Path(sys.argv[2])
    count = int(sys.argv[3]) if len(sys.argv) > 3 else 6

    raw = source.read_text("utf-8", "replace")
    # The feed is wrapped in *vpn_servers / *  markers and has a comment header.
    lines = [ln for ln in raw.splitlines() if ln and not ln.startswith("*")]
    if lines and lines[0].startswith("#"):
        lines[0] = lines[0].lstrip("#")

    rows = list(csv.DictReader(io.StringIO("\n".join(lines))))
    candidates = []
    for row in rows:
        config = (row.get("OpenVPN_ConfigData_Base64") or "").strip()
        if not config:
            continue
        try:
            decoded = base64.b64decode(config).decode("utf-8", "replace")
        except Exception:
            continue
        # UDP relays reconnect faster and VPN Gate's TCP entries are the
        # congested ones; prefer whatever the operator published but rank by
        # throughput, since a slow relay just times the whole run out.
        try:
            speed = int(row.get("Speed") or 0)
            ping = int(row.get("Ping") or 9999)
        except ValueError:
            continue
        candidates.append((speed, -ping, row.get("CountryShort", "??"),
                           row.get("#HostName") or row.get("HostName") or "h",
                           decoded))

    candidates.sort(reverse=True)
    outdir.mkdir(parents=True, exist_ok=True)
    written = 0
    for speed, negping, country, host, decoded in candidates[:count]:
        path = outdir / f"{written:02d}-{country}-{host}.ovpn"
        # Non-interactive: OpenVPN must never block asking for a password, and
        # a dead relay must fail fast rather than retry forever.
        path.write_text(decoded + "\nauth-nocache\npull-filter ignore \"auth-token\"\n", "utf-8")
        print(f"{path.name} speed={speed/1e6:.1f}Mbps ping={-negping}ms country={country}",
              file=sys.stderr)
        written += 1

    if not written:
        print("no usable relays in feed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
