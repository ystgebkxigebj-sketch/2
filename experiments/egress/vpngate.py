#!/usr/bin/env python3
"""Turn the VPN Gate public relay list into ready-to-use OpenVPN configs.

VPN Gate (vpngate.net) publishes a CSV of volunteer-run relays, each carrying
its whole .ovpn config inline as base64 — no account, no credentials, no API
key. Many exits are residential consumer lines (mostly JP/KR), which is the
property that actually matters here: the pipeline has measured that residential
exits tolerate solve rates that collapse a datacenter IP.

``--index``/``--stride`` exist so N concurrent producers pick **disjoint**
relays. Without them every runner ranks the feed identically, all pick the same
top relay, and they collide on one exit IP — the exact failure that caps WARP's
horizontal scale. Job K of N passes ``--index K --stride N`` and receives ranks
K, K+N, K+2N, …: disjoint sets, each with a fair spread of quality rather than
one job hogging every fast relay.

Usage: vpngate.py <csv> <outdir> [--count N] [--index K] [--stride S]
Writes <outdir>/00-<country>-<host>.ovpn … best candidate for this job first.
"""

from __future__ import annotations

import argparse
import base64
import csv
import io
import sys
from pathlib import Path


def parse_feed(raw: str) -> list[tuple[int, int, str, str, str]]:
    """Rank usable relays best-first as (speed, -ping, country, host, config)."""
    # The feed is wrapped in *vpn_servers / * markers and has a commented header.
    lines = [ln for ln in raw.splitlines() if ln and not ln.startswith("*")]
    if lines and lines[0].startswith("#"):
        lines[0] = lines[0].lstrip("#")

    candidates = []
    for row in csv.DictReader(io.StringIO("\n".join(lines))):
        encoded = (row.get("OpenVPN_ConfigData_Base64") or "").strip()
        if not encoded:
            continue
        try:
            config = base64.b64decode(encoded).decode("utf-8", "replace")
            speed = int(row.get("Speed") or 0)
            ping = int(row.get("Ping") or 9999)
        except Exception:
            continue
        # Rank by throughput: a slow relay just times the whole run out.
        candidates.append((speed, -ping, row.get("CountryShort", "??"),
                           row.get("#HostName") or row.get("HostName") or "h",
                           config))
    candidates.sort(reverse=True)
    return candidates


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", type=Path)
    parser.add_argument("outdir", type=Path)
    parser.add_argument("--count", type=int, default=6,
                        help="how many candidates to write (fallbacks included)")
    parser.add_argument("--index", type=int, default=1,
                        help="1-based index of this producer among --stride peers")
    parser.add_argument("--stride", type=int, default=1,
                        help="total number of concurrent producers")
    args = parser.parse_args()

    if args.index < 1 or args.stride < 1 or args.index > args.stride:
        print("need 1 <= index <= stride", file=sys.stderr)
        return 1

    candidates = parse_feed(args.csv.read_text("utf-8", "replace"))
    mine = candidates[args.index - 1::args.stride][:args.count]
    if not mine:
        print("no usable relays in feed for this index", file=sys.stderr)
        return 1

    args.outdir.mkdir(parents=True, exist_ok=True)
    for rank, (speed, negping, country, host, config) in enumerate(mine):
        path = args.outdir / f"{rank:02d}-{country}-{host}.ovpn"
        # Non-interactive: OpenVPN must never block asking for a credential, and
        # a dead relay must fail fast rather than retry forever.
        path.write_text(config + '\nauth-nocache\npull-filter ignore "auth-token"\n',
                        "utf-8")
        print(f"{path.name} speed={speed/1e6:.1f}Mbps ping={-negping}ms "
              f"country={country}", file=sys.stderr)

    print(f"pool={len(candidates)} index={args.index}/{args.stride} "
          f"written={len(mine)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
