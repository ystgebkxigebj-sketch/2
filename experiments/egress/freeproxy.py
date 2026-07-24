#!/usr/bin/env python3
"""Find one free public proxy that can actually reach Cloudflare's challenge host.

Prints a single ``scheme://host:port`` on stdout, or nothing if none qualified.

Individual free proxies are unreliable, so the selection bar is deliberately
high and cheap to apply: the candidate must complete a TLS request to
``challenges.cloudflare.com`` — the host that actually matters — not merely
echo an IP back. Anything that only proxies plaintext, injects an interstitial,
or dies on CONNECT is discarded before a browser is ever launched.
"""

from __future__ import annotations

import concurrent.futures as futures
import socket
import ssl
import re
import sys
import urllib.request

SOURCES = [
    ("http", "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt"),
    ("http", "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&proxy_format=ipport&format=text"),
    ("socks5", "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt"),
    ("socks5", "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=socks5&proxy_format=ipport&format=text"),
]

PROBE = "https://challenges.cloudflare.com/turnstile/v0/api.js"
ENDPOINT = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}:\d{2,5}$")


def fetch_list(url: str) -> list[str]:
    try:
        with urllib.request.urlopen(url, timeout=20) as response:
            body = response.read().decode("utf-8", "replace")
    except Exception:
        return []
    return [ln.strip() for ln in body.splitlines() if ENDPOINT.match(ln.strip())]


def socks5_tunnels_tls(endpoint: str) -> bool:
    """Full SOCKS5 negotiation, CONNECT, and a TLS request to the probe host.

    A bare TCP connect is not evidence: most free SOCKS endpoints accept the
    connection and then fail the negotiation or refuse CONNECT. Only completing
    a real request proves the candidate is usable, and a browser launch is far
    too expensive to spend on a guess.
    """
    host, _, port = endpoint.partition(":")
    target = "challenges.cloudflare.com"
    try:
        with socket.create_connection((host, int(port)), timeout=8) as raw:
            raw.settimeout(8)
            raw.sendall(b"\x05\x01\x00")                     # greet, no auth
            if raw.recv(2) != b"\x05\x00":
                return False
            request = (b"\x05\x01\x00\x03" + bytes([len(target)])
                       + target.encode() + (443).to_bytes(2, "big"))
            raw.sendall(request)
            reply = raw.recv(4)
            if len(reply) < 2 or reply[1] != 0x00:           # 0x00 = granted
                return False
            # Drain the bound-address field so the stream is positioned for TLS.
            kind = reply[3] if len(reply) > 3 else 1
            length = {1: 4, 4: 16}.get(kind)
            if length is None:
                length = raw.recv(1)[0]
            raw.recv(length + 2)
            context = ssl.create_default_context()
            with context.wrap_socket(raw, server_hostname=target) as tls:
                tls.sendall(f"HEAD /turnstile/v0/api.js HTTP/1.1\r\n"
                            f"Host: {target}\r\nConnection: close\r\n\r\n".encode())
                return b"HTTP/1." in tls.recv(64)
    except Exception:
        return False


def works(scheme: str, endpoint: str) -> str | None:
    """A candidate qualifies only if it can tunnel TLS to the challenge host."""
    url = f"{scheme}://{endpoint}"
    if scheme == "socks5":
        return url if socks5_tunnels_tls(endpoint) else None
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({"https": url, "http": url})
    )
    try:
        with opener.open(PROBE, timeout=12) as response:
            if response.status == 200 and b"turnstile" in response.read(4096).lower():
                return url
    except Exception:
        return None
    return None


def main() -> int:
    candidates: list[tuple[str, str]] = []
    for scheme, url in SOURCES:
        for endpoint in fetch_list(url)[:300]:
            candidates.append((scheme, endpoint))
    print(f"candidates={len(candidates)}", file=sys.stderr)
    if not candidates:
        return 1

    with futures.ThreadPoolExecutor(max_workers=60) as pool:
        pending = {pool.submit(works, s, e): (s, e) for s, e in candidates}
        for future in futures.as_completed(pending):
            result = future.result()
            if result:
                print(result)
                for other in pending:
                    other.cancel()
                return 0
    print("none qualified", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
