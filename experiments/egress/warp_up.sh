#!/usr/bin/env bash
# Bring up Cloudflare WARP as a local SOCKS5 listener and report the exit.
#
# Shared by the exit-characterisation probe and the production producer, so both
# are guaranteed to be measuring the same thing.
#
# Usage: warp_up.sh <bind_port> [endpoint_host] [endpoint_port]
#
# Exports nothing; prints `key=value` lines and leaves wireproxy running in the
# background. Exits non-zero if the tunnel does not carry traffic — callers MUST
# treat that as fatal, because falling through to the runner's own egress means
# minting on AS8075, which yields nothing but Cloudflare 600010.
set -euo pipefail

BIND_PORT="${1:?bind port required}"
ENDPOINT_HOST="${2:-}"
ENDPOINT_PORT="${3:-}"

# The release CDN 504s in bursts that outlast any curl --retry budget.
dl() {
  for attempt in 1 2 3 4 5 6; do
    curl -fsSL --retry 3 --retry-delay 3 --retry-all-errors \
      --connect-timeout 20 -o "$2" "$1" && return 0
    echo "download attempt $attempt for $2 failed; backing off" >&2
    sleep $(( attempt * 8 ))
  done
  return 1
}

if [[ ! -x ./wgcf ]]; then
  dl https://github.com/ViRb3/wgcf/releases/download/v2.2.32/wgcf_2.2.32_linux_amd64 wgcf
  chmod +x wgcf
fi
if [[ ! -x ./wireproxy ]]; then
  dl https://github.com/pufferffish/wireproxy/releases/download/v1.1.3/wireproxy_linux_amd64.tar.gz wireproxy.tar.gz
  tar xzf wireproxy.tar.gz
fi

# A fresh anonymous registration each time: no signup, no credentials. Removing
# any previous account file is what makes it fresh rather than reused.
rm -f wgcf-account.toml wgcf-profile.conf
./wgcf register --accept-tos >/dev/null
./wgcf generate >/dev/null

default_endpoint=$(grep -E '^Endpoint' wgcf-profile.conf | head -1 | sed 's/.*= *//')
echo "wgcf_default_endpoint=$default_endpoint"

cp wgcf-profile.conf wp.conf
if [[ -n "$ENDPOINT_HOST" ]]; then
  port="${ENDPOINT_PORT:-2408}"
  sed -i "s|^Endpoint *=.*|Endpoint = ${ENDPOINT_HOST}:${port}|" wp.conf
fi
echo "endpoint_used=$(grep -E '^Endpoint' wp.conf | head -1 | sed 's/.*= *//')"

printf '\n[Socks5]\nBindAddress = 127.0.0.1:%s\n' "$BIND_PORT" >> wp.conf
./wireproxy -c wp.conf >wireproxy.log 2>&1 &

for _ in $(seq 1 30); do
  curl -s --max-time 5 --socks5-hostname "127.0.0.1:$BIND_PORT" \
    https://www.cloudflare.com/cdn-cgi/trace >/dev/null 2>&1 && break
  sleep 2
done

trace=$(curl -s --max-time 20 --socks5-hostname "127.0.0.1:$BIND_PORT" \
  https://www.cloudflare.com/cdn-cgi/trace || true)
exit_ip=$(printf '%s' "$trace" | sed -n 's/^ip=//p')
colo=$(printf '%s' "$trace" | sed -n 's/^colo=//p')
warp=$(printf '%s' "$trace" | sed -n 's/^warp=//p')
loc=$(printf '%s' "$trace" | sed -n 's/^loc=//p')

echo "exit_ip=${exit_ip:-FAILED}"
echo "colo=${colo:-unknown}"
echo "warp=${warp:-unknown}"
echo "loc=${loc:-unknown}"

if [[ -z "$exit_ip" || "$warp" != "on" ]]; then
  echo "WARP tunnel did not come up (warp=$warp)" >&2
  tail -20 wireproxy.log >&2 || true
  exit 1
fi
