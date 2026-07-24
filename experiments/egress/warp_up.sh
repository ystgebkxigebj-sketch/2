#!/usr/bin/env bash
# Bring up Cloudflare WARP as a local SOCKS5 listener, report the exit, and
# refuse to hand back a tunnel whose exit collides with one already in use.
#
# Shared by the exit-characterisation probe and the production producers, so all
# of them are guaranteed to measure the same thing.
#
# Usage: warp_up.sh <bind_port> [endpoint_host] [endpoint_port]
#
# Environment:
#   WARP_FORBID_EXITS  comma-separated exit IPs or IP prefixes to reject, e.g.
#                      "104.28.196.79,104.28.221." A prefix ending in '.'
#                      matches by prefix. On a match the tunnel is torn down and
#                      a FRESH registration is attempted.
#   WARP_MAX_ROLLS     how many registrations to try before giving up (default 4)
#
# Why the forbid list exists: Turnstile solve-rate is capped per exit IP, not per
# machine. Two producers behind one exit is measurably fatal — one sustains ~49
# tok/min while the other gets nothing but Cloudflare 300030. Two runners drawing
# the same anycast exit is not hypothetical; it was observed. So any second
# producer, in this lane or another (the Oracle VM lane also uses WARP), must be
# told which exits are taken and decline them.
#
# Prints `key=value` lines and leaves wireproxy running in the background. Exits
# non-zero if no acceptable tunnel could be established — callers MUST treat
# that as fatal, because falling through to the runner's own egress means minting
# from AS8075, which yields nothing but Cloudflare 600010.
set -euo pipefail

BIND_PORT="${1:?bind port required}"
ENDPOINT_HOST="${2:-}"
ENDPOINT_PORT="${3:-}"
FORBID="${WARP_FORBID_EXITS:-}"
MAX_ROLLS="${WARP_MAX_ROLLS:-4}"

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

# Does $1 match any entry in the comma-separated $FORBID list? An entry ending
# in '.' is treated as a prefix so a whole /24-ish block can be reserved.
is_forbidden() {
  local ip="$1" entry
  [[ -z "$FORBID" ]] && return 1
  local IFS=','
  for entry in $FORBID; do
    entry="${entry// /}"
    [[ -z "$entry" ]] && continue
    if [[ "$entry" == *. ]]; then
      [[ "$ip" == "$entry"* ]] && return 0
    else
      [[ "$ip" == "$entry" ]] && return 0
    fi
  done
  return 1
}

exit_ip=""; colo=""; warp=""; loc=""; roll=0
while (( roll < MAX_ROLLS )); do
  roll=$(( roll + 1 ))
  pkill -f 'wireproxy -c' 2>/dev/null || true
  sleep 1

  # A fresh anonymous registration each roll: no signup, no credentials. Deleting
  # the account file is what makes it fresh rather than reused.
  rm -f wgcf-account.toml wgcf-profile.conf
  ./wgcf register --accept-tos >/dev/null
  ./wgcf generate >/dev/null

  cp wgcf-profile.conf wp.conf
  if [[ -n "$ENDPOINT_HOST" ]]; then
    sed -i "s|^Endpoint *=.*|Endpoint = ${ENDPOINT_HOST}:${ENDPOINT_PORT:-2408}|" wp.conf
  fi
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
  # WARP is dual stack and cdn-cgi/trace reports whichever family the request
  # used — over WARP that is IPv6. Both have to be captured: the v4 address is
  # what earlier collisions were recorded as, and either family can be the one
  # a peer lane has already claimed.
  exit_v4=$(curl -4 -s --max-time 15 --socks5-hostname "127.0.0.1:$BIND_PORT" \
    https://api.ipify.org || true)
  echo "roll=$roll exit_ip=${exit_ip:-FAILED} exit_v4=${exit_v4:-none} colo=${colo:-unknown} warp=${warp:-unknown}"

  if [[ -z "$exit_ip" || "$warp" != "on" ]]; then
    echo "  tunnel did not come up on roll $roll" >&2
    continue
  fi
  if is_forbidden "$exit_ip" || { [[ -n "$exit_v4" ]] && is_forbidden "$exit_v4"; }; then
    echo "  exit ${exit_v4:-$exit_ip} is in WARP_FORBID_EXITS — re-rolling" >&2
    exit_ip=""
    continue
  fi
  break
done

echo "endpoint_used=$(grep -E '^Endpoint' wp.conf | head -1 | sed 's/.*= *//')"
echo "exit_ip=${exit_ip:-FAILED}"
echo "exit_v4=${exit_v4:-none}"
echo "colo=${colo:-unknown}"
echo "warp=${warp:-unknown}"
echo "loc=${loc:-unknown}"
echo "rolls_used=$roll"

if [[ -z "$exit_ip" || "$warp" != "on" ]]; then
  echo "no acceptable WARP exit after $roll rolls (forbid='$FORBID')" >&2
  tail -20 wireproxy.log >&2 || true
  exit 1
fi
