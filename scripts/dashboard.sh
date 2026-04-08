#!/usr/bin/env bash
# Opens an SSH tunnel and starts the dashboard dev server.
# Called by the nix `<env>-dashboard` wrapper which sets $identity, $host_ip, and $ENV.
# shellcheck disable=SC2154  # identity, host_ip, ENV are exported by the nix wrapper

set -euo pipefail

local_port="${1:-8001}"
ctl_socket="/tmp/${ENV}-dashboard-ssh-$$"

cleanup() {
  echo ""
  echo "Closing SSH tunnel..."
  ssh -i "$identity" -S "$ctl_socket" -O exit "root@$host_ip" 2>/dev/null || true
}
trap cleanup EXIT

echo "Opening SSH tunnel: localhost:$local_port -> ${ENV}:8001"
ssh -i "$identity" -f -N -M -S "$ctl_socket" \
  -L "$local_port:localhost:8001" "root@$host_ip"

echo "Starting dashboard at http://localhost:5173"
echo "WebSocket proxied to ws://localhost:$local_port/api/ws"
echo ""

cd dashboard
bun install --frozen-lockfile
PUBLIC_DEFAULT_BROKER=alpaca \
PUBLIC_ALPACA_WS_URL="ws://localhost:$local_port/api/ws" \
bun run dev
