#!/usr/bin/env nu

# Opens an SSH tunnel and starts the dashboard dev server.
# Called by the nix wrapper which sets $env.identity and $env.host_ip.

def main [env_name: string, local_port: int = 8001] {
  let ctl_socket = $"/tmp/($env_name)-dashboard-ssh-($nu.pid)"

  print $"Opening SSH tunnel: localhost:($local_port) -> ($env_name):8001"
  ^ssh -i $env.identity -f -N -M -S $ctl_socket -L $"($local_port):localhost:8001" $"root@($env.host_ip)"

  print "Starting dashboard at http://localhost:5173"
  print $"WebSocket proxied to ws://localhost:($local_port)/api/ws"
  print ""

  # Run bun dev server; cleanup tunnel whether bun exits normally or is interrupted
  try {
    cd dashboard
    ^bun install --frozen-lockfile
    $env.PUBLIC_DEFAULT_BROKER = "alpaca"
    $env.PUBLIC_ALPACA_WS_URL = $"ws://localhost:($local_port)/api/ws"
    ^bun run dev
  }

  print "\nClosing SSH tunnel..."
  try { ^ssh -i $env.identity -S $ctl_socket -O exit $"root@($env.host_ip)" }
}
