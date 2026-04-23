#!/usr/bin/env nu

# Status dump: fetches bot status, Raindex orders, DB, and logs.
# Called by the nix wrapper which sets $env.identity and $env.host_ip.

# --- Helpers ---

# Locate the decode-floats binary without invoking cargo if possible.
# Prefers a pre-built binary in target/ to avoid cargo's startup overhead.
def find-decode-floats []: nothing -> string {
  if (which decode-floats | is-not-empty) {
    "decode-floats"
  } else if ("target/release/decode-floats" | path exists) {
    "target/release/decode-floats"
  } else if ("target/debug/decode-floats" | path exists) {
    "target/debug/decode-floats"
  } else {
    ""
  }
}

# Build ssh flags: only pass -i when an identity file is set.
def ssh-flags []: nothing -> list<string> {
  if ($env.identity? | is-not-empty) and ($env.identity | str trim | is-not-empty) {
    ["-i" $env.identity]
  } else {
    []
  }
}

def ssh-run [cmd: string]: nothing -> string {
  let result = (^ssh ...(ssh-flags) $"root@($env.host_ip)" $cmd | complete)
  if $result.exit_code == 0 { $result.stdout | str trim } else { "" }
}

# Pipe SQL via stdin to avoid quoting issues with json_extract('$.field')
def ssh-sqlite-json [db: string, query: string]: nothing -> list<any> {
  let result = ($query | ^ssh ...(ssh-flags) $"root@($env.host_ip)" $"sqlite3 -json ($db)" | complete)
  if $result.exit_code == 0 and ($result.stdout | str trim | is-not-empty) {
    try { $result.stdout | from json } catch { [] }
  } else {
    []
  }
}

def ssh-sqlite-scalar [db: string, query: string]: nothing -> string {
  let result = ($query | ^ssh ...(ssh-flags) $"root@($env.host_ip)" $"sqlite3 ($db)" | complete)
  if $result.exit_code == 0 { $result.stdout | str trim } else { "" }
}

def fmt-ts [ts: string]: nothing -> string {
  if ($ts | is-empty) { return "" }
  try {
    $ts | into datetime | format date "%b %d, %Y %H:%M UTC"
  } catch {
    $ts
  }
}

def trunc2 [val: string]: nothing -> string {
  if ("." in $val) {
    let parts = ($val | split row ".")
    $"($parts.0).($parts.1 | str substring 0..<2)"
  } else {
    $val
  }
}

# Query a GraphQL endpoint via curl.
# nushell's `http post` hangs on Goldsky subgraphs (likely HTTP/2 negotiation),
# so we shell out to curl which handles it reliably.
def subgraph-query [url: string, query: string]: nothing -> record {
  let body = ({ query: $query } | to json)
  let result = (^curl -s --max-time 30 -X POST -H "Content-Type: application/json" -d $body $url | complete)
  if $result.exit_code != 0 {
    error make { msg: $"curl failed with exit code ($result.exit_code)" }
  }
  $result.stdout | from json
}

def fmt-short-date [ts: string]: nothing -> string {
  let parts = ($ts | split row "T")
  let date_parts = ($parts | first | split row "-")
  let time_parts = ($parts | get 1 | split row ":")
  $"($date_parts.1)/($date_parts.2) ($time_parts.0):($time_parts.1)"
}

def main [env_name: string] {
  if $env_name not-in ["prod", "staging"] {
    print -e $"ERROR: unknown environment '($env_name)'"
    exit 1
  }

  let db = "/mnt/data/st0x-hedge.db"

  let subgraph = "https://api.goldsky.com/api/public/project_clv14x04y9kzi01saerx7bxpg/subgraphs/ob4-base/2026-02-05-c4ef/gn"

  # Fetch running config upfront so order_owner and other fields are available throughout.
  # In Standalone mode, order_owner is set explicitly under [raindex].
  # In Rebalancing mode, order_owner equals the wallet address under [rebalancing.wallet].
  # For private-key wallets (no explicit address), fall back to the secrets file.
  let config_str = (ssh-run "cat /run/st0x/st0x-hedge.config 2>/dev/null")
  let config = if ($config_str | is-not-empty) { try { $config_str | from toml } catch { null } } else { null }
  let order_owner = if $config != null {
    let explicit = (try { $config.raindex.order_owner } catch { "" })
    if ($explicit | is-not-empty) {
      $explicit
    } else {
      # Try config locations that hold an explicit address.
      let wallet_addr = (try { $config.rebalancing.wallet.address } catch {
        try { $config.wallet.address } catch {
          try { $config.rebalancing.address } catch { "" }
        }
      })
      if ($wallet_addr | is-not-empty) {
        $wallet_addr
      } else {
        # Private-key wallets derive the address at runtime.
        # Fall back to reading the key from server secrets and deriving it with `cast`.
        let pk = (ssh-run "cat /run/agenix/st0x-hedge.toml 2>/dev/null" | lines | where ($it | str contains "private_key") | first | default "" | parse --regex 'private_key\s*=\s*"(?P<key>[^"]+)"' | get -o key.0 | default "")
        if ($pk | is-not-empty) {
          try { with-env { ETH_PRIVATE_KEY: $pk } { ^cast wallet address | str trim } } catch { "" }
        } else {
          ""
        }
      }
    }
  } else { "" }

  # Check service status
  let is_active = (^ssh ...(ssh-flags) $"root@($env.host_ip)" "systemctl is-active --quiet st0x-hedge" | complete).exit_code == 0
  let status = if $is_active { "running" } else { "stopped" }
  let status_color = if $is_active { (ansi green) } else { (ansi red) }
  let status_icon = if $is_active { "●" } else { "○" }

  let timestamp = (date now | format date "%Y-%m-%d_%H-%M-%S")
  let out_dir = $"./claude-local-ctx/($timestamp)-($status)"
  mkdir $out_dir

  # --- Header ---
  print ""
  print $"(ansi attr_bold)(ansi white)══════════════════════════════════════(ansi reset)"
  print $"  ($status_color)($status_icon) Bot is (ansi attr_bold)($status)(ansi reset)"
  print $"(ansi attr_bold)(ansi white)══════════════════════════════════════(ansi reset)"

  # --- Deployed Version ---
  try {
    print ""
    print $"(ansi attr_bold)(ansi cyan)▸ Deployed Version(ansi reset)"
    let git_rev = (ssh-run "cat /run/st0x/st0x-hedge.git-rev 2>/dev/null")
    if ($git_rev | is-not-empty) and $git_rev != "unknown" {
      let short_rev = ($git_rev | str substring 0..<8)
      let is_clean_sha = ($git_rev | parse --regex '^[0-9a-f]{7,40}$' | is-not-empty)
      if $is_clean_sha {
        print $"  (ansi attr_dimmed)Commit:(ansi reset)   (ansi attr_bold)(ansi white)($short_rev)(ansi reset)  (ansi attr_dimmed)\(($git_rev)\)(ansi reset)"
        print $"  (ansi attr_dimmed)GitHub:(ansi reset)   https://github.com/ST0x-Technology/st0x.liquidity/commit/($git_rev)"
      } else {
        print $"  (ansi attr_dimmed)Commit:(ansi reset)   (ansi attr_bold)(ansi white)($short_rev)(ansi reset)  (ansi yellow)[dirty](ansi reset) (ansi attr_dimmed)\(($git_rev)\)(ansi reset)"
      }
    } else {
      print $"  (ansi attr_dimmed)Commit:(ansi reset)   (ansi yellow)unknown(ansi reset) (ansi attr_dimmed)\(pre-tracking deployment\)(ansi reset)"
    }

    let deploy_epoch = (ssh-run "stat -c '%Y' /nix/var/nix/profiles/per-service/st0x-hedge 2>/dev/null")
    if ($deploy_epoch | is-not-empty) {
      let deploy_ts = try {
        $deploy_epoch | into int | $in * 1_000_000_000 | into datetime | format date "%b %d, %Y %H:%M UTC"
      } catch { $deploy_epoch }
      print $"  (ansi attr_dimmed)Deployed:(ansi reset) ($deploy_ts)"
    }
  } catch {
    print $"  (ansi red)Failed to fetch deployed version(ansi reset)"
  }

  # --- Active Config ---
  try {
    print ""
    print $"(ansi attr_bold)(ansi cyan)▸ Active Config(ansi reset)"
    if $config != null {
      let log_level = (try { $config.log_level } catch { "" })
      let deployment_block = (try { $config.deployment_block | into string } catch { "" })
      let orderbook = (try { $config.orderbook } catch { "" })

      let rebalancing_enabled = (try { $config.rebalancing | is-not-empty } catch { false })
      let rebalancing_label = if $rebalancing_enabled { $"(ansi green)enabled(ansi reset)" } else { $"(ansi attr_dimmed)disabled(ansi reset)" }

      if ($log_level | is-not-empty) { print $"  (ansi attr_dimmed)Log level:(ansi reset)        (ansi attr_bold)(ansi white)($log_level)(ansi reset)" }
      if ($deployment_block | is-not-empty) { print $"  (ansi attr_dimmed)Deployment block:(ansi reset) (ansi attr_bold)(ansi white)($deployment_block)(ansi reset)" }
      if ($orderbook | is-not-empty) { print $"  (ansi attr_dimmed)Orderbook:(ansi reset)        (ansi attr_bold)(ansi white)($orderbook)(ansi reset)" }
      if ($order_owner | is-not-empty) { print $"  (ansi attr_dimmed)Order owner:(ansi reset)      (ansi attr_bold)(ansi white)($order_owner)(ansi reset)" }
      print $"  (ansi attr_dimmed)Rebalancing:(ansi reset)      ($rebalancing_label)"

      # Assets (nushell parses TOML natively -- no awk needed)
      let equities = try { $config.assets.equities } catch { null }
      if $equities != null {
        print ""
        print $"  (ansi attr_dimmed)Assets:(ansi reset)"
        for symbol in ($equities | columns | sort) {
          let eq = ($equities | get $symbol)
          let trading = (try { $eq.trading } catch { "disabled" })
          let rebalancing = (try { $eq.rebalancing } catch { "disabled" })
          let tc = if $trading == "enabled" { (ansi green) } else { (ansi attr_dimmed) }
          let rc = if $rebalancing == "enabled" { (ansi green) } else { (ansi attr_dimmed) }
          print $"    ($symbol | fill -w 6 -a left)  trade: ($tc)($trading | fill -w 8 -a left)(ansi reset)  rebalance: ($rc)($rebalancing | fill -w 8 -a left)(ansi reset)"
        }
      }
    } else {
      print $"  (ansi attr_dimmed)\(no config found\)(ansi reset)"
    }
  } catch {
    print $"  (ansi red)Failed to fetch config(ansi reset)"
  }

  # --- Last Started ---
  try {
    print ""
    print $"(ansi attr_bold)(ansi cyan)▸ Last Started(ansi reset)"
    let boot_line = (ssh-run "journalctl -u st0x-hedge --no-pager -o short-iso | grep -E '(Started st0x|Initializing.*executor)' | tail -1")
    if ($boot_line | is-not-empty) {
      let boot_ts = try {
        $boot_line | parse --regex '^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[^ ]*)' | get ts.0
      } catch { "" }
      if ($boot_ts | is-not-empty) {
        print $"  (fmt-ts $boot_ts)"
      } else {
        print $"  (ansi attr_dimmed)\(could not determine\)(ansi reset)"
      }
    } else {
      print $"  (ansi attr_dimmed)\(could not determine\)(ansi reset)"
    }
  } catch {
    print $"  (ansi red)Failed to fetch last started time(ansi reset)"
  }

  # --- Raindex Open Orders ---
  try {
    print ""
    print $"(ansi attr_bold)(ansi cyan)▸ Raindex Open Orders(ansi reset)"

    if ($order_owner | is-empty) {
      print $"  (ansi yellow)order_owner not found in config -- skipping subgraph query(ansi reset)"
      print $"  (ansi attr_dimmed)Checked: raindex.order_owner, rebalancing.wallet.address, rebalancing.address(ansi reset)"
      print $"  (ansi attr_dimmed)Private-key wallets derive the address at runtime; add 'address' to config to enable this section.(ansi reset)"
    } else {

    let gql = ('{ orders(where: { owner: "' + $order_owner + '", active: true }, orderBy: timestampAdded, orderDirection: desc) { orderHash active timestampAdded inputs { token { symbol decimals } balance vaultId } outputs { token { symbol decimals } balance vaultId } trades(first: 5, orderBy: timestamp, orderDirection: desc) { timestamp inputVaultBalanceChange { amount vault { token { symbol decimals } } } outputVaultBalanceChange { amount vault { token { symbol decimals } } } } } }')

    mut orders_response = { data: { orders: [] } }
    for attempt in 1..4 {
      try {
        $orders_response = (subgraph-query $subgraph $gql)
        break
      } catch {|err|
        if $attempt < 3 {
          print $"  (ansi yellow)Subgraph request failed \(attempt ($attempt)/3\), retrying...(ansi reset)"
          sleep 2sec
        } else {
          print $"  (ansi red)Subgraph query failed after 3 attempts -- order data may be incomplete(ansi reset)"
          print $"  (ansi attr_dimmed)Error: ($err.msg)(ansi reset)"
        }
      }
    }

    let orders_response = $orders_response
    let orders = ($orders_response | get -o data.orders | default [])
    $orders_response | to json | save -f $"($out_dir)/raindex-orders.json"

    # Extract hex values for batch decoding
    mut all_hex_values: list<string> = []
    for order in $orders {
      for input in $order.inputs { $all_hex_values = ($all_hex_values | append $input.balance) }
      for output in $order.outputs { $all_hex_values = ($all_hex_values | append $output.balance) }
      for trade in ($order | get -o trades | default []) {
        $all_hex_values = ($all_hex_values | append $trade.inputVaultBalanceChange.amount)
        $all_hex_values = ($all_hex_values | append $trade.outputVaultBalanceChange.amount)
      }
    }
    let hex_values = ($all_hex_values | uniq | sort)

    # Batch decode all hex values
    let dec_values = if ($hex_values | is-empty) {
      []
    } else {
      let bin = (find-decode-floats)
      try {
        if ($bin | is-not-empty) {
          $hex_values | str join "\n" | run-external $bin | complete | get stdout | lines
        } else {
          $hex_values | str join "\n" | ^cargo run --quiet --bin decode-floats | complete | get stdout | lines
        }
      } catch { [] }
    }

    # Replace hex values in JSON string with decoded decimals
    mut patched_str = ($orders_response | to json)
    if ($hex_values | length) == ($dec_values | length) {
      for i in 0..<($hex_values | length) {
        let hex = ($hex_values | get $i)
        let dec = ($dec_values | get $i)
        let search = '"' + $hex + '"'
        let replace = '"' + $dec + '"'
        $patched_str = ($patched_str | str replace --all $search $replace)
      }
    }
    let patched = try { $patched_str | from json } catch { $orders_response }
    let patched_orders = ($patched | get -o data.orders | default [])

    $patched_str | save -f $"($out_dir)/raindex-orders-decoded.json"

    print $"  Active orders: (ansi attr_bold)(ansi white)($orders | length)(ansi reset)"
    print ""

    # Display each order summary
    for order in $patched_orders {
      let short_hash = ($order.orderHash | str substring 0..<18)
      let added_ts = try {
        $order.timestampAdded | into int | $in * 1_000_000_000 | into datetime | format date "%b %d, %Y %H:%M UTC"
      } catch { $order.timestampAdded }

      print $"  (ansi attr_bold)(ansi white)($short_hash)...(ansi reset)  (ansi attr_dimmed)Added ($added_ts)(ansi reset)"

      let inputs = ($order.inputs | each { |inp| $"($inp.token.symbol) (trunc2 ($inp.balance | into string))" } | str join ", ")
      let outputs = ($order.outputs | each { |out| $"($out.token.symbol) (trunc2 ($out.balance | into string))" } | str join ", ")
      let trade_count = ($order | get -o trades | default [] | length)

      print $"    (ansi attr_dimmed)├(ansi reset) In:   ($inputs)"
      print $"    (ansi attr_dimmed)├(ansi reset) Out:  ($outputs)"
      print $"    (ansi attr_dimmed)└(ansi reset) Trades: ($trade_count) recent"
      print ""
    }

    # --- Full Trade History ---
    print $"(ansi attr_bold)(ansi cyan)▸ Fetching Full Trade History(ansi reset)"

    mut all_trade_hexes: list<string> = []
    for order in $orders {
      let order_hash = $order.orderHash
      let short_hash = ($order_hash | str substring 0..<18)
      print $"  (ansi attr_dimmed)Fetching trades for ($short_hash)...(ansi reset)"

      mut all_trades: list<any> = []
      mut page = 0
      let page_size = 100

      loop {
        let skip = $page * $page_size
        let trade_gql = ('{ trades(first: ' + ($page_size | into string) + ', skip: ' + ($skip | into string) + ', orderBy: timestamp, orderDirection: desc, where: { order_: { orderHash: "' + $order_hash + '" } }) { timestamp inputVaultBalanceChange { amount vault { token { symbol } } } outputVaultBalanceChange { amount vault { token { symbol } } } tradeEvent { transaction { id blockNumber } } } }')

        let current_page = $page
        mut page_data = { data: { trades: [] } }
        for attempt in 1..4 {
          try {
            $page_data = (subgraph-query $subgraph $trade_gql)
            break
          } catch {
            if $attempt < 3 {
              sleep 2sec
            } else {
              print $"  (ansi yellow)Trade fetch failed for ($short_hash) page ($current_page) after 3 attempts(ansi reset)"
            }
          }
        }

        let page_data = $page_data
        let trades = ($page_data | get -o data.trades | default [])
        if ($trades | is-empty) { break }

        $all_trades = ($all_trades | append $trades)

        if ($trades | length) < $page_size { break }
        $page = $page + 1
      }

      $all_trades | to json | save -f $"($out_dir)/trades_($short_hash).json"
      print $"  (ansi attr_dimmed)Found ($all_trades | length) trades(ansi reset)"

      for trade in $all_trades {
        $all_trade_hexes = ($all_trade_hexes | append $trade.inputVaultBalanceChange.amount)
        $all_trade_hexes = ($all_trade_hexes | append $trade.outputVaultBalanceChange.amount)
      }
    }

    # Batch decode trade hex values and build CSVs
    let unique_trade_hexes = ($all_trade_hexes | uniq | sort)
    if ($unique_trade_hexes | is-not-empty) {
      let bin = (find-decode-floats)
      let decoded_trade_hexes = try {
        if ($bin | is-not-empty) {
          $unique_trade_hexes | str join "\n" | run-external $bin | complete | get stdout | lines
        } else {
          $unique_trade_hexes | str join "\n" | ^cargo run --quiet --bin decode-floats | complete | get stdout | lines
        }
      } catch { [] }

      if ($unique_trade_hexes | length) == ($decoded_trade_hexes | length) {
        for order in $orders {
          let order_hash = $order.orderHash
          let short_hash = ($order_hash | str substring 0..<18)
          let trades_file = $"($out_dir)/trades_($short_hash).json"
          let csv_file = $"($out_dir)/trades_($short_hash).csv"

          # Replace hex amounts with decoded values
          mut patched_trades_str = (open --raw $trades_file)
          for i in 0..<($unique_trade_hexes | length) {
            let hex = ($unique_trade_hexes | get $i)
            let dec = ($decoded_trade_hexes | get $i)
            let search = '"' + $hex + '"'
            let replace = '"' + $dec + '"'
            $patched_trades_str = ($patched_trades_str | str replace --all $search $replace)
          }

          $patched_trades_str | save -f $trades_file

          # Build CSV
          let trades_data: list<any> = try { $patched_trades_str | from json } catch { [] }
          mut csv_lines = ["timestamp,datetime,block_number,tx_hash,input_symbol,input_amount,output_symbol,output_amount"]
          for trade in $trades_data {
            let ts = $trade.timestamp
            let datetime = try {
              $ts | into int | $in * 1_000_000_000 | into datetime | format date "%Y-%m-%d %H:%M:%S"
            } catch { $ts }
            let block = ($trade.tradeEvent.transaction.blockNumber | into string)
            let tx = $trade.tradeEvent.transaction.id
            let in_sym = $trade.inputVaultBalanceChange.vault.token.symbol
            let in_amt = ($trade.inputVaultBalanceChange.amount | into string)
            let out_sym = $trade.outputVaultBalanceChange.vault.token.symbol
            let out_amt = ($trade.outputVaultBalanceChange.amount | into string)
            $csv_lines = ($csv_lines | append $"($ts),($datetime),($block),($tx),($in_sym),($in_amt),($out_sym),($out_amt)")
          }
          $csv_lines | str join "\n" | save -f $csv_file
          print $"  (ansi attr_dimmed)Wrote ($csv_file)(ansi reset)"
        }
      }
    }
    print ""
    } # end else (order_owner is set)
  } catch {
    print $"  (ansi red)Failed to fetch Raindex orders/trades(ansi reset)"
  }

  # --- Positions ---
  try {
    print $"(ansi attr_bold)(ansi cyan)▸ Positions(ansi reset)"
    let pos_data = (ssh-sqlite-json $db "SELECT symbol, net_position, last_updated FROM position_view ORDER BY symbol;")
    if ($pos_data | is-not-empty) {
      for row in $pos_data {
        print $"  ($row.symbol)  net: (trunc2 ($row.net_position | into string))"
      }
    } else {
      print $"  (ansi attr_dimmed)\(no position data\)(ansi reset)"
    }
  } catch {
    print $"  (ansi attr_dimmed)\(no position data -- DB may not be initialized\)(ansi reset)"
  }

  # --- Inventory Balance ---
  try {
    print ""
    print $"(ansi attr_bold)(ansi cyan)▸ Inventory Balance(ansi reset)"
    # Collect configured equity symbols for dynamic inventory display
    let equity_symbols = if $config != null {
      try { $config.assets.equities | columns | sort } catch { [] }
    } else { [] }

    print $"  (ansi yellow)Onchain:(ansi reset)"

    let onchain_equity_ts = (ssh-sqlite-scalar $db "SELECT json_extract(payload, '$.OnchainEquity.fetched_at') FROM events WHERE event_type = 'InventorySnapshotEvent::OnchainEquity' ORDER BY rowid DESC LIMIT 1;")
    for symbol in $equity_symbols {
      let onchain_equity_val = (ssh-sqlite-scalar $db $"SELECT json_extract\(payload, '$.OnchainEquity.balances.($symbol)'\) FROM events WHERE event_type = 'InventorySnapshotEvent::OnchainEquity' ORDER BY rowid DESC LIMIT 1;")
      if ($onchain_equity_val | is-not-empty) {
        print $"    ($symbol | fill -w 6 -a left): (ansi attr_bold)(ansi white)(trunc2 $onchain_equity_val)(ansi reset)  (ansi attr_dimmed)\((fmt-ts $onchain_equity_ts)\)(ansi reset)"
      }
    }

    let onchain_usdc_ts = (ssh-sqlite-scalar $db "SELECT json_extract(payload, '$.OnchainUsdc.fetched_at') FROM events WHERE event_type = 'InventorySnapshotEvent::OnchainUsdc' ORDER BY rowid DESC LIMIT 1;")
    let onchain_usdc_val = (ssh-sqlite-scalar $db "SELECT json_extract(payload, '$.OnchainUsdc.usdc_balance') FROM events WHERE event_type = 'InventorySnapshotEvent::OnchainUsdc' ORDER BY rowid DESC LIMIT 1;")
    if ($onchain_usdc_val | is-not-empty) {
      print $"    ("USDC" | fill -w 6 -a left): (ansi attr_bold)(ansi white)(trunc2 $onchain_usdc_val)(ansi reset)  (ansi attr_dimmed)\((fmt-ts $onchain_usdc_ts)\)(ansi reset)"
    }

    print $"  (ansi yellow)Offchain:(ansi reset)"

    let offchain_equity_ts = (ssh-sqlite-scalar $db "SELECT json_extract(payload, '$.OffchainEquity.fetched_at') FROM events WHERE event_type = 'InventorySnapshotEvent::OffchainEquity' ORDER BY rowid DESC LIMIT 1;")
    for symbol in $equity_symbols {
      let offchain_equity_val = (ssh-sqlite-scalar $db $"SELECT json_extract\(payload, '$.OffchainEquity.positions.($symbol)'\) FROM events WHERE event_type = 'InventorySnapshotEvent::OffchainEquity' ORDER BY rowid DESC LIMIT 1;")
      if ($offchain_equity_val | is-not-empty) {
        print $"    ($symbol | fill -w 6 -a left): (ansi attr_bold)(ansi white)(trunc2 $offchain_equity_val)(ansi reset)  (ansi attr_dimmed)\((fmt-ts $offchain_equity_ts)\)(ansi reset)"
      }
    }

    let offchain_usd_ts = (ssh-sqlite-scalar $db "SELECT json_extract(payload, '$.OffchainUsd.fetched_at') FROM events WHERE event_type = 'InventorySnapshotEvent::OffchainUsd' ORDER BY rowid DESC LIMIT 1;")
    let offchain_usd_val = (ssh-sqlite-scalar $db "SELECT printf('%.2f', json_extract(payload, '$.OffchainUsd.usd_balance_cents') / 100.0) FROM events WHERE event_type = 'InventorySnapshotEvent::OffchainUsd' ORDER BY rowid DESC LIMIT 1;")
    if ($offchain_usd_val | is-not-empty) {
      let usd_display = "$" + $offchain_usd_val
      print $"    ("USD" | fill -w 6 -a left): (ansi attr_bold)(ansi white)($usd_display)(ansi reset)  (ansi attr_dimmed)\((fmt-ts $offchain_usd_ts)\)(ansi reset)"
    }

    let bp_ts = (ssh-sqlite-scalar $db "SELECT json_extract(payload, '$.OffchainMarginSafeBuyingPower.fetched_at') FROM events WHERE event_type = 'InventorySnapshotEvent::OffchainMarginSafeBuyingPower' ORDER BY rowid DESC LIMIT 1;")
    let bp_val = (ssh-sqlite-scalar $db "SELECT printf('%.2f', json_extract(payload, '$.OffchainMarginSafeBuyingPower.margin_safe_buying_power_cents') / 100.0) FROM events WHERE event_type = 'InventorySnapshotEvent::OffchainMarginSafeBuyingPower' ORDER BY rowid DESC LIMIT 1;")
    if ($bp_val | is-not-empty) {
      let bp_display = "$" + $bp_val
      print $"    ("BuyPwr" | fill -w 6 -a left): (ansi attr_bold)(ansi white)($bp_display)(ansi reset)  (ansi attr_dimmed)\((fmt-ts $bp_ts)\)(ansi reset)"
    }
  } catch {
    print $"  (ansi attr_dimmed)\(no inventory data -- DB may not be initialized\)(ansi reset)"
  }

  # --- Recent Onchain Trades ---
  try {
    print ""
    print $"(ansi attr_bold)(ansi cyan)▸ Recent Onchain Trades(ansi reset)"
    print $"  (ansi attr_dimmed)TIME              SIDE  AMOUNT    PRICE     TOTAL      SYMBOL(ansi reset)"
    print $"  (ansi attr_dimmed)────────────────  ────  ────────  ────────  ─────────  ──────(ansi reset)"

    let onchain_trades = (ssh-sqlite-json $db "SELECT json_extract(payload, '$.Filled.symbol') AS symbol, json_extract(payload, '$.Filled.direction') AS direction, printf('%.2f', json_extract(payload, '$.Filled.amount')) AS amount, printf('%.2f', json_extract(payload, '$.Filled.price_usdc')) AS price, json_extract(payload, '$.Filled.block_timestamp') AS time FROM events WHERE event_type = 'OnChainTradeEvent::Filled' ORDER BY json_extract(payload, '$.Filled.block_timestamp') DESC LIMIT 20;")

    for trade in $onchain_trades {
      let formatted_time = (fmt-short-date $trade.time) | fill -w 16 -a left
      let side = if $trade.direction == "Buy" {
        $"(ansi green)BUY (ansi reset)"
      } else {
        $"(ansi red)SELL(ansi reset)"
      }
      let amount = ($trade.amount | fill -w 8 -a left)
      let price = ($trade.price | fill -w 8 -a left)
      let total_val = (($trade.amount | into float) * ($trade.price | into float))
      let total = ($total_val | math round --precision 2 | into string | fill -w 9 -a left)

      print $"  ($formatted_time)  ($side)  ($amount)  $($price)  $($total)  ($trade.symbol)"
    }
  } catch {
    print $"  (ansi attr_dimmed)\(no onchain trades -- DB may not be initialized\)(ansi reset)"
  }

  # --- Recent Offchain Orders ---
  try {
    print ""
    print $"(ansi attr_bold)(ansi cyan)▸ Recent Offchain Orders(ansi reset)"

    let offchain_orders = (ssh-sqlite-json $db "SELECT view_id, status, payload FROM offchain_order_view ORDER BY COALESCE(json_extract(payload, '$.Live.Filled.filled_at'), json_extract(payload, '$.Live.Failed.failed_at'), json_extract(payload, '$.Live.PartiallyFilled.partially_filled_at'), json_extract(payload, '$.Live.Submitted.submitted_at'), json_extract(payload, '$.Live.Filled.placed_at'), json_extract(payload, '$.Live.Failed.placed_at'), json_extract(payload, '$.Live.PartiallyFilled.placed_at'), json_extract(payload, '$.Live.Submitted.placed_at'), json_extract(payload, '$.Live.Pending.placed_at')) DESC, view_id DESC LIMIT 20;")

    for order in $offchain_orders {
      let sc = if $order.status == "Filled" { (ansi green) } else if $order.status == "Failed" { (ansi red) } else if $order.status == "Submitted" { (ansi yellow) } else if $order.status == "Pending" { (ansi blue) } else { (ansi attr_dimmed) }

      let payload = ($order.payload | from json)

      let display = if $order.status == "Filled" {
        let data = $payload.Live.Filled
        let ts = ($data | get -o filled_at | default $data.placed_at)
        $"(fmt-short-date $ts)  ($sc)[($order.status)](ansi reset)  ($data.direction) ($data.shares) ($data.symbol) @ ($data.price)"
      } else if $order.status == "Failed" {
        let data = $payload.Live.Failed
        let ts = ($data | get -o failed_at | default $data.placed_at)
        let error_short = ($data.error | split row "{" | first)
        $"(fmt-short-date $ts)  ($sc)[($order.status)](ansi reset)  ($data.direction) ($data.shares) ($data.symbol) (ansi attr_dimmed)($error_short)(ansi reset)"
      } else if $order.status == "Submitted" {
        let data = $payload.Live.Submitted
        let ts = ($data | get -o submitted_at | default $data.placed_at)
        $"(fmt-short-date $ts)  ($sc)[($order.status)](ansi reset)  ($data.direction) ($data.shares) ($data.symbol)"
      } else if $order.status == "Pending" {
        let data = $payload.Live.Pending
        $"(fmt-short-date $data.placed_at)  ($sc)[($order.status)](ansi reset)  ($data.direction) ($data.shares) ($data.symbol)"
      } else {
        $"($sc)[($order.status)](ansi reset) ($order.view_id)"
      }

      print $"  ($display)"
    }
  } catch {
    print $"  (ansi attr_dimmed)\(no offchain orders -- DB may not be initialized\)(ansi reset)"
  }

  # --- Save logs and DB ---
  print ""
  print $"(ansi attr_dimmed)Saving full data to ($out_dir) ...(ansi reset)"

  mut saved_logs = false
  let logs_result = (^ssh ...(ssh-flags) $"root@($env.host_ip)" `journalctl -u st0x-hedge --no-pager --since "$(stat -c '%y' /nix/var/nix/profiles/per-service/st0x-hedge)"` | complete)
  if $logs_result.exit_code == 0 {
    $logs_result.stdout | save -f $"($out_dir)/logs.txt"
    $saved_logs = true
  }

  mut saved_db = false
  try {
    ^scp ...(ssh-flags) $"root@($env.host_ip):($db)" $"($out_dir)/st0x-hedge.db"
    $saved_db = true
  } catch { }

  # --- Footer ---
  print ""
  print $"(ansi attr_bold)(ansi white)══════════════════════════════════════(ansi reset)"
  print $"  ($status_color)($status_icon) Bot is (ansi attr_bold)($status)(ansi reset)"
  print $"(ansi attr_bold)(ansi white)──────────────────────────────────────(ansi reset)"
  if $saved_logs {
    print $"  (ansi attr_dimmed)Logs:(ansi reset)   ($out_dir)/logs.txt"
  } else {
    print $"  (ansi attr_dimmed)Logs:(ansi reset)   (ansi red)failed to download(ansi reset)"
  }
  if $saved_db {
    print $"  (ansi attr_dimmed)DB:(ansi reset)     ($out_dir)/st0x-hedge.db"
  } else {
    print $"  (ansi attr_dimmed)DB:(ansi reset)     (ansi red)failed to download(ansi reset)"
  }
  print $"  (ansi attr_dimmed)Orders:(ansi reset) ($out_dir)/raindex-orders-decoded.json"
  print $"  (ansi attr_dimmed)Trades:(ansi reset) ($out_dir)/trades_*.csv"
  print $"(ansi attr_bold)(ansi white)══════════════════════════════════════(ansi reset)"
}
