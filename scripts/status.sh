#!/usr/bin/env bash
# Status dump: fetches bot status, Raindex orders, DB, and logs.
# Called by the nix `${env}Status` wrapper which sets $identity and $host_ip.
# shellcheck disable=SC2154  # identity and host_ip are exported by the nix wrapper

set -euo pipefail

env="${1:?usage: status.sh <prod|staging>}"
shift

# --- Colors ---
BOLD='\033[1m'
DIM='\033[2m'
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
RESET='\033[0m'

# Format ISO timestamps like "2026-03-30T18:16:50.908685705Z" or
# "2026-03-27T19:24:39+00:00" into "Mar 30, 2026 18:16 UTC"
fmt_ts() {
  local ts="$1"
  ts=$(echo "$ts" | sed -E 's/\.[0-9]+(Z|[+-])/\1/')
  if date -d "$ts" '+%b %d, %Y %H:%M UTC' 2>/dev/null; then
    return
  fi
  local cleaned
  cleaned=$(echo "$ts" | sed -E 's/Z$/+0000/; s/([+-][0-9]{2}):([0-9]{2})$/\1\2/')
  date -j -f '%Y-%m-%dT%H:%M:%S%z' "$cleaned" '+%b %d, %Y %H:%M UTC' 2>/dev/null || echo "$1"
}

# Truncate a decimal string to 2 places
trunc2() {
  local val="$1"
  if [[ "$val" == *"."* ]]; then
    # integer part + first 2 decimal digits
    echo "$val" | sed -E 's/(\.[0-9]{2})[0-9]*/\1/'
  else
    echo "$val"
  fi
}

ssh_cmd="ssh -i $identity root@$host_ip"
db="/mnt/data/st0x-hedge.db"

case "$env" in
  prod)
    subgraph="https://api.goldsky.com/api/public/project_clv14x04y9kzi01saerx7bxpg/subgraphs/ob4-base/2026-02-05-c4ef/gn"
    order_owner="0x386c24644e532387b03c1992ca83542492a3ac32"
    ;;
  staging)
    subgraph="https://api.goldsky.com/api/public/project_clv14x04y9kzi01saerx7bxpg/subgraphs/ob4-base/2026-02-05-c4ef/gn"
    order_owner="0x386c24644e532387b03c1992ca83542492a3ac32"
    ;;
  *)
    echo "ERROR: unknown environment '$env'" >&2
    exit 1
    ;;
esac

# Check service status
if $ssh_cmd systemctl is-active --quiet st0x-hedge; then
  status="running"
  status_color="$GREEN"
  status_icon="●"
else
  status="stopped"
  status_color="$RED"
  status_icon="○"
fi

timestamp=$(date +"%Y-%m-%d_%H-%M-%S")
out_dir="./claude-local-ctx/${timestamp}-${status}"
mkdir -p "$out_dir"

echo ""
echo -e "${BOLD}${WHITE}══════════════════════════════════════${RESET}"
echo -e "  ${status_color}${status_icon} Bot is ${BOLD}${status}${RESET}"
echo -e "${BOLD}${WHITE}══════════════════════════════════════${RESET}"

# Deployed version
echo ""
echo -e "${BOLD}${CYAN}▸ Deployed Version${RESET}"
git_rev=$($ssh_cmd "cat /run/st0x/st0x-hedge.git-rev 2>/dev/null" || echo "")
if [ -n "$git_rev" ] && [ "$git_rev" != "unknown" ]; then
  short_rev="${git_rev:0:8}"
  if [[ "$git_rev" =~ ^[0-9a-f]{7,40}$ ]]; then
    echo -e "  ${DIM}Commit:${RESET}   ${WHITE}${short_rev}${RESET}  ${DIM}(${git_rev})${RESET}"
    echo -e "  ${DIM}GitHub:${RESET}   https://github.com/ST0x-Technology/st0x.liquidity/commit/${git_rev}"
  else
    echo -e "  ${DIM}Commit:${RESET}   ${WHITE}${short_rev}${RESET}  ${YELLOW}[dirty]${RESET} ${DIM}(${git_rev})${RESET}"
  fi
else
  echo -e "  ${DIM}Commit:${RESET}   ${YELLOW}unknown${RESET} ${DIM}(pre-tracking deployment)${RESET}"
fi
deploy_epoch=$($ssh_cmd "stat -c '%Y' /nix/var/nix/profiles/per-service/st0x-hedge 2>/dev/null" || echo "")
if [ -n "$deploy_epoch" ]; then
  deploy_ts=$(date -u -d "@$deploy_epoch" '+%b %d, %Y %H:%M UTC' 2>/dev/null \
    || date -u -r "$deploy_epoch" '+%b %d, %Y %H:%M UTC' 2>/dev/null \
    || echo "$deploy_epoch")
  echo -e "  ${DIM}Deployed:${RESET} ${deploy_ts}"
fi

# Active config summary
echo ""
echo -e "${BOLD}${CYAN}▸ Active Config${RESET}"
config=$($ssh_cmd "cat /run/st0x/st0x-hedge.config 2>/dev/null" || echo "")
if [ -n "$config" ]; then
  # Operational settings
  log_level=$(echo "$config" | { grep -E '^log_level' || true; } | head -1 | sed 's/.*= *"*\([^"]*\)"*/\1/')
  deployment_block=$(echo "$config" | { grep -E '^deployment_block' || true; } | head -1 | sed 's/.*= *//')
  config_order_owner=$(echo "$config" | { grep -E '^order_owner' || true; } | head -1 | sed 's/.*= *"*\([^"]*\)"*/\1/')
  orderbook=$(echo "$config" | { grep -E '^orderbook' || true; } | head -1 | sed 's/.*= *"*\([^"]*\)"*/\1/')
  [ -n "$config_order_owner" ] && order_owner="$config_order_owner"

  [ -n "$log_level" ] && echo -e "  ${DIM}Log level:${RESET}        ${WHITE}${log_level}${RESET}"
  [ -n "$deployment_block" ] && echo -e "  ${DIM}Deployment block:${RESET} ${WHITE}${deployment_block}${RESET}"
  [ -n "$orderbook" ] && echo -e "  ${DIM}Orderbook:${RESET}        ${WHITE}${orderbook}${RESET}"
  [ -n "$order_owner" ] && echo -e "  ${DIM}Order owner:${RESET}      ${WHITE}${order_owner}${RESET}"

  # Asset trading status
  echo ""
  echo -e "  ${DIM}Assets:${RESET}"
  # Parse each equity section: extract symbol, trading, and rebalancing status
  echo "$config" | awk '
    /^\[assets\.equities\.[A-Z]/ {
      gsub(/\[assets\.equities\./, ""); gsub(/\]/, "");
      symbol = $0; trading = ""; rebalancing = ""
      in_equity = 1; next
    }
    /^\[/ { in_equity = 0; next }
    !in_equity { next }
    /^trading/ { gsub(/.*= *"?/, ""); gsub(/"/, ""); trading = $0 }
    /^rebalancing/ {
      gsub(/.*= *"?/, ""); gsub(/"/, ""); rebalancing = $0
      color_t = (trading == "enabled") ? "\033[32m" : "\033[2m"
      color_r = (rebalancing == "enabled") ? "\033[32m" : "\033[2m"
      printf "    %-6s  trade: %s%-8s\033[0m  rebalance: %s%-8s\033[0m\n", symbol, color_t, trading, color_r, rebalancing
    }
  '
else
  echo -e "  ${DIM}(no config found)${RESET}"
fi

# Last boot time
echo ""
echo -e "${BOLD}${CYAN}▸ Last Started${RESET}"
boot_line=$($ssh_cmd "journalctl -u st0x-hedge --no-pager -o short-iso \
  | grep -E '(Started st0x|Initializing.*executor)' \
  | tail -1" 2>/dev/null || echo "")
if [ -n "$boot_line" ]; then
  boot_ts=$(echo "$boot_line" | grep -oE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[^ ]*')
  if [ -n "$boot_ts" ]; then
    echo -e "  $(fmt_ts "$boot_ts")"
  else
    echo -e "  ${DIM}(could not determine)${RESET}"
  fi
else
  echo -e "  ${DIM}(could not determine)${RESET}"
fi

# Raindex open orders for this owner
echo ""
echo -e "${BOLD}${CYAN}▸ Raindex Open Orders${RESET}"
orders_json=$(curl -s -X POST "$subgraph" \
  -H "Content-Type: application/json" \
  -d "{\"query\": \"{ orders(where: { owner: \\\"$order_owner\\\", active: true }, orderBy: timestampAdded, orderDirection: desc) { orderHash active timestampAdded inputs { token { symbol decimals } balance vaultId } outputs { token { symbol decimals } balance vaultId } trades(first: 5, orderBy: timestamp, orderDirection: desc) { timestamp inputVaultBalanceChange { amount vault { token { symbol decimals } } } outputVaultBalanceChange { amount vault { token { symbol decimals } } } } } }\"}")

echo "$orders_json" | jq '.' > "$out_dir/raindex-orders.json"

# Extract unique Rain Float hex values from balance/amount fields
hex_values=$(echo "$orders_json" | jq -r '
  [.data.orders[]
    | .inputs[].balance, .outputs[].balance,
      .trades[]?.inputVaultBalanceChange.amount,
      .trades[]?.outputVaultBalanceChange.amount
  ] | unique | .[]
')

# Batch decode all hex values in a single invocation
dec_values=$(echo "$hex_values" | cargo run --quiet --bin decode-floats)

# Replace hex values in the JSON with decoded decimals
patched_json="$orders_json"
while IFS= read -r hex && IFS= read -r dec <&3; do
  patched_json="${patched_json//\"$hex\"/\"$dec\"}"
done <<< "$hex_values" 3<<< "$dec_values"

echo "$patched_json" | jq '.' > "$out_dir/raindex-orders-decoded.json"

num_orders=$(echo "$patched_json" | jq '.data.orders | length')
echo -e "  Active orders: ${WHITE}${num_orders}${RESET}"
echo ""

echo "$patched_json" | jq -r '
  def trunc2:
    tostring | capture("^(?<i>[^.]*)\\.(?<d>..)") // {i: tostring, d: ""} |
    if .d == "" then .i else "\(.i).\(.d)" end;

  .data.orders[] |
  "  \u001b[1;37m\(.orderHash[0:18])...\u001b[0m" +
  "  \u001b[2mAdded \(.timestampAdded | tonumber | strftime("%b %d, %Y %H:%M UTC"))\u001b[0m" +
  "\n    \u001b[2m├\u001b[0m In:   " +
  (.inputs | map("\(.token.symbol) \(.balance | trunc2)") | join(", ")) +
  "\n    \u001b[2m├\u001b[0m Out:  " +
  (.outputs | map("\(.token.symbol) \(.balance | trunc2)") | join(", ")) +
  "\n    \u001b[2m└\u001b[0m Trades: \(.trades | length) recent" +
  "\n"
'

# Fetch full trade history for each order from subgraph
echo -e "${BOLD}${CYAN}▸ Fetching Full Trade History${RESET}"
order_hashes=$(echo "$orders_json" | jq -r '.data.orders[].orderHash')

all_trade_hexes=""
for order_hash in $order_hashes; do
  short_hash="${order_hash:0:18}"
  echo -e "  ${DIM}Fetching trades for ${short_hash}...${RESET}"
  page=0
  page_size=100
  trades_file="$out_dir/trades_$short_hash.json"
  echo '[]' > "$trades_file"

  while true; do
    skip=$((page * page_size))
    page_json=$(curl -s -X POST "$subgraph" \
      -H "Content-Type: application/json" \
      -d "{\"query\": \"{ trades(first: $page_size, skip: $skip, orderBy: timestamp, orderDirection: desc, where: { order_: { orderHash: \\\"$order_hash\\\" } }) { timestamp inputVaultBalanceChange { amount vault { token { symbol } } } outputVaultBalanceChange { amount vault { token { symbol } } } tradeEvent { transaction { id blockNumber } } } }\"}")

    count=$(echo "$page_json" | jq '.data.trades | length')
    if [ "$count" -eq 0 ]; then
      break
    fi

    # Append page to trades file
    merged=$(jq -s '.[0] + .[1]' "$trades_file" <(echo "$page_json" | jq '.data.trades'))
    echo "$merged" > "$trades_file"

    if [ "$count" -lt "$page_size" ]; then
      break
    fi
    page=$((page + 1))
  done

  total=$(jq 'length' "$trades_file")
  echo -e "  ${DIM}Found ${total} trades${RESET}"

  # Collect hex values for batch decoding
  file_hexes=$(jq -r '.[].inputVaultBalanceChange.amount, .[].outputVaultBalanceChange.amount' "$trades_file")
  all_trade_hexes="$all_trade_hexes"$'\n'"$file_hexes"
done

# Batch decode all trade hex values
unique_hexes=$(echo "$all_trade_hexes" | sort -u | grep -v '^$')
if [ -n "$unique_hexes" ]; then
  decoded_hexes=$(echo "$unique_hexes" | cargo run --quiet --bin decode-floats)

  # Build CSV for each order's trades
  for order_hash in $order_hashes; do
    short_hash="${order_hash:0:18}"
    trades_file="$out_dir/trades_$short_hash.json"
    csv_file="$out_dir/trades_$short_hash.csv"

    # Replace hex amounts with decoded values in the JSON
    patched_trades=$(cat "$trades_file")
    while IFS= read -r hex && IFS= read -r dec <&3; do
      patched_trades="${patched_trades//\"$hex\"/\"$dec\"}"
    done <<< "$unique_hexes" 3<<< "$decoded_hexes"

    # Write decoded JSON
    echo "$patched_trades" | jq '.' > "$trades_file"

    # Write CSV
    echo "timestamp,datetime,block_number,tx_hash,input_symbol,input_amount,output_symbol,output_amount" > "$csv_file"
    echo "$patched_trades" | jq -r '.[] |
      [
        .timestamp,
        (.timestamp | tonumber | strftime("%Y-%m-%d %H:%M:%S")),
        .tradeEvent.transaction.blockNumber,
        .tradeEvent.transaction.id,
        .inputVaultBalanceChange.vault.token.symbol,
        .inputVaultBalanceChange.amount,
        .outputVaultBalanceChange.vault.token.symbol,
        .outputVaultBalanceChange.amount
      ] | @csv
    ' >> "$csv_file"

    echo -e "  ${DIM}Wrote ${csv_file}${RESET}"
  done
fi
echo ""

# RKLB position (from events, views are empty when bot is stopped)
echo -e "${BOLD}${CYAN}▸ RKLB Position${RESET}"
pos_data=$($ssh_cmd "sqlite3 -json $db \"SELECT symbol, net_position, last_updated FROM position_view WHERE symbol LIKE '%RKLB%';\"" 2>/dev/null || echo "[]")
echo "$pos_data" | jq -r '
  .[] |
  "  \(.symbol)  net: \(.net_position | tostring | capture("^(?<i>[^.]*)\\.(?<d>..)") // {i: (.net_position | tostring), d: ""} | if .d == "" then .i else "\(.i).\(.d)" end)"
' 2>/dev/null || echo -e "  ${DIM}(no position data)${RESET}"

# Inventory balance (latest snapshots from events table)
echo ""
echo -e "${BOLD}${CYAN}▸ Inventory Balance${RESET}"
echo -e "  ${YELLOW}Onchain:${RESET}"

onchain_equity_ts=$($ssh_cmd "sqlite3 $db \"SELECT json_extract(payload, '\\\$.OnchainEquity.fetched_at') FROM events WHERE event_type = 'InventorySnapshotEvent::OnchainEquity' ORDER BY rowid DESC LIMIT 1;\"" 2>/dev/null || echo "")
onchain_equity_val=$($ssh_cmd "sqlite3 $db \"SELECT json_extract(payload, '\\\$.OnchainEquity.balances.RKLB') FROM events WHERE event_type = 'InventorySnapshotEvent::OnchainEquity' ORDER BY rowid DESC LIMIT 1;\"" 2>/dev/null || echo "")
if [ -n "$onchain_equity_val" ]; then
  echo -e "    Equity: ${WHITE}$(trunc2 "$onchain_equity_val") RKLB${RESET}  ${DIM}($(fmt_ts "$onchain_equity_ts"))${RESET}"
fi

onchain_usdc_ts=$($ssh_cmd "sqlite3 $db \"SELECT json_extract(payload, '\\\$.OnchainUsdc.fetched_at') FROM events WHERE event_type = 'InventorySnapshotEvent::OnchainUsdc' ORDER BY rowid DESC LIMIT 1;\"" 2>/dev/null || echo "")
onchain_usdc_val=$($ssh_cmd "sqlite3 $db \"SELECT json_extract(payload, '\\\$.OnchainUsdc.usdc_balance') FROM events WHERE event_type = 'InventorySnapshotEvent::OnchainUsdc' ORDER BY rowid DESC LIMIT 1;\"" 2>/dev/null || echo "")
if [ -n "$onchain_usdc_val" ]; then
  echo -e "    USDC:   ${WHITE}$(trunc2 "$onchain_usdc_val") USDC${RESET}  ${DIM}($(fmt_ts "$onchain_usdc_ts"))${RESET}"
fi

echo -e "  ${YELLOW}Offchain:${RESET}"

offchain_equity_ts=$($ssh_cmd "sqlite3 $db \"SELECT json_extract(payload, '\\\$.OffchainEquity.fetched_at') FROM events WHERE event_type = 'InventorySnapshotEvent::OffchainEquity' ORDER BY rowid DESC LIMIT 1;\"" 2>/dev/null || echo "")
offchain_equity_val=$($ssh_cmd "sqlite3 $db \"SELECT json_extract(payload, '\\\$.OffchainEquity.positions.RKLB') FROM events WHERE event_type = 'InventorySnapshotEvent::OffchainEquity' ORDER BY rowid DESC LIMIT 1;\"" 2>/dev/null || echo "")
if [ -n "$offchain_equity_val" ]; then
  echo -e "    Equity: ${WHITE}$(trunc2 "$offchain_equity_val") RKLB${RESET}  ${DIM}($(fmt_ts "$offchain_equity_ts"))${RESET}"
fi

offchain_usd_ts=$($ssh_cmd "sqlite3 $db \"SELECT json_extract(payload, '\\\$.OffchainUsd.fetched_at') FROM events WHERE event_type = 'InventorySnapshotEvent::OffchainUsd' ORDER BY rowid DESC LIMIT 1;\"" 2>/dev/null || echo "")
offchain_usd_val=$($ssh_cmd "sqlite3 $db \"SELECT printf('%.2f', json_extract(payload, '\\\$.OffchainUsd.usd_balance_cents') / 100.0) FROM events WHERE event_type = 'InventorySnapshotEvent::OffchainUsd' ORDER BY rowid DESC LIMIT 1;\"" 2>/dev/null || echo "")
if [ -n "$offchain_usd_val" ]; then
  echo -e "    USD:    ${WHITE}\$${offchain_usd_val}${RESET}  ${DIM}($(fmt_ts "$offchain_usd_ts"))${RESET}"
fi

# Recent onchain trades (from events table -- views are rebuilt only at startup)
echo ""
echo -e "${BOLD}${CYAN}▸ Recent Onchain Trades${RESET}"
echo -e "  ${DIM}TIME              SIDE  AMOUNT    PRICE     TOTAL      SYMBOL${RESET}"
echo -e "  ${DIM}────────────────  ────  ────────  ────────  ─────────  ──────${RESET}"
$ssh_cmd "sqlite3 -json $db \"SELECT json_extract(payload, '\\\$.Filled.symbol') AS symbol, json_extract(payload, '\\\$.Filled.direction') AS direction, printf('%.2f', json_extract(payload, '\\\$.Filled.amount')) AS amount, printf('%.2f', json_extract(payload, '\\\$.Filled.price_usdc')) AS price, json_extract(payload, '\\\$.Filled.block_timestamp') AS time FROM events WHERE event_type = 'OnChainTradeEvent::Filled' ORDER BY json_extract(payload, '\\\$.Filled.block_timestamp') DESC LIMIT 20;\"" | jq -r '
  def pad(n): tostring | if length < n then . + (" " * (n - length)) else . end;
  def fmt_short_date:
    split("T") | .[0] as $d | .[1] | split(":") | "\(.[0]):\(.[1])" as $t |
    ($d | split("-")) as $parts |
    "\($parts[1])/\($parts[2]) \($t)";
  .[] |
  (.amount | tonumber) as $amt | (.price | tonumber) as $prc |
  ($amt * $prc * 100 | round / 100 | tostring) as $total_raw |
  (if ($total_raw | contains(".")) then
    ($total_raw | split(".") | "\(.[0]).\(.[1] | .[:2] | if length < 2 then . + "0" else . end)")
  else
    ($total_raw + ".00")
  end) as $total |
  "  \(.time | fmt_short_date | pad(16))  \(if .direction == "Buy" then "\u001b[32mBUY \u001b[0m" else "\u001b[31mSELL\u001b[0m" end)  \(.amount | pad(8))  $\(.price | pad(8))  $\($total | pad(9))  \(.symbol)"
'

# Recent offchain orders (formatted)
echo ""
echo -e "${BOLD}${CYAN}▸ Recent Offchain Orders${RESET}"
$ssh_cmd "sqlite3 -json $db 'SELECT view_id, status, payload FROM offchain_order_view ORDER BY view_id DESC LIMIT 20;'" | jq -r '
  def color_status:
    if . == "Filled" then "\u001b[32m"
    elif . == "Failed" then "\u001b[31m"
    elif . == "Submitted" then "\u001b[33m"
    elif . == "Pending" then "\u001b[34m"
    else "\u001b[2m"
    end;

  def fmt_short_date:
    split("T") | .[0] as $d | .[1] | split(":") | "\(.[0]):\(.[1])" as $t |
    ($d | split("-")) as $parts |
    "\($parts[1])/\($parts[2]) \($t)";

  .[] |
  .status as $status |
  (.payload | fromjson) as $p |
  if $status == "Filled" then
    $p.Live.Filled |
    "  \(.filled_at // .placed_at | fmt_short_date)  \($status | color_status)[\($status)]\u001b[0m  \(.direction) \(.shares) \(.symbol) @ \(.price)"
  elif $status == "Failed" then
    $p.Live.Failed |
    "  \(.failed_at // .placed_at | fmt_short_date)  \($status | color_status)[\($status)]\u001b[0m  \(.direction) \(.shares) \(.symbol) \u001b[2m\(.error | split("{")[0])\u001b[0m"
  elif $status == "Submitted" then
    $p.Live.Submitted |
    "  \(.submitted_at // .placed_at | fmt_short_date)  \($status | color_status)[\($status)]\u001b[0m  \(.direction) \(.shares) \(.symbol)"
  elif $status == "Pending" then
    $p.Live.Pending |
    "  \(.placed_at | fmt_short_date)  \($status | color_status)[\($status)]\u001b[0m  \(.direction) \(.shares) \(.symbol)"
  else
    "  \($status | color_status)[\($status)]\u001b[0m \(.view_id)"
  end
'

# Fetch full logs
echo ""
echo -e "${DIM}Saving full data to ${out_dir} ...${RESET}"
$ssh_cmd "journalctl -u st0x-hedge --no-pager" > "$out_dir/logs.txt"

# Fetch DB dump
scp -i "$identity" "root@$host_ip:$db" "$out_dir/st0x-hedge.db"

echo ""
echo -e "${BOLD}${WHITE}══════════════════════════════════════${RESET}"
echo -e "  ${status_color}${status_icon} Bot is ${BOLD}${status}${RESET}"
echo -e "${BOLD}${WHITE}──────────────────────────────────────${RESET}"
echo -e "  ${DIM}Logs:${RESET}   $out_dir/logs.txt"
echo -e "  ${DIM}DB:${RESET}     $out_dir/st0x-hedge.db"
echo -e "  ${DIM}Orders:${RESET} $out_dir/raindex-orders-decoded.json"
echo -e "  ${DIM}Trades:${RESET} $out_dir/trades_*.csv"
echo -e "${BOLD}${WHITE}══════════════════════════════════════${RESET}"
