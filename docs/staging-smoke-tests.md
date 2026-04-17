# Staging Smoke Tests

## Motivation

The e2e test suite (`tests/e2e/`) validates the bot's logic using local Anvil
chains and mock services. This gives us confidence that state machines, CQRS
event flows, and business logic are correct -- but it says nothing about whether
the bot works against **real infrastructure**: real RPC nodes, real Alpaca
sandbox, real CCTP attestation, real Raindex orderbook.

Staging smoke tests bridge that gap. They reuse the same simulation loop from
`tests/e2e/full_system.rs::simulate` -- randomly generating trades, taking
orders onchain, and asserting the bot counter-trades and rebalances -- but
pointed at the live staging environment instead of mocks.

## Goals

1. **Validate integration**: real Alpaca sandbox, real Base RPC, real CCTP, real
   Raindex orderbook
2. **Catch deployment regressions**: config drift, secret rotation issues, RPC
   provider changes, contract upgrades
3. **Run on demand**: developer triggers the test manually (not CI), observes
   via dashboard
4. **Non-destructive**: uses small amounts, dedicated test wallet, does not
   interfere with the running staging bot

## Non-Goals

- Replacing the existing e2e suite (that stays as-is for deterministic CI)
- Automated scheduled execution (future work)
- Production environment testing

## Staging Environment

### Assets Under Test

The staging bot has two assets with `trading = "enabled"`:

| Symbol   | Tokenized Equity                             | Vault Wrapper (ERC-4626)                     | Vault ID |
| -------- | -------------------------------------------- | -------------------------------------------- | -------- |
| **RKLB** | `0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b` | `0xf4f8c66085910d583c01f3b4e44bf731d4e2c565` | `0xfab`  |
| **SGOV** | `0xc941C1506B7555Ba8C506Fb6c9b9CC259902d612` | `0x78c31580c97101694c70022c83d570150c11e935` | --       |

### Chain

- **Base mainnet** (chain ID 8453, CCTP domain 6)
- Orderbook: `0xe522cB4a5fCb2eb31a52Ff41a4653d85A4fd7C9D`
- Order owner (bot wallet): `0xA9C16673F65AE808688cB18952AFE3d9658C808f`

### Broker

- **Alpaca Broker API** in **production mode** (real money, real orders)
- Staging uses the real Alpaca API -- not sandbox -- which is why we restrict
  testing to low-value assets (RKLB, SGOV) and small amounts
- All trades are real executions with real settlement

### CCTP

- **Circle CCTP V2** on Base mainnet <-> Ethereum mainnet
- Real attestation service at `https://iris-api.circle.com`
- Real USDC bridging with ~0.01% fee per transfer

## Architecture

### What Changes vs. e2e Tests

| Component    | e2e (current)                        | Smoke test (proposed)                 |
| ------------ | ------------------------------------ | ------------------------------------- |
| Chains       | Local Anvil (fresh state)            | Base mainnet (persistent state)       |
| Broker       | `AlpacaBrokerMock` (httpmock)        | Real Alpaca production API            |
| Tokenization | `AlpacaTokenizationMock`             | Real Alpaca tokenization API          |
| CCTP         | `CctpAttestationMock` (local signer) | Real Circle attestation service       |
| Contracts    | Freshly deployed per test            | Existing staging contracts            |
| Database     | Temp SQLite (deleted after)          | Staging bot's persistent SQLite       |
| Bot instance | Spawned in-process                   | **Already running** on staging server |

### What Gets Reused

The smoke test **does not start the bot**. The bot is already running on
staging. The smoke test only acts as an **external stimulus** -- a simulated
user taking orders on the Raindex orderbook -- and then **observes** the bot's
reaction.

Reused from `tests/e2e/`:

- **Trade simulation loop**: round-robin order taking with random amounts
- **Polling infrastructure** (`poll.rs`): adapted to poll staging DB or bot's
  HTTP API instead of local SQLite
- **Assertion framework** (`assert.rs`): relaxed for non-deterministic fill
  prices and timing

Not reused (replaced by real infrastructure):

- `TestInfra` (no mock setup needed)
- `BaseChain` contract deployment (contracts already exist)
- All mock servers

### Observation Strategy

The smoke test observes the bot's behavior through:

1. **Bot HTTP API** (`/health`, `/inventory`, dashboard WebSocket) -- confirms
   the bot is alive and processing
2. **Onchain state** -- vault balances, token balances, transaction receipts
3. **Alpaca API** -- broker positions, order history, account state (using the
   same production credentials as the staging bot)

The smoke test does **not** read the bot's SQLite database directly (it runs on
a remote server). All assertions use externally observable state.

## Smoke Test Wallet

### Dedicated Test Wallet

The smoke test needs its own wallet to take orders on the Raindex orderbook.
This wallet acts as the "user" counterparty to the bot's liquidity orders.

**Requirements:**

- Separate from the bot's Turnkey wallet
  (`0xA9C16673F65AE808688cB18952AFE3d9658C808f`)
- Holds USDC and wrapped equity tokens to take both buy and sell orders
- Funded with small amounts (see "Amounts" below)

**Key management options (choose one):**

| Option                         | Pros                           | Cons                           |
| ------------------------------ | ------------------------------ | ------------------------------ |
| **Raw private key in env var** | Simple, CI-friendly            | Key in plaintext on runner     |
| **Agenix-encrypted secret**    | Consistent with existing infra | Requires NixOS host to decrypt |
| **Turnkey sub-wallet**         | Same security as bot wallet    | Overhead of Turnkey API calls  |

**Recommendation:** Use a **raw private key** stored as an agenix-encrypted
secret alongside the existing `st0x-hedge.toml.age`. The smoke test runs from
the staging server (which already has agenix decryption), so the key is never in
plaintext outside the server's tmpfs.

```
secret/smoke-test-wallet.age   # encrypted private key
```

Decrypt path: `/run/agenix/smoke-test-wallet` (same pattern as other secrets).

### Why Not Reuse the Bot Wallet?

The bot wallet is the Raindex order **owner**. Taking your own orders is a no-op
in Raindex (you can't fill your own order). The smoke test must use a different
address to act as a genuine counterparty.

## Funding

### Initial Funding (One-Time Setup)

The smoke test wallet needs tokens to take orders in both directions:

| Token                | Amount    | Purpose                                                  | Source                        |
| -------------------- | --------- | -------------------------------------------------------- | ----------------------------- |
| **USDC** (Base)      | 500 USDC  | Take SellEquity orders (user pays USDC, receives equity) | Bridge from Coinbase/exchange |
| **wtRKLB** (wrapped) | 10 shares | Take BuyEquity orders (user sells equity, receives USDC) | Bot wallet transfers or mint  |
| **wtSGOV** (wrapped) | 10 shares | Take BuyEquity orders for SGOV                           | Bot wallet transfers or mint  |
| **ETH** (Base)       | 0.01 ETH  | Gas for onchain transactions                             | Bridge from Coinbase/exchange |

**Why these amounts?** Small enough to be inconsequential if lost, large enough
to run many rounds. At current prices (~$25/RKLB, ~$100/SGOV), the total
exposure is roughly **$1,750** in equity + $500 USDC + negligible gas.

### Per-Trade Amounts

Each smoke test round takes a **small random amount** from the bot's Raindex
orders:

| Parameter      | Value         | Rationale                                      |
| -------------- | ------------- | ---------------------------------------------- |
| Min trade size | 0.1 shares    | Well above Alpaca's $1 minimum at these prices |
| Max trade size | 1.0 shares    | Small enough to not drain vaults quickly       |
| USDC per trade | ~$2.50 - $100 | Derived from share price * quantity            |

At 1 trade every 5 seconds, a 10-minute session executes ~120 trades consuming
at most ~120 shares total across both symbols. The bot's vault liquidity and
rebalancing should keep up.

### Refunding

The smoke test wallet **naturally gets refunded** by the trading loop itself:

- Taking a SellEquity order spends USDC but receives equity tokens
- Taking a BuyEquity order spends equity tokens but receives USDC
- Round-robin order taking keeps the wallet roughly balanced

Over time, the wallet may drift due to price asymmetry. A simple CLI command
should exist to top up the wallet when needed:

```bash
# Example: top up USDC from a faucet or team wallet
cast send <USDC_ADDRESS> "transfer(address,uint256)" <SMOKE_WALLET> 500e6 \
  --rpc-url $BASE_RPC --private-key $TEAM_WALLET_KEY
```

## Smoke Test Flow

### Phase 1: Preflight Checks

Before generating any trades, verify the environment is healthy:

1. **Bot health**: `GET http://staging:8001/health` returns 200
2. **Wallet funded**: smoke test wallet has sufficient USDC, wtRKLB, wtSGOV, and
   ETH for gas
3. **Raindex orders exist**: query orderbook for bot's active orders on RKLB and
   SGOV -- at least one SellEquity and one BuyEquity per symbol
4. **Broker reachable**: Alpaca sandbox API responds (using staging credentials)
5. **Vault balances**: Raindex vaults have liquidity to fill trades

If any check fails, the test aborts with a clear diagnostic message.

### Phase 2: Trade Simulation (Configurable Duration)

Core loop, directly adapted from `simulate()`:

```
for round in 1..=max_rounds:
    sleep(trade_interval)

    symbol, direction = round_robin([RKLB-Sell, RKLB-Buy, SGOV-Sell, SGOV-Buy])
    amount = random(0.1, 1.0) shares

    take_order(orderbook, order, amount, smoke_wallet)

    if success:
        record trade (symbol, direction, amount, tx_hash)
    if revert:
        log "vault drained, waiting for rebalance"
```

**Default parameters:**

| Parameter        | Default    | Configurable via      |
| ---------------- | ---------- | --------------------- |
| `max_rounds`     | 50         | `--rounds` CLI flag   |
| `trade_interval` | 5 seconds  | `--interval` CLI flag |
| `min_amount`     | 0.1 shares | `--min-amount`        |
| `max_amount`     | 1.0 shares | `--max-amount`        |

### Phase 3: Observation & Assertions

The smoke test connects to the bot's WebSocket (`/api/ws`) to observe its
reaction in real time. On connect, the bot sends a `CurrentState` snapshot
containing recent trades, inventory, positions, active transfers, and settings.
After that, it streams events: `TradeFill`, `PositionUpdate`,
`InventorySnapshot`, and `TransferUpdate`.

The smoke test correlates each onchain trade it placed with the bot's response.

#### 3a. Counter-Trading Assertions

For **each trade the smoke test places onchain**, assert that the bot:

1. **Detects the onchain fill**: a `TradeFill` event with `venue: Raindex`
   appears for the correct symbol and direction. The smoke test knows the
   tx_hash it submitted, so it can match against the trade's `id`
   (`tx_hash:log_index`). **Timeout: 60s** (block propagation + WebSocket
   delivery).

2. **Places the opposite hedge on Alpaca**: a second `TradeFill` event with
   `venue: Alpaca` appears for the same symbol with the **opposite direction**.
   If the onchain trade was a SellEquity (user sold equity to bot), the bot
   should buy on Alpaca. If BuyEquity, bot should sell on Alpaca. **Timeout:
   120s** (real Alpaca order placement + fill).

3. **Position converges toward zero**: after the hedge fill, the
   `PositionUpdate` for that symbol should show `net` within the configured
   execution threshold of zero. The bot batches hedges by threshold, so `net`
   won't be exactly zero after every single trade -- but it should never grow
   unbounded. Assert: `|position.net| <= execution_threshold` after the hedge
   fills.

4. **Hedge direction is correct**: for every Alpaca `TradeFill`, verify the
   direction is opposite to the accumulated net position. A positive net
   (accumulated long) must produce a sell hedge; negative net must produce a buy
   hedge.

**Tracking state:** The smoke test maintains a local ledger:

```
per_symbol:
  onchain_trades: [(tx_hash, direction, shares, timestamp)]
  alpaca_fills:   [(order_id, direction, shares, timestamp)]
  position_net:   Float  (from PositionUpdate events)
```

After the trade phase ends and the observation window closes, assert:

- Every onchain trade has a matching Alpaca fill (by symbol + opposite
  direction)
- `position.net` for each symbol is within threshold of zero
- Total Alpaca fill shares approximately equal total onchain trade shares
  (within Alpaca's 9-decimal truncation epsilon)

#### 3b. Rebalancing Assertions (When Enabled)

If rebalancing is enabled for the test assets, the smoke test additionally
asserts that inventory stays balanced. The bot streams `InventorySnapshot`
events containing per-symbol breakdowns:

```
SymbolInventory {
  symbol, onchain_available, onchain_inflight,
  offchain_available, offchain_inflight
}
```

**Equity rebalancing assertions:**

1. **Imbalance detection**: after sustained one-directional trading (e.g., many
   SellEquity fills drain offchain inventory), the ratio
   `onchain / (onchain + offchain)` should deviate beyond `target +/- deviation`
   (default 0.5 +/- 0.2).

2. **Transfer initiated**: a `TransferUpdate` event should appear with the
   correct type:
   - Too much offchain (ratio < 0.3) -> `Mint` transfer (Alpaca -> tokenize ->
     wrap -> deposit into Raindex)
   - Too much onchain (ratio > 0.7) -> `Redemption` transfer (Raindex -> unwrap
     -> send to Alpaca redemption wallet)

3. **Transfer completes**: the `TransferUpdate` should reach terminal state
   (`Completed`). **Timeout: 10 minutes** (tokenization API + onchain
   transactions).

4. **Inventory converges**: after the transfer completes, the next
   `InventorySnapshot` should show the ratio moved back toward the target (0.5).
   Assert: `|ratio - target| < deviation` eventually.

**USDC rebalancing assertions:**

1. **Cash imbalance detection**: `UsdcInventory` ratio
   `onchain / (onchain + offchain)` deviates beyond threshold.

2. **Bridge initiated**: a `TransferUpdate` of type USDC bridge appears:
   - Too much onchain -> `BaseToAlpaca` (withdraw from Raindex, CCTP bridge Base
     -> Ethereum, deposit into Alpaca)
   - Too much offchain -> `AlpacaToBase` (convert USD -> USDC on Alpaca,
     withdraw, CCTP bridge Ethereum -> Base, deposit into Raindex)

3. **Bridge completes**: transfer reaches `Completed`. **Timeout: 15 minutes**
   (CCTP attestation ~40-70s + Alpaca withdrawal processing).

4. **USDC inventory converges**: ratio moves back toward target.

#### 3c. Invariant Assertions (Continuous)

These are checked throughout the entire test, not just after trades:

1. **Bot stays alive**: WebSocket connection remains open. If the connection
   drops, attempt reconnect once. If it fails again, the test fails.

2. **No stuck transfers**: any `TransferUpdate` that enters a non-terminal state
   must reach a terminal state (`Completed` or `Failed`) within
   `transfer_timeout_secs` (default 1800s / 30 min). A transfer stuck in
   `Minting`, `Bridging`, etc. beyond this window is a hard failure.

3. **No position blowup**: `|position.net|` for any symbol must never exceed a
   safety bound (e.g., 50 shares). If the bot stops hedging, this catches it
   before real exposure accumulates.

4. **Inventory consistency**: `onchain_available` and `offchain_available` in
   `InventorySnapshot` must never go negative.

5. **Smoke wallet solvency**: before each trade, check the smoke wallet has
   sufficient balance to take the order. If not, skip the round and log a
   warning. If the wallet is empty for 5 consecutive rounds, fail (the
   round-robin should keep it funded).

### Phase 4: Report

Print a summary after the observation window closes:

```
Staging Smoke Test Report
=========================
Duration:           4m 12s
Trades placed:      50 (25 RKLB, 25 SGOV)
Trades filled:      48 (2 reverted - vault drain, refilled by rebalance)
Hedges observed:    48/48
Avg hedge latency:  8.3s
Max hedge latency:  23.1s
Position RKLB net:  0.00 (threshold: 1.0)
Position SGOV net:  -0.12 (threshold: 1.0)
Rebalances:         2 (1 equity mint, 1 USDC bridge)
Transfers completed: 2/2
Inventory RKLB:     52% onchain / 48% offchain (target: 50%)
Inventory SGOV:     49% onchain / 51% offchain (target: 50%)
USDC:               55% onchain / 45% offchain (target: 50%)
Bot status:         healthy (WebSocket connected)
Warnings:           1 (hedge latency > 30s on round 17)

RESULT: PASS
```

## Implementation Plan

### Binary

Add a new binary target `smoke` (alongside `server` and `cli`):

```
src/bin/smoke.rs
```

Or extend the existing CLI with a `smoke-test` subcommand:

```bash
cargo run --bin cli -- smoke-test \
    --config config/staging/st0x-hedge.toml \
    --secrets /run/agenix/st0x-hedge.toml \
    --wallet-key /run/agenix/smoke-test-wallet \
    --rounds 50 \
    --interval 5
```

**Recommendation:** CLI subcommand. It reuses the existing config loading, RPC
setup, and Alpaca client construction. No new binary needed.

### Crate Dependencies

The smoke test uses:

- **Existing config/secrets loading** from `src/config.rs`
- **Alloy provider** for onchain transactions (take orders)
- **Alpaca client** for observing broker state (read-only)
- **Raindex orderbook ABI** for `takeOrders` calls

No new crate dependencies required.

### Nix Integration

Add a `nix run .#smoke-test` command that:

1. Decrypts secrets via agenix
2. Runs the CLI subcommand against staging
3. Optionally opens the dashboard alongside (via mprocs, same as `simulate`)

### Files to Create/Modify

| File                           | Change                                        |
| ------------------------------ | --------------------------------------------- |
| `src/cli/smoke.rs`             | New: smoke test CLI subcommand                |
| `src/cli/mod.rs`               | Modified: add `SmokeTest` variant to CLI enum |
| `secret/smoke-test-wallet.age` | New: encrypted test wallet private key        |
| `secret/secrets.nix`           | Modified: add smoke-test-wallet secret        |
| `flake.nix`                    | Modified: add `smoke-test` app                |
| `docs/staging-smoke-tests.md`  | This spec                                     |

## Security Considerations

- **Smoke wallet holds real assets** (small amounts on Base mainnet). Key must
  be encrypted at rest (agenix) and only decrypted on the staging server.
- **Alpaca production credentials** are already managed via agenix. The smoke
  test reuses them read-only (observing positions/orders). It does **not** place
  broker orders -- that's the bot's job. Since this is a real broker account,
  the small trade sizes and low-value assets (RKLB, SGOV) are the primary risk
  controls.
- **No production bot interference**: the smoke test targets staging config
  only. A runtime guard should reject `--config` paths not containing "staging".
- **Rate limiting**: Alpaca production API has rate limits. The observation
  polling should use 2-5s intervals, not 200ms like e2e tests.

## Open Questions

1. **SGOV vault_id**: staging config has no `vault_id` for SGOV. Does the bot
   discover it dynamically, or does it need to be added before smoke tests can
   take SGOV orders?
2. **Rebalancing disabled**: both assets have `rebalancing = "disabled"` in
   staging. Should we enable it for smoke tests, or test hedging-only first?
3. **Operational limits**: RKLB has a commented-out `operational_limit = 1`.
   Should smoke tests respect this or use their own limits?
4. **Dashboard access**: should the smoke test connect to the staging bot's
   dashboard WebSocket for richer observation, or stick to Alpaca API + onchain
   queries?
