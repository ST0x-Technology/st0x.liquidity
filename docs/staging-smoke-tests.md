# Staging Smoke Tests

## Motivation

The e2e test suite (`tests/e2e/`) validates the bot's logic using local Anvil
chains and mock services. This gives us confidence that state machines, CQRS
event flows, and business logic are correct -- but it says nothing about whether
the bot works against **real infrastructure**: real RPC nodes, real Alpaca
production APIs, real CCTP attestation, real Raindex orderbook.

Staging smoke tests bridge that gap. They reuse the same simulation loop from
`tests/e2e/full_system.rs::simulate` -- randomly generating trades, taking
orders onchain, and asserting the bot counter-trades and rebalances -- but
pointed at the live staging environment instead of mocks.

## Goals

1. **Validate integration**: real Alpaca production API, real Base RPC, real
   CCTP, real Raindex orderbook
2. **Catch deployment regressions**: config drift, secret rotation issues, RPC
   provider changes, contract upgrades
3. **Run on demand**: developer triggers the test manually (not CI), observes
   via dashboard
4. **Bounded impact**: runs against a dedicated broker account or during an
   exclusive staging window, with a hard notional cap and post-test
   reconciliation

## Non-Goals

- Replacing the existing e2e suite (that stays as-is for deterministic CI)
- Automated scheduled execution (future work)
- Production environment testing

## Staging Environment

### Assets Under Test

The smoke test derives its eligible symbols from the parsed staging config and
only exercises assets with `trading = "enabled"`. The current config snapshot
is:

| Symbol   | Trading  | Rebalancing | Tokenized Equity                             | Vault Wrapper (ERC-4626)                     | Vault ID |
| -------- | -------- | ----------- | -------------------------------------------- | -------------------------------------------- | -------- |
| **RKLB** | enabled  | enabled     | `0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b` | `0xf4f8c66085910d583c01f3b4e44bf731d4e2c565` | `0xfab`  |
| **SGOV** | disabled | disabled    | `0xc941C1506B7555Ba8C506Fb6c9b9CC259902d612` | `0x78c31580c97101694c70022c83d570150c11e935` | `0xfab`  |

SGOV is not currently eligible. Enabling it is a deployment/config change that
must precede any SGOV smoke-test traffic.

### Chain

- **Base mainnet** (chain ID 8453, CCTP domain 6)
- Orderbook: `0xe522cB4a5fCb2eb31a52Ff41a4653d85A4fd7C9D`
- Inventory and order/vault owner: `0x5FE36e6b6c320f8FD0B6109B2315253387DbdeF2`
- Bot operator/signing wallet: `0x16ca08a5825612aAe805D172a81B1a52b43574bf`

### Broker

- **Alpaca Broker API** in **production mode** (real money, real orders)
- Staging uses the real Alpaca API -- not sandbox. Small amounts alone do not
  make these tests non-destructive: they can collide with concurrent staging
  activity and leave residual positions
- Use a dedicated broker account/subaccount. If that is unavailable, reserve an
  exclusive test window, enforce a hard notional budget, and reconcile orders,
  positions, and cash after the run
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
3. **Alpaca API** -- broker positions, order history, account state. Use a
   separate observer credential without order-placement capability when Alpaca
   exposes that permission boundary. Otherwise isolate the observer process and
   dedicated staging account from other workloads rather than treating read-only
   code paths as a credential boundary

The smoke test does **not** read the bot's SQLite database directly (it runs on
a remote server). All assertions use externally observable state.

## Smoke Test Wallet

### Dedicated Test Wallet

The smoke test needs its own wallet to take orders on the Raindex orderbook.
This wallet acts as the "user" counterparty to the bot's liquidity orders.

**Requirements:**

- Separate from the bot's Turnkey operator wallet
  (`0x16ca08a5825612aAe805D172a81B1a52b43574bf`)
- Holds USDC and the wrapped equity token for every eligible symbol to take both
  buy and sell orders
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

Managed staging orders are owned by the inventory contract, but the bot's
Turnkey wallet is its privileged operator and uses its own nonce stream and
balances for production operations. Reusing it would couple test submissions to
bot transactions and put operational funds and permissions in the test process.
The smoke test therefore uses a separate address as the counterparty.

## Funding

### Initial Funding (One-Time Setup)

The smoke test wallet needs tokens to take orders in both directions:

| Token                | Amount    | Purpose                                                  | Source                        |
| -------------------- | --------- | -------------------------------------------------------- | ----------------------------- |
| **USDC** (Base)      | 500 USDC  | Take SellEquity orders (user pays USDC, receives equity) | Bridge from Coinbase/exchange |
| **wtRKLB** (wrapped) | 10 shares | Take BuyEquity orders (user sells equity, receives USDC) | Bot wallet transfers or mint  |
| **ETH** (Base)       | 0.01 ETH  | Gas for onchain transactions                             | Bridge from Coinbase/exchange |

These are initial funding targets, not an authorization to consume the full
balance. Before each run, query live marks and compute the USD value of all
funded assets. The required `--max-notional-usd` argument is an absolute cap on
the test's gross real-money exposure; preflight fails if the starting inventory,
configured trade range, or requested rounds can exceed it. If additional symbols
become eligible, add their wrapped-token reserves to the funding plan before
enabling their test cases.

### Per-Trade Amounts

Each smoke test round takes a random amount admitted by live prices, the
remaining notional budget, and direction-specific reserves:

| Parameter      | Example value | Admission rule                                     |
| -------------- | ------------- | -------------------------------------------------- |
| Min trade size | 0.1 shares    | Meets the broker minimum at the current live mark  |
| Max trade size | 1.0 shares    | Fits the remaining USD cap and directional reserve |
| USDC per trade | Dynamic       | Live order quote plus the configured safety buffer |

Preflight calculates separate reserves for every symbol and direction:
`SellEquity` consumes smoke-wallet USDC, while `BuyEquity` consumes that
symbol's wrapped shares. Requested rounds, trade sizes, and direction mix are
accepted only when the worst-case sequence fits all reserves and the absolute
USD cap. A later round repeats the check against current balances and live marks
before submitting.

### Refunding

The trading loop redistributes the smoke wallet's inventory:

- Taking a SellEquity order spends USDC but receives equity tokens
- Taking a BuyEquity order spends equity tokens but receives USDC
- Round-robin order taking may reduce drift, but does not guarantee balance

Every run therefore ends with reconciliation against the pre-test wallet and
broker snapshots. Top-ups use the dedicated funding signer/service or a
supported secret-input mechanism; private keys must never be expanded into
process arguments, logs, or shell history.

## Smoke Test Flow

### Phase 1: Preflight Checks

Before generating any trades, verify the environment is healthy:

1. **Bot health**: `GET http://staging:8001/health` returns 200
2. **Wallet funded**: smoke test wallet has sufficient USDC, each eligible
   symbol's wrapped token, and ETH for gas
3. **Raindex orders exist**: query the orderbook for the inventory owner's
   active orders -- at least one SellEquity and one BuyEquity for every eligible
   symbol
4. **Broker identity and access**: read-only calls to the Alpaca production API
   verify credentials, account state, limits, current order access, and the
   exact expected staging account ID. Abort if the endpoint or account identity
   differs from the staging configuration
5. **Vault balances**: Raindex vaults have liquidity to fill trades
6. **Exposure budget**: live marks, per-symbol/per-direction reserves, requested
   rounds, and trade-size bounds fit the required absolute USD cap

If any check fails, the test aborts with a clear diagnostic message.

### Phase 2: Trade Simulation (Configurable Duration)

Core loop, directly adapted from `simulate()`:

```
for round in 1..=max_rounds:
    sleep(trade_interval)

    symbol, direction = round_robin(eligible_symbol_direction_pairs)
    amount = random(0.1, 1.0) shares

    sender_nonce = reserve_nonce(smoke_wallet)
    tx_hash = submit_take_order(orderbook, order, amount, sender_nonce)
    persist pending trade (symbol, direction, amount, sender_nonce, tx_hash)

    receipt = poll_receipt_or_replacement(sender_nonce, tx_hash)
    if receipt.success:
        record confirmed trade (symbol, direction, amount, receipt.tx_hash)
    if receipt.revert:
        record terminal revert
        log "vault drained, waiting for rebalance"
    if receipt.unknown:
        record unknown outcome
        stop new submissions and reconcile the nonce before continuing
```

Persist the transaction hash immediately after broadcast. A timeout, dropped RPC
response, replacement, or nonce mismatch is neither success nor revert. The test
must recover the submitted/replacement transaction by sender and nonce, poll its
receipt, and leave the result `unknown` while inconclusive. It never blindly
retries `take_order` while a prior outcome may still land.

**Default parameters:**

| Parameter        | Default    | Configurable via      |
| ---------------- | ---------- | --------------------- |
| `max_rounds`     | 50         | `--rounds` CLI flag   |
| `trade_interval` | 5 seconds  | `--interval` CLI flag |
| `min_amount`     | 0.1 shares | `--min-amount`        |
| `max_amount`     | 1.0 shares | `--max-amount`        |
| `max_notional`   | Required   | `--max-notional-usd`  |

### Phase 3: Observation & Assertions

The smoke test connects to the bot's WebSocket (`/api/ws`) to observe its
reaction in real time. The JSON envelope uses a lower-snake-case `type` and
camel-case payload fields. On connect, the bot sends `type: "current_state"`
containing recent trades, inventory, positions, active transfers, and settings.
After that, it streams `trade_fill`, `position_update`, `inventory_snapshot`,
and `transfer_update`. Rust names such as `CurrentState` and `InventorySnapshot`
below are conceptual types; assertions use the serialized JSON names (for
example, `perSymbol` and `onchainAvailable`).

The smoke test correlates confirmed onchain trades with the bot's aggregate
per-symbol response.

#### 3a. Counter-Trading Assertions

Track every confirmed onchain trade, then assert:

1. **Detects the onchain fill**: a `trade_fill` event with `venue: Raindex`
   appears for the correct symbol and direction. The smoke test knows the
   tx_hash it submitted, so it can match against the trade's `id`
   (`tx_hash:log_index`). **Timeout: 60s** (block propagation + WebSocket
   delivery).

2. **Hedges threshold-crossing exposure on Alpaca**: when accumulated exposure
   crosses its execution threshold, one or more `trade_fill` events with
   `venue: Alpaca` cover that per-symbol batch. A single broker fill may cover
   several onchain trades. `SellEquity` means the bot's order sold equity to the
   taker, so the bot buys on Alpaca; `BuyEquity` means the bot bought equity
   from the taker, so it sells on Alpaca. **Timeout: 120s** after the threshold
   is crossed.

3. **Position converges toward zero**: after the hedge fill, the
   `position_update` for that symbol should show `net` below the configured
   shares or dollar-value execution threshold. The bot batches hedges by
   threshold, so `net` won't be exactly zero after every single trade -- but it
   should never grow unbounded.

4. **Hedge direction is correct**: for every Alpaca `trade_fill`, verify the
   direction is opposite to the accumulated net position. A positive net
   (accumulated long) must produce a sell hedge; negative net must produce a buy
   hedge.

**Tracking state:** The smoke test maintains a local ledger:

```
per_symbol:
  onchain_trades: [(tx_hash, direction, shares, timestamp)]
  alpaca_fills:   [(order_id, direction, shares, timestamp)]
  uncovered_signed_shares: Float
  position_net:   Float  (from position_update events)
```

Each onchain fill updates `uncovered_signed_shares`. When a broker fill arrives,
the ledger consumes exposure from that per-symbol accumulator in FIFO order,
allowing a many-to-one hedge relationship. It never assigns one broker fill to
exactly one onchain transaction.

After the trade phase ends and the observation window closes, assert:

- Every threshold-crossing batch has sufficient opposite-direction Alpaca fill
  volume
- `position.net` for each symbol is below its configured shares or dollar-value
  threshold
- Covered onchain shares equal opposite-direction Alpaca fill shares within
  Alpaca's 9-decimal truncation epsilon after wrapped amounts are converted to
  underlying shares; any unmatched residual is below the execution threshold

#### 3b. Rebalancing Assertions (When Enabled)

If rebalancing is enabled for the test assets, the smoke test additionally
asserts that inventory stays balanced. The bot streams `inventory_snapshot`
events containing `perSymbol` entries:

```
{
  symbol, onchainAvailable, onchainInflight,
  offchainAvailable, offchainInflight
}
```

**Equity rebalancing assertions:**

1. **Imbalance detection**: after sustained one-directional trading (e.g., many
   SellEquity fills drain offchain inventory), the ratio
   `onchain / (onchain + offchain)` should deviate beyond `target +/- deviation`
   (default 0.5 +/- 0.2). If the denominator is zero, mark this ratio check
   inconclusive and do not divide; normal validation resumes when either balance
   is nonzero.

2. **Transfer initiated**: a `transfer_update` event should appear with the
   correct type:
   - Too much offchain (ratio < 0.3) -> `Mint` transfer (Alpaca -> tokenize ->
     wrap -> deposit into Raindex)
   - Too much onchain (ratio > 0.7) -> `Redemption` transfer (Raindex -> unwrap
     -> send to Alpaca redemption wallet)

3. **Transfer completes**: the `transfer_update` should reach terminal state
   (`Completed`). **Timeout: 10 minutes** (tokenization API + onchain
   transactions).

4. **Inventory converges**: after the transfer completes, the next
   `inventory_snapshot` should show the ratio moved back toward the target
   (0.5). Assert `|ratio - target| < deviation` eventually when the denominator
   is nonzero; otherwise mark the check inconclusive.

**USDC rebalancing assertions:**

1. **Cash imbalance detection**: `UsdcInventory` ratio
   `onchain / (onchain + offchain)` deviates beyond threshold. A zero
   denominator is inconclusive rather than a ratio failure.

2. **Bridge initiated**: a `transfer_update` of type USDC bridge appears:
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
   drops, attempt reconnect once. Before resuming assertions, reload all
   submitted transaction receipts, Alpaca order history, and the latest
   `current_state` dashboard snapshot. Deduplicate the recovered and streamed
   records by stable trade ID, broker order ID, transfer ID, or transaction
   hash. If reconciliation or the second connection fails, the test fails.

2. **No stuck transfers**: any `transfer_update` that enters a non-terminal
   state must reach a terminal state (`Completed` or `Failed`) within
   `transfer_timeout_secs` (default 1800s / 30 min). A transfer stuck in
   `Minting`, `Bridging`, etc. beyond this window is a hard failure.

3. **No position blowup**: the live-mark USD value of `|position.net|` for any
   symbol must never exceed its directional reserve or the remaining absolute
   USD cap. If the bot stops hedging, this catches it before the authorized real
   exposure is exceeded.

4. **Inventory consistency**: `onchainAvailable` and `offchainAvailable` in
   `inventory_snapshot` must never go negative.

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
Trades placed:      50 (50 RKLB)
Trades filled:      48 (2 reverted - vault drain, refilled by rebalance)
Onchain fills seen:  48/48
Hedge batches:       12
Covered shares:      47.8/48.0 (0.2 residual < 1.0 threshold)
Avg hedge latency:  8.3s
Max hedge latency:  23.1s
Position RKLB net:  0.00 (threshold: 1.0)
Rebalances:         1 (equity mint; USDC rebalancing disabled)
Transfers completed: 1/1
Inventory RKLB:     52% onchain / 48% offchain (target: 50%)
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
    --expected-broker-account-id <staging-account-id> \
    --max-notional-usd <required-cap> \
    --rounds 50 \
    --interval 5
```

**Recommendation:** CLI subcommand. It reuses the existing config loading, RPC
setup, and Alpaca client construction. No new binary needed.

### Crate Dependencies

The smoke test uses:

- **Existing config/secrets loading** from the `st0x-config` crate
  (`crates/config/`)
- **Alloy provider** for onchain transactions (take orders)
- **Alpaca client** for observing broker state (read-only)
- **Raindex orderbook ABI** for `takeOrders` calls

No new crate dependencies required.

### Nix Integration

The implementation PR must export a `packages.smokeTest` flake output before
operators use the planned `nix run .#smokeTest` command. That output will:

1. Decrypt secrets via agenix
2. Run the CLI subcommand against staging
3. Optionally open the dashboard alongside (via mprocs, same as `simulate`)

The command is not available in the current flake; documenting it here defines
the required implementation output rather than a command that works today.

### Files to Create/Modify

| File                           | Change                                        |
| ------------------------------ | --------------------------------------------- |
| `src/cli/smoke.rs`             | New: smoke test CLI subcommand                |
| `src/cli/mod.rs`               | Modified: add `SmokeTest` variant to CLI enum |
| `secret/smoke-test-wallet.age` | New: encrypted test wallet private key        |
| `secret/secrets.nix`           | Modified: add smoke-test-wallet secret        |
| `flake.nix`                    | Modified: add `smokeTest` package             |
| `docs/staging-smoke-tests.md`  | This spec                                     |

## Security Considerations

- **Smoke wallet holds real assets** (small amounts on Base mainnet). Key must
  be encrypted at rest (agenix) and only decrypted on the staging server.
- **Alpaca production credentials** are managed via agenix. Prefer a separate
  observer credential without order-placement capability. If Alpaca cannot
  enforce that permission boundary, isolate the observer process and dedicated
  staging account. The smoke test does **not** place broker orders -- that is
  the bot's job.
- **Explicit staging identity**: before loading a signing key or submitting a
  transaction, parse the configuration and require
  `telemetry.environment = "staging"`, Base chain ID 8453, the expected
  orderbook, inventory/vault-owner addresses, production broker endpoint, and
  exact staging broker account ID. A path substring is not an identity check.
  Reject any mismatch before signing or sending.
- **Bounded real exposure**: low-value assets and small nominal trade sizes do
  not make production-API executions non-destructive. A dedicated account or
  exclusive window, the required absolute USD cap, directional reserves, and
  post-test reconciliation are all mandatory.
- **Rate limiting**: Alpaca production API has rate limits. The observation
  polling should use 2-5s intervals, not 200ms like e2e tests.

## Open Questions

1. **Broker isolation**: can Alpaca provide a dedicated staging subaccount and
   observer-only credential, or must runs reserve an exclusive window?
2. **Operational limits**: RKLB has a commented-out `operational_limit = 1`.
   Should smoke tests respect this or use their own limits?
3. **Dashboard access**: should the smoke test connect to the staging bot's
   dashboard WebSocket for richer observation, or stick to Alpaca API + onchain
   queries?
