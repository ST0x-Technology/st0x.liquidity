# Staging Smoke Tests

**Status: proposed, not implemented.** Nothing in this document runs today.
There is no `smoke-test` subcommand, no smoke-test wallet secret, and no
`smokeTest` flake output. This is the design we agreed on, kept next to the code
so the implementer does not have to reconstruct it. Every fact about the current
system below was read out of the repo at the time of writing. When the feature
lands, rewrite this file as a runbook and drop this status block.

## Motivation

The e2e suite in `tests/e2e/` runs the whole bot against local Anvil chains and
mock services. It proves the state machines, the CQRS flows, and the business
logic are correct. It proves nothing about real infrastructure.

These are the things it cannot catch:

- config drift between the repo and the deployed `st0x-hedge.toml`
- secret rotation that breaks Alpaca or Turnkey auth
- an RPC provider that answers differently from Anvil
- a contract upgrade that changes an ABI we depend on
- real Circle attestation latency and failure modes

A staging smoke test covers that gap. It takes real orders on the staging
Raindex orderbook and then checks that the running staging bot reacts correctly.

## Goals

1. Prove the deployed bot works against real infrastructure: real Alpaca, real
   Base RPC, real Circle attestation, and the real Raindex orderbook.
2. Catch deployment regressions that CI cannot see.
3. Run on demand. A developer starts it and watches the dashboard.
4. Stay inside a hard, explicit money cap.

## Non-goals

- Replacing the e2e suite. That stays as the deterministic CI gate.
- Scheduled or automated runs. Later, if the on-demand version proves stable.
- Production. Never point this at prod.

## Staging today

Read from `config/staging/st0x-hedge.toml`. Check the file before a run, because
these values change.

### Settlement model

Staging runs **managed inventory** since PR #1072:

| Key                      | Value                                        |
| ------------------------ | -------------------------------------------- |
| `inventory_mode`         | `managed`                                    |
| `inventory`              | `0x5FE36e6b6c320f8FD0B6109B2315253387DbdeF2` |
| `vault_owner`            | `0x5FE36e6b6c320f8FD0B6109B2315253387DbdeF2` |
| `orderbook`              | `0xe522cB4a5fCb2eb31a52Ff41a4653d85A4fd7C9D` |
| Bot wallet (Turnkey)     | `0x16ca08a5825612aAe805D172a81B1a52b43574bf` |
| `required_confirmations` | 3                                            |
| `ingestion_cutoff`       | `safe`                                       |

The orders and the vaults belong to the shared `RaindexInventory` contract, not
to the bot wallet. The bot wallet only operates that contract through
`OPERATOR_ROLE`. Startup fails closed if the role is missing, so a bot that is
up has the role.

### Assets

The staging config registers about 35 equities for parity with prod. Only one is
tradable:

| Symbol     | Trading  | Rebalancing | Extended hours | Vault ID |
| ---------- | -------- | ----------- | -------------- | -------- |
| RKLB       | enabled  | enabled     | enabled        | `0xfab`  |
| all others | disabled | disabled    | disabled       | `0xfab`  |

So today the smoke test is an RKLB test. Derive the symbol list from the parsed
config and take only symbols with `trading = "enabled"`. Do not hardcode RKLB.
Enabling a second symbol is a config change and a deploy, and it must happen
before that symbol gets smoke-test traffic.

SGOV now has a `vault_id`, which it did not have when this spec was first
written. It is still `trading = "disabled"`.

### Rebalancing

| Setting                   | Value        |
| ------------------------- | ------------ |
| Equity target / deviation | 0.5 / 0.2    |
| Equity rebalancing (RKLB) | enabled      |
| USDC rebalancing          | **disabled** |
| `transfer_timeout_secs`   | 1800         |
| `freeze_check`            | disabled     |

USDC rebalancing is off in two places. `[rebalancing]` sets `usdc.mode` to
`disabled`, and `[assets.cash]` sets `rebalancing` to `disabled`. The USDC
assertions in this document therefore cannot run today. Keep them, but skip them
unless the parsed config says USDC rebalancing is enabled. Never assert a bridge
that config has switched off.

`freeze_check = "disabled"` means the dividend freeze gate will not block a mint
or a redemption on staging. Worth knowing so a rebalancing result is not
misread.

### Broker

Alpaca Broker API in **production mode**. Real money, real orders, real
settlement. Staging does not use the sandbox.

Small trade sizes do not make this safe on their own. A run can collide with
other staging activity and leave positions behind. Use a dedicated account, or
reserve an exclusive window, cap the notional, and reconcile afterwards.

### Hedging threshold

The execution threshold is **not** a config value. It comes from the executor
kind in `crates/config/src/loader.rs`:

- Alpaca executor: dollar-value threshold of **$2** (`ALPACA_MIN_DOLLARS`)
- Dry-run executor: a share threshold

RKLB trades far above $2, so nearly every single onchain fill crosses the
threshold by itself. Expect roughly one broker order per onchain trade, not a
batch of many. Assertions must still allow batching, because the bot is free to
cover several fills with one order.

### Extended hours

RKLB has `extended_hours_counter_trading = "enabled"`. This changes the hedge
path outside regular market hours:

- The counter-trade goes out as a **limit** order, priced off the last trade
  plus `counter_trade_slippage_bps` (100 bps on staging).
- `CheckPositions` cancels and reprices an unfilled extended-hours order after
  `extended_hours_reprice_timeout_secs` (300s).
- Inside `extended_hours_close_flatten_window_secs` (900s) before the extended
  close, it flattens instead.

A single fixed hedge timeout is wrong. The assertion window must depend on the
session:

| Session        | Expected hedge behavior      | Suggested timeout           |
| -------------- | ---------------------------- | --------------------------- |
| Regular hours  | market order, fills fast     | 120s                        |
| Extended hours | limit order, may reprice     | 2 reprice cycles, so ~660s  |
| Closed         | no hedge until the next open | assert deferral, not a fill |

Running inside regular hours is the simplest option and gives the tightest
assertions. Say so in the report if the run crossed a session boundary.

### Chain and CCTP

- Base mainnet, chain ID 8453, CCTP domain 6.
- Circle CCTP V2 between Base and Ethereum, real attestation service.
- CCTP only comes into play if someone enables USDC rebalancing for a run. It is
  disabled today.

## What we can reuse, and what we cannot

The earlier version of this spec said the smoke test reuses the simulation loop
from `tests/e2e/full_system.rs::simulate`, plus `poll.rs` and `assert.rs`. That
is not possible. `tests/e2e/` is a separate integration-test binary with its own
`main.rs`. A binary under `src/` cannot import it. `TestInfra` and `BaseChain`
live there too.

This is not a big loss. The loop itself is about 40 lines: sleep, pick the next
(order, symbol, direction) tuple round-robin, pick a random size, take the
order, and log the result. Copying the shape is fine. The value is in the
assertions, and those have to be written against real infrastructure anyway.

What the smoke test **can** use, because it lives in the production crates:

| Piece                     | Where                                   |
| ------------------------- | --------------------------------------- |
| Orderbook bindings        | `src/bindings.rs`, `IRaindexV6`         |
| Take-order call           | `takeOrders4` with `TakeOrdersConfigV5` |
| Config and secret loading | `st0x-config`                           |
| Wallet and provider       | `st0x-evm`                              |
| WebSocket message types   | `st0x-dto`, the `Statement` enum        |

If we later want the real `poll.rs` and `assert.rs` helpers in both places, they
must move into a crate behind the `test-support` feature. That is a separate
piece of work. Do not block the smoke test on it.

## Observation surface

The smoke test never reads the bot's SQLite file. That file is on the staging
server. Everything is observed from outside.

Staging is on the RAIN tailnet. The host is
`st0x-liquidity-staging.tail6094d7.ts.net`, and the API port is 8001.

### HTTP endpoints that exist

Read from `src/api.rs`. There is **no** `/inventory` endpoint. An earlier
version of this spec claimed one.

| Endpoint                     | Use in the smoke test                        |
| ---------------------------- | -------------------------------------------- |
| `GET /health`                | preflight liveness, and the continuous check |
| `GET /orders/raindex`        | preflight: the orders we are about to take   |
| `GET /trades`                | recovery after a WebSocket drop              |
| `GET /transfers`             | recovery, and the final transfer count       |
| `GET /transfers/interrupted` | preflight: refuse to start with stuck work   |
| `GET /pnl`                   | the cost-inclusive number for the report     |
| `GET /orders/pending`        | preflight: no open broker orders left over   |

### WebSocket

`ws://<host>:8001/api/ws?trade_protocol=terminal_outcomes_v2`

**Pin the protocol version.** The default is `legacy_fills`, and it drops every
failed and cancelled counter-trade. Those are exactly the events a smoke test
exists to catch. The three versions, from `src/dashboard/mod.rs`:

| Version                  | Trade statement | Outcomes delivered            |
| ------------------------ | --------------- | ----------------------------- |
| `legacy_fills` (default) | `trade_fill`    | filled only                   |
| `terminal_outcomes_v1`   | `trade_update`  | filled, failed                |
| `terminal_outcomes_v2`   | `trade_update`  | filled, failed, and cancelled |

The envelope is a serde tag-and-content union, so the payload sits under `data`,
not at the top level:

```json
{ "type": "trade_update", "data": { "...": "..." } }
```

Statement types, from `crates/dto/src/statement.rs`: `current_state`,
`trade_update`, `trade_fill`, `position_update`, `inventory_snapshot`, and
`transfer_update`. On connect the bot sends `current_state` first. Payload
fields are camelCase.

Shapes worth writing down, because they are easy to get wrong:

- `trade_update` carries `id`, `occurredAt`, `venue`, `direction`, `symbol`,
  `shares`, and `outcome`. `outcome` is tagged by `status`: `filled`, `failed`,
  or `cancelled`. The non-filled variants carry `acceptedShares`,
  `filledShares`, `remainingShares`, and `excessShares`.
- `inventory_snapshot` nests one level deeper than you expect:
  `data.inventory.perSymbol[]` and `data.inventory.usdc`, plus `data.fetchedAt`.
  Each `perSymbol` entry has `onchainAvailable`, `onchainInflight`,
  `offchainAvailable`, `offchainInflight`, and `inflightEquity`.
- `transfer_update` is tagged by `kind`: `equity_mint`, `equity_redemption`, or
  `usdc_bridge`. Each has its own `status` union. Terminal statuses are
  `completed`, `failed`, and **`reconciled`**. An earlier version of this spec
  missed `reconciled` and would have hung waiting on a transfer that an operator
  had already closed out.

Non-terminal statuses, so the stuck-transfer check knows what it is waiting on:

| Kind                | Non-terminal statuses                                  |
| ------------------- | ------------------------------------------------------ |
| `equity_mint`       | minting, wrapping, depositing                          |
| `equity_redemption` | withdrawing, unwrapping, sending, pending_confirmation |
| `usdc_bridge`       | converting, withdrawing, bridging, depositing          |

## Smoke test wallet

The test needs its own wallet to take orders. It is the counterparty to the
bot's liquidity.

It must be separate from the bot's Turnkey wallet. Two reasons. The bot wallet
holds operational funds and `OPERATOR_ROLE`, and sharing it puts both in the
test process. It also shares a nonce stream, so test submissions would race the
bot's own transactions.

Key handling: a raw private key stored as an agenix secret, next to
`st0x-hedge.toml.age`.

```
secret/smoke-test-wallet.age
```

It decrypts to `/run/agenix/smoke-test-wallet` on the staging server, the same
pattern as the other secrets. The test runs from that server, so the key is
never in plaintext anywhere else. A key never goes into a process argument, a
log line, or shell history.

## Funding and the money cap

### Initial funding

| Token        | Amount    | Why                       |
| ------------ | --------- | ------------------------- |
| USDC on Base | 500       | to take SellEquity orders |
| wtRKLB       | 10 shares | to take BuyEquity orders  |
| ETH on Base  | 0.01      | gas                       |

These are funding targets, not permission to spend the lot.

### The cap

`--max-notional-usd` is required. It is an absolute ceiling on gross real-money
exposure for the run. Preflight computes the worst-case sequence from the live
marks, the requested rounds, the size bounds, and the direction mix. If that
worst case can exceed the cap, preflight fails and the run does not start.

Each direction consumes a different reserve:

- SellEquity spends the wallet's USDC and returns wrapped shares.
- BuyEquity spends wrapped shares and returns USDC.

Round-robin reduces the drift but does not remove it. Check the reserves again
against live balances before every round, not only at the start.

### Sizing

| Parameter      | Default    | Rule                                           |
| -------------- | ---------- | ---------------------------------------------- |
| Min trade size | 0.1 shares | must clear the broker minimum at the live mark |
| Max trade size | 1.0 shares | must fit the remaining cap and the reserve     |

If `operational_limit` is ever set for the symbol, keep the max trade size below
it. See the open questions.

### After the run

Reconcile against the pre-run snapshots of the wallet and the broker account.
Top-ups go through the funding signer, never through a key on the command line.

## Flow

### Phase 1: preflight

Abort with a clear message on any failure. Nothing is signed before all of these
pass.

1. `GET /health` returns 200.
2. Parsed config says `telemetry.environment = "staging"`, chain ID 8453, and
   the expected orderbook, inventory, and `vault_owner` addresses. A path that
   contains the word "staging" is not an identity check.
3. The broker account ID matches the expected staging account exactly, and the
   endpoint is the expected one.
4. `GET /orders/raindex` shows at least one SellEquity order and one BuyEquity
   order for every eligible symbol.
5. The vaults behind those orders hold enough to fill the planned sizes.
6. The smoke wallet holds enough USDC, wrapped shares, and ETH.
7. `GET /transfers/interrupted` is empty, and `GET /orders/pending` has no
   leftover broker orders. Starting on top of stuck work makes every later
   assertion meaningless.
8. The worst-case exposure fits `--max-notional-usd`.
9. Record the market session. It selects the hedge timeouts.

### Phase 2: take orders

```
for round in 1..=rounds:
    sleep(interval)

    symbol, direction = round_robin(eligible pairs)
    amount           = random(min_amount, max_amount)

    nonce   = reserve_nonce(smoke_wallet)
    tx_hash = takeOrders4(orderbook, order, amount, nonce)
    persist (symbol, direction, amount, nonce, tx_hash)   # before waiting

    receipt = poll_receipt_or_replacement(nonce, tx_hash)
    match receipt:
        success -> record a confirmed trade
        revert  -> record a terminal revert; the vault is drained, wait for a
                   rebalance
        unknown -> record unknown, stop submitting, and reconcile the nonce
```

Persist the transaction hash right after broadcast. A timeout, a dropped RPC
response, a replacement, or a nonce mismatch is neither success nor failure.
Recover the transaction by sender and nonce, then poll its receipt. Leave the
result `unknown` while it is undecided. Never resubmit `takeOrders4` while an
earlier attempt can still land.

Defaults:

| Parameter    | Default    | Flag                 |
| ------------ | ---------- | -------------------- |
| rounds       | 50         | `--rounds`           |
| interval     | 5s         | `--interval`         |
| min amount   | 0.1 shares | `--min-amount`       |
| max amount   | 1.0 shares | `--max-amount`       |
| max notional | required   | `--max-notional-usd` |

### Phase 3: assertions

Keep a local ledger per symbol:

```
onchain_trades:          [(tx_hash, direction, shares, at)]
broker_trades:           [(id, direction, shares, outcome, at)]
uncovered_signed_shares: Float
position_net:            Float   # from position_update
```

#### 3a. Hedging

1. **The fill is detected.** A `trade_update` with `venue: raindex` appears for
   the symbol and direction. Match it on `id`, which is `tx_hash:log_index`, so
   the test can tie it to its own submission. Timeout 60s.

2. **The exposure is hedged.** One or more `trade_update` events with
   `venue: alpaca` cover the exposure. Direction is inverted: a SellEquity fill
   means the bot sold equity onchain, so it buys on Alpaca. A BuyEquity fill
   means it bought onchain, so it sells. Timeout depends on the session, per the
   table above.

3. **Failed and cancelled hedges are reported, not ignored.** With
   `terminal_outcomes_v2` a hedge can arrive as `failed` or `cancelled`. That is
   a finding. Record it, keep the uncovered exposure on the books, and fail the
   run if the exposure never gets covered inside the window.

4. **Coverage is consumed FIFO.** One broker order may cover several onchain
   fills. Never map one broker order to exactly one transaction.

5. **The position stays bounded.** `position_update.net` should sit under the $2
   dollar-value threshold once the bot has caught up. It will not be zero after
   every trade. It must never grow without bound.

At the end of the observation window:

- every batch that crossed the threshold has enough opposite-direction broker
  volume
- covered onchain shares equal opposite-direction broker shares, within Alpaca's
  9-decimal truncation, after converting wrapped amounts to underlying shares
- any residual is below the threshold

#### 3b. Equity rebalancing

Only run this when the parsed config says rebalancing is enabled for the symbol.
On staging that is RKLB.

1. **The imbalance appears.** After sustained one-directional trading, the ratio
   `onchain / (onchain + offchain)` moves outside `0.5 +/- 0.2`. If the
   denominator is zero, mark the check inconclusive. Do not divide.
2. **A transfer starts.** A `transfer_update` appears with the right kind. Too
   much offchain gives `equity_mint`. Too much onchain gives
   `equity_redemption`.
3. **The transfer finishes.** It reaches `completed`. Treat `reconciled` as a
   finding, not a pass: an operator closed it out by hand. Timeout 10 minutes.
4. **The ratio recovers.** A later `inventory_snapshot` moves back toward 0.5.

#### 3c. USDC rebalancing

Skipped while USDC rebalancing is disabled. When it is on:

1. The `usdc` ratio moves outside its threshold.
2. A `usdc_bridge` transfer starts. Too much onchain gives `base_to_alpaca`. Too
   much offchain gives `alpaca_to_base`.
3. It reaches `completed`. Timeout 15 minutes, which covers attestation plus
   Alpaca withdrawal processing.
4. The ratio recovers.

#### 3d. Continuous invariants

1. **The bot stays alive.** The WebSocket stays open. On a drop, reconnect once.
   Before resuming, reload `GET /trades`, `GET /transfers`, and the fresh
   `current_state`, then deduplicate against the streamed records by trade ID,
   broker order ID, transfer ID, or transaction hash. A second failure fails the
   run.
2. **No stuck transfers.** Any transfer that goes non-terminal must reach
   `completed`, `failed`, or `reconciled` inside `transfer_timeout_secs`
   (1800s). Past that is a hard failure.
3. **No exposure blowup.** The USD value of `|position.net|` never exceeds the
   directional reserve or the remaining cap.
4. **No negative inventory.** `onchainAvailable` and `offchainAvailable` never
   go below zero.
5. **The wallet stays solvent.** Check before each round. Skip the round and
   warn if it cannot fund the trade. Fail after 5 consecutive skips.

### Phase 4: report

```
Staging Smoke Test Report
=========================
Session:             regular hours
Duration:            4m 12s
Orders taken:        50 RKLB (48 confirmed, 2 reverted on a drained vault)
Fills detected:      48/48
Broker orders:       46 filled, 1 cancelled, 0 failed
Covered shares:      47.8/48.0 (0.2 residual, under the $2 threshold)
Hedge latency:       8.3s avg, 23.1s max
Position RKLB net:   0.00
Rebalances:          1 equity mint (USDC rebalancing disabled by config)
Transfers:           1/1 completed
Inventory RKLB:      52% onchain / 48% offchain (target 50%)
Realized PnL:        from GET /pnl over the run window
Warnings:            1 (hedge latency over 30s on round 17)

RESULT: PASS
```

Pull the PnL number from `GET /pnl` rather than computing it in the test. That
endpoint already nets out observed costs.

## Implementation plan

A subcommand on the existing CLI, not a new binary. It reuses config loading,
secret loading, provider setup, and the Alpaca client.

```bash
cargo run --bin cli -- smoke-test \
    --config config/staging/st0x-hedge.toml \
    --secrets /run/agenix/st0x-hedge.toml \
    --wallet-key /run/agenix/smoke-test-wallet \
    --expected-broker-account-id <staging-account-id> \
    --max-notional-usd <required> \
    --rounds 50 \
    --interval 5
```

Files:

| File                           | Change                                    |
| ------------------------------ | ----------------------------------------- |
| `src/cli/smoke.rs`             | new: the subcommand                       |
| `src/cli/mod.rs`               | add the `SmokeTest` variant to `Commands` |
| `secret/smoke-test-wallet.age` | new: the encrypted key                    |
| `secret/secrets.nix`           | declare the new secret                    |
| `flake.nix`                    | new `smokeTest` package                   |
| `docs/staging-smoke-tests.md`  | rewrite as a runbook once it works        |

No new crate dependencies.

The `smokeTest` flake output has to exist before anyone can run it with
`nix run`. It decrypts the secrets through agenix, runs the subcommand, and can
pair the dashboard alongside it with mprocs, the same way `mkSimulation` does
for the `simulate` targets.

## Security

- The smoke wallet holds real assets on Base mainnet. The key is encrypted at
  rest and is only decrypted on the staging server.
- The test does not place broker orders. That is the bot's job. Use an
  observer-only Alpaca credential if Alpaca can express that boundary. If it
  cannot, isolate the process and the account instead. Read-only code is not a
  credential boundary.
- Identity is checked before anything is signed: environment, chain ID,
  orderbook, inventory, `vault_owner`, broker endpoint, and broker account ID. A
  mismatch aborts.
- Low prices and small sizes do not make production executions harmless. The
  dedicated account, the cap, the reserves, and the reconciliation are all
  required.
- Alpaca production rate limits apply. Poll every 2 to 5 seconds, not every
  200ms like the e2e tests do.

## Open questions

1. **Broker isolation.** Can we get a dedicated staging subaccount and an
   observer-only credential? If not, every run needs an exclusive window, which
   makes this much harder to use.
2. **`operational_limit`.** It is commented out for RKLB today. It caps the
   bot's own counter-trade size, so it is not a smoke-test knob. If someone sets
   it, the bot under-hedges on purpose and the position will not converge. Do we
   keep max trade size under the limit, or fold the limit into the assertion?
   Keeping sizes under it is simpler.
3. **Session policy.** Do we require regular hours for a run, or support the
   extended-hours timeouts from day one? Regular hours only is less code and
   gives sharper assertions.
4. **Shared test helpers.** Worth moving `poll.rs` and `assert.rs` into a crate
   behind `test-support` so both the e2e suite and the smoke test use them? Not
   a blocker either way.
