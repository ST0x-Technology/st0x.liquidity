# E2E Testing Guide

This crate contains the end-to-end testing infrastructure, mock services, and
shared assertion helpers for `st0x-hedge`.

## Running Tests

### E2E tests (requires Anvil, mock services)

```bash
# All e2e tests
cargo nextest run -p e2e-tests

# Hedging scenarios
cargo nextest run -p e2e-tests --test hedging

# Rebalancing scenarios (equity mint/redeem, USDC CCTP bridge)
cargo nextest run -p e2e-tests --test rebalancing

# Single test
cargo nextest run -p e2e-tests --test hedging -- e2e_hedging_via_launch

# Ignored regression repros (run manually with output)
cargo nextest run -p e2e-tests --test rebalancing -- --ignored --nocapture
```

## Test Architecture

### Test binaries

| Binary        | File                       | Purpose                                                                                                                      |
| ------------- | -------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `hedging`     | `tests/hedging/mod.rs`     | Full hedging lifecycle: single/multi-asset trades, backfill, shutdown/restart, crash recovery, market hours, broker failures |
| `rebalancing` | `tests/rebalancing/mod.rs` | Inventory rebalancing: equity mint/redeem via ERC-4626 vaults, USDC bridging via CCTP                                        |

### Shared infrastructure

- **`src/common.rs`** -- Shared cross-scenario helpers: bot lifecycle
  (`spawn_bot`, `wait_for_processing`, `sleep_or_crash`), CQRS/database
  assertions (`assert_cqrs_state`, `assert_broker_state`), event polling
  (`poll_for_events`, `poll_for_events_with_timeout`), and the
  `assert_decimal_eq!` macro.

- **`tests/hedging/utils.rs`** -- Hedging-only helpers: `build_ctx`,
  `assert_full_hedging_flow` (broker state + onchain vaults + CQRS),
  `assert_onchain_vaults`, `poll_for_hedged_position`, queue counting
  (`count_queued_events`, `count_processed_queue_events`).

- **`tests/rebalancing/utils.rs`** -- Rebalancing-only helpers:
  `build_rebalancing_ctx`, `build_usdc_rebalancing_ctx`,
  `assert_equity_rebalancing_flow`, `assert_usdc_rebalancing_flow`,
  `assert_inventory_snapshots`, `TestWallet` (local signing wallet).

- **`src/services/mod.rs`** -- `TestInfra` builder that orchestrates all mock
  services and chain setup into a single entry point. Manages temp DB, Anvil
  chain, broker mock, tokenization mock, and CCTP attestation mock.

### Mock services (`src/services/`)

| Service                  | File                     | What it mocks                                                                                                              |
| ------------------------ | ------------------------ | -------------------------------------------------------------------------------------------------------------------------- |
| `AlpacaBrokerMock`       | `alpaca_broker.rs`       | Alpaca Broker API: account, positions, calendar, order placement/polling, wallet transfers, whitelisting. Stateful         |
| `AlpacaTokenizationMock` | `alpaca_tokenization.rs` | Alpaca Tokenization API: mint requests, redemption detection, request polling. Chain-watching redemption watcher           |
| `CctpAttestationMock`    | `cctp/attestation.rs`    | Circle CCTP attestation: fee schedule, message signing. Watches both chains for `MessageSent` events                       |
| `BaseChain`              | `base_chain.rs`          | Local Anvil chain with Raindex orderbook, USDC, ERC-4626 vaults. `take_order()` and `setup_order()` builders               |
| `CctpInfra`              | `cctp/infra.rs`          | CCTP infrastructure: spawns Ethereum Anvil, deploys CCTP contracts on both chains, replaces USDC bytecode, starts watchers |
| CCTP contracts           | `cctp/contracts.rs`      | Deploys and configures CCTP `TokenMessenger`/`MessageTransmitter` contracts on Anvil chains                                |

### `TestInfra` pattern

`TestInfra::start()` is the main entry point for all e2e tests:

```rust
let infra = TestInfra::start(
    vec![("AAPL", dec!(150.00))],  // (symbol, broker_fill_price)
    vec![],                         // (symbol, initial_position_qty)
).await?;
```

It handles:

1. Creating a temp directory for the SQLite database
2. Starting an Anvil chain and deploying ERC-4626 equity vaults per symbol
3. Funding the taker account with vault shares
4. Starting the mock broker with configured fill prices and positions
5. Starting the tokenization mock with a chain-watching redemption watcher
6. Starting the CCTP attestation mock

### Broker mock modes

The `AlpacaBrokerMock` supports different behaviors via `MockMode`:

| Mode             | Behavior                                                         |
| ---------------- | ---------------------------------------------------------------- |
| `HappyPath`      | Order placement succeeds, first poll returns filled              |
| `OrderRejected`  | Order placement succeeds, poll returns rejected                  |
| `PlacementFails` | Order placement returns HTTP 422                                 |
| `DelayedFill`    | Order placement succeeds, stays "new" for N polls before filling |

Per-symbol fill delays can also be configured independently of `MockMode`:

```rust
infra.broker_service.set_mode(MockMode::PlacementFails);
infra.broker_service.set_symbol_fill_delay(Symbol::new("AAPL")?, 5);
```

## Adding New Test Scenarios

### Hedging test

1. Create a new `#[test_log::test(tokio::test)]` function in
   `tests/hedging/mod.rs`
2. Use `TestInfra::start()` to set up infrastructure
3. Build context with `build_ctx()`, spawn bot with `spawn_bot()`
4. Wait 2s for WebSocket subscription setup, then submit orders
5. Poll for expected events with `poll_for_events()`
6. Assert with `assert_full_hedging_flow()` or manual DB queries

### Rebalancing test

1. Create a new test in `tests/rebalancing/mod.rs`
2. Use `TestInfra::start()` with initial positions if needed
3. Build context with `build_rebalancing_ctx()` (equity only) or
   `build_usdc_rebalancing_ctx()` (equity + USDC CCTP bridge)
4. Wait 8s for the bot's coordination phase (WebSocket + backfill + CQRS)
5. Submit orders, then poll for the terminal rebalancing event with
   `poll_for_events_with_timeout()` (120s timeout for full pipeline)
6. Assert with `assert_equity_rebalancing_flow()` or
   `assert_usdc_rebalancing_flow()`

### Key timing considerations

- **2s startup sleep** (hedging): Bot needs time to connect WebSocket and
  subscribe to events. Not replaceable with polling -- no observable condition
  to check.
- **8s startup sleep** (rebalancing): Longer coordination phase including
  WebSocket first-event timeout, backfill scanning, and CQRS framework setup.
- **3s inter-trade sleep**: Space between trades so each is individually hedged
  before the next position event arrives on the same symbol.
- **`poll_for_events(bot, db_path, event_type, count)`**: Polls the database for
  expected events with a 30s default timeout. Panics immediately if the bot
  crashes during polling.
- **`wait_for_processing(bot, N)`**: Sleeps for N seconds, panicking if the bot
  exits during the wait. Used in lifecycle tests (not event polling).

## Assertion Helpers

### Shared (`src/common.rs`)

| Helper                                        | Description                                                             |
| --------------------------------------------- | ----------------------------------------------------------------------- |
| `assert_broker_state(expected, broker)`       | Verify broker orders and positions match expected per symbol            |
| `assert_cqrs_state(expected, count, db_url)`  | Full CQRS check: OnChainTrade, Position, OffchainOrder events and views |
| `assert_event_subsequence(events, types)`     | Verify event types appear as ordered subsequence                        |
| `assert_single_clean_aggregate(events, errs)` | Exactly 1 aggregate, no error events                                    |
| `assert_decimal_eq!(left, right, epsilon)`    | Decimal comparison with tolerance                                       |

### Hedging (`tests/hedging/utils.rs`)

| Helper                               | Description                                                 |
| ------------------------------------ | ----------------------------------------------------------- |
| `assert_full_hedging_flow(...)`      | Broker state + onchain vaults + CQRS (combined assertion)   |
| `assert_onchain_vaults(...)`         | Verify vault balance deltas match TakeOrderV3 event amounts |
| `count_queued_events(pool)`          | Count rows in `event_queue`                                 |
| `count_processed_queue_events(pool)` | Count processed queue entries                               |

### Rebalancing (`tests/rebalancing/utils.rs`)

| Helper                             | Description                                                   |
| ---------------------------------- | ------------------------------------------------------------- |
| `assert_equity_rebalancing_flow()` | Builder: broker + CQRS + mint/redeem-specific assertions      |
| `assert_usdc_rebalancing_flow()`   | Builder: broker + CQRS + USDC bridge event/balance assertions |
| `assert_inventory_snapshots(...)`  | Verify InventorySnapshot events for tracked symbols           |

### Database (`src/common.rs`)

| Helper                               | Description                                                 |
| ------------------------------------ | ----------------------------------------------------------- |
| `connect_db(db_path)`                | Open SQLite connection to test database                     |
| `count_events(pool, aggregate_type)` | Count events by aggregate type (e.g. `"OffchainOrder"`)     |
| `count_all_domain_events(pool)`      | Count all non-bookkeeping events                            |
| `count_hedging_domain_events(pool)`  | Count events excluding SchemaRegistry and InventorySnapshot |
| `fetch_all_domain_events(pool)`      | Fetch all events ordered by insertion                       |
| `fetch_events_by_type(pool, type)`   | Fetch events for a specific aggregate type                  |
