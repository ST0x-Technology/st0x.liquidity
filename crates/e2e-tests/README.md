# E2E Testing Guide

This crate contains the end-to-end testing infrastructure, mock services,
and shared assertion helpers for `st0x-hedge`.

## Running Tests

### Library tests (fast, no infrastructure)

```bash
cargo nextest run --workspace --lib
```

### E2E tests (requires Anvil, mock services)

```bash
# Hedging scenarios
cargo nextest run -p e2e-tests --test hedging

# Rebalancing scenarios (equity mint/redeem, USDC CCTP bridge)
cargo nextest run -p e2e-tests --test rebalancing

# Smoke tests
cargo nextest run -p e2e-tests --test main
```

### Running a single test

```bash
cargo nextest run -p e2e-tests --test hedging -- e2e_hedging_via_launch
```

## Test Architecture

### Test binaries

| Binary | File | Purpose |
|--------|------|---------|
| `hedging` | `tests/hedging.rs` | Full hedging lifecycle: single/multi-asset trades, backfill, shutdown/restart, crash recovery, market hours, broker failures |
| `rebalancing` | `tests/rebalancing.rs` | Inventory rebalancing: equity mint/redeem via ERC-4626 vaults, USDC bridging via CCTP |
| `main` | `tests/main.rs` | Smoke tests |

### Shared infrastructure

- **`src/common.rs`** -- Bot lifecycle helpers (`spawn_bot`,
  `wait_for_processing`), database assertion utilities (`count_events`,
  `count_events_for_aggregate`, `connect_db`), context builders (`build_ctx`,
  `build_rebalancing_ctx`), and comprehensive pipeline assertion functions
  (`assert_full_hedging_flow`, `assert_equity_rebalancing_flow`).

- **`src/services/mod.rs`** -- `TestInfra` builder that orchestrates all mock
  services and chain setup into a single entry point. Manages temp DB, Anvil
  chain, broker mock, tokenization mock, and CCTP attestation mock.

### Mock services (`src/services/`)

| Service | File | What it mocks |
|---------|------|---------------|
| `AlpacaBrokerMock` | `alpaca_broker.rs` | Alpaca Broker API: account info, positions, calendar, order placement/polling, wallet transfers, whitelisting. Stateful -- tracks orders, fills, and account balances |
| `AlpacaTokenizationMock` | `alpaca_tokenization.rs` | Alpaca Tokenization API: mint requests, redemption detection, request polling. Watches chain for Transfer events to auto-detect redemptions |
| `CctpAttestationMock` | `cctp_attestation.rs` | Circle CCTP attestation service: fee schedule, message attestation. Watches both Ethereum and Base chains for `MessageSent` events and produces signed attestations |
| `BaseChain` | `base_chain.rs` | Local Anvil chain with Raindex orderbook, USDC, and ERC-4626 vault deployments. Provides `take_order()` and `setup_order()` builders |
| `EthereumChain` | `ethereum_chain.rs` | Forked Ethereum mainnet via Anvil for CCTP cross-chain tests. Provides `USDC_ETHEREUM` address constant |
| CCTP contracts | `cctp_contracts.rs` | Deploys and configures CCTP `TokenMessenger`/`MessageTransmitter` contracts on Anvil chains |

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
2. Starting an Anvil chain and deploying ERC-4626 equity vaults
3. Funding the taker account with vault shares
4. Starting the mock broker with configured prices and positions
5. Starting the tokenization mock with a chain-watching redemption watcher
6. Starting the CCTP attestation mock

### Broker mock modes

The `AlpacaBrokerMock` supports different behaviors via `MockMode`:

| Mode | Behavior |
|------|----------|
| `HappyPath` | Order placement succeeds, first poll returns filled |
| `OrderRejected` | Order placement succeeds, poll returns rejected |
| `PlacementFails` | Order placement returns HTTP 422 |

```rust
infra.broker_service.set_mode(MockMode::PlacementFails);
```

## Adding New Test Scenarios

### Hedging test

1. Create a new `#[test_log::test(tokio::test)]` function in `tests/hedging.rs`
2. Use `TestInfra::start()` to set up infrastructure
3. Build context with `build_ctx()`, spawn bot with `spawn_bot()`
4. Wait 2s for WebSocket subscription setup, then submit orders
5. Use `wait_for_processing()` to wait for the bot to process events
6. Assert with `assert_full_hedging_flow()` or manual DB queries

### Rebalancing test

1. Create a new test in `tests/rebalancing.rs`
2. Use `TestInfra::start()` with initial positions if needed
3. Build context with `build_rebalancing_ctx()` (includes equity token config)
4. Wait 8s for the bot's coordination phase (WebSocket + backfill + CQRS)
5. Submit orders and wait for processing (80s+ for full rebalancing pipeline)
6. Assert with `assert_equity_rebalancing_flow()`

### Key timing considerations

- **2s startup sleep** (hedging): Bot needs time to connect WebSocket and
  subscribe to events. Not replaceable with polling -- no observable condition
  to check.
- **8s startup sleep** (rebalancing): Longer coordination phase including
  WebSocket first-event timeout, backfill scanning, and CQRS framework setup.
- **3s inter-trade sleep**: Space between trades so each is individually hedged
  before the next position event arrives on the same symbol.
- **`wait_for_processing(bot, N)`**: Polls the bot handle for N seconds. If the
  bot crashes during the wait, the test fails immediately with the crash error.

## Database Assertion Helpers

| Helper | Description |
|--------|-------------|
| `count_events(pool, aggregate_type)` | Count events by aggregate type (e.g. `"OffchainOrder"`) |
| `count_events_for_aggregate(pool, type, id)` | Count events for a specific aggregate instance |
| `count_all_domain_events(pool)` | Count all non-bookkeeping events |
| `count_queued_events(pool)` | Count rows in `event_queue` |
| `count_processed_queue_events(pool)` | Count processed queue entries |
| `fetch_all_domain_events(pool)` | Fetch all events ordered by insertion |
