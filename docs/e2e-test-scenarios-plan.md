# Plan: Add E2E Test Scenarios

## Context

The existing e2e test suite in `tests/main.rs` has a single happy-path test (`e2e_hedging_via_launch`) plus smoke tests. The user wants comprehensive scenario coverage for reliability-critical behaviors. New scenarios go into `tests/scenarios.rs`; `main.rs` keeps only the 4 smoke tests.

## File structure

| File | Action |
|------|--------|
| `tests/main.rs` | Remove `e2e_hedging_via_launch` + all helpers, keep only `smoke_test_*` |
| `tests/scenarios.rs` | New file with all 7 scenario tests |
| `tests/common.rs` | Shared helpers extracted from `main.rs` + new utilities |
| `tests/services/alpaca_broker.rs` | Add dynamic calendar control for market hours |
| `tests/services/base_chain.rs` | Add `snapshot()`, `revert()`, `mine_blocks()` |

## Shared helpers (`tests/common.rs`)

Extract from `main.rs`:
- `E2eScenario` struct + impls
- `StoredEvent` struct
- All `assert_*` functions (`assert_broker_state`, `assert_onchain_vaults`, `assert_cqrs_state`, etc.)

Add new helpers:
- `build_ctx(chain, broker, db_path, deployment_block) -> Ctx` - DRY context construction
- `spawn_bot(ctx) -> JoinHandle` - spawn `launch(ctx)` as background task
- `wait_for_processing(bot, seconds)` - sleep + panic-check
- `connect_db(db_path) -> SqlitePool`
- `count_events(pool, aggregate_type) -> usize`
- `count_all_domain_events(pool) -> usize` (excludes SchemaRegistry)

## Scenario implementations

### 1. Multi-asset sustained load

Tests multiple assets, multiple trades, correct fills, and accurate position accounting under sustained load.

- Deploy 3 equity tokens (AAPL, TSLA, MSFT)
- Configure broker mock with per-symbol fill prices via `with_symbol_fill_price()`
- Start bot
- Loop for ~30 seconds issuing take-orders across all 3 symbols in both directions (SellEquity/BuyEquity), varying amounts, randomized 100-500ms intervals
- Wait for bot to process all events + poll fills
- Assert per-symbol position nets match expected values
- Assert broker received correct orders per symbol
- Assert all offchain orders in Filled state

### 2. Backfilling

Tests bot starting after missing onchain events and correctly processing historical data.

- Deploy chain and equity token
- Execute 5+ take-orders BEFORE starting the bot
- Set `deployment_block` to block BEFORE first take-order
- Start bot - backfill runs from deployment_block to current
- Wait for processing
- Assert all historical events picked up (check `event_queue` counts)
- Assert position state matches expected net from all trades
- Assert offchain orders placed and filled

### 3. Resumption after graceful shutdown

Tests bot stops and restarts without duplicate processing.

- Start bot, process 2 trades, wait for fills
- Abort bot (`bot.abort()`)
- Snapshot DB state: event counts, position view, offchain order count
- Execute 2 more take-orders while bot is down
- Restart bot with same `db_path`
- Wait for processing
- Assert no duplicate CQRS events (compare event count = pre-shutdown + new events only)
- Assert position reflects ALL trades correctly
- Assert offchain orders for post-shutdown trades are filled

### 4. Crash recovery with eventual consistency

Tests random crashes converge to same final state as uninterrupted run.

- Run "reference" execution: process N trades (deterministic sequence), record final state
- Run "crash" execution with identical trade sequence on fresh chain + DB:
  - Start bot, process 1-2 trades
  - Abort at random point
  - Restart with same DB, process more trades
  - Abort again after 1-2 more
  - Restart, process remaining
- Compare final states: position view (net, accumulated_long, accumulated_short), offchain order count, total domain event count
- Uses deterministic trades: same symbols, amounts, directions in same order
- `INSERT OR IGNORE` on `event_queue` ensures idempotency across restarts

### 5. Market hours transitions

Tests events arriving after market close and behavior across open/close boundaries.

**Mock enhancement**: Add `set_calendar(entries)` to `AlpacaBrokerMock`. Replace the current static calendar mock with a dynamic `respond_with` closure reading from `MockState`.

- Start bot with calendar showing market closed (open/close times in the past for today)
- Execute take-orders - events should be enqueued, positions accumulated
- Assert: NO offchain orders placed (market closed check in `check_execution_readiness` -> `executor.is_market_open()` -> calendar API)
- Switch calendar to market open via `broker.set_calendar(...)`
- Wait for position checker's periodic 60s cycle to detect pending positions
- Assert: offchain orders now placed and filled
- Assert: same events were NOT re-enqueued or re-processed

### 6. Inventory auto-rebalancing

Tests equity rebalancing (mint/redemption) and USDC rebalancing (CCTP).

Full implementation with `RebalancingCtx`:
- Start all mock services: `AlpacaBrokerMock`, `AlpacaTokenizationMock`, `CctpAttestationMock`
- Generate a fresh signer for `market_maker_wallet` and a different one for `redemption_wallet`
- Configure `RebalancingCtx` with aggressive thresholds (e.g., 30% equity imbalance trigger)
- Build `Ctx` with `rebalancing: Some(rebalancing_ctx)` and `broker: BrokerCtx::AlpacaBrokerApi`
- Process enough one-directional trades to create significant inventory imbalance
- Configure tokenization mock to accept mint requests
- Wait for inventory poller (60s) + rebalancing trigger
- Assert rebalancing CQRS events emitted
- Assert tokenization mock received mint/redeem requests

### 7. Chain reorganization (stub)

Stub with TODO - Anvil doesn't generate `removed=true` WebSocket log notifications on `evm_snapshot`/`evm_revert`, so true e2e reorg testing isn't possible yet.

```rust
#[tokio::test]
#[ignore = "Requires Anvil reorg support (removed=true WebSocket notifications)"]
async fn chain_reorganization() -> anyhow::Result<()> {
    // TODO: implement when Anvil adds anvil_reorg support
    Ok(())
}
```

## Mock enhancements

### `AlpacaBrokerMock` - dynamic calendar

Add `CalendarEntry` struct and `set_calendar()` method. Change `register_calendar_endpoint` from static to dynamic `respond_with` closure.

### `BaseChain` - snapshot/revert/mine

- `snapshot() -> U256` - `evm_snapshot`
- `revert(snapshot_id: U256)` - `evm_revert`
- `mine_blocks(count: u64)` - `anvil_mine`

## Implementation order

1. Extract shared helpers into `tests/common.rs`
2. Move `e2e_hedging_via_launch` to `tests/scenarios.rs`
3. Strip `tests/main.rs` to smoke tests only
4. Verify existing tests pass
5. Add mock enhancements (calendar control, BaseChain snapshot/revert)
6. Implement scenarios 1-6 sequentially
7. Add scenario 7 stub

## Verification

```
cargo check
cargo test --test main -q
cargo test --test scenarios -q
cargo clippy --workspace --all-targets --all-features
cargo fmt
```
