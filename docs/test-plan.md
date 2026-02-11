# Integration Test Plan

## Pre-Existing Tests (What Already Crosses Boundaries)

### Conductor (`src/conductor/mod.rs`)

Several unit-ish tests that wire a few components:

- **Event processing tests** -- queue enqueue -> dequeue -> trade conversion ->
  mark processed. Stop at trade processing (don't cross threshold).
- **Abort/builder lifecycle tests** -- Conductor task management, graceful
  shutdown.
- **Vault discovery tests** -- trade events trigger vault registration via CQRS.

### Rebalancing (`src/rebalancing/`)

- **Trigger tests** use a real `mpsc::channel` and verify `TriggeredOperation`
  is sent, but the receiving end is never connected to a real manager.
- **Rebalancer dispatch tests** use MockMint/MockRedeem/MockUsdcRebalance --
  they verify routing but never run real manager logic.
- **MintManager tests** use real in-memory CQRS (`MemStore`) + mocked HTTP
  (httpmock), but are never triggered by the real trigger or dispatched by the
  real rebalancer.
- **UsdcManager tests** -- same pattern: real CQRS, mocked HTTP, but standalone.

---

## Integration Tests

Integration tests call conductor-level functions that internally orchestrate
CQRS commands and event emission. They exercise the real orchestration with real
in-memory SQLite and only mock external dependencies (broker API).

**What's real**: TradeProcessingCqrs, CQRS frameworks (SqliteCqrs), all
aggregates (OnChainTrade, Position, OffchainOrder), threshold checking, order
placement, order poller.

**What's mocked**: MockExecutor (broker API).

**Onchain events**: Tests use a real Anvil OrderBook (`AnvilOrderBook` helper)
that deploys the full Rain OrderBook contract stack + ERC20 tokens on a local
Anvil node. Each trade deploys a real order, takes it, and parses the resulting
`TakeOrderV3` event through `OnchainTrade::try_from_take_order_if_target_owner`
-- the same pipeline used in production. USDC is cloned to the real `USDC_BASE`
address so vault discovery works identically to production.

**Database**: In-memory SQLite (`:memory:`) with real migrations.

**Assertions**: Query the `events` table to verify the exact sequence of domain
events emitted, plus Position view state via `load_position()`.

### Shared Test Infrastructure (`src/integration_tests/mod.rs`)

- **`StoredEvent`** -- struct for querying CQRS events from the database
- **`ExpectedEvent`** -- helper struct for asserting event sequences
- **`fetch_events()`** -- fetches all events from the database ordered by rowid
- **`assert_events()`** -- asserts that fetched events match expected
  `(aggregate_type, aggregate_id, event_type)` triples exactly

### Arbitrage Tests (`src/integration_tests/arbitrage.rs`)

**Key helpers:**

- **`AnvilOrderBook`** -- deploys Rain OrderBook + Interpreter + Store +
  Parser + ERC20 tokens on Anvil; `deploy_usdc_at_base()` places USDC contract
  at `USDC_BASE` via cheatcodes
- **`AnvilTrade`** -- wraps a parsed `OnchainTrade` + `QueuedEvent`; `.submit()`
  calls `process_queued_trade()`
- **`assert_position()`** -- uses `#[bon::builder]` pattern for readable
  position assertions
- **`poll_and_fill()`** -- creates `OrderStatusPoller` with default
  `MockExecutor` (returns Filled) and polls
- **`create_test_cqrs()`** -- constructs all CQRS frameworks for tests

**Tests (8 total):**

1. **`onchain_trades_accumulate_and_trigger_offchain_fill`** -- Full happy path:
   two sell trades accumulate (0.5 + 0.7 = 1.2 shares), cross whole-share
   threshold, trigger offchain hedge order, poller fills it, position net = 0.
   Verifies complete 9-event sequence with payload spot-checks.

2. **`position_checker_recovers_failed_execution`** -- Recovery path: trades
   cross threshold, broker fails the order, poller handles failure (clears
   pending), position checker detects unhedged position, retries with new
   offchain order, poller fills retry. Verifies 14-event sequence across
   failure + recovery.

3. **`multi_symbol_isolation`** -- Interleaved AAPL and MSFT trades verify no
   cross-contamination. AAPL crosses threshold first (0.6 + 0.6 = 1.2), gets
   filled, then MSFT crosses (0.4 + 0.6 = 1.0), gets filled independently. Both
   positions end at net = 0.

4. **`buy_direction_accumulates_long`** -- Buy trades (0.5 + 0.7 = 1.2)
   accumulate `accumulated_long`, cross threshold, trigger a Sell hedge
   (opposite direction). Verifies hedge direction is Sell in event payloads.

5. **`exact_threshold_triggers_execution`** -- A single 1.0-share sell
   immediately crosses the whole-share threshold, producing all 5 events
   (OnChainOrderFilled, Filled, OffChainOrderPlaced, Placed, Submitted) in one
   `process_queued_trade()` call.

6. **`position_checker_noop_when_hedged`** -- After a complete hedge cycle
   (position net = 0), `check_and_execute_accumulated_positions()` emits no new
   events.

7. **`second_hedge_after_full_lifecycle`** -- Two full cycles: first sell (1.0
   shares) -> hedge -> fill, then second sell (1.5 shares) -> hedge -> fill.
   Verifies the position accumulates across cycles (accumulated_short = 2.5) and
   both hedge orders are independent. Verifies the full 14-event sequence.

8. **`take_order_discovers_equity_vault`** -- A take-order event triggers vault
   discovery via `discover_vaults_for_trade()`, producing `UsdcVaultDiscovered`
   and `EquityVaultDiscovered` events on the VaultRegistry aggregate with
   correct vault IDs, token addresses, and symbols.

### Rebalancing Tests (`src/integration_tests/rebalancing.rs`)

**Key helpers:**

- **`seed_vault_registry()`** -- seeds VaultRegistry with token address and
  deterministic vault ID
- **`discover_deterministic_tx_hash()`** -- uses Anvil snapshot/revert to
  determine the tx_hash produced by an ERC20 transfer before setting up httpmock
  responses
- **`test_trigger_config()`** -- returns standard `RebalancingTriggerConfig`
- **`build_position_cqrs_with_trigger()`** -- constructs Position CQRS with
  `RebalancingTrigger` as a query processor
- **`build_imbalanced_inventory()`** -- creates Equity or USDC imbalances in the
  InventoryView

**Tests (4 total):**

1. **`equity_offchain_imbalance_triggers_mint`** -- Full equity mint pipeline:
   position CQRS commands flow through `RebalancingTrigger` (registered as a
   query processor), update the `InventoryView`, detect an equity imbalance (20%
   onchain / 80% offchain), dispatch a Mint operation through the Rebalancer to
   the real `MintManager`, which drives the `TokenizedEquityMint` aggregate to
   completion via mocked Alpaca tokenization API. Verifies HTTP mock calls and
   CQRS event sequence.

2. **`equity_onchain_imbalance_triggers_redemption`** -- Full equity redemption
   pipeline: detects too much onchain equity (79.8% onchain / 20% offchain),
   dispatches Redemption through the Rebalancer to the real `RedemptionManager`.
   The manager sends real ERC20 tokens on Anvil, then drives the
   `EquityRedemption` aggregate through TokensSent -> Detected -> Completed via
   mocked Alpaca API. Uses Anvil snapshot/revert to discover deterministic
   tx_hash for httpmock setup. Verifies onchain ERC20 balances after redemption.

3. **`usdc_offchain_imbalance_triggers_alpaca_to_base`** -- USDC imbalance
   dispatch: 100 onchain / 900 offchain USDC (10% ratio, below 30% threshold)
   triggers `UsdcAlpacaToBase` operation. Uses mocked managers since the real
   USDC flow requires CCTP bridge. Asserts the USDC manager is called once with
   correct excess amount ($400) and other managers are not called.

4. **`usdc_onchain_imbalance_triggers_base_to_alpaca`** -- Inverse USDC
   dispatch: 900 onchain / 100 offchain USDC (90% ratio, above 70% upper bound)
   triggers `UsdcBaseToAlpaca`. Asserts base_to_alpaca is called once with
   correct excess ($400) and other managers are not called.

---

## Future: Full E2E Harness

### E2E tests (future)

E2E tests start the actual Conductor with all of its concurrent tasks running in
parallel. The Conductor spawns 8 tasks:

1. **`dex_event_receiver`** -- listens to blockchain WebSocket streams (ClearV3
   and TakeOrderV3 events), converts them to `TradeEvent`, and sends them to an
   internal channel
2. **`event_processor`** -- receives events from the channel and calls
   `enqueue()` to persist them in the `event_queue` table for dedup and ordered
   processing
3. **`queue_processor`** -- the main processing loop. Dequeues events, parses
   them via `try_from_clear_v3` (resolves token symbols, fetches AfterClearV2
   amounts), then calls `process_queued_trade()` which orchestrates all CQRS
   commands
4. **`order_poller`** -- runs on a timer (configurable interval + jitter).
   Queries the DB for SUBMITTED orders, calls `executor.get_order_status()` for
   each, and runs `handle_filled_order()` or `handle_failed_order()` to complete
   the lifecycle
5. **`position_checker`** -- runs every 60 seconds. Calls
   `check_and_execute_accumulated_positions()` which scans all symbols for
   positions that crossed the threshold but don't have a pending offchain order
   (catches cases where the queue_processor created the position state but
   didn't execute, e.g. after a crash or race condition)
6. **`rebalancer`** (optional) -- receives `TriggeredOperation` messages from
   the trigger via an mpsc channel. Dispatches to the appropriate manager
   (MintManager, RedemptionManager, or UsdcManager) based on the operation type
7. **`inventory_poller`** (optional) -- polls onchain vault balances via RPC and
   offchain balances via the executor. Records snapshots via InventorySnapshot
   CQRS aggregate. The RebalancingTrigger listens to these snapshots + Position
   events to detect inventory imbalances
8. **`executor_maintenance`** (optional) -- keeps executor authentication tokens
   fresh (e.g. Schwab OAuth refresh)

E2E tests require a simulated blockchain (for tasks 1-3), mock HTTP APIs (for
task 4's broker calls, task 6's Alpaca/CCTP calls, task 7's vault RPC), and
timing management (tasks run in infinite loops). See the "Future: Full E2E
Harness" section at the end.

A separate GitHub issue tracks a full E2E harness that starts the actual bot:
https://github.com/ST0x-Technology/st0x.liquidity/issues/264
