# Integration Testing Plan

## Current Test Coverage

~600+ tests across the workspace. Strong unit and intra-module coverage:

- **Aggregates**: Given-When-Then tests for all 8 aggregates (Position: 31,
  OffchainOrder: 43, UsdcRebalance: 73, TokenizedEquityMint: 20,
  EquityRedemption: 18, VaultRegistry: 12, Lifecycle: 8, OnChainTrade: 5)
- **Views**: InventoryView (80), OffchainOrder and Position views tested
  alongside aggregates
- **Dual-write**: 27 tests -- onchain trade witnessing, position commands,
  offchain order lifecycle, event store + legacy table consistency
- **Conductor**: 42 tests -- event processing, dual-write creation, order
  execution, vault discovery, idempotency, builder/abort lifecycle
- **Order poller**: 6 tests -- filled/failed order handling, execution lease
  renewal, dual-write commands
- **Event queue**: 11 tests -- enqueue, dedup, ordering, buffer handling
- **Accumulator**: 33 tests -- threshold logic, trade linkage, stale execution
  cleanup, concurrent processing
- **Rebalancing trigger**: ~50 tests -- imbalance detection, in-progress guards,
  terminal event handling, inventory updates from position/mint/redemption/USDC
  events
- **Rebalancing managers**: Mint (7), Redemption (2), USDC (30+) with mocked
  HTTP APIs + real in-memory CQRS aggregates
- **Rebalancer dispatch**: 7 tests -- routing verification using
  MockMint/MockRedeem/MockUsdcRebalance
- **Domain logic**: Trade parsing (31), Pyth price (25), config (27), PnL (15),
  venue balance (16), thresholds (19), CCTP (9), Alpaca wallet (8)

## Existing Integration Tests (What Already Crosses Boundaries)

Several tests in `src/conductor/mod.rs` wire multiple real components together:

- **`test_complete_event_processing_flow`** (line 1671) -- queue enqueue ->
  dequeue -> trade conversion -> accumulator -> mark processed. **Stops at
  accumulation.** Doesn't cross threshold, place order, execute, or poll.
- **`test_accumulated_position_execution_emits_placed_event_before_submission`**
  (line 3173) -- pre-built Position state -> threshold check -> place + submit.
  Verifies CQRS events + legacy table consistency. **Starts mid-pipeline**
  (skips onchain trade arrival and accumulation). **Stops before fill** (no
  polling, no position reconciliation).
- **`test_execute_pending_offchain_execution_calls_confirm_submission`**
  (line 2349) -- pre-existing pending execution in DB -> MockExecutor ->
  confirm_submission. **Covers only the execute + submit segment.**
- **`test_execute_pending_offchain_execution_dual_write_consistency`**
  (line 2661) -- same flow, specifically checks legacy table + event store
  match.
- **`test_order_poller_can_find_orders_after_execution`** (line 2577) -- creates
  pending execution -> places via dual-write -> executes -> verifies poller
  query finds SUBMITTED order. **Doesn't actually run the poller to complete the
  fill.**

In `src/rebalancing/`:

- **Trigger tests** use a real `mpsc::channel` and verify `TriggeredOperation`
  is sent, but the receiving end is never connected to a real manager.
- **Rebalancer dispatch tests** use MockMint/MockRedeem/MockUsdcRebalance --
  they verify routing but never run real manager logic.
- **MintManager tests** use real in-memory CQRS (`MemStore`) + mocked HTTP
  (httpmock), but are never triggered by the real trigger or dispatched by the
  real rebalancer.
- **UsdcManager tests** -- same pattern: real CQRS, mocked HTTP, but standalone.

## The Gap

Each test covers a 2-3 step segment. No single test chains a full flow:

**Arbitrage pipeline**: No test runs the chain from "onchain trade arrives"
through to "position fully hedged, net exposure = 0". The segments (accumulate,
threshold, place, execute, poll, fill) are all tested individually but never
wired together.

**Rebalancing pipeline**: No test chains trigger -> rebalancer dispatch -> real
manager -> real aggregate progression. Each boundary is mocked: trigger mocks
the receiver, rebalancer mocks the managers, managers mock external APIs but are
never invoked by a real trigger.

**Multi-symbol concurrency**: Per-symbol locking exists but two symbols
processed simultaneously are never tested for interference.

---

## Testing Approach

### Integration tests

Integration tests call conductor-level functions that internally orchestrate
CQRS commands, legacy transactions, and event emission. They exercise the real
orchestration with real in-memory SQLite and only mock external dependencies
(broker API).

**Key functions** (all `pub(crate)` in `src/conductor/mod.rs`):

- **`process_trade_within_transaction()`** -- the main entry point. Handles
  position initialization, fill acknowledgement, accumulation, threshold
  checking, witness events, and offchain order placement in the correct order.
  Returns `Option<OffchainExecution>` when threshold is crossed.
- **`execute_pending_offchain_execution()`** -- takes an execution ID, calls the
  broker via the Executor trait, and confirms submission via dual_write.
- **`check_and_execute_accumulated_positions()`** -- the periodic safety net.
  Scans all symbols for positions past threshold without pending execution,
  creates executions, emits Placed events, and spawns execution tasks.
  (Currently private; needs `pub(crate)` for Test 2.)

**Key types used from outside the conductor:**

- **`OrderStatusPoller::poll_pending_orders()`** -- queries SUBMITTED orders,
  polls broker for status, and runs fill/failure handling via dual_write.

**What's real**: DualWriteContext, CQRS frameworks (SqliteCqrs), all aggregates
(OnChainTrade, Position, OffchainOrder), threshold checking, accumulator,
execution lease management, stale execution cleanup.

**What's mocked**: MockExecutor (broker API), httpmock (Alpaca
tokenization/wallet API).

**What's constructed directly**: `OnchainTrade` structs (bypasses
`try_from_clear_v3` which needs RPC to fetch the companion `AfterClearV2` event
and resolve token symbols -- that function has 31 tests of its own).

**Database**: In-memory SQLite (`:memory:`) with real migrations.

**Assertions**: Query the `events` table to verify the exact sequence of domain
events emitted, plus Position view state via `load_position()`.

## Recommended Integration Tests

### 1. Full Arbitrage Pipeline (Happy Path) -- IMPLEMENTED

**Status**: Implemented in `src/integration_tests/arbitrage.rs`.

**What it proves**: The core money-making flow works end-to-end -- onchain trade
-> accumulate -> threshold -> hedge on broker -> poll -> fill -> position
reconciled.

**What's real**: DualWriteContext, all CQRS aggregates (OnChainTrade, Position,
OffchainOrder), threshold check, accumulator, execution lease management, order
placement, order poller.

**What's mocked**: MockExecutor (returns order ID, then `Filled` status).

**What's skipped**: Blockchain event parsing (`try_from_clear_v3`), WebSocket
listener, event queue enqueue/dequeue.

**Flow** (calls conductor-level functions directly):

1. Construct `OnchainTrade` directly (symbol=tAAPL, amount=0.5, direction=Sell)
   and a `QueuedEvent` wrapping it
2. Call `process_trade_within_transaction()` -- internally handles position
   initialization, fill acknowledgement, accumulation, threshold checking,
   witness events, and offchain order placement. Returns `None` (below
   threshold)
3. **Checkpoint**: Assert position state (accumulated_short=0.5, net=-0.5, no
   pending execution). Assert events: `Initialized`, `OnChainOrderFilled`,
   `Filled`
4. Construct second trade (amount=0.7) and call
   `process_trade_within_transaction()` again -- this time threshold is crossed,
   returns `Some(OffchainExecution)`
5. **Checkpoint**: Assert position state (accumulated_short=1.2, net=-1.2,
   pending_execution_id set). Assert new events: `OnChainOrderFilled`, `Filled`,
   `OffChainOrderPlaced`, `Placed`
6. Call `execute_pending_offchain_execution()` with MockExecutor -- places the
   order and confirms submission
7. Call `OrderStatusPoller::poll_pending_orders()` -- MockExecutor returns
   Filled, poller handles fill lifecycle
8. **Final checkpoint**: Assert position (net=0, pending_execution_id=None).
   Assert final events: `Submitted`, `Filled`, `OffChainOrderFilled`

**Final event sequence** (queried from `events` table):

- `PositionEvent::Initialized`
- `PositionEvent::OnChainOrderFilled` (trade 1)
- `OnChainTradeEvent::Filled` (trade 1)
- `PositionEvent::OnChainOrderFilled` (trade 2)
- `OnChainTradeEvent::Filled` (trade 2)
- `PositionEvent::OffChainOrderPlaced` (threshold crossed)
- `OffchainOrderEvent::Placed`
- `OffchainOrderEvent::Submitted` (broker accepted)
- `OffchainOrderEvent::Filled` (broker filled)
- `PositionEvent::OffChainOrderFilled` (position hedged, net=0)

### 2. Position Checker Recovers After Failed Broker Execution -- IMPLEMENTED

**Status**: Implemented in `src/integration_tests/arbitrage.rs`.

**What it proves**: When a broker order fails (e.g. Alpaca/Schwab API rejects or
reports failure), the order poller handles the failure, and the periodic
position checker detects the still-unhedged position and retries successfully.

**Why not test a crash**: After `process_trade_within_transaction()` crosses the
threshold, it atomically sets `pending_execution_id` in both SQL and the CQRS
Position aggregate. The position checker queries
`WHERE pending_execution_id IS NULL` in SQL and checks
`position.pending_execution_id.is_some()` in CQRS -- so it won't find a position
with an existing pending execution. The realistic way to reach the "unexecuted
position" state is: submit -> broker reports failure -> poller clears everything
-> position checker picks it up.

**What's real**: Same as Test 1 plus
`check_and_execute_accumulated_positions()`,
`MockExecutor::with_order_status(Failed)`.

**Prerequisite**: `check_and_execute_accumulated_positions` made `pub(crate)`.
`MockExecutor` extended with `with_order_status()` builder method.

**Flow**:

1. Call `process_trade_within_transaction()` x2 -- crosses threshold, returns
   `Some(OffchainExecution)`
2. Call `execute_pending_offchain_execution()` with default MockExecutor --
   broker accepts, confirms submission
3. Create `OrderStatusPoller` with
   `MockExecutor::new().with_order_status(Failed)` -- `poll_pending_orders()`
   discovers the failure, clears `pending_execution_id` in SQL, emits CQRS
   failure events
4. **Checkpoint**: Assert `pending_execution_id` is None on Position, net still
   -1.2 (unhedged). Assert failure events emitted.
5. Call `check_and_execute_accumulated_positions()` -- finds the position (net
   past threshold, no pending execution), creates new execution, spawns broker
   call
6. Create new `OrderStatusPoller` with default MockExecutor (returns Filled) --
   `poll_pending_orders()` completes the retry lifecycle
7. **Final checkpoint**: Position net=0, pending_execution_id=None. Events table
   shows the complete failure-recovery sequence.

### 3. Multi-Symbol Isolation

**What it proves**: Two symbols processed concurrently don't contaminate each
other's state.

**What's real**: Same as Test 1, but for two symbols (AAPL, MSFT).

**Flow**:

1. Call `process_trade_within_transaction()` with interleaved trades: tAAPL sell
   0.6 shares, tMSFT sell 0.4 shares, tAAPL sell 0.6 shares
2. Assert: AAPL's second call returns `Some(OffchainExecution)` (1.2 shares
   crosses threshold), MSFT returns `None` (0.4 below threshold)
3. Assert: Only AAPL has `OffChainOrderPlaced` / `Placed` events
4. Call `execute_pending_offchain_execution()` + `poll_pending_orders()` for
   AAPL
5. Call `process_trade_within_transaction()` with more tMSFT trades to cross
   threshold
6. Assert: MSFT gets its own independent `OffChainOrderPlaced` / `Placed` events
7. Call `execute_pending_offchain_execution()` + `poll_pending_orders()` for
   MSFT
8. Assert: Both Position aggregates independently hedged (net=0), no
   cross-contamination in events. Verify via `load_position()` for each symbol

### 4. Executor Failure and Recovery

**What it proves**: When the broker rejects an order, the system can retry.

**What's real**: Same as Test 1.

**What's mocked**: MockExecutor configured to fail the first
`place_market_order()` call, succeed the second.

**Prerequisite**: Same as Test 2 -- `check_and_execute_accumulated_positions`
needs `pub(crate)`.

**Flow**:

1. Call `process_trade_within_transaction()` with enough trades to cross
   threshold -- returns `Some(OffchainExecution)`
2. Call `execute_pending_offchain_execution()` -- MockExecutor fails
3. Assert: `OffchainOrderEvent::Failed` emitted in events
4. Call `check_and_execute_accumulated_positions()` -- re-detects the threshold
   crossing, creates a new execution, and retries via MockExecutor (which
   succeeds this time)
5. Call `OrderStatusPoller::poll_pending_orders()` to complete the fill
6. Assert: New OffchainOrder succeeds (`Placed` -> `Submitted` -> `Filled`),
   final Position state has net=0

### 5. Trigger -> Rebalancer -> Mint Manager -> Aggregate

**What it proves**: The full rebalancing chain for equity mint works when wired
together -- trigger detects imbalance, rebalancer dispatches, manager calls API,
CQRS aggregate progresses.

**What's real**: RebalancingTrigger, Rebalancer dispatch, MintManager,
TokenizedEquityMint aggregate (CQRS), VaultRegistry.

**What's mocked**: Alpaca tokenization HTTP API (httpmock), RedemptionManager
and UsdcManager (use MockRedeem/MockUsdcRebalance since we're only testing
mint).

**Flow**:

1. Set up InventoryView with equity imbalance (too many shares offchain relative
   to onchain for AAPL)
2. Register AAPL vault in VaultRegistry via CQRS command
3. Create a real MintManager with mocked HTTP + real CQRS aggregate
4. Create Rebalancer with the real MintManager + MockRedeem + MockUsdcRebalance
5. Create RebalancingTrigger connected to Rebalancer via mpsc channel
6. Call `trigger.check_and_trigger_equity()`
7. Run `rebalancer.run()` (will process one operation then exit when channel
   closes)
8. Assert: Alpaca mock received the mint HTTP request
9. Assert: TokenizedEquityMint aggregate progressed through its lifecycle events
10. Assert: MintManager's CQRS commands were executed (check events table)

### 6. Trigger -> Rebalancer -> USDC Manager -> Aggregate

**What it proves**: Same as Test 5 but for USDC rebalancing (AlpacaToBase
direction).

**What's real**: RebalancingTrigger, Rebalancer dispatch, UsdcManager,
UsdcRebalance aggregate (CQRS).

**What's mocked**: Alpaca wallet HTTP API (httpmock for withdrawal/transfer
endpoints), CCTP attestation service, MintManager and RedemptionManager
(MockMint/MockRedeem).

**Flow**:

1. Set up InventoryView with USDC imbalance (too much USD offchain)
2. Create real UsdcManager with mocked HTTP + real CQRS aggregate
3. Create Rebalancer with MockMint + MockRedeem + real UsdcManager
4. Create RebalancingTrigger connected to Rebalancer via mpsc channel
5. Call `trigger.check_and_trigger_usdc()`
6. Run `rebalancer.run()`
7. Assert: Alpaca wallet mocks received the expected HTTP requests
8. Assert: UsdcRebalance aggregate progressed through its lifecycle events

---

## Priority

1. **Full arbitrage pipeline** (#1) -- Proves the core money-making flow. Medium
   effort. Start here.
2. **Periodic position checker** (#2) -- Proves the safety net works. Low-Medium
   effort (reuses Test 1 setup).
3. **Multi-symbol isolation** (#3) -- Validates no cross-contamination. Medium
   effort.
4. **Executor failure and recovery** (#4) -- Proves resilience of core flow.
   Medium effort.
5. **Trigger -> Mint** (#5) -- Highest-value rebalancing flow. Medium effort.
6. **Trigger -> USDC Manager** (#6) -- Most complex rebalancing flow.
   Medium-High effort.

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
   amounts), then orchestrates both CQRS commands and `process_onchain_trade()`
   (see Architecture section above)
4. **`order_poller`** -- runs on a timer (configurable interval + jitter).
   Queries the DB for SUBMITTED orders, calls `executor.get_order_status()` for
   each, and runs `handle_filled_order()` or `handle_failed_order()` to complete
   the lifecycle
5. **`position_checker`** -- runs every 60 seconds. Calls
   `check_and_execute_accumulated_positions()` which scans all symbols for
   positions that crossed the threshold but don't have a pending execution
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
