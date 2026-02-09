# Integration Testing Plan

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

## The Gap

Each test covers a 2-3 step segment. No single test chains a full flow:

**Arbitrage pipeline**: No test runs the chain from "onchain trade arrives"
through to "position fully hedged, net exposure = 0". The segments (accumulate,
threshold, place, poll, fill) are all tested individually but never wired
together.

**Rebalancing pipeline**: No test chains trigger -> rebalancer dispatch -> real
manager -> real aggregate progression. Each boundary is mocked: trigger mocks
the receiver, rebalancer mocks the managers, managers mock external APIs but are
never invoked by a real trigger.

**Multi-symbol concurrency**: Per-symbol position isolation exists but two
symbols processed simultaneously are never tested for interference.

---

## Testing Approach

### Integration tests

Integration tests call conductor-level functions that internally orchestrate
CQRS commands and event emission. They exercise the real orchestration with real
in-memory SQLite and only mock external dependencies (broker API).

**Key functions** (all `pub(crate)` in `src/conductor/mod.rs`):

- **`process_queued_trade()`** -- the main entry point. Takes a `QueuedEvent`
  and `OnchainTrade`, handles position fill acknowledgement, accumulation,
  threshold checking, and offchain order placement. The `PlaceOrder` CQRS
  command atomically calls the executor and emits both `Placed` and `Submitted`
  events. Returns `Option<OffchainOrderId>` when threshold is crossed.
- **`check_and_execute_accumulated_positions()`** -- the periodic safety net.
  Scans all symbols for positions past threshold without a pending offchain
  order, creates new offchain orders, and places them via the executor.

**Key types used from outside the conductor:**

- **`TradeProcessingCqrs`** -- bundles `OnChainTradeCqrs`, `PositionCqrs`,
  `PositionQuery`, `OffchainOrderCqrs`, and `ExecutionThreshold`.
- **`ExecutorOrderPlacer<E>`** -- adapter that bridges the generic `Executor`
  trait to the `OrderPlacer` trait used by the OffchainOrder aggregate.
- **`OrderStatusPoller::poll_pending_orders()`** -- queries SUBMITTED orders
  from the offchain_order_view, polls broker for status, and runs fill/failure
  handling via CQRS commands on both the OffchainOrder and Position aggregates.

**What's real**: TradeProcessingCqrs, CQRS frameworks (SqliteCqrs), all
aggregates (OnChainTrade, Position, OffchainOrder), threshold checking, order
placement, order polling.

**What's mocked**: MockExecutor (broker API).

**What's constructed directly**: `OnchainTrade` structs (bypasses
`try_from_clear_v3` which needs RPC to fetch the companion `AfterClearV2` event
and resolve token symbols -- that function has its own unit tests).

**Database**: In-memory SQLite (`:memory:`) with real migrations.

**Assertions**: Query the `events` table to verify the exact sequence of domain
events emitted, plus Position view state via `load_position()`.

## Recommended Integration Tests

### 1. Full Arbitrage Pipeline (Happy Path)

**What it proves**: The core money-making flow works end-to-end -- onchain trade
-> accumulate -> threshold -> hedge on broker -> poll -> fill -> position
reconciled.

**What's real**: TradeProcessingCqrs, all CQRS aggregates (OnChainTrade,
Position, OffchainOrder), threshold check, order placement, order poller.

**What's mocked**: MockExecutor (returns order ID, then `Filled` status).

**What's skipped**: Blockchain event parsing (`try_from_clear_v3`), WebSocket
listener, event queue enqueue/dequeue.

**Flow** (calls conductor-level functions directly):

1. Construct `OnchainTrade` directly (symbol=tAAPL, amount=0.5, direction=Sell)
   and a `QueuedEvent` wrapping it
2. Call `process_queued_trade()` -- internally handles position fill
   acknowledgement, accumulation, threshold checking, and offchain order
   placement. Returns `None` (below threshold)
3. **Checkpoint**: Assert position state (accumulated_short=0.5, net=-0.5, no
   pending_offchain_order_id). Assert events: `OnChainOrderFilled`, `Filled`
4. Construct second trade (amount=0.7) and call `process_queued_trade()` again
   -- this time threshold is crossed, returns `Some(OffchainOrderId)`
5. **Checkpoint**: Assert position state (accumulated_short=1.2, net=-1.2,
   pending_offchain_order_id set). Assert new events: `OnChainOrderFilled`,
   `Filled`, `OffChainOrderPlaced`, `Placed`, `Submitted` (Placed and Submitted
   are emitted atomically by the PlaceOrder command)
6. Call `OrderStatusPoller::poll_pending_orders()` -- MockExecutor returns
   Filled, poller handles fill lifecycle
7. **Final checkpoint**: Assert position (net=0,
   pending_offchain_order_id=None). Assert final events: `Filled`,
   `OffChainOrderFilled`

**Final event sequence** (queried from `events` table):

- `PositionEvent::OnChainOrderFilled` (trade 1)
- `OnChainTradeEvent::Filled` (trade 1)
- `PositionEvent::OnChainOrderFilled` (trade 2)
- `OnChainTradeEvent::Filled` (trade 2)
- `PositionEvent::OffChainOrderPlaced` (threshold crossed)
- `OffchainOrderEvent::Placed`
- `OffchainOrderEvent::Submitted` (broker accepted, atomic with Placed)
- `OffchainOrderEvent::Filled` (broker filled)
- `PositionEvent::OffChainOrderFilled` (position hedged, net=0)

### 2. Position Checker Recovers After Failed Broker Execution

**What it proves**: When a broker order fails (e.g. Alpaca/Schwab API rejects or
reports failure), the order poller handles the failure, and the periodic
position checker detects the still-unhedged position and retries successfully.

**Why not test a crash**: After `process_queued_trade()` crosses the threshold,
it atomically sets `pending_offchain_order_id` on the Position aggregate. The
position checker queries for positions where `pending_offchain_order_id` is None
-- so it won't find a position with an existing pending order. The realistic way
to reach the "unexecuted position" state is: submit -> broker reports failure ->
poller clears pending order -> position checker picks it up.

**What's real**: Same as Test 1 plus
`check_and_execute_accumulated_positions()`,
`MockExecutor::with_order_status(Failed)`.

**Flow**:

1. Call `process_queued_trade()` x2 -- crosses threshold, returns
   `Some(OffchainOrderId)`. PlaceOrder command atomically emits Placed +
   Submitted.
2. Create `OrderStatusPoller` with
   `MockExecutor::new().with_order_status(Failed)` -- `poll_pending_orders()`
   discovers the failure, emits `OffchainOrderEvent::Failed` and
   `PositionEvent::OffChainOrderFailed` (clears pending_offchain_order_id)
3. **Checkpoint**: Assert `pending_offchain_order_id` is None on Position, net
   still -1.2 (unhedged). Assert failure events emitted.
4. Call `check_and_execute_accumulated_positions()` -- finds the position (net
   past threshold, no pending offchain order), creates new offchain order,
   places via executor
5. Create new `OrderStatusPoller` with default MockExecutor (returns Filled) --
   `poll_pending_orders()` completes the retry lifecycle
6. **Final checkpoint**: Position net=0, pending_offchain_order_id=None. Events
   table shows the complete failure-recovery sequence.

### 3. Multi-Symbol Isolation

**What it proves**: Two symbols processed concurrently don't contaminate each
other's state.

**What's real**: Same as Test 1, but for two symbols (AAPL, MSFT).

**Flow**:

1. Call `process_queued_trade()` with interleaved trades: tAAPL sell 0.6 shares,
   tMSFT sell 0.4 shares, tAAPL sell 0.6 shares
2. Assert: AAPL's second call returns `Some(OffchainOrderId)` (1.2 shares
   crosses threshold), MSFT returns `None` (0.4 below threshold)
3. Assert: Only AAPL has `OffChainOrderPlaced` / `Placed` / `Submitted` events
4. Call `OrderStatusPoller::poll_pending_orders()` for AAPL
5. Call `process_queued_trade()` with more tMSFT trades to cross threshold
6. Assert: MSFT gets its own independent `OffChainOrderPlaced` / `Placed` /
   `Submitted` events
7. Call `OrderStatusPoller::poll_pending_orders()` for MSFT
8. Assert: Both Position aggregates independently hedged (net=0), no
   cross-contamination in events. Verify via `load_position()` for each symbol

### 4. Trigger -> Rebalancer -> Mint Manager -> Aggregate

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

### 5. Trigger -> Rebalancer -> USDC Manager -> Aggregate

**What it proves**: Same as Test 4 but for USDC rebalancing (AlpacaToBase
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
2. **Position checker recovery** (#2) -- Proves the safety net works. Low-Medium
   effort (reuses Test 1 setup).
3. **Multi-symbol isolation** (#3) -- Validates no cross-contamination. Medium
   effort.
4. **Trigger -> Mint** (#4) -- Highest-value rebalancing flow. Medium effort.
5. **Trigger -> USDC Manager** (#5) -- Most complex rebalancing flow.
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
