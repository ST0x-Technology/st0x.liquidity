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

Integration tests exercise the **subprocesses** that the Conductor spawns,
calling their inner functions directly with real internal components and only
external dependencies mocked:

- **Real**: DualWriteContext, CQRS frameworks (SqliteCqrs or MemStore),
  accumulator, threshold check, order poller, trigger, rebalancer dispatch,
  managers
- **Mocked**: MockExecutor (broker API), httpmock (Alpaca tokenization/wallet
  API)
- **Constructed directly**: `OnchainTrade` structs (bypasses `try_from_clear_v3`
  which needs RPC to fetch the companion `AfterClearV2` event and resolve token
  symbols -- that function has 31 tests of its own)
- **Database**: In-memory SQLite (`:memory:`) with real migrations

The arbitrage integration tests call the same functions the Conductor's
subprocesses call internally. For example, `process_onchain_trade()` is what
`spawn_queue_processor` calls after parsing an event, and
`check_and_execute_accumulated_positions()` is what
`spawn_periodic_accumulated_position_check` calls every 60 seconds. By calling
these directly, the test exercises all internal wiring (DualWrite, CQRS
commands, threshold checks, executor calls) without needing to start infinite
loops or manage timing.

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
   amounts), calls `process_onchain_trade()` to accumulate and check threshold,
   and if threshold is crossed, calls `execute_pending_offchain_execution()` to
   place the hedge order on the broker
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

---

## Recommended Integration Tests

### 1. Full Arbitrage Pipeline (Happy Path)

**What it proves**: The core money-making flow works end-to-end -- onchain trade
-> accumulate -> threshold -> hedge on broker -> poll -> fill -> position
reconciled.

**What's real**: DualWriteContext, accumulator, threshold check, order
placement, order poller, all CQRS aggregates (OnChainTrade, Position,
OffchainOrder), legacy tables.

**What's mocked**: MockExecutor (returns `TEST_1` order ID, then `Filled`
status).

**What's skipped**: Blockchain event parsing (`try_from_clear_v3`), WebSocket
listener, event queue enqueue/dequeue.

**Flow**:

1. Construct `OnchainTrade` directly (symbol=AAPL, amount=1.0, direction=Buy)
2. Call `process_onchain_trade()` inside a SQL transaction -- this internally
   witnesses the trade, initializes the position, accumulates shares, checks
   threshold, and if crossed, places the offchain order via DualWrite
3. Assert: `process_onchain_trade` returned an `OffchainExecution` (threshold
   was crossed). Position has `Initialized` + `OnChainOrderFilled` +
   `OffChainOrderPlaced` events. OffchainOrder has `Placed` event.
4. Call `execute_pending_offchain_execution()` with the returned execution +
   MockExecutor -- this calls the broker and confirms submission
5. Assert: OffchainOrder has `Submitted` event with order_id = `TEST_1`. Legacy
   `offchain_trades` row has status = SUBMITTED.
6. Call `poll_pending_orders()` -- this queries DB for SUBMITTED orders, calls
   MockExecutor.get_order_status (returns Filled), and internally runs
   handle_filled_order
7. Assert final state (see below)

**Final state assertions**:

- **Position aggregate events**: Initialized -> OnChainOrderFilled ->
  OffChainOrderPlaced -> OffChainOrderFilled
- **OffchainOrder aggregate events**: Placed -> Submitted -> Filled
- **OnChainTrade aggregate events**: Filled
- **Position view**: net_position approximately 0 (onchain buy hedged by
  offchain sell), no pending execution
- **Legacy tables**: offchain_trades status = FILLED, trade_accumulators
  pending_execution_id = NULL, no active symbol_lock for AAPL

### 2. Periodic Position Checker Catches Unexecuted Position

**What it proves**: If `process_onchain_trade` accumulates enough shares to
cross the threshold but the execution doesn't happen (e.g. the queue_processor
crashed between accumulating and executing), the periodic position checker picks
it up and executes it.

**What's real**: Same as Test 1 plus
`check_and_execute_accumulated_positions()`.

**Flow**:

1. Construct `OnchainTrade` (symbol=AAPL, amount=1.0, direction=Buy)
2. Call `process_onchain_trade()` -- threshold crossed, returns
   `OffchainExecution`
3. **Don't** call `execute_pending_offchain_execution()` -- simulate the
   queue_processor dying before it could execute
4. Call `check_and_execute_accumulated_positions()` -- this is what
   `spawn_periodic_accumulated_position_check` calls every 60s. It scans all
   symbols, finds AAPL has crossed threshold with no pending execution, places
   the order, and executes it via MockExecutor
5. Call `poll_pending_orders()` to complete the fill
6. Assert: same final state as Test 1 -- position fully hedged

### 3. Multi-Symbol Isolation

**What it proves**: Two symbols processed concurrently don't contaminate each
other's state.

**What's real**: Same as Test 1, but for two symbols (AAPL, MSFT).

**Flow**:

1. Process interleaved onchain fills: AAPL buy 0.6 shares, MSFT buy 0.4 shares,
   AAPL buy 0.6 shares
2. Assert: AAPL crosses threshold (1.2 shares), MSFT does not (0.4 shares)
3. Assert: Only AAPL gets an OffchainOrder placed
4. Execute + poll for AAPL
5. Process more MSFT trades to cross threshold
6. Assert: MSFT gets its own independent OffchainOrder
7. Execute + poll for MSFT
8. Assert: Both positions independently hedged, no cross-contamination in
   accumulators or symbol locks

### 4. Executor Failure and Recovery

**What it proves**: When the broker rejects an order, accumulated shares are
preserved and the system can retry.

**What's real**: Same as Test 1.

**What's mocked**: MockExecutor configured to fail the first
`place_market_order()` call, succeed the second.

**Flow**:

1. Process enough onchain trades to cross threshold for AAPL
2. Call `execute_pending_offchain_execution()` -- MockExecutor fails
3. Assert: OffchainOrder reaches Failed state, Position pending execution
   cleared, accumulated shares preserved in Position
4. Call `check_and_execute_accumulated_positions()` -- the periodic checker
   re-detects the threshold crossing and retries
5. This time MockExecutor succeeds
6. Poll and fill
7. Assert: New OffchainOrder succeeds, final Position state is correct

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

## Reference: Full Arbitrage Pipeline Component Detail

This section documents every internal step that `process_onchain_trade()`,
`execute_pending_offchain_execution()`, and `poll_pending_orders()` perform
internally. You don't call these steps directly in the test -- they happen
inside the function calls -- but this is what to expect when debugging.

### process_onchain_trade()

Internally calls (via DualWriteContext within a SQL transaction):

- `witness_trade()` -- inserts into `onchain_trades` legacy table + sends
  `Witness` command to OnChainTrade aggregate (emits
  `OnChainTradeEvent::Filled`)
- `initialize_position()` -- sends `Initialize` command to Position aggregate if
  first trade for this symbol (emits `PositionEvent::Initialized`)
- `acknowledge_onchain_fill()` -- updates `trade_accumulators` legacy table +
  sends `AcknowledgeOnChainFill` command to Position aggregate (emits
  `PositionEvent::OnChainOrderFilled`)
- Saves trade, creates execution link, checks for stale executions
- `Position.is_ready_for_execution()` -- loads Position from event store, checks
  `|net_position| >= threshold` (default 1.0 share)
- If threshold crossed: `place_offchain_order()` -- sends `PlaceOffChainOrder`
  command to Position aggregate (emits `PositionEvent::OffChainOrderPlaced`) +
  updates `trade_accumulators.pending_execution_id` + inserts symbol lock
- If threshold crossed: `place_order()` -- sends `Place` command to
  OffchainOrder aggregate (emits `OffchainOrderEvent::Placed`) + inserts into
  `offchain_trades` legacy table
- Returns `OffchainExecution` with execution details

### execute_pending_offchain_execution()

- Calls
  `MockExecutor.place_market_order(MarketOrder { symbol: "AAPL", shares: 1.0, direction: Sell })`
  -- returns `OrderPlacement { order_id: "TEST_1" }`
- `confirm_submission()` -- sends `ConfirmSubmission` command to OffchainOrder
  aggregate (emits `OffchainOrderEvent::Submitted`) + updates `offchain_trades`
  legacy table to status=SUBMITTED with order_id

### poll_pending_orders()

- Queries `offchain_trades WHERE status = 'SUBMITTED'`
- For each: calls `MockExecutor.get_order_status("TEST_1")` -- returns
  `OrderState::Filled { price_cents, executed_at }`
- `handle_filled_order()`:
  - Sends `CompleteFill` command to OffchainOrder aggregate (emits
    `OffchainOrderEvent::Filled`)
  - Sends `CompleteOffChainOrder` command to Position aggregate (emits
    `PositionEvent::OffChainOrderFilled`)
  - Updates `offchain_trades` to status=FILLED
  - Clears `trade_accumulators.pending_execution_id`
  - Deletes symbol lock

### check_and_execute_accumulated_positions()

- Queries `trade_accumulators WHERE pending_execution_id IS NULL` for all
  symbols
- For each symbol: loads Position from CQRS event store, calls
  `is_ready_for_execution()`
- If threshold crossed: calls `execute_new_execution_dual_write()` (which calls
  `place_offchain_order` + `place_order` via DualWrite), then spawns
  `execute_pending_offchain_execution()` to call the broker

---

## Future: Full E2E Harness

The tests above are integration tests -- they call subprocess functions directly
with constructed inputs. They don't start the Conductor or any of its 8
concurrent tasks.

A separate GitHub issue tracks a full E2E harness that starts the actual bot:
https://github.com/ST0x-Technology/st0x.liquidity/issues/264
