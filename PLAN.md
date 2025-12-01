# Implementation Plan: Dual-Write in Conductor (Issue #130)

## Overview

This plan implements dual-write functionality in the Conductor to write state
changes to both legacy CRUD tables and the CQRS event store during Phase 1
validation. The system already has substantial CQRS infrastructure (aggregates,
events, views, migration tools), making this primarily an integration task
rather than building from scratch.

## Current State Analysis

### Existing CQRS Infrastructure

**Already Implemented:**

- Event store tables: `events`, `snapshots`
- View tables: `onchain_trade_view`, `offchain_order_view`, `position_view`
- Three aggregates with full command/event patterns:
  - `Position` (aggregate_id = symbol)
  - `OffchainOrder` (aggregate_id = execution_id)
  - `OnchainTrade` (aggregate_id = `{tx_hash}:{log_index}`)
- Migration tool (`migrate_to_events`) to convert legacy data to events
- View materialization logic

**Legacy Tables (Current Source of Truth):**

- `onchain_trades` - Immutable blockchain trade records
- `trade_accumulators` - Position tracking per symbol
- `offchain_trades` - Broker execution records
- `trade_execution_links` - Audit trail
- `event_queue` - Idempotency control
- `symbol_locks` - Concurrency control

### Critical Write Paths in Conductor

The Conductor orchestrates 6 concurrent tasks, but the primary write path is the
**queue_processor**:

**Main Flow:** `src/conductor/mod.rs:406-452`

```
get_next_unprocessed_event()
  → convert_event_to_trade()
  → process_onchain_trade() [CRITICAL DUAL-WRITE POINT]
    → OnchainTrade::save_within_transaction()
    → accumulator::process_onchain_trade()
      → Writes to: onchain_trades, trade_accumulators,
                    offchain_trades, trade_execution_links
  → mark_event_processed()
  → execute_pending_offchain_execution()
```

**Accumulator Logic:** `src/onchain/accumulator.rs:26-123`

- Single transaction containing all state changes
- Handles duplicate detection, position accumulation, execution creation,
  linkages

**Order Status Updates:** Order poller updates `offchain_trades` status (PENDING
→ SUBMITTED → FILLED/FAILED)

**Stale Execution Cleanup:** Periodic cleanup marks failed executions

## Design Decisions

### 1. Integration Strategy

**Approach:** Wrap existing accumulator logic with dual-write layer

- Keep legacy transaction logic intact (single source of truth for reads)
- Add event emission after successful legacy writes
- Use try-catch pattern: log errors in event processing but don't block
  operation

### 2. Transaction Boundary Strategy

**Critical Constraint:** All legacy writes happen in a single SQLite transaction
for atomicity.

**Problem:** CQRS framework expects to manage its own transactions via
`CqrsFramework::execute()`.

**Solution Options:**

**Option A: Two-Phase Write (RECOMMENDED)**

1. Complete legacy transaction fully
2. After commit, emit events through CQRS framework
3. Log errors in event emission but don't roll back legacy writes

**Pros:**

- Minimal changes to critical accumulator logic
- Preserves legacy transaction atomicity
- Safe: worst case is events don't get written, but legacy data is consistent
- Easy to debug: clear separation between legacy and event operations

**Cons:**

- Small window where legacy is updated but events aren't yet persisted
- Requires idempotency on event side (checking for duplicate events)

**Option B: Manual Event Table Writes**

- Manually INSERT into `events` table within legacy transaction
- Bypass CQRS framework's transaction management

**Cons:**

- Bypasses framework validation and aggregate loading
- Skips view updates
- More error-prone
- Not recommended by cqrs-es maintainers

**Decision:** Use Option A (Two-Phase Write) for safety and maintainability.

### 3. Error Handling Philosophy

**Principle:** Dual-write is additive validation; failures in event store MUST
NOT break production.

**Implementation:**

- Legacy writes proceed normally
- Event emission wrapped in `Result` with comprehensive error logging
- Use `tracing::error!` for event failures with full context
- Monitor event failure rates via metrics (future task)
- NO panics, NO transaction rollbacks on event failures

### 4. Aggregate Mapping

**OnchainTrade Aggregate:**

- Aggregate ID: `{tx_hash}:{log_index}`
- Single event: `OnChainTradeEvent::Filled`
- Triggered by: `OnchainTrade::save_within_transaction()`

**Position Aggregate:**

- Aggregate ID: `{symbol}` (e.g., "AAPL")
- Events during dual-write:
  - `PositionEvent::OnChainOrderFilled` - When onchain trade processed
  - `PositionEvent::OffChainOrderPlaced` - When execution created
  - `PositionEvent::OffChainOrderFilled` - When execution completed
  - `PositionEvent::OffChainOrderFailed` - When execution failed
- Triggered by: `accumulator::process_onchain_trade()`, order poller, cleanup
  tasks

**OffchainOrder Aggregate:**

- Aggregate ID: `{execution_id}` (from offchain_trades.id)
- Events during dual-write:
  - `OffchainOrderEvent::Placed` - When execution created
  - `OffchainOrderEvent::Submitted` - When broker accepts order
  - `OffchainOrderEvent::Filled` - When order completes
  - `OffchainOrderEvent::Failed` - When order fails
- Triggered by: `accumulator::process_onchain_trade()`, order poller

## Task 1. Setup CQRS Framework Infrastructure

Initialize CQRS framework properly with SqliteEventRepository and CqrsFramework
instances.

**CRITICAL ARCHITECTURE FIX:** The existing migration code and initial
dual-write implementation incorrectly write directly to the `events` table. This
violates CQRS architecture. All event emission MUST go through
`CqrsFramework::execute()` or `execute_with_metadata()`.

**Completed Work:**

- [x] Deleted incorrect `src/dual_write/onchain_trade.rs` implementation
- [x] Created proper `DualWriteContext` in `src/dual_write/mod.rs`:
  - [x] Uses `sqlite_es::SqliteCqrs` type alias (CqrsFramework backed by SQLite)
  - [x] Three framework fields: `onchain_trade`, `position`, `offchain_order`
  - [x] Constructor uses `sqlite_cqrs()` helper to initialize each framework
  - [x] Accessor methods for each framework
- [x] Created minimal `DualWriteError` enum with only basic conversion errors
  - [x] Database, Serialization, IntConversion, DecimalConversion (all using
        `#[from]`)
  - [x] Will discover additional error variants as needed during implementation
- [x] Reverted changes to `OnchainTrade::save_within_transaction()` signature
- [x] Reverted all 45+ call sites (removed `None` parameter)
- [x] Added unit test verifying framework initialization
- [x] All 386 tests pass
- [x] Clippy passes (no warnings)
- [x] Code formatted

**Implementation Notes:**

- Used `sqlite_es::sqlite_cqrs()` helper function for clean initialization
- Each aggregate gets its own SqliteCqrs instance backed by
  SqliteEventRepository
- Framework accessor methods provide immutable references to frameworks
- Services parameter is `()` for all three aggregates (Position and
  OffchainOrder don't require services)
- Error enum starts minimal - will add variants with `#[from]` as we discover
  actual errors during implementation

## Task 2. Fix Migration Script - Position

Fix `src/migration/position.rs` to use CqrsFramework.

**Completed Work:**

- [x] Added `PositionCommand::Migrate` variant to `src/position/cmd.rs`
- [x] Implemented handler for `Migrate` command in Position aggregate
- [x] Updated `migrate_positions()` to accept `SqliteCqrs<Position>` parameter
- [x] Removed `persist_event()` function (direct INSERT into events table)
- [x] Refactored to use `cqrs.execute(&aggregate_id, command).await`
- [x] Updated migration runner in `src/migration/mod.rs` to create and pass
      Position framework
- [x] Updated all 6 position migration tests to use framework
- [x] Added `PositionAggregate` error variant to `MigrationError` with `#[from]`
- [x] Refactored Position aggregate's `handle()` function to meet clippy line
      limits:
  - [x] Extracted `handle_place_offchain_order()` helper method
  - [x] Extracted `handle_complete_offchain_order()` helper method
  - [x] Extracted `handle_fail_offchain_order()` helper method
- [x] Added comprehensive tests for new `Migrate` command (5 new tests):
  - [x] `test_migrate_command_creates_migrated_event` - Verifies command creates
        correct event with all fields
  - [x] `test_migrated_event_sets_position_state` - Verifies all event fields
        match command inputs
  - [x] `test_migrate_with_zero_position` - Tests migration of empty positions
  - [x] `test_migrate_preserves_negative_position` - Tests short positions are
        preserved correctly
  - [x] `test_operations_after_migrate` - Verifies normal operations work after
        migration
- [x] All 59 position tests pass (17 existing + 5 new migration tests + 37 other
      tests)
- [x] All 27 migration tests pass
- [x] Clippy passes (no errors)
- [x] Code formatted

**Implementation Notes:**

- Position's `Migrate` command follows the same pattern as OnChainTrade's
  `Migrate` command
- CqrsFramework automatically handles event persistence, sequence numbers, and
  view updates
- Helper methods extracted from `handle()` keep each function under 100 lines
  (clippy requirement)
- Error conversion through `#[from]` preserves full type information in error
  chain

**Completion Criteria:**

- [x] Tests pass
- [x] No direct events table writes

## Task 3. Fix Migration Script - OffchainOrder

Fix `src/migration/offchain_order.rs` to use CqrsFramework.

**Completed Work:**

- [x] Added `OffchainOrderCommand::Migrate` variant to
      `src/offchain_order/cmd.rs`
- [x] Implemented handler for `Migrate` command in OffchainOrder aggregate
- [x] Updated `migrate_offchain_orders()` to accept `SqliteCqrs<OffchainOrder>`
      parameter
- [x] Removed `persist_event()` function (direct INSERT into events table)
- [x] Refactored to use `cqrs.execute(&aggregate_id, command).await`
- [x] Updated migration runner in `src/migration/mod.rs` to create and pass
      OffchainOrder framework
- [x] Updated all 4 offchain order migration tests to use framework
- [x] Added `OffchainOrderAggregate` error variant to `MigrationError` with
      `#[from]`
- [x] Added comprehensive tests for new `Migrate` command (3 new tests):
  - [x] `test_migrate_command_creates_migrated_event` - Verifies command creates
        correct event with all fields
  - [x] `test_migrate_command_all_status_types` - Tests all 4 status types
        (Pending, Submitted, Filled, Failed)
  - [x] `test_operations_after_migrate` - Verifies normal operations
        (ConfirmSubmission) work after migration
- [x] All 44 offchain order tests pass (41 existing + 3 new migration tests)
- [x] All 27 migration tests pass
- [x] Clippy passes (no errors, only expected dead code warnings for dual_write)
- [x] Code formatted

**Implementation Notes:**

- OffchainOrder's `Migrate` command follows the same pattern as OnChainTrade and
  Position
- The Migrate command handles all 4 status types through the
  `MigratedOrderStatus` enum
- CqrsFramework automatically handles event persistence, sequence numbers, and
  view updates
- Error conversion through `#[from]` preserves full type information in error
  chain

**Completion Criteria:**

- [x] Tests pass
- [x] No direct events table writes

## Task 4. Fix Migration Script - OnchainTrade

Fix `src/migration/onchain_trade.rs` to use CqrsFramework instead of direct
INSERT.

**Status:** COMPLETE ✓

**Completed Work:**

- [x] Verified OnchainTrade migration already uses CqrsFramework properly
- [x] Uses `OnChainTradeCommand::Migrate` with all required fields
- [x] Calls `cqrs.execute(&aggregate_id, command).await` (line 64)
- [x] No direct INSERT INTO events statements
- [x] Properly integrated in migration runner

**Implementation Notes:**

- OnchainTrade migration was already correctly refactored to use CqrsFramework
- Uses the Migrate command variant which preserves all trade metadata
- CqrsFramework automatically handles event persistence, sequence numbers, and
  view updates

**Completion Criteria:**

- [x] No direct INSERT INTO events statements in onchain_trade.rs
- [x] Uses CqrsFramework.execute() for all event emission

## Task 5. Fix Migration Script - SchwabAuth

Fix `src/migration/schwab_auth.rs` to use CqrsFramework and preserve token fetch
timestamps.

**Status:** COMPLETE ✓

**Critical Issue Discovered:**

Initial approach of using `StoreTokens` command would have corrupted data:

- `StoreTokens` always sets `fetched_at` to `Utc::now()`
- Would make expired tokens appear fresh
- Would break authentication (expired refresh token would appear valid for
  another 7 days)

**Completed Work:**

- [x] Added `SchwabAuthCommand::Migrate` variant to
      `crates/broker/src/schwab/auth/cmd.rs`
  - [x] Accepts `EncryptedToken` for both access and refresh tokens
  - [x] Accepts `DateTime<Utc>` for both fetch timestamps
  - [x] Preserves original timestamps instead of using current time
- [x] Implemented `Migrate` command handler in SchwabAuth aggregate
  - [x] Directly emits `TokensStored` event with provided timestamps
  - [x] No encryption needed (tokens already encrypted in database)
- [x] Updated `migrate_schwab_auth()` in `src/migration/schwab_auth.rs`:
  - [x] Accepts `SqliteCqrs<SchwabAuth>` parameter
  - [x] Removed direct INSERT INTO events (persist_event function)
  - [x] Reads encrypted tokens AND timestamps from schwab_auth table
  - [x] Uses `Migrate` command with original encrypted tokens and timestamps
  - [x] Calls `cqrs.execute()` instead of direct SQL
- [x] Updated migration runner in `src/migration/mod.rs`:
  - [x] Creates `schwab_auth_cqrs` with encryption_key as services parameter
  - [x] Passes framework and encryption_key to `migrate_schwab_auth()`
- [x] Added `encryption_key` field to `MigrationEnv` struct
- [x] Updated all 4 migration integration tests with encryption_key
- [x] Updated 3 schwab_auth migration tests to use CqrsFramework
- [x] Added `SchwabAuthAggregate` error variant to `MigrationError`
- [x] Added `EncryptionError` variant to `MigrationError`
- [x] Exported required types from broker crate:
  - [x] `AccessToken`, `RefreshToken` newtypes
  - [x] `SchwabAuthCommand`, `SchwabAuthError`
  - [x] `EncryptionError`, `EncryptionKey`, `decrypt_token`
  - [x] Made encryption functions public (was pub(crate))
- [x] Build passes with only expected dead_code warnings

**Implementation Notes:**

- Migration preserves exact token fetch timestamps from legacy database
- No decrypt/re-encrypt cycle - tokens remain encrypted as-is
- Auth expiry logic works correctly after migration
- Used separate `Migrate` command rather than repurposing `StoreTokens` to
  maintain clear semantics

**Completion Criteria:**

- [x] Tests pass
- [x] No direct events table writes
- [x] Token fetch timestamps preserved correctly

## Task 6. Verify No Direct Events Table Writes

Audit entire codebase to ensure nothing writes directly to events table.

**Status:** COMPLETE ✓

**Completed Work:**

- [x] Ran `grep -r "INSERT INTO events" src/` - only found test fixtures (3
      occurrences in `src/migration/mod.rs` test code)
- [x] Verified all migration scripts use CqrsFramework.execute()
- [x] Fixed compilation errors in schwab_auth test imports
- [x] Ran full test suite: 394 tests pass
- [x] Ran clippy: passes with only expected dead_code warnings for
      DualWriteContext (will be used in Tasks 7-12)

**Implementation Notes:**

- All direct `INSERT INTO events` statements are in test fixture code only
- All migration scripts properly use CqrsFramework for event emission:
  - OnchainTrade migration: uses `OnChainTradeCommand::Migrate`
  - Position migration: uses `PositionCommand::Migrate`
  - OffchainOrder migration: uses `OffchainOrderCommand::Migrate`
  - SchwabAuth migration: uses `SchwabAuthCommand::Migrate`
- No application code writes directly to events table
- CQRS architecture is properly maintained throughout the codebase

**Completion Criteria:**

- [x] Zero direct writes to events table in application code
- [x] All tests pass
- [x] Clippy passes

## Task 7. Implement OnchainTrade Dual-Write (Correct Implementation)

Now implement dual-write properly using CqrsFramework.

**Status:** COMPLETE ✓

**Completed Work:**

- [x] Created `src/dual_write/onchain_trade.rs`
- [x] Implemented `witness_trade()` (executes `Witness` command):
  - [x] Parameters: `context: &DualWriteContext`, `trade: &OnchainTrade`,
        `block_number: u64`
  - [x] Converts legacy OnchainTrade fields to command types:
    - [x] `symbol: TokenizedEquitySymbol` → `Symbol` (via `.base().clone()`)
    - [x] `amount: f64` → `Decimal` (via `Decimal::try_from()`)
    - [x] `price_usdc: f64` → `Decimal` (via `Decimal::try_from()`)
    - [x] `block_timestamp: Option<DateTime<Utc>>` → `DateTime<Utc>` (errors if
          None)
  - [x] Builds aggregate_id:
        `OnChainTrade::aggregate_id(trade.tx_hash,
        trade.log_index.try_into()?)`
  - [x] Creates `OnChainTradeCommand::Witness` with converted data
  - [x] Executes via
        `context.onchain_trade_framework().execute(&aggregate_id,
        command).await`
  - [x] Returns `Result<(), DualWriteError>`
- [x] Added `MissingBlockTimestamp` error variant to `DualWriteError`
- [x] Added aggregate error variants to `DualWriteError`:
  - [x] `OnChainTradeAggregate(#[from] AggregateError<OnChainTradeError>)`
  - [x] `PositionAggregate(#[from] AggregateError<PositionError>)`
  - [x] `OffchainOrderAggregate(#[from] AggregateError<OffchainOrderError>)`
- [x] Added comprehensive integration tests:
  - [x] `test_witness_trade_success` - Verifies command execution succeeds and
        event appears in events table
  - [x] `test_witness_trade_sequence_increments` - Verifies sequence numbers are
        correct
  - [x] `test_witness_trade_missing_block_timestamp` - Verifies proper error
        handling for missing block timestamp
- [x] All 404 tests pass (3 new dual_write tests for OnchainTrade)
- [x] Clippy passes (only expected dead_code warnings for functions not yet
      integrated)
- [x] Code formatted

**Implementation Notes:**

- Function name `witness_trade()` reflects DDD command semantics (executing
  `Witness` command, not emitting events)
- Used `block_number` parameter from `QueuedEvent` since legacy `OnchainTrade`
  doesn't have this field
- `OnchainTrade::Witness` command properly used (not `Migrate`)
- Decimal conversions use `try_from()` for proper error handling
- Block timestamp is required - errors with `MissingBlockTimestamp` if not
  present
- Framework automatically handles all event persistence, sequence numbers, and
  view updates
- Tests verify event appears with correct aggregate_id ("tx_hash:log_index") and
  event_type ("OnChainTradeEvent::Filled")

**Completion Criteria:**

- [x] Tests pass
- [x] Uses CqrsFramework::execute()
- [x] No direct events table access

## Task 8. Implement Position Dual-Write

Implement dual-write for Position aggregate using CqrsFramework.

**Status:** COMPLETE ✓

**Completed Work:**

- [x] Created `src/dual_write/position.rs`
- [x] Implemented `acknowledge_onchain_fill()` (executes
      `AcknowledgeOnChainFill` command):
  - [x] Parameters: `context: &DualWriteContext`, `trade: &OnchainTrade`
  - [x] Converts legacy types to command types (f64 → Decimal)
  - [x] Builds aggregate_id: `Position::aggregate_id(&symbol)`
  - [x] Creates `PositionCommand::AcknowledgeOnChainFill` with TradeId, amount,
        direction, price, timestamp
  - [x] Executes via
        `context.position_framework().execute(&aggregate_id,
        command).await`
- [x] Implemented `place_offchain_order()` (executes `PlaceOffChainOrder`
      command):
  - [x] Parameters: `context: &DualWriteContext`,
        `execution:
        &OffchainExecution`, `symbol: &Symbol`
  - [x] Builds `PositionCommand::PlaceOffChainOrder` with ExecutionId, shares,
        direction, broker
  - [x] Executes via framework
- [x] Implemented `complete_offchain_order()` (executes `CompleteOffChainOrder`
      command):
  - [x] Parameters: `context: &DualWriteContext`,
        `execution:
        &OffchainExecution`, `symbol: &Symbol`
  - [x] Extracts filled state data (order_id, price_cents, executed_at)
  - [x] Builds `PositionCommand::CompleteOffChainOrder` with all required fields
  - [x] Validates execution state (must be OrderState::Filled)
  - [x] Executes via framework
- [x] Implemented `fail_offchain_order()` (executes `FailOffChainOrder`
      command):
  - [x] Parameters: `context: &DualWriteContext`, `execution_id: i64`,
        `symbol:
        &Symbol`, `error: String`
  - [x] Builds `PositionCommand::FailOffChainOrder` with ExecutionId and error
        message
  - [x] Executes via framework
- [x] Added error variants to `DualWriteError`:
  - [x] `MissingExecutionId` - When OffchainExecution has no id
  - [x] `InvalidOrderState` - When execution not in expected state
  - [x] `NegativePriceCents` - For price conversion errors
- [x] Exported `TradeId` from `src/position/mod.rs` (was private in event
      module)
- [x] Added comprehensive integration tests (7 new tests):
  - [x] `test_acknowledge_onchain_fill_success` - Verifies event persisted with
        correct aggregate_id and type
  - [x] `test_acknowledge_onchain_fill_missing_block_timestamp` - Error handling
        test
  - [x] `test_place_offchain_order_success` - Verifies PlaceOffChainOrder event
        persisted
  - [x] `test_complete_offchain_order_success` - Verifies CompleteOffChainOrder
        event persisted
  - [x] `test_fail_offchain_order_success` - Verifies FailOffChainOrder event
        persisted
  - [x] `test_complete_offchain_order_invalid_state` - Error when execution not
        in Filled state
  - [x] `test_place_offchain_order_missing_execution_id` - Error when execution
        id is None
- [x] All 404 tests pass (11 dual_write tests total: 3 OnchainTrade + 7
      Position + 1 context initialization)
- [x] Clippy passes (only expected dead_code warnings for functions not yet
      integrated)
- [x] Code formatted

**Implementation Notes:**

- Function names reflect DDD command semantics (e.g., `acknowledge_onchain_fill`
  executes `AcknowledgeOnChainFill` command, not "emitting" events)
- Position aggregate_id is the symbol string (e.g., "AAPL")
- All four Position lifecycle commands properly implemented
- Type conversions handle legacy broker types (Shares, OrderState) to CQRS types
  (FractionalShares, etc.)
- Framework automatically handles event persistence, sequence numbers, and view
  updates
- Tests verify correct event types appear with proper aggregate_id

**Completion Criteria:**

- [x] Tests pass
- [x] Uses CqrsFramework::execute()
- [x] No direct events table access

## Task 9. Implement OffchainOrder Dual-Write

Implement dual-write for OffchainOrder aggregate using CqrsFramework.

**Status:** COMPLETE ✓

**Completed Work:**

- [x] Created `src/dual_write/offchain_order.rs`
- [x] Implemented `place_order()` (executes `Place` command):
  - [x] Parameters: `context: &DualWriteContext`,
        `execution:
        &OffchainExecution`
  - [x] Converts legacy types to command types (Shares → Decimal)
  - [x] Builds aggregate_id: `OffchainOrder::aggregate_id(execution_id)`
  - [x] Creates `OffchainOrderCommand::Place` with symbol, shares, direction,
        broker
  - [x] Executes via
        `context.offchain_order_framework().execute(&execution_id,
        command).await`
- [x] Implemented `confirm_submission()` (executes `ConfirmSubmission` command):
  - [x] Parameters: `context: &DualWriteContext`, `execution_id: i64`,
        `broker_order_id: String`
  - [x] Creates `OffchainOrderCommand::ConfirmSubmission` with BrokerOrderId
  - [x] Executes via framework
- [x] Implemented `record_fill()` (executes `CompleteFill` command):
  - [x] Parameters: `context: &DualWriteContext`,
        `execution:
        &OffchainExecution`
  - [x] Extracts filled state data (price_cents)
  - [x] Creates `OffchainOrderCommand::CompleteFill` with price_cents
  - [x] Validates execution state (must be OrderState::Filled)
  - [x] Executes via framework
- [x] Implemented `mark_failed()` (executes `MarkFailed` command):
  - [x] Parameters: `context: &DualWriteContext`, `execution_id: i64`,
        `error:
        String`
  - [x] Creates `OffchainOrderCommand::MarkFailed` with error message
  - [x] Executes via framework
- [x] Added comprehensive integration tests (7 new tests):
  - [x] `test_place_order_success` - Verifies Place event persisted with correct
        aggregate_id
  - [x] `test_place_order_missing_execution_id` - Error when execution id is
        None
  - [x] `test_confirm_submission_success` - Verifies ConfirmSubmission event
        persisted
  - [x] `test_record_fill_success` - Verifies CompleteFill event persisted
  - [x] `test_record_fill_invalid_state` - Error when execution not in Filled
        state
  - [x] `test_mark_failed_success` - Verifies MarkFailed event persisted
  - [x] `test_sequence_increments` - Verifies sequence numbers increment
        correctly through lifecycle
- [x] All 411 tests pass (18 dual_write tests total: 3 OnchainTrade + 7 Position
      + 7 OffchainOrder + 1 context initialization)
- [x] Clippy passes (only expected dead_code warnings for functions not yet
      integrated)
- [x] Code formatted

**Implementation Notes:**

- Function names reflect DDD command semantics (e.g., `place_order` executes
  `Place` command, not "emitting" events)
- OffchainOrder aggregate_id is the execution_id formatted as string (e.g.,
  "123")
- All four OffchainOrder lifecycle commands properly implemented (Place,
  ConfirmSubmission, CompleteFill, MarkFailed)
- Type conversions handle legacy broker types (Shares, OrderState) to CQRS types
  (Decimal, PriceCents)
- Framework automatically handles event persistence, sequence numbers, and view
  updates
- Tests verify correct event types appear with proper aggregate_id and sequence
  progression

**Completion Criteria:**

- [x] Tests pass
- [x] Uses CqrsFramework::execute()
- [x] No direct events table access

## Task 10. Integrate Dual-Write into Queue Processor

Wire dual-write into the main trade processing flow.

**Status:** COMPLETE ✓

**Completed Work:**

- [x] Initialize `DualWriteContext` in
      `src/conductor/mod.rs::spawn_queue_processor()` (line 272)
- [x] Pass context through call chain:
  - [x] `spawn_queue_processor()` → `run_queue_processor()`
  - [x] `run_queue_processor()` → `process_next_queued_event()`
  - [x] `process_next_queued_event()` → `process_valid_trade()`
  - [x] `process_valid_trade()` → `process_trade_within_transaction()`
- [x] Execute dual-write commands in `process_trade_within_transaction()` (lines
      678-728):
  - [x] After legacy transaction commits successfully (line 665)
  - [x] Execute `witness_trade()` for OnChainTrade aggregate
  - [x] Execute `acknowledge_onchain_fill()` for Position aggregate
  - [x] If execution created:
    - [x] Execute `place_offchain_order()` for Position aggregate
    - [x] Execute `place_order()` for OffchainOrder aggregate
  - [x] Log all errors with full context (tx_hash, log_index, symbol,
        execution_id)
- [x] Export `DualWriteContext` from `src/dual_write/mod.rs` (already
      pub(crate))
- [x] Update test to pass `dual_write_context` parameter

**Implementation Notes:**

- Context created once per queue processor spawn (not per event)
- Commands never fail the overall transaction (legacy already committed)
- All command errors logged at ERROR level with structured fields
- Used `.clone()` on trade to make it available after move into accumulator
- No changes needed in accumulator - dual-write happens in conductor layer

**Completion Criteria:**

- [x] All 411 tests pass
- [x] Context properly passed through call chain
- [x] Commands executed after legacy writes

## Task 11. Integrate Dual-Write into Order Poller

Execute commands when order status changes.

**Status:** COMPLETE ✓

**Completed Work:**

- [x] Pass `DualWriteContext` to order poller task
  - [x] Added `dual_write_context` field to `OrderStatusPoller` struct
  - [x] Updated constructor to accept `DualWriteContext` parameter
  - [x] Created `DualWriteContext` in `spawn_order_poller()` function (line 214)
- [x] After each status UPDATE:
  - [x] Call appropriate command functions in `handle_filled_order()` (lines
        205-220):
    - [x] `record_fill()` - executes `OffchainOrder::CompleteFill` command
    - [x] `complete_offchain_order()` - executes
          `Position::CompleteOffChainOrder` command
  - [x] Call appropriate command functions in `handle_failed_order()` (lines
        265-285):
    - [x] `mark_failed()` - executes `OffchainOrder::MarkFailed` command
    - [x] `fail_offchain_order()` - executes `Position::FailOffChainOrder`
          command
  - [x] Handle errors with logging
    - [x] All command errors wrapped in error handlers
    - [x] Errors logged at ERROR level with full context (execution_id, symbol,
          error)
    - [x] Errors never block operation (legacy transaction already committed)
- [x] Add tests for status transitions
  - [x] `test_handle_filled_order_executes_dual_write_commands` - Verifies
        filled order dual-write (3 events: Placed, Submitted, Filled)
  - [x] `test_handle_failed_order_executes_dual_write_commands` - Verifies
        failed order dual-write (3 events: Placed, Submitted, Failed)

**Implementation Notes:**

- Dual-write commands execute after legacy transaction commits (two-phase write
  pattern)
- Commands never fail the overall operation - errors only logged
- Tests verify complete event lifecycle with proper sequence numbers
- Used `confirm_submission()` in tests to properly set up order state before
  status transitions

**Completion Criteria:**

- [x] All 413 tests pass (2 new tests added)
- [x] Commands executed on status changes
- [x] Clippy passes (only expected warnings for functions used in Task 12)

## Task 12. Integrate Dual-Write into Stale Execution Cleanup

Execute failure commands when cleaning up stale executions.

**Status:** COMPLETE ✓

**Completed Work:**

- [x] Modified stale execution cleanup flow to support dual-write
  - [x] Updated `clean_up_stale_executions()` return type to
        `Result<Vec<(i64, Symbol, String)>, OnChainError>` (line 66)
  - [x] Returns list of `(execution_id, symbol, error_reason)` tuples for
        cleaned up executions
  - [x] Updated `process_onchain_trade()` return type to
        `Result<(Option<OffchainExecution>, Vec<(i64, Symbol, String)>), OnChainError>`
        (line 26)
  - [x] Propagates cleanup information alongside execution result
- [x] Updated conductor to execute dual-write commands for stale executions
  - [x] Modified `process_trade_within_transaction()` to receive
        `cleaned_up_executions` from accumulator (line 665)
  - [x] Added loop to process each cleaned up execution (lines 747-772):
    - [x] Calls `mark_failed()` - executes `OffchainOrder::MarkFailed` command
    - [x] Calls `fail_offchain_order()` - executes `Position::FailOffChainOrder`
          command
  - [x] All errors logged at ERROR level with full context (execution_id,
        symbol, error)
  - [x] Errors never block operation (legacy transaction already committed)
- [x] Updated all call sites to handle new return type:
  - [x] `src/cli.rs` - destructures tuple `(execution, _cleaned_up)` (line 88)
  - [x] `src/conductor/mod.rs` test - destructures tuple (line 825)
  - [x] `src/onchain/accumulator.rs` test helper - destructures tuple (line 764)
- [x] Added comprehensive integration test:
  - [x] `test_stale_execution_cleanup_executes_dual_write_commands` - Verifies
        complete dual-write lifecycle:
    - [x] Sets up Position aggregate with onchain fill (to meet threshold)
    - [x] Creates stale execution in legacy DB
    - [x] Executes `place_order()` and `place_offchain_order()` to set up
          dual-write state
    - [x] Executes `confirm_submission()` to transition to Submitted state
    - [x] Processes trade that triggers cleanup (returns
          `cleaned_up_executions`)
    - [x] Executes dual-write commands for cleanup
    - [x] Verifies 3 OffchainOrder events (Placed, Submitted, Failed)
    - [x] Verifies Position OffChainOrderFailed event was created

**Implementation Notes:**

- Dual-write commands execute after legacy transaction commits (two-phase write
  pattern)
- Cleanup information flows from accumulator → conductor → dual-write layer
- Commands never fail the overall operation - errors only logged
- Test verifies complete event lifecycle including stale execution cleanup
  scenario
- Return type changes maintain backward compatibility (callers can ignore
  cleanup info with `_`)

**Completion Criteria:**

- [x] All 414 tests pass (1 new test added for stale execution cleanup
      dual-write)
- [x] Commands executed on cleanup
- [x] Clippy passes (no warnings)
- [x] Code formatted

## Success Criteria

**Must Have:**

- [x] All onchain trades execute `Witness` command generating
      `OnChainTradeEvent::Filled` events
- [x] All position changes execute appropriate commands generating
      `PositionEvent` events
- [x] All offchain orders execute lifecycle commands generating
      `OffchainOrderEvent` events
- [x] Legacy tables remain source of truth for reads (no code changes to read
      path)
- [x] Command execution failures logged but don't block trade processing
- [x] Integration tests pass for full trade lifecycle

**Nice to Have:**

- [x] View tables automatically updated from events (works via existing
      PostgresViewRepository in cqrs-es framework)
- [ ] Reconciliation tool to detect divergence between legacy and events (future
      enhancement)

## Risks and Mitigations

**Risk 1: Event emission slows down trade processing**

- Mitigation: Make event emission non-blocking (log errors, don't wait)
- Mitigation: Add timeout to event emission operations if needed

**Risk 2: Event store fills up faster than expected**

- Mitigation: Monitor disk space and event table growth
- Mitigation: Document event retention policy
- Mitigation: Implement event archival if needed

**Risk 3: Events diverge from legacy tables**

- Mitigation: Add reconciliation job comparing counts
- Mitigation: Add alerts for divergence thresholds
- Mitigation: Document recovery procedures

**Risk 4: CQRS framework transaction conflicts**

- Mitigation: Use two-phase write strategy (legacy first, then events)
- Mitigation: Make events idempotent (CQRS handles via sequence numbers)

**Risk 5: Bugs in event emission break production**

- Mitigation: Comprehensive error handling (never panic on event failure)
- Mitigation: Extensive testing including failure injection
- Mitigation: Rollback deployment if dual-write causes issues

## Next Phase Preview (Phase 2 - Not in Scope)

After dual-write validation period succeeds:

1. **Switch Reads to Views:** Update query logic to read from `*_view` tables
   instead of legacy tables
2. **Remove Legacy Writes:** Once views proven reliable, remove legacy
   INSERT/UPDATE logic
3. **Event Store as Primary:** Migrate to event store as single source of truth
4. **Schema Cleanup:** Deprecate and eventually drop legacy tables

This plan focuses solely on Phase 1: dual-write validation.
