# Implementation Plan: Dual-Write in Conductor (Issue #130)

## Overview

This plan implements dual-write functionality in the Conductor to write state changes to both legacy CRUD tables and the CQRS event store during Phase 1 validation. The system already has substantial CQRS infrastructure (aggregates, events, views, migration tools), making this primarily an integration task rather than building from scratch.

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

The Conductor orchestrates 6 concurrent tasks, but the primary write path is the **queue_processor**:

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
- Handles duplicate detection, position accumulation, execution creation, linkages

**Order Status Updates:** Order poller updates `offchain_trades` status (PENDING → SUBMITTED → FILLED/FAILED)

**Stale Execution Cleanup:** Periodic cleanup marks failed executions

## Design Decisions

### 1. Integration Strategy

**Approach:** Wrap existing accumulator logic with dual-write layer

- Keep legacy transaction logic intact (single source of truth for reads)
- Add event emission after successful legacy writes
- Use try-catch pattern: log errors in event processing but don't block operation

### 2. Transaction Boundary Strategy

**Critical Constraint:** All legacy writes happen in a single SQLite transaction for atomicity.

**Problem:** CQRS framework expects to manage its own transactions via `CqrsFramework::execute()`.

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

**Principle:** Dual-write is additive validation; failures in event store MUST NOT break production.

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
- Triggered by: `accumulator::process_onchain_trade()`, order poller, cleanup tasks

**OffchainOrder Aggregate:**
- Aggregate ID: `{execution_id}` (from offchain_trades.id)
- Events during dual-write:
  - `OffchainOrderEvent::Placed` - When execution created
  - `OffchainOrderEvent::Submitted` - When broker accepts order
  - `OffchainOrderEvent::Filled` - When order completes
  - `OffchainOrderEvent::Failed` - When order fails
- Triggered by: `accumulator::process_onchain_trade()`, order poller

## Task 1. Setup CQRS Framework Infrastructure

Initialize CQRS framework properly with SqliteEventRepository and CqrsFramework instances.

**CRITICAL ARCHITECTURE FIX:** The existing migration code and initial dual-write implementation incorrectly write directly to the `events` table. This violates CQRS architecture. All event emission MUST go through `CqrsFramework::execute()` or `execute_with_metadata()`.

**Completed Work:**

- [x] Deleted incorrect `src/dual_write/onchain_trade.rs` implementation
- [x] Created proper `DualWriteContext` in `src/dual_write/mod.rs`:
  - [x] Uses `sqlite_es::SqliteCqrs` type alias (CqrsFramework backed by SQLite)
  - [x] Three framework fields: `onchain_trade`, `position`, `offchain_order`
  - [x] Constructor uses `sqlite_cqrs()` helper to initialize each framework
  - [x] Accessor methods for each framework
- [x] Created minimal `DualWriteError` enum with only basic conversion errors
  - [x] Database, Serialization, IntConversion, DecimalConversion (all using `#[from]`)
  - [x] Will discover additional error variants as needed during implementation
- [x] Reverted changes to `OnchainTrade::save_within_transaction()` signature
- [x] Reverted all 45+ call sites (removed `None` parameter)
- [x] Added unit test verifying framework initialization
- [x] All 386 tests pass
- [x] Clippy passes (no warnings)
- [x] Code formatted

**Implementation Notes:**
- Used `sqlite_es::sqlite_cqrs()` helper function for clean initialization
- Each aggregate gets its own SqliteCqrs instance backed by SqliteEventRepository
- Framework accessor methods provide immutable references to frameworks
- Services parameter is `()` for all three aggregates (Position and OffchainOrder don't require services)
- Error enum starts minimal - will add variants with `#[from]` as we discover actual errors during implementation

## Task 2. Fix Migration Script - OnchainTrade

Fix `src/migration/onchain_trade.rs` to use CqrsFramework instead of direct INSERT.

**Current Problem:**
- `persist_event()` at line 74-102 writes directly to events table
- Bypasses CqrsFramework entirely
- Manual sequence number management

**Subtasks:**
- [ ] Add `CqrsFramework<OnChainTrade>` parameter to `migrate_onchain_trades()`
- [ ] Remove `persist_event()` function entirely
- [ ] Create `OnChainTradeCommand::Migrate` command variant (or reuse existing command)
- [ ] For each legacy trade row:
  - [ ] Build appropriate command with trade data
  - [ ] Execute via `framework.execute(&aggregate_id, command).await`
  - [ ] Handle errors with proper logging
- [ ] Update `src/migration/mod.rs::run_migration()` to initialize framework and pass it
- [ ] Update all migration tests to use framework
- [ ] Remove test code that inserts directly into events table

**Design Notes:**
- May need to add `Migrate` command variant to OnChainTradeCommand enum
- Or reuse existing `Witness` command if semantics match
- CqrsFramework will handle sequence numbers automatically
- Each execute() call is atomic

**Completion Criteria:**
- [ ] `timeout 60 cargo test -q --lib migration` passes
- [ ] No direct INSERT INTO events statements in onchain_trade.rs
- [ ] Migrated events have correct sequence numbers

## Task 3. Fix Migration Script - Position

Fix `src/migration/position.rs` to use CqrsFramework.

**Completed Work:**

- [x] Added `PositionCommand::Migrate` variant to `src/position/cmd.rs`
- [x] Implemented handler for `Migrate` command in Position aggregate
- [x] Updated `migrate_positions()` to accept `SqliteCqrs<Position>` parameter
- [x] Removed `persist_event()` function (direct INSERT into events table)
- [x] Refactored to use `cqrs.execute(&aggregate_id, command).await`
- [x] Updated migration runner in `src/migration/mod.rs` to create and pass Position framework
- [x] Updated all 6 position migration tests to use framework
- [x] Added `PositionAggregate` error variant to `MigrationError` with `#[from]`
- [x] Refactored Position aggregate's `handle()` function to meet clippy line limits:
  - [x] Extracted `handle_place_offchain_order()` helper method
  - [x] Extracted `handle_complete_offchain_order()` helper method
  - [x] Extracted `handle_fail_offchain_order()` helper method
- [x] Added comprehensive tests for new `Migrate` command (5 new tests):
  - [x] `test_migrate_command_creates_migrated_event` - Verifies command creates correct event with all fields
  - [x] `test_migrated_event_sets_position_state` - Verifies all event fields match command inputs
  - [x] `test_migrate_with_zero_position` - Tests migration of empty positions
  - [x] `test_migrate_preserves_negative_position` - Tests short positions are preserved correctly
  - [x] `test_operations_after_migrate` - Verifies normal operations work after migration
- [x] All 59 position tests pass (17 existing + 5 new migration tests + 37 other tests)
- [x] All 27 migration tests pass
- [x] Clippy passes (no errors)
- [x] Code formatted

**Implementation Notes:**
- Position's `Migrate` command follows the same pattern as OnChainTrade's `Migrate` command
- CqrsFramework automatically handles event persistence, sequence numbers, and view updates
- Helper methods extracted from `handle()` keep each function under 100 lines (clippy requirement)
- Error conversion through `#[from]` preserves full type information in error chain

**Completion Criteria:**
- [x] Tests pass
- [x] No direct events table writes

## Task 4. Fix Migration Script - OffchainOrder

Fix `src/migration/offchain_order.rs` to use CqrsFramework.

**Subtasks:**
- [ ] Add `CqrsFramework<OffchainOrder>` parameter to `migrate_offchain_orders()`
- [ ] Remove direct events table INSERT
- [ ] Use appropriate OffchainOrder commands
- [ ] Update migration runner to pass framework
- [ ] Update tests

**Completion Criteria:**
- [ ] Tests pass
- [ ] No direct events table writes

## Task 5. Fix Migration Script - SchwabAuth

Fix `src/migration/schwab_auth.rs` to use CqrsFramework.

**Subtasks:**
- [ ] Add CqrsFramework parameter to `migrate_schwab_auth()`
- [ ] Remove direct events table INSERT
- [ ] Use appropriate auth commands
- [ ] Update migration runner to pass framework
- [ ] Update tests

**Completion Criteria:**
- [ ] Tests pass
- [ ] No direct events table writes

## Task 6. Verify No Direct Events Table Writes

Audit entire codebase to ensure nothing writes directly to events table.

**Subtasks:**
- [ ] Run `grep -r "INSERT INTO events" src/` - should return 0 results
- [ ] Run `grep -r "INSERT.*events.*aggregate_type" src/` - should return 0 results
- [ ] Search for any sqlx queries writing to events table
- [ ] Update AGENTS.md if additional guidance needed
- [ ] Run full test suite: `timeout 180 cargo test -q`
- [ ] Run clippy: `timeout 90 cargo clippy --all-targets -- -D warnings`

**Completion Criteria:**
- [ ] Zero direct writes to events table in application code
- [ ] All tests pass
- [ ] Clippy passes

## Task 7. Implement OnchainTrade Dual-Write (Correct Implementation)

Now implement dual-write properly using CqrsFramework.

**Subtasks:**
- [ ] Create NEW `src/dual_write/onchain_trade.rs`
- [ ] Implement `emit_trade_filled()`:
  - [ ] Parameters: `trade: &LegacyTrade`, `context: &DualWriteContext`
  - [ ] Build aggregate_id: `OnChainTrade::aggregate_id(trade.tx_hash, trade.log_index)`
  - [ ] Create `OnChainTradeCommand::Witness` with trade data
  - [ ] Execute: `context.onchain_trade_cqrs.execute(&aggregate_id, command).await`
  - [ ] Map errors to `DualWriteError`
  - [ ] Return `Result<(), DualWriteError>`
- [ ] Implement `log_event_error()` helper
- [ ] Add integration tests:
  - [ ] Test command execution succeeds
  - [ ] Test event appears in events table (via framework)
  - [ ] Test aggregate can be loaded from event store
  - [ ] Test sequence numbers increment correctly
- [ ] Modify `OnchainTrade::save_within_transaction()` to accept optional `DualWriteContext`
- [ ] Update call sites to pass `None` for now

**Design Notes:**
- CqrsFramework::execute() is async and returns Result
- Framework handles all event persistence, sequences, view updates
- We just build commands and call execute()
- Much simpler than manual event table writes

**Completion Criteria:**
- [ ] Tests pass
- [ ] Uses CqrsFramework::execute()
- [ ] No direct events table access

## Task 8. Implement Position Dual-Write

Implement dual-write for Position aggregate using CqrsFramework.

**Subtasks:**
- [ ] Create `src/dual_write/position.rs`
- [ ] Implement `emit_onchain_fill_acknowledged()`:
  - [ ] Parameters: trade data, `context: &DualWriteContext`
  - [ ] Build `PositionCommand::AcknowledgeOnChainFill` (or appropriate command)
  - [ ] Execute via `context.position_cqrs.execute(&symbol, command).await`
- [ ] Implement `emit_offchain_order_placed()`:
  - [ ] Build `PositionCommand::PlaceOffChainOrder`
  - [ ] Execute via framework
- [ ] Implement `emit_offchain_order_filled()`:
  - [ ] Build appropriate command
  - [ ] Execute via framework
- [ ] Implement `emit_offchain_order_failed()`:
  - [ ] Build appropriate command
  - [ ] Execute via framework
- [ ] Add integration tests
- [ ] Integrate into accumulator (optional context parameter)

**Completion Criteria:**
- [ ] Tests pass
- [ ] Uses CqrsFramework::execute()
- [ ] No direct events table access

## Task 9. Implement OffchainOrder Dual-Write

Implement dual-write for OffchainOrder aggregate using CqrsFramework.

**Subtasks:**
- [ ] Create `src/dual_write/offchain_order.rs`
- [ ] Implement `emit_order_placed()`:
  - [ ] Build `OffchainOrderCommand::Place` (or appropriate command)
  - [ ] Execute via `context.offchain_order_cqrs.execute(&execution_id, command).await`
- [ ] Implement `emit_order_submitted()`:
  - [ ] Build appropriate command
  - [ ] Execute via framework
- [ ] Implement `emit_order_filled()`:
  - [ ] Build appropriate command
  - [ ] Execute via framework
- [ ] Implement `emit_order_failed()`:
  - [ ] Build appropriate command
  - [ ] Execute via framework
- [ ] Add integration tests
- [ ] Integrate into order poller and cleanup tasks

**Completion Criteria:**
- [ ] Tests pass
- [ ] Uses CqrsFramework::execute()
- [ ] No direct events table access

## Task 10. Integrate Dual-Write into Queue Processor

Wire dual-write into the main trade processing flow.

**Subtasks:**
- [ ] Initialize `DualWriteContext` in `src/lib.rs::launch()`
- [ ] Pass context to conductor's queue processor task
- [ ] Modify `src/onchain/accumulator.rs::process_onchain_trade()`:
  - [ ] Accept optional `DualWriteContext` parameter
  - [ ] After successful legacy transaction:
    - [ ] Call `dual_write::emit_trade_filled()`
    - [ ] Call `dual_write::position::emit_onchain_fill_acknowledged()`
    - [ ] If execution created: call dual-write for OffchainOrder and Position
  - [ ] Use pattern: `if let Some(ctx) = context { emit(ctx).await.ok(); }`
  - [ ] Log errors with full context
- [ ] Add integration test

**Completion Criteria:**
- [ ] Tests pass
- [ ] Context properly passed through call chain
- [ ] Events emitted after legacy writes

## Task 11. Integrate Dual-Write into Order Poller

Emit events when order status changes.

**Subtasks:**
- [ ] Pass `DualWriteContext` to order poller task
- [ ] After each status UPDATE:
  - [ ] Call appropriate dual-write functions
  - [ ] Handle errors with logging
- [ ] Add tests for status transitions

**Completion Criteria:**
- [ ] Tests pass
- [ ] Events emitted on status changes

## Task 12. Integrate Dual-Write into Stale Execution Cleanup

Emit failure events when cleaning up stale executions.

**Subtasks:**
- [ ] Pass `DualWriteContext` to cleanup task
- [ ] After marking execution FAILED:
  - [ ] Call dual-write for OffchainOrder and Position
  - [ ] Handle errors with logging
- [ ] Add tests

**Completion Criteria:**
- [ ] Tests pass
- [ ] Events emitted on cleanup

## Success Criteria

**Must Have:**
- [ ] All onchain trades generate `OnChainTradeEvent::Filled` events
- [ ] All position changes generate corresponding `PositionEvent` events
- [ ] All offchain orders generate lifecycle `OffchainOrderEvent` events
- [ ] Legacy tables remain source of truth for reads (no code changes to read path)
- [ ] Event emission failures logged but don't block trade processing
- [ ] Integration tests pass for full trade lifecycle

**Nice to Have:**
- [ ] View tables automatically updated from events (may already work via existing view logic)
- [ ] Reconciliation tool to detect divergence between legacy and events

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

1. **Switch Reads to Views:** Update query logic to read from `*_view` tables instead of legacy tables
2. **Remove Legacy Writes:** Once views proven reliable, remove legacy INSERT/UPDATE logic
3. **Event Store as Primary:** Migrate to event store as single source of truth
4. **Schema Cleanup:** Deprecate and eventually drop legacy tables

This plan focuses solely on Phase 1: dual-write validation.
