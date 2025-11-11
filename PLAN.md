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

Initialize CQRS framework for dual-write operations.

**Subtasks:**
- [ ] Create `src/dual_write/mod.rs` with module structure
- [ ] Research cqrs-es SqliteStore setup (examine existing migration code for patterns)
- [ ] Define `DualWriteContext` struct in `src/dual_write/mod.rs`:
  - [ ] `pool: SqlitePool` reference
  - [ ] Add fields for CQRS framework stores (TBD after research)
- [ ] Add `pub(crate) fn new()` constructor for `DualWriteContext`
- [ ] Add visibility level: make `DualWriteContext` `pub(crate)`, internals private
- [ ] Update `src/lib.rs` to expose dual_write module as `pub(crate)`
- [ ] Add custom `DualWriteError` type in `src/dual_write/error.rs`

**Design Notes:**
- Use `pub(crate)` for all dual-write types (not part of public API)
- Follow import organization: external crates, then blank line, then `crate::`
- Research task may reveal existing CQRS setup patterns to reuse

**Completion Criteria:**
- [ ] `cargo build` succeeds
- [ ] Module structure created and compiles

## Task 2. OnchainTrade Dual-Write

Implement dual-write for OnchainTrade aggregate including CQRS setup, event emission, accumulator integration, queue processor wiring, and testing.

**Subtasks:**

### 2.1 Setup OnchainTrade CQRS Framework
- [ ] Research existing OnchainTrade aggregate implementation
- [ ] Initialize `CqrsFramework` for OnchainTrade in `DualWriteContext::new()`
- [ ] Add `onchain_trade_cqrs` field to `DualWriteContext`
- [ ] Verify OnchainTrade commands exist (or define them)
- [ ] Add unit test verifying framework initialization

### 2.2 Implement OnchainTrade Event Emission
- [ ] Create `src/dual_write/onchain_trade.rs` module
- [ ] Follow import organization: external imports, blank line, internal imports
- [ ] Implement `pub(crate) async fn emit_trade_filled()`:
  - [ ] Parameters: `trade: &OnchainTrade`, `context: &DualWriteContext`
  - [ ] Build aggregate_id as `{tx_hash}:{log_index}`
  - [ ] Create command (research exact command type from aggregate)
  - [ ] Execute via CQRS framework
  - [ ] Wrap in `Result<(), DualWriteError>` with error logging
  - [ ] Use `tracing::error!` on failures with full context
  - [ ] Never panic or return Err to caller
- [ ] Add unit tests with mocked CQRS framework
- [ ] Add helper function `log_event_error()` for consistent error logging

### 2.3 Integrate OnchainTrade into Accumulator
- [ ] Modify `src/onchain/trade.rs::OnchainTrade::save_within_transaction()`:
  - [ ] Add optional parameter: `dual_write_context: Option<&DualWriteContext>`
  - [ ] After successful INSERT, call `emit_trade_filled()` if context provided
  - [ ] Use pattern: `if let Some(ctx) = dual_write_context { emit(...).await.ok(); }`
- [ ] Update call sites in accumulator to pass `None` (preserves existing behavior)

### 2.4 Wire into Queue Processor
- [ ] Initialize `DualWriteContext` in `src/lib.rs::launch()` function
- [ ] Pass context to `spawn_queue_processor()` in conductor
- [ ] Update `process_onchain_trade()` call to pass context
- [ ] Verify tests still pass with context=None

### 2.5 Integration Testing
- [ ] Add integration tests in `src/dual_write/onchain_trade.rs` test module:
  - [ ] Process onchain trade with dual-write enabled
  - [ ] Verify event written to event store
  - [ ] Verify legacy table also written
  - [ ] Verify aggregate can be loaded from event store
- [ ] Add test with event emission failure (verify legacy succeeds)

**Completion Criteria:**
- [ ] `timeout 60 cargo test -q --lib` passes
- [ ] `timeout 90 cargo clippy --all-targets -- -D warnings` passes
- [ ] `cargo fmt --check` passes
- [ ] Integration test demonstrates working dual-write
- [ ] Error logging includes full context

## Task 3. Position Dual-Write

Implement dual-write for Position aggregate including CQRS setup, event emission for onchain fills and offchain order placement, accumulator integration, and testing.

**Subtasks:**

### 3.1 Setup Position CQRS Framework
- [ ] Initialize `CqrsFramework` for Position in `DualWriteContext::new()`
- [ ] Add `position_cqrs` field to `DualWriteContext`
- [ ] Verify Position commands exist (already defined in `src/position/cmd.rs`)
- [ ] Add unit test verifying framework initialization

### 3.2 Implement Position Event Emission Functions
- [ ] Create `src/dual_write/position.rs` module
- [ ] Follow import organization guidelines
- [ ] Implement `pub(crate) async fn emit_onchain_fill_acknowledged()`:
  - [ ] Parameters: `trade: &OnchainTrade`, `symbol: &Symbol`, `context: &DualWriteContext`
  - [ ] Use symbol as aggregate_id
  - [ ] Build `PositionCommand::AcknowledgeOnChainFill`
  - [ ] Execute via CQRS framework
  - [ ] Error handling with logging
- [ ] Implement `pub(crate) async fn emit_offchain_order_placed()`:
  - [ ] Parameters: execution details, `context: &DualWriteContext`
  - [ ] Build `PositionCommand::PlaceOffChainOrder`
  - [ ] Execute via CQRS framework
- [ ] Add unit tests for both emission functions

### 3.3 Integrate Position into Accumulator
- [ ] Modify `src/onchain/accumulator.rs::process_onchain_trade()`:
  - [ ] Add optional parameter: `dual_write_context: Option<&DualWriteContext>`
  - [ ] After onchain trade saved, call `emit_onchain_fill_acknowledged()`
  - [ ] When execution created, call `emit_offchain_order_placed()`
  - [ ] Use non-blocking pattern: `.await.ok()`
- [ ] Update conductor queue processor to pass context
- [ ] Verify existing tests pass

### 3.4 Integration Testing
- [ ] Add test: process onchain trade → position accumulates
- [ ] Add test: position crosses threshold → offchain order placed
- [ ] Add test: verify Position aggregate state matches legacy accumulator
- [ ] Add test: event emission failure doesn't break accumulator
- [ ] Add test: multiple trades for same symbol (concurrent updates)

**Completion Criteria:**
- [ ] `timeout 90 cargo test -q --lib` passes
- [ ] `timeout 90 cargo clippy --all-targets -- -D warnings` passes
- [ ] Integration tests verify position state consistency
- [ ] Accumulator still works with context=None (backward compatibility)

## Task 4. Implement OffchainOrder Aggregate Dual-Write

Emit order lifecycle events.

**Subtasks:**
- [ ] Create `src/dual_write/offchain_order.rs`
- [ ] Implement `emit_offchain_order_placed()`:
  - Called in `accumulator::create_execution_within_transaction()` after INSERT
  - Command: `OffchainOrderCommand::Place`
  - Parameters: symbol, shares, direction, broker
  - Return execution_id to caller
- [ ] Implement `emit_offchain_order_submitted()`:
  - Called in order poller when broker confirms submission
  - Command: `OffchainOrderCommand::ConfirmSubmission`
  - Parameters: broker_order_id
- [ ] Implement `emit_offchain_order_filled()`:
  - Called in order poller when status → FILLED
  - Command: `OffchainOrderCommand::CompleteFill`
  - Parameters: price_cents
- [ ] Implement `emit_offchain_order_failed()`:
  - Called in order poller or cleanup when order fails
  - Command: `OffchainOrderCommand::MarkFailed`
  - Parameters: error message
- [ ] Add unit tests for each emission function
- [ ] Add integration test verifying order lifecycle in event store

**Design Notes:**
- Use `execution_id` (from offchain_trades.id) as aggregate_id
- Execution must be INSERTed to get ID before emitting Placed event
- Consider adding execution_id to context if needed across functions

## Task 5. Integrate Dual-Write into Queue Processor

Wire dual-write into the main trade processing flow.

**Subtasks:**
- [ ] Modify `src/conductor/mod.rs::run_queue_processor()`:
  - Pass `DualWriteContext` to `process_onchain_trade()`
- [ ] Modify `src/onchain/accumulator.rs::process_onchain_trade()`:
  - Accept `dual_write_context: Option<&DualWriteContext>` parameter
  - After successful legacy transaction commit:
    - Call `dual_write::onchain_trade::emit_onchain_trade_filled()`
    - Call `dual_write::position::emit_onchain_fill_acknowledged()`
    - If execution created: call `dual_write::position::emit_offchain_order_placed()`
    - If execution created: call `dual_write::offchain_order::emit_offchain_order_placed()`
  - Wrap each call in error handling
- [ ] Add integration test for full queue processor flow with events

**Design Notes:**
- Make `dual_write_context` Optional for backward compatibility in tests
- Use pattern: `if let Some(ctx) = dual_write_context { emit_event(ctx).await.ok(); }`
- Log errors at ERROR level with full context
- Consider adding distributed tracing span for dual-write operations

## Task 6. Integrate Dual-Write into Order Poller

Emit events when order status changes.

**Subtasks:**
- [ ] Locate order poller implementation (likely `src/schwab/order_poller.rs` or `src/conductor/mod.rs`)
- [ ] Pass `DualWriteContext` to order poller task
- [ ] Identify status transition points:
  - PENDING → SUBMITTED: emit `OffchainOrderEvent::Submitted` + `PositionEvent` (none needed)
  - SUBMITTED → FILLED: emit `OffchainOrderEvent::Filled` + `PositionEvent::OffChainOrderFilled`
  - SUBMITTED → FAILED: emit `OffchainOrderEvent::Failed` + `PositionEvent::OffChainOrderFailed`
- [ ] Add dual-write calls after each legacy status UPDATE
- [ ] Add unit tests for each transition
- [ ] Add integration test for order lifecycle with events

**Design Notes:**
- Order poller may batch-update multiple orders; ensure events are emitted for each
- Include broker_order_id in events for correlation
- Consider using broker timestamps if available

## Task 7. Integrate Dual-Write into Stale Execution Cleanup

Emit failure events when cleaning up stale executions.

**Subtasks:**
- [ ] Modify `src/onchain/accumulator.rs::clean_up_stale_executions()`:
  - Accept `dual_write_context: Option<&DualWriteContext>` parameter
  - After marking execution as FAILED:
    - Call `dual_write::offchain_order::emit_offchain_order_failed()`
    - Call `dual_write::position::emit_offchain_order_failed()`
- [ ] Modify `src/conductor/mod.rs` position_checker task to pass context
- [ ] Add unit tests
- [ ] Add integration test

**Design Notes:**
- Cleanup happens periodically (every 60s)
- Stale executions are those older than threshold without broker confirmation
- Error message should indicate "Stale execution timeout"

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
