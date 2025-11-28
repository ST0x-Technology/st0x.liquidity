# Implementation Plan: UsdcRebalance Aggregate (Issue #136)

## Overview

Implement a CQRS-ES aggregate to manage USDC cross-chain rebalancing between
Alpaca (offchain) and Base (onchain) via Circle's Cross-Chain Transfer Protocol
(CCTP). The aggregate will track the complete asynchronous flow: withdrawal from
source, bridge burn/attestation/mint, and deposit to destination, handling both
AlpacaToBase and BaseToAlpaca directions.

## Architecture

The aggregate uses the typestate pattern via enum variants to represent states:

1. **NotStarted**: Initial state before rebalancing begins
2. **WithdrawalInitiated**: Withdrawal from source requested
3. **BridgingInProgress**: USDC burned on source chain, awaiting attestation and
   mint
4. **DepositPending**: Minted on destination, awaiting deposit confirmation
5. **Completed**: Full rebalancing cycle complete (terminal state)
6. **Failed**: Process failed at any stage (terminal state)

Direction is bidirectional:

- **AlpacaToBase**: Withdraw from Alpaca → Bridge to Base
- **BaseToAlpaca**: Withdraw from Base → Bridge to Alpaca

Reference tracking:

- Alpaca withdrawals: `transfer_id` (String)
- Onchain withdrawals: `tx_hash` (TxHash)
- Bridge operations: `burn_tx_hash`, `nonce`, `message_hash`, `mint_tx_hash`

## Task 1. Implement Core Aggregate with Withdrawal Flow

Create complete aggregate structure supporting withdrawal initiation and
completion.

### Subtasks

- [x] Create module `src/usdc_rebalance/mod.rs`
- [x] Define `UsdcRebalanceId` newtype wrapping String
- [x] Define `RebalanceDirection` enum with AlpacaToBase and BaseToAlpaca
      variants
- [x] Define `UsdcRebalance` enum with variants:
  - [x] `NotStarted`
  - [x] `WithdrawalInitiated { direction, amount, initiated_at }`
  - [x] `Completed { direction, amount, completed_at }` (stub for now, will
        expand later)
  - [x] `Failed { direction, amount, reason, failed_at }`
- [x] Define `UsdcRebalanceError` enum with:
  - [x] `WithdrawalNotInitiated`
  - [x] `AlreadyCompleted`
  - [x] `AlreadyFailed`
- [x] Implement `Default` for `UsdcRebalance` returning `NotStarted`
- [x] Create `src/usdc_rebalance/cmd.rs` with commands:
  - [x] `InitiateWithdrawal { direction, amount }`
  - [x] `Fail { reason }`
- [x] Create `src/usdc_rebalance/event.rs` with events:
  - [x] `WithdrawalInitiated { direction, amount, initiated_at }`
  - [x] `Failed { reason, failed_at }`
- [x] Implement `DomainEvent` trait for events
- [x] Implement `Aggregate` trait for `UsdcRebalance`:
  - [x] `aggregate_type()` returns "UsdcRebalance"
  - [x] `handle()` supports: NotStarted → InitiateWithdrawal, non-terminal →
        Fail
  - [x] `apply()` transitions states based on events
- [x] Export types in module

### Tests

- [x] `test_initiate_withdrawal_alpaca_to_base`
- [x] `test_initiate_withdrawal_base_to_alpaca`
- [x] `test_fail_from_withdrawal_initiated`
- [x] `test_cannot_fail_when_completed`
- [x] `test_cannot_fail_when_already_failed`

### Validation

- [x] Run `cargo test -q` - all tests pass
- [x] Run `cargo clippy -- -D clippy::all` - no warnings
- [x] Run `cargo fmt`

## Task 2. Add Withdrawal Completion with Reference Tracking

Extend aggregate to track withdrawal completion with Alpaca transfer_id or
onchain tx_hash.

### Subtasks

- [ ] Update `UsdcRebalance::WithdrawalInitiated` to include optional
      `transfer_id` and `tx_hash`
- [ ] Update `UsdcRebalance::Failed` to preserve optional `transfer_id` and
      `tx_hash`
- [ ] Add command
      `RecordWithdrawalComplete { transfer_id: Option<String>, tx_hash: Option<TxHash> }`
- [ ] Add event `WithdrawalCompleted { transfer_id, tx_hash, completed_at }`
- [ ] Update `handle()` to support: WithdrawalInitiated →
      RecordWithdrawalComplete
- [ ] Update `apply()` to handle WithdrawalCompleted event
- [ ] Add error `CompletionWithoutReferences` if both transfer_id and tx_hash
      are None

### Tests

- [ ] `test_complete_withdrawal_with_alpaca_transfer_id`
- [ ] `test_complete_withdrawal_with_onchain_tx_hash`
- [ ] `test_cannot_complete_withdrawal_before_initiating`
- [ ] `test_withdrawal_completion_requires_at_least_one_reference`

### Validation

- [ ] Run `cargo test -q` - all tests pass
- [ ] Run `cargo clippy -- -D clippy::all` - no warnings
- [ ] Run `cargo fmt`

## Task 3. Add Bridge Burn and Mint Tracking

Extend aggregate to track CCTP bridge operations (burn and mint).

### Subtasks

- [ ] Add state variant
      `BridgingInProgress { direction, amount, withdrawal_refs, burn_tx_hash, nonce, message_hash, bridging_started_at }`
- [ ] Update `Completed` to store full flow data:
      `{ direction, amount, withdrawal_refs, burn_tx_hash, mint_tx_hash, completed_at }`
- [ ] Update `Failed` to preserve bridge references
- [ ] Add command `RecordBridgeBurn { burn_tx_hash, nonce, message_hash }`
- [ ] Add command `RecordBridgeMint { mint_tx_hash }`
- [ ] Add event
      `BridgeBurnRecorded { burn_tx_hash, nonce, message_hash, burned_at }`
- [ ] Add event `BridgeMintRecorded { mint_tx_hash, minted_at }`
- [ ] Add errors: `BridgingNotInProgress`
- [ ] Update `handle()` to support:
  - [ ] WithdrawalInitiated → RecordBridgeBurn (when withdrawal has references)
  - [ ] BridgingInProgress → RecordBridgeMint
- [ ] Update `apply()` to handle bridge events

### Tests

- [ ] `test_record_bridge_burn`
- [ ] `test_record_bridge_mint`
- [ ] `test_cannot_burn_before_withdrawal_complete`
- [ ] `test_cannot_mint_before_burn`
- [ ] `test_fail_from_bridging_in_progress`

### Validation

- [ ] Run `cargo test -q` - all tests pass
- [ ] Run `cargo clippy -- -D clippy::all` - no warnings
- [ ] Run `cargo fmt`

## Task 4. Add Deposit Completion Flow

Complete the aggregate with deposit pending state and final completion.

### Subtasks

- [ ] Add state variant
      `DepositPending { direction, amount, all_refs, minted_at }`
- [ ] Add command `RecordDepositComplete`
- [ ] Add event `DepositCompleted { completed_at }`
- [ ] Add error `DepositNotPending`
- [ ] Update `handle()` to support: DepositPending → RecordDepositComplete
- [ ] Update `apply()` to handle DepositCompleted event
- [ ] Ensure `Completed` state has all tracking data

### Tests

- [ ] `test_record_deposit_complete`
- [ ] `test_complete_alpaca_to_base_flow` (end-to-end)
- [ ] `test_complete_base_to_alpaca_flow` (end-to-end)
- [ ] `test_cannot_deposit_before_mint`
- [ ] `test_fail_from_deposit_pending`
- [ ] `test_failed_state_preserves_context`

### Validation

- [ ] Run `cargo test -q` - all tests pass
- [ ] Run `cargo clippy -- -D clippy::all` - no warnings
- [ ] Run `cargo fmt`

## Task 5. Implement View Projection

Add view layer for queryable read model.

### Subtasks

- [ ] Create `src/usdc_rebalance/view.rs`
- [ ] Define `UsdcRebalanceView` enum mirroring aggregate states but with
      `rebalance_id` field:
  - [ ] `NotStarted`
  - [ ] `WithdrawalInitiated { rebalance_id, direction, amount, initiated_at }`
  - [ ] `BridgingInProgress { rebalance_id, direction, amount, withdrawal_refs, bridge_data, bridging_started_at }`
  - [ ] `DepositPending { rebalance_id, direction, amount, all_refs, minted_at }`
  - [ ] `Completed { rebalance_id, direction, amount, completed_at }`
  - [ ] `Failed { rebalance_id, direction, amount, optional_refs, failure_reason, failed_at }`
- [ ] Implement `Default` for view
- [ ] Implement `View<UsdcRebalance>` trait with `update()` method
- [ ] Add private handler methods for each event type:
  - [ ] `handle_withdrawal_initiated()`
  - [ ] `handle_withdrawal_completed()`
  - [ ] `handle_bridge_burn_recorded()`
  - [ ] `handle_bridge_mint_recorded()`
  - [ ] `handle_deposit_completed()`
  - [ ] `handle_failed()`
- [ ] Log warnings when events arrive in unexpected states
- [ ] Export view in module

### Tests

- [ ] `test_view_tracks_alpaca_to_base_flow`
- [ ] `test_view_tracks_base_to_alpaca_flow`
- [ ] `test_view_captures_failure_at_withdrawal`
- [ ] `test_view_captures_failure_during_bridging`
- [ ] `test_view_captures_failure_at_deposit`
- [ ] `test_view_preserves_rebalance_id`

### Validation

- [ ] Run `cargo test -q` - all tests pass
- [ ] Run `cargo clippy -- -D clippy::all` - no warnings
- [ ] Run `cargo fmt`

## Task 6. Add Database Migration for View Persistence

Create database schema to persist view state.

### Subtasks

- [ ] Create migration file
      `migrations/$(date +%Y%m%d%H%M%S)_usdc_rebalance_view.sql`
- [ ] Add table `usdc_rebalance_view` with columns:
  - [ ] `rebalance_id TEXT PRIMARY KEY`
  - [ ] `state TEXT NOT NULL` CHECK IN ('NotStarted', 'WithdrawalInitiated',
        'BridgingInProgress', 'DepositPending', 'Completed', 'Failed')
  - [ ] `direction TEXT` CHECK IN ('AlpacaToBase', 'BaseToAlpaca') or NULL
  - [ ] `amount_usdc TEXT`
  - [ ] `alpaca_transfer_id TEXT`
  - [ ] `withdrawal_tx_hash TEXT`
  - [ ] `burn_tx_hash TEXT`
  - [ ] `burn_nonce TEXT`
  - [ ] `burn_message_hash TEXT`
  - [ ] `mint_tx_hash TEXT`
  - [ ] `failure_reason TEXT`
  - [ ] `initiated_at TIMESTAMP`
  - [ ] `withdrawal_completed_at TIMESTAMP`
  - [ ] `bridging_started_at TIMESTAMP`
  - [ ] `minted_at TIMESTAMP`
  - [ ] `completed_at TIMESTAMP`
  - [ ] `failed_at TIMESTAMP`
- [ ] Add constraints:
  - [ ] `CHECK (state = 'NotStarted' OR direction IS NOT NULL)`
  - [ ] `CHECK (state = 'NotStarted' OR amount_usdc IS NOT NULL)`
  - [ ] `CHECK (state != 'Failed' OR failure_reason IS NOT NULL)`

### Validation

- [ ] Run `sqlx migrate run` - migration applies successfully
- [ ] Verify schema with `sqlite3 <db_path> ".schema usdc_rebalance_view"`

## Task 7. Integrate with Event Store Infrastructure

Wire aggregate into application's CQRS-ES framework.

### Subtasks

- [ ] Add `usdc_rebalance` module to `src/lib.rs`
- [ ] Export `UsdcRebalance`, `UsdcRebalanceCommand`, `UsdcRebalanceEvent`,
      `UsdcRebalanceView`
- [ ] Register aggregate with `CqrsFramework` (if centralized registration
      exists)
- [ ] Configure view repository for `UsdcRebalanceView` with SQLite backend
- [ ] Verify event store can persist and replay events

### Validation

- [ ] Run `cargo build` - compiles without errors
- [ ] Run `cargo test -q` - all tests pass
- [ ] Verify aggregate can be loaded via event store (manual/integration test if
      available)

## Task 8. Add Comprehensive Documentation

Document the aggregate's purpose, usage, and architecture.

### Subtasks

- [ ] Add module-level documentation to `src/usdc_rebalance/mod.rs`:
  - [ ] Purpose: Managing cross-chain USDC rebalancing between Alpaca and Base
  - [ ] State flow diagram in ASCII art showing all transitions
  - [ ] Integration points: Alpaca API withdrawals/deposits, CCTP bridge, Base
        chain
  - [ ] Error handling: Type-safe transitions, terminal state protection,
        context preservation
  - [ ] Usage example showing complete flow with commands
- [ ] Document each `UsdcRebalance` state variant:
  - [ ] What it represents in the business flow
  - [ ] Which data fields are populated and why
- [ ] Document each command's purpose and when to use it
- [ ] Document each event's meaning in the domain
- [ ] Add inline comments for non-obvious state transition logic in `handle()`

### Validation

- [ ] Run `cargo doc --open` and review generated documentation
- [ ] Verify all public types have documentation
- [ ] Ensure examples in docs are syntactically valid

## Task 9. Final Validation and Cleanup

Ensure production readiness across all quality metrics.

### Subtasks

- [ ] Run full test suite: `cargo test -q`
- [ ] Run linting: `cargo clippy --all-targets --all-features -- -D clippy::all`
- [ ] Run formatter: `cargo fmt --check` (verify no changes needed)
- [ ] Build release binary: `cargo build --release`
- [ ] Verify migrations are applied: `sqlx migrate info`
- [ ] Review code against AGENTS.md guidelines:
  - [ ] Package by feature: all code in `usdc_rebalance` module ✓
  - [ ] Type modeling: state machine via enum variants ✓
  - [ ] No `#[allow(clippy::*)]` attributes
  - [ ] Financial data: all Decimal amounts use proper error handling
  - [ ] Proper error propagation: no silent failures or unwrap_or defaults
  - [ ] Comments only where logic isn't self-evident
  - [ ] Visibility levels: prefer restrictive (pub(crate) over pub)

### Validation

- [ ] All tests pass
- [ ] Zero clippy warnings
- [ ] Zero formatting changes needed
- [ ] Clean release build
- [ ] Migrations up to date
- [ ] Code adheres to project standards

## Success Criteria

Upon completion, the implementation provides:

1. **Complete aggregate lifecycle**: NotStarted → WithdrawalInitiated →
   BridgingInProgress → DepositPending → Completed/Failed
2. **Bidirectional support**: AlpacaToBase and BaseToAlpaca directions
3. **Comprehensive tracking**: Alpaca transfer_id, onchain tx_hash, CCTP
   burn/mint data
4. **View projection**: Queryable state via database-backed read model
5. **Type-safe state machine**: Invalid transitions rejected at compile time
   where possible
6. **Full test coverage**: Unit tests for all transitions, edge cases, and
   failures
7. **Database persistence**: Migration-backed view table for operational queries
8. **Production quality**: Zero clippy warnings, comprehensive docs, passing
   tests
