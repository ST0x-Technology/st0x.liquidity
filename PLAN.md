# Implementation Plan: UsdcRebalance Aggregate (Issue #136)

## Overview

Implement a CQRS-ES aggregate to manage complete USDC cross-chain rebalancing
between Alpaca (offchain) and Base (onchain) via Circle's Cross-Chain Transfer
Protocol (CCTP). The aggregate tracks the entire asynchronous flow: withdrawal
from source → CCTP bridge (burn/attestation/mint) → deposit to destination.

## Architecture

The aggregate uses the `Lifecycle<T, E>` wrapper pattern (matching
TokenizedEquityMint and EquityRedemption) with enum variants to represent the
complete rebalancing lifecycle with 10 distinct live states grouped by phase.
The `Lifecycle::Uninitialized` state represents "not started".

**Withdrawal Phase:**

1. ~~**NotStarted**: Initial state before rebalancing begins~~ → Use
   `Lifecycle::Uninitialized`
2. **WithdrawalInitiated**: Withdrawal from source requested
3. **WithdrawalConfirmed**: Withdrawal from source confirmed
4. **WithdrawalFailed**: Withdrawal failed (terminal state)

**Bridging Phase:**

5. **BridgingInitiated**: USDC burned on source chain via CCTP
6. **BridgeAttestationReceived**: Circle attestation retrieved
7. **Bridged**: USDC minted on destination chain via CCTP
8. **BridgingFailed**: Bridging failed (terminal state)

**Deposit Phase:**

9. **DepositInitiated**: Deposit to destination requested
10. **DepositConfirmed**: Deposit to destination confirmed (terminal success
    state)
11. **DepositFailed**: Deposit failed (terminal state)

Direction is bidirectional:

- **AlpacaToBase**: Withdraw from Alpaca → CCTP bridge (Ethereum → Base) →
  Deposit to Rain vault on Base
- **BaseToAlpaca**: Withdraw from Rain vault on Base → CCTP bridge (Base →
  Ethereum) → Deposit to Alpaca

Reference tracking:

- **TransferRef**: Alpaca API `transfer_id` (AlpacaTransferId) or onchain
  `tx_hash` (TxHash)
- **Bridge data**: `burn_tx_hash`, `cctp_nonce`, `attestation`, `mint_tx_hash`

## Task 1. Module Setup and Integration

Create module structure, add to lib.rs, and create database migration for view
persistence.

### Subtasks

- [x] Create module `src/usdc_rebalance/mod.rs`
- [x] Add `usdc_rebalance` module to `src/lib.rs`
- [x] Export module types in `src/lib.rs`
- [x] Create migration file using `sqlx migrate add usdc_rebalance_view`
- [x] Create table `usdc_rebalance_view` with standard CQRS-ES view pattern:
  - [x] `view_id TEXT PRIMARY KEY` - rebalance aggregate ID
  - [x] `version BIGINT NOT NULL` - event sequence number
  - [x] `payload JSON NOT NULL` - entire aggregate state as JSON
  - [x] STORED generated columns for efficient querying:
    - [x] `direction TEXT GENERATED ALWAYS AS (json_extract(payload,
          '$.direction')) STORED`
    - [x] `state TEXT GENERATED ALWAYS AS (json_extract(payload, '$.state'))
          STORED`
    - [x] `amount_usdc TEXT GENERATED ALWAYS AS (json_extract(payload,
          '$.amount')) STORED`
- [x] Create indexes on generated columns:
  - [x] `idx_usdc_rebalance_view_direction` on `direction` WHERE NOT NULL
  - [x] `idx_usdc_rebalance_view_state` on `state` WHERE NOT NULL
  - [x] `idx_usdc_rebalance_view_amount` on `amount_usdc` WHERE NOT NULL

### Validation

- [x] Run `sqlx db reset -y` - migration applied successfully
- [x] Run `cargo build` - compiles without errors

### Changes Made

**Migration**: Created `migrations/20251128231254_usdc_rebalance_view.sql` with
CQRS-ES view table pattern matching existing view tables in the codebase. The
migration creates a view table with JSON payload storage and generated columns
for common query patterns (direction, state, amount).

**Module Structure**: Created minimal module skeleton with:

- `mod.rs`: Basic aggregate structure with empty `handle()` and `apply()`
  implementations (to be filled in subsequent tasks)
- `cmd.rs`: All command variants from SPEC (Initiate, ConfirmWithdrawal,
  InitiateBridging, ReceiveAttestation, ConfirmBridging, InitiateDeposit,
  ConfirmDeposit, FailWithdrawal, FailBridging, FailDeposit)
- `event.rs`: All event variants from SPEC with `DomainEvent` implementation
- Supporting types: `UsdcRebalanceId`, `TransferRef` enum (AlpacaId/OnchainTx),
  `RebalanceDirection` enum (AlpacaToBase/BaseToAlpaca), `UsdcRebalanceError`

**Integration**: Module already present in `src/lib.rs` with
`#[allow(dead_code)]` attribute (line 38-39).

**AGENTS.md Update**: Added critical guidelines about never manually creating
migrations or editing Cargo.toml - must use `sqlx migrate add` and `cargo add`
respectively.

## Task 2. Core Aggregate with Initiation and View ✅

Implement NotStarted → WithdrawalInitiated transition with view tracking.

### Subtasks

- [x] Define `UsdcRebalanceId` newtype wrapping String
- [x] Define `RebalanceDirection` enum with AlpacaToBase and BaseToAlpaca
      variants
- [x] Create `src/usdc_rebalance/cmd.rs`:
  - [x] Import `AlpacaTransferId` from `alpaca_wallet` module
  - [x] Define `TransferRef` enum: `AlpacaId(AlpacaTransferId)`,
        `OnchainTx(TxHash)`
  - [x] Add `Initiate { direction, amount, withdrawal }` command
- [x] Create `src/usdc_rebalance/event.rs`:
  - [x] Add `Initiated { direction, amount, withdrawal_ref, initiated_at }`
        event
- [x] Define `UsdcRebalance` enum with states:
  - [x] `NotStarted`
  - [x] `WithdrawalInitiated { direction, amount, withdrawal_ref, initiated_at }`
- [x] Define `UsdcRebalanceError` enum:
  - [x] `AlreadyInitiated`
  - [x] `InvalidStateTransition { from: String, command: String }`
- [x] Implement `Default` for `UsdcRebalance` returning `NotStarted`
- [x] Implement `DomainEvent` trait for events
- [x] Implement `Aggregate::handle()`:
  - [x] `NotStarted` + `Initiate` → emit `Initiated`
- [x] Implement `Aggregate::apply()`:
  - [x] `Initiated` → transition to `WithdrawalInitiated` state
- [x] Create `src/usdc_rebalance/view.rs`:
  - [x] Define `UsdcRebalanceView` enum with Unavailable, NotStarted, and
        WithdrawalInitiated variants
  - [x] Implement `Default` for view
  - [x] Implement `View<UsdcRebalance>` trait with `update()` method
  - [x] Add `handle_initiated()` method
- [x] Export types in module

### Tests

- [x] `test_initiate_alpaca_to_base`
- [x] `test_initiate_base_to_alpaca`
- [x] `test_cannot_initiate_twice`
- [x] `test_view_tracks_initiation`

### Validation

- [x] Run `cargo test -q` - all tests pass (442 tests)
- [x] Run `cargo clippy -- -D clippy::all` - no warnings
- [x] Run `cargo fmt`

### Implementation Summary

**Files Modified:**

- `src/usdc_rebalance/mod.rs`: Extended UsdcRebalance enum with
  WithdrawalInitiated state, implemented Aggregate::handle() with pattern
  matching for NotStarted + Initiate command, implemented Aggregate::apply()
  using if let, added UsdcRebalanceError with AlreadyInitiated and
  InvalidStateTransition variants, added 4 comprehensive tests
- `src/usdc_rebalance/view.rs`: Created new file with UsdcRebalanceView enum
  (Unavailable, NotStarted, WithdrawalInitiated), implemented View trait with if
  let pattern, added handle_initiated() helper with error logging for invalid
  state transitions, added 3 comprehensive tests

**Key Design Decisions:**

- UsdcRebalanceView uses enum variants matching aggregate states rather than a
  struct, following the same pattern as PositionView
- View includes Unavailable variant for initial state before any events
- handle_initiated() validates state before transitioning and logs errors for
  invalid transitions
- All clippy errors fixed using if let instead of match for single patterns and
  explicit enum variants instead of wildcards

## Task 3. Refactor to Lifecycle Pattern ✅

Convert the UsdcRebalance aggregate to use `Lifecycle<T, E>` wrapper pattern,
matching the established patterns in TokenizedEquityMint and EquityRedemption.
This involves converting to a flat file structure, removing the separate View
type, and using `Lifecycle::Uninitialized` instead of `NotStarted` variant.

### Subtasks

- [x] Convert directory structure to flat file:
  - [x] Consolidate `src/usdc_rebalance/mod.rs`, `cmd.rs`, `event.rs`, `view.rs`
        into single `src/usdc_rebalance.rs`
  - [x] Delete `src/usdc_rebalance/` directory
  - [x] Update `src/lib.rs` to import from flat file
- [x] Refactor to use `Lifecycle<UsdcRebalance, Never>`:
  - [x] Remove `NotStarted` variant from `UsdcRebalance` enum
  - [x] Use `Lifecycle::Uninitialized` for uninitialized state
  - [x] Implement `Aggregate` for `Lifecycle<UsdcRebalance, Never>`
  - [x] Add
        `apply_transition(event: &UsdcRebalanceEvent, current: &UsdcRebalance)
        -> Result<UsdcRebalance, LifecycleError<Never>>`
        function
  - [x] Add
        `from_event(event: &UsdcRebalanceEvent) -> Result<UsdcRebalance,
        LifecycleError<Never>>`
        function
  - [x] Use `LifecycleError::Mismatch` for invalid state transitions instead of
        custom errors
- [x] Remove separate View type:
  - [x] Delete `UsdcRebalanceView` enum
  - [x] Implement `View<Lifecycle<UsdcRebalance, Never>> for Lifecycle<...>`
  - [x] Uses `transition()` and `or_initialize()` pattern (no separate function)
- [x] Update error handling:
  - [x] Remove `UsdcRebalanceError::InvalidStateTransition` (use
        `LifecycleError::Mismatch` instead)
  - [x] Keep domain-specific errors like `AlreadyInitiated` with
        `#[from] LifecycleError<Never>` conversion
- [x] Update tests:
  - [x] Update existing tests to use `Lifecycle<UsdcRebalance, Never>`
  - [x] Replace `UsdcRebalance::default()` with `Lifecycle::default()`
  - [x] Update view tests to use `Lifecycle` self-view pattern

### Validation

- [x] Run `cargo test -q` - all tests pass (6 usdc_rebalance tests)
- [x] Run `cargo clippy -- -D clippy::all` - no warnings
- [x] Run `cargo fmt`

### Implementation Summary

**Files Created:**

- `src/usdc_rebalance.rs`: Consolidated flat file containing all types,
  commands, events, aggregate implementation, view, and tests

**Files Deleted:**

- `src/usdc_rebalance/mod.rs`
- `src/usdc_rebalance/cmd.rs`
- `src/usdc_rebalance/event.rs`
- `src/usdc_rebalance/view.rs`

**Key Changes:**

- Removed `NotStarted` variant from `UsdcRebalance` enum - now using
  `Lifecycle::Uninitialized`
- Removed `UsdcRebalanceView` enum - using `impl View<Self> for Lifecycle<...>`
  self-view pattern
- Removed `InvalidStateTransition` error variant - using
  `LifecycleError::Mismatch` via `#[from]` conversion
- Added `apply_transition()` and `from_event()` static methods on
  `UsdcRebalance`
- Tests use `Lifecycle::<UsdcRebalance, Never>::default()` instead of
  `UsdcRebalance::default()`

## Task 4. Withdrawal Confirmation ✅

Implement Withdrawing → WithdrawalComplete/WithdrawalFailed transitions. (View
updates are now handled by the Lifecycle self-view pattern from Task 3.)

### Subtasks

- [x] Add state `WithdrawalComplete`:
  - [x] `direction: RebalanceDirection`
  - [x] `amount: Usdc`
  - [x] `initiated_at: DateTime<Utc>`
  - [x] `confirmed_at: DateTime<Utc>`
- [x] Add state `WithdrawalFailed`:
  - [x] `direction: RebalanceDirection`
  - [x] `amount: Usdc`
  - [x] `withdrawal_ref: TransferRef`
  - [x] `reason: String`
  - [x] `initiated_at: DateTime<Utc>`
  - [x] `failed_at: DateTime<Utc>`
- [x] Add commands:
  - [x] `ConfirmWithdrawal`
  - [x] `FailWithdrawal { reason }`
- [x] Add events:
  - [x] `WithdrawalConfirmed { confirmed_at }`
  - [x] `WithdrawalFailed { reason, failed_at }`
- [x] Update `handle()`:
  - [x] `Withdrawing` + `ConfirmWithdrawal` → emit `WithdrawalConfirmed`
  - [x] `Withdrawing` + `FailWithdrawal` → emit `WithdrawalFailed`
- [x] Update `apply_transition()` for withdrawal events
- [x] Update `from_event()` if these events can initialize (N/A - not
      initialization events)
- [x] Add errors:
  - [x] `WithdrawalNotInitiated`
  - [x] `WithdrawalAlreadyCompleted`

### Tests

- [x] `test_confirm_withdrawal`
- [x] `test_cannot_confirm_withdrawal_before_initiating`
- [x] `test_cannot_confirm_withdrawal_twice`
- [x] `test_fail_withdrawal_after_initiation`
- [x] `test_cannot_fail_withdrawal_before_initiating`
- [x] `test_cannot_fail_already_confirmed_withdrawal`
- [x] `test_cannot_fail_already_failed_withdrawal`
- [x] `test_view_tracks_withdrawal_confirmation`
- [x] `test_view_tracks_withdrawal_failure`

### Validation

- [x] Run `cargo test -q` - all tests pass (15 usdc_rebalance tests)
- [x] Run `cargo clippy -- -D clippy::all` - no warnings
- [x] Run `cargo fmt`

### Implementation Summary

**Key Changes:**

- Renamed `WithdrawalInitiated` to `Withdrawing` and `WithdrawalConfirmed` to
  `WithdrawalComplete` to avoid clippy `enum_variant_names` lint (all variants
  had the same "Withdrawal" prefix)
- Changed `amount: Decimal` to `amount: Usdc` throughout (using proper newtype
  from `threshold.rs`)
- Added `InvalidCommand { command, state }` error for unhandled command/state
  combinations (instead of misusing `LifecycleError::Mismatch` with commands)
- Consolidated error variants: `WithdrawalAlreadyCompleted` covers both
  already-confirmed and already-failed cases

**State Machine:**

```
Lifecycle::Uninitialized --Initiate--> Withdrawing
Withdrawing --ConfirmWithdrawal--> WithdrawalComplete
Withdrawing --FailWithdrawal--> WithdrawalFailed
```

**Tests Added:** 9 new tests covering all withdrawal confirmation/failure
scenarios including edge cases for attempting operations on terminal states.

## Task 5. Bridge Burn Initiation

Implement WithdrawalConfirmed → BridgingInitiated transition. (View updates are
handled by the Lifecycle self-view pattern.)

### Subtasks

- [ ] Add state `BridgingInitiated`:
  - [ ] `direction: RebalanceDirection`
  - [ ] `amount: Decimal`
  - [ ] `burn_tx_hash: TxHash`
  - [ ] `cctp_nonce: u64`
  - [ ] `initiated_at: DateTime<Utc>`
  - [ ] `burned_at: DateTime<Utc>`
- [ ] Add command `InitiateBridging { burn_tx: TxHash, cctp_nonce: u64 }`
- [ ] Add event
      `BridgingInitiated { burn_tx_hash: TxHash, cctp_nonce: u64, burned_at }`
- [ ] Update `handle()`:
  - [ ] `WithdrawalConfirmed` + `InitiateBridging` → emit `BridgingInitiated`
- [ ] Update `apply_transition()` for `BridgingInitiated` event
- [ ] Add error `WithdrawalNotConfirmed`

### Tests

- [ ] `test_initiate_bridging`
- [ ] `test_cannot_bridge_before_withdrawal_confirmed`

### Validation

- [ ] Run `cargo test -q` - all tests pass
- [ ] Run `cargo clippy -- -D clippy::all` - no warnings
- [ ] Run `cargo fmt`

## Task 6. Bridge Attestation Receipt

Implement BridgingInitiated → BridgeAttestationReceived transition. (View
updates are handled by the Lifecycle self-view pattern.)

### Subtasks

- [ ] Add state `BridgeAttestationReceived`:
  - [ ] `direction: RebalanceDirection`
  - [ ] `amount: Decimal`
  - [ ] `burn_tx_hash: TxHash`
  - [ ] `cctp_nonce: u64`
  - [ ] `attestation: Bytes`
  - [ ] `initiated_at: DateTime<Utc>`
  - [ ] `attested_at: DateTime<Utc>`
- [ ] Add command `ReceiveAttestation { attestation: Bytes }`
- [ ] Add event `BridgeAttestationReceived { attestation: Bytes, attested_at }`
- [ ] Update `handle()`:
  - [ ] `BridgingInitiated` + `ReceiveAttestation` → emit
        `BridgeAttestationReceived`
- [ ] Update `apply_transition()` for `BridgeAttestationReceived` event
- [ ] Add error `BridgingNotInitiated`

### Tests

- [ ] `test_receive_attestation`
- [ ] `test_cannot_receive_attestation_before_bridging`

### Validation

- [ ] Run `cargo test -q` - all tests pass
- [ ] Run `cargo clippy -- -D clippy::all` - no warnings
- [ ] Run `cargo fmt`

## Task 7. Bridge Mint Confirmation and Failure

Implement BridgeAttestationReceived → Bridged/BridgingFailed transitions. (View
updates are handled by the Lifecycle self-view pattern.)

### Subtasks

- [ ] Add state `Bridged`:
  - [ ] `direction: RebalanceDirection`
  - [ ] `amount: Decimal`
  - [ ] `burn_tx_hash: TxHash`
  - [ ] `mint_tx_hash: TxHash`
  - [ ] `initiated_at: DateTime<Utc>`
  - [ ] `minted_at: DateTime<Utc>`
- [ ] Add state `BridgingFailed`:
  - [ ] `direction: RebalanceDirection`
  - [ ] `amount: Decimal`
  - [ ] `burn_tx_hash: Option<TxHash>`
  - [ ] `cctp_nonce: Option<u64>`
  - [ ] `reason: String`
  - [ ] `initiated_at: DateTime<Utc>`
  - [ ] `failed_at: DateTime<Utc>`
- [ ] Add commands:
  - [ ] `ConfirmBridging { mint_tx: TxHash }`
  - [ ] `FailBridging { reason: String }`
- [ ] Add events:
  - [ ] `Bridged { mint_tx_hash: TxHash, minted_at }`
  - [ ] `BridgingFailed { burn_tx_hash: Option<TxHash>, cctp_nonce: Option<u64>, reason, failed_at }`
- [ ] Update `handle()`:
  - [ ] `BridgeAttestationReceived` + `ConfirmBridging` → emit `Bridged`
  - [ ] `BridgingInitiated` + `FailBridging` → emit `BridgingFailed` with burn
        data
  - [ ] `BridgeAttestationReceived` + `FailBridging` → emit `BridgingFailed`
        with burn data
- [ ] Update `apply_transition()` for `Bridged` and `BridgingFailed` events
- [ ] Add error `AttestationNotReceived`

### Tests

- [ ] `test_confirm_bridging`
- [ ] `test_cannot_confirm_bridging_before_attestation`
- [ ] `test_fail_bridging_after_initiated`
- [ ] `test_fail_bridging_after_attestation_received`
- [ ] `test_bridging_failed_preserves_burn_data_when_available`

### Validation

- [ ] Run `cargo test -q` - all tests pass
- [ ] Run `cargo clippy -- -D clippy::all` - no warnings
- [ ] Run `cargo fmt`

## Task 8. Deposit Initiation

Implement Bridged → DepositInitiated transition. (View updates are handled by
the Lifecycle self-view pattern.)

### Subtasks

- [ ] Add state `DepositInitiated`:
  - [ ] `direction: RebalanceDirection`
  - [ ] `amount: Decimal`
  - [ ] `burn_tx_hash: TxHash`
  - [ ] `mint_tx_hash: TxHash`
  - [ ] `deposit_ref: TransferRef`
  - [ ] `initiated_at: DateTime<Utc>`
  - [ ] `deposit_initiated_at: DateTime<Utc>`
- [ ] Add command `InitiateDeposit { deposit: TransferRef }`
- [ ] Add event
      `DepositInitiated { deposit_ref: TransferRef, deposit_initiated_at }`
- [ ] Update `handle()`:
  - [ ] `Bridged` + `InitiateDeposit` → emit `DepositInitiated`
- [ ] Update `apply_transition()` for `DepositInitiated` event
- [ ] Add error `BridgingNotCompleted`

### Tests

- [ ] `test_initiate_deposit_with_alpaca_transfer`
- [ ] `test_initiate_deposit_with_onchain_tx`
- [ ] `test_cannot_deposit_before_bridging_complete`

### Validation

- [ ] Run `cargo test -q` - all tests pass
- [ ] Run `cargo clippy -- -D clippy::all` - no warnings
- [ ] Run `cargo fmt`

## Task 9. Deposit Confirmation and Failure

Implement DepositInitiated → DepositConfirmed/DepositFailed transitions. (View
updates are handled by the Lifecycle self-view pattern.)

### Subtasks

- [ ] Add state `DepositConfirmed`:
  - [ ] `direction: RebalanceDirection`
  - [ ] `amount: Decimal`
  - [ ] `burn_tx_hash: TxHash`
  - [ ] `mint_tx_hash: TxHash`
  - [ ] `initiated_at: DateTime<Utc>`
  - [ ] `deposit_confirmed_at: DateTime<Utc>`
- [ ] Add state `DepositFailed`:
  - [ ] `direction: RebalanceDirection`
  - [ ] `amount: Decimal`
  - [ ] `burn_tx_hash: TxHash`
  - [ ] `mint_tx_hash: TxHash`
  - [ ] `deposit_ref: Option<TransferRef>`
  - [ ] `reason: String`
  - [ ] `initiated_at: DateTime<Utc>`
  - [ ] `failed_at: DateTime<Utc>`
- [ ] Add commands:
  - [ ] `ConfirmDeposit`
  - [ ] `FailDeposit { reason: String }`
- [ ] Add events:
  - [ ] `DepositConfirmed { deposit_confirmed_at }`
  - [ ] `DepositFailed { deposit_ref: Option<TransferRef>, reason, failed_at }`
- [ ] Update `handle()`:
  - [ ] `DepositInitiated` + `ConfirmDeposit` → emit `DepositConfirmed`
  - [ ] `DepositInitiated` + `FailDeposit` → emit `DepositFailed`
- [ ] Update `apply_transition()` for `DepositConfirmed` and `DepositFailed`
      events
- [ ] Add error `DepositNotInitiated`

### Tests

- [ ] `test_confirm_deposit`
- [ ] `test_cannot_confirm_deposit_before_initiating`
- [ ] `test_fail_deposit_after_initiated`
- [ ] `test_deposit_failed_preserves_deposit_ref_when_available`

### Validation

- [ ] Run `cargo test -q` - all tests pass
- [ ] Run `cargo clippy -- -D clippy::all` - no warnings
- [ ] Run `cargo fmt`

## Task 10. End-to-End Flow Tests

Add comprehensive end-to-end tests covering full rebalancing flows.

### Subtasks

- [ ] Add test `test_complete_alpaca_to_base_full_flow`:
  - [ ] Initiate with Alpaca transfer
  - [ ] Confirm withdrawal
  - [ ] Initiate bridging
  - [ ] Receive attestation
  - [ ] Confirm bridging
  - [ ] Initiate deposit with onchain tx
  - [ ] Confirm deposit
  - [ ] Verify final state is `DepositConfirmed` with all data
  - [ ] Verify view reflects complete flow
- [ ] Add test `test_complete_base_to_alpaca_full_flow`:
  - [ ] Initiate with onchain tx
  - [ ] Confirm withdrawal
  - [ ] Initiate bridging
  - [ ] Receive attestation
  - [ ] Confirm bridging
  - [ ] Initiate deposit with Alpaca transfer
  - [ ] Confirm deposit
  - [ ] Verify final state is `DepositConfirmed` with all data
  - [ ] Verify view reflects complete flow
- [ ] Add test `test_cannot_execute_commands_on_terminal_states`:
  - [ ] Verify `WithdrawalFailed` rejects all commands
  - [ ] Verify `BridgingFailed` rejects all commands
  - [ ] Verify `DepositFailed` rejects all commands
  - [ ] Verify `DepositConfirmed` rejects all commands

### Validation

- [ ] Run `cargo test -q` - all tests pass
- [ ] Run `cargo clippy -- -D clippy::all` - no warnings
- [ ] Run `cargo fmt`

## Task 11. Add Comprehensive Documentation

Document the aggregate's purpose, usage, and architecture.

### Subtasks

- [ ] Add module-level documentation to `src/usdc_rebalance/mod.rs`:
  - [ ] Purpose: Managing cross-chain USDC rebalancing between Alpaca and Base
  - [ ] State flow diagram in ASCII art showing all 11 state transitions grouped
        by phase
  - [ ] Integration points: Alpaca API withdrawals/deposits, CCTP bridge (burn
        on Ethereum/Base, attestation from Circle, mint on Base/Ethereum), Rain
        orderbook vault operations
  - [ ] Error handling: Type-safe transitions, terminal state protection,
        stage-specific failure states
  - [ ] Usage example showing complete AlpacaToBase flow with all commands
  - [ ] Usage example showing complete BaseToAlpaca flow with all commands
  - [ ] CCTP fast transfer details: ~20-30 seconds bridge time, 1bp fee
- [ ] Document each `UsdcRebalance` state variant:
  - [ ] What it represents in the business flow
  - [ ] Which data fields are populated and why
  - [ ] Valid transitions from this state
- [ ] Document each command's purpose and when to use it
- [ ] Document each event's meaning in the domain
- [ ] Add inline comments for non-obvious state transition logic in `handle()`

### Validation

- [ ] Run `cargo doc --open` and review generated documentation
- [ ] Verify all public types have documentation
- [ ] Ensure examples in docs are syntactically valid

## Task 12. Final Validation and Cleanup

Ensure production readiness across all quality metrics.

### Subtasks

- [ ] Run full test suite: `cargo test -q`
- [ ] Run linting: `cargo clippy --all-targets --all-features -- -D clippy::all`
- [ ] Run formatter: `cargo fmt --check` (verify no changes needed)
- [ ] Build release binary: `cargo build --release`
- [ ] Verify migrations are applied: `sqlx migrate info`
- [ ] Review code against AGENTS.md guidelines:
  - [ ] Package by feature: all code in `usdc_rebalance` module ✓
  - [ ] Type modeling: 11-state state machine via enum variants matching SPEC ✓
  - [ ] No `#[allow(clippy::*)]` attributes
  - [ ] Financial data: all Decimal amounts use proper error handling
  - [ ] Proper error propagation: no silent failures or unwrap_or defaults
  - [ ] Comments only where logic isn't self-evident
  - [ ] Visibility levels: prefer restrictive (pub(crate) over pub)
- [ ] Delete PLAN.md before creating PR

### Validation

- [ ] All tests pass
- [ ] Zero clippy warnings
- [ ] Zero formatting changes needed
- [ ] Clean release build
- [ ] Migrations up to date
- [ ] Code adheres to project standards

## Success Criteria

Upon completion, the implementation provides:

1. **Complete 10-state aggregate lifecycle** wrapped in `Lifecycle<T, E>`:
   `Uninitialized` → `Live(WithdrawalInitiated)` →
   `Live(WithdrawalConfirmed)`/`Live(WithdrawalFailed)` →
   `Live(BridgingInitiated)` → `Live(BridgeAttestationReceived)` →
   `Live(Bridged)`/`Live(BridgingFailed)` → `Live(DepositInitiated)` →
   `Live(DepositConfirmed)`/`Live(DepositFailed)`
2. **Bidirectional support**: AlpacaToBase (Ethereum → Base) and BaseToAlpaca
   (Base → Ethereum) directions
3. **Comprehensive tracking**:
   - Withdrawal: TransferRef (AlpacaTransferId or TxHash)
   - Bridge burn: burn_tx_hash, cctp_nonce
   - Bridge attestation: attestation bytes from Circle API
   - Bridge mint: mint_tx_hash
   - Deposit: TransferRef (AlpacaTransferId or TxHash)
4. **Rich failure events**: Stage-specific failure states (WithdrawalFailed,
   BridgingFailed, DepositFailed) that preserve all available context
5. **Self-view projection**: `impl View<Lifecycle<...>> for Lifecycle<...>`
   pattern - no separate View type, queryable state via database-backed read
   model with all timestamps and references
6. **Type-safe state machine**: 10 distinct live states (plus Uninitialized)
   wrapped in `Lifecycle<T, E>` prevent invalid transitions at compile time
7. **Full test coverage**: Unit tests for all transitions, edge cases, and
   failure scenarios at each stage, plus end-to-end tests
8. **Database persistence**: Migration-backed view table created upfront and
   used throughout
9. **SPEC compliance**: Exact match with SPEC.md design (lines 1403-1573)
10. **Production quality**: Zero clippy warnings, comprehensive docs, passing
    tests
