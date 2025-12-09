# Implementation Plan: Wire Up Rebalancing Triggers (Issue #139)

## Goal

Connect inventory imbalance detection to rebalancing managers using an
event-driven architecture. When aggregate events update the InventoryView and
create an imbalance, automatically trigger the appropriate rebalancing
operation.

## Design Rationale

The cqrs-es `Query` trait provides push-based event delivery - after events are
committed to the event store, they are immediately dispatched to all registered
query processors via `dispatch()`. This enables a reactive architecture:

1. RebalancingTrigger implements cqrs-es `Query` trait
2. On each event dispatch, update inventory state via existing `apply_*` methods
3. After update, check for imbalances using existing threshold logic
4. If imbalanced and no operation in-progress, send operation to channel

This eliminates polling entirely - rebalancing triggers reactively when
inventory state changes.

---

## Task 1. Add SymbolCache Reverse Lookup with Tests

Extend `SymbolCache` to support symbol-to-address reverse lookup for
redemptions.

### Subtasks

- [x] Add `get_address()` method to `SymbolCache`:
  - Iterate through cached `Address -> symbol` mappings
  - Return `Option<Address>` for reverse lookup

- [x] Write tests:
  - Reverse lookup returns correct address for cached symbol
  - Reverse lookup returns None for unknown symbol
  - Reverse lookup works after cache population (multiple entries)

- [x] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

### Changes Made

- Added `get_address(&self, symbol: &str) -> Option<Address>` method to
  `SymbolCache` in `src/symbol/cache.rs:50-60`
- Added 3 new tests:
  - `test_get_address_returns_correct_address` - verifies lookup works
  - `test_get_address_returns_none_for_unknown_symbol` - verifies None for
    unknown
  - `test_get_address_works_with_multiple_entries` - verifies correct address
    returned when cache has multiple symbols

---

## Task 2. Define RebalancingTrigger Core Types with Tests

Create `src/rebalancing/trigger.rs` with core types and trigger logic.

### Subtasks

- [x] Make `Imbalance<T>` and `ImbalanceThreshold` pub(crate) in
      `src/inventory/view.rs` and re-export from `src/inventory/mod.rs`

- [x] Define `RebalancingTriggerConfig`:
  - `equity_threshold: ImbalanceThreshold` (target 0.5, deviation 0.2)
  - `usdc_threshold: ImbalanceThreshold` (target 0.5, deviation 0.3)
  - `wallet: Address` - wallet for receiving minted tokens

- [x] Define `TriggeredOperation` enum:
  ```rust
  pub(crate) enum TriggeredOperation {
      Mint { symbol: Symbol, quantity: FractionalShares },
      Redemption { symbol: Symbol, quantity: FractionalShares, token: Address },
      UsdcAlpacaToBase { amount: Usdc },
      UsdcBaseToAlpaca { amount: Usdc },
  }
  ```

- [x] Define `RebalancingTrigger` struct holding:
  - Config
  - `SymbolCache` for reverse lookup (symbol → token address)
  - `Arc<RwLock<InventoryView>>` for shared inventory state
  - `Arc<RwLock<HashSet<Symbol>>>` for equity in-progress tracking
  - `Arc<AtomicBool>` for USDC in-progress tracking
  - `mpsc::Sender<TriggeredOperation>` for triggered operations

- [x] Implement `check_and_trigger_equity()`:
  - Read inventory, check imbalance for symbol
  - If TooMuchOffchain → send Mint operation
  - If TooMuchOnchain → lookup token address, send Redemption operation
  - Mark symbol as in-progress

- [x] Implement `check_and_trigger_usdc()`:
  - Read inventory, check USDC imbalance
  - If TooMuchOffchain → send UsdcAlpacaToBase
  - If TooMuchOnchain → send UsdcBaseToAlpaca
  - Set USDC in-progress flag

- [x] Write tests:
  - In-progress symbols are skipped
  - USDC in-progress flag prevents duplicates
  - Balanced inventory triggers nothing
  - clear_equity_in_progress removes symbol from set
  - clear_usdc_in_progress resets flag

- [x] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

### Changes Made

- `src/inventory/view.rs`:
  - Made `Imbalance<T>` pub(crate) (line 31)
  - Made `ImbalanceThreshold` and its fields pub(crate) (lines 40-44)
  - Made `InventoryView` pub(crate) (line 197)
  - Added `Default` impl for `Inventory<T>` (lines 115-122)
  - Added `Default` impl for `InventoryView` (lines 240-247)
  - Changed `usdc()` and `get_equity()` from pub(crate) to private (lines
    215, 220) since they expose private `Inventory<T>` type

- `src/inventory/venue_balance.rs`:
  - Added `Default` impl for `VenueBalance<T>` (lines 9-15)
  - Made `InventoryError<T>` pub(crate) to match visibility of
    `InventoryViewError`

- `src/inventory/mod.rs`:
  - Added re-exports:
    `pub(crate) use view::{Imbalance, ImbalanceThreshold,
    InventoryView};`

- `src/rebalancing/trigger/` (new module directory):

  - `equity.rs`:
    - `EquityTriggerSkip` enum - typed error documenting failure modes
      (AlreadyInProgress, NoImbalance, TokenNotInCache)
    - `InProgressGuard` - RAII guard for equity in-progress claims with
      automatic cleanup on drop and `defuse()` to prevent release on success
    - `check_imbalance_and_build_operation()` - checks inventory for equity
      imbalance and returns appropriate Mint or Redemption operation
    - 4 tests for guard behavior and balanced inventory

  - `usdc.rs`:
    - `UsdcTriggerSkip` enum - typed error documenting failure modes
      (AlreadyInProgress, NoImbalance)
    - `InProgressGuard` - RAII guard for USDC in-progress claims using atomic
      compare_exchange for lock-free claiming
    - `check_imbalance_and_build_operation()` - checks inventory for USDC
      imbalance and returns appropriate bridging operation
    - 4 tests for guard behavior and balanced inventory

  - `mod.rs`:
    - `RebalancingTriggerConfig` struct with equity/usdc thresholds and wallet
    - `TriggeredOperation` enum with Mint, Redemption, UsdcAlpacaToBase,
      UsdcBaseToAlpaca variants
    - `RebalancingTrigger` struct with inventory state, in-progress tracking,
      and mpsc sender
    - `check_and_trigger_equity()` returns
      `Result<TriggeredOperation, EquityTriggerSkip>`
      - Uses RAII guard for automatic cleanup on any error path
    - `check_and_trigger_usdc()` returns
      `Result<TriggeredOperation, UsdcTriggerSkip>`
      - Uses RAII guard for automatic cleanup on any error path
    - `clear_equity_in_progress()` and `clear_usdc_in_progress()` helpers
    - Re-exports `EquityTriggerSkip` and `UsdcTriggerSkip`
    - 7 tests covering: in-progress errors, balanced inventory, clear methods

- `src/rebalancing/mod.rs`:
  - Added `mod trigger;`

---

## Task 3. Implement Position Event Query with Tests

Implement `Query<Lifecycle<Position, ArithmeticError<FractionalShares>>>` for
RebalancingTrigger.

### Subtasks

- [x] Implement `Query<Lifecycle<Position, ArithmeticError<FractionalShares>>>`
      for RebalancingTrigger:
  - `dispatch()` receives position events
  - Extract symbol from aggregate_id
  - Apply event to inventory via `apply_position_event()`
  - Call `check_and_trigger_equity()` for that symbol

- [x] Write tests:
  - Position event updates inventory
  - Position event causing imbalance triggers rebalancing
  - Position event maintaining balance triggers nothing
  - Position event for unknown symbol logs error without panic

- [x] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

### Changes Made

- `src/rebalancing/trigger/mod.rs`:
  - Added `impl Query<Lifecycle<Position, ArithmeticError<FractionalShares>>>`
    with async `dispatch()` method
  - `dispatch()` parses aggregate_id as Symbol, applies each event to inventory,
    and checks for equity imbalance
  - Added `apply_position_event_and_check()` - coordinates inventory update and
    trigger check
  - Added `apply_position_event_to_inventory()` - handles RwLock acquisition,
    applies event, returns `Result<(), InventoryViewError>`
  - Added 4 tests:
    - `position_event_for_unknown_symbol_logs_error_without_panic`
    - `position_event_updates_inventory`
    - `position_event_maintaining_balance_triggers_nothing`
    - `position_event_causing_imbalance_triggers_mint`

- `src/inventory/view.rs`:
  - Added `#[cfg(test)] with_equity(symbol)` builder method for test setup
  - Fixed `apply_position_event()` to extract timestamp from the event itself
    instead of taking a `now` parameter - this ensures replay consistency per
    ES/CQRS principles

- `src/inventory/mod.rs`:
  - Added `InventoryViewError` to re-exports

- `src/position.rs`:
  - Added `PositionEvent::timestamp()` method to extract the timestamp from any
    event variant
  - Added 7 tests for timestamp extraction (one per event variant):
    - `timestamp_returns_migrated_at_for_migrated_event`
    - `timestamp_returns_initialized_at_for_initialized_event`
    - `timestamp_returns_seen_at_for_onchain_order_filled_event`
    - `timestamp_returns_placed_at_for_offchain_order_placed_event`
    - `timestamp_returns_broker_timestamp_for_offchain_order_filled_event`
    - `timestamp_returns_failed_at_for_offchain_order_failed_event`
    - `timestamp_returns_updated_at_for_threshold_updated_event`

---

## Task 4. Implement Mint Event Query with Tests

Implement `Query<Lifecycle<TokenizedEquityMint, Never>>` for RebalancingTrigger.

### Subtasks

- [x] Implement `Query<Lifecycle<TokenizedEquityMint, Never>>`:
  - `dispatch()` receives mint events
  - Apply to inventory via `apply_mint_event()`
  - On completion/failure events, clear in-progress flag for symbol

- [x] Write tests:
  - Mint initiation event updates inventory
  - Mint completion clears in-progress flag
  - Mint failure clears in-progress flag

- [x] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

### Changes Made

- `src/rebalancing/trigger/mod.rs`:
  - Added import for `Lifecycle`, `Never`, `TokenizedEquityMint`,
    `TokenizedEquityMintEvent`, and `chrono::Utc`
  - Added `impl Query<Lifecycle<TokenizedEquityMint, Never>>` with async
    `dispatch()` method
  - `dispatch()` extracts symbol/quantity from `MintRequested` event, applies
    all events to inventory, and clears in-progress flag on terminal events
  - Added `extract_mint_info()` helper to find symbol and quantity from
    `MintRequested` event
  - Added `has_terminal_mint_event()` helper to detect terminal events
    (MintCompleted, MintRejected, MintAcceptanceFailed, TokenReceiptFailed)
  - Added `apply_mint_event_to_inventory()` helper to apply mint events to
    inventory
  - Added 8 tests:
    - `mint_event_updates_inventory` - verifies MintAccepted moves shares to
      inflight
    - `mint_completion_clears_in_progress_flag` - verifies MintCompleted is
      terminal
    - `mint_rejection_clears_in_progress_flag` - verifies MintRejected is
      terminal
    - `mint_acceptance_failure_clears_in_progress_flag` - verifies
      MintAcceptanceFailed is terminal
    - `mint_token_receipt_failure_clears_in_progress_flag` - verifies
      TokenReceiptFailed is terminal
    - `extract_mint_info_returns_symbol_and_quantity` - verifies extraction from
      MintRequested
    - `extract_mint_info_returns_none_without_mint_requested` - verifies None
      when no MintRequested
    - `has_terminal_mint_event_returns_false_for_non_terminal` - verifies
      non-terminal events

---

## Task 5. Implement Redemption Event Query with Tests

Implement `Query<Lifecycle<EquityRedemption, Never>>` for RebalancingTrigger.

### Subtasks

- [x] Implement `Query<Lifecycle<EquityRedemption, Never>>`:
  - `dispatch()` receives redemption events
  - Apply to inventory via `apply_redemption_event()`
  - On completion/failure events, clear in-progress flag for symbol

- [x] Write tests:
  - Redemption initiation event updates inventory
  - Redemption completion clears in-progress flag
  - Redemption failure clears in-progress flag

- [x] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

### Changes Made

- `src/rebalancing/trigger/mod.rs`:
  - Added import for `EquityRedemption` and `EquityRedemptionEvent`
  - Added `impl Query<Lifecycle<EquityRedemption, Never>>` with async
    `dispatch()` method
  - `dispatch()` extracts symbol/quantity from `TokensSent` event, applies all
    events to inventory, and clears in-progress flag on terminal events
  - Added `extract_redemption_info()` helper to find symbol and quantity from
    `TokensSent` event
  - Added `has_terminal_redemption_event()` helper to detect terminal events
    (Completed, TokenSendFailed, DetectionFailed, RedemptionRejected)
  - Added `apply_redemption_event_to_inventory()` helper to apply redemption
    events to inventory
  - Added 8 tests:
    - `redemption_event_updates_inventory` - verifies TokensSent moves shares to
      inflight
    - `redemption_completion_clears_in_progress_flag` - verifies Completed is
      terminal
    - `redemption_token_send_failure_clears_in_progress_flag` - verifies
      TokenSendFailed is terminal
    - `redemption_detection_failure_clears_in_progress_flag` - verifies
      DetectionFailed is terminal
    - `redemption_rejection_clears_in_progress_flag` - verifies
      RedemptionRejected is terminal
    - `extract_redemption_info_returns_symbol_and_quantity` - verifies
      extraction from TokensSent
    - `extract_redemption_info_returns_none_without_tokens_sent` - verifies None
      when no TokensSent
    - `has_terminal_redemption_event_returns_false_for_non_terminal` - verifies
      non-terminal events

---

## Task 6. Implement USDC Rebalance Event Query with Tests

Implement `Query<Lifecycle<UsdcRebalance, Never>>` for RebalancingTrigger.

### Subtasks

- [x] Implement `Query<Lifecycle<UsdcRebalance, Never>>`:
  - `dispatch()` receives USDC rebalance events
  - Apply to inventory via `apply_usdc_rebalance_event()`
  - On completion/failure events, clear USDC in-progress flag

- [x] Write tests:
  - USDC rebalance initiation event updates inventory
  - USDC completion clears in-progress flag
  - USDC failure clears in-progress flag

- [x] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

### Changes Made

- `src/rebalancing/trigger/mod.rs`:
  - Added import for `RebalanceDirection`, `UsdcRebalance`, `UsdcRebalanceEvent`
  - Added `impl Query<Lifecycle<UsdcRebalance, Never>>` with async `dispatch()`
    method
  - `dispatch()` extracts direction/amount from `Initiated` event, applies all
    events to inventory, and clears USDC in-progress flag on terminal events
  - Added `extract_usdc_rebalance_info()` helper to find direction and amount
    from `Initiated` event
  - Added `has_terminal_usdc_rebalance_event()` helper to detect terminal events
    (DepositConfirmed, WithdrawalFailed, BridgingFailed, DepositFailed)
  - Added `apply_usdc_rebalance_event_to_inventory()` helper to apply USDC
    rebalance events to inventory
  - Added 7 tests:
    - `usdc_rebalance_completion_clears_in_progress_flag` - verifies
      DepositConfirmed is terminal
    - `usdc_withdrawal_failure_clears_in_progress_flag` - verifies
      WithdrawalFailed is terminal
    - `usdc_bridging_failure_clears_in_progress_flag` - verifies BridgingFailed
      is terminal
    - `usdc_deposit_failure_clears_in_progress_flag` - verifies DepositFailed is
      terminal
    - `extract_usdc_rebalance_info_returns_direction_and_amount` - verifies
      extraction from Initiated
    - `extract_usdc_rebalance_info_returns_none_without_initiated` - verifies
      None when no Initiated
    - `has_terminal_usdc_rebalance_event_returns_false_for_non_terminal` -
      verifies non-terminal events

---

## Task 7. Implement Operation Executor with Tests

Create `src/rebalancing/executor.rs` that consumes triggered operations and
executes them via managers.

### Subtasks

- [ ] Define `OperationExecutor<P, S, ES>` struct holding:
  - `MintManager<P, S, ES>`
  - `RedemptionManager<P, S, ES>`
  - `UsdcRebalanceManager<P, S, ES>`
  - `mpsc::Receiver<TriggeredOperation>`

- [ ] Implement `run()` async method:
  - Loop receiving from channel
  - Match on operation type, call appropriate manager
  - Log results

- [ ] Write tests:
  - Mint operation dispatches to MintManager
  - Redemption operation dispatches to RedemptionManager
  - USDC operations dispatch to UsdcRebalanceManager
  - Channel closed terminates run loop

- [ ] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

---

## Task 8. Add Rebalancing Environment Configuration with Tests

Extend `Env` and `Config` with rebalancing configuration.

### Subtasks

- [ ] Add optional rebalancing fields to `Env`:
  - `rebalancing_enabled: bool` (default false)
  - `equity_target_ratio: Decimal` (default 0.5)
  - `equity_deviation: Decimal` (default 0.2)
  - `usdc_target_ratio: Decimal` (default 0.5)
  - `usdc_deviation: Decimal` (default 0.3)
  - `redemption_wallet: Option<Address>`
  - `ethereum_rpc_url: Option<Url>` (for CCTP on Ethereum)

- [ ] Add `RebalancingConfig` to `Config`:
  ```rust
  pub(crate) struct RebalancingConfig {
      pub(crate) equity_threshold: ImbalanceThreshold,
      pub(crate) usdc_threshold: ImbalanceThreshold,
      pub(crate) redemption_wallet: Address,
      pub(crate) ethereum_rpc_url: Url,
  }
  ```

- [ ] Parse rebalancing config in `into_config()` when `rebalancing_enabled` and
      broker is Alpaca

- [ ] Write tests:
  - Rebalancing disabled by default
  - Rebalancing enabled parses all fields
  - Missing required fields errors when enabled
  - Default values applied correctly

- [ ] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

---

## Task 9. Integrate into Conductor with Tests

Wire the rebalancing executor into ConductorBuilder.

### Subtasks

- [ ] Add `rebalancing_executor: Option<JoinHandle<()>>` to `Conductor`

- [ ] Add `WithRebalancing` typestate to ConductorBuilder with method:
  ```rust
  pub(crate) fn with_rebalancing(
      self,
      executor: OperationExecutor<P, S, ES>,
  ) -> ConductorBuilder<P, B, WithRebalancing>
  ```

- [ ] Update `spawn()` to spawn executor task if configured

- [ ] Update `wait_for_completion()` to await executor task

- [ ] Update `abort_trading_tasks()` and `abort_all()` to abort executor

- [ ] Write tests:
  - Conductor starts without rebalancing when not configured
  - Conductor starts with rebalancing when configured
  - Executor task properly aborted on shutdown

- [ ] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

---

## Task 10. Wire Rebalancing in run_with_broker with Tests

Connect rebalancing to the main startup flow for Alpaca broker.

### Subtasks

- [ ] Update `run_with_broker()` to:
  - Check if config has rebalancing enabled
  - When Alpaca + rebalancing: create services, managers, trigger, executor
  - Register RebalancingTrigger as query with CQRS frameworks
  - Pass executor to ConductorBuilder

- [ ] Create helper to instantiate rebalancing infrastructure:
  - Create operation channel (tokio mpsc)
  - AlpacaTokenizationService (uses existing broker credentials)
  - CQRS frameworks with MemStore, with RebalancingTrigger as query
  - MintManager, RedemptionManager, UsdcRebalanceManager
  - OperationExecutor with receiver end of channel
  - RebalancingTrigger with sender end of channel

- [ ] Wire Position aggregate's CQRS to include RebalancingTrigger as query

- [ ] Write integration test verifying Alpaca + rebalancing startup path

- [ ] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

---

## Task 11. Remove All dead_code Allows with Verification

Remove `#[allow(dead_code)]` and verify everything compiles.

### Subtasks

- [ ] Remove from `mod alpaca_tokenization` in lib.rs
- [ ] Remove from `mod cctp` in lib.rs
- [ ] Remove from `mod tokenized_equity_mint` in lib.rs
- [ ] Remove from `mod equity_redemption` in lib.rs
- [ ] Remove from `mod usdc_rebalance` in lib.rs
- [ ] Remove from `mod rebalancing` in lib.rs
- [ ] Remove TODO(#135), TODO(#137), TODO(#139) comments

- [ ] Run `cargo build` - must succeed with no warnings
- [ ] Run `cargo test -q` - all tests pass
- [ ] Run `rainix-rs-static` - linting passes
- [ ] Run `cargo fmt`

---

## Completion Criteria

1. RebalancingTrigger implements Query trait for all relevant aggregates
2. Events automatically update InventoryView and trigger rebalancing
3. In-progress tracking prevents concurrent duplicate operations
4. Configuration parsed from environment when Alpaca + rebalancing enabled
5. Conductor spawns operation executor as background task
6. All `#[allow(dead_code)]` removed from rebalancing modules
7. No `todo!()` macros, no deferrals
8. Comprehensive test coverage for each task
9. All tests pass, linting passes, code formatted
