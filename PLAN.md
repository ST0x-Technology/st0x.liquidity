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

- [ ] Add `get_address_by_symbol()` method to `SymbolCache`:
  - Iterate through cached `Address -> symbol` mappings
  - Return `Option<Address>` for reverse lookup

- [ ] Write tests:
  - Reverse lookup returns correct address for cached symbol
  - Reverse lookup returns None for unknown symbol
  - Reverse lookup works after cache population

- [ ] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

---

## Task 2. Define RebalancingTrigger Core Types with Tests

Create `src/rebalancing/trigger.rs` with core types and trigger logic.

### Subtasks

- [ ] Make `Imbalance<T>` and `ImbalanceThreshold` pub(crate) in view.rs

- [ ] Define `RebalancingTriggerConfig`:
  - `equity_threshold: ImbalanceThreshold` (target 0.5, deviation 0.2)
  - `usdc_threshold: ImbalanceThreshold` (target 0.5, deviation 0.3)
  - `wallet: Address` - wallet for receiving minted tokens

- [ ] Define `TriggeredOperation` enum:
  ```rust
  pub(crate) enum TriggeredOperation {
      Mint { symbol: Symbol, quantity: FractionalShares },
      Redemption { symbol: Symbol, quantity: FractionalShares, token: Address },
      UsdcAlpacaToBase { amount: Usdc },
      UsdcBaseToAlpaca { amount: Usdc },
  }
  ```

- [ ] Define `RebalancingTrigger` struct holding:
  - Config
  - `SymbolCache` for reverse lookup (symbol → token address)
  - `Arc<RwLock<InventoryView>>` for shared inventory state
  - `Arc<RwLock<HashSet<Symbol>>>` for equity in-progress tracking
  - `Arc<AtomicBool>` for USDC in-progress tracking
  - `mpsc::Sender<TriggeredOperation>` for triggered operations

- [ ] Implement `check_and_trigger_equity()`:
  - Read inventory, check imbalance for symbol
  - If TooMuchOffchain → send Mint operation
  - If TooMuchOnchain → lookup token address, send Redemption operation
  - Mark symbol as in-progress

- [ ] Implement `check_and_trigger_usdc()`:
  - Read inventory, check USDC imbalance
  - If TooMuchOffchain → send UsdcAlpacaToBase
  - If TooMuchOnchain → send UsdcBaseToAlpaca
  - Set USDC in-progress flag

- [ ] Write tests:
  - TooMuchOffchain triggers mint
  - TooMuchOnchain triggers redemption with correct token address
  - In-progress symbols are skipped
  - USDC TooMuchOffchain triggers AlpacaToBase
  - USDC TooMuchOnchain triggers BaseToAlpaca
  - USDC in-progress flag prevents duplicates
  - Balanced inventory triggers nothing

- [ ] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

---

## Task 3. Implement Position Event Query with Tests

Implement `Query<Lifecycle<Position, Never>>` for RebalancingTrigger.

### Subtasks

- [ ] Implement `Query<Lifecycle<Position, Never>>` for RebalancingTrigger:
  - `dispatch()` receives position events
  - Extract symbol from aggregate_id
  - Apply event to inventory via `apply_position_event()`
  - Call `check_and_trigger_equity()` for that symbol

- [ ] Write tests:
  - Position event updates inventory
  - Position event causing imbalance triggers rebalancing
  - Position event maintaining balance triggers nothing

- [ ] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

---

## Task 4. Implement Mint Event Query with Tests

Implement `Query<Lifecycle<TokenizedEquityMint, Never>>` for RebalancingTrigger.

### Subtasks

- [ ] Implement `Query<Lifecycle<TokenizedEquityMint, Never>>`:
  - `dispatch()` receives mint events
  - Apply to inventory via `apply_mint_event()`
  - On completion/failure events, clear in-progress flag for symbol

- [ ] Write tests:
  - Mint initiation event updates inventory
  - Mint completion clears in-progress flag
  - Mint failure clears in-progress flag

- [ ] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

---

## Task 5. Implement Redemption Event Query with Tests

Implement `Query<Lifecycle<EquityRedemption, Never>>` for RebalancingTrigger.

### Subtasks

- [ ] Implement `Query<Lifecycle<EquityRedemption, Never>>`:
  - `dispatch()` receives redemption events
  - Apply to inventory via `apply_redemption_event()`
  - On completion/failure events, clear in-progress flag for symbol

- [ ] Write tests:
  - Redemption initiation event updates inventory
  - Redemption completion clears in-progress flag
  - Redemption failure clears in-progress flag

- [ ] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

---

## Task 6. Implement USDC Rebalance Event Query with Tests

Implement `Query<Lifecycle<UsdcRebalance, Never>>` for RebalancingTrigger.

### Subtasks

- [ ] Implement `Query<Lifecycle<UsdcRebalance, Never>>`:
  - `dispatch()` receives USDC rebalance events
  - Apply to inventory via `apply_usdc_rebalance_event()`
  - On completion/failure events, clear USDC in-progress flag

- [ ] Write tests:
  - USDC rebalance initiation event updates inventory
  - USDC completion clears in-progress flag
  - USDC failure clears in-progress flag

- [ ] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

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
