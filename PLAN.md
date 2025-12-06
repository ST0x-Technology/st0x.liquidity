# Implementation Plan: Wire Up Rebalancing Triggers (Issue #139)

## Goal

Connect inventory imbalance detection to rebalancing managers to automatically
trigger rebalancing operations when equity or USDC positions deviate beyond
configured thresholds.

---

## Task 1. Implement RebalancingTrigger with Tests

Create `src/rebalancing/trigger.rs` with the trigger coordinator and unit tests.

### Subtasks

- [ ] Define `RebalancingTriggerConfig`:
  - `threshold: ImbalanceThreshold` - single threshold for all assets
  - `check_interval: Duration`
  - `token_addresses: HashMap<Symbol, Address>` - maps symbols to token
    contracts
  - `wallet: Address` - wallet for receiving minted tokens

- [ ] Define `TriggeredOperation` enum:
  ```rust
  pub(crate) enum TriggeredOperation {
      Mint { symbol: Symbol, quantity: FractionalShares },
      Redemption { symbol: Symbol, quantity: FractionalShares },
      UsdcAlpacaToBase { amount: Usdc },
      UsdcBaseToAlpaca { amount: Usdc },
  }
  ```

- [ ] Define `RebalancingTrigger<P, S, ES>` struct holding:
  - Config
  - References to MintManager, RedemptionManager, UsdcRebalanceManager
  - `Arc<RwLock<InventoryView>>` for shared inventory state
  - `Arc<RwLock<HashSet<Symbol>>>` for equity in-progress tracking
  - `Arc<AtomicBool>` for USDC in-progress tracking

- [ ] Implement `check_and_trigger()`:
  - Read InventoryView
  - Check equity imbalances, filter already in-progress, spawn operations
  - Check USDC imbalance, spawn if not in-progress
  - Return list of triggered operations

- [ ] Implement `run_periodic()`:
  - Loop with interval at `check_interval`
  - Call `check_and_trigger()` each tick
  - Log triggered operations

- [ ] Write comprehensive unit tests:
  - TooMuchOffchain triggers mint
  - TooMuchOnchain triggers redemption
  - In-progress symbols are skipped
  - USDC TooMuchOffchain triggers AlpacaToBase
  - USDC TooMuchOnchain triggers BaseToAlpaca
  - USDC in-progress flag prevents duplicates
  - Balanced inventory triggers nothing
  - In-progress cleared after completion
  - In-progress cleared on failure

- [ ] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

---

## Task 2. Add Rebalancing Environment Configuration with Tests

Extend `Env` and `Config` with rebalancing configuration.

### Subtasks

- [ ] Add optional rebalancing fields to `Env`:
  - `rebalancing_enabled: bool` (default false)
  - `rebalancing_check_interval: u64` (default 60)
  - `rebalancing_target_ratio: Decimal` (default 0.5)
  - `rebalancing_deviation: Decimal` (default 0.3)
  - `redemption_wallet: Option<Address>`
  - `ethereum_rpc_url: Option<Url>` (for CCTP on Ethereum)

- [ ] Add `RebalancingConfig` to `Config`:
  ```rust
  pub(crate) struct RebalancingConfig {
      pub(crate) check_interval: Duration,
      pub(crate) threshold: ImbalanceThreshold,
      pub(crate) redemption_wallet: Address,
      pub(crate) ethereum_rpc_url: Url,
  }
  ```

- [ ] Parse rebalancing config in `into_config()` when `rebalancing_enabled` and
      broker is Alpaca

- [ ] Write tests for config parsing:
  - Rebalancing disabled by default
  - Rebalancing enabled parses all fields
  - Missing required fields errors when enabled
  - Default values applied correctly

- [ ] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

---

## Task 3. Integrate RebalancingTrigger into Conductor with Tests

Wire the trigger into ConductorBuilder and spawn it as a background task.

### Subtasks

- [ ] Add `rebalancing_trigger: Option<JoinHandle<()>>` to `Conductor`

- [ ] Add `WithRebalancing` typestate to ConductorBuilder with method:
  ```rust
  pub(crate) fn with_rebalancing(
      self,
      trigger: RebalancingTrigger<P, S, ES>,
  ) -> ConductorBuilder<P, B, WithRebalancing>
  ```

- [ ] Update `spawn()` to spawn rebalancing trigger if configured

- [ ] Update `wait_for_completion()` to await rebalancing task

- [ ] Update `abort_trading_tasks()` and `abort_all()` to abort rebalancing

- [ ] Write tests:
  - Conductor starts without rebalancing when not configured
  - Conductor starts with rebalancing when configured
  - Rebalancing task properly aborted on shutdown

- [ ] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

---

## Task 4. Wire Rebalancing in run_with_broker with Tests

Connect rebalancing to the main startup flow for Alpaca broker.

### Subtasks

- [ ] Update `run_with_broker()` to:
  - Check if config has rebalancing enabled
  - When Alpaca + rebalancing: create services, managers, trigger
  - Pass trigger to ConductorBuilder

- [ ] Create helper to instantiate rebalancing infrastructure:
  - AlpacaTokenizationService (uses existing broker credentials)
  - CQRS frameworks with MemStore for aggregates
  - MintManager, RedemptionManager, UsdcRebalanceManager
  - RebalancingTrigger with shared InventoryView

- [ ] Write integration test verifying Alpaca + rebalancing startup path

- [ ] Run `cargo build`, `cargo test -q`, `rainix-rs-static`, `cargo fmt`

---

## Task 5. Remove All dead_code Allows with Verification

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

1. RebalancingTrigger detects imbalances and invokes managers
2. In-progress tracking prevents concurrent duplicate operations
3. Configuration parsed from environment when Alpaca + rebalancing enabled
4. Conductor spawns rebalancing trigger as background task
5. All `#[allow(dead_code)]` removed from rebalancing modules
6. No `todo!()` macros, no deferrals
7. Comprehensive test coverage for each task
8. All tests pass, linting passes, code formatted
