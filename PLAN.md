# Implementation Plan: InventoryView (Issue #138)

InventoryView is a cross-aggregate projection that tracks inventory across
onchain (Rain orderbook on Base) and offchain (Alpaca) venues. It consumes
events from Position, TokenizedEquityMint, EquityRedemption, and UsdcRebalance
aggregates to maintain a unified view of inventory positions.

## Design Decisions

### Cross-Aggregate Projection Pattern

Unlike single-aggregate views that implement `View<Lifecycle<T, E>>`,
InventoryView is a projection that consumes events from multiple unrelated
aggregates. It provides `apply_*` methods for each event type rather than a
single `update` method. The conductor/manager will call these methods as events
are processed.

### Type-Safe Amounts

Using existing newtypes for amounts:

- `FractionalShares` for equity amounts
- `Usdc` for USDC amounts

Both implement checked arithmetic via `Add<Output = Result<Self, E>>` and
`Sub<Output = Result<Self, E>>`.

### Generic VenueBalance

A single generic `VenueBalance<T>` type parameterized by amount type:

```rust
struct VenueBalance<T> {
    available: T,
    inflight: T,
}

type EquityVenueBalance = VenueBalance<FractionalShares>;
type UsdcVenueBalance = VenueBalance<Usdc>;
```

Operations use standard `+`/`-` operators - no custom trait needed.

### Functional Transformations

VenueBalance operations return new values rather than mutating in place:

```rust
fn move_to_inflight(self, amount: T) -> Result<Self, InventoryError>
```

### Imbalance Detection as Query Methods

The view exposes `detect_imbalance` methods that return `Option<Imbalance>`.
Returning `None` means balanced or rebalancing blocked (inflight operations).
The conductor queries these after each event to trigger rebalancing commands.

### Threshold Configuration

Default values from SPEC.md:

- Equity: target=0.5, deviation=0.2 (triggers at <0.3 or >0.7)
- USDC: target=0.5, deviation=0.3 (triggers at <0.2 or >0.8)

---

## Task 1. Add Checked Arithmetic to Usdc

Extend `Usdc` in `src/threshold.rs` with checked arithmetic matching
`FractionalShares` pattern.

- [ ] Add `UsdcArithmeticError` struct (similar to `ArithmeticError`)
- [ ] Implement `Add<Output = Result<Self, UsdcArithmeticError>>` for `Usdc`
- [ ] Implement `Sub<Output = Result<Self, UsdcArithmeticError>>` for `Usdc`
- [ ] Add `Usdc::ZERO` constant
- [ ] Add unit tests for Usdc arithmetic
- [ ] Run `cargo test -q --lib threshold` and `cargo clippy`

---

## Task 2. Generic VenueBalance with Tests

Create `src/inventory/` module and implement generic `VenueBalance<T>`.

- [ ] Create `src/inventory/mod.rs`
- [ ] Create `src/inventory/venue_balance.rs`
- [ ] Add `mod inventory;` to `src/lib.rs`
- [ ] Create `InventoryError` enum:
  - `InsufficientAvailable`
  - `InsufficientInflight`
  - `EquityArithmetic(ArithmeticError)`
  - `UsdcArithmetic(UsdcArithmeticError)`
- [ ] Create generic `VenueBalance<T>` struct with `available: T`, `inflight: T`
- [ ] Implement `total(self) -> Result<T, E>`
- [ ] Implement `move_to_inflight(self, amount) -> Result<Self, InventoryError>`
- [ ] Implement `confirm_inflight(self, amount) -> Result<Self, InventoryError>`
- [ ] Implement `cancel_inflight(self, amount) -> Result<Self, InventoryError>`
- [ ] Implement `add_available(self, amount) -> Result<Self, InventoryError>`
- [ ] Implement `remove_available(self, amount) -> Result<Self, InventoryError>`
- [ ] Add type aliases: `EquityVenueBalance`, `UsdcVenueBalance`
- [ ] Add unit tests for both type aliases (happy paths and error cases)
- [ ] Run `cargo test -q --lib inventory` and `cargo clippy`

---

## Task 3. Inventory Structs and Imbalance Types

Add remaining core types needed for the view.

- [ ] Create `SymbolInventory` struct:
  - `symbol: Symbol`
  - `onchain: EquityVenueBalance`
  - `offchain: EquityVenueBalance`
  - `last_rebalancing: Option<DateTime<Utc>>`
- [ ] Create `UsdcInventory` struct:
  - `onchain: UsdcVenueBalance`
  - `offchain: UsdcVenueBalance`
  - `last_rebalancing: Option<DateTime<Utc>>`
- [ ] Create `ImbalanceThreshold` struct with `target: Decimal`,
      `deviation: Decimal`
- [ ] Create `EquityImbalance` enum:
  - `TooMuchOnchain { excess: FractionalShares }` - triggers redemption
  - `TooMuchOffchain { excess: FractionalShares }` - triggers mint
- [ ] Create `UsdcImbalance` enum:
  - `TooMuchOnchain { excess: Usdc }` - triggers BaseToAlpaca
  - `TooMuchOffchain { excess: Usdc }` - triggers AlpacaToBase
- [ ] Create `InventoryView` struct:
  - `per_symbol: HashMap<Symbol, SymbolInventory>`
  - `usdc: UsdcInventory`
  - `last_updated: DateTime<Utc>`
- [ ] Derive `Debug`, `Clone`, `Serialize`, `Deserialize`, `PartialEq` as
      appropriate
- [ ] Run `cargo check` and `cargo clippy`

---

## Task 4. Imbalance Detection with Tests

Implement ratio calculation and imbalance detection logic.

- [ ] Implement `SymbolInventory::ratio(&self) -> Option<Decimal>`:
  - Returns `onchain.total() / (onchain.total() + offchain.total())`
  - Returns `None` if total is zero
- [ ] Implement `SymbolInventory::has_inflight(&self) -> bool`
- [ ] Implement
      `SymbolInventory::detect_imbalance(&self, threshold: &ImbalanceThreshold) -> Option<EquityImbalance>`:
  - Returns `None` if `has_inflight()` is true
  - Returns `None` if ratio is within `target ± deviation`
  - Returns `TooMuchOnchain` if ratio > target + deviation
  - Returns `TooMuchOffchain` if ratio < target - deviation
- [ ] Implement same methods for `UsdcInventory` returning
      `Option<UsdcImbalance>`
- [ ] Add tests for ratio calculation edge cases (zero inventory, equal split)
- [ ] Add tests for imbalance detection (balanced, imbalanced, inflight
      blocking)
- [ ] Run `cargo test -q --lib inventory` and `cargo clippy`

---

## Task 5. Position Event Handlers with Tests

Handle trading fills that update available balances.

- [ ] Add
      `InventoryView::get_or_create_symbol(&mut self, symbol: &Symbol) -> &mut SymbolInventory`
- [ ] Implement
      `InventoryView::apply_position_event(&mut self, event: &PositionEvent) -> Result<(), InventoryError>`:
  - `OnChainOrderFilled`: Add to `onchain.available` on Buy, remove on Sell
  - `OffChainOrderFilled`: Add to `offchain.available` on Buy, remove on Sell
  - Other events: Update `last_updated` only
- [ ] Add tests:
  - Onchain buy increases onchain available
  - Onchain sell decreases onchain available
  - Offchain buy increases offchain available
  - Offchain sell decreases offchain available
  - Multiple symbols tracked independently
- [ ] Run `cargo test -q --lib inventory` and `cargo clippy`

---

## Task 6. Mint Event Handlers with Tests

Handle shares to tokens conversion lifecycle.

- [ ] Implement
      `InventoryView::apply_mint_event(&mut self, event: &TokenizedEquityMintEvent) -> Result<(), InventoryError>`:
  - `MintRequested`: No balance change (extract symbol for get_or_create)
  - `MintAccepted`: Move quantity from `offchain.available` to
    `offchain.inflight`
  - `TokensReceived`: Remove from `offchain.inflight`, add to
    `onchain.available`
  - `MintCompleted`: Update `last_rebalancing` timestamp
  - `MintFailed`: Move from `offchain.inflight` back to `offchain.available`
- [ ] Add tests:
  - Full mint lifecycle (request → accept → receive → complete)
  - Mint failure recovery (request → accept → fail)
  - Inflight blocks imbalance detection during mint
- [ ] Run `cargo test -q --lib inventory` and `cargo clippy`

---

## Task 7. Redemption Event Handlers with Tests

Handle tokens to shares conversion lifecycle.

- [ ] Implement
      `InventoryView::apply_redemption_event(&mut self, event: &EquityRedemptionEvent) -> Result<(), InventoryError>`:
  - `TokensSent`: Move quantity from `onchain.available` to `onchain.inflight`
  - `Detected`: No balance change
  - `Completed`: Remove from `onchain.inflight`, add to `offchain.available`
  - `Failed`: Move from `onchain.inflight` back to `onchain.available`
- [ ] Add tests:
  - Full redemption lifecycle (send → detect → complete)
  - Redemption failure at TokensSent stage
  - Redemption failure at Pending stage
  - Inflight blocks imbalance detection during redemption
- [ ] Run `cargo test -q --lib inventory` and `cargo clippy`

---

## Task 8. USDC Rebalance Event Handlers with Tests

Handle USDC transfers between venues via CCTP.

- [ ] Implement
      `InventoryView::apply_usdc_rebalance_event(&mut self, event: &UsdcRebalanceEvent, direction: &RebalanceDirection) -> Result<(), InventoryError>`:
  - `Initiated`:
    - AlpacaToBase: Move from `usdc.offchain.available` to
      `usdc.offchain.inflight`
    - BaseToAlpaca: Move from `usdc.onchain.available` to
      `usdc.onchain.inflight`
  - `WithdrawalConfirmed`: No change
  - `WithdrawalFailed`: Cancel inflight back to source available
  - `BridgingInitiated`, `BridgeAttestationReceived`: No change
  - `Bridged`: Move from source inflight to destination available
  - `BridgingFailed`: Cancel inflight back to source available
  - `DepositInitiated`: No change
  - `DepositConfirmed`: Update `usdc.last_rebalancing` timestamp
  - `DepositFailed`: Log warning (USDC on destination but not deposited)
- [ ] Add tests:
  - AlpacaToBase full lifecycle
  - BaseToAlpaca full lifecycle
  - Withdrawal failure recovery
  - Bridging failure recovery
  - Inflight blocks imbalance detection during USDC rebalance
- [ ] Run `cargo test -q --lib inventory` and `cargo clippy`

---

## Task 9. InventoryView Query Methods and Final Validation

Add query methods and run final validation.

- [ ] Implement
      `InventoryView::check_equity_imbalances(&self, thresholds: &HashMap<Symbol, ImbalanceThreshold>) -> Vec<(Symbol, EquityImbalance)>`
- [ ] Implement
      `InventoryView::check_usdc_imbalance(&self, threshold: &ImbalanceThreshold) -> Option<UsdcImbalance>`
- [ ] Implement
      `InventoryView::get_symbol_inventory(&self, symbol: &Symbol) -> Option<&SymbolInventory>`
- [ ] Implement `InventoryView::usdc_inventory(&self) -> &UsdcInventory`
- [ ] Add integration tests:
  - View correctly identifies multiple imbalanced symbols
  - View correctly reports USDC imbalance
  - View blocks rebalancing when inflight operations exist
- [ ] Run full test suite: `cargo test -q`
- [ ] Run `cargo clippy --all-targets -- -D clippy::all`
- [ ] Run `cargo fmt`
