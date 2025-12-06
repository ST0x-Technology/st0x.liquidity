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

### Generic Types

All inventory types are generic over the amount type to avoid code duplication:

```rust
struct VenueBalance<T> {
    available: T,
    inflight: T,
}

struct Inventory<T> {
    onchain: VenueBalance<T>,
    offchain: VenueBalance<T>,
    last_rebalancing: Option<DateTime<Utc>>,
}

enum Imbalance<T> {
    TooMuchOnchain { excess: T },
    TooMuchOffchain { excess: T },
}
```

Concrete types are created via type parameters: `Inventory<FractionalShares>`,
`Inventory<Usdc>`, `Imbalance<FractionalShares>`, `Imbalance<Usdc>`.

Operations use standard `+`/`-` operators with `HasZero` trait for zero checks.

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

- [x] Add `UsdcArithmeticError` struct (similar to `ArithmeticError`)
- [x] Implement `Add<Output = Result<Self, UsdcArithmeticError>>` for `Usdc`
- [x] Implement `Sub<Output = Result<Self, UsdcArithmeticError>>` for `Usdc`
- [x] Add `Usdc::ZERO` constant
- [x] Add unit tests for Usdc arithmetic
- [x] Run `cargo test -q --lib threshold` and `cargo clippy`

**Changes made:**

- Added `UsdcArithmeticError` struct in `src/threshold.rs:12-18`
- Implemented `Add` for `Usdc` in `src/threshold.rs:32-44`
- Implemented `Sub` for `Usdc` in `src/threshold.rs:46-59`
- Added `Usdc::ZERO` constant in `src/threshold.rs:21`
- Added 5 unit tests for Usdc arithmetic (`usdc_add_succeeds`,
  `usdc_sub_succeeds`, `usdc_add_overflow_returns_error`,
  `usdc_sub_overflow_returns_error`, `usdc_zero_constant`)

---

## Task 2. Generic VenueBalance with Tests

Create `src/inventory/` module and implement generic `VenueBalance<T>`.

- [x] Create `src/inventory/mod.rs`
- [x] Create `src/inventory/venue_balance.rs`
- [x] Add `mod inventory;` to `src/lib.rs`
- [x] Create generic `InventoryError<T>` enum with `#[from]` for arithmetic
- [x] Create generic `VenueBalance<T>` struct with `available: T`, `inflight: T`
- [x] Implement `total(self) -> Result<T, ArithmeticError<T>>`
- [x] Implement
      `move_to_inflight(self, amount) -> Result<Self, InventoryError<T>>`
- [x] Implement
      `confirm_inflight(self, amount) -> Result<Self, InventoryError<T>>`
- [x] Implement
      `cancel_inflight(self, amount) -> Result<Self, InventoryError<T>>`
- [x] Implement `add_available(self, amount) -> Result<Self, InventoryError<T>>`
- [x] Implement
      `remove_available(self, amount) -> Result<Self, InventoryError<T>>`
- [x] Add type aliases: `EquityVenueBalance`, `UsdcVenueBalance`
- [x] Add unit tests for both type aliases (happy paths and error cases)
- [x] Run `cargo test -q --lib inventory` and `cargo clippy`

**Changes made:**

- Created `src/inventory/mod.rs` with module declaration
- Created `src/inventory/venue_balance.rs` with:
  - Generic `InventoryError<T>` with `InsufficientAvailable`,
    `InsufficientInflight`, and `Arithmetic(#[from] ArithmeticError<T>)`
    variants
  - Generic `VenueBalance<T>` with field-level docstrings
  - All required methods using functional transformation pattern
  - Type aliases `EquityVenueBalance` and `UsdcVenueBalance`
  - 13 unit tests covering all operations and error cases
- Added `mod inventory;` to `src/lib.rs`
- Made `ArithmeticError<T>` generic in `src/shares.rs` (was non-generic)
- Added `HasZero` trait to `src/shares.rs` with default `is_zero`/`is_negative`
  impls
- Implemented `HasZero` for `FractionalShares` and `Usdc`
- Removed duplicate `UsdcArithmeticError` from `src/threshold.rs` (now uses
  generic)
- Updated `position.rs` to use `ArithmeticError<FractionalShares>`

---

## Task 3. Inventory Structs and Imbalance Types

Add remaining core types needed for the view using generics.

- [x] Create generic `Imbalance<T>` enum with `TooMuchOnchain { excess: T }` and
      `TooMuchOffchain { excess: T }` variants
- [x] Create `ImbalanceThreshold` struct with `target: Decimal`,
      `deviation: Decimal`
- [x] Create generic `Inventory<T>` struct with `onchain: VenueBalance<T>`,
      `offchain: VenueBalance<T>`, `last_rebalancing: Option<DateTime<Utc>>`
- [x] Create `InventoryView` struct with
      `equity: HashMap<Symbol,
      Inventory<FractionalShares>>`,
      `usdc: Inventory<Usdc>`, `last_updated:
      DateTime<Utc>`
- [x] Derive appropriate traits
- [x] Run `cargo check` and `cargo clippy` (passes with expected dead code
      warnings)

**Changes made:**

- Created `src/inventory/view.rs` with:
  - Generic `Imbalance<T>` enum (replaces separate
    `EquityImbalance`/`UsdcImbalance`)
  - `ImbalanceThreshold` struct for configuring rebalancing triggers
  - Generic `Inventory<T>` struct (replaces separate
    `SymbolInventory`/`UsdcInventory`)
  - `InventoryView` struct using `HashMap<Symbol, Inventory<FractionalShares>>`
    for equity and `Inventory<Usdc>` for USDC
- Updated `src/inventory/mod.rs` to include `view` module
- Removed unused type aliases from `venue_balance.rs`

---

## Task 4. Imbalance Detection with Tests

Implement ratio calculation and imbalance detection logic on `Inventory<T>`.

- [x] Implement `Inventory<T>::ratio(&self) -> Option<Decimal>`:
  - Returns `onchain.total() / (onchain.total() + offchain.total())`
  - Returns `None` if total is zero
- [x] Implement `Inventory<T>::has_inflight(&self) -> bool`
- [x] Implement
      `Inventory<T>::detect_imbalance(&self, threshold: &ImbalanceThreshold) -> Option<Imbalance<T>>`:
  - Returns `None` if `has_inflight()` is true
  - Returns `None` if ratio is within `target +- deviation`
  - Returns `TooMuchOnchain` if ratio > target + deviation
  - Returns `TooMuchOffchain` if ratio < target - deviation
- [x] Add tests for ratio calculation edge cases (zero inventory, equal split)
- [x] Add tests for imbalance detection (balanced, imbalanced, inflight
      blocking)
- [x] Run `cargo test -q --lib inventory` and `cargo clippy`

**Changes made:**

- Added `From<FractionalShares> for Decimal` in `src/shares.rs`
- Added `From<Usdc> for Decimal` in `src/threshold.rs`
- Added `Mul<Decimal>` for `FractionalShares` with checked arithmetic
- Added `Mul<Decimal>` for `Usdc` with checked arithmetic
- Made `VenueBalance::new`, `total`, `has_inflight` `pub(super)` for use in view
- Implemented `Inventory<T>::ratio()`, `has_inflight()`, `detect_imbalance()`
- Added 16 tests in `view.rs` covering:
  - `ratio()`: zero total, equal split, all onchain, all offchain, inflight
  - `has_inflight()`: no inflight, onchain inflight, offchain inflight, both
  - `detect_imbalance()`: balanced, inflight blocking, zero total, too much
    onchain, too much offchain, boundary cases
- Added 3 tests in `shares.rs`: `into_decimal`, `mul_decimal`, overflow
- Added 3 tests in `threshold.rs`: `into_decimal`, `mul_decimal`, overflow

---

## Task 5. Position Event Handlers with Tests

Handle trading fills that update available balances.

- [ ] Add
      `InventoryView::get_or_create_equity(&mut self, symbol: &Symbol) -> &mut Inventory<FractionalShares>`
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
      `InventoryView::check_equity_imbalances(&self, thresholds: &HashMap<Symbol, ImbalanceThreshold>) -> Vec<(Symbol, Imbalance<FractionalShares>)>`
- [ ] Implement
      `InventoryView::check_usdc_imbalance(&self, threshold: &ImbalanceThreshold) -> Option<Imbalance<Usdc>>`
- [ ] Implement
      `InventoryView::get_equity(&self, symbol: &Symbol) -> Option<&Inventory<FractionalShares>>`
- [ ] Implement `InventoryView::usdc(&self) -> &Inventory<Usdc>`
- [ ] Add integration tests:
  - View correctly identifies multiple imbalanced symbols
  - View correctly reports USDC imbalance
  - View blocks rebalancing when inflight operations exist
- [ ] Run full test suite: `cargo test -q`
- [ ] Run `cargo clippy --all-targets -- -D clippy::all`
- [ ] Run `cargo fmt`
