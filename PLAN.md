# Plan: Extend InventoryView with In-Flight Balance Tracking (#433)

## Context

InventoryPollingService polls 6+ locations but InventoryView only processes 4.
The in-flight location events (`EthereumCash`, `BaseWalletCash`) are explicitly
ignored in the rebalancing trigger:

```rust
// src/rebalancing/trigger/mod.rs:664
EthereumCash { .. } | BaseWalletCash { .. } => Ok(inventory.clone()),
```

Capital at intermediate locations is invisible to imbalance detection. If USDC
is stuck on Ethereum between bridging steps, the system doesn't know and may
trigger unnecessary rebalancing or miscount total capital.

### Dependency Status

- #429 (Ethereum USDC), #430 (Base USDC), #432 (wrapped equity) — **merged**
- #428 (Alpaca USDC) — PR #485 **open**
- #431 (unwrapped equity) — PR #461 **open, blocked**

PRs #461/#485 add `BaseWalletUnwrappedEquity`, `BaseWalletWrappedEquity`,
`AlpacaWalletCash` variants. Not on master yet.

### What We Can Do Now

Everything that touches `EthereumCash` and `BaseWalletCash` (already on master).
The data model, types, imbalance logic, and unit tests for all 5 locations can
be built now. Wiring the 3 not-yet-landed variants is deferred.

## Design

### Two Kinds of "In-Flight"

1. **Transfer inflight** (existing `VenueBalance.inflight`): Bookkeeping for
   system-initiated transfers. Managed by `TransferOp::Start/Complete/Cancel`.
   Suppresses imbalance detection entirely.

2. **Location balances** (new): Polled balances at intermediate locations.
   Observed reality. Affect the imbalance ratio denominator.

These are complementary, not conflicting.

### Data Model

```rust
// src/inventory/view.rs

/// Locations where cash (USDC) can sit between primary venues.
enum InFlightCashLocation {
    Ethereum,
    BaseWallet,
    AlpacaCryptoWallet,
}

/// Locations where equity tokens can sit between primary venues.
enum InFlightEquityLocation {
    BaseUnwrapped,
    BaseWrapped,
}

struct InventoryView {
    usdc: Inventory<Usdc>,
    equities: HashMap<Symbol, Inventory<FractionalShares>>,
    inflight_cash: HashMap<InFlightCashLocation, Usdc>,
    inflight_equity: HashMap<(Symbol, InFlightEquityLocation), FractionalShares>,
    last_updated: DateTime<Utc>,
}
```

### Imbalance Detection

Current: `ratio = onchain / (onchain + offchain)`, returns None if
transfer-inflight exists.

New: `ratio = onchain / (onchain + offchain + inflight_total)`, still returns
None if transfer-inflight exists.

In-flight balances dilute the ratio, preventing false triggers when capital is
in transit.

## Workflow

Following [docs/workflow.md](docs/workflow.md):

### Step 1: Update SPEC.md

Add the in-flight snapshot events to the "InventoryView listens to" list.
Document the updated imbalance formula that includes in-flight balances in the
denominator. Document in-flight location types and the conservation invariant.

### Step 2: Write failing e2e test

Using TestInfra from `tests/e2e/`: configure a scenario where capital exists at
both venue locations and in-flight locations (Ethereum wallet, Base wallet). Run
the full polling loop. Assert that InventoryView reflects in-flight balances
alongside venue balances. This test fails today because InventoryView ignores
`EthereumCash` and `BaseWalletCash` events.

### Step 3: Identify root cause and write failing unit tests

The root cause is at the unit level: InventoryView lacks the data model and
logic to process in-flight snapshot events. Write unit tests:

**InventoryView unit tests** (`src/inventory/view.rs`):

- Given in-flight cash updates, InventoryView stores and exposes them
- Given both venue and in-flight events, tracks independently without
  interference
- Given in-flight balances exist, imbalance detection includes them in
  denominator (not double-counted, not ignored)
- Conservation invariant:
  `available + transfer_inflight + location_balances =
  total` holds before and
  after transfers

**Rebalancing trigger unit tests** (`src/rebalancing/trigger/mod.rs`):

- `EthereumCash` event updates InventoryView inflight cash (not ignored)
- `BaseWalletCash` event updates InventoryView inflight cash (not ignored)
- In-flight USDC affects USDC imbalance ratio calculation

### Step 4: Types and signatures (compile, don't implement)

**File: `src/inventory/view.rs`**

- Define `InFlightCashLocation`, `InFlightEquityLocation` enums
- Add fields to `InventoryView`
- Add method signatures with `todo!()`: `update_inflight_cash()`,
  `update_inflight_equity()`, `total_inflight_cash()`,
  `inflight_equity_for_symbol()`
- Update `Default`, `Serialize`/`Deserialize`

Confirm `cargo check` passes.

### Step 5: Implement to make unit tests pass

- Implement mutation methods for in-flight location tracking
- Update `detect_imbalance()` to add `total_inflight_cash` to denominator
- Update `detect_imbalance_normalized()` to add inflight equity to denominator
- Update `check_equity_imbalance()` and `check_usdc_imbalance()` to thread
  in-flight data through

### Step 6: Wire into rebalancing trigger

**File: `src/rebalancing/trigger/mod.rs`**

Replace the no-op arm:

```rust
EthereumCash { .. } | BaseWalletCash { .. } => Ok(inventory.clone()),
```

with actual `update_inflight_cash()` calls in both `on_snapshot()` and
`on_snapshot_recovery()`.

### Step 7: Make e2e test pass

Confirm the e2e test from step 2 now passes with the wired-up implementation.

### Step 8: Cleanup

- Run full test suite
- Run clippy, fmt
- Review diff for unjustified changes
- Update ROADMAP.md

### Deferred (after #428 and #431 land)

- Wire `AlpacaWalletCash` -> `InFlightCashLocation::AlpacaCryptoWallet`
- Wire `BaseWalletUnwrappedEquity` -> `InFlightEquityLocation::BaseUnwrapped`
- Wire `BaseWalletWrappedEquity` -> `InFlightEquityLocation::BaseWrapped`
- E2e tests covering all 5 in-flight locations

## Key Files

| File                             | Changes                                                         |
| -------------------------------- | --------------------------------------------------------------- |
| `SPEC.md`                        | Document in-flight location tracking, updated imbalance formula |
| `src/inventory/view.rs`          | Data model, imbalance detection, unit tests                     |
| `src/inventory/venue_balance.rs` | May need `total()` public accessor                              |
| `src/rebalancing/trigger/mod.rs` | Wire events, trigger tests                                      |
| `tests/e2e/rebalancing/`         | E2e test for in-flight visibility                               |

## Verification

1. `cargo check`
2. `cargo nextest run --workspace`
3. `cargo clippy`
4. `cargo fmt`
5. Diff review
