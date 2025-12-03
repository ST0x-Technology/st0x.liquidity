# Implementation Plan: State Wrapper for Error Handling in Aggregates

## Problem Statement

The `FractionalShares` type uses `Decimal` arithmetic (Add, Sub, AddAssign,
SubAssign) which can panic on overflow. In the CQRS/ES pattern implemented via
`cqrs-es`, the `Aggregate::apply` method is **infallible** - it has signature
`fn apply(&mut self, event: Self::Event)` with no `Result` return type.

This creates a fundamental tension:

1. We cannot return errors from `apply`
2. We cannot panic in a financial application (violates AGENTS.md principles)
3. We need to handle arithmetic overflow gracefully

Additionally, there's duplicated boilerplate across aggregates and views for
handling uninitialized state:

- `OnChainTrade` uses `Unfilled` variant
- `PositionView` uses `Unavailable` variant
- `OnChainTradeView` uses `Unavailable` variant
- All represent "no events applied yet"

## Proposed Solution

Create a generic `State<T, E>` wrapper that encodes three possible states:

1. **`Uninitialized`** - Before any events have been applied (default)
2. **`Active(T)`** - Normal operational state with valid aggregate data
3. **`Corrupted(E)`** - Error state entered when event application fails

This approach:

- Makes failure states explicit and observable in the type system
- Eliminates panics by capturing errors as state transitions
- Reduces boilerplate for the "not yet initialized" pattern
- Allows monitoring/alerting on corrupted aggregates
- Works for both aggregates and views (same state machine pattern)

## Design Decisions

### Why `Corrupted` instead of logging and continuing?

In an event-sourced system, events are immutable facts. If applying an event
would corrupt state (e.g., arithmetic overflow), we have options:

1. **Panic** - Unacceptable in financial systems
2. **Log and continue** - Silently produces wrong state; dangerous
3. **Transition to error state** - Makes corruption explicit and recoverable

Option 3 is the safest because:

- The aggregate becomes unusable, preventing further damage
- The error is observable via views and monitoring
- Manual intervention can be triggered (e.g., migrate to corrected state)

### Why a separate module?

The `State` wrapper is a reusable infrastructure type that addresses a pattern
present across multiple aggregates in the codebase. It's not specific to
positions - it's a feature for internal consumption by the CQRS system. This
justifies its existence as a standalone module rather than being embedded in a
specific domain feature.

### Using State for both aggregates and views

Both aggregates and views suffer from the same pattern mismatch problems:

1. Update events on uninitialized state → silent skip with log
2. Initialization events on already-active state → silent overwrite or skip

The `State<T, E>` wrapper with `initialize` and `transition` methods addresses
both cases uniformly. Views use the same API as aggregates since the
`View::update` method has the same infallible signature as `Aggregate::apply`.

### Checked arithmetic approach

The `FractionalShares` newtype should implement arithmetic operations that
return `Result<FractionalShares, ArithmeticError>` instead of panicking.

This follows AGENTS.md financial data integrity guidelines: explicit error types
with context, no silent failures.

### Functional event application

Per AGENTS.md's preference for functional programming, the `State<T, E>` type
provides functional helpers:

- `initialize`: Takes an event implementing `cqrs_es::DomainEvent` and a closure
  that processes the event to produce the initial state
- `transition`: Takes an event and a closure that transforms the current state

The closures return `Result<T, StateError<E>>`, giving callers control over
error handling while the `State` wrapper handles state machine transitions.

This makes each aggregate's `apply` implementation declarative: match on event
variants and call `initialize` or `transition` accordingly.

### Error handling design

There are three categories of errors that can occur in `apply` implementations:

1. **Event on uninitialized state** - A transition event is applied but the
   aggregate hasn't been initialized yet
2. **Event not applicable** - An initialization event is applied but the
   aggregate is already active (includes state name and event name for
   debugging)
3. **Custom error** - Aggregate-specific errors like arithmetic overflow

We wrap the user's error type in `StateError<E>`:

```rust
pub(crate) enum StateError<E> {
    EventOnUninitialized,
    EventNotApplicable { state: String, event: String },
    Custom(E),
}
```

The `initialize` and `transition` methods take events implementing
`cqrs_es::DomainEvent`, which provides `event_type()` for informative error
messages.

---

## Task 1. Create the `State<T, E>` type

Create a new module `src/state.rs` with the generic state wrapper.

- [x] Define `StateError<E>` enum with three variants (including state/event
      names in `EventNotApplicable`)
- [x] Define the `State<T, E>` enum with three variants
- [x] Implement `State::initialize` - takes event `Ev: DomainEvent` and
      `FnOnce(Ev) -> Result<T, StateError<E>>`, transitions `Uninitialized` →
      `Active(T)` or `Corrupted`
- [x] Implement `State::transition` - takes event `Ev: DomainEvent` and
      `FnOnce(Ev, &T) -> Result<T, StateError<E>>`, transforms `Active` state or
      transitions to `Corrupted`
- [x] Add comprehensive tests for the wrapper type
- [x] Add module declaration to `src/lib.rs`

### Type definitions

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
pub(crate) enum StateError<E> {
    #[error("event applied to uninitialized state")]
    EventOnUninitialized,
    #[error("event '{event}' not applicable to state '{state}'")]
    EventNotApplicable { state: String, event: String },
    #[error(transparent)]
    Custom(E),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum State<T, E> {
    Uninitialized,
    Active(T),
    Corrupted {
        error: StateError<E>,
        last_valid_state: Option<Box<T>>,
    },
}
```

### Method signatures

```rust
impl<T, E: Display> State<T, E> {
    pub(crate) fn initialize<Ev, F>(&mut self, event: Ev, f: F)
    where
        Ev: DomainEvent,
        F: FnOnce(Ev) -> Result<T, StateError<E>>;

    pub(crate) fn transition<Ev, F>(&mut self, event: Ev, f: F)
    where
        Ev: DomainEvent,
        F: FnOnce(Ev, &T) -> Result<T, StateError<E>>;
}
```

The `last_valid_state` preserves the state before corruption for
debugging/recovery.

---

## Task 2. Add fallible arithmetic to `FractionalShares`

Extract `FractionalShares` to its own module (`src/shares.rs`) and provide
arithmetic operations that return `Result`.

- [x] Create `src/shares.rs` module with `FractionalShares` and
      `ArithmeticError`
- [x] Define `ArithmeticError` struct with overflow context (operation, lhs,
      rhs)
- [x] Update `Add` impl to set `type Output = Result<Self, ArithmeticError>`
- [x] Update `Sub` impl to set `type Output = Result<Self, ArithmeticError>`
- [x] Add helper methods: `is_negative()`, `is_zero()`, `abs()`
- [x] Add `ZERO` and `ONE` constants
- [x] Add tests for fallible arithmetic, including overflow scenarios
- [x] Update `src/position/event.rs` to import from `crate::shares`
- [x] Move threshold validation to `ExecutionThreshold` constructors
- [x] Add module declaration to `src/lib.rs`

---

## Task 3. Refactor `Position` aggregate to use `State`

Update `src/position/mod.rs` to wrap the position data in `State`.

- [x] Change `Position` struct to contain only the position data
- [x] Implement `Aggregate` for `State<Position, ArithmeticError>` directly
- [x] Implement `Default` returning `Active(Position::default())`
- [x] Refactor `apply` to use `State::transition` with fallible arithmetic
- [x] Set `type Error = PositionError` with `PositionError` having
      `#[from] StateError<ArithmeticError>` for state errors
- [x] Update `handle` method to use `self.active()?` helper
- [x] Update all tests to work with new structure

---

## Task 4. Update `PositionView` to use `State`

Update `src/position/view.rs` to use `State<Position, ArithmeticError>` directly
instead of the manual `Unavailable`/`Position`/`Corrupted` enum. The `Position`
struct already contains all fields needed by the view.

- [x] Implement `View<State<Position, ArithmeticError>>` for
      `State<Position, ArithmeticError>` directly (no type alias needed)
- [x] Reuse `Position::apply_transition` and `Position::from_event` in the
      `View::update` implementation
- [x] Replace silent `return` on pattern mismatches with proper corruption via
      the `State` wrapper
- [x] Update tests to use `State::Active(Position { ... })` instead of the old
      `PositionView::Position { ... }` variant

---

## Task 5. Refactor `OnChainTrade` aggregate to use `State`

Update `src/onchain_trade/mod.rs` to use `State` instead of manual `Unfilled`
variant. Follow the same clean pattern as Position aggregate.

- [x] Create `OnChainTrade` struct with the trade data fields
- [x] Create `Enrichment` struct to group enrichment fields (gas_used,
      pyth_price, enriched_at)
- [x] Implement `Aggregate` for `State<OnChainTrade, Never>` directly
- [x] Create `Never` type (serializable uninhabited type) in `state.rs`
- [x] Implement `Default` returning `Uninitialized`
- [x] Create `OnChainTrade::apply_transition` and `OnChainTrade::from_event`
      helper methods
- [x] Use `.transition().or_initialize()` pattern in `apply`
- [x] Update `handle` to use `self.active()?`
- [x] Update all tests to work with new structure

---

## Task 6. Update `OnChainTradeView` to use `State`

Update `src/onchain_trade/view.rs` to use `State<OnChainTrade, Never>` directly.

- [x] Update `View<OnChainTrade>` to `View<State<OnChainTrade, Never>>`
- [x] Update `EventEnvelope` type parameter
- [x] Tests continue to work (view keeps its own structure with tx_hash and
      log_index)

---

## Task 7. Add logging for corruption events

Ensure corruption is visible in logs.

- [x] Add `error!` log when transitioning to `Corrupted` state
- [x] Include aggregate ID and error details in log

---

## Task 8. Update integration points

Ensure code that uses the aggregates handles the new structure.

- [x] Search for usages of `Position` and `OnChainTrade` aggregates in the
      codebase
- [x] No external usages found - modules are marked `#[allow(dead_code)]`
      pending dual-write implementation (TODO #130)
- [x] Command handlers already reject commands on corrupted aggregates via
      `self.active()?`

---

## Task 9. Clean up and final testing

- [x] Run `cargo test -q` to ensure all tests pass (342 tests)
- [x] Run `cargo clippy --all-targets --all-features -- -D clippy::all`
- [x] Run `cargo fmt`
- [x] Review all changes for adherence to AGENTS.md guidelines

---

## Future Considerations (Out of Scope)

1. **Recovery mechanism**: Add commands to recover from corrupted state (e.g.,
   `Migrate` command that resets position with corrected values)

2. **Metrics and alerting**: Add OpenTelemetry metrics for corrupted aggregate
   counts

3. **Database cleanup**: Consider whether corrupted aggregates should be marked
   in the event store for cleanup
