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

Additionally, there's duplicated boilerplate across aggregates for handling
uninitialized state:

- `OnChainTrade` uses `Unfilled` variant
- `PositionView` uses `Unavailable` variant
- Both represent "no events applied yet"

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

### Checked arithmetic approach

The `FractionalShares` newtype should implement arithmetic operations that
return `Result<FractionalShares, ArithmeticError>` instead of panicking.

This follows AGENTS.md financial data integrity guidelines: explicit error types
with context, no silent failures.

### Functional event application

Per AGENTS.md's preference for functional programming, the `State<T, E>` type
provides functional helpers:

- `initialize`: For the single event variant that can transition `Uninitialized`
  → `Active`
- `mutate`: Takes current state + event, returns new state; handles `Corrupted`
  by logging and ignoring

This makes each aggregate's `apply` implementation declarative: provide an
initializer function and a mutation function, and `State` handles the plumbing.

---

## Task 1. Create the `State<T, E>` type

Create a new module `src/state.rs` with the generic state wrapper.

- [ ] Define the `State<T, E>` enum with three variants
- [ ] Implement helper methods for state inspection (`is_active`,
      `is_corrupted`, etc.)
- [ ] Implement `State::initialize` - takes `FnOnce(Event) -> Result<T, E>`,
      transitions `Uninitialized` → `Active(T)` or `Corrupted(E)`
- [ ] Implement `State::mutate` - takes `FnOnce(T, Event) -> Result<T, E>`,
      transforms `Active` state or logs and ignores if `Corrupted`
- [ ] Add comprehensive tests for the wrapper type
- [ ] Add module declaration to `src/lib.rs`

### Type definition

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum State<T, E> {
    Uninitialized,
    Active(T),
    Corrupted {
        error: E,
        last_valid_state: Option<Box<T>>,
    },
}
```

The `last_valid_state` preserves the state before corruption for
debugging/recovery.

---

## Task 2. Add fallible arithmetic to `FractionalShares`

Modify `src/position/event.rs` to provide arithmetic operations that return
`Result`.

- [ ] Define `ArithmeticError` enum with overflow context (operation, lhs, rhs
      values)
- [ ] Add `try_add` method returning `Result<FractionalShares, ArithmeticError>`
- [ ] Add `try_sub` method returning `Result<FractionalShares, ArithmeticError>`
- [ ] Keep existing operator impls for backwards compatibility (they'll be used
      in controlled contexts)
- [ ] Add tests for fallible arithmetic, including overflow scenarios

### Error type

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("Arithmetic overflow: {lhs:?} {operation} {rhs:?}")]
pub(crate) struct ArithmeticError {
    pub(crate) operation: &'static str,
    pub(crate) lhs: FractionalShares,
    pub(crate) rhs: FractionalShares,
}
```

### Method signatures

```rust
impl FractionalShares {
    pub(crate) fn try_add(self, rhs: Self) -> Result<Self, ArithmeticError>;
    pub(crate) fn try_sub(self, rhs: Self) -> Result<Self, ArithmeticError>;
}
```

---

## Task 3. Define `PositionCorruptionError` type

Create an error type in `src/position/mod.rs` for corruption scenarios.

- [ ] Define `PositionCorruptionError` enum with variants for each failure mode
- [ ] Include `ArithmeticError` from Task 2 via `#[from]`
- [ ] Generate `Display` and `Error` trait implementations via `thiserror`

### Error variants

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
pub(crate) enum PositionCorruptionError {
    #[error(transparent)]
    Arithmetic(#[from] ArithmeticError),
    #[error("Event applied to uninitialized position: {event_type}")]
    EventBeforeInitialization { event_type: String },
}
```

---

## Task 4. Refactor `Position` aggregate to use `State`

Update `src/position/mod.rs` to wrap the position data in `State`.

- [ ] Change `Position` struct to contain only the position data (remove it
      being the aggregate root)
- [ ] Create `PositionAggregate` as `State<Position, PositionCorruptionError>`
- [ ] Implement `Default` for `PositionAggregate` returning
      `Active(Position::default())`
- [ ] Update `Aggregate` implementation to use `PositionAggregate`
- [ ] Refactor `apply` to use `State::mutate` with fallible arithmetic
- [ ] Update `handle` method to check for corrupted state and reject commands
- [ ] Update all tests to work with new structure

---

## Task 5. Refactor `OnChainTrade` aggregate to use `State`

Update `src/onchain_trade/mod.rs` to use `State` instead of manual `Unfilled`
variant.

- [ ] Define `OnChainTradeCorruptionError` for corruption scenarios
- [ ] Remove `Unfilled` variant from `OnChainTrade` enum, keep `Filled` and
      `Enriched` as the inner type
- [ ] Create `OnChainTradeAggregate` as
      `State<OnChainTrade, OnChainTradeCorruptionError>`
- [ ] Implement `Default` for `OnChainTradeAggregate` returning `Uninitialized`
- [ ] Update `Aggregate` implementation to use `State::initialize` for `Witness`
      command and `State::mutate` for `Enrich`
- [ ] Update `handle` method to check for corrupted state and reject commands
- [ ] Update all tests to work with new structure

---

## Task 6. Update `PositionView` to handle corrupted state

Update `src/position/view.rs` to reflect corruption in the view.

- [ ] Add `Corrupted` variant to `PositionView` enum
- [ ] Update `View::update` to handle `Corrupted` aggregate state
- [ ] Add tests for corrupted state propagation to view

### Updated enum

```rust
pub(crate) enum PositionView {
    Unavailable,
    Position { /* ... */ },
    Corrupted {
        symbol: Symbol,
        error: String,
        last_known_position: Option<FractionalShares>,
    },
}
```

---

## Task 7. Add logging for corruption events

Ensure corruption is visible in logs.

- [ ] Add `error!` log when transitioning to `Corrupted` state
- [ ] Include aggregate ID and error details in log

---

## Task 8. Update integration points

Ensure code that uses the aggregates handles the new structure.

- [ ] Search for usages of `Position` and `OnChainTrade` aggregates in the
      codebase
- [ ] Update any code that directly accesses aggregate fields
- [ ] Ensure command handlers properly reject commands on corrupted aggregates

---

## Task 9. Clean up and final testing

- [ ] Run `cargo test -q` to ensure all tests pass
- [ ] Run `cargo clippy --all-targets --all-features -- -D clippy::all`
- [ ] Run `cargo fmt`
- [ ] Review all changes for adherence to AGENTS.md guidelines

---

## Future Considerations (Out of Scope)

1. **Recovery mechanism**: Add commands to recover from corrupted state (e.g.,
   `Migrate` command that resets position with corrected values)

2. **Metrics and alerting**: Add OpenTelemetry metrics for corrupted aggregate
   counts

3. **Database cleanup**: Consider whether corrupted aggregates should be marked
   in the event store for cleanup
