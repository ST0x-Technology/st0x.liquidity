# AGENTS.md

This file provides guidance to AI agents working with the `st0x-broker` crate.

## General Guidelines

**IMPORTANT**: All agents working in this crate MUST obey the top-level
`@AGENTS.md` at the repository root. This file only contains
broker-crate-specific additions and clarifications.

## Broker Crate Scope

This is a **standalone library crate** that provides a unified broker trait
abstraction. When working in this crate:

- **Stay focused on broker abstractions**: This crate should remain independent
  of the parent application
- **No parent crate dependencies**: Never add dependencies on `st0x-hedge` or
  other workspace members
- **Type safety first**: Use newtypes and enums to prevent invalid states
- **Zero-cost abstractions**: Leverage generics and associated types for broker
  implementations

## CRITICAL: No Leaky Abstractions

**This is the most important principle of this crate's architecture.** Export
lists and visibility levels must be **strictly monitored and controlled**.

### Encapsulation Requirements

The encapsulation design **pushes users and maintainers towards using the
`Broker` trait**, not specific broker implementations. Implementation details
must remain hidden.

**What to expose (and ONLY what to expose):**

1. **The `Broker` trait** - The core abstraction
2. **Broker implementation types** - `SchwabBroker`, `AlpacaBroker`, etc.
3. **Configuration/initialization types** - What's needed to construct a broker
   (e.g., `SchwabAuthEnv`, `AlpacaAuthEnv`)
4. **Error types** - Broker-specific errors and shared error types
5. **Domain types** - Shared types like `Symbol`, `Shares`, `Direction`,
   `MarketOrder`, `OrderPlacement`

**What must remain private:**

- Internal authentication logic (except auth initialization types)
- Token management implementation details
- Market hours checking internals
- Order status mapping logic
- HTTP client details
- Database query implementations
- Helper functions and utilities

### Module Organization Pattern

Look at `src/schwab/mod.rs` and `src/alpaca/mod.rs` as reference examples:

```rust
// src/schwab/mod.rs - GOOD EXAMPLE

// Private implementation modules
mod auth;
mod broker;
mod encryption;
mod market_hours;
mod order;
mod order_status;
mod tokens;

// Re-export ONLY what's needed for broker construction
pub use auth::SchwabAuthEnv;
pub use broker::{SchwabBroker, SchwabConfig};

// Re-export for auth CLI command (Schwab-specific, not part of generic broker API)
pub use tokens::SchwabTokens;

// Public error type (needed for error handling)
pub enum SchwabError { ... }

// Utility function needed for initialization
pub fn extract_code_from_url(url: &str) -> Result<String, SchwabError> { ... }
```

**Key principles demonstrated:**

- All implementation modules are **private** (`mod` without `pub`)
- Only **minimal necessary types** are re-exported via `pub use`
- Each re-export has a **clear justification** (commented if non-obvious)
- Internal helper functions and types remain module-private

### Visibility Level Guidelines

When adding new code:

1. **Start with the most restrictive visibility** (private/module-private)
2. **Only increase visibility when required** by external consumers
3. **Document why** something needs to be public if it's not obvious
4. **Prefer `pub(crate)` over `pub`** for crate-internal sharing
5. **Challenge every `pub`** - does this really need to be in the public API?

### Testing Encapsulation

Before exposing anything:

- **Ask**: Can users achieve their goal through the `Broker` trait alone?
- **Ask**: Does exposing this create a dependency on implementation details?
- **Ask**: Will this force users to write broker-specific code?
- **Verify**: Can you use the broker without importing anything except the trait
  and implementation type?

## Testing Requirements

- **Database isolation**: Always use in-memory SQLite (`":memory:"`) for tests
- **HTTP mocking**: Use `httpmock` for API testing, never make real API calls
- **Migration compatibility**: Tests must use
  `sqlx::migrate!("../../migrations")` to ensure compatibility with the parent
  workspace schema
- **No test utils bloat**: Only add test utilities that are reused across
  multiple test modules

## Adding New Brokers

When implementing a new broker (e.g., Interactive Brokers, TD Ameritrade):

1. **Create broker module** in `src/your_broker/` with private submodules
2. **Implement `Broker` trait** with all required methods
3. **Define minimal public API** in `mod.rs`:
   - Broker implementation type (`YourBroker`)
   - Config/auth types needed for construction
   - Broker-specific error type
   - Any initialization utilities (like `extract_code_from_url`)
4. **Keep internals private**: auth logic, token types, HTTP clients, helpers
5. **Add comprehensive tests** covering authentication, orders, status polling
6. **Update `src/lib.rs`** to re-export only the broker implementation type
7. **Document setup** in code comments or broker-specific docs

### Example New Broker Module Structure

```rust
// src/fidelity/mod.rs

mod auth;              // Private - OAuth/token logic
mod broker;            // Private - Broker trait implementation
mod market_hours;      // Private - Market hours checking
mod order;             // Private - Order placement logic
mod order_status;      // Private - Status polling logic

// Public API - ONLY what's needed
pub use auth::FidelityAuthEnv;           // Config for construction
pub use broker::FidelityBroker;          // The implementation

pub enum FidelityError { ... }           // Error type

pub fn initialize_auth(...) -> ... { }   // If needed for setup
```

## Architecture Constraints

- **Trait-based design**: All broker functionality goes through the `Broker`
  trait
- **Database schema shared**: This crate uses the parent workspace's migrations,
  coordinate schema changes
- **No runtime broker selection**: Broker choice happens at compile time through
  generics

## Code Quality Specific to This Crate

- **Linting**: This crate maintains the same strict no-`#[allow(clippy::*)]`
  policy
- **Financial integrity**: All numeric conversions must use `try_into()` and
  explicit error handling
- **Visibility restrictions**: Prefer `pub(crate)` over `pub`, private over
  `pub(crate)`
- **Type modeling**: Use the typestate pattern where beneficial for encoding
  broker lifecycles
- **No leaky abstractions**: Review every `pub` declaration - can it be more
  restrictive?
