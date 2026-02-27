# AGENTS.md

This file provides guidance to AI agents working with the `st0x-execution`
crate.

## General Guidelines

**IMPORTANT**: All agents working in this crate MUST obey the top-level
`@AGENTS.md` at the repository root. This file only contains
execution-crate-specific additions and clarifications.

## Execution Crate Scope

This is a **standalone library crate** that provides a unified `Executor` trait
abstraction. When working in this crate:

- **Stay focused on execution abstractions**: This crate should remain
  independent of the parent application
- **No parent crate dependencies**: Never add dependencies on `st0x-hedge` or
  other workspace members
- **Type safety first**: Use newtypes and enums to prevent invalid states
- **Zero-cost abstractions**: Leverage generics and associated types for
  implementations

## CRITICAL: No Leaky Abstractions

**This is the most important principle of this crate's architecture.** Export
lists and visibility levels must be **strictly monitored and controlled**.

### Encapsulation Requirements

The encapsulation design **pushes users and maintainers towards using the
`Executor` trait**, not specific implementations. Implementation details must
remain hidden.

**What to expose (and ONLY what to expose):**

1. **The `Executor` trait** - The core abstraction
2. **Implementation types** - `SchwabExecutor`, `AlpacaTradingApi`, etc.
3. **Configuration/initialization types** - What's needed to construct an
   executor (e.g., `SchwabAuthEnv`, `AlpacaTradingApiEnv`)
4. **Error types** - Implementation-specific errors and shared error types
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

Look at `src/schwab.rs` and `src/alpaca_trading_api.rs` as reference examples:

```rust
// src/schwab.rs - GOOD EXAMPLE

// Private implementation modules
mod auth;
mod executor;
mod encryption;
mod market_hours;
mod order;
mod order_status;
mod tokens;

// Re-export ONLY what's needed for construction
pub use auth::SchwabAuthEnv;
pub use executor::{SchwabExecutor, SchwabConfig};

// Re-export for auth CLI command (Schwab-specific, not part of generic API)
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

- **Ask**: Can users achieve their goal through the `Executor` trait alone?
- **Ask**: Does exposing this create a dependency on implementation details?
- **Ask**: Will this force users to write implementation-specific code?
- **Verify**: Can you use the executor without importing anything except the
  trait and implementation type?

## Testing Requirements

- **Database isolation**: Always use in-memory SQLite (`":memory:"`) for tests
- **HTTP mocking**: Use `httpmock` for API testing, never make real API calls
- **Migration compatibility**: Tests must use
  `sqlx::migrate!("../../migrations")` to ensure compatibility with the parent
  workspace schema
- **No test utils bloat**: Only add test utilities that are reused across
  multiple test modules

## Adding New Implementations

When adding support for a new brokerage (e.g., Interactive Brokers, TD
Ameritrade):

1. **Create module** in `src/your_executor/` with private submodules
2. **Implement `Executor` trait** with all required methods
3. **Define minimal public API** in the parent module file (e.g.,
   `src/your_executor.rs`):
   - Implementation type (`YourExecutor`)
   - Config/auth types needed for construction
   - Error type
   - Any initialization utilities (like `extract_code_from_url`)
4. **Keep internals private**: auth logic, token types, HTTP clients, helpers
5. **Add comprehensive tests** covering authentication, orders, status polling
6. **Update `src/lib.rs`** to re-export only the implementation type
7. **Document setup** in code comments or implementation-specific docs

### Example New Module Structure

```rust
// src/fidelity.rs

mod auth;              // Private - OAuth/token logic
mod executor;          // Private - Executor trait implementation
mod market_hours;      // Private - Market hours checking
mod order;             // Private - Order placement logic
mod order_status;      // Private - Status polling logic

// Public API - ONLY what's needed
pub use auth::FidelityAuthEnv;           // Config for construction
pub use executor::FidelityExecutor;      // The implementation

pub enum FidelityError { ... }           // Error type

pub fn initialize_auth(...) -> ... { }   // If needed for setup
```

## Architecture Constraints

- **Trait-based design**: All functionality goes through the `Executor` trait
- **Database schema shared**: This crate uses the parent workspace's migrations,
  coordinate schema changes
- **No runtime selection**: Implementation choice happens at compile time
  through generics

## Code Quality Specific to This Crate

- **Linting**: This crate maintains the same strict no-`#[allow(clippy::*)]`
  policy
- **Financial integrity**: All numeric conversions must use `try_into()` and
  explicit error handling
- **Visibility restrictions**: Prefer `pub(crate)` over `pub`, private over
  `pub(crate)`
- **Type modeling**: Use the typestate pattern where beneficial for encoding
  lifecycles
- **No leaky abstractions**: Review every `pub` declaration - can it be more
  restrictive?
