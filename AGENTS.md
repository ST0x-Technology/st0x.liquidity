# AGENTS.md

This file provides guidance to AI agents working with code in this repository.

**CRITICAL: File Size Limit** - AGENTS.md must not exceed 40,000 characters.
When editing this file, check the character count (`wc -c AGENTS.md`). If over
the limit:

- **NEVER remove guidelines** - only condense verbose explanations
- **Condense code examples first** - examples are illustrative, rules are not
- **Remove redundancy** - if a guideline duplicates another, keep one reference
- **Shorten explanations** - preserve the rule, reduce the elaboration

## Ownership Principles

**CRITICAL: Take full ownership. Never deflect responsibility.**

- **Fix all problems immediately** - regardless of who introduced them. Never
  say "this is a pre-existing issue" as justification for not fixing it.
- **Meet ALL constraints** - when editing a file with size limits, ensure the
  ENTIRE file meets the limit, not just your additions.
- **No warnings or errors pass through** - CI and review catch everything. If
  you see a warning/error, it's your responsibility to fix it now.

## Communication

- **Do not run commands to "show" output to the user.** The CLI truncates
  output. If you need the user to review something, explicitly ask them to look
  at it. Do not run `git diff` expecting the user to see output.

## Planning Hierarchy

The project uses a strict document hierarchy:

1. **SPEC.md** - Source of truth for system behavior. Features documented here
   before implementation.
2. **ROADMAP.md / GitHub Issues** - Downstream from spec. Describe problems, not
   solutions.
3. **Planning** - Downstream from issues. Implementation plans before coding.
4. **Tests** - Downstream from plan. Written before implementation (TDD).
5. **Implementation** - Makes the tests pass.

**Before implementing:** Ensure feature is in SPEC.md → has GitHub issue → plan
the implementation.

## Plan & Review

### While implementing

- **CRITICAL: All new or modified logic MUST have corresponding test coverage.**
  Do not move on from a piece of code until tests are written. This is
  non-negotiable.

### Handling questions and approach changes

When the user asks a question or challenges your approach:

1. **Answer the question first.** Do not immediately assume the question implies
   you should change your approach. Provide a clear, direct answer.

2. **If you realize your approach was wrong**, do not silently change it. Ask
   for confirmation: "I see the issue - [explain]. Should I switch to [new
   approach] instead?"

3. **If you encounter errors while trying a different approach**, do not
   silently revert to the previous approach. State what went wrong and ask: "The
   new approach ran into [problem]. Do you want me to continue debugging it or
   go back to the original approach?"

4. **Never assume silence or a question means approval to change direction.**
   Explicit confirmation is required before abandoning one approach for another.

### When issues are pointed out

When the user points out an issue, bug, or problem - fix it immediately. Do not
ask "Want me to fix this?" or "Should I address this?". The user never sends
messages just for the sake of it; when they point out issues, they expect action
(usually a fix, sometimes reproducing, opening a GitHub issue, etc. based on
context).

**CRITICAL: Re-evaluate all work when a pattern is identified.** When the user
points out a mistake, immediately: (1) fix it, (2) re-evaluate ALL session work
for similar issues, (3) proactively fix all instances without being asked.

### When user action is required

**CRITICAL**: The user is not reading every word of your output - they are
monitoring your actions. When you need the user to do something (run a command,
check output, provide input), you must ensure they see the request:

- If you are **blocked** and cannot proceed without user action, STOP after
  stating what you need. Do not continue working on other tasks.
- If you are **not blocked**, you can continue working, but when you're ready to
  stop, clearly state what you need from the user at the end of your response.

The user checks your output when they see you've stopped. If you give them a
command mid-response and keep working, they will miss it.

### Before handing over

After implementation is complete and verification passes (tests, lints, fmt),
perform a self-review:

1. **Review the diff** - examine all changes and ask: can I justify each chunk?
2. **Revert unjustified changes** - if you can't articulate why a change is
   necessary, revert it
3. **Check for scope creep** - did you change things unrelated to the task?

**Justified changes:** explicitly requested by user, required to make the
requested change work, fixes a bug/warning encountered during the task, improves
readability of code being modified, enforces stricter domain boundaries.

**Unjustified changes:** renaming unrelated things, reformatting outside the
change area, LLM-initiated "while I'm here" improvements, changing terminology
without request, adding comments to unchanged code.

This step exists because LLMs are not naturally aware of diff size while
generating, but can effectively review diffs after the fact. When context is
ambiguous (after compaction), if you cannot point to an explicit user request in
visible conversation, treat the change as unjustified and revert it.

## Project Overview

This is a Rust-based market making system for tokenized equities that provides
onchain liquidity via Raindex orders and hedges directional exposure by
executing offsetting trades on traditional brokerages (Charles Schwab or Alpaca
Markets). The system captures arbitrage profits from spreads while attempting to
minimize delta exposure through automated hedging.

## Key Development Commands

### Workspace Structure

This project uses a Cargo workspace with:

- **Root crate (`st0x-hedge`)**: Main arbitrage bot application
- **Execution crate (`st0x-execution`)**: Trade execution abstraction library

### Building & Running

**CRITICAL: NEVER use `cargo build` for verification.** It's slower than
`cargo check` and less useful than `cargo test` or `cargo clippy`. Use:

- `cargo check` for fast compilation verification
- `cargo test` for verification with test coverage
- `cargo clippy` for verification with linting

Only use `cargo build` when you actually need the build artifacts (e.g., final
verification before a release, or when the user explicitly asks to run the
binary).

- `cargo run --bin server` - Run the main arbitrage bot
- `cargo run --bin cli -- auth` - Run the authentication flow for Charles Schwab
  OAuth setup
- `cargo run --bin cli -- test -t AAPL -q 100 -d buy` - Test trading
  functionality with mocked execution
- `cargo run --bin cli` - Run the command-line interface for manual operations

### Testing

- `cargo test -q` - Run all tests (both main and execution crates)
- `cargo test -q --lib` - Run library tests only
- `cargo test -p st0x-execution -q` - Run execution crate tests only
- `cargo test -p st0x-hedge -q` - Run main crate tests only
- `cargo test -q <test_name>` - Run specific test

### Database Management

- `sqlx db create` - Create the database
- `sqlx db reset -y` - Drop the database and re-run all migrations
- `sqlx migrate run` - Apply database migrations
- `sqlx migrate revert` - Revert last migration
- `sqlx migrate add <migration_name>` - Create a new migration file
- Database URL configured via `DATABASE_URL` environment variable

**CRITICAL: If Rust build fails with sqlx macro errors like "unable to open
database file" or "(code: 14)", run `sqlx db reset -y` to fix the database.**
This is the proper solution - NEVER try workarounds like
`DATABASE_URL=sqlite://:memory:` or other hacks.

**CRITICAL: NEVER manually create migration files.** Always use
`sqlx migrate add
<migration_name>` to create migrations. This ensures proper
timestamping and sequencing.

**CRITICAL: New worktrees require database setup.** When working in a new git
worktree, you will encounter sqlx compile errors like "unable to open database
file". Fix this by running `sqlx db reset -y` to create and migrate the local
database before running any cargo commands.

### Dependency Management

**CRITICAL: NEVER manually edit `Cargo.toml` to add dependencies.** Always use
`cargo add <crate_name>` to add dependencies. This ensures proper version
resolution and feature selection.

- `cargo add <crate_name>` - Add a dependency to the current crate
- `cargo add <crate_name> --dev` - Add a dev-dependency
- `cargo add <crate_name> --build` - Add a build-dependency
- `cargo add <crate_name> -F <feature>` - Add a dependency with specific
  features

### Development Tools

- `rainix-rs-static` - Run Rust static analysis
- `cargo clippy --all-targets --all-features -- -D clippy::all` - Run Clippy for
  linting
- `cargo fmt` - Format code

### Nix Development Environment

- `nix develop` - Enter development shell with all dependencies
- `nix run .#prepSolArtifacts` - Build Solidity artifacts for orderbook
  interface

## Development Workflow Notes

- When running `git diff`, make sure to add `--no-pager` to avoid opening it in
  the interactive view, e.g. `git --no-pager diff`
- **CRITICAL: NEVER run `cargo run` unless explicitly asked by the user.** If
  you want to understand CLI commands or configuration options, read the code.
  If you want to test functionality, write proper tests. There is never a reason
  to run the application speculatively.

### Updating ROADMAP.md

After completing work or creating new issues, update ROADMAP.md:

**Section ordering (newest first):**

The roadmap is ordered with highest priority / most recent work at the top:

1. **Current Development Focus** - Active work and immediate priorities
2. **Backlog sections** - Planned future work by category
3. **Completed sections** - Finished work, ordered newest to oldest

This ordering ensures readers see current priorities immediately without
scrolling past historical work. When adding new "Completed" sections, add them
above older completed sections.

**After completing a plan:**

1. Mark completed issues as `[x]` with PR link
2. Use this format:
   ```markdown
   - [x] [#N Issue title](https://github.com/ST0x-Technology/st0x.liquidity/issues/N)
     - PR: [#M PR title](https://github.com/ST0x-Technology/st0x.liquidity/pull/M)
   ```
3. Move completed items from "Current Development Focus" to the appropriate
   "Completed" section (or create a new one if it represents a milestone)

**When creating new issues:**

1. Add the issue to the appropriate roadmap section
2. Use this format:
   ```markdown
   - [ ] [#N Issue title](https://github.com/ST0x-Technology/st0x.liquidity/issues/N)
   ```

**Verification:**

- Use `gh issue list --state all` and `gh pr list --state all` to cross-check
- Ensure no issues are marked `[x]` in ROADMAP.md but still open on GitHub
- Ensure all recent closed issues/PRs are reflected in the roadmap

## Architecture Overview

### Execution Abstraction Layer

**Design Principle**: The application uses a generic `Executor` trait to support
multiple trading platforms while maintaining type safety and zero-cost
abstractions.

**Key Architecture Points**:

- Generic `Executor` trait with associated types (`Error`, `OrderId`, `Config`)
- Main crate stays execution-agnostic via trait; execution-specific logic in
  `st0x-execution` crate
- Newtypes (`Symbol`, `Shares`, `Direction`) and enums prevent invalid states
- Supported implementations: SchwabExecutor, AlpacaTradingApi, AlpacaBrokerApi,
  MockExecutor

**Benefits**:

- Zero changes to core bot logic when adding new implementations
- Type safety via compile-time verification
- Independent testing per implementation
- Zero-cost abstractions via generics (no dynamic dispatch)

For detailed implementation requirements and module organization, see
@crates/execution/AGENTS.md

### Core Event Processing Flow

**Main Event Loop ([`launch` function in `src/lib.rs`])**

- Monitors two concurrent WebSocket event streams: `ClearV2` and `TakeOrderV2`
  from the Raindex orderbook
- Uses `tokio::select!` to handle events from either stream without blocking
- Converts blockchain events to structured `Trade` objects for processing

**Trade Conversion Logic ([`Trade` struct and methods in `src/trade/mod.rs`])**

- Parses onchain events into actionable trade data with strict validation
- Expects symbol pairs of USDC + tokenized equity with "0x" suffix (e.g.,
  "AAPL0x")
- Determines Schwab trade direction: buying tokenized equity onchain → selling
  on Schwab
- Calculates prices in cents and maintains onchain/offchain trade ratios

**Async Event Processing Architecture**

- Each blockchain event spawns independent async execution flow
- Handles throughput mismatch: fast onchain events vs slower Schwab API calls
- No artificial concurrency limits - processes events as they arrive
- Flow: Parse Event → SQLite Deduplication Check → Schwab API Call → Record
  Result

### Authentication & API Integration

**Charles Schwab OAuth (`src/schwab.rs`)**

- OAuth 2.0 flow with 30-minute access tokens and 7-day refresh tokens
- Token storage and retrieval from SQLite database
- Comprehensive error handling for authentication failures

**Symbol Caching (`crate::symbol::cache::SymbolCache`)**

- Thread-safe caching of ERC20 token symbols using `tokio::sync::RwLock`
- Prevents repeated RPC calls for the same token addresses

### Database Schema & Idempotency

**Key tables** (see migrations for full schema):

- `onchain_trades`: Immutable blockchain trade records, keyed by
  `(tx_hash, log_index)`
- `schwab_executions`: Order execution tracking with status transitions
- `trade_accumulators`: Unified position tracking per symbol
- `trade_execution_links`: Many-to-many audit trail between trades and
  executions
- `schwab_auth`: OAuth token storage (singleton)
- `event_queue`: Idempotent event processing queue, keyed by
  `(tx_hash, log_index)`
- `symbol_locks`: Per-symbol execution concurrency control

**Idempotency**: Uses `(tx_hash, log_index)` as unique identifier, status
tracking (pending → completed/failed), retry logic with exponential backoff

### Configuration

Environment variables (can be set via `.env` file):

- `DATABASE_URL`: SQLite database path
- `WS_RPC_URL`: WebSocket RPC endpoint for blockchain monitoring
- `ORDERBOOK`: Raindex orderbook contract address
- `ORDER_OWNER`: Owner address of orders to monitor for trades
- `APP_KEY`, `APP_SECRET`: Charles Schwab API credentials
- `REDIRECT_URI`: OAuth redirect URI (default: https://127.0.0.1)
- `BASE_URL`: Schwab API base URL (default: https://api.schwabapi.com)

### Code Quality & Best Practices

- **CRITICAL: Package by Feature, Not by Layer**: NEVER organize code by
  language primitives or technical layers. ALWAYS organize by business
  feature/domain.
  - **FORBIDDEN**: `types.rs`, `error.rs`, `models.rs`, `utils.rs`,
    `helpers.rs`, `http.rs`, `dto.rs`, `entities.rs`, `services.rs`, `domain.rs`
    (when used as catch-all technical layer modules)
  - **CORRECT**: `position.rs`, `offchain_order.rs`, `onchain_trade.rs`
    (organized by business domain)
  - Each feature module should contain ALL related code: types, errors,
    commands, events, aggregates, views, and endpoints
  - This makes it easy to understand and modify a feature without jumping
    between unrelated files
  - Value objects and newtypes should live in the feature module where they're
    primarily defined/used
  - When types are shared across features, import from the owning feature (e.g.,
    `offchain_order` can import `FractionalShares` from `position` module)
  - **Flat by default**: Start with a single file per feature (e.g.,
    `position.rs`). Only split into a directory with submodules when natural
    business logic boundaries emerge and the split provides clear value
- **Event-Driven Architecture**: Each trade spawns independent async task for
  maximum throughput
- **SQLite Persistence**: Embedded database for trade tracking and
  authentication tokens
- **Symbol Suffix Convention**: Tokenized equities use "0x" suffix to
  distinguish from base assets
- **Price Direction Logic**: Onchain buy = offchain sell (and vice versa) to
  hedge directional exposure
- **Comprehensive Error Handling**: Custom error types (`OnChainError`,
  `SchwabError`) with proper propagation
- **CRITICAL: CQRS/Event Sourcing Architecture**: This application uses the
  cqrs-es framework for event sourcing. **NEVER write directly to the `events`
  table**. This is strictly forbidden and violates the CQRS architecture:
  - **FORBIDDEN**: Direct INSERT statements into the `events` table
  - **FORBIDDEN**: Manual sequence number management for events
  - **FORBIDDEN**: Bypassing the CqrsFramework to write events
  - **REQUIRED**: Always use `CqrsFramework::execute()` or
    `CqrsFramework::execute_with_metadata()` to emit events
  - **REQUIRED**: Events must be emitted through aggregate commands that
    generate domain events
  - The cqrs-es framework handles event persistence, sequence numbers, aggregate
    loading, and consistency guarantees
  - Direct table writes break aggregate consistency, event ordering, and the
    event sourcing pattern
  - If you see existing code writing directly to `events` table, that code is
    incorrect and should be refactored to use CqrsFramework
- **Type Modeling**: Make invalid states unrepresentable through the type
  system. Use algebraic data types (ADTs) and enums to encode business rules and
  state transitions directly in types rather than relying on runtime validation.
  Examples:
  - Use enum variants to represent mutually exclusive states instead of multiple
    boolean flags
  - Encode state-specific data within enum variants rather than using nullable
    fields
  - Use newtypes for domain concepts to prevent mixing incompatible values
  - Leverage the type system to enforce invariants at compile time
- **Schema Design**: Avoid database columns that can contradict each other. Use
  constraints and proper normalization to ensure data consistency at the
  database level. Align database schemas with type modeling principles where
  possible
- **No Denormalized Columns**: Never store values that can be computed from
  other columns. Denormalized data inevitably becomes stale when the source
  columns are updated but the derived column is forgotten. Always compute
  derived values on-demand in queries (e.g., use
  `ABS(accumulated_long - accumulated_short) >= 1.0` instead of storing a
  separate `net_position` column). If performance requires caching, use database
  views or generated columns that auto-update, never manually-maintained columns
- **Functional Programming Patterns**: Favor FP and ADT patterns over OOP
  patterns. Avoid unnecessary encapsulation, inheritance hierarchies, or
  getter/setter patterns that don't make sense with Rust's algebraic data types.
  Use pattern matching, combinators, and type-driven design
- **Idiomatic Functional Programming**: Prefer iterator-based functional
  programming patterns over imperative loops unless it increases complexity. Use
  itertools to be able to do more with iterators and functional programming in
  Rust
- **Comments**: Follow comprehensive commenting guidelines (see detailed section
  below)
- **Spacing**: Leave an empty line in between code blocks to allow vim curly
  braces jumping between blocks and for easier reading
- **CRITICAL: Import Organization**: Follow a consistent two-group import
  pattern throughout the codebase:
  - **Group 1 - External imports**: All imports from external crates including
    `std`, `alloy`, `cqrs_es`, `serde`, `tokio`, etc. No empty lines between
    external imports.
  - **Empty line separating the groups**
  - **Group 2 - Internal imports**: All imports from our codebase using
    `crate::` and `super::`. No empty lines between internal imports.
  - **FORBIDDEN**: Three or more import groups, imports separated by empty lines
    within a group
  - **FORBIDDEN**: Function-level imports. Always use top-of-module imports.
  - Module declarations (`mod foo;`) can appear between imports if needed
  - This pattern applies to ALL modules including test modules
    (`#[cfg(test)] mod tests`)
  - Example: `use std::sync::Arc; use alloy::primitives::Address;` [blank line]
    `use crate::foo::Bar; use super::Baz;`
- **Import Conventions**: Use qualified imports when they prevent ambiguity
  (e.g. `contract::Error` for `alloy::contract::Error`), but avoid them when the
  module is clear (e.g. use `info!` instead of `tracing::info!`). Never use
  imports inside functions. We don't do function-level imports, instead we do
  top-of-module imports. Note that I said top-of-module and not top-of-file,
  e.g. imports required only inside a tests module should be done in the module
  and not hidden behind #[cfg(test)] at the top of the file
- **Error Handling**: Avoid `unwrap()` even post-validation since validation
  logic changes might leave panics in the codebase
- **CRITICAL: Error Type Design**: **NEVER create error variants with opaque
  String values that throw away type information**. This is strictly forbidden
  and violates our error handling principles:
  - **FORBIDDEN**: `SomeError(String)` - throws away all type information
  - **FORBIDDEN**: `SomeError { message: String }` - loses context and source
  - **FORBIDDEN**: Converting errors to strings with `.to_string()` or string
    interpolation
  - **REQUIRED**: Use `#[from]` attribute with thiserror to wrap errors and
    preserve all type information
  - **REQUIRED**: Each error variant must preserve the complete error chain with
    `#[source]`
  - **REQUIRED**: Discover error variants as needed during implementation, not
    preemptively
  - **Principle**: Error types must enable debugging and preserve all context -
    opaque strings make debugging impossible
  - When adding new error variants:
    1. Only add variants when you encounter actual errors during implementation
    2. Use `#[from]` for error types from external crates
    3. Use `#[source]` for wrapped errors in struct variants
    4. Never use `.to_string()`, `.map_err(|e| Foo(e.to_string()))`, or similar
       patterns
    5. If an error needs context, use a struct variant with fields + `#[source]`
- **Silent Early Returns**: Never silently return in error/mismatch cases.
  Always log a warning or error with context before early returns in `let-else`
  or similar patterns. Silent failures hide bugs and make debugging nearly
  impossible
- **No Duplicate Values in Debug Output**: Never hardcode values that exist
  elsewhere in the implementation (URLs, paths, constants, config values, etc.)
  into debug/log statements. These duplicates will inevitably drift out of sync
  with the real implementation, misleading debugging efforts instead of helping
  them. Always log the actual runtime value being used, not a hardcoded copy
- **Visibility Levels**: Always keep visibility levels as restrictive as
  possible (prefer `pub(crate)` over `pub`, private over `pub(crate)`) to enable
  better dead code detection by the compiler and tooling. This makes the
  codebase easier to navigate and understand by making the relevance scope
  explicit

### CRITICAL: Financial Data Integrity

**This is a mission-critical financial application. The following patterns are
STRICTLY FORBIDDEN and can result in catastrophic financial losses:**

**NEVER** write code that silently provides wrong values, hides conversion
errors, or masks failures in any way. This includes but is not limited to:

- Defensive value capping that hides overflow/underflow
- Fallback to default values on conversion failure
- Silent truncation of precision
- Using `unwrap_or(default_value)` on financial calculations
- Using `unwrap_or_default()` on monetary values
- Conversion functions that "gracefully degrade" instead of failing

**ALL financial operations must use explicit error handling with proper error
propagation. Here are examples of forbidden patterns and their correct
alternatives:**

#### Error Categories That Must Fail Fast

1. **Numeric Conversions**: Any conversion between numeric types must use
   `try_into()` or equivalent
2. **Precision Loss**: Operations that could lose precision must be explicit
   about it
3. **Range Violations**: Values outside expected ranges must error, not clamp
4. **Parse Failures**: String-to-number parsing must propagate parse errors
5. **Arithmetic Operations**: Use checked arithmetic for all financial
   calculations
6. **Database Constraints**: Let database constraints fail rather than masking
   violations

**Remember: In financial applications, ALWAYS fail fast with clear errors rather
than continue with potentially corrupted data. Silent data corruption leads to
massive losses and regulatory violations.**

### CRITICAL: Security and Secrets Management

**NEVER read files containing secrets, credentials, or sensitive configuration
without explicit user permission.**

This project handles financial transactions and sensitive API credentials.
Unauthorized access to secrets can lead to:

- Account compromise
- Financial losses
- Security breaches

#### Files That Require Explicit Permission

The following files MUST NOT be read without explicit user permission:

- `.env` - Environment variables containing API keys, secrets, and credentials
- `.env.*` - Environment-specific configuration files (`.env.local`,
  `.env.production`, etc.)
- `credentials.json` - Credential storage files
- `*.key`, `*.pem` - Private keys and certificates
- `*.p12`, `*.pfx` - Certificate bundles
- Database files containing sensitive data (unless necessary for debugging with
  permission)
- Any file that may contain API keys, tokens, passwords, or other secrets

#### Required Practice

**Before reading any file that may contain secrets:**

1. **Ask the user explicitly** for permission to read the file
2. **Explain why** you need to read it
3. **Wait for confirmation** before proceeding

**Alternatives**: Ask user to verify env vars are set, request sanitized output,
check `.env.example` instead of `.env`, or review code that uses configuration.

### Testing Strategy

- **Mock Blockchain Interactions**: Uses `alloy::providers::mock::Asserter` for
  deterministic testing
- **HTTP API Mocking**: `httpmock` crate for Charles Schwab API testing
- **Database Isolation**: In-memory SQLite databases for test isolation
- **Edge Case Coverage**: Comprehensive error scenario testing for trade
  conversion logic
- **Testing Principle**: Only cover happy paths with all components working and
  connected in integration tests and cover everything in unit tests
- **CRITICAL: Tests must assert CORRECT behavior, never "document gaps"**: Tests
  exist to verify the system works correctly. If code is broken or incomplete,
  tests MUST assert the correct expected behavior and FAIL until the code is
  fixed. NEVER write tests that assert incorrect behavior with comments like
  "documenting the gap" or "will fix later". A failing test is the correct way
  to flag broken code - it forces the issue to be addressed. Tests that pass
  while asserting wrong behavior are worse than no tests at all.
- **Debugging failing tests**: When debugging tests with failing assert! macros,
  add additional context to the assert! macro instead of adding temporary
  println! statements
- **No ad-hoc debugging scripts**: Never write ad-hoc scripts, code snippets, or
  temporary files outside the project for debugging. If you need to debug an
  issue, write a proper test function within the project's test suite and remove
  it once you've obtained the information you need.
- **Test Quality**: Never write tests that only exercise language features
  without testing our application logic. Tests should verify actual business
  logic, not just struct field assignments or basic language operations
- **Property-Based Testing**: Use `proptest` for property-based tests whenever
  there are clear invariants to verify. Property tests are excellent for:
  - Parsing/serialization roundtrips
  - Boundary conditions (e.g., message length validation)
  - Invariants that should hold for all inputs (e.g., extracted data matches
    input regardless of surrounding bytes)
  - Numeric operations where edge cases are hard to enumerate manually

#### Writing Meaningful Tests

Tests should verify our application logic, not just language features. Avoid
tests that only exercise struct construction or field access without testing any
business logic.

❌ Bad: Testing struct field assignments (just tests Rust, not our code). ✅
Good: Testing actual business logic like `config.calculate_next_poll_delay()`
returning values within expected bounds.

### Workflow Best Practices

- **Always run verification steps before handing over a piece of work** (skip if
  only documentation/markdown files were changed). Run them in this order to
  fail fast:
  1. `cargo check` - fastest, catches compilation errors first
  2. `cargo test -q` - only run after check passes
  3. `cargo clippy` - only run after tests pass (fixing lints can break tests)
  4. `cargo fmt` - always run last to ensure clean formatting
  5. **Diff review** - after all checks pass, review staged changes and revert
     any chunks without clear justification (see "Before handing over" section)

#### CRITICAL: Lint Policy

**NEVER add `#[allow(clippy::*)]` attributes or disable any lints without
explicit permission.** This is strictly forbidden. When clippy reports issues,
you MUST fix the underlying code problems, not suppress the warnings.

**Required approach for clippy issues:**

1. **Refactor the code** to address the root cause of the lint violation
2. **Break down large functions** into smaller, more focused functions
3. **Improve code structure** to meet clippy's standards
4. **Use proper error handling** instead of suppressing warnings

**Examples of FORBIDDEN practices:**

```rust
// ❌ NEVER DO THIS - Suppressing lints is forbidden
#[allow(clippy::too_many_lines)]
fn large_function() { /* ... */ }

#[allow(clippy::needless_continue)]
// ❌ NEVER DO THIS - Fix the code structure instead
```

**Required approach:**

```rust
// ✅ CORRECT - Refactor to address the issue
fn process_data() -> Result<(), Error> {
    let data = get_data()?;
    validate_data(&data)?;
    save_data(&data)?;
    Ok(())
}

fn validate_data(data: &Data) -> Result<(), Error> {
    // Extracted validation logic
}

fn save_data(data: &Data) -> Result<(), Error> {
    // Extracted saving logic
}
```

**If you encounter a clippy issue:**

1. Understand WHY clippy is flagging the code
2. Refactor the code to address the underlying problem
3. If you believe a lint is incorrect, ask for permission before suppressing it
4. Document your reasoning if given permission to suppress a specific lint

**Exception for third-party macro-generated code:**

When using third-party macros, such as `sol!` to generate Rust code , lint
suppression is acceptable for issues that originate from the contract's function
signatures, which we cannot control.

For example, to deal with a function generated from a smart contract's ABI, we
can add `allow` inside the `sol!` macro invocation.

```rust
// ✅ CORRECT - Suppressing lint for third-party ABI generated code
sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IPyth, "node_modules/@pythnetwork/pyth-sdk-solidity/abis/IPyth.json"
);
```

This policy ensures code quality remains high and prevents technical debt
accumulation through lint suppression.

### Commenting Guidelines

Code should be primarily self-documenting through clear naming, structure, and
type modeling. Comments should only be used when they add meaningful context
that cannot be expressed through code structure alone.

#### When to Use Comments

##### ✅ DO comment when:

- **Complex business logic**: Explaining non-obvious domain-specific rules or
  calculations
- **Algorithm rationale**: Why a particular approach was chosen over
  alternatives
- **External system interactions**: Behavior that depends on external APIs or
  protocols
- **Non-obvious technical constraints**: Performance considerations, platform
  limitations
- **Test data context**: Explaining what mock values represent or test scenarios
- **Workarounds**: Temporary solutions with context about why they exist

##### ❌ DON'T comment when:

- The code is self-explanatory through naming and structure
- Restating what the code obviously does
- Describing function signatures (use doc comments instead)
- Adding obvious test setup descriptions
- Marking code sections that are clear from structure
- **Referencing internal task tracking or ephemeral context**: NEVER leave
  comments like `// Task 7: ...`, `// TODO from ticket XYZ`,
  `// Part of sprint 5 work`, or any reference to task numbers, issue trackers,
  todo lists, or session context that won't exist for future readers. These
  comments are meaningless to PR reviewers, future maintainers, and anyone
  without access to your internal state. If the code needs explanation, explain
  WHAT it does and WHY - not which task number led to writing it.

#### Good Comment Examples

```rust
// If the on-chain order has USDC as input and an 0x tokenized stock as
// output then it means the order received USDC and gave away an 0x  
// tokenized stock, i.e. sold, which means that to take the opposite
// trade in schwab we need to buy and vice versa.
let (schwab_ticker, schwab_instruction) = 
    if onchain_input_symbol == "USDC" && onchain_output_symbol.ends_with("0x") {
        // ... complex mapping logic
    }

// We need to get the corresponding AfterClear event as ClearV2 doesn't
// contain the amounts. So we query the same block number, filter out
// logs with index lower than the ClearV2 log index and with tx hashes
// that don't match the ClearV2 tx hash.
let after_clear_logs = provider.get_logs(/* ... */).await?;

// Test data representing 9 shares with 18 decimal places
alice_output: U256::from_str("9000000000000000000").unwrap(), // 9 shares (18 dps)

/// Helper that converts a fixed-decimal `U256` amount into an `f64` using
/// the provided number of decimals.
///
/// NOTE: Parsing should never fail but precision may be lost.
fn u256_to_f64(amount: U256, decimals: u8) -> Result<f64, ParseFloatError> {
```

#### Bad Comment Examples

```rust
// ❌ Redundant - function name says this: spawn_automatic_token_refresh(pool, env);
// ❌ Obvious from context: // Store test tokens
// ❌ Test section markers: // 1. Test token refresh integration
```

#### Comment Maintenance

Use `///` doc comments for public APIs. Remove/update comments when refactoring.
If a comment is needed to explain what code does, consider refactoring instead.
Keep comments focused on "why" rather than "what".

### Code style

#### ASCII only in code

Use ASCII characters only in code and comments. For arrows, use `->` not `→`.
Unicode breaks vim navigation and grep workflows.

#### Module Organization

Organize code within modules by importance and visibility:

- **Public API first**: Place public functions, types, and traits at the top of
  the module where they are immediately visible to consumers
- **Private helpers below public code**: Place private helper functions, types,
  and traits immediately after the public code that uses them
- **Implementation blocks next to type definitions**: Place `impl` blocks after
  the type definition

This organization pattern makes the module's public interface clear at a glance
and keeps implementation details appropriately subordinate.

**Example:** Public types first -> impl blocks -> public functions -> private
helpers.

This pattern applies across the entire workspace, including both the main crate
and sub-crates like `st0x-execution`.

#### Use `.unwrap` over boolean result assertions in tests

Instead of `assert!(result.is_err()); assert!(matches!(...))`, write
`assert!(matches!(result.unwrap_err(), SchwabError::Reqwest(_)));` directly.
Similarly, instead of `assert!(result.is_ok()); assert_eq!(...)`, write
`assert_eq!(result.unwrap(), "expected_value");` so unexpected values are shown.

#### Assertions must be specific

Test assertions must check for the exact expected behavior, not vague
alternatives. Never use `||` in assertions to accept multiple possible outcomes
unless those outcomes are genuinely equivalent.

```rust
// ❌ BAD - Lazy, accepts vaguely similar outcomes
assert!(
    output.contains("Failed") || output.contains("❌"),
    "Output should indicate failure"
);

// ❌ BAD - Too permissive, doesn't verify actual behavior
assert!(result.is_some());

// ✅ GOOD - Checks for exact expected output
assert!(
    output.contains("❌ Failed to place order"),
    "Expected failure message, got: {output}"
);

// ✅ GOOD - Verifies specific value
assert_eq!(result.unwrap().order_id, "12345");
```

If you find yourself writing `||` in an assertion, ask: are these outcomes
actually equivalent? If not, you probably don't understand what the code should
do, and need to investigate before writing the test.

#### Type modeling examples

**Principle**: Choose the type representation that most accurately models the
domain. Don't blindly apply patterns - think carefully about whether structs,
enums, newtypes, or other constructs best represent the concept at hand.

##### Make invalid states unrepresentable:

Instead of using multiple fields that can contradict each other:

```rust
// ❌ Bad: Multiple fields can be in invalid combinations
pub struct Order {
    pub status: String,  // "pending", "completed", "failed"
    pub order_id: Option<String>,  // Some when completed, None when pending
    pub executed_at: Option<DateTime<Utc>>,  // Some when completed
    pub price_cents: Option<i64>,  // Some when completed
    pub error_reason: Option<String>,  // Some when failed
}
```

Use enum variants to encode valid states:

```rust
// ✅ Good: Each state has exactly the data it needs
pub enum OrderStatus {
    Pending,
    Completed {
        order_id: String,
        executed_at: DateTime<Utc>,
        price_cents: i64,
    },
    Failed {
        failed_at: DateTime<Utc>,
        error_reason: String,
    },
}
```

##### Use newtypes for domain concepts:

```rust
// ❌ Bad: Easy to mix up parameters of the same type
fn place_order(symbol: String, account: String, amount: i64, price: i64) { }

// ✅ Good: Type system prevents mixing incompatible values
#[derive(Debug, Clone)]
struct Symbol(String);

#[derive(Debug, Clone)]
struct AccountId(String);

#[derive(Debug)]
struct Shares(i64);

#[derive(Debug)]
struct PriceCents(i64);

fn place_order(symbol: Symbol, account: AccountId, amount: Shares, price: PriceCents) { }
```

##### The Typestate Pattern:

Encodes runtime state in compile-time types, eliminating runtime checks.

```rust
// ✅ Good: State transitions enforced at compile time
struct Task<State> { data: TaskData, state: State }
impl Task<Start> { fn begin(self) -> Task<InProgress> { ... } }
impl Task<InProgress> { fn complete(self) -> Task<Complete> { ... } }
```

Use typestate for protocol enforcement (`Connection<Unauthenticated>` →
`Connection<Authenticated>`) and builder patterns (`RequestBuilder<NoUrl>` →
`RequestBuilder<HasUrl>`).

#### Avoid deep nesting

Prefer flat code over deeply nested blocks to improve readability and
maintainability. This includes test modules - do NOT nest submodules inside
`mod tests`. Put all tests directly in the `tests` module.

##### Techniques for flat code:

```rust
// ❌ Nested: if let Some(data) = data { if !data.is_empty() { if data.len() > 5 { ... } } }
// ✅ Flat with early returns:
fn process_data(data: Option<&str>) -> Result<String, Error> {
    let data = data.ok_or(Error::None)?;
    if data.is_empty() { return Err(Error::Empty); }
    if data.len() <= 5 { return Err(Error::TooShort); }
    Ok(data.to_uppercase())
}
```

##### Use let-else pattern for guard clauses:

```rust
// Use let-else to flatten nested if-let chains
let Some(trade_data) = convert_event_to_trade(event) else {
    return Err(Error::ConversionFailed);
};
let Some(symbol) = trade_data.extract_symbol() else {
    return Err(Error::NoSymbol);
};
```

##### Use pattern matching with guards:

```rust
// ❌ Nested if-let: if let Some(data) = input { if state == Ready && data.is_valid() { ... } }
// ✅ Pattern match: match (input, state) { (Some(d), Ready) if d.is_valid() => process(d), ... }
```

#### Struct field access

Avoid creating unnecessary constructors or getters when they don't add logic
beyond setting/getting field values. Use public fields directly instead.

Use struct literal syntax directly (`SchwabTokens { access_token: "...", ... }`)
and access fields directly (`tokens.access_token`). Don't create `fn new()`
constructors or `fn field(&self)` getters unless they add meaningful logic
beyond setting/getting field values.
