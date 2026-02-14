# AGENTS.md

This file provides guidance to AI agents working with code in this repository.

**CRITICAL: File Size Limit** - AGENTS.md must not exceed 40,000 characters.
When editing this file, check the character count (`wc -c AGENTS.md`). If over
the limit:

- **NEVER remove guidelines** - only condense verbose explanations
- **Condense code examples first** - examples are illustrative, rules are not
- **Remove redundancy** - if a guideline duplicates another, keep one reference
- **Shorten explanations** - preserve the rule, reduce the elaboration

## Documentation

**Before doing any work**, read these documents:

1. **[SPEC.md](SPEC.md)** — the north star. Describes what this service should
   be. All new features must be spec'ed here first. If your change contradicts
   the spec, either update the spec first (with user approval) or change your
   approach. Implementation is downstream from the spec.
2. **[docs/domain.md](docs/domain.md)** — naming conventions and domain
   terminology. All code must use the names defined here. If a name isn't in
   this doc, check existing code for precedent before inventing one.

**Read when relevant** to your task:

- [docs/alloy.md](docs/alloy.md) - Alloy types, FixedBytes aliases,
  `::random()`, mocks, encoding, compile-time macros
- [docs/cqrs.md](docs/cqrs.md) - CQRS/ES patterns (upcasters, views, replay,
  services)

**Update at the end:**

- **README.md** — if project structure, features, commands, or architecture
  changed
- **ROADMAP.md** — mark completed issues, link PRs

## Ownership Principles

**CRITICAL: Take full ownership. Never deflect responsibility.**

- **Fix all problems immediately** - regardless of who introduced them. Never
  say "this is a pre-existing issue" as justification for not fixing it.
- **Meet ALL constraints** - when editing a file with size limits, ensure the
  ENTIRE file meets the limit, not just your additions.
- **No warnings or errors pass through** - CI and review catch everything. If
  you see a warning/error, it's your responsibility to fix it now.
- **Work until all tasks are complete** - do not stop until every assigned task
  is done, unless you need input from the user to proceed. If blocked on one
  task, move to the next. Only stop working when the task list is clear or you
  genuinely cannot proceed without user input.

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

**Before implementing:** Ensure feature is in SPEC.md -> has GitHub issue ->
plan the implementation.

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

- `cargo test --workspace -q` - Run all tests (both main and execution crates)
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
- `cargo clippy --workspace --all-targets --all-features -- -D clippy::all` -
  Run Clippy for linting
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
- Expects symbol pairs of USDC + tokenized equity with "t" prefix (e.g.,
  "tAAPL")
- Determines Schwab trade direction: buying tokenized equity onchain -> selling
  on Schwab
- Calculates prices in cents and maintains onchain/offchain trade ratios

**Async Event Processing Architecture**

- Each blockchain event spawns independent async execution flow
- Handles throughput mismatch: fast onchain events vs slower Schwab API calls
- No artificial concurrency limits - processes events as they arrive
- Flow: Parse Event -> SQLite Deduplication Check -> Schwab API Call -> Record
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
tracking (pending -> completed/failed), retry logic with exponential backoff

### Configuration

Configuration is split into plaintext config (`--config`, see
`example.config.toml`) and encrypted secrets (`--secrets`, see
`example.secrets.toml`). The reporter binary only takes `--config` (no secrets).

### Naming Conventions

Code names must be consistent with **[docs/domain.md](docs/domain.md)**, which
is the source of truth for terminology and naming conventions.

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
- **Symbol Prefix Convention**: Tokenized equities use "t" prefix to distinguish
  from base assets (e.g., tAAPL, tTSLA, tSPYM)
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
- **CRITICAL: Single CQRS Framework Instance Per Aggregate**: In the main bot
  flow, each aggregate type must have exactly ONE `SqliteCqrs<A>` instance,
  constructed once during startup in `Conductor::start`, then passed to all
  consumers. This prevents silent production bugs where missing query processors
  cause events to persist without triggering required side effects.
  - **FORBIDDEN**: Calling `sqlite_cqrs()` or `CqrsFramework::new()` anywhere in
    the server binary's code path outside `Conductor::start`
  - **FORBIDDEN**: Creating multiple `SqliteCqrs<A>` instances for the same
    aggregate type in the bot flow
  - **REQUIRED**: When adding a new query processor, add it to the framework
    construction in `Conductor::start`
  - **ALLOWED**: Direct construction in test code, CLI code, and migration code
    (different execution contexts with intentionally different query processor
    needs)
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
- **CRITICAL: Import Organization**: Follow a consistent three-group import
  pattern throughout the codebase:
  - **Group 1 - External imports**: All imports from external crates including
    `std`, `alloy`, `cqrs_es`, `serde`, `tokio`, etc. No empty lines within.
  - **Empty line**
  - **Group 2 - Workspace imports**: Imports from other workspace crates
    (`st0x_execution`). No empty lines within.
  - **Empty line**
  - **Group 3 - Crate-internal imports**: Imports using `crate::` and `super::`.
    No empty lines within.
  - Groups 2 or 3 may be absent if unused; never add an empty group
  - **FORBIDDEN**: Empty lines within a group, imports out of group order
  - **FORBIDDEN**: Function-level imports. Always use top-of-module imports.
    **Sole exception**: enum variant imports (`use MyEnum::*` or
    `use MyEnum::{A, B, C}`) inside function bodies to avoid repetitive
    qualification. Enum variant imports are never allowed at module level.
  - Module declarations (`mod foo;`) can appear between imports if needed
  - This pattern applies to ALL modules including test modules
    (`#[cfg(test)] mod tests`)
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
  - **FORBIDDEN**: Using `format!()` to convert typed values into String fields
    in error variants (e.g., `format!("{side:?}")`) - store the actual typed
    value instead
  - **FORBIDDEN**: Unpacking newtypes into their inner type to store in error
    variants or anywhere else. If you have `Symbol(String)`, store `Symbol`, not
    `String`. If you have `OrderSide`, store `OrderSide`, not a `String`
    representation. Never discard type safety by extracting inner values.
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

**NEVER** silently mask failures: no defensive capping, fallback defaults,
precision truncation, `unwrap_or()`, `unwrap_or_default()`, or graceful
degradation on financial values. ALL financial operations must use explicit
error handling with proper error propagation.

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

**Protected files** (require explicit permission): `.env*`, `credentials.json`,
`*.key`, `*.pem`, `*.p12`, `*.pfx`, database files with sensitive data. Ask
permission, explain why, wait for confirmation. Prefer `.env.example` or
reviewing code that uses configuration instead of reading secrets directly.

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
- **CRITICAL: NEVER delete, skip, or bypass existing tests or checks to make a
  refactor easier.** If a refactor breaks existing tests, you MUST either: (1)
  adapt the tests to the new design while preserving their coverage, (2) find a
  design that keeps the tests passing, or (3) stop and ask the user how to
  proceed. Replacing tests with comments like "these no longer apply" or
  "validated by other means" is strictly forbidden. Tests are constraints on
  correctness -- if your change can't satisfy them, the change is wrong.
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

Tests must verify application logic, not language features. Testing struct field
assignments is useless; test actual behavior like
`config.calculate_next_poll_delay()` returning expected values.

### Workflow Best Practices

- **Always run verification steps before handing over a piece of work** (skip if
  only documentation/markdown files were changed). Run them in this order to
  fail fast:
  1. `cargo check` - fastest, catches compilation errors first
  2. `cargo test --workspace -q` - only run after check passes
  3. `cargo clippy` - only run after tests pass (fixing lints can break tests)
  4. `cargo fmt` - always run last to ensure clean formatting
  5. **Diff review** - after all checks pass, review staged changes and revert
     any chunks without clear justification (see "Before handing over" section)
- **CRITICAL: Do NOT run clippy until ALL substantive work is done.** Clippy is
  a polish step. Running it while tasks remain open is wasted effort -
  subsequent code changes will introduce new lint issues. Complete every task on
  the list first (`cargo check` + `cargo test` passing), then run clippy as a
  final pass before handing over.

#### CRITICAL: Lint Policy

**NEVER add `#[allow(clippy::*)]` attributes or disable any lints without
explicit permission.** This is strictly forbidden. When clippy reports issues,
you MUST fix the underlying code problems, not suppress the warnings.

**Required approach for clippy issues:**

Clippy lint errors are not about the exact specific cosmetic thing -- they are
often indications of poor design or broader things worth reconsidering. Upon
encountering a lint violation:

1. **Re-evaluate the design** in the context of what was flagged. If the lint
   reveals a flaw in the broader design or architecture, fix that
2. **Refactor the code** to address the root cause of the lint violation
3. **Break down large functions** into smaller, more focused functions
4. **Improve code structure** to meet clippy's standards
5. **Use proper error handling** instead of suppressing warnings
6. If the violation is intentional and makes perfect sense in context, **stop
   and request explicit permission** from the user before suppressing

**FORBIDDEN: Obscure workarounds that silence the linter without fixing the
problem.** Do not restructure code in weird ways, add unnecessary indirection,
wrap things in newtypes, or use any other trick whose sole purpose is making the
lint go away. Either fix the underlying design issue the lint is pointing at, or
request permission to suppress. There is no third option.

**Exception**: Lint suppression inside `sol!` macros is acceptable for issues
from contract ABI signatures we cannot control.

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

#### Examples

Good: explaining non-obvious business logic ("USDC input + t-prefix output means
the order sold tokenized equity, so hedge by buying on Schwab"), test data
context ("9 shares with 18 decimal places"), or external system constraints
("ClearV2 doesn't contain amounts, so query AfterClear").

Bad: restating what code does (`// Store test tokens`), redundant with function
name, test section markers (`// 1. Test token refresh`).

Use `///` for public APIs. Keep comments focused on "why" not "what".

### Code style

#### ASCII only in code

Use ASCII characters only in code and comments. For arrows, use `->` not `->`.
Unicode breaks vim navigation and grep workflows.

#### No single-letter variables or arguments

Single-letter names (`e`, `x`, `n`, `s`, etc.) are **FORBIDDEN** everywhere -
variables, function arguments, closure parameters, generic type params in
function signatures. Always use descriptive names. The only exception is
conventional iterator variables in very short closures where the type makes the
meaning unambiguous (e.g., `|event| event.payload`), but even then prefer a
descriptive name.

#### Module Organization

Organize code within modules by importance to the reader, not by when it was
added. Public API first, then private implementation, then tests. A reader
should understand what a module provides and how to use it before encountering
implementation details.

**Module docstrings**: Every module should have a `//!` docstring explaining
what it provides. This makes it obvious what parts of the module are most
important (and should be placed higher up) while also helping identify when
something doesn't belong in the module and should be moved elsewhere.

**Determining importance**: What a module _does_ (types consumers use, functions
they call) matters more than what _supports_ it (error types, internal helpers,
private traits). If code A uses code B, then A is more important than B -
because B exists to serve A, not the other way around. For example, a function
that can fail is more important than the error type it returns, since the error
type is a byproduct of the function's implementation.

#### Line width in docstrings and macros

All doc comments (`//!` and `///`) and long strings inside attribute macros
(e.g., `#[error(...)]`) must not exceed 100 characters per line. `cargo fmt`
does not enforce this (without nightly rustfmt), so be careful and check
manually.

For multi-line `#[error]` strings, use `\` continuation:

```rust
#[error(
    "Expected IO to contain USDC and one tokenized equity \
     (t prefix) but got {0} and {1}"
)]
```

#### Never use `is_err()`/`is_ok()` assertions in tests

`assert!(result.is_err())` is never acceptable - every error test must verify
the exact error variant:

```rust
// FORBIDDEN: doesn't check which error
assert!(result.is_err());

// REQUIRED: unwrap first, then assert the exact variant
let error = result.unwrap_err();
assert!(matches!(error, SomeError::SpecificVariant { .. }));
```

`assert!(result.is_ok())` is equally bad - just call `.unwrap()`.

**FORBIDDEN**: `assert!(x.is_err())`, `assert!(x.is_ok())`

#### Assertions must be specific

Check for exact expected behavior. Never use `||` in assertions to accept
multiple outcomes unless genuinely equivalent. Use `assert_eq!` with specific
values, not `assert!(result.is_some())`. If writing `||` in an assertion, you
likely don't understand the expected behavior - investigate first.

#### Type modeling examples

**Principle**: Choose the type representation that most accurately models the
domain. Don't blindly apply patterns - think carefully about whether structs,
enums, newtypes, or other constructs best represent the concept at hand.

##### Make invalid states unrepresentable:

Instead of multiple optional fields that can contradict each other (e.g.,
`status: String` + `order_id: Option<String>` + `error_reason: Option<String>`),
use enum variants where each state carries exactly the data it needs.

##### Use newtypes for domain concepts:

Wrap primitives in newtypes to prevent mixing incompatible values at call sites
(e.g., `Symbol(String)`, `AccountId(String)`, `Shares(i64)`, `PriceCents(i64)`).

##### The Typestate Pattern:

Encode runtime state in compile-time types to eliminate runtime checks. Use for
protocol enforcement and builder patterns (e.g., `Connection<Unauthenticated>`
-> `Connection<Authenticated>`).

#### Avoid deep nesting

Prefer flat code over deeply nested blocks to improve readability and
maintainability. This includes test modules - do NOT nest submodules inside
`mod tests`. Put all tests directly in the `tests` module.

##### Techniques for flat code:

- **Early returns** with `?` and `return Err(...)` instead of nested `if let`
- **let-else** for guard clauses:
  `let Some(value) = expr else { return Err(...); };`
- **Pattern matching with guards** instead of nested `if let` chains

#### Struct field access

Avoid creating unnecessary constructors or getters when they don't add logic
beyond setting/getting field values. Use public fields directly instead.

Use struct literal syntax directly (`SchwabTokens { access_token: "...", ... }`)
and access fields directly (`tokens.access_token`). Don't create `fn new()`
constructors or `fn field(&self)` getters unless they add meaningful logic
beyond setting/getting field values.

#### Prefer destructuring over `.0` access

For newtypes, prefer `let TypeName(inner) = value` over `value.0`. The
destructuring pattern names the type explicitly, making the code
self-documenting:

```rust
// GOOD: reader sees exactly what type is being unwrapped
let Table(table) = Entity::PROJECTION.ok_or(ProjectionError::NoTable)?;

// BAD: .0 is opaque — reader must look up what type this is
let table = entity_projection.0;
```

#### No one-liner helpers

If a helper function's body is a single expression, it's useless indirection --
just inline the call. A function that only wraps another function call adds a
name to learn and a place to jump to without reducing complexity. Helpers earn
their existence by encapsulating multi-step logic, not by renaming a single
operation.

#### Don't split simple-but-long pattern matches

A function that consists of a single `match` with many trivial arms (e.g. state
machine transitions, event mapping) should stay as one function even if it
exceeds line count lints. Each arm is simple field mapping -- extracting arms
into helpers adds indirection without improving readability. When
`too_many_lines` fires on such functions, request permission to suppress the
lint rather than extracting helpers that exist only to satisfy the line count.
