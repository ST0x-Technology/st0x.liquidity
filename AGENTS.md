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
- [docs/conductor.md](docs/conductor.md) - Conductor orchestration layer:
  task-supervisor, apalis workers, OrderFillMonitor, bon builder, lifecycle
- [docs/cqrs.md](docs/cqrs.md) - Event sourcing with st0x-event-sorcery
- [docs/float.md](docs/float.md) - Float type, precision-safe financial
  arithmetic, Solidity-backed operations for guaranteed compatibility with smart
  contracts
- [docs/feature-flags.md](docs/feature-flags.md) - Cargo feature flags,
  `test-support` vs `cfg(test)`, common pitfalls with dead code warnings
- [docs/ttdd.md](docs/ttdd.md) - Type-driven TDD workflow: scientific method
  applied to software, failing tests before implementation

**Update at the end:**

- **README.md** — if project structure, features, commands, or architecture
  changed
- **Linear** — move completed issues to Done and link the PR. When a PR is
  chained (depends on a parent PR), close both so status is current by the time
  they merge.
- **`docs/`** — when research or trial-and-error reveals non-obvious patterns,
  pitfalls, or framework behavior, document it in the relevant `docs/` file (or
  create a new one) to prevent rediscovery. Prioritize documenting:
  framework-specific idioms, integration gotchas, and "X doesn't work because Y"
  findings.

## Ownership Principles

**CRITICAL**: Fix all problems immediately regardless of origin. Meet ALL
constraints (file size limits apply to entire file). No warnings/errors pass
through. Work until all tasks complete unless blocked needing user input.

**CRITICAL: If you know you caused a problem and know how to fix it, fix it
immediately.** Do not restate the request, ask for confirmation, or wait for
permission to do the obvious corrective work.

## Communication

- **Do not run commands to "show" output to the user.** The CLI truncates
  output. If you need the user to review something, explicitly ask them to look
  at it. Do not run `git diff` expecting the user to see output.
- **Never say "standing by" or wait for permission once work has been
  authorized.** When the user has given you a task, do it -- do not ask "want me
  to proceed?" or pause for a go-ahead between subtasks. If multiple related
  review threads or requests have been queued, work through them in sequence
  without asking between each one. Asking for confirmation after the user has
  already told you what to do wastes their time and signals you weren't
  listening. Only stop and ask when you are actually blocked on a decision the
  user must make, not as a polite checkpoint.
- **When you need user input, use the structured input prompt your tooling
  exposes -- not prose at the end of a long response.** Wall-of-text questions
  get missed. Batch all open decisions in one prompt, not a trickle.

## Planning Hierarchy

The project uses a strict document hierarchy:

1. **SPEC.md** - Source of truth for system behavior. Features documented here
   before implementation.
2. **Linear issues/projects** - Downstream from spec. Describe problems, not
   solutions. See [docs/linear-workflow.md](docs/linear-workflow.md).
3. **Planning** - Downstream from issues. Implementation plans before coding.
4. **Tests** - Downstream from plan. Written before implementation (TDD).
5. **Implementation** - Makes the tests pass.

**Before implementing:** SPEC.md -> Linear issue -> plan.

### Goal-Oriented Planning

Organize around the **goal**, not implementation streams. Start from the desired
end state and work backwards. Implementation details (crate, branch) are
downstream from the goal.

### Epic Decomposition for Parallel Execution

Decompose epics to maximize independent parallel execution:

1. **Identify coupling boundaries.** Work touching disjoint code areas can
   proceed in parallel.
2. **Sequence only where necessary.** A branch depends on another only when it
   needs types/traits/behavior introduced by that branch.
3. **Every branch must be independently valid.** Each PR must pass CI and make
   sense on its own -- never leave a broken intermediate state.
4. **Defer integration.** Push integration PRs to the end, stacked on both
   parallel branches.
5. **Shared dependencies go first.** Extract shared types/traits/schemas into a
   base PR to unblock all downstream branches.
6. **Conflict-prone work goes last.** Schedule after parallel branches merge.
7. **Converge to a single terminal node.** One final PR depends on all parallel
   streams for integration.

### Managing Epics in Linear

An epic is a Linear project grouping related issues toward a single goal. See
[docs/linear-workflow.md](docs/linear-workflow.md) for issue-vs-project rules.

- **Lead with motivation**: the project description states why this work matters
  and what the end state looks like.
- **Show the dependency structure**: use issue relations (blocks/blocked-by) and
  sub-issues so execution order and parallelism are explicit.
- **Reference issues, not solutions**: each issue describes the desired outcome;
  the PR (linked later) describes the solution.
- **Mark progress via status**: advance issues through workflow states and link
  the PR as branches merge; the project completes when its issues do.

## Plan & Review

### Task management

- Keep a granular task list for the current request and update it as work
  progresses.
- Clear completed tasks from the active list so the remaining work is always
  obvious.
- **Questions awaiting user answers are tasks: track them.** Untracked prose
  questions get lost when the user replies about something else.

### While implementing

- **CRITICAL: All new or modified logic MUST have corresponding test coverage.**
  Do not move on from a piece of code until tests are written. This is
  non-negotiable.

### Before deciding on an implementation

Before making changes, deeply understand the task: read the relevant repo docs,
read the relevant source code, form an initial approach, criticize it, and
improve it until the plan is coherent with the repo architecture. Keep the
resulting diff as small and reviewable as possible.

### Handling questions and approach changes

Answer the question first. Don't silently change approach - ask confirmation. If
new approach fails, state what went wrong and ask before reverting. Explicit
confirmation required before changing direction.

When the user has already clearly told you what to do, start doing it
immediately. Do not paraphrase the request back as a confirmation step.

### When issues are pointed out

When the user points out an issue, bug, or problem - fix it immediately. Do not
ask "Want me to fix this?" or "Should I address this?". The user never sends
messages just for the sake of it; when they point out issues, they expect action
(usually a fix, sometimes reproducing, opening a Linear issue, etc. based on
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

Significant architectural decisions are a special case. When existing docs do
not already answer an architectural choice and the decision is important enough
to record, write an ADR under `adrs/$INDEX-$PROPOSAL_NAME.md`, give the user a
brief summary, and stop for review before continuing with that direction. Once
the ADR is approved, treat it as the standing decision and do not ask the same
question again.

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
executing offsetting trades on traditional brokerages (currently Alpaca
Markets). The system captures arbitrage profits from spreads while attempting to
minimize delta exposure through automated hedging.

## Key Development Commands

### Workspace Structure

This project uses a Cargo workspace with:

- **Root crate (`st0x-hedge`)**: Main arbitrage bot application
- **Execution crate (`st0x-execution`)**: Trade execution abstraction library

### Building & Running

**CRITICAL: NEVER use `cargo build` for verification.** It's slower than
`cargo check` and less useful than `cargo nextest run` or `cargo clippy`. Use:

- `cargo check` for fast compilation verification
- `cargo nextest run` for verification with test coverage
- `cargo clippy` for verification with linting

Only use `cargo build` when you actually need the build artifacts (e.g., final
verification before a release, or when the user explicitly asks to run the
binary).

- `cargo run --bin server` - Run the main arbitrage bot
- `cargo run --bin cli -- buy -s AAPL -q 1` - Submit a manual buy order via the
  configured broker
- `cargo run --bin cli` - Run the command-line interface for manual operations

### Testing

- `cargo nextest run --workspace` - Run all tests (both main and execution
  crates)
- `cargo nextest run --lib` - Run library tests only
- `cargo nextest run -p st0x-execution` - Run execution crate tests only
- `cargo nextest run -p st0x-hedge` - Run main crate tests only
- `cargo nextest run <test_name>` - Run specific test

### Database Management

- `sqlx db create` - Create the database
- `sqlx db reset -y` - Drop the database and re-run all migrations
- `sqlx migrate run` - Apply database migrations
- `sqlx migrate revert` - Revert last migration
- `sqlx migrate add <migration_name>` - Create a new migration file
- Database URL configured via `DATABASE_URL` environment variable

**CRITICAL: If Rust build fails with sqlx macro errors like "unable to open
database file" or "(code: 14)", run `sqlx db reset -y` to fix the database.**
NEVER use workarounds like `DATABASE_URL=sqlite://:memory:`.

**CRITICAL: NEVER manually create migration files.** Use
`sqlx migrate add <migration_name>` for proper timestamping and sequencing.

**CRITICAL: New worktrees require database setup.** A new worktree will hit sqlx
compile errors like "unable to open database file". Run `sqlx db reset -y` to
create and migrate the local database before any cargo commands.

**Verifying a migration against real data**: `sqlx db reset -y` only proves a
migration applies to an empty schema, not that it behaves correctly against real
prod/staging data or that legacy events still deserialize under current code.
While developing a migration, run `nix run .#prod-verify-migrations` (or
`.#staging-verify-migrations`) -- it takes a _consistent_ server-side
`VACUUM INTO` snapshot of the live database (`<env>-db-snapshot` in
`infra/default.nix`; a plain `scp` of a live WAL-mode SQLite file can race the
bot's writes and download a torn/corrupt copy, so never `scp` the `.db` file
directly for this purpose), downloads only that snapshot, and runs it through
the `verify-migrations` binary. That binary never mutates the file it's pointed
at (runs against its own internal `VACUUM INTO` copy) and replays every
persisted aggregate under current code, catching legacy event shapes a migration
needs to repair. The same binary gates staging/prod deploys in `deploy.nix`
before the real restart. To point it at an already-downloaded snapshot instead
of pulling a fresh one, run `cargo run --bin verify-migrations -- --db <path>`
directly.

### Dependency Management

**CRITICAL: NEVER manually edit `Cargo.toml` to add dependencies.** Use
`cargo add <crate_name>` for proper version resolution and feature selection.

- `cargo add <crate_name>` - Add a dependency to the current crate
- `cargo add <crate_name> --dev` - Add a dev-dependency
- `cargo add <crate_name> --build` - Add a build-dependency
- `cargo add <crate_name> -F <feature>` - Add a dependency with specific
  features

### Development Tools

- `rainix-rs-static` - Run Rust static analysis
- `cargo clippy --workspace --all-targets --all-features` - Run Clippy for
  linting
- `cargo fmt` - Format code

### Nix Development Environment

- `nix develop` - Enter development shell with all dependencies. ABIs from
  upstream Solidity repos are built by per-feature Nix derivations under `nix/`
  (`forge-std.nix`, `pyth.nix`, `rain-math-float.nix`, `rain-orderbook.nix`) and
  exposed via `ST0X_*_ABI` environment variables.

### Generated paths

Anything generated by tooling (symlinks the dev shell creates, prefetch logs,
scratch output) goes under `.tmp/` — already gitignored. Do **not** add ad-hoc
top-level entries like `.foo-bar.json` to `.gitignore`. If a tool needs a stable
on-disk path, point it at `.tmp/<name>` and let the existing `.tmp/` ignore rule
cover it. Same for `claude-local-ctx/` and `.bacon-locations` which predate this
convention -- new generated paths should not extend that list.

### Configuration Files

| File                    | Purpose                                                                           |
| ----------------------- | --------------------------------------------------------------------------------- |
| `Cargo.toml`            | Workspace definition, `[workspace.lints.clippy]` lint config, shared dependencies |
| `clippy.toml`           | Clippy behavior settings (thresholds, disallowed methods, test permissions)       |
| `flake.nix`             | Nix flake: dev shell, NixOS deployment, helper scripts                            |
| `rust.nix`              | Rust toolchain and cargo/clippy nix config                                        |
| `os.nix`                | NixOS server configuration for deployment                                         |
| `services.nix`          | Systemd service definitions for the deployed server                               |
| `deploy.nix`            | Deployment targets and settings                                                   |
| `disko.nix`             | Disk partitioning for server                                                      |
| `infra/default.nix`     | Infrastructure module aggregator                                                  |
| `infra/secrets.nix`     | Agenix secret declarations (paths, not values)                                    |
| `dashboard/default.nix` | Dashboard build derivation                                                        |
| `dashboard/bun.nix`     | Bun runtime nix packaging for dashboard                                           |
| `e2e/config.toml`       | End-to-end test configuration                                                     |
| `.config/nextest.toml`  | Nextest runner configuration                                                      |
| `crates/*/Cargo.toml`   | Per-crate dependencies and `[lints] workspace = true`                             |

## Development Workflow Notes

- See `.github/PULL_REQUEST_TEMPLATE.md` for PR format requirements
- When running `git diff`, make sure to add `--no-pager` to avoid opening it in
  the interactive view, e.g. `git --no-pager diff`
- **CRITICAL: NEVER run `cargo run` unless explicitly asked by the user.** If
  you want to understand CLI commands or configuration options, read the code.
  If you want to test functionality, write proper tests. There is never a reason
  to run the application speculatively.
- When clippy flags function length (`too_many_lines`) or cognitive complexity,
  treat suppression as the LAST resort and work the options in order first: (1)
  extract a genuine logical chunk -- a cohesive multi-step unit, never a
  one-liner helper; (2) if a repeated pattern is driving the blowout, factor the
  repetition out; (3) if it is irreducible boilerplate, consider a custom macro.
  Only when none of these can bring the function under the threshold without
  making the code worse may an allow be considered -- and adding any
  `#[allow(...)]` requires EXPLICIT user permission first (per the Quality
  Control Policy). Precedent elsewhere in the codebase is NOT permission.

### Push policy

master is protected (PR + CI + 2 approvals), so mistakes can't publish to
master. For feature branches, push is part of the work.

- **Default: `gt ss`** to submit the whole stack. Plain `git push` leaves
  restacked descendants stale.
- **In-flight CI:** pushing cancels in-progress CI; hold if you don't want it
  restarted.
- **Shared stacks:** `gt ss` would clobber upstack work; use `gt submit` on
  current + downstack only. If unsure, ask.

### Commit message hygiene

Set the message at `gt create` time. After that, use `gt modify` without `-m` so
the existing message stays. If amended work has different scope than the
branch's message, it belongs on a different branch -- don't rewrite the message
to absorb it.

### Updating Linear

After completing work or creating issues, keep Linear current:

- **Completed work**: move the issue to Done and link the merged PR. If a PR is
  chained on a parent, close both so status reflects reality at merge time.
- **New issues**: file under the right project/milestone per
  [docs/linear-workflow.md](docs/linear-workflow.md). Bundle a tight cluster of
  related issues under one parent issue.
- **Verification**: cross-check with `linear issue list` (issues) and
  `gh pr list --state all` (PRs). No issue should sit Done with its PR unmerged,
  and no merged PR should leave its issue open.

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
- Supported implementations: AlpacaBrokerApi, MockExecutor

For detailed implementation requirements, see @crates/execution/AGENTS.md

### Core Flow

The `OrderFillMonitor` (`src/conductor/monitor/order_fills.rs`) polls
`ClearV3`/`TakeOrderV3` fills via HTTP `eth_getLogs` and enqueues each fill as
an `AccountForDexTrade` job into the apalis `DexTradeAccountingJobQueue`. The
job worker resolves the symbol, discovers vaults, records the `OnChainTrade`,
updates the `Position` aggregate, and places an offsetting broker order.
Idempotency via `(tx_hash, log_index)` keys.

### Configuration

Plaintext config (`--config`, see `example.config.toml`) and encrypted secrets
(`--secrets`, see `example.secrets.toml`).

**CRITICAL: No silent fallback defaults.** Unless told otherwise, every
operational parameter must be explicitly configured -- missing fields must fail
in tests and at startup, never assume values.

**CRITICAL: A secrets-schema change needs an encrypted-secret update.** When a
field is added to or required in the secrets file, the deployed
`secret/st0x-hedge.toml.age` (shared by prod + staging) must gain it before
deploy, or the `validate-config` gate in `deploy.nix` fails the deploy closed.
CI cannot see it, so reviewers and review-agents must treat a secrets-schema
change with no matching `.age` update as a blocker.

### Naming Conventions

Code names must be consistent with **[docs/domain.md](docs/domain.md)**, which
is the source of truth for terminology and naming conventions.

### Code Quality & Best Practices

- **CRITICAL: Package by Feature, Not by Layer**: Organize by business domain,
  not technical layers. **FORBIDDEN** catch-all modules: `types.rs`, `error.rs`,
  `models.rs`, `utils.rs`, `helpers.rs`, `http.rs`, `dto.rs`, `entities.rs`,
  `services.rs`, `domain.rs`. **CORRECT**: `position.rs`, `offchain_order.rs`,
  `onchain_trade.rs`. Each feature module contains ALL related code. Shared
  types import from the owning feature. **Flat by default**: single file per
  feature, split into directory only when business logic boundaries emerge
- **Event-Driven Architecture**: Each trade spawns independent async task for
  maximum throughput
- **SQLite Persistence**: Embedded database for trade tracking, CQRS state, and
  runtime data
- **Symbol Prefix Convention**: Tokenized equities use "t" prefix to distinguish
  from base assets (e.g., tAAPL, tTSLA, tSPYM)
- **Price Direction Logic**: Onchain buy = offchain sell (and vice versa) to
  hedge directional exposure
- **Comprehensive Error Handling**: Custom error types (`OnChainError`,
  `AlpacaBrokerApiError`) with proper propagation
- **CRITICAL: Onchain Transaction Confirmations**: All onchain operations must
  wait for the configured confirmations -- load-balanced RPC providers (e.g.
  dRPC) may route later requests to nodes that haven't seen the tx. Call
  `.with_required_confirmations(self.required_confirmations).get_receipt()`
  (count from `REQUIRED_CONFIRMATIONS` in `crate::onchain`) on all pending txs;
  never bare `.get_receipt().await` in production.
- **CRITICAL: CQRS/Event Sourcing Architecture**: **NEVER write directly to the
  `events` table** — no direct INSERTs, no manual sequence numbers, no bypassing
  `CqrsFramework`. Always use `CqrsFramework::execute()` or
  `execute_with_metadata()` to emit events through aggregate commands. The
  framework handles persistence, sequence numbers, and consistency
- **CRITICAL: Single CQRS Framework Instance Per Aggregate**: Each aggregate
  must have exactly ONE `SqliteCqrs<A>` in the server binary, constructed in
  `Conductor::start`. Never call `sqlite_cqrs()` or `CqrsFramework::new()`
  elsewhere in the server path. Direct construction is fine in
  test/CLI/migration code
- **CQRS Aggregate Services Pattern**: Use cqrs-es Services for side-effects in
  `handle()` to ensure atomicity with events. **Naming:** `{Action}er` trait ->
  `{Domain}Service` implements -> `{Domain}Manager` orchestrates. See
  `OffchainOrder`/`OrderPlacer`
- **Log in command handlers, not callers**: All logging for command execution
  belongs in the aggregate's `handle()` method, not at the call site. The
  handler has full aggregate state (symbol, net position, thresholds, etc.)
  making log messages rich without the caller needing to load or pass extra
  context. This keeps logging consistent and centralized — one place per
  command, not scattered across every caller
- **Type Modeling**: Make invalid states unrepresentable through the type
  system. Use ADTs and enums to encode business rules and state transitions
  directly in types rather than runtime validation. See "Type modeling" in Code
  Style for details
- **SDK Boundary Conversion**: Accept domain newtypes and convert to SDK
  primitives inside the callee. Exception: cross-crate boundaries where the
  callee can't depend on caller's domain types -- destructure at the call site
- **Schema Design**: No contradictory columns. Use constraints and
  normalization. Align schemas with type modeling principles
- **No Denormalized Columns**: Never store values computable from other columns.
  Compute on-demand; if caching needed, use views or generated columns
- **Functional Programming**: Favor FP/ADT patterns over OOP. Use pattern
  matching, combinators, type-driven design. Prefer iterators over imperative
  loops unless it increases complexity. Use itertools for richer iterator chains
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
    **Exceptions** (function-body `use` only when one of these applies):
    1. Enum variant imports (`use MyEnum::*` or `use MyEnum::{A, B, C}`) to
       avoid repetitive qualification. Enum variant imports are never allowed at
       module level.
    2. Imports used only inside `#[cfg(...)]`-gated code, where pulling them to
       module scope would produce unused-import warnings under the inverse
       feature configuration.
    3. Imports inside macro definitions / generated code, where hygiene requires
       the import live in the expansion site, not the caller's module.
  - Module declarations (`mod foo;`) can appear between imports if needed
  - This pattern applies to ALL modules including test modules
    (`#[cfg(test)] mod tests`)
- **Import Conventions**: Qualify imports only to prevent ambiguity (e.g.
  `contract::Error`), not when the module is clear (e.g. `info!` not
  `tracing::info!`). Top-of-module imports only (not top-of-file -- test module
  imports go in the test module, not behind `#[cfg(test)]` at file top)
- **Error Handling**: No `unwrap()`/`.expect()` in production code (validation
  logic may change, leaving panics). **Exception**: fine in test code
  (`#[cfg(test)]` modules)
- **CRITICAL: Error Type Design**: **NEVER create error variants with opaque
  String values.** No `SomeError(String)`, no `.to_string()` or `format!()`
  conversions, no unpacking newtypes (store `Symbol` not `String`). Prefer
  `#[from]` + `?` for error conversion; preserve error chains with `#[source]`;
  discover variants during implementation not preemptively. `.map_err` is
  permitted when adding call-site context or adapting a source error type that
  cannot implement `From`/`#[from]` - do not reach for it as the default when
  `#[from]` + `?` suffices. To log before converting:
  `.inspect_err(|error| error!(?error, "ctx"))` before `?`
- **Silent Early Returns**: Always log a warning/error before early returns in
  `let-else` or similar patterns. Silent failures hide bugs
- **No Duplicate Values in Debug Output**: Log actual runtime values, never
  hardcoded copies (they drift from the real implementation)
- **Visibility Levels**: Keep visibility as restrictive as possible (private >
  `pub(crate)` > `pub`) for better dead code detection and clearer scope
- **Type Aliases**: Only add when clippy complains about type complexity. If
  clippy doesn't flag it, the full type is clearer. Use newtypes (not aliases)
  to distinguish types with the same representation.

### CRITICAL: Financial Data Integrity

**NEVER** silently mask failures on financial values: no defensive capping,
fallback defaults, precision truncation, `unwrap_or()`, `unwrap_or_default()`.
ALL financial operations must use explicit error handling.

**Must fail fast**: numeric conversions (`try_into()`), precision loss, range
violations (error, not clamp), parse failures, arithmetic (checked), database
constraints. Silent data corruption leads to massive losses.

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
- **HTTP API Mocking**: `httpmock` crate for broker and tokenization API tests
- **Database Isolation**: In-memory SQLite databases for test isolation
- **Edge Case Coverage**: Comprehensive error scenario testing for trade
  conversion logic
- **Testing Principle**: Follow the testing pyramid — most coverage in unit
  tests, fewer integration tests, fewest e2e tests. Integration tests may cover
  failure scenarios when those failures can only be triggered by wiring multiple
  components together
- **CRITICAL: Tests must assert CORRECT behavior, never "document gaps"**: If
  code is broken, tests MUST assert correct behavior and FAIL until fixed. NEVER
  assert incorrect behavior with "will fix later" comments. A failing test is
  better than a passing test that asserts wrong behavior.
- **CRITICAL: NEVER delete, skip, or bypass existing tests to ease a refactor.**
  Either: (1) adapt tests to the new design preserving coverage, (2) find a
  design that keeps tests passing, or (3) stop and ask. Tests are correctness
  constraints -- if your change can't satisfy them, the change is wrong.
- **Debugging failing tests**: Add context to the assert! macro, not temporary
  println! statements
- **No ad-hoc debugging scripts**: Debug via test functions, not scripts or temp
  files
- **Test Quality**: Tests must verify business logic, not language features or
  struct field assignments
- **Property-Based Testing**: Use `proptest` for property-based tests whenever
  there are clear invariants to verify. Property tests are excellent for:
  - Parsing/serialization roundtrips
  - Boundary conditions (e.g., message length validation)
  - Invariants that should hold for all inputs (e.g., extracted data matches
    input regardless of surrounding bytes)
  - Numeric operations where edge cases are hard to enumerate manually

### Workflow Best Practices

- **Incremental verification during development** -- scope checks to the package
  you're actively working on for fast feedback:
  1. `cargo check -p <crate>` after every edit -- fast, catches type errors
  2. `cargo nextest run -p <crate>` after completing a logical unit -- runs that
     crate's tests only, skips slow e2e and unrelated crates
  3. `cargo clippy -p <crate>` only after all substantive edits to that crate
     are done
  4. Reserve `--workspace` variants for the final verification pass
- **Final verification before handing over** (skip for doc-only changes):
  `nix run .#ci` enters the right dev shells (`ci-backend`, `ci-dashboard`) and
  runs the full matrix end-to-end -- backend `cargo check` (with and without
  `--all-features`), `cargo nextest run`, `cargo clippy`, `cargo fmt --check`,
  plus dashboard `bun.nix` freshness check, DTO regeneration, lint,
  `svelte-check`. Mirrors CI.

  For iteration (e.g. backend-only), run individual steps in the corresponding
  shell:
  1. `cargo check --workspace`
  2. `cargo nextest run --workspace --all-features` -- spawns anvil for CCTP
     integration tests; only the `ci-backend` shell exposes the foundry binary.
     Run via `nix develop .#ci-backend -c cargo nextest run ...`.
  3. `cargo clippy --workspace --all-targets --all-features` - full linting
  4. `cargo fmt` - always run last to ensure clean formatting
  5. **Diff review** - after all checks pass, review staged changes and revert
     any chunks without clear justification (see "Before handing over" section)
- **CRITICAL: ALL workspace checks must pass before work is done** (except
  doc-only changes). Fix every warning/error/failure regardless of origin. CI
  blocks merging on any failure.
- **CRITICAL: Do NOT run clippy until ALL substantive work is done.** It's a
  polish step -- subsequent changes introduce new lints. Run clippy only as the
  final pass.
- **CRITICAL: Do NOT run `cargo nextest run --workspace` repeatedly.** Use
  `-p <crate>` during iteration; full workspace suite only in final
  verification.

#### CRITICAL: Quality Control Policy

**NEVER bypass, disable, or suppress ANY quality control mechanism without
explicit permission being granted.** This applies to ALL checks including but
not limited to:

- Clippy lints (`#[allow(clippy::*)]`)
- Compiler warnings (`#[allow(deprecated)]`, `#[allow(dead_code)]`, etc.)
- Deadnix, rustfmt, or any other linting/formatting tools
- Test assertions or validation logic
- Any other strictness or quality enforcement

Clippy lints often indicate poor design worth reconsidering. Upon a lint
violation:

1. **Re-evaluate the design** in the context of what was flagged. If the lint
   reveals a flaw in the broader design or architecture, fix that
2. **Refactor the code** to address the root cause of the lint violation
3. **Break down large functions** into smaller, more focused functions
4. **Improve code structure** to meet clippy's standards
5. **Use proper error handling** instead of suppressing warnings
6. If the violation is intentional and makes perfect sense in context, **stop
   and request explicit permission** from the user before suppressing

**FORBIDDEN: Obscure workarounds that silence the linter without fixing the
problem.** Either fix the underlying design issue or request permission to
suppress. No third option.

**Exception**: Lint suppression inside `sol!` macros is acceptable for issues
from contract ABI signatures we cannot control.

### Commenting Guidelines

Code should be self-documenting. Comments only when they add context that code
structure cannot express.

**DO comment**: complex business logic, algorithm rationale, external system
behavior, non-obvious constraints, test data context, workarounds.

**DON'T comment**: self-explanatory code, restating what code does, function
signature descriptions (use `///`), obvious test setup, section markers. **NEVER
reference task numbers or issue trackers** in comments.

Use `///` for public APIs. Keep comments focused on "why" not "what".

### Code style

#### ASCII in code, unicode in user-facing output

Use ASCII characters only in identifiers, comments, log messages, and config
keys. For arrows in comments, use `->` not `→`. Unicode breaks vim navigation
and grep workflows.

In user-facing string literals (GUI templates, CLI display, rendered text),
prefer unicode characters (`←`, `→`, `·`, `▲`, `▼`, etc.) for readability and
polish.

#### No single-letter variables or arguments

Single-letter names are **FORBIDDEN** everywhere -- variables, arguments,
closure params, generic type params. Always use descriptive names. Exception:
short closures where the type is unambiguous (e.g., `|event| event.payload`).

**Generic type parameters**: Single-letter type vars forbidden when multiple
type vars exist. Use descriptive names (`Call`, `Registry`, `Wallet`). A lone
type var (e.g., `ReadOnlyEvm<P>`) is acceptable when unambiguous.

#### Module Organization

Order by importance: public API first, private implementation, then tests. Every
module should have a `//!` docstring. What a module _does_ (consumer-facing
types/functions) goes before what _supports_ it (error types, helpers).

#### Line width in docstrings and macros

All doc comments (`//!` and `///`) and long strings inside attribute macros
(e.g., `#[error(...)]`) must not exceed 100 characters per line. `cargo fmt`
does not enforce this (without nightly rustfmt), so be careful and check
manually.

For multi-line `#[error]` strings, use `\` continuation.

#### Never use `is_err()`/`is_ok()` assertions in tests

**FORBIDDEN**: `assert!(x.is_err())`, `assert!(x.is_ok())`. For errors, unwrap
and assert the exact variant with `matches!`. For ok, just `.unwrap()`.

#### Prefer exhaustive `match` over `matches!` in production code

Exhaustive `match` forces handling new variants; `matches!` hides them behind
`_ => false`. **Test code**: `matches!` is fine for assertions. **Production
code**: always exhaustive `match` so the compiler flags new variants.

#### Assertions must be specific

Use `assert_eq!` with exact values, not `assert!(result.is_some())`. Never use
`||` in assertions unless outcomes are genuinely equivalent.

#### Serialization test assertions must use literals

When testing serialized output (JSON, etc.), assert against `json!()` literals,
never against re-serialized domain types. Comparing
`serde_json::to_value(field)` against the parent's serialized output tests
serde's Serialize derive against itself — if the derive is wrong, both sides are
wrong and the test still passes. Use `assert_eq!(parsed["field"], json!("10"))`
so the expected value is independent of the code under test.

#### Type modeling

Use enums (not optional fields) for mutually exclusive states, newtypes for
domain concepts (`Symbol`, `Shares`), and typestate for protocol enforcement.
Make invalid states unrepresentable.

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

Use struct literal syntax and direct field access. Don't create `fn new()`
constructors or getters unless they add logic beyond setting/getting values.

#### Prefer destructuring over `.0` access

For newtypes, prefer `let TypeName(inner) = value` over `value.0` -- names the
type explicitly.

#### No one-liner helpers

If a helper's body is a single expression, inline it. Wrapping one function call
in another adds indirection without reducing complexity. Helpers must
encapsulate multi-step logic.

#### Don't split simple-but-long pattern matches

A single `match` with many trivial arms (state transitions, event mapping)
should stay as one function even if it exceeds line count lints. Request
permission to suppress `too_many_lines` rather than extracting pointless
helpers.
