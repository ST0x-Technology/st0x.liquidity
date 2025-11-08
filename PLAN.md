# Implementation Plan: Data Migration Binary (#129)

## Overview

Create a one-time migration binary (`migrate_to_events`) that converts existing
CRUD data from legacy tables into event-sourced aggregates using `Migrated`
events. This is a critical step in the phased CQRS/ES migration that must
preserve all existing data with exact fidelity.

## Context

### Existing Aggregates with Migrated Events

1. **OnChainTrade** (`OnChainTradeEvent::Migrated`)
2. **Position** (`PositionEvent::Migrated`)
3. **OffchainOrder** (`OffchainOrderEvent::Migrated`)
4. **SchwabAuth** (inferred from `schwab_auth_view` table)

### Legacy CRUD Tables

- `onchain_trades` - Immutable blockchain trade records
- `schwab_executions` - Broker order execution tracking
- `trade_accumulators` - Per-symbol position aggregation
- `schwab_auth` - OAuth token storage (singleton, id=1)

### Design Constraints

- Migration must be **idempotent** (safe to run multiple times)
- Must handle **empty databases** gracefully
- View projections must **exactly match** old table data after migration
- Events must be emitted **in chronological order** to preserve causality
- Migration should produce **detailed logs** for auditability

## Task 1. Foundation: Binary skeleton and infrastructure ✅

**Goal**: Build complete foundation that all migration tasks will use. This task
establishes the patterns and infrastructure, making subsequent tasks
straightforward.

**Status**: COMPLETED

**Subtasks**:

- [x] Create `src/bin/migrate_to_events.rs` with main function
- [x] Add CLI argument parsing with clap (using subcommands instead of flags for modes):
  - `--database-url`: SQLite database path (from env or arg)
  - `dry-run` subcommand: Preview migration without committing
  - `verify-only` subcommand: Skip migration, only verify data integrity
  - `--force`: Skip interactive confirmation prompts
  - `--clean`: Drop all events before migrating (requires double confirmation)
- [x] Set up database connection pool and run migrations
- [x] Add logging initialization with tracing (info level default)
- [x] Create `MigrationSummary` struct to track progress across all aggregates
- [x] Implement idempotency check:
  - Query `events` table for existing events by `aggregate_type`
  - If found, prompt: "Events detected for {type}. Continue? [y/N]"
  - Respect `--force` flag to skip all prompts
- [x] Implement safety prompt:
  - Before migration: "⚠️ Create database backup before proceeding! Continue?
    [y/N]"
  - Respect `--force` flag
- [x] Create verification framework structure (deferred to later tasks when needed)
- [x] Set up test harness (deferred - not using tests/ directory)
- [x] Wire up main migration flow with empty implementations
- [x] Add basic smoke test: Binary runs with `--help`, shows usage
- [x] Run clippy and fmt

**Deliverable**: Can run `cargo run --bin migrate_to_events -- dry-run` →
shows "Migration complete: 0 trades, 0 positions, 0 orders migrated" (because no
migrations configured yet). Binary compiles and passes all clippy checks.

**Design rationale**: Establishes complete infrastructure first so each
migration task is just "implement, verify, test" without boilerplate.
Idempotency and safety checks built-in from the start.

**Implementation Notes**:
- Used clap subcommands (`dry-run`, `verify-only`) instead of multiple boolean flags to satisfy clippy's struct_excessive_bools lint
- Deferred verification framework implementation to later tasks when actually needed (YAGNI principle)
- Deferred test harness setup - will add tests as modules when needed rather than creating `tests/` directory
- Fixed BrokerOrderId and PriceCents visibility in position module to allow usage from offchain_order module
- All clippy lints pass without any #[allow] attributes

## Task 2. OnChainTrade migration (complete vertical slice)

**Goal**: Deliver fully working, tested, and verified OnChainTrade migration.

**Subtasks**:

**2.1 Migration implementation**:

- [ ] Create
      `migrate_onchain_trades(pool: &SqlitePool, opts: &MigrationOpts) ->
      Result<usize>`
      function
- [ ] Query all rows from `onchain_trades` ordered by `created_at` ASC
- [ ] For each row, create `OnChainTradeEvent::Migrated`:
  - Aggregate ID: `format!("{}:{}", tx_hash, log_index)`
  - Convert SQL types with explicit error handling:
    - TEXT → `Symbol::new(...)` (handle invalid symbols)
    - REAL → `Decimal::from_f64(...)` (handle precision loss)
    - Direction:
      `match direction.as_str() { "BUY" => Buy, "SELL" => Sell, _ =>
      error }`
  - Handle optional fields (NULL in database):
    - `block_number`, `block_timestamp` (may not exist in very old data)
    - `gas_used` (added later, NULL in old records)
    - `pyth_price` (from separate table, may be missing)
  - Use `block_timestamp` if available, else `created_at`, else `Utc::now()`
- [ ] Persist events using `cqrs-es` + `sqlite-es`:
  - Respect `--dry-run` flag (skip persistence if set)
  - Use event store's `CqrsFramework::execute_with_metadata(...)`
- [ ] Log progress every 100 trades: `"Migrated 100/500 OnChainTrades..."`
- [ ] Add detailed error context:
      `context!("Failed to migrate trade at
      tx_hash={tx_hash}, log_index={log_index}")`

**2.2 Verification implementation**:

- [ ] Implement `OnChainTradeVerifier: MigrationVerifier`
- [ ] `verify_counts()`:
  - Count rows in `onchain_trades`: `SELECT COUNT(*) FROM onchain_trades`
  - Count events in event store:
    `SELECT COUNT(*) FROM events WHERE
    aggregate_type = 'OnChainTrade'`
  - Return `CountMatch { source: n1, migrated: n2, match: n1 == n2 }`
- [ ] `verify_sample()`:
  - Select 10 random trades from `onchain_trades`
  - For each, load aggregate from event store via aggregate ID
  - Compare: symbol, amount, direction, price_usdc, block_number,
    block_timestamp
  - Return list of `Mismatch` for any differences
- [ ] Wire into main migration flow: Call verification after migration completes

**2.3 Testing**:

- [ ] Integration test: `test_migrate_onchain_trades_empty_table()`
  - Empty database → migration completes with 0 migrated
- [ ] Integration test: `test_migrate_onchain_trades_full()`
  - Insert 10 test trades via `populate_legacy_onchain_trades(pool, 10)`
  - Run migration
  - Assert: 10 events persisted, verification passes
  - Query `onchain_trade_view`, verify data matches source
- [ ] Integration test: `test_migrate_onchain_trades_idempotency()`
  - Populate trades, migrate
  - Run migration again without `--force`
  - Assert: Prompt shown, no duplication if user says "no"
- [ ] Unit test: `test_parse_onchain_trade_row()`
  - Test conversion of sample row to `OnChainTradeEvent::Migrated`
  - Test error handling for invalid symbol, negative amount, etc.
- [ ] Integration test: `test_dry_run_no_persistence()`
  - Populate trades, run with `--dry-run`
  - Assert: No events in event store, summary shows correct count
- [ ] Run `cargo test`, ensure all tests pass
- [ ] Run `cargo clippy --all-targets -- -D warnings`
- [ ] Run `cargo fmt`

**Data mapping**:

```rust
// SQL row
struct OnChainTradeRow {
    tx_hash: String,         // e.g. "0xabc..."
    log_index: i64,
    symbol: String,          // e.g. "AAPL"
    amount: f64,             // e.g. 10.5
    direction: String,       // "BUY" or "SELL"
    price_usdc: f64,         // e.g. 150.25
    block_number: Option<i64>,
    block_timestamp: Option<DateTime<Utc>>,
    gas_used: Option<i64>,
    // pyth_price: TODO - determine if stored as JSON TEXT or in separate table
    created_at: DateTime<Utc>,
}

// Maps to
OnChainTradeEvent::Migrated {
    symbol: Symbol::new(&row.symbol)?,
    amount: Decimal::from_f64(row.amount).ok_or(...)?,
    direction: match row.direction.as_str() {
        "BUY" => Direction::Buy,
        "SELL" => Direction::Sell,
        _ => return Err(MigrationError::InvalidDirection(row.direction)),
    },
    price_usdc: Decimal::from_f64(row.price_usdc).ok_or(...)?,
    block_number: row.block_number.unwrap_or(0), // Or return error?
    block_timestamp: row.block_timestamp.unwrap_or(row.created_at),
    gas_used: row.gas_used.map(|g| g as u64),
    pyth_price: None, // TODO: Fetch from pyth_prices table if exists
    migrated_at: Utc::now(),
}
```

**Deliverable**: Can run `cargo run --bin migrate_to_events` on database with
onchain_trades → migrates successfully, verification passes, all tests green.
Binary output shows: "✓ OnChainTrade: 10 migrated, verification passed".

**Design rationale**: Complete vertical slice ensures OnChainTrade migration is
fully working before moving to next aggregate. Pattern established here will be
replicated for other aggregates.

## Task 3. Position migration (complete vertical slice)

**Goal**: Deliver fully working, tested, and verified Position migration.
Following same pattern as Task 2.

**Subtasks**:

**3.1 Migration implementation**:

- [ ] Create
      `migrate_positions(pool: &SqlitePool, opts: &MigrationOpts) ->
      Result<usize>`
      function
- [ ] Query all rows from `trade_accumulators`
- [ ] For each row, create `PositionEvent::Migrated`:
  - Aggregate ID: `row.symbol` (symbol is the natural ID for positions)
  - Convert `net_position`, `accumulated_long`, `accumulated_short` from REAL to
    `Decimal` to `FractionalShares`
  - Set default threshold: `ExecutionThreshold::Shares(Decimal::ONE)`
  - Use `last_updated` timestamp
- [ ] Handle positions with pending executions:
  - If `pending_execution_id IS NOT NULL`, log warning:
    `"Position {symbol} has pending execution {id} - will be reconciled in
    dual-write phase"`
  - Don't block migration, just log for awareness
- [ ] Persist events respecting `--dry-run` flag
- [ ] Log summary: "Migrated N positions, M with pending executions"

**3.2 Verification implementation**:

- [ ] Implement `PositionVerifier: MigrationVerifier`
- [ ] `verify_counts()`: Compare `trade_accumulators` count vs Position events
- [ ] `verify_sample()`:
  - Select random positions from `trade_accumulators`
  - Load aggregates from event store
  - Query `position_view` table (the view projection)
  - Compare: net_position, accumulated_long, accumulated_short, threshold
  - Return mismatches
- [ ] Wire into main flow

**3.3 Testing**:

- [ ] Integration test: Empty table
- [ ] Integration test: Full migration with 5 positions
  - Verify view projections match source data exactly
- [ ] Integration test: Position with pending execution
  - Verify warning logged, migration succeeds
- [ ] Unit test: Row conversion
- [ ] Integration test: Idempotency check
- [ ] Run tests, clippy, fmt

**Deliverable**: Can run migration on database with positions → migrates
OnChainTrades + Positions, verifies both. Output: "✓ OnChainTrade: X migrated, ✓
Position: Y migrated, verification passed".

**Design rationale**: Per-symbol singletons. Default threshold safe for later
adjustment via commands.

## Task 4. OffchainOrder migration (complete vertical slice)

**Goal**: Deliver fully working, tested, and verified OffchainOrder migration.

**Subtasks**:

**4.1 Migration implementation**:

- [ ] Create
      `migrate_offchain_orders(pool: &SqlitePool, opts: &MigrationOpts)
      -> Result<usize>`
      function
- [ ] Query all rows from `schwab_executions` ordered by `id` ASC
- [ ] For each row, create `OffchainOrderEvent::Migrated`:
  - Aggregate ID: `format!("{}", row.id)`
  - Map status with validation:
    - "PENDING" → `MigratedOrderStatus::Pending`
    - "SUBMITTED" → `MigratedOrderStatus::Submitted`
    - "FILLED" → `MigratedOrderStatus::Filled`
    - "FAILED" →
      `MigratedOrderStatus::Failed { error: "Migrated from legacy
      system".to_string() }`
  - Handle SUBMITTED with missing `order_id`:
    - Database constraint should prevent this, but add defensive check
    - If NULL, log warning and use placeholder: `BrokerOrderId("UNKNOWN")`
  - Convert `shares` (INTEGER) to `FractionalShares(Decimal::from(i64))`
  - Convert `price_cents` (INTEGER) to `PriceCents(u64)`
  - Broker is always `SupportedBroker::Schwab` for this table
- [ ] Track status breakdown in memory, log at end:
      `"Status breakdown: 5 PENDING, 120 FILLED, 3 FAILED"`
- [ ] Persist events respecting `--dry-run` flag

**4.2 Verification implementation**:

- [ ] Implement `OffchainOrderVerifier: MigrationVerifier`
- [ ] `verify_counts()`: Compare `schwab_executions` count vs OffchainOrder
      events
- [ ] `verify_sample()`:
  - Select random executions from `schwab_executions`
  - Load aggregates from event store
  - Query `offchain_order_view` table
  - Compare: symbol, shares, direction, status, broker_order_id, price_cents
- [ ] Wire into main flow

**4.3 Testing**:

- [ ] Integration test: Empty table
- [ ] Integration test: Full migration with all status types
  - 2 PENDING, 2 SUBMITTED, 2 FILLED, 1 FAILED
  - Verify status breakdown logged correctly
- [ ] Integration test: SUBMITTED with NULL order_id (edge case)
  - Verify warning logged, placeholder used
- [ ] Unit test: Status mapping
- [ ] Run tests, clippy, fmt

**Deliverable**: Can run migration → migrates 3 aggregates successfully. Output:
"✓ OnChainTrade: X, ✓ Position: Y, ✓ OffchainOrder: Z (status breakdown), all
verified".

**Design rationale**: Integer IDs natural for orders. Status mapping preserves
lifecycle state for dual-write reconciliation.

## Task 5. SchwabAuth migration (complete vertical slice)

**Goal**: Migrate schwab_auth singleton to SchwabAuth aggregate. May require
creating the aggregate if it doesn't exist.

**Subtasks**:

**5.1 Research and setup**:

- [ ] Check if SchwabAuth aggregate exists:
  - Search codebase for `SchwabAuthEvent`, `SchwabAuth` aggregate
  - Check existing aggregate modules in src/
- [ ] If aggregate doesn't exist:
  - Create `src/schwab_auth/mod.rs` following OnChainTrade pattern
  - Define minimal `SchwabAuth` aggregate with state: `Unauthenticated`,
    `Authenticated { tokens }`
  - Define `SchwabAuthEvent::Migrated` with token fields
  - Implement `Aggregate` trait with `apply()` for Migrated event
  - Create `schwab_auth/view.rs` for read model projection (likely already
    exists: `schwab_auth_view` table)
  - Add basic tests
  - Get aggregate working standalone before migration

**5.2 Migration implementation**:

- [ ] Create
      `migrate_schwab_auth(pool: &SqlitePool, opts: &MigrationOpts) ->
      Result<bool>`
      function (returns bool: migrated or not)
- [ ] Query: `SELECT * FROM schwab_auth WHERE id = 1`
- [ ] Handle empty table gracefully:
  - If no row, log: "No SchwabAuth to migrate (table empty)"
  - Return `Ok(false)` - this is not an error, just nothing to migrate
- [ ] If row exists, create `SchwabAuthEvent::Migrated`:
  - Aggregate ID: `"schwab"` (fixed singleton ID)
  - Map all token fields from row
  - Handle potential NULL fields (though schema shows NOT NULL)
- [ ] Persist event respecting `--dry-run` flag
- [ ] Log: "✓ SchwabAuth migrated" or "ℹ No SchwabAuth to migrate"

**5.3 Verification implementation**:

- [ ] Implement `SchwabAuthVerifier: MigrationVerifier`
- [ ] `verify_counts()`:
  - Count rows in `schwab_auth` (should be 0 or 1)
  - Count events for aggregate_type='SchwabAuth', aggregate_id='schwab'
  - Match: both 0, or both 1
- [ ] `verify_sample()`:
  - If auth exists, load aggregate from event store
  - Query `schwab_auth_view` table
  - Compare: access_token, refresh_token, timestamps
- [ ] Wire into main flow

**5.4 Testing**:

- [ ] Integration test: Empty table (no auth configured)
  - Migration completes without error, logs "nothing to migrate"
- [ ] Integration test: Singleton row exists
  - Migrate, verify event created, view projection matches
- [ ] Integration test: Idempotency with singleton
- [ ] If new aggregate created: Add aggregate-level unit tests
- [ ] Run tests, clippy, fmt

**Deliverable**: Full migration of all 4 aggregates working end-to-end. Binary
output: "✓ OnChainTrade: X, ✓ Position: Y, ✓ OffchainOrder: Z, ✓ SchwabAuth:
migrated, all verified". If SchwabAuth aggregate created, it's tested and
working standalone.

**Design rationale**: Singleton pattern for global auth state. Creating
aggregate if needed keeps migration self-contained and unblocks this PR from
dependencies.

## Task 6. Edge cases, error handling, and polish

**Goal**: Harden migration with robust error handling and user experience
improvements. By now all 4 aggregates migrate successfully - this task makes it
production-ready.

**Subtasks**:

- [ ] Improve error handling for malformed data:
  - Invalid symbols (empty string, invalid characters)
  - Negative amounts or prices
  - Unknown status strings
  - NULL values in NOT NULL columns (data corruption)
  - Each error should: log detailed context, skip row, continue migration
  - Track error count, include in summary: "Migrated N, skipped M (errors)"
- [ ] Add retry logic with backoff for transient failures:
  - Database connection failures
  - Event store write failures
  - Use `backon` crate with exponential backoff
- [ ] Improve progress indicators:
  - Show progress bar for large migrations (optional: use `indicatif` crate)
  - Update every 100 items: "OnChainTrade: 300/1000 (30%)"
  - Show ETA for long-running migrations
- [ ] Test `--clean` flag thoroughly:
  - Double confirmation: "⚠️ This will DELETE all events! Type 'DELETE' to
    confirm:"
  - Delete from `events` and `snapshots` tables
  - Log: "Deleted N events from event store"
- [ ] Add `--verify-only` mode:
  - Skip migration, only run verification
  - Useful for checking migration without re-running
- [ ] Integration test: Large dataset (1000+ trades)
  - Verify performance acceptable
  - Verify progress logging works
- [ ] Integration test: Malformed data
  - Insert rows with invalid symbol, negative amount, NULL required field
  - Verify logged and skipped, migration continues
- [ ] Run full test suite, clippy, fmt

**Deliverable**: Production-ready migration binary that handles edge cases
gracefully. Can run on messy real-world data without crashing.

**Design rationale**: Real databases have edge cases. Robust error handling
prevents migration failures and provides clear diagnostics.

## Task 7. Documentation and usage guidance

**Goal**: Document migration process for safe production use.

**Subtasks**:

- [ ] Improve `--help` output:
  - Clear description of each flag
  - Usage examples for common scenarios
  - Warning about backup requirement
- [ ] Create migration runbook (add to README or docs/MIGRATION.md):
  - **Prerequisites**: Database backup, downtime window
  - **When to run**: Before enabling dual-write (#130)
  - **Recommended workflow**:
    1. Backup database: `cp st0x.db st0x.db.backup`
    2. Dry run: `./migrate_to_events --dry-run`
    3. Review output, ensure no errors
    4. Run for real: `./migrate_to_events`
    5. Verify: `./migrate_to_events --verify-only`
    6. If verification fails, restore backup and investigate
  - **Example commands**:
    - Dry run: `cargo run --bin migrate_to_events -- --dry-run`
    - Force mode (no prompts): `cargo run --bin migrate_to_events -- --force`
    - Clean and migrate: `cargo run --bin migrate_to_events -- --clean --force`
  - **Troubleshooting**:
    - "Events detected" → Already migrated, safe to skip
    - "Verification failed" → Check logs, compare sample mismatches
    - Malformed data errors → Review skipped rows in logs
  - **Rollback**: `mv st0x.db.backup st0x.db`
- [ ] Add inline code comments for critical sections:
  - Aggregate ID generation (why format is important)
  - Status mapping logic
  - NULL handling rationale
- [ ] Document expected dry-run output in README
- [ ] Add TODOs for known limitations:
  - `TODO(migration): pyth_price not migrated, fetch from separate table?`
  - Any other deferred work

**Deliverable**: Clear, actionable documentation that a teammate can follow to
run migration safely.

**Design rationale**: Good docs prevent user error and build confidence in
critical migration operation.

## Task 8. Final validation and integration

**Goal**: Validate migration with realistic data and ensure conductor
integration works.

**Subtasks**:

- [ ] Add migration binary to Cargo.toml:
  ```toml
  [[bin]]
  name = "migrate_to_events"
  path = "src/bin/migrate_to_events.rs"
  ```
- [ ] Copy production database to local dev environment (anonymize sensitive
      data if needed)
- [ ] Run migration on production-like database:
  - Note: May reveal edge cases not in tests
  - Time the migration, document performance
  - Verify all aggregates migrated successfully
- [ ] Test conductor can read from event-sourced aggregates:
  - Comment out old CRUD queries in conductor
  - Wire up event store queries
  - Run conductor, verify it works with migrated data
  - **Note**: This is exploratory validation, not full conductor refactor
    (that's #130)
- [ ] Check nix flake, update if needed for binary
- [ ] Run migration as CLI: `nix run .#migrate_to_events -- --help`
- [ ] Benchmark migration performance:
  - Time with 1k, 10k, 100k trades (if possible)
  - Identify bottlenecks if too slow
- [ ] Document any manual post-migration steps discovered
- [ ] Final PR checklist:
  - [ ] All tests pass: `cargo test`
  - [ ] No clippy warnings: `cargo clippy --all-targets -- -D warnings`
  - [ ] Formatted: `cargo fmt --check`
  - [ ] Migration binary runs successfully
  - [ ] Documentation complete
  - [ ] PLAN.md deleted (per AGENTS.md)

**Deliverable**: Migration validated on realistic data. Confirmed that conductor
can read from event store. PR ready for review.

**Design rationale**: Real-world validation catches issues missed by unit tests.
Conductor validation ensures migration output is actually usable.

## Success Criteria

- ✅ Binary converts all existing data to events
- ✅ View projections match old table data exactly
- ✅ Verification logic confirms data integrity
- ✅ Handles empty database gracefully
- ✅ Idempotent - safe to run multiple times
- ✅ Comprehensive error handling and logging
- ✅ Integration tests cover all scenarios
- ✅ Documentation provides clear usage instructions

## Risk Mitigation

### Data Loss Risk

- **Mitigation**: Comprehensive verification logic that compares source and
  migrated data
- **Mitigation**: Require database backup before migration (enforced via prompt)
- **Mitigation**: Dry-run mode for preview

### Performance Risk

- **Mitigation**: Log progress for long migrations
- **Mitigation**: Batch event persistence if needed (benchmark first)

### Complexity Risk

- **Mitigation**: Break migration into independent per-aggregate functions
- **Mitigation**: Extensive testing with realistic data sizes

## Open Questions

1. **Does SchwabAuth aggregate exist?** If not, should we create it as part of
   this task or defer to separate PR?
   - **Decision**: Check existing code first. If doesn't exist, create minimal
     aggregate in this PR.

2. **How to handle trade_execution_links table?** It's an audit trail linking
   trades to executions.
   - **Decision**: Not needed for event-sourced system - events provide complete
     audit trail. Skip migration.

3. **Should we migrate pyth_prices table?**
   - **Decision**: No - that's external data, not domain events. Keep as-is.

4. **What if pending executions exist during migration?**
   - **Decision**: Migrate them as-is. Dual-write implementation (#130) will
     handle reconciliation.

## Dependencies

This task depends on:

- ✅ #125 - Event sourcing infrastructure (merged)
- ✅ #126 - OnChainTrade aggregate (merged)
- ✅ #127 - Position aggregate (merged)
- ✅ #128 - OffchainOrder aggregate (merged)

Next steps after this task:

- #130 - Implement dual-write in Conductor
- #131 - Add monitoring and validation tooling for dual-write period
