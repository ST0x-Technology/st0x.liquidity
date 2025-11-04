# CQRS/ES Migration Implementation Plan

This plan outlines the implementation of the CQRS/ES architecture described in
SPEC.md. The migration will transform the current CRUD-style database into an
event-sourced system with immutable events, snapshots, and materialized views.

## Reference Material

- **Reference implementation**: `../st0x.issuance-b/` - Follow module structure
  and patterns from this codebase
- **sqlite-es crate**: `../st0x.issuance-b/crates/sqlite-es/` - Provides
  SqliteCqrs, SqliteEventRepository, SqliteViewRepository
- **SPEC.md**: DDD/CQRS/ES Migration Proposal section contains the complete
  architecture specification

## Module Organization Principles

Following "package by feature, not by layer" as used in reference repo:

- Each aggregate gets its own feature module: `src/onchain_trade/`,
  `src/position/`, `src/offchain_order/`
- Within each feature module:
  - `mod.rs` - Aggregate enum definition and Aggregate trait impl
  - `cmd.rs` - Command enum
  - `event.rs` - Event enum
  - `view.rs` - View enum and View trait impl
  - `*_manager.rs` - Manager for cross-aggregate orchestration (when needed)
- No `src/cqrs/`, `src/views/`, `src/managers/` layers - these violate the
  principle

## Task 1. Dependencies and Event Store Schema

Add required dependencies and create the foundational event store tables.

**Reasoning**: Infrastructure must exist before implementing aggregates. The
event store and snapshot tables are the single source of truth.

- [x] Update `Cargo.toml` workspace dependencies
  - [x] Add `sqlite-es` from GitHub:
        `sqlite-es = { git = "https://github.com/ST0x-Technology/st0x.issuance", package = "sqlite-es" }`
  - [x] Add `cqrs-es = "0.4"`
  - [x] Add `async-trait` if not already present
- [x] Create migration using `sqlx migrate add event_store`
  - [x] Copy exact schema from
        `../st0x.issuance-b/migrations/20251016210348_init.sql`
  - [x] Tables: `events`, `snapshots`
  - [x] Events table has composite PK: (aggregate_type, aggregate_id, sequence)
  - [x] Indexes: idx_events_type, idx_events_aggregate
  - [x] All columns use exact types from reference (TEXT, BIGINT, JSON)
- [x] Run migration: `sqlx migrate run`
- [x] Verify tables created: `sqlite3 schwab.db .schema events`

## Task 2. OnChainTrade Feature Module

Implement the OnChainTrade aggregate - the simplest aggregate with linear
lifecycle.

**Reasoning**: OnChainTrade has simple state (Unfilled → Filled → Enriched) and
no complex business rules. It's a good template for understanding the aggregate
pattern before tackling Position. The aggregate enforces lifecycle rules (must
fill before enriching), while the view provides query-optimized projection (just
trade data with optional enrichment fields).

- [x] Create `src/onchain_trade/` directory
- [x] Create `src/onchain_trade/event.rs`
  - [x] Define `OnChainTradeEvent` enum with variants: Filled, Enriched,
        Migrated
  - [x] Fields as specified in "OnChainTrade Aggregate" section of SPEC.md
  - [x] Derive Serialize, Deserialize, Debug, Clone
  - [x] Migrated variant for migration
- [x] Create `src/onchain_trade/cmd.rs`
  - [x] Define `OnChainTradeCommand` enum: Witness, Enrich
  - [x] Fields from "OnChainTrade Aggregate" section of SPEC.md
- [x] Create `src/onchain_trade/mod.rs`
  - [x] Define `OnChainTrade` enum: Unfilled, Filled, Enriched
  - [x] State fields from SPEC.md
  - [x] Implement `Aggregate` trait following pattern in
        `../st0x.issuance-b/src/mint/mod.rs` (search for "impl Aggregate for
        Mint")
  - [x] Associated types: Command, Event, Error, Services
  - [x] `handle()` method dispatches commands
  - [x] Business rules: can only enrich once, cannot enrich before fill
- [x] Define `OnChainTradeError` enum in mod.rs for aggregate errors
- [x] Create `src/onchain_trade/view.rs`
  - [x] Define `OnChainTradeView` enum per "OnChainTradeView" section of SPEC.md
  - [x] States: Unavailable, Trade (with all fields including tx_hash,
        log_index)
  - [x] Note: View structure differs from aggregate - optimized for queries, not
        lifecycle
  - [x] Implement `View` trait from cqrs-es
  - [x] `update()` method handles OnChainTradeEvent variants
- [x] Create migration using `sqlx migrate add onchain_trade_view`
  - [x] Follow pattern from
        `../st0x.issuance-b/migrations/20251017184504_create_mint_view.sql`
  - [x] Table: `onchain_trade_view` with columns: view_id (PK), version, payload
        (JSON)
  - [x] Add STORED generated columns for frequently queried fields (tx_hash,
        symbol, block_number)
  - [x] Add json_extract indexes for less common fields
  - [x] Schema details in "OnChain trade view" section of SPEC.md
- [x] Write unit tests in `src/onchain_trade/mod.rs`
  - [x] Test Witness command creates Filled event
  - [x] Test Enrich command creates Enriched event
  - [x] Test cannot enrich twice
  - [x] Test Migrated event initialization

## Task 3. Position Feature Module

Implement the Position aggregate - the core business logic with threshold
management.

**Reasoning**: Position is the most complex aggregate. It tracks fractional
shares, decides when to execute based on configurable thresholds, and
coordinates with broker orders. This needs careful testing.

- [x] Create `src/position/` directory
- [x] Create `src/position/event.rs`
  - [x] Define newtypes: TradeId (struct with tx_hash, log_index), ExecutionId,
        BrokerOrderId, PriceCents, FractionalShares
  - [x] TradeId uses TxHash from alloy (not String)
  - [x] FractionalShares wraps Decimal with arithmetic traits (Add, Sub,
        AddAssign, SubAssign)
  - [x] Define `PositionEvent` enum with all variants from "Position Aggregate"
        section of SPEC.md
  - [x] Variants: Initialized, OnChainOrderFilled, OffChainOrderPlaced,
        OffChainOrderFilled, OffChainOrderFailed, ThresholdUpdated, Migrated
  - [x] Define `TriggerReason` enum
  - [x] Define `ExecutionThreshold` enum: Shares, DollarValue
  - [x] Migrated event includes threshold field for migration
- [x] Create `src/position/cmd.rs`
  - [x] Define `PositionCommand` enum from SPEC.md
  - [x] Commands: Initialize, AcknowledgeOnChainFill, PlaceOffChainOrder,
        CompleteOffChainOrder, FailOffChainOrder, UpdateThreshold
- [x] Create `src/position/mod.rs`
  - [x] Define `Position` struct with fields from "Position Aggregate" section
  - [x] Field naming: `net` (not `net_position`) to avoid clippy
        struct_field_names warning
  - [x] Implement `Aggregate` trait
  - [x] `handle()` method with business rules from SPEC.md:
    - [x] Threshold required before processing fills
    - [x] Check shares threshold (dollar threshold check returns None for now)
    - [x] No multiple pending executions
    - [x] Always apply onchain fills
    - [x] Emit OffChainOrderPlaced when threshold crossed
  - [x] Define `PositionError` enum
  - [x] Migrated event handling in apply() method with threshold default to
        Shares(Decimal::ONE)
- [x] Create `src/position/view.rs`
  - [x] Define `PositionView` enum (Unavailable, Position)
  - [x] Implement `View` trait
  - [x] Broke down large update() into smaller handler methods to satisfy clippy
        too_many_lines
  - [x] Added error logging to all handlers (no silent failures or panics)
  - [x] Fixed direction handling in OffChainOrderFilled (buy vs sell)
  - [x] Replaces current `trade_accumulators` table
- [x] Create migration using `sqlx migrate add position_view`
  - [x] Table: `position_view` with schema from "Position view" section of
        SPEC.md
  - [x] STORED generated columns for: symbol, net_position, last_updated
  - [x] Indexes on symbol, net_position, last_updated
- [x] Write comprehensive unit tests (47 total: 32 aggregate + 15 view)
  - [x] Aggregate tests in mod.rs:
    - [x] Test threshold initialization
    - [x] Test onchain fill accumulation
    - [x] Test shares threshold triggers execution
    - [x] Test pending execution prevents new execution
    - [x] Test threshold update audit trail
    - [x] Test Migrated event with non-optional threshold
    - [x] Test error conditions (ThresholdNotMet, PendingExecution, etc.)
  - [x] View tests in view.rs:
    - [x] Test all event handlers update state correctly
    - [x] Test onchain fills (buy and sell) update net and accumulators
    - [x] Test offchain placed/filled/failed lifecycle
    - [x] Test direction logic in OffChainOrderFilled (buy increases, sell
          decreases)
    - [x] Test threshold updated event
    - [x] Test Migrated event reconstruction
    - [x] Test error handling (events on Unavailable state log and are ignored)
    - [x] Test invalid symbol remains Unavailable with error logging

## Task 4. OffchainOrder Feature Module

Implement the OffchainOrder aggregate tracking broker order lifecycle.

**Reasoning**: OffchainOrder models the state machine for broker orders. It has
multiple valid state transitions that need to be encoded correctly.

- [x] Create `src/offchain_order/` directory
- [x] Create `src/offchain_order/event.rs`
  - [x] Define `OffchainOrderEvent` enum from "OffchainOrder Aggregate" section
        of SPEC.md
  - [x] Variants: Placed, Submitted, PartiallyFilled, Filled, Failed, Migrated
  - [x] Define `MigratedOrderStatus` enum for migration
- [x] Create `src/offchain_order/cmd.rs`
  - [x] Define `OffchainOrderCommand` enum from SPEC.md
  - [x] Commands: Place, ConfirmSubmission, UpdatePartialFill, CompleteFill,
        MarkFailed
- [x] Create `src/offchain_order/mod.rs`
  - [x] Define `OffchainOrder` enum from SPEC.md
  - [x] States: NotPlaced, Pending, Submitted, PartiallyFilled, Filled, Failed
  - [x] Implement `Aggregate` trait with helper methods to satisfy clippy
  - [x] Valid state transitions in `handle()` method
  - [x] Define `OffchainOrderError` enum
  - [x] Added error logging for inconsistent state transitions (no silent
        failures)
- [x] Create `src/offchain_order/view.rs`
  - [x] Define `OffchainOrderView` enum (renamed from OffchainTradeView for
        consistency)
  - [x] Define `ExecutionStatus` enum (Pending, Submitted, Filled, Failed)
  - [x] Implement `View` trait with error logging
  - [x] Replaces current `offchain_trades` table
- [x] Create migration using `sqlx migrate add offchain_order_view`
  - [x] Table: `offchain_order_view` with schema from "Offchain trade view"
        section of SPEC.md
  - [x] STORED generated columns for: execution_id, symbol, status, broker,
        broker_order_id
  - [x] Indexes on execution_id, symbol, status, broker, broker_order_id
- [x] Write comprehensive unit tests (35 total: 19 aggregate + 16 view)
  - [x] Aggregate tests (19): Test all valid state transitions (Place → Pending
        → Submitted → PartiallyFilled → Filled/Failed)
  - [x] Aggregate tests: Test invalid transitions return errors (NotPlaced,
        AlreadySubmitted, AlreadyCompleted)
  - [x] Aggregate tests: Test Migrated event with different statuses (Pending,
        Submitted, Filled, Failed with error)
  - [x] View tests (16): Test all event handlers update state correctly
  - [x] View tests: Test events on Unavailable state log errors and don't change
        state
  - [x] View tests: Test invalid aggregate_id remains Unavailable with error
        logging

## Task 5. SchwabAuth Aggregate

Update Schwab authentication to use CQRS pattern.

**Reasoning**: SchwabAuth is simple (NotAuthenticated → Authenticated) but needs
event sourcing for audit trail of token refreshes. This goes in existing
`src/schwab/` as it's Schwab-specific.

- [ ] Create `src/schwab/auth_event.rs`
  - [ ] Define `SchwabAuthEvent` enum from "SchwabAuth Aggregate" section of
        SPEC.md
  - [ ] Variants: TokensStored, AccessTokenRefreshed
  - [ ] Use existing `EncryptedToken` type from `src/schwab/tokens.rs`
- [ ] Create `src/schwab/auth_cmd.rs`
  - [ ] Define `SchwabAuthCommand` enum from SPEC.md
  - [ ] Commands: StoreTokens, RefreshAccessToken
- [ ] Update `src/schwab/mod.rs` or create `src/schwab/auth_aggregate.rs`
  - [ ] Define `SchwabAuth` enum (NotAuthenticated, Authenticated)
  - [ ] Implement `Aggregate` trait
  - [ ] Business rules: StoreTokens from NotAuthenticated, RefreshAccessToken
        only when Authenticated
- [ ] Create `src/schwab/auth_view.rs`
  - [ ] Define `SchwabAuthView` for token storage
  - [ ] Implement `View` trait
- [ ] Create migration using `sqlx migrate add schwab_auth_view`
  - [ ] Table: `schwab_auth_view` singleton with schema from "Schwab auth view"
        section
  - [ ] view_id = 'schwab' (always)
  - [ ] payload contains encrypted tokens
- [ ] Write tests for token storage and refresh

## Task 6. MetricsPnL View

Add PnL metrics view as described in SPEC.md.

**Reasoning**: PnL metrics provide pre-computed financial analysis. This view
calculates from both OnChainTrade and Position events to track profitability.

- [ ] Create `src/metrics_pnl/` directory
- [ ] Create `src/metrics_pnl/view.rs`
  - [ ] Define `MetricsPnLView` enum from "MetricsPnLView" section of SPEC.md
  - [ ] Define `Venue` enum (OnChain, OffChain with broker)
  - [ ] Implement `View` trait
  - [ ] Calculate PnL from OnChainTradeEvent::Filled and
        PositionEvent::OffChainOrderFilled
  - [ ] Track cumulative PnL per symbol
- [ ] Create migration using `sqlx migrate add metrics_pnl_view`
  - [ ] Table: `metrics_pnl_view` with schema from "PnL metrics view" section
  - [ ] STORED generated columns for: symbol, timestamp, trade_type
  - [ ] Indexes on symbol, timestamp, and composite
- [ ] Add query functions for PnL analysis

**Note**: MetricsPnLView registration with CQRS instances happens in Task 8.

## Task 7. Manager Implementations

Implement manager patterns for cross-aggregate orchestration.

**Reasoning**: Managers handle workflows that span multiple aggregates.
TradeManager is stateless (simple event→command mapping). OrderManager is
stateful (tracks in-flight orders for polling). Managers are implemented and
tested with mocks in this task; actual wiring to CQRS instances happens in
Task 10.

- [ ] Create `src/position/trade_manager.rs`
  - [ ] Stateless manager subscribing to OnChainTradeEvent::Filled
  - [ ] Extracts trade data and sends PositionCommand::AcknowledgeOnChainFill
  - [ ] Follows pattern from `../st0x.issuance-b/src/mint/callback_manager.rs`
  - [ ] Error handling and logging
- [ ] Create `src/offchain_order/order_manager.rs`
  - [ ] Stateful manager with in-flight order tracking
  - [ ] Subscribes to PositionEvent::OffChainOrderPlaced
  - [ ] Places broker orders via Broker trait
  - [ ] Sends OffchainOrderCommand::ConfirmSubmission
  - [ ] Spawns polling tasks for order status
  - [ ] Sends OffchainOrderCommand::CompleteFill or MarkFailed
  - [ ] Notifies Position aggregate via PositionCommand::CompleteOffChainOrder
        or FailOffChainOrder
  - [ ] Replaces current `src/offchain/order_poller.rs` logic
  - [ ] Follows pattern from `../st0x.issuance-b/src/redemption/burn_manager.rs`
- [ ] Update manager tests
  - [ ] Test TradeManager event→command flow with mock CQRS
  - [ ] Test OrderManager places broker orders
  - [ ] Test OrderManager polling and state updates

## Task 8. CQRS Framework Integration

Wire up the CQRS framework with aggregates and views.

**Reasoning**: This creates the SqliteCqrs instances that will replace direct
database access. Each aggregate gets its own CQRS instance.

- [ ] Create `src/lib.rs` setup functions
  - [ ] `setup_onchain_trade_cqrs(pool: SqlitePool) -> SqliteCqrs<OnChainTrade>`
    - [ ] Uses `sqlite_cqrs()` from sqlite-es crate
    - [ ] Registers OnChainTradeView as query processor
    - [ ] Registers MetricsPnLView as query processor (listens to
          OnChainTradeEvent)
  - [ ] `setup_position_cqrs(pool: SqlitePool) -> SqliteCqrs<Position>`
    - [ ] Registers PositionView as query processor
    - [ ] Registers MetricsPnLView as query processor (listens to PositionEvent)
  - [ ] `setup_offchain_order_cqrs(pool: SqlitePool) -> SqliteCqrs<OffchainOrder>`
    - [ ] Registers OffchainTradeView as query processor
  - [ ] `setup_schwab_auth_cqrs(pool: SqlitePool) -> SqliteCqrs<SchwabAuth>`
    - [ ] Registers SchwabAuthView as query processor
- [ ] Create query helper functions in each view module
  - [ ] `src/onchain_trade/view.rs`: query functions for finding trades by
        tx_hash, symbol, block range
  - [ ] `src/position/view.rs`: query functions for finding positions by symbol,
        pending executions
  - [ ] `src/offchain_order/view.rs`: query functions for finding executions by
        status, symbol, broker
  - [ ] `src/schwab/auth_view.rs`: query function for retrieving tokens
  - [ ] `src/metrics_pnl/view.rs`: query functions for PnL by symbol, time range
- [ ] Write integration tests
  - [ ] Test command execution persists events
  - [ ] Test events update views
  - [ ] Test queries return correct data
  - [ ] Test snapshots are created after N events

## Task 9. Data Migration Script

Implement the migration script to backfill existing data using genesis events.

**Reasoning**: Production data must be preserved. Migrated events snapshot
legacy state without synthesizing complete event histories.

- [ ] Create `src/bin/migrate_to_events.rs`
- [ ] Implement main migration flow
  - [ ] Check if events table has data (detect prior migration)
  - [ ] Prompt user for confirmation
  - [ ] Execute in transaction with rollback on error
  - [ ] Report progress for each table
- [ ] Implement
      `migrate_onchain_trades(pool: &SqlitePool, cqrs: &SqliteCqrs<OnChainTrade>)`
  - [ ] Query all rows from `onchain_trades` ordered by created_at
  - [ ] For each row, execute command that produces Migrated event
  - [ ] Handle optional pyth_price fields (NULL → None)
  - [ ] Use tx_hash:log_index as aggregate_id
- [ ] Implement
      `migrate_positions(pool: &SqlitePool, cqrs: &SqliteCqrs<Position>)`
  - [ ] Query all rows from `trade_accumulators`
  - [ ] For each row, execute command that produces Migrated event
  - [ ] Default threshold: ExecutionThreshold::Shares(1.0)
  - [ ] Use symbol as aggregate_id
- [ ] Implement
      `migrate_offchain_orders(pool: &SqlitePool, cqrs: &SqliteCqrs<OffchainOrder>)`
  - [ ] Query all rows from `offchain_trades` ordered by id
  - [ ] For each row, execute command that produces Migrated event
  - [ ] Map status strings to MigratedOrderStatus enum
  - [ ] Use execution id as aggregate_id
- [ ] Implement
      `migrate_schwab_auth(pool: &SqlitePool, cqrs: &SqliteCqrs<SchwabAuth>)`
  - [ ] Query `schwab_auth` table (singleton, id=1)
  - [ ] Execute command that produces TokensStored event
  - [ ] Skip if no auth data exists
- [ ] Implement `verify_migration(pool: &SqlitePool)`
  - [ ] Rebuild views from events (idempotent view updates)
  - [ ] Compare counts: onchain_trades vs onchain_trade_view
  - [ ] Compare counts: trade_accumulators vs position_view
  - [ ] Compare counts: offchain_trades vs offchain_trade_view
  - [ ] Sample random records and verify field values match
  - [ ] Return detailed error report if mismatches found
- [ ] Write migration tests with synthetic legacy data

## Task 10. Update Conductor to Use CQRS

Refactor the conductor to use commands instead of direct database writes, and
wire up managers.

**Reasoning**: The conductor currently writes directly to tables. It needs to
send commands through CQRS instead. This task also wires up the managers
(implemented in Task 7) to the actual CQRS instances (created in Task 8).

- [ ] Update `src/conductor/mod.rs`
  - [ ] Add CQRS instances as fields in `Conductor` struct
  - [ ] Instantiate TradeManager and OrderManager with CQRS instances
  - [ ] Wire up TradeManager to listen to OnChainTradeEvent::Filled from
        OnChainTrade CQRS
  - [ ] Wire up OrderManager to listen to PositionEvent::OffChainOrderPlaced
        from Position CQRS
  - [ ] Remove direct database write code
  - [ ] Replace with command execution through CQRS
- [ ] Update event processing flow
  - [ ] When blockchain event received, execute OnChainTradeCommand::Witness
  - [ ] Event flow: OnChainTradeEvent::Filled → TradeManager →
        PositionCommand::AcknowledgeOnChainFill
  - [ ] Position aggregate checks threshold and emits
        PositionEvent::OffChainOrderPlaced if met
  - [ ] Event flow: PositionEvent::OffChainOrderPlaced → OrderManager → broker
        execution → OffchainOrder updates
- [ ] Remove `src/onchain/accumulator.rs` (logic now in Position aggregate)
- [ ] Remove `src/offchain/order_poller.rs` (logic now in OrderManager)
- [ ] Update imports and error handling
- [ ] Integration tests for full flow

## Task 11. Update CLI to Use CQRS

Update CLI commands to use CQRS queries and commands.

**Reasoning**: CLI currently queries tables directly. It needs to use view
queries and command execution.

- [ ] Update `src/cli.rs`
  - [ ] Initialize CQRS instances in main()
  - [ ] Replace direct `sqlx::query!` with view queries
  - [ ] Use command execution for any state-changing operations
- [ ] Update query commands
  - [ ] List trades: query `onchain_trade_view`
  - [ ] List positions: query `position_view`
  - [ ] List executions: query `offchain_trade_view`
- [ ] Update test command
  - [ ] Execute commands through CQRS instead of direct broker calls
- [ ] Remove deprecated database query functions
- [ ] CLI tests still pass

## Task 12. Update API to Use CQRS

Update REST API endpoints to query views instead of tables.

**Reasoning**: API currently queries tables directly. Views are the read models
in CQRS.

- [ ] Update `src/api.rs`
  - [ ] Pass CQRS instances or view repositories to Rocket state
  - [ ] Replace direct table queries with view queries
- [ ] Update each endpoint
  - [ ] Trades endpoint: query `onchain_trade_view`
  - [ ] Positions endpoint: query `position_view`
  - [ ] Executions endpoint: query `offchain_trade_view`
  - [ ] PnL endpoint: query `metrics_pnl_view`
- [ ] API integration tests pass

## Task 13. Broker Crate Review

Review broker crate for any needed changes.

**Reasoning**: Broker crate is separate library. It should remain
broker-agnostic and unchanged, but verify assumptions.

- [ ] Review `crates/broker/src/lib.rs`
  - [ ] Verify Broker trait doesn't need changes
  - [ ] Confirm order types (MarketOrder, OrderPlacement, OrderState) work with
        CQRS
- [ ] Review Schwab and Alpaca implementations
  - [ ] Confirm no direct database dependencies
  - [ ] Verify they remain stateless (good for CQRS)
- [ ] Update broker tests if needed
- [ ] **Expected outcome**: No changes needed, but document any required updates

## Task 14. Testing and Validation

Comprehensive testing of all components.

**Reasoning**: Validate each aggregate and manager works correctly in isolation
before full integration.

- [ ] Verify all unit tests pass for aggregates
  - [ ] OnChainTrade aggregate tests
  - [ ] Position aggregate tests with all threshold scenarios
  - [ ] OffchainOrder aggregate tests with state transitions
  - [ ] SchwabAuth aggregate tests
- [ ] Verify integration tests pass
  - [ ] Event persistence and replay
  - [ ] View updates from events
  - [ ] Query functions return correct data
- [ ] Test migration script with synthetic data
  - [ ] Create test database with legacy schema
  - [ ] Run migration
  - [ ] Verify data integrity
- [ ] Test with dry-run broker
  - [ ] Full system operation without real trades
  - [ ] Verify all events recorded correctly
- [ ] Performance testing
  - [ ] Measure event processing throughput
  - [ ] Measure view query performance
  - [ ] Check snapshot overhead
- [ ] Run full test suite: `timeout 180 cargo test -q`
- [ ] Run clippy:
      `timeout 90 cargo clippy --all-targets --all-features -- -D clippy::all`
- [ ] Run formatter: `cargo fmt`

**Note**: Comprehensive E2E tests with real blockchain events should be tracked
in a separate GitHub issue.

## Task 15. Documentation and Deployment

Update documentation and prepare for production deployment.

**Reasoning**: Ensure system is documented and ready for production deployment
with proper backup procedures.

- [ ] Update inline documentation
  - [ ] Document aggregate business rules in each aggregate's mod.rs
  - [ ] Document manager patterns in manager files
  - [ ] Document view update logic in view files
- [ ] Create deployment runbook as separate document (not in README)
  - [ ] Pre-deployment: Backup entire deployment folder to timestamped backup
        location
  - [ ] Execute migration script: `cargo run --bin migrate-to-events`
  - [ ] Verify data integrity checks pass
  - [ ] Rollback procedure: restore from backup folder and restart
  - [ ] Grafana dashboard updates (if column names changed)
- [ ] Remove temporary development artifacts
  - [ ] Delete old database access code (accumulator, old poller)
  - [ ] Remove commented-out code
  - [ ] Remove migration binary after successful production migration
  - [ ] Verify all tests pass
  - [ ] Final clippy and fmt check
- [ ] Update Grafana dashboards
  - [ ] Change table names from old tables to `_view` tables
  - [ ] Test all dashboards work with new schema
  - [ ] Document any query changes needed

**Note**: Do NOT add "Implementation Notes" to SPEC.md - it's a specification
document, not a changelog. Do NOT document migration steps in README.md - that's
in the deployment runbook.
