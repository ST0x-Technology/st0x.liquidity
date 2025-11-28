# Implementation Plan: Manual Integration Testing CLI Commands

## Overview

Add CLI commands for manual testing of rebalancing system components: position
accumulation, execution creation, order placement, status polling, and audit
trails.

## Technical Context

Existing infrastructure:

- Position accumulators in `src/onchain/accumulator.rs` with `find_by_symbol`
  query
- Execution records in `src/offchain/execution.rs` with
  `find_executions_by_symbol_status_and_broker` and `find_execution_by_id`
- Onchain trades in `src/onchain/trade.rs`
- CLI infrastructure in `src/cli.rs` with `Commands` enum and
  `run_command_with_writers` pattern

Database schema:

- `onchain_trades`: Immutable blockchain trade records
- `offchain_trades`: Execution tracking (PENDING → SUBMITTED → FILLED/FAILED)
- `trade_accumulators`: Position state per symbol (accumulated_long,
  accumulated_short, net_position, pending_execution_id)
- `trade_execution_links`: Many-to-many audit trail linking trades to executions

Code organization: Follow existing CLI pattern (add `Commands` variant →
implement handler in `run_command_with_writers` → test with mocked writer). Use
`run_command_with_writers<W: Write>` signature for testability.

## Task 1. Implement show-position command

Inspect position accumulator state for a symbol.

- [ ] Add `Commands::ShowPosition` variant with symbol field
- [ ] Create handler `show_position_with_writer`
  - Validate symbol using `validate_ticker`
  - Call `accumulator::find_by_symbol(pool, &symbol)`
  - Handle `None` result with helpful error message
  - Format output: net position, accumulated long/short, pending execution ID,
    threshold, last updated
- [ ] Add integration tests
  - `test_show_position_not_found`: helpful message when position doesn't exist
  - `test_show_position_with_accumulation`: verify fractional display
  - `test_show_position_with_pending_execution`: verify pending execution ID
    shown
- [ ] Run `cargo test -q` and `cargo clippy`

## Task 2. Implement show-executions and show-execution commands

Query offchain execution status with optional filtering.

- [ ] Add `Commands::ShowExecutions` variant with optional symbol, status,
      broker filters
- [ ] Add `Commands::ShowExecution` variant with execution id field
- [ ] Create handler `show_executions_with_writer`
  - Parse and validate optional status, broker, symbol strings
  - Call `find_executions_by_symbol_status_and_broker`
  - Format as table: ID | Symbol | Shares | Direction | Broker | Status | Order
    ID | Price
  - Convert price_cents to dollars, show "N/A" for None fields
- [ ] Create handler `show_execution_with_writer`
  - Call `find_execution_by_id`, handle None with clear error
  - Display full execution details with timestamps and order state
- [ ] Add integration tests
  - `test_show_executions_no_filters`,
    `test_show_executions_with_symbol_filter`,
    `test_show_executions_with_status_filter`,
    `test_show_executions_combined_filters`,
    `test_show_executions_empty_results`
  - `test_show_executions_invalid_status`, `test_show_executions_invalid_broker`
  - `test_show_execution_by_id`, `test_show_execution_not_found`
- [ ] Run `cargo test -q` and `cargo clippy`

## Task 3. Implement show-trades and show-trade commands

Inspect stored onchain trades and their linkages. Requires new database queries.

- [ ] Add query function to `src/onchain/trade.rs`:
      `find_recent_trades(pool, symbol_filter, limit)` querying `onchain_trades`
      ordered by `created_at DESC`
- [ ] Add query function to `src/onchain/trade.rs`:
      `find_trade_linkages(pool, trade_id)` querying `trade_execution_links`
      with joins to `offchain_trades`
- [ ] Define `TradeExecutionLink` struct with fields: link_id, execution_id,
      contributed_shares, created_at, execution_symbol, execution_direction
- [ ] Add `Commands::ShowTrades` variant with optional symbol filter and limit
      (default 50)
- [ ] Add `Commands::ShowTrade` variant with trade id field
- [ ] Create handler `show_trades_with_writer`
  - Validate limit (1-500 range), validate symbol if provided
  - Call `find_recent_trades`
  - Format as table: ID | TX Hash | Log Index | Symbol | Amount | Direction |
    Price | Created
  - Truncate tx_hash to "0x1234...abcd" format
- [ ] Create handler `show_trade_with_writer`
  - Call `OnchainTrade::find_by_id` (add function if needed), call
    `find_trade_linkages`
  - Display full trade details with complete tx_hash
  - Display linked executions table with contributed_shares
- [ ] Add integration tests
  - `test_show_trades_default_limit`, `test_show_trades_custom_limit`,
    `test_show_trades_with_symbol_filter`, `test_show_trades_limit_validation`
  - `test_show_trade_detail`, `test_show_trade_not_found`,
    `test_show_trade_no_linkages`
- [ ] Run `cargo test -q` and `cargo clippy`

## Task 4. Implement show-links command

Inspect trade-execution linkages. Requires new database queries.

- [ ] Create `src/onchain/linkage.rs` module
- [ ] Define `LinkageDetail` struct with fields: link_id, trade_id,
      execution_id, contributed_shares, created_at, trade_symbol, trade_amount,
      execution_symbol, execution_shares
- [ ] Add function `find_links_by_execution(pool, execution_id)` querying
      `trade_execution_links` with joins
- [ ] Add function `find_links_by_trade(pool, trade_id)` querying
      `trade_execution_links` with joins
- [ ] Export linkage module in `src/onchain/mod.rs`
- [ ] Add `Commands::ShowLinks` variant with optional execution_id and trade_id
      fields
- [ ] Create handler `show_links_with_writer`
  - Validate at least one ID provided
  - Call appropriate query function
  - Format as table showing linked trades or executions
  - Display summary: total contributed shares, number of links, date range
- [ ] Add integration tests
  - `test_show_links_requires_id`, `test_show_links_by_execution`,
    `test_show_links_by_trade`, `test_show_links_not_found`,
    `test_show_links_summary_calculation`
- [ ] Run `cargo test -q` and `cargo clippy`

## Task 5. Implement check-threshold command

Manually trigger threshold checking logic for a symbol.

- [ ] Add `Commands::CheckThreshold` variant with symbol field
- [ ] Create handler `check_threshold_with_writer`
  - Validate symbol
  - Begin SQL transaction
  - Call `accumulator::check_all_accumulated_positions` or equivalent for single
    symbol
  - Capture outcome: threshold met/not met, execution created, stale cleanup,
    lock held
  - Display detailed decision process with current position state
  - Commit transaction if successful
- [ ] Format output for different scenarios:
  - Below threshold: show gap to threshold
  - Threshold met: show execution created + remaining shares
  - Stale execution cleaned: show cleanup details
  - Lock held: explain concurrency protection
- [ ] Add integration tests
  - `test_check_threshold_below`, `test_check_threshold_exactly_one`,
    `test_check_threshold_above`
  - `test_check_threshold_with_pending`, `test_check_threshold_stale_cleanup`
- [ ] Run `cargo test -q` and `cargo clippy`

## Task 6. Implement poll-order command

Manually poll broker for order status and update database.

- [ ] Add `Commands::PollOrder` variant with execution_id field
- [ ] Create handler `poll_order_with_writer`
  - Call `find_execution_by_id`, verify execution exists
  - Verify execution is in Submitted status (error if Pending/Filled/Failed)
  - Extract order_id from Submitted state
  - Initialize broker based on execution.broker field (Schwab/Alpaca/DryRun)
  - Call `broker.get_order_status(order_id)`
  - Begin SQL transaction, update execution state, commit
  - Display before/after state transition
- [ ] Format output for different scenarios:
  - No change: "still pending"
  - Fill: show executed_at, price, total value
  - Failure: show failure reason
  - Error: explain why polling not applicable (wrong status)
- [ ] Add integration tests with mocked broker
  - `test_poll_order_submitted_to_filled`, `test_poll_order_no_change`
  - `test_poll_order_pending_error`, `test_poll_order_filled_error`,
    `test_poll_order_not_found`, `test_poll_order_broker_error`
- [ ] Run `cargo test -q` and `cargo clippy`

## Task 7. Implement simulate-execution command

Create mock executions for testing database operations without broker API.

- [ ] Add `Commands::SimulateExecution` variant with symbol, shares, direction
      fields
- [ ] Create handler `simulate_execution_with_writer`
  - Validate symbol, validate shares > 0
  - Parse direction string to Direction enum
  - Create OffchainExecution with DryRun broker, Pending status
  - Begin SQL transaction
  - Save execution (Pending)
  - Transition to Submitted with mock order_id (e.g., "MOCK_1234567890")
  - Transition to Filled with mock price ($100.00) and current timestamp
  - Commit transaction
  - Display each state transition
- [ ] Add integration tests
  - `test_simulate_execution_buy`, `test_simulate_execution_sell`
  - `test_simulate_execution_invalid_direction`,
    `test_simulate_execution_zero_shares`
  - `test_simulate_execution_database_state`
- [ ] Run `cargo test -q` and `cargo clippy`

## Task 8. Implement reset-position command

Reset position accumulator state for testing. Requires safety confirmation.

- [ ] Add `Commands::ResetPosition` variant with symbol and confirm fields
- [ ] Create handler `reset_position_with_writer`
  - Check confirm flag (error if false with helpful message showing command
    usage)
  - Load current accumulator state, display it
  - Begin SQL transaction
  - If pending_execution_id exists: load execution, mark as FAILED with reason
    "Reset by user"
  - Reset accumulator: accumulated_long = 0.0, accumulated_short = 0.0
  - Clear pending_execution_id, update last_updated timestamp
  - Commit transaction
  - Display final state, warn that onchain trades remain intact
- [ ] Add integration tests
  - `test_reset_position_without_confirm`, `test_reset_position_with_confirm`
  - `test_reset_position_with_pending_execution`,
    `test_reset_position_no_pending_execution`
  - `test_reset_position_onchain_trades_intact`
- [ ] Run `cargo test -q` and `cargo clippy`

## Task 9. Add documentation and help text

Document all commands with usage examples.

- [ ] Add doc comments to each command variant
  ```rust
  /// Inspect position accumulator state for a symbol
  ShowPosition { ... }
  ```
- [ ] Create `examples/CLI_MANUAL_TESTING.md` with:
  - Overview of manual testing workflow
  - Example scenarios: inspecting accumulation, monitoring executions, audit
    trail, testing, reset
  - Expected outputs for each scenario
  - Common troubleshooting tips
- [ ] Test complete workflow end-to-end:
  - Reset position → Process transaction → Check position → Check threshold →
    Show executions → Show trades → Show links
- [ ] Verify `cargo run --bin cli -- --help` shows all commands with
      descriptions
- [ ] Run final `cargo test -q`,
      `cargo clippy --all-targets --all-features -- -D clippy::all`, `cargo fmt`
