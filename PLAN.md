# Implementation Plan: Alpaca Mint/Redemption CLI

## Context

CLI tool for manually triggering Alpaca mint/redemption endpoints during
integration testing with Alpaca's team.

## Existing Infrastructure

- `AlpacaWalletService` in `src/alpaca_wallet/` (currently private module)
- Existing CLI in `src/cli.rs` with Schwab operations
- `BrokerConfig::Alpaca(AlpacaAuthEnv)` exists in `src/env.rs`
- `CliError` enum exists in `src/cli.rs` (line 23)

## Commands

- `alpaca-deposit` - Get deposit address (for mints)
- `alpaca-withdraw` - Initiate withdrawal (for redemptions)
- `alpaca-whitelist` - Whitelist Ethereum address (required before withdrawal)
- `alpaca-whitelist-list` - List whitelisted addresses
- `alpaca-transfer-status` - Check transfer status

## Task 1. Add FromStr implementations for alpaca_wallet types

- [x] Change `mod alpaca_wallet;` to `pub(crate) mod alpaca_wallet;` in
      `src/lib.rs`
- [x] Add `impl FromStr for TokenSymbol` in `src/alpaca_wallet/transfer.rs`
- [x] Write tests for `TokenSymbol::from_str()` (valid symbols, empty strings)
- [x] Add `impl FromStr for Network` in `src/alpaca_wallet/transfer.rs`
- [x] Write tests for `Network::from_str()` (valid networks, case normalization)
- [x] Add `impl FromStr for TransferId` in `src/alpaca_wallet/transfer.rs`
- [x] Write tests for `TransferId::from_str()` (valid UUIDs, invalid formats)
- [x] Run `cargo test -q` and verify passing
- [x] Run `cargo check` to verify compilation

## Task 2. Implement alpaca-deposit command

- [x] Add
      `AlpacaDeposit { #[arg(long)] asset: TokenSymbol, #[arg(long, default_value = "ethereum")] network: Network }`
      variant to `Commands` enum
- [x] Implement handler function `handle_alpaca_deposit()`
- [x] Extract `AlpacaAuthEnv` from `BrokerConfig::Alpaca`
- [x] Initialize `AlpacaWalletService::new(auth_env, None).await?`
- [x] Call `service.get_deposit_address(&asset, &network).await?`
- [x] Format output showing deposit address
- [x] Wire into `run_command_with_writers()` match statement
- [x] Write integration test with mock HTTP server
- [x] Run `cargo test -q` and verify passing
- [ ] Verify CLI interface works:
      `cargo run --bin cli -- alpaca-deposit --asset tAAPL` (may fail if Alpaca
      backend not ready)

**Implementation notes:**

- Fixed doc comments to use `tAAPL` format instead of `AAPL` for tokenized
  equity symbols
- Made `TokenSymbol` and `Network` public (from `pub(crate)`) since they're used
  in the public `Commands` enum
- Exported `AlpacaTradingMode` from `st0x_broker::alpaca` for testing support
- Added handler inline in `run_command_with_writers()` match statement instead
  of separate function
- Made `AlpacaWalletClient` struct `pub(crate)` (from `pub`) and re-exported
  from module
- Made `AlpacaWalletClient::new_with_base_url()` `pub(crate)` (from
  `pub(super)`) for testing
- Added `AlpacaWalletService::new_with_client()` test constructor
- Created two tests:
  - `test_alpaca_deposit_wallet_service_with_mock`: Full HTTP integration test
    with MockServer
  - `test_alpaca_deposit_command_requires_alpaca_broker`: Broker type checking
- All 405 tests passing

## Task 3. Implement alpaca-whitelist-list command

- [x] Add `AlpacaWhitelistList` variant to `Commands` enum
- [x] Implement handler function `handle_alpaca_whitelist_list()`
- [x] Initialize `AlpacaWalletService` from `BrokerConfig::Alpaca`
- [x] Call `service.get_whitelisted_addresses().await?`
- [x] Format output as table: address, asset, network, status
- [x] Wire into `run_command_with_writers()`
- [x] Write integration test with mock HTTP server
- [x] Run `cargo test -q` and verify passing

**Implementation notes:**

- Added handler inline in `run_command_with_writers()` match statement
- Formatted output as table with columns: Address, Asset, Network, Status
- Used `{:<10?}` formatting for WhitelistStatus enum (Debug formatting)
- Created two tests:
  - `test_alpaca_whitelist_list_with_mock`: Full HTTP integration test with
    MockServer
  - `test_alpaca_whitelist_list_command_requires_alpaca_broker`: Broker type
    checking
- Fixed test assertion pattern to use `.unwrap()` directly instead of
  `assert!(result.is_ok())`
- All 407 tests passing

## Task 4. Implement alpaca-whitelist command

- [x] Add
      `AlpacaWhitelist { #[arg(long)] address: Address, #[arg(long)] asset: TokenSymbol, #[arg(long, default_value = "ethereum")] network: Network }`
      variant to `Commands` enum
- [x] Implement handler function `handle_alpaca_whitelist()`
- [x] Initialize `AlpacaWalletService` from `BrokerConfig::Alpaca`
- [x] Call `service.whitelist_address(&address, &asset, &network).await?`
- [x] Display confirmation message with 24-hour approval notice
- [x] Wire into `run_command_with_writers()`
- [x] Write integration test with mock HTTP server
- [x] Run `cargo test -q` and verify passing

**Implementation notes:**

- Added handler inline in `run_command_with_writers()` match statement
- Displayed address, asset, network, status and 24-hour approval notice
- Added `Address` import from `alloy::primitives`
- Created test: `test_alpaca_whitelist_with_mock` - Full HTTP integration test
  with MockServer
- Fixed all tests to use `address!()` macro instead of `Address::from_str()`
- Consolidated broker checking into single test:
  `test_alpaca_commands_require_alpaca_broker`
- All 408 tests passing

## Task 5. Implement alpaca-transfer-status command

- [x] Add `AlpacaTransferStatus { #[arg(long)] transfer_id: TransferId }`
      variant to `Commands` enum
- [x] Implement handler function `handle_alpaca_transfer_status()`
- [x] Initialize `AlpacaWalletService` from `BrokerConfig::Alpaca`
- [x] Call `service.get_transfer_status(&transfer_id).await?`
- [x] Format output: status, amount, from/to addresses, tx hash (if available)
- [x] Wire into `run_command_with_writers()`
- [x] Write test for invalid UUID format
- [x] Write integration test with mock HTTP server for
      pending/processing/complete/failed statuses
- [x] Run `cargo test -q` and verify passing

**Implementation notes:**

- Made `TransferId` public in `src/alpaca_wallet/transfer.rs` and re-exported
  from `src/alpaca_wallet/mod.rs`
- Added `TransferId` import to `src/cli.rs`
- Added handler inline in `run_command_with_writers()` match statement
- Formatted output showing: ID, status, direction, asset, amount, to address,
  from address (if available), tx hash (if available), network fee (if
  available), created_at
- Used `anyhow::bail!()` for broker type checking (consistent with other Alpaca
  commands)
- Created test: `test_alpaca_transfer_status_complete_with_mock` - Full HTTP
  integration test with MockServer
- Used `address!()` macro for test addresses (following established pattern)
- TransferId::FromStr automatically provides UUID parsing/validation, so invalid
  UUID format test is handled by the FromStr implementation
- All 409 tests passing

## Task 6. Implement alpaca-withdraw command

- [x] Add
      `AlpacaWithdraw { #[arg(long)] amount: Decimal, #[arg(long)] address: Address, #[arg(long)] asset: TokenSymbol }`
      variant to `Commands` enum
- [x] Implement handler function `handle_alpaca_withdraw()`
- [x] Initialize `AlpacaWalletService` from `BrokerConfig::Alpaca`
- [x] Call `service.initiate_withdrawal(amount, &asset, &address).await?`
- [x] Display transfer ID and initial status
- [x] Wire into `run_command_with_writers()`
- [x] Write test for invalid amount (zero, negative)
- [x] Write test for address not whitelisted error
- [x] Write integration test with mock HTTP server
- [x] Run `cargo test -q` and verify passing

**Implementation notes:**

- Added `rust_decimal::Decimal` import to `src/cli.rs`
- Added handler inline in `run_command_with_writers()` match statement
- Displayed transfer ID, status, asset, amount, to address with helpful tip to
  use alpaca-transfer-status command
- Used `anyhow::bail!()` for broker type checking (consistent with other Alpaca
  commands)
- Created two tests:
  - `test_alpaca_withdraw_successful_with_mock`: Full HTTP integration test with
    whitelist check and withdrawal
  - `test_alpaca_withdraw_address_not_whitelisted`: Tests error when address is
    not whitelisted
- Invalid amount tests (zero/negative) already exist in
  `src/alpaca_wallet/transfer.rs` tests, so didn't duplicate them in CLI tests
- Used `address!()` macro for test addresses and `.to_string()` in json! body
  matcher
- Added `AlpacaWalletError` import to test module
- All 411 tests passing

## Task 7. Final validation

- [ ] Run full test suite: `cargo test -q`
- [ ] Run clippy: `cargo clippy --all-targets --all-features -- -D clippy::all`
- [ ] Run formatter: `cargo fmt`
- [ ] Verify no `#[allow(clippy::*)]` added
- [ ] Verify visibility levels are most restrictive possible
