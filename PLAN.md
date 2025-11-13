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
- Created two tests:
  - `test_alpaca_whitelist_with_mock`: Full HTTP integration test with
    MockServer
  - `test_alpaca_whitelist_command_requires_alpaca_broker`: Broker type checking
- All 409 tests passing

## Task 5. Implement alpaca-transfer-status command

- [ ] Add `AlpacaTransferStatus { #[arg(long)] transfer_id: TransferId }`
      variant to `Commands` enum
- [ ] Implement handler function `handle_alpaca_transfer_status()`
- [ ] Initialize `AlpacaWalletService` from `BrokerConfig::Alpaca`
- [ ] Call `service.get_transfer_status(&transfer_id).await?`
- [ ] Format output: status, amount, from/to addresses, tx hash (if available)
- [ ] Wire into `run_command_with_writers()`
- [ ] Write test for invalid UUID format
- [ ] Write integration test with mock HTTP server for
      pending/processing/complete/failed statuses
- [ ] Run `cargo test -q` and verify passing

## Task 6. Implement alpaca-withdraw command

- [ ] Add
      `AlpacaWithdraw { #[arg(long)] amount: Decimal, #[arg(long)] address: Address, #[arg(long)] asset: TokenSymbol }`
      variant to `Commands` enum
- [ ] Implement handler function `handle_alpaca_withdraw()`
- [ ] Initialize `AlpacaWalletService` from `BrokerConfig::Alpaca`
- [ ] Call `service.initiate_withdrawal(amount, &asset, &address).await?`
- [ ] Display transfer ID and initial status
- [ ] Wire into `run_command_with_writers()`
- [ ] Write test for invalid amount (zero, negative)
- [ ] Write test for address not whitelisted error
- [ ] Write integration test with mock HTTP server
- [ ] Run `cargo test -q` and verify passing

## Task 7. Final validation

- [ ] Run full test suite: `cargo test -q`
- [ ] Run clippy: `cargo clippy --all-targets --all-features -- -D clippy::all`
- [ ] Run formatter: `cargo fmt`
- [ ] Verify no `#[allow(clippy::*)]` added
- [ ] Verify visibility levels are most restrictive possible
