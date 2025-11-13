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

- [ ] Change `mod alpaca_wallet;` to `pub(crate) mod alpaca_wallet;` in
      `src/lib.rs`
- [ ] Add `impl FromStr for TokenSymbol` in `src/alpaca_wallet/transfer.rs`
- [ ] Write tests for `TokenSymbol::from_str()` (valid symbols, empty strings)
- [ ] Add `impl FromStr for Network` in `src/alpaca_wallet/transfer.rs`
- [ ] Write tests for `Network::from_str()` (valid networks, case normalization)
- [ ] Add `impl FromStr for TransferId` in `src/alpaca_wallet/transfer.rs`
- [ ] Write tests for `TransferId::from_str()` (valid UUIDs, invalid formats)
- [ ] Run `cargo test -q` and verify passing
- [ ] Run `cargo check` to verify compilation

## Task 2. Implement alpaca-deposit command

- [ ] Add
      `AlpacaDeposit { #[arg(long)] asset: TokenSymbol, #[arg(long, default_value = "ethereum")] network: Network }`
      variant to `Commands` enum
- [ ] Implement handler function `handle_alpaca_deposit()`
- [ ] Extract `AlpacaAuthEnv` from `BrokerConfig::Alpaca`
- [ ] Initialize `AlpacaWalletService::new(auth_env, None).await?`
- [ ] Call `service.get_deposit_address(&asset, &network).await?`
- [ ] Format output showing deposit address
- [ ] Wire into `run_command_with_writers()` match statement
- [ ] Write integration test with mock HTTP server
- [ ] Run `cargo test -q` and verify passing
- [ ] Verify CLI interface works:
      `cargo run --bin cli -- alpaca-deposit --asset AAPL` (may fail if Alpaca
      backend not ready)

## Task 3. Implement alpaca-whitelist-list command

- [ ] Add `AlpacaWhitelistList` variant to `Commands` enum
- [ ] Implement handler function `handle_alpaca_whitelist_list()`
- [ ] Initialize `AlpacaWalletService` from `BrokerConfig::Alpaca`
- [ ] Call `service.get_whitelisted_addresses().await?`
- [ ] Format output as table: address, asset, network, status
- [ ] Wire into `run_command_with_writers()`
- [ ] Write integration test with mock HTTP server
- [ ] Run `cargo test -q` and verify passing

## Task 4. Implement alpaca-whitelist command

- [ ] Add
      `AlpacaWhitelist { #[arg(long)] address: Address, #[arg(long)] asset: TokenSymbol, #[arg(long, default_value = "ethereum")] network: Network }`
      variant to `Commands` enum
- [ ] Implement handler function `handle_alpaca_whitelist()`
- [ ] Initialize `AlpacaWalletService` from `BrokerConfig::Alpaca`
- [ ] Call `service.whitelist_address(&address, &asset, &network).await?`
- [ ] Display confirmation message with 24-hour approval notice
- [ ] Wire into `run_command_with_writers()`
- [ ] Write test for invalid address format
- [ ] Write integration test with mock HTTP server
- [ ] Run `cargo test -q` and verify passing

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
