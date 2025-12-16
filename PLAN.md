# Plan: Manual Rebalancing CLI Commands

Add CLI commands to manually test rebalancing operations using the existing
rebalancing infrastructure before enabling the full automated flow.

## Task 1. Add CLI Command Definitions ✅

Add two CLI subcommands:

- [x] `transfer-equity` - Transfer tokenized equity between venues
  - Parameters: `--direction` (to-raindex | to-alpaca), `--ticker`,
    `--quantity`, `--token-address` (optional, for to-alpaca)
  - to-raindex: Uses existing `MintManager` (Alpaca → Raindex)
  - to-alpaca: Uses existing `RedemptionManager` (Raindex → Alpaca)

- [x] `transfer-usdc` - Transfer USDC between venues
  - Parameters: `--direction` (to-raindex | to-alpaca), `--amount`
  - to-raindex: Uses existing `UsdcRebalanceManager.execute_alpaca_to_base`
  - to-alpaca: Uses existing `UsdcRebalanceManager.execute_base_to_alpaca`

**Changes made:**

- Added `TransferDirection` enum (ToRaindex, ToAlpaca)
- Added `TransferEquity` and `TransferUsdc` commands with proper types
- Used `Symbol`, `FractionalShares`, `Usdc` types (made public for CLI
  visibility)
- Added `FromStr` and `Display` impls for `FractionalShares` and `Usdc`
- Added tests for command structure validation

## Task 2. Implement transfer-equity Command ✅

- [x] Require Alpaca broker configuration
- [x] Require rebalancing configuration
- [x] For to-raindex direction:
  - Create `MintManager` using existing `spawn.rs` service construction
  - Generate `IssuerRequestId`
  - Call `mint_manager.execute_mint()`
- [x] For to-alpaca direction:
  - Create `RedemptionManager` using existing service construction
  - Require `--token-address` parameter for redemption
  - Generate `RedemptionAggregateId`
  - Call `redemption_manager.execute_redemption()`
- [x] Add tests for CLI command structure

**Changes made:**

- Implemented `transfer_equity_command` function in `src/cli.rs`
- Validates broker is Alpaca (rejects Schwab/DryRun)
- Validates rebalancing config exists
- Creates `AlpacaTokenizationService` with Base provider connection
- For `ToRaindex`: Creates `MintManager` with CQRS event store, generates
  `IssuerRequestId`, calls `execute_mint()` with market maker wallet
- For `ToAlpaca`: Requires `--token-address` parameter, creates
  `RedemptionManager`, generates `RedemptionAggregateId`, converts quantity to
  U256 with 18 decimals, calls `execute_redemption()`
- Added `decimal_to_u256_18_decimals` helper for token amount conversion

## Task 3. Implement transfer-usdc Command ✅

- [x] Require Alpaca broker configuration
- [x] Require rebalancing configuration
- [x] Create `UsdcRebalanceManager` using existing service construction
- [x] Generate `UsdcRebalanceId`
- [x] For to-raindex: Call `usdc_manager.execute_alpaca_to_base()`
- [x] For to-alpaca: Call `usdc_manager.execute_base_to_alpaca()`
- [x] Tests: CLI command structure tests already cover transfer-usdc

**Changes made:**

- Implemented `transfer_usdc_command` function in `src/cli.rs`
- Validates broker is Alpaca (rejects Schwab/DryRun)
- Validates rebalancing config exists
- Creates all required services following `spawn.rs` pattern:
  - `PrivateKeySigner` and `EthereumWallet` from config's private key
  - Ethereum HTTP provider for CCTP operations
  - `AlpacaWalletService` for wallet operations
  - `CctpBridge` with Ethereum and Base EVMs for cross-chain bridging
  - `VaultService` for vault deposits/withdrawals
  - CQRS framework for USDC rebalance aggregate
- Creates `UsdcRebalanceManager` with all dependencies
- Generates unique `UsdcRebalanceId`
- For `ToRaindex`: Calls `execute_alpaca_to_base()` (Alpaca withdrawal → CCTP
  bridge → vault deposit)
- For `ToAlpaca`: Calls `execute_base_to_alpaca()` (vault withdrawal → CCTP
  bridge → Alpaca deposit)

## Key Principle

**Reuse existing code.** The managers (`MintManager`, `RedemptionManager`,
`UsdcRebalanceManager`) already implement the full transfer logic. The CLI
commands should construct these managers the same way `spawn_rebalancer` does
and call their existing methods.
