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

## Task 2. Implement transfer-equity Command

- [ ] Require Alpaca broker configuration
- [ ] Require rebalancing configuration
- [ ] For to-raindex direction:
  - Create `MintManager` using existing `spawn.rs` service construction
  - Generate `IssuerRequestId`
  - Call `mint_manager.execute_mint()`
- [ ] For to-alpaca direction:
  - Create `RedemptionManager` using existing service construction
  - Look up token address from ticker (or require as parameter)
  - Generate `RedemptionAggregateId`
  - Call `redemption_manager.execute_redemption()`
- [ ] Add tests

## Task 3. Implement transfer-usdc Command

- [ ] Require Alpaca broker configuration
- [ ] Require rebalancing configuration
- [ ] Create `UsdcRebalanceManager` using existing service construction
- [ ] Generate `UsdcRebalanceId`
- [ ] For to-raindex: Call `usdc_manager.execute_alpaca_to_base()`
- [ ] For to-alpaca: Call `usdc_manager.execute_base_to_alpaca()`
- [ ] Add tests

## Key Principle

**Reuse existing code.** The managers (`MintManager`, `RedemptionManager`,
`UsdcRebalanceManager`) already implement the full transfer logic. The CLI
commands should construct these managers the same way `spawn_rebalancer` does
and call their existing methods.
