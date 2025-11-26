# Implementation Plan: Rain OrderBook Vault Service (Issue #134)

## Overview

Build a service layer for Rain OrderBook vault operations on Base to deposit and
withdraw USDC using the `deposit2` and `withdraw2` contract functions.

## Design Decisions

**Module Location**: Create `src/onchain/vault.rs` organized by business feature
(vault operations).

**Type Safety**: Use `VaultId` newtype wrapping `U256` to prevent parameter
confusion.

**EVM Account Pattern**: Reuse existing `EvmAccount<P, S>` pattern from
`cctp.rs` for provider and signer management.

**Contract Bindings**: Use existing `IOrderBookV4` bindings from
`src/bindings.rs` - `deposit2` and `withdraw2` already available.

**Tasks Parameter**: Pass empty arrays for the `TaskV1[]` parameter - no
additional tasks needed for basic vault operations.

**Testing Strategy**: Use `alloy::providers::mock::Asserter` for deterministic
contract interaction tests.

## Task 1. Create vault service module with types and core structure

- [x] Create `src/onchain/vault.rs` module file
- [x] Define `VaultId` newtype wrapping `U256`
- [x] Define `VaultError` enum with variants:
  - `Transaction(alloy::providers::PendingTransactionError)`
  - `Contract(alloy::contract::Error)`
  - `InsufficientBalance { requested: U256, available: U256 }`
  - `ZeroAmount`
- [x] Implement error traits using `thiserror`
- [x] Define `VaultService<P, S>` struct with:
  - `account: EvmAccount<P, S>`
  - `orderbook: Address`
- [x] Implement
      `VaultService::new(account: EvmAccount<P, S>, orderbook: Address) -> Self`
- [x] Export module in `src/onchain/mod.rs`
- [x] Run `cargo build` and `cargo clippy` to verify clean compilation

### Completed Changes

Created `src/onchain/vault.rs` with:

- `VaultId` newtype for type-safe vault identifiers
- `VaultError` enum with comprehensive error variants using `thiserror`
- `VaultService<P, S>` generic struct using `EvmAccount` pattern from cctp
  module
- Constructor method for initializing service with account and orderbook address
- Module exported in `src/onchain/mod.rs`

Build and clippy checks pass with only expected dead_code warnings for unused
types.

## Task 2. Update bindings to use rain.orderbook submodule

- [x] Update `src/bindings.rs` to reference OrderBook from
      `lib/rain.orderbook/out/OrderBook.sol/OrderBook.json`
- [x] Verify `nix run .#prepSolArtifacts` builds artifacts correctly
- [x] Run `cargo build` to ensure bindings compile
- [x] Run `cargo clippy`

### Completed Changes

Updated bindings to reference the full OrderBook contract from rain.orderbook
submodule:

- Modified `flake.nix` line 23 to build rain.orderbook instead of
  rain.orderbook.interface
- Updated `src/bindings.rs` to reference
  `lib/rain.orderbook/out/OrderBook.sol/OrderBook.json`
- Ran `nix run .#prepSolArtifacts` to build artifacts (compilation succeeded in
  138.52s, linting error from rain.interpreter dependency is ignorable)
- Verified bindings compile successfully with cargo build and clippy

## Task 3. Create LocalEvm test infrastructure

- [x] Create `src/onchain/vault/test_utils.rs` module
- [x] Implement `LocalEvm` struct:
  - Spawn Anvil instance
  - Deploy OrderBook contract from artifacts
  - Deploy mock ERC20 token
  - Store provider, signer, contract instances
  - Implement helper methods for minting and approving tokens
- [x] Export test_utils module behind `#[cfg(test)]`
- [x] Run `cargo test -q` to verify setup compiles

### Completed Changes

Created comprehensive LocalEvm test infrastructure:

- Used existing Token contract artifact from
  lib/rain.orderbook/out/ArbTest.sol/Token.json for test ERC20
- Added TestERC20 binding in bindings.rs pointing to Token.json artifact
- Created src/onchain/vault/test_utils.rs with:
  - LocalEvm struct with Anvil instance, provider, signer, and deployed contract
    addresses
  - new() method that spawns Anvil, deploys OrderBook and TestERC20 contracts
  - Helper methods: mint_tokens, approve_tokens, get_balance, get_vault_balance
  - LocalEvmError with proper error propagation using #[from] and #[source]
  - Concrete type alias LocalEvmProvider for the complex Fill Provider type
    chain
- All errors properly propagated using `?` operator without string formatting
- All 404 tests pass
- Clippy passes

## Task 4. Implement and test deposit functionality

- [x] Implement `deposit` method:
  ```rust
  pub async fn deposit(
      &self,
      token: Address,
      vault_id: VaultId,
      amount: U256,
  ) -> Result<TxHash, VaultError>
  ```
  - Validate amount is non-zero
  - Call `IOrderBookV4::deposit2` with empty tasks array
  - Get transaction receipt before returning
- [x] Write integration test: `test_deposit_succeeds_with_deployed_contract`
  - Use LocalEvm to set up test environment
  - Mint tokens to test account
  - Approve orderbook to spend tokens
  - Call deposit via VaultService
  - Verify transaction succeeds
  - Query vault balance and verify it increased correctly
- [x] Write unit test: `deposit_rejects_zero_amount`
  - Assert `VaultError::ZeroAmount` without contract call
- [x] Run `cargo test -q` and `cargo clippy`

### Completed Changes

Wrote comprehensive tests for deposit functionality:

- Updated `deposit_rejects_zero_amount` test to use LocalEvm instead of mocks
- Wrote `test_deposit_succeeds_with_deployed_contract` integration test that:
  - Deploys real OrderBook and ERC20 contracts via LocalEvm
  - Approves orderbook to spend tokens
  - Calls deposit via VaultService
  - Verifies vault balance before deposit is zero
  - Verifies vault balance after deposit equals deposited amount
  - Verifies transaction hash is non-zero
- Both tests pass (2/2 passed in 1.66s)
- Clippy passes with no errors

## Task 5. Implement and test withdraw functionality

- [x] Implement `withdraw` method:
  ```rust
  pub async fn withdraw(
      &self,
      token: Address,
      vault_id: VaultId,
      target_amount: U256,
  ) -> Result<TxHash, VaultError>
  ```
  - Validate target_amount is non-zero
  - Call `IOrderBookV4::withdraw2` with empty tasks array
  - Get transaction receipt before returning
- [x] Write integration test: `test_withdraw_succeeds_with_deployed_contract`
  - Use LocalEvm to set up test environment
  - Deposit tokens first to create vault balance
  - Call withdraw via VaultService
  - Verify transaction succeeds
  - Query vault balance and verify it decreased correctly
- [x] Write unit test: `withdraw_rejects_zero_amount`
  - Assert `VaultError::ZeroAmount` without contract call
- [x] Run `cargo test -q` and `cargo clippy`

### Completed Changes

Implemented withdraw functionality with comprehensive tests:

- Implemented `withdraw` method in `VaultService`:
  - Validates target_amount is non-zero
  - Calls `IOrderBookV4::withdraw2` with empty tasks array
  - Returns transaction hash after getting receipt
- Wrote `withdraw_rejects_zero_amount` test:
  - Verifies `VaultError::ZeroAmount` is returned for zero amount
- Wrote `test_withdraw_succeeds_with_deployed_contract` integration test:
  - Deposits 1000 tokens to vault first
  - Withdraws 500 tokens via VaultService
  - Verifies vault balance decreased from 1000 to 500 tokens
  - Verifies transaction hash is non-zero
- All 4 vault tests pass (2 deposit + 2 withdraw)
- Clippy passes with no errors

## Task 6. Add and test USDC convenience methods

- [ ] Define constant:
      `USDC_BASE: Address = address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")`
- [ ] Implement `deposit_usdc`:
  ```rust
  pub async fn deposit_usdc(
      &self,
      vault_id: VaultId,
      amount: U256,
  ) -> Result<TxHash, VaultError>
  ```
  - Delegates to `self.deposit(USDC_BASE, vault_id, amount)`
- [ ] Implement `withdraw_usdc`:
  ```rust
  pub async fn withdraw_usdc(
      &self,
      vault_id: VaultId,
      target_amount: U256,
  ) -> Result<TxHash, VaultError>
  ```
  - Delegates to `self.withdraw(USDC_BASE, vault_id, target_amount)`
- [ ] Write test: `deposit_usdc_uses_correct_token_address`
  - Verify USDC_BASE address passed to deposit
- [ ] Write test: `withdraw_usdc_uses_correct_token_address`
  - Verify USDC_BASE address passed to withdraw
- [ ] Run `cargo test -q` and `cargo clippy`

## Task 5. Add documentation

- [ ] Add module-level doc comment:
  - Purpose: vault deposit/withdraw for Rain OrderBook on Base
  - Contract functions: `deposit2`, `withdraw2`
  - Primary use: USDC vault management for inventory rebalancing
- [ ] Add doc comments to public types:
  - `VaultId`: Vault identifier newtype
  - `VaultService`: Usage example showing deposit/withdraw
  - `deposit`: Parameters and error conditions
  - `withdraw`: Parameters and target amount semantics
  - `deposit_usdc`/`withdraw_usdc`: USDC-specific convenience methods
- [ ] Add inline comments only where non-obvious:
  - Empty tasks array for MVP scope
  - USDC address for Base network
  - Zero amount validation required by contract
- [ ] Run `cargo fmt`
- [ ] Final `cargo build`, `cargo test -q`,
      `cargo clippy --all-targets --all-features -- -D clippy::all`
