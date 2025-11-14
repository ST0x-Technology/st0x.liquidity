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

## Task 2. Implement and test deposit functionality

- [ ] Implement `deposit` method:
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
- [ ] Create `#[cfg(test)] mod tests` module
- [ ] Write test: `deposit_succeeds_with_valid_parameters`
  - Mock provider, signer, and successful transaction
  - Verify correct contract parameters
- [ ] Write test: `deposit_rejects_zero_amount`
  - Assert `VaultError::ZeroAmount` returned
  - Verify no contract call made
- [ ] Write test: `deposit_propagates_contract_error`
  - Mock contract error, verify wrapped correctly
- [ ] Run `cargo test -q` and `cargo clippy`

## Task 3. Implement and test withdraw functionality

- [ ] Implement `withdraw` method:
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
- [ ] Write test: `withdraw_succeeds_with_valid_parameters`
  - Mock successful withdrawal transaction
  - Verify correct contract parameters
- [ ] Write test: `withdraw_rejects_zero_amount`
  - Assert `VaultError::ZeroAmount` returned
- [ ] Write test: `withdraw_propagates_transaction_error`
  - Mock transaction failure, verify wrapped correctly
- [ ] Run `cargo test -q` and `cargo clippy`

## Task 4. Add and test USDC convenience methods

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
