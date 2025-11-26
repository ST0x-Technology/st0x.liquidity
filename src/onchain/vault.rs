//! Rain OrderBook V4 vault operations on Base.
//!
//! This module provides a service layer for depositing and withdrawing tokens to/from
//! Rain OrderBook vaults using the `deposit2` and `withdraw2` contract functions.
//!
//! The primary use case is USDC vault management for inventory rebalancing in the
//! market making system.

use alloy::primitives::{Address, TxHash, U256, address};
use alloy::providers::Provider;
use alloy::signers::Signer;

use crate::bindings::IOrderBookV4;
use crate::cctp::EvmAccount;

const USDC_BASE: Address = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");

/// Vault identifier for Rain OrderBook vaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VaultId(pub(crate) U256);

#[derive(Debug, thiserror::Error)]
pub(crate) enum VaultError {
    #[error("Transaction error: {0}")]
    Transaction(#[from] alloy::providers::PendingTransactionError),
    #[error("Contract error: {0}")]
    Contract(#[from] alloy::contract::Error),
    #[error("Insufficient balance: requested {requested}, available {available}")]
    InsufficientBalance { requested: U256, available: U256 },
    #[error("Amount cannot be zero")]
    ZeroAmount,
}

/// Service for managing Rain OrderBook vault operations.
///
/// # Example
///
/// ```ignore
/// let account = EvmAccount::new(provider, signer);
/// let service = VaultService::new(account, orderbook_address);
///
/// // Deposit USDC to vault
/// let vault_id = VaultId(U256::from(1));
/// let amount = U256::from(1000) * U256::from(10).pow(U256::from(6)); // 1000 USDC
/// service.deposit_usdc(vault_id, amount).await?;
///
/// // Withdraw USDC from vault
/// service.withdraw_usdc(vault_id, amount).await?;
/// ```
pub(crate) struct VaultService<P, S>
where
    P: Provider + Clone,
    S: Signer + Clone + Sync,
{
    account: EvmAccount<P, S>,
    orderbook: Address,
}

impl<P, S> VaultService<P, S>
where
    P: Provider + Clone,
    S: Signer + Clone + Sync,
{
    pub(crate) fn new(account: EvmAccount<P, S>, orderbook: Address) -> Self {
        Self { account, orderbook }
    }

    /// Deposits tokens to a Rain OrderBook vault.
    ///
    /// # Parameters
    ///
    /// * `token` - ERC20 token address to deposit
    /// * `vault_id` - Target vault identifier
    /// * `amount` - Amount of tokens to deposit (in token's base units)
    ///
    /// # Errors
    ///
    /// Returns `VaultError::ZeroAmount` if amount is zero.
    /// Returns `VaultError::Transaction` or `VaultError::Contract` for blockchain errors.
    pub(crate) async fn deposit(
        &self,
        token: Address,
        vault_id: VaultId,
        amount: U256,
    ) -> Result<TxHash, VaultError> {
        if amount.is_zero() {
            return Err(VaultError::ZeroAmount);
        }

        let contract = IOrderBookV4::new(self.orderbook, &self.account.provider);

        let tasks = Vec::new();

        let receipt = contract
            .deposit2(token, vault_id.0, amount, tasks)
            .send()
            .await?
            .get_receipt()
            .await?;

        Ok(receipt.transaction_hash)
    }

    /// Withdraws tokens from a Rain OrderBook vault.
    ///
    /// # Parameters
    ///
    /// * `token` - ERC20 token address to withdraw
    /// * `vault_id` - Source vault identifier
    /// * `target_amount` - Target amount of tokens to withdraw (in token's base units)
    ///
    /// # Errors
    ///
    /// Returns `VaultError::ZeroAmount` if target_amount is zero.
    /// Returns `VaultError::Transaction` or `VaultError::Contract` for blockchain errors.
    pub(crate) async fn withdraw(
        &self,
        token: Address,
        vault_id: VaultId,
        target_amount: U256,
    ) -> Result<TxHash, VaultError> {
        if target_amount.is_zero() {
            return Err(VaultError::ZeroAmount);
        }

        let contract = IOrderBookV4::new(self.orderbook, &self.account.provider);

        let tasks = Vec::new();

        let receipt = contract
            .withdraw2(token, vault_id.0, target_amount, tasks)
            .send()
            .await?
            .get_receipt()
            .await?;

        Ok(receipt.transaction_hash)
    }

    /// Deposits USDC to a Rain OrderBook vault on Base.
    ///
    /// Convenience method that calls `deposit` with the Base USDC address.
    ///
    /// # Parameters
    ///
    /// * `vault_id` - Target vault identifier
    /// * `amount` - Amount of USDC to deposit (in USDC's base units, 6 decimals)
    pub(crate) async fn deposit_usdc(
        &self,
        vault_id: VaultId,
        amount: U256,
    ) -> Result<TxHash, VaultError> {
        self.deposit(USDC_BASE, vault_id, amount).await
    }

    /// Withdraws USDC from a Rain OrderBook vault on Base.
    ///
    /// Convenience method that calls `withdraw` with the Base USDC address.
    ///
    /// # Parameters
    ///
    /// * `vault_id` - Source vault identifier
    /// * `target_amount` - Target amount of USDC to withdraw (in USDC's base units, 6 decimals)
    pub(crate) async fn withdraw_usdc(
        &self,
        vault_id: VaultId,
        target_amount: U256,
    ) -> Result<TxHash, VaultError> {
        self.withdraw(USDC_BASE, vault_id, target_amount).await
    }
}

#[cfg(test)]
mod test_utils;

#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::LocalEvm;

    const TEST_VAULT_ID: u64 = 1;

    #[tokio::test]
    async fn deposit_rejects_zero_amount() {
        let local_evm = LocalEvm::new().await.unwrap();

        let account = EvmAccount::new(local_evm.provider.clone(), local_evm.signer.clone());
        let service = VaultService::new(account, local_evm.orderbook_address);

        let result = service
            .deposit(
                local_evm.token_address,
                VaultId(U256::from(TEST_VAULT_ID)),
                U256::ZERO,
            )
            .await;

        assert!(matches!(result.unwrap_err(), VaultError::ZeroAmount));
    }

    #[tokio::test]
    async fn test_deposit_succeeds_with_deployed_contract() {
        let local_evm = LocalEvm::new().await.unwrap();

        let deposit_amount = U256::from(1000) * U256::from(10).pow(U256::from(18));
        let vault_id = VaultId(U256::from(TEST_VAULT_ID));

        local_evm
            .approve_tokens(
                local_evm.token_address,
                local_evm.orderbook_address,
                deposit_amount,
            )
            .await
            .unwrap();

        let account = EvmAccount::new(local_evm.provider.clone(), local_evm.signer.clone());
        let service = VaultService::new(account, local_evm.orderbook_address);

        let vault_balance_before = local_evm
            .get_vault_balance(local_evm.token_address, vault_id.0)
            .await
            .unwrap();

        assert_eq!(vault_balance_before, U256::ZERO);

        let tx_hash = service
            .deposit(local_evm.token_address, vault_id, deposit_amount)
            .await
            .unwrap();

        assert!(!tx_hash.is_zero());

        let vault_balance_after = local_evm
            .get_vault_balance(local_evm.token_address, vault_id.0)
            .await
            .unwrap();

        assert_eq!(vault_balance_after, deposit_amount);
    }

    #[tokio::test]
    async fn withdraw_rejects_zero_amount() {
        let local_evm = LocalEvm::new().await.unwrap();

        let account = EvmAccount::new(local_evm.provider.clone(), local_evm.signer.clone());
        let service = VaultService::new(account, local_evm.orderbook_address);

        let result = service
            .withdraw(
                local_evm.token_address,
                VaultId(U256::from(TEST_VAULT_ID)),
                U256::ZERO,
            )
            .await;

        assert!(matches!(result.unwrap_err(), VaultError::ZeroAmount));
    }

    #[tokio::test]
    async fn test_withdraw_succeeds_with_deployed_contract() {
        let local_evm = LocalEvm::new().await.unwrap();

        let deposit_amount = U256::from(1000) * U256::from(10).pow(U256::from(18));
        let withdraw_amount = U256::from(500) * U256::from(10).pow(U256::from(18));
        let vault_id = VaultId(U256::from(TEST_VAULT_ID));

        local_evm
            .approve_tokens(
                local_evm.token_address,
                local_evm.orderbook_address,
                deposit_amount,
            )
            .await
            .unwrap();

        let account = EvmAccount::new(local_evm.provider.clone(), local_evm.signer.clone());
        let service = VaultService::new(account, local_evm.orderbook_address);

        service
            .deposit(local_evm.token_address, vault_id, deposit_amount)
            .await
            .unwrap();

        let vault_balance_before = local_evm
            .get_vault_balance(local_evm.token_address, vault_id.0)
            .await
            .unwrap();

        assert_eq!(vault_balance_before, deposit_amount);

        let tx_hash = service
            .withdraw(local_evm.token_address, vault_id, withdraw_amount)
            .await
            .unwrap();

        assert!(!tx_hash.is_zero());

        let vault_balance_after = local_evm
            .get_vault_balance(local_evm.token_address, vault_id.0)
            .await
            .unwrap();

        assert_eq!(vault_balance_after, deposit_amount - withdraw_amount);
    }

    #[test]
    fn usdc_base_address_is_correct() {
        assert_eq!(
            USDC_BASE,
            address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")
        );
    }
}
