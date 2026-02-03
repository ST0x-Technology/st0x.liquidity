//! Redeemer trait for equity redemption operations.

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use thiserror::Error;

use crate::alpaca_tokenization::AlpacaTokenizationError;
use crate::onchain::vault::VaultError;

/// Error type for Redeemer service operations.
#[derive(Debug, Error)]
pub(crate) enum RedeemError {
    #[error("Token {0} not found in vault registry")]
    VaultNotFound(Address),
    #[error("Vault withdraw failed: {0}")]
    VaultWithdraw(#[from] VaultError),
    #[error("Send for redemption failed: {0}")]
    SendForRedemption(#[from] AlpacaTokenizationError),
}

/// Abstraction for redemption operations, injected via cqrs-es Services.
///
/// Implementations handle vault withdrawal and token transfer to Alpaca.
#[async_trait]
pub(crate) trait Redeemer: Send + Sync {
    /// Withdraws tokens from the Rain OrderBook vault.
    async fn withdraw_from_vault(
        &self,
        token: Address,
        amount: U256,
    ) -> Result<TxHash, RedeemError>;

    /// Sends tokens to Alpaca's redemption wallet.
    async fn send_for_redemption(
        &self,
        token: Address,
        amount: U256,
    ) -> Result<(Address, TxHash), RedeemError>;
}
