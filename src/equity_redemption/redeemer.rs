//! Redeemer trait for equity redemption operations.

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Error type for Redeemer service operations.
///
/// Must be serializable because it's embedded in `EquityRedemptionError`
/// which is persisted as part of the aggregate's lifecycle error state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub(crate) enum RedeemError {
    #[error("Token {0} not found in vault registry")]
    VaultNotFound(Address),
    #[error("Vault withdraw failed")]
    VaultWithdraw,
    #[error("Send for redemption failed")]
    SendForRedemption,
    #[error("Transaction failed: {tx_hash}")]
    Transaction { tx_hash: TxHash },
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
