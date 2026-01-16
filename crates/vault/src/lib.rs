//! Vault operations for token deposit and withdrawal.
//!
//! This crate provides the `Vault` trait and implementations for depositing and withdrawing tokens.
//!
//! ## Feature Flags
//!
//! - `rain` - Enables Rain OrderBook V5 vault implementation for Base

use alloy::primitives::{Address, B256, TxHash, U256};
use async_trait::async_trait;

#[cfg(feature = "rain")]
mod rain;

#[cfg(feature = "rain")]
pub use rain::{VaultError, VaultService};

/// Trait for vault deposit and withdrawal operations.
#[async_trait]
pub trait Vault: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Deposits tokens to a vault.
    async fn deposit(
        &self,
        token: Address,
        vault_id: VaultId,
        amount: U256,
        decimals: u8,
    ) -> Result<TxHash, Self::Error>;

    /// Withdraws tokens from a vault.
    async fn withdraw(
        &self,
        token: Address,
        vault_id: VaultId,
        target_amount: U256,
        decimals: u8,
    ) -> Result<TxHash, Self::Error>;

    /// Deposits USDC to a vault (convenience method).
    async fn deposit_usdc(&self, vault_id: VaultId, amount: U256) -> Result<TxHash, Self::Error>;

    /// Withdraws USDC from a vault (convenience method).
    async fn withdraw_usdc(
        &self,
        vault_id: VaultId,
        target_amount: U256,
    ) -> Result<TxHash, Self::Error>;
}

/// Vault identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VaultId(pub B256);
