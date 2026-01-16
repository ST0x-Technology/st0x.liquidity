//! Cross-chain bridge abstractions for USDC transfers.
//!
//! This crate provides the `Bridge` trait and implementations for cross-chain USDC transfers.
//!
//! ## Feature Flags
//!
//! - `cctp` - Enables Circle CCTP bridge implementation for Ethereum <-> Base transfers

use alloy::primitives::{Address, Bytes, FixedBytes, TxHash, U256};
use async_trait::async_trait;

#[cfg(feature = "cctp")]
mod cctp;

#[cfg(feature = "cctp")]
pub use cctp::{
    AttestationError, CctpBridge, CctpError, Evm, MESSAGE_TRANSMITTER_V2, TOKEN_MESSENGER_V2,
    USDC_BASE, USDC_ETHEREUM,
};

/// Trait for cross-chain USDC bridge operations.
///
/// Implementations handle the full lifecycle of cross-chain transfers:
/// burn on source -> attestation -> mint on destination.
#[async_trait]
pub trait Bridge: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Burns USDC on Ethereum for transfer to Base.
    async fn burn_on_ethereum(
        &self,
        amount: U256,
        recipient: Address,
    ) -> Result<BurnReceipt, Self::Error>;

    /// Burns USDC on Base for transfer to Ethereum.
    async fn burn_on_base(
        &self,
        amount: U256,
        recipient: Address,
    ) -> Result<BurnReceipt, Self::Error>;

    /// Polls for attestation until ready.
    async fn poll_attestation(&self, hash: FixedBytes<32>) -> Result<Bytes, Self::Error>;

    /// Mints USDC on Base after receiving attestation.
    async fn mint_on_base(&self, message: Bytes, attestation: Bytes)
    -> Result<TxHash, Self::Error>;

    /// Mints USDC on Ethereum after receiving attestation.
    async fn mint_on_ethereum(
        &self,
        message: Bytes,
        attestation: Bytes,
    ) -> Result<TxHash, Self::Error>;
}

/// Receipt from burning USDC on the source chain.
///
/// Contains all information needed to poll for attestation and mint on the destination chain.
#[derive(Debug)]
pub struct BurnReceipt {
    /// Transaction hash of the burn transaction
    pub tx: TxHash,
    /// Bridge-specific message nonce
    pub nonce: FixedBytes<32>,
    /// Hash of the bridge message (used for attestation polling)
    pub hash: FixedBytes<32>,
    /// Full bridge message bytes (passed to receive/mint on destination)
    pub message: Bytes,
    /// Amount of USDC burned (in smallest unit, 6 decimals for USDC)
    pub amount: U256,
}
