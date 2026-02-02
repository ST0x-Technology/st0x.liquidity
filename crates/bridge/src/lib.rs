//! Bridge abstraction for cross-chain USDC transfers.
//!
//! This crate provides a generic `Bridge` trait for bridging USDC between chains.
//! The default (no features) build ships only the trait and shared domain types.
//! Enable the `cctp` feature for the Circle CCTP V2 implementation.

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;

#[cfg(feature = "cctp")]
pub mod cctp;
#[cfg(feature = "cctp")]
mod error_decoding;

/// Direction of a bridge transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeDirection {
    /// Bridge USDC from Ethereum to Base
    EthereumToBase,
    /// Bridge USDC from Base to Ethereum
    BaseToEthereum,
}

/// Receipt from burning USDC on the source chain.
#[derive(Debug)]
pub struct BurnReceipt {
    /// Transaction hash of the burn transaction
    pub tx: TxHash,
    /// Amount of USDC burned (in smallest unit, 6 decimals for USDC).
    /// This is the INPUT amount sent to the contract, NOT the amount received
    /// on the destination chain (which is amount minus fee).
    pub amount: U256,
}

/// Receipt from minting USDC on the destination chain.
#[derive(Debug)]
pub struct MintReceipt {
    /// Transaction hash of the mint transaction
    pub tx: TxHash,
    /// Actual USDC minted to recipient (NET of fees).
    pub amount: U256,
    /// Actual fee collected for this transfer.
    pub fee: U256,
}

/// Attestation data required to mint on the destination chain.
pub trait Attestation: Send + Sync {
    /// Returns the nonce for this attestation.
    fn nonce(&self) -> u64;

    /// Returns the raw attestation bytes.
    fn as_bytes(&self) -> &[u8];
}

/// Generic bridge trait for cross-chain USDC transfers.
///
/// Implementations handle the three-step flow: burn -> poll attestation -> mint.
#[async_trait]
pub trait Bridge: Send + Sync + 'static {
    /// Error type for bridge operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Attestation type returned by polling.
    type Attestation: Attestation;

    /// Burns USDC on the source chain.
    async fn burn(
        &self,
        direction: BridgeDirection,
        amount: U256,
        recipient: Address,
    ) -> Result<BurnReceipt, Self::Error>;

    /// Polls for an attestation confirming the burn.
    async fn poll_attestation(
        &self,
        direction: BridgeDirection,
        burn_tx: TxHash,
    ) -> Result<Self::Attestation, Self::Error>;

    /// Mints USDC on the destination chain using the attestation.
    async fn mint(
        &self,
        direction: BridgeDirection,
        attestation: &Self::Attestation,
    ) -> Result<MintReceipt, Self::Error>;
}
