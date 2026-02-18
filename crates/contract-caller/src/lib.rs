//! Contract call submission abstraction.
//!
//! This crate provides a `ContractCaller` trait that abstracts how onchain
//! contract calls are submitted. Production uses `FireblocksCaller` for
//! MPC-based key management. `LocalCaller` exists for test code only.

use alloy::primitives::{Address, Bytes};
use alloy::rpc::types::TransactionReceipt;
use async_trait::async_trait;
#[cfg(feature = "fireblocks")]
use fireblocks_sdk::apis::transactions_api::CreateTransactionError;

pub mod error_decoding;

#[cfg(feature = "fireblocks")]
pub mod fireblocks;

#[cfg(feature = "local-signer")]
pub mod local;

/// Errors that can occur when submitting a contract call.
#[derive(Debug, thiserror::Error)]
pub enum ContractCallError {
    #[error("transaction error: {0}")]
    Transaction(#[from] alloy::providers::PendingTransactionError),
    #[error("transport error: {0}")]
    Transport(#[from] alloy::transports::RpcError<alloy::transports::TransportErrorKind>),
    #[error("transaction reverted: {tx_hash}")]
    Reverted { tx_hash: alloy::primitives::TxHash },
    #[cfg(feature = "fireblocks")]
    #[error("Fireblocks error: {0}")]
    Fireblocks(#[from] fireblocks::FireblocksError),
}

#[cfg(feature = "fireblocks")]
impl From<fireblocks_sdk::FireblocksError> for ContractCallError {
    fn from(error: fireblocks_sdk::FireblocksError) -> Self {
        Self::Fireblocks(fireblocks::FireblocksError::from(error))
    }
}

#[cfg(feature = "fireblocks")]
impl From<fireblocks_sdk::apis::Error<CreateTransactionError>> for ContractCallError {
    fn from(error: fireblocks_sdk::apis::Error<CreateTransactionError>) -> Self {
        Self::Fireblocks(fireblocks::FireblocksError::from(error))
    }
}

#[cfg(feature = "fireblocks")]
impl From<alloy::hex::FromHexError> for ContractCallError {
    fn from(error: alloy::hex::FromHexError) -> Self {
        Self::Fireblocks(fireblocks::FireblocksError::from(error))
    }
}

/// Abstraction for submitting contract calls to the blockchain.
///
/// Implementations handle signing and submission. Consumers build calldata
/// and pass it to `call_contract` without knowing how the transaction is
/// signed or submitted.
#[async_trait]
pub trait ContractCaller: Send + Sync {
    /// Submit a contract call transaction.
    ///
    /// - `contract` -- target contract address
    /// - `calldata` -- ABI-encoded function call
    /// - `note` -- human-readable operation description (used for logging
    ///   and, in `FireblocksCaller`, for the Fireblocks dashboard)
    async fn call_contract(
        &self,
        contract: Address,
        calldata: Bytes,
        note: &str,
    ) -> Result<TransactionReceipt, ContractCallError>;
}
