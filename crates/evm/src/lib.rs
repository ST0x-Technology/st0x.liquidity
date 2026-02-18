//! EVM chain interaction abstraction.
//!
//! This crate provides two traits for interacting with EVM chains:
//!
//! - [`Evm`] — read-only chain access with error-decoded view calls.
//!   Provides the underlying provider and a `call` method that
//!   automatically decodes Solidity revert data via the OpenChain
//!   selector registry.
//!
//! - [`Wallet`] — extends `Evm` with a signing identity and
//!   transaction submission. Implementations handle key management
//!   and signing (Fireblocks MPC in production,
//!   `RawPrivateKeyWallet` in tests).
//!
//! Error decoding is built into both `Evm::call` (view calls) and
//! `Wallet::send` (write transactions), so consumers get
//! human-readable revert reasons without manual wiring.

use std::sync::Arc;

use alloy::primitives::{Address, Bytes};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionReceipt;
use async_trait::async_trait;
#[cfg(feature = "fireblocks")]
use fireblocks_sdk::apis::transactions_api::CreateTransactionError;
use rain_error_decoding::AbiDecodedErrorType;

pub mod error_decoding;

#[cfg(feature = "fireblocks")]
pub mod fireblocks;

#[cfg(feature = "local-signer")]
pub mod local;

/// Errors that can occur during EVM operations.
#[derive(Debug, thiserror::Error)]
pub enum EvmError {
    #[error("transaction error: {0}")]
    Transaction(#[from] alloy::providers::PendingTransactionError),
    #[error("transport error: {0}")]
    Transport(#[from] alloy::transports::RpcError<alloy::transports::TransportErrorKind>),
    #[error("contract error: {0}")]
    Contract(#[from] alloy::contract::Error),
    #[error("decoded contract error: {0}")]
    DecodedRevert(AbiDecodedErrorType),
    #[error("transaction reverted: {tx_hash}")]
    Reverted { tx_hash: alloy::primitives::TxHash },
    #[error("invalid private key: {0}")]
    InvalidPrivateKey(#[from] alloy::signers::k256::ecdsa::Error),
    #[cfg(feature = "fireblocks")]
    #[error("Fireblocks error: {0}")]
    Fireblocks(#[from] fireblocks::FireblocksError),
}

#[cfg(feature = "fireblocks")]
impl From<fireblocks_sdk::FireblocksError> for EvmError {
    fn from(error: fireblocks_sdk::FireblocksError) -> Self {
        Self::Fireblocks(fireblocks::FireblocksError::from(error))
    }
}

#[cfg(feature = "fireblocks")]
impl From<fireblocks_sdk::apis::Error<CreateTransactionError>> for EvmError {
    fn from(error: fireblocks_sdk::apis::Error<CreateTransactionError>) -> Self {
        Self::Fireblocks(fireblocks::FireblocksError::from(error))
    }
}

#[cfg(feature = "fireblocks")]
impl From<alloy::hex::FromHexError> for EvmError {
    fn from(error: alloy::hex::FromHexError) -> Self {
        Self::Fireblocks(fireblocks::FireblocksError::from(error))
    }
}

/// Read-only EVM chain access with error-decoded view calls.
///
/// Provides the underlying provider for direct chain queries (balance
/// checks, block subscriptions, etc.) and a [`call`](Evm::call) method
/// that executes `eth_call` with automatic Solidity revert decoding.
///
/// Implementations only need to supply the provider — `call` has a
/// default implementation that handles error decoding.
#[async_trait]
pub trait Evm: Send + Sync + 'static {
    /// The provider type used for chain access.
    type Provider: Provider + Clone + Send + Sync;

    /// Returns the underlying provider for direct chain queries.
    fn provider(&self) -> &Self::Provider;

    /// Execute a view call with automatic revert decoding.
    ///
    /// Runs `eth_call` against the given contract and calldata. On
    /// revert, attempts to decode the Solidity error via the OpenChain
    /// selector registry before returning.
    async fn call(&self, contract: Address, calldata: Bytes) -> Result<Bytes, EvmError> {
        let tx = alloy::rpc::types::TransactionRequest::default()
            .to(contract)
            .input(calldata.into());

        match self.provider().call(tx).await {
            Ok(result) => Ok(result),
            Err(rpc_err) => {
                // Wrap in alloy::contract::Error to reuse its revert data extraction
                let contract_err = alloy::contract::Error::TransportError(rpc_err);

                if let Some(revert_data) = contract_err.as_revert_data() {
                    if let Ok(decoded) = AbiDecodedErrorType::selector_registry_abi_decode(
                        revert_data.as_ref(),
                        None,
                    )
                    .await
                    {
                        return Err(EvmError::DecodedRevert(decoded));
                    }
                }

                Err(EvmError::Contract(contract_err))
            }
        }
    }
}

/// Signing wallet on an EVM chain.
///
/// Extends [`Evm`] with a wallet identity (address) and transaction
/// submission. The `send` method submits a signed transaction and
/// waits for a receipt. Implementations handle key management:
/// [`FireblocksWallet`](fireblocks::FireblocksWallet) uses MPC-based
/// signing via the Fireblocks API, while
/// [`RawPrivateKeyWallet`](local::RawPrivateKeyWallet) signs locally
/// (test-only).
#[async_trait]
pub trait Wallet: Evm {
    /// Returns the address this wallet signs transactions from.
    fn address(&self) -> Address;

    /// Submit a signed contract call transaction.
    ///
    /// - `contract` — target contract address
    /// - `calldata` — ABI-encoded function call
    /// - `note` — human-readable operation description (used for
    ///   logging and, in `FireblocksWallet`, the Fireblocks dashboard)
    async fn send(
        &self,
        contract: Address,
        calldata: Bytes,
        note: &str,
    ) -> Result<TransactionReceipt, EvmError>;
}

#[async_trait]
impl<T: Evm> Evm for Arc<T> {
    type Provider = T::Provider;

    fn provider(&self) -> &Self::Provider {
        (**self).provider()
    }

    async fn call(&self, contract: Address, calldata: Bytes) -> Result<Bytes, EvmError> {
        (**self).call(contract, calldata).await
    }
}

#[async_trait]
impl<T: Wallet> Wallet for Arc<T> {
    fn address(&self) -> Address {
        (**self).address()
    }

    async fn send(
        &self,
        contract: Address,
        calldata: Bytes,
        note: &str,
    ) -> Result<TransactionReceipt, EvmError> {
        (**self).send(contract, calldata, note).await
    }
}
