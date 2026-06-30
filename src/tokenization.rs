//! Tokenization abstraction for converting between offchain shares and onchain tokens.
//!
//! This module provides the `Tokenizer` trait that abstracts tokenization operations,
//! allowing different implementations (Alpaca, mock, etc.) to be used interchangeably.

pub(crate) mod alpaca;
#[cfg(feature = "mock")]
pub mod mock_api;

#[cfg(test)]
pub(crate) mod mock;

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;
use uuid::Uuid;

use st0x_evm::EvmError;
use st0x_execution::{FractionalShares, Symbol};

pub(crate) use alpaca::{
    AlpacaTokenizationError, AlpacaTokenizationService, TokenizationRequest,
    TokenizationRequestStatus, TokenizationRequestType,
};

/// Our internal tracking id for a tokenized equity mint, chosen at enqueue time.
///
/// Mirrors [`crate::usdc_rebalance::UsdcRebalanceId`]: a UUID so invalid ids are
/// unrepresentable and apalis/CLI retries always target the same aggregate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct IssuerRequestId(pub(crate) Uuid);

impl IssuerRequestId {
    pub(crate) fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Display for IssuerRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for IssuerRequestId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(value)?))
    }
}

/// Deterministic issuer request id for tests. Maps a human-readable label to a
/// UUID v5 so test aggregate ids stay valid [`IssuerRequestId`] values.
#[cfg(test)]
pub(crate) fn issuer_request_id(label: &str) -> IssuerRequestId {
    IssuerRequestId(Uuid::new_v5(&Uuid::NAMESPACE_OID, label.as_bytes()))
}

/// Alpaca tokenization request identifier used to track the mint operation through their API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct TokenizationRequestId(pub(crate) String);

impl std::fmt::Display for TokenizationRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Error type for Tokenizer operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TokenizerError {
    #[error(transparent)]
    Alpaca(#[from] AlpacaTokenizationError),
    #[error(transparent)]
    MintVerification(#[from] MintVerificationError),
}

/// Errors from verifying a mint transaction onchain.
#[derive(Debug, thiserror::Error)]
pub(crate) enum MintVerificationError {
    #[error("Transaction receipt not found for {tx_hash}")]
    ReceiptNotFound { tx_hash: TxHash },
    #[error("Transaction {tx_hash} reverted")]
    TransactionReverted { tx_hash: TxHash },
    #[error(
        "No matching ERC20 Transfer event in tx {tx_hash} \
         to wallet {wallet} for token {token}"
    )]
    NoMatchingTransfer {
        tx_hash: TxHash,
        wallet: Address,
        token: Address,
    },
    #[error(
        "Transfer amount insufficient in tx {tx_hash}: \
         expected {expected}, found {actual}"
    )]
    InsufficientTransferAmount {
        tx_hash: TxHash,
        expected: U256,
        actual: U256,
    },
    #[error("Transfer amount overflow summing events in tx {tx_hash}")]
    TransferOverflow { tx_hash: TxHash },
    #[error("Provider error during mint verification: {0}")]
    Provider(#[from] alloy::transports::RpcError<alloy::transports::TransportErrorKind>),
}

/// Abstraction for equity tokenization operations.
///
/// Implementations handle tokenization API calls for:
/// - Minting: converting offchain shares to onchain tokens
/// - Redemption: converting onchain tokens back to offchain shares
#[async_trait]
pub(crate) trait Tokenizer: Send + Sync {
    /// Request a mint operation to convert offchain shares to onchain tokens.
    async fn request_mint(
        &self,
        symbol: Symbol,
        quantity: FractionalShares,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
    ) -> Result<TokenizationRequest, TokenizerError>;

    /// Poll a mint request until it reaches a terminal state.
    async fn poll_mint_until_complete(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError>;

    /// Get a single tokenization provider request by ID.
    async fn get_request(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError>;

    /// Returns the redemption wallet address where tokens should be sent,
    /// if configured.
    fn redemption_wallet(&self) -> Option<Address>;

    /// Wait for the tokenizer's RPC node to reach `block` before issuing the
    /// dependent redemption transfer. The wait must run on the same provider
    /// that performs the transfer (`send_for_redemption`), so it lives on the
    /// tokenizer rather than the wrapper -- a load-balanced wrapper provider
    /// catching up gives no guarantee about the tokenizer's provider.
    async fn wait_for_block(&self, block: u64) -> Result<(), EvmError>;

    /// Send tokens to the redemption wallet to initiate redemption.
    async fn send_for_redemption(
        &self,
        token: Address,
        amount: U256,
    ) -> Result<TxHash, TokenizerError>;

    /// Poll until the tokenization provider detects the redemption transfer.
    async fn poll_for_redemption(
        &self,
        tx_hash: &TxHash,
    ) -> Result<TokenizationRequest, TokenizerError>;

    /// Find a redemption request by its onchain token transfer transaction.
    async fn find_redemption_by_tx(
        &self,
        tx_hash: &TxHash,
    ) -> Result<Option<TokenizationRequest>, TokenizerError>;

    /// Poll a redemption request until it reaches a terminal state.
    async fn poll_redemption_until_complete(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError>;

    /// Verify that a mint transaction landed onchain.
    ///
    /// Checks that the transaction receipt exists and was not reverted,
    /// then parses Transfer event logs to confirm the expected tokens
    /// were transferred to the destination wallet.
    async fn verify_mint_tx(
        &self,
        tx_hash: TxHash,
        token_address: Address,
        wallet: Address,
        expected_amount: U256,
    ) -> Result<(), MintVerificationError>;

    /// List all pending tokenization requests from the external provider.
    ///
    /// Returns requests that are currently in-flight (status = pending),
    /// used by inventory polling to reconcile in-flight balances.
    async fn list_pending_requests(&self) -> Result<Vec<TokenizationRequest>, TokenizerError>;
}
