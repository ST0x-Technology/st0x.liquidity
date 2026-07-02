//! Tokenization abstraction for converting between offchain shares and onchain
//! tokens.
//!
//! This crate provides the [`Tokenizer`] trait that abstracts tokenization
//! operations, allowing different implementations (Alpaca, mock, etc.) to be
//! used interchangeably.

mod alpaca;
mod bindings;

#[cfg(feature = "mock")]
pub mod mock_api;

#[cfg(any(test, feature = "test-support"))]
pub mod mock;

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;
use uuid::Uuid;

use st0x_evm::EvmError;
use st0x_execution::{FractionalShares, Symbol};

pub use alpaca::{
    AlpacaTokenizationError, AlpacaTokenizationService, TokenizationRequest,
    TokenizationRequestStatus, TokenizationRequestType,
};

/// Our internal tracking id for a tokenized equity mint, chosen at enqueue time.
///
/// A UUID so invalid ids are unrepresentable and apalis/CLI retries always
/// target the same aggregate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct IssuerRequestId(pub Uuid);

impl IssuerRequestId {
    pub fn generate() -> Self {
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
#[cfg(any(test, feature = "test-support"))]
pub fn issuer_request_id(label: &str) -> IssuerRequestId {
    IssuerRequestId(Uuid::new_v5(&Uuid::NAMESPACE_OID, label.as_bytes()))
}

/// Alpaca tokenization request identifier used to track the mint operation through their API.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct TokenizationRequestId(String);

/// Error parsing a [`TokenizationRequestId`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TokenizationRequestIdError {
    #[error("tokenization request id must be non-empty")]
    Empty,
}

impl TokenizationRequestId {
    /// Parses a provider-issued tokenization request id.
    pub fn try_new(value: impl AsRef<str>) -> Result<Self, TokenizationRequestIdError> {
        let value = value.as_ref();
        if value.is_empty() {
            return Err(TokenizationRequestIdError::Empty);
        }
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for TokenizationRequestId {
    type Error = TokenizationRequestIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl FromStr for TokenizationRequestId {
    type Err = TokenizationRequestIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_new(value)
    }
}

impl std::fmt::Display for TokenizationRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for TokenizationRequestId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for TokenizationRequestId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_new(value).map_err(serde::de::Error::custom)
    }
}

/// Deterministic tokenization request id for tests.
#[cfg(any(test, feature = "test-support"))]
pub fn tokenization_request_id(label: &str) -> TokenizationRequestId {
    TokenizationRequestId::try_new(label)
        .unwrap_or_else(|_| unreachable!("test tokenization request id must be non-empty"))
}

/// Error type for Tokenizer operations.
#[derive(Debug, thiserror::Error)]
pub enum TokenizerError {
    #[error(transparent)]
    Alpaca(#[from] AlpacaTokenizationError),
    #[error(transparent)]
    MintVerification(#[from] MintVerificationError),
}

/// Errors from verifying a mint transaction onchain.
#[derive(Debug, thiserror::Error)]
pub enum MintVerificationError {
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
pub trait Tokenizer: Send + Sync {
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

#[cfg(test)]
mod tests {
    use super::{TokenizationRequestId, TokenizationRequestIdError};

    #[test]
    fn tokenization_request_id_rejects_empty_string() {
        let error = TokenizationRequestId::try_new("").unwrap_err();
        assert_eq!(error, TokenizationRequestIdError::Empty);
    }

    #[test]
    fn tokenization_request_id_from_str_parses_non_empty_value() {
        let request_id = "tok-req-123".parse::<TokenizationRequestId>().unwrap();
        assert_eq!(request_id.as_ref(), "tok-req-123");
    }

    #[test]
    fn tokenization_request_id_deserialize_rejects_empty_string() {
        let error = serde_json::from_str::<TokenizationRequestId>("\"\"")
            .expect_err("empty tokenization request id must fail deserialization");
        assert!(
            error
                .to_string()
                .contains("tokenization request id must be non-empty")
        );
    }

    #[test]
    fn tokenization_request_id_deserialize_accepts_non_empty_value() {
        let request_id: TokenizationRequestId = serde_json::from_str("\"tok-req-456\"").unwrap();
        assert_eq!(request_id.as_ref(), "tok-req-456");
    }
}
