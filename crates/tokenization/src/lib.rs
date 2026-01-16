//! Tokenization operations for converting between offchain shares and onchain tokens.
//!
//! This crate provides the `Tokenizer` trait and implementations for mint and redemption operations.
//!
//! ## Feature Flags
//!
//! - `alpaca` - Enables Alpaca tokenization API implementation

use std::time::Duration;

use alloy::contract;
use alloy::primitives::{Address, TxHash};
use alloy::providers::PendingTransactionError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[cfg(feature = "alpaca")]
mod alpaca;

#[cfg(feature = "alpaca")]
pub use alpaca::AlpacaTokenizationService;

/// Trait for tokenization operations (minting and redemption).
#[async_trait]
pub trait Tokenizer: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Request a mint operation to convert offchain shares to onchain tokens.
    async fn request_mint(
        &self,
        underlying_symbol: &str,
        quantity: f64,
        wallet: Address,
    ) -> Result<TokenizationRequest, Self::Error>;

    /// Poll a mint request until it reaches a terminal state.
    async fn poll_mint_until_complete(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, Self::Error>;

    /// Returns the redemption wallet address.
    fn redemption_wallet(&self) -> Address;

    /// Poll until a redemption transfer is detected.
    async fn poll_for_redemption(
        &self,
        tx_hash: &TxHash,
    ) -> Result<TokenizationRequest, Self::Error>;

    /// Poll a redemption request until it reaches a terminal state.
    async fn poll_redemption_until_complete(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, Self::Error>;
}

/// A tokenization request returned by the API.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct TokenizationRequest {
    #[serde(rename = "tokenization_request_id")]
    pub id: TokenizationRequestId,
    r#type: TokenizationRequestType,
    pub status: TokenizationRequestStatus,
    pub underlying_symbol: String,
    pub token_symbol: Option<String>,
    #[serde(rename = "qty", deserialize_with = "deserialize_float")]
    pub quantity: f64,
    issuer: Issuer,
    network: Network,
    #[serde(rename = "wallet_address")]
    wallet: Address,
    pub issuer_request_id: Option<IssuerRequestId>,
    pub tx_hash: Option<TxHash>,
    fees: Option<Decimal>,
    created_at: DateTime<Utc>,
    updated_at: Option<DateTime<Utc>>,
}

/// Alpaca tokenization request identifier used to track the mint operation through their API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenizationRequestId(pub String);

impl std::fmt::Display for TokenizationRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Errors that can occur when interacting with tokenization APIs.
#[derive(Debug, Error)]
pub enum AlpacaTokenizationError {
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error("API error (status {status}): {message}")]
    ApiError { status: StatusCode, message: String },

    #[error("Insufficient position for symbol: {symbol}")]
    InsufficientPosition { symbol: String },

    #[error("Account not supported for tokenization")]
    UnsupportedAccount,

    #[error("Invalid parameters: {details}")]
    InvalidParameters { details: String },

    #[error("Request not found: {id}")]
    RequestNotFound { id: TokenizationRequestId },

    #[error("Redemption transfer failed: {0}")]
    RedemptionTransferFailed(#[from] contract::Error),

    #[error("Transaction error: {0}")]
    Transaction(#[from] PendingTransactionError),

    #[error("Poll timeout after {elapsed:?}")]
    PollTimeout { elapsed: Duration },
}

/// Alpaca issuer request identifier returned when a tokenization request is accepted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IssuerRequestId(pub String);

impl IssuerRequestId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

/// Status of a tokenization request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TokenizationRequestStatus {
    Pending,
    Completed,
    Rejected,
}

/// Configuration for polling operations.
#[derive(Debug, Clone)]
pub struct PollingConfig {
    pub interval: Duration,
    pub timeout: Duration,
    pub max_retries: usize,
    pub min_retry_delay: Duration,
    pub max_retry_delay: Duration,
}

impl Default for PollingConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(10),
            timeout: Duration::from_secs(30 * 60),
            max_retries: 10,
            min_retry_delay: Duration::from_secs(1),
            max_retry_delay: Duration::from_secs(60),
        }
    }
}

/// Network identifier for tokenization operations.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Network(String);

impl Network {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into().to_lowercase())
    }
}

impl<'de> Deserialize<'de> for Network {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Self::new(s))
    }
}

/// Type of tokenization request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TokenizationRequestType {
    Mint,
    Redeem,
}

/// Token issuer identifier.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Issuer(String);

fn deserialize_float<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum FloatOrString {
        Float(f64),
        String(String),
    }

    match FloatOrString::deserialize(deserializer)? {
        FloatOrString::Float(f) => Ok(f),
        FloatOrString::String(s) => s.parse().map_err(serde::de::Error::custom),
    }
}
