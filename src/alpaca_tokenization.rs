//! Alpaca tokenization API client for mint and redemption operations.
//!
//! This module provides a client for interacting with Alpaca's tokenization API,
//! which enables converting offchain shares to onchain tokens (minting) and
//! converting onchain tokens back to offchain shares (redemption).
//!
//! # API Endpoints
//!
//! - `POST /v2/tokenization/mint` - Request mint (shares to tokens)
//! - `GET /v2/tokenization/requests` - List/poll tokenization requests
//!
//! # Workflows
//!
//! **Mint** (TokenizedEquityMint aggregate):
//! 1. Call `request_mint` with symbol, qty, wallet
//! 2. Receive `TokenizationRequest` with `tokenization_request_id`
//! 3. Poll until status is `Completed` or `Rejected`
//!
//! **Redemption** (EquityRedemption aggregate):
//! 1. Send tokens to redemption wallet (onchain tx)
//! 2. Poll `list_requests` for Alpaca's detection
//! 3. Poll until status is `Completed` or `Rejected`

use alloy::primitives::{Address, TxHash};
use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::Symbol;
use thiserror::Error;

use crate::alpaca_wallet::Network;
use crate::onchain::io::TokenizedEquitySymbol;
use crate::tokenized_equity_mint::{IssuerRequestId, TokenizationRequestId};

fn deserialize_tokenized_symbol<'de, D>(
    deserializer: D,
) -> Result<Option<TokenizedEquitySymbol>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| s.parse().map_err(serde::de::Error::custom))
        .transpose()
}

/// Type of tokenization request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum TokenizationRequestType {
    Mint,
    Redeem,
}

/// Status of a tokenization request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum TokenizationRequestStatus {
    Pending,
    Completed,
    Rejected,
}

/// Token issuer identifier.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Issuer(String);

impl Issuer {
    fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

/// A tokenization request returned by the Alpaca API.
#[derive(Debug, Clone, Deserialize, PartialEq)]
struct TokenizationRequest {
    #[serde(rename = "tokenization_request_id")]
    id: TokenizationRequestId,
    r#type: TokenizationRequestType,
    status: TokenizationRequestStatus,
    underlying_symbol: Symbol,
    #[serde(deserialize_with = "deserialize_tokenized_symbol")]
    token_symbol: Option<TokenizedEquitySymbol>,
    #[serde(rename = "qty")]
    quantity: Decimal,
    issuer: Issuer,
    network: Network,
    #[serde(rename = "wallet_address")]
    wallet: Address,
    issuer_request_id: Option<IssuerRequestId>,
    tx_hash: Option<TxHash>,
    fees: Option<Decimal>,
    created_at: DateTime<Utc>,
    updated_at: Option<DateTime<Utc>>,
}

/// Request body for initiating a mint operation.
#[derive(Debug, Clone, Serialize)]
struct MintRequest {
    underlying_symbol: Symbol,
    #[serde(rename = "qty")]
    quantity: Decimal,
    issuer: Issuer,
    network: Network,
    #[serde(rename = "wallet_address")]
    wallet: Address,
}

/// Errors that can occur when interacting with the Alpaca tokenization API.
#[derive(Debug, Error)]
enum AlpacaTokenizationError {
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error("API error (status {status}): {message}")]
    ApiError { status: StatusCode, message: String },

    #[error("Insufficient position for symbol: {symbol}")]
    InsufficientPosition { symbol: Symbol },

    #[error("Account not supported for tokenization")]
    UnsupportedAccount,

    #[error("Invalid parameters: {details}")]
    InvalidParameters { details: String },
}

/// Client for Alpaca's tokenization API.
struct AlpacaTokenizationClient {
    client: Client,
    base_url: String,
    api_key: String,
    api_secret: String,
}

impl AlpacaTokenizationClient {
    #[cfg(test)]
    fn new_with_base_url(base_url: String, api_key: String, api_secret: String) -> Self {
        Self {
            client: Client::new(),
            base_url,
            api_key,
            api_secret,
        }
    }

    /// Request a mint operation to convert offchain shares to onchain tokens.
    ///
    /// # Errors
    ///
    /// - `InsufficientPosition` if the account lacks the required shares (403)
    /// - `UnsupportedAccount` if the account is not enabled for tokenization (403)
    /// - `InvalidParameters` if the request parameters are invalid (422)
    /// - `ApiError` for other API errors
    /// - `Reqwest` for network errors
    async fn request_mint(
        &self,
        request: MintRequest,
    ) -> Result<TokenizationRequest, AlpacaTokenizationError> {
        let url = format!("{}/v2/tokenization/mint", self.base_url);

        let response = self
            .client
            .post(&url)
            .header("APCA-API-KEY-ID", &self.api_key)
            .header("APCA-API-SECRET-KEY", &self.api_secret)
            .json(&request)
            .send()
            .await?;

        let status = response.status();

        if status.is_success() {
            let tokenization_request: TokenizationRequest = response.json().await?;
            return Ok(tokenization_request);
        }

        let message = response.text().await?;

        match status {
            StatusCode::FORBIDDEN => {
                if message.contains("insufficient") || message.contains("position") {
                    Err(AlpacaTokenizationError::InsufficientPosition {
                        symbol: request.underlying_symbol,
                    })
                } else {
                    Err(AlpacaTokenizationError::UnsupportedAccount)
                }
            }
            StatusCode::UNPROCESSABLE_ENTITY => {
                Err(AlpacaTokenizationError::InvalidParameters { details: message })
            }
            _ => Err(AlpacaTokenizationError::ApiError { status, message }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;
    use httpmock::prelude::*;
    use rust_decimal_macros::dec;
    use serde_json::json;

    fn create_test_client(server: &MockServer) -> AlpacaTokenizationClient {
        AlpacaTokenizationClient::new_with_base_url(
            server.base_url(),
            "test_api_key".to_string(),
            "test_api_secret".to_string(),
        )
    }

    fn create_mint_request() -> MintRequest {
        MintRequest {
            underlying_symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            issuer: Issuer::new("st0x"),
            network: Network::new("base"),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
        }
    }

    #[tokio::test]
    async fn test_request_mint_success() {
        let server = MockServer::start();
        let client = create_test_client(&server);

        let mint_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v2/tokenization/mint")
                .header("APCA-API-KEY-ID", "test_api_key")
                .header("APCA-API-SECRET-KEY", "test_api_secret")
                .json_body(json!({
                    "underlying_symbol": "AAPL",
                    "qty": "100.5",
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address": "0x1234567890abcdef1234567890abcdef12345678"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "tokenization_request_id": "tok_req_123",
                    "type": "mint",
                    "status": "pending",
                    "underlying_symbol": "AAPL",
                    "token_symbol": "tAAPL",
                    "qty": "100.5",
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "issuer_request_id": "iss_req_456",
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        });

        let request = create_mint_request();
        let result = client.request_mint(request).await.unwrap();

        assert_eq!(result.id, TokenizationRequestId("tok_req_123".to_string()));
        assert_eq!(result.r#type, TokenizationRequestType::Mint);
        assert_eq!(result.status, TokenizationRequestStatus::Pending);
        assert_eq!(result.underlying_symbol.to_string(), "AAPL");
        assert_eq!(
            result.token_symbol.as_ref().map(ToString::to_string),
            Some("tAAPL".to_string())
        );
        assert_eq!(result.quantity, dec!(100.5));
        assert_eq!(result.issuer, Issuer::new("st0x"));
        assert_eq!(result.network, Network::new("base"));
        assert_eq!(
            result.issuer_request_id,
            Some(IssuerRequestId("iss_req_456".to_string()))
        );

        mint_mock.assert();
    }

    #[tokio::test]
    async fn test_request_mint_insufficient_position() {
        let server = MockServer::start();
        let client = create_test_client(&server);

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path("/v2/tokenization/mint");
            then.status(403)
                .header("content-type", "application/json")
                .json_body(json!({
                    "code": 40_310_000,
                    "message": "insufficient position for AAPL"
                }));
        });

        let request = create_mint_request();
        let result = client.request_mint(request).await;

        let err = result.unwrap_err();
        assert!(
            matches!(&err, AlpacaTokenizationError::InsufficientPosition { symbol } if symbol.to_string() == "AAPL"),
            "expected InsufficientPosition for AAPL, got: {err:?}"
        );

        mint_mock.assert();
    }

    #[tokio::test]
    async fn test_request_mint_invalid_parameters() {
        let server = MockServer::start();
        let client = create_test_client(&server);

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path("/v2/tokenization/mint");
            then.status(422)
                .header("content-type", "application/json")
                .json_body(json!({
                    "code": 42_210_000,
                    "message": "invalid wallet address format"
                }));
        });

        let request = create_mint_request();
        let result = client.request_mint(request).await;

        let err = result.unwrap_err();
        assert!(
            matches!(&err, AlpacaTokenizationError::InvalidParameters { details } if details.contains("invalid wallet address")),
            "expected InvalidParameters with 'invalid wallet address', got: {err:?}"
        );

        mint_mock.assert();
    }
}
