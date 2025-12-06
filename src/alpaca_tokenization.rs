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

use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::Provider;
use alloy::signers::Signer;
use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::Symbol;
use thiserror::Error;

use crate::alpaca_wallet::Network;
use crate::bindings::IERC20;
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

/// Parameters for filtering tokenization requests.
#[derive(Debug, Clone, Default)]
struct ListRequestsParams {
    request_type: Option<TokenizationRequestType>,
    status: Option<TokenizationRequestStatus>,
    underlying_symbol: Option<Symbol>,
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

    #[error("Request not found: {id}")]
    RequestNotFound { id: TokenizationRequestId },

    #[error("Redemption transfer failed: {0}")]
    RedemptionTransferFailed(#[from] alloy::contract::Error),

    #[error("Transaction error: {0}")]
    Transaction(#[from] alloy::providers::PendingTransactionError),
}

/// Client for Alpaca's tokenization API and redemption transfers.
struct AlpacaTokenizationClient<P, S>
where
    P: Provider + Clone,
    S: Signer + Clone + Sync,
{
    http_client: Client,
    base_url: String,
    api_key: String,
    api_secret: String,
    provider: P,
    signer: S,
    redemption_wallet: Address,
}

impl<P, S> AlpacaTokenizationClient<P, S>
where
    P: Provider + Clone,
    S: Signer + Clone + Sync,
{
    #[cfg(test)]
    fn new_with_base_url(
        base_url: String,
        api_key: String,
        api_secret: String,
        provider: P,
        signer: S,
        redemption_wallet: Address,
    ) -> Self {
        Self {
            http_client: Client::new(),
            base_url,
            api_key,
            api_secret,
            provider,
            signer,
            redemption_wallet,
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
            .http_client
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

    /// List tokenization requests with optional filtering.
    ///
    /// # Errors
    ///
    /// - `ApiError` for API errors
    /// - `Reqwest` for network errors
    async fn list_requests(
        &self,
        params: ListRequestsParams,
    ) -> Result<Vec<TokenizationRequest>, AlpacaTokenizationError> {
        let mut url = format!("{}/v2/tokenization/requests", self.base_url);
        let mut query_params = Vec::new();

        if let Some(ref request_type) = params.request_type {
            let type_str = match request_type {
                TokenizationRequestType::Mint => "mint",
                TokenizationRequestType::Redeem => "redeem",
            };
            query_params.push(format!("type={type_str}"));
        }

        if let Some(ref status) = params.status {
            let status_str = match status {
                TokenizationRequestStatus::Pending => "pending",
                TokenizationRequestStatus::Completed => "completed",
                TokenizationRequestStatus::Rejected => "rejected",
            };
            query_params.push(format!("status={status_str}"));
        }

        if let Some(ref symbol) = params.underlying_symbol {
            query_params.push(format!("underlying_symbol={symbol}"));
        }

        if !query_params.is_empty() {
            url.push('?');
            url.push_str(&query_params.join("&"));
        }

        let response = self
            .http_client
            .get(&url)
            .header("APCA-API-KEY-ID", &self.api_key)
            .header("APCA-API-SECRET-KEY", &self.api_secret)
            .send()
            .await?;

        let status = response.status();

        if status.is_success() {
            let requests: Vec<TokenizationRequest> = response.json().await?;
            return Ok(requests);
        }

        let message = response.text().await?;
        Err(AlpacaTokenizationError::ApiError { status, message })
    }

    /// Get a single tokenization request by ID.
    ///
    /// # Errors
    ///
    /// - `RequestNotFound` if the request doesn't exist
    /// - `ApiError` for API errors
    /// - `Reqwest` for network errors
    async fn get_request(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, AlpacaTokenizationError> {
        let requests = self.list_requests(ListRequestsParams::default()).await?;

        requests
            .into_iter()
            .find(|r| r.id == *id)
            .ok_or_else(|| AlpacaTokenizationError::RequestNotFound { id: id.clone() })
    }

    /// Send tokens to the redemption wallet to initiate a redemption.
    ///
    /// This transfers ERC20 tokens from the signer's address to the configured
    /// redemption wallet. Once Alpaca detects this transfer, a redemption request
    /// will appear in `list_requests`.
    ///
    /// # Errors
    ///
    /// - `RedemptionTransferFailed` if the contract call fails
    /// - `Transaction` if the transaction fails to confirm
    async fn send_tokens_for_redemption(
        &self,
        token: Address,
        amount: U256,
    ) -> Result<TxHash, AlpacaTokenizationError> {
        let erc20 = IERC20::new(token, self.provider.clone());

        let receipt = erc20
            .transfer(self.redemption_wallet, amount)
            .send()
            .await?
            .get_receipt()
            .await?;

        Ok(receipt.transaction_hash)
    }

    /// Find a redemption request by its onchain transaction hash.
    ///
    /// This polls the Alpaca API to check if they have detected a token transfer
    /// that initiates a redemption. Returns `None` if Alpaca hasn't detected
    /// the transfer yet.
    ///
    /// # Errors
    ///
    /// - `ApiError` for API errors
    /// - `Reqwest` for network errors
    async fn find_redemption_by_tx(
        &self,
        tx_hash: &TxHash,
    ) -> Result<Option<TokenizationRequest>, AlpacaTokenizationError> {
        let params = ListRequestsParams {
            request_type: Some(TokenizationRequestType::Redeem),
            ..Default::default()
        };

        let requests = self.list_requests(params).await?;

        Ok(requests
            .into_iter()
            .find(|r| r.tx_hash.as_ref() == Some(tx_hash)))
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{B256, address, fixed_bytes};
    use alloy::providers::ProviderBuilder;
    use alloy::signers::local::PrivateKeySigner;
    use httpmock::prelude::*;
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::*;
    use crate::bindings::TestERC20;

    fn setup_anvil() -> (AnvilInstance, String, B256) {
        let anvil = Anvil::new().spawn();
        let endpoint = anvil.endpoint();
        let private_key = B256::from_slice(&anvil.keys()[0].to_bytes());
        (anvil, endpoint, private_key)
    }

    async fn create_test_client(
        server: &MockServer,
        anvil_endpoint: &str,
        private_key: &B256,
        redemption_wallet: Address,
    ) -> AlpacaTokenizationClient<impl Provider + Clone, PrivateKeySigner> {
        let signer = PrivateKeySigner::from_bytes(private_key).unwrap();
        let wallet = EthereumWallet::from(signer.clone());

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(anvil_endpoint)
            .await
            .unwrap();

        AlpacaTokenizationClient::new_with_base_url(
            server.base_url(),
            "test_api_key".to_string(),
            "test_api_secret".to_string(),
            provider,
            signer,
            redemption_wallet,
        )
    }

    const TEST_REDEMPTION_WALLET: Address = address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

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
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

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
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

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
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

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

    fn sample_tokenization_request_json(
        id: &str,
        request_type: &str,
        symbol: &str,
    ) -> serde_json::Value {
        json!({
            "tokenization_request_id": id,
            "type": request_type,
            "status": "pending",
            "underlying_symbol": symbol,
            "token_symbol": format!("t{symbol}"),
            "qty": "50.0",
            "issuer": "st0x",
            "network": "base",
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
            "created_at": "2024-01-15T10:30:00Z"
        })
    }

    #[tokio::test]
    async fn test_list_requests_all() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let list_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v2/tokenization/requests")
                .header("APCA-API-KEY-ID", "test_api_key")
                .header("APCA-API-SECRET-KEY", "test_api_secret");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    sample_tokenization_request_json("req_1", "mint", "AAPL"),
                    sample_tokenization_request_json("req_2", "redeem", "TSLA")
                ]));
        });

        let result = client
            .list_requests(ListRequestsParams::default())
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, TokenizationRequestId("req_1".to_string()));
        assert_eq!(result[0].r#type, TokenizationRequestType::Mint);
        assert_eq!(result[1].id, TokenizationRequestId("req_2".to_string()));
        assert_eq!(result[1].r#type, TokenizationRequestType::Redeem);

        list_mock.assert();
    }

    #[tokio::test]
    async fn test_list_requests_filter_by_type_mint() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let list_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v2/tokenization/requests")
                .query_param("type", "mint");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_tokenization_request_json(
                    "req_1", "mint", "AAPL"
                )]));
        });

        let params = ListRequestsParams {
            request_type: Some(TokenizationRequestType::Mint),
            ..Default::default()
        };
        let result = client.list_requests(params).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].r#type, TokenizationRequestType::Mint);

        list_mock.assert();
    }

    #[tokio::test]
    async fn test_list_requests_filter_by_type_redeem() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let list_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v2/tokenization/requests")
                .query_param("type", "redeem");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_tokenization_request_json(
                    "req_2", "redeem", "TSLA"
                )]));
        });

        let params = ListRequestsParams {
            request_type: Some(TokenizationRequestType::Redeem),
            ..Default::default()
        };
        let result = client.list_requests(params).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].r#type, TokenizationRequestType::Redeem);

        list_mock.assert();
    }

    #[tokio::test]
    async fn test_get_request_found() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let list_mock = server.mock(|when, then| {
            when.method(GET).path("/v2/tokenization/requests");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    sample_tokenization_request_json("req_1", "mint", "AAPL"),
                    sample_tokenization_request_json("req_2", "redeem", "TSLA")
                ]));
        });

        let id = TokenizationRequestId("req_2".to_string());
        let result = client.get_request(&id).await.unwrap();

        assert_eq!(result.id, id);
        assert_eq!(result.r#type, TokenizationRequestType::Redeem);
        assert_eq!(result.underlying_symbol.to_string(), "TSLA");

        list_mock.assert();
    }

    #[tokio::test]
    async fn test_get_request_not_found() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let list_mock = server.mock(|when, then| {
            when.method(GET).path("/v2/tokenization/requests");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_tokenization_request_json(
                    "req_1", "mint", "AAPL"
                )]));
        });

        let id = TokenizationRequestId("nonexistent".to_string());
        let result = client.get_request(&id).await;

        let err = result.unwrap_err();
        assert!(
            matches!(&err, AlpacaTokenizationError::RequestNotFound { id: found_id } if found_id.0 == "nonexistent"),
            "expected RequestNotFound, got: {err:?}"
        );

        list_mock.assert();
    }

    #[tokio::test]
    async fn test_send_tokens_for_redemption_success() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let signer = PrivateKeySigner::from_bytes(&key).unwrap();
        let wallet = EthereumWallet::from(signer.clone());

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(&endpoint)
            .await
            .unwrap();

        let token = TestERC20::deploy(&provider).await.unwrap();
        let token_address = *token.address();

        let mint_amount = U256::from(1_000_000_000u64);
        token
            .mint(signer.address(), mint_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let client = AlpacaTokenizationClient::new_with_base_url(
            server.base_url(),
            "test_api_key".to_string(),
            "test_api_secret".to_string(),
            provider.clone(),
            signer,
            TEST_REDEMPTION_WALLET,
        );

        let transfer_amount = U256::from(100_000u64);
        let result = client
            .send_tokens_for_redemption(token_address, transfer_amount)
            .await;

        assert!(
            result.is_ok(),
            "expected successful transfer, got: {result:?}"
        );

        let balance = token
            .balanceOf(TEST_REDEMPTION_WALLET)
            .call()
            .await
            .unwrap();
        assert_eq!(
            balance, transfer_amount,
            "redemption wallet should have received tokens"
        );
    }

    #[tokio::test]
    async fn test_send_tokens_for_redemption_insufficient_balance() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let signer = PrivateKeySigner::from_bytes(&key).unwrap();
        let wallet = EthereumWallet::from(signer.clone());

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(&endpoint)
            .await
            .unwrap();

        let token = TestERC20::deploy(&provider).await.unwrap();
        let token_address = *token.address();

        let client = AlpacaTokenizationClient::new_with_base_url(
            server.base_url(),
            "test_api_key".to_string(),
            "test_api_secret".to_string(),
            provider,
            signer,
            TEST_REDEMPTION_WALLET,
        );

        let transfer_amount = U256::from(100_000u64);
        let result = client
            .send_tokens_for_redemption(token_address, transfer_amount)
            .await;

        assert!(
            matches!(
                result,
                Err(AlpacaTokenizationError::RedemptionTransferFailed(_))
            ),
            "expected RedemptionTransferFailed with insufficient balance, got: {result:?}"
        );
    }

    fn sample_redemption_request_json_with_tx(
        id: &str,
        symbol: &str,
        tx_hash: TxHash,
    ) -> serde_json::Value {
        json!({
            "tokenization_request_id": id,
            "type": "redeem",
            "status": "pending",
            "underlying_symbol": symbol,
            "token_symbol": format!("t{symbol}"),
            "qty": "50.0",
            "issuer": "st0x",
            "network": "base",
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
            "tx_hash": tx_hash,
            "created_at": "2024-01-15T10:30:00Z"
        })
    }

    #[tokio::test]
    async fn test_find_redemption_by_tx_found() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");

        let list_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v2/tokenization/requests")
                .query_param("type", "redeem");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    sample_redemption_request_json_with_tx("redeem_1", "AAPL", hash),
                    sample_redemption_request_json_with_tx(
                        "redeem_2",
                        "TSLA",
                        fixed_bytes!(
                            "0x1111111111111111111111111111111111111111111111111111111111111111"
                        )
                    )
                ]));
        });

        let result = client.find_redemption_by_tx(&hash).await.unwrap();

        assert!(result.is_some(), "expected to find redemption request");
        let request = result.unwrap();
        assert_eq!(request.id, TokenizationRequestId("redeem_1".to_string()));
        assert_eq!(request.tx_hash, Some(hash));

        list_mock.assert();
    }

    #[tokio::test]
    async fn test_find_redemption_by_tx_not_detected() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let client = create_test_client(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let list_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v2/tokenization/requests")
                .query_param("type", "redeem");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let hash: TxHash =
            fixed_bytes!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
        let result = client.find_redemption_by_tx(&hash).await.unwrap();

        assert!(
            result.is_none(),
            "expected None when redemption not yet detected"
        );

        list_mock.assert();
    }
}
