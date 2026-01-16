//! Alpaca tokenization API client implementation.

use reqwest::{Client, StatusCode};
use serde::Serialize;
use tokio::time::{Instant, MissedTickBehavior};

use crate::{
    AlpacaTokenizationError, Issuer, Network, PollingConfig, TokenizationRequest,
    TokenizationRequestId, TokenizationRequestStatus, TokenizationRequestType, Tokenizer,
};

use alloy::primitives::{Address, TxHash};
use async_trait::async_trait;

impl Issuer {
    fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde's serialize_with requires `fn(&T, S)` signature
fn serialize_float<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&value.to_string())
}

/// Request body for initiating a mint operation.
#[derive(Debug, Clone, Serialize)]
struct MintRequest {
    underlying_symbol: String,
    #[serde(rename = "qty", serialize_with = "serialize_float")]
    quantity: f64,
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
    underlying_symbol: Option<String>,
}

/// Client for Alpaca's tokenization API.
struct AlpacaTokenizationClient {
    http_client: Client,
    base_url: String,
    api_key: String,
    api_secret: String,
    redemption_wallet: Address,
}

impl AlpacaTokenizationClient {
    fn new(
        base_url: String,
        api_key: String,
        api_secret: String,
        redemption_wallet: Address,
    ) -> Self {
        Self {
            http_client: Client::new(),
            base_url,
            api_key,
            api_secret,
            redemption_wallet,
        }
    }

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

    async fn get_request(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, AlpacaTokenizationError> {
        let url = format!("{}/v2/tokenization/requests/{}", self.base_url, id);

        let response = self
            .http_client
            .get(&url)
            .header("APCA-API-KEY-ID", &self.api_key)
            .header("APCA-API-SECRET-KEY", &self.api_secret)
            .send()
            .await?;

        let status = response.status();

        if status.is_success() {
            let request: TokenizationRequest = response.json().await?;
            return Ok(request);
        }

        if status == StatusCode::NOT_FOUND {
            return Err(AlpacaTokenizationError::RequestNotFound { id: id.clone() });
        }

        let message = response.text().await?;
        Err(AlpacaTokenizationError::ApiError { status, message })
    }

    async fn list_requests(
        &self,
        params: &ListRequestsParams,
    ) -> Result<Vec<TokenizationRequest>, AlpacaTokenizationError> {
        let mut url = format!("{}/v2/tokenization/requests", self.base_url);
        let mut query_parts = Vec::new();

        if let Some(ref t) = params.request_type {
            let type_str = match t {
                TokenizationRequestType::Mint => "mint",
                TokenizationRequestType::Redeem => "redeem",
            };
            query_parts.push(format!("type={type_str}"));
        }

        if let Some(ref s) = params.status {
            let status_str = match s {
                TokenizationRequestStatus::Pending => "pending",
                TokenizationRequestStatus::Completed => "completed",
                TokenizationRequestStatus::Rejected => "rejected",
            };
            query_parts.push(format!("status={status_str}"));
        }

        if let Some(ref sym) = params.underlying_symbol {
            query_parts.push(format!("underlying_symbol={sym}"));
        }

        if !query_parts.is_empty() {
            url = format!("{}?{}", url, query_parts.join("&"));
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

    async fn poll_until_terminal(
        &self,
        id: &TokenizationRequestId,
        config: &PollingConfig,
    ) -> Result<TokenizationRequest, AlpacaTokenizationError> {
        let start = Instant::now();
        let mut interval = tokio::time::interval(config.interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            if start.elapsed() > config.timeout {
                return Err(AlpacaTokenizationError::PollTimeout {
                    elapsed: start.elapsed(),
                });
            }

            let request = self.get_request(id).await?;

            match request.status {
                TokenizationRequestStatus::Completed | TokenizationRequestStatus::Rejected => {
                    return Ok(request);
                }
                TokenizationRequestStatus::Pending => {}
            }
        }
    }

    async fn find_redemption_by_tx(
        &self,
        tx_hash: &TxHash,
    ) -> Result<Option<TokenizationRequest>, AlpacaTokenizationError> {
        let params = ListRequestsParams {
            request_type: Some(TokenizationRequestType::Redeem),
            ..Default::default()
        };

        let requests = self.list_requests(&params).await?;
        let formatted_hash = format!("{tx_hash:#x}");

        Ok(requests.into_iter().find(|r| {
            r.tx_hash
                .as_ref()
                .is_some_and(|h| format!("{h:#x}") == formatted_hash)
        }))
    }

    async fn poll_for_redemption_detection(
        &self,
        tx_hash: &TxHash,
        config: &PollingConfig,
    ) -> Result<TokenizationRequest, AlpacaTokenizationError> {
        let start = Instant::now();
        let mut interval = tokio::time::interval(config.interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            if start.elapsed() > config.timeout {
                return Err(AlpacaTokenizationError::PollTimeout {
                    elapsed: start.elapsed(),
                });
            }

            if let Some(request) = self.find_redemption_by_tx(tx_hash).await? {
                return Ok(request);
            }
        }
    }
}

/// High-level service for Alpaca tokenization operations.
///
/// Wraps `AlpacaTokenizationClient` with default polling configuration.
pub struct AlpacaTokenizationService {
    client: AlpacaTokenizationClient,
    polling_config: PollingConfig,
}

impl AlpacaTokenizationService {
    /// Create a new tokenization service.
    pub fn new(
        base_url: String,
        api_key: String,
        api_secret: String,
        redemption_wallet: Address,
    ) -> Self {
        let client =
            AlpacaTokenizationClient::new(base_url, api_key, api_secret, redemption_wallet);

        Self {
            client,
            polling_config: PollingConfig::default(),
        }
    }

    /// Request a mint operation to convert offchain shares to onchain tokens.
    ///
    /// # Arguments
    ///
    /// * `underlying_symbol` - The equity symbol (e.g., "AAPL")
    /// * `quantity` - Number of shares to tokenize (as f64)
    /// * `wallet` - Destination wallet address for minted tokens
    pub async fn request_mint(
        &self,
        underlying_symbol: &str,
        quantity: f64,
        wallet: Address,
    ) -> Result<TokenizationRequest, AlpacaTokenizationError> {
        let request = MintRequest {
            underlying_symbol: underlying_symbol.to_string(),
            quantity,
            issuer: Issuer::new("st0x"),
            network: Network::new("base"),
            wallet,
        };
        self.client.request_mint(request).await
    }

    /// Poll a mint request until it reaches a terminal state.
    pub async fn poll_mint_until_complete(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, AlpacaTokenizationError> {
        self.client
            .poll_until_terminal(id, &self.polling_config)
            .await
    }

    /// Returns the redemption wallet address.
    pub fn redemption_wallet(&self) -> Address {
        self.client.redemption_wallet
    }

    /// Poll until Alpaca detects a redemption transfer.
    pub async fn poll_for_redemption(
        &self,
        tx_hash: &TxHash,
    ) -> Result<TokenizationRequest, AlpacaTokenizationError> {
        self.client
            .poll_for_redemption_detection(tx_hash, &self.polling_config)
            .await
    }

    /// Poll a redemption request until it reaches a terminal state.
    pub async fn poll_redemption_until_complete(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, AlpacaTokenizationError> {
        self.client
            .poll_until_terminal(id, &self.polling_config)
            .await
    }
}

#[async_trait]
impl Tokenizer for AlpacaTokenizationService {
    type Error = AlpacaTokenizationError;

    async fn request_mint(
        &self,
        underlying_symbol: &str,
        quantity: f64,
        wallet: Address,
    ) -> Result<TokenizationRequest, Self::Error> {
        Self::request_mint(self, underlying_symbol, quantity, wallet).await
    }

    async fn poll_mint_until_complete(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, Self::Error> {
        Self::poll_mint_until_complete(self, id).await
    }

    fn redemption_wallet(&self) -> Address {
        Self::redemption_wallet(self)
    }

    async fn poll_for_redemption(
        &self,
        tx_hash: &TxHash,
    ) -> Result<TokenizationRequest, Self::Error> {
        Self::poll_for_redemption(self, tx_hash).await
    }

    async fn poll_redemption_until_complete(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, Self::Error> {
        Self::poll_redemption_until_complete(self, id).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use alloy::primitives::{Address, TxHash, address, fixed_bytes};
    use httpmock::prelude::*;
    use serde_json::json;

    use super::*;
    use crate::IssuerRequestId;

    const TEST_REDEMPTION_WALLET: Address = address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

    fn create_test_client(server: &MockServer) -> AlpacaTokenizationClient {
        AlpacaTokenizationClient {
            http_client: reqwest::Client::new(),
            base_url: server.base_url(),
            api_key: "test_api_key".to_string(),
            api_secret: "test_api_secret".to_string(),
            redemption_wallet: TEST_REDEMPTION_WALLET,
        }
    }

    fn create_test_service(server: &MockServer) -> AlpacaTokenizationService {
        let client = create_test_client(server);
        let polling_config = PollingConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_millis(100),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(50),
        };
        AlpacaTokenizationService {
            client,
            polling_config,
        }
    }

    fn create_mint_request() -> MintRequest {
        MintRequest {
            underlying_symbol: "AAPL".to_string(),
            quantity: 100.5,
            issuer: Issuer::new("st0x"),
            network: Network::new("base"),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
        }
    }

    #[tokio::test]
    async fn request_mint_success() {
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
        assert_eq!(result.status, TokenizationRequestStatus::Pending);
        assert_eq!(result.underlying_symbol, "AAPL");
        assert_eq!(result.token_symbol, Some("tAAPL".to_string()));
        assert!((result.quantity - 100.5).abs() < f64::EPSILON);
        assert_eq!(
            result.issuer_request_id,
            Some(IssuerRequestId("iss_req_456".to_string()))
        );

        mint_mock.assert();
    }

    #[tokio::test]
    async fn request_mint_insufficient_position() {
        let server = MockServer::start();
        let client = create_test_client(&server);

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path("/v2/tokenization/mint");
            then.status(403)
                .header("content-type", "application/json")
                .body(r#"{"code": 40310000, "message": "insufficient position for AAPL"}"#);
        });

        let request = create_mint_request();
        let result = client.request_mint(request).await;

        let err = result.unwrap_err();
        assert!(
            matches!(&err, AlpacaTokenizationError::InsufficientPosition { symbol } if symbol == "AAPL"),
            "expected InsufficientPosition for AAPL, got: {err:?}"
        );

        mint_mock.assert();
    }

    #[tokio::test]
    async fn request_mint_invalid_parameters() {
        let server = MockServer::start();
        let client = create_test_client(&server);

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path("/v2/tokenization/mint");
            then.status(422)
                .header("content-type", "application/json")
                .body(r#"{"code": 42210000, "message": "invalid wallet address format"}"#);
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
    async fn list_requests_all() {
        let server = MockServer::start();
        let client = create_test_client(&server);

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
            .list_requests(&ListRequestsParams::default())
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, TokenizationRequestId("req_1".to_string()));
        assert_eq!(result[1].id, TokenizationRequestId("req_2".to_string()));

        list_mock.assert();
    }

    #[tokio::test]
    async fn list_requests_filter_by_type_mint() {
        let server = MockServer::start();
        let client = create_test_client(&server);

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
        let result = client.list_requests(&params).await.unwrap();

        assert_eq!(result.len(), 1);
        list_mock.assert();
    }

    #[tokio::test]
    async fn list_requests_filter_by_type_redeem() {
        let server = MockServer::start();
        let client = create_test_client(&server);

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
        let result = client.list_requests(&params).await.unwrap();

        assert_eq!(result.len(), 1);
        list_mock.assert();
    }

    #[tokio::test]
    async fn get_request_found() {
        let server = MockServer::start();
        let client = create_test_client(&server);

        let get_mock = server.mock(|when, then| {
            when.method(GET).path("/v2/tokenization/requests/req_123");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(sample_tokenization_request_json("req_123", "mint", "AAPL"));
        });

        let id = TokenizationRequestId("req_123".to_string());
        let result = client.get_request(&id).await.unwrap();

        assert_eq!(result.id, id);
        assert_eq!(result.underlying_symbol, "AAPL");

        get_mock.assert();
    }

    #[tokio::test]
    async fn get_request_not_found() {
        let server = MockServer::start();
        let client = create_test_client(&server);

        let get_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v2/tokenization/requests/nonexistent");
            then.status(404)
                .header("content-type", "application/json")
                .body(r#"{"message": "not found"}"#);
        });

        let id = TokenizationRequestId("nonexistent".to_string());
        let result = client.get_request(&id).await;

        let err = result.unwrap_err();
        assert!(
            matches!(&err, AlpacaTokenizationError::RequestNotFound { id: found_id } if found_id.0 == "nonexistent"),
            "expected RequestNotFound, got: {err:?}"
        );

        get_mock.assert();
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
    async fn find_redemption_by_tx_found() {
        let server = MockServer::start();
        let client = create_test_client(&server);

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
    async fn find_redemption_by_tx_not_detected() {
        let server = MockServer::start();
        let client = create_test_client(&server);

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

    fn sample_request_with_status(id: &str, status: &str) -> serde_json::Value {
        json!({
            "tokenization_request_id": id,
            "type": "mint",
            "status": status,
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "qty": "100.0",
            "issuer": "st0x",
            "network": "base",
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
            "created_at": "2024-01-15T10:30:00Z"
        })
    }

    #[tokio::test]
    async fn poll_until_terminal_completed() {
        let server = MockServer::start();
        let client = create_test_client(&server);

        let get_mock = server.mock(|when, then| {
            when.method(GET).path("/v2/tokenization/requests/req_1");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(sample_request_with_status("req_1", "completed"));
        });

        let config = PollingConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let id = TokenizationRequestId("req_1".to_string());
        let result = client.poll_until_terminal(&id, &config).await.unwrap();

        assert_eq!(result.status, TokenizationRequestStatus::Completed);
        get_mock.assert();
    }

    #[tokio::test]
    async fn poll_until_terminal_rejected() {
        let server = MockServer::start();
        let client = create_test_client(&server);

        let get_mock = server.mock(|when, then| {
            when.method(GET).path("/v2/tokenization/requests/req_1");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(sample_request_with_status("req_1", "rejected"));
        });

        let config = PollingConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let id = TokenizationRequestId("req_1".to_string());
        let result = client.poll_until_terminal(&id, &config).await.unwrap();

        assert_eq!(result.status, TokenizationRequestStatus::Rejected);
        get_mock.assert();
    }

    #[tokio::test]
    async fn poll_for_redemption_detection_success() {
        let server = MockServer::start();
        let client = create_test_client(&server);

        let hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");

        let list_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v2/tokenization/requests")
                .query_param("type", "redeem");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_redemption_request_json_with_tx(
                    "redeem_1", "AAPL", hash
                )]));
        });

        let config = PollingConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let result = client
            .poll_for_redemption_detection(&hash, &config)
            .await
            .unwrap();

        assert_eq!(result.id, TokenizationRequestId("redeem_1".to_string()));
        assert_eq!(result.tx_hash, Some(hash));
        list_mock.assert();
    }

    #[tokio::test]
    async fn poll_timeout() {
        let server = MockServer::start();
        let client = create_test_client(&server);

        let _get_mock = server.mock(|when, then| {
            when.method(GET).path("/v2/tokenization/requests/req_1");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(sample_request_with_status("req_1", "pending"));
        });

        let config = PollingConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_millis(50),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let id = TokenizationRequestId("req_1".to_string());
        let result = client.poll_until_terminal(&id, &config).await;

        assert!(
            matches!(result, Err(AlpacaTokenizationError::PollTimeout { .. })),
            "expected PollTimeout, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn service_mint_poll_completed() {
        let server = MockServer::start();
        let service = create_test_service(&server);

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path("/v2/tokenization/mint");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(sample_request_with_status("mint_123", "pending"));
        });

        let get_mock = server.mock(|when, then| {
            when.method(GET).path("/v2/tokenization/requests/mint_123");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(sample_request_with_status("mint_123", "completed"));
        });

        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let mint_result = service.request_mint("AAPL", 100.0, wallet).await.unwrap();

        assert_eq!(
            mint_result.id,
            TokenizationRequestId("mint_123".to_string())
        );
        assert_eq!(mint_result.status, TokenizationRequestStatus::Pending);

        let poll_result = service
            .poll_mint_until_complete(&mint_result.id)
            .await
            .unwrap();

        assert_eq!(poll_result.status, TokenizationRequestStatus::Completed);

        mint_mock.assert();
        get_mock.assert();
    }

    #[tokio::test]
    async fn service_redemption_detected_poll_completed() {
        let server = MockServer::start();
        let service = create_test_service(&server);

        let hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");

        let detection_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v2/tokenization/requests")
                .query_param("type", "redeem");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "tokenization_request_id": "redeem_456",
                    "type": "redeem",
                    "status": "pending",
                    "underlying_symbol": "AAPL",
                    "token_symbol": "tAAPL",
                    "qty": "50.0",
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "tx_hash": hash,
                    "created_at": "2024-01-15T10:30:00Z"
                }]));
        });

        let complete_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v2/tokenization/requests/redeem_456");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "tokenization_request_id": "redeem_456",
                    "type": "redeem",
                    "status": "completed",
                    "underlying_symbol": "AAPL",
                    "token_symbol": "tAAPL",
                    "qty": "50.0",
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "tx_hash": hash,
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        });

        let detected = service.poll_for_redemption(&hash).await.unwrap();

        assert_eq!(detected.id, TokenizationRequestId("redeem_456".to_string()));
        assert_eq!(detected.status, TokenizationRequestStatus::Pending);

        let completed = service
            .poll_redemption_until_complete(&detected.id)
            .await
            .unwrap();

        assert_eq!(completed.status, TokenizationRequestStatus::Completed);

        detection_mock.assert();
        complete_mock.assert();
    }
}
