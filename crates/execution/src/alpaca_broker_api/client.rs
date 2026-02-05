use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::Serialize;
use tracing::debug;
use uuid::Uuid;

use super::AlpacaBrokerApiError;
use super::auth::{AccountResponse, AlpacaBrokerApiCtx, AlpacaBrokerApiMode};
use super::executor::AssetResponse;
use super::order::{CryptoOrderRequest, CryptoOrderResponse, OrderRequest, OrderResponse};
use crate::Symbol;

/// Alpaca Broker API HTTP client with Basic authentication
pub(crate) struct AlpacaBrokerApiClient {
    http_client: reqwest::Client,
    base_url: String,
    account_id: AlpacaAccountId,
    mode: AlpacaBrokerApiMode,
}

impl std::fmt::Debug for AlpacaBrokerApiClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaBrokerApiClient")
            .field("base_url", &self.base_url)
            .field("account_id", &self.account_id)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

impl AlpacaBrokerApiClient {
    pub(crate) fn new(ctx: &AlpacaBrokerApiCtx) -> Result<Self, AlpacaBrokerApiError> {
        let credentials = format!("{}:{}", ctx.api_key, ctx.api_secret);
        let encoded_credentials = BASE64_STANDARD.encode(credentials.as_bytes());
        let auth_value = format!("Basic {encoded_credentials}");

        let headers = HeaderMap::from_iter([
            (AUTHORIZATION, HeaderValue::from_str(&auth_value)?),
            (CONTENT_TYPE, HeaderValue::from_static("application/json")),
        ]);

        let http_client = reqwest::Client::builder()
            .default_headers(headers)
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()?;

        Ok(Self {
            http_client,
            base_url: ctx.base_url().to_string(),
            account_id: ctx.account_id.clone(),
            mode: ctx.mode(),
        })
    }

    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    pub(crate) fn account_id(&self) -> AlpacaAccountId {
        self.account_id
    }

    pub(crate) fn is_sandbox(&self) -> bool {
        !matches!(self.mode, AlpacaBrokerApiMode::Production)
    }

    /// Verify the account by fetching account details
    pub(crate) async fn verify_account(&self) -> Result<AccountResponse, AlpacaBrokerApiError> {
        let url = format!(
            "{}/v1/trading/accounts/{}/account",
            self.base_url, self.account_id
        );

        debug!("Verifying Alpaca Broker API account at {}", url);

        self.get(&url).await
    }

    /// Place an order
    pub(super) async fn place_order(
        &self,
        request: &OrderRequest,
    ) -> Result<OrderResponse, AlpacaBrokerApiError> {
        let url = format!(
            "{}/v1/trading/accounts/{}/orders",
            self.base_url, self.account_id
        );

        debug!("Placing order at {}: {:?}", url, request);

        self.post(&url, request).await
    }

    /// Get an order by ID
    pub(super) async fn get_order(
        &self,
        order_id: Uuid,
    ) -> Result<OrderResponse, AlpacaBrokerApiError> {
        let url = format!(
            "{}/v1/trading/accounts/{}/orders/{}",
            self.base_url, self.account_id, order_id
        );

        debug!("Fetching order {} from {}", order_id, url);

        self.get(&url).await
    }

    /// Get asset information by symbol
    pub(super) async fn get_asset(
        &self,
        symbol: &Symbol,
    ) -> Result<AssetResponse, AlpacaBrokerApiError> {
        let url = format!("{}/v1/assets/{symbol}", self.base_url);
        debug!("Fetching asset info for {symbol}");
        self.get(&url).await
    }

    /// Place a crypto order (e.g., USDC/USD conversion)
    pub(crate) async fn place_crypto_order(
        &self,
        request: &CryptoOrderRequest,
    ) -> Result<CryptoOrderResponse, AlpacaBrokerApiError> {
        let url = format!(
            "{}/v1/trading/accounts/{}/orders",
            self.base_url, self.account_id
        );

        debug!("Placing crypto order at {}: {:?}", url, request);

        self.post(&url, request).await
    }

    /// Get a crypto order by ID
    pub(crate) async fn get_crypto_order(
        &self,
        order_id: Uuid,
    ) -> Result<CryptoOrderResponse, AlpacaBrokerApiError> {
        let url = format!(
            "{}/v1/trading/accounts/{}/orders/{}",
            self.base_url, self.account_id, order_id
        );

        debug!("Fetching crypto order {} from {}", order_id, url);

        self.get(&url).await
    }

    /// Perform a GET request
    pub(super) async fn get<T: serde::de::DeserializeOwned + Send>(
        &self,
        url: &str,
    ) -> Result<T, AlpacaBrokerApiError> {
        let response = self.http_client.get(url).send().await?;

        self.handle_response(response).await
    }

    /// Perform a POST request with JSON body
    pub(super) async fn post<T: serde::de::DeserializeOwned + Send, B: Serialize + Sync>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<T, AlpacaBrokerApiError> {
        let response = self.http_client.post(url).json(body).send().await?;

        self.handle_response(response).await
    }

    async fn handle_response<T: serde::de::DeserializeOwned + Send>(
        &self,
        response: reqwest::Response,
    ) -> Result<T, AlpacaBrokerApiError> {
        let status = response.status();

        if status.is_success() {
            return Ok(response.json().await?);
        }

        let error_body = response.text().await.unwrap_or_default();

        Err(AlpacaBrokerApiError::ApiError {
            status,
            body: error_body,
        })
    }
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use uuid::uuid;

    use super::*;
    use crate::alpaca_broker_api::auth::AlpacaAccountId;
    use crate::alpaca_broker_api::{AssetStatus, TimeInForce};

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    fn create_test_ctx(mode: AlpacaBrokerApiMode) -> AlpacaBrokerApiCtx {
        AlpacaBrokerApiCtx {
            api_key: "test_key_id".to_string(),
            api_secret: "test_secret_key".to_string(),
            account_id: TEST_ACCOUNT_ID,
            mode: Some(mode),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
        }
    }

    #[test]
    fn test_alpaca_broker_api_client_new_valid_ctx() {
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Sandbox);
        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        assert!(client.is_sandbox());
        assert_eq!(client.account_id(), TEST_ACCOUNT_ID);
    }

    #[test]
    fn test_alpaca_broker_api_client_sandbox_vs_production() {
        let sandbox_ctx = create_test_ctx(AlpacaBrokerApiMode::Sandbox);
        let production_ctx = create_test_ctx(AlpacaBrokerApiMode::Production);
        let mock_ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(
            "http://localhost:8080".to_string(),
        ));

        let sandbox_client = AlpacaBrokerApiClient::new(&sandbox_ctx).unwrap();
        let production_client = AlpacaBrokerApiClient::new(&production_ctx).unwrap();
        let mock_client = AlpacaBrokerApiClient::new(&mock_ctx).unwrap();

        assert!(sandbox_client.is_sandbox());
        assert!(!production_client.is_sandbox());
        assert!(
            mock_client.is_sandbox(),
            "Mock mode should be treated as non-production"
        );
    }

    #[test]
    fn test_alpaca_broker_api_client_debug_does_not_leak_credentials() {
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Sandbox);
        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        let debug_output = format!("{client:?}");

        assert!(!debug_output.contains("test_key_id"));
        assert!(!debug_output.contains("test_secret_key"));
        assert!(debug_output.contains("904837e3-3b76-47ec-b432-046db621571b"));
        assert!(debug_output.contains("Sandbox"));
    }

    #[tokio::test]
    async fn test_verify_account_success() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account")
                .header(
                    "authorization",
                    "Basic dGVzdF9rZXlfaWQ6dGVzdF9zZWNyZXRfa2V5",
                );
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "status": "ACTIVE",
                    "currency": "USD",
                    "buying_power": "100000.00"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let account = client.verify_account().await.unwrap();

        mock.assert();
        assert_eq!(
            account.id.to_string(),
            "904837e3-3b76-47ec-b432-046db621571b"
        );
        assert_eq!(account.status, super::super::auth::AccountStatus::Active);
    }

    #[tokio::test]
    async fn test_verify_account_unauthorized() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(401)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "code": 40_110_000,
                    "message": "Invalid credentials"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let err = client.verify_account().await.unwrap_err();

        mock.assert();
        assert!(
            matches!(err, AlpacaBrokerApiError::ApiError { status, .. } if status.as_u16() == 401)
        );
    }

    #[tokio::test]
    async fn test_get_asset_success() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(GET).path("/v1/assets/AAPL");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "AAPL",
                    "status": "active",
                    "tradable": true
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let symbol = Symbol::new("AAPL").unwrap();
        let asset = client.get_asset(&symbol).await.unwrap();

        mock.assert();
        assert_eq!(asset.status, AssetStatus::Active);
        assert!(asset.tradable);
    }

    #[tokio::test]
    async fn test_get_asset_not_found() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(GET).path("/v1/assets/INVALID");
            then.status(404)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "code": 40_410_000,
                    "message": "asset not found for INVALID"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let symbol = Symbol::new("INVALID").unwrap();
        let result = client.get_asset(&symbol).await;

        mock.assert();
        let err = result.unwrap_err();
        assert!(
            matches!(err, AlpacaBrokerApiError::ApiError { status, .. } if status.as_u16() == 404)
        );
    }
}
