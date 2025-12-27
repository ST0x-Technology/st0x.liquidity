use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::{DateTime, FixedOffset};
use clap::{Parser, ValueEnum};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::debug;
use uuid::Uuid;

use super::AlpacaBrokerApiError;
use crate::{Shares, Symbol};

/// Mode for Alpaca Broker API
#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum AlpacaBrokerApiMode {
    /// Sandbox environment (paper trading)
    Sandbox,
    /// Production environment (real money)
    Production,
    /// Mock mode for testing (test-only)
    #[cfg(test)]
    #[clap(skip)]
    Mock(String),
}

impl AlpacaBrokerApiMode {
    fn base_url(&self) -> &str {
        match self {
            Self::Sandbox => "https://broker-api.sandbox.alpaca.markets",
            Self::Production => "https://broker-api.alpaca.markets",
            #[cfg(test)]
            Self::Mock(url) => url,
        }
    }
}

/// Alpaca Broker API authentication environment configuration
#[derive(Parser, Clone)]
pub struct AlpacaBrokerApiAuthEnv {
    /// Alpaca Broker API key
    #[clap(long, env)]
    pub alpaca_broker_api_key: String,

    /// Alpaca Broker API secret
    #[clap(long, env)]
    pub alpaca_broker_api_secret: String,

    /// Alpaca account ID for trading operations
    #[clap(long, env)]
    pub alpaca_account_id: String,

    /// Broker API mode: sandbox or production (defaults to sandbox for safety)
    #[clap(long, env, default_value = "sandbox")]
    pub alpaca_broker_api_mode: AlpacaBrokerApiMode,
}

impl std::fmt::Debug for AlpacaBrokerApiAuthEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaBrokerApiAuthEnv")
            .field("alpaca_broker_api_key", &"[REDACTED]")
            .field("alpaca_broker_api_secret", &"[REDACTED]")
            .field("alpaca_account_id", &self.alpaca_account_id)
            .field("alpaca_broker_api_mode", &self.alpaca_broker_api_mode)
            .finish()
    }
}

impl AlpacaBrokerApiAuthEnv {
    /// Returns the base URL for Alpaca Broker API.
    pub fn base_url(&self) -> &str {
        self.alpaca_broker_api_mode.base_url()
    }

    /// Returns true if using sandbox mode (paper trading).
    pub fn is_sandbox(&self) -> bool {
        !matches!(self.alpaca_broker_api_mode, AlpacaBrokerApiMode::Production)
    }
}

/// Account status from Alpaca Broker API
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AccountStatus {
    Onboarding,
    SubmissionFailed,
    Submitted,
    AccountUpdated,
    ApprovalPending,
    Active,
    Rejected,
    Disabled,
    DisableRequested,
    AccountClosed,
}

/// Response from the account verification endpoint
#[derive(Debug, Deserialize)]
pub(super) struct AccountResponse {
    pub id: Uuid,
    pub status: AccountStatus,
}

/// Response from the clock endpoint
#[derive(Debug, Deserialize)]
pub(super) struct ClockResponse {
    pub timestamp: DateTime<FixedOffset>,
    pub is_open: bool,
    pub next_open: DateTime<FixedOffset>,
    pub next_close: DateTime<FixedOffset>,
}

/// Order side
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum OrderSide {
    Buy,
    Sell,
}

/// Order status from Alpaca Broker API
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum BrokerOrderStatus {
    New,
    PendingNew,
    PartiallyFilled,
    Filled,
    DoneForDay,
    Canceled,
    Expired,
    Replaced,
    PendingCancel,
    PendingReplace,
    Rejected,
    Suspended,
    Calculated,
    Stopped,
    AcceptedForBidding,
    Accepted,
}

/// Order request for placing market orders
#[derive(Debug, Serialize)]
pub(super) struct OrderRequest {
    #[serde(serialize_with = "serialize_symbol")]
    pub symbol: Symbol,
    #[serde(rename = "qty", serialize_with = "serialize_shares")]
    pub quantity: Shares,
    pub side: OrderSide,
    #[serde(rename = "type")]
    pub order_type: &'static str,
    pub time_in_force: &'static str,
}

fn serialize_symbol<S>(symbol: &Symbol, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&symbol.to_string())
}

// serde's serialize_with requires the field to be passed by reference
#[allow(clippy::trivially_copy_pass_by_ref)]
fn serialize_shares<S>(shares: &Shares, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&shares.value().to_string())
}

/// Order response from the Alpaca Broker API
#[derive(Debug, Deserialize)]
pub(super) struct OrderResponse {
    pub id: Uuid,
    pub symbol: Symbol,
    #[serde(rename = "qty", deserialize_with = "deserialize_shares_from_string")]
    pub quantity: Shares,
    #[serde(
        rename = "filled_qty",
        default,
        deserialize_with = "deserialize_optional_decimal"
    )]
    pub filled_quantity: Option<Decimal>,
    pub side: OrderSide,
    pub status: BrokerOrderStatus,
    #[serde(
        rename = "filled_avg_price",
        default,
        deserialize_with = "deserialize_optional_price"
    )]
    pub filled_average_price: Option<f64>,
}

fn deserialize_shares_from_string<'de, D>(deserializer: D) -> Result<Shares, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let value: u64 = s.parse().map_err(serde::de::Error::custom)?;
    Shares::new(value).map_err(serde::de::Error::custom)
}

fn deserialize_optional_price<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    opt.map_or(Ok(None), |s| {
        s.parse::<f64>().map(Some).map_err(serde::de::Error::custom)
    })
}

fn deserialize_optional_decimal<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    opt.map_or(Ok(None), |s| {
        s.parse::<Decimal>()
            .map(Some)
            .map_err(serde::de::Error::custom)
    })
}

/// Alpaca Broker API HTTP client with Basic authentication
pub(crate) struct AlpacaBrokerApiClient {
    http_client: reqwest::Client,
    base_url: String,
    account_id: String,
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
    pub(crate) fn new(env: &AlpacaBrokerApiAuthEnv) -> Result<Self, AlpacaBrokerApiError> {
        let credentials = format!(
            "{}:{}",
            env.alpaca_broker_api_key, env.alpaca_broker_api_secret
        );
        let encoded_credentials = BASE64_STANDARD.encode(credentials.as_bytes());

        let mut headers = HeaderMap::new();
        let auth_value = format!("Basic {encoded_credentials}");
        headers.insert(AUTHORIZATION, HeaderValue::from_str(&auth_value)?);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let http_client = reqwest::Client::builder()
            .default_headers(headers)
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()?;

        Ok(Self {
            http_client,
            base_url: env.base_url().to_string(),
            account_id: env.alpaca_account_id.clone(),
            mode: env.alpaca_broker_api_mode.clone(),
        })
    }

    pub(crate) fn is_sandbox(&self) -> bool {
        !matches!(self.mode, AlpacaBrokerApiMode::Production)
    }

    #[cfg(test)]
    pub(crate) fn account_id(&self) -> &str {
        &self.account_id
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

    /// Get the current market clock
    pub(crate) async fn get_clock(&self) -> Result<ClockResponse, AlpacaBrokerApiError> {
        let url = format!(
            "{}/v1/trading/accounts/{}/clock",
            self.base_url, self.account_id
        );

        debug!("Fetching market clock from {}", url);

        self.get(&url).await
    }

    /// Place an order
    pub(crate) async fn place_order(
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
    pub(crate) async fn get_order(
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

    /// List open orders
    pub(crate) async fn list_open_orders(
        &self,
    ) -> Result<Vec<OrderResponse>, AlpacaBrokerApiError> {
        let url = format!(
            "{}/v1/trading/accounts/{}/orders?status=open",
            self.base_url, self.account_id
        );

        debug!("Listing open orders from {}", url);

        self.get(&url).await
    }

    /// Perform a GET request
    async fn get<T: serde::de::DeserializeOwned + Send>(
        &self,
        url: &str,
    ) -> Result<T, AlpacaBrokerApiError> {
        let response = self.http_client.get(url).send().await?;

        self.handle_response(response).await
    }

    /// Perform a POST request with JSON body
    async fn post<T: serde::de::DeserializeOwned + Send, B: Serialize + Sync>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<T, AlpacaBrokerApiError> {
        let response = self.http_client.post(url).json(body).send().await?;

        self.handle_response(response).await
    }

    /// Handle HTTP response, including error status codes
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
    use super::*;
    use httpmock::prelude::*;

    fn create_test_sandbox_config() -> AlpacaBrokerApiAuthEnv {
        AlpacaBrokerApiAuthEnv {
            alpaca_broker_api_key: "test_key_id".to_string(),
            alpaca_broker_api_secret: "test_secret_key".to_string(),
            alpaca_account_id: "test_account_123".to_string(),
            alpaca_broker_api_mode: AlpacaBrokerApiMode::Sandbox,
        }
    }

    fn create_test_production_config() -> AlpacaBrokerApiAuthEnv {
        AlpacaBrokerApiAuthEnv {
            alpaca_broker_api_key: "test_key_id".to_string(),
            alpaca_broker_api_secret: "test_secret_key".to_string(),
            alpaca_account_id: "test_account_123".to_string(),
            alpaca_broker_api_mode: AlpacaBrokerApiMode::Production,
        }
    }

    fn create_test_mock_config(base_url: &str) -> AlpacaBrokerApiAuthEnv {
        AlpacaBrokerApiAuthEnv {
            alpaca_broker_api_key: "test_key_id".to_string(),
            alpaca_broker_api_secret: "test_secret_key".to_string(),
            alpaca_account_id: "test_account_123".to_string(),
            alpaca_broker_api_mode: AlpacaBrokerApiMode::Mock(base_url.to_string()),
        }
    }

    #[test]
    fn test_alpaca_broker_api_mode_urls() {
        assert_eq!(
            AlpacaBrokerApiMode::Sandbox.base_url(),
            "https://broker-api.sandbox.alpaca.markets"
        );
        assert_eq!(
            AlpacaBrokerApiMode::Production.base_url(),
            "https://broker-api.alpaca.markets"
        );
    }

    #[test]
    fn test_alpaca_broker_api_auth_env_base_url() {
        let sandbox_config = create_test_sandbox_config();
        assert_eq!(
            sandbox_config.base_url(),
            "https://broker-api.sandbox.alpaca.markets"
        );

        let production_config = create_test_production_config();
        assert_eq!(
            production_config.base_url(),
            "https://broker-api.alpaca.markets"
        );
    }

    #[test]
    fn test_alpaca_broker_api_client_new_valid_config() {
        let config = create_test_sandbox_config();
        let client = AlpacaBrokerApiClient::new(&config).unwrap();

        assert!(client.is_sandbox());
        assert_eq!(client.account_id(), "test_account_123");
    }

    #[test]
    fn test_alpaca_broker_api_client_sandbox_vs_production() {
        let sandbox_config = create_test_sandbox_config();
        let production_config = create_test_production_config();
        let mock_config = create_test_mock_config("http://localhost:8080");

        let sandbox_client = AlpacaBrokerApiClient::new(&sandbox_config).unwrap();
        let production_client = AlpacaBrokerApiClient::new(&production_config).unwrap();
        let mock_client = AlpacaBrokerApiClient::new(&mock_config).unwrap();

        assert!(sandbox_client.is_sandbox());
        assert!(!production_client.is_sandbox());
        assert!(
            mock_client.is_sandbox(),
            "Mock mode should be treated as non-production"
        );
    }

    #[test]
    fn test_alpaca_broker_api_auth_env_debug_redacts_secrets() {
        let config = AlpacaBrokerApiAuthEnv {
            alpaca_broker_api_key: "super_secret_key_123".to_string(),
            alpaca_broker_api_secret: "ultra_secret_secret_456".to_string(),
            alpaca_account_id: "account_789".to_string(),
            alpaca_broker_api_mode: AlpacaBrokerApiMode::Sandbox,
        };

        let debug_output = format!("{config:?}");

        assert!(debug_output.contains("[REDACTED]"));
        assert!(!debug_output.contains("super_secret_key_123"));
        assert!(!debug_output.contains("ultra_secret_secret_456"));
        assert!(debug_output.contains("account_789"));
        assert!(debug_output.contains("Sandbox"));
    }

    #[test]
    fn test_alpaca_broker_api_client_debug_does_not_leak_credentials() {
        let config = create_test_sandbox_config();
        let client = AlpacaBrokerApiClient::new(&config).unwrap();

        let debug_output = format!("{client:?}");

        assert!(!debug_output.contains("test_key_id"));
        assert!(!debug_output.contains("test_secret_key"));
        assert!(debug_output.contains("test_account_123"));
        assert!(debug_output.contains("Sandbox"));
    }

    #[tokio::test]
    async fn test_verify_account_success() {
        let server = MockServer::start();
        let config = create_test_mock_config(&server.base_url());

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/account")
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

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let account = client.verify_account().await.unwrap();

        mock.assert();
        assert_eq!(
            account.id.to_string(),
            "904837e3-3b76-47ec-b432-046db621571b"
        );
        assert_eq!(account.status, AccountStatus::Active);
    }

    #[tokio::test]
    async fn test_verify_account_unauthorized() {
        let server = MockServer::start();
        let config = create_test_mock_config(&server.base_url());

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/account");
            then.status(401)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "code": 40_110_000,
                    "message": "Invalid credentials"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let result = client.verify_account().await;

        mock.assert();
        let err = result.unwrap_err();
        assert!(
            matches!(err, AlpacaBrokerApiError::ApiError { status, .. } if status.as_u16() == 401)
        );
    }

    #[tokio::test]
    async fn test_get_clock_success() {
        let server = MockServer::start();
        let config = create_test_mock_config(&server.base_url());

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/clock");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "timestamp": "2025-01-03T14:30:00-05:00",
                    "is_open": true,
                    "next_open": "2025-01-06T09:30:00-05:00",
                    "next_close": "2025-01-03T16:00:00-05:00"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let clock = client.get_clock().await.unwrap();

        mock.assert();
        assert!(clock.is_open);
    }

    #[tokio::test]
    async fn test_place_order_success() {
        let server = MockServer::start();
        let config = create_test_mock_config(&server.base_url());

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/test_account_123/orders")
                .json_body(serde_json::json!({
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "type": "market",
                    "time_in_force": "day"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let request = OrderRequest {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: Shares::new(100).unwrap(),
            side: OrderSide::Buy,
            order_type: "market",
            time_in_force: "day",
        };

        let order = client.place_order(&request).await.unwrap();

        mock.assert();
        assert_eq!(order.id.to_string(), "904837e3-3b76-47ec-b432-046db621571b");
        assert_eq!(order.symbol.to_string(), "AAPL");
        assert_eq!(order.quantity.value(), 100);
        assert_eq!(order.status, BrokerOrderStatus::New);
    }

    #[tokio::test]
    async fn test_get_order_success() {
        let server = MockServer::start();
        let config = create_test_mock_config(&server.base_url());
        let order_id = Uuid::parse_str("904837e3-3b76-47ec-b432-046db621571b").unwrap();

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/test_account_123/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "status": "filled",
                    "filled_avg_price": "150.25"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let order = client.get_order(order_id).await.unwrap();

        mock.assert();
        assert_eq!(order.id, order_id);
        assert_eq!(order.status, BrokerOrderStatus::Filled);
        assert_eq!(order.filled_average_price, Some(150.25));
    }

    #[tokio::test]
    async fn test_list_open_orders_success() {
        let server = MockServer::start();
        let config = create_test_mock_config(&server.base_url());

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/orders")
                .query_param("status", "open");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!([
                    {
                        "id": "904837e3-3b76-47ec-b432-046db621571b",
                        "symbol": "AAPL",
                        "qty": "100",
                        "filled_qty": "0",
                        "side": "buy",
                        "status": "new",
                        "filled_avg_price": null
                    },
                    {
                        "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                        "symbol": "TSLA",
                        "qty": "50",
                        "filled_qty": "25",
                        "side": "sell",
                        "status": "partially_filled",
                        "filled_avg_price": "245.50"
                    }
                ]));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let orders = client.list_open_orders().await.unwrap();

        mock.assert();
        assert_eq!(orders.len(), 2);
        assert_eq!(orders[0].symbol.to_string(), "AAPL");
        assert_eq!(orders[0].quantity.value(), 100);
        assert_eq!(orders[0].filled_quantity, Some(Decimal::ZERO));
        assert_eq!(orders[0].status, BrokerOrderStatus::New);
        assert_eq!(orders[1].symbol.to_string(), "TSLA");
        assert_eq!(orders[1].quantity.value(), 50);
        assert_eq!(orders[1].filled_quantity, Some(Decimal::from(25)));
        assert_eq!(orders[1].status, BrokerOrderStatus::PartiallyFilled);
    }

    #[tokio::test]
    async fn test_list_open_orders_empty() {
        let server = MockServer::start();
        let config = create_test_mock_config(&server.base_url());

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/orders")
                .query_param("status", "open");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!([]));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let orders = client.list_open_orders().await.unwrap();

        mock.assert();
        assert!(orders.is_empty());
    }
}
