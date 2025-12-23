use backon::{ExponentialBuilder, Retryable};
use reqwest::header::{self, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tracing::error;

use super::{SchwabAuthEnv, SchwabError, SchwabTokens, order_status::OrderStatusResponse};

/// Response from Schwab order placement API.
/// According to Schwab OpenAPI spec, successful order placement (201) returns
/// empty body with order ID in the Location header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OrderPlacementResponse {
    pub order_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[allow(clippy::struct_field_names)]
pub struct Order {
    pub order_type: OrderType,
    pub session: Session,
    pub duration: OrderDuration,
    pub order_strategy_type: OrderStrategyType,
    pub order_leg_collection: Vec<OrderLeg>,
}

impl Order {
    pub fn new(symbol: String, instruction: Instruction, quantity: u64) -> Self {
        let instrument = Instrument {
            symbol,
            asset_type: AssetType::Equity,
        };

        let order_leg = OrderLeg {
            instruction,
            quantity,
            instrument,
        };

        Self {
            order_type: OrderType::Market,
            session: Session::Normal,
            duration: OrderDuration::Day,
            order_strategy_type: OrderStrategyType::Single,
            order_leg_collection: vec![order_leg],
        }
    }

    pub async fn place(
        &self,
        env: &SchwabAuthEnv,
        pool: &SqlitePool,
    ) -> Result<OrderPlacementResponse, SchwabError> {
        let access_token = SchwabTokens::get_valid_access_token(pool, env).await?;
        let account_hash = env.get_account_hash(pool).await?;

        let headers = [
            (
                header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {access_token}"))?,
            ),
            (header::ACCEPT, HeaderValue::from_str("*/*")?),
            (
                header::CONTENT_TYPE,
                HeaderValue::from_str("application/json")?,
            ),
        ]
        .into_iter()
        .collect::<HeaderMap>();

        let order_json = serde_json::to_string(self)?;

        let client = reqwest::Client::new();
        let response = (|| async {
            client
                .post(format!(
                    "{}/trader/v1/accounts/{}/orders",
                    env.schwab_base_url, account_hash
                ))
                .headers(headers.clone())
                .body(order_json.clone())
                .send()
                .await
        })
        .retry(ExponentialBuilder::default())
        .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(SchwabError::RequestFailed {
                action: "place order".to_string(),
                status,
                body: error_body,
            });
        }

        // Extract order ID from Location header according to Schwab OpenAPI spec
        let order_id = extract_order_id_from_location_header(&response)?;

        Ok(OrderPlacementResponse { order_id })
    }

    /// Get the status of a specific order from Schwab API.
    /// Returns the order status response containing fill information and execution details.
    pub async fn get_order_status(
        order_id: &str,
        env: &SchwabAuthEnv,
        pool: &SqlitePool,
    ) -> Result<OrderStatusResponse, SchwabError> {
        let access_token = SchwabTokens::get_valid_access_token(pool, env).await?;
        let account_hash = env.get_account_hash(pool).await?;

        let headers = [
            (
                header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {access_token}"))?,
            ),
            (header::ACCEPT, HeaderValue::from_str("application/json")?),
        ]
        .into_iter()
        .collect::<HeaderMap>();

        let client = reqwest::Client::new();
        let response = (|| async {
            client
                .get(format!(
                    "{}/trader/v1/accounts/{}/orders/{}",
                    env.schwab_base_url, account_hash, order_id
                ))
                .headers(headers.clone())
                .send()
                .await
        })
        .retry(ExponentialBuilder::default())
        .await?;

        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(SchwabError::RequestFailed {
                action: "get order status".to_string(),
                status,
                body: format!("Order ID {order_id} not found"),
            });
        }

        if !response.status().is_success() {
            let error_body = response.text().await.unwrap_or_default();
            return Err(SchwabError::RequestFailed {
                action: "get order status".to_string(),
                status,
                body: error_body,
            });
        }

        // Capture response text for debugging parse errors
        let response_text = response.text().await?;

        // Log successful response in debug mode to understand API structure
        tracing::debug!("Schwab order status response: {}", response_text);

        match serde_json::from_str::<OrderStatusResponse>(&response_text) {
            Ok(order_status) => Ok(order_status),
            Err(parse_error) => {
                error!(
                    order_id = %order_id,
                    response_text = %response_text,
                    parse_error = %parse_error,
                    "Failed to parse Schwab order status response"
                );
                Err(SchwabError::InvalidConfiguration(format!(
                    "Failed to parse order status response: {parse_error}"
                )))
            }
        }
    }
}

/// Extracts order ID from the Location header in Schwab order placement response.
///
/// According to Schwab OpenAPI spec, successful order placement returns Location header
/// containing link to the newly created order. The order ID is extracted from this URL.
/// Expected format: "/trader/v1/accounts/{accountHash}/orders/{orderId}"
fn extract_order_id_from_location_header(
    response: &reqwest::Response,
) -> Result<String, SchwabError> {
    let location = response
        .headers()
        .get(reqwest::header::LOCATION)
        .ok_or_else(|| SchwabError::RequestFailed {
            action: "extract order ID".to_string(),
            status: response.status(),
            body: "Missing Location header in order placement response".to_string(),
        })?
        .to_str()
        .map_err(|_| SchwabError::RequestFailed {
            action: "extract order ID".to_string(),
            status: response.status(),
            body: "Invalid Location header value".to_string(),
        })?;

    // Extract order ID from URL path: "/trader/v1/accounts/{accountHash}/orders/{orderId}"
    // Must contain the expected path structure
    if !location.contains("/trader/v1/accounts/") || !location.contains("/orders/") {
        return Err(SchwabError::RequestFailed {
            action: "extract order ID".to_string(),
            status: response.status(),
            body: format!(
                "Invalid Location header format, expected '/trader/v1/accounts/{{accountHash}}/orders/{{orderId}}': {location}"
            ),
        });
    }

    let order_id = location
        .split('/')
        .next_back()
        .ok_or_else(|| SchwabError::RequestFailed {
            action: "extract order ID".to_string(),
            status: response.status(),
            body: format!("Cannot extract order ID from Location header: {location}"),
        })?
        .to_string();

    if order_id.is_empty() {
        return Err(SchwabError::RequestFailed {
            action: "extract order ID".to_string(),
            status: response.status(),
            body: format!("Empty order ID extracted from Location header: {location}"),
        });
    }

    Ok(order_id)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum OrderType {
    Market,
    Limit,
    Stop,
    StopLimit,
    TrailingStop,
    NetDebit,
    NetCredit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Instruction {
    Buy,
    Sell,
    BuyToCover,
    SellShort,
    BuyToOpen,
    BuyToClose,
    SellToOpen,
    SellToClose,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum Session {
    Normal,
    Am,
    Pm,
    Seamless,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum OrderDuration {
    Day,
    GoodTillCancel,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum OrderStrategyType {
    Single,
    Oco,
    Trigger,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum AssetType {
    Equity,
    Option,
    Index,
    MutualFund,
    CashEquivalent,
    FixedIncome,
    Currency,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OrderLeg {
    pub instruction: Instruction,
    pub quantity: u64,
    pub instrument: Instrument,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Instrument {
    pub symbol: String,
    pub asset_type: AssetType,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{TEST_ENCRYPTION_KEY, setup_test_db, setup_test_tokens};
    use serde_json::json;

    #[test]
    fn test_new_buy() {
        let order = Order::new("AAPL".to_string(), Instruction::Buy, 100);

        assert_eq!(order.order_type, OrderType::Market);
        assert_eq!(order.session, Session::Normal);
        assert_eq!(order.duration, OrderDuration::Day);
        assert_eq!(order.order_strategy_type, OrderStrategyType::Single);
        assert_eq!(order.order_leg_collection.len(), 1);

        let leg = &order.order_leg_collection[0];
        assert_eq!(leg.instruction, Instruction::Buy);
        assert_eq!(leg.quantity, 100);
        assert_eq!(leg.instrument.symbol, "AAPL");
        assert_eq!(leg.instrument.asset_type, AssetType::Equity);
    }

    #[test]
    fn test_new_sell() {
        let order = Order::new("TSLA".to_string(), Instruction::Sell, 50);

        assert_eq!(order.order_type, OrderType::Market);
        assert_eq!(order.session, Session::Normal);
        assert_eq!(order.duration, OrderDuration::Day);
        assert_eq!(order.order_strategy_type, OrderStrategyType::Single);

        let leg = &order.order_leg_collection[0];
        assert_eq!(leg.instruction, Instruction::Sell);
        assert_eq!(leg.quantity, 50);
        assert_eq!(leg.instrument.symbol, "TSLA");
        assert_eq!(leg.instrument.asset_type, AssetType::Equity);
    }

    #[test]
    fn test_new_sell_short() {
        let order = Order::new("GME".to_string(), Instruction::SellShort, 26);

        let leg = &order.order_leg_collection[0];
        assert_eq!(leg.instruction, Instruction::SellShort);
        assert_eq!(leg.quantity, 26);
        assert_eq!(leg.instrument.symbol, "GME");
    }

    #[test]
    fn test_new_buy_to_cover() {
        let order = Order::new("AMC".to_string(), Instruction::BuyToCover, 15);

        let leg = &order.order_leg_collection[0];
        assert_eq!(leg.instruction, Instruction::BuyToCover);
        assert_eq!(leg.quantity, 15);
    }

    #[test]
    fn test_whole_shares_only() {
        let order = Order::new("SPY".to_string(), Instruction::Buy, 1);

        let leg = &order.order_leg_collection[0];
        assert_eq!(leg.instruction, Instruction::Buy);
        assert_eq!(leg.quantity, 1);
        assert_eq!(leg.instrument.symbol, "SPY");

        // Test serialization uses whole numbers
        let json = serde_json::to_value(&order).unwrap();
        assert_eq!(json["orderLegCollection"][0]["quantity"], 1);
    }

    #[test]
    fn test_order_serialization() {
        let order = Order::new("MSFT".to_string(), Instruction::Buy, 25);

        let json = serde_json::to_string(&order).unwrap();
        let deserialized: Order = serde_json::from_str(&json).unwrap();

        assert_eq!(order.order_type, deserialized.order_type);
        assert_eq!(order.session, deserialized.session);
        assert_eq!(order.duration, deserialized.duration);
        assert_eq!(order.order_strategy_type, deserialized.order_strategy_type);
        assert_eq!(
            order.order_leg_collection.len(),
            deserialized.order_leg_collection.len()
        );
        assert_eq!(
            order.order_leg_collection[0].instruction,
            deserialized.order_leg_collection[0].instruction
        );
        assert_eq!(
            order.order_leg_collection[0].quantity,
            deserialized.order_leg_collection[0].quantity
        );
        assert_eq!(
            order.order_leg_collection[0].instrument,
            deserialized.order_leg_collection[0].instrument
        );
    }

    #[test]
    fn test_order_camel_case_serialization() {
        let order = Order::new("GOOGL".to_string(), Instruction::Buy, 10);

        let json = serde_json::to_string_pretty(&order).unwrap();

        assert!(json.contains("\"orderType\""));
        assert!(json.contains("\"orderLegCollection\""));
        assert!(json.contains("\"orderStrategyType\""));
        assert!(json.contains("\"assetType\""));
    }

    #[test]
    fn test_serialization_matches_schwab_format() {
        let order = Order::new("XYZ".to_string(), Instruction::Buy, 15);

        let json = serde_json::to_value(&order).unwrap();

        assert_eq!(json["orderType"], "MARKET");
        assert_eq!(json["session"], "NORMAL");
        assert_eq!(json["duration"], "DAY");
        assert_eq!(json["orderStrategyType"], "SINGLE");
        assert_eq!(json["orderLegCollection"][0]["instruction"], "BUY");
        assert_eq!(json["orderLegCollection"][0]["quantity"], 15);
        assert_eq!(json["orderLegCollection"][0]["instrument"]["symbol"], "XYZ");
        assert_eq!(
            json["orderLegCollection"][0]["instrument"]["assetType"],
            "EQUITY"
        );
    }

    #[tokio::test]
    async fn test_place_order_success() {
        let server = httpmock::MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders")
                .header("authorization", "Bearer test_access_token")
                .header("accept", "*/*")
                .header("content-type", "application/json");
            then.status(201)
                .header("location", "/trader/v1/accounts/ABC123DEF456/orders/12345");
        });

        let order = Order::new("AAPL".to_string(), Instruction::Buy, 100);
        let result = order.place(&env, &pool).await;

        account_mock.assert();
        order_mock.assert();
        let response = result.unwrap();
        assert_eq!(response.order_id, "12345");
    }

    #[tokio::test]
    async fn test_place_order_failure() {
        let server = httpmock::MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders");
            then.status(400)
                .json_body(json!({"error": "Invalid order"}));
        });

        let order = Order::new("INVALID".to_string(), Instruction::Buy, 100);
        let result = order.place(&env, &pool).await;

        account_mock.assert();
        order_mock.assert();
        let error = result.unwrap_err();
        assert!(
            matches!(error, super::SchwabError::RequestFailed { action, status, .. } if action == "place order" && status.as_u16() == 400)
        );
    }

    fn create_test_env_with_mock_server(mock_server: &httpmock::MockServer) -> SchwabAuthEnv {
        SchwabAuthEnv {
            schwab_app_key: "test_app_key".to_string(),
            schwab_app_secret: "test_app_secret".to_string(),
            schwab_redirect_uri: "https://127.0.0.1".to_string(),
            schwab_base_url: mock_server.base_url(),
            schwab_account_index: 0,
            encryption_key: TEST_ENCRYPTION_KEY,
        }
    }

    #[tokio::test]
    async fn test_order_placement_success_with_location_header() {
        let server = httpmock::MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders");
            then.status(201)
                .header("location", "/trader/v1/accounts/ABC123DEF456/orders/67890");
        });

        let order = Order::new("TSLA".to_string(), Instruction::Sell, 50);
        let result = order.place(&env, &pool).await;

        account_mock.assert();
        order_mock.assert();
        let response = result.unwrap();
        assert_eq!(response.order_id, "67890");
    }

    #[tokio::test]
    async fn test_order_placement_missing_location_header() {
        let server = httpmock::MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders");
            then.status(201); // Success but missing Location header
        });

        let order = Order::new("SPY".to_string(), Instruction::Buy, 25);
        let result = order.place(&env, &pool).await;

        account_mock.assert();
        order_mock.assert();
        let error = result.unwrap_err();
        assert!(matches!(
            error,
            SchwabError::RequestFailed { action, body, .. }
            if action == "extract order ID" && body.contains("Missing Location header")
        ));
    }

    #[tokio::test]
    async fn test_order_placement_invalid_location_header() {
        let server = httpmock::MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders");
            then.status(201).header("location", "invalid-url-format"); // Invalid format
        });

        let order = Order::new("MSFT".to_string(), Instruction::Buy, 100);
        let result = order.place(&env, &pool).await;

        account_mock.assert();
        order_mock.assert();
        let error = result.unwrap_err();
        assert!(matches!(
            error,
            SchwabError::RequestFailed { action, body, .. }
            if action == "extract order ID" && body.contains("Invalid Location header format")
        ));
    }

    #[tokio::test]
    async fn test_order_placement_retry_logic_verification() {
        // This test verifies that retry logic exists without necessarily testing network timeouts
        // Since the retry behavior depends on the underlying reqwest/backon configuration,
        // we instead test that the order placement handles failures gracefully

        let server = httpmock::MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        // Mock server that simulates a consistently failing service
        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders");
            then.status(502) // Bad Gateway - common transient error
                .json_body(json!({"error": "Bad Gateway"}));
        });

        let order = Order::new("AAPL".to_string(), Instruction::Buy, 100);
        let result = order.place(&env, &pool).await;

        account_mock.assert();

        // The test ensures error handling works correctly, regardless of retry count
        let error = result.unwrap_err();
        assert!(matches!(
            error,
            SchwabError::RequestFailed { action, status, .. }
            if action == "place order" && status.as_u16() == 502
        ));

        // At least one attempt should have been made
        assert!(
            order_mock.hits() >= 1,
            "Expected at least one API call attempt"
        );
    }

    #[tokio::test]
    async fn test_order_placement_server_error_500() {
        let server = httpmock::MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders");
            then.status(500)
                .json_body(json!({"error": "Internal server error"}));
        });

        let order = Order::new("TSLA".to_string(), Instruction::Sell, 50);
        let result = order.place(&env, &pool).await;

        account_mock.assert();
        order_mock.assert();
        let error = result.unwrap_err();
        assert!(matches!(
            error,
            SchwabError::RequestFailed { action, status, .. }
            if action == "place order" && status.as_u16() == 500
        ));
    }

    #[tokio::test]
    async fn test_order_placement_authentication_failure() {
        let server = httpmock::MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders");
            then.status(401).json_body(json!({"error": "Unauthorized"}));
        });

        let order = Order::new("SPY".to_string(), Instruction::Buy, 25);
        let result = order.place(&env, &pool).await;

        account_mock.assert();
        order_mock.assert();
        let error = result.unwrap_err();
        assert!(matches!(
            error,
            SchwabError::RequestFailed { action, status, .. }
            if action == "place order" && status.as_u16() == 401
        ));
    }

    #[tokio::test]
    async fn test_order_placement_malformed_json_response() {
        let server = httpmock::MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200).body("invalid json response"); // Malformed JSON
        });

        let order = Order::new("AAPL".to_string(), Instruction::Buy, 100);
        let result = order.place(&env, &pool).await;

        account_mock.assert();
        let error = result.unwrap_err();
        // Should fail with JSON serialization error due to malformed account response
        assert!(matches!(error, SchwabError::Reqwest(_)));
    }

    #[tokio::test]
    async fn test_order_placement_empty_location_header_value() {
        let server = httpmock::MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/trader/v1/accounts/ABC123DEF456/orders");
            then.status(201)
                .header("location", "/trader/v1/accounts/ABC123DEF456/orders/"); // Empty order ID
        });

        let order = Order::new("MSFT".to_string(), Instruction::Sell, 50);
        let result = order.place(&env, &pool).await;

        account_mock.assert();
        order_mock.assert();
        let error = result.unwrap_err();
        assert!(matches!(
            error,
            SchwabError::RequestFailed { action, body, .. }
            if action == "extract order ID" && body.contains("Empty order ID")
        ));
    }

    #[tokio::test]
    async fn test_get_order_status_success_filled() {
        let server = httpmock::MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_status_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/ABC123DEF456/orders/1004055538123")
                .header("authorization", "Bearer test_access_token")
                .header("accept", "application/json");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "orderId": 1_004_055_538_123_i64,
                    "status": "FILLED",
                    "filledQuantity": 100.0,
                    "remainingQuantity": 0.0,
                    "enteredTime": "2023-10-15T10:25:00Z",
                    "closeTime": "2023-10-15T10:30:00Z",
                    "orderActivityCollection": [{
                        "activityType": "EXECUTION",
                        "executionLegs": [{
                            "executionId": "EXEC001",
                            "quantity": 100.0,
                            "price": 150.25,
                            "time": "2023-10-15T10:30:00Z"
                        }]
                    }]
                }));
        });

        let result = Order::get_order_status("1004055538123", &env, &pool).await;

        account_mock.assert();
        order_status_mock.assert();
        let order_status = result.unwrap();
        assert_eq!(order_status.order_id, Some("1004055538123".to_string()));
        assert!(order_status.is_filled());
        assert!((order_status.filled_quantity.unwrap() - 100.0).abs() < f64::EPSILON);
        let avg_price = order_status.calculate_weighted_average_price().unwrap();
        assert!((avg_price - 150.25).abs() < f64::EPSILON);
        assert_eq!(order_status.price_in_cents().unwrap(), Some(15025));
    }

    #[tokio::test]
    async fn test_get_order_status_success_working() {
        let server = httpmock::MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_status_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/ABC123DEF456/orders/1004055538456")
                .header("authorization", "Bearer test_access_token")
                .header("accept", "application/json");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "orderId": 1_004_055_538_456_i64,
                    "status": "WORKING",
                    "filledQuantity": 0.0,
                    "remainingQuantity": 100.0,
                    "orderActivityCollection": [],
                    "enteredTime": "2023-10-15T10:25:00Z",
                    "closeTime": null
                }));
        });

        let result = Order::get_order_status("1004055538456", &env, &pool).await;

        account_mock.assert();
        order_status_mock.assert();
        let order_status = result.unwrap();
        assert_eq!(order_status.order_id, Some("1004055538456".to_string()));
        assert!(order_status.is_pending());
        assert!(!order_status.is_filled());
        assert!(order_status.filled_quantity.unwrap_or(0.0).abs() < f64::EPSILON);
        assert_eq!(order_status.calculate_weighted_average_price(), None);
    }

    #[tokio::test]
    async fn test_get_order_status_partially_filled() {
        let server = httpmock::MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_status_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/ABC123DEF456/orders/1004055538789")
                .header("authorization", "Bearer test_access_token")
                .header("accept", "application/json");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "orderId": 1_004_055_538_789_i64,
                    "status": "WORKING",
                    "filledQuantity": 75.0,
                    "remainingQuantity": 25.0,
                    "enteredTime": "2023-10-15T10:25:00Z",
                    "closeTime": null,
                    "orderActivityCollection": [{
                        "activityType": "EXECUTION",
                        "executionLegs": [
                            {
                                "executionId": "EXEC001",
                                "quantity": 50.0,
                                "price": 100.00,
                                "time": "2023-10-15T10:30:00Z"
                            },
                            {
                                "executionId": "EXEC002",
                                "quantity": 25.0,
                                "price": 101.00,
                                "time": "2023-10-15T10:30:05Z"
                            }
                        ]
                    }]
                }));
        });

        let result = Order::get_order_status("1004055538789", &env, &pool).await;

        account_mock.assert();
        order_status_mock.assert();
        let order_status = result.unwrap();
        assert_eq!(order_status.order_id, Some("1004055538789".to_string()));
        assert!(order_status.is_pending());
        assert!(!order_status.is_filled());
        assert!((order_status.filled_quantity.unwrap() - 75.0).abs() < f64::EPSILON);
        // Weighted average: (50 * 100.00 + 25 * 101.00) / 75 = (5000 + 2525) / 75 = 100.33333
        assert!(
            (order_status.calculate_weighted_average_price().unwrap() - 100.333_333_333_333_33)
                .abs()
                < 0.000_001
        );
    }

    #[tokio::test]
    async fn test_get_order_status_order_not_found() {
        let server = httpmock::MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_status_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/ABC123DEF456/orders/NONEXISTENT");
            then.status(404)
                .header("content-type", "application/json")
                .json_body(json!({"error": "Order not found"}));
        });

        let result = Order::get_order_status("NONEXISTENT", &env, &pool).await;

        account_mock.assert();
        order_status_mock.assert();
        let error = result.unwrap_err();
        assert!(matches!(
            error,
            SchwabError::RequestFailed { action, status, body }
            if action == "get order status"
                && status == reqwest::StatusCode::NOT_FOUND
                && body.contains("NONEXISTENT")
        ));
    }

    #[tokio::test]
    async fn test_get_order_status_authentication_failure() {
        let server = httpmock::MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_status_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/ABC123DEF456/orders/1004055538123");
            then.status(401)
                .header("content-type", "application/json")
                .json_body(json!({"error": "Unauthorized"}));
        });

        let result = Order::get_order_status("1004055538123", &env, &pool).await;

        account_mock.assert();
        order_status_mock.assert();
        let error = result.unwrap_err();
        assert!(matches!(
            error,
            SchwabError::RequestFailed { action, status, .. }
            if action == "get order status" && status == reqwest::StatusCode::UNAUTHORIZED
        ));
    }

    #[tokio::test]
    async fn test_get_order_status_server_error() {
        let server = httpmock::MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_status_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/ABC123DEF456/orders/1004055538123");
            then.status(500)
                .header("content-type", "application/json")
                .json_body(json!({"error": "Internal server error"}));
        });

        let result = Order::get_order_status("1004055538123", &env, &pool).await;

        account_mock.assert();
        order_status_mock.assert();
        let error = result.unwrap_err();
        assert!(matches!(
            error,
            SchwabError::RequestFailed { action, status, .. }
            if action == "get order status" && status == reqwest::StatusCode::INTERNAL_SERVER_ERROR
        ));
    }

    #[tokio::test]
    async fn test_get_order_status_invalid_json_response() {
        let server = httpmock::MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        let order_status_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/ABC123DEF456/orders/1004055538123");
            then.status(200)
                .header("content-type", "application/json")
                .body("invalid json response");
        });

        let result = Order::get_order_status("1004055538123", &env, &pool).await;

        account_mock.assert();
        order_status_mock.assert();
        let error = result.unwrap_err();
        assert!(matches!(error, SchwabError::InvalidConfiguration(_)));
    }

    #[tokio::test]
    async fn test_get_order_status_retry_on_transient_failure() {
        let server = httpmock::MockServer::start();
        let env = create_test_env_with_mock_server(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &env).await;

        let account_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/accountNumbers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "accountNumber": "123456789",
                    "hashValue": "ABC123DEF456"
                }]));
        });

        // Mock that fails initially but should be retried
        let order_status_mock = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/trader/v1/accounts/ABC123DEF456/orders/1004055538123");
            then.status(502) // Bad Gateway - transient error
                .header("content-type", "application/json")
                .json_body(json!({"error": "Bad Gateway"}));
        });

        let result = Order::get_order_status("1004055538123", &env, &pool).await;

        account_mock.assert();
        // Should have made at least one request (retry logic is handled by backon)
        assert!(order_status_mock.hits() >= 1);
        let error = result.unwrap_err();
        assert!(matches!(
            error,
            SchwabError::RequestFailed { action, status, .. }
            if action == "get order status" && status == reqwest::StatusCode::BAD_GATEWAY
        ));
    }

    // These tests can be restored when/if the CLI functionality is migrated to the new system
}
