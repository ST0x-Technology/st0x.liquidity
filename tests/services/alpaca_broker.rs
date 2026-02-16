//! Mock Alpaca Broker API for E2E tests.
//!
//! Provides an HTTP mock server that responds to the endpoints the bot calls
//! when interacting with the Alpaca Broker API: account verification, market
//! calendar, positions, order placement, order polling, and crypto conversions.

use chrono::Utc;
use httpmock::prelude::*;
use serde_json::json;

pub const TEST_ACCOUNT_ID: &str = "904837e3-3b76-47ec-b432-046db621571b";
pub const TEST_API_KEY: &str = "e2e_test_key";
pub const TEST_API_SECRET: &str = "e2e_test_secret";

/// Owns the `MockServer` and pre-configures happy-path responses for the
/// Alpaca Broker API. Scenario methods allow per-test customisation.
pub struct AlpacaBrokerMock {
    server: MockServer,
}

impl AlpacaBrokerMock {
    /// Starts the mock server and registers default happy-path endpoints:
    /// account (ACTIVE), market calendar (open all day today), empty positions.
    pub async fn start() -> Self {
        let server = MockServer::start_async().await;

        let today = Utc::now().format("%Y-%m-%d").to_string();

        server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/trading/accounts/{TEST_ACCOUNT_ID}/account"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": TEST_ACCOUNT_ID,
                    "status": "ACTIVE"
                }));
        });

        server.mock(|when, then| {
            when.method(GET).path("/v1/calendar");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "date": today,
                    "open": "00:00",
                    "close": "23:59"
                }]));
        });

        server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/trading/accounts/{TEST_ACCOUNT_ID}/positions"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        Self { server }
    }

    pub fn base_url(&self) -> String {
        self.server.base_url()
    }

    /// Mocks a successful equity order placement returning `order_id` with
    /// status "new".
    pub fn mock_place_equity_order(&self, order_id: &str) {
        let order_id = order_id.to_string();

        self.server.mock(|when, then| {
            when.method(POST)
                .path(format!("/v1/trading/accounts/{TEST_ACCOUNT_ID}/orders"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });
    }

    /// Mocks a filled order status response for the given order.
    pub fn mock_order_filled(&self, order_id: &str, symbol: &str, price: &str) {
        let order_id = order_id.to_string();
        let symbol = symbol.to_string();
        let price = price.to_string();

        self.server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/{TEST_ACCOUNT_ID}/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": symbol,
                    "qty": "100",
                    "side": "buy",
                    "status": "filled",
                    "filled_avg_price": price
                }));
        });
    }

    /// Mocks a crypto USDCUSD conversion order that is immediately filled.
    /// Matches on `json_body_partial` containing `"symbol": "USDCUSD"` to
    /// distinguish from equity orders on the same path.
    pub fn mock_crypto_conversion(&self, order_id: &str, qty: &str) {
        let order_id = order_id.to_string();
        let qty = qty.to_string();

        self.server.mock(|when, then| {
            when.method(POST)
                .path(format!("/v1/trading/accounts/{TEST_ACCOUNT_ID}/orders"))
                .json_body_partial(r#"{"symbol": "USDCUSD"}"#);
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "USDCUSD",
                    "qty": qty,
                    "side": "sell",
                    "status": "filled",
                    "filled_avg_price": "1.0001",
                    "filled_qty": qty,
                    "created_at": "2025-01-06T12:00:00Z"
                }));
        });
    }

    /// Replaces the default empty positions response with a custom JSON array.
    pub fn mock_positions(&self, positions: serde_json::Value) {
        self.server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/trading/accounts/{TEST_ACCOUNT_ID}/positions"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(positions);
        });
    }
}
