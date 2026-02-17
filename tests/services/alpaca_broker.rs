//! Mock Alpaca Broker API for E2E tests.
//!
//! Provides an HTTP mock server that responds to the endpoints the bot calls
//! when interacting with the Alpaca Broker API: account verification, market
//! calendar, positions, order placement, order polling, and crypto conversions.
//!
//! Scenario methods register mocks and store expectations internally. After
//! the bot runs, call `assert_*` methods with expected values to verify
//! the bot sent the correct requests.

use chrono::Utc;
use httpmock::Mock;
use httpmock::prelude::*;
use serde_json::json;
use std::cell::RefCell;

pub const TEST_ACCOUNT_ID: &str = "904837e3-3b76-47ec-b432-046db621571b";
pub const TEST_API_KEY: &str = "e2e_test_key";
pub const TEST_API_SECRET: &str = "e2e_test_secret";

/// Captured expectation for an equity order placement.
struct PlacedOrderExpectation {
    mock_id: usize,
    symbol: String,
    side: String,
}

/// Captured expectation for an order fill poll.
struct FilledOrderExpectation {
    mock_id: usize,
    order_id: String,
}

/// Owns the `MockServer` and pre-configures happy-path responses for the
/// Alpaca Broker API. Scenario methods store expectations internally for
/// later assertions.
pub struct AlpacaBrokerMock {
    server: MockServer,
    placed_orders: RefCell<Vec<PlacedOrderExpectation>>,
    filled_orders: RefCell<Vec<FilledOrderExpectation>>,
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

        Self {
            server,
            placed_orders: RefCell::new(Vec::new()),
            filled_orders: RefCell::new(Vec::new()),
        }
    }

    pub fn base_url(&self) -> String {
        self.server.base_url()
    }

    /// Mocks a successful equity order placement. The mock uses strict
    /// `json_body_partial` matching on `symbol`, `side`, and `type`, so
    /// it only responds when the bot sends the correct values.
    pub fn mock_place_equity_order(&self, order_id: &str, symbol: &str, side: &str) {
        let order_id = order_id.to_string();
        let symbol = symbol.to_string();
        let side = side.to_string();

        let mock = self.server.mock(|when, then| {
            when.method(POST)
                .path(format!("/v1/trading/accounts/{TEST_ACCOUNT_ID}/orders"))
                .json_body_partial(
                    json!({
                        "symbol": symbol,
                        "side": side,
                        "type": "market",
                    })
                    .to_string(),
                );
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": symbol,
                    "qty": "100",
                    "side": side,
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        self.placed_orders
            .borrow_mut()
            .push(PlacedOrderExpectation {
                mock_id: mock.id,
                symbol,
                side,
            });
    }

    /// Mocks a filled order status response for the given order.
    pub fn mock_order_filled(&self, order_id: &str, symbol: &str, price: &str) {
        let order_id = order_id.to_string();
        let symbol = symbol.to_string();
        let price = price.to_string();

        let mock = self.server.mock(|when, then| {
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

        self.filled_orders
            .borrow_mut()
            .push(FilledOrderExpectation {
                mock_id: mock.id,
                order_id,
            });
    }

    /// Asserts the bot placed exactly one order matching `symbol` and `side`.
    ///
    /// Looks up the stored expectation for that (symbol, side) pair and
    /// verifies the mock was hit exactly once -- which proves the bot sent
    /// a request with the correct values (enforced by `json_body_partial`).
    pub fn assert_order_placed(&self, symbol: &str, side: &str) {
        let placed = self.placed_orders.borrow();
        let matching = placed
            .iter()
            .find(|order| order.symbol == symbol && order.side == side);

        let Some(order) = matching else {
            panic!(
                "no mock configured for symbol={symbol}, side={side}. \
                 Call mock_place_equity_order first."
            );
        };

        Mock::new(order.mock_id, &self.server).assert();
    }

    /// Asserts the bot polled the fill status for `order_id` at least once.
    pub fn assert_order_filled(&self, order_id: &str) {
        let filled = self.filled_orders.borrow();
        let matching = filled.iter().find(|order| order.order_id == order_id);

        let Some(order) = matching else {
            panic!(
                "no mock configured for order_id={order_id}. \
                 Call mock_order_filled first."
            );
        };

        let hits = Mock::new(order.mock_id, &self.server).hits();
        assert!(
            hits >= 1,
            "Expected order fill endpoint for {order_id} to be polled at least once, \
             got {hits} hits"
        );
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
