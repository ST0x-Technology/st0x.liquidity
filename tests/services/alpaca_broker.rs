//! Autonomous mock Alpaca Broker API for E2E tests.
//!
//! Uses `respond_with` dynamic closures backed by shared `Arc<Mutex<MockState>>`
//! so the mock auto-responds based on request content. Tests configure a mode
//! (happy path, rejected, placement fails) and optionally set initial account
//! state via the builder — no per-request mock setup needed.

use chrono::Utc;
use httpmock::prelude::*;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};
use uuid::Uuid;

pub const TEST_ACCOUNT_ID: &str = "904837e3-3b76-47ec-b432-046db621571b";
pub const TEST_API_KEY: &str = "e2e_test_key";
pub const TEST_API_SECRET: &str = "e2e_test_secret";

/// Controls how the mock responds to order placement and polling.
#[derive(Debug, Clone, Copy)]
pub enum MockMode {
    /// Place succeeds, first poll returns filled.
    HappyPath,
    /// Place succeeds, poll returns rejected.
    OrderRejected,
    /// Place returns HTTP 422.
    PlacementFails,
}

struct MockPosition {
    symbol: String,
    qty: Decimal,
    market_value: Decimal,
}

struct MockAccount {
    cash: Decimal,
    buying_power: Decimal,
    positions: HashMap<String, MockPosition>,
}

struct MockOrder {
    symbol: String,
    qty: String,
    side: String,
    status: String,
    poll_count: usize,
    filled_price: Option<String>,
}

/// A single calendar entry controlling market open/close times.
pub struct CalendarEntry {
    pub date: String,
    pub open: String,
    pub close: String,
}

/// A mock tokenization request tracked in shared state.
struct MockTokenizationRequest {
    tokenization_request_id: String,
    issuer_request_id: String,
    underlying_symbol: String,
    qty: String,
    wallet_address: String,
    status: String,
    poll_count: usize,
    /// Number of polls before transitioning from "pending" to "completed".
    polls_until_complete: usize,
}

struct MockState {
    account: MockAccount,
    orders: HashMap<String, MockOrder>,
    default_fill_price: String,
    /// Per-symbol fill prices, checked before `default_fill_price`.
    symbol_fill_prices: HashMap<String, String>,
    mode: MockMode,
    /// Dynamic calendar entries. The endpoint reads from this on each request.
    calendar_entries: Vec<CalendarEntry>,
    /// Tokenization requests created via the mint endpoint.
    tokenization_requests: Vec<MockTokenizationRequest>,
    /// Number of polls before mint requests transition to "completed".
    tokenization_polls_until_complete: usize,
}

/// Snapshot of a placed order, returned by [`AlpacaBrokerMock::orders`].
pub struct MockOrderSnapshot {
    pub order_id: String,
    pub symbol: String,
    pub qty: String,
    pub side: String,
    pub status: String,
    pub poll_count: usize,
    pub filled_price: Option<String>,
}

/// Snapshot of the broker account, returned by [`AlpacaBrokerMock::account`].
pub struct MockAccountSnapshot {
    pub cash: String,
    pub buying_power: String,
}

/// Snapshot of a position, returned by [`AlpacaBrokerMock::positions`].
pub struct MockPositionSnapshot {
    pub symbol: String,
    pub qty: String,
    pub market_value: String,
}

/// Builder for configuring initial broker state before starting the mock.
pub struct AlpacaBrokerMockBuilder {
    cash: Decimal,
    buying_power: Decimal,
    positions: HashMap<String, MockPosition>,
    fill_price: String,
    symbol_fill_prices: HashMap<String, String>,
    mode: MockMode,
    market_closed: bool,
}

impl AlpacaBrokerMockBuilder {
    pub fn with_cash(mut self, amount: &str) -> Self {
        let parsed = Decimal::from_str(amount).unwrap_or(Decimal::ZERO);
        self.cash = parsed;
        self.buying_power = parsed;
        self
    }

    pub fn with_position(mut self, symbol: &str, qty: &str, market_value: &str) -> Self {
        self.positions.insert(
            symbol.to_string(),
            MockPosition {
                symbol: symbol.to_string(),
                qty: Decimal::from_str(qty).unwrap_or(Decimal::ZERO),
                market_value: Decimal::from_str(market_value).unwrap_or(Decimal::ZERO),
            },
        );
        self
    }

    pub fn with_fill_price(mut self, price: &str) -> Self {
        self.fill_price = price.to_string();
        self
    }

    /// Sets a fill price for a specific symbol, overriding the default.
    pub fn with_symbol_fill_price(mut self, symbol: &str, price: &str) -> Self {
        self.symbol_fill_prices
            .insert(symbol.to_string(), price.to_string());
        self
    }

    pub fn with_mode(mut self, mode: MockMode) -> Self {
        self.mode = mode;
        self
    }

    /// Start the mock with the market closed. Calendar shows today's
    /// open/close times in the past so `is_market_open()` returns false.
    pub fn with_market_closed(mut self) -> Self {
        self.market_closed = true;
        self
    }

    pub async fn build(self) -> AlpacaBrokerMock {
        let today = Utc::now().format("%Y-%m-%d").to_string();

        let calendar_entries = if self.market_closed {
            // Market already closed: open/close both in the past
            vec![CalendarEntry {
                date: today,
                open: "04:00".to_string(),
                close: "04:01".to_string(),
            }]
        } else {
            // Market open all day
            vec![CalendarEntry {
                date: today,
                open: "00:00".to_string(),
                close: "23:59".to_string(),
            }]
        };

        let state = Arc::new(Mutex::new(MockState {
            account: MockAccount {
                cash: self.cash,
                buying_power: self.buying_power,
                positions: self.positions,
            },
            orders: HashMap::new(),
            default_fill_price: self.fill_price,
            symbol_fill_prices: self.symbol_fill_prices,
            mode: self.mode,
            calendar_entries,
            tokenization_requests: Vec::new(),
            tokenization_polls_until_complete: 2,
        }));

        let server = MockServer::start_async().await;
        register_endpoints(&server, &state);

        AlpacaBrokerMock { server, state }
    }
}

/// Owns the `MockServer` and shared state. All endpoints respond dynamically
/// based on `MockState`, which updates as orders are placed and filled.
pub struct AlpacaBrokerMock {
    server: MockServer,
    state: Arc<Mutex<MockState>>,
}

impl AlpacaBrokerMock {
    /// Returns a builder for configuring the mock's initial state.
    pub fn start() -> AlpacaBrokerMockBuilder {
        AlpacaBrokerMockBuilder {
            cash: Decimal::from(100_000),
            buying_power: Decimal::from(100_000),
            positions: HashMap::new(),
            fill_price: "150.00".to_string(),
            symbol_fill_prices: HashMap::new(),
            mode: MockMode::HappyPath,
            market_closed: false,
        }
    }

    pub fn base_url(&self) -> String {
        self.server.base_url()
    }

    /// Exposes the underlying mock server for registering additional
    /// endpoints (e.g., tokenization or wallet endpoints) in tests that
    /// need Alpaca services beyond the core broker API.
    ///
    /// The conductor resolves all Alpaca services (broker, tokenization,
    /// wallet) from `AlpacaBrokerApiCtx.base_url()`, which points at this
    /// mock server in e2e tests. Rebalancing tests will need to register
    /// tokenization endpoints here once ERC-4626 infrastructure is ready.
    pub fn server(&self) -> &MockServer {
        &self.server
    }

    /// Changes the default fill price for subsequent order fills.
    pub fn set_fill_price(&self, price: &str) {
        lock(&self.state).default_fill_price = price.to_string();
    }

    /// Sets a fill price for a specific symbol, overriding the default.
    pub fn set_symbol_fill_price(&self, symbol: &str, price: &str) {
        lock(&self.state)
            .symbol_fill_prices
            .insert(symbol.to_string(), price.to_string());
    }

    /// Changes the mock mode for subsequent requests.
    pub fn set_mode(&self, mode: MockMode) {
        lock(&self.state).mode = mode;
    }

    /// Switches the calendar to market-open (all day) for today.
    pub fn set_market_open(&self) {
        let today = Utc::now().format("%Y-%m-%d").to_string();
        lock(&self.state).calendar_entries = vec![CalendarEntry {
            date: today,
            open: "00:00".to_string(),
            close: "23:59".to_string(),
        }];
    }

    /// Replaces the calendar entries with custom values.
    pub fn set_calendar(&self, entries: Vec<CalendarEntry>) {
        lock(&self.state).calendar_entries = entries;
    }

    /// Returns a snapshot of all orders placed through this mock.
    pub fn orders(&self) -> Vec<MockOrderSnapshot> {
        let state = lock(&self.state);
        state
            .orders
            .iter()
            .map(|(order_id, order)| MockOrderSnapshot {
                order_id: order_id.clone(),
                symbol: order.symbol.clone(),
                qty: order.qty.clone(),
                side: order.side.clone(),
                status: order.status.clone(),
                poll_count: order.poll_count,
                filled_price: order.filled_price.clone(),
            })
            .collect()
    }

    /// Returns a snapshot of the current account state.
    pub fn account(&self) -> MockAccountSnapshot {
        let state = lock(&self.state);
        MockAccountSnapshot {
            cash: state.account.cash.to_string(),
            buying_power: state.account.buying_power.to_string(),
        }
    }

    /// Registers tokenization API endpoints (mint + request polling) on
    /// this mock server. Call after `build()` for tests that exercise the
    /// equity rebalancing pipeline.
    ///
    /// Endpoints registered:
    /// - `POST /v1/accounts/{id}/tokenization/mint` → creates a pending request
    /// - `GET /v1/accounts/{id}/tokenization/requests` → returns all requests,
    ///   transitioning pending requests to "completed" after N polls
    pub fn register_tokenization_endpoints(&self) {
        register_mint_endpoint(&self.server, &self.state);
        register_tokenization_requests_endpoint(&self.server, &self.state);
    }

    /// Returns a snapshot of all current positions.
    pub fn positions(&self) -> Vec<MockPositionSnapshot> {
        let state = lock(&self.state);
        state
            .account
            .positions
            .values()
            .map(|pos| MockPositionSnapshot {
                symbol: pos.symbol.clone(),
                qty: pos.qty.to_string(),
                market_value: pos.market_value.to_string(),
            })
            .collect()
    }
}

/// Locks the mutex, recovering from poisoning (a previous holder panicked).
/// Safe for test mocks — we still want to inspect state after panics.
fn lock(state: &Mutex<MockState>) -> MutexGuard<'_, MockState> {
    match state.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Registers all dynamic endpoints on the mock server.
fn register_endpoints(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    register_account_endpoint(server, state);
    register_calendar_endpoint(server, state);
    register_positions_endpoint(server, state);
    register_asset_endpoint(server);
    register_order_placement_endpoint(server, state);
    register_order_status_endpoint(server, state);
}

fn register_account_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);
    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/v1/trading/accounts/{TEST_ACCOUNT_ID}/account"));
        then.respond_with(move |_request: &HttpMockRequest| {
            let state = lock(&state);
            json_response(
                200,
                &json!({
                    "id": TEST_ACCOUNT_ID,
                    "status": "ACTIVE",
                    "cash": state.account.cash.to_string(),
                    "buying_power": state.account.buying_power.to_string(),
                }),
            )
        });
    });
}

fn register_calendar_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);
    server.mock(|when, then| {
        when.method(GET).path("/v1/calendar");
        then.respond_with(move |_request: &HttpMockRequest| {
            let entries: Vec<Value> = lock(&state)
                .calendar_entries
                .iter()
                .map(|entry| {
                    json!({
                        "date": entry.date,
                        "open": entry.open,
                        "close": entry.close
                    })
                })
                .collect();
            json_response(200, &Value::Array(entries))
        });
    });
}

fn register_positions_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);
    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/v1/trading/accounts/{TEST_ACCOUNT_ID}/positions"));
        then.respond_with(move |_request: &HttpMockRequest| {
            let positions: Vec<Value> = {
                let state = lock(&state);
                state
                    .account
                    .positions
                    .values()
                    .map(|pos| {
                        json!({
                            "symbol": pos.symbol,
                            "qty": pos.qty.to_string(),
                            "market_value": pos.market_value.to_string(),
                            "side": "long",
                            "avg_entry_price": "0",
                        })
                    })
                    .collect()
            };
            json_response(200, &Value::Array(positions))
        });
    });
}

fn register_asset_endpoint(server: &MockServer) {
    server.mock(|when, then| {
        when.method(GET).path_prefix("/v1/assets/");
        then.respond_with(|request: &HttpMockRequest| {
            let path = request.uri().path().to_string();
            let symbol = path.strip_prefix("/v1/assets/").unwrap_or("UNKNOWN");
            json_response(
                200,
                &json!({
                    "id": "00000000-0000-0000-0000-000000000000",
                    "symbol": symbol,
                    "status": "active",
                    "tradable": true,
                }),
            )
        });
    });
}

fn register_order_placement_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);
    server.mock(|when, then| {
        when.method(POST)
            .path(format!("/v1/trading/accounts/{TEST_ACCOUNT_ID}/orders"));
        then.respond_with(move |request: &HttpMockRequest| {
            let body: Value = serde_json::from_slice(request.body().as_ref()).unwrap_or(json!({}));

            let symbol = body["symbol"].as_str().unwrap_or("UNKNOWN").to_string();
            let qty = body["qty"].as_str().unwrap_or("0").to_string();
            let side = body["side"].as_str().unwrap_or("buy").to_string();
            let order_id = Uuid::new_v4().to_string();

            let mut state = lock(&state);

            if matches!(state.mode, MockMode::PlacementFails) {
                return json_response(422, &json!({"message": "order rejected"}));
            }

            if symbol == "USDCUSD" {
                return handle_crypto_order(&mut state, &order_id, &symbol, &qty, &side);
            }

            // Equity order: store as "new"
            state.orders.insert(
                order_id.clone(),
                MockOrder {
                    symbol: symbol.clone(),
                    qty: qty.clone(),
                    side: side.clone(),
                    status: "new".to_string(),
                    poll_count: 0,
                    filled_price: None,
                },
            );
            drop(state);

            json_response(
                200,
                &json!({
                    "id": order_id,
                    "symbol": symbol,
                    "qty": qty,
                    "side": side,
                    "status": "new",
                    "filled_avg_price": null,
                }),
            )
        });
    });
}

/// Handles crypto (USDCUSD) orders which fill immediately.
fn handle_crypto_order(
    state: &mut MockState,
    order_id: &str,
    symbol: &str,
    qty: &str,
    side: &str,
) -> HttpMockResponse {
    let qty_dec = Decimal::from_str(qty).unwrap_or(Decimal::ZERO);

    if side == "buy" {
        state.account.cash -= qty_dec;
    } else {
        state.account.cash += qty_dec;
    }

    state.orders.insert(
        order_id.to_string(),
        MockOrder {
            symbol: symbol.to_string(),
            qty: qty.to_string(),
            side: side.to_string(),
            status: "filled".to_string(),
            poll_count: 0,
            filled_price: Some("1.00".to_string()),
        },
    );

    json_response(
        200,
        &json!({
            "id": order_id,
            "symbol": symbol,
            "qty": qty,
            "side": side,
            "status": "filled",
            "filled_avg_price": "1.00",
            "filled_qty": qty,
            "created_at": "2025-01-06T12:00:00Z"
        }),
    )
}

fn register_order_status_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);
    let prefix = format!("/v1/trading/accounts/{TEST_ACCOUNT_ID}/orders/");

    server.mock(|when, then| {
        when.method(GET).path_prefix(&prefix);
        then.respond_with(move |request: &HttpMockRequest| {
            let path = request.uri().path().to_string();

            let order_id = path.strip_prefix(&prefix).unwrap_or("").to_string();

            let response_body = {
                let mut state = lock(&state);

                if !state.orders.contains_key(&order_id) {
                    return json_response(404, &json!({"message": "order not found"}));
                }

                // Increment poll count
                if let Some(order) = state.orders.get_mut(&order_id) {
                    order.poll_count += 1;
                }

                let mode = state.mode;

                if matches!(mode, MockMode::HappyPath) {
                    let symbol = state.orders[&order_id].symbol.clone();
                    let fill_price_str = state
                        .symbol_fill_prices
                        .get(&symbol)
                        .cloned()
                        .unwrap_or_else(|| state.default_fill_price.clone());

                    apply_happy_path_fill(&mut state, &order_id, &fill_price_str);
                }

                if matches!(mode, MockMode::OrderRejected)
                    && let Some(order) = state.orders.get_mut(&order_id)
                {
                    order.status = "rejected".to_string();
                }

                let order = &state.orders[&order_id];
                let body = json!({
                    "id": order_id,
                    "symbol": order.symbol,
                    "qty": order.qty,
                    "side": order.side,
                    "status": order.status,
                    "filled_avg_price": order.filled_price,
                });
                drop(state);
                body
            };

            json_response(200, &response_body)
        });
    });
}

/// Transitions a "new" order to "filled" and updates account balances.
fn apply_happy_path_fill(state: &mut MockState, order_id: &str, fill_price_str: &str) {
    let should_fill = state
        .orders
        .get(order_id)
        .is_some_and(|o| o.status == "new");
    if !should_fill {
        return;
    }

    let symbol = state.orders[order_id].symbol.clone();
    let qty_str = state.orders[order_id].qty.clone();
    let side = state.orders[order_id].side.clone();

    let qty = Decimal::from_str(&qty_str).unwrap_or(Decimal::ZERO);
    let fill_price = Decimal::from_str(fill_price_str).unwrap_or(Decimal::ZERO);

    // Update order status
    if let Some(order) = state.orders.get_mut(order_id) {
        order.status = "filled".to_string();
        order.filled_price = Some(fill_price_str.to_string());
    }

    // Update account balances
    if side == "buy" {
        state.account.cash -= qty * fill_price;
        let position = state
            .account
            .positions
            .entry(symbol.clone())
            .or_insert_with(|| MockPosition {
                symbol,
                qty: Decimal::ZERO,
                market_value: Decimal::ZERO,
            });
        position.qty += qty;
        position.market_value += qty * fill_price;
    } else {
        state.account.cash += qty * fill_price;
        if let Some(position) = state.account.positions.get_mut(&symbol) {
            position.qty -= qty;
            position.market_value -= qty * fill_price;
        }
    }
}

// ── Tokenization endpoints ───────────────────────────────────────────

fn register_mint_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);

    server.mock(|when, then| {
        when.method(POST)
            .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/tokenization/mint"));
        then.respond_with(move |request: &HttpMockRequest| {
            let body: Value = serde_json::from_slice(request.body().as_ref()).unwrap_or(json!({}));

            let underlying_symbol = body["underlying_symbol"]
                .as_str()
                .unwrap_or("UNKNOWN")
                .to_string();
            let qty = body["qty"].as_str().unwrap_or("0").to_string();
            let wallet_address = body["wallet_address"].as_str().unwrap_or("").to_string();
            let issuer_request_id = body["issuer_request_id"].as_str().unwrap_or("").to_string();

            let tokenization_request_id = Uuid::new_v4().to_string();

            {
                let mut state = lock(&state);
                let polls_until_complete = state.tokenization_polls_until_complete;

                state.tokenization_requests.push(MockTokenizationRequest {
                    tokenization_request_id: tokenization_request_id.clone(),
                    issuer_request_id: issuer_request_id.clone(),
                    underlying_symbol: underlying_symbol.clone(),
                    qty: qty.clone(),
                    wallet_address: wallet_address.clone(),
                    status: "pending".to_string(),
                    poll_count: 0,
                    polls_until_complete,
                });
            }

            json_response(
                200,
                &json!({
                    "tokenization_request_id": tokenization_request_id,
                    "type": "mint",
                    "status": "pending",
                    "underlying_symbol": underlying_symbol,
                    "token_symbol": format!("t{underlying_symbol}"),
                    "qty": qty,
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address": wallet_address,
                    "issuer_request_id": issuer_request_id,
                    "tx_hash": "",
                    "created_at": Utc::now().to_rfc3339(),
                }),
            )
        });
    });
}

fn register_tokenization_requests_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);

    server.mock(|when, then| {
        when.method(GET).path(format!(
            "/v1/accounts/{TEST_ACCOUNT_ID}/tokenization/requests"
        ));
        then.respond_with(move |_request: &HttpMockRequest| {
            let requests: Vec<Value> = {
                let mut state = lock(&state);

                // Advance each pending request's poll count and transition
                // to "completed" once the threshold is reached.
                for request in &mut state.tokenization_requests {
                    if request.status == "pending" {
                        request.poll_count += 1;
                        if request.poll_count >= request.polls_until_complete {
                            request.status = "completed".to_string();
                        }
                    }
                }

                state
                    .tokenization_requests
                    .iter()
                    .map(|request| {
                        // Completed mint requests include a fake tx_hash to
                        // represent the onchain token transfer from Alpaca.
                        let tx_hash = if request.status == "completed" {
                            format!("0x{}", "ab".repeat(32))
                        } else {
                            String::new()
                        };

                        json!({
                            "tokenization_request_id": request.tokenization_request_id,
                            "type": "mint",
                            "status": request.status,
                            "underlying_symbol": request.underlying_symbol,
                            "token_symbol": format!("t{}", request.underlying_symbol),
                            "qty": request.qty,
                            "issuer": "st0x",
                            "network": "base",
                            "wallet_address": request.wallet_address,
                            "issuer_request_id": request.issuer_request_id,
                            "tx_hash": tx_hash,
                            "created_at": "2025-01-01T00:00:00Z",
                        })
                    })
                    .collect()
            };

            json_response(200, &Value::Array(requests))
        });
    });
}

/// Builds an `HttpMockResponse` with JSON content-type and serialized body.
fn json_response(status: u16, body: &Value) -> HttpMockResponse {
    let serialized = serde_json::to_vec(body).unwrap_or_default();
    HttpMockResponse {
        status: Some(status),
        headers: Some(vec![(
            "content-type".to_string(),
            "application/json".to_string(),
        )]),
        body: Some(serialized.into()),
    }
}
