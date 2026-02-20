//! Autonomous mock Alpaca Broker API for E2E tests.
//!
//! Uses `respond_with` dynamic closures backed by shared `Arc<Mutex<MockState>>`
//! so the mock auto-responds based on request content. Tests configure a mode
//! (happy path, rejected, placement fails) and optionally set initial account
//! state via the builder — no per-request mock setup needed.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};

use alloy::primitives::Address;
use bon::bon;
use chrono::Utc;
use httpmock::prelude::*;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use st0x_execution::Symbol;
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

pub struct MockPosition {
    pub symbol: Symbol,
    pub qty: Decimal,
    pub market_value: Decimal,
}

struct MockAccount {
    cash: Decimal,
    buying_power: Decimal,
    positions: HashMap<Symbol, MockPosition>,
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

/// A mock wallet transfer tracked in shared state.
struct MockWalletTransfer {
    transfer_id: String,
    direction: String,
    amount: String,
    asset: String,
    from_address: String,
    to_address: String,
    status: String,
    tx_hash: String,
    poll_count: usize,
    /// Number of polls before transitioning from "pending" to "complete".
    polls_until_complete: usize,
}

struct MockState {
    account: MockAccount,
    orders: HashMap<String, MockOrder>,
    symbol_fill_prices: HashMap<Symbol, Decimal>,
    mode: MockMode,
    /// Dynamic calendar entries. The endpoint reads from this on each request.
    calendar_entries: Vec<CalendarEntry>,
    /// Wallet transfers (withdrawals and deposits).
    wallet_transfers: Vec<MockWalletTransfer>,
    /// Alpaca deposit wallet address for incoming USDC.
    alpaca_deposit_address: String,
    /// Whitelisted withdrawal addresses.
    whitelisted_addresses: Vec<WhitelistEntry>,
}

/// A whitelisted address entry in the mock state.
struct WhitelistEntry {
    id: String,
    address: String,
    asset: String,
    chain: String,
    status: String,
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

/// Snapshot of a wallet transfer, returned by
/// [`AlpacaBrokerMock::wallet_transfers`].
pub struct MockWalletTransferSnapshot {
    pub transfer_id: String,
    pub direction: String,
    pub amount: String,
    pub asset: String,
    pub status: String,
}

/// Owns the `MockServer` and shared state. All endpoints respond dynamically
/// based on `MockState`, which updates as orders are placed and filled.
pub struct AlpacaBrokerMock {
    server: MockServer,
    state: Arc<Mutex<MockState>>,
}

#[bon]
impl AlpacaBrokerMock {
    /// Starts the mock server with all core broker + tokenization endpoints
    /// registered. Optionally configure per-symbol fill prices.
    #[builder]
    pub async fn start(
        symbol_fill_prices: Vec<(Symbol, Decimal)>,
        symbol_positions: Vec<MockPosition>,
    ) -> Self {
        let today = Utc::now().format("%Y-%m-%d").to_string();

        // Market open all day by default
        let calendar_entries = vec![CalendarEntry {
            date: today,
            open: "00:00".to_string(),
            close: "23:59".to_string(),
        }];

        let symbol_fill_prices = symbol_fill_prices.into_iter().collect();
        let positions = symbol_positions
            .into_iter()
            .map(|pos| (pos.symbol.clone(), pos))
            .collect();

        let state = Arc::new(Mutex::new(MockState {
            account: MockAccount {
                cash: Decimal::from(100_000),
                buying_power: Decimal::from(100_000),
                positions,
            },
            orders: HashMap::new(),
            symbol_fill_prices,
            mode: MockMode::HappyPath,
            calendar_entries,
            wallet_transfers: Vec::new(),
            alpaca_deposit_address: String::new(),
            whitelisted_addresses: Vec::new(),
        }));

        let server = MockServer::start_async().await;
        register_endpoints(&server, &state);

        Self { server, state }
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

    /// Sets a fill price for a specific symbol.
    pub fn set_symbol_fill_price(&self, symbol: Symbol, price: Decimal) {
        lock(&self.state).symbol_fill_prices.insert(symbol, price);
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

    /// Switches the calendar to market-closed for today.
    pub fn set_market_closed(&self) {
        let today = Utc::now().format("%Y-%m-%d").to_string();
        lock(&self.state).calendar_entries = vec![CalendarEntry {
            date: today,
            open: "04:00".to_string(),
            close: "04:01".to_string(),
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

    /// Registers wallet API endpoints (whitelist, transfers, deposit address)
    /// on this mock server. Call after `build()` for tests that exercise the
    /// USDC rebalancing pipeline.
    ///
    /// `owner_address` is the market maker's Ethereum address, pre-approved
    /// in the whitelist for USDC withdrawals.
    pub fn register_wallet_endpoints(&self, owner_address: Address) {
        let owner_hex = format!("{owner_address:#x}");

        // Pre-populate whitelist with the owner address as approved
        {
            let mut state = lock(&self.state);
            state.alpaca_deposit_address = format!("{:#x}", Address::random());
            state.whitelisted_addresses.push(WhitelistEntry {
                id: Uuid::new_v4().to_string(),
                address: owner_hex,
                asset: "USDC".to_string(),
                chain: "ETH".to_string(),
                status: "approved".to_string(),
            });
        }

        register_whitelist_get_endpoint(&self.server, &self.state);
        register_whitelist_post_endpoint(&self.server, &self.state);
        register_wallet_address_endpoint(&self.server, &self.state);
        register_wallet_transfers_post_endpoint(&self.server, &self.state);
        register_wallet_transfers_get_endpoint(&self.server, &self.state);
    }

    /// Returns a snapshot of all wallet transfers.
    pub fn wallet_transfers(&self) -> Vec<MockWalletTransferSnapshot> {
        let state = lock(&self.state);
        state
            .wallet_transfers
            .iter()
            .map(|transfer| MockWalletTransferSnapshot {
                transfer_id: transfer.transfer_id.clone(),
                direction: transfer.direction.clone(),
                amount: transfer.amount.clone(),
                asset: transfer.asset.clone(),
                status: transfer.status.clone(),
            })
            .collect()
    }

    /// Returns a snapshot of all current positions.
    pub fn positions(&self) -> Vec<MockPositionSnapshot> {
        let state = lock(&self.state);
        state
            .account
            .positions
            .values()
            .map(|pos| MockPositionSnapshot {
                symbol: pos.symbol.inner(),
                qty: pos.qty.to_string(),
                market_value: pos.market_value.to_string(),
            })
            .collect()
    }
}

/// Locks the mutex, recovering from poisoning (a previous holder panicked).
/// Safe for test mocks — we still want to inspect state after panics.
fn lock(state: &Mutex<MockState>) -> MutexGuard<'_, MockState> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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
                    let fill_price = state
                        .symbol_fill_prices
                        .get(&Symbol::force_new(symbol))
                        .copied()
                        .unwrap();

                    apply_happy_path_fill(&mut state, &order_id, fill_price);
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
fn apply_happy_path_fill(state: &mut MockState, order_id: &str, fill_price: Decimal) {
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
    let symbol_key = Symbol::force_new(symbol);

    // Update order status
    if let Some(order) = state.orders.get_mut(order_id) {
        order.status = "filled".to_string();
        order.filled_price = Some(fill_price.to_string());
    }

    // Update account balances
    if side == "buy" {
        state.account.cash -= qty * fill_price;
        let position = state
            .account
            .positions
            .entry(symbol_key.clone())
            .or_insert_with(|| MockPosition {
                symbol: symbol_key,
                qty: Decimal::ZERO,
                market_value: Decimal::ZERO,
            });
        position.qty += qty;
        position.market_value += qty * fill_price;
    } else {
        state.account.cash += qty * fill_price;
        let position = state
            .account
            .positions
            .entry(symbol_key.clone())
            .or_insert_with(|| MockPosition {
                symbol: symbol_key,
                qty: Decimal::ZERO,
                market_value: Decimal::ZERO,
            });
        position.qty -= qty;
        position.market_value -= qty * fill_price;
    }
}

// ── Wallet endpoints ─────────────────────────────────────────────────

fn register_whitelist_get_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);

    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/whitelists"));
        then.respond_with(move |_request: &HttpMockRequest| {
            let entries: Vec<Value> = {
                let state = lock(&state);
                state
                    .whitelisted_addresses
                    .iter()
                    .map(|entry| {
                        json!({
                            "id": entry.id,
                            "address": entry.address,
                            "asset": entry.asset,
                            "chain": entry.chain,
                            "status": entry.status,
                            "created_at": "2025-01-01T00:00:00Z"
                        })
                    })
                    .collect()
            };

            json_response(200, &Value::Array(entries))
        });
    });
}

fn register_whitelist_post_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);

    server.mock(|when, then| {
        when.method(POST)
            .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/whitelists"));
        then.respond_with(move |request: &HttpMockRequest| {
            let body: Value = serde_json::from_slice(request.body().as_ref()).unwrap_or(json!({}));

            let address = body["address"].as_str().unwrap_or("").to_string();
            let asset = body["asset"].as_str().unwrap_or("USDC").to_string();
            let entry_id = Uuid::new_v4().to_string();

            {
                let mut state = lock(&state);
                state.whitelisted_addresses.push(WhitelistEntry {
                    id: entry_id.clone(),
                    address: address.clone(),
                    asset: asset.clone(),
                    chain: "ETH".to_string(),
                    status: "approved".to_string(),
                });
            }

            json_response(
                200,
                &json!({
                    "id": entry_id,
                    "address": address,
                    "asset": asset,
                    "chain": "ETH",
                    "status": "approved",
                    "created_at": "2025-01-01T00:00:00Z"
                }),
            )
        });
    });
}

fn register_wallet_address_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);

    server.mock(|when, then| {
        when.method(GET)
            .path_prefix(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets"));
        then.respond_with(move |request: &HttpMockRequest| {
            let path = request.uri().path().to_string();

            // Don't match transfer or whitelist sub-paths
            if path.contains("transfers") || path.contains("whitelists") {
                return json_response(404, &json!({"message": "not found"}));
            }

            let deposit_addr = lock(&state).alpaca_deposit_address.clone();

            json_response(
                200,
                &json!({
                    "asset_id": "00000000-0000-0000-0000-000000000000",
                    "address": deposit_addr,
                    "created_at": "2025-01-01T00:00:00Z"
                }),
            )
        });
    });
}

fn register_wallet_transfers_post_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);

    server.mock(|when, then| {
        when.method(POST)
            .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
        then.respond_with(move |request: &HttpMockRequest| {
            let body: Value = serde_json::from_slice(request.body().as_ref()).unwrap_or(json!({}));

            let amount = body["amount"].as_str().unwrap_or("0").to_string();
            let asset = body["asset"].as_str().unwrap_or("USDC").to_string();
            let to_address = body["address"].as_str().unwrap_or("").to_string();
            let transfer_id = Uuid::new_v4().to_string();

            {
                let mut state = lock(&state);
                let from_address = state.alpaca_deposit_address.clone();

                state.wallet_transfers.push(MockWalletTransfer {
                    transfer_id: transfer_id.clone(),
                    direction: "OUTGOING".to_string(),
                    amount: amount.clone(),
                    asset: asset.clone(),
                    from_address,
                    to_address: to_address.clone(),
                    status: "PENDING".to_string(),
                    tx_hash: String::new(),
                    poll_count: 0,
                    polls_until_complete: 2,
                });
            }

            json_response(
                200,
                &json!({
                    "id": transfer_id,
                    "direction": "OUTGOING",
                    "amount": amount,
                    "usd_value": amount,
                    "asset": asset,
                    "chain": "ETH",
                    "from_address": "",
                    "to_address": to_address,
                    "status": "PENDING",
                    "tx_hash": "",
                    "created_at": Utc::now().to_rfc3339(),
                    "network_fee": "0",
                    "fees": "0"
                }),
            )
        });
    });
}

fn register_wallet_transfers_get_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);

    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
        then.respond_with(move |_request: &HttpMockRequest| {
            let transfers: Vec<Value> = {
                let mut state = lock(&state);

                // Advance pending transfers toward completion
                for transfer in &mut state.wallet_transfers {
                    if transfer.status == "PENDING" || transfer.status == "PROCESSING" {
                        transfer.poll_count += 1;

                        if transfer.poll_count >= transfer.polls_until_complete {
                            transfer.status = "COMPLETE".to_string();
                            // Generate a fake tx_hash for completed transfers
                            if transfer.tx_hash.is_empty() {
                                transfer.tx_hash = format!("0x{}", "cd".repeat(32));
                            }
                        } else {
                            transfer.status = "PROCESSING".to_string();
                        }
                    }
                }

                state
                    .wallet_transfers
                    .iter()
                    .map(|transfer| {
                        json!({
                            "id": transfer.transfer_id,
                            "direction": transfer.direction,
                            "amount": transfer.amount,
                            "usd_value": transfer.amount,
                            "asset": transfer.asset,
                            "chain": "ETH",
                            "from_address": transfer.from_address,
                            "to_address": transfer.to_address,
                            "status": transfer.status,
                            "tx_hash": transfer.tx_hash,
                            "created_at": "2025-01-01T00:00:00Z",
                            "network_fee": "0",
                            "fees": "0"
                        })
                    })
                    .collect()
            };

            json_response(200, &Value::Array(transfers))
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
