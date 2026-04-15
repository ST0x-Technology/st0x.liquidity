//! Autonomous mock Alpaca Broker API for E2E tests.
//!
//! Uses `respond_with` dynamic closures backed by shared `Arc<Mutex<MockState>>`
//! so the mock auto-responds based on request content. Tests configure a mode
//! (happy path, rejected, placement fails) and optionally set initial account
//! state via the builder - no per-request mock setup needed.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::sol;
use alloy::sol_types::SolEvent;
use bon::bon;
use chrono::Utc;
use httpmock::prelude::*;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use uuid::Uuid;

use rain_math_float::Float;
use st0x_float_serde::format_float_with_fallback;

use crate::Symbol;
use st0x_float_macro::float;

sol! {
    #[sol(all_derives = true)]
    event Transfer(address indexed from, address indexed to, uint256 value);
}

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
    /// Place succeeds, order stays "new" for N polls before filling.
    /// Simulates real broker latency where fills aren't instant.
    DelayedFill { polls_before_fill: usize },
}

pub struct MockPosition {
    pub symbol: Symbol,
    pub quantity: Float,
    pub market_value: Float,
}

struct MockAccount {
    cash: Float,
    buying_power: Float,
    positions: HashMap<Symbol, MockPosition>,
}

/// Side of a broker order (buy or sell).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl std::fmt::Display for OrderSide {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Buy => write!(formatter, "buy"),
            Self::Sell => write!(formatter, "sell"),
        }
    }
}

/// Status of a broker order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    New,
    Filled,
    Rejected,
}

impl std::fmt::Display for OrderStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::New => write!(formatter, "new"),
            Self::Filled => write!(formatter, "filled"),
            Self::Rejected => write!(formatter, "rejected"),
        }
    }
}

struct MockOrder {
    symbol: Symbol,
    quantity: Float,
    side: OrderSide,
    status: OrderStatus,
    poll_count: usize,
    filled_price: Option<Float>,
}

/// A single calendar entry controlling market open/close times.
struct CalendarEntry {
    pub date: String,
    pub open: String,
    pub close: String,
}

/// A mock wallet transfer tracked in shared state.
struct MockWalletTransfer {
    transfer_id: String,
    direction: TransferDirection,
    amount: Float,
    asset: String,
    from_address: Address,
    to_address: Address,
    status: TransferStatus,
    tx_hash: String,
    poll_count: usize,
    /// Number of polls before transitioning from "pending" to "complete".
    polls_until_complete: usize,
}

struct MockState {
    account: MockAccount,
    orders: HashMap<String, MockOrder>,
    symbol_fill_prices: HashMap<Symbol, Float>,
    mode: MockMode,
    /// Per-symbol fill delays: number of polls before filling.
    /// Symbols without an entry fill immediately in `HappyPath` mode.
    symbol_fill_delays: HashMap<Symbol, usize>,
    /// Dynamic calendar entries. The endpoint reads from this on each request.
    calendar_entries: Vec<CalendarEntry>,
    /// Wallet transfers (withdrawals and deposits).
    wallet_transfers: Vec<MockWalletTransfer>,
    /// Alpaca deposit wallet address for incoming USDC.
    alpaca_deposit_address: String,
    /// Wallet balances by asset symbol (e.g. USDC in the crypto wallet).
    wallet_balances: HashMap<String, Float>,
    /// Whitelisted withdrawal addresses.
    whitelisted_addresses: Vec<WhitelistEntry>,
}

/// Status of a whitelisted address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WhitelistStatus {
    Approved,
    Pending,
}

impl std::fmt::Display for WhitelistStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Approved => write!(formatter, "APPROVED"),
            Self::Pending => write!(formatter, "PENDING"),
        }
    }
}

/// A whitelisted address entry in the mock state.
struct WhitelistEntry {
    id: String,
    address: Address,
    asset: String,
    chain: String,
    status: WhitelistStatus,
}

/// Snapshot of a placed order, returned by [`AlpacaBrokerMock::orders`].
pub struct MockOrderSnapshot {
    pub order_id: String,
    pub symbol: String,
    pub quantity: Float,
    pub side: OrderSide,
    pub status: OrderStatus,
    pub poll_count: usize,
    pub filled_price: Option<Float>,
}

/// Snapshot of a position, returned by [`AlpacaBrokerMock::positions`].
pub struct MockPositionSnapshot {
    pub symbol: String,
    pub quantity: Float,
    pub market_value: Float,
}

/// Direction of a wallet transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferDirection {
    Incoming,
    Outgoing,
}

impl std::fmt::Display for TransferDirection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Incoming => write!(formatter, "INCOMING"),
            Self::Outgoing => write!(formatter, "OUTGOING"),
        }
    }
}

/// Status of a wallet transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferStatus {
    Pending,
    Processing,
    Complete,
}

impl std::fmt::Display for TransferStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(formatter, "PENDING"),
            Self::Processing => write!(formatter, "PROCESSING"),
            Self::Complete => write!(formatter, "COMPLETE"),
        }
    }
}

/// Snapshot of a wallet transfer, returned by
/// [`AlpacaBrokerMock::wallet_transfers`].
pub struct MockWalletTransferSnapshot {
    pub transfer_id: String,
    pub direction: TransferDirection,
    pub amount: Float,
    pub asset: String,
    pub status: TransferStatus,
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
        symbol_fill_prices: Vec<(Symbol, Float)>,
        symbol_positions: Vec<MockPosition>,
    ) -> Self {
        let today = Utc::now().format("%Y-%m-%d").to_string();

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
                cash: float!(100000),
                buying_power: float!(100000),
                positions,
            },
            orders: HashMap::new(),
            symbol_fill_prices,
            mode: MockMode::HappyPath,
            symbol_fill_delays: HashMap::new(),
            calendar_entries,
            wallet_transfers: Vec::new(),
            alpaca_deposit_address: String::new(),
            wallet_balances: HashMap::new(),
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
    /// mock server in e2e tests.
    pub fn server(&self) -> &MockServer {
        &self.server
    }

    /// Sets a fill price for a specific symbol.
    pub fn set_symbol_fill_price(&self, symbol: Symbol, price: Float) {
        lock(&self.state).symbol_fill_prices.insert(symbol, price);
    }

    /// Changes the mock mode for subsequent requests.
    pub fn set_mode(&self, mode: MockMode) {
        lock(&self.state).mode = mode;
    }

    /// Sets a per-symbol fill delay: the order stays "new" for
    /// `polls_before_fill` polls before transitioning to "filled".
    /// Only applies in `HappyPath` mode. Symbols without a delay fill
    /// immediately.
    pub fn set_symbol_fill_delay(&self, symbol: Symbol, polls_before_fill: usize) {
        lock(&self.state)
            .symbol_fill_delays
            .insert(symbol, polls_before_fill);
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

    /// Sets the USDC balance reported by the Alpaca crypto wallet endpoints.
    pub fn set_wallet_usdc_balance(&self, balance: Float) {
        lock(&self.state)
            .wallet_balances
            .insert("USDC".to_string(), balance);
    }

    /// Returns a snapshot of all orders placed through this mock.
    pub fn orders(&self) -> Vec<MockOrderSnapshot> {
        let state = lock(&self.state);
        state
            .orders
            .iter()
            .map(|(order_id, order)| MockOrderSnapshot {
                order_id: order_id.clone(),
                symbol: order.symbol.to_string(),
                quantity: order.quantity,
                side: order.side,
                status: order.status,
                poll_count: order.poll_count,
                filled_price: order.filled_price,
            })
            .collect()
    }

    /// Registers wallet API endpoints (whitelist, transfers, deposit address)
    /// on this mock server. Call after `build()` for tests that exercise the
    /// USDC rebalancing pipeline.
    ///
    /// `owner_address` is the market maker's Ethereum address, pre-approved
    /// in the whitelist for USDC withdrawals.
    pub fn register_wallet_endpoints(&self, owner_address: Address) {
        {
            let mut state = lock(&self.state);
            state.alpaca_deposit_address = format!("{:#x}", Address::random());
            state.whitelisted_addresses.push(WhitelistEntry {
                id: Uuid::new_v4().to_string(),
                address: owner_address,
                asset: "USDC".to_string(),
                chain: "ETH".to_string(),
                status: WhitelistStatus::Approved,
            });
        }

        register_whitelist_get_endpoint(&self.server, &self.state);
        register_whitelist_post_endpoint(&self.server, &self.state);
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
                direction: transfer.direction,
                amount: transfer.amount,
                asset: transfer.asset.clone(),
                status: transfer.status,
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
                symbol: pos.symbol.to_string(),
                quantity: pos.quantity,
                market_value: pos.market_value,
            })
            .collect()
    }

    /// Starts a background watcher that monitors the Ethereum chain for USDC
    /// mints to the bot's wallet. When detected, it auto-creates INCOMING
    /// wallet transfer records in mock state so the bot's deposit polling
    /// succeeds.
    ///
    /// In the BaseToAlpaca flow, CCTP mints USDC to `mint_recipient` (the
    /// bot's Ethereum wallet). The bot then polls Alpaca for a deposit
    /// matching the mint tx hash. This watcher simulates Alpaca detecting
    /// the on-chain mint and creating the corresponding transfer record.
    pub async fn start_deposit_watcher<P>(
        &self,
        provider: P,
        usdc_address: Address,
        mint_recipient: Address,
    ) -> anyhow::Result<JoinHandle<()>>
    where
        P: Provider + Clone + Send + Sync + 'static,
    {
        let state = Arc::clone(&self.state);
        let start_block = provider.get_block_number().await?;

        Ok(tokio::spawn(async move {
            let mut last_block = start_block;

            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;

                let Ok(current) = provider.get_block_number().await else {
                    continue;
                };

                if current <= last_block {
                    continue;
                }

                let range_ok = scan_deposit_range(
                    &provider,
                    &state,
                    last_block + 1,
                    current,
                    usdc_address,
                    mint_recipient,
                )
                .await;

                if range_ok {
                    last_block = current;
                }
            }
        }))
    }
}

/// Scans a single block for USDC Transfer events to `mint_recipient`
/// and records them as incoming wallet transfers in mock state.
/// Returns `true` if the block was fully processed, `false` on RPC error.
///
/// Only records transfers where `from == Address::ZERO` (a mint event),
/// filtering out regular transfers that happen to target `mint_recipient`.
async fn scan_block_for_deposits<P: Provider>(
    provider: &P,
    state: &Mutex<MockState>,
    block_num: u64,
    usdc_address: Address,
    mint_recipient: Address,
) -> bool {
    let Ok(Some(block)) = provider.get_block_by_number(block_num.into()).full().await else {
        return false;
    };

    for tx_hash in block.transactions.hashes() {
        let Ok(Some(receipt)) = provider.get_transaction_receipt(tx_hash).await else {
            return false;
        };

        for log in receipt.inner.logs() {
            let Ok(event) = Transfer::decode_log(log.as_ref()) else {
                continue;
            };

            if log.address() != usdc_address
                || event.to != mint_recipient
                || event.from != Address::ZERO
            {
                continue;
            }

            let tx_hash_hex = format!("{tx_hash:#x}");

            let already_exists = lock(state)
                .wallet_transfers
                .iter()
                .any(|transfer| transfer.tx_hash == tx_hash_hex);

            if already_exists {
                continue;
            }

            let amount_usdc = format_u256_as_usdc(event.value);

            let Ok(amount) = Float::parse(amount_usdc) else {
                continue;
            };

            lock(state).wallet_transfers.push(MockWalletTransfer {
                transfer_id: Uuid::new_v4().to_string(),
                direction: TransferDirection::Incoming,
                amount,
                asset: "USDC".to_string(),
                from_address: event.from,
                to_address: mint_recipient,
                status: TransferStatus::Complete,
                tx_hash: tx_hash_hex,
                poll_count: 0,
                polls_until_complete: 0,
            });
        }
    }

    true
}

/// Scans a range of blocks for deposits, returning true only if
/// every block in the range was fully processed.
async fn scan_deposit_range<P: Provider>(
    provider: &P,
    state: &Mutex<MockState>,
    from: u64,
    to: u64,
    usdc_address: Address,
    mint_recipient: Address,
) -> bool {
    for block_num in from..=to {
        if !scan_block_for_deposits(provider, state, block_num, usdc_address, mint_recipient).await
        {
            return false;
        }
    }

    true
}

/// Locks the mutex, recovering from poisoning (a previous holder panicked).
/// Safe for test mocks - we still want to inspect state after panics.
fn lock(state: &Mutex<MockState>) -> MutexGuard<'_, MockState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Registers all dynamic endpoints on the mock server.
fn register_endpoints(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    register_account_endpoint(server, state);
    register_calendar_endpoint(server, state);
    register_positions_endpoint(server, state);
    register_wallet_get_endpoint(server, state);
    register_latest_trade_endpoint(server, state);
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
            let cash = format_float_with_fallback(&state.account.cash);
            let buying_power = format_float_with_fallback(&state.account.buying_power);
            drop(state);
            json_response(
                200,
                &json!({
                    "id": TEST_ACCOUNT_ID,
                    "status": "ACTIVE",
                    "cash": cash,
                    "buying_power": buying_power,
                    "non_marginable_buying_power": buying_power,
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
            let result: Result<Vec<Value>, rain_math_float::FloatError> = {
                let state = lock(&state);
                state
                    .account
                    .positions
                    .values()
                    .map(|pos| {
                        let is_neg = pos
                            .quantity
                            .lt(Float::from_raw(alloy::primitives::B256::ZERO))?;
                        let side = if is_neg { "short" } else { "long" };
                        let abs_qty = pos.quantity.abs()?;
                        Ok(json!({
                            "symbol": pos.symbol,
                            "qty_available": format_float_with_fallback(&abs_qty),
                            "market_value": format_float_with_fallback(&pos.market_value),
                            "side": side,
                            "avg_entry_price": "0",
                        }))
                    })
                    .collect()
            };

            match result {
                Ok(positions) => json_response(200, &Value::Array(positions)),
                Err(error) => json_response(
                    500,
                    &json!({"message": format!("Mock /positions FloatError: {error}")}),
                ),
            }
        });
    });
}

fn register_latest_trade_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);
    let prefix = "/v2/stocks/";

    server.mock(|when, then| {
        when.method(GET).path_prefix(prefix);
        then.respond_with(move |request: &HttpMockRequest| {
            let request_path = request.uri().path().to_owned();
            let Some(symbol) = request_path
                .strip_prefix(prefix)
                .and_then(|path| path.strip_suffix("/trades/latest"))
            else {
                return json_response(404, &json!({"message": "not found"}));
            };

            let Ok(symbol) = Symbol::new(symbol) else {
                return json_response(404, &json!({"message": "unknown symbol"}));
            };

            let Some(price) = lock(&state).symbol_fill_prices.get(&symbol).copied() else {
                return json_response(404, &json!({"message": "no latest trade configured"}));
            };

            json_response(
                200,
                &json!({
                    "trade": {
                        "p": format_float_with_fallback(&price),
                    }
                }),
            )
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
            let body: Value = match serde_json::from_slice(request.body().as_ref()) {
                Ok(parsed) => parsed,
                Err(parse_error) => {
                    return json_response(
                        400,
                        &json!({"message": format!("invalid JSON: {parse_error}")}),
                    );
                }
            };

            let Some(symbol) = body["symbol"].as_str() else {
                return json_response(
                    400,
                    &json!({"message": "missing or non-string field: symbol"}),
                );
            };
            let Some(qty) = body["qty"].as_str() else {
                return json_response(400, &json!({"message": "missing or non-string field: qty"}));
            };
            let Some(side) = body["side"].as_str() else {
                return json_response(
                    400,
                    &json!({"message": "missing or non-string field: side"}),
                );
            };

            let Ok(symbol) = Symbol::new(symbol) else {
                return json_response(400, &json!({"message": "symbol cannot be empty"}));
            };
            let Ok(quantity) = Float::parse(qty.to_string()) else {
                return json_response(400, &json!({"message": format!("invalid qty: {qty}")}));
            };
            let side = match side {
                "buy" => OrderSide::Buy,
                "sell" => OrderSide::Sell,
                other => {
                    return json_response(
                        400,
                        &json!({"message": format!("invalid side: {other}")}),
                    );
                }
            };
            let order_id = Uuid::new_v4().to_string();

            let mut state = lock(&state);

            if matches!(state.mode, MockMode::PlacementFails) {
                return json_response(422, &json!({"message": "order rejected"}));
            }

            if symbol.to_string() == "USDCUSD" {
                return handle_crypto_order(&mut state, &order_id, &symbol, quantity, side);
            }

            state.orders.insert(
                order_id.clone(),
                MockOrder {
                    symbol: symbol.clone(),
                    quantity,
                    side,
                    status: OrderStatus::New,
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
                    "qty": format_float_with_fallback(&quantity),
                    "side": side.to_string(),
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
    symbol: &Symbol,
    quantity: Float,
    side: OrderSide,
) -> HttpMockResponse {
    let quantized_quantity = match crate::truncate_to_decimal_places(quantity, 6) {
        Ok(Some(value)) => value,
        Ok(None) => {
            return json_response(
                422,
                &json!({"message": "crypto quantity is below USDC precision"}),
            );
        }
        Err(error) => {
            return json_response(
                500,
                &json!({"message": format!("crypto quantity conversion error: {error}")}),
            );
        }
    };

    let matches_requested_precision = match quantized_quantity.eq(quantity) {
        Ok(result) => result,
        Err(error) => {
            return json_response(
                500,
                &json!({"message": format!("crypto quantity comparison error: {error}")}),
            );
        }
    };

    if !matches_requested_precision {
        return json_response(
            422,
            &json!({"message": "crypto quantity exceeds USDC precision"}),
        );
    }

    // Cash ledger is USD-denominated and modeled at cent precision in inventory.
    // Match equity fill handling by quantizing crypto conversion cash deltas to 2dp.
    let cash_delta = match quantized_quantity
        .to_fixed_decimal_lossy(2)
        .and_then(|(fixed, _lossless)| Float::from_fixed_decimal(fixed, 2))
    {
        Ok(value) => value,
        Err(error) => {
            return json_response(
                500,
                &json!({"message": format!("cash delta conversion error: {error}")}),
            );
        }
    };

    let updated_cash = match side {
        OrderSide::Buy => state.account.cash - cash_delta,
        OrderSide::Sell => state.account.cash + cash_delta,
    };
    match updated_cash {
        Ok(cash) => state.account.cash = cash,
        Err(error) => {
            return json_response(
                500,
                &json!({"message": format!("cash arithmetic error: {error}")}),
            );
        }
    }

    let fill_price = float!(1);
    let quantity_formatted = format_float_with_fallback(&quantized_quantity);
    let fill_price_formatted = format_float_with_fallback(&fill_price);

    state.orders.insert(
        order_id.to_string(),
        MockOrder {
            symbol: symbol.clone(),
            quantity: quantized_quantity,
            side,
            status: OrderStatus::Filled,
            poll_count: 0,
            filled_price: Some(fill_price),
        },
    );

    json_response(200, &{
        json!({
            "id": order_id,
            "symbol": symbol,
            "qty": quantity_formatted.as_str(),
            "side": side.to_string(),
            "status": "filled",
            "filled_avg_price": fill_price_formatted.as_str(),
            "filled_qty": quantity_formatted.as_str(),
            "created_at": "2025-01-06T12:00:00Z"
        })
    })
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

                if let Some(order) = state.orders.get_mut(&order_id) {
                    order.poll_count += 1;
                }

                let mode = state.mode;

                match mode {
                    MockMode::HappyPath => {
                        let symbol_key = state.orders[&order_id].symbol.clone();

                        let delay = state
                            .symbol_fill_delays
                            .get(&symbol_key)
                            .copied()
                            .unwrap_or(0);
                        let poll_count = state.orders[&order_id].poll_count;

                        if poll_count >= delay {
                            let Some(fill_price) =
                                state.symbol_fill_prices.get(&symbol_key).copied()
                            else {
                                return json_response(
                                    500,
                                    &json!({"message": "no fill price configured"}),
                                );
                            };

                            if let Err(error) =
                                apply_happy_path_fill(&mut state, &order_id, fill_price)
                            {
                                return json_response(
                                    500,
                                    &json!({"message": format!("fill arithmetic error: {error}")}),
                                );
                            }
                        }
                    }
                    MockMode::OrderRejected => {
                        if let Some(order) = state.orders.get_mut(&order_id) {
                            order.status = OrderStatus::Rejected;
                        }
                    }
                    MockMode::DelayedFill { polls_before_fill } => {
                        let ready = state
                            .orders
                            .get(&order_id)
                            .is_some_and(|o| o.poll_count >= polls_before_fill);

                        if ready {
                            let symbol_key = state.orders[&order_id].symbol.clone();
                            let Some(fill_price) =
                                state.symbol_fill_prices.get(&symbol_key).copied()
                            else {
                                return json_response(
                                    500,
                                    &json!({"message": "no fill price configured"}),
                                );
                            };

                            if let Err(error) =
                                apply_happy_path_fill(&mut state, &order_id, fill_price)
                            {
                                return json_response(
                                    500,
                                    &json!({"message": format!("fill arithmetic error: {error}")}),
                                );
                            }
                        }
                    }
                    MockMode::PlacementFails => {
                        // Placement already rejected at order creation -
                        // no orders exist to poll.
                    }
                }

                let order = &state.orders[&order_id];
                let filled_quantity = if order.status == OrderStatus::Filled {
                    Some(format_float_with_fallback(&order.quantity))
                } else {
                    None
                };
                let filled_price: Option<String> =
                    order.filled_price.as_ref().map(format_float_with_fallback);
                let quantity = format_float_with_fallback(&order.quantity);
                let body = json!({
                    "id": order_id,
                    "symbol": order.symbol,
                    "qty": quantity,
                    "side": order.side.to_string(),
                    "status": order.status.to_string(),
                    "filled_avg_price": filled_price,
                    "filled_qty": filled_quantity,
                    "created_at": "2025-01-01T00:00:00Z",
                });
                drop(state);
                body
            };

            json_response(200, &response_body)
        });
    });
}

/// Transitions a "new" order to "filled" and updates account balances.
fn apply_happy_path_fill(
    state: &mut MockState,
    order_id: &str,
    fill_price: Float,
) -> Result<(), rain_math_float::FloatError> {
    let should_fill = state
        .orders
        .get(order_id)
        .is_some_and(|order| order.status == OrderStatus::New);
    if !should_fill {
        return Ok(());
    }

    let symbol_key = state.orders[order_id].symbol.clone();
    let qty = state.orders[order_id].quantity;
    let side = state.orders[order_id].side;
    let raw_cost = (qty * fill_price)?;
    let (cost_fixed, _) = raw_cost.to_fixed_decimal_lossy(2)?;
    let cost = Float::from_fixed_decimal(cost_fixed, 2)?;

    if let Some(order) = state.orders.get_mut(order_id) {
        order.status = OrderStatus::Filled;
        order.filled_price = Some(fill_price);
    }

    match side {
        OrderSide::Buy => {
            state.account.cash = (state.account.cash - cost)?;
            let position = state
                .account
                .positions
                .entry(symbol_key.clone())
                .or_insert_with(|| MockPosition {
                    symbol: symbol_key,
                    quantity: Float::from_raw(alloy::primitives::B256::ZERO),
                    market_value: Float::from_raw(alloy::primitives::B256::ZERO),
                });
            position.quantity = (position.quantity + qty)?;
            position.market_value = (position.market_value + cost)?;
        }
        OrderSide::Sell => {
            state.account.cash = (state.account.cash + cost)?;
            let position = state
                .account
                .positions
                .entry(symbol_key.clone())
                .or_insert_with(|| MockPosition {
                    symbol: symbol_key,
                    quantity: Float::from_raw(alloy::primitives::B256::ZERO),
                    market_value: Float::from_raw(alloy::primitives::B256::ZERO),
                });
            position.quantity = (position.quantity - qty)?;
            position.market_value = (position.market_value - cost)?;
        }
    }

    Ok(())
}

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
                            "status": entry.status.to_string(),
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
            let body: Value = match serde_json::from_slice(request.body().as_ref()) {
                Ok(parsed) => parsed,
                Err(parse_error) => {
                    return json_response(
                        400,
                        &json!({"message": format!("invalid JSON: {parse_error}")}),
                    );
                }
            };

            let Some(address_str) = body["address"].as_str() else {
                return json_response(
                    400,
                    &json!({"message": "missing or non-string field: address"}),
                );
            };
            let Some(asset) = body["asset"].as_str() else {
                return json_response(
                    400,
                    &json!({"message": "missing or non-string field: asset"}),
                );
            };
            let Ok(address) = address_str.parse::<Address>() else {
                return json_response(
                    400,
                    &json!({"message": format!("invalid address: {address_str}")}),
                );
            };
            let asset = asset.to_string();
            let entry_id = Uuid::new_v4().to_string();

            {
                let mut state = lock(&state);
                state.whitelisted_addresses.push(WhitelistEntry {
                    id: entry_id.clone(),
                    address,
                    asset: asset.clone(),
                    chain: "ETH".to_string(),
                    status: WhitelistStatus::Approved,
                });
            }

            json_response(
                200,
                &json!({
                    "id": entry_id,
                    "address": address,
                    "asset": asset,
                    "chain": "ETH",
                    "status": "APPROVED",
                    "created_at": "2025-01-01T00:00:00Z"
                }),
            )
        });
    });
}

fn register_wallet_get_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);

    server.mock(|when, then| {
        when.method(GET)
            .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets"));
        then.respond_with(move |request: &HttpMockRequest| {
            let query: HashMap<_, _> =
                url::form_urlencoded::parse(request.uri().query().unwrap_or_default().as_bytes())
                    .into_owned()
                    .collect();

            if let Some(asset) = query.get("asset") {
                let Some(network) = query.get("network") else {
                    return json_response(400, &json!({ "message": "missing network query" }));
                };

                if network != "ethereum" {
                    return json_response(
                        400,
                        &json!({ "message": format!("unsupported network: {network}") }),
                    );
                }

                if asset != "USDC" {
                    return json_response(
                        400,
                        &json!({ "message": format!("unsupported asset: {asset}") }),
                    );
                }

                let (deposit_addr, balance) = {
                    let state = lock(&state);
                    (
                        state.alpaca_deposit_address.clone(),
                        state
                            .wallet_balances
                            .get(asset)
                            .copied()
                            .unwrap_or_else(|| float!(0)),
                    )
                };

                return json_response(
                    200,
                    &json!({
                        "asset_id": "00000000-0000-0000-0000-000000000000",
                        "asset": asset,
                        "address": deposit_addr,
                        "balance": format_float_with_fallback(&balance),
                        "created_at": "2025-01-01T00:00:00Z"
                    }),
                );
            }

            let wallets: Vec<Value> = {
                let state = lock(&state);
                state
                    .wallet_balances
                    .iter()
                    .map(|(asset, balance)| {
                        json!({
                            "asset": asset,
                            "balance": format_float_with_fallback(balance)
                        })
                    })
                    .collect()
            };

            json_response(200, &Value::Array(wallets))
        });
    });
}

fn register_wallet_transfers_post_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);

    server.mock(|when, then| {
        when.method(POST)
            .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
        then.respond_with(move |request: &HttpMockRequest| {
            let body: Value = match serde_json::from_slice(request.body().as_ref()) {
                Ok(parsed) => parsed,
                Err(parse_error) => {
                    return json_response(
                        400,
                        &json!({"message": format!("invalid JSON: {parse_error}")}),
                    );
                }
            };

            let Some(amount_str) = body["amount"].as_str() else {
                return json_response(
                    400,
                    &json!({"message": "missing or non-string field: amount"}),
                );
            };
            let Some(asset) = body["asset"].as_str() else {
                return json_response(
                    400,
                    &json!({"message": "missing or non-string field: asset"}),
                );
            };
            let Some(to_address_str) = body["address"].as_str() else {
                return json_response(
                    400,
                    &json!({"message": "missing or non-string field: address"}),
                );
            };
            let Ok(amount) = Float::parse(amount_str.to_string()) else {
                return json_response(
                    400,
                    &json!({"message": format!("invalid amount: {amount_str}")}),
                );
            };
            let Ok(to_address) = to_address_str.parse::<Address>() else {
                return json_response(
                    400,
                    &json!({"message": format!("invalid address: {to_address_str}")}),
                );
            };
            let asset = asset.to_string();
            let transfer_id = Uuid::new_v4().to_string();
            let amount_formatted = format_float_with_fallback(&amount);

            let from_address = {
                let mut state = lock(&state);
                let from_address = state.alpaca_deposit_address.clone();

                let Ok(parsed_from) = from_address.parse::<Address>() else {
                    return json_response(
                        500,
                        &json!({"message": format!(
                            "mock misconfigured: invalid alpaca_deposit_address: \
                             {from_address}"
                        )}),
                    );
                };

                state.wallet_transfers.push(MockWalletTransfer {
                    transfer_id: transfer_id.clone(),
                    direction: TransferDirection::Outgoing,
                    amount,
                    asset: asset.clone(),
                    from_address: parsed_from,
                    to_address,
                    status: TransferStatus::Pending,
                    tx_hash: String::new(),
                    poll_count: 0,
                    polls_until_complete: 2,
                });

                from_address
            };

            json_response(200, &{
                let amount = amount_formatted.as_str();
                json!({
                    "id": transfer_id,
                    "direction": "OUTGOING",
                    "amount": amount,
                    "usd_value": amount,
                    "asset": asset,
                    "chain": "ETH",
                    "from_address": from_address,
                    "to_address": format!("{to_address:#x}"),
                    "status": "PENDING",
                    "tx_hash": null,
                    "created_at": Utc::now().to_rfc3339(),
                    "network_fee": "0",
                    "fees": "0"
                })
            })
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

                for transfer in &mut state.wallet_transfers {
                    let is_pending = matches!(
                        transfer.status,
                        TransferStatus::Pending | TransferStatus::Processing
                    );
                    if is_pending {
                        transfer.poll_count += 1;

                        if transfer.poll_count >= transfer.polls_until_complete {
                            transfer.status = TransferStatus::Complete;
                            if transfer.tx_hash.is_empty() {
                                transfer.tx_hash = format!("0x{}", "cd".repeat(32));
                            }
                        } else {
                            transfer.status = TransferStatus::Processing;
                        }
                    }
                }

                state
                    .wallet_transfers
                    .iter()
                    .map(|transfer| {
                        let tx_hash: Value = if transfer.tx_hash.is_empty() {
                            Value::Null
                        } else {
                            Value::String(transfer.tx_hash.clone())
                        };
                        let amount = format_float_with_fallback(&transfer.amount);

                        json!({
                            "id": transfer.transfer_id,
                            "direction": transfer.direction.to_string(),
                            "amount": amount.as_str(),
                            "usd_value": amount.as_str(),
                            "asset": transfer.asset,
                            "chain": "ETH",
                            "from_address": transfer.from_address,
                            "to_address": transfer.to_address,
                            "status": transfer.status.to_string(),
                            "tx_hash": tx_hash,
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

/// Formats a U256 raw amount as a USDC decimal string (6 decimal places).
///
/// Operates entirely on the string representation to avoid narrowing U256
/// through u64 (which would silently overflow on large values).
/// E.g. `U256::from(1_500_000)` -> `"1.5"`, `U256::from(500)` -> `"0.0005"`.
fn format_u256_as_usdc(raw: alloy::primitives::U256) -> String {
    const USDC_DECIMALS: usize = 6;

    let digits = raw.to_string();

    if digits.len() <= USDC_DECIMALS {
        let whole = "0";
        let fractional = format!("{digits:0>USDC_DECIMALS$}");
        let trimmed = fractional.trim_end_matches('0');
        if trimmed.is_empty() {
            return whole.to_string();
        }
        return format!("{whole}.{trimmed}");
    }

    let (whole, fractional) = digits.split_at(digits.len() - USDC_DECIMALS);
    let trimmed = fractional.trim_end_matches('0');
    if trimmed.is_empty() {
        return whole.to_string();
    }
    format!("{whole}.{trimmed}")
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use alloy::primitives::{Address, U256};

    use super::{
        MockAccount, MockMode, MockState, OrderSide, format_u256_as_usdc, handle_crypto_order,
    };
    use crate::Symbol;
    use st0x_float_macro::float;

    #[test]
    fn format_u256_as_usdc_cases() {
        assert_eq!(format_u256_as_usdc(U256::ZERO), "0");
        assert_eq!(format_u256_as_usdc(U256::from(1u64)), "0.000001");
        assert_eq!(format_u256_as_usdc(U256::from(500u64)), "0.0005");
        assert_eq!(format_u256_as_usdc(U256::from(1_000_000u64)), "1");
        assert_eq!(format_u256_as_usdc(U256::from(1_230_000u64)), "1.23");
        assert_eq!(format_u256_as_usdc(U256::from(1_500_000u64)), "1.5");
        assert_eq!(
            format_u256_as_usdc(U256::from(100_000_000_000u64)),
            "100000"
        );

        // Beyond u64::MAX - the bug the refactor fixed
        let beyond_u64 = U256::from(u64::MAX) + U256::from(1);
        assert_eq!(format_u256_as_usdc(beyond_u64), "18446744073709.551616");
    }

    #[test]
    fn crypto_cash_delta_is_quantized_to_cents() {
        let mut state = MockState {
            account: MockAccount {
                cash: float!(100),
                buying_power: float!(100),
                positions: HashMap::new(),
            },
            orders: HashMap::new(),
            symbol_fill_prices: HashMap::new(),
            mode: MockMode::HappyPath,
            symbol_fill_delays: HashMap::new(),
            calendar_entries: vec![],
            wallet_transfers: vec![],
            alpaca_deposit_address: format!("{:#x}", Address::ZERO),
            wallet_balances: HashMap::new(),
            whitelisted_addresses: vec![],
        };

        let response = handle_crypto_order(
            &mut state,
            "order-1",
            &Symbol::new("USDCUSD").unwrap(),
            float!(12.345678),
            OrderSide::Buy,
        );

        assert_eq!(response.status, Some(200));

        let expected_cash = float!(87.66);
        assert!(
            state.account.cash.eq(expected_cash).unwrap(),
            "cash should be 100 - 12.34 after cent quantization, got: {:?}",
            state.account.cash
        );

        let order = state.orders.get("order-1").unwrap();
        assert!(order.quantity.eq(float!(12.345678)).unwrap());
    }

    #[test]
    fn crypto_quantity_with_excess_precision_is_rejected() {
        let mut state = MockState {
            account: MockAccount {
                cash: float!(100),
                buying_power: float!(100),
                positions: HashMap::new(),
            },
            orders: HashMap::new(),
            symbol_fill_prices: HashMap::new(),
            mode: MockMode::HappyPath,
            symbol_fill_delays: HashMap::new(),
            calendar_entries: vec![],
            wallet_transfers: vec![],
            alpaca_deposit_address: format!("{:#x}", Address::ZERO),
            wallet_balances: HashMap::new(),
            whitelisted_addresses: vec![],
        };

        let response = handle_crypto_order(
            &mut state,
            "order-1",
            &Symbol::new("USDCUSD").unwrap(),
            float!(12.3456789),
            OrderSide::Buy,
        );

        assert_eq!(response.status, Some(422));
        assert!(!state.orders.contains_key("order-1"));
    }
}
