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
use chrono::{DateTime, Utc};
use chrono_tz::America::New_York;
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
    /// Place succeeds, the first poll returns a half-quantity partial fill
    /// and the next poll returns "canceled" retaining that fill. Models a
    /// broker-side cancellation the bot never requested.
    PartialFillThenCancel,
    /// Place succeeds and each new equity order is assigned its own terminal
    /// outcome in round-robin order -- filled, rejected, then partially
    /// filled and cancelled. Unlike the single-outcome modes, one run drives
    /// the trade history through every outcome the dashboard renders.
    /// Crypto (USDCUSD) orders are unaffected and keep filling immediately.
    RotatingOutcomes,
}

/// The terminal outcome the mock drives one order to, assigned at placement
/// under [`MockMode::RotatingOutcomes`] so that concurrent orders keep their
/// own outcome instead of racing on the server-wide mode.
#[derive(Debug, Clone, Copy)]
enum PlannedOutcome {
    Filled,
    Rejected,
    CancelledAfterPartialFill,
}

/// The outcomes [`MockMode::RotatingOutcomes`] cycles through, in the order
/// they are handed to newly placed orders.
const ROTATING_OUTCOMES: [PlannedOutcome; 3] = [
    PlannedOutcome::Filled,
    PlannedOutcome::Rejected,
    PlannedOutcome::CancelledAfterPartialFill,
];

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
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
}

impl std::fmt::Display for OrderStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::New => write!(formatter, "new"),
            Self::PartiallyFilled => write!(formatter, "partially_filled"),
            Self::Filled => write!(formatter, "filled"),
            Self::Canceled => write!(formatter, "canceled"),
            Self::Rejected => write!(formatter, "rejected"),
        }
    }
}

struct MockOrder {
    symbol: Symbol,
    quantity: Float,
    /// USD named by a `notional` placement, `None` for a `qty` placement.
    /// The real API keeps `qty` null on such orders, so status responses
    /// must render `notional` instead of `quantity`.
    notional: Option<Float>,
    side: OrderSide,
    status: OrderStatus,
    /// Quantity executed so far. Zero until a fill applies, the ordered
    /// quantity once filled, and the partial quantity for an order that was
    /// cancelled after a partial fill.
    filled_quantity: Float,
    poll_count: usize,
    filled_price: Option<Float>,
    client_order_id: Option<String>,
    /// Set only for orders placed under [`MockMode::RotatingOutcomes`];
    /// overrides the server-wide mode when this order is polled.
    planned_outcome: Option<PlannedOutcome>,
}

/// A single calendar entry controlling market open/close times.
///
/// Mirrors the real Alpaca calendar payload: `open`/`close` are the regular
/// trading hours and `session_open`/`session_close` span the full extended
/// session. All four are required by the `CalendarDay` parser.
struct CalendarEntry {
    pub date: String,
    pub open: String,
    pub close: String,
    pub session_open: String,
    pub session_close: String,
}

/// Builds a calendar entry that keeps the regular session open all day.
///
/// The date is computed in Eastern Time because the market-hours client
/// queries the calendar with today's ET date and rejects entries for any
/// other day; a UTC date would mismatch between 19:00 and 24:00 ET.
fn market_open_calendar_entry() -> CalendarEntry {
    let today = Utc::now().with_timezone(&New_York).format("%Y-%m-%d");

    CalendarEntry {
        date: today.to_string(),
        open: "00:00".to_string(),
        close: "23:59".to_string(),
        session_open: "00:00".to_string(),
        session_close: "23:59".to_string(),
    }
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

/// Flat synthetic broker fee recorded per executed fill, as the signed
/// `net_amount` the real API reports (negative = cash decrease). Obviously
/// fake on purpose: enough for `/pnl` to classify a broker fee per fill
/// without modeling Alpaca's real fee schedule.
const MOCK_FILL_FEE_USD: &str = "-0.1";

/// A broker account activity served by `/v1/accounts/activities`.
///
/// The mock records one FEE activity per executed fill so PnL reports built
/// against the simulated stack exercise the broker-fee cost path with
/// non-zero tracked costs; tests and simulations can seed further rows
/// (dividends, interest) via [`AlpacaBrokerMock::push_activity`].
struct MockActivity {
    id: String,
    activity_type: String,
    net_amount: String,
    symbol: Option<String>,
    transaction_time: DateTime<Utc>,
}

struct MockState {
    account: MockAccount,
    orders: HashMap<String, MockOrder>,
    symbol_fill_prices: HashMap<Symbol, Float>,
    mode: MockMode,
    /// Cursor into [`ROTATING_OUTCOMES`], advanced once per equity order
    /// placed under [`MockMode::RotatingOutcomes`].
    rotating_outcome_index: usize,
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
    /// Number of upcoming `place_order` requests that should be answered
    /// with a 5xx *after* the order has already been recorded in
    /// [`MockState::orders`]. Models the adversarial case where the
    /// broker processes the request but the response is lost in flight:
    /// any caller-side retry creates a second order even though one
    /// already exists. Decrements per request until zero.
    transient_placement_failures_remaining: usize,
    /// Number of upcoming `/v1/calendar` requests answered with a 503
    /// before the endpoint resumes serving [`MockState::calendar_entries`].
    /// Models a transient market-hours data outage. Decrements per
    /// request until zero.
    calendar_failures_remaining: usize,
    /// Number of upcoming `place_order` requests answered with a 401
    /// *before* the order is recorded -- the complement of
    /// [`MockState::transient_placement_failures_remaining`]. Models a
    /// session/credential expiry mid-request where the broker never
    /// processed the placement. Decrements per request until zero.
    unauthorized_placement_failures_remaining: usize,
    /// Broker-side account-activity ledger, in ascending insertion order
    /// (ids are monotonic and double as pagination cursors). Fills append
    /// FEE rows; [`AlpacaBrokerMock::push_activity`] seeds arbitrary rows.
    activities: Vec<MockActivity>,
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
///
/// Direction-specific counterparty data lives in [`TransferFlow`]; common
/// metadata stays on the snapshot so consumers don't pay a match cost for
/// status / amount / id checks. The `flow` discriminant subsumes any
/// separate "direction" tag, so neither the meaning of an address nor the
/// invariant linking address-to-direction needs to be reasoned about.
pub struct MockWalletTransferSnapshot {
    pub transfer_id: String,
    pub amount: Float,
    pub asset: String,
    pub status: TransferStatus,
    pub flow: TransferFlow,
}

/// Direction-specific counterparty data attached to a wallet transfer.
pub enum TransferFlow {
    /// USDC moving from Alpaca to an on-chain `recipient`.
    Outgoing { recipient: Address },
    /// USDC arriving at Alpaca from an on-chain `sender`.
    Incoming { sender: Address },
}

impl TransferFlow {
    #[must_use]
    pub fn direction(&self) -> TransferDirection {
        match self {
            Self::Outgoing { .. } => TransferDirection::Outgoing,
            Self::Incoming { .. } => TransferDirection::Incoming,
        }
    }
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
        initial_cash: Option<Float>,
    ) -> Self {
        let calendar_entries = vec![market_open_calendar_entry()];

        let symbol_fill_prices = symbol_fill_prices.into_iter().collect();
        let positions = symbol_positions
            .into_iter()
            .map(|pos| (pos.symbol.clone(), pos))
            .collect();

        let cash = initial_cash.unwrap_or(float!(100000));

        let state = Arc::new(Mutex::new(MockState {
            account: MockAccount {
                cash,
                buying_power: cash,
                positions,
            },
            orders: HashMap::new(),
            symbol_fill_prices,
            mode: MockMode::HappyPath,
            rotating_outcome_index: 0,
            symbol_fill_delays: HashMap::new(),
            calendar_entries,
            wallet_transfers: Vec::new(),
            alpaca_deposit_address: String::new(),
            wallet_balances: HashMap::new(),
            whitelisted_addresses: Vec::new(),
            transient_placement_failures_remaining: 0,
            calendar_failures_remaining: 0,
            unauthorized_placement_failures_remaining: 0,
            activities: Vec::new(),
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

    /// Arms the mock to answer the next `count` `place_order` requests
    /// with a 5xx *after* the order has already been recorded in the
    /// mock's internal state. Models adversarial broker behaviour where
    /// the request reaches the upstream and is processed but the
    /// response is lost in flight; any caller-side retry creates a
    /// second order with the same intent.
    pub fn set_transient_placement_failures(&self, count: usize) {
        lock(&self.state).transient_placement_failures_remaining = count;
    }

    /// Arms the mock to answer the next `count` `/v1/calendar` requests
    /// with a 503 before resuming normal calendar service. Models a
    /// transient market-hours data outage.
    pub fn set_calendar_failures(&self, count: usize) {
        lock(&self.state).calendar_failures_remaining = count;
    }

    /// Remaining armed calendar failures. Tests assert this reached zero
    /// to prove the outage actually hit the market-hours path.
    pub fn calendar_failures_remaining(&self) -> usize {
        lock(&self.state).calendar_failures_remaining
    }

    /// Arms the mock to answer the next `count` `place_order` requests
    /// with a 401 *before* recording anything -- the broker never
    /// processed the placement. Models a session/credential expiry
    /// mid-request, the complement of
    /// [`AlpacaBrokerMock::set_transient_placement_failures`].
    pub fn set_unauthorized_placement_failures(&self, count: usize) {
        lock(&self.state).unauthorized_placement_failures_remaining = count;
    }

    /// Remaining armed unauthorized placement failures. Tests assert
    /// this reached zero to prove the 401 was actually served.
    pub fn unauthorized_placement_failures_remaining(&self) -> usize {
        lock(&self.state).unauthorized_placement_failures_remaining
    }

    /// Appends an arbitrary account activity to the broker-side ledger
    /// served by `/v1/accounts/activities`. Lets tests and simulations
    /// seed non-fee rows (dividends, margin interest) without driving
    /// fills.
    pub fn push_activity(
        &self,
        activity_type: &str,
        net_amount: &str,
        symbol: Option<Symbol>,
        transaction_time: DateTime<Utc>,
    ) {
        push_state_activity(
            &mut lock(&self.state),
            activity_type,
            net_amount,
            symbol.map(|symbol| symbol.to_string()),
            transaction_time,
        );
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
        lock(&self.state).calendar_entries = vec![market_open_calendar_entry()];
    }

    /// Switches the calendar to market-closed for today.
    pub fn set_market_closed(&self) {
        // An empty calendar is how the real API reports a non-trading day
        // and is the only representation with no open window at all -- a
        // tiny synthetic session window would still classify as open if a
        // test happened to run inside it.
        lock(&self.state).calendar_entries = Vec::new();
    }

    /// Sets the USDC balance reported by the Alpaca crypto wallet endpoints.
    pub fn set_wallet_usdc_balance(&self, balance: Float) {
        lock(&self.state)
            .wallet_balances
            .insert("USDC".to_string(), balance);
    }

    /// Returns the current broker account cash balance.
    pub fn cash_balance(&self) -> Float {
        lock(&self.state).account.cash
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
        register_wallet_transfer_get_by_id_endpoint(&self.server, &self.state);
    }

    /// Returns a snapshot of all wallet transfers.
    pub fn wallet_transfers(&self) -> Vec<MockWalletTransferSnapshot> {
        let state = lock(&self.state);
        state
            .wallet_transfers
            .iter()
            .map(|transfer| MockWalletTransferSnapshot {
                transfer_id: transfer.transfer_id.clone(),
                amount: transfer.amount,
                asset: transfer.asset.clone(),
                status: transfer.status,
                flow: match transfer.direction {
                    TransferDirection::Outgoing => TransferFlow::Outgoing {
                        recipient: transfer.to_address,
                    },
                    TransferDirection::Incoming => TransferFlow::Incoming {
                        sender: transfer.from_address,
                    },
                },
            })
            .collect()
    }

    /// Records the on-chain tx hash for an outgoing wallet transfer by
    /// transfer_id. Used by test infrastructure to inject the real Ethereum
    /// mint tx hash so the bot's withdrawal-confirmation check can verify it.
    pub fn set_transfer_tx_hash(&self, transfer_id: &str, tx_hash_hex: String) {
        lock(&self.state)
            .wallet_transfers
            .iter_mut()
            .find(|transfer| transfer.transfer_id == transfer_id)
            .unwrap_or_else(|| panic!("set_transfer_tx_hash: no transfer with id {transfer_id}"))
            .tx_hash = tx_hash_hex;
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

    /// Adjusts a position's quantity by `delta`. Positive delta adds shares,
    /// negative removes. Used by the mock tokenizer to reflect that minting
    /// locks shares (deducts from position) and redeeming releases them.
    pub fn adjust_position(
        &self,
        symbol: &Symbol,
        delta: Float,
    ) -> Result<(), rain_math_float::FloatError> {
        let mut state = lock(&self.state);
        let price = state.symbol_fill_prices.get(symbol).copied();

        if let Some(position) = state.account.positions.get_mut(symbol) {
            position.quantity = (position.quantity + delta)?;
            if let Some(price) = price {
                position.market_value = (position.quantity * price)?;
            }
        } else {
            let market_value =
                price.map_or(Ok(st0x_float_macro::float!(0)), |price| delta * price)?;

            state.account.positions.insert(
                symbol.clone(),
                MockPosition {
                    symbol: symbol.clone(),
                    quantity: delta,
                    market_value,
                },
            );
        }

        drop(state);
        Ok(())
    }

    /// Starts a background watcher that monitors the Ethereum chain for the USDC
    /// deposit SEND to Alpaca's deposit address. When detected, it auto-creates
    /// an INCOMING wallet transfer record (keyed on the send tx) in mock state so
    /// the bot's deposit polling by the send tx succeeds.
    ///
    /// In the BaseToAlpaca flow, CCTP mints USDC to the bot's Ethereum wallet
    /// (`sender`), and the bot then SENDS that USDC to Alpaca's per-account
    /// deposit address and polls Alpaca for a deposit matching the SEND tx hash
    /// (not the mint tx). This watcher detects that on-chain
    /// `Transfer(sender -> alpaca_deposit_address)` and creates the corresponding
    /// transfer record, simulating Alpaca crediting the deposit.
    pub async fn start_deposit_watcher<P>(
        &self,
        provider: P,
        usdc_address: Address,
        sender: Address,
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
                    sender,
                )
                .await;

                if range_ok {
                    last_block = current;
                }
            }
        }))
    }
}

/// Scans a single block for the USDC deposit SEND
/// (`Transfer(sender -> alpaca_deposit_address)`) and records it as an incoming
/// wallet transfer (keyed on the send tx) in mock state.
/// Returns `true` if the block was fully processed, `false` on RPC error.
///
/// Matches `from == sender` and `to == alpaca_deposit_address` so the recorded
/// tx hash is the bot's fund-moving deposit send -- exactly the tx the bot polls
/// by -- not the upstream CCTP mint (whose `to` is the bot wallet itself).
async fn scan_block_for_deposits<P: Provider>(
    provider: &P,
    state: &Mutex<MockState>,
    block_num: u64,
    usdc_address: Address,
    sender: Address,
) -> bool {
    let Ok(Some(block)) = provider.get_block_by_number(block_num.into()).full().await else {
        return false;
    };

    let Ok(deposit_address) = lock(state).alpaca_deposit_address.parse::<Address>() else {
        // The deposit address is only set once `register_wallet_endpoints` runs;
        // until then there is nothing to match, so treat the block as processed.
        return true;
    };

    for tx_hash in block.transactions.hashes() {
        let Ok(Some(receipt)) = provider.get_transaction_receipt(tx_hash).await else {
            return false;
        };

        for log in receipt.inner.logs() {
            let Ok(event) = Transfer::decode_log(log.as_ref()) else {
                continue;
            };

            if log.address() != usdc_address || event.from != sender || event.to != deposit_address
            {
                continue;
            }

            let tx_hash_hex = format!("{tx_hash:#x}");

            let mut state = lock(state);
            let already_exists = state
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

            if add_wallet_balance(&mut state, "USDC", amount).is_err() {
                continue;
            }

            state.wallet_transfers.push(MockWalletTransfer {
                transfer_id: Uuid::new_v4().to_string(),
                direction: TransferDirection::Incoming,
                amount,
                asset: "USDC".to_string(),
                from_address: event.from,
                to_address: deposit_address,
                status: TransferStatus::Complete,
                tx_hash: tx_hash_hex,
                poll_count: 0,
                polls_until_complete: 0,
            });
        }
    }

    true
}

/// Scans a range of blocks for the deposit send, returning true only if
/// every block in the range was fully processed.
async fn scan_deposit_range<P: Provider>(
    provider: &P,
    state: &Mutex<MockState>,
    from: u64,
    to: u64,
    usdc_address: Address,
    sender: Address,
) -> bool {
    for block_num in from..=to {
        if !scan_block_for_deposits(provider, state, block_num, usdc_address, sender).await {
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

fn add_wallet_balance(
    state: &mut MockState,
    asset: &str,
    amount: Float,
) -> Result<(), rain_math_float::FloatError> {
    let current = state
        .wallet_balances
        .get(asset)
        .copied()
        .unwrap_or_else(|| float!(0));
    let updated = (current + amount)?;
    state.wallet_balances.insert(asset.to_string(), updated);

    Ok(())
}

fn subtract_wallet_balance(
    state: &mut MockState,
    asset: &str,
    amount: Float,
) -> Result<bool, rain_math_float::FloatError> {
    let current = state
        .wallet_balances
        .get(asset)
        .copied()
        .unwrap_or_else(|| float!(0));
    let updated = (current - amount)?;

    if updated.lt(float!(0))? {
        return Ok(false);
    }

    state.wallet_balances.insert(asset.to_string(), updated);

    Ok(true)
}

/// Registers all dynamic endpoints on the mock server.
fn register_endpoints(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    register_account_endpoint(server, state);
    register_activities_endpoint(server, state);
    register_calendar_endpoint(server, state);
    register_positions_endpoint(server, state);
    register_wallet_get_endpoint(server, state);
    register_market_data_endpoint(server, state);
    register_asset_endpoint(server);
    register_order_placement_endpoint(server, state);
    register_order_by_client_order_id_endpoint(server, state);
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
                    "cash_withdrawable": cash,
                    "buying_power": buying_power,
                    "non_marginable_buying_power": buying_power,
                }),
            )
        });
    });
}

/// Serves `/v1/accounts/activities` from the mock's activity ledger, honoring
/// the query params the production client sends: `activity_types` (comma
/// list), `after` / `until` (RFC3339, exclusive bounds on
/// `transaction_time`), `page_size`, and `page_token` (the id of the last
/// activity of the previous page; the next page starts strictly after it).
/// Without this endpoint every `/pnl` request against the mock fails with a
/// 502 instead of rendering.
fn register_activities_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);
    server.mock(|when, then| {
        when.method(GET).path("/v1/accounts/activities");
        then.respond_with(move |request: &HttpMockRequest| {
            let query: HashMap<_, _> =
                url::form_urlencoded::parse(request.uri().query().unwrap_or_default().as_bytes())
                    .into_owned()
                    .collect();

            let activity_types: Option<Vec<&str>> = query
                .get("activity_types")
                .map(|types| types.split(',').collect());
            let after = query.get("after").and_then(|value| parse_rfc3339(value));
            let until = query.get("until").and_then(|value| parse_rfc3339(value));
            let page_size = query
                .get("page_size")
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(100);
            let page_token = query.get("page_token");

            let state = lock(&state);
            let matching: Vec<&MockActivity> = state
                .activities
                .iter()
                .filter(|activity| {
                    activity_types
                        .as_ref()
                        .is_none_or(|types| types.contains(&activity.activity_type.as_str()))
                })
                .filter(|activity| after.is_none_or(|after| activity.transaction_time > after))
                .filter(|activity| until.is_none_or(|until| activity.transaction_time < until))
                .collect();
            // An unknown token yields an empty page rather than an error --
            // matching "no activities after this cursor".
            let page_start = page_token.map_or(0, |token| {
                matching
                    .iter()
                    .position(|activity| &activity.id == token)
                    .map_or(matching.len(), |position| position + 1)
            });
            let page: Vec<Value> = matching
                .iter()
                .skip(page_start)
                .take(page_size)
                .map(|activity| {
                    json!({
                        "id": activity.id,
                        "activity_type": activity.activity_type,
                        "net_amount": activity.net_amount,
                        "symbol": activity.symbol,
                        "transaction_time": activity.transaction_time.to_rfc3339(),
                    })
                })
                .collect();
            drop(state);

            json_response(200, &Value::Array(page))
        });
    });
}

/// Parses an RFC3339 query-param timestamp, dropping unparseable values.
fn parse_rfc3339(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|parsed| parsed.with_timezone(&Utc))
}

fn register_calendar_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);
    server.mock(|when, then| {
        when.method(GET).path("/v1/calendar");
        then.respond_with(move |_request: &HttpMockRequest| {
            let mut state = lock(&state);

            if state.calendar_failures_remaining > 0 {
                state.calendar_failures_remaining -= 1;
                drop(state);
                return json_response(
                    503,
                    &json!({"message": "calendar temporarily unavailable (chaos)"}),
                );
            }

            let entries: Vec<Value> = state
                .calendar_entries
                .iter()
                .map(|entry| {
                    json!({
                        "date": entry.date,
                        "open": entry.open,
                        "close": entry.close,
                        "session_open": entry.session_open,
                        "session_close": entry.session_close
                    })
                })
                .collect();
            drop(state);
            json_response(200, &Value::Array(entries))
        });
    });
}

fn register_positions_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);
    let positions_path = format!("/v1/trading/accounts/{TEST_ACCOUNT_ID}/positions");
    let position_prefix = format!("{positions_path}/");

    server.mock(move |when, then| {
        when.method(GET).path_prefix(positions_path.clone());
        then.respond_with(move |request: &HttpMockRequest| {
            let result = (|| -> Result<Vec<Value>, rain_math_float::FloatError> {
                let state = lock(&state);
                let mut positions = state
                    .account
                    .positions
                    .values()
                    .map(|pos| {
                        let is_neg = pos
                            .quantity
                            .lt(Float::from_raw(alloy::primitives::B256::ZERO))?;
                        let side = if is_neg { "short" } else { "long" };
                        let abs_qty = pos.quantity.abs()?;
                        let mut entry = json!({
                            "symbol": pos.symbol,
                            "qty_available": format_float_with_fallback(&abs_qty),
                            "market_value": format_float_with_fallback(&pos.market_value),
                            "side": side,
                            "avg_entry_price": "0",
                        });
                        if let Some(mark) = state.symbol_fill_prices.get(&pos.symbol) {
                            entry["current_price"] =
                                Value::String(format_float_with_fallback(mark));
                        }
                        Ok::<Value, rain_math_float::FloatError>(entry)
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let usdc_balance = state
                    .wallet_balances
                    .get("USDC")
                    .copied()
                    .unwrap_or_else(|| float!(0));
                drop(state);

                if !usdc_balance.is_zero()? {
                    positions.push(json!({
                        "symbol": "USDCUSD",
                        "qty_available": format_float_with_fallback(&usdc_balance),
                        "qty": format_float_with_fallback(&usdc_balance),
                        "market_value": format_float_with_fallback(&usdc_balance),
                        "side": "long",
                        "asset_class": "crypto",
                        "exchange": "CRYPTO",
                        "avg_entry_price": "1",
                    }));
                }

                Ok(positions)
            })();

            match result {
                Ok(positions) if request.uri().path() == positions_path => {
                    json_response(200, &Value::Array(positions))
                }
                Ok(positions) => {
                    let request_uri = request.uri();
                    let request_path = request_uri.path();
                    let Some(symbol) = request_path.strip_prefix(&position_prefix) else {
                        return json_response(404, &json!({"message": "not found"}));
                    };
                    let Some(position) = positions.into_iter().find(|position| {
                        position.get("symbol").and_then(Value::as_str) == Some(symbol)
                    }) else {
                        return json_response(404, &json!({"message": "position does not exist"}));
                    };
                    json_response(200, &position)
                }
                Err(error) => json_response(
                    500,
                    &json!({"message": format!("Mock /positions FloatError: {error}")}),
                ),
            }
        });
    });
}

enum MockMarketDataKind {
    Trade,
    Quote,
}

fn register_market_data_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);
    let prefix = "/v2/stocks/";

    server.mock(|when, then| {
        when.method(GET).path_prefix(prefix);
        then.respond_with(move |request: &HttpMockRequest| {
            let request_path = request.uri().path().to_owned();
            let Some(path) = request_path.strip_prefix(prefix) else {
                return json_response(404, &json!({"message": "not found"}));
            };
            let (symbol, kind) = if let Some(symbol) = path.strip_suffix("/trades/latest") {
                (symbol, MockMarketDataKind::Trade)
            } else if let Some(symbol) = path.strip_suffix("/quotes/latest") {
                if request.uri().query() != Some("feed=delayed_sip") {
                    return json_response(
                        400,
                        &json!({"message": "mock quotes require feed=delayed_sip"}),
                    );
                }
                (symbol, MockMarketDataKind::Quote)
            } else {
                return json_response(404, &json!({"message": "not found"}));
            };

            let Ok(symbol) = Symbol::new(symbol) else {
                return json_response(404, &json!({"message": "unknown symbol"}));
            };

            let Some(price) = lock(&state).symbol_fill_prices.get(&symbol).copied() else {
                return json_response(404, &json!({"message": "no market price configured"}));
            };

            let body = match kind {
                MockMarketDataKind::Trade => json!({
                    "trade": {
                        "p": format_float_with_fallback(&price),
                    }
                }),
                MockMarketDataKind::Quote => json!({
                    "symbol": symbol,
                    "quote": {
                        "bp": format_float_with_fallback(&price),
                        "ap": format_float_with_fallback(&price),
                    }
                }),
            };
            json_response(200, &body)
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
            let Some(side) = body["side"].as_str() else {
                return json_response(
                    400,
                    &json!({"message": "missing or non-string field: side"}),
                );
            };

            let Ok(symbol) = Symbol::new(symbol) else {
                return json_response(400, &json!({"message": "symbol cannot be empty"}));
            };
            let sizing = match (body["qty"].as_str(), body["notional"].as_str()) {
                (Some(_), Some(_)) => {
                    return json_response(
                        400,
                        &json!({"message": "qty and notional are mutually exclusive"}),
                    );
                }
                (None, None) => {
                    return json_response(
                        400,
                        &json!({"message": "missing or non-string field: qty"}),
                    );
                }
                (Some(qty), None) => {
                    let Ok(quantity) = Float::parse(qty.to_string()) else {
                        return json_response(
                            400,
                            &json!({"message": format!("invalid qty: {qty}")}),
                        );
                    };
                    OrderSizing::Quantity(quantity)
                }
                (None, Some(notional)) => {
                    let Ok(amount) = Float::parse(notional.to_string()) else {
                        return json_response(
                            400,
                            &json!({"message": format!("invalid notional: {notional}")}),
                        );
                    };
                    OrderSizing::Notional(amount)
                }
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
            let client_order_id = body["client_order_id"].as_str().map(str::to_owned);
            let order_id = Uuid::new_v4().to_string();

            let mut state = lock(&state);

            // Session/credential expiry: reject before recording anything,
            // so the caller's retry must place fresh rather than dedupe.
            if state.unauthorized_placement_failures_remaining > 0 {
                state.unauthorized_placement_failures_remaining -= 1;
                drop(state);
                return json_response(
                    401,
                    &json!({"message": "request is not authorized (chaos)"}),
                );
            }

            if matches!(state.mode, MockMode::PlacementFails) {
                return json_response(422, &json!({"message": "order rejected"}));
            }

            if symbol == "USDCUSD" {
                return handle_crypto_order(&mut state, &order_id, &symbol, sizing, side);
            }

            // The bot only sizes crypto conversions by notional; equity
            // orders always name a quantity, so the mock keeps its equity
            // ledger quantity-only.
            let OrderSizing::Quantity(quantity) = sizing else {
                return json_response(
                    422,
                    &json!({"message": "notional orders are supported only for USDCUSD in this mock"}),
                );
            };

            // Real Alpaca rejects a re-used `client_order_id` on an active order
            // with a 422 ("client_order_id must be unique") rather than deduping
            // to a 2xx. Mirror that so the executor's recovery path -- look the
            // order up by client_order_id and adopt it -- is exercised
            // end-to-end instead of being papered over by a mock-only 200.
            if let Some(client_id) = client_order_id.as_deref() {
                let already_recorded = state
                    .orders
                    .values()
                    .any(|order| order.client_order_id.as_deref() == Some(client_id));
                if already_recorded {
                    drop(state);
                    return json_response(422, &super::duplicate_client_order_id_body());
                }
            }

            let planned_outcome = match state.mode {
                MockMode::RotatingOutcomes => {
                    let outcome = ROTATING_OUTCOMES[state.rotating_outcome_index];
                    state.rotating_outcome_index =
                        (state.rotating_outcome_index + 1) % ROTATING_OUTCOMES.len();
                    Some(outcome)
                }
                MockMode::HappyPath
                | MockMode::OrderRejected
                | MockMode::PlacementFails
                | MockMode::DelayedFill { .. }
                | MockMode::PartialFillThenCancel => None,
            };

            state.orders.insert(
                order_id.clone(),
                MockOrder {
                    symbol: symbol.clone(),
                    quantity,
                    notional: None,
                    side,
                    status: OrderStatus::New,
                    filled_quantity: float!(0),
                    poll_count: 0,
                    filled_price: None,
                    client_order_id: client_order_id.clone(),
                    planned_outcome,
                },
            );

            if state.transient_placement_failures_remaining > 0 {
                state.transient_placement_failures_remaining -= 1;
                drop(state);
                return json_response(
                    503,
                    &json!({"message": "transient upstream failure (chaos)"}),
                );
            }

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
                    "client_order_id": client_order_id,
                }),
            )
        });
    });
}

/// How an order names its size, mirroring the mutually exclusive
/// `qty`/`notional` fields of the real orders endpoint.
#[derive(Debug, Clone, Copy)]
enum OrderSizing {
    /// Base-asset units to trade (e.g. USDC), at most six decimals.
    Quantity(Float),
    /// USD to spend, which the real API caps at whole cents.
    Notional(Float),
}

/// Handles crypto (USDCUSD) orders which fill immediately.
///
/// A `notional` order fills at the mock's $1 crypto price, so the derived
/// USDC quantity equals the dollars named; the real API additionally keeps
/// `qty` null on such orders, which the response mirrors.
fn handle_crypto_order(
    state: &mut MockState,
    order_id: &str,
    symbol: &Symbol,
    sizing: OrderSizing,
    side: OrderSide,
) -> HttpMockResponse {
    let (quantity, max_decimal_places, excess_precision_message, below_precision_message) =
        match sizing {
            OrderSizing::Quantity(quantity) => (
                quantity,
                6,
                "crypto quantity exceeds USDC precision",
                "crypto quantity is below USDC precision",
            ),
            OrderSizing::Notional(amount) => (
                amount,
                2,
                "notional value must be limited to 2 decimal places",
                "notional value must be limited to 2 decimal places",
            ),
        };

    let quantized_quantity = match crate::truncate_to_decimal_places(quantity, max_decimal_places) {
        Ok(Some(value)) => value,
        Ok(None) => {
            return json_response(422, &json!({"message": below_precision_message}));
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
        return json_response(422, &json!({"message": excess_precision_message}));
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

    let current_wallet_balance = state
        .wallet_balances
        .get("USDC")
        .copied()
        .unwrap_or_else(|| float!(0));
    let updated_wallet_balance = match side {
        OrderSide::Buy => current_wallet_balance + quantized_quantity,
        OrderSide::Sell => current_wallet_balance - quantized_quantity,
    };
    let updated_wallet_balance = match updated_wallet_balance {
        Ok(balance) => balance,
        Err(error) => {
            return json_response(
                500,
                &json!({"message": format!("wallet balance arithmetic error: {error}")}),
            );
        }
    };

    match updated_wallet_balance.lt(float!(0)) {
        Ok(false) => {}
        Ok(true) => {
            return json_response(
                422,
                &json!({"message": "insufficient USDC balance for crypto sell"}),
            );
        }
        Err(error) => {
            return json_response(
                500,
                &json!({"message": format!("wallet balance comparison error: {error}")}),
            );
        }
    }

    match updated_cash {
        Ok(cash) => state.account.cash = cash,
        Err(error) => {
            return json_response(
                500,
                &json!({"message": format!("cash arithmetic error: {error}")}),
            );
        }
    }
    state
        .wallet_balances
        .insert("USDC".to_string(), updated_wallet_balance);

    let fill_price = float!(1);
    let quantity_formatted = format_float_with_fallback(&quantized_quantity);
    let fill_price_formatted = format_float_with_fallback(&fill_price);
    let (qty_response, notional_response, stored_notional) = match sizing {
        OrderSizing::Quantity(_) => (json!(quantity_formatted.as_str()), Value::Null, None),
        OrderSizing::Notional(_) => (
            Value::Null,
            json!(quantity_formatted.as_str()),
            Some(quantized_quantity),
        ),
    };

    state.orders.insert(
        order_id.to_string(),
        MockOrder {
            symbol: symbol.clone(),
            quantity: quantized_quantity,
            notional: stored_notional,
            side,
            status: OrderStatus::Filled,
            filled_quantity: quantized_quantity,
            poll_count: 0,
            filled_price: Some(fill_price),
            client_order_id: None,
            planned_outcome: None,
        },
    );

    json_response(200, &{
        json!({
            "id": order_id,
            "symbol": symbol,
            "qty": qty_response,
            "notional": notional_response,
            "side": side.to_string(),
            "status": "filled",
            "filled_avg_price": fill_price_formatted.as_str(),
            "filled_qty": quantity_formatted.as_str(),
            "created_at": "2025-01-06T12:00:00Z"
        })
    })
}

fn register_order_by_client_order_id_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);
    let path = format!("/v1/trading/accounts/{TEST_ACCOUNT_ID}/orders:by_client_order_id");

    server.mock(|when, then| {
        when.method(GET).path(path.clone());
        then.respond_with(move |request: &HttpMockRequest| {
            let Some(client_order_id) = request.uri().query().and_then(|query| {
                query
                    .split('&')
                    .find_map(|pair| pair.strip_prefix("client_order_id=").map(str::to_owned))
            }) else {
                return json_response(
                    400,
                    &json!({"message": "missing client_order_id query param"}),
                );
            };

            let found = lock(&state).orders.iter().find_map(|(id, order)| {
                (order.client_order_id.as_deref() == Some(client_order_id.as_str())).then(|| {
                    json!({
                        "id": id,
                        "symbol": order.symbol.to_string(),
                        "qty": format_float_with_fallback(&order.quantity),
                        "side": order.side.to_string(),
                        "status": order.status.to_string(),
                        "filled_avg_price": order
                            .filled_price
                            .as_ref()
                            .map(format_float_with_fallback),
                        "client_order_id": client_order_id,
                    })
                })
            });

            found.map_or_else(
                || json_response(404, &json!({"message": "order not found"})),
                |payload| json_response(200, &payload),
            )
        });
    });
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

                if let Err(error) = advance_polled_order(&mut state, &order_id) {
                    return json_response(500, &json!({"message": error.to_string()}));
                }

                let order = &state.orders[&order_id];
                // The real API always reports filled_qty ("0" when unfilled);
                // the client fails closed on terminal responses without it.
                let filled_quantity = format_float_with_fallback(&order.filled_quantity);
                let filled_price: Option<String> =
                    order.filled_price.as_ref().map(format_float_with_fallback);
                // The real API keeps `qty` null on a notional order even
                // after it fills, reporting only `notional` and `filled_qty`.
                let notional = order.notional.as_ref().map(format_float_with_fallback);
                let quantity = notional
                    .is_none()
                    .then(|| format_float_with_fallback(&order.quantity));
                // The real API stamps per-status timestamps; the client fails
                // fast when a terminal status lacks its specific timestamp.
                let filled_at =
                    (order.status == OrderStatus::Filled).then_some("2025-01-01T00:00:01Z");
                let failed_at =
                    (order.status == OrderStatus::Rejected).then_some("2025-01-01T00:00:01Z");
                let canceled_at =
                    (order.status == OrderStatus::Canceled).then_some("2025-01-01T00:00:01Z");
                let body = json!({
                    "id": order_id,
                    "symbol": order.symbol,
                    "qty": quantity,
                    "notional": notional,
                    "side": order.side.to_string(),
                    "status": order.status.to_string(),
                    "filled_avg_price": filled_price,
                    "filled_qty": filled_quantity,
                    "created_at": "2025-01-01T00:00:00Z",
                    "updated_at": "2025-01-01T00:00:01Z",
                    "filled_at": filled_at,
                    "failed_at": failed_at,
                    "canceled_at": canceled_at,
                });
                drop(state);
                body
            };

            json_response(200, &response_body)
        });
    });
}

/// Resolves which single-outcome mode governs one polled order. An order
/// placed under [`MockMode::RotatingOutcomes`] follows the outcome it was
/// assigned at placement; every other order follows the server-wide mode.
const fn poll_mode(mode: MockMode, planned_outcome: Option<PlannedOutcome>) -> MockMode {
    match planned_outcome {
        Some(PlannedOutcome::Filled) => MockMode::HappyPath,
        Some(PlannedOutcome::Rejected) => MockMode::OrderRejected,
        Some(PlannedOutcome::CancelledAfterPartialFill) => MockMode::PartialFillThenCancel,
        // Orders placed before the switch to `RotatingOutcomes` carry no
        // planned outcome; they keep the happy path they were placed under.
        None => match mode {
            MockMode::RotatingOutcomes => MockMode::HappyPath,
            other => other,
        },
    }
}

/// Why the mock could not advance a polled order.
#[derive(Debug, thiserror::Error)]
enum PollAdvanceError {
    #[error("no fill price configured for {symbol}")]
    NoFillPrice { symbol: Symbol },
    #[error("fill arithmetic error: {0}")]
    Arithmetic(#[from] rain_math_float::FloatError),
}

/// Applies one poll's worth of state transition to an order, per the mode
/// governing it. Called once per status request, after the poll counter has
/// been incremented.
fn advance_polled_order(state: &mut MockState, order_id: &str) -> Result<(), PollAdvanceError> {
    let order = &state.orders[order_id];
    let symbol = order.symbol.clone();
    let poll_count = order.poll_count;

    match poll_mode(state.mode, order.planned_outcome) {
        MockMode::HappyPath => {
            let delay = state.symbol_fill_delays.get(&symbol).copied().unwrap_or(0);
            if poll_count >= delay {
                apply_fill(
                    state,
                    order_id,
                    fill_price(state, &symbol)?,
                    FillExtent::Full,
                )?;
            }
            Ok(())
        }
        MockMode::DelayedFill { polls_before_fill } => {
            if poll_count >= polls_before_fill {
                apply_fill(
                    state,
                    order_id,
                    fill_price(state, &symbol)?,
                    FillExtent::Full,
                )?;
            }
            Ok(())
        }
        MockMode::OrderRejected => {
            if let Some(order) = state.orders.get_mut(order_id) {
                order.status = OrderStatus::Rejected;
            }
            Ok(())
        }
        MockMode::PartialFillThenCancel => {
            // The first poll reports the partial fill so the bot records it
            // on the aggregate; the next poll cancels, retaining that fill.
            if state.orders[order_id].status == OrderStatus::New {
                apply_fill(
                    state,
                    order_id,
                    fill_price(state, &symbol)?,
                    FillExtent::Half,
                )?;
            } else if let Some(order) = state.orders.get_mut(order_id) {
                order.status = OrderStatus::Canceled;
            }
            Ok(())
        }
        // `PlacementFails` rejects at order creation, so no order exists to
        // poll; `poll_mode` never resolves to the rotating mode itself.
        MockMode::PlacementFails | MockMode::RotatingOutcomes => Ok(()),
    }
}

fn fill_price(state: &MockState, symbol: &Symbol) -> Result<Float, PollAdvanceError> {
    state
        .symbol_fill_prices
        .get(symbol)
        .copied()
        .ok_or_else(|| PollAdvanceError::NoFillPrice {
            symbol: symbol.clone(),
        })
}

/// How much of an order's quantity a fill executes.
#[derive(Debug, Clone, Copy)]
enum FillExtent {
    Full,
    Half,
}

/// Executes a fill against a "new" order and updates account balances.
///
/// A [`FillExtent::Full`] fill moves the order to "filled"; a
/// [`FillExtent::Half`] fill moves it to "partially_filled", leaving the
/// remainder open for a later cancellation.
fn apply_fill(
    state: &mut MockState,
    order_id: &str,
    fill_price: Float,
    extent: FillExtent,
) -> Result<(), rain_math_float::FloatError> {
    let should_fill = state
        .orders
        .get(order_id)
        .is_some_and(|order| order.status == OrderStatus::New);
    if !should_fill {
        return Ok(());
    }

    let symbol_key = state.orders[order_id].symbol.clone();
    let side = state.orders[order_id].side;
    let ordered_quantity = state.orders[order_id].quantity;
    let (qty, status) = match extent {
        FillExtent::Full => (ordered_quantity, OrderStatus::Filled),
        FillExtent::Half => (
            (ordered_quantity * float!(0.5))?,
            OrderStatus::PartiallyFilled,
        ),
    };
    let raw_cost = (qty * fill_price)?;
    let (cost_fixed, _) = raw_cost.to_fixed_decimal_lossy(2)?;
    let cost = Float::from_fixed_decimal(cost_fixed, 2)?;

    if let Some(order) = state.orders.get_mut(order_id) {
        order.status = status;
        order.filled_quantity = qty;
        order.filled_price = Some(fill_price);
    }

    // One FEE activity per executed fill (full or partial). Ledger-only: the
    // amount is NOT deducted from mock cash, so e2e balance-convergence
    // assertions are unaffected while /pnl still sees a broker fee to
    // classify.
    push_state_activity(
        state,
        "FEE",
        MOCK_FILL_FEE_USD,
        Some(symbol_key.to_string()),
        Utc::now(),
    );

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

/// Appends an activity with a monotonic id; insertion order is ascending
/// serve order, and the id doubles as the pagination cursor.
fn push_state_activity(
    state: &mut MockState,
    activity_type: &str,
    net_amount: &str,
    symbol: Option<String>,
    transaction_time: DateTime<Utc>,
) {
    let id = format!("mock-activity-{:06}", state.activities.len() + 1);
    state.activities.push(MockActivity {
        id,
        activity_type: activity_type.to_string(),
        net_amount: net_amount.to_string(),
        symbol,
        transaction_time,
    });
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

                let deposit_addr = lock(&state).alpaca_deposit_address.clone();

                return json_response(
                    200,
                    &json!({
                        "asset_id": "00000000-0000-0000-0000-000000000000",
                        "address": deposit_addr,
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

                match subtract_wallet_balance(&mut state, &asset, amount) {
                    Ok(true) => {}
                    Ok(false) => {
                        return json_response(
                            422,
                            &json!({"message": "insufficient wallet balance for transfer"}),
                        );
                    }
                    Err(error) => {
                        return json_response(
                            500,
                            &json!({"message": format!(
                                "wallet balance arithmetic error: {error}"
                            )}),
                        );
                    }
                }

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
                    advance_wallet_transfer_status(transfer);
                }

                state
                    .wallet_transfers
                    .iter()
                    .map(wallet_transfer_response)
                    .collect()
            };

            json_response(200, &Value::Array(transfers))
        });
    });
}

fn register_wallet_transfer_get_by_id_endpoint(server: &MockServer, state: &Arc<Mutex<MockState>>) {
    let state = Arc::clone(state);
    let prefix = format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers/");

    server.mock(|when, then| {
        when.method(GET).path_prefix(&prefix);
        then.respond_with(move |request: &HttpMockRequest| {
            let path = request.uri().path().to_string();
            let transfer_id = path.strip_prefix(&prefix).unwrap_or("");

            if transfer_id.is_empty() || transfer_id.contains('/') {
                return json_response(404, &json!({"message": "transfer not found"}));
            }

            let Some(response_body) = wallet_transfer_response_by_id(&state, transfer_id) else {
                return json_response(404, &json!({"message": "transfer not found"}));
            };

            json_response(200, &response_body)
        });
    });
}

fn wallet_transfer_response_by_id(state: &Mutex<MockState>, transfer_id: &str) -> Option<Value> {
    let mut state = lock(state);
    let transfer = state
        .wallet_transfers
        .iter_mut()
        .find(|transfer| transfer.transfer_id == transfer_id)?;

    advance_wallet_transfer_status(transfer);
    let response = wallet_transfer_response(transfer);
    drop(state);

    Some(response)
}

fn advance_wallet_transfer_status(transfer: &mut MockWalletTransfer) {
    match transfer.status {
        TransferStatus::Pending | TransferStatus::Processing => {}
        TransferStatus::Complete => return,
    }

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

fn wallet_transfer_response(transfer: &MockWalletTransfer) -> Value {
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
    use std::time::Duration;

    use alloy::primitives::{Address, U256};
    use chrono::{NaiveTime, TimeZone, Utc};
    use chrono_tz::America::New_York;
    use uuid::Uuid;

    use st0x_float_macro::float;

    use super::{
        AlpacaBrokerMock, MockAccount, MockMode, MockPosition, MockState, OrderSide, OrderSizing,
        TEST_ACCOUNT_ID, TEST_API_KEY, TEST_API_SECRET, format_u256_as_usdc, handle_crypto_order,
    };
    use crate::alpaca_broker_api::auth::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode,
    };
    use crate::alpaca_broker_api::client::AlpacaBrokerApiClient;
    use crate::alpaca_broker_api::{
        AccountActivitiesQuery, AlpacaBrokerApi, TimeInForce, market_hours,
    };
    use crate::{Executor, MarketSession, Symbol, Usd};

    /// Regression test for the calendar contract between `AlpacaBrokerMock`
    /// and the market-hours parser: `CalendarDay` requires `session_open` /
    /// `session_close`, so the mock's `/v1/calendar` payload must include
    /// them or every `is_market_open()` / `market_session()` call against
    /// the mock fails deserialization.
    #[tokio::test]
    async fn market_hours_parse_the_mock_calendar() {
        let mock = AlpacaBrokerMock::start()
            .symbol_fill_prices(vec![])
            .symbol_positions(vec![])
            .call()
            .await;

        let ctx = AlpacaBrokerApiCtx {
            api_key: TEST_API_KEY.to_string(),
            api_secret: TEST_API_SECRET.to_string(),
            account_id: AlpacaAccountId::new(Uuid::parse_str(TEST_ACCOUNT_ID).unwrap()),
            mode: Some(AlpacaBrokerApiMode::Mock(mock.base_url())),
            asset_cache_ttl: Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        };

        // Use market_session_at with a pinned noon-ET timestamp so the test is
        // deterministic regardless of when it runs. The mock's calendar entry
        // uses today's ET date with regular hours [00:00, 23:59), so noon ET
        // always falls within the regular window -- unlike Utc::now() which
        // would hit the 1-minute gap at 23:59 ET.
        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let today_et = Utc::now().with_timezone(&New_York).date_naive();
        let noon_et = today_et
            .and_time(NaiveTime::from_hms_opt(12, 0, 0).unwrap())
            .and_local_timezone(New_York)
            .single()
            .unwrap()
            .with_timezone(&Utc);

        mock.set_market_open();
        assert_eq!(
            market_hours::market_session_at(&client, noon_et)
                .await
                .unwrap(),
            MarketSession::Regular,
            "market_session_at must return Regular at noon ET when the calendar is set open"
        );

        mock.set_market_closed();
        assert_eq!(
            market_hours::market_session_at(&client, noon_et)
                .await
                .unwrap(),
            MarketSession::Closed,
            "market_session_at must return Closed when the calendar is set closed"
        );
    }

    /// The dashboard's `/pnl` endpoint fetches account activities (fees,
    /// dividends, margin interest) live from the broker on every request, so
    /// the mock must serve `/v1/accounts/activities` -- otherwise every PnL
    /// report against the simulated stack fails with a 502 instead of
    /// rendering.
    #[tokio::test]
    async fn pnl_activities_fetch_returns_empty_against_the_mock() {
        let mock = AlpacaBrokerMock::start()
            .symbol_fill_prices(vec![])
            .symbol_positions(vec![])
            .call()
            .await;

        let ctx = AlpacaBrokerApiCtx {
            api_key: TEST_API_KEY.to_string(),
            api_secret: TEST_API_SECRET.to_string(),
            account_id: AlpacaAccountId::new(Uuid::parse_str(TEST_ACCOUNT_ID).unwrap()),
            mode: Some(AlpacaBrokerApiMode::Mock(mock.base_url())),
            asset_cache_ttl: Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        };

        let activities = ctx
            .fetch_account_activities(&AccountActivitiesQuery::pnl(None, None))
            .await
            .unwrap();
        assert_eq!(activities.len(), 0);
    }

    /// Broker-API context pointed at the given mock, for driving the
    /// account-activities client against it.
    fn activities_ctx(mock: &AlpacaBrokerMock) -> AlpacaBrokerApiCtx {
        AlpacaBrokerApiCtx {
            api_key: TEST_API_KEY.to_string(),
            api_secret: TEST_API_SECRET.to_string(),
            account_id: AlpacaAccountId::new(Uuid::parse_str(TEST_ACCOUNT_ID).unwrap()),
            mode: Some(AlpacaBrokerApiMode::Mock(mock.base_url())),
            asset_cache_ttl: Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        }
    }

    #[tokio::test]
    async fn delayed_sip_quote_is_served_by_the_mock_broker() {
        let symbol = Symbol::new("AAPL").unwrap();
        let mock = AlpacaBrokerMock::start()
            .symbol_fill_prices(vec![(symbol.clone(), float!(150))])
            .symbol_positions(vec![])
            .call()
            .await;
        let ctx = AlpacaBrokerApiCtx {
            api_key: TEST_API_KEY.to_string(),
            api_secret: TEST_API_SECRET.to_string(),
            account_id: AlpacaAccountId::new(Uuid::parse_str(TEST_ACCOUNT_ID).unwrap()),
            mode: Some(AlpacaBrokerApiMode::Mock(mock.base_url())),
            asset_cache_ttl: Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        };
        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();

        let quote = executor
            .fetch_latest_quote(&symbol)
            .await
            .unwrap()
            .expect("Alpaca mock supports latest quotes");

        assert_eq!(quote.bid().inner(), Usd::new(float!(150)));
        assert_eq!(quote.ask().inner(), Usd::new(float!(150)));
    }

    #[tokio::test]
    async fn position_mark_uses_the_mock_brokers_single_position_endpoint() {
        let symbol = Symbol::new("AAPL").unwrap();
        let mock = AlpacaBrokerMock::start()
            .symbol_fill_prices(vec![(symbol.clone(), float!(150))])
            .symbol_positions(vec![MockPosition {
                symbol: symbol.clone(),
                quantity: float!(3),
                market_value: float!(450),
            }])
            .call()
            .await;
        let ctx = AlpacaBrokerApiCtx {
            api_key: TEST_API_KEY.to_string(),
            api_secret: TEST_API_SECRET.to_string(),
            account_id: AlpacaAccountId::new(Uuid::parse_str(TEST_ACCOUNT_ID).unwrap()),
            mode: Some(AlpacaBrokerApiMode::Mock(mock.base_url())),
            asset_cache_ttl: Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        };
        let executor = AlpacaBrokerApi::try_from_ctx(ctx).await.unwrap();

        let mark = executor
            .fetch_position_mark(&symbol)
            .await
            .unwrap()
            .expect("configured mock position carries a mark");

        assert_eq!(mark.inner(), Usd::new(float!(150)));
    }

    /// Every executed fill leaves a broker-fee activity in the ledger, so a
    /// simulated PnL report classifies non-zero Alpaca fees instead of
    /// rendering every broker cost bucket as `not_ingested`.
    #[tokio::test]
    async fn fill_records_a_fee_activity() {
        let symbol = Symbol::new("AAPL").unwrap();
        let mock = AlpacaBrokerMock::start()
            .symbol_fill_prices(vec![(symbol, float!(150))])
            .symbol_positions(vec![])
            .call()
            .await;

        let client = reqwest::Client::new();
        let orders_url = format!(
            "{}/v1/trading/accounts/{TEST_ACCOUNT_ID}/orders",
            mock.base_url()
        );
        let placed: serde_json::Value = client
            .post(&orders_url)
            .json(&serde_json::json!({
                "symbol": "AAPL",
                "qty": "4",
                "side": "buy",
                "client_order_id": "fee-fill",
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let order_id = placed["id"].as_str().unwrap();
        // HappyPath with no configured delay fills on the first poll.
        client
            .get(format!("{orders_url}/{order_id}"))
            .send()
            .await
            .unwrap();

        let activities = activities_ctx(&mock)
            .fetch_account_activities(&AccountActivitiesQuery::pnl(None, None))
            .await
            .unwrap();
        assert_eq!(activities.len(), 1);
        assert_eq!(activities[0].activity_type, "FEE");
        assert_eq!(activities[0].net_amount.as_deref(), Some("-0.1"));
        assert_eq!(activities[0].symbol.as_deref(), Some("AAPL"));
    }

    /// The endpoint honors the filters the production client sends: only
    /// requested `activity_types` come back, and `after` is an exclusive
    /// bound on `transaction_time`.
    #[tokio::test]
    async fn activities_respect_type_and_time_filters() {
        let mock = AlpacaBrokerMock::start()
            .symbol_fill_prices(vec![])
            .symbol_positions(vec![])
            .call()
            .await;
        let symbol = Symbol::new("AAPL").unwrap();
        let at = |day: u32| Utc.with_ymd_and_hms(2026, 1, day, 12, 0, 0).unwrap();
        mock.push_activity("FEE", "-0.5", Some(symbol.clone()), at(1));
        mock.push_activity("INT", "-2", None, at(2));
        mock.push_activity("FEE", "-0.7", Some(symbol), at(3));

        let fees = activities_ctx(&mock)
            .fetch_account_activities(&AccountActivitiesQuery {
                activity_types: vec!["FEE".to_string()],
                after: Some(at(1)),
                until: None,
            })
            .await
            .unwrap();
        assert_eq!(fees.len(), 1);
        assert_eq!(fees[0].net_amount.as_deref(), Some("-0.7"));
    }

    /// A ledger larger than one page round-trips through the client's
    /// `page_token` pagination without tripping its pagination-invariant
    /// checks.
    #[tokio::test]
    async fn activities_paginate_beyond_one_page() {
        let mock = AlpacaBrokerMock::start()
            .symbol_fill_prices(vec![])
            .symbol_positions(vec![])
            .call()
            .await;
        let symbol = Symbol::new("AAPL").unwrap();
        for _ in 0..101 {
            mock.push_activity("FEE", "-0.1", Some(symbol.clone()), Utc::now());
        }

        let activities = activities_ctx(&mock)
            .fetch_account_activities(&AccountActivitiesQuery::pnl(None, None))
            .await
            .unwrap();
        assert_eq!(activities.len(), 101);
        assert_eq!(activities[100].id, "mock-activity-000101");
    }

    /// Places one order per rotation step and polls each to its terminal
    /// status. `RotatingOutcomes` is only useful if a single run yields all
    /// three outcomes the dashboard renders, so this asserts the full cycle
    /// -- including the fill quantities and terminal timestamps the order
    /// status parser needs to accept a cancellation.
    #[tokio::test]
    async fn rotating_outcomes_drives_each_order_to_its_own_terminal_status() {
        let symbol = Symbol::new("AAPL").unwrap();
        let broker = AlpacaBrokerMock::start()
            .symbol_fill_prices(vec![(symbol, float!(150))])
            .symbol_positions(vec![])
            .call()
            .await;
        broker.set_mode(MockMode::RotatingOutcomes);

        let client = reqwest::Client::new();
        let orders_url = format!(
            "{}/v1/trading/accounts/{TEST_ACCOUNT_ID}/orders",
            broker.base_url()
        );

        let mut order_ids = Vec::new();
        for index in 0..3 {
            let placed: serde_json::Value = client
                .post(&orders_url)
                .json(&serde_json::json!({
                    "symbol": "AAPL",
                    "qty": "4",
                    "side": "buy",
                    "client_order_id": format!("rotating-{index}"),
                }))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            order_ids.push(placed["id"].as_str().unwrap().to_string());
        }

        let poll = async |order_id: &str| -> serde_json::Value {
            client
                .get(format!("{orders_url}/{order_id}"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap()
        };

        let filled = poll(&order_ids[0]).await;
        assert_eq!(filled["status"], serde_json::json!("filled"));
        assert_eq!(filled["filled_qty"], serde_json::json!("4"));
        assert_eq!(
            filled["filled_at"],
            serde_json::json!("2025-01-01T00:00:01Z")
        );

        let rejected = poll(&order_ids[1]).await;
        assert_eq!(rejected["status"], serde_json::json!("rejected"));
        assert_eq!(rejected["filled_qty"], serde_json::json!("0"));

        let partial = poll(&order_ids[2]).await;
        assert_eq!(partial["status"], serde_json::json!("partially_filled"));
        assert_eq!(partial["filled_qty"], serde_json::json!("2"));

        let cancelled = poll(&order_ids[2]).await;
        assert_eq!(cancelled["status"], serde_json::json!("canceled"));
        assert_eq!(cancelled["filled_qty"], serde_json::json!("2"));
        assert_eq!(
            cancelled["canceled_at"],
            serde_json::json!("2025-01-01T00:00:01Z")
        );
    }

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

    #[tokio::test]
    async fn wallet_transfer_by_id_endpoint_returns_single_transfer_and_advances_status() {
        let broker = AlpacaBrokerMock::start()
            .symbol_fill_prices(vec![])
            .symbol_positions(vec![])
            .call()
            .await;
        let owner = Address::random();
        broker.set_wallet_usdc_balance(float!(25));
        broker.register_wallet_endpoints(owner);

        let client = reqwest::Client::new();
        let transfer_url = format!(
            "{}/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers",
            broker.base_url()
        );
        let created: serde_json::Value = client
            .post(&transfer_url)
            .json(&serde_json::json!({
                "amount": "12.5",
                "asset": "USDC",
                "address": format!("{owner:#x}")
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let transfer_id = created["id"].as_str().unwrap();
        let by_id_url = format!("{transfer_url}/{transfer_id}");

        let first_poll: serde_json::Value = client
            .get(&by_id_url)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(first_poll["id"], transfer_id);
        assert_eq!(first_poll["status"], "PROCESSING");
        assert!(first_poll["tx_hash"].is_null());

        let second_poll: serde_json::Value = client
            .get(&by_id_url)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(second_poll["id"], transfer_id);
        assert_eq!(second_poll["status"], "COMPLETE");
        assert!(second_poll["tx_hash"].as_str().unwrap().starts_with("0x"));

        let missing = client
            .get(format!("{transfer_url}/missing-transfer-id"))
            .send()
            .await
            .unwrap();
        assert_eq!(missing.status().as_u16(), 404);
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
            rotating_outcome_index: 0,
            symbol_fill_delays: HashMap::new(),
            calendar_entries: vec![],
            wallet_transfers: vec![],
            alpaca_deposit_address: format!("{:#x}", Address::ZERO),
            wallet_balances: HashMap::new(),
            whitelisted_addresses: vec![],
            transient_placement_failures_remaining: 0,
            calendar_failures_remaining: 0,
            unauthorized_placement_failures_remaining: 0,
            activities: vec![],
        };

        let response = handle_crypto_order(
            &mut state,
            "order-1",
            &Symbol::new("USDCUSD").unwrap(),
            OrderSizing::Quantity(float!(12.345678)),
            OrderSide::Buy,
        );

        assert_eq!(response.status, Some(200));

        let expected_cash = float!(87.66);
        assert!(
            state.account.cash.eq(expected_cash).unwrap(),
            "cash should be 100 - 12.34 after cent quantization, got: {:?}",
            state.account.cash
        );
        assert!(
            state.wallet_balances["USDC"].eq(float!(12.345678)).unwrap(),
            "buying USDCUSD should increase Alpaca USDC balance"
        );

        let order = state.orders.get("order-1").unwrap();
        assert!(order.quantity.eq(float!(12.345678)).unwrap());
    }

    #[test]
    fn crypto_sell_debits_wallet_usdc_before_crediting_cash() {
        let mut state = MockState {
            account: MockAccount {
                cash: float!(100),
                buying_power: float!(100),
                positions: HashMap::new(),
            },
            orders: HashMap::new(),
            symbol_fill_prices: HashMap::new(),
            mode: MockMode::HappyPath,
            rotating_outcome_index: 0,
            symbol_fill_delays: HashMap::new(),
            calendar_entries: vec![],
            wallet_transfers: vec![],
            alpaca_deposit_address: format!("{:#x}", Address::ZERO),
            wallet_balances: HashMap::from([("USDC".to_string(), float!(20))]),
            whitelisted_addresses: vec![],
            transient_placement_failures_remaining: 0,
            calendar_failures_remaining: 0,
            unauthorized_placement_failures_remaining: 0,
            activities: vec![],
        };

        let response = handle_crypto_order(
            &mut state,
            "order-1",
            &Symbol::new("USDCUSD").unwrap(),
            OrderSizing::Quantity(float!(5)),
            OrderSide::Sell,
        );

        assert_eq!(response.status, Some(200));
        assert!(state.account.cash.eq(float!(105)).unwrap());
        assert!(state.wallet_balances["USDC"].eq(float!(15)).unwrap());

        let rejected = handle_crypto_order(
            &mut state,
            "order-2",
            &Symbol::new("USDCUSD").unwrap(),
            OrderSizing::Quantity(float!(16)),
            OrderSide::Sell,
        );

        assert_eq!(rejected.status, Some(422));
        assert!(
            state.account.cash.eq(float!(105)).unwrap(),
            "rejected USDC sell must not credit USD cash"
        );
        assert!(
            state.wallet_balances["USDC"].eq(float!(15)).unwrap(),
            "rejected USDC sell must not debit Alpaca USDC"
        );
        assert!(!state.orders.contains_key("order-2"));
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
            rotating_outcome_index: 0,
            symbol_fill_delays: HashMap::new(),
            calendar_entries: vec![],
            wallet_transfers: vec![],
            alpaca_deposit_address: format!("{:#x}", Address::ZERO),
            wallet_balances: HashMap::new(),
            whitelisted_addresses: vec![],
            transient_placement_failures_remaining: 0,
            calendar_failures_remaining: 0,
            unauthorized_placement_failures_remaining: 0,
            activities: vec![],
        };

        let response = handle_crypto_order(
            &mut state,
            "order-1",
            &Symbol::new("USDCUSD").unwrap(),
            OrderSizing::Quantity(float!(12.3456789)),
            OrderSide::Buy,
        );

        assert_eq!(response.status, Some(422));
        assert!(!state.orders.contains_key("order-1"));
    }

    /// The production client sizes the USD->USDC buy as a `notional` order
    /// with no `qty` field, which the mock used to reject with 400 "missing
    /// or non-string field: qty" -- timing out every AlpacaToBase rebalancing
    /// e2e flow. Placement and the status poll must both round-trip through
    /// the real client with `qty` null and the fill reported in `filled_qty`.
    #[tokio::test]
    async fn notional_usd_to_usdc_buy_round_trips_against_the_mock() {
        let mock = AlpacaBrokerMock::start()
            .symbol_fill_prices(vec![(Symbol::new("USDCUSD").unwrap(), float!(1))])
            .symbol_positions(vec![])
            .call()
            .await;
        let client = AlpacaBrokerApiClient::new(&activities_ctx(&mock)).unwrap();

        let placed = crate::alpaca_broker_api::order::convert_usdc_usd(
            &client,
            float!(500.25),
            crate::ConversionDirection::UsdToUsdc,
            &crate::ClientOrderId::from_uuid(Uuid::new_v4()),
        )
        .await
        .unwrap();

        assert!(
            placed.quantity.is_none(),
            "a notional order names no qty, got {:?}",
            placed.quantity
        );
        assert!(placed.notional.unwrap().eq(float!(500.25)).unwrap());

        let settled = crate::alpaca_broker_api::order::poll_crypto_order_until_filled(
            &client,
            placed.id,
            crate::alpaca_broker_api::order::ConversionPollDeadlines::PRODUCTION,
        )
        .await
        .unwrap();

        assert!(
            settled.quantity.is_none(),
            "the status poll must keep qty null on a notional order, got {:?}",
            settled.quantity
        );
        assert!(settled.filled_quantity.unwrap().eq(float!(500.25)).unwrap());

        let orders = mock.orders();
        assert_eq!(orders.len(), 1);
        assert!(orders[0].quantity.eq(float!(500.25)).unwrap());
        assert_eq!(orders[0].side, OrderSide::Buy);
    }

    #[test]
    fn crypto_notional_buy_debits_the_dollars_named() {
        let mut state = MockState {
            account: MockAccount {
                cash: float!(100),
                buying_power: float!(100),
                positions: HashMap::new(),
            },
            orders: HashMap::new(),
            symbol_fill_prices: HashMap::new(),
            mode: MockMode::HappyPath,
            rotating_outcome_index: 0,
            symbol_fill_delays: HashMap::new(),
            calendar_entries: vec![],
            wallet_transfers: vec![],
            alpaca_deposit_address: format!("{:#x}", Address::ZERO),
            wallet_balances: HashMap::new(),
            whitelisted_addresses: vec![],
            transient_placement_failures_remaining: 0,
            calendar_failures_remaining: 0,
            unauthorized_placement_failures_remaining: 0,
            activities: vec![],
        };

        let response = handle_crypto_order(
            &mut state,
            "order-1",
            &Symbol::new("USDCUSD").unwrap(),
            OrderSizing::Notional(float!(12.34)),
            OrderSide::Buy,
        );

        assert_eq!(response.status, Some(200));
        assert!(state.account.cash.eq(float!(87.66)).unwrap());
        assert!(
            state.wallet_balances["USDC"].eq(float!(12.34)).unwrap(),
            "a $12.34 notional buy at the mock's $1 fill price must credit 12.34 USDC"
        );

        let order = state.orders.get("order-1").unwrap();
        assert!(order.quantity.eq(float!(12.34)).unwrap());
        assert!(order.notional.unwrap().eq(float!(12.34)).unwrap());
    }

    /// Mirrors the real API's `422 / 42210000` on a notional with more than
    /// two decimal places, so a client regression that stops truncating to
    /// whole cents fails against the mock the same way it would in prod.
    #[test]
    fn crypto_notional_with_sub_cent_precision_is_rejected() {
        let mut state = MockState {
            account: MockAccount {
                cash: float!(100),
                buying_power: float!(100),
                positions: HashMap::new(),
            },
            orders: HashMap::new(),
            symbol_fill_prices: HashMap::new(),
            mode: MockMode::HappyPath,
            rotating_outcome_index: 0,
            symbol_fill_delays: HashMap::new(),
            calendar_entries: vec![],
            wallet_transfers: vec![],
            alpaca_deposit_address: format!("{:#x}", Address::ZERO),
            wallet_balances: HashMap::new(),
            whitelisted_addresses: vec![],
            transient_placement_failures_remaining: 0,
            calendar_failures_remaining: 0,
            unauthorized_placement_failures_remaining: 0,
            activities: vec![],
        };

        let response = handle_crypto_order(
            &mut state,
            "order-1",
            &Symbol::new("USDCUSD").unwrap(),
            OrderSizing::Notional(float!(10.005)),
            OrderSide::Buy,
        );

        assert_eq!(response.status, Some(422));
        assert!(!state.orders.contains_key("order-1"));
    }
}
