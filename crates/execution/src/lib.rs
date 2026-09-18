//! Unified executor trait and implementations for brokerage integration.

use alloy::primitives::U256;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rain_math_float::{Float, FloatError};
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
use std::sync::LazyLock;
use std::time::Duration;
use tracing::{debug, info};

pub(crate) use st0x_float_serde::{
    deserialize_float_from_number_or_string, deserialize_option_float_from_number_or_string,
    serialize_float_as_string,
};

pub use st0x_float_macro::float;

mod alpaca_amount;
pub mod alpaca_broker_api;
mod alpaca_market_data;
mod alpaca_wallet;
pub mod error;
mod hedge_floor;
pub mod mock;
pub mod order;
mod rate_limit;

pub use alpaca_amount::AlpacaAmount;
pub use alpaca_broker_api::{ALPACA_TOKEN_URL, AuthRuntime, KmsJwtError};
pub use alpaca_broker_api::{
    AlpacaAccountId, AlpacaBrokerApi, AlpacaBrokerApiCtx, AlpacaBrokerApiError,
    AlpacaBrokerApiMode, AlpacaBrokerAuth, AssetDetails, ConversionDirection, ConversionOrder,
    CryptoOrderOutcome, DeadlineCancel, JournalResponse, JournalStatus, TimeInForce,
};
// `AlpacaMarketDataError` is wrapped by `AlpacaBrokerApiError::LatestTrade`,
// which delegates its own `backpressure()` classification straight to the
// wrapped error (RAI-1494), so the parent app never needs to name or
// downcast this type in production -- test-only, so tests can still
// construct the exact wrapped shape (`LatestTrade(AlpacaMarketDataError::
// ApiError { .. })`) that `fetch_latest_trade_price` produces.
#[cfg(any(test, feature = "test-support"))]
pub use alpaca_market_data::AlpacaMarketDataError;
pub use error::PersistenceError;
pub use hedge_floor::HedgeFloor;
pub use mock::{MockExecutor, MockExecutorCtx};
pub use order::{
    CancellationOutcome, ClientOrderId, ClientOrderIdError, LimitOrder, MarketOrder,
    OrderFailureTerminality, OrderPlacement, OrderState, OrderStatus, OrderUpdate,
    RecoveredOrderPlacement,
};
pub use rate_limit::retry_after_from_response_headers;

#[cfg(any(test, feature = "test-support"))]
pub use alpaca_wallet::AlpacaWalletClient;
pub use alpaca_wallet::{
    AlpacaTransferId, AlpacaWalletError, AlpacaWalletService, Network, PollingConfig, TokenSymbol,
    Transfer, TransferStatus, TravelRuleInfo, WhitelistEntry, WhitelistStatus,
};

pub use st0x_finance::{
    EmptySymbolError, FractionalShares, HasZero, NotPositive, Positive, SharesConversionError,
    Symbol, ToWholeSharesError, Usd, Usdc,
};

/// Alpaca supports a maximum of 9 decimal places for order quantities.
///
/// Public because it is part of the broker contract consumers validate
/// against (the CLI rejects over-precise manual quantities before
/// submission rather than silently truncating them).
pub const ALPACA_MAX_DECIMAL_PLACES: u8 = 9;

/// Truncates a Float to at most `max_decimals` decimal places.
///
/// Truncation (floor) is used rather than rounding because rounding up could
/// cause an order for more shares than we actually have.
///
/// Returns `Ok(None)` when truncation would collapse a non-zero value to zero,
/// indicating the value is below the precision threshold and should be
/// preserved in inventory rather than submitted to the broker.
pub(crate) fn truncate_to_decimal_places(
    value: Float,
    max_decimals: u8,
) -> Result<Option<Float>, FloatError> {
    let (fixed, lossless) = value.to_fixed_decimal_lossy(max_decimals)?;

    if lossless {
        return Ok(Some(value));
    }

    let is_nonzero = !value.is_zero()?;
    let truncated_is_zero = fixed == U256::ZERO;

    if is_nonzero && truncated_is_zero {
        return Ok(None);
    }

    Float::from_fixed_decimal(fixed, max_decimals).map(Some)
}

/// Describes the current trading session, driving order-type selection.
///
/// - `Regular` -- standard market hours; market orders are used.
/// - `Extended` -- pre-market or after-hours; only limit orders with
///   `extended_hours: true` are allowed by the broker.
/// - `Overnight` -- 20:00-04:00 ET on the Blue Ocean ATS; only limit orders
///   with `day` time-in-force and `extended_hours: true` are allowed, priced
///   from the indicative overnight feed.
/// - `Closed` -- outside all trading sessions: weekends, holidays until 20:00
///   ET that evening (including the overnight window immediately preceding
///   the holiday), and the gap between an early close's session end and
///   20:00 ET.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketSession {
    Regular,
    Extended,
    Overnight,
    Closed,
}

/// Classifies the closure after the current extended session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostCloseGap {
    /// The next trading session begins on the following calendar day.
    OrdinaryOvernight,
    /// At least one full calendar day separates this close from the next
    /// trading session, as on weekends and exchange holidays.
    MultiDayClosure,
    /// The executor could not identify the next trading session.
    Unknown,
    /// The executor does not provide post-close gap classification.
    Unavailable,
}

/// Current market-session classification, with close metadata available only
/// for an extended session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketSessionStatus {
    pub session: MarketSession,
    /// Earliest eligible broker session start for this calendar interval.
    pub session_opens_at: Option<DateTime<Utc>>,
    pub regular_session_closes_at: Option<DateTime<Utc>>,
    pub extended_session_closes_at: Option<DateTime<Utc>>,
    pub post_close_gap: PostCloseGap,
}

impl MarketSessionStatus {
    #[must_use]
    pub const fn without_close_metadata(session: MarketSession) -> Self {
        Self {
            session,
            session_opens_at: None,
            regular_session_closes_at: None,
            extended_session_closes_at: None,
            post_close_gap: PostCloseGap::Unavailable,
        }
    }

    #[must_use]
    pub const fn session(self) -> MarketSession {
        self.session
    }
}

/// Latest national best bid and offer for a symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatestQuote {
    bid: Positive<Usd>,
    ask: Positive<Usd>,
}

impl LatestQuote {
    /// Builds a validated quote whose bid does not exceed its ask.
    pub fn new(bid: Positive<Usd>, ask: Positive<Usd>) -> Result<Self, LatestQuoteError> {
        if ask.inner().lt(&bid.inner())? {
            return Err(LatestQuoteError::Crossed { bid, ask });
        }

        Ok(Self { bid, ask })
    }

    #[must_use]
    pub const fn bid(self) -> Positive<Usd> {
        self.bid
    }

    #[must_use]
    pub const fn ask(self) -> Positive<Usd> {
        self.ask
    }
}

/// An indicative overnight quote with the broker timestamp it was generated
/// at, so consumers can judge its age before pricing from it.
///
/// The overnight feed is indicative (derived from Blue Ocean data), not a
/// firm tape quote: fills can deviate from it, and a stale indicative quote
/// must never be priced from silently. That is why the timestamp is required
/// here while the regular [`LatestQuote`] path ignores it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndicativeQuote {
    pub quote: LatestQuote,
    pub at: DateTime<Utc>,
}

/// Error returned when constructing a latest quote.
#[derive(Debug, thiserror::Error)]
pub enum LatestQuoteError {
    #[error("quote comparison failed: {0}")]
    Float(#[from] FloatError),
    #[error("crossed quote: bid {bid} exceeds ask {ask}")]
    Crossed {
        bid: Positive<Usd>,
        ask: Positive<Usd>,
    },
}

/// A classified rate-limit (HTTP 429) response from an Alpaca API, carrying
/// the broker's `Retry-After` hint when it sent one.
///
/// Each Alpaca error type (`AlpacaBrokerApiError`, `AlpacaMarketDataError`,
/// `AlpacaWalletError`, and `st0x-tokenization`'s `AlpacaTokenizationError`)
/// exposes an inherent `.backpressure() -> Option<Backpressure>` method
/// rather than a free classifier function: these error types are already
/// `pub` domain types call sites match on directly, so a free function would
/// only relocate the "know about these concrete types" fact, not remove it
/// (see RAI-1494's plan). `None` inside `retry_after` already means "429, no
/// usable `Retry-After` header" -- a caller does not need a separate
/// enum case for that, only `Option`'s existing `None`.
///
/// Lives at this crate's root (rather than nested under `alpaca_broker_api`)
/// because `AlpacaWalletError` and `st0x-tokenization`'s
/// `AlpacaTokenizationError` (a downstream crate that already depends on
/// `st0x-execution`) both need to return it too.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backpressure {
    pub retry_after: Option<Duration>,
}

/// Whether an Alpaca failure can plausibly resolve on an immediate retry.
///
/// Classified the same way as [`Backpressure`]: an inherent `.permanence()`
/// on each Alpaca error type, so the decision is made at the type that owns
/// the variant.
///
/// The distinction exists because a caller cannot infer it from the call
/// site: one lookup can fail with an entitlement rejection the account will
/// keep producing, or with a TCP reset that is gone a second later. Only the
/// former is worth abandoning an attempt over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permanence {
    /// The same request against the same account fails the same way: a
    /// deterministic 4xx rejection, or a failure decided locally from a
    /// response that has already arrived.
    Permanent,
    /// Retrying can plausibly succeed: transport errors and timeouts, 5xx
    /// server-side failures, and rate limiting.
    Transient,
}

/// The one place that decides which HTTP statuses clear on their own: 5xx is
/// server-side, 408 is a request timeout, and 429 is rate limiting, all of
/// which can pass on a later attempt. Every other supported 4xx is the account
/// or request itself being rejected. Shared by every Alpaca error type's
/// `permanence()` so the policy cannot drift between them.
pub(crate) fn status_permanence(status: reqwest::StatusCode) -> Permanence {
    match status {
        reqwest::StatusCode::REQUEST_TIMEOUT | reqwest::StatusCode::TOO_MANY_REQUESTS => {
            Permanence::Transient
        }
        status if status.is_client_error() => Permanence::Permanent,
        _ => Permanence::Transient,
    }
}

#[async_trait]
pub trait Executor: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type OrderId: Display + Debug + Send + Sync + Clone;
    type Ctx: Send + Sync + Clone + 'static;

    /// Create and validate executor instance from context
    /// All initialization and validation happens here
    async fn try_from_ctx(ctx: Self::Ctx) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Returns true if the market is currently open for trading.
    async fn is_market_open(&self) -> Result<bool, Self::Error>;

    /// Place a market order for the specified symbol and quantity
    /// Returns order placement details including executor-assigned order ID
    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<OrderPlacement<Self::OrderId>, Self::Error>;

    /// Reads an earlier placement without submitting a new order.
    async fn recover_order_by_client_id(
        &self,
        _order: &MarketOrder,
    ) -> Result<Option<OrderPlacement<Self::OrderId>>, Self::Error> {
        Ok(None)
    }

    /// Get the current status of a specific order
    /// Used to check if pending orders have been filled or failed
    async fn get_order_status(&self, order_id: &Self::OrderId) -> Result<OrderState, Self::Error>;

    async fn get_order_by_client_order_id(
        &self,
        _client_order_id: &ClientOrderId,
    ) -> Result<Option<RecoveredOrderPlacement<Self::OrderId>>, Self::Error> {
        Ok(None)
    }

    /// Return the enum variant representing this executor type
    /// Used for database storage and conditional logic
    fn to_supported_executor(&self) -> SupportedExecutor;

    /// Convert a string representation to the executor's OrderId type
    /// This is needed for converting database-stored order IDs back to executor types
    fn parse_order_id(&self, order_id_str: &str) -> Result<Self::OrderId, Self::Error>;

    /// Tick interval for executor-specific background maintenance work
    /// (token refresh, connection health, etc.).
    ///
    /// Returning `None` means this executor has no maintenance work; the
    /// conductor skips registering the supervised maintenance task entirely.
    /// Returning `Some(interval)` causes the conductor to register a
    /// supervised task that calls [`maintenance_tick`](Self::maintenance_tick)
    /// on every tick.
    fn maintenance_interval(&self) -> Option<Duration> {
        None
    }

    /// One iteration of executor maintenance. Invoked by the supervised
    /// maintenance task on every [`maintenance_interval`](Self::maintenance_interval)
    /// tick. Transient errors are logged by the supervisor wrapper and do not
    /// halt the loop; a panic inside this method is caught by task-supervisor
    /// and triggers a restart.
    async fn maintenance_tick(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Fetches current inventory (positions and cash balance) from the broker.
    ///
    /// Returns `InventoryResult::Unimplemented` if not implemented for the executor.
    /// Returns `InventoryResult::Fetched(Inventory)` on success.
    //
    // NOTE: InventoryResult::Unimplemented is a workaround. This method is needed
    // for auto-rebalancing but not all executors support auto-rebalancing, so
    // implementing the method for non-auto-rebalancing executors is lower priority
    async fn get_inventory(&self) -> Result<InventoryResult, Self::Error>;

    /// Checks whether a counter-trade can be submitted without relying on
    /// margin or short inventory.
    ///
    /// Executors that do not implement preflight checks return
    /// [`CounterTradePreflight::Allowed`] by default so existing non-Alpaca
    /// flows remain unchanged.
    async fn preflight_counter_trade(
        &self,
        _order: MarketOrder,
    ) -> Result<CounterTradePreflight, Self::Error> {
        Ok(CounterTradePreflight::Allowed { reservation: None })
    }

    /// Re-checks a counter-trade while accounting for durable buying-power
    /// reservations.
    ///
    /// The default ignores `reserved` and delegates to
    /// [`preflight_counter_trade`](Self::preflight_counter_trade). Executors
    /// that model cash must override this method to account for reservations.
    async fn preflight_counter_trade_with_reserved_buying_power(
        &self,
        order: MarketOrder,
        _reserved: BuyingPowerReservationCents,
    ) -> Result<CounterTradePreflight, Self::Error> {
        self.preflight_counter_trade(order).await
    }

    /// Checks whether a counter-trade can be submitted at an exact limit price
    /// without relying on margin or short inventory.
    ///
    /// The supplied buy price is already the order's hard cost ceiling, so
    /// implementations must not add another slippage buffer. Sell orders are
    /// unaffected by price because inventory availability does not depend on
    /// it. No default is provided: every implementor must explicitly honor the
    /// exact-price contract rather than silently inheriting a fallback that
    /// ignores the price.
    async fn preflight_counter_trade_at_price(
        &self,
        order: MarketOrder,
        limit_price: Positive<Usd>,
    ) -> Result<CounterTradePreflight, Self::Error>;

    /// Exact-price counterpart to the reservation-aware market preflight.
    ///
    /// The default likewise ignores `reserved`.
    async fn preflight_counter_trade_at_price_with_reserved_buying_power(
        &self,
        order: MarketOrder,
        limit_price: Positive<Usd>,
        _reserved: BuyingPowerReservationCents,
    ) -> Result<CounterTradePreflight, Self::Error> {
        self.preflight_counter_trade_at_price(order, limit_price)
            .await
    }

    /// Returns the current market session (regular, extended, or closed).
    ///
    /// Default implementation delegates to `is_market_open()`, mapping
    /// `true -> Regular` and `false -> Closed`. Executors with extended-hours
    /// support (e.g. Alpaca) override this to distinguish `Extended` sessions.
    async fn market_session(&self) -> Result<MarketSession, Self::Error> {
        if self.is_market_open().await? {
            Ok(MarketSession::Regular)
        } else {
            Ok(MarketSession::Closed)
        }
    }

    /// Returns current market-session classification with close metadata when
    /// the executor can provide it.
    async fn market_session_status(&self) -> Result<MarketSessionStatus, Self::Error> {
        self.market_session()
            .await
            .map(MarketSessionStatus::without_close_metadata)
    }

    /// Fetches an optional current bid/ask quote suitable as the primary
    /// reference for an extended-hours limit order. Executors return `None`
    /// when no such market-data provider is wired; callers then fall back to the
    /// position mark (ADR 0019).
    async fn fetch_primary_limit_quote(
        &self,
        _symbol: &Symbol,
    ) -> Result<Option<LatestQuote>, Self::Error> {
        Ok(None)
    }

    /// Fetches the broker's mark for a symbol -- the price it values an open
    /// position at. This is the required fallback for every extended-hours
    /// limit price when the optional primary market-data source is unavailable
    /// or fails (ADR 0019).
    ///
    /// Returns `None` when the executor holds no position in the symbol, cannot
    /// supply a mark, or reports an unusable one. Callers treat that as "try the
    /// next reference source", never as a reason to abandon the hedge.
    async fn fetch_position_mark(
        &self,
        _symbol: &Symbol,
    ) -> Result<Option<Positive<Usd>>, Self::Error> {
        Ok(None)
    }

    /// Fetches the validated emergency quote for a symbol. Alpaca implements
    /// this with the hardcoded `delayed_sip` feed, after both the optional
    /// primary quote and the position mark fail. Returns `None` when the
    /// executor does not support the fallback lookup.
    async fn fetch_latest_quote(
        &self,
        _symbol: &Symbol,
    ) -> Result<Option<LatestQuote>, Self::Error> {
        Ok(None)
    }

    /// Place a limit order for the specified symbol, quantity, and price.
    ///
    /// Used for counter-trading during extended hours when market orders
    /// are not accepted.
    async fn place_limit_order(
        &self,
        order: LimitOrder,
    ) -> Result<OrderPlacement<Self::OrderId>, Self::Error>;

    /// Cancel a previously placed order by its executor-assigned ID.
    ///
    /// Returns [`CancellationOutcome::Requested`] when the broker accepted
    /// the cancel request, and [`CancellationOutcome::OrderNotFound`] when
    /// the broker does not recognise the order id. The caller must resolve
    /// `OrderNotFound` as terminal rather than retry: re-sending the cancel
    /// can never succeed for an id the broker does not know.
    async fn cancel_order(
        &self,
        order_id: &Self::OrderId,
    ) -> Result<CancellationOutcome, Self::Error>;
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidSharesError {
    #[error("Shares cannot be zero")]
    Zero,
    #[error(transparent)]
    NotPositive(#[from] NotPositive<FractionalShares>),
    #[error(transparent)]
    WholeShares(#[from] ToWholeSharesError),
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),
    #[error("Float conversion failed: {0}")]
    FloatConversion(#[from] FloatError),
}

impl From<SharesConversionError> for InvalidSharesError {
    fn from(error: SharesConversionError) -> Self {
        match error {
            SharesConversionError::NegativeValue(value) => Self::NotPositive(NotPositive {
                value: FractionalShares::new(value),
            }),
            SharesConversionError::FloatConversion(error) => Self::FloatConversion(error),
        }
    }
}

/// Share quantity newtype wrapper with validation
///
/// Represents whole share quantities with bounds checking.
/// Values are constrained to 1..=u32::MAX for practical trading limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct Shares(u32);

impl Shares {
    pub fn new(shares: u64) -> Result<Self, InvalidSharesError> {
        if shares == 0 {
            return Err(InvalidSharesError::Zero);
        }
        Ok(Self(u32::try_from(shares)?))
    }

    pub fn value(&self) -> u32 {
        self.0
    }
}

impl<'de> Deserialize<'de> for Shares {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let shares = u64::deserialize(deserializer)?;
        Self::new(shares).map_err(serde::de::Error::custom)
    }
}

impl Display for Shares {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SupportedExecutor {
    AlpacaBrokerApi,
    DryRun,
}

impl std::fmt::Display for SupportedExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlpacaBrokerApi => write!(f, "alpaca-broker-api"),
            Self::DryRun => write!(f, "dry-run"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid executor: {executor_provided}")]
pub struct InvalidExecutorError {
    executor_provided: String,
}

impl std::str::FromStr for SupportedExecutor {
    type Err = InvalidExecutorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "alpaca-broker-api" => Ok(Self::AlpacaBrokerApi),
            "dry-run" => Ok(Self::DryRun),
            _ => Err(InvalidExecutorError {
                executor_provided: s.to_string(),
            }),
        }
    }
}

pub use st0x_dto::{Direction, InvalidDirectionError};

/// An equity position with symbol, quantity, and optional market value.
#[derive(Debug, Clone)]
pub struct EquityPosition {
    pub symbol: Symbol,
    pub quantity: FractionalShares,
    pub market_value: Option<Float>,
}

/// Account state from the broker.
#[derive(Debug, Clone)]
pub struct Inventory {
    pub positions: Vec<EquityPosition>,
    /// USDC held at Alpaca after USD/USDC conversion and before withdrawal.
    /// `None` when the executor does not model an Alpaca USDC venue (e.g.
    /// `MockExecutor`); a reporting executor uses `Some(Usdc::ZERO)` for a zero
    /// balance so the snapshot is still emitted.
    pub alpaca_usdc: Option<Usdc>,
    pub usd_balance_cents: i64,
    /// Cash buying power available for equity hedges -- Alpaca's `cash`
    /// field, which includes unsettled T+1 equity-sale proceeds and excludes
    /// margin. Used for counter-trade preflight. `None` when the broker
    /// omits the field or the value cannot be converted. See
    /// adrs/1-cash-bp-for-equity-hedges.md.
    pub cash_buying_power_cents: Option<i64>,
    /// Settled cash that can be withdrawn or transferred out -- Alpaca's
    /// `cash_withdrawable` field, excluding T+1 unsettled equity-sale
    /// proceeds. This is the amount actually movable to Raindex during
    /// rebalancing. `None` when the broker omits the field.
    pub cash_withdrawable_cents: Option<i64>,
}

/// Result of fetching inventory from an executor.
///
/// Custom enum to force explicit handling. Unlike `Option` which is easy to `.unwrap()`,
/// this type requires callers to explicitly match on the `Unimplemented` variant.
#[derive(Debug, Clone)]
pub enum InventoryResult {
    /// Fetching inventory is unimplemented for this executor.
    ///
    /// This is a workaround. We need to fetch inventory for auto-rebalancing
    /// but not all executors support auto-rebalancing, so implementing the
    /// method for non-auto-rebalancing executors is lower priority
    Unimplemented,
    /// Successfully fetched inventory.
    Fetched(Inventory),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CounterTradeSkipReason {
    #[error(
        "non-fractionable asset {symbol} requires at least one whole share; requested {requested}"
    )]
    NonFractionableQuantityBelowOne {
        symbol: Symbol,
        requested: Positive<FractionalShares>,
    },
    #[error(
        "insufficient offchain equity inventory: need {required}, but only {available} shares are available"
    )]
    InsufficientEquity {
        required: Positive<FractionalShares>,
        available: FractionalShares,
    },
    #[error(
        "sell held at the hedge floor for {symbol}: {available} shares available, \
         floor keeps {floor}"
    )]
    HeldAtFloor {
        symbol: Symbol,
        floor: FractionalShares,
        available: FractionalShares,
    },
    #[error(
        "insufficient cash buying power: estimated cost {estimated_cost_cents} cents \
         exceeds available {available_buying_power_cents} cents"
    )]
    InsufficientBuyingPower {
        estimated_cost_cents: i64,
        available_buying_power_cents: i64,
    },
    #[error(
        "requested quantity {requested} is below the broker precision of \
         {quantity_decimals} decimals"
    )]
    BelowBrokerPrecision {
        requested: Positive<FractionalShares>,
        quantity_decimals: u8,
    },
    #[error("fractional order notional is below the $1 minimum")]
    BelowMinimumNotional,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CounterTradeReservation {
    Equity {
        symbol: Symbol,
        required: Positive<FractionalShares>,
        available: FractionalShares,
    },
    BuyingPower {
        required: Positive<FractionalShares>,
        estimated_cost_cents: i64,
        available_buying_power_cents: i64,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BuyingPowerReservationCents(u64);

impl BuyingPowerReservationCents {
    pub const ZERO: Self = Self(0);

    pub fn new(cents: i64) -> Result<Self, std::num::TryFromIntError> {
        cents.try_into().map(Self)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn checked_add(self, other: Self) -> Option<Self> {
        self.0.checked_add(other.0).map(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CounterTradePreflight {
    Allowed {
        reservation: Option<CounterTradeReservation>,
    },
    Skipped(CounterTradeSkipReason),
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("{status:?} order requires order_id")]
    MissingOrderId { status: OrderStatus },
    #[error("{status:?} order requires price")]
    MissingPrice { status: OrderStatus },
    #[error("{status:?} order requires executed_at timestamp")]
    MissingExecutedAt { status: OrderStatus },
    #[error("Order not found: {order_id}")]
    OrderNotFound { order_id: String },
    #[error(transparent)]
    InvalidMockOrderId(#[from] mock::MockOrderIdError),
    #[error("Mock executor failure: {message}")]
    MockFailure { message: String },
    #[error("configured mock preflight price is not positive: {0}")]
    NonPositivePreflightPrice(#[from] NotPositive<Usd>),
    #[error(transparent)]
    CounterTradeCost(#[from] CounterTradeCostError),
    #[error("Incomplete order response: {field} missing for {status:?} order")]
    IncompleteOrderResponse { field: String, status: OrderStatus },
    #[error(transparent)]
    EmptySymbol(#[from] EmptySymbolError),
    #[error(transparent)]
    InvalidShares(#[from] InvalidSharesError),
    #[error(transparent)]
    InvalidDirection(#[from] InvalidDirectionError),
    #[error("Numeric conversion error: {0}")]
    NumericConversion(#[from] std::num::TryFromIntError),
    #[error("Date/time parse error: {0}")]
    DateTimeParse(#[from] chrono::ParseError),
    #[error("Float operation failed: {0}")]
    Float(#[from] FloatError),
    #[error(
        "buying power reservation overflow: current reserved {current_reserved_cents} cents, \
         additional {additional_cents} cents"
    )]
    BuyingPowerReservationOverflow {
        current_reserved_cents: i64,
        additional_cents: i64,
    },
}

pub const DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS: u16 = 100;

#[derive(Debug, thiserror::Error)]
pub enum CounterTradeCostError {
    #[error("Float conversion failed: {0}")]
    Float(#[from] FloatError),
    #[error("estimated cost in cents does not fit in i64: {formatted_cents}")]
    EstimatedCostOverflow { formatted_cents: String },
    #[error(transparent)]
    NonPositiveCandidate(#[from] NotPositive<FractionalShares>),
}

pub(crate) fn estimate_buffered_cost_cents(
    shares: Positive<FractionalShares>,
    reference_price: Float,
    slippage_bps: u16,
) -> Result<i64, CounterTradeCostError> {
    let basis_points = Float::parse("10000".to_string())?;
    let slippage = Float::parse(u64::from(slippage_bps).to_string())?;
    let multiplier = ((basis_points + slippage)? / basis_points)?;
    let raw_cost = (shares.inner().inner() * reference_price)?;
    let buffered_cost = (raw_cost * multiplier)?;
    let (fixed_cents, lossless) = buffered_cost.to_fixed_decimal_lossy(2)?;

    let rounded_cents = if lossless {
        fixed_cents
    } else {
        fixed_cents + U256::from(1)
    };

    let formatted_cents = rounded_cents.to_string();
    formatted_cents
        .parse()
        .map_err(|_| CounterTradeCostError::EstimatedCostOverflow { formatted_cents })
}

pub(crate) fn resolve_buy_preflight(
    order: &MarketOrder,
    reference_price: Positive<Usd>,
    slippage_bps: u16,
    available_buying_power_cents: i64,
    quantity_decimals: u8,
) -> Result<CounterTradePreflight, CounterTradeCostError> {
    let reference_price = reference_price.inner().inner();
    let requested = order.shares;
    let Some(quantized) = truncate_to_decimal_places(requested.inner().inner(), quantity_decimals)?
    else {
        return Ok(CounterTradePreflight::Skipped(
            CounterTradeSkipReason::BelowBrokerPrecision {
                requested,
                quantity_decimals,
            },
        ));
    };
    let quantized = Positive::new(FractionalShares::new(quantized))?;
    let requested_cost = estimate_buffered_cost_cents(quantized, reference_price, slippage_bps)?;
    let requested_notional = (quantized.inner().inner() * reference_price)?;
    let (_, requested_is_whole) = quantized.inner().inner().to_fixed_decimal_lossy(0)?;
    let requested_meets_minimum = requested_is_whole || requested_notional.gte(float!(1))?;

    if !requested_meets_minimum {
        let (whole_units, _) = quantized.inner().inner().to_fixed_decimal_lossy(0)?;
        if whole_units == U256::ZERO {
            return Ok(CounterTradePreflight::Skipped(
                CounterTradeSkipReason::BelowMinimumNotional,
            ));
        }

        let whole_shares = Positive::new(FractionalShares::new(Float::from_fixed_decimal(
            whole_units,
            0,
        )?))?;
        let whole_cost = estimate_buffered_cost_cents(whole_shares, reference_price, slippage_bps)?;
        if whole_cost > available_buying_power_cents {
            return Ok(CounterTradePreflight::Skipped(
                CounterTradeSkipReason::InsufficientBuyingPower {
                    estimated_cost_cents: whole_cost,
                    available_buying_power_cents,
                },
            ));
        }
        return Ok(CounterTradePreflight::Allowed {
            reservation: Some(CounterTradeReservation::BuyingPower {
                required: whole_shares,
                estimated_cost_cents: whole_cost,
                available_buying_power_cents,
            }),
        });
    }

    let allowed = if available_buying_power_cents >= requested_cost {
        Some((quantized, requested_cost))
    } else if available_buying_power_cents <= 0 {
        None
    } else {
        let (maximum_units, _) = quantized
            .inner()
            .inner()
            .to_fixed_decimal_lossy(quantity_decimals)?;
        let mut low = U256::ZERO;
        let mut high = maximum_units;

        while low < high {
            let distance = high - low;
            let midpoint = low + distance / U256::from(2) + distance % U256::from(2);
            let shares = Positive::new(FractionalShares::new(Float::from_fixed_decimal(
                midpoint,
                quantity_decimals,
            )?))?;
            let cost = estimate_buffered_cost_cents(shares, reference_price, slippage_bps)?;

            if cost <= available_buying_power_cents {
                low = midpoint;
            } else {
                high = midpoint - U256::from(1);
            }
        }

        if low == U256::ZERO {
            None
        } else {
            let shares = Positive::new(FractionalShares::new(Float::from_fixed_decimal(
                low,
                quantity_decimals,
            )?))?;
            let cost = estimate_buffered_cost_cents(shares, reference_price, slippage_bps)?;
            let notional = (shares.inner().inner() * reference_price)?;
            let (_, is_whole) = shares.inner().inner().to_fixed_decimal_lossy(0)?;
            let meets_fractional_minimum = is_whole || notional.gte(float!(1))?;

            if !meets_fractional_minimum {
                let (whole_units, _) = shares.inner().inner().to_fixed_decimal_lossy(0)?;
                if whole_units == U256::ZERO {
                    return Ok(CounterTradePreflight::Skipped(
                        CounterTradeSkipReason::BelowMinimumNotional,
                    ));
                }

                let whole_shares = Positive::new(FractionalShares::new(
                    Float::from_fixed_decimal(whole_units, 0)?,
                ))?;
                let whole_cost =
                    estimate_buffered_cost_cents(whole_shares, reference_price, slippage_bps)?;
                return Ok(CounterTradePreflight::Allowed {
                    reservation: Some(CounterTradeReservation::BuyingPower {
                        required: whole_shares,
                        estimated_cost_cents: whole_cost,
                        available_buying_power_cents,
                    }),
                });
            }

            Some((shares, cost))
        }
    };

    match allowed {
        Some((required, estimated_cost_cents)) => Ok(CounterTradePreflight::Allowed {
            reservation: Some(CounterTradeReservation::BuyingPower {
                required,
                estimated_cost_cents,
                available_buying_power_cents,
            }),
        }),
        None => Ok(CounterTradePreflight::Skipped(
            CounterTradeSkipReason::InsufficientBuyingPower {
                estimated_cost_cents: requested_cost,
                available_buying_power_cents,
            },
        )),
    }
}

/// Minimum shares threshold for partial hedges. Below this amount, the order
/// is too small for most brokers to accept and would produce repeated
/// rejected-order attempts.
pub(crate) static MINIMUM_PARTIAL_HEDGE_SHARES: LazyLock<Float> = LazyLock::new(|| float!(0.01));

/// Resolves whether a sell counter-trade should proceed given the available
/// broker inventory and the shares the hedge floor keeps in the account.
/// Only `available - floor` is ever offered to the order; the reservation
/// carries that same figure so a batch of hedges cannot sum past the floor.
/// Returns:
/// - `Allowed` with full shares when the sellable book covers the request
/// - `Allowed` with capped shares when the sellable book is partial but
///   above the minimum threshold
/// - `Skipped` with `HeldAtFloor` when inventory exists but the floor keeps
///   all of it, and `InsufficientEquity` when there is no inventory to speak
///   of
pub(crate) fn resolve_sell_preflight(
    order: MarketOrder,
    available: FractionalShares,
    floor: FractionalShares,
) -> Result<CounterTradePreflight, FloatError> {
    let sellable = sellable_above_floor(available, floor)?;
    let requested = order.shares.inner().inner();
    let sufficient = sellable.inner().gte(requested)?;

    if sufficient {
        debug!(
            target: "broker",
            symbol = %order.symbol,
            available = %available,
            floor = %floor,
            required = %order.shares,
            "Preflight passed: sufficient equity for sell"
        );

        return Ok(CounterTradePreflight::Allowed {
            reservation: Some(CounterTradeReservation::Equity {
                symbol: order.symbol,
                required: order.shares,
                available: sellable,
            }),
        });
    }

    let above_minimum = sellable.inner().gte(*MINIMUM_PARTIAL_HEDGE_SHARES)?;

    if above_minimum && let Ok(capped) = Positive::new(sellable) {
        if available.inner().gte(requested)? {
            info!(
                target: "broker",
                symbol = %order.symbol,
                available = %available,
                floor = %floor,
                requested = %order.shares,
                "Partial hedge: holding shares at the hedge floor"
            );
        } else {
            info!(
                target: "broker",
                symbol = %order.symbol,
                available = %available,
                floor = %floor,
                requested = %order.shares,
                "Partial hedge: capping sell to available inventory"
            );
        }

        return Ok(CounterTradePreflight::Allowed {
            reservation: Some(CounterTradeReservation::Equity {
                symbol: order.symbol,
                required: capped,
                available: sellable,
            }),
        });
    }

    // Inventory the broker would have sold sits under the floor: expected,
    // and a different runbook from an empty account.
    if available.inner().gte(*MINIMUM_PARTIAL_HEDGE_SHARES)? {
        return Ok(CounterTradePreflight::Skipped(
            CounterTradeSkipReason::HeldAtFloor {
                symbol: order.symbol,
                floor,
                available,
            },
        ));
    }

    Ok(CounterTradePreflight::Skipped(
        CounterTradeSkipReason::InsufficientEquity {
            required: order.shares,
            available,
        },
    ))
}

/// `available - floor`, clamped at zero.
fn sellable_above_floor(
    available: FractionalShares,
    floor: FractionalShares,
) -> Result<FractionalShares, FloatError> {
    let sellable = (available - floor)?;

    if sellable.inner().lt(FractionalShares::ZERO.inner())? {
        return Ok(FractionalShares::ZERO);
    }

    Ok(sellable)
}

/// Trait for converting executor contexts into their corresponding executor implementations
#[async_trait]
pub trait TryIntoExecutor {
    type Executor: Executor;

    async fn try_into_executor(self)
    -> Result<Self::Executor, <Self::Executor as Executor>::Error>;
}

/// The order ID assigned by the executor (broker) when an order is placed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutorOrderId(String);

impl ExecutorOrderId {
    pub fn new(id: &(impl ToString + ?Sized)) -> Self {
        Self(id.to_string())
    }
}

impl AsRef<str> for ExecutorOrderId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Display for ExecutorOrderId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    #[test]
    fn session_status_without_close_metadata_preserves_each_session_variant() {
        for session in [
            MarketSession::Regular,
            MarketSession::Extended,
            MarketSession::Overnight,
            MarketSession::Closed,
        ] {
            let status = MarketSessionStatus::without_close_metadata(session);
            assert_eq!(status.session(), session);
            assert!(status.session_opens_at.is_none());
            assert!(status.regular_session_closes_at.is_none());
            assert!(status.extended_session_closes_at.is_none());
            assert_eq!(status.post_close_gap, PostCloseGap::Unavailable);
        }
    }

    #[test]
    fn extended_status_keeps_close_metadata_on_the_extended_variant() {
        let closes_at = Utc::now();
        let status = MarketSessionStatus {
            session: MarketSession::Extended,
            session_opens_at: None,
            regular_session_closes_at: None,
            extended_session_closes_at: Some(closes_at),
            post_close_gap: PostCloseGap::Unknown,
        };

        assert_eq!(status.session(), MarketSession::Extended);
        assert_eq!(status.extended_session_closes_at, Some(closes_at));
        assert_eq!(status.post_close_gap, PostCloseGap::Unknown);
        assert_ne!(
            status,
            MarketSessionStatus::without_close_metadata(MarketSession::Extended),
            "a metadata-capable executor with an unknown gap must remain distinct from an executor that cannot report the gap"
        );
    }

    #[test]
    fn positive_to_whole_shares_succeeds_for_whole_numbers() {
        let shares = Positive::new(FractionalShares::new(float!(5))).unwrap();
        assert_eq!(shares.to_whole_shares().unwrap(), 5);

        let shares = Positive::new(FractionalShares::new(float!(100))).unwrap();
        assert_eq!(shares.to_whole_shares().unwrap(), 100);
    }

    #[test]
    fn estimate_buffered_cost_cents_applies_slippage_and_rounds_up() {
        let estimated_cost_cents = estimate_buffered_cost_cents(
            Positive::new(FractionalShares::new(float!(2))).unwrap(),
            float!(100),
            DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        )
        .unwrap();

        assert_eq!(estimated_cost_cents, 20_200);

        let rounded_up_cost_cents = estimate_buffered_cost_cents(
            Positive::new(FractionalShares::new(float!(1))).unwrap(),
            float!(100.005),
            DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        )
        .unwrap();

        assert_eq!(rounded_up_cost_cents, 10_101);
    }

    fn buy_order(shares: &str) -> MarketOrder {
        MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(
                Float::parse(shares.to_string()).unwrap(),
            ))
            .unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
        }
    }

    fn positive_usd(value: Float) -> Positive<Usd> {
        Positive::new(Usd::new(value)).unwrap()
    }

    #[test]
    fn resolve_buy_preflight_caps_to_cash_at_broker_precision() {
        let result =
            resolve_buy_preflight(&buy_order("2"), positive_usd(float!(100)), 100, 10_100, 9)
                .unwrap();

        let CounterTradePreflight::Allowed {
            reservation:
                Some(CounterTradeReservation::BuyingPower {
                    required,
                    estimated_cost_cents,
                    ..
                }),
        } = result
        else {
            panic!("expected a partial buying-power reservation");
        };

        assert!(required.inner().inner().eq(float!(1)).unwrap());
        assert_eq!(estimated_cost_cents, 10_100);
    }

    #[test]
    fn resolve_buy_preflight_reports_quantity_below_broker_precision() {
        let requested = buy_order("0.0000000009");
        let result =
            resolve_buy_preflight(&requested, positive_usd(float!(100)), 100, 1_000_000, 9)
                .unwrap();

        assert_eq!(
            result,
            CounterTradePreflight::Skipped(CounterTradeSkipReason::BelowBrokerPrecision {
                requested: requested.shares,
                quantity_decimals: 9,
            })
        );
    }

    #[test]
    fn resolve_buy_preflight_skips_zero_cash_and_fractional_dust() {
        let zero =
            resolve_buy_preflight(&buy_order("2"), positive_usd(float!(100)), 100, 0, 9).unwrap();
        assert!(matches!(
            zero,
            CounterTradePreflight::Skipped(CounterTradeSkipReason::InsufficientBuyingPower { .. })
        ));

        let dust =
            resolve_buy_preflight(&buy_order("1"), positive_usd(float!(0.5)), 0, 25, 9).unwrap();
        assert!(matches!(
            dust,
            CounterTradePreflight::Skipped(CounterTradeSkipReason::BelowMinimumNotional)
        ));

        let fully_funded_whole_share =
            resolve_buy_preflight(&buy_order("1"), positive_usd(float!(0.5)), 0, 100, 9).unwrap();
        assert!(matches!(
            fully_funded_whole_share,
            CounterTradePreflight::Allowed { .. }
        ));
    }

    #[test]
    fn resolve_buy_preflight_floors_whole_share_orders() {
        let result =
            resolve_buy_preflight(&buy_order("3.75"), positive_usd(float!(100)), 0, 25_000, 0)
                .unwrap();

        let CounterTradePreflight::Allowed {
            reservation: Some(CounterTradeReservation::BuyingPower { required, .. }),
        } = result
        else {
            panic!("expected a whole-share partial reservation");
        };
        assert!(required.inner().inner().eq(float!(2)).unwrap());
    }

    #[test]
    fn resolve_buy_preflight_falls_back_from_fractional_dust_to_one_whole_share() {
        let result =
            resolve_buy_preflight(&buy_order("5"), positive_usd(float!(0.60)), 0, 95, 9).unwrap();

        let CounterTradePreflight::Allowed {
            reservation:
                Some(CounterTradeReservation::BuyingPower {
                    required,
                    estimated_cost_cents,
                    ..
                }),
        } = result
        else {
            panic!("expected an affordable whole-share reservation");
        };

        assert!(required.inner().inner().eq(float!(1)).unwrap());
        assert_eq!(estimated_cost_cents, 60);
    }

    #[test]
    fn resolve_buy_preflight_falls_back_from_fully_funded_fractional_dust() {
        let result =
            resolve_buy_preflight(&buy_order("1.5"), positive_usd(float!(0.60)), 0, 90, 9).unwrap();

        let CounterTradePreflight::Allowed {
            reservation:
                Some(CounterTradeReservation::BuyingPower {
                    required,
                    estimated_cost_cents,
                    ..
                }),
        } = result
        else {
            panic!("expected a whole-share reservation instead of fractional dust");
        };

        assert!(required.inner().inner().eq(float!(1)).unwrap());
        assert_eq!(estimated_cost_cents, 60);
    }

    #[test]
    fn resolve_buy_preflight_does_not_overdraw_whole_share_fallback() {
        let result =
            resolve_buy_preflight(&buy_order("1.5"), positive_usd(float!(0.60)), 0, 59, 9).unwrap();

        assert_eq!(
            result,
            CounterTradePreflight::Skipped(CounterTradeSkipReason::InsufficientBuyingPower {
                estimated_cost_cents: 60,
                available_buying_power_cents: 59,
            })
        );
    }

    #[test]
    fn status_permanence_classifies_retryable_and_permanent_statuses() {
        assert_eq!(
            status_permanence(reqwest::StatusCode::REQUEST_TIMEOUT),
            Permanence::Transient
        );
        assert_eq!(
            status_permanence(reqwest::StatusCode::TOO_MANY_REQUESTS),
            Permanence::Transient
        );
        assert_eq!(
            status_permanence(reqwest::StatusCode::BAD_GATEWAY),
            Permanence::Transient
        );
        assert_eq!(
            status_permanence(reqwest::StatusCode::FORBIDDEN),
            Permanence::Permanent
        );
    }

    #[test]
    fn positive_to_whole_shares_errors_for_fractional_values() {
        let shares = Positive::new(FractionalShares::new(float!(1.212))).unwrap();
        let err = shares.to_whole_shares().unwrap_err();
        assert!(matches!(err, ToWholeSharesError::Fractional(_)));
    }

    #[test]
    fn fractional_shares_is_whole_returns_true_for_whole_numbers() {
        assert!(FractionalShares::new(float!(1)).is_whole().unwrap());
        assert!(FractionalShares::new(float!(42)).is_whole().unwrap());
    }

    #[test]
    fn fractional_shares_is_whole_returns_false_for_fractional_values() {
        assert!(!FractionalShares::new(float!(1.5)).is_whole().unwrap());
        assert!(!FractionalShares::new(float!(0.001)).is_whole().unwrap());
    }

    #[test]
    fn add_succeeds() {
        let result = (FractionalShares::new(float!(1)) + FractionalShares::new(float!(2))).unwrap();
        assert!(result.inner().eq(float!(3)).unwrap());
    }

    #[test]
    fn sub_succeeds() {
        let result = (FractionalShares::new(float!(5)) - FractionalShares::new(float!(2))).unwrap();
        assert!(result.inner().eq(float!(3)).unwrap());
    }

    #[test]
    fn abs_returns_absolute_value() {
        let result = FractionalShares::new(float!(-1)).abs().unwrap();
        assert!(result.inner().eq(float!(1)).unwrap());
    }

    #[test]
    fn into_float_extracts_inner_value() {
        let float: Float = FractionalShares::new(float!(42)).into();
        assert!(float.eq(float!(42)).unwrap());
    }

    #[test]
    fn mul_float_succeeds() {
        let result = (FractionalShares::new(float!(100)) * float!(0.5)).unwrap();
        assert!(result.inner().eq(float!(50)).unwrap());
    }

    #[test]
    fn test_symbol_new_valid() {
        let symbol = Symbol::new("AAPL").unwrap();
        assert_eq!(symbol.to_string(), "AAPL");
    }

    #[test]
    fn test_symbol_new_empty_fails() {
        let result = Symbol::new("");
        assert!(matches!(result.unwrap_err(), EmptySymbolError));
    }

    #[test]
    fn test_symbol_new_boundary_valid() {
        let symbol = Symbol::new("A").unwrap();
        assert_eq!(symbol.to_string(), "A");

        let symbol = Symbol::new("ABCDEFGHIJ").unwrap();
        assert_eq!(symbol.to_string(), "ABCDEFGHIJ");
    }

    #[test]
    fn test_shares_new_valid() {
        let shares = Shares::new(100).unwrap();
        assert_eq!(shares.to_string(), "100");
    }

    #[test]
    fn test_shares_new_zero_fails() {
        let result = Shares::new(0);
        assert!(matches!(result.unwrap_err(), InvalidSharesError::Zero));
    }

    #[test]
    fn test_shares_new_max_boundary() {
        let shares = Shares::new(u64::from(u32::MAX)).unwrap();
        assert_eq!(shares.to_string(), u32::MAX.to_string());

        let result = Shares::new(u64::from(u32::MAX) + 1);
        assert!(matches!(
            result.unwrap_err(),
            InvalidSharesError::TryFromInt(_)
        ));
    }

    #[test]
    fn test_shares_new_one() {
        let shares = Shares::new(1).unwrap();
        assert_eq!(shares.to_string(), "1");
    }

    #[test]
    fn from_str_rejects_removed_schwab_executor_name() {
        let error = "schwab".parse::<SupportedExecutor>().unwrap_err();
        assert_eq!(error.executor_provided, "schwab");
    }

    #[test]
    fn from_str_rejects_removed_alpaca_trading_api_executor_name() {
        let error = "alpaca-trading-api"
            .parse::<SupportedExecutor>()
            .unwrap_err();
        assert_eq!(error.executor_provided, "alpaca-trading-api");
    }

    #[test]
    fn from_str_accepts_supported_runtime_executor_names() {
        assert_eq!(
            "alpaca-broker-api".parse::<SupportedExecutor>().unwrap(),
            SupportedExecutor::AlpacaBrokerApi
        );
        assert_eq!(
            "dry-run".parse::<SupportedExecutor>().unwrap(),
            SupportedExecutor::DryRun
        );
    }

    #[test]
    fn truncate_whole_number_unchanged() {
        let value = float!(100);
        let result = truncate_to_decimal_places(value, 9).unwrap().unwrap();
        assert!(
            result.eq(value).unwrap(),
            "expected {}, got {}",
            value.format().unwrap(),
            result.format().unwrap(),
        );
    }

    #[test]
    fn truncate_fewer_decimals_unchanged() {
        let value = float!(1.5);
        let result = truncate_to_decimal_places(value, 9).unwrap().unwrap();
        assert!(
            result.eq(value).unwrap(),
            "expected {}, got {}",
            value.format().unwrap(),
            result.format().unwrap(),
        );

        let value = float!(0.123456789);
        let result = truncate_to_decimal_places(value, 9).unwrap().unwrap();
        assert!(
            result.eq(value).unwrap(),
            "expected {}, got {}",
            value.format().unwrap(),
            result.format().unwrap(),
        );
    }

    #[test]
    fn truncate_excess_decimals_floors() {
        let value = Float::parse("0.996350331351928059".to_string()).unwrap();
        let expected = float!(0.996350331);
        let result = truncate_to_decimal_places(value, 9).unwrap().unwrap();
        assert!(
            result.eq(expected).unwrap(),
            "expected {}, got {}",
            expected.format().unwrap(),
            result.format().unwrap(),
        );
    }

    #[test]
    fn truncate_preserves_whole_part() {
        let value = Float::parse("1.500000000000000001".to_string()).unwrap();
        let expected = float!(1.5);
        let result = truncate_to_decimal_places(value, 9).unwrap().unwrap();
        assert!(
            result.eq(expected).unwrap(),
            "expected {}, got {}",
            expected.format().unwrap(),
            result.format().unwrap(),
        );
    }

    #[test]
    fn truncate_integer_value_with_excess_decimals() {
        let value = Float::parse("2.000000000000000001".to_string()).unwrap();
        let expected = float!(2);
        let result = truncate_to_decimal_places(value, 9).unwrap().unwrap();
        assert!(
            result.eq(expected).unwrap(),
            "expected {}, got {}",
            expected.format().unwrap(),
            result.format().unwrap(),
        );
    }

    #[test]
    fn truncate_zero_decimal_places() {
        let value = float!(1.234);
        let expected = float!(1);
        let result = truncate_to_decimal_places(value, 0).unwrap().unwrap();
        assert!(
            result.eq(expected).unwrap(),
            "expected {}, got {}",
            expected.format().unwrap(),
            result.format().unwrap(),
        );
    }

    #[test]
    fn truncate_sub_precision_value_returns_none() {
        let value = float!(0.0000000009);
        assert!(
            truncate_to_decimal_places(value, 9).unwrap().is_none(),
            "sub-precision value {} should return None",
            value.format().unwrap(),
        );
    }

    #[test]
    fn truncate_exact_zero_returns_some() {
        let result = truncate_to_decimal_places(float!(0), 9).unwrap().unwrap();
        assert!(
            result.is_zero().unwrap(),
            "expected zero, got {}",
            result.format().unwrap(),
        );
    }

    fn sell_order(symbol: &str, shares: &str) -> MarketOrder {
        MarketOrder {
            symbol: Symbol::new(symbol).unwrap(),
            shares: Positive::new(FractionalShares::new(
                Float::parse(shares.to_string()).unwrap(),
            ))
            .unwrap(),
            direction: Direction::Sell,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
        }
    }

    fn frac_shares(value: &str) -> FractionalShares {
        FractionalShares::new(Float::parse(value.to_string()).unwrap())
    }

    #[test]
    fn resolve_sell_preflight_returns_full_shares_when_sufficient() {
        let order = sell_order("AAPL", "10");
        let available = frac_shares("15");

        let result = resolve_sell_preflight(order, available, FractionalShares::ZERO).unwrap();

        match result {
            CounterTradePreflight::Allowed {
                reservation: Some(CounterTradeReservation::Equity { required, .. }),
            } => {
                assert!(
                    required.inner().inner().eq(float!(10)).unwrap(),
                    "Should use full requested shares, got {required:?}"
                );
            }
            other => panic!("Expected Allowed with full shares, got {other:?}"),
        }
    }

    #[test]
    fn resolve_sell_preflight_caps_to_available_when_partial() {
        let order = sell_order("AAPL", "20");
        let available = frac_shares("10");

        let result = resolve_sell_preflight(order, available, FractionalShares::ZERO).unwrap();

        match result {
            CounterTradePreflight::Allowed {
                reservation: Some(CounterTradeReservation::Equity { required, .. }),
            } => {
                assert!(
                    required.inner().inner().eq(float!(10)).unwrap(),
                    "Should cap to available shares, got {required:?}"
                );
            }
            other => panic!("Expected Allowed with capped shares, got {other:?}"),
        }
    }

    #[test]
    fn resolve_sell_preflight_skips_when_zero_inventory() {
        let order = sell_order("AAPL", "5");
        let available = FractionalShares::ZERO;

        let result = resolve_sell_preflight(order, available, FractionalShares::ZERO).unwrap();

        assert!(
            matches!(
                result,
                CounterTradePreflight::Skipped(CounterTradeSkipReason::InsufficientEquity { .. })
            ),
            "Should skip when available is zero, got {result:?}"
        );
    }

    #[test]
    fn resolve_sell_preflight_skips_dust_below_minimum_threshold() {
        let order = sell_order("AAPL", "5");
        let available = frac_shares("0.001");

        let result = resolve_sell_preflight(order, available, FractionalShares::ZERO).unwrap();

        assert!(
            matches!(
                result,
                CounterTradePreflight::Skipped(CounterTradeSkipReason::InsufficientEquity { .. })
            ),
            "Should skip when available is below minimum threshold (0.01), got {result:?}"
        );
    }

    #[test]
    fn resolve_sell_preflight_allows_partial_at_minimum_threshold() {
        let order = sell_order("AAPL", "5");
        let available = frac_shares("0.01");

        let result = resolve_sell_preflight(order, available, FractionalShares::ZERO).unwrap();

        match result {
            CounterTradePreflight::Allowed {
                reservation: Some(CounterTradeReservation::Equity { required, .. }),
            } => {
                assert!(
                    required.inner().inner().eq(float!(0.01)).unwrap(),
                    "Should allow partial at exactly the minimum threshold, got {required:?}"
                );
            }
            other => panic!("Expected Allowed at minimum threshold, got {other:?}"),
        }
    }

    /// The 2026-09-14 COIN drain: 5.27 available against a 21.48 request
    /// sold every share and left pricing with no position to mark from.
    #[test]
    fn resolve_sell_preflight_keeps_the_floor_out_of_a_partial_hedge() {
        let order = sell_order("COIN", "21.48");

        let result = resolve_sell_preflight(order, frac_shares("5.27"), frac_shares("1")).unwrap();

        let CounterTradePreflight::Allowed {
            reservation:
                Some(CounterTradeReservation::Equity {
                    required,
                    available,
                    ..
                }),
        } = result
        else {
            panic!("Expected Allowed with a floored cap, got {result:?}");
        };
        assert_eq!(required.inner(), frac_shares("4.27"));
        assert_eq!(available, frac_shares("4.27"));
    }

    #[test]
    fn resolve_sell_preflight_holds_at_floor_instead_of_reporting_no_inventory() {
        let order = sell_order("COIN", "5");

        let result = resolve_sell_preflight(order, frac_shares("0.8"), frac_shares("1")).unwrap();

        let CounterTradePreflight::Skipped(CounterTradeSkipReason::HeldAtFloor {
            symbol,
            floor,
            available,
        }) = result
        else {
            panic!("Expected HeldAtFloor, got {result:?}");
        };
        assert_eq!(symbol, Symbol::new("COIN").unwrap());
        assert_eq!(floor, frac_shares("1"));
        assert_eq!(available, frac_shares("0.8"));
    }

    #[test]
    fn resolve_sell_preflight_with_zero_floor_still_sells_the_whole_book() {
        let order = sell_order("COIN", "21.48");

        let result =
            resolve_sell_preflight(order, frac_shares("5.27"), FractionalShares::ZERO).unwrap();

        let CounterTradePreflight::Allowed {
            reservation:
                Some(CounterTradeReservation::Equity {
                    required,
                    available,
                    ..
                }),
        } = result
        else {
            panic!("Expected Allowed capped to the book, got {result:?}");
        };
        assert_eq!(required.inner(), frac_shares("5.27"));
        assert_eq!(available, frac_shares("5.27"));
    }
}
