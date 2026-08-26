use chrono::{DateTime, Utc};
use rain_math_float::Float;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use st0x_float_macro::float;
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, trace, warn};
use uuid::Uuid;

use super::client::AlpacaBrokerApiClient;
use super::{
    AlpacaBrokerApiError, CryptoOrderFailureReason, DeadlineCancel, MissingOrderField, TimeInForce,
};
use crate::{
    CancellationOutcome, ClientOrderId, Direction, ExecutorOrderId, FractionalShares, MarketOrder,
    OrderFailureTerminality, OrderPlacement, OrderStatus, OrderUpdate, Positive, Symbol, Usd, Usdc,
    deserialize_float_from_number_or_string, deserialize_option_float_from_number_or_string,
    serialize_float_as_string,
};

const ALPACA_CRYPTO_MAX_DECIMAL_PLACES: u8 = 6;

/// Decimal places Alpaca accepts in a `notional`, which is a USD amount rather
/// than a token quantity: past two it answers `422 / 42210000`, "notional value
/// must be limited to 2 decimal places".
const ALPACA_NOTIONAL_MAX_DECIMAL_PLACES: u8 = 2;

/// Order side
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum OrderSide {
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

/// Direction for USDC/USD conversion
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversionDirection {
    /// Convert USDC to USD buying power (sell USDC/USD)
    UsdcToUsd,
    /// Convert USD buying power to USDC (buy USDC/USD)
    UsdToUsdc,
}

/// A USDC/USD conversion to place, carrying its amount in the unit that
/// direction is denominated in.
///
/// The two directions are not interchangeable quantities -- the sell names the
/// USDC it holds, the buy names the dollars it spends -- so the amount travels
/// with the direction that gives it meaning rather than as a bare number
/// alongside it. A caller cannot hand a USDC balance to the buy leg without
/// saying, in the type, that it means dollars. Both amounts are [`Positive`],
/// so a conversion sized at zero or below cannot be constructed and nothing
/// non-positive ever reaches the broker.
#[derive(Debug, Clone, Copy)]
pub enum ConversionOrder {
    /// Sell this much USDC for USD buying power.
    SellUsdc(Positive<Usdc>),
    /// Spend this many dollars buying USDC.
    BuyWithUsd(Positive<Usd>),
}

impl ConversionOrder {
    /// The direction this order converts in, for the callers that record the
    /// settled amounts against it.
    pub const fn direction(&self) -> ConversionDirection {
        match self {
            Self::SellUsdc(_) => ConversionDirection::UsdcToUsd,
            Self::BuyWithUsd(_) => ConversionDirection::UsdToUsdc,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AlpacaLimitOrder {
    pub symbol: Symbol,
    pub shares: Positive<FractionalShares>,
    pub direction: Direction,
    pub limit_price: AlpacaLimitPrice,
    pub extended_hours: bool,
    pub client_order_id: ClientOrderId,
}

#[derive(Debug, Clone, Serialize)]
#[serde(transparent)]
pub struct AlpacaLimitPrice(Positive<Usd>);

#[derive(Debug, thiserror::Error)]
pub enum ParseAlpacaLimitPriceError {
    #[error(transparent)]
    Float(#[from] rain_math_float::FloatError),

    #[error("limit price must be positive")]
    NotPositive,

    #[error(transparent)]
    Validation(#[from] AlpacaBrokerApiError),
}

impl AlpacaLimitPrice {
    pub fn try_new(limit_price: Positive<Usd>) -> Result<Self, AlpacaBrokerApiError> {
        validate_limit_price_precision(limit_price)?;
        Ok(Self(limit_price))
    }

    pub fn as_price(&self) -> &Positive<Usd> {
        &self.0
    }

    pub fn into_inner(self) -> Positive<Usd> {
        self.0
    }
}

impl FromStr for AlpacaLimitPrice {
    type Err = ParseAlpacaLimitPriceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let price = value.parse::<Usd>()?;
        let positive_price = Positive::new(price).map_err(|_| Self::Err::NotPositive)?;
        Self::try_new(positive_price).map_err(Self::Err::from)
    }
}

/// Order request for placing market orders.
///
/// The `quantity` field must already be truncated to Alpaca's decimal precision
/// before constructing this struct.
#[derive(Debug, Serialize)]
pub(super) struct OrderRequest {
    #[serde(serialize_with = "serialize_symbol")]
    pub symbol: Symbol,
    #[serde(rename = "qty", serialize_with = "serialize_shares_as_string")]
    pub quantity: Positive<FractionalShares>,
    pub side: OrderSide,
    #[serde(rename = "type")]
    pub order_type: &'static str,
    pub time_in_force: &'static str,
    pub extended_hours: bool,
    pub client_order_id: ClientOrderId,
}

/// Order request for placing limit orders.
///
/// The `quantity` field must already be truncated to Alpaca's decimal precision
/// before constructing this struct.
#[derive(Debug, Serialize)]
pub(super) struct LimitOrderRequest {
    #[serde(serialize_with = "serialize_symbol")]
    pub symbol: Symbol,
    #[serde(rename = "qty", serialize_with = "serialize_shares_as_string")]
    pub quantity: Positive<FractionalShares>,
    pub side: OrderSide,
    #[serde(rename = "type")]
    pub order_type: &'static str,
    pub limit_price: AlpacaLimitPrice,
    pub time_in_force: &'static str,
    pub extended_hours: bool,
    pub client_order_id: ClientOrderId,
}

fn serialize_symbol<S>(symbol: &Symbol, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&symbol.to_string())
}

// serde's serialize_with requires the field to be passed by reference
#[allow(clippy::trivially_copy_pass_by_ref)]
fn serialize_shares_as_string<S>(
    shares: &Positive<FractionalShares>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let formatted = shares
        .inner()
        .inner()
        .format_with_scientific(false)
        .map_err(serde::ser::Error::custom)?;
    serializer.serialize_str(&formatted)
}

/// Order response from the Alpaca Broker API
#[derive(Debug, Deserialize)]
pub(super) struct OrderResponse {
    pub id: Uuid,
    pub symbol: Symbol,
    #[serde(
        rename = "qty",
        deserialize_with = "deserialize_positive_shares_from_string"
    )]
    pub quantity: Positive<FractionalShares>,
    #[serde(
        rename = "filled_qty",
        default,
        deserialize_with = "deserialize_option_float_from_number_or_string"
    )]
    pub filled_quantity: Option<Float>,
    pub side: OrderSide,
    pub status: BrokerOrderStatus,
    #[serde(
        rename = "filled_avg_price",
        default,
        deserialize_with = "deserialize_option_float_from_number_or_string"
    )]
    pub filled_average_price: Option<Float>,
    /// Whether the broker holds this as an extended-hours order. Part of the
    /// documented order entity
    /// (https://docs.alpaca.markets/reference/getorderforaccount). Needed so
    /// the duplicate-`client_order_id` adoption path reports the ADOPTED
    /// order's session terms, which may differ from the current request's.
    /// `Option` so an omitted echo is distinguishable from a real `false`:
    /// adoption paths fall back to the request's terms when the broker omits
    /// the field.
    #[serde(default)]
    pub extended_hours: Option<bool>,
    /// The broker-held limit price, present for limit orders
    /// (https://docs.alpaca.markets/reference/getorderforaccount). Same
    /// adoption rationale as `extended_hours`.
    #[serde(
        default,
        deserialize_with = "deserialize_option_float_from_number_or_string"
    )]
    pub limit_price: Option<Float>,
    /// Broker-side timestamps from the documented order entity
    /// (https://docs.alpaca.markets/reference/getorderforaccount). Filled and
    /// canceled states use these event times downstream in
    /// `Position.last_updated` and recency logic -- never substitute the local
    /// observation time for those terminal timestamps. Rejected orders may fall
    /// back to observation time when Alpaca omits `failed_at`.
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub filled_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub canceled_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub failed_at: Option<DateTime<Utc>>,
}

/// How a crypto order is sized. Alpaca accepts exactly one of `qty` and
/// `notional`, and which one is sent decides where its ~2% collar lands: a
/// `qty` buy is held at `quantity x price x 1.02` and refused at 100% of
/// settled cash, a `notional` buy at the dollars it names.
#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) enum CryptoOrderSize {
    /// Base-asset units to trade (e.g. USDC).
    #[serde(rename = "qty")]
    Quantity(#[serde(serialize_with = "serialize_float_as_string")] Float),
    /// USD to spend, letting the broker derive the quantity.
    #[serde(rename = "notional")]
    Notional(#[serde(serialize_with = "serialize_float_as_string")] Float),
}

/// Order request for crypto trading (e.g., USDC/USD conversion).
/// Uses decimal quantity and trading pair symbol format.
#[derive(Debug, Serialize)]
pub(crate) struct CryptoOrderRequest {
    /// Trading pair symbol (e.g., "USDCUSD" for USDC/USD)
    pub symbol: String,
    #[serde(flatten)]
    pub size: CryptoOrderSize,
    pub side: OrderSide,
    #[serde(rename = "type")]
    pub order_type: &'static str,
    pub time_in_force: &'static str,
    /// Caller-supplied idempotency/correlation key. Recorded before placement
    /// so a crashed conversion can be looked up by this key on resume.
    pub client_order_id: ClientOrderId,
}

/// Response from a crypto order placement
#[derive(Debug, Clone, Deserialize)]
pub struct CryptoOrderResponse {
    pub id: Uuid,
    pub symbol: String,
    /// Base-asset units requested, absent on an order placed by `notional`:
    /// Alpaca derives the quantity from the fill and answers `"qty": null`
    /// until then. Read `filled_quantity` for what was actually bought.
    #[serde(
        rename = "qty",
        default,
        deserialize_with = "deserialize_option_float_from_number_or_string"
    )]
    pub quantity: Option<Float>,
    /// USD requested, present only on an order placed by `notional`. Carried
    /// so the partial-fill warning can still report what was asked for on the
    /// direction that has no `qty`.
    #[serde(
        default,
        deserialize_with = "deserialize_option_float_from_number_or_string"
    )]
    pub notional: Option<Float>,
    status: BrokerOrderStatus,
    #[serde(
        rename = "filled_avg_price",
        default,
        deserialize_with = "deserialize_option_float_from_number_or_string"
    )]
    pub filled_average_price: Option<Float>,
    #[serde(
        rename = "filled_qty",
        default,
        deserialize_with = "deserialize_option_float_from_number_or_string"
    )]
    pub filled_quantity: Option<Float>,
    pub created_at: DateTime<Utc>,
}

/// Terminal/intermediate decision for a crypto order, exposing the outcome
/// without leaking the private `BrokerOrderStatus`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CryptoOrderOutcome {
    Filled,
    Pending,
    Failed(CryptoOrderFailureReason),
}

impl CryptoOrderResponse {
    /// Returns the status as a display-friendly string.
    ///
    /// Exhaustive rather than wildcarded to "other": the conversion poll logs
    /// this when it decides to keep waiting on a stalled order, and the
    /// statuses that drive that decision are exactly the ones a wildcard
    /// would hide.
    pub fn status_display(&self) -> &'static str {
        use BrokerOrderStatus::*;

        match self.status {
            Filled => "filled",
            New => "new",
            PendingNew => "pending_new",
            PartiallyFilled => "partially_filled",
            Canceled => "canceled",
            Expired => "expired",
            Rejected => "rejected",
            Accepted => "accepted",
            AcceptedForBidding => "accepted_for_bidding",
            PendingCancel => "pending_cancel",
            PendingReplace => "pending_replace",
            Stopped => "stopped",
            DoneForDay => "done_for_day",
            Replaced => "replaced",
            Suspended => "suspended",
            Calculated => "calculated",
        }
    }

    /// Classifies the order's current status into a fill/pending/failed outcome,
    /// consistent with the terminal mapping in `classify_broker_status`.
    ///
    /// The match is exhaustive (no wildcard) so a newly added Alpaca status forces
    /// a compile error here rather than silently mapping to `Pending` and retrying
    /// forever.
    pub fn classify(&self) -> CryptoOrderOutcome {
        use BrokerOrderStatus::*;

        let reason = match self.status {
            Filled => return CryptoOrderOutcome::Filled,
            New | PendingNew | PartiallyFilled | Accepted | AcceptedForBidding | PendingCancel
            | PendingReplace | Stopped => return CryptoOrderOutcome::Pending,
            Canceled => CryptoOrderFailureReason::Canceled,
            Expired => CryptoOrderFailureReason::Expired,
            Rejected => CryptoOrderFailureReason::Rejected,
            DoneForDay => CryptoOrderFailureReason::DoneForDay,
            Replaced => CryptoOrderFailureReason::Replaced,
            Suspended => CryptoOrderFailureReason::Suspended,
            Calculated => CryptoOrderFailureReason::Calculated,
        };

        CryptoOrderOutcome::Failed(reason)
    }
}

/// An outcome the order can no longer leave.
///
/// Distinct from [`CryptoOrderOutcome`], which also covers the states an order
/// can still move out of: pending, and the `Failed` statuses Alpaca may resume
/// or fill from. Returning this from `cancel_and_settle` is what lets its
/// caller match two real arms instead of a third impossible one.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TerminalCryptoOutcome {
    Filled,
    Failed(CryptoOrderFailureReason),
}

impl CryptoOrderOutcome {
    /// Whether this classification is an answer, or the order can still
    /// change state.
    ///
    /// Exposed so callers outside this crate decide "keep waiting?" from the
    /// same rule the polls use, rather than re-deriving it from the variant.
    /// Matching on [`Self::Pending`] alone is not equivalent: a `Failed`
    /// status the broker may still resume from is also not an answer.
    pub fn is_terminal(self) -> bool {
        self.terminal().is_some()
    }

    /// The terminal outcome this classification represents, or `None` while
    /// the order can still change state.
    ///
    /// A `Failed` status whose [`CryptoOrderFailureReason::terminality`] is
    /// `NotTerminal` is deliberately `None`: the order may still resume and
    /// fill, so treating it as an answer would terminalize a rebalance while
    /// the broker can still move real money.
    ///
    /// `DoneForDay` is the one status this reads differently from
    /// `terminality`, whose `Terminal` answer holds for the Day
    /// time-in-force equity orders it also serves. Alpaca defines the status
    /// as "no further updates until the next trading day", and does not
    /// document what it means for a `gtc` order on a 24/7 crypto pair, so the
    /// order is not assumed dead: waiting it out costs one deadline and ends
    /// in a cancel that settles the order, while calling it terminal records
    /// a failed rebalance against an order that may still fill.
    fn terminal(self) -> Option<TerminalCryptoOutcome> {
        match self {
            Self::Filled => Some(TerminalCryptoOutcome::Filled),
            // Not an answer yet, for two different reasons: the order is still
            // working, or it is `DoneForDay`, which this deliberately reads as
            // non-final for a crypto `gtc` order (see above).
            Self::Pending | Self::Failed(CryptoOrderFailureReason::DoneForDay) => None,
            Self::Failed(reason) => match reason.terminality() {
                OrderFailureTerminality::Terminal => Some(TerminalCryptoOutcome::Failed(reason)),
                OrderFailureTerminality::NotTerminal => None,
            },
        }
    }
}

fn deserialize_positive_shares_from_string<'de, D>(
    deserializer: D,
) -> Result<Positive<FractionalShares>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let exact = deserialize_float_from_number_or_string(deserializer)?;
    Positive::new(FractionalShares::new(exact)).map_err(serde::de::Error::custom)
}

fn validate_limit_price_precision(limit_price: Positive<Usd>) -> Result<(), AlpacaBrokerApiError> {
    let max_decimals = if limit_price.inner().inner().lt(float!(1))? {
        4
    } else {
        2
    };

    let (_, lossless) = limit_price
        .inner()
        .inner()
        .to_fixed_decimal_lossy(max_decimals)?;

    if !lossless {
        return Err(AlpacaBrokerApiError::InvalidLimitPricePrecision {
            limit_price,
            max_decimals,
        });
    }

    Ok(())
}

pub(super) async fn place_market_order(
    client: &AlpacaBrokerApiClient,
    market_order: MarketOrder,
    time_in_force: TimeInForce,
) -> Result<OrderPlacement<String>, AlpacaBrokerApiError> {
    debug!(
        "Placing Alpaca Broker API market order: {} {} shares of {} (time_in_force: {:?})",
        market_order.direction, market_order.shares, market_order.symbol, time_in_force
    );

    let placed_shares = truncate_shares_to_alpaca_precision(market_order.shares)?;

    let side = match market_order.direction {
        Direction::Buy => OrderSide::Buy,
        Direction::Sell => OrderSide::Sell,
    };

    let request = OrderRequest {
        symbol: market_order.symbol.clone(),
        quantity: placed_shares,
        side,
        order_type: "market",
        time_in_force: time_in_force.as_api_str(),
        // Alpaca only allows extended_hours=true for limit orders, not market orders
        extended_hours: false,
        client_order_id: market_order.client_order_id.clone(),
    };

    // Alpaca rejects a re-used `client_order_id` on an active order with a 422
    // ("client_order_id must be unique"), not a duplicate-tolerant 2xx. That is
    // not a real failure: it means a prior attempt's 2xx response was lost after
    // the broker already recorded the order. Reconcile by adopting the order the
    // broker actually accepted (looked up by `client_order_id`), so the retry is
    // idempotent instead of failing and leaving the position un-hedged. The
    // adopted order's quantity is the broker's recorded intent, which may differ
    // from this attempt's recomputed `placed_shares`; any residual is picked up
    // by the next position scan.
    // A fresh placement's terms are the REQUEST's terms (the broker created
    // exactly what was asked; its response may even omit the echo fields). A
    // duplicate-key adoption instead reports the ADOPTED order's terms from
    // the lookup response: the broker may hold a prior attempt's
    // extended-hours limit order (e.g. a regular-hours market retry after a
    // lost extended-hours placement response), and the caller's convergence
    // sweep keys off the recorded `extended_hours` flag.
    let (order_id, shares, extended_hours, limit_price) = match client.place_order(&request).await {
        Ok(response) => (response.id, placed_shares, false, None),
        Err(error) if is_duplicate_client_order_id(&error) => {
            warn!(
                %error,
                client_order_id = %market_order.client_order_id,
                "Broker rejected duplicate client_order_id; reconciling the order it already accepted"
            );
            let existing = client
                .get_order_by_client_order_id(&market_order.client_order_id)
                .await?
                .ok_or_else(|| AlpacaBrokerApiError::DuplicateOrderNotFound {
                    client_order_id: market_order.client_order_id.clone(),
                })?;
            (
                existing.id,
                existing.quantity,
                // No request terms to fall back to for a market request: an
                // omitted echo most plausibly means a plain market order.
                existing.extended_hours.unwrap_or(false),
                parse_limit_price(existing.limit_price)?,
            )
        }
        Err(error) => return Err(error),
    };

    Ok(OrderPlacement {
        order_id: order_id.to_string(),
        symbol: market_order.symbol,
        shares,
        direction: market_order.direction,
        placed_at: Utc::now(),
        extended_hours,
        limit_price,
    })
}

/// Converts the broker-reported limit price into the domain type, failing
/// fast on a non-positive value rather than silently dropping it.
fn parse_limit_price(
    limit_price: Option<Float>,
) -> Result<Option<Positive<Usd>>, AlpacaBrokerApiError> {
    limit_price
        .map(|price| Positive::new(Usd::new(price)))
        .transpose()
        .map_err(Into::into)
}

/// Alpaca returns a 422 with "client_order_id must be unique" when a placement
/// re-uses a `client_order_id` already attached to an active order. This is the
/// recoverable duplicate-submission case (the original 2xx was lost in flight),
/// distinct from other 422s such as insufficient buying power or invalid order.
fn is_duplicate_client_order_id(error: &AlpacaBrokerApiError) -> bool {
    use AlpacaBrokerApiError::*;

    match error {
        ApiError {
            status, message, ..
        } => {
            *status == reqwest::StatusCode::UNPROCESSABLE_ENTITY
                && message.contains("client_order_id must be unique")
        }
        HttpClient(_)
        | KmsJwt(_)
        | JsonParse(_)
        | PositionSymbolMismatch { .. }
        | InvalidHeader(_)
        | InvalidOrderId(_)
        | IncompleteOrder { .. }
        | FilledQuantityMismatch { .. }
        | AccountNotActive { .. }
        | CryptoOrderFailed { .. }
        | ConversionTimedOut { .. }
        | ConversionCancelNotSettled { .. }
        | ConversionOrderNotFound { .. }
        | DuplicateOrderNotFound { .. }
        | CalendarIterationInvariantViolation
        | CalendarDateMismatch { .. }
        | CalendarLocalTimeUnresolvable { .. }
        | InvalidAccountActivitiesUrl { .. }
        | AccountActivitiesPaginationInvariantViolation
        | AccountActivitiesPageLimitExceeded { .. }
        | AssetNotActive { .. }
        | AssetNotTradable { .. }
        | InvalidLimitPricePrecision { .. }
        | UsdBalanceConversion(_)
        | FractionalCents(_)
        | InvalidSymbol(_)
        | MissingPositionQuantity
        | BelowPrecision { .. }
        | UsdcBelowPrecision { .. }
        | UsdcPrecisionExceeded { .. }
        | NotPositive(_)
        | NotPositiveLimitPrice(_)
        | FloatConversion(_)
        | LatestTrade(_)
        | LatestQuote(_)
        | CounterTradeCost(_) => false,
    }
}

pub(super) async fn place_limit_order(
    client: &AlpacaBrokerApiClient,
    limit_order: AlpacaLimitOrder,
) -> Result<OrderPlacement<String>, AlpacaBrokerApiError> {
    debug!(
        direction = ?limit_order.direction,
        shares = %limit_order.shares,
        symbol = %limit_order.symbol,
        limit_price = ?limit_order.limit_price,
        extended_hours = limit_order.extended_hours,
        "Placing Alpaca Broker API limit order"
    );

    let placed_shares = truncate_shares_to_alpaca_precision(limit_order.shares)?;

    let side = match limit_order.direction {
        Direction::Buy => OrderSide::Buy,
        Direction::Sell => OrderSide::Sell,
    };

    let request = LimitOrderRequest {
        symbol: limit_order.symbol.clone(),
        quantity: placed_shares,
        side,
        order_type: "limit",
        limit_price: limit_order.limit_price.clone(),
        time_in_force: TimeInForce::Day.as_api_str(),
        extended_hours: limit_order.extended_hours,
        client_order_id: limit_order.client_order_id.clone(),
    };

    // Same lost-response reconciliation as place_market_order: a re-used
    // client_order_id rejected with a 422 means the broker already accepted a
    // prior attempt whose response was lost. Adopt the order it accepted so the
    // apalis retry is idempotent instead of double-submitting a live limit
    // order during thin extended-hours liquidity.
    // Fresh placements report the REQUEST's terms; adoptions report the
    // ADOPTED order's terms -- see the matching comment in
    // `place_market_order`.
    let (order_id, shares, extended_hours, limit_price) = match client
        .place_limit_order(&request)
        .await
    {
        Ok(response) => (
            response.id,
            placed_shares,
            limit_order.extended_hours,
            Some(*limit_order.limit_price.as_price()),
        ),
        Err(error) if is_duplicate_client_order_id(&error) => {
            warn!(
                %error,
                client_order_id = %limit_order.client_order_id,
                "Broker rejected duplicate client_order_id; reconciling the order it already accepted"
            );
            let existing = client
                .get_order_by_client_order_id(&limit_order.client_order_id)
                .await?
                .ok_or_else(|| AlpacaBrokerApiError::DuplicateOrderNotFound {
                    client_order_id: limit_order.client_order_id.clone(),
                })?;
            (
                existing.id,
                existing.quantity,
                // An omitted echo falls back to the REQUEST's terms: the
                // adopted order under this client_order_id was created by a
                // prior attempt of this same extended-hours placement, and
                // recording it as regular would hide it from the regular-open
                // cancel-and-replace sweep.
                existing
                    .extended_hours
                    .unwrap_or(limit_order.extended_hours),
                parse_limit_price(existing.limit_price)?
                    .or(Some(*limit_order.limit_price.as_price())),
            )
        }
        Err(error) => return Err(error),
    };

    Ok(OrderPlacement {
        order_id: order_id.to_string(),
        symbol: limit_order.symbol,
        shares,
        direction: limit_order.direction,
        placed_at: Utc::now(),
        extended_hours,
        limit_price,
    })
}

pub(super) async fn get_order_status(
    client: &AlpacaBrokerApiClient,
    order_id: &str,
) -> Result<OrderUpdate<String>, AlpacaBrokerApiError> {
    debug!(
        "Querying Alpaca Broker API order status for order ID: {}",
        order_id
    );

    let order_uuid = Uuid::parse_str(order_id)?;
    let response = client.get_order(order_uuid).await?;

    let direction = match response.side {
        OrderSide::Buy => Direction::Buy,
        OrderSide::Sell => Direction::Sell,
    };

    let (status, failure_terminality) = classify_broker_status(response.status);
    let price = response.filled_average_price;
    let shares_filled = response.filled_quantity.map(FractionalShares::new);

    if response.status == BrokerOrderStatus::PartiallyFilled {
        debug!(
            order_id,
            symbol = %response.symbol,
            ordered_qty = %response.quantity.inner(),
            filled_qty = ?shares_filled,
            "Order is partially filled"
        );
    }

    // The broker's event time for the mapped status, not the local
    // observation time: queue delays, polling intervals, and retries would
    // otherwise rewrite broker event time downstream
    // (`Position.last_updated`, fill/cancel timestamps). Filled and
    // Cancelled must carry their specific broker timestamps; Failed covers
    // several broker statuses (expired, replaced, ...) that only reliably
    // carry `updated_at`, so it falls back through `failed_at`.
    // The order entity marks every timestamp nullable
    // (https://docs.alpaca.markets/reference/getorderforaccount), so terminal
    // states prefer their specific timestamp but fall back through
    // `updated_at` (warned, not silent) rather than blocking fill/cancel
    // recording on a missing echo. Only a response with no usable timestamp
    // at all fails.
    let updated_at = match status {
        OrderStatus::Filled => terminal_broker_time(
            response.filled_at,
            response.updated_at,
            order_id,
            MissingOrderField::FilledAt,
        )?,
        OrderStatus::Cancelled => terminal_broker_time(
            response.canceled_at,
            response.updated_at,
            order_id,
            MissingOrderField::CanceledAt,
        )?,
        OrderStatus::Failed => {
            broker_time_or_observation(response.failed_at.or(response.updated_at), order_id)
        }
        OrderStatus::Pending | OrderStatus::Submitted | OrderStatus::PartiallyFilled => {
            broker_time_or_observation(response.updated_at, order_id)
        }
    };

    Ok(OrderUpdate {
        order_id: order_id.to_string(),
        symbol: response.symbol,
        shares: response.quantity,
        direction,
        status,
        updated_at,
        price,
        shares_filled,
        failure_terminality,
    })
}

/// Picks the broker event time for a terminal state: the status-specific
/// timestamp when present, else `updated_at` with a warning (the doc marks
/// both nullable). A terminal response carrying NEITHER is unusable --
/// `Position.last_updated` and fill/cancel records need a broker time -- so
/// that fails rather than silently substituting the observation clock.
fn terminal_broker_time(
    specific: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    order_id: &str,
    field: MissingOrderField,
) -> Result<DateTime<Utc>, AlpacaBrokerApiError> {
    if let Some(time) = specific {
        return Ok(time);
    }
    if let Some(time) = updated_at {
        warn!(
            order_id,
            ?field,
            "Terminal order response omitted its status timestamp; using updated_at"
        );
        return Ok(time);
    }
    Err(AlpacaBrokerApiError::IncompleteOrder {
        order_id: ExecutorOrderId::new(order_id),
        field,
    })
}

/// Falls back to the local observation time -- loudly, never silently --
/// when the broker response omits the relevant timestamp. Only acceptable
/// for non-terminal-specific timestamps; `filled_at`/`canceled_at` for
/// terminal states must fail instead (see `terminal_broker_time`).
fn broker_time_or_observation(broker_time: Option<DateTime<Utc>>, order_id: &str) -> DateTime<Utc> {
    broker_time.unwrap_or_else(|| {
        warn!(
            order_id,
            "Broker response omitted the status timestamp; using observation time"
        );
        Utc::now()
    })
}

/// Classifies a broker status into the `OrderStatus` it maps to, and --
/// whenever that status is `OrderStatus::Failed` -- the failure's terminality.
/// A caller may release its idempotency key only on a `Terminal` failure.
///
/// The terminality half is not decided here: it is read off
/// [`CryptoOrderFailureReason::terminality`], the crate's single source, so
/// this mapping and the conversion poll's wait-vs-fail decision cannot drift
/// apart.
///
/// A single exhaustive match producing both classifications together, rather
/// than two independent matches over `BrokerOrderStatus`: the previous shape
/// let a status be added to one match's `Failed`/terminal arm without the
/// other match being updated to agree, which only surfaced as a runtime
/// `MissingOrderField::FailureTerminality` error. Here, a newly added status
/// must be classified for both dimensions in the same arm or the match is
/// non-exhaustive and the crate fails to compile.
fn classify_broker_status(
    status: BrokerOrderStatus,
) -> (OrderStatus, Option<OrderFailureTerminality>) {
    use BrokerOrderStatus::*;
    use CryptoOrderFailureReason as Reason;

    match status {
        // Submitted to broker and in progress.
        New | Accepted | PendingNew | AcceptedForBidding | PendingCancel | PendingReplace
        | Stopped => (OrderStatus::Submitted, None),

        // Partially filled -- distinct from Submitted so the poll loop can
        // drive `UpdatePartialFill` on the aggregate before any cancel.
        PartiallyFilled => (OrderStatus::PartiallyFilled, None),

        // Successfully filled.
        Filled => (OrderStatus::Filled, None),

        // Cancelled by the broker after a cancel request was accepted.
        Canceled => (OrderStatus::Cancelled, None),

        Expired => (OrderStatus::Failed, Some(Reason::Expired.terminality())),
        Rejected => (OrderStatus::Failed, Some(Reason::Rejected.terminality())),
        DoneForDay => (OrderStatus::Failed, Some(Reason::DoneForDay.terminality())),
        Replaced => (OrderStatus::Failed, Some(Reason::Replaced.terminality())),
        Suspended => (OrderStatus::Failed, Some(Reason::Suspended.terminality())),
        Calculated => (OrderStatus::Failed, Some(Reason::Calculated.terminality())),
    }
}

fn truncate_shares_to_alpaca_precision(
    shares: Positive<FractionalShares>,
) -> Result<Positive<FractionalShares>, AlpacaBrokerApiError> {
    let original = shares.inner().inner();
    let truncated_float =
        crate::truncate_to_decimal_places(original, crate::ALPACA_MAX_DECIMAL_PLACES)?.ok_or(
            AlpacaBrokerApiError::BelowPrecision {
                shares,
                max_decimals: crate::ALPACA_MAX_DECIMAL_PLACES,
            },
        )?;

    if !truncated_float.eq(original)? {
        debug!(
            original = %shares,
            truncated = %FractionalShares::new(truncated_float),
            "Truncated order quantity to {} decimal places for Alpaca",
            crate::ALPACA_MAX_DECIMAL_PLACES,
        );
    }

    Ok(Positive::new(FractionalShares::new(truncated_float))?)
}

fn validate_usdc_amount_for_alpaca_precision(amount: Float) -> Result<Float, AlpacaBrokerApiError> {
    let truncated_amount =
        crate::truncate_to_decimal_places(amount, ALPACA_CRYPTO_MAX_DECIMAL_PLACES)?.ok_or(
            AlpacaBrokerApiError::UsdcBelowPrecision {
                amount,
                max_decimals: ALPACA_CRYPTO_MAX_DECIMAL_PLACES,
            },
        )?;

    if !truncated_amount.eq(amount)? {
        return Err(AlpacaBrokerApiError::UsdcPrecisionExceeded {
            amount,
            max_decimals: ALPACA_CRYPTO_MAX_DECIMAL_PLACES,
        });
    }

    Ok(amount)
}

/// Truncate a notional to the whole cents Alpaca accepts.
///
/// Unlike the `qty` side this truncates rather than rejecting excess precision:
/// the transfer amount is sized from an imbalance ratio and routinely carries
/// six decimals, so rejecting would fail the common path outright. Truncation
/// is always downwards, which is the only safe direction -- the notional is
/// held in full against settled cash, so rounding up could ask for more than
/// the transfer was sized against. Sub-cent amounts have nothing left to place
/// and are refused instead.
fn truncate_notional_to_whole_cents(amount: Float) -> Result<Float, AlpacaBrokerApiError> {
    let truncated = crate::truncate_to_decimal_places(amount, ALPACA_NOTIONAL_MAX_DECIMAL_PLACES)?
        .ok_or(AlpacaBrokerApiError::UsdcBelowPrecision {
            amount,
            max_decimals: ALPACA_NOTIONAL_MAX_DECIMAL_PLACES,
        })?;

    if !truncated.eq(amount)? {
        debug!(
            ?amount,
            ?truncated,
            "Truncated USD->USDC notional to whole cents for Alpaca"
        );
    }

    Ok(truncated)
}

/// Convert USDC to/from USD on Alpaca.
///
/// This uses the USDC/USD trading pair:
/// - To convert USDC to USD buying power: sell USDC/USD
/// - To convert USD buying power to USDC: buy USDC/USD
///
/// Each direction names the quantity it is actually constrained by, which is
/// what keeps a conversion sized at everything available from being refused.
/// The sell holds USDC and names USDC (`qty`). The buy is bounded by settled
/// cash and names dollars (`notional`), so Alpaca's ~2% collar has nothing to
/// inflate: the hold equals the dollars named, and the collar and execution
/// price instead bound the fill to less USDC than those dollars. The buy is
/// also truncated to whole cents, which is all Alpaca accepts in a notional.
/// Callers therefore size downstream steps from the fill, never from the
/// amount requested.
pub(crate) async fn convert_usdc_usd(
    client: &AlpacaBrokerApiClient,
    order: ConversionOrder,
    client_order_id: &ClientOrderId,
) -> Result<CryptoOrderResponse, AlpacaBrokerApiError> {
    let (side, order_size) = match order {
        ConversionOrder::SellUsdc(quantity) => (
            OrderSide::Sell,
            CryptoOrderSize::Quantity(validate_usdc_amount_for_alpaca_precision(
                quantity.inner().inner(),
            )?),
        ),
        ConversionOrder::BuyWithUsd(dollars) => (
            OrderSide::Buy,
            CryptoOrderSize::Notional(truncate_notional_to_whole_cents(dollars.inner().inner())?),
        ),
    };

    debug!(?side, ?order_size, %client_order_id, "Placing USDC/USD conversion order");

    let request = CryptoOrderRequest {
        symbol: "USDCUSD".to_string(),
        size: order_size,
        side,
        order_type: "market",
        time_in_force: "gtc",
        client_order_id: client_order_id.clone(),
    };

    client.place_crypto_order(&request).await
}

/// How long a conversion order may stay non-terminal before the remainder is
/// cancelled. `USDCUSD` market orders normally fill in seconds; minutes with
/// no terminal state means the collared limit sits outside the market and
/// will not complete on its own.
///
/// On Alpaca->Base this deadline is the ONLY bound on the wait: that transfer
/// job carries no per-attempt timeout. On Base->Alpaca it must additionally
/// stay well below the transfer job's per-attempt timeout (1h in prod), so
/// the resolution propagates while the outer await is still active.
const CONVERSION_ORDER_DEADLINE: Duration = Duration::from_secs(300);

/// After a deadline cancel, how long to wait for the broker to report the
/// cancelled order's terminal state before giving up.
///
/// A chosen bound, not a documented one: Alpaca publishes no cancel
/// acknowledgement or settlement time for crypto orders, so this value is not
/// backed by the API reference and has not been measured against a live
/// `pending_cancel`. Exceeding it does not abandon the order silently -- the
/// poll fails with `ConversionCancelNotSettled`, which records the last
/// observed fill and that the order may still be live.
const CANCEL_SETTLE_DEADLINE: Duration = Duration::from_secs(30);

/// The bounds a conversion poll runs under, carried together so a call
/// site cannot swap them and so tests control the real timing: a settle
/// deadline shorter than the poll interval would otherwise silently wait a
/// full interval anyway.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ConversionPollDeadlines {
    /// How long the order may stay non-terminal before its remainder is
    /// cancelled.
    order: Duration,
    /// How long the broker then has to report the cancelled order terminal.
    cancel_settle: Duration,
    /// How long to wait between order reads, both before the deadline and
    /// inside the settle window.
    interval: Duration,
}

impl ConversionPollDeadlines {
    pub(super) const PRODUCTION: Self = Self {
        order: CONVERSION_ORDER_DEADLINE,
        cancel_settle: CANCEL_SETTLE_DEADLINE,
        interval: Duration::from_millis(500),
    };
}

/// A conversion order that reached a state it can no longer leave, and
/// whether the deadline cancel is what forced it there -- the fact that
/// separates "the broker resolved this order" from "we gave up on it", which
/// [`poll_crypto_order_until_filled`] needs to accept a partial fill only on
/// the path that cancelled the remainder itself.
struct SettledConversionOrder {
    order: CryptoOrderResponse,
    outcome: TerminalCryptoOutcome,
    deadline_cancelled: bool,
}

/// Poll a crypto order until it fills, treating a stall as a decision point
/// rather than waiting forever.
///
/// An order that can still change state -- pending, partially filled, or
/// failed with a status it may resume from -- is waited on until
/// `deadlines.order`. Past it the remainder is cancelled: a fill that raced
/// the cancel is a success, a cancelled order with a partial fill is accepted
/// as-is (callers size downstream amounts from `filled_qty`, not the request),
/// and a cancelled order with nothing filled is a timeout failure. This is
/// what keeps a stalled conversion from holding the single-concurrency
/// transfer worker and the in-flight USDC rebalance guard indefinitely.
pub(crate) async fn poll_crypto_order_until_filled(
    client: &AlpacaBrokerApiClient,
    order_id: Uuid,
    deadlines: ConversionPollDeadlines,
) -> Result<CryptoOrderResponse, AlpacaBrokerApiError> {
    use TerminalCryptoOutcome::*;

    let settled = poll_until_terminal(client, order_id, deadlines).await?;

    match settled.outcome {
        Filled => Ok(settled.order),
        Failed(CryptoOrderFailureReason::Canceled) if settled.deadline_cancelled => {
            // An absent `filled_qty` is not evidence of a zero fill, which is
            // exactly what `ConversionTimedOut` asserts, so the unknown is
            // surfaced as the missing field it is.
            let filled = settled.order.filled_quantity.ok_or_else(|| {
                AlpacaBrokerApiError::IncompleteOrder {
                    order_id: ExecutorOrderId::new(&order_id),
                    field: MissingOrderField::FilledQty,
                }
            })?;

            if filled.is_zero()? {
                return Err(AlpacaBrokerApiError::ConversionTimedOut { order_id });
            }

            warn!(
                target: "broker",
                order_id = %order_id,
                filled = ?settled.order.filled_quantity,
                requested_qty = ?settled.order.quantity,
                requested_notional = ?settled.order.notional,
                "Accepting partially filled conversion; remainder cancelled"
            );
            Ok(settled.order)
        }
        Failed(reason) => Err(AlpacaBrokerApiError::CryptoOrderFailed { order_id, reason }),
    }
}

/// Poll a crypto order until any terminal state, bounded by `deadlines`.
///
/// Unlike [`poll_crypto_order_until_filled`], terminal failures are returned
/// as `Ok` for the caller to classify (the resume path records its own
/// failure events). Past the deadline the remainder is cancelled and the
/// settled terminal order returned; the errors a stall can produce are the
/// ones `cancel_and_settle` raises when the cancel itself does not resolve.
pub(crate) async fn poll_crypto_order_to_terminal(
    client: &AlpacaBrokerApiClient,
    order_id: Uuid,
    deadlines: ConversionPollDeadlines,
) -> Result<CryptoOrderResponse, AlpacaBrokerApiError> {
    poll_until_terminal(client, order_id, deadlines)
        .await
        .map(|settled| settled.order)
}

/// The deadline-and-cancel loop both polls share: wait for a terminal state,
/// and past `deadlines.order` force one by cancelling the remainder.
///
/// One body on purpose -- the deadline handling is the safety-critical part
/// (it is what bounds the wait), and a correction applied to one poll but not
/// the other would reintroduce the unbounded wait for the path it missed.
/// Callers only interpret the returned outcome.
async fn poll_until_terminal(
    client: &AlpacaBrokerApiClient,
    order_id: Uuid,
    deadlines: ConversionPollDeadlines,
) -> Result<SettledConversionOrder, AlpacaBrokerApiError> {
    let started = tokio::time::Instant::now();

    loop {
        let order = client.get_crypto_order(order_id).await?;

        match order.classify().terminal() {
            Some(outcome) => {
                return Ok(SettledConversionOrder {
                    order,
                    outcome,
                    deadline_cancelled: false,
                });
            }
            None if started.elapsed() >= deadlines.order => {
                warn!(
                    target: "broker",
                    order_id = %order_id,
                    deadline = ?deadlines.order,
                    status = order.status_display(),
                    filled = ?order.filled_quantity,
                    "Conversion order still not terminal at the deadline; cancelling the remainder"
                );
                let (order, outcome) = cancel_and_settle(client, order_id, deadlines).await?;

                return Ok(SettledConversionOrder {
                    order,
                    outcome,
                    deadline_cancelled: true,
                });
            }
            None => {
                trace!(
                    target: "broker",
                    order_id = %order_id,
                    status = order.status_display(),
                    "Crypto order can still change state, waiting..."
                );
                tokio::time::sleep(deadlines.interval).await;
            }
        }
    }
}

/// Cancel `order_id` and wait up to `deadlines.cancel_settle` for the broker
/// to report its terminal state, returning that state alongside the order.
///
/// A cancel the broker never accepted is re-issued inside the settle window,
/// so a single transient failure cannot leave the remainder live. An order
/// still non-terminal after the settle window errors with
/// [`AlpacaBrokerApiError::ConversionCancelNotSettled`], carrying how the
/// broker answered the cancel so the persisted reason does not claim one that
/// never took effect. See [`request_cancel`] for the per-answer handling.
async fn cancel_and_settle(
    client: &AlpacaBrokerApiClient,
    order_id: Uuid,
    deadlines: ConversionPollDeadlines,
) -> Result<(CryptoOrderResponse, TerminalCryptoOutcome), AlpacaBrokerApiError> {
    let mut cancel = request_cancel(client, order_id).await?;

    let started = tokio::time::Instant::now();
    let mut filled_quantity = None;

    loop {
        // A cancel the broker never took leaves the remainder live, so it is
        // re-issued for as long as the settle window lasts. Retrying is only
        // meaningful after a transport or 5xx failure: an accepted cancel
        // needs no repeat, and a declined one is refused for a reason that
        // will not change.
        if cancel == DeadlineCancel::Failed {
            cancel = request_cancel(client, order_id).await?;
        }

        match client.get_crypto_order(order_id).await {
            Ok(order) => {
                if let Some(outcome) = order.classify().terminal() {
                    return Ok((order, outcome));
                }
                filled_quantity = order.filled_quantity;
            }
            // A read that fails leaves the order's fate as unknown as a
            // non-terminal one, so it is retried inside the window rather
            // than abandoning a cancel that may still settle.
            Err(error) => warn!(
                target: "broker",
                order_id = %order_id,
                %error,
                "Failed to read back the cancelled conversion order"
            ),
        }

        if started.elapsed() >= deadlines.cancel_settle {
            return Err(AlpacaBrokerApiError::ConversionCancelNotSettled {
                order_id,
                cancel,
                filled_quantity,
            });
        }

        tokio::time::sleep(deadlines.interval).await;
    }
}

/// Issue the deadline cancel once, mapping the broker's answer to what may
/// later be claimed about the remainder.
///
/// Only an unrecognised order aborts: reading it back would hit the same id
/// and 404 again, so its fill state is unobservable. A 422 means the order is
/// no longer cancelable -- it went terminal between the last poll and the
/// DELETE -- and a transport or 5xx failure leaves the request unmade; both
/// fall through to the status read, which observes whatever the order became.
async fn request_cancel(
    client: &AlpacaBrokerApiClient,
    order_id: Uuid,
) -> Result<DeadlineCancel, AlpacaBrokerApiError> {
    match client.cancel_order(order_id).await {
        Ok(CancellationOutcome::Requested) => Ok(DeadlineCancel::Accepted),
        Ok(CancellationOutcome::OrderNotFound) => {
            Err(AlpacaBrokerApiError::ConversionOrderNotFound { order_id })
        }
        Err(AlpacaBrokerApiError::ApiError { status, .. })
            if status == StatusCode::UNPROCESSABLE_ENTITY =>
        {
            Ok(DeadlineCancel::Declined)
        }
        Err(error) => {
            warn!(
                target: "broker",
                order_id = %order_id,
                %error,
                "Cancelling the stalled conversion order failed; retrying inside the settle window"
            );
            Ok(DeadlineCancel::Failed)
        }
    }
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use proptest::prelude::*;
    use reqwest::StatusCode;
    use serde_json::json;
    use tracing_test::traced_test;
    use uuid::uuid;

    #[test]
    fn kms_jwt_errors_are_never_the_duplicate_order_case() {
        let error = AlpacaBrokerApiError::KmsJwt(crate::KmsJwtError::ClockBeforeEpoch);
        assert!(!is_duplicate_client_order_id(&error));
    }

    use super::*;
    use crate::ClientOrderId;
    use crate::alpaca_broker_api::auth::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode,
    };
    use crate::alpaca_broker_api::duplicate_client_order_id_body;
    use st0x_float_macro::float;

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    fn create_test_ctx(mode: AlpacaBrokerApiMode) -> AlpacaBrokerApiCtx {
        AlpacaBrokerApiCtx {
            auth: crate::AlpacaBrokerAuth::Basic {
                api_key: "test_key".to_string(),
                api_secret: "test_secret".to_string(),
            },
            account_id: TEST_ACCOUNT_ID,
            mode: Some(mode),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        }
    }

    #[test]
    fn classify_maps_every_broker_status_to_its_outcome() {
        use crate::alpaca_broker_api::CryptoOrderFailureReason;

        let cases = [
            ("filled", CryptoOrderOutcome::Filled),
            ("new", CryptoOrderOutcome::Pending),
            ("pending_new", CryptoOrderOutcome::Pending),
            ("partially_filled", CryptoOrderOutcome::Pending),
            ("accepted", CryptoOrderOutcome::Pending),
            ("accepted_for_bidding", CryptoOrderOutcome::Pending),
            ("pending_cancel", CryptoOrderOutcome::Pending),
            ("pending_replace", CryptoOrderOutcome::Pending),
            ("stopped", CryptoOrderOutcome::Pending),
            (
                "canceled",
                CryptoOrderOutcome::Failed(CryptoOrderFailureReason::Canceled),
            ),
            (
                "expired",
                CryptoOrderOutcome::Failed(CryptoOrderFailureReason::Expired),
            ),
            (
                "rejected",
                CryptoOrderOutcome::Failed(CryptoOrderFailureReason::Rejected),
            ),
            (
                "done_for_day",
                CryptoOrderOutcome::Failed(CryptoOrderFailureReason::DoneForDay),
            ),
            (
                "replaced",
                CryptoOrderOutcome::Failed(CryptoOrderFailureReason::Replaced),
            ),
            (
                "suspended",
                CryptoOrderOutcome::Failed(CryptoOrderFailureReason::Suspended),
            ),
            (
                "calculated",
                CryptoOrderOutcome::Failed(CryptoOrderFailureReason::Calculated),
            ),
        ];

        for (status, expected) in cases {
            let order: CryptoOrderResponse = serde_json::from_value(json!({
                "id": "904837e3-3b76-47ec-b432-046db621571b",
                "symbol": "USDCUSD",
                "qty": "100",
                "status": status,
                "created_at": "2025-01-06T12:00:00Z"
            }))
            .unwrap();

            assert_eq!(order.classify(), expected, "status {status} misclassified");
        }
    }

    #[tokio::test]
    async fn test_place_market_order_buy_success() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body(json!({
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "type": "market",
                    "time_in_force": "day",
                    "extended_hours": false,
                    "client_order_id": "33333333-3333-4333-8333-333333333333"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let market_order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(100))).unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(uuid!(
                "33333333-3333-4333-8333-333333333333"
            )),
        };

        let placement = place_market_order(&client, market_order, TimeInForce::Day)
            .await
            .unwrap();

        mock.assert();
        assert_eq!(placement.order_id, "904837e3-3b76-47ec-b432-046db621571b");
        assert_eq!(placement.symbol.to_string(), "AAPL");
        assert_eq!(placement.shares.inner(), FractionalShares::new(float!(100)));
        assert_eq!(placement.direction, Direction::Buy);
    }

    #[tokio::test]
    async fn test_place_market_order_sell_success() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body(json!({
                    "symbol": "TSLA",
                    "qty": "50",
                    "side": "sell",
                    "type": "market",
                    "time_in_force": "day",
                    "extended_hours": false,
                    "client_order_id": "44444444-4444-4444-8444-444444444444"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": "TSLA",
                    "qty": "50",
                    "side": "sell",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let market_order = MarketOrder {
            symbol: Symbol::new("TSLA").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(50))).unwrap(),
            direction: Direction::Sell,
            client_order_id: ClientOrderId::from_uuid(uuid!(
                "44444444-4444-4444-8444-444444444444"
            )),
        };

        let placement = place_market_order(&client, market_order, TimeInForce::Day)
            .await
            .unwrap();

        mock.assert();
        assert_eq!(placement.order_id, "61e7b016-9c91-4a97-b912-615c9d365c9d");
        assert_eq!(placement.symbol.to_string(), "TSLA");
        assert_eq!(placement.shares.inner(), FractionalShares::new(float!(50)));
        assert_eq!(placement.direction, Direction::Sell);
        // A fresh market placement carries no session terms.
        assert!(!placement.extended_hours);
        assert_eq!(placement.limit_price, None);
    }

    #[tokio::test]
    async fn place_market_order_reconciles_duplicate_client_order_id() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let client_order_uuid = uuid!("66666666-6666-4666-8666-666666666666");
        let client_order_id = client_order_uuid.to_string();
        let existing_order_id = "904837e3-3b76-47ec-b432-046db621571b";

        // The broker rejects the re-used client_order_id with a 422 because it
        // already recorded the original attempt (whose 2xx was lost in flight).
        let place_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
            then.status(422)
                .header("content-type", "application/json")
                .json_body(duplicate_client_order_id_body());
        });

        // We reconcile by adopting the order the broker actually accepted.
        let lookup_mock = server.mock(|when, then| {
            when.method(GET)
                .path(
                    "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders:by_client_order_id",
                )
                .query_param("client_order_id", client_order_id.as_str());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": existing_order_id,
                    "symbol": "AAPL",
                    "qty": "7",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let market_order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            // This attempt's recomputed intent is 10 shares, but the broker
            // already holds the original 7-share order under this key.
            shares: Positive::new(FractionalShares::new(float!(10))).unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(client_order_uuid),
        };

        let placement = place_market_order(&client, market_order, TimeInForce::Day)
            .await
            .unwrap();

        place_mock.assert();
        lookup_mock.assert();
        // Adopts the broker's recorded order id and its recorded quantity (7),
        // not this attempt's recomputed 10 shares -- the residual is left for
        // the next position scan to hedge.
        assert_eq!(placement.order_id, existing_order_id);
        assert_eq!(placement.shares.inner(), FractionalShares::new(float!(7)));
        assert_eq!(placement.direction, Direction::Buy);
        // A fresh adoption of a plain market order carries no session terms.
        assert!(!placement.extended_hours);
        assert_eq!(placement.limit_price, None);
    }

    /// The duplicate-422 warn log includes the Alpaca-reported numeric code
    /// alongside the message. `place_market_order`'s reconciliation swallows
    /// the intermediate `ApiError` on success, so this drives `place_order`
    /// directly against a realistic duplicate-client_order_id body to prove
    /// `code` is actually parsed into `alpaca_code`, not just `message`.
    #[tokio::test]
    async fn place_order_parses_alpaca_code_from_duplicate_client_order_id_body() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
            then.status(422)
                .header("content-type", "application/json")
                .json_body(duplicate_client_order_id_body());
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let request = OrderRequest {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: Positive::new(FractionalShares::new(float!(1))).unwrap(),
            side: OrderSide::Buy,
            order_type: "market",
            time_in_force: TimeInForce::Day.as_api_str(),
            extended_hours: false,
            client_order_id: ClientOrderId::from_uuid(uuid!(
                "77777777-7777-4777-8777-777777777777"
            )),
        };

        let error = client.place_order(&request).await.unwrap_err();

        mock.assert();
        let AlpacaBrokerApiError::ApiError {
            alpaca_code,
            message,
            ..
        } = error
        else {
            panic!("expected ApiError, got {error:?}");
        };
        assert_eq!(alpaca_code, Some(40_010_001));
        assert_eq!(message, "client_order_id must be unique");
    }

    #[tokio::test]
    async fn place_market_order_adoption_reports_adopted_extended_hours_terms() {
        // Lost-response scenario the convergence sweep depends on: an
        // extended-hours limit order was accepted but its 2xx was lost, the
        // retry runs after the regular open as a MARKET order under the same
        // client_order_id, and the broker 422s. The adoption must report the
        // ADOPTED order's extended-hours flag and limit price -- not this
        // attempt's market terms -- so the aggregate records broker reality
        // and the regular-open cancel-and-replace sweep can converge it.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let client_order_uuid = uuid!("99999999-9999-4999-8999-999999999999");
        let client_order_id = client_order_uuid.to_string();
        let existing_order_id = "904837e3-3b76-47ec-b432-046db621571b";

        let place_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
            then.status(422)
                .header("content-type", "application/json")
                .json_body(duplicate_client_order_id_body());
        });

        let lookup_mock = server.mock(|when, then| {
            when.method(GET)
                .path(
                    "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders:by_client_order_id",
                )
                .query_param("client_order_id", client_order_id.as_str());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": existing_order_id,
                    "symbol": "AAPL",
                    "qty": "7",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null,
                    "type": "limit",
                    "limit_price": "195.25",
                    "extended_hours": true
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let market_order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(10))).unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(client_order_uuid),
        };

        let placement = place_market_order(&client, market_order, TimeInForce::Day)
            .await
            .unwrap();

        place_mock.assert();
        lookup_mock.assert();
        assert_eq!(placement.order_id, existing_order_id);
        assert!(placement.extended_hours);
        assert_eq!(
            placement.limit_price,
            Some(Positive::new(Usd::new(float!(195.25))).unwrap())
        );
    }

    #[tokio::test]
    async fn place_market_order_adoption_errors_on_non_positive_limit_price() {
        // Broker holds an existing order under this client_order_id (lost-response
        // adoption), but reports a zero limit_price in the lookup response.
        // parse_limit_price must fail fast rather than silently passing None.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let client_order_uuid = uuid!("aaaabbbb-aaaa-4aaa-8aaa-aaaaaaaabbbb");
        let client_order_id = client_order_uuid.to_string();
        let existing_order_id = "904837e3-3b76-47ec-b432-046db621571b";

        let place_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
            then.status(422)
                .header("content-type", "application/json")
                .json_body(duplicate_client_order_id_body());
        });

        let lookup_mock = server.mock(|when, then| {
            when.method(GET)
                .path(
                    "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders:by_client_order_id",
                )
                .query_param("client_order_id", client_order_id.as_str());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": existing_order_id,
                    "symbol": "AAPL",
                    "qty": "7",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null,
                    "type": "limit",
                    "limit_price": "0",
                    "extended_hours": true
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let market_order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(10))).unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(client_order_uuid),
        };

        let err = place_market_order(&client, market_order, TimeInForce::Day)
            .await
            .unwrap_err();

        place_mock.assert();
        lookup_mock.assert();
        assert!(
            matches!(err, AlpacaBrokerApiError::NotPositiveLimitPrice(_)),
            "Expected NotPositiveLimitPrice error on zero limit_price, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn place_limit_order_reconciles_duplicate_client_order_id() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let client_order_uuid = uuid!("88888888-8888-4888-8888-888888888888");
        let client_order_id = client_order_uuid.to_string();
        let existing_order_id = "904837e3-3b76-47ec-b432-046db621571b";

        // Same lost-response case as the market path: the broker rejects the
        // re-used client_order_id with a 422 because it already recorded the
        // original extended-hours limit order whose 2xx was lost in flight.
        let place_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
            then.status(422)
                .header("content-type", "application/json")
                .json_body(duplicate_client_order_id_body());
        });

        let lookup_mock = server.mock(|when, then| {
            when.method(GET)
                .path(
                    "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders:by_client_order_id",
                )
                .query_param("client_order_id", client_order_id.as_str());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": existing_order_id,
                    "symbol": "AAPL",
                    "qty": "7",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let limit_order = AlpacaLimitOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            // Recomputed intent is 10 shares, but the broker already holds the
            // original 7-share limit order under this key; we adopt it.
            shares: Positive::new(FractionalShares::new(float!(10))).unwrap(),
            direction: Direction::Buy,
            limit_price: AlpacaLimitPrice::try_new(
                Positive::new(Usd::new(float!(195.25))).unwrap(),
            )
            .unwrap(),
            extended_hours: true,
            client_order_id: ClientOrderId::from_uuid(client_order_uuid),
        };

        let placement = place_limit_order(&client, limit_order).await.unwrap();

        place_mock.assert();
        lookup_mock.assert();
        assert_eq!(placement.order_id, existing_order_id);
        assert_eq!(placement.shares.inner(), FractionalShares::new(float!(7)));
        assert_eq!(placement.direction, Direction::Buy);
        // The lookup response omitted the term echo fields; adoption must
        // fall back to the REQUEST's terms (the adopted order was created by
        // a prior attempt of this same extended-hours placement), or the
        // regular-open cancel-and-replace sweep never sees the live order.
        assert!(placement.extended_hours);
        assert_eq!(
            placement.limit_price,
            Some(Positive::new(Usd::new(float!(195.25))).unwrap())
        );
    }

    #[tokio::test]
    async fn place_limit_order_adoption_errors_on_non_positive_limit_price() {
        // Broker holds an existing order under this client_order_id (lost-response
        // adoption), but reports a zero limit_price in the lookup response.
        // parse_limit_price must fail fast rather than silently falling back to None.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let client_order_uuid = uuid!("bbbbcccc-bbbb-4bbb-8bbb-bbbbbbbbcccc");
        let client_order_id = client_order_uuid.to_string();
        let existing_order_id = "904837e3-3b76-47ec-b432-046db621571b";

        let place_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
            then.status(422)
                .header("content-type", "application/json")
                .json_body(duplicate_client_order_id_body());
        });

        let lookup_mock = server.mock(|when, then| {
            when.method(GET)
                .path(
                    "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders:by_client_order_id",
                )
                .query_param("client_order_id", client_order_id.as_str());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": existing_order_id,
                    "symbol": "AAPL",
                    "qty": "7",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null,
                    "type": "limit",
                    "limit_price": "0",
                    "extended_hours": true
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let limit_order = AlpacaLimitOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(10))).unwrap(),
            direction: Direction::Buy,
            limit_price: AlpacaLimitPrice::try_new(
                Positive::new(Usd::new(float!(195.25))).unwrap(),
            )
            .unwrap(),
            extended_hours: true,
            client_order_id: ClientOrderId::from_uuid(client_order_uuid),
        };

        let err = place_limit_order(&client, limit_order).await.unwrap_err();

        place_mock.assert();
        lookup_mock.assert();
        assert!(
            matches!(err, AlpacaBrokerApiError::NotPositiveLimitPrice(_)),
            "Expected NotPositiveLimitPrice error on zero limit_price, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn place_limit_order_fresh_placement_records_request_terms() {
        // A fresh extended-hours limit placement must record the REQUEST's
        // terms even when the broker's placement response omits the echo
        // fields -- otherwise the regular-open convergence sweep (keyed off
        // extended_hours) never sees the order.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let place_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "AAPL",
                    "qty": "10",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let limit_price = Positive::new(Usd::new(float!(195.25))).unwrap();
        let limit_order = AlpacaLimitOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(10))).unwrap(),
            direction: Direction::Buy,
            limit_price: AlpacaLimitPrice::try_new(limit_price).unwrap(),
            extended_hours: true,
            client_order_id: ClientOrderId::from_uuid(uuid!(
                "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
            )),
        };

        let placement = place_limit_order(&client, limit_order).await.unwrap();

        place_mock.assert();
        assert!(placement.extended_hours);
        assert_eq!(placement.limit_price, Some(limit_price));
    }

    #[tokio::test]
    async fn place_market_order_errors_when_duplicate_order_not_found() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let client_order_uuid = uuid!("77777777-7777-4777-8777-777777777777");

        let place_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
            then.status(422)
                .header("content-type", "application/json")
                .json_body(duplicate_client_order_id_body());
        });

        // The broker reported a duplicate but the lookup finds nothing -- an
        // inconsistent state that must surface as an error so the job retries.
        let lookup_mock = server.mock(|when, then| {
            when.method(GET).path(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders:by_client_order_id",
            );
            then.status(404)
                .header("content-type", "application/json")
                .json_body(json!({"code": 40_410_000_u64, "message": "order not found"}));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let market_order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(10))).unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(client_order_uuid),
        };

        let error = place_market_order(&client, market_order, TimeInForce::Day)
            .await
            .unwrap_err();

        place_mock.assert();
        lookup_mock.assert();
        assert!(
            matches!(
                error,
                AlpacaBrokerApiError::DuplicateOrderNotFound { ref client_order_id }
                    if client_order_id == &ClientOrderId::from_uuid(client_order_uuid)
            ),
            "expected DuplicateOrderNotFound, got {error:?}"
        );
    }

    #[tokio::test]
    async fn place_market_order_propagates_non_duplicate_422() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        // A 422 that is NOT the duplicate-key case must propagate unchanged and
        // must not trigger the by-client-order-id reconciliation lookup.
        let place_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders");
            then.status(422)
                .header("content-type", "application/json")
                .json_body(json!({"message": "insufficient buying power"}));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let market_order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(10))).unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(uuid!(
                "88888888-8888-4888-8888-888888888888"
            )),
        };

        let error = place_market_order(&client, market_order, TimeInForce::Day)
            .await
            .unwrap_err();

        place_mock.assert();
        assert!(
            matches!(
                error,
                AlpacaBrokerApiError::ApiError { status, .. } if status.as_u16() == 422
            ),
            "expected a propagated 422 ApiError, got {error:?}"
        );
    }

    #[tokio::test]
    async fn test_place_limit_order_buy_success() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body(json!({
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "type": "limit",
                    "limit_price": "195.25",
                    "time_in_force": "day",
                    "extended_hours": false,
                    "client_order_id": "44444444-4444-4444-8444-444444444444"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "AAPL",
                    "qty": "100",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let limit_order = AlpacaLimitOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(100))).unwrap(),
            direction: Direction::Buy,
            limit_price: AlpacaLimitPrice::try_new(
                Positive::new(Usd::new(float!(195.25))).unwrap(),
            )
            .unwrap(),
            extended_hours: false,
            client_order_id: ClientOrderId::from_uuid(uuid!(
                "44444444-4444-4444-8444-444444444444"
            )),
        };

        let placement = place_limit_order(&client, limit_order).await.unwrap();

        mock.assert();
        assert_eq!(placement.order_id, "904837e3-3b76-47ec-b432-046db621571b");
        assert_eq!(placement.symbol.to_string(), "AAPL");
        assert_eq!(placement.shares.inner(), FractionalShares::new(float!(100)));
        assert_eq!(placement.direction, Direction::Buy);
    }

    #[tokio::test]
    async fn test_place_limit_order_sell_success_with_extended_hours() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body(json!({
                    "symbol": "TSLA",
                    "qty": "50",
                    "side": "sell",
                    "type": "limit",
                    "limit_price": "210",
                    "time_in_force": "day",
                    "extended_hours": true,
                    "client_order_id": "55555555-5555-4555-8555-555555555555"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": "TSLA",
                    "qty": "50",
                    "side": "sell",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let limit_order = AlpacaLimitOrder {
            symbol: Symbol::new("TSLA").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(50))).unwrap(),
            direction: Direction::Sell,
            limit_price: AlpacaLimitPrice::try_new(Positive::new(Usd::new(float!(210))).unwrap())
                .unwrap(),
            extended_hours: true,
            client_order_id: ClientOrderId::from_uuid(uuid!(
                "55555555-5555-4555-8555-555555555555"
            )),
        };

        let placement = place_limit_order(&client, limit_order).await.unwrap();

        mock.assert();
        assert_eq!(placement.order_id, "61e7b016-9c91-4a97-b912-615c9d365c9d");
        assert_eq!(placement.symbol.to_string(), "TSLA");
        assert_eq!(placement.shares.inner(), FractionalShares::new(float!(50)));
        assert_eq!(placement.direction, Direction::Sell);
        // A fresh placement records the REQUEST's terms even when the broker
        // response omits the echo fields.
        assert!(placement.extended_hours);
        assert_eq!(
            placement.limit_price,
            Some(Positive::new(Usd::new(float!(210))).unwrap())
        );
    }

    #[test]
    fn test_alpaca_limit_price_rejects_more_than_two_decimals_at_or_above_one() {
        let error = AlpacaLimitPrice::try_new(Positive::new(Usd::new(float!(195.255))).unwrap())
            .unwrap_err();

        assert!(
            matches!(
                error,
                AlpacaBrokerApiError::InvalidLimitPricePrecision {
                    limit_price,
                    max_decimals: 2,
                } if limit_price == Positive::new(Usd::new(float!(195.255))).unwrap()
            ),
            "Expected InvalidLimitPricePrecision error, got: {error:?}"
        );
    }

    #[test]
    fn test_alpaca_limit_price_rejects_more_than_four_decimals_below_one() {
        let error = AlpacaLimitPrice::try_new(Positive::new(Usd::new(float!(0.12345))).unwrap())
            .unwrap_err();

        assert!(
            matches!(
                error,
                AlpacaBrokerApiError::InvalidLimitPricePrecision {
                    limit_price,
                    max_decimals: 4,
                } if limit_price == Positive::new(Usd::new(float!(0.12345))).unwrap()
            ),
            "Expected InvalidLimitPricePrecision error, got: {error:?}"
        );
    }

    // Boundary cases for Alpaca's minimum price variance. Per the order docs
    // (https://docs.alpaca.markets/us/docs/orders-at-alpaca): "Limit price
    // >=$1.00: Max Decimals = 2" and "Limit price <$1.00: Max Decimals = 4".
    // A price exactly at $1.00 falls in the >= $1.00 (2-decimal) bucket, and a
    // sub-penny there is rejected at submission with error 42210000. These pin
    // that boundary so a rounding regression that mis-buckets the $1.00 edge
    // (emitting a sub-penny limit Alpaca would reject) fails locally.
    #[test]
    fn alpaca_limit_price_accepts_penny_increments_at_one_dollar_boundary() {
        // Exactly $1.00 and a penny just above it are both in the 2-decimal
        // bucket and accepted.
        AlpacaLimitPrice::try_new(Positive::new(Usd::new(float!(1.00))).unwrap()).unwrap();
        AlpacaLimitPrice::try_new(Positive::new(Usd::new(float!(1.01))).unwrap()).unwrap();
    }

    #[test]
    fn alpaca_limit_price_rejects_sub_penny_at_one_dollar_boundary() {
        // $1.005 is sub-penny in the >= $1.00 bucket: the boundary uses the
        // 2-decimal rule (NOT the sub-dollar 4-decimal rule), so it is rejected.
        let error =
            AlpacaLimitPrice::try_new(Positive::new(Usd::new(float!(1.005))).unwrap()).unwrap_err();

        assert!(
            matches!(
                error,
                AlpacaBrokerApiError::InvalidLimitPricePrecision {
                    limit_price,
                    max_decimals: 2,
                } if limit_price == Positive::new(Usd::new(float!(1.005))).unwrap()
            ),
            "a sub-penny price at the $1.00 boundary must use the 2-decimal rule, got: {error:?}"
        );
    }

    #[test]
    fn alpaca_limit_price_accepts_four_decimals_just_below_one_dollar() {
        // $0.9999 is in the < $1.00 bucket, where 4 decimals are allowed.
        AlpacaLimitPrice::try_new(Positive::new(Usd::new(float!(0.9999))).unwrap()).unwrap();
    }

    #[tokio::test]
    async fn test_place_limit_order_accepts_price_with_four_decimals_below_one() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body(json!({
                    "symbol": "AAPL",
                    "qty": "1",
                    "side": "buy",
                    "type": "limit",
                    "limit_price": "0.1234",
                    "time_in_force": "day",
                    "extended_hours": false,
                    "client_order_id": "66666666-6666-4666-8666-666666666666"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "AAPL",
                    "qty": "1",
                    "side": "buy",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let limit_order = AlpacaLimitOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
            direction: Direction::Buy,
            limit_price: AlpacaLimitPrice::try_new(
                Positive::new(Usd::new(float!(0.1234))).unwrap(),
            )
            .unwrap(),
            extended_hours: false,
            client_order_id: ClientOrderId::from_uuid(uuid!(
                "66666666-6666-4666-8666-666666666666"
            )),
        };

        let placement = place_limit_order(&client, limit_order).await.unwrap();

        mock.assert();
        assert_eq!(placement.order_id, "904837e3-3b76-47ec-b432-046db621571b");
    }

    #[tokio::test]
    async fn test_get_order_status_pending() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let order_id = "904837e3-3b76-47ec-b432-046db621571b";

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
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

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let order_update = get_order_status(&client, order_id).await.unwrap();

        mock.assert();
        assert_eq!(order_update.order_id, order_id);
        assert_eq!(order_update.symbol.to_string(), "AAPL");
        assert_eq!(
            order_update.shares.inner(),
            FractionalShares::new(float!(100))
        );
        assert_eq!(order_update.direction, Direction::Buy);
        assert_eq!(order_update.status, OrderStatus::Submitted);
        assert!(order_update.price.is_none());
    }

    #[tokio::test]
    async fn test_get_order_status_filled() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let order_id = "61e7b016-9c91-4a97-b912-615c9d365c9d";

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "TSLA",
                    "qty": "50",
                    "side": "sell",
                    "status": "filled",
                    "filled_avg_price": "245.67",
                    "updated_at": "2025-01-06T14:32:05.000000Z",
                    "filled_at": "2025-01-06T14:32:01.111111Z"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let order_update = get_order_status(&client, order_id).await.unwrap();

        mock.assert();
        assert_eq!(order_update.order_id, order_id);
        assert_eq!(order_update.symbol.to_string(), "TSLA");
        // The update must carry the broker's fill time, not the local
        // observation time (and not the order's generic updated_at).
        assert_eq!(
            order_update.updated_at,
            "2025-01-06T14:32:01.111111Z"
                .parse::<DateTime<Utc>>()
                .unwrap()
        );
        assert_eq!(
            order_update.shares.inner(),
            FractionalShares::new(float!(50))
        );
        assert_eq!(order_update.direction, Direction::Sell);
        assert_eq!(order_update.status, OrderStatus::Filled);
        assert!(order_update.price.is_some_and(|price| {
            price
                .eq(Float::parse("245.67".to_string()).unwrap())
                .unwrap()
        }));
    }

    #[tokio::test]
    async fn test_get_order_status_partially_filled_parses_typed_fill() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let order_id = "0e9151b6-3b9f-4bf2-9f9b-1d6a8a1c1f0e";

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "AAPL",
                    "qty": "100",
                    "filled_qty": "40.5",
                    "side": "buy",
                    "status": "partially_filled",
                    "filled_avg_price": "199.50"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let order_update = get_order_status(&client, order_id).await.unwrap();

        mock.assert();
        assert_eq!(order_update.status, OrderStatus::PartiallyFilled);
        assert_eq!(
            order_update.shares_filled,
            Some(FractionalShares::new(float!(40.5)))
        );
        assert!(order_update.price.is_some_and(|price| {
            price
                .eq(Float::parse("199.50".to_string()).unwrap())
                .unwrap()
        }));
    }

    #[tokio::test]
    async fn test_get_order_status_cancelled_after_partial_fill_preserves_fill() {
        // Full realistic Broker API order object for a limit order canceled
        // after a partial fill, per the order entity reference
        // (https://docs.alpaca.markets/reference/getorderforaccount-1):
        // numeric fields are string-encoded ("filled_qty" is always present,
        // "0" when unfilled) and absent values are null. The cancel-and-replace
        // flow depends on this shape to preserve fills on cancellation.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let order_id = "61e7b016-9c91-4a97-b912-615c9d365c9d";

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "client_order_id": "33333333-3333-4333-8333-333333333333",
                    "created_at": "2025-01-06T08:00:05.221046Z",
                    "updated_at": "2025-01-06T14:32:01.118401Z",
                    "submitted_at": "2025-01-06T08:00:05.211088Z",
                    "filled_at": null,
                    "expired_at": null,
                    "canceled_at": "2025-01-06T14:32:01.111111Z",
                    "failed_at": null,
                    "replaced_at": null,
                    "replaced_by": null,
                    "replaces": null,
                    "asset_id": "b0b6dd9d-8b9b-48a9-ba46-b9d54906e415",
                    "symbol": "AAPL",
                    "asset_class": "us_equity",
                    "notional": null,
                    "qty": "100",
                    "filled_qty": "40.5",
                    "filled_avg_price": "199.50",
                    "order_class": "",
                    "order_type": "limit",
                    "type": "limit",
                    "side": "buy",
                    "time_in_force": "day",
                    "limit_price": "200.00",
                    "stop_price": null,
                    "status": "canceled",
                    "extended_hours": true,
                    "legs": null,
                    "trail_percent": null,
                    "trail_price": null,
                    "hwm": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let order_update = get_order_status(&client, order_id).await.unwrap();

        mock.assert();
        assert_eq!(order_update.status, OrderStatus::Cancelled);
        assert_eq!(
            order_update.shares_filled,
            Some(FractionalShares::new(float!(40.5)))
        );
        assert!(order_update.price.is_some_and(|price| {
            price
                .eq(Float::parse("199.50".to_string()).unwrap())
                .unwrap()
        }));
        // Broker cancellation time, not updated_at and not observation time.
        assert_eq!(
            order_update.updated_at,
            "2025-01-06T14:32:01.111111Z"
                .parse::<DateTime<Utc>>()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_get_order_status_cancelled_without_fill() {
        // Canceled before any fill: Alpaca reports filled_qty as the string
        // "0" (the field is always present) and filled_avg_price as null.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let order_id = "7f3b1c2a-5d4e-4f6a-8b9c-0d1e2f3a4b5c";

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "client_order_id": "44444444-4444-4444-8444-444444444444",
                    "created_at": "2025-01-06T08:00:05.221046Z",
                    "updated_at": "2025-01-06T14:32:01.118401Z",
                    "submitted_at": "2025-01-06T08:00:05.211088Z",
                    "filled_at": null,
                    "canceled_at": "2025-01-06T14:32:01.111111Z",
                    "failed_at": null,
                    "asset_id": "b0b6dd9d-8b9b-48a9-ba46-b9d54906e415",
                    "symbol": "TSLA",
                    "asset_class": "us_equity",
                    "notional": null,
                    "qty": "50",
                    "filled_qty": "0",
                    "filled_avg_price": null,
                    "order_class": "",
                    "order_type": "limit",
                    "type": "limit",
                    "side": "sell",
                    "time_in_force": "day",
                    "limit_price": "250.00",
                    "stop_price": null,
                    "status": "canceled",
                    "extended_hours": true
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let order_update = get_order_status(&client, order_id).await.unwrap();

        mock.assert();
        assert_eq!(order_update.status, OrderStatus::Cancelled);
        assert_eq!(
            order_update.shares_filled,
            Some(FractionalShares::new(float!(0)))
        );
        assert!(order_update.price.is_none());
        // Broker cancellation time, not the local observation time.
        assert_eq!(
            order_update.updated_at,
            "2025-01-06T14:32:01.111111Z"
                .parse::<DateTime<Utc>>()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn get_order_status_cancelled_with_null_canceled_at_uses_updated_at() {
        // Branch 2 of terminal_broker_time: canceled_at absent but updated_at
        // present -> warn and substitute updated_at.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let order_id = "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d";

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "TSLA",
                    "qty": "50",
                    "side": "sell",
                    "status": "canceled",
                    "filled_qty": "0",
                    "filled_avg_price": null,
                    "updated_at": "2025-01-06T14:32:05.000000Z",
                    "canceled_at": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let order_update = get_order_status(&client, order_id).await.unwrap();

        mock.assert();
        assert_eq!(order_update.status, OrderStatus::Cancelled);
        // canceled_at is null so updated_at must be used as the fallback.
        assert_eq!(
            order_update.updated_at,
            "2025-01-06T14:32:05.000000Z"
                .parse::<DateTime<Utc>>()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn get_order_status_cancelled_with_both_timestamps_null_errors() {
        // Branch 3 of terminal_broker_time: both canceled_at and updated_at
        // absent -> IncompleteOrder error rather than silent substitution.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let order_id = "b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e";

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "TSLA",
                    "qty": "50",
                    "side": "sell",
                    "status": "canceled",
                    "filled_qty": "0",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let error = get_order_status(&client, order_id).await.unwrap_err();

        mock.assert();
        assert!(
            matches!(
                error,
                AlpacaBrokerApiError::IncompleteOrder {
                    field: MissingOrderField::CanceledAt,
                    ..
                }
            ),
            "expected IncompleteOrder with CanceledAt, got {error:?}"
        );
    }

    #[tokio::test]
    async fn get_order_status_filled_with_null_filled_at_uses_updated_at() {
        // Branch 2 of terminal_broker_time: filled_at absent but updated_at
        // present -> warn and substitute updated_at.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let order_id = "c3d4e5f6-a7b8-4c9d-0e1f-2a3b4c5d6e7f";

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "TSLA",
                    "qty": "50",
                    "side": "sell",
                    "status": "filled",
                    "filled_qty": "50",
                    "filled_avg_price": "200.00",
                    "updated_at": "2025-01-06T14:32:05.000000Z",
                    "filled_at": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let order_update = get_order_status(&client, order_id).await.unwrap();

        mock.assert();
        assert_eq!(order_update.status, OrderStatus::Filled);
        // filled_at is null so updated_at must be used as the fallback.
        assert_eq!(
            order_update.updated_at,
            "2025-01-06T14:32:05.000000Z"
                .parse::<DateTime<Utc>>()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn get_order_status_filled_with_both_timestamps_null_errors() {
        // Branch 3 of terminal_broker_time: both filled_at and updated_at
        // absent -> IncompleteOrder error rather than silent substitution.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let order_id = "d4e5f6a7-b8c9-4d0e-1f2a-3b4c5d6e7f8a";

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "TSLA",
                    "qty": "50",
                    "side": "sell",
                    "status": "filled",
                    "filled_qty": "50",
                    "filled_avg_price": "200.00"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let error = get_order_status(&client, order_id).await.unwrap_err();

        mock.assert();
        assert!(
            matches!(
                error,
                AlpacaBrokerApiError::IncompleteOrder {
                    field: MissingOrderField::FilledAt,
                    ..
                }
            ),
            "expected IncompleteOrder with FilledAt, got {error:?}"
        );
    }

    #[tokio::test]
    async fn test_get_order_status_rejected() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let order_id = "c7ca82d4-3c95-4f89-9b42-abc123def456";

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "MSFT",
                    "qty": "25",
                    "side": "buy",
                    "status": "rejected",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let order_update = get_order_status(&client, order_id).await.unwrap();

        mock.assert();
        assert_eq!(order_update.order_id, order_id);
        assert_eq!(order_update.status, OrderStatus::Failed);
    }

    #[tokio::test]
    async fn get_order_status_failed_with_failed_at_uses_failed_at() {
        // The Failed arm calls broker_time_or_observation(failed_at.or(updated_at)).
        // When failed_at is present it takes priority over updated_at.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let order_id = "e5f6a7b8-c9d0-4e1f-2a3b-4c5d6e7f8a9b";

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "AAPL",
                    "qty": "25",
                    "side": "buy",
                    "status": "rejected",
                    "filled_qty": "0",
                    "filled_avg_price": null,
                    "failed_at": "2025-01-06T14:32:01.000000Z",
                    "updated_at": "2025-01-06T14:32:05.000000Z"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let order_update = get_order_status(&client, order_id).await.unwrap();

        mock.assert();
        assert_eq!(order_update.status, OrderStatus::Failed);
        // failed_at is present so it must be used, not the later updated_at.
        assert_eq!(
            order_update.updated_at,
            "2025-01-06T14:32:01.000000Z"
                .parse::<DateTime<Utc>>()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn get_order_status_failed_with_null_failed_at_uses_updated_at() {
        // When failed_at is absent, the Failed arm falls back to updated_at
        // via broker_time_or_observation (many rejected/expired payloads omit
        // failed_at and only carry updated_at).
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let order_id = "f6a7b8c9-d0e1-4f2a-3b4c-5d6e7f8a9b0c";

        let mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{order_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": order_id,
                    "symbol": "AAPL",
                    "qty": "25",
                    "side": "buy",
                    "status": "rejected",
                    "filled_qty": "0",
                    "filled_avg_price": null,
                    "failed_at": null,
                    "updated_at": "2025-01-06T14:32:05.000000Z"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let order_update = get_order_status(&client, order_id).await.unwrap();

        mock.assert();
        assert_eq!(order_update.status, OrderStatus::Failed);
        // failed_at is null so updated_at must be used as the fallback.
        assert_eq!(
            order_update.updated_at,
            "2025-01-06T14:32:05.000000Z"
                .parse::<DateTime<Utc>>()
                .unwrap()
        );
    }

    #[test]
    fn classify_broker_status_new_is_submitted_with_no_terminality() {
        assert_eq!(
            classify_broker_status(BrokerOrderStatus::New),
            (OrderStatus::Submitted, None)
        );
    }

    #[test]
    fn classify_broker_status_filled_has_no_terminality() {
        assert_eq!(
            classify_broker_status(BrokerOrderStatus::Filled),
            (OrderStatus::Filled, None)
        );
    }

    #[test]
    fn classify_broker_status_partially_filled_has_no_terminality() {
        assert_eq!(
            classify_broker_status(BrokerOrderStatus::PartiallyFilled),
            (OrderStatus::PartiallyFilled, None)
        );
    }

    #[test]
    fn classify_broker_status_cancelled_has_no_terminality() {
        assert_eq!(
            classify_broker_status(BrokerOrderStatus::Canceled),
            (OrderStatus::Cancelled, None)
        );
    }

    #[test]
    fn classify_broker_status_rejected_is_failed_and_terminal() {
        assert_eq!(
            classify_broker_status(BrokerOrderStatus::Rejected),
            (OrderStatus::Failed, Some(OrderFailureTerminality::Terminal))
        );
    }

    #[test]
    fn classify_broker_status_expired_is_failed_and_terminal() {
        assert_eq!(
            classify_broker_status(BrokerOrderStatus::Expired),
            (OrderStatus::Failed, Some(OrderFailureTerminality::Terminal))
        );
    }

    #[test]
    fn classify_broker_status_done_for_day_is_failed_and_terminal() {
        assert_eq!(
            classify_broker_status(BrokerOrderStatus::DoneForDay),
            (OrderStatus::Failed, Some(OrderFailureTerminality::Terminal))
        );
    }

    #[test]
    fn classify_broker_status_suspended_is_failed_and_not_terminal() {
        assert_eq!(
            classify_broker_status(BrokerOrderStatus::Suspended),
            (
                OrderStatus::Failed,
                Some(OrderFailureTerminality::NotTerminal)
            )
        );
    }

    #[test]
    fn classify_broker_status_replaced_is_failed_and_not_terminal() {
        assert_eq!(
            classify_broker_status(BrokerOrderStatus::Replaced),
            (
                OrderStatus::Failed,
                Some(OrderFailureTerminality::NotTerminal)
            )
        );
    }

    #[test]
    fn classify_broker_status_calculated_is_failed_and_not_terminal() {
        assert_eq!(
            classify_broker_status(BrokerOrderStatus::Calculated),
            (
                OrderStatus::Failed,
                Some(OrderFailureTerminality::NotTerminal)
            )
        );
    }

    /// Pins the whole mapping, one row per `BrokerOrderStatus`. Asserting the
    /// exact pair rather than only the Failed-implies-terminality invariant
    /// also catches a status routed to the wrong non-failure `OrderStatus`,
    /// which carries `None` either way.
    #[test]
    fn classify_broker_status_maps_every_status_to_its_expected_pair() {
        use BrokerOrderStatus::*;
        use OrderFailureTerminality::{NotTerminal, Terminal};

        let expected = [
            (New, OrderStatus::Submitted, None),
            (PendingNew, OrderStatus::Submitted, None),
            (Accepted, OrderStatus::Submitted, None),
            (AcceptedForBidding, OrderStatus::Submitted, None),
            (PendingCancel, OrderStatus::Submitted, None),
            (PendingReplace, OrderStatus::Submitted, None),
            (Stopped, OrderStatus::Submitted, None),
            (PartiallyFilled, OrderStatus::PartiallyFilled, None),
            (Filled, OrderStatus::Filled, None),
            (Canceled, OrderStatus::Cancelled, None),
            (Expired, OrderStatus::Failed, Some(Terminal)),
            (Rejected, OrderStatus::Failed, Some(Terminal)),
            (DoneForDay, OrderStatus::Failed, Some(Terminal)),
            (Replaced, OrderStatus::Failed, Some(NotTerminal)),
            (Suspended, OrderStatus::Failed, Some(NotTerminal)),
            (Calculated, OrderStatus::Failed, Some(NotTerminal)),
        ];

        for (status, expected_status, expected_terminality) in expected {
            assert_eq!(
                classify_broker_status(status),
                (expected_status, expected_terminality),
                "unexpected classification for {status:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_convert_usdc_to_usd() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let client_order_id =
            ClientOrderId::from_uuid(uuid!("11111111-1111-4111-8111-111111111111"));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body(json!({
                    "symbol": "USDCUSD",
                    "qty": "1000.5",
                    "side": "sell",
                    "type": "market",
                    "time_in_force": "gtc",
                    "client_order_id": "11111111-1111-4111-8111-111111111111"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "USDCUSD",
                    "qty": "1000.5",
                    "side": "sell",
                    "status": "filled",
                    "filled_avg_price": "1.0001",
                    "filled_qty": "1000.5",
                    "created_at": "2025-01-06T12:00:00Z"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let order = convert_usdc_usd(
            &client,
            ConversionOrder::SellUsdc(Positive::new(Usdc::new(float!(1000.5))).unwrap()),
            &client_order_id,
        )
        .await
        .unwrap();

        mock.assert();
        assert_eq!(order.id.to_string(), "904837e3-3b76-47ec-b432-046db621571b");
        assert_eq!(order.symbol, "USDCUSD");
        assert!(order.quantity.unwrap().eq(float!(1000.5)).unwrap());
        assert_eq!(order.status_display(), "filled");
    }

    #[tokio::test]
    async fn test_convert_usd_to_usdc() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let client_order_id =
            ClientOrderId::from_uuid(uuid!("22222222-2222-4222-8222-222222222222"));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body(json!({
                    "symbol": "USDCUSD",
                    "notional": "500",
                    "side": "buy",
                    "type": "market",
                    "time_in_force": "gtc",
                    "client_order_id": "22222222-2222-4222-8222-222222222222"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": "USDCUSD",
                    "qty": null,
                    "notional": "500",
                    "side": "buy",
                    "status": "filled",
                    "filled_avg_price": "1.00101001",
                    "filled_qty": "489.700985",
                    "created_at": "2025-01-06T12:30:00Z"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let order = convert_usdc_usd(
            &client,
            ConversionOrder::BuyWithUsd(Positive::new(Usd::new(float!(500))).unwrap()),
            &client_order_id,
        )
        .await
        .unwrap();

        mock.assert();
        assert_eq!(order.id.to_string(), "61e7b016-9c91-4a97-b912-615c9d365c9d");
        assert_eq!(order.symbol, "USDCUSD");
        assert!(
            order
                .filled_quantity
                .unwrap()
                .eq(float!(489.700985))
                .unwrap()
        );
        assert_eq!(order.status_display(), "filled");
    }

    /// A notional order names dollars, so Alpaca answers with `qty: null` and
    /// reports the USDC actually bought in `filled_qty` -- the collar shows up
    /// as the gap between the two (observed on a sandbox account: notional 10
    /// filled 9.794019706 at 1.00101001).
    #[tokio::test]
    async fn notional_conversion_response_carries_no_requested_quantity() {
        let order: CryptoOrderResponse = serde_json::from_value(json!({
            "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
            "symbol": "USDCUSD",
            "qty": null,
            "notional": "10",
            "status": "filled",
            "filled_avg_price": "1.00101001",
            "filled_qty": "9.794019706",
            "created_at": "2025-01-06T12:30:00Z"
        }))
        .unwrap();

        assert!(
            order.quantity.is_none(),
            "a notional order names no quantity, got {:?}",
            order.quantity
        );
        assert!(order.notional.unwrap().eq(float!(10)).unwrap());
        assert!(
            order
                .filled_quantity
                .unwrap()
                .eq(float!(9.794019706))
                .unwrap()
        );
        assert_eq!(order.status_display(), "filled");
    }

    /// Alpaca refuses a notional past two decimals (`422 / 42210000`,
    /// "notional value must be limited to 2 decimal places"), and the transfer
    /// amount routinely carries six when the imbalance excess is what bounds
    /// it. The sub-cent remainder is dropped downwards so the buy can never
    /// ask for more cash than it was sized against.
    #[tokio::test]
    async fn usd_to_usdc_notional_is_truncated_to_whole_cents() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let client_order_id =
            ClientOrderId::from_uuid(uuid!("33333333-3333-4333-8333-333333333333"));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body(json!({
                    "symbol": "USDCUSD",
                    "notional": "5726.78",
                    "side": "buy",
                    "type": "market",
                    "time_in_force": "gtc",
                    "client_order_id": "33333333-3333-4333-8333-333333333333"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "61e7b016-9c91-4a97-b912-615c9d365c9d",
                    "symbol": "USDCUSD",
                    "qty": null,
                    "notional": "5726.78",
                    "side": "buy",
                    "status": "filled",
                    "filled_avg_price": "1.00101001",
                    "filled_qty": "5608.94436",
                    "created_at": "2025-01-06T12:30:00Z"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        convert_usdc_usd(
            &client,
            ConversionOrder::BuyWithUsd(Positive::new(Usd::new(float!(5726.787463))).unwrap()),
            &client_order_id,
        )
        .await
        .unwrap();

        mock.assert();
    }

    #[tokio::test]
    async fn usd_to_usdc_rejects_a_notional_below_one_cent() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        let error = convert_usdc_usd(
            &client,
            ConversionOrder::BuyWithUsd(Positive::new(Usd::new(float!(0.004))).unwrap()),
            &ClientOrderId::from_uuid(uuid!("44444444-4444-4444-8444-444444444444")),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                error,
                AlpacaBrokerApiError::UsdcBelowPrecision { max_decimals, .. }
                    if max_decimals == 2
            ),
            "Expected UsdcBelowPrecision at two decimals, got: {error:?}"
        );
    }

    const STALLED_ORDER_ID: Uuid = uuid!("61e7b016-9c91-4a97-b912-615c9d365c9d");

    fn crypto_order_path() -> String {
        format!(
            "/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders/{STALLED_ORDER_ID}"
        )
    }

    fn stalled_order_body(status: &str, filled_qty: &str) -> serde_json::Value {
        json!({
            "id": STALLED_ORDER_ID.to_string(),
            "symbol": "USDCUSD",
            "qty": "500",
            "side": "buy",
            "status": status,
            "filled_avg_price": "1.0001",
            "filled_qty": filled_qty,
            "created_at": "2025-01-06T12:30:00Z"
        })
    }

    /// Mocks a broker whose order never progresses past `status`, answers the
    /// deadline cancel with `cancel_status`, and after the DELETE arrives
    /// swaps the order to `final_body`. Returns the cancel mock for hit
    /// assertions.
    async fn respond_to_deadline_cancel<'server>(
        server: &'server MockServer,
        status: &str,
        filled_qty: &str,
        cancel_status: u16,
        final_body: serde_json::Value,
    ) -> httpmock::Mock<'server> {
        let mut stalled = server.mock(|when, then| {
            when.method(GET).path(crypto_order_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(stalled_order_body(status, filled_qty));
        });
        let cancel = server.mock(|when, then| {
            when.method(DELETE).path(crypto_order_path());
            then.status(cancel_status);
        });

        // Wait (bounded) for the poll under test to issue the cancel, then
        // swap the stalled snapshot for the settled terminal one. The swap is
        // registration-then-delete so there is never a window where the GET
        // has no matching mock.
        for _ in 0..100 {
            if cancel.calls() >= 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert_eq!(cancel.calls(), 1, "poll never issued the deadline cancel");

        server.mock(|when, then| {
            when.method(GET).path(crypto_order_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(final_body);
        });
        stalled.delete();

        cancel
    }

    /// A poll deadline short enough that the tests reach the cancel almost
    /// immediately, with a settle window wide enough that a mock which answers
    /// the follow-up GET always beats it. Tests of the settle window itself
    /// pass their own.
    const FAST_DEADLINES: ConversionPollDeadlines = ConversionPollDeadlines {
        order: Duration::from_millis(100),
        cancel_settle: Duration::from_secs(30),
        interval: Duration::from_millis(10),
    };

    /// A conversion order that partially fills and then stops progressing must
    /// reach a decision rather than polling indefinitely: past the deadline the
    /// remainder is cancelled and the filled portion is accepted -- downstream
    /// sizes the withdrawal from `filled_qty`, not the requested amount.
    #[tokio::test]
    async fn stalled_partial_fill_is_cancelled_and_the_filled_portion_accepted() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let poll = tokio::spawn(async move {
            let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
            poll_crypto_order_until_filled(&client, STALLED_ORDER_ID, FAST_DEADLINES).await
        });

        let cancel = respond_to_deadline_cancel(
            &server,
            "partially_filled",
            "300",
            204,
            stalled_order_body("canceled", "300"),
        )
        .await;

        let order = tokio::time::timeout(std::time::Duration::from_secs(10), poll)
            .await
            .expect("poll must terminate once the cancel settles")
            .unwrap()
            .unwrap();

        cancel.assert();
        assert!(order.filled_quantity.unwrap().eq(float!(300)).unwrap());
        assert_eq!(
            order.classify(),
            CryptoOrderOutcome::Failed(CryptoOrderFailureReason::Canceled)
        );
    }

    /// A cancelled order that omits `filled_qty` must surface the missing
    /// field, not assume a zero fill: `ConversionTimedOut` asserts nothing
    /// was filled, and an absent field is not evidence of that.
    #[tokio::test]
    async fn cancelled_order_without_filled_qty_reports_the_missing_field() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let poll = tokio::spawn(async move {
            let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
            poll_crypto_order_until_filled(&client, STALLED_ORDER_ID, FAST_DEADLINES).await
        });

        let cancel = respond_to_deadline_cancel(
            &server,
            "new",
            "0",
            204,
            json!({
                "id": STALLED_ORDER_ID.to_string(),
                "symbol": "USDCUSD",
                "qty": "500",
                "side": "buy",
                "status": "canceled",
                "created_at": "2025-01-06T12:30:00Z"
            }),
        )
        .await;

        let error = tokio::time::timeout(std::time::Duration::from_secs(10), poll)
            .await
            .expect("poll must terminate once the cancel settles")
            .unwrap()
            .unwrap_err();

        cancel.assert();
        match error {
            AlpacaBrokerApiError::IncompleteOrder { order_id, field } => {
                assert_eq!(order_id, ExecutorOrderId::new(&STALLED_ORDER_ID));
                assert_eq!(field, MissingOrderField::FilledQty);
            }
            other => panic!("expected IncompleteOrder, got {other:?}"),
        }
    }

    /// The same stall with nothing filled is a timeout failure, not a success
    /// and not an infinite loop.
    #[tokio::test]
    async fn stalled_unfilled_conversion_is_cancelled_and_errors() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let poll = tokio::spawn(async move {
            let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
            poll_crypto_order_until_filled(&client, STALLED_ORDER_ID, FAST_DEADLINES).await
        });

        let cancel = respond_to_deadline_cancel(
            &server,
            "new",
            "0",
            204,
            stalled_order_body("canceled", "0"),
        )
        .await;

        let error = tokio::time::timeout(std::time::Duration::from_secs(10), poll)
            .await
            .expect("poll must terminate once the cancel settles")
            .unwrap()
            .unwrap_err();

        cancel.assert();
        assert!(matches!(
            error,
            AlpacaBrokerApiError::ConversionTimedOut { order_id } if order_id == STALLED_ORDER_ID
        ));
    }

    /// The cancel races the fill: the order goes `filled` between the last
    /// poll and the DELETE, so the settle read observes a fill and the
    /// conversion succeeds instead of being reported as a timeout.
    #[tokio::test]
    async fn conversion_that_fills_while_the_cancel_is_in_flight_succeeds() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let poll = tokio::spawn(async move {
            let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
            poll_crypto_order_until_filled(&client, STALLED_ORDER_ID, FAST_DEADLINES).await
        });

        let cancel = respond_to_deadline_cancel(
            &server,
            "partially_filled",
            "300",
            204,
            stalled_order_body("filled", "500"),
        )
        .await;

        let order = tokio::time::timeout(std::time::Duration::from_secs(10), poll)
            .await
            .expect("poll must terminate once the cancel settles")
            .unwrap()
            .unwrap();

        cancel.assert();
        assert_eq!(order.classify(), CryptoOrderOutcome::Filled);
        assert!(order.filled_quantity.unwrap().eq(float!(500)).unwrap());
    }

    /// Alpaca answers the DELETE with 422 when the order is no longer
    /// cancelable because it already went terminal. That is not a failure:
    /// the settle read observes what it became.
    #[tokio::test]
    async fn cancel_rejected_as_no_longer_cancelable_settles_on_the_terminal_state() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let poll = tokio::spawn(async move {
            let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
            poll_crypto_order_until_filled(&client, STALLED_ORDER_ID, FAST_DEADLINES).await
        });

        let cancel = respond_to_deadline_cancel(
            &server,
            "partially_filled",
            "300",
            422,
            stalled_order_body("filled", "500"),
        )
        .await;

        let order = tokio::time::timeout(std::time::Duration::from_secs(10), poll)
            .await
            .expect("poll must terminate once the cancel settles")
            .unwrap()
            .unwrap();

        cancel.assert();
        assert_eq!(order.classify(), CryptoOrderOutcome::Filled);
    }

    /// A 404 on the DELETE means the broker does not recognise the order id,
    /// so the settle read (same URL) could only 404 too. Resolve it as its own
    /// named condition rather than reading back and surfacing a bare 404.
    #[tokio::test]
    async fn cancel_of_an_unknown_order_reports_the_order_as_missing() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        server.mock(|when, then| {
            when.method(GET).path(crypto_order_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(stalled_order_body("partially_filled", "300"));
        });
        let cancel = server.mock(|when, then| {
            when.method(DELETE).path(crypto_order_path());
            then.status(404);
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let error = poll_crypto_order_until_filled(&client, STALLED_ORDER_ID, FAST_DEADLINES)
            .await
            .unwrap_err();

        cancel.assert();
        assert!(matches!(
            error,
            AlpacaBrokerApiError::ConversionOrderNotFound { order_id }
                if order_id == STALLED_ORDER_ID
        ));
    }

    /// The order goes terminal on a status other than `canceled` after the
    /// cancel: that is a real broker failure and keeps its reason, rather than
    /// being flattened into the timeout error.
    #[tokio::test]
    async fn conversion_that_expires_after_the_cancel_reports_the_broker_reason() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let poll = tokio::spawn(async move {
            let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
            poll_crypto_order_until_filled(&client, STALLED_ORDER_ID, FAST_DEADLINES).await
        });

        let cancel = respond_to_deadline_cancel(
            &server,
            "partially_filled",
            "300",
            204,
            stalled_order_body("expired", "300"),
        )
        .await;

        let error = tokio::time::timeout(std::time::Duration::from_secs(10), poll)
            .await
            .expect("poll must terminate once the cancel settles")
            .unwrap()
            .unwrap_err();

        cancel.assert();
        assert!(matches!(
            error,
            AlpacaBrokerApiError::CryptoOrderFailed {
                order_id,
                reason: CryptoOrderFailureReason::Expired
            } if order_id == STALLED_ORDER_ID
        ));
    }

    /// A cancel the broker never settles must not be recorded as a confirmed
    /// cancellation with nothing filled: the order may still be live, so the
    /// error carries the last observed fill and says so.
    #[tokio::test]
    #[traced_test]
    async fn cancel_that_never_settles_reports_the_order_as_possibly_live() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        server.mock(|when, then| {
            when.method(GET).path(crypto_order_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(stalled_order_body("pending_cancel", "300"));
        });
        let cancel = server.mock(|when, then| {
            when.method(DELETE).path(crypto_order_path());
            then.status(204);
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let error = poll_crypto_order_until_filled(
            &client,
            STALLED_ORDER_ID,
            ConversionPollDeadlines {
                order: Duration::from_millis(50),
                cancel_settle: Duration::from_millis(200),
                interval: Duration::from_millis(25),
            },
        )
        .await
        .unwrap_err();

        cancel.assert();
        match error {
            AlpacaBrokerApiError::ConversionCancelNotSettled {
                order_id,
                cancel,
                filled_quantity,
            } => {
                assert_eq!(order_id, STALLED_ORDER_ID);
                assert_eq!(cancel, DeadlineCancel::Accepted);
                assert!(filled_quantity.unwrap().eq(float!(300)).unwrap());
            }
            other => panic!("expected ConversionCancelNotSettled, got {other:?}"),
        }
    }

    /// A cancel the broker never accepts leaves the remainder live, so it must
    /// be re-issued for the whole settle window rather than abandoned after
    /// one transient failure -- and the persisted reason must not go on to
    /// claim a cancellation that never took effect.
    #[tokio::test]
    #[traced_test]
    async fn cancel_that_keeps_failing_is_retried_and_not_reported_as_cancelled() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        server.mock(|when, then| {
            when.method(GET).path(crypto_order_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(stalled_order_body("partially_filled", "300"));
        });
        let cancel = server.mock(|when, then| {
            when.method(DELETE).path(crypto_order_path());
            then.status(500);
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let error = poll_crypto_order_until_filled(
            &client,
            STALLED_ORDER_ID,
            ConversionPollDeadlines {
                order: Duration::from_millis(50),
                cancel_settle: Duration::from_millis(200),
                interval: Duration::from_millis(25),
            },
        )
        .await
        .unwrap_err();

        assert!(
            cancel.calls() > 1,
            "a failed cancel must be re-issued inside the settle window, got {} attempt(s)",
            cancel.calls()
        );

        // This message is persisted verbatim as the rebalance's failure
        // reason, so it is asserted directly rather than only through the
        // variant that renders it.
        let message = error.to_string();
        assert!(
            message.contains("its remainder was never successfully cancelled"),
            "the reason must not claim a cancellation the broker never accepted, got: {message}"
        );

        match error {
            AlpacaBrokerApiError::ConversionCancelNotSettled {
                order_id,
                cancel,
                filled_quantity,
            } => {
                assert_eq!(order_id, STALLED_ORDER_ID);
                assert_eq!(cancel, DeadlineCancel::Failed);
                assert!(filled_quantity.unwrap().eq(float!(300)).unwrap());
            }
            other => panic!("expected ConversionCancelNotSettled, got {other:?}"),
        }
    }

    /// The broker declining the cancel as no longer cancelable is not a
    /// cancellation either, and is reported as its own answer so an operator
    /// reconciling the order is not told the remainder was withdrawn.
    #[tokio::test]
    async fn cancel_declined_as_uncancelable_is_reported_as_declined() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        server.mock(|when, then| {
            when.method(GET).path(crypto_order_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(stalled_order_body("partially_filled", "300"));
        });
        let cancel = server.mock(|when, then| {
            when.method(DELETE).path(crypto_order_path());
            then.status(422);
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let error = poll_crypto_order_until_filled(
            &client,
            STALLED_ORDER_ID,
            ConversionPollDeadlines {
                order: Duration::from_millis(50),
                cancel_settle: Duration::from_millis(200),
                interval: Duration::from_millis(25),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(
            cancel.calls(),
            1,
            "a declined cancel is refused for a reason that will not change, so it must not \
             be retried"
        );
        match error {
            AlpacaBrokerApiError::ConversionCancelNotSettled { cancel, .. } => {
                assert_eq!(cancel, DeadlineCancel::Declined);
            }
            other => panic!("expected ConversionCancelNotSettled, got {other:?}"),
        }
    }

    /// `DoneForDay` is the one status the conversion poll deliberately reads
    /// differently from `terminality()`: terminal for the Day time-in-force
    /// equity orders, waited on for a `gtc` crypto order Alpaca documents no
    /// meaning for. This pins the override directly -- dropping the
    /// `DoneForDay` arm from `terminal()` would keep every poll test green
    /// while turning a stalled conversion into an immediately recorded
    /// failure.
    #[test]
    fn done_for_day_is_waited_on_by_the_conversion_poll_despite_being_terminal_for_equities() {
        assert_eq!(
            CryptoOrderOutcome::Failed(CryptoOrderFailureReason::DoneForDay).terminal(),
            None
        );
        assert_eq!(
            CryptoOrderFailureReason::DoneForDay.terminality(),
            OrderFailureTerminality::Terminal
        );
    }

    /// `suspended` is a failure status the order can still resume from, so it
    /// must be waited out and resolved by the deadline cancel -- not turned
    /// into an immediate terminal failure while the broker may still fill.
    #[tokio::test]
    async fn resumable_failure_status_is_cancelled_at_the_deadline_not_failed_on_sight() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let poll = tokio::spawn(async move {
            let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
            poll_crypto_order_until_filled(&client, STALLED_ORDER_ID, FAST_DEADLINES).await
        });

        let cancel = respond_to_deadline_cancel(
            &server,
            "suspended",
            "0",
            204,
            stalled_order_body("canceled", "0"),
        )
        .await;

        let error = tokio::time::timeout(std::time::Duration::from_secs(10), poll)
            .await
            .expect("poll must terminate once the cancel settles")
            .unwrap()
            .unwrap_err();

        cancel.assert();
        assert!(matches!(
            error,
            AlpacaBrokerApiError::ConversionTimedOut { order_id } if order_id == STALLED_ORDER_ID
        ));
    }

    /// The deadline must not touch a healthy order: one that is still pending
    /// on the first polls and fills before the deadline is returned as a plain
    /// fill, with no cancel issued.
    #[tokio::test]
    async fn pending_conversion_that_fills_before_the_deadline_is_not_cancelled() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mut pending = server.mock(|when, then| {
            when.method(GET).path(crypto_order_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(stalled_order_body("new", "0"));
        });
        let cancel = server.mock(|when, then| {
            when.method(DELETE).path(crypto_order_path());
            then.status(204);
        });

        let poll = tokio::spawn(async move {
            let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
            poll_crypto_order_until_filled(
                &client,
                STALLED_ORDER_ID,
                ConversionPollDeadlines {
                    order: Duration::from_secs(30),
                    cancel_settle: Duration::from_secs(30),
                    interval: Duration::from_millis(10),
                },
            )
            .await
        });

        for _ in 0..100 {
            if pending.calls() >= 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(pending.calls() >= 1, "poll never read the pending order");

        server.mock(|when, then| {
            when.method(GET).path(crypto_order_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(stalled_order_body("filled", "500"));
        });
        pending.delete();

        let order = tokio::time::timeout(std::time::Duration::from_secs(10), poll)
            .await
            .expect("poll must terminate once the order fills")
            .unwrap()
            .unwrap();

        assert_eq!(cancel.calls(), 0, "a healthy order must not be cancelled");
        assert_eq!(order.classify(), CryptoOrderOutcome::Filled);
    }

    /// The resume-path variant returns the settled terminal order for the
    /// caller to classify instead of erroring, preserving its contract while
    /// still refusing to wait forever.
    #[tokio::test]
    async fn stalled_conversion_resume_poll_returns_the_cancelled_terminal_order() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let poll = tokio::spawn(async move {
            let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
            poll_crypto_order_to_terminal(&client, STALLED_ORDER_ID, FAST_DEADLINES).await
        });

        let cancel = respond_to_deadline_cancel(
            &server,
            "partially_filled",
            "300",
            204,
            stalled_order_body("canceled", "300"),
        )
        .await;

        let order = tokio::time::timeout(std::time::Duration::from_secs(10), poll)
            .await
            .expect("poll must terminate once the cancel settles")
            .unwrap()
            .unwrap();

        cancel.assert();
        assert_eq!(
            order.classify(),
            CryptoOrderOutcome::Failed(CryptoOrderFailureReason::Canceled)
        );
        assert!(order.filled_quantity.unwrap().eq(float!(300)).unwrap());
    }

    #[tokio::test]
    /// The sell names a USDC quantity, so precision past the token's six
    /// decimals is a caller error rather than something to round away. The buy
    /// names dollars and truncates instead -- see
    /// `usd_to_usdc_notional_is_truncated_to_whole_cents`.
    async fn usdc_to_usd_rejects_excess_quantity_precision() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        let error = convert_usdc_usd(
            &client,
            ConversionOrder::SellUsdc(Positive::new(Usdc::new(float!(1000.1234567))).unwrap()),
            &ClientOrderId::from_uuid(Uuid::new_v4()),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                error,
                AlpacaBrokerApiError::UsdcPrecisionExceeded {
                    amount,
                    max_decimals: 6,
                } if amount.eq(float!(1000.1234567)).unwrap()
            ),
            "Expected UsdcPrecisionExceeded error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn truncates_18_decimal_quantity_to_9() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body(json!({
                    "symbol": "RKLB",
                    "qty": "0.996350331",
                    "side": "sell",
                    "type": "market",
                    "time_in_force": "day",
                    "extended_hours": false,
                    "client_order_id": "55555555-5555-4555-8555-555555555555"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "RKLB",
                    "qty": "0.996350331",
                    "side": "sell",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        // Simulate an onchain value with 18 decimal places
        let onchain_shares = Float::parse("0.996350331351928059".to_string()).unwrap();
        let market_order = MarketOrder {
            symbol: Symbol::new("RKLB").unwrap(),
            shares: Positive::new(FractionalShares::new(onchain_shares)).unwrap(),
            direction: Direction::Sell,
            client_order_id: ClientOrderId::from_uuid(uuid!(
                "55555555-5555-4555-8555-555555555555"
            )),
        };

        let placement = place_market_order(&client, market_order, TimeInForce::Day)
            .await
            .unwrap();

        mock.assert();
        assert_eq!(placement.symbol.to_string(), "RKLB");
        assert_eq!(placement.direction, Direction::Sell);
        assert!(
            placement
                .shares
                .inner()
                .inner()
                .eq(float!(0.996350331))
                .unwrap()
        );
    }

    #[tokio::test]
    async fn tiny_shares_below_precision_returns_error() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        let tiny = Float::parse("0.0000000001".to_string()).unwrap();
        let market_order = MarketOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(tiny)).unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
        };

        let err = place_market_order(&client, market_order, TimeInForce::Day)
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                AlpacaBrokerApiError::BelowPrecision {
                    max_decimals,
                    ..
                } if max_decimals == crate::ALPACA_MAX_DECIMAL_PLACES
            ),
            "Expected BelowPrecision error, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn truncates_limit_order_quantity_to_9() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/orders")
                .json_body(json!({
                    "symbol": "RKLB",
                    "qty": "0.996350331",
                    "side": "sell",
                    "type": "limit",
                    "limit_price": "17.45",
                    "time_in_force": "day",
                    "extended_hours": false,
                    "client_order_id": "77777777-7777-4777-8777-777777777777"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "904837e3-3b76-47ec-b432-046db621571b",
                    "symbol": "RKLB",
                    "qty": "0.996350331",
                    "side": "sell",
                    "status": "new",
                    "filled_avg_price": null
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let limit_order = AlpacaLimitOrder {
            symbol: Symbol::new("RKLB").unwrap(),
            shares: Positive::new(FractionalShares::new(
                Float::parse("0.996350331351928059".to_string()).unwrap(),
            ))
            .unwrap(),
            direction: Direction::Sell,
            limit_price: AlpacaLimitPrice::try_new(Positive::new(Usd::new(float!(17.45))).unwrap())
                .unwrap(),
            extended_hours: false,
            client_order_id: ClientOrderId::from_uuid(uuid!(
                "77777777-7777-4777-8777-777777777777"
            )),
        };

        let placement = place_limit_order(&client, limit_order).await.unwrap();

        mock.assert();
        assert!(
            placement
                .shares
                .inner()
                .inner()
                .eq(float!(0.996350331))
                .unwrap()
        );
    }

    #[tokio::test]
    async fn tiny_limit_order_shares_below_precision_returns_error() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));
        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();

        let limit_order = AlpacaLimitOrder {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(
                Float::parse("0.0000000001".to_string()).unwrap(),
            ))
            .unwrap(),
            direction: Direction::Buy,
            limit_price: AlpacaLimitPrice::try_new(
                Positive::new(Usd::new(float!(195.25))).unwrap(),
            )
            .unwrap(),
            extended_hours: false,
            client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
        };

        let err = place_limit_order(&client, limit_order).await.unwrap_err();

        assert!(
            matches!(
                err,
                AlpacaBrokerApiError::BelowPrecision {
                    max_decimals,
                    ..
                } if max_decimals == crate::ALPACA_MAX_DECIMAL_PLACES
            ),
            "Expected BelowPrecision error, got: {err:?}"
        );
    }

    #[test]
    fn test_crypto_order_response_status_display() {
        let make_order = |status: BrokerOrderStatus| CryptoOrderResponse {
            id: Uuid::new_v4(),
            symbol: "USDCUSD".to_string(),
            quantity: Some(float!(100)),
            notional: None,
            status,
            filled_average_price: None,
            filled_quantity: None,
            created_at: Utc::now(),
        };

        assert_eq!(
            make_order(BrokerOrderStatus::Filled).status_display(),
            "filled"
        );
        assert_eq!(make_order(BrokerOrderStatus::New).status_display(), "new");
        assert_eq!(
            make_order(BrokerOrderStatus::Rejected).status_display(),
            "rejected"
        );
        assert_eq!(
            make_order(BrokerOrderStatus::Canceled).status_display(),
            "canceled"
        );
    }

    fn api_error(status: StatusCode, message: impl Into<String>) -> AlpacaBrokerApiError {
        AlpacaBrokerApiError::ApiError {
            status,
            alpaca_code: None,
            message: message.into(),
            retry_after: None,
        }
    }

    proptest! {
        #[test]
        fn duplicate_client_order_id_detected_for_422_unique_violation(
            prefix in "\\PC*",
            suffix in "\\PC*",
        ) {
            let message = format!("{prefix}client_order_id must be unique{suffix}");
            let error = api_error(StatusCode::UNPROCESSABLE_ENTITY, message);
            prop_assert!(is_duplicate_client_order_id(&error));
        }

        #[test]
        fn duplicate_client_order_id_rejects_non_422_status(
            status_code in 100u16..600u16,
            message in "\\PC*",
        ) {
            prop_assume!(status_code != StatusCode::UNPROCESSABLE_ENTITY.as_u16());
            let status = StatusCode::from_u16(status_code)
                .expect("status codes in 100..600 are valid HTTP codes");
            let error = api_error(status, message);
            prop_assert!(!is_duplicate_client_order_id(&error));
        }

        #[test]
        fn duplicate_client_order_id_rejects_422_without_unique_message(
            message in prop::string::string_regex("([^\n]|\\n)*").unwrap(),
        ) {
            prop_assume!(!message.contains("client_order_id must be unique"));
            let error = api_error(StatusCode::UNPROCESSABLE_ENTITY, message);
            prop_assert!(!is_duplicate_client_order_id(&error));
        }
    }
}
