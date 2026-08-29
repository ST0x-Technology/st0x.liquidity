use chrono::{NaiveDate, NaiveTime};
use rain_math_float::Float;
use rain_math_float::FloatError;
use serde::Deserialize;
use st0x_float_serde::format_float_with_fallback;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

use crate::alpaca_market_data::AlpacaMarketDataError;
use crate::{
    Backpressure, ClientOrderId, CounterTradeCostError, ExecutorOrderId, FractionalShares,
    OrderFailureTerminality, Permanence, Positive, Symbol, Usd,
};

/// Time-in-force specifies how long an order remains active before it expires.
///
/// This is specific to Alpaca Broker API and configurable at the executor level.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    /// Day order - expires at the end of the regular trading day
    #[default]
    Day,
    /// Market-on-close - executes at or near the market close price.
    /// Orders placed between 3:50pm-7:00pm ET are rejected.
    /// Orders after 7pm ET are queued for the next trading day.
    MarketOnClose,
}

mod activity;
mod auth;
mod client;
mod executor;
mod journal;
pub(crate) mod kms_jwt;
mod market_hours;
#[cfg(feature = "mock")]
mod mock_api;
#[cfg(feature = "mock")]
pub use mock_api::{
    AlpacaBrokerMock, MockMode, MockOrderSnapshot, MockPosition, MockPositionSnapshot,
    MockWalletTransferSnapshot, OrderSide, OrderStatus, TEST_ACCOUNT_ID, TEST_API_KEY,
    TEST_API_SECRET, TransferDirection, TransferFlow, TransferStatus, WhitelistStatus,
};
mod order;
mod overnight_eligibility;
mod positions;

/// Asset status from Alpaca Broker API (public because it's exposed in error types)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AssetStatus {
    Active,
    Inactive,
}

pub use activity::{AccountActivitiesQuery, AccountActivity};
pub use auth::{
    AccountStatus, AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, AlpacaBrokerAuth,
};
// Exposed as the single source of truth for the broker HTTP request timeout so
// timing-sensitive integration tests derive their boundaries from it.
pub use client::HTTP_REQUEST_TIMEOUT;

// Crate-visible so the market-data module can build authenticated
// requests through the client instead of reaching for raw reqwest.
pub(crate) use client::AlpacaBrokerApiClient;

// `AssetDetails` is the CLI's overnight/fractional asset-inspection surface.
pub use executor::{AlpacaBrokerApi, AssetDetails};
// The overnight eligibility gate: consumed by the CLI today and the
// automated hedge path (RAI-1951) next.
pub use journal::{JournalResponse, JournalStatus};
pub use kms_jwt::{ALPACA_TOKEN_URL, AuthRuntime, KmsJwtError};
pub use order::{
    AlpacaLimitOrder, AlpacaLimitPrice, ConversionDirection, ConversionOrder, CryptoOrderOutcome,
    CryptoOrderResponse, OvernightLimitOrder, OvernightOrderError, ParseAlpacaLimitPriceError,
};
pub use overnight_eligibility::{
    EligibilitySnapshot, EligibilitySnapshots, EligibilitySyncError, OvernightEligibilityError,
    OvernightOrderShape, eligibility_sync_window_start, next_eligibility_sync_at, sync_eligibility,
    validate_overnight_eligibility,
};

impl fmt::Display for CryptoOrderFailureReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Canceled => formatter.write_str("Canceled"),
            Self::Expired => formatter.write_str("Expired"),
            Self::Rejected => formatter.write_str("Rejected"),
            Self::DoneForDay => formatter.write_str("DoneForDay"),
            Self::Replaced => formatter.write_str("Replaced"),
            Self::Suspended => formatter.write_str("Suspended"),
            Self::Calculated => formatter.write_str("Calculated"),
        }
    }
}

impl fmt::Display for TimeInForce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Day => write!(f, "day"),
            Self::MarketOnClose => write!(f, "market-on-close"),
        }
    }
}

#[derive(Debug, Error)]
#[error("invalid time-in-force: {time_in_force_provided}")]
pub struct ParseTimeInForceError {
    time_in_force_provided: String,
}

impl FromStr for TimeInForce {
    type Err = ParseTimeInForceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "day" => Ok(Self::Day),
            "market-on-close" | "market_on_close" | "cls" => Ok(Self::MarketOnClose),
            _ => Err(ParseTimeInForceError {
                time_in_force_provided: value.to_string(),
            }),
        }
    }
}

impl TimeInForce {
    /// Returns the API string representation for this time-in-force value.
    pub(crate) fn as_api_str(self) -> &'static str {
        match self {
            Self::Day => "day",
            Self::MarketOnClose => "cls",
        }
    }
}

/// Terminal failure states for crypto orders.
///
/// Every non-fill terminal `BrokerOrderStatus` maps to one of these so the
/// conversion resume path never treats an unexpected terminal status as
/// still-pending (which would retry forever).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CryptoOrderFailureReason {
    Canceled,
    Expired,
    Rejected,
    DoneForDay,
    Replaced,
    Suspended,
    Calculated,
}

impl CryptoOrderFailureReason {
    /// Whether the order can still resume or fill after reporting this
    /// failure, per Alpaca's order lifecycle
    /// (https://docs.alpaca.markets/docs/orders-at-alpaca).
    ///
    /// The single source for both terminality decisions in this crate: whether
    /// a caller may release its idempotency key (`classify_broker_status`) and
    /// whether a conversion poll may stop waiting. Deriving both from here is
    /// what keeps them from drifting apart -- a `Replaced`/`Suspended`/
    /// `Calculated` order that is treated as terminal is a rebalance recorded
    /// as failed while the broker may still move real money.
    ///
    /// `DoneForDay` is terminal because every equity order this bot places is
    /// Day time-in-force, so it cannot resume in a later session. That
    /// precondition does not hold for the `gtc` conversion order, and Alpaca
    /// documents no meaning for the status on a 24/7 crypto pair, so the
    /// conversion poll overrides it (`CryptoOrderOutcome::terminal`) rather
    /// than declare an order dead that may still fill.
    fn terminality(self) -> OrderFailureTerminality {
        match self {
            Self::Canceled | Self::Expired | Self::Rejected | Self::DoneForDay => {
                OrderFailureTerminality::Terminal
            }
            Self::Replaced | Self::Suspended | Self::Calculated => {
                OrderFailureTerminality::NotTerminal
            }
        }
    }
}

/// A field absent from a broker order response that the reported order
/// status requires.
///
/// A closed enum (not a string) so call sites and tests cannot drift on
/// spelling and new fields force exhaustive handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingOrderField {
    FilledQty,
    Price,
    FilledAt,
    CanceledAt,
    FailureTerminality,
}

/// How the broker answered the cancel issued when a conversion order stalls
/// past its deadline.
///
/// Carried into [`AlpacaBrokerApiError::ConversionCancelNotSettled`] so the
/// persisted failure reason states what actually happened to the remainder. A
/// cancel that was never accepted leaves it live, which is a materially
/// different reconciliation than one the broker took and simply never
/// reported settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadlineCancel {
    /// The broker accepted the cancel request.
    Accepted,
    /// The broker declined it as no longer cancelable (422), so no cancel took
    /// effect and the order was already leaving the cancelable states.
    Declined,
    /// Every cancel request failed, so the remainder was never cancelled.
    Failed,
}

impl DeadlineCancel {
    /// The clause naming what became of the remainder, so a failure message
    /// never claims a cancellation the broker did not accept.
    fn clause(self) -> &'static str {
        match self {
            Self::Accepted => "its remainder was cancelled",
            Self::Declined => "the broker declined to cancel its remainder as no longer cancelable",
            Self::Failed => "its remainder was never successfully cancelled",
        }
    }
}

/// The real Alpaca 422 body for a re-used `client_order_id`: a numeric
/// `code` alongside the message. Shared by `order.rs`'s unit tests and the
/// `mock_api` E2E mock so both speak this exact shape instead of two
/// fixtures drifting apart (a code-less mock would let an `alpaca_code`
/// parsing regression pass unnoticed). `#[cfg(any(test, feature = "mock"))]`
/// covers both consumers: `order.rs`'s tests compile under plain `cfg(test)`,
/// `mock_api` only under the `mock` feature.
#[cfg(any(test, feature = "mock"))]
pub(crate) fn duplicate_client_order_id_body() -> serde_json::Value {
    serde_json::json!({
        "code": 40_010_001,
        "message": "client_order_id must be unique",
    })
}

#[derive(Debug, Error)]
pub enum AlpacaBrokerApiError {
    #[error("HTTP client error: {0}")]
    HttpClient(#[from] reqwest::Error),

    #[error("Failed to parse Alpaca API response: {0}")]
    JsonParse(#[from] serde_json::Error),

    #[error(
        "position endpoint returned {returned} when {requested} was requested; refusing to price \
         from another symbol"
    )]
    PositionSymbolMismatch { requested: Symbol, returned: Symbol },

    #[error("Invalid header value: {0}")]
    InvalidHeader(#[from] reqwest::header::InvalidHeaderValue),

    #[error("keyless Alpaca auth failed: {0}")]
    KmsJwt(#[from] kms_jwt::KmsJwtError),

    #[error("{}", format_api_error(*status, alpaca_code.as_ref(), message))]
    ApiError {
        status: reqwest::StatusCode,
        /// Alpaca error code (e.g., 40310000 for PDT restriction)
        alpaca_code: Option<u64>,
        /// Human-readable error message from Alpaca
        message: String,
        /// Parsed `Retry-After` header from the response, when the broker
        /// sent one. Only meaningful when `status == 429 Too Many Requests`;
        /// captured unconditionally regardless of status since it costs
        /// nothing to carry and keeps `parse_api_error` a single call site.
        retry_after: Option<Duration>,
    },

    #[error("Invalid order ID: {0}")]
    InvalidOrderId(#[from] uuid::Error),

    #[error("Order {order_id} is missing required field {field:?} for its reported state")]
    IncompleteOrder {
        order_id: ExecutorOrderId,
        field: MissingOrderField,
    },

    #[error("Order {order_id} reports filled quantity {filled} for ordered quantity {ordered}")]
    FilledQuantityMismatch {
        order_id: ExecutorOrderId,
        ordered: Positive<FractionalShares>,
        filled: Positive<FractionalShares>,
    },

    #[error("Account {account_id} is not active (status: {status:?})")]
    AccountNotActive {
        account_id: Uuid,
        status: AccountStatus,
    },

    #[error("Crypto order {order_id} failed: {reason}")]
    CryptoOrderFailed {
        order_id: Uuid,
        reason: CryptoOrderFailureReason,
    },

    #[error(
        "Conversion order {order_id} stayed non-terminal past its deadline with nothing \
         filled; the remainder was cancelled"
    )]
    ConversionTimedOut { order_id: Uuid },

    /// The deadline cancel did not resolve: the broker never reported the
    /// order terminal. Kept separate from [`Self::ConversionTimedOut`] because
    /// nothing here is confirmed: the order may still be live, and the fill
    /// quantity is only the last value observed before giving up. The message
    /// is persisted verbatim as the rebalance's failure reason, so it must not
    /// claim a cancellation or a zero fill that was never verified -- which is
    /// why it reports how the broker answered the cancel rather than assuming
    /// one took effect.
    #[error(
        "Conversion order {order_id} never reported a terminal state and may still be live \
         at the broker: {}, with {} filled when last observed -- manual reconciliation \
         required",
        .cancel.clause(),
        .filled_quantity
            .as_ref()
            .map_or_else(|| "an unreported quantity".to_string(), format_float_with_fallback)
    )]
    ConversionCancelNotSettled {
        order_id: Uuid,
        cancel: DeadlineCancel,
        filled_quantity: Option<Float>,
    },

    /// The broker answered the deadline cancel with a 404. Reading the order
    /// back would hit the same id and 404 again, so its fill state cannot be
    /// observed at all.
    #[error(
        "Conversion order {order_id} was not recognised by the broker when cancelling its \
         stalled remainder; its fill state cannot be read back -- manual reconciliation \
         required"
    )]
    ConversionOrderNotFound { order_id: Uuid },

    #[error(
        "Broker rejected client_order_id {client_order_id} as a duplicate (422) but no \
         order with that id was found on lookup; broker state is inconsistent and the \
         placement must be retried"
    )]
    DuplicateOrderNotFound { client_order_id: ClientOrderId },

    #[error("Internal error: calendar was non-empty but iteration returned None")]
    CalendarIterationInvariantViolation,

    #[error(
        "Calendar endpoint returned an entry for {returned} when {queried} was \
         requested; refusing to classify the market session from another day's hours"
    )]
    CalendarDateMismatch {
        queried: NaiveDate,
        returned: NaiveDate,
    },

    #[error(
        "Calendar endpoint returned a local market time {date} {time} that cannot be resolved \
         to a single UTC instant -- either ambiguous (DST fall-back) or nonexistent (DST \
         spring-forward)"
    )]
    CalendarLocalTimeUnresolvable { date: NaiveDate, time: NaiveTime },

    #[error("Invalid Alpaca account activities URL {url}")]
    InvalidAccountActivitiesUrl {
        url: String,
        #[source]
        source: url::ParseError,
    },

    #[error("Alpaca account activities pagination returned the same page token twice")]
    AccountActivitiesPaginationInvariantViolation,

    #[error("Alpaca account activities pagination exceeded {pages} pages")]
    AccountActivitiesPageLimitExceeded { pages: usize },

    #[error("Asset {symbol} is not active (status: {status:?})")]
    AssetNotActive { symbol: Symbol, status: AssetStatus },

    #[error("Asset {symbol} is not tradable on Alpaca")]
    AssetNotTradable { symbol: Symbol },

    #[error(
        "Limit price {limit_price} exceeds Alpaca's \
         {max_decimals}-decimal-place precision for this price range"
    )]
    InvalidLimitPricePrecision {
        limit_price: Positive<Usd>,
        max_decimals: u8,
    },

    #[error("USD balance {} cannot be converted to cents", format_float_with_fallback(.0))]
    UsdBalanceConversion(Float),

    #[error("Cash balance {} has fractional cents after conversion", format_float_with_fallback(.0))]
    FractionalCents(Float),

    #[error("Invalid symbol in position: {0}")]
    InvalidSymbol(#[from] crate::EmptySymbolError),

    #[error("Alpaca USDCUSD position is missing the required total quantity (qty) field")]
    MissingPositionQuantity,

    #[error(
        "Order quantity {shares} is below Alpaca's \
         {max_decimals}-decimal-place precision"
    )]
    BelowPrecision {
        shares: Positive<FractionalShares>,
        max_decimals: u8,
    },

    #[error(
        "USDC conversion amount {} is below Alpaca's \
         {max_decimals}-decimal-place precision",
        format_float_with_fallback(.amount)
    )]
    UsdcBelowPrecision { amount: Float, max_decimals: u8 },

    #[error(
        "USDC conversion amount {} exceeds Alpaca's \
         {max_decimals}-decimal-place precision",
        format_float_with_fallback(.amount)
    )]
    UsdcPrecisionExceeded { amount: Float, max_decimals: u8 },

    #[error(transparent)]
    NotPositive(#[from] st0x_finance::NotPositive<FractionalShares>),

    #[error(transparent)]
    NotPositiveLimitPrice(#[from] st0x_finance::NotPositive<Usd>),

    #[error("Float conversion error: {0}")]
    FloatConversion(#[from] FloatError),
    // Boxed to keep `AlpacaBrokerApiError` inside clippy's large-`Err`
    // budget: two variants carrying `AlpacaMarketDataError` inline push
    // every `Result` in the crate's call graph over the threshold.
    #[error("latest trade lookup failed: {0}")]
    LatestTrade(#[source] Box<AlpacaMarketDataError>),
    #[error("latest quote lookup failed: {0}")]
    LatestQuote(#[source] Box<AlpacaMarketDataError>),
    #[error("counter-trade cost estimation failed: {0}")]
    CounterTradeCost(#[from] CounterTradeCostError),
}

fn format_api_error(
    status: reqwest::StatusCode,
    alpaca_code: Option<&u64>,
    message: &str,
) -> String {
    alpaca_code.map_or_else(
        || format!("Alpaca API error ({status}): {message}"),
        |code| format!("Alpaca API error {code} ({status}): {message}"),
    )
}

impl AlpacaBrokerApiError {
    /// Classifies this error as broker rate-limiting (HTTP 429), returning
    /// its `Retry-After` hint when the broker sent one. Every other variant
    /// returns `None` -- an exhaustive match so a new variant added later
    /// forces a conscious decision here rather than silently classifying as
    /// "not backpressure".
    ///
    /// The bare-429 assumption is not a guess: it is the classification
    /// RAI-1492's actual incident (a `PollOrderStatus` job's persisted
    /// `last_result`) recorded, and matches RFC 6585's standard status code
    /// for rate limiting that Alpaca (like virtually every REST API) uses.
    /// The `Retry-After` hint carried alongside it is a separate, softer
    /// assumption -- see `rate_limit::parse_retry_after`'s doc comment and
    /// its fixture test for that one, since Alpaca's own SDKs do not trust
    /// the header even when present.
    pub fn backpressure(&self) -> Option<Backpressure> {
        match self {
            Self::ApiError {
                status,
                retry_after,
                ..
            } if *status == reqwest::StatusCode::TOO_MANY_REQUESTS => Some(Backpressure {
                retry_after: *retry_after,
            }),

            // A rate-limited token mint throttles every keyless call at
            // once; surface it with its Retry-After hint.
            Self::KmsJwt(error) if error.is_rate_limited() => Some(Backpressure {
                retry_after: error.retry_after(),
            }),

            Self::ApiError { .. }
            | Self::HttpClient(_)
            | Self::KmsJwt(_)
            | Self::JsonParse(_)
            | Self::PositionSymbolMismatch { .. }
            | Self::InvalidHeader(_)
            | Self::InvalidOrderId(_)
            | Self::IncompleteOrder { .. }
            | Self::FilledQuantityMismatch { .. }
            | Self::AccountNotActive { .. }
            | Self::CryptoOrderFailed { .. }
            | Self::ConversionTimedOut { .. }
            | Self::ConversionCancelNotSettled { .. }
            | Self::ConversionOrderNotFound { .. }
            | Self::DuplicateOrderNotFound { .. }
            | Self::CalendarIterationInvariantViolation
            | Self::CalendarDateMismatch { .. }
            | Self::CalendarLocalTimeUnresolvable { .. }
            | Self::InvalidAccountActivitiesUrl { .. }
            | Self::AccountActivitiesPaginationInvariantViolation
            | Self::AccountActivitiesPageLimitExceeded { .. }
            | Self::AssetNotActive { .. }
            | Self::AssetNotTradable { .. }
            | Self::InvalidLimitPricePrecision { .. }
            | Self::UsdBalanceConversion(_)
            | Self::FractionalCents(_)
            | Self::InvalidSymbol(_)
            | Self::MissingPositionQuantity
            | Self::BelowPrecision { .. }
            | Self::UsdcBelowPrecision { .. }
            | Self::UsdcPrecisionExceeded { .. }
            | Self::NotPositive(_)
            | Self::NotPositiveLimitPrice(_)
            | Self::FloatConversion(_)
            | Self::CounterTradeCost(_) => None,

            // Delegate rather than returning `None`: `find_backpressure`'s
            // chain-walk downcasts `AlpacaBrokerApiError` before it ever
            // reaches the wrapped `AlpacaMarketDataError`, so classifying
            // here means a 429 from `fetch_latest_trade_price` or
            // `fetch_latest_quote` is caught at this first hop instead of
            // relying on a second, separate `AlpacaMarketDataError`
            // downcast one level further down the chain.
            Self::LatestTrade(source) | Self::LatestQuote(source) => source.backpressure(),
        }
    }

    /// Classifies whether an immediate retry of the same request can
    /// plausibly succeed, so a caller can tell a rejection it must stop
    /// re-sending from a blip it should re-send within the second. Delegates
    /// for the wrapped market-data variants for the same reason
    /// [`Self::backpressure`] does.
    pub fn permanence(&self) -> Permanence {
        match self {
            Self::ApiError { status, .. } => crate::status_permanence(*status),

            // Request-builder failures are deterministic for the same inputs.
            // Once a request is built, connect failures, resets, and the
            // client's own request timeout are transient. A single-symbol
            // endpoint returning another symbol is likewise an upstream
            // routing/cache failure: a fresh request can clear it, but the
            // mismatched financial value must never be consumed.
            Self::HttpClient(source) if source.is_builder() => Permanence::Permanent,
            // A deterministic mint failure (revoked signerVerifier grant,
            // disabled BrokerDash credential: 4xx from KMS or the token
            // endpoint) fails identically on every retry, exactly like a
            // Basic-auth 401/403; everything else about a mint is
            // network-shaped and retryable.
            Self::KmsJwt(error) if error.is_deterministic() => Permanence::Permanent,
            Self::HttpClient(_) | Self::KmsJwt(_) | Self::PositionSymbolMismatch { .. } => {
                Permanence::Transient
            }

            // Everything else is decided locally -- from a response that
            // already arrived, from configuration, or from arithmetic on
            // values in hand -- so the same inputs fail the same way.
            Self::JsonParse(_)
            | Self::InvalidHeader(_)
            | Self::InvalidOrderId(_)
            | Self::IncompleteOrder { .. }
            | Self::FilledQuantityMismatch { .. }
            | Self::AccountNotActive { .. }
            | Self::CryptoOrderFailed { .. }
            | Self::DuplicateOrderNotFound { .. }
            | Self::CalendarIterationInvariantViolation
            | Self::CalendarDateMismatch { .. }
            | Self::CalendarLocalTimeUnresolvable { .. }
            | Self::InvalidAccountActivitiesUrl { .. }
            | Self::AccountActivitiesPaginationInvariantViolation
            | Self::AccountActivitiesPageLimitExceeded { .. }
            | Self::AssetNotActive { .. }
            | Self::AssetNotTradable { .. }
            | Self::InvalidLimitPricePrecision { .. }
            | Self::UsdBalanceConversion(_)
            | Self::FractionalCents(_)
            | Self::InvalidSymbol(_)
            | Self::MissingPositionQuantity
            | Self::BelowPrecision { .. }
            | Self::UsdcBelowPrecision { .. }
            | Self::UsdcPrecisionExceeded { .. }
            | Self::NotPositive(_)
            | Self::NotPositiveLimitPrice(_)
            | Self::FloatConversion(_)
            | Self::CounterTradeCost(_)
            // The conversion trio are not request failures but concluded
            // poll outcomes: the deadline ran out, or the cancel's effect
            // could not be read back. An immediate re-send cannot clear
            // them, and for the latter two the original order may still be
            // live, so re-sending is exactly the double-conversion the
            // resume path exists to prevent.
            | Self::ConversionTimedOut { .. }
            | Self::ConversionCancelNotSettled { .. }
            | Self::ConversionOrderNotFound { .. } => Permanence::Permanent,

            Self::LatestTrade(source) | Self::LatestQuote(source) => source.permanence(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backpressure_some_for_429_with_retry_after() {
        let error = AlpacaBrokerApiError::ApiError {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            alpaca_code: None,
            message: "rate limited".to_string(),
            retry_after: Some(Duration::from_secs(20)),
        };

        assert_eq!(
            error.backpressure(),
            Some(Backpressure {
                retry_after: Some(Duration::from_secs(20))
            })
        );
    }

    #[test]
    fn backpressure_some_with_none_retry_after_for_429_without_header() {
        let error = AlpacaBrokerApiError::ApiError {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            alpaca_code: None,
            message: "rate limited".to_string(),
            retry_after: None,
        };

        assert_eq!(
            error.backpressure(),
            Some(Backpressure { retry_after: None })
        );
    }

    #[test]
    fn backpressure_none_for_non_429_api_error() {
        let error = AlpacaBrokerApiError::ApiError {
            status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            alpaca_code: None,
            message: "boom".to_string(),
            retry_after: None,
        };

        assert_eq!(error.backpressure(), None);
    }

    #[test]
    fn backpressure_none_for_a_non_api_error_variant() {
        let error = AlpacaBrokerApiError::MissingPositionQuantity;

        assert_eq!(error.backpressure(), None);
    }

    fn api_error(status: reqwest::StatusCode) -> AlpacaBrokerApiError {
        AlpacaBrokerApiError::ApiError {
            status,
            alpaca_code: None,
            message: "boom".to_string(),
            retry_after: None,
        }
    }

    #[test]
    fn permanence_permanent_for_a_403() {
        assert_eq!(
            api_error(reqwest::StatusCode::FORBIDDEN).permanence(),
            Permanence::Permanent
        );
    }

    #[test]
    fn permanence_transient_for_a_500() {
        assert_eq!(
            api_error(reqwest::StatusCode::INTERNAL_SERVER_ERROR).permanence(),
            Permanence::Transient
        );
    }

    /// 429 is the one 4xx that clears on its own; `backpressure()` already
    /// routes it to a reschedule, and it must never read as permanent if it
    /// reaches this classification some other way.
    #[test]
    fn permanence_transient_for_a_429() {
        assert_eq!(
            api_error(reqwest::StatusCode::TOO_MANY_REQUESTS).permanence(),
            Permanence::Transient
        );
    }

    /// The transport variant every non-market-data broker call fails with
    /// when the network drops. Driven through a real `reqwest::Error` rather
    /// than a constructed one, since the type has no public constructor.
    #[tokio::test]
    async fn permanence_transient_for_a_transport_failure() {
        // Port 1 is reserved and never listening, so this is a genuine
        // connect failure carried in a real `reqwest::Error`.
        let transport = reqwest::Client::new()
            .get("http://127.0.0.1:1/v1/trading/accounts")
            .send()
            .await
            .expect_err("connecting to a closed port must fail");

        assert_eq!(
            AlpacaBrokerApiError::from(transport).permanence(),
            Permanence::Transient
        );
    }

    #[test]
    fn permanence_permanent_for_a_request_builder_failure() {
        let builder = reqwest::Client::new()
            .get("not a valid URL")
            .build()
            .expect_err("an invalid URL must fail while building the request");

        assert!(builder.is_builder());
        assert_eq!(
            AlpacaBrokerApiError::from(builder).permanence(),
            Permanence::Permanent
        );
    }

    #[test]
    fn permanence_transient_for_a_position_symbol_mismatch() {
        assert_eq!(
            AlpacaBrokerApiError::PositionSymbolMismatch {
                requested: Symbol::new("AAPL").unwrap(),
                returned: Symbol::new("TSLA").unwrap(),
            }
            .permanence(),
            Permanence::Transient
        );
    }

    #[test]
    fn permanence_permanent_for_a_locally_decided_variant() {
        assert_eq!(
            AlpacaBrokerApiError::MissingPositionQuantity.permanence(),
            Permanence::Permanent
        );
    }

    /// The real wrapping shape of a market-data failure: classification must
    /// come from the wrapped error, not from the wrapper's own variant.
    #[test]
    fn permanence_delegates_through_a_wrapped_market_data_error() {
        let transient =
            AlpacaBrokerApiError::LatestQuote(Box::new(AlpacaMarketDataError::ApiError {
                status: reqwest::StatusCode::BAD_GATEWAY,
                body: "upstream down".to_string(),
                retry_after: None,
            }));
        let permanent =
            AlpacaBrokerApiError::LatestTrade(Box::new(AlpacaMarketDataError::ApiError {
                status: reqwest::StatusCode::FORBIDDEN,
                body: "subscription does not permit querying recent SIP data".to_string(),
                retry_after: None,
            }));

        assert_eq!(transient.permanence(), Permanence::Transient);
        assert_eq!(permanent.permanence(), Permanence::Permanent);
    }
}
