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
    Positive, Symbol, Usd,
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
mod positions;

/// Asset status from Alpaca Broker API (public because it's exposed in error types)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AssetStatus {
    Active,
    Inactive,
}

pub use activity::{AccountActivitiesQuery, AccountActivity};
pub use auth::{AccountStatus, AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode};
// Exposed as the single source of truth for the broker HTTP request timeout so
// timing-sensitive integration tests derive their boundaries from it.
pub use client::HTTP_REQUEST_TIMEOUT;
pub use executor::AlpacaBrokerApi;
pub use journal::{JournalResponse, JournalStatus};
pub use order::{
    AlpacaLimitOrder, AlpacaLimitPrice, ConversionDirection, CryptoOrderOutcome,
    CryptoOrderResponse, ParseAlpacaLimitPriceError,
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
}

#[derive(Debug, Error)]
pub enum AlpacaBrokerApiError {
    #[error("HTTP client error: {0}")]
    HttpClient(#[from] reqwest::Error),

    #[error("Failed to parse Alpaca API response: {0}")]
    JsonParse(#[from] serde_json::Error),

    #[error("Invalid header value: {0}")]
    InvalidHeader(#[from] reqwest::header::InvalidHeaderValue),

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
    #[error("latest trade lookup failed: {0}")]
    LatestTrade(#[from] AlpacaMarketDataError),
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

            Self::ApiError { .. }
            | Self::HttpClient(_)
            | Self::JsonParse(_)
            | Self::InvalidHeader(_)
            | Self::InvalidOrderId(_)
            | Self::IncompleteOrder { .. }
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
            | Self::CounterTradeCost(_) => None,

            // Delegate rather than returning `None`: `find_backpressure`'s
            // chain-walk downcasts `AlpacaBrokerApiError` before it ever
            // reaches the wrapped `AlpacaMarketDataError`, so classifying
            // here means a 429 from `fetch_latest_trade_price` (e.g. the
            // extended-hours limit-price lookup) is caught at this first
            // hop instead of relying on a second, separate
            // `AlpacaMarketDataError` downcast one level further down the
            // chain.
            Self::LatestTrade(source) => source.backpressure(),
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
}
