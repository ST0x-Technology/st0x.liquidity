use chrono::{NaiveDate, NaiveTime};
use rain_math_float::Float;
use rain_math_float::FloatError;
use serde::Deserialize;
use st0x_finance::UsdcConversionError;
use st0x_float_serde::format_float_with_fallback;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    AlpacaAmount, Backpressure, ClientOrderId, CounterTradeCostError, ExecutorOrderId,
    FractionalShares, Permanence, Positive, Symbol, Usd,
};
use st0x_alpaca::broker::AlpacaMarketDataError;

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

mod auth;
mod shared_executor;

pub use auth::{AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, AlpacaBrokerAuth};
pub use shared_executor::AlpacaBrokerApi;
#[cfg(feature = "mock")]
pub use st0x_alpaca::broker::mock::{
    AlpacaBrokerMock, MockMode, MockOrderSnapshot, MockPosition, MockPositionSnapshot,
    MockWalletTransferSnapshot, OrderSide, OrderStatus, TEST_ACCOUNT_ID, TEST_API_KEY,
    TEST_API_SECRET, TransferDirection, TransferFlow, TransferStatus, WhitelistStatus,
};
pub use st0x_alpaca::broker::{
    AccountActivitiesQuery, AccountActivity, AccountStatus, AssetDetails, AssetStatus,
    ConversionDirection, ConversionOrder, CryptoOrderFailureReason, CryptoOrderOutcome,
    CryptoOrderResponse, DeadlineCancel, HTTP_REQUEST_TIMEOUT, JournalResponse, JournalStatus,
    MissingOrderField,
};
pub use st0x_alpaca::broker::{AlpacaLimitPrice, ParseAlpacaLimitPriceError};
pub use st0x_alpaca::{ALPACA_TOKEN_URL, KmsJwtError};

#[derive(Debug, Clone)]
pub struct AlpacaLimitOrder {
    pub symbol: Symbol,
    pub shares: Positive<FractionalShares>,
    pub direction: crate::Direction,
    pub limit_price: AlpacaLimitPrice,
    pub extended_hours: bool,
    pub client_order_id: ClientOrderId,
}

impl AlpacaBrokerApiCtx {
    pub(crate) fn to_shared(&self) -> st0x_alpaca::broker::AlpacaBrokerApiCtx {
        let mode = self.mode.clone().map(|mode| match mode {
            AlpacaBrokerApiMode::Sandbox => st0x_alpaca::broker::AlpacaBrokerApiMode::Sandbox,
            AlpacaBrokerApiMode::Production => st0x_alpaca::broker::AlpacaBrokerApiMode::Production,
            #[cfg(any(test, feature = "mock"))]
            AlpacaBrokerApiMode::Mock(url) => st0x_alpaca::broker::AlpacaBrokerApiMode::Mock(url),
        });

        st0x_alpaca::broker::AlpacaBrokerApiCtx {
            auth: self.auth.clone(),
            account_id: self.account_id,
            mode,
            asset_cache_ttl: self.asset_cache_ttl,
            time_in_force: match self.time_in_force {
                TimeInForce::Day => st0x_alpaca::broker::TimeInForce::Day,
                TimeInForce::MarketOnClose => st0x_alpaca::broker::TimeInForce::MarketOnClose,
            },
        }
    }

    pub async fn fetch_account_activities(
        &self,
        query: &AccountActivitiesQuery,
    ) -> Result<Vec<AccountActivity>, AlpacaBrokerApiError> {
        Ok(self.to_shared().fetch_account_activities(query).await?)
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

#[derive(Debug, Error)]
pub enum AlpacaBrokerApiError {
    #[error(transparent)]
    Shared(#[from] Box<st0x_alpaca::broker::AlpacaBrokerApiError>),
    #[error(transparent)]
    QuoteInvariant(#[from] crate::LatestQuoteError),
    #[error("HTTP client error: {0}")]
    HttpClient(#[from] reqwest::Error),

    #[error("Failed to parse Alpaca API response: {0}")]
    JsonParse(#[from] serde_json::Error),

    #[error("Invalid Alpaca crypto amount: {0}")]
    AlpacaAmount(#[from] UsdcConversionError),

    #[error(
        "position endpoint returned {returned} when {requested} was requested; refusing to price \
         from another symbol"
    )]
    PositionSymbolMismatch { requested: Symbol, returned: Symbol },

    #[error("Invalid header value: {0}")]
    InvalidHeader(#[from] reqwest::header::InvalidHeaderValue),

    #[error("keyless Alpaca auth failed: {0}")]
    KmsJwt(#[from] KmsJwtError),

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

    /// Alpaca definitively rejected a USD-notional USDC conversion at order
    /// placement because the available USD balance was insufficient. The
    /// placement boundary wraps the original API error so callers can resize
    /// this one safe-to-retry case without treating the same code from a later
    /// order read as proof that no order exists.
    #[error("USD-to-USDC conversion placement rejected for insufficient USD balance: {source}")]
    UsdConversionInsufficientBalance {
        #[source]
        source: Box<Self>,
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
        match .cancel {
            DeadlineCancel::Accepted => "its remainder was cancelled",
            DeadlineCancel::Declined => "the broker declined to cancel its remainder as no longer cancelable",
            DeadlineCancel::Failed => "its remainder was never successfully cancelled",
        },
        .filled_quantity
            .as_ref()
            .map_or_else(|| "an unreported quantity".to_string(), ToString::to_string)
    )]
    ConversionCancelNotSettled {
        order_id: Uuid,
        cancel: DeadlineCancel,
        filled_quantity: Option<AlpacaAmount>,
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

    #[error("buying-power reservation {reserved_cents} cents does not fit in i64")]
    BuyingPowerReservationOutOfRange { reserved_cents: u64 },

    #[error(
        "cannot subtract buying-power reservation {reserved_cents} cents from available {available_cents} cents"
    )]
    BuyingPowerReservationOverflow {
        available_cents: i64,
        reserved_cents: i64,
    },

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

impl From<st0x_alpaca::broker::AlpacaBrokerApiError> for AlpacaBrokerApiError {
    fn from(source: st0x_alpaca::broker::AlpacaBrokerApiError) -> Self {
        use st0x_alpaca::broker::AlpacaBrokerApiError as Shared;

        match source {
            Shared::JsonParse(source) => Self::JsonParse(source),
            Shared::AlpacaAmount(source) => Self::AlpacaAmount(source),
            Shared::PositionSymbolMismatch {
                requested,
                returned,
            } => Self::PositionSymbolMismatch {
                requested,
                returned,
            },
            Shared::InvalidHeader(source) => Self::InvalidHeader(source),
            Shared::KmsJwt(source) => Self::KmsJwt(source),
            Shared::ApiError {
                status,
                alpaca_code,
                message,
                retry_after,
            } => Self::ApiError {
                status,
                alpaca_code,
                message,
                retry_after,
            },
            Shared::UsdConversionInsufficientBalance { source } => {
                Self::UsdConversionInsufficientBalance {
                    source: Box::new(Self::from(*source)),
                }
            }
            Shared::InvalidOrderId(source) => Self::InvalidOrderId(source),
            Shared::CryptoOrderFailed { order_id, reason } => {
                Self::CryptoOrderFailed { order_id, reason }
            }
            Shared::IncompleteOrder { order_id, field } => Self::IncompleteOrder {
                order_id: ExecutorOrderId::new(order_id.as_ref()),
                field,
            },
            Shared::FilledQuantityMismatch {
                order_id,
                ordered,
                filled,
            } => Self::FilledQuantityMismatch {
                order_id: ExecutorOrderId::new(order_id.as_ref()),
                ordered,
                filled,
            },
            Shared::AccountNotActive { account_id, status } => {
                Self::AccountNotActive { account_id, status }
            }
            Shared::DuplicateOrderNotFound { client_order_id } => {
                let client_order_id = match client_order_id {
                    st0x_alpaca::broker::ClientOrderId::Automated(id) => {
                        ClientOrderId::Automated(id)
                    }
                    st0x_alpaca::broker::ClientOrderId::Cli(id) => ClientOrderId::Cli(id),
                };
                Self::DuplicateOrderNotFound { client_order_id }
            }
            Shared::CalendarIterationInvariantViolation => {
                Self::CalendarIterationInvariantViolation
            }
            Shared::CalendarDateMismatch { queried, returned } => {
                Self::CalendarDateMismatch { queried, returned }
            }
            Shared::CalendarLocalTimeUnresolvable { date, time } => {
                Self::CalendarLocalTimeUnresolvable { date, time }
            }
            Shared::InvalidAccountActivitiesUrl { url, source } => {
                Self::InvalidAccountActivitiesUrl { url, source }
            }
            Shared::AccountActivitiesPaginationInvariantViolation => {
                Self::AccountActivitiesPaginationInvariantViolation
            }
            Shared::AccountActivitiesPageLimitExceeded { pages } => {
                Self::AccountActivitiesPageLimitExceeded { pages }
            }
            Shared::UsdBalanceConversion(value) => Self::UsdBalanceConversion(value),
            Shared::FractionalCents(value) => Self::FractionalCents(value),
            Shared::MissingPositionQuantity => Self::MissingPositionQuantity,
            Shared::InvalidSymbol(source) => Self::InvalidSymbol(source),
            Shared::NotPositive(source) => Self::NotPositive(source),
            Shared::NotPositiveLimitPrice(source) => Self::NotPositiveLimitPrice(source),
            Shared::FloatConversion(source) => Self::FloatConversion(source),
            Shared::ConversionCancelNotSettled {
                order_id,
                cancel,
                filled_quantity,
            } => Self::ConversionCancelNotSettled {
                order_id,
                cancel,
                filled_quantity,
            },
            Shared::ConversionOrderNotFound { order_id } => {
                Self::ConversionOrderNotFound { order_id }
            }
            Shared::ConversionTimedOut { order_id } => Self::ConversionTimedOut { order_id },
            Shared::AssetNotActive { symbol, status } => Self::AssetNotActive { symbol, status },
            Shared::AssetNotTradable { symbol } => Self::AssetNotTradable { symbol },
            Shared::InvalidLimitPricePrecision {
                limit_price,
                max_decimals,
            } => Self::InvalidLimitPricePrecision {
                limit_price,
                max_decimals,
            },
            Shared::BelowPrecision {
                shares,
                max_decimals,
            } => Self::BelowPrecision {
                shares,
                max_decimals,
            },
            Shared::UsdcBelowPrecision {
                amount,
                max_decimals,
            } => Self::UsdcBelowPrecision {
                amount,
                max_decimals,
            },
            Shared::UsdcPrecisionExceeded {
                amount,
                max_decimals,
            } => Self::UsdcPrecisionExceeded {
                amount,
                max_decimals,
            },
            Shared::LatestTrade(source) => Self::LatestTrade(source),
            Shared::LatestQuote(source) => Self::LatestQuote(source),
            other => Self::Shared(Box::new(other)),
        }
    }
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
            Self::Shared(source) => source.backpressure(),
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

            Self::QuoteInvariant(_)
            | Self::ApiError { .. }
            | Self::UsdConversionInsufficientBalance { .. }
            | Self::HttpClient(_)
            | Self::KmsJwt(_)
            | Self::JsonParse(_)
            | Self::AlpacaAmount(_)
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
            | Self::BuyingPowerReservationOutOfRange { .. }
            | Self::BuyingPowerReservationOverflow { .. }
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
            Self::Shared(source) => match source.permanence() {
                st0x_alpaca::Permanence::Permanent => Permanence::Permanent,
                st0x_alpaca::Permanence::Transient => Permanence::Transient,
            },
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
            Self::QuoteInvariant(_)
            | Self::JsonParse(_)
            | Self::AlpacaAmount(_)
            | Self::UsdConversionInsufficientBalance { .. }
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
            | Self::BuyingPowerReservationOutOfRange { .. }
            | Self::BuyingPowerReservationOverflow { .. }
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

            Self::LatestTrade(source) | Self::LatestQuote(source) => market_data_permanence(source),
        }
    }
}

fn market_data_permanence(error: &AlpacaMarketDataError) -> Permanence {
    match error {
        AlpacaMarketDataError::ApiError { status, .. }
            if status.is_server_error() || *status == reqwest::StatusCode::TOO_MANY_REQUESTS =>
        {
            Permanence::Transient
        }
        AlpacaMarketDataError::Http(_)
        | AlpacaMarketDataError::LatestQuoteSymbolMismatch { .. }
        | AlpacaMarketDataError::MissingQuote { .. }
        | AlpacaMarketDataError::MissingBid { .. }
        | AlpacaMarketDataError::MissingAsk { .. }
        | AlpacaMarketDataError::MissingQuoteTimestamp { .. }
        | AlpacaMarketDataError::NonPositiveBid { .. }
        | AlpacaMarketDataError::NonPositiveAsk { .. }
        | AlpacaMarketDataError::InvalidQuote { .. } => Permanence::Transient,
        AlpacaMarketDataError::Auth(error) if !error.is_deterministic() => Permanence::Transient,
        AlpacaMarketDataError::ApiError { .. }
        | AlpacaMarketDataError::Auth(_)
        | AlpacaMarketDataError::JsonParse(_)
        | AlpacaMarketDataError::LatestQuoteJsonParse(_)
        | AlpacaMarketDataError::Entitlement { .. }
        | AlpacaMarketDataError::MissingPrice { .. }
        | AlpacaMarketDataError::NonPositivePrice { .. } => Permanence::Permanent,
    }
}

#[cfg(test)]
mod tests {
    use st0x_float_macro::float;

    use super::*;

    #[test]
    fn shared_duplicate_order_error_keeps_local_client_id() {
        let id = Uuid::new_v4();
        let error = AlpacaBrokerApiError::from(
            st0x_alpaca::broker::AlpacaBrokerApiError::DuplicateOrderNotFound {
                client_order_id: st0x_alpaca::broker::ClientOrderId::cli(id),
            },
        );
        assert!(matches!(
            error,
            AlpacaBrokerApiError::DuplicateOrderNotFound {
                client_order_id: ClientOrderId::Cli(found)
            } if found == id
        ));
    }

    #[test]
    fn shared_account_and_calendar_errors_keep_local_variants() {
        let account_id = Uuid::new_v4();
        let error = AlpacaBrokerApiError::from(
            st0x_alpaca::broker::AlpacaBrokerApiError::AccountNotActive {
                account_id,
                status: AccountStatus::Disabled,
            },
        );
        assert!(matches!(
            error,
            AlpacaBrokerApiError::AccountNotActive { account_id: found, .. } if found == account_id
        ));
        let error = AlpacaBrokerApiError::from(
            st0x_alpaca::broker::AlpacaBrokerApiError::CalendarIterationInvariantViolation,
        );
        assert!(matches!(
            error,
            AlpacaBrokerApiError::CalendarIterationInvariantViolation
        ));
    }

    #[test]
    fn shared_activity_pagination_error_keeps_local_variant() {
        let error = AlpacaBrokerApiError::from(
            st0x_alpaca::broker::AlpacaBrokerApiError::AccountActivitiesPageLimitExceeded {
                pages: 101,
            },
        );
        assert!(matches!(
            error,
            AlpacaBrokerApiError::AccountActivitiesPageLimitExceeded { pages: 101 }
        ));
    }

    #[test]
    fn shared_calendar_and_activity_details_keep_local_payloads() {
        let queried = NaiveDate::from_ymd_opt(2026, 9, 24).unwrap();
        let returned = NaiveDate::from_ymd_opt(2026, 9, 25).unwrap();
        let error = AlpacaBrokerApiError::from(
            st0x_alpaca::broker::AlpacaBrokerApiError::CalendarDateMismatch { queried, returned },
        );
        assert!(matches!(
            error,
            AlpacaBrokerApiError::CalendarDateMismatch {
                queried: found_queried,
                returned: found_returned
            } if found_queried == queried && found_returned == returned
        ));
        let error = AlpacaBrokerApiError::from(
            st0x_alpaca::broker::AlpacaBrokerApiError::AccountActivitiesPaginationInvariantViolation,
        );
        assert!(matches!(
            error,
            AlpacaBrokerApiError::AccountActivitiesPaginationInvariantViolation
        ));
    }

    #[test]
    fn shared_value_errors_keep_local_variants() {
        use st0x_alpaca::broker::AlpacaBrokerApiError as Shared;

        let error = AlpacaBrokerApiError::from(Shared::UsdBalanceConversion(float!(1.001)));
        assert!(matches!(
            error,
            AlpacaBrokerApiError::UsdBalanceConversion(_)
        ));
        let error = AlpacaBrokerApiError::from(Shared::FractionalCents(float!(1.001)));
        assert!(matches!(error, AlpacaBrokerApiError::FractionalCents(_)));
        let error = AlpacaBrokerApiError::from(Shared::MissingPositionQuantity);
        assert!(matches!(
            error,
            AlpacaBrokerApiError::MissingPositionQuantity
        ));
    }

    #[test]
    fn shared_rate_limit_keeps_local_api_error_and_retry_hint() {
        let error =
            AlpacaBrokerApiError::from(st0x_alpaca::broker::AlpacaBrokerApiError::ApiError {
                status: reqwest::StatusCode::TOO_MANY_REQUESTS,
                alpaca_code: Some(42),
                message: "rate limited".to_string(),
                retry_after: Some(Duration::from_secs(7)),
            });

        assert!(matches!(
            error,
            AlpacaBrokerApiError::ApiError {
                alpaca_code: Some(42),
                ..
            }
        ));
        assert_eq!(
            error.backpressure(),
            Some(Backpressure {
                retry_after: Some(Duration::from_secs(7))
            })
        );
    }

    #[test]
    fn shared_uncertain_conversion_keeps_recovery_variant() {
        let order_id = Uuid::new_v4();
        let error = AlpacaBrokerApiError::from(
            st0x_alpaca::broker::AlpacaBrokerApiError::ConversionOrderNotFound { order_id },
        );

        assert!(
            matches!(error, AlpacaBrokerApiError::ConversionOrderNotFound { order_id: found } if found == order_id)
        );
        assert_eq!(error.permanence(), Permanence::Permanent);
    }

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

    #[test]
    fn insufficient_usd_conversion_balance_is_permanent_without_backpressure() {
        let error = AlpacaBrokerApiError::UsdConversionInsufficientBalance {
            source: Box::new(AlpacaBrokerApiError::ApiError {
                status: reqwest::StatusCode::FORBIDDEN,
                alpaca_code: Some(40_310_000),
                message: "insufficient balance for USD".to_string(),
                retry_after: None,
            }),
        };

        assert_eq!(error.backpressure(), None);
        assert_eq!(error.permanence(), Permanence::Permanent);
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
