use rain_math_float::Float;
use rain_math_float::FloatError;
use serde::Deserialize;
use st0x_float_serde::format_float_with_fallback;
use std::fmt;
use std::str::FromStr;
use thiserror::Error;
use uuid::Uuid;

use crate::{FractionalShares, Positive, Symbol, Usd};

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
    TEST_API_SECRET, TransferDirection, TransferStatus, WhitelistStatus,
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

pub use auth::{AccountStatus, AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode};
pub use executor::AlpacaBrokerApi;
pub use journal::{JournalResponse, JournalStatus};
pub use order::{AlpacaLimitOrder, ConversionDirection, CryptoOrderResponse};

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

/// Terminal failure states for crypto orders
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CryptoOrderFailureReason {
    Canceled,
    Expired,
    Rejected,
}

#[derive(Debug, Error)]
pub enum AlpacaBrokerApiError {
    #[error("HTTP client error: {0}")]
    HttpClient(#[from] reqwest::Error),

    #[error("Invalid header value: {0}")]
    InvalidHeader(#[from] reqwest::header::InvalidHeaderValue),

    #[error("{}", format_api_error(*status, alpaca_code.as_ref(), message))]
    ApiError {
        status: reqwest::StatusCode,
        /// Alpaca error code (e.g., 40310000 for PDT restriction)
        alpaca_code: Option<u64>,
        /// Human-readable error message from Alpaca
        message: String,
    },

    #[error("Invalid order ID: {0}")]
    InvalidOrderId(#[from] uuid::Error),

    #[error("Filled order {order_id} is missing required field: {field}")]
    IncompleteFilledOrder { order_id: String, field: String },

    #[error("Account {account_id} is not active (status: {status:?})")]
    AccountNotActive {
        account_id: Uuid,
        status: AccountStatus,
    },

    #[error("Crypto order {order_id} failed: {reason:?}")]
    CryptoOrderFailed {
        order_id: Uuid,
        reason: CryptoOrderFailureReason,
    },

    #[error("Internal error: calendar was non-empty but iteration returned None")]
    CalendarIterationInvariantViolation,

    #[error("Asset {symbol} is not active (status: {status:?})")]
    AssetNotActive { symbol: Symbol, status: AssetStatus },

    #[error("Asset {symbol} is not tradable on Alpaca")]
    AssetNotTradable { symbol: Symbol },

    #[error("limit orders only support day time-in-force, got {time_in_force}")]
    InvalidLimitOrderTimeInForce { time_in_force: TimeInForce },

    #[error(
        "Limit price {limit_price} exceeds Alpaca's \
         {max_decimals}-decimal-place precision for this price range"
    )]
    InvalidLimitPricePrecision {
        limit_price: Positive<Usd>,
        max_decimals: u8,
    },

    #[error("Cash balance {} cannot be converted to cents", format_float_with_fallback(.0))]
    CashBalanceConversion(Float),

    #[error("Cash balance {} has fractional cents after conversion", format_float_with_fallback(.0))]
    FractionalCents(Float),

    #[error("Invalid symbol in position: {0}")]
    InvalidSymbol(#[from] crate::EmptySymbolError),

    #[error(
        "Order quantity {shares} is below Alpaca's \
         {max_decimals}-decimal-place precision"
    )]
    BelowPrecision {
        shares: Positive<FractionalShares>,
        max_decimals: u8,
    },

    #[error(transparent)]
    NotPositive(#[from] st0x_finance::NotPositive<FractionalShares>),

    #[error("Float conversion error: {0}")]
    FloatConversion(#[from] FloatError),
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
