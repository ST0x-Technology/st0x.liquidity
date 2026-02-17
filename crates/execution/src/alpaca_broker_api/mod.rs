use rust_decimal::Decimal;
use serde::Deserialize;
use std::fmt;
use std::str::FromStr;
use thiserror::Error;
use uuid::Uuid;

use crate::Symbol;
use crate::alpaca_broker_api::order::OrderSide;

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
mod market_hours;
mod order;
mod positions;

/// Asset status from Alpaca Broker API (public because it's exposed in error types)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AssetStatus {
    Active,
    Inactive,
}

/// Response from the asset endpoint
#[derive(Debug, Clone, Deserialize)]
pub(super) struct AssetResponse {
    pub status: AssetStatus,
    pub tradable: bool,
}

pub use auth::{AccountStatus, AlpacaBrokerApiCtx, AlpacaBrokerApiMode};
pub use executor::AlpacaBrokerApi;
pub use order::{ConversionDirection, CryptoOrderResponse};

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

    #[error("API error ({status}): {body}")]
    ApiError {
        status: reqwest::StatusCode,
        body: String,
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

    #[error("Cash balance {0} cannot be converted to cents")]
    CashBalanceConversion(Decimal),

    #[error("Cash balance {0} has fractional cents after conversion")]
    FractionalCents(Decimal),

    #[error("Asset {symbol} is not active (status: {status:?})")]
    AssetNotActive { symbol: Symbol, status: AssetStatus },

    #[error("Asset {symbol} is not tradable on Alpaca")]
    AssetNotTradable { symbol: Symbol },

    #[error("Invalid symbol in position: {0}")]
    InvalidSymbol(#[from] crate::EmptySymbolError),

    #[error(
        "Response side {response_side:?} does not match \
         requested side {requested_side:?}"
    )]
    SideMismatch {
        requested_side: OrderSide,
        response_side: OrderSide,
    },
}
