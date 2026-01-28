use chrono::{NaiveDate, NaiveTime};
use rust_decimal::Decimal;
use thiserror::Error;
use uuid::Uuid;

mod auth;
mod client;
mod executor;
mod market_hours;
mod order;
mod positions;

pub use auth::{AccountStatus, AlpacaBrokerApiAuthEnv, AlpacaBrokerApiMode};
pub use executor::AlpacaBrokerApi;
pub use order::{ConversionDirection, CryptoOrderResponse};

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

    #[error(
        "Duration conversion failed: chrono duration cannot be converted to std::time::Duration"
    )]
    DurationConversion,

    #[error("Invalid order ID: {0}")]
    InvalidOrderId(#[from] uuid::Error),

    #[error("Price {0} cannot be converted to cents")]
    PriceConversion(f64),

    #[error("Filled order {order_id} is missing required field: {field}")]
    IncompleteFilledOrder { order_id: String, field: String },

    #[error("Account {account_id} is not active (status: {status:?})")]
    AccountNotActive {
        account_id: Uuid,
        status: AccountStatus,
    },

    #[error("Ambiguous datetime when constructing calendar time: {date} {time}")]
    AmbiguousDateTime { date: NaiveDate, time: NaiveTime },

    #[error("No trading days found between {from} and {to}")]
    NoTradingDaysFound { from: NaiveDate, to: NaiveDate },

    #[error("Crypto order {order_id} failed: {reason:?}")]
    CryptoOrderFailed {
        order_id: Uuid,
        reason: CryptoOrderFailureReason,
    },

    #[error("Internal error: calendar was non-empty but iteration returned None")]
    CalendarIterationInvariantViolation,

    #[error("Cash balance {0} cannot be converted to cents")]
    CashBalanceConversion(Decimal),
}
