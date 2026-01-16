use thiserror::Error;

mod auth;
mod executor;
mod market_hours;
mod order;

pub use auth::AlpacaBrokerApiAuthEnv;
pub use executor::AlpacaBrokerApi;

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
        "Inconsistent market data: market is open but next_close ({next_close}) is not in the future (now: {now})"
    )]
    MarketOpenButCloseInPast {
        next_close: chrono::DateTime<chrono::Utc>,
        now: chrono::DateTime<chrono::Utc>,
    },

    #[error(
        "Inconsistent market data: market is closed but next_open ({next_open}) is not in the future (now: {now})"
    )]
    MarketClosedButOpenInPast {
        next_open: chrono::DateTime<chrono::Utc>,
        now: chrono::DateTime<chrono::Utc>,
    },

    #[error(
        "Duration conversion failed: chrono duration cannot be converted to std::time::Duration"
    )]
    DurationConversion,

    #[error("Invalid order ID: {0}")]
    InvalidOrderId(#[from] uuid::Error),

    #[error("Price {0} cannot be converted to cents")]
    PriceConversion(f64),
}
