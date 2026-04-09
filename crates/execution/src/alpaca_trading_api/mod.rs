use rain_math_float::FloatError;
use thiserror::Error;

use crate::alpaca_market_data::AlpacaMarketDataError;
use crate::{CounterTradeCostError, FractionalShares, Positive};

mod auth;
mod executor;
mod inventory;
mod market_hours;
mod order;

pub use AlpacaTradingApiError as Error;
pub use auth::{AlpacaTradingApiCtx, AlpacaTradingApiMode};
pub use executor::AlpacaTradingApi;
pub use market_hours::MarketHoursError;

#[derive(Debug, Error)]
pub enum AlpacaTradingApiError {
    #[error("API error: {0}")]
    Api(Box<apca::Error>),
    #[error("Account verification failed: {0}")]
    AccountVerification(#[from] apca::RequestError<apca::api::v2::account::GetError>),
    #[error("HTTP client setup failed: {0}")]
    HttpClient(#[from] reqwest::Error),
    #[error("Invalid header value: {0}")]
    InvalidHeader(#[from] reqwest::header::InvalidHeaderValue),
    #[error("Order creation failed: {0}")]
    OrderCreate(#[from] apca::RequestError<apca::api::v2::order::CreateError>),
    #[error("Order query failed: {0}")]
    OrderGet(#[from] apca::RequestError<apca::api::v2::order::GetError>),
    #[error("Order listing failed: {0}")]
    OrderList(#[from] apca::RequestError<apca::api::v2::orders::ListError>),
    #[error("Market hours error: {0}")]
    MarketHours(#[from] MarketHoursError),
    #[error("Invalid order ID: {0}")]
    InvalidOrderId(#[from] uuid::Error),
    #[error("Empty symbol")]
    EmptySymbol(#[from] crate::EmptySymbolError),
    #[error("Invalid shares: {0}")]
    InvalidShares(#[from] crate::InvalidSharesError),
    #[error(transparent)]
    NotPositive(#[from] st0x_finance::NotPositive<st0x_finance::FractionalShares>),
    #[error("Num parse error: {0}")]
    NumParse(#[from] num_decimal::ParseNumError),
    #[error(
        "Cannot convert Float formatted string \
         '{formatted}' to Num: {source}"
    )]
    NumConversion {
        formatted: String,
        source: num_decimal::ParseNumError,
    },
    #[error(
        "Order quantity {shares} is below Alpaca's \
         {max_decimals}-decimal-place precision"
    )]
    BelowPrecision {
        shares: Positive<FractionalShares>,
        max_decimals: u8,
    },
    #[error("Float conversion error: {0}")]
    FloatConversion(#[from] FloatError),
    #[error("Notional orders not supported")]
    NotionalOrdersNotSupported,
    #[error("Filled order {order_id} is missing required field: {field}")]
    IncompleteFilledOrder { order_id: String, field: String },
    #[error("latest trade lookup failed: {0}")]
    LatestTrade(#[from] AlpacaMarketDataError),
    #[error("counter-trade cost estimation failed: {0}")]
    CounterTradeCost(#[from] CounterTradeCostError),
    #[error("cash balance {} cannot be converted to cents", st0x_float_serde::format_float_with_fallback(.0))]
    CashBalanceConversion(rain_math_float::Float),
    #[error("cash balance {} has fractional cents after conversion", st0x_float_serde::format_float_with_fallback(.0))]
    FractionalCents(rain_math_float::Float),
}

impl From<apca::Error> for AlpacaTradingApiError {
    fn from(error: apca::Error) -> Self {
        Self::Api(Box::new(error))
    }
}
