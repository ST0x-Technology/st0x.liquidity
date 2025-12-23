use thiserror::Error;

mod auth;
mod executor;
mod market_hours;
mod order;

pub use AlpacaTradingApiError as Error;
pub use auth::{AlpacaTradingApiAuthEnv, AlpacaTradingApiMode};
pub use executor::AlpacaTradingApi;
pub use market_hours::MarketHoursError;

#[derive(Debug, Error)]
pub enum AlpacaTradingApiError {
    #[error("API error: {0}")]
    Api(Box<apca::Error>),
    #[error("Account verification failed: {0}")]
    AccountVerification(#[from] apca::RequestError<apca::api::v2::account::GetError>),
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
    #[error("Invalid order: {0}")]
    InvalidOrder(String),
    #[error("Parse error: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),
    #[error("Parse error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("Price {0} cannot be converted to cents")]
    PriceConversion(f64),
    #[error("Notional orders not supported")]
    NotionalOrdersNotSupported,
    #[error("Filled order {order_id} is missing required field: {field}")]
    IncompleteFilledOrder { order_id: String, field: String },
}

impl From<apca::Error> for AlpacaTradingApiError {
    fn from(error: apca::Error) -> Self {
        Self::Api(Box::new(error))
    }
}
