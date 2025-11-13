use async_trait::async_trait;
use std::fmt::{Debug, Display};
use tokio::task::JoinHandle;

pub mod alpaca;
pub mod error;
pub mod mock;
pub mod order;
pub mod schwab;

#[cfg(test)]
pub mod test_utils;

pub use alpaca::AlpacaBroker;
pub use error::PersistenceError;
pub use mock::{MockBroker, MockBrokerConfig};
pub use order::{MarketOrder, OrderPlacement, OrderState, OrderStatus, OrderUpdate};
pub use schwab::SchwabBroker;

use alpaca::{AlpacaAuthEnv, MarketHoursError};

#[async_trait]
pub trait Broker: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type OrderId: Display + Debug + Send + Sync + Clone;
    type Config: Send + Sync + Clone + 'static;

    /// Create and validate broker instance from config
    /// All initialization and validation happens here
    async fn try_from_config(config: Self::Config) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Wait until market opens (blocks if market closed), then return time until market close
    /// Implementations without market hours should return a very long duration
    async fn wait_until_market_open(&self) -> Result<std::time::Duration, Self::Error>;

    /// Place a market order for the specified symbol and quantity
    /// Returns order placement details including broker-assigned order ID
    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<OrderPlacement<Self::OrderId>, Self::Error>;

    /// Get the current status of a specific order
    /// Used to check if pending orders have been filled or failed
    async fn get_order_status(&self, order_id: &Self::OrderId) -> Result<OrderState, Self::Error>;

    /// Poll all pending orders for status updates
    /// More efficient than individual get_order_status calls for multiple orders
    async fn poll_pending_orders(&self) -> Result<Vec<OrderUpdate<Self::OrderId>>, Self::Error>;

    /// Return the enum variant representing this broker type
    /// Used for database storage and conditional logic
    fn to_supported_broker(&self) -> SupportedBroker;

    /// Convert a string representation to the broker's OrderId type
    /// This is needed for converting database-stored order IDs back to broker types
    fn parse_order_id(&self, order_id_str: &str) -> Result<Self::OrderId, Self::Error>;

    /// Run broker-specific maintenance tasks (token refresh, connection health, etc.)
    /// Returns None if no maintenance needed, Some(handle) if maintenance task spawned
    /// Tasks should run indefinitely and be aborted by the caller when shutdown is needed
    /// Errors are logged inside the task and do not propagate to the caller
    async fn run_broker_maintenance(&self) -> Option<JoinHandle<()>>;
}

/// Stock symbol newtype wrapper with validation
///
/// Ensures symbols are non-empty and provides type safety to prevent
/// mixing symbols with other string types.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Symbol(String);

impl Symbol {
    /// Create a new symbol with validation
    ///
    /// # Errors
    /// Returns `BrokerError::InvalidOrder` if symbol is empty
    pub fn new(symbol: impl Into<String>) -> Result<Self, BrokerError> {
        let symbol = symbol.into();
        if symbol.is_empty() {
            return Err(BrokerError::InvalidOrder {
                reason: "Symbol cannot be empty".to_string(),
            });
        }
        Ok(Self(symbol))
    }
}

impl Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Share quantity newtype wrapper with validation
///
/// Represents whole share quantities with bounds checking.
/// Values are constrained to 1..=u32::MAX for practical trading limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shares(u32);

impl Shares {
    /// Create a new share quantity with validation
    ///
    /// # Errors
    /// Returns `BrokerError::InvalidOrder` if shares is 0 or exceeds u32::MAX
    pub fn new(shares: u64) -> Result<Self, BrokerError> {
        if shares == 0 {
            return Err(BrokerError::InvalidOrder {
                reason: "Shares must be greater than 0".to_string(),
            });
        }
        u32::try_from(shares)
            .map(Self)
            .map_err(|_| BrokerError::InvalidOrder {
                reason: "Shares exceeds maximum allowed value".to_string(),
            })
    }

    pub fn value(&self) -> u32 {
        self.0
    }
}

impl Display for Shares {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidDirectionError(String);

impl std::fmt::Display for InvalidDirectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Invalid direction: {}", self.0)
    }
}

impl std::error::Error for InvalidDirectionError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum SupportedBroker {
    Schwab,
    Alpaca,
    DryRun,
}

impl std::fmt::Display for SupportedBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Schwab => write!(f, "schwab"),
            Self::Alpaca => write!(f, "alpaca"),
            Self::DryRun => write!(f, "dry_run"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Invalid broker: {0}")]
pub struct InvalidBrokerError(String);

impl std::str::FromStr for SupportedBroker {
    type Err = InvalidBrokerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "schwab" => Ok(Self::Schwab),
            "alpaca" => Ok(Self::Alpaca),
            "dry_run" => Ok(Self::DryRun),
            _ => Err(InvalidBrokerError(s.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Buy,
    Sell,
}

impl Direction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

impl Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for Direction {
    type Err = InvalidDirectionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "BUY" => Ok(Self::Buy),
            "SELL" => Ok(Self::Sell),
            _ => Err(InvalidDirectionError(s.to_string())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Schwab API error: {0}")]
    Schwab(#[from] schwab::SchwabError),

    #[error("Alpaca API error: {0}")]
    Alpaca(Box<apca::Error>),

    #[error("Alpaca request error: {0}")]
    AlpacaRequest(String),

    #[error("Market hours error: {0}")]
    MarketHours(#[from] MarketHoursError),

    #[error("Authentication failed: {0}")]
    Authentication(String),

    #[error("Order placement failed: {0}")]
    OrderPlacement(String),

    #[error("Order not found: {order_id}")]
    OrderNotFound { order_id: String },

    #[error("Network error: {0}")]
    Network(String),

    #[error("Rate limited: retry after {retry_after_seconds} seconds")]
    RateLimit { retry_after_seconds: u64 },

    #[error("Broker unavailable: {message}")]
    Unavailable { message: String },

    #[error("Invalid order: {reason}")]
    InvalidOrder { reason: String },

    #[error("Numeric conversion error: {0}")]
    NumericConversion(#[from] std::num::TryFromIntError),

    #[error("Date/time parse error: {0}")]
    DateTimeParse(#[from] chrono::ParseError),

    #[error("Price conversion failed: {price} cannot be converted to cents")]
    PriceConversion { price: f64 },
}

impl From<apca::Error> for BrokerError {
    fn from(error: apca::Error) -> Self {
        Self::Alpaca(Box::new(error))
    }
}

/// Trait for converting broker-specific configs into their corresponding broker implementations
#[async_trait]
pub trait TryIntoBroker {
    type Broker: Broker;

    async fn try_into_broker(self) -> Result<Self::Broker, <Self::Broker as Broker>::Error>;
}

#[async_trait]
impl TryIntoBroker for schwab::SchwabConfig {
    type Broker = SchwabBroker;

    async fn try_into_broker(self) -> Result<Self::Broker, <Self::Broker as Broker>::Error> {
        SchwabBroker::try_from_config(self).await
    }
}

#[async_trait]
impl TryIntoBroker for AlpacaAuthEnv {
    type Broker = AlpacaBroker;

    async fn try_into_broker(self) -> Result<Self::Broker, <Self::Broker as Broker>::Error> {
        AlpacaBroker::try_from_config(self).await
    }
}

#[async_trait]
impl TryIntoBroker for MockBrokerConfig {
    type Broker = MockBroker;

    async fn try_into_broker(self) -> Result<Self::Broker, <Self::Broker as Broker>::Error> {
        MockBroker::try_from_config(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_symbol_new_valid() {
        let symbol = Symbol::new("AAPL").unwrap();
        assert_eq!(symbol.to_string(), "AAPL");
    }

    #[test]
    fn test_symbol_new_empty_fails() {
        let result = Symbol::new("");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            BrokerError::InvalidOrder { .. }
        ));
    }

    #[test]
    fn test_symbol_new_boundary_valid() {
        let symbol = Symbol::new("A").unwrap();
        assert_eq!(symbol.to_string(), "A");

        let symbol = Symbol::new("ABCDEFGHIJ").unwrap(); // 10 chars
        assert_eq!(symbol.to_string(), "ABCDEFGHIJ");
    }

    #[test]
    fn test_shares_new_valid() {
        let shares = Shares::new(100).unwrap();
        assert_eq!(shares.to_string(), "100");
    }

    #[test]
    fn test_shares_new_zero_fails() {
        let result = Shares::new(0);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            BrokerError::InvalidOrder { .. }
        ));
    }

    #[test]
    fn test_shares_new_max_boundary() {
        let shares = Shares::new(u32::MAX as u64).unwrap();
        assert_eq!(shares.to_string(), u32::MAX.to_string());

        let result = Shares::new(u32::MAX as u64 + 1);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            BrokerError::InvalidOrder { .. }
        ));
    }

    #[test]
    fn test_shares_new_one() {
        let shares = Shares::new(1).unwrap();
        assert_eq!(shares.to_string(), "1");
    }
}
