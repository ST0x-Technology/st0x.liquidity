use async_trait::async_trait;
use clap::ValueEnum;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
use tokio::task::JoinHandle;

pub mod alpaca_broker_api;
pub mod alpaca_trading_api;
pub mod error;
pub mod mock;
pub mod order;
pub mod schwab;

#[cfg(test)]
pub mod test_utils;

pub use alpaca_broker_api::AlpacaBrokerApi;
pub use alpaca_trading_api::AlpacaTradingApi;
pub use error::PersistenceError;
pub use mock::{MockExecutor, MockExecutorConfig};
pub use order::{MarketOrder, OrderPlacement, OrderState, OrderStatus, OrderUpdate};
pub use schwab::SchwabExecutor;

#[async_trait]
pub trait Executor: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type OrderId: Display + Debug + Send + Sync + Clone;
    type Config: Send + Sync + Clone + 'static;

    /// Create and validate executor instance from config
    /// All initialization and validation happens here
    async fn try_from_config(config: Self::Config) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Wait until market opens (blocks if market closed), then return time until market close
    /// Implementations without market hours should return a very long duration
    async fn wait_until_market_open(&self) -> Result<std::time::Duration, Self::Error>;

    /// Place a market order for the specified symbol and quantity
    /// Returns order placement details including executor-assigned order ID
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

    /// Return the enum variant representing this executor type
    /// Used for database storage and conditional logic
    fn to_supported_executor(&self) -> SupportedExecutor;

    /// Convert a string representation to the executor's OrderId type
    /// This is needed for converting database-stored order IDs back to executor types
    fn parse_order_id(&self, order_id_str: &str) -> Result<Self::OrderId, Self::Error>;

    /// Run executor-specific maintenance tasks (token refresh, connection health, etc.)
    /// Returns None if no maintenance needed, Some(handle) if maintenance task spawned
    /// Tasks should run indefinitely and be aborted by the caller when shutdown is needed
    /// Errors are logged inside the task and do not propagate to the caller
    async fn run_executor_maintenance(&self) -> Option<JoinHandle<()>>;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Symbol cannot be empty")]
pub struct EmptySymbolError;

/// Stock symbol newtype wrapper with validation
///
/// Ensures symbols are non-empty and provides type safety to prevent
/// mixing symbols with other string types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub struct Symbol(String);

impl Symbol {
    pub fn new(symbol: impl Into<String>) -> Result<Self, EmptySymbolError> {
        let symbol = symbol.into();
        if symbol.is_empty() {
            return Err(EmptySymbolError);
        }
        Ok(Self(symbol))
    }
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::new(s).map_err(serde::de::Error::custom)
    }
}

impl Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for Symbol {
    type Err = EmptySymbolError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum InvalidSharesError {
    #[error("Shares cannot be zero")]
    Zero,
    #[error("Value must be positive, got {0}")]
    NonPositive(Decimal),
    #[error("Cannot convert fractional shares {0} to whole shares")]
    Fractional(Decimal),
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),
    #[error(transparent)]
    DecimalConversion(#[from] rust_decimal::Error),
}

/// Wrapper that guarantees the inner value is positive (greater than zero).
///
/// Use this when an API requires strictly positive values, such as order quantities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(transparent)]
pub struct Positive<T>(T);

impl<T> Positive<T>
where
    T: PartialOrd + HasZero + Copy,
{
    pub fn new(value: T) -> Result<Self, InvalidSharesError>
    where
        T: Into<Decimal>,
    {
        if value <= T::ZERO {
            return Err(InvalidSharesError::NonPositive(value.into()));
        }
        Ok(Self(value))
    }

    pub fn value(self) -> T {
        self.0
    }
}

impl<'de, T> Deserialize<'de> for Positive<T>
where
    T: Deserialize<'de> + PartialOrd + HasZero + Copy + Into<Decimal>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = T::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

impl<T: Display> Display for Positive<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Trait for types that have a zero value.
pub trait HasZero {
    const ZERO: Self;
}

/// Share quantity newtype wrapper with validation
///
/// Represents whole share quantities with bounds checking.
/// Values are constrained to 1..=u32::MAX for practical trading limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct Shares(u32);

impl Shares {
    pub fn new(shares: u64) -> Result<Self, InvalidSharesError> {
        if shares == 0 {
            return Err(InvalidSharesError::Zero);
        }
        Ok(Self(u32::try_from(shares)?))
    }

    pub fn value(&self) -> u32 {
        self.0
    }
}

impl<'de> Deserialize<'de> for Shares {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let shares = u64::deserialize(deserializer)?;
        Self::new(shares).map_err(serde::de::Error::custom)
    }
}

impl Display for Shares {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Fractional share quantity newtype wrapper.
///
/// Represents share quantities that can include fractional amounts (e.g., 1.212 shares).
/// Can be negative (for position tracking). Use `Positive<FractionalShares>` when
/// strictly positive values are required (e.g., order quantities).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(transparent)]
pub struct FractionalShares(Decimal);

impl HasZero for FractionalShares {
    const ZERO: Self = Self(Decimal::ZERO);
}

impl From<FractionalShares> for Decimal {
    fn from(value: FractionalShares) -> Self {
        value.0
    }
}

impl FractionalShares {
    pub const fn new(value: Decimal) -> Self {
        Self(value)
    }

    pub fn value(self) -> Decimal {
        self.0
    }

    pub fn abs(self) -> Self {
        Self(self.0.abs())
    }

    pub fn is_zero(self) -> bool {
        self.0.is_zero()
    }

    pub fn is_negative(self) -> bool {
        self.0.is_sign_negative()
    }

    /// Returns true if this represents a whole number of shares (no fractional part).
    pub fn is_whole(self) -> bool {
        self.0.fract().is_zero()
    }

    /// Creates FractionalShares from an f64 value (typically from database REAL column).
    pub fn from_f64(value: f64) -> Result<Self, InvalidSharesError> {
        let decimal = Decimal::try_from(value)?;
        Ok(Self(decimal))
    }
}

impl Positive<FractionalShares> {
    /// Converts to whole shares count, returning error if value has a fractional part.
    /// Use this when the target API does not support fractional shares.
    pub fn to_whole_shares(self) -> Result<u64, InvalidSharesError> {
        let inner = self.value();
        if !inner.is_whole() {
            return Err(InvalidSharesError::Fractional(inner.0));
        }

        inner
            .0
            .to_u64()
            .ok_or(InvalidSharesError::Fractional(inner.0))
    }
}

impl<'de> Deserialize<'de> for FractionalShares {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <Decimal as serde::Deserialize>::deserialize(deserializer)?;
        Ok(Self::new(value))
    }
}

impl Display for FractionalShares {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum SupportedExecutor {
    Schwab,
    AlpacaTradingApi,
    AlpacaBrokerApi,
    DryRun,
}

impl std::fmt::Display for SupportedExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Schwab => write!(f, "schwab"),
            Self::AlpacaTradingApi => write!(f, "alpaca-trading-api"),
            Self::AlpacaBrokerApi => write!(f, "alpaca-broker-api"),
            Self::DryRun => write!(f, "dry-run"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Invalid executor: {0}")]
pub struct InvalidExecutorError(String);

impl std::str::FromStr for SupportedExecutor {
    type Err = InvalidExecutorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "schwab" => Ok(Self::Schwab),
            "alpaca-trading-api" => Ok(Self::AlpacaTradingApi),
            "alpaca-broker-api" => Ok(Self::AlpacaBrokerApi),
            "dry-run" => Ok(Self::DryRun),
            _ => Err(InvalidExecutorError(s.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
pub enum ExecutionError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Schwab error: {0}")]
    Schwab(#[from] schwab::SchwabError),
    #[error("{status:?} order requires order_id")]
    MissingOrderId { status: OrderStatus },
    #[error("{status:?} order requires price_cents")]
    MissingPriceCents { status: OrderStatus },
    #[error("{status:?} order requires executed_at timestamp")]
    MissingExecutedAt { status: OrderStatus },
    #[error("Order not found: {order_id}")]
    OrderNotFound { order_id: String },
    #[error("Mock executor failure: {message}")]
    MockFailure { message: String },
    #[error("Incomplete order response: {field} missing for {status:?} order")]
    IncompleteOrderResponse { field: String, status: OrderStatus },
    #[error(transparent)]
    EmptySymbol(#[from] EmptySymbolError),
    #[error(transparent)]
    InvalidShares(#[from] InvalidSharesError),
    #[error(transparent)]
    InvalidDirection(#[from] InvalidDirectionError),
    #[error("Negative shares value: {value}")]
    NegativeShares { value: f64 },
    #[error("Price {price} cannot be converted to cents")]
    PriceConversion { price: f64 },
    #[error("Numeric conversion error: {0}")]
    NumericConversion(#[from] std::num::TryFromIntError),
    #[error("Date/time parse error: {0}")]
    DateTimeParse(#[from] chrono::ParseError),
}

/// Trait for converting executor-specific configs into their corresponding executor implementations
#[async_trait]
pub trait TryIntoExecutor {
    type Executor: Executor;

    async fn try_into_executor(self)
    -> Result<Self::Executor, <Self::Executor as Executor>::Error>;
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use proptest::prelude::*;

    use super::*;

    #[test]
    fn fractional_shares_to_whole_shares_succeeds_for_whole_numbers() {
        let shares = FractionalShares::new(Decimal::from(5)).unwrap();
        assert_eq!(shares.to_whole_shares().unwrap(), 5);

        let shares = FractionalShares::new(Decimal::from_str("100.0").unwrap()).unwrap();
        assert_eq!(shares.to_whole_shares().unwrap(), 100);
    }

    #[test]
    fn fractional_shares_to_whole_shares_errors_for_fractional_values() {
        let shares = FractionalShares::new(Decimal::from_str("1.212").unwrap()).unwrap();
        let err = shares.to_whole_shares().unwrap_err();
        assert!(
            matches!(err, InvalidSharesError::Fractional(v) if v == Decimal::from_str("1.212").unwrap()),
            "Expected Fractional error with value 1.212, got: {err:?}"
        );
    }

    #[test]
    fn fractional_shares_is_whole_returns_true_for_whole_numbers() {
        let shares = FractionalShares::new(Decimal::from(1)).unwrap();
        assert!(shares.is_whole());

        let shares = FractionalShares::new(Decimal::from_str("42.0").unwrap()).unwrap();
        assert!(shares.is_whole());
    }

    #[test]
    fn fractional_shares_is_whole_returns_false_for_fractional_values() {
        let shares = FractionalShares::new(Decimal::from_str("1.5").unwrap()).unwrap();
        assert!(!shares.is_whole());

        let shares = FractionalShares::new(Decimal::from_str("0.001").unwrap()).unwrap();
        assert!(!shares.is_whole());
    }

    #[test]
    fn test_symbol_new_valid() {
        let symbol = Symbol::new("AAPL").unwrap();
        assert_eq!(symbol.to_string(), "AAPL");
    }

    #[test]
    fn test_symbol_new_empty_fails() {
        let result = Symbol::new("");
        assert!(matches!(result.unwrap_err(), EmptySymbolError));
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
        assert!(matches!(result.unwrap_err(), InvalidSharesError::Zero));
    }

    #[test]
    fn test_shares_new_max_boundary() {
        let shares = Shares::new(u64::from(u32::MAX)).unwrap();
        assert_eq!(shares.to_string(), u32::MAX.to_string());

        let result = Shares::new(u64::from(u32::MAX) + 1);
        assert!(matches!(
            result.unwrap_err(),
            InvalidSharesError::TryFromInt(_)
        ));
    }

    #[test]
    fn test_shares_new_one() {
        let shares = Shares::new(1).unwrap();
        assert_eq!(shares.to_string(), "1");
    }

    proptest! {
        #[test]
        fn fractional_shares_construction_preserves_value(
            mantissa in 1i64..=i64::MAX,
            scale in 0u32..=10,
        ) {
            let decimal = Decimal::new(mantissa, scale);
            let shares = FractionalShares::new(decimal).unwrap();
            prop_assert_eq!(shares.value(), decimal);
        }

        #[test]
        fn fractional_shares_rejects_zero_and_negative(
            mantissa in i64::MIN..=0i64,
            scale in 0u32..=10,
        ) {
            let decimal = Decimal::new(mantissa, scale);
            let result = FractionalShares::new(decimal);
            prop_assert!(matches!(result, Err(InvalidSharesError::NonPositive)));
        }

        #[test]
        fn fractional_shares_is_whole_matches_fract_is_zero(
            mantissa in 1i64..=i64::MAX,
            scale in 0u32..=10,
        ) {
            let decimal = Decimal::new(mantissa, scale);
            let shares = FractionalShares::new(decimal).unwrap();
            prop_assert_eq!(shares.is_whole(), decimal.fract().is_zero());
        }

        #[test]
        fn fractional_shares_to_whole_roundtrips_integers(value in 1u64..=u64::MAX) {
            let decimal = Decimal::from(value);
            let shares = FractionalShares::new(decimal).unwrap();
            prop_assert_eq!(shares.to_whole_shares().unwrap(), value);
        }

        #[test]
        fn fractional_shares_to_whole_rejects_fractional(
            whole in 0i64..=1_000_000,
            frac in 1u32..=999_999_999,
        ) {
            let decimal = Decimal::new(whole * 1_000_000_000 + i64::from(frac), 9);
            if decimal > Decimal::ZERO {
                let shares = FractionalShares::new(decimal).unwrap();
                prop_assert!(matches!(
                    shares.to_whole_shares(),
                    Err(InvalidSharesError::Fractional(_))
                ));
            }
        }

        #[test]
        fn fractional_shares_from_f64_roundtrip_within_precision(
            mantissa in 1i64..=999_999_999_999i64,
            scale in 0u32..=6,
        ) {
            let decimal = Decimal::new(mantissa, scale);
            let shares = FractionalShares::new(decimal).unwrap();

            if let Some(f64_value) = shares.value().to_f64()
                && f64_value.is_finite()
                && f64_value > 0.0
            {
                let roundtrip = FractionalShares::from_f64(f64_value).unwrap();
                let diff = (shares.value() - roundtrip.value()).abs();
                prop_assert!(
                    diff < Decimal::new(1, 10),
                    "Roundtrip diff too large: {} for original {}",
                    diff,
                    decimal
                );
            }
        }
    }
}
