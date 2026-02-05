use alloy::primitives::U256;
use async_trait::async_trait;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
use std::str::FromStr;
use tokio::task::JoinHandle;

pub mod alpaca_broker_api;
pub mod alpaca_trading_api;
pub mod error;
pub mod mock;
pub mod order;
pub mod schwab;

#[cfg(test)]
pub mod test_utils;

pub use alpaca_broker_api::{
    AlpacaBrokerApi, AlpacaBrokerApiCtx, AlpacaBrokerApiError, AlpacaBrokerApiMode,
    ConversionDirection, TimeInForce,
};
pub use alpaca_trading_api::{
    AlpacaTradingApi, AlpacaTradingApiCtx, AlpacaTradingApiError, AlpacaTradingApiMode,
};
pub use error::PersistenceError;
pub use mock::{MockExecutor, MockExecutorCtx};
pub use order::{MarketOrder, OrderPlacement, OrderState, OrderStatus, OrderUpdate};
pub use schwab::{Schwab, SchwabCtx, SchwabError, SchwabTokens, extract_code_from_url};

#[async_trait]
pub trait Executor: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type OrderId: Display + Debug + Send + Sync + Clone;
    type Ctx: Send + Sync + Clone + 'static;

    /// Create and validate executor instance from context
    /// All initialization and validation happens here
    async fn try_from_ctx(ctx: Self::Ctx) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Wait until market opens (blocks if market closed), then return time until market close
    /// Implementations without market hours should return a very long duration
    async fn wait_until_market_open(&self) -> Result<std::time::Duration, Self::Error>;

    /// Returns true if the market is currently open for trading.
    /// Unlike `wait_until_market_open`, this is a non-blocking check.
    async fn is_market_open(&self) -> Result<bool, Self::Error>;

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

    /// Fetches current inventory (positions and cash balance) from the broker.
    ///
    /// Returns `InventoryResult::Unimplemented` if not implemented for the executor.
    /// Returns `InventoryResult::Fetched(Inventory)` on success.
    //
    // NOTE: InventoryResult::Unimplemented is a workaround. This method is needed
    // for auto-rebalancing but not all executors support auto-rebalancing, so
    // implementing the method for non-auto-rebalancing executors is lower priority
    async fn get_inventory(&self) -> Result<InventoryResult, Self::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Symbol cannot be empty")]
pub struct EmptySymbolError;

/// Stock symbol newtype wrapper with validation
///
/// Ensures symbols are non-empty and provides type safety to prevent
/// mixing symbols with other string types.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
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
    #[error("Shares value {0} exceeds u64 range")]
    Overflow(Decimal),
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

    pub fn inner(self) -> T {
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

/// Trait for types that have a zero value and can be compared to it.
pub trait HasZero: PartialOrd + Sized {
    const ZERO: Self;

    fn is_zero(&self) -> bool
    where
        Self: PartialEq,
    {
        self == &Self::ZERO
    }

    fn is_negative(&self) -> bool {
        self < &Self::ZERO
    }
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

/// 10^18 scale factor for tokenized equity decimal conversion.
///
/// Tokenized equities use 18 decimals (unlike USDC which uses 6).
/// This equals 1,000,000,000,000,000,000 (one quintillion).
const TOKENIZED_EQUITY_SCALE: Decimal =
    Decimal::from_parts(2_808_348_672, 232_830_643, 0, false, 0);

impl FractionalShares {
    pub const ZERO: Self = Self(Decimal::ZERO);
    pub const ONE: Self = Self(Decimal::ONE);

    pub const fn new(value: Decimal) -> Self {
        Self(value)
    }

    pub fn inner(self) -> Decimal {
        self.0
    }

    #[must_use]
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

    /// Converts to U256 with 18 decimal places (standard ERC20 decimals).
    ///
    /// Returns an error for negative values, underflow (values < 1e-18),
    /// or overflow during scaling.
    pub fn to_u256_18_decimals(self) -> Result<U256, SharesConversionError> {
        if self.0.is_sign_negative() {
            return Err(SharesConversionError::NegativeValue(self.0));
        }

        if self.0.is_zero() {
            return Ok(U256::ZERO);
        }

        let scaled = self
            .0
            .checked_mul(TOKENIZED_EQUITY_SCALE)
            .ok_or(SharesConversionError::Overflow)?;

        let truncated = scaled.trunc();

        if truncated.is_zero() {
            return Err(SharesConversionError::Underflow(self.0));
        }

        Ok(U256::from_str_radix(&truncated.to_string(), 10)?)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SharesConversionError {
    #[error("shares value cannot be negative: {0}")]
    NegativeValue(Decimal),
    #[error("shares value too small to represent with 18 decimals: {0}")]
    Underflow(Decimal),
    #[error("overflow when scaling shares to 18 decimals")]
    Overflow,
    #[error("failed to parse U256: {0}")]
    ParseError(#[from] alloy::primitives::ruint::ParseError),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("arithmetic overflow: {lhs:?} {operation} {rhs:?}")]
pub struct ArithmeticError<T> {
    pub operation: String,
    pub lhs: T,
    pub rhs: T,
}

impl std::ops::Add for FractionalShares {
    type Output = Result<Self, ArithmeticError<Self>>;

    fn add(self, rhs: Self) -> Self::Output {
        self.0
            .checked_add(rhs.0)
            .map(Self)
            .ok_or_else(|| ArithmeticError {
                operation: "+".to_string(),
                lhs: self,
                rhs,
            })
    }
}

impl std::ops::Sub for FractionalShares {
    type Output = Result<Self, ArithmeticError<Self>>;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0
            .checked_sub(rhs.0)
            .map(Self)
            .ok_or_else(|| ArithmeticError {
                operation: "-".to_string(),
                lhs: self,
                rhs,
            })
    }
}

impl std::ops::Mul<Decimal> for FractionalShares {
    type Output = Result<Self, ArithmeticError<Self>>;

    fn mul(self, rhs: Decimal) -> Self::Output {
        self.0
            .checked_mul(rhs)
            .map(Self)
            .ok_or_else(|| ArithmeticError {
                operation: "*".to_string(),
                lhs: self,
                rhs: Self(rhs),
            })
    }
}

impl FromStr for FractionalShares {
    type Err = rust_decimal::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Decimal::from_str(s).map(Self)
    }
}

impl Display for FractionalShares {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Positive<FractionalShares> {
    pub const ONE: Self = Self(FractionalShares::ONE);

    /// Converts to whole shares count, returning error if value has a fractional part
    /// or exceeds u64 range. Use this when the target API does not support fractional shares.
    pub fn to_whole_shares(self) -> Result<u64, InvalidSharesError> {
        let inner = self.inner();
        if !inner.is_whole() {
            return Err(InvalidSharesError::Fractional(inner.0));
        }

        inner
            .0
            .to_u64()
            .ok_or(InvalidSharesError::Overflow(inner.0))
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid direction: {direction_provided}")]
pub struct InvalidDirectionError {
    direction_provided: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SupportedExecutor {
    Schwab,
    AlpacaTradingApi,
    AlpacaBrokerApi,
    DryRun,
}

impl SupportedExecutor {
    /// Returns whether this executor supports fractional share orders.
    pub const fn supports_fractional_shares(self) -> bool {
        match self {
            Self::Schwab => false,
            Self::AlpacaTradingApi | Self::AlpacaBrokerApi | Self::DryRun => true,
        }
    }
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
#[error("invalid executor: {executor_provided}")]
pub struct InvalidExecutorError {
    executor_provided: String,
}

impl std::str::FromStr for SupportedExecutor {
    type Err = InvalidExecutorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "schwab" => Ok(Self::Schwab),
            "alpaca-trading-api" => Ok(Self::AlpacaTradingApi),
            "alpaca-broker-api" => Ok(Self::AlpacaBrokerApi),
            "dry-run" => Ok(Self::DryRun),
            _ => Err(InvalidExecutorError {
                executor_provided: s.to_string(),
            }),
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
            _ => Err(InvalidDirectionError {
                direction_provided: s.to_string(),
            }),
        }
    }
}

/// An equity position with symbol, quantity, and optional market value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EquityPosition {
    pub symbol: Symbol,
    pub quantity: FractionalShares,
    pub market_value_cents: Option<i64>,
}

/// Account state from the broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inventory {
    pub positions: Vec<EquityPosition>,
    pub cash_balance_cents: i64,
}

/// Result of fetching inventory from an executor.
///
/// Custom enum to force explicit handling. Unlike `Option` which is easy to `.unwrap()`,
/// this type requires callers to explicitly match on the `Unimplemented` variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InventoryResult {
    /// Fetching inventory is unimplemented for this executor.
    ///
    /// This is a workaround. We need to fetch inventory for auto-rebalancing
    /// but not all executors support auto-rebalancing, so implementing the
    /// method for non-auto-rebalancing executors is lower priority
    Unimplemented,
    /// Successfully fetched inventory.
    Fetched(Inventory),
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

/// Trait for converting executor contexts into their corresponding executor implementations
#[async_trait]
pub trait TryIntoExecutor {
    type Executor: Executor;

    async fn try_into_executor(self)
    -> Result<Self::Executor, <Self::Executor as Executor>::Error>;
}

/// The order ID assigned by the executor (broker) when an order is placed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutorOrderId(String);

impl ExecutorOrderId {
    pub fn new(id: &(impl ToString + ?Sized)) -> Self {
        Self(id.to_string())
    }
}

impl AsRef<str> for ExecutorOrderId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Display for ExecutorOrderId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use alloy::primitives::U256;
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn positive_to_whole_shares_succeeds_for_whole_numbers() {
        let shares = Positive::new(FractionalShares::new(Decimal::from(5))).unwrap();
        assert_eq!(shares.to_whole_shares().unwrap(), 5);

        let shares = Positive::new(FractionalShares::new(dec!(100.0))).unwrap();
        assert_eq!(shares.to_whole_shares().unwrap(), 100);
    }

    #[test]
    fn positive_to_whole_shares_errors_for_fractional_values() {
        let shares = Positive::new(FractionalShares::new(dec!(1.212))).unwrap();
        let err = shares.to_whole_shares().unwrap_err();
        assert!(
            matches!(err, InvalidSharesError::Fractional(v) if v == dec!(1.212)),
            "Expected Fractional error with value 1.212, got: {err:?}"
        );
    }

    #[test]
    fn fractional_shares_is_whole_returns_true_for_whole_numbers() {
        let shares = FractionalShares::new(Decimal::from(1));
        assert!(shares.is_whole());

        let shares = FractionalShares::new(dec!(42.0));
        assert!(shares.is_whole());
    }

    #[test]
    fn fractional_shares_is_whole_returns_false_for_fractional_values() {
        let shares = FractionalShares::new(dec!(1.5));
        assert!(!shares.is_whole());

        let shares = FractionalShares::new(dec!(0.001));
        assert!(!shares.is_whole());
    }

    #[test]
    fn tokenized_equity_scale_equals_10_pow_18() {
        let expected = Decimal::from_str("1000000000000000000").unwrap();
        assert_eq!(
            TOKENIZED_EQUITY_SCALE, expected,
            "TOKENIZED_EQUITY_SCALE must equal 10^18"
        );
    }

    #[test]
    fn add_succeeds() {
        let a = FractionalShares::new(Decimal::ONE);
        let b = FractionalShares::new(Decimal::TWO);
        let result = (a + b).unwrap();
        assert_eq!(result.inner(), Decimal::from(3));
    }

    #[test]
    fn sub_succeeds() {
        let a = FractionalShares::new(Decimal::from(5));
        let b = FractionalShares::new(Decimal::TWO);
        let result = (a - b).unwrap();
        assert_eq!(result.inner(), Decimal::from(3));
    }

    #[test]
    fn add_overflow_returns_error() {
        let max = FractionalShares::new(Decimal::MAX);
        let one = FractionalShares::new(Decimal::ONE);
        let err = (max + one).unwrap_err();
        assert_eq!(err.operation, "+");
        assert_eq!(err.lhs, max);
        assert_eq!(err.rhs, one);
    }

    #[test]
    fn sub_overflow_returns_error() {
        let min = FractionalShares::new(Decimal::MIN);
        let one = FractionalShares::new(Decimal::ONE);
        let err = (min - one).unwrap_err();
        assert_eq!(err.operation, "-");
        assert_eq!(err.lhs, min);
        assert_eq!(err.rhs, one);
    }

    #[test]
    fn abs_returns_absolute_value() {
        let negative = FractionalShares::new(Decimal::NEGATIVE_ONE);
        assert_eq!(negative.abs().inner(), Decimal::ONE);
    }

    #[test]
    fn into_decimal_extracts_inner_value() {
        let shares = FractionalShares::new(Decimal::from(42));
        let decimal: Decimal = shares.into();
        assert_eq!(decimal, Decimal::from(42));
    }

    #[test]
    fn mul_decimal_succeeds() {
        let shares = FractionalShares::new(Decimal::from(100));
        let ratio = Decimal::new(5, 1); // 0.5
        let result = (shares * ratio).unwrap();
        assert_eq!(result.inner(), Decimal::from(50));
    }

    #[test]
    fn mul_decimal_overflow_returns_error() {
        let max = FractionalShares::new(Decimal::MAX);
        let two = Decimal::TWO;
        let err = (max * two).unwrap_err();
        assert_eq!(err.operation, "*");
        assert_eq!(err.lhs, max);
        assert_eq!(err.rhs, FractionalShares::new(two));
    }

    #[test]
    fn to_u256_18_decimals_zero_returns_zero() {
        let shares = FractionalShares::new(Decimal::ZERO);
        let result = shares.to_u256_18_decimals().unwrap();
        assert_eq!(result, U256::ZERO);
    }

    #[test]
    fn to_u256_18_decimals_one_returns_10_pow_18() {
        let shares = FractionalShares::new(Decimal::ONE);
        let result = shares.to_u256_18_decimals().unwrap();
        assert_eq!(result, U256::from_str("1000000000000000000").unwrap());
    }

    #[test]
    fn to_u256_18_decimals_fractional_value() {
        let shares = FractionalShares::new(dec!(1.5));
        let result = shares.to_u256_18_decimals().unwrap();
        assert_eq!(result, U256::from_str("1500000000000000000").unwrap());
    }

    #[test]
    fn to_u256_18_decimals_small_fractional_value() {
        let shares = FractionalShares::new(dec!(0.000000000000000001));
        let result = shares.to_u256_18_decimals().unwrap();
        assert_eq!(result, U256::from(1));
    }

    #[test]
    fn to_u256_18_decimals_negative_returns_error() {
        let shares = FractionalShares::new(Decimal::NEGATIVE_ONE);
        let err = shares.to_u256_18_decimals().unwrap_err();
        assert!(
            matches!(err, SharesConversionError::NegativeValue(_)),
            "Expected NegativeValue error, got: {err:?}"
        );
    }

    #[test]
    fn to_u256_18_decimals_truncates_sub_wei_digits() {
        // Values with >18 decimal places are truncated (sub-wei digits are meaningless)
        let shares = FractionalShares::new(dec!(1.1234567890123456789));
        let result = shares.to_u256_18_decimals().unwrap();
        assert_eq!(
            result,
            U256::from_str("1123456789012345678").unwrap(),
            "Expected sub-wei digits to be truncated"
        );
    }

    #[test]
    fn to_u256_18_decimals_underflow_returns_error() {
        let shares = FractionalShares::new(dec!(0.0000000000000000001));
        let err = shares.to_u256_18_decimals().unwrap_err();
        assert!(
            matches!(err, SharesConversionError::Underflow(_)),
            "Expected Underflow error, got: {err:?}"
        );
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
            mantissa in i64::MIN..=i64::MAX,
            scale in 0u32..=10,
        ) {
            let decimal = Decimal::new(mantissa, scale);
            let shares = FractionalShares::new(decimal);
            prop_assert_eq!(shares.inner(), decimal);
        }

        #[test]
        fn positive_rejects_zero_and_negative(
            mantissa in i64::MIN..=0i64,
            scale in 0u32..=10,
        ) {
            let decimal = Decimal::new(mantissa, scale);
            let result = Positive::new(FractionalShares::new(decimal));
            prop_assert!(matches!(result, Err(InvalidSharesError::NonPositive(_))));
        }

        #[test]
        fn fractional_shares_is_whole_matches_fract_is_zero(
            mantissa in i64::MIN..=i64::MAX,
            scale in 0u32..=10,
        ) {
            let decimal = Decimal::new(mantissa, scale);
            let shares = FractionalShares::new(decimal);
            prop_assert_eq!(shares.is_whole(), decimal.fract().is_zero());
        }

        #[test]
        fn positive_to_whole_roundtrips_integers(value in 1u64..=u64::MAX) {
            let decimal = Decimal::from(value);
            let shares = Positive::new(FractionalShares::new(decimal)).unwrap();
            prop_assert_eq!(shares.to_whole_shares().unwrap(), value);
        }

        #[test]
        fn positive_to_whole_rejects_fractional(
            whole in 0i64..=1_000_000,
            frac in 1u32..=999_999_999,
        ) {
            let decimal = Decimal::new(whole * 1_000_000_000 + i64::from(frac), 9);
            if decimal > Decimal::ZERO {
                let shares = Positive::new(FractionalShares::new(decimal)).unwrap();
                prop_assert!(matches!(
                    shares.to_whole_shares(),
                    Err(InvalidSharesError::Fractional(_))
                ));
            }
        }

        #[test]
        #[ignore = "flaky precision test. we have an issue for removing floating point calculations"]
        fn fractional_shares_from_f64_roundtrip_within_precision(
            mantissa in 1i64..=999_999_999_999i64,
            scale in 0u32..=6,
        ) {
            let decimal = Decimal::new(mantissa, scale);
            let shares = FractionalShares::new(decimal);

            if let Some(f64_value) = shares.inner().to_f64()
                && f64_value.is_finite()
                && f64_value > 0.0
            {
                let roundtrip = FractionalShares::from_f64(f64_value).unwrap();
                let diff = (shares.inner() - roundtrip.inner()).abs();
                // f64 has ~15-16 significant decimal digits. For large values like
                // 4322285221.77, some precision loss in lower digits is expected.
                // Use 1e-6 as tolerance which is realistic for f64 roundtrips.
                prop_assert!(
                    diff <= Decimal::new(1, 6),
                    "Roundtrip diff too large: {} for original {}",
                    diff,
                    decimal
                );
            }
        }
    }

    #[test]
    fn schwab_does_not_support_fractional_shares() {
        assert!(!SupportedExecutor::Schwab.supports_fractional_shares());
    }

    #[test]
    fn alpaca_trading_api_supports_fractional_shares() {
        assert!(SupportedExecutor::AlpacaTradingApi.supports_fractional_shares());
    }

    #[test]
    fn alpaca_broker_api_supports_fractional_shares() {
        assert!(SupportedExecutor::AlpacaBrokerApi.supports_fractional_shares());
    }

    #[test]
    fn dry_run_supports_fractional_shares() {
        assert!(SupportedExecutor::DryRun.supports_fractional_shares());
    }
}
