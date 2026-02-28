use alloy::primitives::U256;
use async_trait::async_trait;
use rain_math_float::FloatError;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_exact_decimal::ExactDecimal;
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
    AlpacaAccountId, AlpacaBrokerApi, AlpacaBrokerApiCtx, AlpacaBrokerApiError,
    AlpacaBrokerApiMode, ConversionDirection, JournalResponse, JournalStatus, TimeInForce,
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

    /// Returns true if the market is currently open for trading.
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
    pub fn inner(&self) -> String {
        self.0.clone()
    }
    #[cfg(any(test, feature = "test-support"))]
    pub fn force_new(symbol: String) -> Self {
        Self(symbol)
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

#[derive(Debug, thiserror::Error)]
pub enum InvalidSharesError {
    #[error("Shares cannot be zero")]
    Zero,
    #[error("Value must be positive, got {0}")]
    NonPositive(ExactDecimal),
    #[error("Cannot convert fractional shares {0} to whole shares")]
    Fractional(ExactDecimal),
    #[error("Shares value {0} exceeds u64 range")]
    Overflow(ExactDecimal),
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),
    #[error("Float conversion failed: {0}")]
    FloatConversion(#[from] FloatError),
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
        T: Into<ExactDecimal>,
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
    T: Deserialize<'de> + PartialOrd + HasZero + Copy + Into<ExactDecimal>,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct FractionalShares(ExactDecimal);

impl HasZero for FractionalShares {
    const ZERO: Self = Self(ExactDecimal::zero());
}

impl From<FractionalShares> for ExactDecimal {
    fn from(value: FractionalShares) -> Self {
        value.0
    }
}

impl FractionalShares {
    pub const ZERO: Self = Self(ExactDecimal::zero());

    pub fn new(value: ExactDecimal) -> Self {
        Self(value)
    }

    pub fn inner(self) -> ExactDecimal {
        self.0
    }

    pub fn abs(self) -> Result<Self, FloatError> {
        self.0.abs().map(Self)
    }

    pub fn is_zero(self) -> bool {
        self == Self::ZERO
    }

    pub fn is_negative(self) -> bool {
        self < Self::ZERO
    }

    /// Returns true if this represents a whole number of shares (no fractional part).
    pub fn is_whole(self) -> Result<bool, FloatError> {
        let frac = self.0.frac()?;
        frac.is_zero()
    }

    /// Converts to U256 with 18 decimal places (standard ERC20 decimals).
    ///
    /// Uses lossy conversion because Float's 224-bit coefficient may carry
    /// more than 18 decimal places of precision, but ERC-20 tokens are 18
    /// decimals so the extra precision is representational noise.
    pub fn to_u256_18_decimals(self) -> Result<U256, SharesConversionError> {
        if self.is_negative() {
            return Err(SharesConversionError::NegativeValue(self.0));
        }

        if self.is_zero() {
            return Ok(U256::ZERO);
        }

        self.0
            .to_fixed_decimal_lossy(18)
            .map(|(fixed, _lossless)| fixed)
            .map_err(SharesConversionError::FloatConversion)
    }

    /// Creates `FractionalShares` from a U256 value with 18 decimal places.
    pub fn from_u256_18_decimals(value: U256) -> Result<Self, SharesConversionError> {
        if value.is_zero() {
            return Ok(Self::ZERO);
        }

        ExactDecimal::from_fixed_decimal(value, 18)
            .map(Self)
            .map_err(SharesConversionError::FloatConversion)
    }

    /// Convert to `rust_decimal::Decimal` for broker API boundaries.
    ///
    /// This is an internal-only helper for Alpaca/Schwab APIs that speak Decimal.
    pub(crate) fn to_decimal(self) -> Result<Decimal, FloatError> {
        let formatted = self.0.format_decimal()?;
        formatted
            .parse::<Decimal>()
            .map_err(|error| FloatError::InvalidHex(error.to_string()))
    }

    /// Construct from a `rust_decimal::Decimal` at broker API boundaries.
    pub(crate) fn from_decimal(value: Decimal) -> Result<Self, FloatError> {
        ExactDecimal::parse(&value.to_string()).map(Self)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SharesConversionError {
    #[error("shares value cannot be negative: {0}")]
    NegativeValue(ExactDecimal),
    #[error("Float conversion failed: {0}")]
    FloatConversion(FloatError),
}

impl From<SharesConversionError> for InvalidSharesError {
    fn from(error: SharesConversionError) -> Self {
        match error {
            SharesConversionError::NegativeValue(value) => Self::NonPositive(value),
            SharesConversionError::FloatConversion(error) => Self::FloatConversion(error),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("arithmetic overflow: {lhs:?} {operation} {rhs:?}")]
pub struct ArithmeticError<T> {
    pub operation: String,
    pub lhs: T,
    pub rhs: T,
}

impl std::ops::Add for FractionalShares {
    type Output = Result<Self, FloatError>;

    fn add(self, rhs: Self) -> Self::Output {
        (self.0 + rhs.0).map(Self)
    }
}

impl std::ops::Sub for FractionalShares {
    type Output = Result<Self, FloatError>;

    fn sub(self, rhs: Self) -> Self::Output {
        (self.0 - rhs.0).map(Self)
    }
}

impl std::ops::Mul for FractionalShares {
    type Output = Result<Self, FloatError>;

    fn mul(self, rhs: Self) -> Self::Output {
        (self.0 * rhs.0).map(Self)
    }
}

impl std::ops::Mul<ExactDecimal> for FractionalShares {
    type Output = Result<Self, FloatError>;

    fn mul(self, rhs: ExactDecimal) -> Self::Output {
        (self.0 * rhs).map(Self)
    }
}

impl FromStr for FractionalShares {
    type Err = FloatError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        ExactDecimal::parse(value).map(Self)
    }
}

impl Display for FractionalShares {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl Positive<FractionalShares> {
    /// Converts to whole shares count, returning error if value has a fractional part
    /// or exceeds u64 range. Use this when the target API does not support fractional shares.
    pub fn to_whole_shares(self) -> Result<u64, InvalidSharesError> {
        let inner = self.inner();

        let is_whole = inner.is_whole()?;
        if !is_whole {
            return Err(InvalidSharesError::Fractional(inner.0));
        }

        let integer_part = inner.0.integer()?;
        let formatted = integer_part.format_decimal()?;

        formatted
            .parse::<u64>()
            .map_err(|_| InvalidSharesError::Overflow(inner.0))
    }
}

impl<'de> Deserialize<'de> for FractionalShares {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = ExactDecimal::deserialize(deserializer)?;
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
    pub market_value: Option<ExactDecimal>,
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
    #[error("{status:?} order requires price")]
    MissingPrice { status: OrderStatus },
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
    use alloy::primitives::U256;
    use proptest::prelude::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    use super::*;

    /// Helper to create FractionalShares from a string in tests.
    fn fs(value: &str) -> FractionalShares {
        FractionalShares::new(ExactDecimal::parse(value).unwrap())
    }

    /// Helper to create ExactDecimal from a string in tests.
    fn ed(value: &str) -> ExactDecimal {
        ExactDecimal::parse(value).unwrap()
    }

    #[test]
    fn positive_to_whole_shares_succeeds_for_whole_numbers() {
        let shares = Positive::new(fs("5")).unwrap();
        assert_eq!(shares.to_whole_shares().unwrap(), 5);

        let shares = Positive::new(fs("100")).unwrap();
        assert_eq!(shares.to_whole_shares().unwrap(), 100);
    }

    #[test]
    fn positive_to_whole_shares_errors_for_fractional_values() {
        let shares = Positive::new(fs("1.212")).unwrap();
        let err = shares.to_whole_shares().unwrap_err();
        assert!(
            matches!(err, InvalidSharesError::Fractional(value) if value == ed("1.212")),
            "Expected Fractional error with value 1.212, got: {err:?}"
        );
    }

    #[test]
    fn fractional_shares_is_whole_returns_true_for_whole_numbers() {
        let shares = fs("1");
        assert!(shares.is_whole().unwrap());

        let shares = fs("42");
        assert!(shares.is_whole().unwrap());
    }

    #[test]
    fn fractional_shares_is_whole_returns_false_for_fractional_values() {
        let shares = fs("1.5");
        assert!(!shares.is_whole().unwrap());

        let shares = fs("0.001");
        assert!(!shares.is_whole().unwrap());
    }

    #[test]
    fn add_succeeds() {
        let value_a = fs("1");
        let value_b = fs("2");
        let result = (value_a + value_b).unwrap();
        assert_eq!(result.inner(), ed("3"));
    }

    #[test]
    fn sub_succeeds() {
        let value_a = fs("5");
        let value_b = fs("2");
        let result = (value_a - value_b).unwrap();
        assert_eq!(result.inner(), ed("3"));
    }

    #[test]
    fn abs_returns_absolute_value() {
        let negative = fs("-1");
        assert_eq!(negative.abs().unwrap().inner(), ed("1"));
    }

    #[test]
    fn into_exact_decimal_extracts_inner_value() {
        let shares = fs("42");
        let exact: ExactDecimal = shares.into();
        assert_eq!(exact, ed("42"));
    }

    #[test]
    fn mul_succeeds() {
        let shares = fs("100");
        let ratio = fs("0.5");
        let result = (shares * ratio).unwrap();
        assert_eq!(result.inner(), ed("50"));
    }

    #[test]
    fn to_u256_18_decimals_zero_returns_zero() {
        let shares = FractionalShares::ZERO;
        let result = shares.to_u256_18_decimals().unwrap();
        assert_eq!(result, U256::ZERO);
    }

    #[test]
    fn to_u256_18_decimals_one_returns_10_pow_18() {
        let shares = fs("1");
        let result = shares.to_u256_18_decimals().unwrap();
        assert_eq!(result, U256::from_str("1000000000000000000").unwrap());
    }

    #[test]
    fn to_u256_18_decimals_fractional_value() {
        let shares = fs("1.5");
        let result = shares.to_u256_18_decimals().unwrap();
        assert_eq!(result, U256::from_str("1500000000000000000").unwrap());
    }

    #[test]
    fn to_u256_18_decimals_small_fractional_value() {
        let shares = fs("0.000000000000000001");
        let result = shares.to_u256_18_decimals().unwrap();
        assert_eq!(result, U256::from(1));
    }

    #[test]
    fn to_u256_18_decimals_negative_returns_error() {
        let shares = fs("-1");
        let err = shares.to_u256_18_decimals().unwrap_err();
        assert!(
            matches!(err, SharesConversionError::NegativeValue(_)),
            "Expected NegativeValue error, got: {err:?}"
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

    #[test]
    fn from_decimal_roundtrips() {
        let decimal = Decimal::new(12345, 3);
        let shares = FractionalShares::from_decimal(decimal).unwrap();
        let back = shares.to_decimal().unwrap();
        assert_eq!(decimal, back);
    }

    #[test]
    fn positive_rejects_zero() {
        let result = Positive::new(FractionalShares::ZERO);
        assert!(matches!(result, Err(InvalidSharesError::NonPositive(_))));
    }

    #[test]
    fn positive_rejects_negative() {
        let result = Positive::new(fs("-1"));
        assert!(matches!(result, Err(InvalidSharesError::NonPositive(_))));
    }

    #[test]
    fn positive_to_whole_roundtrips_integer() {
        let shares = Positive::new(fs("42")).unwrap();
        assert_eq!(shares.to_whole_shares().unwrap(), 42);
    }

    #[test]
    fn positive_to_whole_rejects_fractional() {
        let shares = Positive::new(fs("1.5")).unwrap();
        assert!(matches!(
            shares.to_whole_shares(),
            Err(InvalidSharesError::Fractional(_))
        ));
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

    #[test]
    fn from_u256_18_decimals_zero_returns_zero() {
        let result = FractionalShares::from_u256_18_decimals(U256::ZERO).unwrap();
        assert_eq!(result, FractionalShares::ZERO);
    }

    #[test]
    fn from_u256_18_decimals_one_whole_share() {
        let one_share = U256::from_str("1000000000000000000").unwrap();
        let result = FractionalShares::from_u256_18_decimals(one_share).unwrap();
        assert_eq!(result.inner(), ed("1"));
    }

    #[test]
    fn from_u256_18_decimals_fractional_amount() {
        let one_and_a_half = U256::from_str("1500000000000000000").unwrap();
        let result = FractionalShares::from_u256_18_decimals(one_and_a_half).unwrap();
        assert_eq!(result.inner(), ed("1.5"));
    }

    #[test]
    fn from_u256_18_decimals_overflow_returns_error() {
        let result = FractionalShares::from_u256_18_decimals(U256::MAX);
        let error = result.unwrap_err();
        assert!(
            matches!(error, SharesConversionError::FloatConversion(_)),
            "Expected FloatConversion error, got: {error:?}"
        );
    }

    #[test]
    fn div_by_zero_returns_float_error() {
        let numerator = fs("10");
        let zero = FractionalShares::ZERO;
        let result = numerator.0 / zero.0;
        assert!(result.is_err(), "Division by zero should return an error");
    }

    /// Generates arbitrary ExactDecimal values for property testing.
    fn arb_exact_decimal() -> impl Strategy<Value = ExactDecimal> {
        (any::<i64>(), 0u32..=10).prop_filter_map(
            "ExactDecimal::parse must succeed",
            |(mantissa, scale)| {
                let divisor = 10i64.checked_pow(scale).unwrap_or(1);
                let integer_part = mantissa / divisor;
                let frac_part = (mantissa % divisor).unsigned_abs();

                let value_str = format!(
                    "{integer_part}.{frac_part:0>width$}",
                    width = scale as usize
                );
                ExactDecimal::parse(&value_str).ok()
            },
        )
    }

    proptest! {
        #[test]
        fn fractional_shares_construction_preserves_value(
            value in arb_exact_decimal(),
        ) {
            let shares = FractionalShares::new(value);
            prop_assert_eq!(shares.inner(), value);
        }

        #[test]
        fn positive_rejects_zero_and_negative(
            value in arb_exact_decimal().prop_filter(
                "must be <= 0",
                |value| *value <= ExactDecimal::zero(),
            ),
        ) {
            let result = Positive::new(FractionalShares::new(value));
            prop_assert!(matches!(result, Err(InvalidSharesError::NonPositive(_))));
        }

        #[test]
        fn fractional_shares_is_whole_matches_frac_is_zero(
            value in arb_exact_decimal(),
        ) {
            let shares = FractionalShares::new(value);
            let is_whole = shares.is_whole().map_err(|error| {
                TestCaseError::Fail(format!("is_whole() failed: {error}").into())
            })?;
            let frac = value.frac().map_err(|error| {
                TestCaseError::Fail(format!("frac() failed: {error}").into())
            })?;
            let frac_is_zero = frac.is_zero().map_err(|error| {
                TestCaseError::Fail(format!("is_zero() failed: {error}").into())
            })?;
            prop_assert_eq!(is_whole, frac_is_zero);
        }

        #[test]
        fn positive_to_whole_roundtrips_integers(value in 1u64..=1_000_000u64) {
            let shares = Positive::new(
                FractionalShares::new(ExactDecimal::parse(&value.to_string()).unwrap())
            ).unwrap();
            prop_assert_eq!(shares.to_whole_shares().unwrap(), value);
        }

        #[test]
        fn positive_to_whole_rejects_fractional_values(
            whole in 0i64..=1_000_000,
            frac in 1u32..=999_999,
        ) {
            let value_str = format!("{whole}.{frac:06}");
            if let Ok(exact) = ExactDecimal::parse(&value_str)
                && exact > ExactDecimal::zero()
            {
                let shares = Positive::new(FractionalShares::new(exact)).unwrap();
                prop_assert!(matches!(
                    shares.to_whole_shares(),
                    Err(InvalidSharesError::Fractional(_))
                ));
            }
        }
    }
}
