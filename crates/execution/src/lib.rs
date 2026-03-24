//! Unified executor trait and implementations for brokerage integration.

use async_trait::async_trait;
use rain_math_float::{Float, FloatError};
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
use tokio::task::JoinHandle;

pub(crate) use st0x_float_serde::{
    deserialize_float_from_number_or_string, deserialize_option_float_from_number_or_string,
    serialize_float_as_string,
};

pub use st0x_float_macro::float;

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

pub use st0x_finance::{
    EmptySymbolError, FractionalShares, HasZero, NotPositive, Positive, Symbol, ToWholeSharesError,
};

/// Extension trait for U256 conversions on `FractionalShares`.
///
/// These depend on alloy types and therefore live in st0x-execution
/// rather than the leaf st0x-finance crate.
pub trait SharesBlockchain {
    /// Converts to U256 with 18 decimal places (standard ERC20 decimals).
    ///
    /// Uses lossy conversion because Float's 224-bit coefficient may carry
    /// more than 18 decimal places of precision, but ERC-20 tokens are 18
    /// decimals so the extra precision is representational noise.
    fn to_u256_18_decimals(self) -> Result<alloy::primitives::U256, SharesConversionError>;

    /// Creates `FractionalShares` from a U256 value with 18 decimal places.
    ///
    /// # Errors
    ///
    /// Returns [`SharesConversionError`] if the Float conversion fails.
    fn from_u256_18_decimals(
        value: alloy::primitives::U256,
    ) -> Result<FractionalShares, SharesConversionError>;
}

impl SharesBlockchain for FractionalShares {
    fn to_u256_18_decimals(self) -> Result<alloy::primitives::U256, SharesConversionError> {
        if self.is_negative()? {
            return Err(SharesConversionError::NegativeValue(self.inner()));
        }

        if self.is_zero()? {
            return Ok(alloy::primitives::U256::ZERO);
        }

        self.inner()
            .to_fixed_decimal_lossy(18)
            .map(|(fixed, _lossless)| fixed)
            .map_err(SharesConversionError::FloatConversion)
    }

    fn from_u256_18_decimals(
        value: alloy::primitives::U256,
    ) -> Result<FractionalShares, SharesConversionError> {
        if value.is_zero() {
            return Ok(Self::ZERO);
        }

        Float::from_fixed_decimal(value, 18)
            .map(Self::new)
            .map_err(SharesConversionError::FloatConversion)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SharesConversionError {
    #[error("shares value cannot be negative: {0:?}")]
    NegativeValue(Float),
    #[error("Float conversion failed: {0}")]
    FloatConversion(#[from] FloatError),
}

/// Alpaca supports a maximum of 9 decimal places for order quantities.
pub(crate) const ALPACA_MAX_DECIMAL_PLACES: usize = 9;

/// Truncates a decimal string to at most `max_decimals` decimal places.
///
/// Truncation (floor) is used rather than rounding because rounding up could
/// cause an order for more shares than we actually have.
pub(crate) fn truncate_decimal_places(formatted: &str, max_decimals: usize) -> String {
    let Some(dot_index) = formatted.find('.') else {
        return formatted.to_string();
    };

    let decimal_digits = formatted.len() - dot_index - 1;

    if decimal_digits <= max_decimals {
        return formatted.to_string();
    }

    let truncated = &formatted[..dot_index + 1 + max_decimals];

    // Strip trailing zeros after truncation (e.g., "1.500000000" -> "1.5")
    let trimmed = truncated.trim_end_matches('0').trim_end_matches('.');
    trimmed.to_string()
}

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

#[derive(Debug, thiserror::Error)]
pub enum InvalidSharesError {
    #[error("Shares cannot be zero")]
    Zero,
    #[error(transparent)]
    NotPositive(#[from] NotPositive<FractionalShares>),
    #[error(transparent)]
    WholeShares(#[from] ToWholeSharesError),
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),
    #[error("Float conversion failed: {0}")]
    FloatConversion(#[from] FloatError),
}

impl From<SharesConversionError> for InvalidSharesError {
    fn from(error: SharesConversionError) -> Self {
        match error {
            SharesConversionError::NegativeValue(value) => Self::NotPositive(NotPositive {
                value: FractionalShares::new(value),
            }),
            SharesConversionError::FloatConversion(error) => Self::FloatConversion(error),
        }
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
#[derive(Debug, Clone)]
pub struct EquityPosition {
    pub symbol: Symbol,
    pub quantity: FractionalShares,
    pub market_value: Option<Float>,
}

/// Account state from the broker.
#[derive(Debug, Clone)]
pub struct Inventory {
    pub positions: Vec<EquityPosition>,
    pub cash_balance_cents: i64,
}

/// Result of fetching inventory from an executor.
///
/// Custom enum to force explicit handling. Unlike `Option` which is easy to `.unwrap()`,
/// this type requires callers to explicitly match on the `Unimplemented` variant.
#[derive(Debug, Clone)]
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
    #[error("Float operation failed: {0}")]
    Float(#[from] FloatError),
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
    use std::str::FromStr;

    use super::*;

    #[test]
    fn positive_to_whole_shares_succeeds_for_whole_numbers() {
        let shares = Positive::new(FractionalShares::new(float!(5))).unwrap();
        assert_eq!(shares.to_whole_shares().unwrap(), 5);

        let shares = Positive::new(FractionalShares::new(float!(100))).unwrap();
        assert_eq!(shares.to_whole_shares().unwrap(), 100);
    }

    #[test]
    fn positive_to_whole_shares_errors_for_fractional_values() {
        let shares = Positive::new(FractionalShares::new(float!(1.212))).unwrap();
        let err = shares.to_whole_shares().unwrap_err();
        assert!(matches!(err, ToWholeSharesError::Fractional(_)));
    }

    #[test]
    fn fractional_shares_is_whole_returns_true_for_whole_numbers() {
        assert!(FractionalShares::new(float!(1)).is_whole().unwrap());
        assert!(FractionalShares::new(float!(42)).is_whole().unwrap());
    }

    #[test]
    fn fractional_shares_is_whole_returns_false_for_fractional_values() {
        assert!(!FractionalShares::new(float!(1.5)).is_whole().unwrap());
        assert!(!FractionalShares::new(float!(0.001)).is_whole().unwrap());
    }

    #[test]
    fn add_succeeds() {
        let result = (FractionalShares::new(float!(1)) + FractionalShares::new(float!(2))).unwrap();
        assert!(result.inner().eq(float!(3)).unwrap());
    }

    #[test]
    fn sub_succeeds() {
        let result = (FractionalShares::new(float!(5)) - FractionalShares::new(float!(2))).unwrap();
        assert!(result.inner().eq(float!(3)).unwrap());
    }

    #[test]
    fn abs_returns_absolute_value() {
        let result = FractionalShares::new(float!(-1)).abs().unwrap();
        assert!(result.inner().eq(float!(1)).unwrap());
    }

    #[test]
    fn into_float_extracts_inner_value() {
        let float: Float = FractionalShares::new(float!(42)).into();
        assert!(float.eq(float!(42)).unwrap());
    }

    #[test]
    fn mul_float_succeeds() {
        let result = (FractionalShares::new(float!(100)) * float!(0.5)).unwrap();
        assert!(result.inner().eq(float!(50)).unwrap());
    }

    #[test]
    fn to_u256_18_decimals_zero_returns_zero() {
        let result = FractionalShares::ZERO.to_u256_18_decimals().unwrap();
        assert_eq!(result, U256::ZERO);
    }

    #[test]
    fn to_u256_18_decimals_one_returns_10_pow_18() {
        let result = FractionalShares::new(float!(1))
            .to_u256_18_decimals()
            .unwrap();
        assert_eq!(result, U256::from_str("1000000000000000000").unwrap());
    }

    #[test]
    fn to_u256_18_decimals_fractional_value() {
        let result = FractionalShares::new(float!(1.5))
            .to_u256_18_decimals()
            .unwrap();
        assert_eq!(result, U256::from_str("1500000000000000000").unwrap());
    }

    #[test]
    fn to_u256_18_decimals_negative_returns_error() {
        let err = FractionalShares::new(float!(-1))
            .to_u256_18_decimals()
            .unwrap_err();
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

        let symbol = Symbol::new("ABCDEFGHIJ").unwrap();
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
    fn from_u256_18_decimals_zero_returns_zero() {
        let result = FractionalShares::from_u256_18_decimals(U256::ZERO).unwrap();
        assert_eq!(result, FractionalShares::ZERO);
    }

    #[test]
    fn from_u256_18_decimals_one_whole_share() {
        let one_share = U256::from_str("1000000000000000000").unwrap();
        let result = FractionalShares::from_u256_18_decimals(one_share).unwrap();
        assert!(result.inner().eq(float!(1)).unwrap());
    }

    #[test]
    fn from_u256_18_decimals_fractional_amount() {
        let one_and_a_half = U256::from_str("1500000000000000000").unwrap();
        let result = FractionalShares::from_u256_18_decimals(one_and_a_half).unwrap();
        assert!(result.inner().eq(float!(1.5)).unwrap());
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
    fn truncate_no_decimal_point_unchanged() {
        assert_eq!(truncate_decimal_places("100", 9), "100");
    }

    #[test]
    fn truncate_fewer_decimals_unchanged() {
        assert_eq!(truncate_decimal_places("1.5", 9), "1.5");
        assert_eq!(truncate_decimal_places("0.123456789", 9), "0.123456789");
    }

    #[test]
    fn truncate_excess_decimals() {
        assert_eq!(
            truncate_decimal_places("0.996350331351928059", 9),
            "0.996350331"
        );
    }

    #[test]
    fn truncate_strips_trailing_zeros() {
        assert_eq!(truncate_decimal_places("1.500000000000000000", 9), "1.5");
        assert_eq!(truncate_decimal_places("2.000000000000000000", 9), "2");
    }

    #[test]
    fn truncate_zero_decimal_places() {
        assert_eq!(truncate_decimal_places("1.234", 0), "1");
    }
}
