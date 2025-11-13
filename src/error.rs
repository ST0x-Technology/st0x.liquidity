//! Domain-specific error types following clean error handling architecture.
//! Separates concerns instead of mixing database, business logic, and external API errors.

use alloy::primitives::{B256, ruint::FromUintError};
use alloy::transports::{RpcError, TransportErrorKind};
use st0x_broker::order::status::ParseOrderStatusError;
use st0x_broker::{InvalidBrokerError, PersistenceError};
use std::num::ParseFloatError;

use crate::onchain::position_calculator::ConversionError;

/// Business logic validation errors for trade processing rules.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TradeValidationError {
    #[error("No transaction hash found in log")]
    NoTxHash,
    #[error("No log index found in log")]
    NoLogIndex,
    #[error("No block number found in log")]
    NoBlockNumber,
    #[error("Invalid IO index: {0}")]
    InvalidIndex(#[from] FromUintError<usize>),
    #[error("No input found at index: {0}")]
    NoInputAtIndex(usize),
    #[error("No output found at index: {0}")]
    NoOutputAtIndex(usize),
    #[error(
        "Expected IO to contain USDC and one tokenized equity (t prefix, 0x or s1 suffix) but got {0} and {1}"
    )]
    InvalidSymbolConfiguration(String, String),
    #[error(
        "Could not fully allocate execution shares for symbol {symbol}. Remaining: {remaining_shares}"
    )]
    InsufficientTradeAllocation {
        symbol: String,
        remaining_shares: f64,
    },
    #[error("Failed to convert U256 to f64: {0}")]
    U256ToF64(#[from] ParseFloatError),
    #[error("Transaction not found: {0}")]
    TransactionNotFound(B256),
    #[error("No AfterClear log found for ClearV2 log")]
    NoAfterClearLog,
    #[error("Negative shares amount: {0}")]
    NegativeShares(f64),
    #[error("Negative USDC amount: {0}")]
    NegativeUsdc(f64),
    #[error(
        "Symbol '{0}' is not a tokenized equity (must start with 't' or end with '0x' or 's1')"
    )]
    NotTokenizedEquity(String),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AlloyError {
    #[error("Failed to get symbol: {0}")]
    GetSymbol(#[from] alloy::contract::Error),
    #[error("Sol type error: {0}")]
    SolType(#[from] alloy::sol_types::Error),
    #[error("RPC transport error: {0}")]
    RpcTransport(#[from] RpcError<TransportErrorKind>),
}

/// Event queue persistence and processing errors.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EventQueueError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Event queue error: {0}")]
    Processing(String),
}

/// Event processing errors for live event handling.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EventProcessingError {
    #[error("Event queue error: {0}")]
    Queue(#[from] EventQueueError),
    #[error("Failed to enqueue ClearV3 event: {0}")]
    EnqueueClearV3(#[source] EventQueueError),
    #[error("Failed to enqueue TakeOrderV3 event: {0}")]
    EnqueueTakeOrderV3(#[source] EventQueueError),
    #[error("Failed to process trade through accumulator: {0}")]
    AccumulatorProcessing(String),
    #[error("Onchain trade processing error: {0}")]
    OnChain(#[from] OnChainError),
    #[error("Schwab execution error: {0}")]
    Schwab(#[from] st0x_broker::schwab::SchwabError),
    #[error("Broker error: {0}")]
    Broker(#[from] st0x_broker::BrokerError),
}

/// Order polling errors for order status monitoring.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OrderPollingError {
    #[error("Broker error: {0}")]
    Broker(Box<dyn std::error::Error + Send + Sync>),
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Persistence error: {0}")]
    Persistence(#[from] PersistenceError),
    #[error("Onchain error: {0}")]
    OnChain(#[from] OnChainError),
}

impl From<st0x_broker::BrokerError> for OrderPollingError {
    fn from(err: st0x_broker::BrokerError) -> Self {
        Self::Broker(Box::new(err))
    }
}

/// Unified error type for onchain trade processing with clear domain boundaries.
/// Provides error mapping between layers while maintaining separation of concerns.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OnChainError {
    #[error("Trade validation error: {0}")]
    Validation(#[from] TradeValidationError),
    #[error("Database persistence error: {0}")]
    Persistence(#[from] PersistenceError),
    #[error("Alloy error: {0}")]
    Alloy(#[from] AlloyError),
    #[error("Broker error: {0}")]
    Broker(#[from] st0x_broker::BrokerError),
    #[error("Event queue error: {0}")]
    EventQueue(#[from] EventQueueError),
    #[error("Order status parse error: {0}")]
    OrderStatusParse(#[from] ParseOrderStatusError),
    #[error("Invalid broker: {0}")]
    InvalidBroker(#[from] InvalidBrokerError),
    #[error("Numeric conversion error: {0}")]
    Conversion(#[from] ConversionError),
    #[error("Float/decimal conversion error: {0}")]
    FloatConversion(String),
}

impl From<sqlx::Error> for OnChainError {
    fn from(err: sqlx::Error) -> Self {
        Self::Persistence(PersistenceError::Database(err))
    }
}

impl From<alloy::contract::Error> for OnChainError {
    fn from(err: alloy::contract::Error) -> Self {
        Self::Alloy(AlloyError::GetSymbol(err))
    }
}

impl From<ParseFloatError> for OnChainError {
    fn from(err: ParseFloatError) -> Self {
        Self::Validation(TradeValidationError::U256ToF64(err))
    }
}

impl From<st0x_broker::InvalidDirectionError> for OnChainError {
    fn from(err: st0x_broker::InvalidDirectionError) -> Self {
        Self::Persistence(PersistenceError::InvalidDirection(err))
    }
}

impl From<FromUintError<usize>> for OnChainError {
    fn from(err: FromUintError<usize>) -> Self {
        Self::Validation(TradeValidationError::InvalidIndex(err))
    }
}

impl From<alloy::sol_types::Error> for OnChainError {
    fn from(err: alloy::sol_types::Error) -> Self {
        Self::Alloy(AlloyError::SolType(err))
    }
}

impl From<RpcError<TransportErrorKind>> for OnChainError {
    fn from(err: RpcError<TransportErrorKind>) -> Self {
        Self::Alloy(AlloyError::RpcTransport(err))
    }
}
