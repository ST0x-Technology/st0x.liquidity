//! Onchain event processing: EVM configuration, trade
//! parsing, event backfilling, position accumulation, and
//! vault management.

use alloy::primitives::ruint::FromUintError;
use alloy::primitives::{Address, address};
use alloy::rpc::client::RpcClient;
use alloy::transports::layers::RetryBackoffLayer;
use alloy::transports::{RpcError, TransportErrorKind};
use rain_math_float::FloatError;
use serde::Deserialize;
use std::num::TryFromIntError;
use url::Url;

use st0x_execution::order::status::ParseOrderStatusError;
use st0x_execution::{
    EmptySymbolError, ExecutionError, InvalidDirectionError, InvalidExecutorError,
    InvalidSharesError, PersistenceError, SharesConversionError,
};

use crate::position::PositionError;
use crate::queue::EventQueueError;

pub(crate) mod accumulator;
pub(crate) mod backfill;
mod clear;
pub(crate) mod io;
pub(crate) mod pyth;
mod take_order;
pub(crate) mod trade;
pub(crate) mod vault;

pub(crate) use trade::OnchainTrade;
pub(crate) use trade::TradeValidationError;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EvmConfig {
    pub(crate) orderbook: Address,
    pub(crate) order_owner: Option<Address>,
    pub(crate) deployment_block: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EvmSecrets {
    pub(crate) ws_rpc_url: Url,
}

#[derive(Clone)]
pub(crate) struct EvmCtx {
    pub(crate) ws_rpc_url: Url,
    pub(crate) orderbook: Address,
    pub(crate) order_owner: Option<Address>,
    pub(crate) deployment_block: u64,
}

impl std::fmt::Debug for EvmCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvmCtx")
            .field("ws_rpc_url", &"[REDACTED]")
            .field("orderbook", &self.orderbook)
            .field("order_owner", &self.order_owner)
            .field("deployment_block", &self.deployment_block)
            .finish()
    }
}

impl EvmCtx {
    pub(crate) fn new(config: &EvmConfig, secrets: EvmSecrets) -> Self {
        Self {
            ws_rpc_url: secrets.ws_rpc_url,
            orderbook: config.orderbook,
            order_owner: config.order_owner,
            deployment_block: config.deployment_block,
        }
    }
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
    #[error("Execution error: {0}")]
    Execution(#[from] ExecutionError),
    #[error("Event queue error: {0}")]
    EventQueue(#[from] EventQueueError),
    #[error("Order status parse error: {0}")]
    OrderStatusParse(#[from] ParseOrderStatusError),
    #[error("Invalid executor: {0}")]
    InvalidExecutor(#[from] InvalidExecutorError),
    #[error("Float conversion error: {0}")]
    FloatConversion(#[from] FloatError),
    #[error("Integer conversion error: {0}")]
    IntConversion(#[from] TryFromIntError),
    #[error(transparent)]
    EmptySymbol(#[from] EmptySymbolError),
    #[error(transparent)]
    InvalidShares(#[from] InvalidSharesError),
    #[error(transparent)]
    InvalidDirection(#[from] InvalidDirectionError),
    #[error("Position error: {0}")]
    Position(#[from] PositionError),
    #[error("Shares conversion error: {0}")]
    SharesConversion(#[from] SharesConversionError),
    #[error("Decimal parse error: {0}")]
    DecimalParse(#[from] rust_decimal::Error),
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("UUID parse error: {0}")]
    Uuid(#[from] uuid::Error),
    #[error("Projection query error: {0}")]
    Projection(#[from] st0x_event_sorcery::ProjectionError),
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

pub(crate) const USDC_ETHEREUM: Address = address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
pub(crate) const USDC_BASE: Address = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
pub(crate) const USDC_ETHEREUM_SEPOLIA: Address =
    address!("0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238");

/// Number of block confirmations to wait after transactions before subsequent
/// operations that depend on the state change. This ensures state propagates
/// across load-balanced RPC providers (like dRPC) that may route requests to
/// different backend nodes.
pub(crate) const REQUIRED_CONFIRMATIONS: u64 = 3;

/// Maximum retries for transient RPC errors (rate limits, null responses, etc.)
const RPC_MAX_RETRIES: u32 = 10;

/// Initial backoff duration in milliseconds before retrying
const RPC_INITIAL_BACKOFF_MS: u64 = 1000;

/// Compute units per second budget for rate limiting
const RPC_COMPUTE_UNITS_PER_SECOND: u64 = 100;

/// Creates an HTTP RPC client with retry layer for transient errors.
///
/// Use with `ProviderBuilder::new().connect_client(client)` for read-only calls,
/// or `ProviderBuilder::new().wallet(w).connect_client(client)` for signing.
pub(crate) fn http_client_with_retry(url: Url) -> RpcClient {
    let retry_layer = RetryBackoffLayer::new(
        RPC_MAX_RETRIES,
        RPC_INITIAL_BACKOFF_MS,
        RPC_COMPUTE_UNITS_PER_SECOND,
    );
    RpcClient::builder().layer(retry_layer).http(url)
}
