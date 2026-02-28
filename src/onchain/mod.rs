//! Onchain event processing: EVM configuration, trade
//! parsing, event backfilling, position accumulation, and
//! vault management.

use alloy::primitives::ruint::FromUintError;
use alloy::primitives::{Address, address};
use alloy::transports::{RpcError, TransportErrorKind};
use rain_math_float::FloatError;
use serde::Deserialize;
use std::num::TryFromIntError;
use url::Url;

use st0x_event_sorcery::ProjectionError;
use st0x_execution::order::status::ParseOrderStatusError;
use st0x_execution::{
    EmptySymbolError, ExecutionError, FractionalShares, InvalidDirectionError,
    InvalidExecutorError, InvalidSharesError, NotPositive, PersistenceError, SharesConversionError,
};

use crate::position::{Position, PositionError};
use crate::queue::EventQueueError;

pub(crate) mod accumulator;
pub(crate) mod backfill;
mod clear;
pub(crate) mod io;
#[cfg(test)]
pub(crate) mod mock;
pub(crate) mod pyth;
pub(crate) mod raindex;
mod take_order;
pub(crate) mod trade;

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
    pub(crate) deployment_block: u64,
}

impl std::fmt::Debug for EvmCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvmCtx")
            .field("ws_rpc_url", &"[REDACTED]")
            .field("orderbook", &self.orderbook)
            .field("deployment_block", &self.deployment_block)
            .finish()
    }
}

impl EvmCtx {
    pub(crate) fn new(config: &EvmConfig, secrets: EvmSecrets) -> Self {
        Self {
            ws_rpc_url: secrets.ws_rpc_url,
            orderbook: config.orderbook,
            deployment_block: config.deployment_block,
        }
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
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("EVM error: {0}")]
    Evm(#[from] st0x_evm::EvmError),
    #[error("Sol type error: {0}")]
    SolType(#[from] alloy::sol_types::Error),
    #[error("RPC transport error: {0}")]
    RpcTransport(#[from] RpcError<TransportErrorKind>),
    #[error("Invalid IO index: {0}")]
    InvalidIndex(#[from] FromUintError<usize>),
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
    #[error(transparent)]
    NotPositive(#[from] NotPositive<FractionalShares>),
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("UUID parse error: {0}")]
    Uuid(#[from] uuid::Error),
    #[error("Position projection error: {0}")]
    PositionProjection(#[from] ProjectionError<Position>),
    #[error("Market hours check failed")]
    MarketHoursCheck(#[source] Box<dyn std::error::Error + Send + Sync>),
}

pub(crate) const USDC_ETHEREUM: Address = address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
pub(crate) const USDC_BASE: Address = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
pub(crate) const USDC_ETHEREUM_SEPOLIA: Address =
    address!("0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238");
