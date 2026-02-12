//! Dual-write layer for CQRS/ES and legacy persistence during migration.
//!
//! Writes to both the new event-sourced aggregates and legacy SQLite tables,
//! ensuring consistency while the codebase transitions to full CQRS/ES.

use std::sync::Arc;

use alloy::primitives::TxHash;
use cqrs_es::AggregateError;
use sqlite_es::SqliteCqrs;
#[cfg(test)]
use sqlite_es::sqlite_cqrs;
use sqlx::SqlitePool;
use st0x_execution::PersistenceError;

use crate::lifecycle::{Lifecycle, LifecycleError, Never};
use crate::offchain_order::{NegativePriceCents, OffchainOrder, OffchainOrderError};
use crate::onchain_trade::{OnChainTrade, OnChainTradeError};
use crate::position::{Position, PositionError};
use crate::shares::{ArithmeticError, FractionalShares};
use crate::threshold::ExecutionThreshold;

mod offchain_order;
mod onchain_trade;
mod position;

pub(crate) use offchain_order::{confirm_submission, mark_failed, place_order, record_fill};
pub(crate) use onchain_trade::witness_trade;
pub(crate) use position::{
    acknowledge_onchain_fill, complete_offchain_order, fail_offchain_order, initialize_position,
    load_position, place_offchain_order,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum DualWriteError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Persistence error: {0}")]
    Persistence(#[from] PersistenceError),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Integer conversion error: {0}")]
    IntConversion(#[from] std::num::TryFromIntError),
    #[error("Decimal conversion error: {0}")]
    DecimalConversion(#[from] rust_decimal::Error),
    #[error("OnChainTrade aggregate error: {0}")]
    OnChainTradeAggregate(#[from] AggregateError<OnChainTradeError>),
    #[error("Position aggregate error: {0}")]
    PositionAggregate(#[from] AggregateError<PositionError>),
    #[error("OffchainOrder aggregate error: {0}")]
    OffchainOrderAggregate(#[from] AggregateError<OffchainOrderError>),
    #[error("Missing block timestamp for trade: tx_hash={tx_hash:?}, log_index={log_index}")]
    MissingBlockTimestamp { tx_hash: TxHash, log_index: u64 },
    #[error("Missing execution ID")]
    MissingExecutionId,
    #[error(
        "Invalid order state for execution {execution_id}: expected {expected}, got different state"
    )]
    InvalidOrderState { execution_id: i64, expected: String },
    #[error("Negative price in cents: {0}")]
    NegativePriceCents(#[from] NegativePriceCents),
    #[error("Position aggregate {aggregate_id} is in failed state: {error}")]
    PositionAggregateFailed {
        aggregate_id: String,
        error: LifecycleError<ArithmeticError<FractionalShares>>,
    },
}

#[derive(Clone)]
pub(crate) struct DualWriteContext {
    pool: SqlitePool,
    onchain_trade: Arc<SqliteCqrs<Lifecycle<OnChainTrade, Never>>>,
    position: Arc<SqliteCqrs<Lifecycle<Position, ArithmeticError<FractionalShares>>>>,
    offchain_order: Arc<SqliteCqrs<Lifecycle<OffchainOrder, Never>>>,
    execution_threshold: ExecutionThreshold,
}

impl DualWriteContext {
    #[cfg(test)]
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self {
            pool: pool.clone(),
            onchain_trade: Arc::new(sqlite_cqrs(pool.clone(), vec![], ())),
            position: Arc::new(sqlite_cqrs(pool.clone(), vec![], ())),
            offchain_order: Arc::new(sqlite_cqrs(pool, vec![], ())),
            execution_threshold: ExecutionThreshold::whole_share(),
        }
    }

    pub(crate) fn with_threshold(
        pool: SqlitePool,
        onchain_trade: Arc<SqliteCqrs<Lifecycle<OnChainTrade, Never>>>,
        position: Arc<SqliteCqrs<Lifecycle<Position, ArithmeticError<FractionalShares>>>>,
        offchain_order: Arc<SqliteCqrs<Lifecycle<OffchainOrder, Never>>>,
        execution_threshold: ExecutionThreshold,
    ) -> Self {
        Self {
            pool,
            onchain_trade,
            position,
            offchain_order,
            execution_threshold,
        }
    }

    pub(crate) fn execution_threshold(&self) -> ExecutionThreshold {
        self.execution_threshold
    }

    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub(crate) fn onchain_trade_framework(&self) -> &SqliteCqrs<Lifecycle<OnChainTrade, Never>> {
        &self.onchain_trade
    }

    pub(crate) fn position_framework(
        &self,
    ) -> &SqliteCqrs<Lifecycle<Position, ArithmeticError<FractionalShares>>> {
        &self.position
    }

    pub(crate) fn offchain_order_framework(&self) -> &SqliteCqrs<Lifecycle<OffchainOrder, Never>> {
        &self.offchain_order
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn create_test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn test_dual_write_context_initialization() {
        let pool = create_test_pool().await;

        let context = DualWriteContext::new(pool);

        let _onchain_trade_fw = context.onchain_trade_framework();
        let _position_fw = context.position_framework();
        let _offchain_order_fw = context.offchain_order_framework();
    }
}
