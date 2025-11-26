use alloy::primitives::TxHash;
use cqrs_es::AggregateError;
use sqlite_es::{SqliteCqrs, sqlite_cqrs};
use sqlx::SqlitePool;

use crate::offchain_order::{OffchainOrder, OffchainOrderError};
use crate::onchain_trade::{OnChainTrade, OnChainTradeError};
use crate::position::{Position, PositionError};

mod onchain_trade;

pub(crate) use onchain_trade::emit_trade_filled;

#[derive(Debug, thiserror::Error)]
pub(crate) enum DualWriteError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
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
}

pub(crate) struct DualWriteContext {
    onchain_trade: SqliteCqrs<OnChainTrade>,
    position: SqliteCqrs<Position>,
    offchain_order: SqliteCqrs<OffchainOrder>,
}

impl DualWriteContext {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self {
            onchain_trade: sqlite_cqrs(pool.clone(), vec![], ()),
            position: sqlite_cqrs(pool.clone(), vec![], ()),
            offchain_order: sqlite_cqrs(pool, vec![], ()),
        }
    }

    pub(crate) fn onchain_trade_framework(&self) -> &SqliteCqrs<OnChainTrade> {
        &self.onchain_trade
    }

    pub(crate) fn position_framework(&self) -> &SqliteCqrs<Position> {
        &self.position
    }

    pub(crate) fn offchain_order_framework(&self) -> &SqliteCqrs<OffchainOrder> {
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
