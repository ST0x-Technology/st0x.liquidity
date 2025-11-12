use sqlite_es::{SqliteCqrs, sqlite_cqrs};
use sqlx::SqlitePool;

use crate::offchain_order::OffchainOrder;
use crate::onchain_trade::OnChainTrade;
use crate::position::Position;

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
}

#[derive(Clone)]
pub(crate) struct DualWriteContext {
    pool: SqlitePool,
}

impl DualWriteContext {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}
