mod onchain_trade;

pub(crate) use onchain_trade::{emit_trade_filled, log_event_error};

use sqlx::SqlitePool;

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
