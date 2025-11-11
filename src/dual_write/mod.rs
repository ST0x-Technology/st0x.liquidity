use sqlx::SqlitePool;

#[derive(Debug, thiserror::Error)]
pub(crate) enum DualWriteError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
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
