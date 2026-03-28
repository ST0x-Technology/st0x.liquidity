//! Block cursor persistence for crash recovery.
//!
//! Tracks the last fully processed block number so the bot can resume
//! from where it left off after a restart, rather than re-scanning
//! from `deployment_block`.

use sqlx::SqlitePool;

/// Errors from block cursor operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum BlockCursorError {
    #[error("block number {0} exceeds i64::MAX")]
    BlockNumberOverflow(u64),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

/// Manages the persisted block cursor in SQLite.
pub(crate) struct BlockCursor<'pool> {
    pool: &'pool SqlitePool,
}

impl<'pool> BlockCursor<'pool> {
    pub(crate) fn new(pool: &'pool SqlitePool) -> Self {
        Self { pool }
    }

    /// Returns the last processed block, or `None` on first run.
    pub(crate) async fn last_block(&self) -> Result<Option<u64>, BlockCursorError> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT last_block FROM block_cursor WHERE id = 1")
                .fetch_optional(self.pool)
                .await?;

        row.map(|(block,)| {
            // The CHECK constraint on block_cursor guarantees last_block >= 0,
            // so negative values should be impossible. If one appears, treat
            // it as overflow (the u64 representation doesn't matter for the
            // error message — we use 0 as a sentinel).
            u64::try_from(block).map_err(|_| BlockCursorError::BlockNumberOverflow(0))
        })
        .transpose()
    }

    /// Persists the last processed block number.
    ///
    /// Uses INSERT OR REPLACE (upsert) since the table has a single row
    /// (id = 1).
    pub(crate) async fn update(&self, block: u64) -> Result<(), BlockCursorError> {
        let block_i64 =
            i64::try_from(block).map_err(|_| BlockCursorError::BlockNumberOverflow(block))?;

        sqlx::query(
            "INSERT OR REPLACE INTO block_cursor (id, last_block, updated_at) \
             VALUES (1, ?, CURRENT_TIMESTAMP)",
        )
        .bind(block_i64)
        .execute(self.pool)
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;

    use super::*;

    async fn test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn last_block_returns_none_on_first_run() {
        let pool = test_pool().await;
        let cursor = BlockCursor::new(&pool);

        assert_eq!(cursor.last_block().await.unwrap(), None);
    }

    #[tokio::test]
    async fn update_then_read_returns_saved_block() {
        let pool = test_pool().await;
        let cursor = BlockCursor::new(&pool);

        cursor.update(42).await.unwrap();
        assert_eq!(cursor.last_block().await.unwrap(), Some(42));
    }

    #[tokio::test]
    async fn update_overwrites_previous_value() {
        let pool = test_pool().await;
        let cursor = BlockCursor::new(&pool);

        cursor.update(100).await.unwrap();
        cursor.update(200).await.unwrap();
        assert_eq!(cursor.last_block().await.unwrap(), Some(200));
    }
}
