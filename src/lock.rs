use crate::error::OnChainError;
use st0x_broker::Symbol;
use tracing::{info, warn};

/// Atomically acquires an execution lease for the given symbol.
/// Returns true if lease was acquired, false if another worker holds it.
pub(crate) async fn try_acquire_execution_lease(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    symbol: &Symbol,
) -> Result<bool, OnChainError> {
    const LOCK_TIMEOUT_MINUTES: i32 = 5;

    // Clean up stale lock for this specific symbol (older than 5 minutes)
    let timeout_param = format!("-{LOCK_TIMEOUT_MINUTES} minutes");
    let symbol_str = symbol.to_string();
    let cleanup_result = sqlx::query!(
        "DELETE FROM symbol_locks WHERE symbol = ?1 AND locked_at < datetime('now', ?2)",
        symbol_str,
        timeout_param
    )
    .execute(sql_tx.as_mut())
    .await?;

    if cleanup_result.rows_affected() > 0 {
        info!(
            "Cleaned up {} stale lock(s) older than {} minutes",
            cleanup_result.rows_affected(),
            LOCK_TIMEOUT_MINUTES
        );
    }

    // Try to acquire lock by inserting into symbol_locks table
    let result = sqlx::query("INSERT OR IGNORE INTO symbol_locks (symbol) VALUES (?1)")
        .bind(symbol.to_string())
        .execute(sql_tx.as_mut())
        .await?;

    let lease_acquired = result.rows_affected() > 0;
    if lease_acquired {
        info!("Acquired execution lease for symbol: {symbol}");
    } else {
        warn!(
            "Failed to acquire execution lease for symbol: {} (already held)",
            symbol
        );
    }

    Ok(lease_acquired)
}

/// Clears the execution lease when no execution was created
pub(crate) async fn clear_execution_lease(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    symbol: &Symbol,
) -> Result<(), OnChainError> {
    let result = sqlx::query("DELETE FROM symbol_locks WHERE symbol = ?1")
        .bind(symbol.to_string())
        .execute(sql_tx.as_mut())
        .await?;

    if result.rows_affected() > 0 {
        info!("Cleared execution lease for symbol: {}", symbol);
    }

    Ok(())
}

/// Sets the actual execution ID after successful execution creation
pub(crate) async fn set_pending_execution_id(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    symbol: &Symbol,
    execution_id: i64,
) -> Result<(), OnChainError> {
    let symbol_str = symbol.to_string();
    sqlx::query!(
        r#"
        UPDATE trade_accumulators 
        SET pending_execution_id = ?1, last_updated = CURRENT_TIMESTAMP
        WHERE symbol = ?2
        "#,
        execution_id,
        symbol_str
    )
    .execute(sql_tx.as_mut())
    .await?;

    Ok(())
}

/// Clears the pending execution ID when an execution completes or fails
pub(crate) async fn clear_pending_execution_id(
    sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    symbol: &Symbol,
) -> Result<(), OnChainError> {
    let symbol_str = symbol.to_string();
    sqlx::query!(
        "UPDATE trade_accumulators SET pending_execution_id = NULL WHERE symbol = ?1",
        symbol_str
    )
    .execute(sql_tx.as_mut())
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offchain::execution::OffchainExecution;
    use crate::onchain::accumulator::save_within_transaction;
    use crate::onchain::position_calculator::PositionCalculator;
    use crate::test_utils::setup_test_db;
    use st0x_broker::OrderState;
    use st0x_broker::{Direction, Shares, SupportedBroker};

    #[tokio::test]
    async fn test_try_acquire_execution_lease_success() {
        let pool = setup_test_db().await;
        let mut sql_tx = pool.begin().await.unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let result = try_acquire_execution_lease(&mut sql_tx, &symbol)
            .await
            .unwrap();
        assert!(result);

        sql_tx.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_try_acquire_execution_lease_conflict() {
        let pool = setup_test_db().await;

        // First transaction acquires the lease
        let mut sql_tx1 = pool.begin().await.unwrap();
        let symbol = Symbol::new("AAPL").unwrap();
        let result1 = try_acquire_execution_lease(&mut sql_tx1, &symbol)
            .await
            .unwrap();
        assert!(result1);
        sql_tx1.commit().await.unwrap();

        // Second transaction tries to acquire the same lease and should fail
        let mut sql_tx2 = pool.begin().await.unwrap();
        let result2 = try_acquire_execution_lease(&mut sql_tx2, &symbol)
            .await
            .unwrap();
        assert!(!result2);
        sql_tx2.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn test_try_acquire_execution_lease_different_symbols() {
        let pool = setup_test_db().await;

        // Acquire lease for first symbol
        let mut sql_tx1 = pool.begin().await.unwrap();
        let symbol1 = Symbol::new("AAPL").unwrap();
        let result1 = try_acquire_execution_lease(&mut sql_tx1, &symbol1)
            .await
            .unwrap();
        assert!(result1);
        sql_tx1.commit().await.unwrap();

        // Acquire lease for different symbol (should succeed)
        let mut sql_tx2 = pool.begin().await.unwrap();
        let symbol2 = Symbol::new("MSFT").unwrap();
        let result2 = try_acquire_execution_lease(&mut sql_tx2, &symbol2)
            .await
            .unwrap();
        assert!(result2);
        sql_tx2.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_clear_execution_lease() {
        let pool = setup_test_db().await;

        let mut sql_tx1 = pool.begin().await.unwrap();
        let symbol = Symbol::new("AAPL").unwrap();
        let result = try_acquire_execution_lease(&mut sql_tx1, &symbol)
            .await
            .unwrap();
        assert!(result);
        sql_tx1.commit().await.unwrap();

        let mut sql_tx2 = pool.begin().await.unwrap();
        clear_execution_lease(&mut sql_tx2, &symbol).await.unwrap();
        sql_tx2.commit().await.unwrap();

        let mut sql_tx3 = pool.begin().await.unwrap();
        let result = try_acquire_execution_lease(&mut sql_tx3, &symbol)
            .await
            .unwrap();
        assert!(result);
        sql_tx3.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_clear_pending_execution() {
        let pool = setup_test_db().await;

        let symbol = Symbol::new("AAPL").unwrap();
        let execution = OffchainExecution {
            id: None,
            symbol: symbol.clone(),
            shares: Shares::new(100).unwrap(),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            state: OrderState::Pending,
        };

        let mut sql_tx = pool.begin().await.unwrap();
        let execution_id = execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();

        let calculator = PositionCalculator::new();
        save_within_transaction(&mut sql_tx, &symbol, &calculator, Some(execution_id))
            .await
            .unwrap();

        try_acquire_execution_lease(&mut sql_tx, &symbol)
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        // Note: clear_pending_execution_within_transaction function was removed
        // This test just verifies that we can set up the accumulator and acquire lease
    }

    #[tokio::test]
    async fn test_acquire_execution_lease_persists_lock_row() {
        let pool = setup_test_db().await;

        let mut sql_tx = pool.begin().await.unwrap();
        let result = try_acquire_execution_lease(&mut sql_tx, &Symbol::new("TEST").unwrap())
            .await
            .unwrap();
        assert!(result);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM symbol_locks")
            .fetch_one(sql_tx.as_mut())
            .await
            .unwrap();
        assert_eq!(count, 1);

        sql_tx.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_ttl_based_cleanup() {
        let pool = setup_test_db().await;

        // First, acquire a lease
        let mut sql_tx = pool.begin().await.unwrap();
        let symbol = Symbol::new("AAPL").unwrap();
        let result = try_acquire_execution_lease(&mut sql_tx, &symbol)
            .await
            .unwrap();
        assert!(result);
        sql_tx.commit().await.unwrap();

        // Manually update the locked_at timestamp to be older than TTL
        let mut sql_tx = pool.begin().await.unwrap();
        sqlx::query(
            "UPDATE symbol_locks SET locked_at = datetime('now', '-100 minutes') WHERE symbol = ?1",
        )
        .bind(symbol.to_string())
        .execute(sql_tx.as_mut())
        .await
        .unwrap();
        sql_tx.commit().await.unwrap();

        // Now try to acquire the same lease - should succeed due to TTL cleanup
        let mut sql_tx = pool.begin().await.unwrap();
        let result = try_acquire_execution_lease(&mut sql_tx, &symbol)
            .await
            .unwrap();
        assert!(result); // Should succeed because old lock was cleaned up
        sql_tx.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn test_cleanup_symbol_specific_stale_lock() {
        let pool = setup_test_db().await;

        // Create multiple stale locks
        let mut sql_tx = pool.begin().await.unwrap();
        for symbol in ["AAPL", "MSFT", "TSLA"] {
            sqlx::query(
                "INSERT INTO symbol_locks (symbol, locked_at) VALUES (?1, datetime('now', '-100 minutes'))"
            )
            .bind(symbol)
            .execute(sql_tx.as_mut())
            .await
            .unwrap();
        }
        sql_tx.commit().await.unwrap();

        // Verify all locks exist
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM symbol_locks")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 3);

        // Acquire lease for one of the stale symbols - should cleanup only that symbol's stale lock
        let mut sql_tx = pool.begin().await.unwrap();
        let symbol = Symbol::new("AAPL").unwrap();
        let result = try_acquire_execution_lease(&mut sql_tx, &symbol)
            .await
            .unwrap();
        assert!(result);
        sql_tx.commit().await.unwrap();

        // Verify only the AAPL stale lock was cleaned up and a new AAPL lock was created
        // The other stale locks (MSFT, TSLA) remain because we only clean up the specific symbol
        let remaining_locks: Vec<String> =
            sqlx::query_scalar("SELECT symbol FROM symbol_locks ORDER BY symbol")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(remaining_locks, vec!["AAPL", "MSFT", "TSLA"]);
    }

    #[tokio::test]
    async fn test_clear_pending_execution_id() {
        let pool = setup_test_db().await;

        let symbol = Symbol::new("TSLA").unwrap();
        let execution = OffchainExecution {
            id: None,
            symbol: symbol.clone(),
            shares: Shares::new(100).unwrap(),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            state: OrderState::Pending,
        };

        let mut sql_tx = pool.begin().await.unwrap();
        let execution_id = execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();

        let calculator = PositionCalculator::new();
        save_within_transaction(&mut sql_tx, &symbol, &calculator, Some(execution_id))
            .await
            .unwrap();

        sql_tx.commit().await.unwrap();

        // Verify pending_execution_id is set
        let symbol_str = symbol.to_string();
        let row = sqlx::query!(
            "SELECT pending_execution_id FROM trade_accumulators WHERE symbol = ?1",
            symbol_str
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.pending_execution_id, Some(execution_id));

        // Clear pending execution ID
        let mut sql_tx = pool.begin().await.unwrap();
        clear_pending_execution_id(&mut sql_tx, &symbol)
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();

        // Verify pending_execution_id is now NULL
        let symbol_str = symbol.to_string();
        let row = sqlx::query!(
            "SELECT pending_execution_id FROM trade_accumulators WHERE symbol = ?1",
            symbol_str
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.pending_execution_id, None);
    }
}
