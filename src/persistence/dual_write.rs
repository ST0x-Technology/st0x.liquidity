use async_trait::async_trait;
use sqlx::SqlitePool;
use st0x_execution::{
    Direction, OrderState, OrderStatePersistence, PersistenceError, Shares, SupportedExecutor,
    Symbol,
};

/// Persistence for dual-write mode.
/// Writes order state to the `offchain_trades` table alongside event sourcing.
pub struct DualWritePersistence {
    pool: SqlitePool,
}

impl DualWritePersistence {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Store a new order state within an existing transaction.
    /// Use this when the store operation needs to be atomic with other operations.
    pub async fn store_in_transaction(
        sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        state: &OrderState,
        symbol: &Symbol,
        shares: Shares,
        direction: Direction,
        executor: SupportedExecutor,
    ) -> Result<i64, PersistenceError> {
        let status_str = state.status().as_str();
        let db_fields = Self::to_db_fields(state)?;

        let symbol_str = symbol.to_string();
        let shares_i64 = i64::from(shares.value());
        let direction_str = direction.as_str();
        let executor_str = executor.to_string();

        let result = sqlx::query!(
            r#"
            INSERT INTO offchain_trades (
                symbol,
                shares,
                direction,
                broker,
                order_id,
                price_cents,
                status,
                executed_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
            symbol_str,
            shares_i64,
            direction_str,
            executor_str,
            db_fields.order_id,
            db_fields.price_cents,
            status_str,
            db_fields.executed_at
        )
        .execute(&mut **sql_tx)
        .await?;

        Ok(result.last_insert_rowid())
    }

    /// Update an existing order state within an existing transaction.
    /// Use this when the update operation needs to be atomic with other operations.
    pub async fn store_update_in_transaction(
        sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        state: &OrderState,
        execution_id: i64,
    ) -> Result<(), PersistenceError> {
        let status_str = state.status().as_str();
        let db_fields = Self::to_db_fields(state)?;

        let result = sqlx::query!(
            "
            UPDATE offchain_trades
            SET status = ?1, order_id = ?2, price_cents = ?3, executed_at = ?4
            WHERE id = ?5
            ",
            status_str,
            db_fields.order_id,
            db_fields.price_cents,
            db_fields.executed_at,
            execution_id
        )
        .execute(&mut **sql_tx)
        .await?;

        if result.rows_affected() == 0 {
            return Err(PersistenceError::RowNotFound { execution_id });
        }

        Ok(())
    }

    fn to_db_fields(state: &OrderState) -> Result<OrderStateDbFields, PersistenceError> {
        match state {
            OrderState::Pending => Ok(OrderStateDbFields {
                order_id: None,
                price_cents: None,
                executed_at: None,
            }),
            OrderState::Submitted { order_id } => Ok(OrderStateDbFields {
                order_id: Some(order_id.clone()),
                price_cents: None,
                executed_at: None,
            }),
            OrderState::Filled {
                executed_at,
                order_id,
                price_cents,
            } => {
                let price_cents_i64 = i64::try_from(*price_cents)
                    .map_err(|_| PersistenceError::PriceCentsOverflow(*price_cents))?;
                Ok(OrderStateDbFields {
                    order_id: Some(order_id.clone()),
                    price_cents: Some(price_cents_i64),
                    executed_at: Some(executed_at.naive_utc()),
                })
            }
            OrderState::Failed {
                failed_at,
                error_reason: _,
            } => Ok(OrderStateDbFields {
                order_id: None,
                price_cents: None,
                executed_at: Some(failed_at.naive_utc()),
            }),
        }
    }
}

struct OrderStateDbFields {
    order_id: Option<String>,
    price_cents: Option<i64>,
    executed_at: Option<chrono::NaiveDateTime>,
}

#[async_trait]
impl OrderStatePersistence for DualWritePersistence {
    type Error = PersistenceError;

    async fn store(
        &self,
        state: &OrderState,
        symbol: &Symbol,
        shares: Shares,
        direction: Direction,
        executor: SupportedExecutor,
    ) -> Result<i64, Self::Error> {
        let mut tx = self.pool.begin().await?;
        let id = Self::store_in_transaction(&mut tx, state, symbol, shares, direction, executor).await?;
        tx.commit().await?;
        Ok(id)
    }

    async fn store_update(&self, state: &OrderState, execution_id: i64) -> Result<(), Self::Error> {
        let mut tx = self.pool.begin().await?;
        Self::store_update_in_transaction(&mut tx, state, execution_id).await?;
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use st0x_execution::OrderStatus;

    use super::*;
    use crate::test_utils::setup_test_db;

    #[tokio::test]
    async fn test_store_pending_order() {
        let pool = setup_test_db().await;
        let persistence = DualWritePersistence::new(pool.clone());

        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Shares::new(10).unwrap();

        let execution_id = persistence
            .store(
                &OrderState::Pending,
                &symbol,
                shares,
                Direction::Buy,
                SupportedExecutor::Schwab,
            )
            .await
            .unwrap();

        assert!(execution_id > 0);

        let row = sqlx::query!(
            "SELECT symbol, shares, direction, broker, status, order_id, price_cents
             FROM offchain_trades WHERE id = ?",
            execution_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(row.symbol, "AAPL");
        assert_eq!(row.shares, 10);
        assert_eq!(row.direction, "BUY");
        assert_eq!(row.broker, "schwab");
        assert_eq!(row.status, "PENDING");
        assert!(row.order_id.is_none());
        assert!(row.price_cents.is_none());
    }

    #[tokio::test]
    async fn test_store_update_to_submitted() {
        let pool = setup_test_db().await;
        let persistence = DualWritePersistence::new(pool.clone());

        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Shares::new(5).unwrap();

        let execution_id = persistence
            .store(
                &OrderState::Pending,
                &symbol,
                shares,
                Direction::Sell,
                SupportedExecutor::Schwab,
            )
            .await
            .unwrap();

        let submitted_state = OrderState::Submitted {
            order_id: "ORD123".to_string(),
        };

        persistence
            .store_update(&submitted_state, execution_id)
            .await
            .unwrap();

        let row = sqlx::query!(
            "SELECT status, order_id FROM offchain_trades WHERE id = ?",
            execution_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(row.status, "SUBMITTED");
        assert_eq!(row.order_id.unwrap(), "ORD123");
    }

    #[tokio::test]
    async fn test_store_update_to_filled() {
        let pool = setup_test_db().await;
        let persistence = DualWritePersistence::new(pool.clone());

        let symbol = Symbol::new("NVDA").unwrap();
        let shares = Shares::new(3).unwrap();

        let execution_id = persistence
            .store(
                &OrderState::Pending,
                &symbol,
                shares,
                Direction::Buy,
                SupportedExecutor::Schwab,
            )
            .await
            .unwrap();

        let filled_state = OrderState::Filled {
            executed_at: Utc::now(),
            order_id: "ORD456".to_string(),
            price_cents: 15000,
        };

        persistence
            .store_update(&filled_state, execution_id)
            .await
            .unwrap();

        let row = sqlx::query!(
            "SELECT status, order_id, price_cents FROM offchain_trades WHERE id = ?",
            execution_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(row.status, "FILLED");
        assert_eq!(row.order_id.unwrap(), "ORD456");
        assert_eq!(row.price_cents.unwrap(), 15000);
    }

    #[tokio::test]
    async fn test_store_update_nonexistent_returns_error() {
        let pool = setup_test_db().await;
        let persistence = DualWritePersistence::new(pool);

        let submitted_state = OrderState::Submitted {
            order_id: "ORD999".to_string(),
        };

        let result = persistence.store_update(&submitted_state, 99999).await;

        assert!(
            matches!(
                result.unwrap_err(),
                PersistenceError::RowNotFound { execution_id: 99999 }
            ),
            "Expected RowNotFound error for execution_id 99999"
        );
    }

    #[tokio::test]
    async fn test_from_db_row_roundtrip_pending() {
        let pool = setup_test_db().await;
        let persistence = DualWritePersistence::new(pool.clone());

        let symbol = Symbol::new("GOOG").unwrap();
        let shares = Shares::new(1).unwrap();

        let execution_id = persistence
            .store(
                &OrderState::Pending,
                &symbol,
                shares,
                Direction::Buy,
                SupportedExecutor::Schwab,
            )
            .await
            .unwrap();

        let row = sqlx::query!(
            "SELECT status, order_id, price_cents, executed_at FROM offchain_trades WHERE id = ?",
            execution_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let status: OrderStatus = row.status.parse().unwrap();
        let state = OrderState::from_db_row(status, row.order_id, row.price_cents, row.executed_at)
            .unwrap();

        assert_eq!(state, OrderState::Pending);
    }
}
