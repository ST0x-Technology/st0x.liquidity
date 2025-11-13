use sqlx::SqlitePool;

use crate::error::OnChainError;
use st0x_broker::{
    Direction, OrderState, OrderStatus, PersistenceError, Shares, SupportedBroker, Symbol,
};

#[derive(sqlx::FromRow)]
struct ExecutionRow {
    id: i64,
    symbol: String,
    shares: i64,
    direction: String,
    broker: String,
    order_id: Option<String>,
    price_cents: Option<i64>,
    status: String,
    executed_at: Option<chrono::NaiveDateTime>,
}

/// Converts database row data to an OffchainExecution instance.
/// Centralizes the conversion logic and casting operations.
fn row_to_execution(
    ExecutionRow {
        id,
        symbol,
        shares,
        direction,
        broker,
        order_id,
        price_cents,
        status,
        executed_at,
    }: ExecutionRow,
) -> Result<OffchainExecution, OnChainError> {
    let parsed_direction = direction.parse()?;
    let parsed_broker = broker.parse()?;
    let status_enum = status.parse()?;
    let parsed_state = OrderState::from_db_row(status_enum, order_id, price_cents, executed_at)
        .map_err(|e| {
            OnChainError::Persistence(PersistenceError::InvalidTradeStatus(e.to_string()))
        })?;

    let shares_u64 = shares
        .try_into()
        .map_err(|_| OnChainError::Persistence(PersistenceError::InvalidShareQuantity(shares)))?;

    Ok(OffchainExecution {
        id: Some(id),
        symbol: Symbol::new(symbol)?,
        shares: Shares::new(shares_u64)?,
        direction: parsed_direction,
        broker: parsed_broker,
        state: parsed_state,
    })
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OffchainExecution {
    pub(crate) id: Option<i64>,
    pub(crate) symbol: Symbol,
    pub(crate) shares: Shares,
    pub(crate) direction: Direction,
    pub(crate) broker: SupportedBroker,
    pub(crate) state: OrderState,
}

pub(crate) async fn find_executions_by_symbol_status_and_broker(
    pool: &SqlitePool,
    symbol: Option<Symbol>,
    status: OrderStatus,
    broker: Option<SupportedBroker>,
) -> Result<Vec<OffchainExecution>, OnChainError> {
    let status_str = status.as_str();

    let rows = match (symbol, broker) {
        (None, None) => query_by_status(pool, status_str).await?,
        (None, Some(broker)) => {
            query_by_status_and_broker(pool, status_str, &broker.to_string()).await?
        }
        (Some(symbol), None) => {
            query_by_symbol_and_status(pool, &symbol.to_string(), status_str).await?
        }
        (Some(symbol), Some(broker)) => {
            query_by_symbol_status_and_broker(
                pool,
                &symbol.to_string(),
                status_str,
                &broker.to_string(),
            )
            .await?
        }
    };

    rows.into_iter()
        .map(row_to_execution)
        .collect::<Result<Vec<_>, _>>()
}

pub(crate) async fn find_execution_by_id(
    pool: &SqlitePool,
    execution_id: i64,
) -> Result<Option<OffchainExecution>, OnChainError> {
    let row = sqlx::query!("SELECT * FROM offchain_trades WHERE id = ?1", execution_id)
        .fetch_optional(pool)
        .await?;

    if let Some(row) = row {
        row_to_execution(ExecutionRow {
            id: row.id,
            symbol: row.symbol,
            shares: row.shares,
            direction: row.direction,
            broker: row.broker,
            order_id: row.order_id,
            price_cents: row.price_cents,
            status: row.status,
            executed_at: row.executed_at,
        })
        .map(Some)
    } else {
        Ok(None)
    }
}

async fn query_by_status(
    pool: &SqlitePool,
    status_str: &str,
) -> Result<Vec<ExecutionRow>, sqlx::Error> {
    sqlx::query_as::<_, ExecutionRow>(
        "
        SELECT
            id,
            symbol,
            shares,
            direction,
            broker,
            order_id,
            price_cents,
            status,
            executed_at
        FROM offchain_trades
        WHERE status = ?1
        ORDER BY id ASC
        ",
    )
    .bind(status_str)
    .fetch_all(pool)
    .await
}

async fn query_by_status_and_broker(
    pool: &SqlitePool,
    status_str: &str,
    broker_str: &str,
) -> Result<Vec<ExecutionRow>, sqlx::Error> {
    sqlx::query_as::<_, ExecutionRow>(
        "
        SELECT
            id,
            symbol,
            shares,
            direction,
            broker,
            order_id,
            price_cents,
            status,
            executed_at
        FROM offchain_trades
        WHERE status = ?1 AND broker = ?2
        ORDER BY id ASC
        ",
    )
    .bind(status_str)
    .bind(broker_str)
    .fetch_all(pool)
    .await
}

async fn query_by_symbol_and_status(
    pool: &SqlitePool,
    symbol_str: &str,
    status_str: &str,
) -> Result<Vec<ExecutionRow>, sqlx::Error> {
    sqlx::query_as::<_, ExecutionRow>(
        "
        SELECT
            id,
            symbol,
            shares,
            direction,
            broker,
            order_id,
            price_cents,
            status,
            executed_at
        FROM offchain_trades
        WHERE symbol = ?1 AND status = ?2
        ORDER BY id ASC
        ",
    )
    .bind(symbol_str)
    .bind(status_str)
    .fetch_all(pool)
    .await
}

async fn query_by_symbol_status_and_broker(
    pool: &SqlitePool,
    symbol_str: &str,
    status_str: &str,
    broker_str: &str,
) -> Result<Vec<ExecutionRow>, sqlx::Error> {
    sqlx::query_as::<_, ExecutionRow>(
        "
        SELECT
            id,
            symbol,
            shares,
            direction,
            broker,
            order_id,
            price_cents,
            status,
            executed_at
        FROM offchain_trades
        WHERE symbol = ?1 AND status = ?2 AND broker = ?3
        ORDER BY id ASC
        ",
    )
    .bind(symbol_str)
    .bind(status_str)
    .bind(broker_str)
    .fetch_all(pool)
    .await
}

impl OffchainExecution {
    pub(crate) async fn save_within_transaction(
        &self,
        sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    ) -> Result<i64, PersistenceError> {
        self.state
            .store(
                sql_tx,
                &self.symbol,
                self.shares,
                self.direction,
                self.broker,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{OffchainExecutionBuilder, setup_test_db};
    use chrono::Utc;
    use st0x_broker::OrderState;

    #[tokio::test]
    async fn test_offchain_execution_save_and_find() {
        let pool = setup_test_db().await;

        let execution = OffchainExecutionBuilder::new().build();

        let mut sql_tx = pool.begin().await.unwrap();
        let id = execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();
        assert!(id > 0);

        let count = sqlx::query!("SELECT COUNT(*) as count FROM offchain_trades")
            .fetch_one(&pool)
            .await
            .unwrap()
            .count;
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_find_by_symbol_and_status() {
        let pool = setup_test_db().await;

        let execution1 = OffchainExecution {
            id: None,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Shares::new(50).unwrap(),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            state: OrderState::Pending,
        };

        let execution2 = OffchainExecution {
            id: None,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Shares::new(25).unwrap(),
            direction: Direction::Sell,
            broker: SupportedBroker::Schwab,
            state: OrderState::Filled {
                executed_at: Utc::now(),
                order_id: "1004055538123".to_string(),
                price_cents: 15025,
            },
        };

        let execution3 = OffchainExecution {
            id: None,
            symbol: Symbol::new("MSFT").unwrap(),
            shares: Shares::new(10).unwrap(),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            state: OrderState::Pending,
        };

        let mut sql_tx1 = pool.begin().await.unwrap();
        execution1
            .save_within_transaction(&mut sql_tx1)
            .await
            .unwrap();
        sql_tx1.commit().await.unwrap();

        let mut sql_tx2 = pool.begin().await.unwrap();
        execution2
            .save_within_transaction(&mut sql_tx2)
            .await
            .unwrap();
        sql_tx2.commit().await.unwrap();

        let mut sql_tx3 = pool.begin().await.unwrap();
        execution3
            .save_within_transaction(&mut sql_tx3)
            .await
            .unwrap();
        sql_tx3.commit().await.unwrap();

        let pending_aapl = find_executions_by_symbol_status_and_broker(
            &pool,
            Some(Symbol::new("AAPL").unwrap()),
            OrderStatus::Pending,
            None,
        )
        .await
        .unwrap();

        assert_eq!(pending_aapl.len(), 1);
        assert_eq!(pending_aapl[0].shares, Shares::new(50).unwrap());
        assert_eq!(pending_aapl[0].direction, Direction::Buy);

        let completed_aapl = find_executions_by_symbol_status_and_broker(
            &pool,
            Some(Symbol::new("AAPL").unwrap()),
            OrderStatus::Filled,
            None,
        )
        .await
        .unwrap();

        assert_eq!(completed_aapl.len(), 1);
        assert_eq!(completed_aapl[0].shares, Shares::new(25).unwrap());
        assert_eq!(completed_aapl[0].direction, Direction::Sell);
        assert!(matches!(
            &completed_aapl[0].state,
            OrderState::Filled { order_id, price_cents, .. }
            if order_id == "1004055538123" && *price_cents == 15025
        ));
    }

    #[tokio::test]
    async fn test_database_tracks_different_brokers() {
        let pool = setup_test_db().await;

        let schwab_execution = OffchainExecution {
            id: None,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Shares::new(100).unwrap(),
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            state: OrderState::Pending,
        };

        let alpaca_execution = OffchainExecution {
            id: None,
            symbol: Symbol::new("TSLA").unwrap(),
            shares: Shares::new(50).unwrap(),
            direction: Direction::Sell,
            broker: SupportedBroker::Alpaca,
            state: OrderState::Pending,
        };

        let dry_run_execution = OffchainExecution {
            id: None,
            symbol: Symbol::new("MSFT").unwrap(),
            shares: Shares::new(25).unwrap(),
            direction: Direction::Buy,
            broker: SupportedBroker::DryRun,
            state: OrderState::Pending,
        };

        let mut sql_tx1 = pool.begin().await.unwrap();
        let schwab_id = schwab_execution
            .save_within_transaction(&mut sql_tx1)
            .await
            .unwrap();
        sql_tx1.commit().await.unwrap();

        let mut sql_tx2 = pool.begin().await.unwrap();
        let alpaca_id = alpaca_execution
            .save_within_transaction(&mut sql_tx2)
            .await
            .unwrap();
        sql_tx2.commit().await.unwrap();

        let mut sql_tx3 = pool.begin().await.unwrap();
        let dry_run_id = dry_run_execution
            .save_within_transaction(&mut sql_tx3)
            .await
            .unwrap();
        sql_tx3.commit().await.unwrap();

        let schwab_retrieved = find_execution_by_id(&pool, schwab_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(schwab_retrieved.broker, SupportedBroker::Schwab);
        assert_eq!(schwab_retrieved.symbol, Symbol::new("AAPL").unwrap());
        assert_eq!(schwab_retrieved.shares, Shares::new(100).unwrap());

        let alpaca_retrieved = find_execution_by_id(&pool, alpaca_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(alpaca_retrieved.broker, SupportedBroker::Alpaca);
        assert_eq!(alpaca_retrieved.symbol, Symbol::new("TSLA").unwrap());
        assert_eq!(alpaca_retrieved.shares, Shares::new(50).unwrap());

        let dry_run_retrieved = find_execution_by_id(&pool, dry_run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dry_run_retrieved.broker, SupportedBroker::DryRun);
        assert_eq!(dry_run_retrieved.symbol, Symbol::new("MSFT").unwrap());
        assert_eq!(dry_run_retrieved.shares, Shares::new(25).unwrap());

        let all_pending =
            find_executions_by_symbol_status_and_broker(&pool, None, OrderStatus::Pending, None)
                .await
                .unwrap();
        assert_eq!(all_pending.len(), 3);

        let schwab_only = find_executions_by_symbol_status_and_broker(
            &pool,
            None,
            OrderStatus::Pending,
            Some(SupportedBroker::Schwab),
        )
        .await
        .unwrap();
        assert_eq!(schwab_only.len(), 1);
        assert_eq!(schwab_only[0].broker, SupportedBroker::Schwab);

        let alpaca_only = find_executions_by_symbol_status_and_broker(
            &pool,
            None,
            OrderStatus::Pending,
            Some(SupportedBroker::Alpaca),
        )
        .await
        .unwrap();
        assert_eq!(alpaca_only.len(), 1);
        assert_eq!(alpaca_only[0].broker, SupportedBroker::Alpaca);

        let dry_run_only = find_executions_by_symbol_status_and_broker(
            &pool,
            None,
            OrderStatus::Pending,
            Some(SupportedBroker::DryRun),
        )
        .await
        .unwrap();
        assert_eq!(dry_run_only.len(), 1);
        assert_eq!(dry_run_only[0].broker, SupportedBroker::DryRun);
    }
}
