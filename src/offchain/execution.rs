use sqlx::SqlitePool;

use crate::error::OnChainError;
use st0x_execution::{
    Direction, FractionalShares, OrderState, OrderStatus, PersistenceError, SupportedExecutor,
    Symbol,
};

#[derive(sqlx::FromRow)]
struct ExecutionRow {
    id: i64,
    symbol: String,
    shares: f64,
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
    let parsed_executor = broker.parse()?;
    let status_enum = status.parse()?;
    let parsed_state = OrderState::from_db_row(status_enum, order_id, price_cents, executed_at)
        .map_err(|e| {
            OnChainError::Persistence(PersistenceError::InvalidTradeStatus(e.to_string()))
        })?;

    if shares <= 0.0 {
        return Err(OnChainError::Persistence(
            PersistenceError::InvalidShareQuantity(shares),
        ));
    }
    let fractional_shares = FractionalShares::from_f64(shares)?;

    Ok(OffchainExecution {
        id: Some(id),
        symbol: Symbol::new(symbol)?,
        shares: fractional_shares,
        direction: parsed_direction,
        executor: parsed_executor,
        state: parsed_state,
    })
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OffchainExecution {
    pub(crate) id: Option<i64>,
    pub(crate) symbol: Symbol,
    pub(crate) shares: FractionalShares,
    pub(crate) direction: Direction,
    pub(crate) executor: SupportedExecutor,
    pub(crate) state: OrderState,
}

pub(crate) async fn find_executions_by_symbol_status_and_broker(
    pool: &SqlitePool,
    symbol: Option<Symbol>,
    status: OrderStatus,
    broker: Option<SupportedExecutor>,
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
                self.executor,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;
    use crate::test_utils::{OffchainExecutionBuilder, setup_test_db};
    use chrono::Utc;
    use st0x_execution::OrderState;

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
            shares: FractionalShares::new(Decimal::from(50)).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        let execution2 = OffchainExecution {
            id: None,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: FractionalShares::new(Decimal::from(25)).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Filled {
                executed_at: Utc::now(),
                order_id: "1004055538123".to_string(),
                price_cents: 15025,
            },
        };

        let execution3 = OffchainExecution {
            id: None,
            symbol: Symbol::new("MSFT").unwrap(),
            shares: FractionalShares::new(Decimal::from(10)).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
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
        assert_eq!(
            pending_aapl[0].shares,
            FractionalShares::new(Decimal::from(50)).unwrap()
        );
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
        assert_eq!(
            completed_aapl[0].shares,
            FractionalShares::new(Decimal::from(25)).unwrap()
        );
        assert_eq!(completed_aapl[0].direction, Direction::Sell);
        assert!(matches!(
            &completed_aapl[0].state,
            OrderState::Filled { order_id, price_cents, .. }
            if order_id == "1004055538123" && *price_cents == 15025
        ));
    }

    async fn save_execution(pool: &SqlitePool, execution: OffchainExecution) -> i64 {
        let mut sql_tx = pool.begin().await.unwrap();
        let id = execution
            .save_within_transaction(&mut sql_tx)
            .await
            .unwrap();
        sql_tx.commit().await.unwrap();
        id
    }

    async fn find_by_executor(
        pool: &SqlitePool,
        executor: SupportedExecutor,
    ) -> Vec<OffchainExecution> {
        find_executions_by_symbol_status_and_broker(
            pool,
            None,
            OrderStatus::Pending,
            Some(executor),
        )
        .await
        .unwrap()
    }

    fn make_execution(
        symbol: &str,
        shares: u64,
        direction: Direction,
        executor: SupportedExecutor,
    ) -> OffchainExecution {
        OffchainExecution {
            id: None,
            symbol: Symbol::new(symbol).unwrap(),
            shares: FractionalShares::new(Decimal::from(shares)).unwrap(),
            direction,
            executor,
            state: OrderState::Pending,
        }
    }

    #[tokio::test]
    async fn test_database_tracks_different_brokers() {
        let pool = setup_test_db().await;

        let schwab_id = save_execution(
            &pool,
            make_execution("AAPL", 100, Direction::Buy, SupportedExecutor::Schwab),
        )
        .await;
        let alpaca_id = save_execution(
            &pool,
            make_execution(
                "TSLA",
                50,
                Direction::Sell,
                SupportedExecutor::AlpacaTradingApi,
            ),
        )
        .await;
        let dry_run_id = save_execution(
            &pool,
            make_execution("MSFT", 25, Direction::Buy, SupportedExecutor::DryRun),
        )
        .await;

        let schwab = find_execution_by_id(&pool, schwab_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(schwab.executor, SupportedExecutor::Schwab);
        assert_eq!(schwab.symbol, Symbol::new("AAPL").unwrap());

        let alpaca = find_execution_by_id(&pool, alpaca_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(alpaca.executor, SupportedExecutor::AlpacaTradingApi);
        assert_eq!(alpaca.symbol, Symbol::new("TSLA").unwrap());

        let dry_run = find_execution_by_id(&pool, dry_run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dry_run.executor, SupportedExecutor::DryRun);
        assert_eq!(dry_run.symbol, Symbol::new("MSFT").unwrap());

        let all =
            find_executions_by_symbol_status_and_broker(&pool, None, OrderStatus::Pending, None)
                .await
                .unwrap();
        assert_eq!(all.len(), 3);

        assert_eq!(
            find_by_executor(&pool, SupportedExecutor::Schwab)
                .await
                .len(),
            1
        );
        assert_eq!(
            find_by_executor(&pool, SupportedExecutor::AlpacaTradingApi)
                .await
                .len(),
            1
        );
        assert_eq!(
            find_by_executor(&pool, SupportedExecutor::DryRun)
                .await
                .len(),
            1
        );
    }
}
