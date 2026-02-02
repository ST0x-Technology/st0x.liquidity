use serde::Serialize;
use sqlx::SqlitePool;
use st0x_execution::{OrderStatus, SupportedExecutor, Symbol};

use crate::error::OnChainError;
use crate::offchain_order::{OffchainOrderId, OffchainOrderView};

pub(crate) async fn find_executions_by_symbol_status_and_broker(
    pool: &SqlitePool,
    symbol: Option<Symbol>,
    status: OrderStatus,
    broker: Option<SupportedExecutor>,
) -> Result<Vec<OffchainOrderView>, OnChainError> {
    let status_str = to_json_str(&status)?;

    let rows = match (symbol, broker) {
        (None, None) => query_by_status(pool, &status_str).await?,
        (None, Some(broker)) => {
            let broker_str = to_json_str(&broker)?;
            query_by_status_and_executor(pool, &status_str, &broker_str).await?
        }
        (Some(symbol), None) => {
            query_by_symbol_and_status(pool, &symbol.to_string(), &status_str).await?
        }
        (Some(symbol), Some(broker)) => {
            let broker_str = to_json_str(&broker)?;
            query_by_symbol_status_and_executor(pool, &symbol.to_string(), &status_str, &broker_str)
                .await?
        }
    };

    rows.into_iter()
        .map(|payload| Ok(serde_json::from_str(&payload)?))
        .collect()
}

pub(crate) async fn find_execution_by_id(
    pool: &SqlitePool,
    offchain_order_id: OffchainOrderId,
) -> Result<Option<OffchainOrderView>, OnChainError> {
    let id_str = offchain_order_id.to_string();
    let row: Option<String> =
        sqlx::query_scalar("SELECT payload FROM offchain_order_view WHERE view_id = ?1")
            .bind(&id_str)
            .fetch_optional(pool)
            .await?;

    row.map(|payload| Ok(serde_json::from_str(&payload)?))
        .transpose()
}

async fn query_by_status(pool: &SqlitePool, status_str: &str) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT payload FROM offchain_order_view \
         WHERE status = ?1 \
         ORDER BY offchain_order_id ASC",
    )
    .bind(status_str)
    .fetch_all(pool)
    .await
}

async fn query_by_status_and_executor(
    pool: &SqlitePool,
    status_str: &str,
    executor_str: &str,
) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT payload FROM offchain_order_view \
         WHERE status = ?1 AND executor = ?2 \
         ORDER BY offchain_order_id ASC",
    )
    .bind(status_str)
    .bind(executor_str)
    .fetch_all(pool)
    .await
}

async fn query_by_symbol_and_status(
    pool: &SqlitePool,
    symbol_str: &str,
    status_str: &str,
) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT payload FROM offchain_order_view \
         WHERE symbol = ?1 AND status = ?2 \
         ORDER BY offchain_order_id ASC",
    )
    .bind(symbol_str)
    .bind(status_str)
    .fetch_all(pool)
    .await
}

async fn query_by_symbol_status_and_executor(
    pool: &SqlitePool,
    symbol_str: &str,
    status_str: &str,
    executor_str: &str,
) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT payload FROM offchain_order_view \
         WHERE symbol = ?1 AND status = ?2 AND executor = ?3 \
         ORDER BY offchain_order_id ASC",
    )
    .bind(symbol_str)
    .bind(status_str)
    .bind(executor_str)
    .fetch_all(pool)
    .await
}

fn to_json_str<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    let json = serde_json::to_string(value)?;
    Ok(json.trim_matches('"').to_string())
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;
    use crate::dual_write::DualWriteContext;
    use crate::offchain_order::BrokerOrderId;
    use crate::test_utils::setup_test_db;
    use chrono::Utc;
    use st0x_execution::OrderState;

    /// Helper: creates an OffchainExecution, places it via CQRS, and returns the execution_id.
    async fn place_execution_via_cqrs(
        pool: &SqlitePool,
        context: &DualWriteContext,
        symbol: &str,
        shares: u64,
        direction: Direction,
        executor: SupportedExecutor,
        state: OrderState,
    ) -> i64 {
        let execution = OffchainExecution {
            id: None,
            symbol: Symbol::new(symbol).unwrap(),
            shares: Positive::new(FractionalShares::new(Decimal::from(shares))).unwrap(),
            direction,
            executor,
            state: OrderState::Pending,
        };

        let mut tx = pool.begin().await.unwrap();
        let execution_id = execution.save_within_transaction(&mut tx).await.unwrap();
        tx.commit().await.unwrap();

        let execution_with_id = OffchainExecution {
            id: Some(execution_id),
            ..execution.clone()
        };

        crate::dual_write::place_order(context, &execution_with_id)
            .await
            .unwrap();

        match &state {
            OrderState::Submitted { order_id } => {
                crate::dual_write::confirm_submission(
                    context,
                    execution_id,
                    BrokerOrderId::new(order_id),
                )
                .await
                .unwrap();
            }
            OrderState::Filled { order_id, .. } => {
                crate::dual_write::confirm_submission(
                    context,
                    execution_id,
                    BrokerOrderId::new(order_id),
                )
                .await
                .unwrap();

                let filled_execution = OffchainExecution {
                    id: Some(execution_id),
                    state: state.clone(),
                    ..execution.clone()
                };

                crate::dual_write::record_fill(context, &filled_execution)
                    .await
                    .unwrap();
            }
            OrderState::Pending | OrderState::Failed { .. } => {}
        }

        execution_id
    }

    #[tokio::test]
    async fn test_find_execution_by_id() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let execution_id = place_execution_via_cqrs(
            &pool,
            &context,
            "AAPL",
            50,
            Direction::Buy,
            SupportedExecutor::Schwab,
            OrderState::Pending,
        )
        .await;

        let found = find_execution_by_id(&pool, execution_id)
            .await
            .unwrap()
            .unwrap();

        let OffchainOrderView::Execution {
            execution_id: found_id,
            symbol,
            shares,
            direction,
            executor,
            status,
            ..
        } = found
        else {
            panic!("Expected Execution variant, got Unavailable");
        };

        assert_eq!(found_id.0, execution_id);
        assert_eq!(symbol, Symbol::new("AAPL").unwrap());
        assert_eq!(shares, FractionalShares::new(Decimal::from(50)));
        assert_eq!(direction, Direction::Buy);
        assert_eq!(executor, SupportedExecutor::Schwab);
        assert_eq!(status, OrderStatus::Pending);
    }

    #[tokio::test]
    async fn test_find_execution_by_id_not_found() {
        let pool = setup_test_db().await;

        let result = find_execution_by_id(&pool, 99999).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_find_by_symbol_and_status() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        place_execution_via_cqrs(
            &pool,
            &context,
            "AAPL",
            50,
            Direction::Buy,
            SupportedExecutor::Schwab,
            OrderState::Pending,
        )
        .await;

        place_execution_via_cqrs(
            &pool,
            &context,
            "AAPL",
            25,
            Direction::Sell,
            SupportedExecutor::Schwab,
            OrderState::Filled {
                order_id: "1004055538123".to_string(),
                price_cents: 15025,
                executed_at: Utc::now(),
            },
        )
        .await;

        place_execution_via_cqrs(
            &pool,
            &context,
            "MSFT",
            10,
            Direction::Buy,
            SupportedExecutor::Schwab,
            OrderState::Pending,
        )
        .await;

        let pending_aapl = find_executions_by_symbol_status_and_broker(
            &pool,
            Some(Symbol::new("AAPL").unwrap()),
            OrderStatus::Pending,
            None,
        )
        .await
        .unwrap();

        assert_eq!(pending_aapl.len(), 1);
        let OffchainOrderView::Execution {
            shares, direction, ..
        } = &pending_aapl[0]
        else {
            panic!("Expected Execution variant");
        };
        assert_eq!(*shares, FractionalShares::new(Decimal::from(50)));
        assert_eq!(*direction, Direction::Buy);

        let filled_aapl = find_executions_by_symbol_status_and_broker(
            &pool,
            Some(Symbol::new("AAPL").unwrap()),
            OrderStatus::Filled,
            None,
        )
        .await
        .unwrap();

        assert_eq!(filled_aapl.len(), 1);
        let OffchainOrderView::Execution {
            shares,
            direction,
            executor_order_id,
            price_cents,
            ..
        } = &filled_aapl[0]
        else {
            panic!("Expected Execution variant");
        };
        assert_eq!(*shares, FractionalShares::new(Decimal::from(25)));
        assert_eq!(*direction, Direction::Sell);
        assert_eq!(executor_order_id.as_ref().unwrap().0, "1004055538123");
        assert_eq!(price_cents.as_ref().unwrap().0, 15025);
    }

    #[tokio::test]
    async fn test_database_tracks_different_brokers() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let schwab_id = place_execution_via_cqrs(
            &pool,
            &context,
            "AAPL",
            100,
            Direction::Buy,
            SupportedExecutor::Schwab,
            OrderState::Pending,
        )
        .await;

        let alpaca_id = place_execution_via_cqrs(
            &pool,
            &context,
            "TSLA",
            50,
            Direction::Sell,
            SupportedExecutor::AlpacaTradingApi,
            OrderState::Pending,
        )
        .await;

        let dry_run_id = place_execution_via_cqrs(
            &pool,
            &context,
            "MSFT",
            25,
            Direction::Buy,
            SupportedExecutor::DryRun,
            OrderState::Pending,
        )
        .await;

        let schwab = find_execution_by_id(&pool, schwab_id)
            .await
            .unwrap()
            .unwrap();
        let OffchainOrderView::Execution {
            executor, symbol, ..
        } = &schwab
        else {
            panic!("Expected Execution variant");
        };
        assert_eq!(*executor, SupportedExecutor::Schwab);
        assert_eq!(*symbol, Symbol::new("AAPL").unwrap());

        let alpaca = find_execution_by_id(&pool, alpaca_id)
            .await
            .unwrap()
            .unwrap();
        let OffchainOrderView::Execution {
            executor, symbol, ..
        } = &alpaca
        else {
            panic!("Expected Execution variant");
        };
        assert_eq!(*executor, SupportedExecutor::AlpacaTradingApi);
        assert_eq!(*symbol, Symbol::new("TSLA").unwrap());

        let dry_run = find_execution_by_id(&pool, dry_run_id)
            .await
            .unwrap()
            .unwrap();
        let OffchainOrderView::Execution {
            executor, symbol, ..
        } = &dry_run
        else {
            panic!("Expected Execution variant");
        };
        assert_eq!(*executor, SupportedExecutor::DryRun);
        assert_eq!(*symbol, Symbol::new("MSFT").unwrap());

        let all =
            find_executions_by_symbol_status_and_broker(&pool, None, OrderStatus::Pending, None)
                .await
                .unwrap();
        assert_eq!(all.len(), 3);

        let schwab_only = find_executions_by_symbol_status_and_broker(
            &pool,
            None,
            OrderStatus::Pending,
            Some(SupportedExecutor::Schwab),
        )
        .await
        .unwrap();
        assert_eq!(schwab_only.len(), 1);

        let alpaca_only = find_executions_by_symbol_status_and_broker(
            &pool,
            None,
            OrderStatus::Pending,
            Some(SupportedExecutor::AlpacaTradingApi),
        )
        .await
        .unwrap();
        assert_eq!(alpaca_only.len(), 1);

        let dry_run_only = find_executions_by_symbol_status_and_broker(
            &pool,
            None,
            OrderStatus::Pending,
            Some(SupportedExecutor::DryRun),
        )
        .await
        .unwrap();
        assert_eq!(dry_run_only.len(), 1);
    }

    #[test]
    fn test_to_json_str_strips_quotes() {
        assert_eq!(to_json_str(&OrderStatus::Pending).unwrap(), "Pending");
        assert_eq!(to_json_str(&OrderStatus::Submitted).unwrap(), "Submitted");
        assert_eq!(to_json_str(&OrderStatus::Filled).unwrap(), "Filled");
        assert_eq!(to_json_str(&OrderStatus::Failed).unwrap(), "Failed");

        assert_eq!(to_json_str(&SupportedExecutor::Schwab).unwrap(), "Schwab");
        assert_eq!(
            to_json_str(&SupportedExecutor::AlpacaTradingApi).unwrap(),
            "AlpacaTradingApi"
        );
        assert_eq!(to_json_str(&SupportedExecutor::DryRun).unwrap(), "DryRun");
    }
}
