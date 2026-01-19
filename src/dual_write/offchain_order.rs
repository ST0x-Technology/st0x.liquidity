use rust_decimal::Decimal;
use st0x_execution::OrderState;
use tracing::info;

use crate::offchain::execution::OffchainExecution;
use crate::offchain_order::{BrokerOrderId, PriceCents};
use crate::offchain_order::{OffchainOrder, OffchainOrderCommand};
use crate::shares::FractionalShares;

use super::{DualWriteContext, DualWriteError};

pub(crate) async fn place_order(
    context: &DualWriteContext,
    execution: &OffchainExecution,
) -> Result<(), DualWriteError> {
    let execution_id = execution
        .id
        .ok_or_else(|| DualWriteError::MissingExecutionId)?;

    let aggregate_id = OffchainOrder::aggregate_id(execution_id);

    let command = OffchainOrderCommand::Place {
        symbol: execution.symbol.clone(),
        shares: FractionalShares(Decimal::from(execution.shares.value())),
        direction: execution.direction,
        executor: execution.executor,
    };

    context
        .offchain_order_framework()
        .execute(&aggregate_id, command)
        .await?;

    Ok(())
}

pub(crate) async fn confirm_submission(
    context: &DualWriteContext,
    execution_id: i64,
    broker_order_id: BrokerOrderId,
) -> Result<(), DualWriteError> {
    let aggregate_id = OffchainOrder::aggregate_id(execution_id);

    let command = OffchainOrderCommand::ConfirmSubmission {
        broker_order_id: broker_order_id.clone(),
    };

    context
        .offchain_order_framework()
        .execute(&aggregate_id, command)
        .await?;

    let submitted_state = OrderState::Submitted {
        order_id: broker_order_id.0.clone(),
    };

    let mut tx = context.pool().begin().await?;
    submitted_state.store_update(&mut tx, execution_id).await?;
    tx.commit().await?;

    info!(
        "Updated execution {execution_id} to SUBMITTED with order_id={:?}",
        broker_order_id
    );

    Ok(())
}

pub(crate) async fn record_fill(
    context: &DualWriteContext,
    execution: &OffchainExecution,
) -> Result<(), DualWriteError> {
    let execution_id = execution
        .id
        .ok_or_else(|| DualWriteError::MissingExecutionId)?;

    let aggregate_id = OffchainOrder::aggregate_id(execution_id);

    let price_cents = match &execution.state {
        OrderState::Filled { price_cents, .. } => PriceCents(*price_cents),
        _ => {
            return Err(DualWriteError::InvalidOrderState {
                execution_id,
                expected: "Filled".to_string(),
            });
        }
    };

    let command = OffchainOrderCommand::CompleteFill { price_cents };

    context
        .offchain_order_framework()
        .execute(&aggregate_id, command)
        .await?;

    Ok(())
}

pub(crate) async fn mark_failed(
    context: &DualWriteContext,
    execution_id: i64,
    error: String,
) -> Result<(), DualWriteError> {
    let aggregate_id = OffchainOrder::aggregate_id(execution_id);

    let command = OffchainOrderCommand::MarkFailed { error };

    context
        .offchain_order_framework()
        .execute(&aggregate_id, command)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use st0x_execution::{Direction, Shares, SupportedExecutor, Symbol};

    use super::*;
    use crate::test_utils::setup_test_db;

    #[tokio::test]
    async fn test_place_order_success() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("AAPL").unwrap();
        let execution = OffchainExecution {
            id: Some(1),
            symbol: symbol.clone(),
            shares: Shares::new(10).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        let result = place_order(&context, &execution).await;
        assert!(result.is_ok());

        let event_count = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM events WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = '1'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(event_count, 1);

        let event_type = sqlx::query_scalar!(
            "SELECT event_type FROM events WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = '1'"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(event_type, "OffchainOrderEvent::Placed");
    }

    #[tokio::test]
    async fn test_place_order_missing_execution_id() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool);

        let symbol = Symbol::new("AAPL").unwrap();
        let execution = OffchainExecution {
            id: None,
            symbol,
            shares: Shares::new(10).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        let result = place_order(&context, &execution).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DualWriteError::MissingExecutionId
        ));
    }

    #[tokio::test]
    async fn test_confirm_submission_success() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Shares::new(10).unwrap();

        let mut tx = pool.begin().await.unwrap();
        let execution_id = OrderState::Pending
            .store(
                &mut tx,
                &symbol,
                shares,
                Direction::Buy,
                SupportedExecutor::Schwab,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let execution = OffchainExecution {
            id: Some(execution_id),
            symbol,
            shares,
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        place_order(&context, &execution).await.unwrap();

        let result = confirm_submission(&context, execution_id, BrokerOrderId::new("ORD123")).await;
        assert!(result.is_ok());

        let event_count = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM events \
             WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = ?",
            execution_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(event_count, 2);

        let event_type = sqlx::query_scalar!(
            "SELECT event_type FROM events \
             WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = ? \
             ORDER BY sequence DESC LIMIT 1",
            execution_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(event_type, "OffchainOrderEvent::Submitted");
    }

    #[tokio::test]
    async fn test_record_fill_success() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Shares::new(10).unwrap();

        let mut tx = pool.begin().await.unwrap();
        let execution_id = OrderState::Pending
            .store(
                &mut tx,
                &symbol,
                shares,
                Direction::Buy,
                SupportedExecutor::Schwab,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let execution = OffchainExecution {
            id: Some(execution_id),
            symbol,
            shares,
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Filled {
                order_id: "ORD456".to_string(),
                price_cents: 15025,
                executed_at: Utc::now(),
            },
        };

        place_order(&context, &execution).await.unwrap();

        confirm_submission(&context, execution_id, BrokerOrderId::new("ORD456"))
            .await
            .unwrap();

        let result = record_fill(&context, &execution).await;
        assert!(result.is_ok());

        let event_type = sqlx::query_scalar!(
            "SELECT event_type FROM events \
             WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = ? \
             ORDER BY sequence DESC LIMIT 1",
            execution_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(event_type, "OffchainOrderEvent::Filled");
    }

    #[tokio::test]
    async fn test_record_fill_invalid_state() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool);

        let symbol = Symbol::new("AAPL").unwrap();
        let execution = OffchainExecution {
            id: Some(4),
            symbol,
            shares: Shares::new(10).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        let result = record_fill(&context, &execution).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DualWriteError::InvalidOrderState { .. }
        ));
    }

    #[tokio::test]
    async fn test_mark_failed_success() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("AAPL").unwrap();
        let execution = OffchainExecution {
            id: Some(5),
            symbol,
            shares: Shares::new(10).unwrap(),
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        place_order(&context, &execution).await.unwrap();

        let result = mark_failed(&context, 5, "Broker API timeout".to_string()).await;
        assert!(result.is_ok());

        let event_type = sqlx::query_scalar!(
            "SELECT event_type FROM events WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = '5' ORDER BY sequence DESC LIMIT 1"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(event_type, "OffchainOrderEvent::Failed");
    }

    #[tokio::test]
    async fn test_sequence_increments() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Shares::new(10).unwrap();

        let mut tx = pool.begin().await.unwrap();
        let execution_id = OrderState::Pending
            .store(
                &mut tx,
                &symbol,
                shares,
                Direction::Buy,
                SupportedExecutor::Schwab,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let execution = OffchainExecution {
            id: Some(execution_id),
            symbol,
            shares,
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Filled {
                order_id: "ORD789".to_string(),
                price_cents: 15000,
                executed_at: Utc::now(),
            },
        };

        place_order(&context, &execution).await.unwrap();

        confirm_submission(&context, execution_id, BrokerOrderId::new("ORD789"))
            .await
            .unwrap();

        record_fill(&context, &execution).await.unwrap();

        let sequences: Vec<i64> = sqlx::query_scalar!(
            "SELECT sequence FROM events \
             WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = ? \
             ORDER BY sequence",
            execution_id
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(sequences, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_confirm_submission_updates_both_es_and_legacy_consistently() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Shares::new(5).unwrap();
        let broker_order_id = BrokerOrderId::new("SCHWAB-12345");

        let mut tx = pool.begin().await.unwrap();
        let execution_id = OrderState::Pending
            .store(
                &mut tx,
                &symbol,
                shares,
                Direction::Buy,
                SupportedExecutor::Schwab,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let execution = OffchainExecution {
            id: Some(execution_id),
            symbol,
            shares,
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        place_order(&context, &execution).await.unwrap();
        confirm_submission(&context, execution_id, broker_order_id.clone())
            .await
            .unwrap();

        let es_event = sqlx::query_scalar!(
            "SELECT event_type FROM events \
             WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = ? \
             ORDER BY sequence DESC LIMIT 1",
            execution_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let legacy_row = sqlx::query!(
            "SELECT status, order_id FROM offchain_trades WHERE id = ?",
            execution_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(es_event, "OffchainOrderEvent::Submitted");
        assert_eq!(legacy_row.status, "SUBMITTED");
        assert_eq!(legacy_row.order_id.unwrap(), broker_order_id.0);
    }

    #[tokio::test]
    async fn test_confirm_submission_order_id_matches_between_es_and_legacy() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("NVDA").unwrap();
        let shares = Shares::new(3).unwrap();
        let broker_order_id = BrokerOrderId::new("ORDER-ABC-789");

        let mut tx = pool.begin().await.unwrap();
        let execution_id = OrderState::Pending
            .store(
                &mut tx,
                &symbol,
                shares,
                Direction::Sell,
                SupportedExecutor::Schwab,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let execution = OffchainExecution {
            id: Some(execution_id),
            symbol,
            shares,
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        place_order(&context, &execution).await.unwrap();
        confirm_submission(&context, execution_id, broker_order_id.clone())
            .await
            .unwrap();

        let es_event_payload: String = sqlx::query_scalar(
            "SELECT payload FROM events \
             WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = ? \
             AND event_type = 'OffchainOrderEvent::Submitted'",
        )
        .bind(execution_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        let legacy_order_id: String =
            sqlx::query_scalar("SELECT order_id FROM offchain_trades WHERE id = ?")
                .bind(execution_id)
                .fetch_one(&pool)
                .await
                .unwrap();

        let payload: serde_json::Value = serde_json::from_str(&es_event_payload).unwrap();
        let es_order_id = payload["Submitted"]["broker_order_id"].as_str().unwrap();

        assert_eq!(
            es_order_id, legacy_order_id,
            "Order ID must match between ES event and legacy table for safe deprecation"
        );
    }

    #[tokio::test]
    async fn test_es_failure_prevents_legacy_write() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("GOOG").unwrap();
        let shares = Shares::new(5).unwrap();

        let mut tx = pool.begin().await.unwrap();
        let execution_id = OrderState::Pending
            .store(
                &mut tx,
                &symbol,
                shares,
                Direction::Buy,
                SupportedExecutor::Schwab,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let result =
            confirm_submission(&context, execution_id, BrokerOrderId::new("ORD-FAIL")).await;

        assert!(
            matches!(
                result.unwrap_err(),
                DualWriteError::OffchainOrderAggregate(_)
            ),
            "ES should fail because place_order was never called"
        );

        let legacy_row = sqlx::query!(
            "SELECT status, order_id FROM offchain_trades WHERE id = ?",
            execution_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(
            legacy_row.status, "PENDING",
            "Legacy row should remain PENDING when ES fails"
        );
        assert!(
            legacy_row.order_id.is_none(),
            "Legacy order_id should remain None when ES fails"
        );
    }

    #[tokio::test]
    async fn test_legacy_failure_after_es_success_propagates_error() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("META").unwrap();
        let shares = Shares::new(5).unwrap();
        let nonexistent_execution_id = 99999i64;

        let execution = OffchainExecution {
            id: Some(nonexistent_execution_id),
            symbol,
            shares,
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        place_order(&context, &execution).await.unwrap();

        let result = confirm_submission(
            &context,
            nonexistent_execution_id,
            BrokerOrderId::new("ORD-ORPHAN"),
        )
        .await;

        assert!(
            matches!(result.unwrap_err(), DualWriteError::Persistence(_)),
            "Should fail with RowNotFound when legacy row doesn't exist"
        );

        let es_event_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events \
             WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = ?",
        )
        .bind(nonexistent_execution_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(
            es_event_count, 2,
            "ES events exist (Placed + Submitted) even though legacy write failed - \
             this documents the dual-write inconsistency risk"
        );
    }

    #[tokio::test]
    async fn test_legacy_transaction_rollback_on_commit_failure() {
        let pool = setup_test_db().await;
        let context = DualWriteContext::new(pool.clone());

        let symbol = Symbol::new("AMZN").unwrap();
        let shares = Shares::new(5).unwrap();

        let mut tx = pool.begin().await.unwrap();
        let execution_id = OrderState::Pending
            .store(
                &mut tx,
                &symbol,
                shares,
                Direction::Buy,
                SupportedExecutor::Schwab,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let execution = OffchainExecution {
            id: Some(execution_id),
            symbol: symbol.clone(),
            shares,
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        place_order(&context, &execution).await.unwrap();

        confirm_submission(&context, execution_id, BrokerOrderId::new("ORD-SUCCESS"))
            .await
            .unwrap();

        let legacy_row = sqlx::query!(
            "SELECT status, order_id FROM offchain_trades WHERE id = ?",
            execution_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(legacy_row.status, "SUBMITTED");
        assert_eq!(legacy_row.order_id.unwrap(), "ORD-SUCCESS");

        let result =
            confirm_submission(&context, execution_id, BrokerOrderId::new("ORD-DUPLICATE")).await;

        assert!(
            result.is_err(),
            "Second confirm_submission should fail at ES level (invalid state transition)"
        );

        let legacy_row_after = sqlx::query!(
            "SELECT status, order_id FROM offchain_trades WHERE id = ?",
            execution_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(
            legacy_row_after.order_id.unwrap(),
            "ORD-SUCCESS",
            "Legacy order_id should remain unchanged after failed duplicate submission"
        );
    }
}
