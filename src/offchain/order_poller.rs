use num_traits::ToPrimitive;
use rand::Rng;
use sqlx::SqlitePool;
use st0x_broker::{Broker, OrderState, OrderStatus, PersistenceError};
use std::time::Duration;
use tokio::time::{Interval, interval};
use tracing::{debug, error, info};

use super::execution::{
    OffchainExecution, find_execution_by_id, find_executions_by_symbol_status_and_broker,
};
use crate::dual_write::DualWriteContext;
use crate::error::{OnChainError, OrderPollingError};
use crate::lock::{clear_execution_lease, clear_pending_execution_id};

#[derive(Debug, Clone)]
pub struct OrderPollerConfig {
    pub polling_interval: Duration,
    pub max_jitter: Duration,
}

impl Default for OrderPollerConfig {
    fn default() -> Self {
        Self {
            polling_interval: Duration::from_secs(15),
            max_jitter: Duration::from_secs(5),
        }
    }
}

pub struct OrderStatusPoller<B: Broker> {
    config: OrderPollerConfig,
    pool: SqlitePool,
    interval: Interval,
    broker: B,
    dual_write_context: DualWriteContext,
}

impl<B: Broker> OrderStatusPoller<B> {
    pub fn new(
        config: OrderPollerConfig,
        pool: SqlitePool,
        broker: B,
        dual_write_context: DualWriteContext,
    ) -> Self {
        let interval = interval(config.polling_interval);

        Self {
            config,
            pool,
            interval,
            broker,
            dual_write_context,
        }
    }

    pub async fn run(mut self) -> Result<(), OrderPollingError> {
        info!(
            "Starting order status poller with interval: {:?}",
            self.config.polling_interval
        );

        loop {
            self.interval.tick().await;

            if let Err(e) = self.poll_pending_orders().await {
                error!("Polling cycle failed: {e}");
            }
        }
    }

    #[tracing::instrument(skip(self), level = tracing::Level::DEBUG)]
    async fn poll_pending_orders(&self) -> Result<(), OrderPollingError> {
        debug!("Starting polling cycle for submitted orders");

        let broker = self.broker.to_supported_broker();
        let submitted_executions = find_executions_by_symbol_status_and_broker(
            &self.pool,
            None,
            OrderStatus::Submitted,
            Some(broker),
        )
        .await?;

        if submitted_executions.is_empty() {
            debug!("No submitted orders to poll");
            return Ok(());
        }

        info!("Polling {} submitted orders", submitted_executions.len());

        for execution in submitted_executions {
            let Some(execution_id) = execution.id else {
                continue;
            };

            if let Err(e) = self.poll_execution_status(&execution).await {
                error!("Failed to poll execution {execution_id}: {e}");
            }

            self.add_jittered_delay().await;
        }

        debug!("Completed polling cycle");
        Ok(())
    }

    async fn poll_execution_status(
        &self,
        execution: &OffchainExecution,
    ) -> Result<(), OrderPollingError> {
        let Some(execution_id) = execution.id else {
            error!("Execution missing ID: {execution:?}");
            return Ok(());
        };

        let order_id = match &execution.state {
            OrderState::Pending => {
                debug!("Execution {execution_id} is PENDING but no order_id yet");
                return Ok(());
            }
            OrderState::Submitted { order_id } | OrderState::Filled { order_id, .. } => {
                order_id.clone()
            }
            OrderState::Failed { .. } => {
                debug!("Execution {execution_id} already failed, skipping poll");
                return Ok(());
            }
        };

        let parsed_order_id = self
            .broker
            .parse_order_id(&order_id)
            .map_err(|e| OrderPollingError::Broker(Box::new(e)))?;

        let order_state = self
            .broker
            .get_order_status(&parsed_order_id)
            .await
            .map_err(|e| OrderPollingError::Broker(Box::new(e)))?;

        match &order_state {
            OrderState::Filled { .. } => {
                self.handle_filled_order(execution_id, &order_state).await?;
            }
            OrderState::Failed { .. } => {
                self.handle_failed_order(execution_id, &order_state).await?;
            }
            OrderState::Pending | OrderState::Submitted { .. } => {
                debug!(
                    "Order {order_id} (execution {execution_id}) still pending with state: {:?}",
                    order_state
                );
            }
        }

        Ok(())
    }

    async fn handle_filled_order(
        &self,
        execution_id: i64,
        order_state: &OrderState,
    ) -> Result<(), OrderPollingError> {
        let new_status = order_state.clone();

        let mut tx = self.pool.begin().await?;

        let Some(execution) = find_execution_by_id(&self.pool, execution_id).await? else {
            error!("Execution {execution_id} not found in database");
            return Err(OrderPollingError::OnChain(OnChainError::Persistence(
                PersistenceError::InvalidTradeStatus("Execution not found".to_string()),
            )));
        };

        new_status.store_update(&mut tx, execution_id).await?;

        clear_pending_execution_id(&mut tx, &execution.symbol).await?;

        clear_execution_lease(&mut tx, &execution.symbol).await?;

        tx.commit().await?;

        if let OrderState::Filled { price_cents, .. } = order_state {
            info!(
                "Updated execution {execution_id} to FILLED with price: {} cents and cleared locks for symbol: {}",
                price_cents, execution.symbol
            );
        } else {
            info!(
                "Updated execution {execution_id} to FILLED and cleared locks for symbol: {}",
                execution.symbol
            );
        }

        let execution_with_state = OffchainExecution {
            id: Some(execution_id),
            symbol: execution.symbol.clone(),
            shares: execution.shares,
            direction: execution.direction,
            broker: execution.broker,
            state: order_state.clone(),
        };

        if let Err(e) =
            crate::dual_write::record_fill(&self.dual_write_context, &execution_with_state).await
        {
            error!(
                "Failed to execute OffchainOrder::CompleteFill command for execution {execution_id}: {e}"
            );
        }

        if let Err(e) = crate::dual_write::complete_offchain_order(
            &self.dual_write_context,
            &execution_with_state,
            &execution.symbol,
        )
        .await
        {
            error!(
                "Failed to execute Position::CompleteOffChainOrder command for execution {execution_id}, symbol {}: {e}",
                execution.symbol
            );
        }

        Ok(())
    }

    async fn handle_failed_order(
        &self,
        execution_id: i64,
        order_state: &OrderState,
    ) -> Result<(), OrderPollingError> {
        let new_status = order_state.clone();

        let mut tx = self.pool.begin().await?;

        let Some(execution) = find_execution_by_id(&self.pool, execution_id).await? else {
            error!("Execution {execution_id} not found in database");
            return Err(OrderPollingError::OnChain(OnChainError::Persistence(
                PersistenceError::InvalidTradeStatus("Execution not found".to_string()),
            )));
        };

        new_status.store_update(&mut tx, execution_id).await?;

        clear_pending_execution_id(&mut tx, &execution.symbol).await?;

        clear_execution_lease(&mut tx, &execution.symbol).await?;

        tx.commit().await?;

        info!(
            "Updated execution {execution_id} to FAILED and cleared locks for symbol: {}",
            execution.symbol
        );

        let error_message = match order_state {
            OrderState::Failed { error_reason, .. } => error_reason
                .clone()
                .unwrap_or_else(|| "Order failed with no error reason".to_string()),
            _ => "Unknown failure reason".to_string(),
        };

        if let Err(e) = crate::dual_write::mark_failed(
            &self.dual_write_context,
            execution_id,
            error_message.clone(),
        )
        .await
        {
            error!(
                "Failed to execute OffchainOrder::MarkFailed command for execution {execution_id}: {e}"
            );
        }

        if let Err(e) = crate::dual_write::fail_offchain_order(
            &self.dual_write_context,
            execution_id,
            &execution.symbol,
            error_message,
        )
        .await
        {
            error!(
                "Failed to execute Position::FailOffChainOrder command for execution {execution_id}, symbol {}: {e}",
                execution.symbol
            );
        }

        Ok(())
    }

    async fn add_jittered_delay(&self) {
        if self.config.max_jitter > Duration::ZERO {
            let max_jitter_u128 = self.config.max_jitter.as_millis().min(u128::from(u64::MAX));
            let max_jitter_millis = max_jitter_u128.to_u64().unwrap_or(u64::MAX);
            let jitter_millis = rand::thread_rng().gen_range(0..max_jitter_millis);
            let jitter = Duration::from_millis(jitter_millis);
            tokio::time::sleep(jitter).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use st0x_broker::{Direction, Shares, SupportedBroker};

    use super::*;
    use crate::test_utils::setup_test_db;

    #[tokio::test]
    async fn test_handle_filled_order_executes_dual_write_commands() {
        let pool = setup_test_db().await;
        let dual_write_context = DualWriteContext::new(pool.clone());
        let broker = st0x_broker::MockBroker::default();
        let config = OrderPollerConfig::default();

        let symbol = st0x_broker::Symbol::new("AAPL").unwrap();
        let shares = Shares::new(10).unwrap();

        let mut tx = pool.begin().await.unwrap();
        let submitted_state = OrderState::Submitted {
            order_id: "ORD123".to_string(),
        };
        let execution_id = submitted_state
            .store(
                &mut tx,
                &symbol,
                shares,
                Direction::Buy,
                SupportedBroker::Schwab,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let pending_execution = OffchainExecution {
            id: Some(execution_id),
            symbol: symbol.clone(),
            shares,
            direction: Direction::Buy,
            broker: SupportedBroker::Schwab,
            state: OrderState::Pending,
        };

        crate::dual_write::place_order(&dual_write_context, &pending_execution)
            .await
            .unwrap();

        crate::dual_write::confirm_submission(
            &dual_write_context,
            execution_id,
            "ORD123".to_string(),
        )
        .await
        .unwrap();

        let filled_state = OrderState::Filled {
            order_id: "ORD123".to_string(),
            price_cents: 15025,
            executed_at: Utc::now(),
        };

        let poller = OrderStatusPoller::new(config, pool.clone(), broker, dual_write_context);

        let result = poller
            .handle_filled_order(execution_id, &filled_state)
            .await;
        assert!(result.is_ok());

        let aggregate_id = execution_id.to_string();
        let offchain_order_events: Vec<String> = sqlx::query_scalar!(
            "SELECT event_type FROM events WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = ? ORDER BY sequence",
            aggregate_id
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(
            offchain_order_events.len(),
            3,
            "Expected 3 events (Placed, Submitted, Filled), got {offchain_order_events:?}"
        );

        assert_eq!(offchain_order_events[0], "OffchainOrderEvent::Placed");
        assert_eq!(offchain_order_events[1], "OffchainOrderEvent::Submitted");
        assert_eq!(offchain_order_events[2], "OffchainOrderEvent::Filled");
    }

    #[tokio::test]
    async fn test_handle_failed_order_executes_dual_write_commands() {
        let pool = setup_test_db().await;
        let dual_write_context = DualWriteContext::new(pool.clone());
        let broker = st0x_broker::MockBroker::default();
        let config = OrderPollerConfig::default();

        let symbol = st0x_broker::Symbol::new("TSLA").unwrap();
        let shares = Shares::new(5).unwrap();

        let mut tx = pool.begin().await.unwrap();
        let submitted_state = OrderState::Submitted {
            order_id: "ORD456".to_string(),
        };
        let execution_id = submitted_state
            .store(
                &mut tx,
                &symbol,
                shares,
                Direction::Sell,
                SupportedBroker::Schwab,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let pending_execution = OffchainExecution {
            id: Some(execution_id),
            symbol: symbol.clone(),
            shares,
            direction: Direction::Sell,
            broker: SupportedBroker::Schwab,
            state: OrderState::Pending,
        };

        crate::dual_write::place_order(&dual_write_context, &pending_execution)
            .await
            .unwrap();

        crate::dual_write::confirm_submission(
            &dual_write_context,
            execution_id,
            "ORD456".to_string(),
        )
        .await
        .unwrap();

        let failed_state = OrderState::Failed {
            failed_at: Utc::now(),
            error_reason: Some("Broker API timeout".to_string()),
        };

        let poller = OrderStatusPoller::new(config, pool.clone(), broker, dual_write_context);

        let result = poller
            .handle_failed_order(execution_id, &failed_state)
            .await;
        assert!(result.is_ok());

        let aggregate_id = execution_id.to_string();
        let offchain_order_events: Vec<String> = sqlx::query_scalar!(
            "SELECT event_type FROM events WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = ? ORDER BY sequence",
            aggregate_id
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(
            offchain_order_events.len(),
            3,
            "Expected 3 events (Placed, Submitted, Failed), got {offchain_order_events:?}"
        );

        assert_eq!(offchain_order_events[0], "OffchainOrderEvent::Placed");
        assert_eq!(offchain_order_events[1], "OffchainOrderEvent::Submitted");
        assert_eq!(offchain_order_events[2], "OffchainOrderEvent::Failed");
    }
}
