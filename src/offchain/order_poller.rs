use num_traits::ToPrimitive;
use rand::Rng;
use sqlx::SqlitePool;
use st0x_execution::{Executor, OrderState, OrderStatus, PersistenceError, Symbol};
use std::time::Duration;
use tokio::time::{Interval, interval};
use tracing::{debug, error, info, warn};

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

pub struct OrderStatusPoller<E: Executor> {
    config: OrderPollerConfig,
    pool: SqlitePool,
    interval: Interval,
    executor: E,
    dual_write_context: DualWriteContext,
}

impl<E: Executor> OrderStatusPoller<E> {
    pub fn new(
        config: OrderPollerConfig,
        pool: SqlitePool,
        executor: E,
        dual_write_context: DualWriteContext,
    ) -> Self {
        let interval = interval(config.polling_interval);

        Self {
            config,
            pool,
            interval,
            executor,
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

        let executor_type = self.executor.to_supported_executor();
        let submitted_executions = find_executions_by_symbol_status_and_broker(
            &self.pool,
            None,
            OrderStatus::Submitted,
            Some(executor_type),
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

        let Some(order_id) = extract_order_id(execution_id, &execution.state) else {
            warn!(
                execution_id = execution_id,
                symbol = %execution.symbol,
                state = ?execution.state,
                "Missing order_id for polled execution"
            );
            return Ok(());
        };

        let parsed_order_id = self
            .executor
            .parse_order_id(&order_id)
            .map_err(|e| OrderPollingError::Executor(Box::new(e)))?;

        let order_state = self
            .executor
            .get_order_status(&parsed_order_id)
            .await
            .map_err(|e| OrderPollingError::Executor(Box::new(e)))?;

        self.process_order_state(execution_id, &order_id, &order_state)
            .await
    }

    async fn process_order_state(
        &self,
        execution_id: i64,
        order_id: &str,
        order_state: &OrderState,
    ) -> Result<(), OrderPollingError> {
        match order_state {
            OrderState::Filled { .. } => self.handle_filled_order(execution_id, order_state).await,
            OrderState::Failed { .. } => self.handle_failed_order(execution_id, order_state).await,
            OrderState::Pending | OrderState::Submitted { .. } => {
                debug!(
                    "Order {order_id} (execution {execution_id}) still pending with state: {order_state:?}"
                );
                Ok(())
            }
        }
    }

    async fn handle_filled_order(
        &self,
        execution_id: i64,
        order_state: &OrderState,
    ) -> Result<(), OrderPollingError> {
        let execution = self.finalize_order(execution_id, order_state).await?;

        let OrderState::Filled { price_cents, .. } = order_state else {
            return Ok(());
        };

        log_filled_order(execution_id, *price_cents, &execution);

        let execution_with_state = OffchainExecution {
            id: Some(execution_id),
            symbol: execution.symbol.clone(),
            shares: execution.shares,
            direction: execution.direction,
            executor: execution.executor,
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
        let execution = self.finalize_order(execution_id, order_state).await?;
        let symbol = &execution.symbol;
        info!("Updated execution {execution_id} to FAILED and cleared locks for symbol: {symbol}");

        let error_message = extract_error_message(order_state);
        self.execute_failed_order_dual_write(execution_id, symbol, error_message)
            .await;

        Ok(())
    }

    async fn execute_failed_order_dual_write(
        &self,
        execution_id: i64,
        symbol: &Symbol,
        error_message: String,
    ) {
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
            symbol,
            error_message,
        )
        .await
        {
            error!(
                "Failed to execute Position::FailOffChainOrder command for execution {execution_id}, symbol {symbol}: {e}"
            );
        }
    }

    async fn finalize_order(
        &self,
        execution_id: i64,
        order_state: &OrderState,
    ) -> Result<OffchainExecution, OrderPollingError> {
        let Some(execution) = find_execution_by_id(&self.pool, execution_id).await? else {
            error!("Execution {execution_id} not found in database");
            return Err(OrderPollingError::OnChain(OnChainError::Persistence(
                PersistenceError::InvalidTradeStatus("Execution not found".to_string()),
            )));
        };

        let mut tx = self.pool.begin().await?;
        order_state
            .clone()
            .store_update(&mut tx, execution_id)
            .await?;
        clear_pending_execution_id(&mut tx, &execution.symbol).await?;
        clear_execution_lease(&mut tx, &execution.symbol).await?;
        tx.commit().await?;

        Ok(execution)
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

fn extract_error_message(order_state: &OrderState) -> String {
    match order_state {
        OrderState::Failed { error_reason, .. } => error_reason
            .clone()
            .unwrap_or_else(|| "Order failed with no error reason".to_string()),
        _ => "Unknown failure reason".to_string(),
    }
}

fn extract_order_id(execution_id: i64, state: &OrderState) -> Option<String> {
    match state {
        OrderState::Pending => {
            debug!("Execution {execution_id} is PENDING but no order_id yet");
            None
        }
        OrderState::Submitted { order_id } | OrderState::Filled { order_id, .. } => {
            Some(order_id.clone())
        }
        OrderState::Failed { .. } => {
            debug!("Execution {execution_id} already failed, skipping poll");
            None
        }
    }
}

fn log_filled_order(execution_id: i64, price_cents: u64, execution: &OffchainExecution) {
    let symbol = &execution.symbol;
    info!(
        execution_id,
        price_cents,
        %symbol,
        "Updated execution to FILLED and cleared locks"
    );
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use st0x_execution::{Direction, MockExecutor, Shares, SupportedExecutor, Symbol};

    use super::*;
    use crate::offchain_order::BrokerOrderId;
    use crate::test_utils::{OnchainTradeBuilder, setup_test_db};
    use crate::threshold::ExecutionThreshold;

    async fn setup_position_with_onchain_fill(
        dual_write_context: &DualWriteContext,
        symbol: &Symbol,
        tokenized_symbol: &str,
        amount: f64,
    ) {
        crate::dual_write::initialize_position(
            dual_write_context,
            symbol,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();

        let mut onchain_trade = OnchainTradeBuilder::new()
            .with_symbol(tokenized_symbol)
            .with_amount(amount)
            .with_price(150.0)
            .build();
        onchain_trade.direction = Direction::Buy;
        onchain_trade.block_timestamp = Some(Utc::now());

        crate::dual_write::acknowledge_onchain_fill(dual_write_context, &onchain_trade)
            .await
            .unwrap();
    }

    async fn setup_offchain_order_aggregate(
        dual_write_context: &DualWriteContext,
        execution: &OffchainExecution,
        symbol: &st0x_execution::Symbol,
        order_id: &str,
    ) {
        crate::dual_write::place_order(dual_write_context, execution)
            .await
            .unwrap();

        crate::dual_write::place_offchain_order(dual_write_context, execution, symbol)
            .await
            .unwrap();

        crate::dual_write::confirm_submission(
            dual_write_context,
            execution.id.unwrap(),
            BrokerOrderId::new(order_id),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_handle_filled_order_executes_dual_write_commands() {
        let pool = setup_test_db().await;
        let dual_write_context = DualWriteContext::new(pool.clone());
        let broker = MockExecutor::default();
        let config = OrderPollerConfig::default();

        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Shares::new(10).unwrap();

        setup_position_with_onchain_fill(&dual_write_context, &symbol, "AAPL0x", 10.0).await;

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
                SupportedExecutor::Schwab,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let pending_execution = OffchainExecution {
            id: Some(execution_id),
            symbol: symbol.clone(),
            shares,
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        setup_offchain_order_aggregate(&dual_write_context, &pending_execution, &symbol, "ORD123")
            .await;

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
            "SELECT event_type FROM events \
            WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = ? ORDER BY sequence",
            aggregate_id
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(offchain_order_events.len(), 3);
        assert_eq!(offchain_order_events[0], "OffchainOrderEvent::Placed");
        assert_eq!(offchain_order_events[1], "OffchainOrderEvent::Submitted");
        assert_eq!(offchain_order_events[2], "OffchainOrderEvent::Filled");

        let position_events: Vec<String> = sqlx::query_scalar!(
            "SELECT event_type FROM events \
            WHERE aggregate_type = 'Position' AND aggregate_id = 'AAPL' ORDER BY sequence"
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(position_events.len(), 4);
        assert_eq!(position_events[0], "PositionEvent::Initialized");
        assert_eq!(position_events[1], "PositionEvent::OnChainOrderFilled");
        assert_eq!(position_events[2], "PositionEvent::OffChainOrderPlaced");
        assert_eq!(position_events[3], "PositionEvent::OffChainOrderFilled");
    }

    #[tokio::test]
    async fn test_handle_failed_order_executes_dual_write_commands() {
        let pool = setup_test_db().await;
        let dual_write_context = DualWriteContext::new(pool.clone());
        let broker = MockExecutor::default();
        let config = OrderPollerConfig::default();

        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Shares::new(5).unwrap();

        setup_position_with_onchain_fill(&dual_write_context, &symbol, "TSLA0x", 5.0).await;

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
                SupportedExecutor::Schwab,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let pending_execution = OffchainExecution {
            id: Some(execution_id),
            symbol: symbol.clone(),
            shares,
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        setup_offchain_order_aggregate(&dual_write_context, &pending_execution, &symbol, "ORD456")
            .await;

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
            "SELECT event_type FROM events \
            WHERE aggregate_type = 'OffchainOrder' AND aggregate_id = ? ORDER BY sequence",
            aggregate_id
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(offchain_order_events.len(), 3);
        assert_eq!(offchain_order_events[0], "OffchainOrderEvent::Placed");
        assert_eq!(offchain_order_events[1], "OffchainOrderEvent::Submitted");
        assert_eq!(offchain_order_events[2], "OffchainOrderEvent::Failed");

        let position_events: Vec<String> = sqlx::query_scalar!(
            "SELECT event_type FROM events \
            WHERE aggregate_type = 'Position' AND aggregate_id = 'TSLA' ORDER BY sequence"
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(position_events.len(), 4);
        assert_eq!(position_events[0], "PositionEvent::Initialized");
        assert_eq!(position_events[1], "PositionEvent::OnChainOrderFilled");
        assert_eq!(position_events[2], "PositionEvent::OffChainOrderPlaced");
        assert_eq!(position_events[3], "PositionEvent::OffChainOrderFailed");
    }

    async fn setup_stuck_execution_state(pool: &SqlitePool, symbol: &Symbol, execution_id: i64) {
        let symbol_str = symbol.to_string();

        sqlx::query!(
            r#"
            INSERT INTO trade_accumulators (
                symbol,
                accumulated_long,
                accumulated_short,
                pending_execution_id
            )
            VALUES (?1, 0.0, 0.0, ?2)
            ON CONFLICT(symbol) DO UPDATE SET
                pending_execution_id = excluded.pending_execution_id
            "#,
            symbol_str,
            execution_id
        )
        .execute(pool)
        .await
        .unwrap();

        sqlx::query("INSERT INTO symbol_locks (symbol) VALUES (?1)")
            .bind(symbol.to_string())
            .execute(pool)
            .await
            .unwrap();
    }

    async fn assert_locks_cleared(pool: &SqlitePool, symbol: &Symbol) {
        let symbol_str = symbol.to_string();

        let row = sqlx::query!(
            "SELECT pending_execution_id FROM trade_accumulators WHERE symbol = ?1",
            symbol_str
        )
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(
            row.pending_execution_id, None,
            "pending_execution_id should be cleared"
        );

        let lock_count: i64 = sqlx::query_scalar!(
            "SELECT COUNT(*) as count FROM symbol_locks WHERE symbol = ?1",
            symbol_str
        )
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(lock_count, 0, "symbol_locks should be cleared");
    }

    /// Reproduces the production recovery scenario:
    /// - Execution is SUBMITTED with order_id
    /// - pending_execution_id is set in trade_accumulators
    /// - symbol_locks has a stale lock
    /// - Order poller finds order is FILLED
    /// - Verifies BOTH pending_execution_id AND symbol_locks are cleared
    #[tokio::test]
    async fn test_handle_filled_order_clears_pending_execution_id_and_symbol_lock() {
        let pool = setup_test_db().await;
        let dual_write_context = DualWriteContext::new(pool.clone());
        let broker = MockExecutor::default();
        let config = OrderPollerConfig::default();

        let symbol = Symbol::new("BMNR").unwrap();
        let shares = Shares::new(1).unwrap();
        let order_id = "1005070742758";

        setup_position_with_onchain_fill(&dual_write_context, &symbol, "BMNR0x", 1.0).await;

        let mut tx = pool.begin().await.unwrap();
        let submitted_state = OrderState::Submitted {
            order_id: order_id.to_string(),
        };
        let execution_id = submitted_state
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

        let pending_execution = OffchainExecution {
            id: Some(execution_id),
            symbol: symbol.clone(),
            shares,
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            state: OrderState::Pending,
        };

        setup_offchain_order_aggregate(&dual_write_context, &pending_execution, &symbol, order_id)
            .await;

        setup_stuck_execution_state(&pool, &symbol, execution_id).await;

        let filled_state = OrderState::Filled {
            order_id: order_id.to_string(),
            price_cents: 3181,
            executed_at: Utc::now(),
        };

        let poller = OrderStatusPoller::new(config, pool.clone(), broker, dual_write_context);
        poller
            .handle_filled_order(execution_id, &filled_state)
            .await
            .expect("handle_filled_order should succeed");

        assert_locks_cleared(&pool, &symbol).await;

        let legacy_row = sqlx::query!(
            "SELECT status, order_id, price_cents FROM offchain_trades WHERE id = ?1",
            execution_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(legacy_row.status, "FILLED");
        assert_eq!(legacy_row.order_id, Some(order_id.to_string()));
        assert_eq!(legacy_row.price_cents, Some(3181));
    }
}
