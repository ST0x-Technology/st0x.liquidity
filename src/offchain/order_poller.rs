use std::sync::Arc;
use std::time::Duration;

use num_traits::ToPrimitive;
use rand::Rng;
use sqlite_es::SqliteCqrs;
use sqlx::SqlitePool;
use st0x_execution::{
    ArithmeticError, Executor, ExecutorOrderId, FractionalShares, OrderState, OrderStatus, Symbol,
};
use tokio::time::{Interval, interval};
use tracing::{debug, error, info, warn};

use super::execution::find_executions_by_symbol_status_and_broker;
use crate::error::OrderPollingError;
use crate::lifecycle::{Lifecycle, Never};
use crate::offchain_order::{
    OffchainOrder, OffchainOrderCommand, OffchainOrderCqrs, OffchainOrderId, OffchainOrderView,
    PriceCents,
};
use crate::position::{Position, PositionCommand};
type PositionCqrs = SqliteCqrs<Lifecycle<Position, ArithmeticError<FractionalShares>>>;

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
    offchain_order_cqrs: Arc<OffchainOrderCqrs>,
    position_cqrs: Arc<PositionCqrs>,
}

impl<E: Executor> OrderStatusPoller<E> {
    pub fn new(
        config: OrderPollerConfig,
        pool: SqlitePool,
        executor: E,
        offchain_order_cqrs: Arc<OffchainOrderCqrs>,
        position_cqrs: Arc<PositionCqrs>,
    ) -> Self {
        let interval = interval(config.polling_interval);

        Self {
            config,
            pool,
            interval,
            executor,
            offchain_order_cqrs,
            position_cqrs,
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

        for execution in &submitted_executions {
            let OffchainOrderView::Execution {
                offchain_order_id, ..
            } = execution
            else {
                continue;
            };

            if let Err(e) = self.poll_execution_status(execution).await {
                error!("Failed to poll execution {}: {e}", offchain_order_id);
            }

            self.add_jittered_delay().await;
        }

        debug!("Completed polling cycle");
        Ok(())
    }

    async fn poll_execution_status(
        &self,
        execution: &OffchainOrderView,
    ) -> Result<(), OrderPollingError> {
        let OffchainOrderView::Execution {
            offchain_order_id,
            executor_order_id,
            symbol,
            ..
        } = execution
        else {
            return Ok(());
        };

        let Some(executor_order_id) = executor_order_id else {
            warn!(
                offchain_order_id = %offchain_order_id,
                %symbol,
                "Missing executor_order_id for submitted execution"
            );
            return Ok(());
        };

        let parsed_order_id = self
            .executor
            .parse_order_id(executor_order_id.as_ref())
            .map_err(|e| OrderPollingError::Executor(Box::new(e)))?;

        let order_state = self
            .executor
            .get_order_status(&parsed_order_id)
            .await
            .map_err(|e| OrderPollingError::Executor(Box::new(e)))?;

        match &order_state {
            OrderState::Filled {
                price_cents,
                order_id,
                executed_at,
            } => {
                self.handle_filled_order(
                    execution,
                    PriceCents(*price_cents),
                    &ExecutorOrderId::new(&order_id),
                    *executed_at,
                )
                .await
            }
            OrderState::Failed { error_reason, .. } => {
                let error_message = error_reason
                    .clone()
                    .unwrap_or_else(|| "Order failed with no error reason".to_string());
                self.handle_failed_order(execution, error_message).await
            }
            OrderState::Pending | OrderState::Submitted { .. } => {
                debug!(
                    offchain_order_id = %offchain_order_id,
                    %symbol,
                    "Order still pending"
                );
                Ok(())
            }
        }
    }

    async fn handle_filled_order(
        &self,
        execution: &OffchainOrderView,
        price_cents: PriceCents,
        executor_order_id: &ExecutorOrderId,
        broker_timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), OrderPollingError> {
        let OffchainOrderView::Execution {
            offchain_order_id,
            symbol,
            shares,
            direction,
            ..
        } = execution
        else {
            return Ok(());
        };

        info!(
            offchain_order_id = %offchain_order_id,
            price_cents = price_cents.0,
            %symbol,
            "Order filled, executing CQRS commands"
        );

        let offchain_aggregate_id = offchain_order_id.to_string();
        if let Err(e) = self
            .offchain_order_cqrs
            .execute(
                &offchain_aggregate_id,
                OffchainOrderCommand::CompleteFill { price_cents },
            )
            .await
        {
            error!(
                "Failed to execute OffchainOrder::CompleteFill for execution {offchain_order_id}: {e}"
            );
        }

        let position_aggregate_id = Position::aggregate_id(symbol);
        if let Err(e) = self
            .position_cqrs
            .execute(
                &position_aggregate_id,
                PositionCommand::CompleteOffChainOrder {
                    offchain_order_id: *offchain_order_id,
                    shares_filled: shares.inner(),
                    direction: *direction,
                    executor_order_id: executor_order_id.clone(),
                    price_cents,
                    broker_timestamp,
                },
            )
            .await
        {
            error!(
                "Failed to execute Position::CompleteOffChainOrder for execution {offchain_order_id}, symbol {symbol}: {e}"
            );
        }

        Ok(())
    }

    async fn handle_failed_order(
        &self,
        execution: &OffchainOrderView,
        error_message: String,
    ) -> Result<(), OrderPollingError> {
        let OffchainOrderView::Execution {
            offchain_order_id,
            symbol,
            ..
        } = execution
        else {
            return Ok(());
        };

        info!(
            offchain_order_id = %offchain_order_id,
            %symbol,
            "Order failed, executing CQRS commands"
        );

        let offchain_aggregate_id = offchain_order_id.to_string();
        if let Err(e) = self
            .offchain_order_cqrs
            .execute(
                &offchain_aggregate_id,
                OffchainOrderCommand::MarkFailed {
                    error: error_message.clone(),
                },
            )
            .await
        {
            error!(
                "Failed to execute OffchainOrder::MarkFailed for execution {offchain_order_id}: {e}"
            );
        }

        let position_aggregate_id = Position::aggregate_id(symbol);
        if let Err(e) = self
            .position_cqrs
            .execute(
                &position_aggregate_id,
                PositionCommand::FailOffChainOrder {
                    offchain_order_id: *offchain_order_id,
                    error: error_message,
                },
            )
            .await
        {
            error!(
                "Failed to execute Position::FailOffChainOrder for execution {offchain_order_id}, symbol {symbol}: {e}"
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
    use rust_decimal::Decimal;
    use sqlite_es::sqlite_cqrs;
    use st0x_execution::{
        Direction, FractionalShares, MockExecutor, Positive, SupportedExecutor, Symbol,
    };

    use super::*;
    use crate::onchain::OnchainTrade;
    use crate::onchain::io::Usdc;
    use crate::onchain_trade::{OnChainTrade, OnChainTradeCommand};
    use crate::position::TradeId;
    use crate::test_utils::{OnchainTradeBuilder, setup_test_db};
    use crate::threshold::ExecutionThreshold;

    fn create_test_frameworks(pool: &SqlitePool) -> (Arc<OffchainOrderCqrs>, Arc<PositionCqrs>) {
        (
            Arc::new(sqlite_cqrs(pool.clone(), vec![], ())),
            Arc::new(sqlite_cqrs(pool.clone(), vec![], ())),
        )
    }

    async fn setup_position_with_onchain_fill(
        position_cqrs: &PositionCqrs,
        symbol: &Symbol,
        tokenized_symbol: &str,
        amount: f64,
    ) {
        let aggregate_id = Position::aggregate_id(symbol);

        position_cqrs
            .execute(
                &aggregate_id,
                PositionCommand::Initialize {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                },
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

        let trade_id = TradeId {
            tx_hash: onchain_trade.tx_hash,
            log_index: onchain_trade.log_index,
        };
        let decimal_amount =
            FractionalShares::new(Decimal::try_from(onchain_trade.amount).unwrap());
        let price_usdc = Decimal::try_from(onchain_trade.price.value()).unwrap();

        position_cqrs
            .execute(
                &aggregate_id,
                PositionCommand::AcknowledgeOnChainFill {
                    trade_id,
                    amount: decimal_amount,
                    direction: onchain_trade.direction,
                    price_usdc,
                    block_timestamp: onchain_trade.block_timestamp.unwrap(),
                },
            )
            .await
            .unwrap();
    }

    async fn setup_offchain_order_aggregate(
        offchain_order_cqrs: &OffchainOrderCqrs,
        position_cqrs: &PositionCqrs,
        offchain_order_id: OffchainOrderId,
        symbol: &Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
        order_id: &str,
    ) {
        let offchain_agg_id = offchain_order_id.to_string();

        offchain_order_cqrs
            .execute(
                &offchain_agg_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares,
                    direction,
                    executor,
                },
            )
            .await
            .unwrap();

        let position_agg_id = Position::aggregate_id(symbol);
        position_cqrs
            .execute(
                &position_agg_id,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares,
                    direction,
                    executor,
                },
            )
            .await
            .unwrap();

        offchain_order_cqrs
            .execute(
                &offchain_agg_id,
                OffchainOrderCommand::ConfirmSubmission {
                    executor_order_id: ExecutorOrderId::new(order_id),
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_handle_filled_order_executes_cqrs_commands() {
        let pool = setup_test_db().await;
        let (offchain_order_cqrs, position_cqrs) = create_test_frameworks(&pool);
        let broker = MockExecutor::default();
        let config = OrderPollerConfig::default();

        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(Decimal::from(10))).unwrap();

        setup_position_with_onchain_fill(&position_cqrs, &symbol, "AAPL0x", 10.0).await;

        let offchain_order_id = OffchainOrder::aggregate_id();
        setup_offchain_order_aggregate(
            &offchain_order_cqrs,
            &position_cqrs,
            offchain_order_id,
            &symbol,
            shares,
            Direction::Buy,
            SupportedExecutor::Schwab,
            "ORD123",
        )
        .await;

        let execution = OffchainOrderView::Execution {
            offchain_order_id,
            symbol: symbol.clone(),
            shares,
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            status: OrderStatus::Submitted,
            executor_order_id: Some(ExecutorOrderId::new("ORD123")),
            price_cents: None,
            initiated_at: Utc::now(),
            completed_at: None,
        };

        let poller = OrderStatusPoller::new(
            config,
            pool.clone(),
            broker,
            offchain_order_cqrs,
            position_cqrs,
        );

        let result = poller
            .handle_filled_order(
                &execution,
                PriceCents(15025),
                &ExecutorOrderId::new("ORD123"),
                Utc::now(),
            )
            .await;
        assert!(result.is_ok());

        let aggregate_id = offchain_order_id.to_string();
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
    async fn test_handle_failed_order_executes_cqrs_commands() {
        let pool = setup_test_db().await;
        let (offchain_order_cqrs, position_cqrs) = create_test_frameworks(&pool);
        let broker = MockExecutor::default();
        let config = OrderPollerConfig::default();

        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(Decimal::from(5))).unwrap();

        setup_position_with_onchain_fill(&position_cqrs, &symbol, "TSLA0x", 5.0).await;

        let offchain_order_id = OffchainOrder::aggregate_id();
        setup_offchain_order_aggregate(
            &offchain_order_cqrs,
            &position_cqrs,
            offchain_order_id,
            &symbol,
            shares,
            Direction::Sell,
            SupportedExecutor::Schwab,
            "ORD456",
        )
        .await;

        let execution = OffchainOrderView::Execution {
            offchain_order_id,
            symbol: symbol.clone(),
            shares,
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            status: OrderStatus::Submitted,
            executor_order_id: Some(ExecutorOrderId::new("ORD456")),
            price_cents: None,
            initiated_at: Utc::now(),
            completed_at: None,
        };

        let poller = OrderStatusPoller::new(
            config,
            pool.clone(),
            broker,
            offchain_order_cqrs,
            position_cqrs,
        );

        let result = poller
            .handle_failed_order(&execution, "Broker API timeout".to_string())
            .await;
        assert!(result.is_ok());

        let aggregate_id = offchain_order_id.to_string();
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
}
