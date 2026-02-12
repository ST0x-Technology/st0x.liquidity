//! Polls broker APIs for order status updates and reconciles fills.

use num_traits::ToPrimitive;
use rand::Rng;
use sqlx::SqlitePool;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Interval, interval};
use tracing::{debug, error, info, warn};

use cqrs_es::AggregateError;
use st0x_execution::{
    ExecutionError, Executor, ExecutorOrderId, OrderState, OrderStatus, PersistenceError, Symbol,
};

use super::execution::find_orders_by_status;
use crate::lifecycle::{Lifecycle, LifecycleError};
use crate::offchain_order::{
    OffchainOrder, OffchainOrderCommand, OffchainOrderCqrs, OffchainOrderId, PriceCents,
};
use crate::onchain::OnChainError;
use crate::position::{Position, PositionCommand, PositionCqrs};

/// Order polling errors for order status monitoring.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OrderPollingError {
    #[error("Executor error: {0}")]
    Executor(Box<dyn std::error::Error + Send + Sync>),
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Persistence error: {0}")]
    Persistence(#[from] PersistenceError),
    #[error("Onchain error: {0}")]
    OnChain(#[from] OnChainError),
    #[error("Offchain order aggregate error: {0}")]
    OffchainOrderAggregate(#[from] AggregateError<LifecycleError<OffchainOrder>>),
    #[error("Position aggregate error: {0}")]
    PositionAggregate(#[from] AggregateError<LifecycleError<Position>>),
}

impl From<ExecutionError> for OrderPollingError {
    fn from(err: ExecutionError) -> Self {
        Self::Executor(Box::new(err))
    }
}

#[derive(Debug, Clone)]
pub struct OrderPollerCtx {
    pub polling_interval: Duration,
    pub max_jitter: Duration,
}

impl Default for OrderPollerCtx {
    fn default() -> Self {
        Self {
            polling_interval: Duration::from_secs(15),
            max_jitter: Duration::from_secs(5),
        }
    }
}

pub struct OrderStatusPoller<E: Executor> {
    ctx: OrderPollerCtx,
    pool: SqlitePool,
    interval: Interval,
    executor: E,
    offchain_order_cqrs: Arc<OffchainOrderCqrs>,
    position_cqrs: Arc<PositionCqrs>,
}

impl<E: Executor> OrderStatusPoller<E> {
    pub fn new(
        ctx: OrderPollerCtx,
        pool: SqlitePool,
        executor: E,
        offchain_order_cqrs: Arc<OffchainOrderCqrs>,
        position_cqrs: Arc<PositionCqrs>,
    ) -> Self {
        let interval = interval(ctx.polling_interval);

        Self {
            ctx,
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
            self.ctx.polling_interval
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
        let submitted_executions: Vec<_> =
            find_orders_by_status(&self.pool, OrderStatus::Submitted)
                .await?
                .into_iter()
                .filter(|(_, lifecycle)| {
                    lifecycle
                        .live()
                        .map(|order| order.executor() == executor_type)
                        .unwrap_or(false)
                })
                .collect();

        if submitted_executions.is_empty() {
            debug!("No submitted orders to poll");
            return Ok(());
        }

        info!("Polling {} submitted orders", submitted_executions.len());

        for (offchain_order_id, aggregate) in &submitted_executions {
            let Lifecycle::Live(order) = aggregate else {
                warn!(%offchain_order_id, "Skipping non-live aggregate");
                continue;
            };

            if let Err(error) = self.poll_execution_status(*offchain_order_id, order).await {
                error!("Failed to poll execution {offchain_order_id}: {error}");
            }

            self.add_jittered_delay().await;
        }

        debug!("Completed polling cycle");
        Ok(())
    }

    async fn poll_execution_status(
        &self,
        offchain_order_id: OffchainOrderId,
        order: &OffchainOrder,
    ) -> Result<(), OrderPollingError> {
        let symbol = order.symbol();

        let Some(executor_order_id) = order.executor_order_id() else {
            warn!(
                %offchain_order_id,
                %symbol,
                "Missing executor_order_id for submitted execution"
            );
            return Ok(());
        };

        let parsed_order_id = self
            .executor
            .parse_order_id(executor_order_id.as_ref())
            .map_err(|error| OrderPollingError::Executor(Box::new(error)))?;

        let order_state = self
            .executor
            .get_order_status(&parsed_order_id)
            .await
            .map_err(|error| OrderPollingError::Executor(Box::new(error)))?;

        match &order_state {
            OrderState::Filled {
                price_cents,
                order_id,
                executed_at,
            } => {
                self.handle_filled_order(
                    offchain_order_id,
                    order,
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
                self.handle_failed_order(offchain_order_id, order, error_message)
                    .await
            }
            OrderState::Pending | OrderState::Submitted { .. } => {
                debug!(
                    %offchain_order_id,
                    %symbol,
                    "Order still pending"
                );
                Ok(())
            }
        }
    }

    async fn handle_filled_order(
        &self,
        offchain_order_id: OffchainOrderId,
        order: &OffchainOrder,
        price_cents: PriceCents,
        executor_order_id: &ExecutorOrderId,
        broker_timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), OrderPollingError> {
        let symbol = order.symbol();

        info!(
            %offchain_order_id,
            price_cents = price_cents.0,
            %symbol,
            "Order filled, executing CQRS commands"
        );

        self.complete_offchain_order_fill(offchain_order_id, price_cents)
            .await?;

        self.complete_position_order(
            offchain_order_id,
            order,
            price_cents,
            executor_order_id,
            broker_timestamp,
        )
        .await
    }

    async fn complete_offchain_order_fill(
        &self,
        offchain_order_id: OffchainOrderId,
        price_cents: PriceCents,
    ) -> Result<(), OrderPollingError> {
        let offchain_aggregate_id = OffchainOrder::aggregate_id(offchain_order_id);
        self.offchain_order_cqrs
            .execute(
                &offchain_aggregate_id,
                OffchainOrderCommand::CompleteFill { price_cents },
            )
            .await?;
        Ok(())
    }

    async fn complete_position_order(
        &self,
        offchain_order_id: OffchainOrderId,
        order: &OffchainOrder,
        price_cents: PriceCents,
        executor_order_id: &ExecutorOrderId,
        broker_timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), OrderPollingError> {
        let symbol = order.symbol();
        let position_aggregate_id = Position::aggregate_id(symbol);
        self.position_cqrs
            .execute(
                &position_aggregate_id,
                PositionCommand::CompleteOffChainOrder {
                    offchain_order_id,
                    shares_filled: order.shares(),
                    direction: order.direction(),
                    executor_order_id: executor_order_id.clone(),
                    price_cents,
                    broker_timestamp,
                },
            )
            .await?;
        Ok(())
    }

    async fn handle_failed_order(
        &self,
        offchain_order_id: OffchainOrderId,
        order: &OffchainOrder,
        error_message: String,
    ) -> Result<(), OrderPollingError> {
        let symbol = order.symbol();

        info!(
            %offchain_order_id,
            %symbol,
            "Order failed, executing CQRS commands"
        );

        self.mark_offchain_order_failed(offchain_order_id, &error_message)
            .await?;

        self.fail_position_order(offchain_order_id, symbol, error_message)
            .await
    }

    async fn mark_offchain_order_failed(
        &self,
        offchain_order_id: OffchainOrderId,
        error_message: &str,
    ) -> Result<(), OrderPollingError> {
        let offchain_aggregate_id = OffchainOrder::aggregate_id(offchain_order_id);
        self.offchain_order_cqrs
            .execute(
                &offchain_aggregate_id,
                OffchainOrderCommand::MarkFailed {
                    error: error_message.to_owned(),
                },
            )
            .await?;
        Ok(())
    }

    async fn fail_position_order(
        &self,
        offchain_order_id: OffchainOrderId,
        symbol: &Symbol,
        error_message: String,
    ) -> Result<(), OrderPollingError> {
        let position_aggregate_id = Position::aggregate_id(symbol);
        self.position_cqrs
            .execute(
                &position_aggregate_id,
                PositionCommand::FailOffChainOrder {
                    offchain_order_id,
                    error: error_message,
                },
            )
            .await?;
        Ok(())
    }

    async fn add_jittered_delay(&self) {
        if self.ctx.max_jitter > Duration::ZERO {
            let max_jitter_u128 = self.ctx.max_jitter.as_millis().min(u128::from(u64::MAX));
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
    use rust_decimal_macros::dec;
    use sqlite_es::sqlite_cqrs;

    use st0x_execution::{
        Direction, FractionalShares, MockExecutor, Positive, SupportedExecutor, Symbol,
    };

    use super::*;
    use crate::position::TradeId;
    use crate::test_utils::{OnchainTradeBuilder, setup_test_db};
    use crate::threshold::ExecutionThreshold;

    fn create_test_frameworks(pool: &SqlitePool) -> (Arc<OffchainOrderCqrs>, Arc<PositionCqrs>) {
        (
            Arc::new(sqlite_cqrs(
                pool.clone(),
                vec![],
                crate::offchain_order::noop_order_placer(),
            )),
            Arc::new(sqlite_cqrs(pool.clone(), vec![], ())),
        )
    }

    async fn setup_position_with_onchain_fill(
        position_cqrs: &PositionCqrs,
        symbol: &Symbol,
        tokenized_symbol: &str,
        amount: Decimal,
    ) {
        let aggregate_id = Position::aggregate_id(symbol);

        let mut onchain_trade = OnchainTradeBuilder::new()
            .with_symbol(tokenized_symbol)
            .with_amount(amount)
            .with_price(dec!(150.0))
            .build();
        onchain_trade.direction = Direction::Buy;
        onchain_trade.block_timestamp = Some(Utc::now());

        let trade_id = TradeId {
            tx_hash: onchain_trade.tx_hash,
            log_index: onchain_trade.log_index,
        };
        let decimal_amount = onchain_trade.amount;
        let price_usdc = onchain_trade.price.value();

        position_cqrs
            .execute(
                &aggregate_id,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
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

    struct TestOrderParams<'a> {
        symbol: &'a Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        executor: SupportedExecutor,
    }

    async fn setup_offchain_order_aggregate(
        offchain_order_cqrs: &OffchainOrderCqrs,
        position_cqrs: &PositionCqrs,
        offchain_order_id: OffchainOrderId,
        params: TestOrderParams<'_>,
    ) {
        let offchain_agg_id = OffchainOrder::aggregate_id(offchain_order_id);

        offchain_order_cqrs
            .execute(
                &offchain_agg_id,
                OffchainOrderCommand::Place {
                    symbol: params.symbol.clone(),
                    shares: params.shares,
                    direction: params.direction,
                    executor: params.executor,
                },
            )
            .await
            .unwrap();

        let position_agg_id = Position::aggregate_id(params.symbol);
        position_cqrs
            .execute(
                &position_agg_id,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares: params.shares,
                    direction: params.direction,
                    executor: params.executor,
                    threshold: ExecutionThreshold::whole_share(),
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
        let ctx = OrderPollerCtx::default();

        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(Decimal::from(10))).unwrap();

        setup_position_with_onchain_fill(&position_cqrs, &symbol, "tAAPL", dec!(10)).await;

        let offchain_order_id = OffchainOrderId::new();
        setup_offchain_order_aggregate(
            &offchain_order_cqrs,
            &position_cqrs,
            offchain_order_id,
            TestOrderParams {
                symbol: &symbol,
                shares,
                direction: Direction::Buy,
                executor: SupportedExecutor::Schwab,
            },
        )
        .await;

        let poller = OrderStatusPoller::new(
            ctx,
            pool.clone(),
            broker,
            offchain_order_cqrs,
            position_cqrs,
        );

        let order = OffchainOrder::Submitted {
            symbol: symbol.clone(),
            shares,
            direction: Direction::Buy,
            executor: SupportedExecutor::Schwab,
            executor_order_id: ExecutorOrderId::new("ORD123"),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
        };

        poller
            .handle_filled_order(
                offchain_order_id,
                &order,
                PriceCents(15025),
                &ExecutorOrderId::new("ORD123"),
                Utc::now(),
            )
            .await
            .unwrap();

        let aggregate_id = OffchainOrder::aggregate_id(offchain_order_id);
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
        let ctx = OrderPollerCtx::default();

        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(Decimal::from(5))).unwrap();

        setup_position_with_onchain_fill(&position_cqrs, &symbol, "tTSLA", dec!(5)).await;

        let offchain_order_id = OffchainOrderId::new();
        setup_offchain_order_aggregate(
            &offchain_order_cqrs,
            &position_cqrs,
            offchain_order_id,
            TestOrderParams {
                symbol: &symbol,
                shares,
                direction: Direction::Sell,
                executor: SupportedExecutor::Schwab,
            },
        )
        .await;

        let poller = OrderStatusPoller::new(
            ctx,
            pool.clone(),
            broker,
            offchain_order_cqrs,
            position_cqrs,
        );

        let order = OffchainOrder::Submitted {
            symbol: symbol.clone(),
            shares,
            direction: Direction::Sell,
            executor: SupportedExecutor::Schwab,
            executor_order_id: ExecutorOrderId::new("ORD456"),
            placed_at: Utc::now(),
            submitted_at: Utc::now(),
        };

        poller
            .handle_failed_order(offchain_order_id, &order, "Broker API timeout".to_string())
            .await
            .unwrap();

        let aggregate_id = OffchainOrder::aggregate_id(offchain_order_id);
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
