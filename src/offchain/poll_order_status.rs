//! Per-order apalis job that monitors a single broker order through
//! its lifecycle.
//!
//! On each execution the job polls the executor for the current order
//! state and either:
//! - **Filled** -> sends CQRS commands to complete the offchain order
//!   and position
//! - **Failed** -> sends CQRS commands to mark the order and position
//!   as failed
//! - **Pending/Submitted** -> re-enqueues itself for another poll cycle

use apalis::prelude::TaskSink;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use st0x_event_sorcery::SendError;
use st0x_execution::{
    Direction, Executor, ExecutorOrderId, FractionalShares, OrderState, Positive, Symbol,
};

use super::order_status_ctx::OrderStatusCtx;
use crate::conductor::job::{Job, Label};
use crate::offchain_order::{Dollars, OffchainOrder, OffchainOrderCommand, OffchainOrderId};
use crate::position::{Position, PositionCommand};

/// Polls a single broker order for status updates and handles
/// terminal states (fill or rejection) inline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PollOrderStatus {
    pub(crate) offchain_order_id: OffchainOrderId,
    pub(crate) executor_order_id: ExecutorOrderId,
    pub(crate) symbol: Symbol,
    pub(crate) shares: Positive<FractionalShares>,
    pub(crate) direction: Direction,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PollOrderStatusError {
    #[error("Executor error: {0}")]
    Executor(Box<dyn std::error::Error + Send + Sync>),

    #[error("Failed to re-enqueue poll job: {0}")]
    Enqueue(Box<dyn std::error::Error + Send + Sync>),

    #[error("Offchain order aggregate error: {0}")]
    OffchainOrder(#[from] SendError<OffchainOrder>),

    #[error("Position aggregate error: {0}")]
    Position(#[from] SendError<Position>),
}

impl<E: Executor> Job<OrderStatusCtx<E>> for PollOrderStatus {
    type Error = PollOrderStatusError;

    fn label(&self) -> Label {
        Label::new(format!(
            "poll-order-status[{}:{}]",
            self.symbol, self.offchain_order_id
        ))
    }

    async fn execute(&self, ctx: &OrderStatusCtx<E>) -> Result<(), Self::Error> {
        let parsed_order_id = ctx
            .executor
            .parse_order_id(self.executor_order_id.as_ref())
            .map_err(|error| PollOrderStatusError::Executor(Box::new(error)))?;

        let order_state = ctx
            .executor
            .get_order_status(&parsed_order_id)
            .await
            .map_err(|error| PollOrderStatusError::Executor(Box::new(error)))?;

        match order_state {
            OrderState::Filled {
                price,
                order_id,
                executed_at,
            } => {
                info!(
                    offchain_order_id = %self.offchain_order_id,
                    %self.symbol,
                    ?price,
                    "Order filled, reconciling with aggregates"
                );

                ctx.offchain_order
                    .send(
                        &self.offchain_order_id,
                        OffchainOrderCommand::CompleteFill {
                            price: Dollars(price),
                        },
                    )
                    .await?;

                ctx.position
                    .send(
                        &self.symbol,
                        PositionCommand::CompleteOffChainOrder {
                            offchain_order_id: self.offchain_order_id,
                            shares_filled: self.shares,
                            direction: self.direction,
                            executor_order_id: ExecutorOrderId::new(&order_id),
                            price: Dollars(price),
                            broker_timestamp: executed_at,
                        },
                    )
                    .await?;
            }

            OrderState::Failed { error_reason, .. } => {
                let error_message =
                    error_reason.unwrap_or_else(|| "Order failed with no error reason".to_string());

                info!(
                    offchain_order_id = %self.offchain_order_id,
                    %self.symbol,
                    %error_message,
                    "Order rejected, updating aggregates"
                );

                ctx.offchain_order
                    .send(
                        &self.offchain_order_id,
                        OffchainOrderCommand::MarkFailed {
                            error: error_message.clone(),
                        },
                    )
                    .await?;

                ctx.position
                    .send(
                        &self.symbol,
                        PositionCommand::FailOffChainOrder {
                            offchain_order_id: self.offchain_order_id,
                            error: error_message,
                        },
                    )
                    .await?;
            }

            OrderState::Pending | OrderState::Submitted { .. } => {
                debug!(
                    offchain_order_id = %self.offchain_order_id,
                    %self.symbol,
                    "Order still pending, re-enqueuing poll"
                );

                ctx.poll_queue
                    .lock()
                    .await
                    .push(self.clone())
                    .await
                    .map_err(|error| PollOrderStatusError::Enqueue(Box::new(error)))?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::Utc;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use tokio::sync::Mutex;

    use st0x_event_sorcery::{Projection, Store, StoreBuilder, test_store};
    use st0x_execution::{
        Direction, ExecutorOrderId, FractionalShares, MockExecutor, OrderState, Positive,
        SupportedExecutor, Symbol,
    };

    use super::*;
    use crate::conductor::job::Job;
    use crate::conductor::setup_apalis_tables;
    use crate::offchain::order_status_ctx::OrderStatusCtx;
    use crate::offchain_order::{OffchainOrder, OffchainOrderCommand, OffchainOrderId};
    use crate::position::{Position, PositionCommand, TradeId};
    use crate::test_utils::{OnchainTradeBuilder, setup_test_db};
    use crate::threshold::ExecutionThreshold;

    async fn create_test_frameworks(
        pool: &sqlx::SqlitePool,
    ) -> (
        Projection<OffchainOrder>,
        Arc<Store<OffchainOrder>>,
        Arc<Store<Position>>,
    ) {
        let (offchain_order_store, offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(crate::offchain_order::noop_order_placer())
                .await
                .unwrap();
        (
            Arc::unwrap_or_clone(offchain_order_projection),
            offchain_order_store,
            Arc::new(test_store(pool.clone(), ())),
        )
    }

    async fn setup_position_with_onchain_fill(
        position_store: &Store<Position>,
        symbol: &Symbol,
        tokenized_symbol: &str,
        amount: Decimal,
    ) {
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

        position_store
            .send(
                symbol,
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

    async fn setup_offchain_order_aggregate(
        offchain_order_store: &Store<OffchainOrder>,
        position_store: &Store<Position>,
        offchain_order_id: OffchainOrderId,
        symbol: &Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
    ) {
        offchain_order_store
            .send(
                &offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares,
                    direction,
                    executor: SupportedExecutor::Schwab,
                },
            )
            .await
            .unwrap();

        position_store
            .send(
                symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares,
                    direction,
                    executor: SupportedExecutor::Schwab,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();
    }

    async fn build_ctx(
        pool: &sqlx::SqlitePool,
        executor: MockExecutor,
        offchain_order_store: Arc<Store<OffchainOrder>>,
        position_store: Arc<Store<Position>>,
    ) -> OrderStatusCtx<MockExecutor> {
        setup_apalis_tables(pool).await.unwrap();

        OrderStatusCtx {
            executor,
            offchain_order: offchain_order_store,
            position: position_store,
            poll_queue: Mutex::new(apalis_sqlite::SqliteStorage::new(pool)),
            poll_interval: Duration::from_secs(1),
        }
    }

    #[tokio::test]
    async fn filled_order_reconciles_with_aggregates() {
        let pool = setup_test_db().await;
        let (_projection, offchain_order_store, position_store) =
            create_test_frameworks(&pool).await;

        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(Decimal::from(10))).unwrap();

        setup_position_with_onchain_fill(&position_store, &symbol, "wtAAPL", dec!(10)).await;

        let offchain_order_id = OffchainOrderId::new();
        setup_offchain_order_aggregate(
            &offchain_order_store,
            &position_store,
            offchain_order_id,
            &symbol,
            shares,
            Direction::Buy,
        )
        .await;

        let executor = MockExecutor::default().with_order_status(OrderState::Filled {
            price: dec!(150.25),
            order_id: "ORD123".to_string(),
            executed_at: Utc::now(),
        });

        let ctx = build_ctx(&pool, executor, offchain_order_store, position_store).await;

        let job = PollOrderStatus {
            offchain_order_id,
            executor_order_id: ExecutorOrderId::new("ORD123"),
            symbol,
            shares,
            direction: Direction::Buy,
        };

        job.execute(&ctx).await.unwrap();
    }

    #[tokio::test]
    async fn pending_order_reenqueues_poll() {
        let pool = setup_test_db().await;
        let (_projection, offchain_order_store, position_store) =
            create_test_frameworks(&pool).await;

        let symbol = Symbol::new("TSLA").unwrap();
        let shares = Positive::new(FractionalShares::new(Decimal::from(5))).unwrap();

        let executor = MockExecutor::default().with_order_status(OrderState::Submitted {
            order_id: "ORD456".to_string(),
        });

        let ctx = build_ctx(&pool, executor, offchain_order_store, position_store).await;

        let job = PollOrderStatus {
            offchain_order_id: OffchainOrderId::new(),
            executor_order_id: ExecutorOrderId::new("ORD456"),
            symbol,
            shares,
            direction: Direction::Buy,
        };

        job.execute(&ctx).await.unwrap();
    }

    #[tokio::test]
    async fn rejected_order_updates_aggregates() {
        let pool = setup_test_db().await;
        let (_projection, offchain_order_store, position_store) =
            create_test_frameworks(&pool).await;

        let symbol = Symbol::new("GOOG").unwrap();
        let shares = Positive::new(FractionalShares::new(Decimal::from(3))).unwrap();

        setup_position_with_onchain_fill(&position_store, &symbol, "wtGOOG", dec!(3)).await;

        let offchain_order_id = OffchainOrderId::new();
        setup_offchain_order_aggregate(
            &offchain_order_store,
            &position_store,
            offchain_order_id,
            &symbol,
            shares,
            Direction::Sell,
        )
        .await;

        let executor = MockExecutor::default().with_order_status(OrderState::Failed {
            failed_at: Utc::now(),
            error_reason: Some("Insufficient funds".to_string()),
        });

        let ctx = build_ctx(&pool, executor, offchain_order_store, position_store).await;

        let job = PollOrderStatus {
            offchain_order_id,
            executor_order_id: ExecutorOrderId::new("ORD789"),
            symbol,
            shares,
            direction: Direction::Sell,
        };

        job.execute(&ctx).await.unwrap();
    }
}
