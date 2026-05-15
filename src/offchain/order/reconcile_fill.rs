//! [`ReconcileOrderFill`] job: emits the CQRS commands that record a
//! successful broker fill on the [`OffchainOrder`] and
//! [`Position`](crate::position::Position) aggregates.
//!
//! Split out from
//! [`PollOrderStatus`](crate::offchain::order::PollOrderStatus) so the CQRS
//! write happens in its own retryable unit -- a transient DB failure here
//! does not force us to re-call the broker.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use st0x_event_sorcery::Store;
use st0x_execution::ExecutorOrderId;
use st0x_finance::Usd;

#[cfg(any(test, feature = "test-support"))]
use crate::conductor::job::JobKind;
use crate::conductor::job::{Job, JobQueue, Label};
use crate::offchain::order::{JobError, OffchainOrder, OffchainOrderCommand, OffchainOrderId};
use crate::position::{Position, PositionCommand};

pub(crate) type ReconcileOrderFillJobQueue = JobQueue<ReconcileOrderFill>;

/// Dependencies [`ReconcileOrderFill`] needs to record a fill: the two
/// aggregate stores it writes to.
pub(crate) struct ReconcileOrderFillCtx {
    pub(crate) offchain_order: Arc<Store<OffchainOrder>>,
    pub(crate) position: Arc<Store<Position>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ReconcileOrderFill {
    pub(crate) offchain_order_id: OffchainOrderId,
    pub(crate) price: Usd,
    pub(crate) executor_order_id: ExecutorOrderId,
    pub(crate) broker_timestamp: DateTime<Utc>,
}

impl Job<ReconcileOrderFillCtx> for ReconcileOrderFill {
    type Output = ();
    type Error = JobError;

    const WORKER_NAME: &'static str = "reconcile-order-fill-worker";
    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: JobKind = JobKind::ReconcileOrderFill;

    fn label(&self) -> Label {
        Label::new(format!("ReconcileOrderFill:{}", self.offchain_order_id))
    }

    async fn perform(&self, ctx: &ReconcileOrderFillCtx) -> Result<Self::Output, Self::Error> {
        let Some(order) = ctx.offchain_order.load(&self.offchain_order_id).await? else {
            warn!(
                offchain_order_id = %self.offchain_order_id,
                "ReconcileOrderFill: order not found, skipping"
            );
            return Ok(());
        };

        let symbol = order.symbol().clone();
        let shares_filled = order.shares();
        let direction = order.direction();

        // Retry-safe: the two writes (OffchainOrder CompleteFill +
        // Position CompleteOffChainOrder) are not atomic. If a prior
        // attempt completed step 1 but failed step 2, apalis re-runs us
        // with the order already in `Filled`. Re-sending `CompleteFill`
        // would surface `AlreadyCompleted` and stall the job forever, so
        // we only run step 1 when the order is still pre-terminal.
        use OffchainOrder::{
            Cancelled, Cancelling, Failed, Filled, PartiallyFilled, Pending, Submitted,
        };
        match &order {
            Filled { .. } => {
                info!(
                    offchain_order_id = %self.offchain_order_id,
                    "ReconcileOrderFill: order already Filled, resuming position update"
                );
            }

            Submitted { .. } | PartiallyFilled { .. } | Cancelling { .. } => {
                ctx.offchain_order
                    .send(
                        &self.offchain_order_id,
                        OffchainOrderCommand::CompleteFill { price: self.price },
                    )
                    .await?;
            }

            Pending { .. } | Failed { .. } | Cancelled { .. } => {
                warn!(
                    offchain_order_id = %self.offchain_order_id,
                    state = ?order,
                    "ReconcileOrderFill: order not in a submitted state, skipping"
                );
                return Ok(());
            }
        }

        // Retry-safe step 2: if a prior attempt or the startup recovery
        // job already cleared the position's pending id, sending the
        // command again would fail `validate_pending_execution`. Detect
        // and no-op instead.
        let position_pending = ctx
            .position
            .load(&symbol)
            .await?
            .and_then(|position| position.pending_offchain_order_id);
        if position_pending != Some(self.offchain_order_id) {
            info!(
                offchain_order_id = %self.offchain_order_id,
                ?position_pending,
                "ReconcileOrderFill: position no longer expecting this fill, skipping"
            );
            return Ok(());
        }

        ctx.position
            .send(
                &symbol,
                PositionCommand::CompleteOffChainOrder {
                    offchain_order_id: self.offchain_order_id,
                    shares_filled,
                    direction,
                    executor_order_id: self.executor_order_id.clone(),
                    price: self.price,
                    broker_timestamp: self.broker_timestamp,
                },
            )
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::{Direction, FractionalShares, Positive, SupportedExecutor, Symbol};
    use st0x_float_macro::float;

    use super::*;
    use crate::conductor::setup_apalis_tables;
    use crate::offchain::order::{noop_order_placer, noop_placed_shares};
    use crate::position::TradeId;
    use crate::test_utils::{OnchainTradeBuilder, setup_test_db};
    use crate::threshold::ExecutionThreshold;

    struct TestInfra {
        ctx: ReconcileOrderFillCtx,
    }

    async fn build_test_infra() -> TestInfra {
        let pool = setup_test_db().await;
        setup_apalis_tables(&pool).await.unwrap();

        let (offchain_order, _projection) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(noop_order_placer())
            .await
            .unwrap();

        let (position, _position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        TestInfra {
            ctx: ReconcileOrderFillCtx {
                offchain_order,
                position,
            },
        }
    }

    async fn submit_offchain_order(
        infra: &TestInfra,
        symbol: &Symbol,
        tokenized_symbol: &str,
        shares: Positive<FractionalShares>,
        direction: Direction,
    ) -> OffchainOrderId {
        let onchain = OnchainTradeBuilder::new()
            .with_symbol(tokenized_symbol)
            .with_amount(shares.inner().inner())
            .build();
        let trade_id = TradeId {
            tx_hash: onchain.tx_hash,
            log_index: onchain.log_index,
        };

        infra
            .ctx
            .position
            .send(
                symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id,
                    amount: onchain.amount,
                    direction: Direction::Buy,
                    price_usdc: onchain.price.value(),
                    block_timestamp: Utc::now(),
                },
            )
            .await
            .unwrap();

        let offchain_order_id = OffchainOrderId::new();

        infra
            .ctx
            .position
            .send(
                symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares,
                    direction,
                    executor: SupportedExecutor::DryRun,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();

        infra
            .ctx
            .offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares,
                    direction,
                    executor: SupportedExecutor::DryRun,
                    kind: crate::offchain::order::CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();

        offchain_order_id
    }

    #[tokio::test]
    async fn reconcile_order_fill_emits_offchain_and_position_commands() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(3))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        let fill_price = Usd::new(float!(150.25));
        let broker_ts = Utc::now();
        let broker_order_id = ExecutorOrderId::new("BROKER_42");

        ReconcileOrderFill {
            offchain_order_id: order_id,
            price: fill_price,
            executor_order_id: broker_order_id.clone(),
            broker_timestamp: broker_ts,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        let offchain = infra
            .ctx
            .offchain_order
            .load(&order_id)
            .await
            .unwrap()
            .expect("offchain order should exist");
        let OffchainOrder::Filled {
            price: filled_price,
            executor_order_id: filled_broker_id,
            shares: filled_shares,
            ..
        } = offchain
        else {
            panic!("expected OffchainOrder::Filled, got {offchain:?}");
        };
        assert_eq!(filled_price, fill_price);
        assert_eq!(filled_broker_id, ExecutorOrderId::new("noop"));
        assert_eq!(filled_shares, noop_placed_shares(shares));

        let position = infra
            .ctx
            .position
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id, None,
            "Position must have no pending order after reconcile"
        );
    }

    /// Simulates apalis retrying the job after step 1 (CompleteFill)
    /// succeeded but step 2 (Position update) failed. The order is already
    /// `Filled` and the position still has `pending_offchain_order_id` set.
    /// The retry must resume by completing step 2 without re-applying step 1,
    /// which would otherwise surface `AlreadyCompleted` and stall the job.
    #[tokio::test]
    async fn retry_after_position_failure_resumes_position_update() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(3))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        // Simulate step 1 having succeeded in a prior attempt by manually
        // driving the OffchainOrder to Filled. The position keeps its
        // pending_offchain_order_id from `submit_offchain_order`.
        let fill_price = Usd::new(float!(150.25));
        infra
            .ctx
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::CompleteFill { price: fill_price },
            )
            .await
            .unwrap();

        let position_before = infra
            .ctx
            .position
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position_before.pending_offchain_order_id,
            Some(order_id),
            "test setup: position must still be expecting this fill"
        );

        ReconcileOrderFill {
            offchain_order_id: order_id,
            price: fill_price,
            executor_order_id: ExecutorOrderId::new("BROKER_42"),
            broker_timestamp: Utc::now(),
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        let position_after = infra
            .ctx
            .position
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position_after.pending_offchain_order_id, None,
            "Retry must clear the position's pending state by running step 2"
        );
    }

    /// Simulates apalis retrying the job after both steps succeeded -- the
    /// order is `Filled` and the position no longer has a pending id. The
    /// retry must no-op rather than surface `NoPendingExecution`.
    #[tokio::test]
    async fn retry_after_full_success_is_noop() {
        let infra = build_test_infra().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(3))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        let fill_price = Usd::new(float!(150.25));
        let broker_ts = Utc::now();
        let broker_order_id = ExecutorOrderId::new("BROKER_42");

        // First run drives both order and position to completion.
        ReconcileOrderFill {
            offchain_order_id: order_id,
            price: fill_price,
            executor_order_id: broker_order_id.clone(),
            broker_timestamp: broker_ts,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        // Second run: identical job, both writes already applied.
        ReconcileOrderFill {
            offchain_order_id: order_id,
            price: fill_price,
            executor_order_id: broker_order_id,
            broker_timestamp: broker_ts,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        let position = infra
            .ctx
            .position
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id, None,
            "Position must remain cleared after no-op retry"
        );
    }
}
