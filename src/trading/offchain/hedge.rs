//! Durable hedge placement job.
//!
//! [`PlaceHedge`] is an apalis-backed [`Job`] that places an offsetting
//! broker order for an accumulated position. The position monitor enqueues
//! these; the apalis worker processes them with retry semantics.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tracing::info;

use st0x_config::ExecutionThreshold;
use st0x_event_sorcery::{AggregateError, LifecycleError, Store};
use st0x_execution::{
    ClientOrderId, Direction, FractionalShares, Positive, SupportedExecutor, Symbol,
};

use crate::conductor::job::{Job, JobQueue, Label};
use crate::offchain::order::{
    OffchainOrder, OffchainOrderCommand, OffchainOrderId, PollOrderStatus, PollOrderStatusJobQueue,
};
use crate::position::{Position, PositionCommand, PositionError};
use crate::trading::onchain::trade_accountant::TradeAccountingError;

/// Persistent job queue for hedge placement.
pub(crate) type HedgeJobQueue = JobQueue<PlaceHedge>;

/// Shared dependencies for hedge placement jobs.
pub(crate) struct HedgeCtx {
    pub(crate) position: Arc<Store<Position>>,
    pub(crate) offchain_order: Arc<Store<OffchainOrder>>,
    pub(crate) poll_status_queue: PollOrderStatusJobQueue,
}

/// A durable job that places an offsetting broker order for an accumulated
/// position, then rolls back the position if the broker rejects.
///
/// `offchain_order_id` is generated at enqueue time (not inside `perform`)
/// so that retries reuse the same ID. Without this, a crash between
/// `PlaceOffChainOrder` and `OffchainOrderCommand::Place` would leave the
/// position stuck with a pending ID that no retry can ever claim.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PlaceHedge {
    pub(crate) symbol: Symbol,
    pub(crate) direction: Direction,
    pub(crate) shares: Positive<FractionalShares>,
    pub(crate) executor: SupportedExecutor,
    pub(crate) threshold: ExecutionThreshold,
    pub(crate) offchain_order_id: OffchainOrderId,
}

/// Recovery path for the `PendingExecution` rejection. The previous attempt
/// for this position may have already submitted the order to the broker but
/// failed before enqueueing the `PollOrderStatus` job (e.g. the queue push
/// returned a transient error and apalis re-ran us). Without this re-enqueue,
/// `Submitted`/`PartiallyFilled` orders stay un-polled until the bot restarts
/// and the startup recovery sweep finds them.
///
/// Duplicate poll jobs are harmless: `dispatch_for_order_state` drops jobs
/// whose target order is already in a terminal state.
async fn recover_pending_poll_status(
    ctx: &HedgeCtx,
    pending_id: OffchainOrderId,
) -> Result<(), TradeAccountingError> {
    use OffchainOrder::{
        Cancelled, Cancelling, Failed, Filled, PartiallyFilled, Pending, Submitted,
    };
    match ctx.offchain_order.load(&pending_id).await? {
        Some(Submitted { .. } | PartiallyFilled { .. } | Cancelling { .. }) => {
            ctx.poll_status_queue
                .clone()
                .push(PollOrderStatus {
                    offchain_order_id: pending_id,
                })
                .await?;
            Ok(())
        }
        Some(Pending { .. } | Filled { .. } | Failed { .. } | Cancelled { .. }) | None => Ok(()),
    }
}

impl Job<HedgeCtx> for PlaceHedge {
    type Output = ();
    type Error = TradeAccountingError;

    const WORKER_NAME: &'static str = "hedge-worker";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind = crate::conductor::job::JobKind::Hedge;

    fn label(&self) -> Label {
        Label::new(format!(
            "PlaceHedge:{}:{}:{:?}",
            self.symbol, self.shares, self.direction
        ))
    }

    async fn perform(&self, ctx: &HedgeCtx) -> Result<Self::Output, Self::Error> {
        // Only specific business rejections are safe to swallow:
        // - PendingExecution: another attempt already claimed this position
        //   — usually idempotent, but if that attempt got the broker submitted
        //   *without* enqueueing PollOrderStatus (e.g. the queue push failed
        //   and apalis is now retrying us), we must re-enqueue the poll here
        //   or the order sits in Submitted until the next bot restart.
        // - ThresholdNotMet: position moved below threshold since the monitor
        //   scanned — stale job, no action needed.
        //
        // Everything else (lifecycle bugs, aggregate conflicts, DB errors)
        // propagates so backon retries the job.
        match ctx
            .position
            .send(
                &self.symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id: self.offchain_order_id,
                    shares: self.shares,
                    direction: self.direction,
                    executor: self.executor,
                    threshold: self.threshold,
                },
            )
            .await
        {
            Ok(()) => {}

            Err(AggregateError::UserError(LifecycleError::Apply(
                PositionError::PendingExecution {
                    offchain_order_id: pending_id,
                },
            ))) => {
                info!(
                    target: "hedge",
                    symbol = %self.symbol, %pending_id,
                    "Position already has a pending execution; recovering poll-status enqueue if needed"
                );
                return recover_pending_poll_status(ctx, pending_id).await;
            }

            Err(AggregateError::UserError(LifecycleError::Apply(
                ref error @ PositionError::ThresholdNotMet { .. },
            ))) => {
                info!(
                    target: "hedge",
                    symbol = %self.symbol, %error,
                    "Position below execution threshold, skipping"
                );
                return Ok(());
            }

            Err(error) => return Err(error.into()),
        }

        // Derive the broker-side `client_order_id` from the *live* position
        // aggregate, read after `PlaceOffChainOrder` has claimed it -- never
        // captured at enqueue. If a prior attempt failed, the aggregate holds
        // its `OffchainOrderId` as the idempotency anchor, so this retry reuses
        // the same key and the broker dedupes the duplicate submission (a 422
        // the executor reconciles by adopting the order it already accepted).
        // Reading it live means a failure recorded *after* this job was enqueued
        // is still honored, instead of placing under a fresh key and
        // double-submitting. Falls back to this attempt's own id on the first
        // try, when no anchor exists yet.
        let anchor = ctx
            .position
            .load(&self.symbol)
            .await?
            .and_then(|position| position.last_failed_offchain_order_id);
        let client_order_id = client_order_id_for_placement(self.offchain_order_id, anchor);

        ctx.offchain_order
            .send(
                &self.offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: self.symbol.clone(),
                    shares: self.shares,
                    direction: self.direction,
                    executor: self.executor,
                    client_order_id,
                    // Regular-hours market order; session-aware order-kind
                    // selection is added with the cancel-and-replace wiring.
                    kind: crate::offchain::order::CounterTradeOrderKind::Market,
                },
            )
            .await?;

        use OffchainOrder::{
            Cancelled, Cancelling, Failed, Filled, PartiallyFilled, Pending, Submitted,
        };
        match ctx.offchain_order.load(&self.offchain_order_id).await? {
            Some(Failed { error, .. }) => {
                ctx.position
                    .send(
                        &self.symbol,
                        PositionCommand::FailOffChainOrder {
                            offchain_order_id: self.offchain_order_id,
                            error,
                        },
                    )
                    .await?;
            }

            Some(Submitted { .. } | PartiallyFilled { .. } | Cancelling { .. }) => {
                let mut queue = ctx.poll_status_queue.clone();

                queue
                    .push(PollOrderStatus {
                        offchain_order_id: self.offchain_order_id,
                    })
                    .await?;
            }

            Some(Filled { .. } | Pending { .. } | Cancelled { .. }) | None => {}
        }

        Ok(())
    }
}

/// Derives the broker-side [`ClientOrderId`] for a placement attempt.
///
/// When a prior attempt failed after the broker accepted the order, the
/// position aggregate stashes that attempt's [`OffchainOrderId`] as the
/// idempotency anchor. Retries must reuse its UUID as `client_order_id` so
/// the broker dedupes rather than double-submitting.
fn client_order_id_for_placement(
    offchain_order_id: OffchainOrderId,
    last_failed_offchain_order_id: Option<OffchainOrderId>,
) -> ClientOrderId {
    let idempotency_source = last_failed_offchain_order_id.unwrap_or(offchain_order_id);
    ClientOrderId::from_uuid(idempotency_source.as_uuid())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::TxHash;
    use proptest::prelude::*;
    use std::any::type_name;
    use std::sync::Arc;
    use uuid::Uuid;

    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::{
        ClientOrderId, Direction, ExecutorOrderId, FractionalShares, Positive, SupportedExecutor,
        Symbol,
    };
    use st0x_float_macro::float;

    use super::*;
    use crate::conductor::job::Job;
    use crate::offchain::order::{OffchainOrder, OrderPlacementResult, OrderPlacer};
    use crate::position::{Position, PositionCommand, TradeId};
    use st0x_config::ExecutionThreshold;

    fn succeeding_order_placer() -> Arc<dyn OrderPlacer> {
        struct SucceedingPlacer;

        #[async_trait::async_trait]
        impl OrderPlacer for SucceedingPlacer {
            async fn place_market_order(
                &self,
                order: st0x_execution::MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("test-order-123"),
                    placed_shares: order.shares,
                    is_extended_hours: false,
                    limit_price: None,
                })
            }

            async fn place_limit_order(
                &self,
                order: st0x_execution::LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("test-limit-order-123"),
                    placed_shares: order.shares,
                    is_extended_hours: order.extended_hours,
                    limit_price: Some(order.limit_price),
                })
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &st0x_execution::ExecutorOrderId,
            ) -> Result<st0x_execution::CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(st0x_execution::CancellationOutcome::Requested)
            }
        }

        Arc::new(SucceedingPlacer)
    }

    fn rejecting_order_placer() -> Arc<dyn OrderPlacer> {
        struct RejectingPlacer;

        #[async_trait::async_trait]
        impl OrderPlacer for RejectingPlacer {
            async fn place_market_order(
                &self,
                _order: st0x_execution::MarketOrder,
            ) -> Result<
                crate::offchain::order::OrderPlacementResult,
                Box<dyn std::error::Error + Send + Sync>,
            > {
                Err("Broker rejected: insufficient buying power".into())
            }

            async fn place_limit_order(
                &self,
                _order: st0x_execution::LimitOrder,
            ) -> Result<
                crate::offchain::order::OrderPlacementResult,
                Box<dyn std::error::Error + Send + Sync>,
            > {
                Err("Broker rejected: insufficient buying power".into())
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &st0x_execution::ExecutorOrderId,
            ) -> Result<st0x_execution::CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(st0x_execution::CancellationOutcome::Requested)
            }
        }

        Arc::new(RejectingPlacer)
    }

    struct TestInfra {
        ctx: HedgeCtx,
        apalis_pool: apalis_sqlite::SqlitePool,
        position_projection: Arc<st0x_event_sorcery::Projection<Position>>,
        offchain_order_projection: Arc<st0x_event_sorcery::Projection<OffchainOrder>>,
    }

    async fn create_hedge_ctx(order_placer: Arc<dyn OrderPlacer>) -> TestInfra {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;

        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (offchain_order, offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(order_placer)
                .await
                .unwrap();

        let ctx = HedgeCtx {
            position: position.clone(),
            offchain_order,
            poll_status_queue: PollOrderStatusJobQueue::new(&apalis_pool),
        };

        TestInfra {
            ctx,
            apalis_pool,
            position_projection,
            offchain_order_projection,
        }
    }

    async fn fill_position(
        store: &Store<Position>,
        symbol: &Symbol,
        amount: FractionalShares,
        direction: Direction,
    ) {
        store
            .send(
                symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::random(),
                        log_index: 1,
                    },
                    amount,
                    direction,
                    price_usdc: float!(150.0),
                    block_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
    }

    fn hedge_job(symbol: &Symbol, shares: f64, direction: Direction) -> PlaceHedge {
        PlaceHedge {
            symbol: symbol.clone(),
            direction,
            shares: Positive::new(FractionalShares::new(float!(shares))).unwrap(),
            executor: SupportedExecutor::DryRun,
            threshold: ExecutionThreshold::whole_share(),
            offchain_order_id: OffchainOrderId::new(),
        }
    }

    #[tokio::test]
    async fn places_offchain_order_and_marks_position_pending() {
        let TestInfra {
            ctx,
            position_projection: projection,
            ..
        } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let job = hedge_job(&symbol, 2.0, Direction::Sell);
        job.perform(&ctx).await.unwrap();

        let position = projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");

        assert_eq!(
            position.pending_offchain_order_id,
            Some(job.offchain_order_id),
            "Position should store the hedge job's offchain order ID"
        );
    }

    #[tokio::test]
    async fn clears_pending_state_on_broker_rejection() {
        let TestInfra {
            ctx,
            position_projection: projection,
            ..
        } = create_hedge_ctx(rejecting_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(5.0)),
            Direction::Buy,
        )
        .await;

        let job = hedge_job(&symbol, 5.0, Direction::Sell);
        job.perform(&ctx).await.unwrap();

        let position = projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");

        assert_eq!(
            position.pending_offchain_order_id, None,
            "Position should not be stuck with pending order after broker rejection"
        );
    }

    #[tokio::test]
    async fn duplicate_hedge_is_idempotent() {
        let TestInfra {
            ctx,
            position_projection,
            offchain_order_projection,
            ..
        } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(3.0)),
            Direction::Buy,
        )
        .await;

        let job = hedge_job(&symbol, 3.0, Direction::Sell);

        // First hedge should succeed
        job.perform(&ctx).await.unwrap();

        let position_after_first = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        let first_pending_id = position_after_first.pending_offchain_order_id;
        assert!(
            first_pending_id.is_some(),
            "First hedge should set a pending order"
        );

        // Second hedge for the same symbol should be rejected
        // by the aggregate (pending order already exists) and
        // must not create a second offchain order.
        job.perform(&ctx).await.unwrap();

        let all_orders = offchain_order_projection.load_all().await.unwrap();
        assert_eq!(
            all_orders.len(),
            1,
            "Only one offchain order should exist after duplicate hedge attempt, got {}",
            all_orders.len(),
        );

        let position_after_second = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");

        assert_eq!(
            position_after_second.pending_offchain_order_id, first_pending_id,
            "Second hedge must not change the pending order"
        );
    }

    #[tokio::test]
    async fn uninitialized_position_propagates_error() {
        let TestInfra { ctx, .. } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();

        // No position exists -- PlaceOffChainOrder is rejected with Uninitialized,
        // which is NOT a safe-to-swallow rejection (unlike PendingExecution or
        // ThresholdNotMet), so the error propagates for retry.
        let job = hedge_job(&symbol, 1.0, Direction::Sell);
        let result = job.perform(&ctx).await;

        assert!(
            matches!(result, Err(TradeAccountingError::PositionCommand(_))),
            "expected PositionCommand error for uninitialized position, got: {result:?}"
        );
    }

    /// Simulates the retry path: a prior hedge attempt got the broker
    /// `Submitted` but failed to enqueue the `PollOrderStatus` job, and apalis
    /// is re-running the hedge. The retry must re-enqueue the poll so the
    /// order doesn't sit `Submitted` until the next bot restart.
    #[tokio::test]
    async fn retry_after_failed_poll_enqueue_re_enqueues_poll() {
        let TestInfra {
            ctx, apalis_pool, ..
        } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let job = hedge_job(&symbol, 2.0, Direction::Sell);

        // First run: drives the order to `Submitted` and enqueues
        // PollOrderStatus exactly once.
        job.perform(&ctx).await.unwrap();

        let poll_jobs_after_first: i64 =
            sqlx_apalis::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(type_name::<PollOrderStatus>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        assert_eq!(
            poll_jobs_after_first, 1,
            "First hedge should enqueue exactly one PollOrderStatus job"
        );

        // Retry the same job. Position rejects with PendingExecution because
        // the first run set the pending id. The recovery path must observe
        // that the offchain order is still `Submitted` and push another
        // PollOrderStatus rather than silently returning Ok.
        job.perform(&ctx).await.unwrap();

        let poll_jobs_after_retry: i64 =
            sqlx_apalis::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(type_name::<PollOrderStatus>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        assert_eq!(
            poll_jobs_after_retry, 2,
            "Retry must re-enqueue PollOrderStatus when the order is still Submitted"
        );
    }

    fn offchain_order_id_from(uuid: Uuid) -> OffchainOrderId {
        uuid.to_string().parse().unwrap()
    }

    fn arb_uuid() -> impl Strategy<Value = Uuid> {
        prop::array::uniform16(any::<u8>()).prop_map(Uuid::from_bytes)
    }

    proptest! {
        #[test]
        fn client_order_id_for_placement_reuses_anchor_uuid(
            attempt_uuid in arb_uuid(),
            anchor_uuid in arb_uuid(),
        ) {
            let attempt_id = offchain_order_id_from(attempt_uuid);
            let anchor_id = offchain_order_id_from(anchor_uuid);

            let derived = client_order_id_for_placement(attempt_id, Some(anchor_id));
            prop_assert_eq!(derived, ClientOrderId::from_uuid(anchor_uuid));
        }

        #[test]
        fn client_order_id_for_placement_falls_back_to_attempt_without_anchor(
            attempt_uuid in arb_uuid(),
        ) {
            let attempt_id = offchain_order_id_from(attempt_uuid);

            let derived = client_order_id_for_placement(attempt_id, None);
            prop_assert_eq!(derived, ClientOrderId::from_uuid(attempt_uuid));
        }

        #[test]
        fn retries_with_same_anchor_share_broker_client_order_id(
            first_attempt in arb_uuid(),
            second_attempt in arb_uuid(),
            anchor in arb_uuid(),
        ) {
            prop_assume!(first_attempt != second_attempt);

            let first = client_order_id_for_placement(
                offchain_order_id_from(first_attempt),
                Some(offchain_order_id_from(anchor)),
            );
            let second = client_order_id_for_placement(
                offchain_order_id_from(second_attempt),
                Some(offchain_order_id_from(anchor)),
            );

            prop_assert_eq!(first, second);
        }
    }
}
