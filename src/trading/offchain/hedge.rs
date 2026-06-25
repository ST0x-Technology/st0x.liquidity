//! Durable hedge placement job.
//!
//! [`PlaceHedge`] is an apalis-backed [`Job`] that places an offsetting
//! broker order for an accumulated position. The position monitor enqueues
//! these; the apalis worker processes them with retry semantics.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{info, warn};

use st0x_config::ExecutionThreshold;
use st0x_event_sorcery::{AggregateError, LifecycleError, Store};
use st0x_execution::{Direction, FractionalShares, Positive, SupportedExecutor, Symbol};

use crate::conductor::job::{Job, JobQueue, Label};
use crate::offchain::order::{
    OffchainOrder, OffchainOrderId, OrderPlacer, PollOrderStatus, PollOrderStatusJobQueue,
    client_order_id_for_placement, place_offchain_order_at_broker,
};
use crate::position::{Position, PositionCommand, PositionError};
use crate::trading::onchain::trade_accountant::TradeAccountingError;

/// Persistent job queue for hedge placement.
pub(crate) type HedgeJobQueue = JobQueue<PlaceHedge>;

/// Shared dependencies for hedge placement jobs.
pub(crate) struct HedgeCtx {
    pub(crate) position: Arc<Store<Position>>,
    pub(crate) offchain_order: Arc<Store<OffchainOrder>>,
    /// Places the broker order, lifted out of the (now pure)
    /// `OffchainOrder::Place` handler.
    pub(crate) order_placer: Arc<dyn OrderPlacer>,
    pub(crate) poll_status_queue: PollOrderStatusJobQueue,
    /// Shared with the trade-processing path so every broker-placement attempt
    /// for a symbol serializes (ADR 0014): the original placement and any
    /// recovery re-drive of the same order cannot interleave and race
    /// `MarkFailed` against `MarkAccepted`.
    pub(crate) counter_trade_submission_lock: Arc<Mutex<()>>,
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

/// Recovery path for the `PendingExecution` rejection. A previous attempt for
/// this position already claimed it, but may not have completed the broker
/// placement, so this retry reconciles the pending order's actual state:
///
/// - `Submitted`/`PartiallyFilled`: the order reached the broker but the prior
///   attempt may have failed to enqueue the `PollOrderStatus` job (e.g. the
///   queue push returned a transient error and apalis re-ran us), so re-enqueue
///   it. Duplicate poll jobs are harmless -- `dispatch_for_order_state` drops
///   jobs whose target order is already terminal.
/// - `Pending`: the broker outcome was never committed -- the `MarkAccepted`/
///   `MarkFailed` write was lost after a successful broker call, or a crash hit
///   before the broker call. Re-drive the idempotent placement so the order
///   reaches a submitted/terminal state instead of sitting `Pending` with a
///   live, unpolled broker order until the next bot restart. `Place` is a no-op
///   on the existing aggregate and the broker dedupes on `client_order_id`.
/// - terminal/absent: nothing to do.
async fn recover_pending_poll_status(
    ctx: &HedgeCtx,
    pending_id: OffchainOrderId,
) -> Result<(), TradeAccountingError> {
    use OffchainOrder::{Failed, Filled, PartiallyFilled, Pending, Submitted};
    match ctx.offchain_order.load(&pending_id).await? {
        Some(Submitted { .. } | PartiallyFilled { .. }) => {
            ctx.poll_status_queue
                .clone()
                .push(PollOrderStatus {
                    offchain_order_id: pending_id,
                })
                .await?;
            Ok(())
        }
        Some(Pending {
            symbol,
            shares,
            direction,
            executor,
            ..
        }) => {
            let anchor = ctx
                .position
                .load(&symbol)
                .await?
                .and_then(|position| position.last_failed_offchain_order_id);
            let client_order_id = client_order_id_for_placement(pending_id, anchor);

            let placed = place_offchain_order_at_broker(
                &ctx.offchain_order,
                ctx.order_placer.as_ref(),
                &pending_id,
                symbol.clone(),
                shares,
                direction,
                executor,
                client_order_id,
            )
            .await?;

            route_placement_outcome(ctx, &symbol, pending_id, placed).await
        }
        Some(Filled { .. } | Failed { .. }) | None => Ok(()),
    }
}

/// Routes the result of [`place_offchain_order_at_broker`] to its follow-up,
/// resolving the position claim for every outcome so it can never be left
/// stranded:
///
/// - `Failed`: roll the position back (clear the claim).
/// - `Submitted`/`PartiallyFilled`: enqueue a `PollOrderStatus` job.
/// - `None` (no order after a successful `Place`): clear the claim, since there
///   is nothing left to track.
/// - `Pending`/`Filled`: surface a retryable error without clearing the claim,
///   since the order may be live at the broker.
///
/// Shared by the primary placement path and the `Pending` re-drive in
/// [`recover_pending_poll_status`], and kept in lockstep with the
/// trade-processing path's `dispatch_post_place_state`, so the placement paths
/// cannot diverge.
async fn route_placement_outcome(
    ctx: &HedgeCtx,
    symbol: &Symbol,
    offchain_order_id: OffchainOrderId,
    placed: Option<OffchainOrder>,
) -> Result<(), TradeAccountingError> {
    use OffchainOrder::{Failed, Filled, PartiallyFilled, Pending, Submitted};
    match placed {
        Some(Failed { error, .. }) => {
            ctx.position
                .send(
                    symbol,
                    PositionCommand::FailOffChainOrder {
                        offchain_order_id,
                        error,
                    },
                )
                .await?;
        }

        Some(Submitted { .. } | PartiallyFilled { .. }) => {
            ctx.poll_status_queue
                .clone()
                .push(PollOrderStatus { offchain_order_id })
                .await?;
        }

        // No order exists after a successful `Place` -- there is nothing to
        // track, so clear the position claim (matching `dispatch_post_place_state`)
        // instead of leaving the position stuck behind a phantom id.
        None => {
            ctx.position
                .send(
                    symbol,
                    PositionCommand::FailOffChainOrder {
                        offchain_order_id,
                        error: "Offchain order missing after Place".to_string(),
                    },
                )
                .await?;
        }

        // `place_offchain_order_at_broker` only returns once the order has left
        // `Pending`, and the broker never reports `Filled` synchronously, so
        // observing either here means the outcome commit was lost. Surface it as
        // a retryable error (matching `dispatch_post_place_state`) and -- unlike
        // the `None` arm -- do NOT clear the position claim, which would strand a
        // possibly-live broker order.
        Some(state @ (Pending { .. } | Filled { .. })) => {
            warn!(
                target: "hedge",
                %offchain_order_id,
                "placement returned an unexpected post-place state; the broker outcome commit was lost -- retrying"
            );
            return Err(TradeAccountingError::UnexpectedPostPlaceState {
                offchain_order_id,
                state,
            });
        }
    }

    Ok(())
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
        // Serialize every broker placement (ADR 0014): the trade-processing path
        // holds this same lock across its placement, so the original placement and
        // any recovery re-drive of the same order cannot interleave -- closing the
        // window where a stale `MarkFailed` could strand a live order accepted by
        // a concurrent attempt. Held for the whole job, including
        // `recover_pending_poll_status`.
        let _submission_guard = ctx.counter_trade_submission_lock.lock().await;

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

        let placed = place_offchain_order_at_broker(
            &ctx.offchain_order,
            ctx.order_placer.as_ref(),
            &self.offchain_order_id,
            self.symbol.clone(),
            self.shares,
            self.direction,
            self.executor,
            client_order_id,
        )
        .await?;

        route_placement_outcome(ctx, &self.symbol, self.offchain_order_id, placed).await
    }
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
    use st0x_finance::Usd;
    use st0x_float_macro::float;

    use super::*;
    use crate::conductor::job::Job;
    use crate::offchain::order::{
        OffchainOrder, OffchainOrderCommand, OrderPlacementResult, OrderPlacer,
    };
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
                })
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
            .build()
            .await
            .unwrap();

        let (offchain_order, offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build()
                .await
                .unwrap();

        let ctx = HedgeCtx {
            position: position.clone(),
            offchain_order,
            poll_status_queue: PollOrderStatusJobQueue::new(&apalis_pool),
            order_placer,
            counter_trade_submission_lock: Arc::new(Mutex::new(())),
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
    async fn route_placement_outcome_errors_when_order_left_pending() {
        // `place_offchain_order_at_broker` only returns `Pending` when the broker
        // outcome commit was lost. `route_placement_outcome` must surface that as
        // a retryable error so apalis re-drives the job, rather than silently
        // succeeding and leaving a live, unpolled order stuck `Pending`.
        let TestInfra { ctx, .. } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let offchain_order_id = OffchainOrderId::new();

        let pending = OffchainOrder::Pending {
            symbol: symbol.clone(),
            shares: Positive::new(FractionalShares::new(float!(1.0))).unwrap(),
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            placed_at: chrono::Utc::now(),
        };

        let error =
            route_placement_outcome(&ctx, &symbol, offchain_order_id, Some(pending.clone()))
                .await
                .unwrap_err();

        let TradeAccountingError::UnexpectedPostPlaceState {
            offchain_order_id: returned,
            state,
        } = error
        else {
            panic!("expected UnexpectedPostPlaceState, got {error:?}");
        };
        assert_eq!(returned, offchain_order_id);
        assert_eq!(state, pending);
    }

    #[tokio::test]
    async fn route_placement_outcome_errors_and_keeps_claim_when_order_filled() {
        // `place_offchain_order_at_broker` never returns `Filled`, so observing it
        // here means the broker outcome commit was lost. `route_placement_outcome`
        // must surface a retryable error and -- crucially -- must NOT clear the
        // position claim, which would strand an order that has already filled.
        let TestInfra {
            ctx,
            position_projection,
            ..
        } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2.0))).unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let offchain_order_id = OffchainOrderId::new();
        ctx.position
            .send(
                &symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares,
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();

        let filled = OffchainOrder::Filled {
            symbol: symbol.clone(),
            shares,
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            executor_order_id: ExecutorOrderId::new("test-order-123"),
            price: Usd::new(float!(150.0)),
            placed_at: chrono::Utc::now(),
            submitted_at: chrono::Utc::now(),
            filled_at: chrono::Utc::now(),
        };

        let error = route_placement_outcome(&ctx, &symbol, offchain_order_id, Some(filled.clone()))
            .await
            .unwrap_err();

        let TradeAccountingError::UnexpectedPostPlaceState {
            offchain_order_id: returned,
            state,
        } = error
        else {
            panic!("expected UnexpectedPostPlaceState, got {error:?}");
        };
        assert_eq!(returned, offchain_order_id);
        assert_eq!(state, filled);

        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id,
            Some(offchain_order_id),
            "an unexpected Filled state must not clear the position claim"
        );
    }

    #[tokio::test]
    async fn route_placement_outcome_clears_claim_when_order_missing() {
        // A missing order after a successful `Place` leaves nothing to track, so
        // `route_placement_outcome` must clear the position claim rather than
        // leaving the position stuck behind a phantom id.
        let TestInfra {
            ctx,
            position_projection,
            ..
        } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2.0))).unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let offchain_order_id = OffchainOrderId::new();
        ctx.position
            .send(
                &symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares,
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();

        route_placement_outcome(&ctx, &symbol, offchain_order_id, None)
            .await
            .unwrap();

        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id, None,
            "a missing order after Place must clear the position claim"
        );
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

    /// A prior attempt claimed the position and recorded the offchain order as
    /// `Pending`, but the broker outcome commit was lost before `MarkAccepted`.
    /// A fresh `perform` hits `PendingExecution`, so `recover_pending_poll_status`
    /// must re-drive the still-`Pending` order through the broker to `Submitted`
    /// and enqueue its `PollOrderStatus` job, rather than leaving it stuck with a
    /// live, unpolled broker order until the next bot restart.
    #[tokio::test]
    async fn pending_redrive_advances_order_to_submitted_and_enqueues_poll() {
        let TestInfra {
            ctx,
            apalis_pool,
            offchain_order_projection,
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

        // Seed the lost-commit state: the position claims the order and the
        // offchain order sits `Pending`, with no broker outcome committed.
        ctx.position
            .send(
                &symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id: job.offchain_order_id,
                    shares: job.shares,
                    direction: job.direction,
                    executor: job.executor,
                    threshold: job.threshold,
                },
            )
            .await
            .unwrap();
        ctx.offchain_order
            .send(
                &job.offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares: job.shares,
                    direction: job.direction,
                    executor: job.executor,
                },
            )
            .await
            .unwrap();

        // Fresh perform: PlaceOffChainOrder is rejected with PendingExecution, so
        // recover_pending_poll_status re-drives the Pending order to Submitted.
        job.perform(&ctx).await.unwrap();

        let order = offchain_order_projection
            .load(&job.offchain_order_id)
            .await
            .unwrap()
            .expect("offchain order should exist");
        assert!(
            matches!(order, OffchainOrder::Submitted { .. }),
            "Pending re-drive must advance the order to Submitted, got {order:?}"
        );

        let poll_jobs: i64 =
            sqlx_apalis::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(type_name::<PollOrderStatus>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        assert_eq!(
            poll_jobs, 1,
            "Pending re-drive must enqueue exactly one PollOrderStatus job"
        );
    }

    #[tokio::test]
    async fn perform_blocks_while_submission_lock_held() {
        // ADR 0014: PlaceHedge::perform serializes on the shared submission lock,
        // so it cannot place while another placement (the trade-processing path or
        // a recovery re-drive) holds it -- closing the MarkFailed/MarkAccepted race.
        let TestInfra {
            ctx,
            position_projection,
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

        // Hold the lock; perform must block on it and never reach placement.
        // `perform`'s first statement is `lock().await`, so it parks immediately
        // and `timeout` always elapses (perform can never resolve while the lock
        // is held). The window only needs to outlast `perform` reaching the lock,
        // so keep it small to avoid burning wall-clock on every run.
        let guard = ctx.counter_trade_submission_lock.clone().lock_owned().await;
        let blocked =
            tokio::time::timeout(std::time::Duration::from_millis(20), job.perform(&ctx)).await;
        blocked.unwrap_err();

        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id, None,
            "no placement may occur while the submission lock is held"
        );

        // Releasing the lock lets the same job proceed and place.
        drop(guard);
        job.perform(&ctx).await.unwrap();

        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id,
            Some(job.offchain_order_id),
            "placement proceeds once the lock is released"
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
