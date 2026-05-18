//! [`PollOrderStatus`] job: polls the broker for one order's state, then
//! either self-reschedules (still pending) or enqueues one of the reconcile
//! jobs.
//!
//! Each order gets its own job instance so a transient broker failure for
//! one order does not block polling of another, and unprocessed jobs survive
//! a crash/restart through apalis's SQLite-backed queue.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use st0x_event_sorcery::Projection;
use st0x_execution::{Executor, ExecutorOrderId, OrderState, SupportedExecutor, Symbol};
use st0x_finance::Usd;

#[cfg(any(test, feature = "test-support"))]
use crate::conductor::job::JobKind;
use crate::conductor::job::{Job, JobQueue, Label};
use crate::offchain::order::handle_rejection::{
    HandleOrderRejection, HandleOrderRejectionJobQueue,
};
use crate::offchain::order::reconcile_fill::{ReconcileOrderFill, ReconcileOrderFillJobQueue};
use crate::offchain::order::{JobError, OffchainOrder, OffchainOrderId};

pub(crate) type PollOrderStatusJobQueue = JobQueue<PollOrderStatus>;

/// Dependencies [`PollOrderStatus`] needs to query the broker and route the
/// result. Carries no aggregate stores -- the actual state mutation happens
/// in [`ReconcileOrderFill`] / [`HandleOrderRejection`], which this job
/// enqueues via their respective queues.
pub(crate) struct PollOrderStatusCtx<E: Executor + Clone + Send + Sync + 'static> {
    pub(crate) executor: E,
    pub(crate) offchain_order_projection: Arc<Projection<OffchainOrder>>,
    pub(crate) poll_status_queue: PollOrderStatusJobQueue,
    pub(crate) reconcile_queue: ReconcileOrderFillJobQueue,
    pub(crate) rejection_queue: HandleOrderRejectionJobQueue,
    pub(crate) poll_interval: Duration,
}

/// Polls the broker for the status of a single offchain order.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PollOrderStatus {
    pub(crate) offchain_order_id: OffchainOrderId,
}

impl<E> Job<PollOrderStatusCtx<E>> for PollOrderStatus
where
    E: Executor + Clone + Send + Sync + 'static,
    JobError: From<E::Error>,
{
    type Output = ();
    type Error = JobError;

    const WORKER_NAME: &'static str = "poll-order-status-worker";
    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: JobKind = JobKind::PollOrderStatus;

    fn label(&self) -> Label {
        Label::new(format!("PollOrderStatus:{}", self.offchain_order_id))
    }

    async fn perform(&self, ctx: &PollOrderStatusCtx<E>) -> Result<Self::Output, Self::Error> {
        let Some(order) = ctx
            .offchain_order_projection
            .load(&self.offchain_order_id)
            .await?
        else {
            warn!(
                target: "broker",
                offchain_order_id = %self.offchain_order_id,
                "PollOrderStatus: order not found in projection, skipping"
            );
            return Ok(());
        };

        let configured_executor = ctx.executor.to_supported_executor();
        let order_executor = order.executor();
        if order_executor != configured_executor {
            warn!(
                target: "broker",
                offchain_order_id = %self.offchain_order_id,
                symbol = %order.symbol(),
                ?order_executor,
                ?configured_executor,
                "PollOrderStatus: order placed via a different executor than the \
                 one currently configured, dropping job without polling"
            );
            return Ok(());
        }

        match dispatch_for_order_state(self.offchain_order_id, &order) {
            PollAction::Drop => Ok(()),
            PollAction::Reschedule => reschedule_self(ctx, self.offchain_order_id).await,
            PollAction::Query { executor_order_id } => {
                self.query_broker_and_dispatch(ctx, order.symbol(), executor_order_id)
                    .await
            }
        }
    }
}

impl PollOrderStatus {
    async fn query_broker_and_dispatch<E>(
        &self,
        ctx: &PollOrderStatusCtx<E>,
        symbol: &Symbol,
        executor_order_id: ExecutorOrderId,
    ) -> Result<(), JobError>
    where
        E: Executor + Clone + Send + Sync + 'static,
        JobError: From<E::Error>,
    {
        let parsed_order_id = ctx.executor.parse_order_id(executor_order_id.as_ref())?;
        let order_state = ctx.executor.get_order_status(&parsed_order_id).await?;

        self.dispatch_for_broker_state(ctx, symbol, order_state)
            .await
    }

    async fn dispatch_for_broker_state<E>(
        &self,
        ctx: &PollOrderStatusCtx<E>,
        symbol: &Symbol,
        order_state: OrderState,
    ) -> Result<(), JobError>
    where
        E: Executor + Clone + Send + Sync + 'static,
        JobError: From<E::Error>,
    {
        use OrderState::{Failed, Filled, Pending, Submitted};
        match order_state {
            Filled {
                price,
                order_id,
                executed_at,
            } => {
                self.enqueue_reconcile(ctx, symbol, price, order_id, executed_at)
                    .await
            }

            Failed { error_reason, .. } => self.enqueue_rejection(ctx, symbol, error_reason).await,

            Pending | Submitted { .. } => {
                debug!(
                    target: "broker",
                    %symbol,
                    offchain_order_id = %self.offchain_order_id,
                    "PollOrderStatus: broker reports still pending, re-enqueuing with delay"
                );

                reschedule_self(ctx, self.offchain_order_id).await
            }
        }
    }

    async fn enqueue_reconcile<E>(
        &self,
        ctx: &PollOrderStatusCtx<E>,
        symbol: &Symbol,
        price: rain_math_float::Float,
        order_id: String,
        executed_at: DateTime<Utc>,
    ) -> Result<(), JobError>
    where
        E: Executor + Clone + Send + Sync + 'static,
    {
        info!(
            target: "broker",
            %symbol,
            offchain_order_id = %self.offchain_order_id,
            "PollOrderStatus: broker reports Filled, enqueueing ReconcileOrderFill"
        );

        let mut queue = ctx.reconcile_queue.clone();
        queue
            .push(ReconcileOrderFill {
                offchain_order_id: self.offchain_order_id,
                price: Usd::new(price),
                executor_order_id: ExecutorOrderId::new(&order_id),
                broker_timestamp: executed_at,
            })
            .await?;

        Ok(())
    }

    async fn enqueue_rejection<E>(
        &self,
        ctx: &PollOrderStatusCtx<E>,
        symbol: &Symbol,
        error_reason: Option<String>,
    ) -> Result<(), JobError>
    where
        E: Executor + Clone + Send + Sync + 'static,
    {
        let error_message =
            error_reason.unwrap_or_else(|| "Order failed with no error reason".to_string());
        info!(
            target: "broker",
            %symbol,
            offchain_order_id = %self.offchain_order_id,
            %error_message,
            "PollOrderStatus: broker reports Failed, enqueueing HandleOrderRejection"
        );

        let mut queue = ctx.rejection_queue.clone();
        queue
            .push(HandleOrderRejection {
                offchain_order_id: self.offchain_order_id,
                error: error_message,
            })
            .await?;

        Ok(())
    }
}

/// Decision derived from the order's current aggregate state, before any
/// broker call. Lets [`PollOrderStatus::perform`] stay flat: branch on this,
/// then either return or do the broker round-trip.
enum PollAction {
    /// Already terminal or unrecoverable -- discard this poll job.
    Drop,
    /// Not yet submitted to the broker. Re-queue and wait.
    Reschedule,
    /// Submitted (or partially filled). Ask the broker for status.
    Query { executor_order_id: ExecutorOrderId },
}

fn dispatch_for_order_state(
    offchain_order_id: OffchainOrderId,
    order: &OffchainOrder,
) -> PollAction {
    use OffchainOrder::{Failed, Filled, PartiallyFilled, Pending, Submitted};
    let symbol = order.symbol();
    match order {
        Filled { .. } | Failed { .. } => {
            debug!(
                target: "broker",
                %symbol,
                %offchain_order_id,
                "PollOrderStatus: order already in terminal state, dropping"
            );

            PollAction::Drop
        }

        Pending { .. } => {
            debug!(
                target: "broker",
                %symbol,
                %offchain_order_id,
                "PollOrderStatus: order still Pending (not yet submitted), \
                 re-enqueuing with delay"
            );

            PollAction::Reschedule
        }

        Submitted { .. } | PartiallyFilled { .. } => order.executor_order_id().map_or_else(
            || {
                warn!(
                    target: "broker",
                    %symbol,
                    %offchain_order_id,
                    "PollOrderStatus: missing executor_order_id on submitted order"
                );

                PollAction::Drop
            },
            |executor_order_id| PollAction::Query {
                executor_order_id: executor_order_id.clone(),
            },
        ),
    }
}

async fn reschedule_self<E>(
    ctx: &PollOrderStatusCtx<E>,
    offchain_order_id: OffchainOrderId,
) -> Result<(), JobError>
where
    E: Executor + Clone + Send + Sync + 'static,
{
    let mut queue = ctx.poll_status_queue.clone();
    queue
        .push_with_delay(PollOrderStatus { offchain_order_id }, ctx.poll_interval)
        .await?;

    Ok(())
}

/// Re-enqueue a [`PollOrderStatus`] job for every offchain order still in a
/// non-terminal post-submission state. Idempotent: workers that pick up a
/// duplicate poll job for an already-reconciled order observe the terminal
/// state and exit cleanly.
///
/// Closes the gap between
/// [`OffchainOrderCommand::Place`](crate::offchain::order::OffchainOrderCommand::Place)
/// succeeding and the in-process push of the poll job that follows it -- if
/// the bot crashes between those two writes, the order would otherwise sit in
/// `Submitted` forever waiting for a poll that never comes.
pub(crate) async fn recover_submitted_offchain_orders(
    offchain_order_projection: &Projection<OffchainOrder>,
    poll_status_queue: &mut PollOrderStatusJobQueue,
    executor_type: SupportedExecutor,
) -> Result<(), JobError> {
    use OffchainOrder::{Failed, Filled, PartiallyFilled, Pending, Submitted};
    let orders = offchain_order_projection.load_all().await?;

    let pending_poll: Vec<_> = orders
        .into_iter()
        .filter_map(|(id, order)| match order {
            Submitted { .. } | PartiallyFilled { .. } if order.executor() == executor_type => {
                Some(id)
            }
            Submitted { .. }
            | PartiallyFilled { .. }
            | Pending { .. }
            | Filled { .. }
            | Failed { .. } => None,
        })
        .collect();

    if pending_poll.is_empty() {
        return Ok(());
    }

    info!(
        count = pending_poll.len(),
        "Re-enqueuing PollOrderStatus jobs for offchain orders awaiting broker fill"
    );

    for offchain_order_id in pending_poll {
        poll_status_queue
            .push(PollOrderStatus { offchain_order_id })
            .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use chrono::Utc;
    use sqlx::SqlitePool;

    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::{
        Direction, ExecutionError, FractionalShares, MockExecutor, Positive, Symbol,
    };
    use st0x_float_macro::float;

    use super::*;
    use crate::conductor::setup_apalis_tables;
    use crate::offchain::order::{OffchainOrderCommand, noop_order_placer};
    use crate::position::{Position, PositionCommand, TradeId};
    use crate::test_utils::{OnchainTradeBuilder, setup_test_db};
    use st0x_config::ExecutionThreshold;

    const TEST_POLL_INTERVAL: Duration = Duration::from_secs(15);

    struct TestInfra<E: Executor + Clone + Send + Sync + 'static> {
        ctx: PollOrderStatusCtx<E>,
        pool: SqlitePool,
        offchain_order: Arc<st0x_event_sorcery::Store<OffchainOrder>>,
        position: Arc<st0x_event_sorcery::Store<Position>>,
    }

    async fn build_test_infra<E: Executor + Clone + Send + Sync + 'static>(
        executor: E,
    ) -> TestInfra<E> {
        let pool = setup_test_db().await;
        setup_apalis_tables(&pool).await.unwrap();

        let (offchain_order, offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(noop_order_placer())
                .await
                .unwrap();

        let (position, _position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let ctx = PollOrderStatusCtx {
            executor,
            offchain_order_projection,
            poll_status_queue: PollOrderStatusJobQueue::new(&pool),
            reconcile_queue: ReconcileOrderFillJobQueue::new(&pool),
            rejection_queue: HandleOrderRejectionJobQueue::new(&pool),
            poll_interval: TEST_POLL_INTERVAL,
        };

        TestInfra {
            ctx,
            pool,
            offchain_order,
            position,
        }
    }

    async fn submit_offchain_order<E: Executor + Clone + Send + Sync + 'static>(
        infra: &TestInfra<E>,
        symbol: &Symbol,
        tokenized_symbol: &str,
        shares: Positive<FractionalShares>,
        direction: Direction,
    ) -> OffchainOrderId {
        submit_offchain_order_with_executor(
            infra,
            symbol,
            tokenized_symbol,
            shares,
            direction,
            SupportedExecutor::DryRun,
        )
        .await
    }

    async fn submit_offchain_order_with_executor<E: Executor + Clone + Send + Sync + 'static>(
        infra: &TestInfra<E>,
        symbol: &Symbol,
        tokenized_symbol: &str,
        shares: Positive<FractionalShares>,
        direction: Direction,
        order_executor: SupportedExecutor,
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
            .position
            .send(
                symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares,
                    direction,
                    executor: order_executor,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();

        infra
            .offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares,
                    direction,
                    executor: order_executor,
                },
            )
            .await
            .unwrap();

        offchain_order_id
    }

    async fn count_jobs(pool: &SqlitePool, job_type: &str) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
            .bind(job_type)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    fn poll_order_status_job_type() -> &'static str {
        std::any::type_name::<PollOrderStatus>()
    }

    fn reconcile_order_fill_job_type() -> &'static str {
        std::any::type_name::<ReconcileOrderFill>()
    }

    fn handle_order_rejection_job_type() -> &'static str {
        std::any::type_name::<HandleOrderRejection>()
    }

    async fn min_run_at(pool: &SqlitePool, job_type: &str) -> i64 {
        sqlx::query_scalar::<_, i64>(
            "SELECT MIN(run_at) FROM Jobs WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(job_type)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn poll_with_filled_broker_enqueues_reconcile_fill() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.pool, reconcile_order_fill_job_type()).await,
            1,
            "PollOrderStatus must enqueue exactly one ReconcileOrderFill on Filled"
        );
        assert_eq!(
            count_jobs(&infra.pool, handle_order_rejection_job_type()).await,
            0,
            "Filled state must not enqueue HandleOrderRejection"
        );
        assert_eq!(
            count_jobs(&infra.pool, poll_order_status_job_type()).await,
            0,
            "Filled state must not re-enqueue PollOrderStatus"
        );
    }

    #[tokio::test]
    async fn poll_with_pending_broker_reschedules_self_with_delay() {
        let executor = MockExecutor::new().with_order_status(OrderState::Pending);
        let infra = build_test_infra(executor).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        let before_now: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .try_into()
            .unwrap();

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.pool, poll_order_status_job_type()).await,
            1,
            "Pending broker state must self-reschedule a PollOrderStatus job"
        );
        assert_eq!(
            count_jobs(&infra.pool, reconcile_order_fill_job_type()).await,
            0
        );
        assert_eq!(
            count_jobs(&infra.pool, handle_order_rejection_job_type()).await,
            0
        );

        let scheduled_at = min_run_at(&infra.pool, poll_order_status_job_type()).await;
        let poll_interval_secs: i64 = TEST_POLL_INTERVAL.as_secs().try_into().unwrap();
        let expected_min = before_now + poll_interval_secs;
        assert!(
            scheduled_at >= expected_min,
            "Re-enqueued poll should run at >= now + poll_interval ({expected_min}s); got {scheduled_at}s"
        );
    }

    #[tokio::test]
    async fn poll_with_failed_broker_enqueues_handle_rejection() {
        let executor = MockExecutor::new().with_order_status(OrderState::Failed {
            failed_at: Utc::now(),
            error_reason: Some("broker rejected".to_string()),
        });
        let infra = build_test_infra(executor).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.pool, handle_order_rejection_job_type()).await,
            1,
            "Failed broker state must enqueue exactly one HandleOrderRejection"
        );
        assert_eq!(
            count_jobs(&infra.pool, reconcile_order_fill_job_type()).await,
            0,
            "Failed state must not enqueue ReconcileOrderFill"
        );
        assert_eq!(
            count_jobs(&infra.pool, poll_order_status_job_type()).await,
            0,
            "Terminal state must not re-enqueue PollOrderStatus"
        );
    }

    /// Two orders, two PollOrderStatus jobs. The broker fails the call for the
    /// first order and fills the second. The failure of the first must not
    /// prevent the second from progressing to ReconcileOrderFill -- the whole
    /// point of converting from a single-sweep poller to per-order jobs.
    #[tokio::test]
    async fn poll_jobs_for_different_orders_run_independently() {
        #[derive(Clone)]
        struct FailFirstExecutor {
            counter: Arc<AtomicUsize>,
            inner: MockExecutor,
        }

        #[async_trait::async_trait]
        impl Executor for FailFirstExecutor {
            type Error = ExecutionError;
            type OrderId = String;
            type Ctx = ();

            async fn try_from_ctx(_ctx: Self::Ctx) -> Result<Self, Self::Error> {
                unimplemented!()
            }

            async fn is_market_open(&self) -> Result<bool, Self::Error> {
                self.inner.is_market_open().await
            }

            async fn place_market_order(
                &self,
                order: st0x_execution::MarketOrder,
            ) -> Result<st0x_execution::OrderPlacement<Self::OrderId>, Self::Error> {
                self.inner.place_market_order(order).await
            }

            async fn get_order_status(
                &self,
                order_id: &Self::OrderId,
            ) -> Result<OrderState, Self::Error> {
                let n = self.counter.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    return Err(ExecutionError::MockFailure {
                        message: "simulated broker outage for first call".to_string(),
                    });
                }
                self.inner.get_order_status(order_id).await
            }

            fn parse_order_id(&self, order_id_str: &str) -> Result<Self::OrderId, Self::Error> {
                self.inner.parse_order_id(order_id_str)
            }

            async fn run_executor_maintenance(&self) -> Option<tokio::task::JoinHandle<()>> {
                self.inner.run_executor_maintenance().await
            }

            async fn get_inventory(&self) -> Result<st0x_execution::InventoryResult, Self::Error> {
                self.inner.get_inventory().await
            }

            fn to_supported_executor(&self) -> SupportedExecutor {
                SupportedExecutor::DryRun
            }

            async fn preflight_counter_trade(
                &self,
                order: st0x_execution::MarketOrder,
            ) -> Result<st0x_execution::CounterTradePreflight, Self::Error> {
                self.inner.preflight_counter_trade(order).await
            }
        }

        let infra = build_test_infra(FailFirstExecutor {
            counter: Arc::new(AtomicUsize::new(0)),
            inner: MockExecutor::new(),
        })
        .await;

        let symbol_failing = Symbol::new("AAPL").unwrap();
        let symbol_succeeding = Symbol::new("MSFT").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(1))).unwrap();
        let failing_id =
            submit_offchain_order(&infra, &symbol_failing, "wtAAPL", shares, Direction::Sell).await;
        let succeeding_id = submit_offchain_order(
            &infra,
            &symbol_succeeding,
            "wtMSFT",
            shares,
            Direction::Sell,
        )
        .await;

        let first_result = PollOrderStatus {
            offchain_order_id: failing_id,
        }
        .perform(&infra.ctx)
        .await;
        assert!(
            matches!(
                first_result,
                Err(JobError::Execution(ExecutionError::MockFailure { .. })),
            ),
            "first poll should propagate broker error, got {first_result:?}"
        );

        PollOrderStatus {
            offchain_order_id: succeeding_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.pool, reconcile_order_fill_job_type()).await,
            1,
            "Second order's poll must enqueue ReconcileOrderFill despite first order's failure"
        );
    }

    #[tokio::test]
    async fn recover_with_no_orders_enqueues_nothing() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let mut queue = infra.ctx.poll_status_queue.clone();

        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &mut queue,
            SupportedExecutor::DryRun,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.pool, poll_order_status_job_type()).await,
            0,
            "Empty projection must not enqueue any poll jobs"
        );
    }

    #[tokio::test]
    async fn recover_enqueues_poll_for_submitted_order() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let _ = submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;

        let mut queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &mut queue,
            SupportedExecutor::DryRun,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.pool, poll_order_status_job_type()).await,
            1,
            "Submitted order must trigger exactly one PollOrderStatus on recovery"
        );
    }

    #[tokio::test]
    async fn recover_enqueues_poll_for_partially_filled_order() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(10))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;
        infra
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(5)),
                    avg_price: Usd::new(float!(150.0)),
                },
            )
            .await
            .unwrap();

        let mut queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &mut queue,
            SupportedExecutor::DryRun,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.pool, poll_order_status_job_type()).await,
            1,
            "PartiallyFilled order must trigger one PollOrderStatus on recovery"
        );
    }

    #[tokio::test]
    async fn recover_skips_filled_order() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;
        infra
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.0)),
                },
            )
            .await
            .unwrap();

        let mut queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &mut queue,
            SupportedExecutor::DryRun,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.pool, poll_order_status_job_type()).await,
            0,
            "Filled order must not be re-polled"
        );
    }

    #[tokio::test]
    async fn recover_skips_failed_order() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id =
            submit_offchain_order(&infra, &symbol, "wtAAPL", shares, Direction::Sell).await;
        infra
            .offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::MarkFailed {
                    error: "broker rejected".to_string(),
                },
            )
            .await
            .unwrap();

        let mut queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &mut queue,
            SupportedExecutor::DryRun,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.pool, poll_order_status_job_type()).await,
            0,
            "Failed order must not be re-polled"
        );
    }

    #[tokio::test]
    async fn recover_enqueues_one_poll_per_eligible_order() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();

        let aapl = Symbol::new("AAPL").unwrap();
        let _ = submit_offchain_order(&infra, &aapl, "wtAAPL", shares, Direction::Sell).await;

        let tsla = Symbol::new("TSLA").unwrap();
        let _ = submit_offchain_order(&infra, &tsla, "wtTSLA", shares, Direction::Sell).await;

        let msft = Symbol::new("MSFT").unwrap();
        let msft_id = submit_offchain_order(&infra, &msft, "wtMSFT", shares, Direction::Sell).await;
        infra
            .offchain_order
            .send(
                &msft_id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.0)),
                },
            )
            .await
            .unwrap();

        let mut queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &mut queue,
            SupportedExecutor::DryRun,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.pool, poll_order_status_job_type()).await,
            2,
            "Two Submitted orders + one Filled must enqueue exactly two polls"
        );
    }

    #[tokio::test]
    async fn recover_skips_orders_from_different_executor() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();

        let aapl = Symbol::new("AAPL").unwrap();
        let _ = submit_offchain_order_with_executor(
            &infra,
            &aapl,
            "wtAAPL",
            shares,
            Direction::Sell,
            SupportedExecutor::AlpacaBrokerApi,
        )
        .await;

        let tsla = Symbol::new("TSLA").unwrap();
        let _ = submit_offchain_order_with_executor(
            &infra,
            &tsla,
            "wtTSLA",
            shares,
            Direction::Sell,
            SupportedExecutor::DryRun,
        )
        .await;

        let mut queue = infra.ctx.poll_status_queue.clone();
        recover_submitted_offchain_orders(
            &infra.ctx.offchain_order_projection,
            &mut queue,
            SupportedExecutor::DryRun,
        )
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.pool, poll_order_status_job_type()).await,
            1,
            "Only the order placed by the currently-configured executor must be re-polled"
        );
    }

    #[tokio::test]
    async fn poll_drops_job_when_order_belongs_to_different_executor() {
        let infra = build_test_infra(MockExecutor::new()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2))).unwrap();
        let order_id = submit_offchain_order_with_executor(
            &infra,
            &symbol,
            "wtAAPL",
            shares,
            Direction::Sell,
            SupportedExecutor::AlpacaBrokerApi,
        )
        .await;

        PollOrderStatus {
            offchain_order_id: order_id,
        }
        .perform(&infra.ctx)
        .await
        .unwrap();

        assert_eq!(
            count_jobs(&infra.pool, poll_order_status_job_type()).await,
            0,
            "Executor mismatch must drop the job without rescheduling"
        );
        assert_eq!(
            count_jobs(&infra.pool, reconcile_order_fill_job_type()).await,
            0,
            "Executor mismatch must not enqueue ReconcileOrderFill"
        );
        assert_eq!(
            count_jobs(&infra.pool, handle_order_rejection_job_type()).await,
            0,
            "Executor mismatch must not enqueue HandleOrderRejection"
        );
    }
}
