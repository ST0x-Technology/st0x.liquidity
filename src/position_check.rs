//! Periodic position scan as a durable, self-rescheduling job.
//!
//! Replaces the supervised polling task with a [`CheckPositions`] apalis job
//! that re-enqueues itself with the configured interval after each scan. Each
//! ready symbol becomes an independent [`PlaceHedge`] job, so a transient
//! failure for one symbol does not affect others.

use std::sync::Arc;
use std::time::Duration;

use apalis::prelude::Status;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tracing::{debug, error, info, warn};

use st0x_config::Ctx;
use st0x_event_sorcery::{AggregateError, LifecycleError, Projection, Store};
use st0x_execution::{
    ClientOrderId, CounterTradePreflight, Executor, MarketOrder, MarketSession, Symbol,
};

use crate::conductor::clamp_shares_to_reservation;
use crate::conductor::job::{Job, JobQueue, Label, QueuePushError};
use crate::equity_redemption::symbols_with_active_transfers;
use crate::offchain::order::{
    CancellationReason, OffchainOrder, OffchainOrderCommand, OffchainOrderId,
    TerminalPositionFinalization, position_command_for_finalization,
    terminal_position_finalization,
};
use crate::onchain::accumulator::{ExecutionCtx, check_execution_readiness};
use crate::position::{Position, PositionError};
use crate::trading::offchain::hedge::{HedgeJobQueue, PlaceHedge};

pub(crate) type CheckPositionsJobQueue = JobQueue<CheckPositions>;

/// Shared dependencies for the [`CheckPositions`] job.
pub(crate) struct CheckPositionsCtx<E: Executor + Clone + Send + Sync + 'static> {
    pub(crate) executor: E,
    pub(crate) position_projection: Arc<Projection<Position>>,
    /// Command stores used by the extended-hours cancel-and-replace pass to
    /// cancel stale limit orders and finalize the owning position.
    pub(crate) offchain_order: Arc<Store<OffchainOrder>>,
    pub(crate) position: Arc<Store<Position>>,
    pub(crate) hedge_queue: HedgeJobQueue,
    pub(crate) check_positions_queue: CheckPositionsJobQueue,
    pub(crate) ctx: Ctx,
    pub(crate) pool: SqlitePool,
    pub(crate) check_interval: Duration,
}

/// Errors surfaced by [`CheckPositions::perform`].
///
/// Per-symbol scan errors are logged and swallowed so one symbol's failure
/// cannot prevent others from being checked. Only failures that compromise
/// the periodic loop itself (loading the projection, querying transfers,
/// re-enqueuing the next tick) propagate.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CheckPositionsError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Position projection query error: {0}")]
    PositionProjection(#[from] st0x_event_sorcery::ProjectionError<Position>),
    #[error("Failed to enqueue follow-up job: {0}")]
    Enqueue(#[from] QueuePushError),
}

/// A durable, self-rescheduling job that scans every position and enqueues a
/// [`PlaceHedge`] for any symbol whose net exposure has crossed the execution
/// threshold.
///
/// The scan reads positions from the projection on each run. A single instance
/// is enqueued at startup; each run re-enqueues itself with a delay equal to
/// the configured check interval.
///
/// The job is stateless. In particular, the extended-hours cancel-and-replace
/// pass is level-triggered -- every scan that observes a Regular session sweeps
/// for still-live extended-hours orders -- so no previously-observed session
/// needs to be carried between runs. (An earlier edge-triggered design carried
/// a `last_seen_session` payload field; the empty braces keep old payloads
/// deserializing cleanly by ignoring it.)
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct CheckPositions {}

impl<E> Job<CheckPositionsCtx<E>> for CheckPositions
where
    E: Executor + Clone + Send + Sync + 'static,
{
    type Output = ();
    type Error = CheckPositionsError;

    const WORKER_NAME: &'static str = "check-positions-worker";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind = crate::conductor::job::JobKind::CheckPositions;

    fn label(&self) -> Label {
        Label::new("CheckPositions")
    }

    async fn perform(&self, ctx: &CheckPositionsCtx<E>) -> Result<Self::Output, Self::Error> {
        // Every tick, independent of the feature flag: clear any position
        // whose pending order has gone terminal (e.g. a cancellation the
        // poller has since confirmed). Terminal `Cancelled` orders are
        // produced by ungated paths too -- a manual broker-dashboard cancel,
        // or an order left `Cancelling` across a flag-off restart -- and this
        // sweep is the only runtime path that releases the position's pending
        // slot for them; gating it would strand such symbols unhedged until
        // the next restart.
        ctx.finalize_terminal_pending_positions().await;

        if ctx.ctx.extended_hours_counter_trading {
            // Every regular-hours tick: request cancellation of still-live
            // extended-hours limit orders so they're replaced with market
            // orders. Level-triggered so an order that slipped past the
            // session boundary, survived a restart, or whose cancellation
            // request failed on a previous tick is caught on this one.
            ctx.request_extended_hours_cancellations().await;
        }

        ctx.scan_and_enqueue().await?;
        ctx.reschedule().await
    }
}

impl<E> CheckPositionsCtx<E>
where
    E: Executor + Clone + Send + Sync + 'static,
{
    async fn scan_and_enqueue(&self) -> Result<(), CheckPositionsError> {
        let all_positions = self.position_projection.load_all().await?;
        let active_transfers = symbols_with_active_transfers(&self.pool).await?;

        let eligible: Vec<Symbol> = all_positions
            .iter()
            .filter(|(symbol, _)| self.ctx.is_trading_enabled(symbol))
            .filter(|(symbol, _)| {
                if active_transfers.contains(symbol) {
                    debug!(%symbol, "Skipping hedge: equity transfer in progress");
                    false
                } else {
                    true
                }
            })
            .map(|(symbol, _)| symbol.clone())
            .collect();

        for symbol in &eligible {
            self.check_and_enqueue_symbol(symbol).await;
        }

        Ok(())
    }

    async fn check_and_enqueue_symbol(&self, symbol: &Symbol) {
        let readiness = check_execution_readiness(
            &self.executor,
            &self.position_projection,
            symbol,
            self.executor.to_supported_executor(),
            &self.ctx.assets,
            true,
            self.ctx.extended_hours_counter_trading,
        )
        .await
        .inspect_err(|error| error!(%symbol, %error, "Execution readiness check failed"));

        let Ok(Some(mut ready)) = readiness else {
            debug!(%symbol, "Skipping hedge: no execution-ready position");
            return;
        };

        if !self.preflight_and_clamp_shares(&mut ready).await {
            return;
        }

        debug!(
            %ready.symbol, %ready.shares, ?ready.direction,
            "Enqueuing hedge job"
        );

        let job = PlaceHedge {
            symbol: ready.symbol.clone(),
            direction: ready.direction,
            shares: ready.shares,
            executor: ready.executor,
            threshold: self.ctx.execution_threshold,
            offchain_order_id: OffchainOrderId::new(),
            market_session: ready.market_session,
        };

        let mut queue = self.hedge_queue.clone();
        if let Err(error) = queue.push(job).await {
            error!(%ready.symbol, %error, "Failed to enqueue hedge job");
        }
    }

    /// Checks broker inventory before enqueueing a hedge job. Returns `true`
    /// if the order should proceed (possibly with reduced shares), `false` if
    /// it should be skipped entirely.
    async fn preflight_and_clamp_shares(&self, ready: &mut ExecutionCtx) -> bool {
        let order = MarketOrder {
            symbol: ready.symbol.clone(),
            shares: ready.shares,
            direction: ready.direction,
            // Preflight only; this id is never sent to the broker. Use a
            // fresh value so callers cannot mistake it for a real key.
            client_order_id: ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
        };

        match self.executor.preflight_counter_trade(order).await {
            Ok(CounterTradePreflight::Allowed { reservation }) => {
                clamp_shares_to_reservation(ready, reservation.as_ref());
                true
            }
            Ok(CounterTradePreflight::Skipped(reason)) => {
                warn!(
                    target: "hedge",
                    symbol = %ready.symbol, %reason,
                    "Skipping hedge enqueue: preflight rejected"
                );
                false
            }
            Err(error) => {
                error!(
                    target: "hedge",
                    symbol = %ready.symbol, %error,
                    "Preflight check failed during position scan"
                );
                false
            }
        }
    }

    async fn reschedule(&self) -> Result<(), CheckPositionsError> {
        let mut queue = self.check_positions_queue.clone();
        queue
            .push_with_delay(CheckPositions {}, self.check_interval)
            .await?;
        Ok(())
    }

    /// Clears every position whose pending offchain order has reached a
    /// terminal state, applying any recorded fill and releasing the pending
    /// reference so the symbol can resume hedging.
    ///
    /// Runs every scan, independent of the market session: this is the recovery
    /// half of cancel-and-replace. The cancellation pass only *requests*
    /// cancellation (moving the order to `Cancelling`); the poller later drives
    /// it terminal, and this method clears the owning position on a subsequent
    /// tick. It also recovers any position left referencing an already-terminal
    /// order by a prior transient failure.
    async fn finalize_terminal_pending_positions(&self) {
        let all_positions = match self.position_projection.load_all().await {
            Ok(positions) => positions,
            Err(error) => {
                warn!("Failed to load positions for terminal-order finalization: {error}");
                return;
            }
        };

        for (symbol, position) in &all_positions {
            let Some(offchain_order_id) = position.pending_offchain_order_id else {
                continue;
            };

            let order = match self.offchain_order.load(&offchain_order_id).await {
                Ok(Some(order)) => order,
                // Same claimed-but-not-recorded window as the cancel sweep:
                // PlaceHedge claims the position before creating the order
                // aggregate. Persistent recurrence (a crash inside that
                // window) is an anomaly an operator should see.
                Ok(None) => {
                    warn!(%symbol, %offchain_order_id, "Pending order aggregate not found during finalization; will retry next tick");
                    continue;
                }
                Err(error) => {
                    warn!(%symbol, %offchain_order_id, %error, "Failed to load offchain order for finalization");
                    continue;
                }
            };

            // Only terminal orders need position finalization here. Live and
            // in-flight orders (Pending/Submitted/PartiallyFilled/Cancelling)
            // are owned by the poll loop and reconcile jobs.
            if matches!(
                order,
                OffchainOrder::Cancelled { .. }
                    | OffchainOrder::Failed { .. }
                    | OffchainOrder::Filled { .. }
            ) {
                self.finalize_position_for_terminal_order(symbol, offchain_order_id, &order)
                    .await;
            }
        }
    }

    /// While the market is in regular hours, requests broker cancellation of
    /// any still-live extended-hours limit orders so they can be replaced
    /// with market orders on a subsequent scan.
    ///
    /// Level-triggered: the sweep runs on every regular-hours tick rather than
    /// only on an observed session transition. Idempotency comes from the
    /// per-order filter -- orders already `Cancelling` or terminal are skipped
    /// -- so re-running is safe and no cheaper edge trigger is needed
    /// ([`Self::finalize_terminal_pending_positions`] already performs an
    /// equivalent every-tick sweep). This catches orders an edge-triggered
    /// pass would strand for the whole session: a limit order submitted by a
    /// hedge job that read `Extended` just before 9:30 but placed after the
    /// transition tick scanned, a live order surviving a restart into regular
    /// hours (startup orphan-recovery only finalizes *terminal* orders), and
    /// any order whose lookup or cancellation request failed on a previous
    /// tick. The pass only *requests* cancellation (the order moves to
    /// `Cancelling`); the poller drives it terminal and
    /// [`Self::finalize_terminal_pending_positions`] clears the position on a
    /// later tick.
    async fn request_extended_hours_cancellations(&self) {
        let session = match self.executor.market_session().await {
            Ok(session) => session,
            Err(error) => {
                warn!("Failed to check market session for cancel-and-replace: {error}");
                return;
            }
        };

        if session != MarketSession::Regular {
            return;
        }

        let all_positions = match self.position_projection.load_all().await {
            Ok(positions) => positions,
            Err(error) => {
                warn!("Failed to load positions for cancel-and-replace: {error}");
                return;
            }
        };

        for (symbol, position) in &all_positions {
            let Some(offchain_order_id) = position.pending_offchain_order_id else {
                continue;
            };

            let order = match self.offchain_order.load(&offchain_order_id).await {
                Ok(Some(order)) => order,
                // The position references a pending order whose aggregate does
                // not exist yet: `PlaceHedge` claims the position before it
                // creates the offchain-order aggregate, so there is a brief
                // window where the order is "claimed but not recorded". The
                // level-triggered sweep picks it up on the next regular-hours
                // tick once the aggregate exists.
                Ok(None) => {
                    warn!(%symbol, %offchain_order_id, "Pending order aggregate not found during cancel-and-replace; will retry next tick");
                    continue;
                }
                Err(error) => {
                    warn!(%symbol, %offchain_order_id, %error, "Failed to load offchain order for cancel-and-replace; will retry next tick");
                    continue;
                }
            };

            // Skip orders placed via a different executor than the one
            // currently configured: cancellation dispatches through our
            // executor's broker, so cancelling a foreign order would
            // mis-target. Mirrors the guard in PollOrderStatus and
            // recover_submitted_offchain_orders.
            if order.executor() != self.executor.to_supported_executor() {
                continue;
            }

            // Only live extended-hours orders need cancelling. Terminal orders
            // are handled by finalize_terminal_pending_positions, and orders
            // already Cancelling are awaiting the poller's confirmation -- both
            // are skipped here, which is what makes the every-tick sweep
            // idempotent.
            let is_live_extended_hours = matches!(
                &order,
                OffchainOrder::Submitted { is_extended_hours, .. }
                    | OffchainOrder::PartiallyFilled { is_extended_hours, .. }
                    if *is_extended_hours
            );
            if !is_live_extended_hours {
                continue;
            }

            info!(
                target: "hedge",
                %symbol,
                %offchain_order_id,
                "Regular hours: cancelling extended-hours limit order for market-order replacement"
            );

            if let Err(error) = self
                .offchain_order
                .send(
                    &offchain_order_id,
                    OffchainOrderCommand::CancelOrder {
                        reason: CancellationReason::MarketOpenReplacement,
                    },
                )
                .await
            {
                warn!(%symbol, %offchain_order_id, %error, "Failed to request cancellation of extended-hours order; will retry next tick");
            }
        }
    }

    /// After a successful cancel (or a recovery scan finding an already-
    /// terminal order), propagate the broker's actual fill quantity to the
    /// position aggregate so net is correctly debited. Otherwise a partial
    /// fill recorded on the offchain side is invisible to the position
    /// scanner and the next cycle re-hedges the same shares.
    async fn finalize_position_for_terminal_order(
        &self,
        symbol: &Symbol,
        offchain_order_id: OffchainOrderId,
        order: &OffchainOrder,
    ) -> bool {
        let command = match terminal_position_finalization(order) {
            Some(TerminalPositionFinalization::UnpricedFill { shares_filled }) => {
                error!(
                    %symbol, %offchain_order_id, ?shares_filled,
                    "Terminal order has a partial fill without avg price; position left \
                     pending -- no automated path can finalize this, operator intervention \
                     required"
                );
                return false;
            }
            None => {
                warn!(
                    %symbol, %offchain_order_id, state = ?order,
                    "Order in non-terminal state during finalization; skipping"
                );
                return false;
            }
            // Complete and both NoFill outcomes map through the shared
            // helper so the terminal-state -> position-command mapping
            // cannot drift from the recovery paths.
            Some(finalization) => {
                let Some(command) =
                    position_command_for_finalization(finalization, offchain_order_id)
                else {
                    // Unreachable: UnpricedFill (the only None mapping) is
                    // handled above. Leave the position pending rather than
                    // guessing a command.
                    warn!(
                        %symbol, %offchain_order_id,
                        "Terminal finalization produced no position command; leaving pending"
                    );
                    return false;
                };
                command
            }
        };

        if let Err(error) = self.position.send(symbol, command).await {
            // A benign race: the poll loop (or a prior finalize tick) already
            // finalized this position, but our projection read was stale. The
            // aggregate rejects the duplicate finalize via
            // `validate_pending_execution`. Log it at debug -- warn here trains
            // operators to ignore a self-healing condition and would mask a
            // genuine finalize failure.
            match &error {
                AggregateError::UserError(LifecycleError::Apply(
                    PositionError::NoPendingExecution
                    | PositionError::OffchainOrderIdMismatch { .. },
                )) => {
                    debug!(
                        %symbol, %offchain_order_id, %error,
                        "Position already finalized by another writer; skipping"
                    );
                }
                _ => {
                    warn!(
                        %symbol, %offchain_order_id, %error,
                        "Failed to finalize position for terminal order"
                    );
                }
            }
            return false;
        }
        true
    }
}

/// Removes any non-terminal [`CheckPositions`] jobs and pushes a fresh one.
///
/// Each scan re-enqueues itself with a delay, so a still-scheduled job from a
/// previous run remains in the queue across restarts. Without the purge the
/// number of concurrent CheckPositions loops would grow by one with every
/// restart, multiplying scan load and duplicate hedge enqueues. The fresh
/// push guarantees the periodic scan starts immediately on this run.
pub(crate) async fn bootstrap_check_positions(
    pool: &SqlitePool,
    queue: &CheckPositionsJobQueue,
) -> Result<(), CheckPositionsError> {
    purge_pending_check_positions_jobs(pool).await?;
    queue.clone().push(CheckPositions::default()).await?;
    Ok(())
}

async fn purge_pending_check_positions_jobs(pool: &SqlitePool) -> Result<u64, sqlx::Error> {
    let job_type = std::any::type_name::<CheckPositions>();
    let deleted = sqlx::query(
        "DELETE FROM Jobs WHERE job_type = ? AND (status IN (?, ?) \
         OR (status = ? AND attempts < max_attempts))",
    )
    .bind(job_type)
    .bind(Status::Pending.to_string())
    .bind(Status::Running.to_string())
    .bind(Status::Failed.to_string())
    .execute(pool)
    .await?
    .rows_affected();

    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use alloy::primitives::{Address, TxHash, address};
    use sqlx::SqlitePool;

    use st0x_config::{
        AssetsConfig, EquitiesConfig, EquityAssetConfig, ExecutionThreshold, OperationMode,
        create_test_ctx_with_order_owner,
    };
    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::{
        ClientOrderId, Direction, ExecutorOrderId, FractionalShares, MockExecutor, MockExecutorCtx,
        OrderState, Positive, SupportedExecutor, Symbol, TryIntoExecutor,
    };
    use st0x_finance::Usd;
    use st0x_float_macro::float;

    use super::*;
    use crate::conductor::setup_apalis_tables;
    use crate::offchain::order::CounterTradeOrderKind;
    use crate::position::{PositionCommand, TradeId};
    use crate::test_utils::setup_test_db;

    async fn build_ctx(
        pool: SqlitePool,
        ctx_cfg: Ctx,
        check_interval: Duration,
    ) -> (
        CheckPositionsCtx<MockExecutor>,
        Arc<st0x_event_sorcery::Store<Position>>,
    ) {
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        build_ctx_with_executor(pool, ctx_cfg, check_interval, executor).await
    }

    async fn build_ctx_with_executor(
        pool: SqlitePool,
        ctx_cfg: Ctx,
        check_interval: Duration,
        executor: MockExecutor,
    ) -> (
        CheckPositionsCtx<MockExecutor>,
        Arc<st0x_event_sorcery::Store<Position>>,
    ) {
        setup_apalis_tables(&pool).await.unwrap();

        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let order_placer: Arc<dyn crate::offchain::order::OrderPlacer> = Arc::new(
            crate::offchain::order::ExecutorOrderPlacer(executor.clone()),
        );
        let (offchain_order, _offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(order_placer)
                .await
                .unwrap();

        let ctx = CheckPositionsCtx {
            executor,
            position_projection,
            offchain_order,
            position: position.clone(),
            hedge_queue: HedgeJobQueue::new(&pool),
            check_positions_queue: CheckPositionsJobQueue::new(&pool),
            ctx: ctx_cfg,
            pool,
            check_interval,
        };

        (ctx, position)
    }

    async fn accumulate_position(
        position: &st0x_event_sorcery::Store<Position>,
        symbol: &Symbol,
        amount: FractionalShares,
        direction: Direction,
    ) {
        position
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

    async fn count_jobs(pool: &SqlitePool, job_type: &str) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
            .bind(job_type)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    fn hedge_job_type() -> String {
        std::any::type_name::<PlaceHedge>().to_string()
    }

    fn check_positions_job_type() -> String {
        std::any::type_name::<CheckPositions>().to_string()
    }

    fn dry_run_ctx(symbols: &[&str]) -> Ctx {
        let mut equity_symbols = HashMap::new();
        for symbol in symbols {
            equity_symbols.insert(
                Symbol::new(*symbol).unwrap(),
                EquityAssetConfig {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    pyth_feed_id: None,
                    vault_ids: Vec::new(),
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                },
            );
        }

        Ctx {
            assets: AssetsConfig {
                equities: EquitiesConfig {
                    operational_limit: None,
                    symbols: equity_symbols,
                },
                cash: None,
            },
            execution_threshold: ExecutionThreshold::whole_share(),
            ..create_test_ctx_with_order_owner(address!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ))
        }
    }

    #[tokio::test]
    async fn enqueues_one_hedge_per_ready_symbol() {
        let pool = setup_test_db().await;
        let cfg = dry_run_ctx(&["AAPL", "TSLA"]);
        let (ctx, position) = build_ctx(pool.clone(), cfg, Duration::from_secs(60)).await;

        let aapl = Symbol::new("AAPL").unwrap();
        let tsla = Symbol::new("TSLA").unwrap();

        accumulate_position(
            &position,
            &aapl,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;
        accumulate_position(
            &position,
            &tsla,
            FractionalShares::new(float!(3.0)),
            Direction::Buy,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(count_jobs(&pool, &hedge_job_type()).await, 2);
    }

    #[tokio::test]
    async fn no_positions_above_threshold_enqueues_no_hedge_jobs() {
        let pool = setup_test_db().await;
        let cfg = dry_run_ctx(&["AAPL"]);
        let (ctx, position) = build_ctx(pool.clone(), cfg, Duration::from_secs(60)).await;

        let aapl = Symbol::new("AAPL").unwrap();

        accumulate_position(
            &position,
            &aapl,
            FractionalShares::new(float!(0.1)),
            Direction::Buy,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(count_jobs(&pool, &hedge_job_type()).await, 0);
    }

    #[tokio::test]
    async fn reschedules_itself_with_configured_interval() {
        let pool = setup_test_db().await;
        let cfg = dry_run_ctx(&["AAPL"]);
        let interval = Duration::from_secs(42);
        let (ctx, _position) = build_ctx(pool.clone(), cfg, interval).await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(count_jobs(&pool, &check_positions_job_type()).await, 1);

        let run_at: i64 = sqlx::query_scalar("SELECT run_at FROM Jobs WHERE job_type = ? LIMIT 1")
            .bind(check_positions_job_type())
            .fetch_one(&pool)
            .await
            .unwrap();

        let now = chrono::Utc::now().timestamp();
        let expected = now + i64::try_from(interval.as_secs()).unwrap();
        assert!(
            (run_at - expected).abs() <= 2,
            "expected run_at near {expected}, got {run_at}"
        );
    }

    /// Claims `symbol`'s position with the given pending offchain order id.
    /// Mirrors the first half of `PlaceHedge::perform`; deliberately does NOT
    /// create the offchain-order aggregate, so callers can model the
    /// "claimed but not yet recorded" window.
    async fn claim_position(
        ctx: &CheckPositionsCtx<MockExecutor>,
        symbol: &Symbol,
        offchain_order_id: OffchainOrderId,
    ) {
        ctx.position
            .send(
                symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();
    }

    /// Records a live extended-hours limit order aggregate for a previously
    /// claimed position, completing the second half of `PlaceHedge::perform`.
    async fn record_extended_hours_order(
        ctx: &CheckPositionsCtx<MockExecutor>,
        symbol: &Symbol,
        offchain_order_id: OffchainOrderId,
    ) {
        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    client_order_id: ClientOrderId::from_uuid(offchain_order_id.as_uuid()),
                    kind: CounterTradeOrderKind::ExtendedHoursLimit {
                        limit_price: Positive::new(Usd::new(float!(195.25))).unwrap(),
                    },
                },
            )
            .await
            .unwrap();
    }

    /// MockExecutor reporting a Regular session whose `get_order_status`
    /// returns `Submitted`, so the pre-cancel reconcile does not short-circuit
    /// and a DELETE drives the order to `Cancelling`.
    fn regular_session_executor() -> MockExecutor {
        MockExecutor::new()
            .with_market_session(MarketSession::Regular)
            .with_order_status(OrderState::Submitted {
                order_id: ExecutorOrderId::new("broker-eh-1"),
            })
    }

    #[tokio::test]
    async fn restart_into_regular_hours_cancels_live_extended_hours_order() {
        // A live extended-hours limit order that survives a restart into
        // regular hours: the very first scan after the restart observes
        // Regular and the level-triggered cancel-and-replace pass must fire.
        // Startup orphan-recovery finalizes only *terminal* orders, so without
        // this the live limit order would rest unconverted for the whole
        // session, leaving the position under-hedged.
        let pool = setup_test_db().await;
        let cfg = Ctx {
            extended_hours_counter_trading: true,
            ..dry_run_ctx(&["AAPL"])
        };
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            cfg,
            Duration::from_secs(60),
            regular_session_executor(),
        )
        .await;

        let aapl = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &aapl,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let offchain_order_id = OffchainOrderId::new();
        claim_position(&ctx, &aapl, offchain_order_id).await;
        record_extended_hours_order(&ctx, &aapl, offchain_order_id).await;

        // First scan after the restart: market already Regular.
        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        assert!(
            matches!(order, OffchainOrder::Cancelling { .. }),
            "restart catch-up must request cancellation of the live extended-hours order, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn finalize_sweep_releases_broker_cancelled_position_with_feature_disabled() {
        // The finalize sweep must run on EVERY tick, independent of the
        // extended-hours flag: the paths that produce terminal Cancelled
        // orders (poller confirming a manual broker-dashboard cancel, an
        // order left Cancelling across a flag-off restart) are not gated, and
        // this sweep -- driven here through the real CheckPositions::perform
        // -- is the only runtime path that releases the position's pending
        // slot for them.
        let pool = setup_test_db().await;
        let cfg = Ctx {
            extended_hours_counter_trading: false,
            ..dry_run_ctx(&["AAPL"])
        };
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            cfg,
            Duration::from_secs(60),
            regular_session_executor(),
        )
        .await;

        let aapl = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &aapl,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let offchain_order_id = OffchainOrderId::new();
        claim_position(&ctx, &aapl, offchain_order_id).await;
        record_extended_hours_order(&ctx, &aapl, offchain_order_id).await;

        // Drive the order terminal: request cancellation (broker DELETE) and
        // confirm it, as the poller would after a broker-side cancel.
        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::Unrequested,
                },
            )
            .await
            .unwrap();
        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::ConfirmCancellation {
                    cancelled_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        CheckPositions::default().perform(&ctx).await.unwrap();

        let recovered = ctx
            .position_projection
            .load(&aapl)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            recovered.pending_offchain_order_id, None,
            "flag-off finalize sweep must release the broker-cancelled position"
        );
        assert_eq!(
            recovered.last_failed_offchain_order_id, None,
            "an intentional cancellation must not set the failure anchor"
        );
    }

    #[tokio::test]
    async fn finalize_sweep_applies_retained_partial_fill_to_position_net() {
        // The Complete branch of the sweep: a cancelled order that retained a
        // priced partial fill must debit the position's net through the real
        // CheckPositions::perform, not just release the pending slot --
        // otherwise the next scan re-hedges shares the broker already filled.
        let pool = setup_test_db().await;
        let cfg = Ctx {
            extended_hours_counter_trading: false,
            ..dry_run_ctx(&["AAPL"])
        };
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            cfg,
            Duration::from_secs(60),
            regular_session_executor(),
        )
        .await;

        let aapl = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &aapl,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let offchain_order_id = OffchainOrderId::new();
        claim_position(&ctx, &aapl, offchain_order_id).await;
        record_extended_hours_order(&ctx, &aapl, offchain_order_id).await;

        // Half the 1-share sell order fills before the cancellation lands.
        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: FractionalShares::new(float!(0.5)),
                    avg_price: Usd::new(float!(195.25)),
                    partially_filled_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::Unrequested,
                },
            )
            .await
            .unwrap();
        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::ConfirmCancellation {
                    cancelled_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        CheckPositions::default().perform(&ctx).await.unwrap();

        let recovered = ctx
            .position_projection
            .load(&aapl)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            recovered.pending_offchain_order_id, None,
            "finalize sweep must release the position"
        );
        assert_eq!(
            recovered.net,
            FractionalShares::new(float!(1.5)),
            "the retained 0.5-share sell fill must debit net (2.0 -> 1.5)"
        );

        // Idempotency: a second tick over the already-finalized position must
        // succeed without re-applying the fill.
        CheckPositions::default().perform(&ctx).await.unwrap();
        let after_second_tick = ctx
            .position_projection
            .load(&aapl)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(after_second_tick.net, FractionalShares::new(float!(1.5)));
    }

    #[tokio::test]
    async fn regular_tick_cancels_extended_hours_order_placed_after_previous_tick() {
        // Boundary straddle: a hedge job that read Extended just before 9:30
        // can submit its extended-hours limit order AFTER the first
        // regular-hours scan already ran (and found nothing to cancel). The
        // cancel-and-replace pass is level-triggered -- it sweeps every
        // regular-hours tick -- so the next tick must still converge the
        // straddling order. An edge-triggered pass would have consumed the
        // transition on the first tick and stranded the order for the whole
        // session.
        let pool = setup_test_db().await;
        let cfg = Ctx {
            extended_hours_counter_trading: true,
            ..dry_run_ctx(&["AAPL"])
        };
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            cfg,
            Duration::from_secs(60),
            regular_session_executor(),
        )
        .await;

        let aapl = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &aapl,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        // First regular-hours tick: no pending order exists yet, the sweep
        // finds nothing.
        CheckPositions::default().perform(&ctx).await.unwrap();

        // The boundary-straddling extended-hours order lands after that tick.
        let offchain_order_id = OffchainOrderId::new();
        claim_position(&ctx, &aapl, offchain_order_id).await;
        record_extended_hours_order(&ctx, &aapl, offchain_order_id).await;

        // The next regular-hours tick must still request cancellation.
        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        assert!(
            matches!(order, OffchainOrder::Cancelling { .. }),
            "a regular-hours tick after the transition must still cancel a \
             boundary-straddling extended-hours order, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn unresolvable_order_aggregate_is_retried_next_tick_without_blocking_others() {
        // AAPL's position is claimed but its offchain-order aggregate does not
        // exist yet (`PlaceHedge` claims the position before recording the
        // aggregate, so a scan can land inside that window). The sweep must
        // still cancel TSLA's live extended-hours order on this tick, skip
        // AAPL without failing, and pick AAPL up on a later tick once its
        // aggregate exists -- a failed per-order pass is retried, never lost.
        let pool = setup_test_db().await;
        let cfg = Ctx {
            extended_hours_counter_trading: true,
            ..dry_run_ctx(&["AAPL", "TSLA"])
        };
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            cfg,
            Duration::from_secs(60),
            regular_session_executor(),
        )
        .await;

        let aapl = Symbol::new("AAPL").unwrap();
        let tsla = Symbol::new("TSLA").unwrap();
        for symbol in [&aapl, &tsla] {
            accumulate_position(
                &position,
                symbol,
                FractionalShares::new(float!(2.0)),
                Direction::Buy,
            )
            .await;
        }

        let aapl_order_id = OffchainOrderId::new();
        claim_position(&ctx, &aapl, aapl_order_id).await;

        let tsla_order_id = OffchainOrderId::new();
        claim_position(&ctx, &tsla, tsla_order_id).await;
        record_extended_hours_order(&ctx, &tsla, tsla_order_id).await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let tsla_order = ctx
            .offchain_order
            .load(&tsla_order_id)
            .await
            .unwrap()
            .expect("TSLA order should exist");
        assert!(
            matches!(tsla_order, OffchainOrder::Cancelling { .. }),
            "an unresolvable order for one symbol must not block another \
             symbol's cancellation, got: {tsla_order:?}"
        );

        // The hedge job catches up: AAPL's aggregate now exists as a live
        // extended-hours order.
        record_extended_hours_order(&ctx, &aapl, aapl_order_id).await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let aapl_order = ctx
            .offchain_order
            .load(&aapl_order_id)
            .await
            .unwrap()
            .expect("AAPL order should exist");
        assert!(
            matches!(aapl_order, OffchainOrder::Cancelling { .. }),
            "the level-triggered sweep must retry the previously unresolvable \
             order on the next tick, got: {aapl_order:?}"
        );
    }

    #[tokio::test]
    async fn extended_hours_disabled_does_not_cancel_live_extended_hours_order() {
        // With the feature flag off, the cancel-and-replace pass must not run
        // at all: a live extended-hours order is left untouched even when the
        // scan observes regular hours.
        let pool = setup_test_db().await;
        let cfg = dry_run_ctx(&["AAPL"]);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            cfg,
            Duration::from_secs(60),
            regular_session_executor(),
        )
        .await;

        let aapl = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &aapl,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let offchain_order_id = OffchainOrderId::new();
        claim_position(&ctx, &aapl, offchain_order_id).await;
        record_extended_hours_order(&ctx, &aapl, offchain_order_id).await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        assert!(
            matches!(
                order,
                OffchainOrder::Submitted {
                    is_extended_hours: true,
                    ..
                }
            ),
            "with extended hours disabled the cancel pass must not touch the order, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn skips_trading_disabled_symbols_without_blocking_others() {
        let pool = setup_test_db().await;
        // RKLB is intentionally absent from the trading config -- the scan
        // must skip it without aborting the rest of the loop.
        let cfg = dry_run_ctx(&["AAPL", "TSLA"]);
        let (ctx, position) = build_ctx(pool.clone(), cfg, Duration::from_secs(60)).await;

        let aapl = Symbol::new("AAPL").unwrap();
        let rklb = Symbol::new("RKLB").unwrap();
        let tsla = Symbol::new("TSLA").unwrap();

        for (symbol, shares) in [(&aapl, 2.0), (&rklb, 5.0), (&tsla, 4.0)] {
            accumulate_position(
                &position,
                symbol,
                FractionalShares::new(float!(shares)),
                Direction::Buy,
            )
            .await;
        }

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&pool, &hedge_job_type()).await,
            2,
            "AAPL and TSLA should produce hedges; RKLB (untraded) is skipped"
        );
    }

    #[tokio::test]
    async fn purge_removes_pending_running_and_retryable_failed_but_keeps_terminal() {
        let pool = setup_test_db().await;
        setup_apalis_tables(&pool).await.unwrap();

        let job_type = check_positions_job_type();

        async fn insert(pool: &SqlitePool, job_type: &str, id: &str, status: &str, attempts: i64) {
            sqlx::query(
                "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts) \
                 VALUES (?, ?, ?, ?, ?, 25)",
            )
            .bind("{}")
            .bind(id)
            .bind(job_type)
            .bind(status)
            .bind(attempts)
            .execute(pool)
            .await
            .unwrap();
        }

        insert(
            &pool,
            &job_type,
            "pending-1",
            &Status::Pending.to_string(),
            0,
        )
        .await;
        insert(
            &pool,
            &job_type,
            "running-1",
            &Status::Running.to_string(),
            0,
        )
        .await;
        insert(
            &pool,
            &job_type,
            "failed-retryable",
            &Status::Failed.to_string(),
            3,
        )
        .await;
        insert(
            &pool,
            &job_type,
            "failed-exhausted",
            &Status::Failed.to_string(),
            25,
        )
        .await;
        insert(&pool, &job_type, "done-1", &Status::Done.to_string(), 1).await;
        insert(&pool, &job_type, "killed-1", &Status::Killed.to_string(), 1).await;

        let deleted = purge_pending_check_positions_jobs(&pool).await.unwrap();
        assert_eq!(deleted, 3);

        let remaining: Vec<String> = sqlx::query_scalar("SELECT id FROM Jobs WHERE job_type = ?")
            .bind(&job_type)
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(remaining.len(), 3);
        assert!(remaining.contains(&"failed-exhausted".to_string()));
        assert!(remaining.contains(&"done-1".to_string()));
        assert!(remaining.contains(&"killed-1".to_string()));
    }
}
