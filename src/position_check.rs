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
use st0x_event_sorcery::{Projection, Store};
use st0x_execution::{
    ClientOrderId, CounterTradePreflight, Executor, MarketOrder, MarketSession, Symbol,
};

use crate::conductor::clamp_shares_to_reservation;
use crate::conductor::job::{Job, JobQueue, Label, QueuePushError};
use crate::equity_redemption::symbols_with_active_transfers;
use crate::offchain::order::{
    CancellationReason, OffchainOrder, OffchainOrderCommand, OffchainOrderId,
    TerminalPositionFinalization, terminal_position_finalization,
};
use crate::onchain::accumulator::{ExecutionCtx, check_execution_readiness};
use crate::position::{Position, PositionCommand};
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
/// The only state carried across runs is `last_seen_session`: the stateless
/// job persists the previously observed market session in its own payload so
/// the next run can detect an Extended -> Regular transition and trigger the
/// extended-hours cancel-and-replace pass exactly once per transition.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct CheckPositions {
    /// Market session observed by the previous scan, carried in the job payload
    /// so the stateless self-rescheduling job can detect an Extended/Closed ->
    /// Regular transition. `None` on the bootstrap job and after any
    /// process restart: a `None` previous session deliberately does NOT trigger
    /// cancel-and-replace (startup recovery owns orphaned extended-hours
    /// orders), it only records the observed session for the next tick.
    /// `#[serde(default)]` lets job payloads enqueued before this field existed
    /// deserialize cleanly.
    #[serde(default)]
    last_seen_session: Option<MarketSession>,
}

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
        let next_session = if ctx.ctx.extended_hours_counter_trading {
            // Every tick: clear any position whose pending order has gone
            // terminal (e.g. a cancellation the poller has since confirmed).
            // This is the recovery half of cancel-and-replace and is what lets
            // the transition pass below fire exactly once instead of re-running
            // until each cancellation confirms.
            ctx.finalize_terminal_pending_positions().await;
            // Only on a fresh transition into regular hours: request
            // cancellation of still-live extended-hours limit orders so they're
            // replaced with market orders. Advances the carried session as soon
            // as cancellation is requested.
            ctx.request_extended_hours_cancellations(self.last_seen_session)
                .await
        } else {
            self.last_seen_session
        };

        ctx.scan_and_enqueue().await?;
        ctx.reschedule(next_session).await
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

    async fn reschedule(
        &self,
        last_seen_session: Option<MarketSession>,
    ) -> Result<(), CheckPositionsError> {
        let mut queue = self.check_positions_queue.clone();
        queue
            .push_with_delay(CheckPositions { last_seen_session }, self.check_interval)
            .await?;
        Ok(())
    }

    /// Clears every position whose pending offchain order has reached a
    /// terminal state, applying any recorded fill and releasing the pending
    /// reference so the symbol can resume hedging.
    ///
    /// Runs every scan, independent of the market session: this is the recovery
    /// half of cancel-and-replace. The transition pass only *requests*
    /// cancellation (moving the order to `Cancelling`); the poller later drives
    /// it terminal, and this method clears the owning position on a subsequent
    /// tick -- so the transition pass need not re-run until confirmation. It
    /// also recovers any position left referencing an already-terminal order by
    /// a prior transient failure.
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
                Ok(None) => continue,
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

    /// On a fresh transition into regular hours, requests broker cancellation
    /// of any still-live extended-hours limit orders so they can be replaced
    /// with market orders. Returns the session value to carry forward in the
    /// next job payload.
    ///
    /// Fires exactly once per transition: it advances the carried session as
    /// soon as cancellation has been *requested* (the order moves to
    /// `Cancelling`), rather than blocking until the poller confirms the
    /// terminal `Cancelled`. The poller drives the order terminal and
    /// [`Self::finalize_terminal_pending_positions`] clears the position on a
    /// later tick. `previous_session = None` (bootstrap / post-restart) does
    /// NOT fire -- startup recovery owns orphaned orders -- it only records the
    /// observed session. A transient lookup/send failure keeps the prior
    /// session so the transition is retried next tick rather than skipped.
    async fn request_extended_hours_cancellations(
        &self,
        previous_session: Option<MarketSession>,
    ) -> Option<MarketSession> {
        let session = match self.executor.market_session().await {
            Ok(session) => session,
            Err(error) => {
                warn!("Failed to check market session for cancel-and-replace: {error}");
                return previous_session;
            }
        };

        let transitioned_into_regular = session == MarketSession::Regular
            && matches!(previous_session, Some(previous) if previous != MarketSession::Regular);
        if !transitioned_into_regular {
            return Some(session);
        }

        let all_positions = match self.position_projection.load_all().await {
            Ok(positions) => positions,
            Err(error) => {
                warn!("Failed to load positions for cancel-and-replace: {error}");
                return previous_session;
            }
        };

        let mut all_requested = true;
        for (symbol, position) in &all_positions {
            let Some(offchain_order_id) = position.pending_offchain_order_id else {
                continue;
            };

            let order = match self.offchain_order.load(&offchain_order_id).await {
                Ok(Some(order)) => order,
                Ok(None) => continue,
                Err(error) => {
                    warn!(%symbol, %offchain_order_id, %error, "Failed to load offchain order for cancel-and-replace");
                    all_requested = false;
                    continue;
                }
            };

            // Only live extended-hours orders need cancelling. Terminal orders
            // are handled by finalize_terminal_pending_positions, and orders
            // already Cancelling are awaiting the poller's confirmation.
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
                warn!(%symbol, %offchain_order_id, %error, "Failed to request cancellation of extended-hours order");
                all_requested = false;
            }
        }

        if all_requested {
            Some(session)
        } else {
            previous_session
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
            Some(TerminalPositionFinalization::Complete {
                shares_filled,
                direction,
                executor_order_id,
                price,
                broker_timestamp,
            }) => PositionCommand::CompleteOffChainOrder {
                offchain_order_id,
                shares_filled,
                direction,
                executor_order_id,
                price,
                broker_timestamp,
            },
            Some(TerminalPositionFinalization::NoFill) => {
                let error = match order {
                    OffchainOrder::Cancelled { reason, .. } => format!("Cancelled: {reason:?}"),
                    OffchainOrder::Failed { error, .. } => error.clone(),
                    _ => "terminal order finalized with no fill".to_string(),
                };
                PositionCommand::FailOffChainOrder {
                    offchain_order_id,
                    error,
                }
            }
            Some(TerminalPositionFinalization::UnpricedFill { shares_filled }) => {
                warn!(
                    %symbol, %offchain_order_id, ?shares_filled,
                    "Terminal order has a partial fill without avg price; leaving position pending"
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
        };

        if let Err(error) = self.position.send(symbol, command).await {
            warn!(
                %symbol, %offchain_order_id, %error,
                "Failed to finalize position for terminal order"
            );
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

    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::{
        Direction, FractionalShares, MockExecutor, MockExecutorCtx, Symbol, TryIntoExecutor,
    };
    use st0x_float_macro::float;

    use st0x_config::{
        AssetsConfig, EquitiesConfig, EquityAssetConfig, ExecutionThreshold, OperationMode,
        create_test_ctx_with_order_owner,
    };

    use super::*;
    use crate::conductor::setup_apalis_tables;
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
        setup_apalis_tables(&pool).await.unwrap();

        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let executor = MockExecutorCtx.try_into_executor().await.unwrap();

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

    /// Reads the `last_seen_session` carried in the single rescheduled
    /// [`CheckPositions`] job payload.
    async fn rescheduled_session(pool: &SqlitePool) -> Option<MarketSession> {
        let job: Vec<u8> = sqlx::query_scalar("SELECT job FROM Jobs WHERE job_type = ? LIMIT 1")
            .bind(check_positions_job_type())
            .fetch_one(pool)
            .await
            .unwrap();
        let parsed: CheckPositions = serde_json::from_slice(&job).unwrap();
        parsed.last_seen_session
    }

    #[tokio::test]
    async fn carries_observed_session_into_rescheduled_payload() {
        let pool = setup_test_db().await;
        // Extended-hours enabled so the cancel-and-replace pass runs and the
        // observed Regular session is carried forward. MockExecutor reports an
        // open (Regular) market by default; with no prior session the pass runs
        // cleanly (no pending orders) and advances the carried session.
        let cfg = Ctx {
            extended_hours_counter_trading: true,
            ..dry_run_ctx(&["AAPL"])
        };
        let (ctx, _position) = build_ctx(pool.clone(), cfg, Duration::from_secs(60)).await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            rescheduled_session(&pool).await,
            Some(MarketSession::Regular),
            "first scan in regular hours should carry the observed session forward"
        );
    }

    #[tokio::test]
    async fn extended_hours_disabled_leaves_carried_session_unset() {
        let pool = setup_test_db().await;
        let cfg = dry_run_ctx(&["AAPL"]);
        let (ctx, _position) = build_ctx(pool.clone(), cfg, Duration::from_secs(60)).await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            rescheduled_session(&pool).await,
            None,
            "with extended hours disabled the cancel-and-replace pass never runs, so no session is observed"
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
