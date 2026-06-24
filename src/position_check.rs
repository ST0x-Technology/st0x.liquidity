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
use tracing::{debug, error, warn};

use st0x_config::Ctx;
use st0x_event_sorcery::Projection;
use st0x_execution::{ClientOrderId, CounterTradePreflight, Executor, MarketOrder, Symbol};

use crate::conductor::clamp_shares_to_reservation;
use crate::conductor::job::{Job, JobQueue, Label, QueuePushError};
use crate::equity_redemption::symbols_with_active_transfers;
use crate::offchain::order::OffchainOrderId;
use crate::onchain::accumulator::{ExecutionCtx, check_execution_readiness};
use crate::position::Position;
use crate::trading::offchain::hedge::{HedgeJobQueue, PlaceHedge};

pub(crate) type CheckPositionsJobQueue = JobQueue<CheckPositions>;

/// Shared dependencies for the [`CheckPositions`] job.
pub(crate) struct CheckPositionsCtx<E: Executor + Clone + Send + Sync + 'static> {
    pub(crate) executor: E,
    pub(crate) position_projection: Arc<Projection<Position>>,
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
    #[error("Apalis database error: {0}")]
    ApalisDatabase(#[from] sqlx_apalis::Error),
    #[error("Position projection query error: {0}")]
    PositionProjection(#[from] st0x_event_sorcery::ProjectionError<Position>),
    #[error("Failed to enqueue follow-up job: {0}")]
    Enqueue(#[from] QueuePushError),
}

/// A durable, self-rescheduling job that scans every position and enqueues a
/// [`PlaceHedge`] for any symbol whose net exposure has crossed the execution
/// threshold.
///
/// The job carries no state -- the scan reads everything from the position
/// projection on each run. A single instance is enqueued at startup; each
/// run re-enqueues itself with a delay equal to the configured check
/// interval.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct CheckPositions;

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
            .push_with_delay(CheckPositions, self.check_interval)
            .await?;
        Ok(())
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
    apalis_pool: &apalis_sqlite::SqlitePool,
    queue: &CheckPositionsJobQueue,
) -> Result<(), CheckPositionsError> {
    purge_pending_check_positions_jobs(apalis_pool).await?;
    queue.clone().push(CheckPositions).await?;
    Ok(())
}

async fn purge_pending_check_positions_jobs(
    apalis_pool: &apalis_sqlite::SqlitePool,
) -> Result<u64, sqlx_apalis::Error> {
    let job_type = std::any::type_name::<CheckPositions>();
    let deleted = sqlx_apalis::query(
        "DELETE FROM Jobs WHERE job_type = ? AND (status IN (?, ?) \
         OR (status = ? AND attempts < max_attempts))",
    )
    .bind(job_type)
    .bind(Status::Pending.to_string())
    .bind(Status::Running.to_string())
    .bind(Status::Failed.to_string())
    .execute(apalis_pool)
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
    use crate::position::{PositionCommand, TradeId};
    use crate::test_utils::setup_test_pools;

    async fn build_ctx(
        pool: SqlitePool,
        apalis_pool: apalis_sqlite::SqlitePool,
        ctx_cfg: Ctx,
        check_interval: Duration,
    ) -> (
        CheckPositionsCtx<MockExecutor>,
        Arc<st0x_event_sorcery::Store<Position>>,
    ) {
        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let executor = MockExecutorCtx.try_into_executor().await.unwrap();

        let ctx = CheckPositionsCtx {
            executor,
            position_projection,
            hedge_queue: HedgeJobQueue::new(&apalis_pool),
            check_positions_queue: CheckPositionsJobQueue::new(&apalis_pool),
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

    async fn count_jobs(apalis_pool: &apalis_sqlite::SqlitePool, job_type: &str) -> i64 {
        sqlx_apalis::query_scalar::<_, i64>("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
            .bind(job_type)
            .fetch_one(apalis_pool)
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
                    extended_hours_counter_trading: OperationMode::Disabled,
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

    /// A broker outage must not kill the periodic position scan: the
    /// per-symbol broker preflight errors (the mock's market-hours check
    /// passes, so the failure surfaces in `preflight_counter_trade`), are
    /// logged and swallowed, no hedge is enqueued against the dead broker, and
    /// the scan reschedules itself for the next tick. This invariant is what
    /// lets a fill recorded during an outage get hedged by the first
    /// healthy rescan instead of sitting as silent exposure.
    #[tokio::test]
    async fn broker_outage_does_not_kill_scan_and_reschedules() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"]);

        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let aapl = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &aapl,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let ctx = CheckPositionsCtx {
            executor: MockExecutor::with_failure("connection refused"),
            position_projection,
            hedge_queue: HedgeJobQueue::new(&apalis_pool),
            check_positions_queue: CheckPositionsJobQueue::new(&apalis_pool),
            ctx: cfg,
            pool: pool.clone(),
            check_interval: Duration::from_secs(60),
        };

        CheckPositions.perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            0,
            "No hedge can be enqueued against a dead broker"
        );
        assert_eq!(
            count_jobs(&apalis_pool, &check_positions_job_type()).await,
            1,
            "The scan must reschedule itself despite the outage"
        );
    }

    #[tokio::test]
    async fn enqueues_one_hedge_per_ready_symbol() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL", "TSLA"]);
        let (ctx, position) = build_ctx(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
        )
        .await;

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

        CheckPositions.perform(&ctx).await.unwrap();

        assert_eq!(count_jobs(&apalis_pool, &hedge_job_type()).await, 2);
    }

    #[tokio::test]
    async fn no_positions_above_threshold_enqueues_no_hedge_jobs() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"]);
        let (ctx, position) = build_ctx(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
        )
        .await;

        let aapl = Symbol::new("AAPL").unwrap();

        accumulate_position(
            &position,
            &aapl,
            FractionalShares::new(float!(0.1)),
            Direction::Buy,
        )
        .await;

        CheckPositions.perform(&ctx).await.unwrap();

        assert_eq!(count_jobs(&apalis_pool, &hedge_job_type()).await, 0);
    }

    #[tokio::test]
    async fn reschedules_itself_with_configured_interval() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"]);
        let interval = Duration::from_secs(42);
        let (ctx, _position) = build_ctx(pool.clone(), apalis_pool.clone(), cfg, interval).await;

        CheckPositions.perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &check_positions_job_type()).await,
            1
        );

        let run_at: i64 =
            sqlx_apalis::query_scalar("SELECT run_at FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(check_positions_job_type())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();

        let now = chrono::Utc::now().timestamp();
        let expected = now + i64::try_from(interval.as_secs()).unwrap();
        assert!(
            (run_at - expected).abs() <= 2,
            "expected run_at near {expected}, got {run_at}"
        );
    }

    #[tokio::test]
    async fn skips_trading_disabled_symbols_without_blocking_others() {
        let (pool, apalis_pool) = setup_test_pools().await;
        // RKLB is intentionally absent from the trading config -- the scan
        // must skip it without aborting the rest of the loop.
        let cfg = dry_run_ctx(&["AAPL", "TSLA"]);
        let (ctx, position) = build_ctx(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
        )
        .await;

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

        CheckPositions.perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            2,
            "AAPL and TSLA should produce hedges; RKLB (untraded) is skipped"
        );
    }

    #[tokio::test]
    async fn purge_removes_pending_running_and_retryable_failed_but_keeps_terminal() {
        let (_pool, apalis_pool) = setup_test_pools().await;

        let job_type = check_positions_job_type();

        async fn insert(
            apalis_pool: &apalis_sqlite::SqlitePool,
            job_type: &str,
            id: &str,
            status: &str,
            attempts: i64,
        ) {
            sqlx_apalis::query(
                "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts) \
                 VALUES (?, ?, ?, ?, ?, 25)",
            )
            .bind("{}")
            .bind(id)
            .bind(job_type)
            .bind(status)
            .bind(attempts)
            .execute(apalis_pool)
            .await
            .unwrap();
        }

        insert(
            &apalis_pool,
            &job_type,
            "pending-1",
            &Status::Pending.to_string(),
            0,
        )
        .await;
        insert(
            &apalis_pool,
            &job_type,
            "running-1",
            &Status::Running.to_string(),
            0,
        )
        .await;
        insert(
            &apalis_pool,
            &job_type,
            "failed-retryable",
            &Status::Failed.to_string(),
            3,
        )
        .await;
        insert(
            &apalis_pool,
            &job_type,
            "failed-exhausted",
            &Status::Failed.to_string(),
            25,
        )
        .await;
        insert(
            &apalis_pool,
            &job_type,
            "done-1",
            &Status::Done.to_string(),
            1,
        )
        .await;
        insert(
            &apalis_pool,
            &job_type,
            "killed-1",
            &Status::Killed.to_string(),
            1,
        )
        .await;

        let deleted = purge_pending_check_positions_jobs(&apalis_pool)
            .await
            .unwrap();
        assert_eq!(deleted, 3);

        let remaining: Vec<String> =
            sqlx_apalis::query_scalar("SELECT id FROM Jobs WHERE job_type = ?")
                .bind(&job_type)
                .fetch_all(&apalis_pool)
                .await
                .unwrap();
        assert_eq!(remaining.len(), 3);
        assert!(remaining.contains(&"failed-exhausted".to_string()));
        assert!(remaining.contains(&"done-1".to_string()));
        assert!(remaining.contains(&"killed-1".to_string()));
    }
}
