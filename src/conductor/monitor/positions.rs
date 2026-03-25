//! Supervised position scanner that enqueues [`PlaceHedge`] jobs.
//!
//! [`PositionMonitor`] is a long-running [`SupervisedTask`] that
//! periodically scans all positions, filters by orchestration policy
//! (trading enabled, no equity transfers in progress), and enqueues
//! durable [`PlaceHedge`] jobs for any positions ready to hedge.

use std::sync::Arc;
use std::time::Duration;

use sqlx::SqlitePool;
use task_supervisor::{SupervisedTask, TaskResult};
use tracing::{debug, error, info, trace};

use st0x_event_sorcery::Projection;
use st0x_execution::Executor;

use crate::config::Ctx;
use crate::equity_redemption::symbols_with_active_transfers;
use crate::offchain_order::OffchainOrderId;
use crate::onchain::accumulator::check_execution_readiness;
use crate::position::Position;
use crate::trading::offchain::hedge::{HedgeJobQueue, PlaceHedge};

/// Periodically scans positions and enqueues hedge jobs for those
/// that exceed their execution threshold.
///
/// Transfer-in-progress checks query the event store directly via
/// [`symbols_with_active_transfers`], so the guard is durable across
/// restarts.
///
/// Duplicate prevention is handled at the aggregate level: the
/// Position aggregate rejects `PlaceOffChainOrder` when a pending
/// order already exists, and `PlaceHedge::perform` respects this
/// rejection by bailing out. This makes duplicate enqueues safe
/// -- multiple jobs for the same symbol are idempotent.
#[derive(Clone)]
pub(crate) struct PositionMonitor<Exec> {
    executor: Exec,
    position_projection: Arc<Projection<Position>>,
    hedge_queue: HedgeJobQueue,
    check_interval: Duration,
    ctx: Ctx,
    pool: SqlitePool,
}

impl<Exec> PositionMonitor<Exec> {
    pub(crate) fn new(
        executor: Exec,
        position_projection: Arc<Projection<Position>>,
        hedge_queue: HedgeJobQueue,
        check_interval: Duration,
        ctx: Ctx,
        pool: SqlitePool,
    ) -> Self {
        Self {
            executor,
            position_projection,
            hedge_queue,
            check_interval,
            ctx,
            pool,
        }
    }
}

impl<Exec> SupervisedTask for PositionMonitor<Exec>
where
    Exec: Executor + Clone + Send + Sync + 'static,
{
    async fn run(&mut self) -> TaskResult {
        info!("Position monitor started");

        let mut interval = tokio::time::interval(self.check_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            trace!("Scanning positions for hedge readiness");

            if let Err(error) = self.scan_and_enqueue().await {
                error!("Position scan failed: {error}");
            }
        }
    }
}

impl<Exec> PositionMonitor<Exec>
where
    Exec: Executor + Clone + Send + Sync + 'static,
{
    async fn scan_and_enqueue(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let all_positions = self.position_projection.load_all().await?;
        let active_transfers = symbols_with_active_transfers(&self.pool).await?;

        let eligible: Vec<_> = all_positions
            .iter()
            .filter(|(symbol, _)| self.ctx.is_trading_enabled(symbol))
            .filter(|(symbol, _)| {
                if active_transfers.contains(symbol) {
                    info!(%symbol, "Skipping hedge: equity transfer in progress");
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

    async fn check_and_enqueue_symbol(&mut self, symbol: &st0x_execution::Symbol) {
        let executor_type = self.executor.to_supported_executor();

        let readiness = match check_execution_readiness(
            &self.executor,
            &self.position_projection,
            symbol,
            executor_type,
            &self.ctx.assets,
            true,
        )
        .await
        {
            Ok(readiness) => readiness,
            Err(error) => {
                error!(%symbol, %error, "Execution readiness check failed");
                return;
            }
        };

        let Some(ready) = readiness else {
            return;
        };

        debug!(
            %ready.symbol, %ready.shares, ?ready.direction,
            "Enqueuing hedge job"
        );

        let job = PlaceHedge {
            symbol: ready.symbol,
            direction: ready.direction,
            shares: ready.shares,
            executor: ready.executor,
            threshold: self.ctx.execution_threshold,
            offchain_order_id: OffchainOrderId::new(),
        };

        if let Err(error) = self.hedge_queue.push(job).await {
            error!(%symbol, %error, "Failed to enqueue hedge job");
        }
    }
}
