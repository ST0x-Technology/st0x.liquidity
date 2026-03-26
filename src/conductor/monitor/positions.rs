//! Supervised position scanner that enqueues [`PlaceHedge`] jobs.
//!
//! [`PositionMonitor`] is a long-running [`SupervisedTask`] that
//! periodically scans all positions, filters by orchestration policy
//! (trading enabled, no equity transfers in progress), and enqueues
//! durable [`PlaceHedge`] jobs for any positions ready to hedge.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use apalis::prelude::TaskSink;
use task_supervisor::{SupervisedTask, TaskResult};
use tracing::{debug, error, info, trace};

use st0x_event_sorcery::Projection;
use st0x_execution::{Executor, Symbol};

use crate::config::Ctx;
use crate::onchain::accumulator::check_execution_readiness;
use crate::position::Position;
use crate::trading::offchain::hedge::{HedgeJobQueue, PlaceHedge};

/// Periodically scans positions and enqueues hedge jobs for those
/// that exceed their execution threshold.
#[derive(Clone)]
pub(crate) struct PositionMonitor<Exec> {
    executor: Exec,
    position_projection: Arc<Projection<Position>>,
    hedge_queue: HedgeJobQueue,
    check_interval: Duration,
    ctx: Ctx,
    equity_transfers_in_progress: Arc<std::sync::RwLock<HashSet<Symbol>>>,
}

impl<Exec> PositionMonitor<Exec> {
    pub(crate) fn new(
        executor: Exec,
        position_projection: Arc<Projection<Position>>,
        hedge_queue: HedgeJobQueue,
        check_interval: Duration,
        ctx: Ctx,
        equity_transfers_in_progress: Arc<std::sync::RwLock<HashSet<Symbol>>>,
    ) -> Self {
        Self {
            executor,
            position_projection,
            hedge_queue,
            check_interval,
            ctx,
            equity_transfers_in_progress,
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
        let executor_type = self.executor.to_supported_executor();
        let all_positions = self.position_projection.load_all().await?;

        for (symbol, _position) in &all_positions {
            if !self.ctx.is_trading_enabled(symbol) {
                continue;
            }

            if self.is_transfer_in_progress(symbol) {
                info!(%symbol, "Skipping hedge: equity transfer in progress");
                continue;
            }

            let readiness = check_execution_readiness(
                &self.executor,
                &self.position_projection,
                symbol,
                executor_type,
                &self.ctx.assets,
                true,
            )
            .await?;

            let Some(ready) = readiness else {
                continue;
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
            };

            self.hedge_queue.push(job).await?;
        }

        Ok(())
    }

    fn is_transfer_in_progress(&self, symbol: &Symbol) -> bool {
        self.equity_transfers_in_progress
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(symbol)
    }
}
