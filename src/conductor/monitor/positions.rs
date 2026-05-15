//! Supervised position scanner that enqueues [`PlaceHedge`] jobs.
//!
//! [`PositionMonitor`] is a long-running [`SupervisedTask`] that
//! periodically scans all positions, filters by orchestration policy
//! (trading enabled, no equity transfers in progress), and enqueues
//! durable [`PlaceHedge`] jobs for any positions ready to hedge.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use sqlx::SqlitePool;
use task_supervisor::{SupervisedTask, TaskResult};
use tracing::{debug, error, info, trace, warn};

use st0x_event_sorcery::{Projection, Store};
use st0x_execution::{CounterTradePreflight, Executor, MarketOrder, MarketSession, Positive};

use crate::config::Ctx;
use crate::equity_redemption::symbols_with_active_transfers;
use crate::offchain::order::{
    CancellationReason, OffchainOrder, OffchainOrderCommand, OffchainOrderId,
};
use crate::onchain::accumulator::check_execution_readiness;
use crate::position::{Position, PositionCommand};
use crate::trading::offchain::hedge::{HedgeJobQueue, PlaceHedge};

/// What `maybe_cancel_extended_hours_orders` should do for one position
/// whose pending order matches one of the recoverable states.
#[derive(Debug, Clone, Copy)]
enum CancelAction {
    /// Order is live (Submitted or PartiallyFilled with extended_hours=true):
    /// cancel at the broker, then finalize the position (applying any
    /// partial fill via CompleteOffChainOrder or clearing pending via
    /// FailOffChainOrder).
    CancelThenFinalizePosition,
    /// Order is already terminally Cancelled or Failed but the position
    /// still references it: a previous scan succeeded broker-side but the
    /// position update didn't land. Retry position finalization only.
    FinalizePositionOnly,
}

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
    offchain_order_store: Arc<Store<OffchainOrder>>,
    position_store: Arc<Store<Position>>,
    /// Last observed market session, used to detect Extended -> Regular
    /// transitions for cancel-and-replace without thrashing the calendar
    /// API on every scan during regular hours. `None` on first scan.
    last_seen_session: Option<MarketSession>,
}

impl<Exec> PositionMonitor<Exec> {
    pub(crate) fn new(
        executor: Exec,
        position_projection: Arc<Projection<Position>>,
        hedge_queue: HedgeJobQueue,
        check_interval: Duration,
        ctx: Ctx,
        pool: SqlitePool,
        offchain_order_store: Arc<Store<OffchainOrder>>,
        position_store: Arc<Store<Position>>,
    ) -> Self {
        Self {
            executor,
            position_projection,
            hedge_queue,
            check_interval,
            ctx,
            pool,
            offchain_order_store,
            position_store,
            last_seen_session: None,
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
        // When extended hours is enabled and we've transitioned to regular
        // hours, cancel any pending limit orders so they can be replaced
        // with market orders on the next cycle.
        if self.ctx.extended_hours_counter_trading {
            self.maybe_cancel_extended_hours_orders().await;
        }

        let all_positions = self.position_projection.load_all().await?;
        let active_transfers = symbols_with_active_transfers(&self.pool).await?;

        let eligible: Vec<_> = all_positions
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

    /// Runs cancel-and-replace only on Extended -> Regular transitions, so
    /// we don't hit the calendar API and load all positions on every scan
    /// during the 6.5 hours of regular trading.
    async fn maybe_cancel_extended_hours_orders(&mut self) {
        let session = match self.executor.market_session().await {
            Ok(session) => session,
            Err(error) => {
                warn!("Failed to check market session for cancel-and-replace: {error}");
                return;
            }
        };

        // Only cancel on a fresh transition into Regular hours. Subsequent
        // scans within the same Regular window are no-ops. First scan after
        // startup (previous = None) does not trigger -- the startup recovery
        // sweep is responsible for orphaned extended-hours orders.
        //
        // Deliberately do NOT replace `last_seen_session` until after the
        // load_all succeeds: a transient DB failure during the very first
        // scan after market open would otherwise "consume" the transition
        // and silently skip cancel-and-replace for the rest of the session.
        let should_run = session == MarketSession::Regular
            && self.last_seen_session != Some(MarketSession::Regular);
        if !should_run {
            self.last_seen_session = Some(session);
            return;
        }

        let all_positions = match self.position_projection.load_all().await {
            Ok(positions) => positions,
            Err(error) => {
                warn!("Failed to load positions for cancel-and-replace: {error}");
                // Don't update last_seen_session -- retry on next scan.
                return;
            }
        };
        let mut pass_succeeded = true;

        for (symbol, position) in &all_positions {
            let Some(offchain_order_id) = position.pending_offchain_order_id else {
                continue;
            };

            let order = match self.offchain_order_store.load(&offchain_order_id).await {
                Ok(Some(order)) => order,
                Ok(None) => continue,
                Err(error) => {
                    warn!(%symbol, %offchain_order_id, %error, "Failed to load offchain order");
                    pass_succeeded = false;
                    continue;
                }
            };

            let action = match &order {
                OffchainOrder::Submitted {
                    is_extended_hours, ..
                }
                | OffchainOrder::PartiallyFilled {
                    is_extended_hours, ..
                } if *is_extended_hours => CancelAction::CancelThenFinalizePosition,
                // Order is already in a terminal Cancelled or Failed state
                // but the position still references it as pending -- this
                // means a previous scan succeeded at the broker side but
                // the position-side clear didn't land. Retry just the
                // position update so the symbol can resume hedging.
                OffchainOrder::Cancelled { .. } | OffchainOrder::Failed { .. } => {
                    CancelAction::FinalizePositionOnly
                }
                OffchainOrder::Cancelling { .. } => {
                    pass_succeeded = false;
                    continue;
                }
                OffchainOrder::Submitted { .. }
                | OffchainOrder::PartiallyFilled { .. }
                | OffchainOrder::Pending { .. }
                | OffchainOrder::Filled { .. } => continue,
            };

            if matches!(action, CancelAction::CancelThenFinalizePosition) {
                info!(
                    target: "hedge",
                    %symbol,
                    %offchain_order_id,
                    "Regular hours: cancelling extended-hours limit order for market-order replacement"
                );

                if let Err(error) = self
                    .offchain_order_store
                    .send(
                        &offchain_order_id,
                        OffchainOrderCommand::CancelOrder {
                            reason: CancellationReason::MarketOpenReplacement,
                        },
                    )
                    .await
                {
                    warn!(%symbol, %offchain_order_id, %error, "Failed to cancel extended-hours order");
                    pass_succeeded = false;
                    continue;
                }
            } else {
                info!(
                    target: "hedge",
                    %symbol,
                    %offchain_order_id,
                    "Position still references already-terminal order; retrying position finalization"
                );
            }

            // Re-load the order after the cancel attempt; it should now be
            // Cancelled (or Filled if the cancel short-circuited because
            // the broker filled it concurrently).
            let finalized = match self.offchain_order_store.load(&offchain_order_id).await {
                Ok(Some(order)) => order,
                Ok(None) => continue,
                Err(error) => {
                    warn!(%symbol, %offchain_order_id, %error, "Failed to re-load offchain order after cancel");
                    pass_succeeded = false;
                    continue;
                }
            };

            pass_succeeded &= self
                .finalize_position_for_terminal_order(symbol, offchain_order_id, &finalized)
                .await;
        }

        if pass_succeeded {
            self.last_seen_session = Some(session);
        }
    }

    /// After a successful cancel (or a recovery scan finding an already-
    /// terminal order), propagate the broker's actual fill quantity to the
    /// position aggregate so net is correctly debited. Otherwise a partial
    /// fill recorded on the offchain side is invisible to the position
    /// scanner and the next cycle re-hedges the same shares.
    async fn finalize_position_for_terminal_order(
        &self,
        symbol: &st0x_execution::Symbol,
        offchain_order_id: OffchainOrderId,
        order: &OffchainOrder,
    ) -> bool {
        // Try to apply any partial fill first.
        if let OffchainOrder::Cancelled {
            shares_filled,
            avg_price,
            direction,
            executor_order_id,
            ..
        } = order
            && let Ok(positive_filled) = Positive::new(*shares_filled)
        {
            let Some(avg_price) = avg_price else {
                warn!(
                    %symbol, %offchain_order_id,
                    "Cancelled order has partial fill without avg price; leaving position pending"
                );
                return false;
            };

            if let Err(error) = self
                .position_store
                .send(
                    symbol,
                    PositionCommand::CompleteOffChainOrder {
                        offchain_order_id,
                        shares_filled: positive_filled,
                        direction: *direction,
                        executor_order_id: executor_order_id.clone(),
                        price: *avg_price,
                        broker_timestamp: Utc::now(),
                    },
                )
                .await
            {
                warn!(
                    %symbol, %offchain_order_id, %error,
                    "Failed to apply partial-fill CompleteOffChainOrder after cancel"
                );
                return false;
            }
            return true;
        }

        match order {
            // Filled (short-circuit case): the broker fully filled the
            // order between our last poll and the cancel. Apply the full
            // fill to the position.
            OffchainOrder::Filled {
                shares,
                direction,
                executor_order_id,
                price,
                ..
            } => {
                if let Err(error) = self
                    .position_store
                    .send(
                        symbol,
                        PositionCommand::CompleteOffChainOrder {
                            offchain_order_id,
                            shares_filled: *shares,
                            direction: *direction,
                            executor_order_id: executor_order_id.clone(),
                            price: *price,
                            broker_timestamp: Utc::now(),
                        },
                    )
                    .await
                {
                    warn!(
                        %symbol, %offchain_order_id, %error,
                        "Failed to apply short-circuit fill after cancel"
                    );
                    return false;
                }
                true
            }
            OffchainOrder::Failed {
                shares_filled: Some(shares_filled),
                avg_price: Some(avg_price),
                direction,
                executor_order_id: Some(executor_order_id),
                ..
            } => {
                let command = Positive::new(*shares_filled).map_or_else(
                    |_| PositionCommand::FailOffChainOrder {
                        offchain_order_id,
                        error: "broker failed order before any fill".to_string(),
                    },
                    |positive_filled| PositionCommand::CompleteOffChainOrder {
                        offchain_order_id,
                        shares_filled: positive_filled,
                        direction: *direction,
                        executor_order_id: executor_order_id.clone(),
                        price: *avg_price,
                        broker_timestamp: Utc::now(),
                    },
                );

                if let Err(error) = self.position_store.send(symbol, command).await {
                    warn!(
                        %symbol, %offchain_order_id, %error,
                        "Failed to apply partial-fill CompleteOffChainOrder after broker failure"
                    );
                    return false;
                }
                true
            }
            // Cancelled with no partial fill, or Failed at the broker with
            // no recorded fill -- nothing to apply to net, just clear pending.
            OffchainOrder::Cancelled { .. } | OffchainOrder::Failed { .. } => {
                let error_message = match order {
                    OffchainOrder::Cancelled { reason, .. } => {
                        format!("Cancelled: {reason:?}")
                    }
                    OffchainOrder::Failed { error, .. } => error.clone(),
                    _ => unreachable!(),
                };
                if let Err(error) = self
                    .position_store
                    .send(
                        symbol,
                        PositionCommand::FailOffChainOrder {
                            offchain_order_id,
                            error: error_message,
                        },
                    )
                    .await
                {
                    warn!(
                        %symbol, %offchain_order_id, %error,
                        "Failed to clear position pending state after cancel"
                    );
                    return false;
                }
                true
            }
            // Live or pre-terminal states should not appear here -- we
            // just successfully cancelled or the scan retry was triggered.
            // Log and skip.
            OffchainOrder::Pending { .. }
            | OffchainOrder::Submitted { .. }
            | OffchainOrder::PartiallyFilled { .. }
            | OffchainOrder::Cancelling { .. } => {
                warn!(
                    %symbol, %offchain_order_id, state = ?order,
                    "Order in non-terminal state after cancel attempt; skipping position finalize"
                );
                false
            }
        }
    }

    async fn check_and_enqueue_symbol(&mut self, symbol: &st0x_execution::Symbol) {
        let Ok(Some(mut ready)) = check_execution_readiness(
            &self.executor,
            &self.position_projection,
            symbol,
            self.executor.to_supported_executor(),
            &self.ctx.assets,
            true,
            self.ctx.extended_hours_counter_trading,
        )
        .await
        .inspect_err(|error| error!(%symbol, %error, "Execution readiness check failed")) else {
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

        if let Err(error) = self.hedge_queue.push(job).await {
            error!(%ready.symbol, %error, "Failed to enqueue hedge job");
        }
    }

    /// Checks broker inventory before enqueueing a hedge job. Returns `true`
    /// if the order should proceed (possibly with reduced shares), `false` if
    /// it should be skipped entirely.
    async fn preflight_and_clamp_shares(
        &self,
        ready: &mut crate::onchain::accumulator::ExecutionCtx,
    ) -> bool {
        let order = MarketOrder {
            symbol: ready.symbol.clone(),
            shares: ready.shares,
            direction: ready.direction,
        };

        match self.executor.preflight_counter_trade(order).await {
            Ok(CounterTradePreflight::Allowed { reservation }) => {
                crate::conductor::clamp_shares_to_reservation(ready, reservation.as_ref());
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
}
