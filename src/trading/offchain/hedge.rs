//! Durable hedge placement job.
//!
//! [`PlaceHedge`] is an apalis-backed [`Job`] that places an offsetting
//! broker order for an accumulated position. The position monitor enqueues
//! these; the apalis worker processes them with retry semantics.

use std::sync::Arc;

use alloy::primitives::U256;
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use st0x_float_macro::float;
use tokio::sync::Mutex;
use tracing::{info, warn};

use st0x_config::{AssetsConfig, ExecutionThreshold};
use st0x_event_sorcery::{AggregateError, LifecycleError, Store};
use st0x_execution::{
    Direction, FractionalShares, MarketSession, Positive, SupportedExecutor, Symbol, Usd,
};

use crate::conductor::job::{Job, JobQueue, Label};
use crate::offchain::order::{
    CounterTradeOrderKind, OffchainOrder, OffchainOrderId, OffchainOrderPlacement, OrderPlacer,
    PollOrderStatus, PollOrderStatusJobQueue, client_order_id_for_placement,
    finalize_cancelled_position_or_log_unpriced, place_offchain_order_at_broker,
};
use crate::position::{Position, PositionCommand, PositionError};
use crate::trading::onchain::trade_accountant::TradeAccountingError;

/// Error returned by [`apply_slippage`].
#[derive(Debug, thiserror::Error)]
pub(crate) enum SlippageError {
    #[error("float arithmetic failed: {0}")]
    FloatArith(#[from] rain_math_float::FloatError),
    #[error("slippage-adjusted price is non-positive (slippage_bps too large for sell)")]
    NonPositive(#[from] st0x_finance::NotPositive<Usd>),
}

/// Applies slippage buffer to a reference price: adds for buys, subtracts
/// for sells. Rounds the result to Alpaca's required precision (2 decimal
/// places for prices >= $1, 4 for prices < $1). Rounding direction
/// maximizes fill probability (ceiling for buys, floor for sells), which
/// can push the realized limit slightly beyond the configured slippage
/// budget by up to one tick.
fn apply_slippage(
    price: Usd,
    direction: Direction,
    slippage_bps: u16,
) -> Result<Positive<Usd>, SlippageError> {
    let price = Float::from(price);
    let basis_points = float!(10000);
    let slippage = Float::from_fixed_decimal(U256::from(slippage_bps), 0)?;

    let adjusted = match direction {
        Direction::Buy => {
            let multiplier = ((basis_points + slippage)? / basis_points)?;
            (price * multiplier)?
        }
        Direction::Sell => {
            let multiplier = ((basis_points - slippage)? / basis_points)?;
            (price * multiplier)?
        }
    };

    // Precision is keyed off the *adjusted* (limit) price, not the reference
    // price, on purpose: SEC Rule 612 / Alpaca's minimum price variance is a
    // rule about the ORDER's price -- orders priced >= $1.00 must be in $0.01
    // increments, orders < $1.00 may use $0.0001. A sub-$1 reference price that
    // slips to >= $1.00 must therefore round to pennies, or the broker rejects
    // the sub-penny limit. Keying off the reference price would emit invalid
    // orders at the $1 boundary.
    let max_decimals: u8 = if adjusted.lt(float!(1))? { 4 } else { 2 };
    let (fixed, lossless) = adjusted.to_fixed_decimal_lossy(max_decimals)?;

    let rounded = if lossless {
        adjusted
    } else {
        // Buys: round up (ceiling) to ensure fill
        // Sells: round down (floor/truncate) to ensure fill
        let rounded_fixed = match direction {
            Direction::Buy => fixed + U256::from(1),
            Direction::Sell => fixed,
        };
        Float::from_fixed_decimal(rounded_fixed, max_decimals)?
    };

    Ok(Positive::new(Usd::new(rounded))?)
}

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
    /// Per-symbol asset config. Gates the extended-hours limit path: only a
    /// symbol with `extended_hours_counter_trading = enabled` may place a limit
    /// order during an Extended session. A disabled symbol skips (the
    /// regular-open cancel-and-replace sweep is keyed off the same per-symbol
    /// flag, so an extended order for a disabled symbol would be orphaned).
    pub(crate) assets: AssetsConfig,
    pub(crate) counter_trade_slippage_bps: u16,
    /// Serialises broker submissions across hedge jobs and the inline
    /// counter-trade path in `conductor.rs`, so a preflight running under
    /// this same lock (the inline path's) observes any prior submission
    /// rather than racing it. It does NOT re-check buying power for hedge
    /// jobs themselves: their preflight ran at enqueue time, so two jobs
    /// enqueued in the same scan window can still collectively exceed the
    /// budget snapshot they were preflighted against. Broker-side rejection
    /// is the backstop for that gap -- the rejected order lands as `Failed`
    /// and releases the position for a later re-hedge.
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
    /// Enqueue-time session for staleness diagnostics; perform re-fetches the
    /// live session before selecting the broker order kind.
    #[serde(default = "default_market_session")]
    pub(crate) market_session: MarketSession,
}

fn default_market_session() -> MarketSession {
    MarketSession::Regular
}

async fn select_order_kind_for_current_session(
    ctx: &HedgeCtx,
    symbol: &Symbol,
    direction: Direction,
    enqueued_session: MarketSession,
) -> Result<Option<CounterTradeOrderKind>, TradeAccountingError> {
    let current_session = ctx.order_placer.market_session().await.map_err(|source| {
        TradeAccountingError::MarketSessionCheck {
            symbol: symbol.clone(),
            source,
        }
    })?;

    if current_session != enqueued_session {
        info!(
            target: "hedge",
            %symbol,
            ?enqueued_session,
            ?current_session,
            "Market session changed between enqueue and perform; using current"
        );
    }

    match current_session {
        MarketSession::Regular => Ok(Some(CounterTradeOrderKind::Market)),
        MarketSession::Closed => {
            info!(
                target: "hedge",
                %symbol,
                "Market closed at perform time; skipping hedge, CheckPositions will re-enqueue when the venue reopens"
            );
            Ok(None)
        }
        MarketSession::Extended => {
            if !ctx.assets.is_extended_hours_enabled(symbol) {
                info!(
                    target: "hedge",
                    %symbol,
                    "Extended session but symbol is not enabled for extended-hours \
                     counter-trading; skipping, CheckPositions will re-enqueue during \
                     regular hours"
                );
                return Ok(None);
            }

            let latest_price = ctx
                .order_placer
                .fetch_latest_trade_price(symbol)
                .await
                .map_err(|source| TradeAccountingError::LimitPriceFetch {
                    symbol: symbol.clone(),
                    source,
                })?
                .ok_or_else(|| TradeAccountingError::LimitPriceUnavailable {
                    symbol: symbol.clone(),
                })?;

            let limit_price = apply_slippage(
                latest_price.inner(),
                direction,
                ctx.counter_trade_slippage_bps,
            )?;

            info!(
                target: "hedge",
                %symbol,
                %limit_price,
                direction = ?direction,
                "Extended hours: placing limit order"
            );

            Ok(Some(CounterTradeOrderKind::ExtendedHoursLimit {
                limit_price,
            }))
        }
    }
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
        Some(Pending {
            symbol,
            shares,
            direction,
            executor,
            market_session,
            ..
        }) => {
            let Some(order_kind) =
                select_order_kind_for_current_session(ctx, &symbol, direction, market_session)
                    .await?
            else {
                return Ok(());
            };

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
                OffchainOrderPlacement::with_kind(
                    symbol.clone(),
                    shares,
                    direction,
                    executor,
                    client_order_id,
                    order_kind,
                ),
            )
            .await?;

            route_placement_outcome(ctx, &symbol, pending_id, placed).await
        }
        // The position still references a pending order that is already
        // terminal (Cancelled/Failed) -- a stale apalis retry re-claimed the
        // position before the finalize sweep released it. Do NOT stay silent:
        // surface the stuck reference so it is visible to operators. The
        // CheckPositions `finalize_terminal_pending_positions` sweep releases
        // the position on its next tick, so no inline finalization is needed
        // here (and inline finalization would race that sweep).
        Some(terminal @ (Failed { .. } | Cancelled { .. })) => {
            warn!(
                target: "hedge",
                %pending_id,
                state = ?terminal,
                "Position references a pending offchain order that is already \
                 terminal; the CheckPositions finalize sweep will release the \
                 position on its next tick"
            );
            Ok(())
        }
        Some(Filled { .. }) | None => Ok(()),
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
    use OffchainOrder::{
        Cancelled, Cancelling, Failed, Filled, PartiallyFilled, Pending, Submitted,
    };
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

        Some(Submitted { .. } | PartiallyFilled { .. } | Cancelling { .. }) => {
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

        Some(cancelled @ Cancelled { .. }) => {
            finalize_cancelled_position_or_log_unpriced(
                ctx.position.as_ref(),
                symbol,
                offchain_order_id,
                &cancelled,
            )
            .await?;
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
        // Residual TOCTOU: the session read, the limit-price fetch, and the
        // broker submission are three separate awaits, so the venue clock can
        // cross a 9:30/16:00 boundary between them. This is inherent (the clock
        // is external -- acquiring the submission lock earlier wouldn't close
        // it, only serialise the price fetch). It is bounded and self-healing:
        // a boundary-straddling order is either rejected by the broker (and
        // retried, re-reading the session) or, if it lands as an extended-hours
        // limit during regular hours, converged by the CheckPositions
        // cancel-and-replace pass -- that pass is level-triggered (it sweeps
        // every regular-hours tick, not just the transition tick), so an order
        // submitted after the first regular-hours scan is still cancelled on
        // the next one. The order kind is computed before the position is
        // claimed, so a rejection never strands the position.
        //
        // Re-check the market session at execution time, ALWAYS -- independent
        // of whether any asset enables extended-hours trading. The placer wraps
        // the executor and is always present, so this recheck is no longer
        // gated by the extended-hours feature flag. The enqueue-time value
        // (self.market_session) can be stale by minutes if the job sat in
        // apalis across a 9:30 or 16:00 ET boundary: a regular job that crossed
        // the close must not blindly submit a market order into a closed or
        // extended venue using its stale serialized session.
        let Some(order_kind) = select_order_kind_for_current_session(
            ctx,
            &self.symbol,
            self.direction,
            self.market_session,
        )
        .await?
        else {
            // Not a plain Ok(()): a retry whose first attempt submitted the
            // order but lost the poll enqueue must re-enqueue it here, or
            // the live order sits un-polled (and its fill unrecorded) until
            // the next restart.
            return recover_pending_poll_status(ctx, self.offchain_order_id).await;
        };

        // Serialize every broker placement (ADR 0014): the trade-processing path
        // holds this same lock across its placement, so the position claim and
        // broker side effect cannot interleave with a recovery re-drive or inline
        // counter-trade placement.
        let _submission_guard = ctx.counter_trade_submission_lock.lock().await;

        // Only specific business rejections are safe to swallow:
        // - PendingExecution: another attempt already claimed this position
        //   -- usually idempotent, but if that attempt got the broker submitted
        //   *without* enqueueing PollOrderStatus (e.g. the queue push failed
        //   and apalis is now retrying us), we must re-enqueue the poll here
        //   or the order sits in Submitted until the next bot restart.
        // - ThresholdNotMet: position moved below threshold since the monitor
        //   scanned -- stale job, no action needed.
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
            OffchainOrderPlacement::with_kind(
                self.symbol.clone(),
                self.shares,
                self.direction,
                self.executor,
                client_order_id,
                order_kind,
            ),
        )
        .await?;

        route_placement_outcome(ctx, &self.symbol, self.offchain_order_id, placed).await
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, TxHash};
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
    use st0x_config::{EquitiesConfig, EquityAssetConfig, ExecutionThreshold, OperationMode};

    /// Builds an [`AssetsConfig`] with a single equity whose extended-hours
    /// counter-trading flag is set as given. Used to drive the per-symbol
    /// extended-hours gate in `PlaceHedge::perform`.
    fn extended_hours_assets(symbol: &str, enabled: bool) -> AssetsConfig {
        let extended_hours_counter_trading = if enabled {
            OperationMode::Enabled
        } else {
            OperationMode::Disabled
        };

        AssetsConfig {
            equities: EquitiesConfig {
                operational_limit: None,
                symbols: std::iter::once((
                    Symbol::new(symbol).unwrap(),
                    EquityAssetConfig {
                        tokenized_equity: Address::ZERO,
                        tokenized_equity_derivative: Address::ZERO,
                        pyth_feed_id: None,
                        vault_ids: Vec::new(),
                        trading: OperationMode::Disabled,
                        rebalancing: OperationMode::Disabled,
                        wrapped_equity_recovery: OperationMode::Disabled,
                        extended_hours_counter_trading,
                        operational_limit: None,
                    },
                ))
                .collect(),
            },
            cash: None,
        }
    }

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
                .build(order_placer.clone())
                .await
                .unwrap();

        let ctx = HedgeCtx {
            position: position.clone(),
            offchain_order,
            poll_status_queue: PollOrderStatusJobQueue::new(&apalis_pool),
            // The placer doubles as the session source; the default stubs
            // report a Regular session, so these ctxs exercise the regular
            // market-order path. AAPL is enabled for extended hours so the
            // Regular path's session gate is not what skips them.
            order_placer,
            assets: extended_hours_assets("AAPL", true),
            counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
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
            market_session: MarketSession::Regular,
        }
    }

    #[test]
    fn legacy_place_hedge_payload_defaults_market_session_to_regular() {
        let symbol = Symbol::new("AAPL").unwrap();
        let offchain_order_id = OffchainOrderId::new();
        let payload = serde_json::json!({
            "symbol": symbol,
            "direction": Direction::Sell,
            "shares": Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
            "executor": SupportedExecutor::DryRun,
            "threshold": ExecutionThreshold::whole_share(),
            "offchain_order_id": offchain_order_id,
        });

        let job: PlaceHedge = serde_json::from_value(payload).unwrap();

        assert_eq!(
            job.market_session,
            MarketSession::Regular,
            "legacy PlaceHedge jobs without market_session must deserialize as Regular"
        );
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
            market_session: MarketSession::Regular,
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
    fn extended_hours_order_placer(price: rain_math_float::Float) -> Arc<dyn OrderPlacer> {
        struct ExtHoursPlacer(rain_math_float::Float);

        #[async_trait::async_trait]
        impl OrderPlacer for ExtHoursPlacer {
            async fn place_market_order(
                &self,
                order: st0x_execution::MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("market-order-1"),
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
                    executor_order_id: ExecutorOrderId::new("limit-order-1"),
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

            async fn fetch_latest_trade_price(
                &self,
                _symbol: &Symbol,
            ) -> Result<
                Option<st0x_execution::Positive<Usd>>,
                Box<dyn std::error::Error + Send + Sync>,
            > {
                Ok(Some(st0x_execution::Positive::new(Usd::new(self.0))?))
            }

            async fn market_session(
                &self,
            ) -> Result<MarketSession, Box<dyn std::error::Error + Send + Sync>> {
                Ok(MarketSession::Extended)
            }
        }

        Arc::new(ExtHoursPlacer(price))
    }

    async fn create_extended_hours_ctx(price: rain_math_float::Float) -> TestInfra {
        let placer = extended_hours_order_placer(price);

        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;

        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (offchain_order, offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(placer.clone())
                .await
                .unwrap();

        let ctx = HedgeCtx {
            position: position.clone(),
            offchain_order,
            poll_status_queue: PollOrderStatusJobQueue::new(&apalis_pool),
            order_placer: placer,
            assets: extended_hours_assets("AAPL", true),
            counter_trade_slippage_bps: 100,
            counter_trade_submission_lock: Arc::new(tokio::sync::Mutex::new(())),
        };

        TestInfra {
            ctx,
            apalis_pool,
            position_projection,
            offchain_order_projection,
        }
    }

    #[test]
    fn apply_slippage_buy_adds_to_price() {
        let price = Usd::new(float!(150.0));
        let result = apply_slippage(price, Direction::Buy, 100).unwrap();
        // 150 * 1.01 = 151.50, already 2 decimal places, no rounding needed
        assert!(
            result.inner().inner().eq(float!(151.50)).unwrap(),
            "Buy slippage should increase the price, got: {result}"
        );
    }

    #[test]
    fn apply_slippage_sell_subtracts_from_price() {
        let price = Usd::new(float!(150.0));
        let result = apply_slippage(price, Direction::Sell, 100).unwrap();
        // 150 * 0.99 = 148.50
        assert!(
            result.inner().inner().eq(float!(148.50)).unwrap(),
            "Sell slippage should decrease the price, got: {result}"
        );
    }

    #[test]
    fn apply_slippage_rounds_buy_up_to_two_decimals() {
        // 151.23 * 1.01 = 152.7423 -> rounds UP to 152.75 for a buy
        let price = Usd::new(float!(151.23));
        let result = apply_slippage(price, Direction::Buy, 100).unwrap();
        assert!(
            result.inner().inner().eq(float!(152.75)).unwrap(),
            "Buy should round up to the nearest cent, got: {result}"
        );
    }

    #[test]
    fn apply_slippage_rounds_sell_down_to_two_decimals() {
        // 151.23 * 0.99 = 149.7177 -> truncates to 149.71 for a sell
        let price = Usd::new(float!(151.23));
        let result = apply_slippage(price, Direction::Sell, 100).unwrap();
        assert!(
            result.inner().inner().eq(float!(149.71)).unwrap(),
            "Sell should round down to the nearest cent, got: {result}"
        );
    }

    #[test]
    fn apply_slippage_zero_bps_is_identity() {
        let price = Usd::new(float!(100.0));
        let buy_result = apply_slippage(price, Direction::Buy, 0).unwrap();
        let sell_result = apply_slippage(price, Direction::Sell, 0).unwrap();
        assert_eq!(buy_result.inner(), price);
        assert_eq!(sell_result.inner(), price);
    }

    #[test]
    fn apply_slippage_zero_bps_still_rounds_unclean_price() {
        // At 0 bps the price is unchanged, but the result is still rounded to the
        // min price variance: buy ceils, sell floors. The clean-$100 identity
        // test never exercises this branch (100.00 is already 2-decimal).
        let price = Usd::new(float!(100.001));
        let buy = apply_slippage(price, Direction::Buy, 0).unwrap();
        assert!(
            buy.inner().inner().eq(float!(100.01)).unwrap(),
            "0-bps buy must still ceil an unclean price to cents, got: {buy}"
        );
        let sell = apply_slippage(price, Direction::Sell, 0).unwrap();
        assert!(
            sell.inner().inner().eq(float!(100.0)).unwrap(),
            "0-bps sell must still floor an unclean price to cents, got: {sell}"
        );
    }

    #[test]
    fn apply_slippage_sub_dollar_uses_four_decimals() {
        // 0.5000 * 1.01 = 0.5050 - 4-decimal precision branch
        let result = apply_slippage(Usd::new(float!(0.5)), Direction::Buy, 100).unwrap();
        assert!(
            result.inner().inner().eq(float!(0.5050)).unwrap(),
            "Sub-$1 buy should round to 4 decimals (0.5050), got: {result}"
        );

        let sell = apply_slippage(Usd::new(float!(0.5)), Direction::Sell, 100).unwrap();
        assert!(
            sell.inner().inner().eq(float!(0.4950)).unwrap(),
            "Sub-$1 sell should round to 4 decimals (0.4950), got: {sell}"
        );
    }

    #[test]
    fn apply_slippage_sub_dollar_reference_crossing_one_dollar_rounds_to_pennies() {
        // 0.99 * 1.02 = 1.0098: the reference is sub-$1 but the ADJUSTED
        // (limit) price crosses $1.00, so Rule 612 requires penny precision.
        // The buy must ceil to $1.01 -- a regression that keys precision off
        // the reference price would emit a sub-penny $1.0098 limit and the
        // broker would reject the order.
        let result = apply_slippage(Usd::new(float!(0.99)), Direction::Buy, 200).unwrap();
        assert!(
            result.inner().inner().eq(float!(1.01)).unwrap(),
            "adjusted price crossing $1.00 must round to pennies, got: {result}"
        );
    }

    #[test]
    fn apply_slippage_just_below_one_dollar_keeps_four_decimals() {
        // 0.99 * 1.0033 = 0.993267 stays below $1.00, so sub-penny (4-decimal)
        // precision applies: ceil to 0.9933. A regression that always rounded
        // to pennies would ceil this to $1.00 instead.
        let result = apply_slippage(Usd::new(float!(0.99)), Direction::Buy, 33).unwrap();
        assert!(
            result.inner().inner().eq(float!(0.9933)).unwrap(),
            "adjusted price below $1.00 must keep 4-decimal precision, got: {result}"
        );
    }

    #[test]
    fn apply_slippage_at_exactly_one_dollar_uses_two_decimals() {
        // Reference exactly $1.00 with 13 bps buy slippage: 1.00 * 1.0013 =
        // 1.0013, which is >= $1.00 and must round to pennies ($1.01), not to
        // four decimals ($1.0013).
        let result = apply_slippage(Usd::new(float!(1.0)), Direction::Buy, 13).unwrap();
        assert!(
            result.inner().inner().eq(float!(1.01)).unwrap(),
            "price at the $1.00 boundary must use penny precision, got: {result}"
        );
    }

    #[test]
    fn apply_slippage_max_bps_sell_succeeds_at_one_cent() {
        // 9999 bps slippage on a sell: 100 * 0.0001 = 0.01, still positive.
        // Config validation caps counter_trade_slippage_bps at 9_999
        // (loader's MAX_COUNTER_TRADE_SLIPPAGE_BPS); apply_slippage itself
        // accepts any u16. This guards against future bound regressions:
        // 9999 must succeed for prices >= $1.
        let result = apply_slippage(Usd::new(float!(100.0)), Direction::Sell, 9999).unwrap();
        assert!(
            result.inner().inner().eq(float!(0.01)).unwrap(),
            "Max-bps sell should produce 1 cent, got: {result}"
        );
    }

    #[test]
    fn apply_slippage_max_bps_sub_dollar_sell_errors_non_positive() {
        // A sub-dollar reference at max slippage floors below the minimum
        // tick: 0.50 * 0.0001 = 0.00005, floored to 0.0000 at sub-dollar
        // precision. Producing a zero limit must surface as an explicit
        // error -- never a zero-priced order at the broker.
        let error = apply_slippage(Usd::new(float!(0.50)), Direction::Sell, 9999).unwrap_err();
        assert!(
            matches!(error, SlippageError::NonPositive(_)),
            "expected NonPositive for a zeroed sub-dollar sell limit, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn extended_hours_places_limit_order() {
        let TestInfra {
            ctx,
            position_projection,
            offchain_order_projection,
            ..
        } = create_extended_hours_ctx(float!(150.0)).await;
        let symbol = Symbol::new("AAPL").unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let job = PlaceHedge {
            symbol: symbol.clone(),
            direction: Direction::Sell,
            shares: Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
            executor: SupportedExecutor::DryRun,
            threshold: ExecutionThreshold::whole_share(),
            offchain_order_id: OffchainOrderId::new(),
            market_session: MarketSession::Extended,
        };

        job.perform(&ctx).await.unwrap();

        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id,
            Some(job.offchain_order_id),
            "Position should store the hedge job's offchain order ID"
        );

        let order = offchain_order_projection
            .load(&job.offchain_order_id)
            .await
            .unwrap()
            .expect("offchain order should exist");
        assert!(
            matches!(
                order,
                OffchainOrder::Submitted {
                    market_session: MarketSession::Extended,
                    ..
                }
            ),
            "Order should be submitted as extended-hours, got: {order:?}"
        );
        // The exact limit price computation is covered by the dedicated
        // `apply_slippage_*` unit tests; this integration test only checks
        // the lifecycle path.
    }

    /// Failure mode (some asset enabled): a stale Regular hedge job for a symbol
    /// whose extended-hours flag is DISABLED arrives during an Extended session.
    /// The decoupled per-symbol gate must skip it -- placing an extended-hours
    /// limit would be orphaned, since the regular-open cancel-and-replace sweep
    /// is keyed off the same per-symbol flag and skips disabled symbols. The
    /// position must be left unclaimed and no order recorded.
    #[tokio::test]
    async fn extended_session_for_disabled_symbol_skips_without_placing_order() {
        let TestInfra {
            ctx,
            position_projection,
            offchain_order_projection,
            ..
        } = create_hedge_ctx_with(
            extended_hours_order_placer(float!(150.0)),
            extended_hours_assets("AAPL", false),
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let job = PlaceHedge {
            symbol: symbol.clone(),
            direction: Direction::Sell,
            shares: Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
            executor: SupportedExecutor::DryRun,
            threshold: ExecutionThreshold::whole_share(),
            offchain_order_id: OffchainOrderId::new(),
            // Stale: enqueued during regular hours, retried during Extended.
            market_session: MarketSession::Regular,
        };

        job.perform(&ctx)
            .await
            .expect("a disabled-symbol extended job must skip cleanly, not error");

        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id, None,
            "A symbol not enabled for extended hours must not be claimed during an Extended session"
        );

        let order = offchain_order_projection
            .load(&job.offchain_order_id)
            .await
            .unwrap();
        assert_eq!(
            order, None,
            "No order may be placed for a disabled symbol during extended hours, got: {order:?}"
        );
    }

    /// Failure mode (all assets disabled): the perform-time session recheck must
    /// run UNCONDITIONALLY, not only when extended-hours is enabled. A stale
    /// Regular job that crossed the close boundary must re-read the live session
    /// (Closed here) and skip -- never submit a market order into a closed venue
    /// off its stale serialized Regular session. Before the decoupling this job
    /// would keep its stale `Regular` and submit a market order.
    #[tokio::test]
    async fn regular_job_rechecks_live_session_and_skips_when_venue_closed() {
        let TestInfra {
            ctx,
            position_projection,
            offchain_order_projection,
            ..
        } = create_hedge_ctx_with(
            market_session_overriding_placer(MarketSession::Closed),
            extended_hours_assets("AAPL", false),
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let job = PlaceHedge {
            symbol: symbol.clone(),
            direction: Direction::Sell,
            shares: Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
            executor: SupportedExecutor::DryRun,
            threshold: ExecutionThreshold::whole_share(),
            offchain_order_id: OffchainOrderId::new(),
            // Stale serialized session: enqueued during regular hours.
            market_session: MarketSession::Regular,
        };

        job.perform(&ctx)
            .await
            .expect("a stale Regular job must skip when the live venue is closed");

        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id, None,
            "A closed-venue recheck must not claim the position off a stale Regular session"
        );

        let order = offchain_order_projection
            .load(&job.offchain_order_id)
            .await
            .unwrap();
        assert_eq!(
            order, None,
            "No market order may be submitted into a closed venue, got: {order:?}"
        );
    }

    /// `OrderPlacer` that reports an Extended session but fails the
    /// latest-trade-price lookup -- simulates the market-data endpoint being
    /// down during pre-market. Placement methods error because the job must
    /// never reach them on this path.
    fn price_fetch_failing_placer() -> Arc<dyn OrderPlacer> {
        struct FailingPricePlacer;

        #[async_trait::async_trait]
        impl OrderPlacer for FailingPricePlacer {
            async fn place_market_order(
                &self,
                _order: st0x_execution::MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("place_market_order must not be called when the price fetch fails".into())
            }

            async fn place_limit_order(
                &self,
                _order: st0x_execution::LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("place_limit_order must not be called when the price fetch fails".into())
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &st0x_execution::ExecutorOrderId,
            ) -> Result<st0x_execution::CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(st0x_execution::CancellationOutcome::Requested)
            }

            async fn fetch_latest_trade_price(
                &self,
                _symbol: &Symbol,
            ) -> Result<
                Option<st0x_execution::Positive<Usd>>,
                Box<dyn std::error::Error + Send + Sync>,
            > {
                Err("market data endpoint down".into())
            }

            async fn market_session(
                &self,
            ) -> Result<MarketSession, Box<dyn std::error::Error + Send + Sync>> {
                Ok(MarketSession::Extended)
            }
        }

        Arc::new(FailingPricePlacer)
    }

    #[tokio::test]
    async fn price_fetch_failure_during_extended_session_does_not_claim_position() {
        // order_placer is Some but the latest-trade-price lookup fails (e.g.
        // market data endpoint down during pre-market). The error must surface
        // BEFORE the position is claimed: a regression that claims the
        // position first would leave a dangling pending_offchain_order_id with
        // no actual order, silently blocking all future hedging of the symbol.
        let TestInfra {
            ctx,
            position_projection,
            offchain_order_projection,
            ..
        } = create_hedge_ctx_with(
            price_fetch_failing_placer(),
            extended_hours_assets("AAPL", true),
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let job = PlaceHedge {
            symbol: symbol.clone(),
            direction: Direction::Sell,
            shares: Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
            executor: SupportedExecutor::DryRun,
            threshold: ExecutionThreshold::whole_share(),
            offchain_order_id: OffchainOrderId::new(),
            market_session: MarketSession::Extended,
        };

        // The job fails with a retryable error (it propagates, so apalis
        // re-runs it) rather than being swallowed.
        let result = job.perform(&ctx).await;
        assert!(
            matches!(result, Err(TradeAccountingError::LimitPriceFetch { .. })),
            "expected LimitPriceFetch, got: {result:?}"
        );

        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id, None,
            "price fetch failure must not claim the position"
        );

        let order = offchain_order_projection
            .load(&job.offchain_order_id)
            .await
            .unwrap();
        assert!(
            order.is_none(),
            "no offchain order may be recorded when the price fetch fails, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn market_session_failure_surfaces_dedicated_error_without_claiming_position() {
        // The session re-check at the top of perform fails (broker calendar /
        // clock endpoint down). The error must be MarketSessionCheck -- not
        // LimitPriceFetch, which would point operators at the market-data
        // endpoint -- and the position must remain unclaimed so the retry can
        // start clean.
        struct SessionFailingPlacer;

        #[async_trait::async_trait]
        impl OrderPlacer for SessionFailingPlacer {
            async fn place_market_order(
                &self,
                _order: st0x_execution::MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("place_market_order must not be called when the session check fails".into())
            }

            async fn place_limit_order(
                &self,
                _order: st0x_execution::LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("place_limit_order must not be called when the session check fails".into())
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &st0x_execution::ExecutorOrderId,
            ) -> Result<st0x_execution::CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(st0x_execution::CancellationOutcome::Requested)
            }

            async fn market_session(
                &self,
            ) -> Result<MarketSession, Box<dyn std::error::Error + Send + Sync>> {
                Err("broker calendar endpoint down".into())
            }
        }

        let TestInfra {
            ctx,
            position_projection,
            ..
        } = create_hedge_ctx_with(
            Arc::new(SessionFailingPlacer),
            extended_hours_assets("AAPL", true),
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let job = hedge_job(&symbol, 2.0, Direction::Sell);
        let result = job.perform(&ctx).await;
        assert!(
            matches!(result, Err(TradeAccountingError::MarketSessionCheck { .. })),
            "expected MarketSessionCheck, got: {result:?}"
        );

        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id, None,
            "session-check failure must not claim the position"
        );
    }

    #[tokio::test]
    async fn perform_skips_without_claiming_when_session_changes_to_closed() {
        // Job was enqueued in Extended hours, but by the time perform runs
        // the market has closed. perform must NOT submit and must NOT model
        // this as a job error (apalis backoff can't span a multi-hour
        // closure). It returns Ok and leaves the position unclaimed so the
        // next CheckPositions scan re-enqueues when the venue reopens.
        let placer = market_session_overriding_placer(MarketSession::Closed);
        let TestInfra {
            ctx,
            position_projection,
            ..
        } = create_hedge_ctx_with(placer, extended_hours_assets("AAPL", true)).await;
        let symbol = Symbol::new("AAPL").unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let job = PlaceHedge {
            symbol: symbol.clone(),
            direction: Direction::Sell,
            shares: Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
            executor: SupportedExecutor::DryRun,
            threshold: ExecutionThreshold::whole_share(),
            offchain_order_id: OffchainOrderId::new(),
            market_session: MarketSession::Extended,
        };

        job.perform(&ctx)
            .await
            .expect("perform must succeed (skip), not error, when the market is closed");

        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id, None,
            "Position must not be claimed when perform skips a closed-market hedge"
        );
    }

    #[tokio::test]
    async fn perform_uses_current_session_when_enqueued_session_is_stale() {
        // Job was enqueued during Extended hours but Regular has begun by
        // the time perform runs -- it must submit a market order, not a
        // limit order with extended_hours=true.
        let placer = market_session_overriding_placer(MarketSession::Regular);
        let TestInfra {
            ctx,
            offchain_order_projection,
            ..
        } = create_hedge_ctx_with(placer, extended_hours_assets("AAPL", true)).await;
        let symbol = Symbol::new("AAPL").unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let job = PlaceHedge {
            symbol: symbol.clone(),
            direction: Direction::Sell,
            shares: Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
            executor: SupportedExecutor::DryRun,
            threshold: ExecutionThreshold::whole_share(),
            offchain_order_id: OffchainOrderId::new(),
            market_session: MarketSession::Extended,
        };

        job.perform(&ctx).await.unwrap();

        let order = offchain_order_projection
            .load(&job.offchain_order_id)
            .await
            .unwrap()
            .expect("offchain order should exist");

        // Critical: the order was placed as a *market* order even though the
        // job was enqueued during extended hours, because perform re-checked
        // the session and found Regular.
        assert!(
            matches!(
                order,
                OffchainOrder::Submitted {
                    market_session: MarketSession::Regular,
                    ..
                }
            ),
            "Stale Extended job should submit a Regular market order, got: {order:?}"
        );
    }

    /// Returns an `OrderPlacer` that reports a configured market_session
    /// while delegating placement to a succeeding stub. Used to test the
    /// session re-check inside `PlaceHedge::perform`.
    fn market_session_overriding_placer(session: MarketSession) -> Arc<dyn OrderPlacer> {
        struct Stub {
            session: MarketSession,
        }

        #[async_trait::async_trait]
        impl OrderPlacer for Stub {
            async fn place_market_order(
                &self,
                order: st0x_execution::MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("market-1"),
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
                    executor_order_id: ExecutorOrderId::new("limit-1"),
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

            async fn fetch_latest_trade_price(
                &self,
                _symbol: &Symbol,
            ) -> Result<
                Option<st0x_execution::Positive<Usd>>,
                Box<dyn std::error::Error + Send + Sync>,
            > {
                Ok(Some(
                    st0x_execution::Positive::new(Usd::new(float!(100.0))).unwrap(),
                ))
            }

            async fn market_session(
                &self,
            ) -> Result<MarketSession, Box<dyn std::error::Error + Send + Sync>> {
                Ok(self.session)
            }
        }

        Arc::new(Stub { session })
    }

    /// Variant of `create_hedge_ctx` that wires a specific placer (the session
    /// source) and asset config through to `HedgeCtx`, so `perform` exercises a
    /// chosen session and per-symbol extended-hours eligibility.
    async fn create_hedge_ctx_with(
        placer: Arc<dyn OrderPlacer>,
        assets: AssetsConfig,
    ) -> TestInfra {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;

        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (offchain_order, offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(placer.clone())
                .await
                .unwrap();

        let ctx = HedgeCtx {
            position: position.clone(),
            offchain_order,
            poll_status_queue: PollOrderStatusJobQueue::new(&apalis_pool),
            order_placer: placer,
            assets,
            counter_trade_slippage_bps: 100,
            counter_trade_submission_lock: Arc::new(tokio::sync::Mutex::new(())),
        };

        TestInfra {
            ctx,
            apalis_pool,
            position_projection,
            offchain_order_projection,
        }
    }

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
                    client_order_id: ClientOrderId::from_uuid(job.offchain_order_id.as_uuid()),
                    kind: CounterTradeOrderKind::Market,
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

    /// The recovery path must NOT re-enqueue a poll for an order that is
    /// already terminal (a stale retry landing after the order was cancelled or
    /// failed). It returns Ok (so apalis marks the job Done) after warning that
    /// the position is still pending against the terminal order; the
    /// CheckPositions finalize sweep releases the position.
    #[tokio::test]
    async fn recover_pending_poll_status_skips_terminal_order_without_enqueuing_poll() {
        let TestInfra {
            ctx, apalis_pool, ..
        } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();

        // Drive an offchain order to terminal Failed (Place -> Submitted ->
        // MarkFailed) without ever enqueueing a poll for it.
        let order_id = OffchainOrderId::new();
        ctx.offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares: Positive::new(FractionalShares::new(float!(1.0))).unwrap(),
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    client_order_id: ClientOrderId::from_uuid(order_id.as_uuid()),
                    kind: CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();
        ctx.offchain_order
            .send(
                &order_id,
                OffchainOrderCommand::MarkFailed {
                    error: "broker rejected".to_string(),
                    failed_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        recover_pending_poll_status(&ctx, order_id)
            .await
            .expect("recovery must not error for a terminal pending order");

        let poll_jobs: i64 =
            sqlx_apalis::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(type_name::<PollOrderStatus>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        assert_eq!(
            poll_jobs, 0,
            "A terminal pending order must not be re-polled by the recovery path"
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

        // Hold the lock; after the perform-time session check the job must
        // block before it claims the position or places at the broker.
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
