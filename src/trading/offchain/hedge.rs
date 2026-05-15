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
use tracing::info;

use st0x_config::ExecutionThreshold;
use st0x_event_sorcery::{AggregateError, LifecycleError, Store};
use st0x_execution::{
    ClientOrderId, Direction, FractionalShares, MarketSession, Positive, SupportedExecutor, Symbol,
    Usd,
};

use crate::conductor::job::{Job, JobQueue, Label};
use crate::offchain::order::{
    OffchainOrder, OffchainOrderCommand, OffchainOrderId, PollOrderStatus, PollOrderStatusJobQueue,
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
    price: rain_math_float::Float,
    direction: Direction,
    slippage_bps: u16,
) -> Result<Positive<Usd>, SlippageError> {
    let basis_points = float!(10000);
    let slippage = Float::parse(u64::from(slippage_bps).to_string())?;

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
    pub(crate) poll_status_queue: PollOrderStatusJobQueue,
    /// Order placer for fetching latest trade prices during extended hours.
    /// Only needed when `extended_hours_counter_trading` is enabled.
    pub(crate) order_placer: Option<Arc<dyn crate::offchain::order::OrderPlacer>>,
    pub(crate) counter_trade_slippage_bps: u16,
    /// Serialises broker submissions across concurrent hedge jobs so two
    /// jobs for different symbols don't both submit against the same
    /// buying-power snapshot and collectively exceed available capital.
    /// Shared with the inline counter-trade path in `conductor.rs`.
    pub(crate) counter_trade_submission_lock: Arc<tokio::sync::Mutex<()>>,
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
    #[serde(default = "default_market_session")]
    pub(crate) market_session: st0x_execution::MarketSession,
}

fn default_market_session() -> st0x_execution::MarketSession {
    st0x_execution::MarketSession::Regular
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
        // Residual TOCTOU: the session read, the limit-price fetch, and the
        // broker submission are three separate awaits, so the venue clock can
        // cross a 9:30/16:00 boundary between them. This is inherent (the clock
        // is external -- acquiring the submission lock earlier wouldn't close
        // it, only serialise the price fetch). It is bounded and self-healing:
        // a boundary-straddling order is either rejected by the broker (and
        // retried, re-reading the session) or, if it lands as an extended-hours
        // limit during regular hours, converged by the CheckPositions
        // cancel-and-replace pass. The order kind is computed before the
        // position is claimed, so a rejection never strands the position.
        //
        // Re-check the market session at execution time. The enqueue-time
        // value (self.market_session) can be stale by minutes if the job
        // sat in apalis across a 9:30 or 16:00 ET boundary.
        let current_session = match ctx.order_placer.as_ref() {
            Some(placer) => placer.market_session().await.map_err(|source| {
                TradeAccountingError::LimitPriceFetch {
                    symbol: self.symbol.clone(),
                    source,
                }
            })?,
            // No placer => extended-hours feature disabled => session was
            // necessarily Regular at enqueue, and we trust that.
            None => self.market_session,
        };

        if current_session != self.market_session {
            info!(
                target: "hedge",
                symbol = %self.symbol,
                enqueued_session = ?self.market_session,
                ?current_session,
                "Market session changed between enqueue and perform; using current"
            );
        }

        // Compute the order kind BEFORE claiming the position. If this fails
        // (e.g. price fetch error during extended hours), we avoid leaving the
        // position stuck with a pending offchain_order_id and no actual order.
        let order_kind = match current_session {
            MarketSession::Regular => crate::offchain::order::CounterTradeOrderKind::Market,
            MarketSession::Closed => {
                // Market closed between enqueue and perform. Do NOT model this
                // as a job error: apalis retries are short exponential backoff
                // (seconds), not a session-aware scheduler, so a retryable
                // error would burn the retry budget and land the job terminally
                // Failed long before the venue reopens. The position has not
                // been claimed yet (order kind is computed before
                // PlaceOffChainOrder), so returning Ok cleanly leaves the net
                // exposure for the next CheckPositions scan to re-enqueue once
                // the market is tradeable again.
                info!(
                    target: "hedge",
                    symbol = %self.symbol,
                    "Market closed at perform time; skipping hedge, CheckPositions will re-enqueue when the venue reopens"
                );
                return Ok(());
            }
            MarketSession::Extended => {
                let Some(placer) = ctx.order_placer.as_ref() else {
                    return Err(TradeAccountingError::OrderPlacerNotConfigured);
                };

                let latest_price = placer
                    .fetch_latest_trade_price(&self.symbol)
                    .await
                    .map_err(|source| TradeAccountingError::LimitPriceFetch {
                        symbol: self.symbol.clone(),
                        source,
                    })?
                    .ok_or_else(|| TradeAccountingError::LimitPriceUnavailable {
                        symbol: self.symbol.clone(),
                    })?;

                let limit_price = apply_slippage(
                    latest_price.inner(),
                    self.direction,
                    ctx.counter_trade_slippage_bps,
                )?;

                info!(
                    target: "hedge",
                    symbol = %self.symbol,
                    %limit_price,
                    direction = ?self.direction,
                    "Extended hours: placing limit order"
                );

                crate::offchain::order::CounterTradeOrderKind::ExtendedHoursLimit { limit_price }
            }
        };

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
        let client_order_id_source = anchor.unwrap_or(self.offchain_order_id);
        let client_order_id = ClientOrderId::from_uuid(client_order_id_source.as_uuid());

        // Serialise broker submissions with the inline counter-trade path.
        // Without this, two hedge jobs for different symbols enqueued in
        // the same scan window can both call the broker simultaneously and
        // collectively exceed buying power.
        let _submission_guard = ctx.counter_trade_submission_lock.lock().await;

        ctx.offchain_order
            .send(
                &self.offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: self.symbol.clone(),
                    shares: self.shares,
                    direction: self.direction,
                    executor: self.executor,
                    client_order_id,
                    kind: order_kind,
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

            // Cancelled here is unexpected -- a freshly Placed order should
            // not be Cancelled by the time we re-load it. Treat as Failed so
            // the position releases its pending slot rather than getting
            // stuck pending forever.
            Some(Cancelled { reason, .. }) => {
                ctx.position
                    .send(
                        &self.symbol,
                        PositionCommand::FailOffChainOrder {
                            offchain_order_id: self.offchain_order_id,
                            error: format!("cancelled unexpectedly: {reason:?}"),
                        },
                    )
                    .await?;
            }

            Some(Filled { .. } | Pending { .. }) | None => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::TxHash;
    use std::any::type_name;
    use std::sync::Arc;

    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::{
        Direction, ExecutorOrderId, FractionalShares, Positive, SupportedExecutor, Symbol,
    };
    use st0x_float_macro::float;

    use super::*;
    use crate::conductor::job::Job;
    use crate::offchain::order::{OffchainOrder, OrderPlacementResult, OrderPlacer};
    use crate::position::{Position, PositionCommand, TradeId};
    use crate::test_utils::setup_test_db;
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

            async fn place_limit_order(
                &self,
                order: st0x_execution::LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("test-limit-order-123"),
                    placed_shares: order.shares,
                })
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &st0x_execution::ExecutorOrderId,
            ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                Ok(())
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
            ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                Ok(())
            }
        }

        Arc::new(RejectingPlacer)
    }

    struct TestInfra {
        ctx: HedgeCtx,
        pool: sqlx::SqlitePool,
        position_projection: Arc<st0x_event_sorcery::Projection<Position>>,
        offchain_order_projection: Arc<st0x_event_sorcery::Projection<OffchainOrder>>,
    }

    async fn create_hedge_ctx(order_placer: Arc<dyn OrderPlacer>) -> TestInfra {
        let pool = setup_test_db().await;
        crate::conductor::setup_apalis_tables(&pool).await.unwrap();

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
            poll_status_queue: PollOrderStatusJobQueue::new(&pool),
            order_placer: None,
            counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
            counter_trade_submission_lock: Arc::new(tokio::sync::Mutex::new(())),
        };

        TestInfra {
            ctx,
            pool,
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
                })
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &st0x_execution::ExecutorOrderId,
            ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                Ok(())
            }

            async fn fetch_latest_trade_price(
                &self,
                _symbol: &Symbol,
            ) -> Result<
                Option<st0x_execution::Positive<rain_math_float::Float>>,
                Box<dyn std::error::Error + Send + Sync>,
            > {
                Ok(Some(st0x_execution::Positive::new(self.0)?))
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

        let pool = setup_test_db().await;
        crate::conductor::setup_apalis_tables(&pool).await.unwrap();

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
            poll_status_queue: PollOrderStatusJobQueue::new(&pool),
            order_placer: Some(placer),
            counter_trade_slippage_bps: 100,
            counter_trade_submission_lock: Arc::new(tokio::sync::Mutex::new(())),
        };

        TestInfra {
            ctx,
            pool,
            position_projection,
            offchain_order_projection,
        }
    }

    #[test]
    fn apply_slippage_buy_adds_to_price() {
        let price = float!(150.0);
        let result = apply_slippage(price, Direction::Buy, 100).unwrap();
        // 150 * 1.01 = 151.50, already 2 decimal places, no rounding needed
        assert!(
            result.inner().inner().eq(float!(151.50)).unwrap(),
            "Buy slippage should increase the price, got: {result}"
        );
    }

    #[test]
    fn apply_slippage_sell_subtracts_from_price() {
        let price = float!(150.0);
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
        let price = float!(151.23);
        let result = apply_slippage(price, Direction::Buy, 100).unwrap();
        assert!(
            result.inner().inner().eq(float!(152.75)).unwrap(),
            "Buy should round up to the nearest cent, got: {result}"
        );
    }

    #[test]
    fn apply_slippage_rounds_sell_down_to_two_decimals() {
        // 151.23 * 0.99 = 149.7177 -> truncates to 149.71 for a sell
        let price = float!(151.23);
        let result = apply_slippage(price, Direction::Sell, 100).unwrap();
        assert!(
            result.inner().inner().eq(float!(149.71)).unwrap(),
            "Sell should round down to the nearest cent, got: {result}"
        );
    }

    #[test]
    fn apply_slippage_zero_bps_is_identity() {
        let price = float!(100.0);
        let buy_result = apply_slippage(price, Direction::Buy, 0).unwrap();
        let sell_result = apply_slippage(price, Direction::Sell, 0).unwrap();
        assert!(buy_result.inner().inner().eq(price).unwrap());
        assert!(sell_result.inner().inner().eq(price).unwrap());
    }

    #[test]
    fn apply_slippage_sub_dollar_uses_four_decimals() {
        // 0.5000 * 1.01 = 0.5050 — 4-decimal precision branch
        let result = apply_slippage(float!(0.5), Direction::Buy, 100).unwrap();
        assert!(
            result.inner().inner().eq(float!(0.5050)).unwrap(),
            "Sub-$1 buy should round to 4 decimals (0.5050), got: {result}"
        );

        let sell = apply_slippage(float!(0.5), Direction::Sell, 100).unwrap();
        assert!(
            sell.inner().inner().eq(float!(0.4950)).unwrap(),
            "Sub-$1 sell should round to 4 decimals (0.4950), got: {sell}"
        );
    }

    #[test]
    fn apply_slippage_max_bps_for_sell_returns_non_positive_error() {
        // 9999 bps slippage on a sell: 100 * 0.0001 = 0.01, still positive.
        // But conceptually max safe bps is now 9999 (config rejects 10_000).
        // This test guards against future bound regressions: 9999 must succeed.
        let result = apply_slippage(float!(100.0), Direction::Sell, 9999).unwrap();
        assert!(
            result.inner().inner().eq(float!(0.01)).unwrap(),
            "Max-bps sell should produce 1 cent, got: {result}"
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
                    is_extended_hours: true,
                    ..
                }
            ),
            "Order should be submitted as extended-hours, got: {order:?}"
        );
        // The exact limit price computation is covered by the dedicated
        // `apply_slippage_*` unit tests; this integration test only checks
        // the lifecycle path.
    }

    #[tokio::test]
    async fn extended_hours_without_order_placer_does_not_leave_position_stuck() {
        // Use None order_placer to simulate price fetch unavailable
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

        let job = PlaceHedge {
            symbol: symbol.clone(),
            direction: Direction::Sell,
            shares: Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
            executor: SupportedExecutor::DryRun,
            threshold: ExecutionThreshold::whole_share(),
            offchain_order_id: OffchainOrderId::new(),
            market_session: MarketSession::Extended,
        };

        // ctx.order_placer is None, so this should fail
        let result = job.perform(&ctx).await;
        assert!(
            matches!(result, Err(TradeAccountingError::OrderPlacerNotConfigured)),
            "expected OrderPlacerNotConfigured, got: {result:?}"
        );

        // Position should NOT have a pending order
        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id, None,
            "Position should not be stuck with a pending order after limit-price failure"
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
        } = create_hedge_ctx_with(placer).await;
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
        } = create_hedge_ctx_with(placer).await;
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
                    is_extended_hours: false,
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
                })
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &st0x_execution::ExecutorOrderId,
            ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                Ok(())
            }

            async fn fetch_latest_trade_price(
                &self,
                _symbol: &Symbol,
            ) -> Result<
                Option<st0x_execution::Positive<rain_math_float::Float>>,
                Box<dyn std::error::Error + Send + Sync>,
            > {
                Ok(Some(st0x_execution::Positive::new(float!(100.0)).unwrap()))
            }

            async fn market_session(
                &self,
            ) -> Result<MarketSession, Box<dyn std::error::Error + Send + Sync>> {
                Ok(self.session)
            }
        }

        Arc::new(Stub { session })
    }

    /// Variant of `create_hedge_ctx` that wires a specific placer through to
    /// `HedgeCtx::order_placer` (Some, not None), so `perform` will call
    /// `market_session()` on it.
    async fn create_hedge_ctx_with(placer: Arc<dyn OrderPlacer>) -> TestInfra {
        let pool = setup_test_db().await;
        crate::conductor::setup_apalis_tables(&pool).await.unwrap();

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
            poll_status_queue: PollOrderStatusJobQueue::new(&pool),
            order_placer: Some(placer),
            counter_trade_slippage_bps: 100,
            counter_trade_submission_lock: Arc::new(tokio::sync::Mutex::new(())),
        };

        TestInfra {
            ctx,
            pool,
            position_projection,
            offchain_order_projection,
        }
    }

    #[tokio::test]
    async fn retry_after_failed_poll_enqueue_re_enqueues_poll() {
        let TestInfra { ctx, pool, .. } = create_hedge_ctx(succeeding_order_placer()).await;
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
            sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(type_name::<PollOrderStatus>())
                .fetch_one(&pool)
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
            sqlx::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(type_name::<PollOrderStatus>())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            poll_jobs_after_retry, 2,
            "Retry must re-enqueue PollOrderStatus when the order is still Submitted"
        );
    }
}
