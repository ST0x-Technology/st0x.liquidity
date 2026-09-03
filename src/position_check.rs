//! Periodic position scan as a durable, self-rescheduling job.
//!
//! Replaces the supervised polling task with a [`CheckPositions`] apalis job
//! that re-enqueues itself with the configured interval after each scan. Each
//! ready symbol becomes an independent [`PlaceHedge`] job, so a transient
//! failure for one symbol does not affect others.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use apalis::prelude::Status;
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, stream};
use metrics::counter;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio::sync::Mutex;
use tracing::{debug, error, warn};

use st0x_config::Ctx;
use st0x_event_sorcery::{AggregateError, LifecycleError, Projection, Store};
use st0x_execution::{
    ClientOrderId, CounterTradePreflight, Direction, Executor, MarketOrder, MarketSession,
    Permanence, SupportedExecutor, Symbol,
};

use crate::alerts::Notifier;
use crate::conductor::job::{
    BackpressureStreak, Job, JobQueue, Label, QueuePushError, find_backpressure, find_permanence,
};
use crate::conductor::{clamp_shares_to_reservation, recover_orphaned_pending_offchain_orders};
use crate::equity_redemption::symbols_with_active_transfers;
use crate::offchain::order::{
    CancellationReason, OffchainOrder, OffchainOrderCommand, OffchainOrderId, OrderPlacer,
    PollOrderStatusJobQueue, TerminalPositionFinalization, position_command_for_finalization,
    recover_submitted_offchain_orders, session_metric_label, terminal_position_finalization,
};
use crate::onchain::accumulator::{ExecutionCtx, check_execution_readiness};
use crate::position::{Position, PositionError};
use crate::trading::offchain::close_flatten::{
    CloseFlattenCrossRamp, CloseFlattenPolicy, CloseFlattenWindow, preflight_skip_reason_label,
};
use crate::trading::offchain::hedge::{
    HedgeJobQueue, OvernightReferenceError, PlaceHedge, ReferencePriceError,
    TransientFailureStreak, alert_dead_letter, apply_slippage,
    resolve_extended_hours_reference_price, resolve_overnight_reference_price,
};
use crate::trading::onchain::trade_accountant::{DeadLetterReason, SymbolScopedReason};

pub(crate) type CheckPositionsJobQueue = JobQueue<CheckPositions>;
const MAX_CONCURRENT_CANCELLATION_REQUESTS: usize = 8;

/// Shared dependencies for the [`CheckPositions`] job.
pub(crate) struct CheckPositionsCtx<E: Executor + Clone + Send + Sync + 'static> {
    pub(crate) executor: E,
    pub(crate) position: Arc<Store<Position>>,
    pub(crate) position_projection: Arc<Projection<Position>>,
    pub(crate) offchain_order: Arc<Store<OffchainOrder>>,
    pub(crate) offchain_order_projection: Arc<Projection<OffchainOrder>>,
    /// Re-drives `Pending` placements stuck between broker acceptance and the
    /// outcome commit (ADR 0014): `CheckPositions` skips pending-claimed
    /// positions, so without a periodic sweep such an order is only recovered at
    /// the next restart.
    pub(crate) order_placer: Arc<dyn OrderPlacer>,
    /// Shared with the placement paths so the periodic recovery's broker re-drive
    /// serializes against live placements (ADR 0014).
    pub(crate) counter_trade_submission_lock: Arc<Mutex<()>>,
    pub(crate) hedge_queue: HedgeJobQueue,
    pub(crate) check_positions_queue: CheckPositionsJobQueue,
    /// Catches up `PollOrderStatus` for orders the pending re-drive leaves
    /// `Submitted` at runtime. The startup recovery is followed by
    /// `recover_submitted_offchain_orders`; the periodic sweep has no such
    /// follow-up, so without re-running it here a runtime-recovered order would
    /// sit `Submitted` (unpolled) until the next restart (ADR 0014).
    pub(crate) poll_status_queue: PollOrderStatusJobQueue,
    pub(crate) ctx: Ctx,
    pub(crate) pool: SqlitePool,
    pub(crate) check_interval: Duration,
    /// Validated once at construction (`conductor/builder.rs`) instead of
    /// re-parsed from `ctx.extended_hours_close_flatten_window_secs` on
    /// every scan tick -- the window is fixed for the process lifetime, so
    /// re-validating it per tick just threads an always-succeeds-in-practice
    /// `Result` through the hot scan path.
    pub(crate) close_flatten_policy: CloseFlattenPolicy,
    pub(crate) close_flatten_ramp: CloseFlattenCrossRamp,
    /// Passed through to `recover_submitted_offchain_orders`'s dedup guard, so
    /// its stranded-row staleness bound scales with the configured poll
    /// cadence instead of a value hardcoded far from where it is configured.
    pub(crate) poll_interval: Duration,
    /// Pages the operator when the scan drops an extended-hours buy for a
    /// non-retryable reference-price failure. The same `Arc<dyn Notifier>` the
    /// hedge job's dead-letter alert uses, so both share one delivery channel.
    pub(crate) notifier: Arc<dyn Notifier>,
    /// The hedge job's dead-letter dedup set, shared rather than duplicated:
    /// a scan-time drop and the dead-letter it would have become are the same
    /// standing delta, so they must not page twice, and the release performed
    /// when one of the symbol's hedges reaches the broker must clear both.
    pub(crate) alerted_dead_letters: Arc<Mutex<HashSet<(Symbol, DeadLetterReason)>>>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HedgeScanSkipReason {
    MarketSessionCheck,
    ReferencePriceUnavailable,
    MarkFetchFailed,
    QuoteFetchFailed,
    SlippageCalculation,
    /// The overnight knobs were absent despite an enabled symbol (a wiring
    /// bug; startup validation makes them present whenever any asset opts
    /// in). Shares the label the hedge job uses for the same fail-closed
    /// deferral.
    OvernightIneligible,
    /// The indicative overnight feed produced no usable reference (failed
    /// fetch or a quote staler than `overnight_max_quote_age_secs`). Shares
    /// the label the hedge job uses when its own resolution defers.
    OvernightUnpriceable,
}

impl HedgeScanSkipReason {
    pub(crate) const fn metric_label(self) -> &'static str {
        match self {
            Self::MarketSessionCheck => "market_session_check",
            Self::ReferencePriceUnavailable => "reference_price_unavailable",
            Self::MarkFetchFailed => "mark_fetch_failed",
            Self::QuoteFetchFailed => "quote_fetch_failed",
            Self::SlippageCalculation => "slippage_calculation",
            Self::OvernightIneligible => "overnight_ineligible",
            Self::OvernightUnpriceable => "overnight_unpriceable",
        }
    }
}

impl From<&ReferencePriceError> for HedgeScanSkipReason {
    fn from(error: &ReferencePriceError) -> Self {
        match error {
            ReferencePriceError::Unavailable => Self::ReferencePriceUnavailable,
            ReferencePriceError::MarkFetch(_) => Self::MarkFetchFailed,
            ReferencePriceError::QuoteFetch(_) => Self::QuoteFetchFailed,
        }
    }
}

/// Counts an extended-hours buy the scan dropped before it could become a
/// `PlaceHedge` job, with the cause that dropped it.
///
/// The hedge job owns `hedge_dead_lettered_total`, which cannot fire for a buy
/// that never reaches the queue -- so a symbol whose mark and quote both keep
/// failing would otherwise accumulate a standing delta behind nothing but a log
/// line. A non-retryable failure's operator page is shared with the hedge job's
/// dead-letter alert (see `preflight_extended_hours_buy`); this is its counter.
/// Emitted for every extended-hours buy, not only close-flatten ones: the daily
/// path lost its own preflight signal when it moved onto the mark. Inside the
/// window it additionally keeps the close-flatten dashboard's counter, now
/// labelled with the real cause instead of one blanket reason.
pub(crate) fn record_scan_skip(
    symbol: &Symbol,
    session: MarketSession,
    reason: HedgeScanSkipReason,
    close_flatten_window: Option<CloseFlattenWindow>,
) {
    let reason = reason.metric_label();
    counter!(
        "hedge_scan_skipped_total",
        "symbol" => symbol.to_string(),
        "session" => session_metric_label(session),
        "reason" => reason
    )
    .increment(1);

    if close_flatten_window.is_some() {
        counter!(
            "close_flatten_blocked_total",
            "symbol" => symbol.to_string(),
            "reason" => reason
        )
        .increment(1);
    }
}

fn should_page_reference_price_failure(
    error: &ReferencePriceError,
    executor: SupportedExecutor,
) -> bool {
    match error {
        ReferencePriceError::Unavailable => executor != SupportedExecutor::DryRun,
        ReferencePriceError::MarkFetch(source) | ReferencePriceError::QuoteFetch(source) => {
            find_backpressure(source.as_ref()).is_none()
                && find_permanence(source.as_ref()) != Some(Permanence::Transient)
        }
    }
}

/// A stale quote is a freshness race the next scan retries against a live
/// feed, never an incident. A failed fetch pages under the same
/// permanent-or-unclassified rule as [`should_page_reference_price_failure`]:
/// an entitlement rejection (classified `Permanent`) must page rather than
/// leave the standing delta behind nothing but a counter, while transient and
/// rate-limited failures wait for the next scan.
fn should_page_overnight_reference_failure(error: &OvernightReferenceError) -> bool {
    match error {
        OvernightReferenceError::Stale { .. } => false,
        OvernightReferenceError::QuoteFetch(source) => {
            find_backpressure(source.as_ref()).is_none()
                && find_permanence(source.as_ref()) != Some(Permanence::Transient)
        }
    }
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

#[derive(Debug, Default)]
enum CloseFlattenWindowCache {
    #[default]
    Unresolved,
    Resolved(Option<CloseFlattenWindow>),
    Failed,
}

enum CloseFlattenWindowResolutionError<E> {
    Source(E),
    CachedFailure,
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
        let mut close_flatten_window_cache = CloseFlattenWindowCache::default();

        // Every tick, independent of the feature flag: clear any position
        // whose pending order has gone terminal (e.g. a cancellation the
        // poller has since confirmed). Terminal `Cancelled` orders are
        // produced by ungated paths too -- a manual broker-dashboard cancel,
        // or an order left `Cancelling` across a flag-off restart -- and this
        // sweep is the only runtime path that releases the position's pending
        // slot for them; gating it would strand such symbols unhedged until
        // the next restart.
        ctx.finalize_terminal_pending_positions().await;

        // Enqueue unrelated ready hedges before broker-backed cancellation
        // maintenance. A slow cancellation must not extend the exposure window
        // for another symbol that is already ready to hedge.
        ctx.scan_and_enqueue(&mut close_flatten_window_cache)
            .await?;

        // Each arm gates its sweeps on the session family that owns them:
        // the extended sweeps warn when their knobs are absent and the
        // close-flatten sweep makes a calendar request, so running them in a
        // deployment that only enables overnight would spam false warnings
        // and waste a round trip every tick (and vice versa).
        if ctx.ctx.assets.any_extended_hours_enabled() || ctx.ctx.assets.any_overnight_enabled() {
            match ctx.executor.market_session().await {
                Ok(MarketSession::Extended) => {
                    // Deliberately NOT gated on `any_overnight_enabled()`: a
                    // full overnight rollback (every symbol flipped back to
                    // disabled) must still converge the survivors it strands,
                    // and the sweep makes no broker call when nothing
                    // matches. Its per-symbol filter scopes the rest.
                    ctx.request_overnight_session_boundary_cancellations().await;
                    if ctx.ctx.assets.any_extended_hours_enabled() {
                        ctx.request_extended_hours_close_flatten_cancellations(
                            &mut close_flatten_window_cache,
                        )
                        .await;
                        ctx.request_extended_hours_reprice_timeout_cancellations()
                            .await;
                    }
                }
                Ok(MarketSession::Regular) => {
                    // Every regular-hours tick: request cancellation of
                    // still-live extended-hours and overnight limit orders so
                    // they're replaced with market orders. Level-triggered so
                    // an order that slipped past the session boundary,
                    // survived a restart, or whose cancellation request
                    // failed on a previous tick is caught on this one.
                    // Ungated within the arm for the same rollback reasoning
                    // as the pre-market boundary sweep: the outer gate is
                    // already the sweep's exact condition.
                    ctx.request_market_open_cancellations().await;
                }
                Ok(MarketSession::Overnight) => {
                    if ctx.ctx.assets.any_overnight_enabled() {
                        ctx.request_overnight_reprice_timeout_cancellations().await;
                    }
                }
                // A Closed tick has nothing to sweep: no session's limit
                // orders can be repriced into a closed venue, and the 20:00
                // entry into Overnight relies on the broker's day-order
                // auto-cancel observed by the status poller, not on a sweep.
                Ok(MarketSession::Closed) => {}
                Err(error) => {
                    warn!("Failed to check market session for order cancellation: {error}");
                }
            }
        }

        ctx.reschedule().await
    }
}

impl<E> CheckPositionsCtx<E>
where
    E: Executor + Clone + Send + Sync + 'static,
{
    async fn scan_and_enqueue(
        &self,
        close_flatten_window_cache: &mut CloseFlattenWindowCache,
    ) -> Result<(), CheckPositionsError> {
        // Reconcile placements stuck between broker acceptance and the outcome
        // commit (ADR 0014). `is_ready_for_execution` skips pending-claimed
        // positions, so the main scan never re-drives these; this periodic sweep
        // does. Best-effort: a failure is logged and retried next interval rather
        // than killing the loop. Held under the shared submission lock so the
        // broker re-drive serializes against live placements.
        {
            let _submission_guard = self.counter_trade_submission_lock.lock().await;
            if let Err(error) = recover_orphaned_pending_offchain_orders(
                &self.position,
                &self.position_projection,
                &self.offchain_order,
                self.order_placer.as_ref(),
            )
            .await
            {
                error!(%error, "Periodic stuck-pending recovery failed; retrying next interval");
            }
        }

        // The submitted-order poll catch-up runs OUTSIDE the submission lock: it
        // only enqueues apalis `PollOrderStatus` jobs (no broker I/O), so it does
        // not need to serialize against live placements -- holding the lock here
        // would needlessly block placements behind a queue-only sweep.
        //
        // It runs every tick: a placement that reaches `Submitted` but whose poll
        // job died (a transient broker error exhausts the apalis retries ->
        // `Failed`, which `requeue_orphaned` deliberately skips) or was never
        // enqueued (crash window) is otherwise stuck until the next restart,
        // leaving the hedge unreconciled. `recover_submitted_offchain_orders`
        // dedupes per order before pushing: it queries the live apalis Jobs
        // rows via `json_extract(CAST(job AS TEXT), '$.offchain_order_id')`
        // (`reconcile_live_poll_jobs`) and skips the push when a live poll already
        // exists for that order, so this tick is a no-op for orders already
        // being polled and only arms polling for orders that have none
        // (an unconditional re-push here forked an independent,
        // self-perpetuating poll chain on every tick an order stayed open).
        // It also collapses any pre-existing duplicate live rows for one
        // order down to one.
        let poll_status_queue = self.poll_status_queue.clone();
        if let Err(error) = recover_submitted_offchain_orders(
            &self.offchain_order_projection,
            &poll_status_queue,
            self.executor.to_supported_executor(),
            self.poll_interval,
        )
        .await
        {
            error!(
                %error,
                "Periodic submitted-order poll recovery failed; retrying next interval"
            );
        }

        let all_positions = self.position_projection.load_all().await?;
        let active_transfers = symbols_with_active_transfers(&self.pool).await?;

        let eligible: Vec<Symbol> = all_positions
            .iter()
            .filter(|(symbol, _)| self.ctx.assets.is_trading_enabled(symbol))
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
            self.check_and_enqueue_symbol(symbol, close_flatten_window_cache)
                .await;
        }

        Ok(())
    }

    async fn check_and_enqueue_symbol(
        &self,
        symbol: &Symbol,
        close_flatten_window_cache: &mut CloseFlattenWindowCache,
    ) {
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

        if !self
            .preflight_and_clamp_shares(&mut ready, close_flatten_window_cache)
            .await
        {
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
            backpressure_streak: BackpressureStreak::default(),
            transient_streak: TransientFailureStreak::default(),
        };

        let mut queue = self.hedge_queue.clone();
        if let Err(error) = queue.push(job).await {
            error!(%ready.symbol, %error, "Failed to enqueue hedge job");
        }
    }

    /// Checks broker inventory before enqueueing a hedge job. Returns `true`
    /// if the order should proceed (possibly with reduced shares), `false` if
    /// it should be skipped entirely.
    async fn preflight_and_clamp_shares(
        &self,
        ready: &mut ExecutionCtx,
        close_flatten_window_cache: &mut CloseFlattenWindowCache,
    ) -> bool {
        let order = MarketOrder {
            symbol: ready.symbol.clone(),
            shares: ready.shares,
            direction: ready.direction,
            // Preflight only; this id is never sent to the broker. Use a
            // fresh value so callers cannot mistake it for a real key.
            client_order_id: ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
        };

        // Every enabled extended-hours buy uses the shared reference-price
        // resolver and cross at scan time, matching the placement path whether
        // close-flatten is active or not. A failed session/window lookup fails
        // this scan tick closed; it never falls through to
        // `preflight_counter_trade`'s different reference. Overnight buys do
        // the same from the indicative feed and `overnight_slippage_bps`.
        // Regular-hours buys and all sells keep the ordinary preflight,
        // avoiding an unnecessary calendar request and exact-price check where
        // price cannot constrain the reservation.
        let extended_hours_buy = ready.direction == Direction::Buy
            && ready.market_session == MarketSession::Extended
            && self.ctx.assets.is_extended_hours_enabled(&ready.symbol);

        let overnight_buy = ready.direction == Direction::Buy
            && ready.market_session == MarketSession::Overnight
            && self.ctx.assets.is_overnight_enabled(&ready.symbol);

        let close_flatten_window = if extended_hours_buy {
            match self
                .active_close_flatten_window(close_flatten_window_cache)
                .await
            {
                Ok(window) => window,
                Err(CloseFlattenWindowResolutionError::Source(error)) => {
                    counter!(
                        "close_flatten_blocked_total",
                        "symbol" => ready.symbol.to_string(),
                        "reason" => "session_status_check_failed"
                    )
                    .increment(1);
                    warn!(
                        target: "hedge",
                        symbol = %ready.symbol, %error,
                        "Skipping hedge enqueue: failed to verify close-flatten window status"
                    );
                    return false;
                }
                Err(CloseFlattenWindowResolutionError::CachedFailure) => {
                    counter!(
                        "close_flatten_blocked_total",
                        "symbol" => ready.symbol.to_string(),
                        "reason" => "session_status_check_failed"
                    )
                    .increment(1);
                    warn!(
                        target: "hedge",
                        symbol = %ready.symbol,
                        "Skipping hedge enqueue: close-flatten status lookup failed earlier in this scan"
                    );
                    return false;
                }
            }
        } else {
            None
        };

        let preflight = if extended_hours_buy {
            match self
                .preflight_extended_hours_buy(order, close_flatten_window)
                .await
            {
                Ok(Some(preflight)) => Ok(preflight),
                // `preflight_extended_hours_buy` counted and logged the cause
                // it dropped this buy for; the scan just skips the tick.
                Ok(None) => return false,
                Err(error) => Err(error),
            }
        } else if overnight_buy {
            match self.preflight_overnight_buy(order).await {
                Ok(Some(preflight)) => Ok(preflight),
                // `preflight_overnight_buy` counted and logged the cause it
                // dropped this buy for; the scan just skips the tick.
                Ok(None) => return false,
                Err(error) => Err(error),
            }
        } else {
            self.executor.preflight_counter_trade(order).await
        };

        match preflight {
            Ok(CounterTradePreflight::Allowed { reservation }) => {
                clamp_shares_to_reservation(ready, reservation.as_ref());
                true
            }
            Ok(CounterTradePreflight::Skipped(reason)) => {
                let blocked_by_close_flatten = self
                    .is_blocked_by_close_flatten(
                        close_flatten_window,
                        ready.market_session,
                        close_flatten_window_cache,
                    )
                    .await;
                if blocked_by_close_flatten {
                    counter!(
                        "close_flatten_blocked_total",
                        "symbol" => ready.symbol.to_string(),
                        "reason" => preflight_skip_reason_label(&reason)
                    )
                    .increment(1);
                    error!(
                        target: "hedge",
                        symbol = %ready.symbol, %reason,
                        "Close flatten blocked: preflight rejected"
                    );
                } else {
                    warn!(
                        target: "hedge",
                        symbol = %ready.symbol, %reason,
                        "Skipping hedge enqueue: preflight rejected"
                    );
                }
                false
            }
            Err(error) => {
                let blocked_by_close_flatten = self
                    .is_blocked_by_close_flatten(
                        close_flatten_window,
                        ready.market_session,
                        close_flatten_window_cache,
                    )
                    .await;
                if blocked_by_close_flatten {
                    counter!(
                        "close_flatten_blocked_total",
                        "symbol" => ready.symbol.to_string(),
                        "reason" => "preflight_failed"
                    )
                    .increment(1);
                }
                error!(
                    target: "hedge",
                    symbol = %ready.symbol, %error,
                    "Preflight check failed during position scan"
                );
                false
            }
        }
    }

    /// Whether a preflight rejection happened during an active close-flatten
    /// window, for log-severity/metric labeling only. `close_flatten_window`
    /// is `Some` when the caller already resolved it for a close-flatten buy;
    /// otherwise (sells, or buys on symbols without extended-hours enabled,
    /// or any preflight outside the `Extended` session) this re-checks
    /// directly -- but only when `session` is `Extended`, since close-flatten
    /// can never be active otherwise and calling `market_session_status`
    /// (a calendar HTTP round trip) outside that session would be a pure
    /// waste on the hot path. A transient status-check failure here only
    /// affects how an unrelated rejection is labeled, not whether the hedge
    /// proceeds, so it is logged at debug and treated as "not blocked" rather
    /// than propagated.
    async fn is_blocked_by_close_flatten(
        &self,
        close_flatten_window: Option<CloseFlattenWindow>,
        session: MarketSession,
        close_flatten_window_cache: &mut CloseFlattenWindowCache,
    ) -> bool {
        if close_flatten_window.is_some() {
            return true;
        }

        if session != MarketSession::Extended {
            return false;
        }

        match self
            .active_close_flatten_window(close_flatten_window_cache)
            .await
        {
            Ok(window) => window.is_some(),
            Err(CloseFlattenWindowResolutionError::Source(error)) => {
                debug!(
                    %error,
                    "Failed to check close-flatten window status while labeling a preflight \
                     rejection"
                );
                false
            }
            Err(CloseFlattenWindowResolutionError::CachedFailure) => false,
        }
    }

    /// Preflights an extended-hours buy against the exact price it will be
    /// submitted at.
    ///
    /// Both this and `select_order_kind_for_current_session` derive the limit
    /// from `resolve_extended_hours_reference_price` and the same cross, so the
    /// only difference between the scan-time and placement-time price is the
    /// clock. That matters more with a ramped cross than it did with a flat
    /// band: preflighting against an un-crossed reference would understate the
    /// cash a late-window buy actually needs by the full width of the ramp.
    ///
    /// `Ok(None)` means the buy has no price to preflight against, which the
    /// caller treats as "skip this tick" rather than an error. The skip is
    /// counted with its own cause on `hedge_scan_skipped_total`, since the job
    /// that carries the dead-letter counter is never enqueued for it. A
    /// non-retryable reference-price failure additionally pages the operator
    /// through the hedge job's own `alert_dead_letter`, under the
    /// `(symbol, reason)` key that job would have used. Transient and
    /// rate-limited failures wait for the next scan instead of creating a
    /// dead-letter page on their first observation.
    async fn preflight_extended_hours_buy(
        &self,
        order: MarketOrder,
        close_flatten_window: Option<CloseFlattenWindow>,
    ) -> Result<Option<CounterTradePreflight>, E::Error> {
        let reference = match resolve_extended_hours_reference_price(
            self.order_placer.as_ref(),
            &order.symbol,
            order.direction,
        )
        .await
        {
            Ok(reference) => reference,
            Err(error) => {
                let skip_reason = HedgeScanSkipReason::from(&error);
                record_scan_skip(
                    &order.symbol,
                    MarketSession::Extended,
                    skip_reason,
                    close_flatten_window,
                );
                warn!(
                    target: "hedge",
                    symbol = %order.symbol,
                    ?error,
                    "Skipping hedge enqueue: no reference price to preflight against"
                );
                if should_page_reference_price_failure(
                    &error,
                    self.executor.to_supported_executor(),
                ) {
                    alert_dead_letter(
                        self.notifier.as_ref(),
                        &self.alerted_dead_letters,
                        &order.symbol,
                        error.dead_letter_reason(),
                        &format!(
                            "Hedge for {} skipped: {} failure left no reference price to \
                             preflight against. The scan keeps skipping it, so the symbol \
                             carries a standing delta until the market-data failure is fixed.",
                            order.symbol,
                            error.dead_letter_reason().metric_label()
                        ),
                    )
                    .await;
                }

                return Ok(None);
            }
        };

        let cross_bps = self
            .close_flatten_ramp
            .cross_bps(close_flatten_window, Utc::now());

        let limit_price = match apply_slippage(reference.price.inner(), order.direction, cross_bps)
        {
            Ok(limit_price) => limit_price,
            Err(error) => {
                record_scan_skip(
                    &order.symbol,
                    MarketSession::Extended,
                    HedgeScanSkipReason::SlippageCalculation,
                    close_flatten_window,
                );
                warn!(
                    target: "hedge",
                    symbol = %order.symbol,
                    %error,
                    "Skipping hedge enqueue: could not cross the reference price"
                );
                alert_dead_letter(
                    self.notifier.as_ref(),
                    &self.alerted_dead_letters,
                    &order.symbol,
                    DeadLetterReason::SymbolScoped(SymbolScopedReason::SlippageCalculation),
                    &format!(
                        "Hedge for {} skipped: the reference price could not be crossed at \
                         {cross_bps} bps. The scan keeps skipping the symbol while the cross \
                         stays this wide, so it carries a standing delta.",
                        order.symbol,
                    ),
                )
                .await;
                return Ok(None);
            }
        };

        self.executor
            .preflight_counter_trade_at_price(order, limit_price)
            .await
            .map(Some)
    }

    /// Preflights an overnight buy against the exact limit it will be
    /// submitted at: the indicative ask crossed by `overnight_slippage_bps`,
    /// the same derivation the placement path performs. The overnight twin of
    /// `preflight_extended_hours_buy`, without the close-flatten coupling
    /// that path carries. Every skip is counted on
    /// `hedge_scan_skipped_total`; a non-retryable quote-fetch failure (an
    /// entitlement rejection above all) additionally pages the operator under
    /// the shared dead-letter dedup, since the hedge job defers rather than
    /// errors overnight and would never page for the standing delta itself.
    ///
    /// `Ok(None)` means the buy has no price to preflight against, which the
    /// caller treats as "skip this tick" rather than an error.
    async fn preflight_overnight_buy(
        &self,
        order: MarketOrder,
    ) -> Result<Option<CounterTradePreflight>, E::Error> {
        // Present whenever any asset enables overnight (startup validation),
        // and only an enabled symbol reaches here; absence is a wiring bug,
        // so skip fail-closed rather than silently assume a bound.
        let (Some(max_quote_age_secs), Some(slippage_bps)) = (
            self.ctx.overnight_max_quote_age_secs,
            self.ctx.overnight_slippage_bps,
        ) else {
            record_scan_skip(
                &order.symbol,
                MarketSession::Overnight,
                HedgeScanSkipReason::OvernightIneligible,
                None,
            );
            warn!(
                target: "hedge",
                symbol = %order.symbol,
                "Skipping hedge enqueue: overnight knobs absent despite an enabled symbol"
            );
            return Ok(None);
        };

        let reference = match resolve_overnight_reference_price(
            self.order_placer.as_ref(),
            &order.symbol,
            order.direction,
            Duration::from_secs(max_quote_age_secs.get()),
            Utc::now(),
        )
        .await
        {
            Ok(reference) => reference,
            Err(error) => {
                record_scan_skip(
                    &order.symbol,
                    MarketSession::Overnight,
                    HedgeScanSkipReason::OvernightUnpriceable,
                    None,
                );
                warn!(
                    target: "hedge",
                    symbol = %order.symbol,
                    %error,
                    "Skipping hedge enqueue: no indicative reference price to preflight against"
                );
                if should_page_overnight_reference_failure(&error) {
                    alert_dead_letter(
                        self.notifier.as_ref(),
                        &self.alerted_dead_letters,
                        &order.symbol,
                        DeadLetterReason::OvernightQuoteFetch,
                        &format!(
                            "Hedge for {} skipped: the overnight indicative quote fetch \
                             failed with a non-retryable classification, leaving no \
                             reference price. The scan keeps skipping it, so the symbol \
                             carries a standing delta until the feed access is fixed.",
                            order.symbol
                        ),
                    )
                    .await;
                }
                return Ok(None);
            }
        };

        let limit_price =
            match apply_slippage(reference.price.inner(), order.direction, slippage_bps) {
                Ok(limit_price) => limit_price,
                Err(error) => {
                    record_scan_skip(
                        &order.symbol,
                        MarketSession::Overnight,
                        HedgeScanSkipReason::SlippageCalculation,
                        None,
                    );
                    warn!(
                        target: "hedge",
                        symbol = %order.symbol,
                        %error,
                        "Skipping hedge enqueue: could not cross the indicative reference price"
                    );
                    return Ok(None);
                }
            };

        self.executor
            .preflight_counter_trade_at_price(order, limit_price)
            .await
            .map(Some)
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
                // aggregate. The stuck-pending recovery later in this tick
                // clears the claim if the aggregate is still missing.
                Ok(None) => {
                    warn!(%symbol, %offchain_order_id, "Pending order aggregate not found during finalization; orphan recovery will handle it");
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
            match &order {
                OffchainOrder::Cancelled { .. }
                | OffchainOrder::Failed { .. }
                | OffchainOrder::Filled { .. } => {
                    self.finalize_position_for_terminal_order(symbol, offchain_order_id, &order)
                        .await;
                }
                OffchainOrder::Pending { .. }
                | OffchainOrder::Submitted { .. }
                | OffchainOrder::PartiallyFilled { .. }
                | OffchainOrder::Cancelling { .. } => {}
            }
        }
    }

    /// While the market is in regular hours, requests broker cancellation of
    /// any still-live extended-hours or overnight limit orders so they can
    /// be replaced with market orders on a subsequent scan. Only symbols
    /// with extended-hours or overnight counter-trading enabled in the
    /// per-asset config are swept; orders for symbols with every session
    /// flag disabled are left to the ops rollback procedure.
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
    async fn request_market_open_cancellations(&self) {
        let all_positions = match self.position_projection.load_all().await {
            Ok(positions) => positions,
            Err(error) => {
                warn!("Failed to load positions for cancel-and-replace: {error}");
                return;
            }
        };

        for (symbol, position) in &all_positions {
            if !(self.ctx.assets.is_extended_hours_enabled(symbol)
                || self.ctx.assets.is_overnight_enabled(symbol))
            {
                continue;
            }

            let Some(offchain_order_id) = position.pending_offchain_order_id else {
                continue;
            };

            let order = match self.offchain_order.load(&offchain_order_id).await {
                Ok(Some(order)) => order,
                // The position references a pending order whose aggregate does
                // not exist yet: `PlaceHedge` claims the position before it
                // creates the offchain-order aggregate, so there is a brief
                // window where the order is "claimed but not recorded". The
                // stuck-pending recovery later in this tick clears the claim if
                // the aggregate is still missing.
                Ok(None) => {
                    warn!(%symbol, %offchain_order_id, "Pending order aggregate not found during cancel-and-replace; orphan recovery will handle it");
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

            // Only live extended-hours and overnight orders need cancelling:
            // any limit surviving into regular hours converges to the
            // regular market-order behavior (SPEC "Session boundaries").
            // Terminal orders are handled by
            // finalize_terminal_pending_positions, and orders already
            // Cancelling are awaiting the poller's confirmation -- both are
            // skipped here, which is what makes the every-tick sweep
            // idempotent.
            match &order {
                OffchainOrder::Submitted {
                    market_session: MarketSession::Extended | MarketSession::Overnight,
                    ..
                }
                | OffchainOrder::PartiallyFilled {
                    market_session: MarketSession::Extended | MarketSession::Overnight,
                    ..
                } => {}
                OffchainOrder::Submitted { .. }
                | OffchainOrder::PartiallyFilled { .. }
                | OffchainOrder::Pending { .. }
                | OffchainOrder::Cancelling { .. }
                | OffchainOrder::Filled { .. }
                | OffchainOrder::Failed { .. }
                | OffchainOrder::Cancelled { .. } => {
                    continue;
                }
            }

            match self
                .offchain_order
                .send(
                    &offchain_order_id,
                    OffchainOrderCommand::CancelOrder {
                        reason: CancellationReason::MarketOpenReplacement,
                    },
                )
                .await
            {
                Ok(()) => {
                    counter!(
                        "hedge_cancellations_requested_total",
                        "symbol" => symbol.to_string(),
                        "session" => session_metric_label(order.market_session()),
                        "reason" => CancellationReason::MarketOpenReplacement.metric_label()
                    )
                    .increment(1);
                }
                Err(error) => {
                    warn!(
                        %symbol, %offchain_order_id, %error,
                        "Failed to request cancellation of extended-hours order; \
                         will retry next tick"
                    );
                }
            }
        }
    }

    /// Cancels live extended-hours limit orders that have not filled within
    /// the configured timeout, so the position can be released and re-hedged
    /// with a fresh marketable limit on a later scan.
    async fn request_extended_hours_reprice_timeout_cancellations(&self) {
        let Some(timeout_secs) = self.ctx.extended_hours_reprice_timeout_secs else {
            warn!(
                "Extended-hours reprice timeout is absent while extended hours is enabled; \
                 skipping stale limit-order sweep"
            );
            return;
        };
        let ordinary_timeout =
            match chrono::Duration::from_std(Duration::from_secs(timeout_secs.get())) {
                Ok(timeout) => timeout,
                Err(error) => {
                    warn!(
                        %error,
                        timeout_secs = timeout_secs.get(),
                        "Invalid extended-hours reprice timeout; skipping stale limit-order sweep"
                    );
                    return;
                }
            };
        let close_flatten_timeout = match chrono::Duration::from_std(Duration::from_secs(
            self.ctx.close_flatten_reprice_timeout_secs,
        )) {
            Ok(timeout) => timeout,
            Err(error) => {
                warn!(
                    %error,
                    timeout_secs = self.ctx.close_flatten_reprice_timeout_secs,
                    "Invalid close-flatten reprice timeout; skipping stale limit-order sweep"
                );
                return;
            }
        };
        let now = Utc::now();

        self.sweep_live_orders_for_cancellation(
            CancellationReason::ExtendedHoursRepriceTimeout,
            "reprice timeout",
            |symbol| self.ctx.assets.is_extended_hours_enabled(symbol),
            |order| {
                let timeout = extended_hours_reprice_timeout_for_order(
                    order,
                    ordinary_timeout,
                    close_flatten_timeout,
                );
                live_extended_hours_order_is_stale(order, now, timeout)
            },
        )
        .await;
    }

    /// Cancels live overnight limit orders that have not filled within
    /// `overnight_reprice_timeout_secs`, so the position is released and
    /// re-hedged with a fresh limit crossed from a current indicative quote
    /// on a later scan. The overnight twin of
    /// `request_extended_hours_reprice_timeout_cancellations`, with a single
    /// cadence: close flatten is a non-concept overnight.
    async fn request_overnight_reprice_timeout_cancellations(&self) {
        // Present whenever any asset enables overnight (startup validation),
        // and the Overnight arm only runs this sweep when one does; absence
        // is a wiring bug, so skip fail-closed rather than assume a cadence.
        let Some(timeout_secs) = self.ctx.overnight_reprice_timeout_secs else {
            warn!(
                "Overnight reprice timeout is absent while overnight counter-trading is \
                 enabled; skipping stale overnight limit-order sweep"
            );
            return;
        };
        let timeout = match chrono::Duration::from_std(Duration::from_secs(timeout_secs.get())) {
            Ok(timeout) => timeout,
            Err(error) => {
                warn!(
                    %error,
                    timeout_secs = timeout_secs.get(),
                    "Invalid overnight reprice timeout; skipping stale overnight \
                     limit-order sweep"
                );
                return;
            }
        };
        let now = Utc::now();

        self.sweep_live_orders_for_cancellation(
            CancellationReason::OvernightRepriceTimeout,
            "overnight reprice timeout",
            |symbol| self.ctx.assets.is_overnight_enabled(symbol),
            |order| live_overnight_order_is_stale(order, now, timeout),
        )
        .await;
    }

    /// At/after the 04:00 ET pre-market open, cancels every still-live
    /// overnight limit order: it is stale by regime, not by age -- the
    /// indicative feed that priced it no longer governs the venue -- so no
    /// timeout applies. The released position reprices from the
    /// extended-hours reference chain on a later scan; cancel-before-replace
    /// holds because a replacement only places after the poller confirms the
    /// cancellation and the position releases.
    ///
    /// The per-symbol filter accepts either session flag: an overnight-only
    /// symbol's survivor must still be cancelled (its released exposure then
    /// waits for a session it may trade in), and a symbol whose overnight
    /// flag was disabled mid-flight is still converged when its extended
    /// flag remains on.
    async fn request_overnight_session_boundary_cancellations(&self) {
        self.sweep_live_orders_for_cancellation(
            CancellationReason::PreMarketOpenReplacement,
            "pre-market open replacement",
            |symbol| {
                self.ctx.assets.is_overnight_enabled(symbol)
                    || self.ctx.assets.is_extended_hours_enabled(symbol)
            },
            |order| live_overnight_order_placed_at(order).is_some(),
        )
        .await;
    }

    /// Near the extended-hours close before a long gap, cancels any still-live
    /// extended-hours limit hedge that predates the flatten window. The
    /// released position is repeatedly repriced from a fresh quote on the
    /// normal timeout cycle until the venue closes.
    async fn request_extended_hours_close_flatten_cancellations(
        &self,
        close_flatten_window_cache: &mut CloseFlattenWindowCache,
    ) {
        let window = match self
            .active_close_flatten_window(close_flatten_window_cache)
            .await
        {
            Ok(Some(window)) => window,
            Ok(None) | Err(CloseFlattenWindowResolutionError::CachedFailure) => return,
            Err(CloseFlattenWindowResolutionError::Source(error)) => {
                warn!(
                    %error,
                    "Failed to check market session for extended-hours close-flatten sweep; \
                     skipping this tick"
                );
                return;
            }
        };

        self.sweep_live_orders_for_cancellation(
            CancellationReason::ExtendedHoursCloseFlatten,
            "close flatten",
            |symbol| self.ctx.assets.is_extended_hours_enabled(symbol),
            |order| live_extended_hours_order_needs_close_flatten(order, window.started_at),
        )
        .await;
    }

    /// Shared skeleton for the session cancellation sweeps (extended-hours
    /// reprice timeout, close flatten, overnight reprice timeout).
    /// `symbol_enabled` scopes the sweep to the session family's per-symbol
    /// opt-in; `needs_cancellation` decides per order. Broker-backed work is
    /// bounded and concurrent so one slow cancellation cannot serialize the
    /// whole maintenance pass. Every successful cancel request increments
    /// `hedge_cancellations_requested_total{symbol,reason}`, the cadence
    /// signal for the reprice policies.
    async fn sweep_live_orders_for_cancellation(
        &self,
        reason: CancellationReason,
        sweep_label: &str,
        symbol_enabled: impl Fn(&Symbol) -> bool,
        needs_cancellation: impl Fn(&OffchainOrder) -> bool + Sync,
    ) {
        let all_positions = match self.position_projection.load_all().await {
            Ok(positions) => positions,
            Err(error) => {
                warn!(
                    %error, sweep = sweep_label,
                    "Failed to load positions for cancellation sweep"
                );
                return;
            }
        };

        let candidates: Vec<_> = all_positions
            .iter()
            .filter(|(symbol, _)| symbol_enabled(symbol))
            .filter_map(|(symbol, position)| {
                position
                    .pending_offchain_order_id
                    .map(|offchain_order_id| (symbol.clone(), offchain_order_id))
            })
            .collect();
        let needs_cancellation = &needs_cancellation;

        stream::iter(candidates)
            .for_each_concurrent(
                MAX_CONCURRENT_CANCELLATION_REQUESTS,
                |(symbol, offchain_order_id)| async move {
                    let order = match self.offchain_order.load(&offchain_order_id).await {
                        Ok(Some(order)) => order,
                        Ok(None) => {
                            warn!(
                                %symbol, %offchain_order_id, sweep = sweep_label,
                                "Pending order aggregate not found during cancellation \
                                 sweep; orphan recovery will handle it"
                            );
                            return;
                        }
                        Err(error) => {
                            warn!(
                                %symbol, %offchain_order_id, %error, sweep = sweep_label,
                                "Failed to load offchain order for cancellation sweep; \
                                 will retry next tick"
                            );
                            return;
                        }
                    };

                    if order.executor() != self.executor.to_supported_executor()
                        || !needs_cancellation(&order)
                    {
                        return;
                    }

                    match self
                        .offchain_order
                        .send(
                            &offchain_order_id,
                            OffchainOrderCommand::CancelOrder { reason },
                        )
                        .await
                    {
                        Ok(()) => {
                            counter!(
                                "hedge_cancellations_requested_total",
                                "symbol" => symbol.to_string(),
                                "session" => session_metric_label(order.market_session()),
                                "reason" => reason.metric_label()
                            )
                            .increment(1);
                        }
                        Err(error) => {
                            warn!(
                                %symbol, %offchain_order_id, %error, sweep = sweep_label,
                                "Failed to request cancellation of live order; \
                                 will retry next tick"
                            );
                        }
                    }
                },
            )
            .await;
    }

    async fn active_close_flatten_window(
        &self,
        cache: &mut CloseFlattenWindowCache,
    ) -> Result<Option<CloseFlattenWindow>, CloseFlattenWindowResolutionError<E::Error>> {
        match cache {
            CloseFlattenWindowCache::Resolved(window) => Ok(*window),
            CloseFlattenWindowCache::Failed => {
                Err(CloseFlattenWindowResolutionError::CachedFailure)
            }
            CloseFlattenWindowCache::Unresolved => {
                match self.executor.market_session_status().await {
                    Ok(status) => {
                        let window = self.close_flatten_policy.active_window(status, Utc::now());
                        *cache = CloseFlattenWindowCache::Resolved(window);
                        Ok(window)
                    }
                    Err(error) => {
                        *cache = CloseFlattenWindowCache::Failed;
                        Err(CloseFlattenWindowResolutionError::Source(error))
                    }
                }
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
    ) {
        let command = match terminal_position_finalization(order) {
            Some(TerminalPositionFinalization::UnpricedFill { shares_filled }) => {
                error!(
                    %symbol, %offchain_order_id, ?shares_filled,
                    "Terminal order has a partial fill without avg price; position left \
                     pending -- no automated path can finalize this, operator intervention \
                     required"
                );
                return;
            }
            None => {
                warn!(
                    %symbol, %offchain_order_id, state = ?order,
                    "Order in non-terminal state during finalization; skipping"
                );
                return;
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
                    return;
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
        }
    }
}

/// Staleness is measured from `placed_at` even for partially filled orders:
/// fills do not reset the clock, since the remaining shares sit at the
/// original (stale) limit price regardless of fill trickle.
fn live_extended_hours_order_is_stale(
    order: &OffchainOrder,
    now: DateTime<Utc>,
    timeout: chrono::Duration,
) -> bool {
    live_extended_hours_order_placed_at(order)
        .is_some_and(|placed_at| now.signed_duration_since(placed_at) >= timeout)
}

fn extended_hours_reprice_timeout_for_order(
    order: &OffchainOrder,
    ordinary_timeout: chrono::Duration,
    close_flatten_timeout: chrono::Duration,
) -> chrono::Duration {
    match order {
        OffchainOrder::Submitted {
            close_flatten: true,
            ..
        }
        | OffchainOrder::PartiallyFilled {
            close_flatten: true,
            ..
        } => close_flatten_timeout,
        OffchainOrder::Pending { .. }
        | OffchainOrder::Submitted { .. }
        | OffchainOrder::PartiallyFilled { .. }
        | OffchainOrder::Cancelling { .. }
        | OffchainOrder::Filled { .. }
        | OffchainOrder::Failed { .. }
        | OffchainOrder::Cancelled { .. } => ordinary_timeout,
    }
}

fn live_overnight_order_is_stale(
    order: &OffchainOrder,
    now: DateTime<Utc>,
    timeout: chrono::Duration,
) -> bool {
    live_overnight_order_placed_at(order)
        .is_some_and(|placed_at| now.signed_duration_since(placed_at) >= timeout)
}

/// Only Overnight-session orders participate in the overnight reprice
/// sweep, mirroring `live_extended_hours_order_placed_at`: each session
/// family's cadence sweeps only the orders it priced.
fn live_overnight_order_placed_at(order: &OffchainOrder) -> Option<DateTime<Utc>> {
    match order {
        OffchainOrder::Submitted {
            market_session: MarketSession::Overnight,
            placed_at,
            ..
        }
        | OffchainOrder::PartiallyFilled {
            market_session: MarketSession::Overnight,
            placed_at,
            ..
        } => Some(*placed_at),
        OffchainOrder::Pending { .. }
        | OffchainOrder::Submitted { .. }
        | OffchainOrder::PartiallyFilled { .. }
        | OffchainOrder::Cancelling { .. }
        | OffchainOrder::Filled { .. }
        | OffchainOrder::Failed { .. }
        | OffchainOrder::Cancelled { .. } => None,
    }
}

/// Only Extended-session orders participate in the extended stale-limit
/// reprice sweep. Orders recorded with an Overnight session return `None`
/// on purpose: overnight repricing runs on its own cadence
/// (`live_overnight_order_placed_at`, driven by
/// `overnight_reprice_timeout_secs`) rather than inheriting the
/// extended-hours timeout.
fn live_extended_hours_order_placed_at(order: &OffchainOrder) -> Option<DateTime<Utc>> {
    match order {
        OffchainOrder::Submitted {
            market_session: MarketSession::Extended,
            placed_at,
            ..
        }
        | OffchainOrder::PartiallyFilled {
            market_session: MarketSession::Extended,
            placed_at,
            ..
        } => Some(*placed_at),
        OffchainOrder::Pending { .. }
        | OffchainOrder::Submitted { .. }
        | OffchainOrder::PartiallyFilled { .. }
        | OffchainOrder::Cancelling { .. }
        | OffchainOrder::Filled { .. }
        | OffchainOrder::Failed { .. }
        | OffchainOrder::Cancelled { .. } => None,
    }
}

fn live_extended_hours_order_needs_close_flatten(
    order: &OffchainOrder,
    close_window_started_at: DateTime<Utc>,
) -> bool {
    live_extended_hours_order_placed_at(order)
        .is_some_and(|placed_at| placed_at < close_window_started_at)
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
    queue.clone().push(CheckPositions::default()).await?;
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
    use alloy::primitives::{Address, TxHash, address};
    use async_trait::async_trait;
    use rain_math_float::Float;
    use reqwest::StatusCode;
    use sqlx::SqlitePool;
    use std::collections::HashMap;
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Barrier;

    use st0x_config::{
        AssetsConfig, EquitiesConfig, EquityAssetConfig, ExecutionThreshold, OperationMode,
        create_test_ctx_with_order_owner,
    };
    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::{
        AlpacaBrokerApiError, AlpacaMarketDataError, CancellationOutcome, ClientOrderId, Direction,
        ExecutorOrderId, FractionalShares, IndicativeQuote, Inventory, LatestQuote, LimitOrder,
        MockExecutor, MockExecutorCtx, OrderState, Positive, SupportedExecutor, Symbol,
        TryIntoExecutor,
    };
    use st0x_finance::Usd;
    use st0x_float_macro::float;

    use super::*;
    use crate::alerts::{CapturingNotifier, NoopNotifier};
    use crate::offchain::order::poll_status::PollOrderStatusCtx;
    use crate::offchain::order::{
        CounterTradeOrderKind, HandleOrderRejectionJobQueue, OffchainOrder, OffchainOrderCommand,
        OrderPlacementResult, PollOrderStatus, ReconcileOrderFillJobQueue,
    };
    use crate::position::{PositionCommand, TradeId};
    use crate::test_utils::{TEST_POLL_INTERVAL, eligible_overnight_snapshot, setup_test_pools};

    async fn build_ctx(
        pool: SqlitePool,
        apalis_pool: apalis_sqlite::SqlitePool,
        ctx_cfg: Ctx,
        check_interval: Duration,
    ) -> (
        CheckPositionsCtx<MockExecutor>,
        Arc<st0x_event_sorcery::Store<Position>>,
    ) {
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();
        build_ctx_with_executor(pool, apalis_pool, ctx_cfg, check_interval, executor).await
    }

    async fn build_ctx_with_executor(
        pool: SqlitePool,
        apalis_pool: apalis_sqlite::SqlitePool,
        ctx_cfg: Ctx,
        check_interval: Duration,
        executor: MockExecutor,
    ) -> (
        CheckPositionsCtx<MockExecutor>,
        Arc<st0x_event_sorcery::Store<Position>>,
    ) {
        let order_placer: Arc<dyn OrderPlacer> = Arc::new(
            crate::offchain::order::ExecutorOrderPlacer(executor.clone()),
        );
        build_ctx_with_order_placer(
            pool,
            apalis_pool,
            ctx_cfg,
            check_interval,
            executor,
            order_placer,
        )
        .await
    }

    async fn build_ctx_with_order_placer(
        pool: SqlitePool,
        apalis_pool: apalis_sqlite::SqlitePool,
        ctx_cfg: Ctx,
        check_interval: Duration,
        executor: MockExecutor,
        order_placer: Arc<dyn OrderPlacer>,
    ) -> (
        CheckPositionsCtx<MockExecutor>,
        Arc<st0x_event_sorcery::Store<Position>>,
    ) {
        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let (offchain_order, offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(order_placer.clone())
                .await
                .unwrap();

        let close_flatten_policy =
            CloseFlattenPolicy::from_secs(ctx_cfg.extended_hours_close_flatten_window_secs)
                .unwrap();
        let ctx = CheckPositionsCtx {
            executor,
            position: position.clone(),
            position_projection,
            offchain_order,
            offchain_order_projection,
            order_placer,
            counter_trade_submission_lock: Arc::new(Mutex::new(())),
            hedge_queue: HedgeJobQueue::new(&apalis_pool),
            check_positions_queue: CheckPositionsJobQueue::new(&apalis_pool),
            poll_status_queue: PollOrderStatusJobQueue::new(&apalis_pool),
            ctx: ctx_cfg,
            pool,
            check_interval,
            close_flatten_policy,
            close_flatten_ramp: CloseFlattenCrossRamp::new(100, 400).unwrap(),
            poll_interval: TEST_POLL_INTERVAL,
            notifier: Arc::new(NoopNotifier),
            alerted_dead_letters: Arc::new(Mutex::new(HashSet::new())),
        };

        (ctx, position)
    }

    struct CoordinatedCancelOrderPlacer {
        barrier: Arc<Barrier>,
    }

    struct CountingCancelOrderPlacer {
        cancel_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl OrderPlacer for CountingCancelOrderPlacer {
        async fn place_market_order(
            &self,
            _order: MarketOrder,
        ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
            panic!("timeout-absence test does not place market orders")
        }

        async fn place_limit_order(
            &self,
            _order: LimitOrder,
        ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
            panic!("timeout-absence test does not place limit orders")
        }

        async fn cancel_order(
            &self,
            _executor_order_id: &ExecutorOrderId,
        ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
            self.cancel_calls.fetch_add(1, Ordering::SeqCst);
            Ok(CancellationOutcome::Requested)
        }

        async fn get_order_status(
            &self,
            executor_order_id: &ExecutorOrderId,
        ) -> Result<OrderState, Box<dyn std::error::Error + Send + Sync>> {
            Ok(OrderState::Submitted {
                order_id: executor_order_id.clone(),
            })
        }
    }

    #[async_trait]
    impl OrderPlacer for CoordinatedCancelOrderPlacer {
        async fn place_market_order(
            &self,
            _order: MarketOrder,
        ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
            panic!("concurrency test does not place market orders")
        }

        async fn place_limit_order(
            &self,
            _order: LimitOrder,
        ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
            panic!("concurrency test does not place limit orders")
        }

        async fn cancel_order(
            &self,
            _executor_order_id: &ExecutorOrderId,
        ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
            self.barrier.wait().await;
            Ok(CancellationOutcome::Requested)
        }

        async fn get_order_status(
            &self,
            executor_order_id: &ExecutorOrderId,
        ) -> Result<OrderState, Box<dyn std::error::Error + Send + Sync>> {
            Ok(OrderState::Submitted {
                order_id: executor_order_id.clone(),
            })
        }
    }

    #[tokio::test]
    async fn periodic_scan_redrives_stuck_pending_order() {
        // ADR 0014: a position claimed by a Pending order (placement crashed
        // before the broker outcome committed) is re-driven at RUNTIME by the
        // periodic scan. CheckPositions itself skips pending-claimed positions
        // (is_ready_for_execution short-circuits), so without this sweep the
        // order would sit Pending until the next bot restart.
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Disabled);
        let (ctx, position) = build_ctx(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
        )
        .await;

        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        // Claim the position, then leave the order Pending (Place committed, broker
        // outcome never recorded) -- the crash window ADR 0014 recovers.
        let offchain_order_id = OffchainOrderId::new();
        let shares = Positive::new(FractionalShares::new(float!(2.0))).unwrap();
        position
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
        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares,
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    client_order_id: st0x_execution::ClientOrderId::from_uuid(
                        offchain_order_id.as_uuid(),
                    ),
                    kind: crate::offchain::order::CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            ctx.offchain_order
                .load(&offchain_order_id)
                .await
                .unwrap()
                .unwrap(),
            OffchainOrder::Pending { .. }
        ));

        ctx.scan_and_enqueue(&mut CloseFlattenWindowCache::default())
            .await
            .unwrap();

        // The periodic recovery re-drove the placement (the noop broker accepts),
        // so the stuck order reaches Submitted at runtime instead of waiting for a
        // restart.
        assert!(matches!(
            ctx.offchain_order
                .load(&offchain_order_id)
                .await
                .unwrap()
                .unwrap(),
            OffchainOrder::Submitted { .. }
        ));

        // ...and a PollOrderStatus job was enqueued for it. Without this the
        // runtime-recovered order would sit Submitted, unpolled, until the next
        // restart (the gap the submitted-order catch-up closes).
        assert_eq!(
            count_jobs(&apalis_pool, &poll_status_job_type()).await,
            1,
            "the runtime re-drive must enqueue a PollOrderStatus so the recovered \
             order is polled to a fill instead of waiting for the next restart"
        );
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
                    block_number: None,
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

    fn poll_status_job_type() -> String {
        std::any::type_name::<crate::offchain::order::PollOrderStatus>().to_string()
    }

    fn check_positions_job_type() -> String {
        std::any::type_name::<CheckPositions>().to_string()
    }

    fn dry_run_ctx(symbols: &[&str], extended_hours: OperationMode) -> Ctx {
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
                    extended_hours_counter_trading: extended_hours,
                    overnight_counter_trading: OperationMode::Disabled,
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
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Disabled);

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

        let executor = MockExecutor::with_failure("connection refused");
        let order_placer: Arc<dyn crate::offchain::order::OrderPlacer> = Arc::new(
            crate::offchain::order::ExecutorOrderPlacer(executor.clone()),
        );
        let (offchain_order, offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(order_placer.clone())
                .await
                .unwrap();

        let close_flatten_policy =
            CloseFlattenPolicy::from_secs(cfg.extended_hours_close_flatten_window_secs).unwrap();
        let ctx = CheckPositionsCtx {
            executor,
            position: position.clone(),
            position_projection,
            offchain_order,
            offchain_order_projection,
            order_placer,
            counter_trade_submission_lock: Arc::new(Mutex::new(())),
            hedge_queue: HedgeJobQueue::new(&apalis_pool),
            check_positions_queue: CheckPositionsJobQueue::new(&apalis_pool),
            poll_status_queue: PollOrderStatusJobQueue::new(&apalis_pool),
            ctx: cfg,
            pool: pool.clone(),
            check_interval: Duration::from_secs(60),
            close_flatten_policy,
            close_flatten_ramp: CloseFlattenCrossRamp::new(100, 400).unwrap(),
            poll_interval: TEST_POLL_INTERVAL,
            notifier: Arc::new(NoopNotifier),
            alerted_dead_letters: Arc::new(Mutex::new(HashSet::new())),
        };

        CheckPositions {}.perform(&ctx).await.unwrap();

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
        let cfg = dry_run_ctx(&["AAPL", "TSLA"], OperationMode::Disabled);
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

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(count_jobs(&apalis_pool, &hedge_job_type()).await, 2);
    }

    #[tokio::test]
    async fn no_positions_above_threshold_enqueues_no_hedge_jobs() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Disabled);
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

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(count_jobs(&apalis_pool, &hedge_job_type()).await, 0);
    }

    #[tokio::test]
    async fn reschedules_itself_with_configured_interval() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Disabled);
        let interval = Duration::from_secs(42);
        let (ctx, _position) = build_ctx(pool.clone(), apalis_pool.clone(), cfg, interval).await;

        CheckPositions::default().perform(&ctx).await.unwrap();

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
        record_extended_hours_order_at(ctx, symbol, offchain_order_id, chrono::Utc::now()).await;
    }

    async fn record_extended_hours_order_at(
        ctx: &CheckPositionsCtx<MockExecutor>,
        symbol: &Symbol,
        offchain_order_id: OffchainOrderId,
        placed_at: chrono::DateTime<chrono::Utc>,
    ) {
        record_extended_hours_order_with_close_flatten_at(
            ctx,
            symbol,
            offchain_order_id,
            placed_at,
            false,
        )
        .await;
    }

    async fn record_extended_hours_order_with_close_flatten_at(
        ctx: &CheckPositionsCtx<MockExecutor>,
        symbol: &Symbol,
        offchain_order_id: OffchainOrderId,
        placed_at: chrono::DateTime<chrono::Utc>,
        close_flatten: bool,
    ) {
        let shares = Positive::new(FractionalShares::new(float!(1))).unwrap();
        let limit_price = Positive::new(Usd::new(float!(195.25))).unwrap();

        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::PlaceAt {
                    symbol: symbol.clone(),
                    shares,
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    client_order_id: ClientOrderId::from_uuid(offchain_order_id.as_uuid()),
                    kind: CounterTradeOrderKind::ExtendedHoursLimit {
                        limit_price,
                        close_flatten,
                        reference_price: None,
                    },
                    placed_at,
                },
            )
            .await
            .unwrap();
        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("broker-eh-1"),
                    placed_shares: shares,
                    submitted_at: chrono::Utc::now(),
                    is_extended_hours: true,
                    limit_price: Some(limit_price),
                },
            )
            .await
            .unwrap();
    }

    /// Records a live overnight limit order aggregate for a previously
    /// claimed position: the overnight twin of
    /// `record_extended_hours_order_at`.
    async fn record_overnight_order_at(
        ctx: &CheckPositionsCtx<MockExecutor>,
        symbol: &Symbol,
        offchain_order_id: OffchainOrderId,
        placed_at: chrono::DateTime<chrono::Utc>,
    ) {
        let shares = Positive::new(FractionalShares::new(float!(1))).unwrap();
        let limit_price = Positive::new(Usd::new(float!(195.25))).unwrap();

        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::PlaceAt {
                    symbol: symbol.clone(),
                    shares,
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    client_order_id: ClientOrderId::from_uuid(offchain_order_id.as_uuid()),
                    kind: CounterTradeOrderKind::OvernightLimit {
                        limit_price,
                        snapshot: eligible_overnight_snapshot(),
                        reference_price: None,
                    },
                    placed_at,
                },
            )
            .await
            .unwrap();
        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("broker-ovn-1"),
                    placed_shares: shares,
                    submitted_at: chrono::Utc::now(),
                    is_extended_hours: true,
                    limit_price: Some(limit_price),
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn stale_overnight_order_is_cancelled_for_reprice() {
        // A live overnight limit that sits unfilled past
        // `overnight_reprice_timeout_secs` must be cancelled during the
        // Overnight session so the next scan reprices it from a current
        // indicative quote, and the cancel request must be counted so the
        // cadence is observable.
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = overnight_ctx(&["AAPL"]);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Overnight)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-ovn-1"),
                }),
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
        record_overnight_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(301),
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        let OffchainOrder::Cancelling { reason, .. } = order else {
            panic!("stale overnight order must be cancelling, got: {order:?}");
        };
        assert_eq!(reason, CancellationReason::OvernightRepriceTimeout);

        let rendered = metrics_handle.render();
        assert!(
            rendered.contains("hedge_cancellations_requested_total{")
                && rendered.contains("reason=\"overnight_reprice_timeout\"")
                && rendered.contains("session=\"overnight\""),
            "the cancel request must be counted with its reason and session, in:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn fresh_overnight_order_survives_the_reprice_sweep() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = overnight_ctx(&["AAPL"]);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Overnight)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-ovn-1"),
                }),
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
        record_overnight_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(10),
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        assert!(
            matches!(order, OffchainOrder::Submitted { .. }),
            "an overnight order inside its cadence must keep broker time priority, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn overnight_reprice_sweep_ignores_extended_session_orders() {
        // Each session family's cadence sweeps only the orders it priced: an
        // extended-hours order observed during an Overnight tick belongs to
        // the 04:00/09:30 policies (or the broker's own day-order expiry),
        // never to the overnight cadence.
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = overnight_ctx(&["AAPL"]);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Overnight)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-eh-1"),
                }),
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
        record_extended_hours_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(3_000),
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        assert!(
            matches!(order, OffchainOrder::Submitted { .. }),
            "the overnight cadence must not cancel an extended-session order, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn missing_overnight_reprice_timeout_skips_the_sweep() {
        // The knob is present whenever any asset enables overnight (startup
        // validation); its absence here is a wiring bug and the sweep must
        // fail closed -- skip with a warning -- rather than assume a cadence.
        let (pool, apalis_pool) = setup_test_pools().await;
        let mut cfg = overnight_ctx(&["AAPL"]);
        cfg.overnight_reprice_timeout_secs = None;
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Overnight)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-ovn-1"),
                }),
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
        record_overnight_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(3_000),
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        assert!(
            matches!(order, OffchainOrder::Submitted { .. }),
            "a missing cadence must skip the sweep, not cancel on an assumed one, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn overnight_reprice_sweep_skips_a_symbol_disabled_mid_flight() {
        // The accepted, pre-existing gap shared with the extended sweeps:
        // sweeps key off the CURRENT config flags, so a symbol disabled with
        // an order still live keeps that order (the ops runbook's rollback
        // note owns the manual cancel). MSFT's overnight flag is off; AAPL's
        // keeps `any_overnight_enabled()` true so the sweep itself runs.
        let (pool, apalis_pool) = setup_test_pools().await;
        let mut cfg = overnight_ctx(&["AAPL", "MSFT"]);
        cfg.assets
            .equities
            .symbols
            .get_mut(&Symbol::new("MSFT").unwrap())
            .unwrap()
            .overnight_counter_trading = OperationMode::Disabled;
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Overnight)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-ovn-1"),
                }),
        )
        .await;

        let msft = Symbol::new("MSFT").unwrap();
        accumulate_position(
            &position,
            &msft,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let offchain_order_id = OffchainOrderId::new();
        claim_position(&ctx, &msft, offchain_order_id).await;
        record_overnight_order_at(
            &ctx,
            &msft,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(3_000),
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        assert!(
            matches!(order, OffchainOrder::Submitted { .. }),
            "a disabled symbol's live order is left to the rollback procedure, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn overnight_reprice_sweep_leaves_a_cancelling_order_untouched() {
        // Idempotency across ticks: an order already awaiting the poller's
        // cancellation confirmation must not be re-sent a CancelOrder (which
        // would churn its recorded reason).
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = overnight_ctx(&["AAPL"]);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Overnight)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-ovn-1"),
                }),
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
        record_overnight_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(3_000),
        )
        .await;
        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::CancelOrder {
                    reason: CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();

        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        let OffchainOrder::Cancelling { reason, .. } = order else {
            panic!("expected the order to stay Cancelling, got: {order:?}");
        };
        assert_eq!(
            reason,
            CancellationReason::MarketOpenReplacement,
            "the sweep must not overwrite an in-flight cancellation's reason"
        );
    }

    #[tokio::test]
    async fn overnight_order_is_cancelled_at_the_pre_market_open() {
        // Only ten seconds old, far inside any reprice cadence: an overnight
        // limit observed during the Extended session is stale by REGIME, so
        // the boundary sweep cancels it unconditionally. The overnight_ctx
        // has extended hours disabled, which also pins that an
        // overnight-only symbol's survivor is swept.
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = overnight_ctx(&["AAPL"]);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Extended)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-ovn-1"),
                }),
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
        record_overnight_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(10),
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        let OffchainOrder::Cancelling { reason, .. } = order else {
            panic!("an overnight order past 04:00 must be cancelling, got: {order:?}");
        };
        assert_eq!(reason, CancellationReason::PreMarketOpenReplacement);

        let rendered = metrics_handle.render();
        assert!(
            rendered.contains("hedge_cancellations_requested_total{")
                && rendered.contains("reason=\"pre_market_open_replacement\"")
                && rendered.contains("session=\"overnight\""),
            "the boundary cancel request must be counted with its reason and session, \
             in:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn pre_market_boundary_sweep_ignores_extended_session_orders() {
        // The boundary sweep converges only orders the overnight feed
        // priced; an extended-hours order during the Extended session is in
        // its own regime and belongs to the extended cadences.
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = overnight_ctx(&["AAPL"]);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Extended)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-eh-1"),
                }),
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
        record_extended_hours_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(3_000),
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        assert!(
            matches!(order, OffchainOrder::Submitted { .. }),
            "the pre-market boundary sweep must not touch extended-session orders, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn overnight_survivor_of_a_disabled_symbol_is_converged_at_pre_market() {
        // Full overnight rollback: the symbol's overnight flag is off again
        // (dry_run_ctx enables only extended hours) while its overnight
        // order is still live. The boundary sweep runs ungated and the
        // per-symbol filter accepts the extended flag, so the stranded
        // survivor still converges instead of waiting for a manual cancel.
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Extended)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-ovn-1"),
                }),
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
        record_overnight_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(10),
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        let OffchainOrder::Cancelling { reason, .. } = order else {
            panic!("a rollback-stranded overnight order must still converge, got: {order:?}");
        };
        assert_eq!(reason, CancellationReason::PreMarketOpenReplacement);
    }

    #[tokio::test]
    async fn pre_market_cancel_converges_to_an_extended_hours_rehedge() {
        // The full 04:00 loop the SPEC promises: boundary cancel (tick 1),
        // the poller's cancellation confirmation, then release and re-hedge
        // on a later tick -- with the replacement enqueued only after the
        // original went terminal (cancel-before-replace), carrying the
        // Extended session so the job prices from the extended-hours chain.
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Extended)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-ovn-1"),
                })
                .with_inventory(Inventory {
                    positions: vec![st0x_execution::EquityPosition {
                        symbol: Symbol::new("AAPL").unwrap(),
                        quantity: FractionalShares::new(float!(10.0)),
                        market_value: None,
                    }],
                    usd_balance_cents: 1_000_000,
                    cash_buying_power_cents: Some(1_000_000),
                    alpaca_usdc: None,
                    cash_withdrawable_cents: None,
                }),
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
        record_overnight_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(10),
        )
        .await;

        // Tick 1: the boundary sweep requests the cancel; the position stays
        // claimed, so no replacement is enqueued yet.
        CheckPositions::default().perform(&ctx).await.unwrap();
        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            0,
            "no replacement may exist while the original order is still live"
        );

        // The poller observes the broker cancellation and confirms it.
        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::ConfirmCancellation {
                    filled_shares: FractionalShares::ZERO,
                    cancelled_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        // Tick 2: finalization releases the position, and the same tick's
        // scan re-hedges it for the Extended session.
        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            1,
            "the released exposure must be re-hedged on the next scan"
        );
        let job_bytes: Vec<u8> =
            sqlx_apalis::query_scalar("SELECT job FROM Jobs WHERE job_type = ?")
                .bind(hedge_job_type())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let job: PlaceHedge = serde_json::from_slice(&job_bytes).unwrap();
        assert_eq!(
            job.market_session,
            MarketSession::Extended,
            "the replacement must carry the Extended session so it prices from the \
             extended-hours chain, not the overnight feed"
        );
    }

    #[tokio::test]
    async fn regular_tick_converges_a_surviving_overnight_order() {
        // The 09:30 policy: any limit surviving into regular hours converges
        // to the regular market-order behavior, overnight orders included.
        // The overnight_ctx has extended hours disabled, so this also pins
        // the widened per-symbol filter -- an overnight-only symbol's
        // survivor must not be skipped by the old extended-hours-only check.
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = overnight_ctx(&["AAPL"]);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Regular)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-ovn-1"),
                }),
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
        record_overnight_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(10),
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        let OffchainOrder::Cancelling { reason, .. } = order else {
            panic!("an overnight order surviving into regular hours must converge, got: {order:?}");
        };
        assert_eq!(reason, CancellationReason::MarketOpenReplacement);
    }

    #[tokio::test]
    async fn market_open_cancel_converges_to_a_regular_market_order_rehedge() {
        // The full 09:30 loop for an overnight-only symbol: the sweep
        // cancels the survivor, the poller confirms, and the next tick
        // re-hedges the released exposure -- regular-hours readiness gates
        // only on the `trading` flag, so the overnight-only symbol still
        // converges to the regular market-order behavior SPEC promises.
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = overnight_ctx(&["AAPL"]);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Regular)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-ovn-1"),
                })
                .with_inventory(Inventory {
                    positions: vec![st0x_execution::EquityPosition {
                        symbol: Symbol::new("AAPL").unwrap(),
                        quantity: FractionalShares::new(float!(10.0)),
                        market_value: None,
                    }],
                    usd_balance_cents: 1_000_000,
                    cash_buying_power_cents: Some(1_000_000),
                    alpaca_usdc: None,
                    cash_withdrawable_cents: None,
                }),
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
        record_overnight_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(10),
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();
        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            0,
            "no replacement may exist while the original order is still live"
        );

        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::ConfirmCancellation {
                    filled_shares: FractionalShares::ZERO,
                    cancelled_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            1,
            "the released exposure must be re-hedged on the next regular-hours scan"
        );
        let job_bytes: Vec<u8> =
            sqlx_apalis::query_scalar("SELECT job FROM Jobs WHERE job_type = ?")
                .bind(hedge_job_type())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let job: PlaceHedge = serde_json::from_slice(&job_bytes).unwrap();
        assert_eq!(
            job.market_session,
            MarketSession::Regular,
            "the replacement must carry the Regular session so it places as a market order"
        );
    }

    #[tokio::test]
    async fn broker_auto_cancel_at_2000_converges_to_an_overnight_rehedge() {
        // The 20:00 entry: Alpaca cancels unfilled day orders at 20:00, and
        // the bot never assumes it -- the poller observes the broker-side
        // cancellation (recorded here via MarkUnrequestedCancellation, the
        // command that observation lands as), the position releases, and the
        // next Overnight tick re-hedges through the overnight path. Until
        // the terminal observation arrives, the claim holds and no
        // replacement exists.
        let (pool, apalis_pool) = setup_test_pools().await;
        let mut cfg = overnight_ctx(&["AAPL"]);
        // The surviving day order is an extended-hours limit from the prior
        // cycle, so the symbol legitimately has both flags on.
        cfg.assets
            .equities
            .symbols
            .get_mut(&Symbol::new("AAPL").unwrap())
            .unwrap()
            .extended_hours_counter_trading = OperationMode::Enabled;
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Overnight)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-eh-1"),
                })
                .with_inventory(Inventory {
                    positions: vec![st0x_execution::EquityPosition {
                        symbol: Symbol::new("AAPL").unwrap(),
                        quantity: FractionalShares::new(float!(10.0)),
                        market_value: None,
                    }],
                    usd_balance_cents: 1_000_000,
                    cash_buying_power_cents: Some(1_000_000),
                    alpaca_usdc: None,
                    cash_withdrawable_cents: None,
                }),
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
        record_extended_hours_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(3_000),
        )
        .await;

        // Overnight tick while the broker cancellation is still unobserved:
        // the claim holds, nothing is enqueued, and no sweep touches the
        // extended-session order.
        CheckPositions::default().perform(&ctx).await.unwrap();
        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            0,
            "a lagging broker cancellation must delay the overnight hedge, never double it"
        );

        // The poller observes the broker's 20:00 auto-cancel: no local
        // cancel request exists, so it lands as an unrequested cancellation.
        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::MarkUnrequestedCancellation {
                    filled_shares: FractionalShares::ZERO,
                    cancelled_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        // Next Overnight tick: finalization releases, the scan re-hedges.
        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            1,
            "released exposure must re-hedge once the auto-cancel is observed"
        );
        let job_bytes: Vec<u8> =
            sqlx_apalis::query_scalar("SELECT job FROM Jobs WHERE job_type = ?")
                .bind(hedge_job_type())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let job: PlaceHedge = serde_json::from_slice(&job_bytes).unwrap();
        assert_eq!(
            job.market_session,
            MarketSession::Overnight,
            "the re-hedge must go through the overnight path"
        );
    }

    #[tokio::test]
    async fn restart_into_extended_hours_cancels_live_overnight_order() {
        // Crash-resume across the 04:00 boundary: the process died before
        // the boundary sweep could run, and the first scan after the restart
        // observes Extended. The level-triggered sweep needs no remembered
        // transition -- it converges the survivor from persisted state alone.
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = overnight_ctx(&["AAPL"]);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Extended)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-ovn-1"),
                }),
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
        record_overnight_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(3_000),
        )
        .await;

        // Simulate the restart: a fresh CheckPositionsCtx over the same
        // persisted state, as the first post-restart tick would have.
        drop(ctx);
        let restarted_cfg = overnight_ctx(&["AAPL"]);
        let (restarted_ctx, _) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            restarted_cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Extended)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-ovn-1"),
                }),
        )
        .await;

        CheckPositions::default()
            .perform(&restarted_ctx)
            .await
            .unwrap();

        let order = restarted_ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        assert!(
            matches!(
                order,
                OffchainOrder::Cancelling {
                    reason: CancellationReason::PreMarketOpenReplacement,
                    ..
                }
            ),
            "restart catch-up must converge a surviving overnight order, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn failed_cancel_request_is_retried_on_the_next_tick() {
        // Level-triggered crash/failure safety: a cancel request that dies at
        // the broker leaves the order Submitted, and the next tick's sweep
        // re-derives the same work from persisted state -- no boundary or
        // cadence decision is lost with the failed call.
        struct FlakyCancelPlacer {
            attempts: AtomicUsize,
        }

        #[async_trait]
        impl OrderPlacer for FlakyCancelPlacer {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("the sweep must never place orders")
            }

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("the sweep must never place orders")
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err("broker 500 on cancel".into());
                }
                Ok(CancellationOutcome::Requested)
            }

            async fn get_order_status(
                &self,
                executor_order_id: &ExecutorOrderId,
            ) -> Result<st0x_execution::OrderState, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(st0x_execution::OrderState::Submitted {
                    order_id: executor_order_id.clone(),
                })
            }
        }

        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = overnight_ctx(&["AAPL"]);
        let placer = Arc::new(FlakyCancelPlacer {
            attempts: AtomicUsize::new(0),
        });
        let (ctx, position) = build_ctx_with_order_placer(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            MockExecutor::new().with_market_session(MarketSession::Overnight),
            placer.clone(),
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
        record_overnight_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(3_000),
        )
        .await;

        // Tick 1: the sweep attempts the cancel and the broker call fails;
        // the order must remain live rather than half-cancelled.
        CheckPositions::default().perform(&ctx).await.unwrap();
        assert_eq!(
            placer.attempts.load(Ordering::SeqCst),
            1,
            "the first tick must have attempted the broker cancel"
        );
        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        assert!(
            matches!(order, OffchainOrder::Submitted { .. }),
            "a failed cancel request must leave the order Submitted, got: {order:?}"
        );

        // Tick 2: level-triggered, the sweep re-requests and succeeds.
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
                OffchainOrder::Cancelling {
                    reason: CancellationReason::OvernightRepriceTimeout,
                    ..
                }
            ),
            "the next tick must retry the failed cancel request, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn closed_tick_leaves_overnight_orders_untouched() {
        // A Closed tick sweeps nothing: no session's limit can be repriced
        // into a closed venue, and the 20:00 entry relies on the broker's
        // day-order auto-cancel observed by the poller, not a sweep.
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = overnight_ctx(&["AAPL"]);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_closed()
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-ovn-1"),
                }),
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
        record_overnight_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(3_000),
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        assert!(
            matches!(order, OffchainOrder::Submitted { .. }),
            "a Closed tick must not cancel a live overnight order, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn stale_extended_hours_order_is_cancelled_for_reprice() {
        // A live extended-hours limit order that sits unfilled past the
        // configured timeout must be cancelled during the extended session so
        // the next scan can place a fresh marketable limit instead of carrying
        // hours of unhedged exposure.
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Extended)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-eh-1"),
                }),
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
        record_extended_hours_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(301),
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        let OffchainOrder::Cancelling { reason, .. } = order else {
            panic!("stale extended-hours order must be cancelling, got: {order:?}");
        };
        assert_eq!(reason, CancellationReason::ExtendedHoursRepriceTimeout);
    }

    #[tokio::test]
    async fn overnight_session_issues_no_cancellation_maintenance() {
        // An extended-hours-only deployment (no overnight opt-in anywhere):
        // the Overnight arm's sweep is gated off, so an Overnight tick runs
        // no cancellation maintenance at all. The order here is stale enough
        // that an Extended tick would cancel it for reprice and a Regular
        // tick would cancel it for replacement -- the Overnight tick must
        // leave it untouched.
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Overnight)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-eh-1"),
                }),
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
        record_extended_hours_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(301),
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        assert!(
            matches!(order, OffchainOrder::Submitted { .. }),
            "an Overnight tick must not cancel a live extended-hours order, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn missing_reprice_timeout_skips_extended_hours_cancellation() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let mut cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        cfg.extended_hours_reprice_timeout_secs = None;
        let cancel_calls = Arc::new(AtomicUsize::new(0));
        let order_placer: Arc<dyn OrderPlacer> = Arc::new(CountingCancelOrderPlacer {
            cancel_calls: cancel_calls.clone(),
        });
        let (ctx, position) = build_ctx_with_order_placer(
            pool,
            apalis_pool,
            cfg,
            Duration::from_secs(60),
            MockExecutor::new().with_market_session(MarketSession::Extended),
            order_placer,
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;
        let offchain_order_id = OffchainOrderId::new();
        claim_position(&ctx, &symbol, offchain_order_id).await;
        record_extended_hours_order_at(
            &ctx,
            &symbol,
            offchain_order_id,
            Utc::now() - chrono::Duration::seconds(301),
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert!(matches!(
            ctx.offchain_order
                .load(&offchain_order_id)
                .await
                .unwrap()
                .unwrap(),
            OffchainOrder::Submitted { .. }
        ));
        assert_eq!(cancel_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn missing_reprice_timeout_skips_close_flatten_cancellation() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let mut cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        cfg.extended_hours_reprice_timeout_secs = None;
        cfg.close_flatten_reprice_timeout_secs = 0;
        let now = Utc::now();
        let (ctx, position) = build_ctx_with_executor(
            pool,
            apalis_pool,
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Extended)
                .with_extended_session_closes_at(now + chrono::Duration::seconds(300))
                .with_post_close_gap(st0x_execution::PostCloseGap::MultiDayClosure)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-eh-1"),
                }),
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;
        let offchain_order_id = OffchainOrderId::new();
        claim_position(&ctx, &symbol, offchain_order_id).await;
        record_extended_hours_order_with_close_flatten_at(
            &ctx,
            &symbol,
            offchain_order_id,
            now - chrono::Duration::seconds(61),
            true,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert!(matches!(
            ctx.offchain_order
                .load(&offchain_order_id)
                .await
                .unwrap()
                .unwrap(),
            OffchainOrder::Submitted { .. }
        ));
    }

    #[tokio::test]
    async fn ordinary_extended_hours_order_keeps_the_five_minute_reprice_cadence() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let (ctx, position) = build_ctx_with_executor(
            pool,
            apalis_pool,
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Extended)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-eh-1"),
                }),
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;
        let offchain_order_id = OffchainOrderId::new();
        claim_position(&ctx, &symbol, offchain_order_id).await;
        record_extended_hours_order_at(
            &ctx,
            &symbol,
            offchain_order_id,
            Utc::now() - chrono::Duration::seconds(61),
        )
        .await;

        ctx.request_extended_hours_reprice_timeout_cancellations()
            .await;

        assert!(matches!(
            ctx.offchain_order
                .load(&offchain_order_id)
                .await
                .unwrap()
                .unwrap(),
            OffchainOrder::Submitted { .. }
        ));
    }

    #[tokio::test]
    async fn close_flatten_order_uses_the_one_minute_reprice_cadence() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let (ctx, position) = build_ctx_with_executor(
            pool,
            apalis_pool,
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Extended)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-eh-1"),
                }),
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;
        let offchain_order_id = OffchainOrderId::new();
        claim_position(&ctx, &symbol, offchain_order_id).await;
        record_extended_hours_order_with_close_flatten_at(
            &ctx,
            &symbol,
            offchain_order_id,
            Utc::now() - chrono::Duration::seconds(61),
            true,
        )
        .await;

        ctx.request_extended_hours_reprice_timeout_cancellations()
            .await;

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(
                order,
                OffchainOrder::Cancelling {
                    reason: CancellationReason::ExtendedHoursRepriceTimeout,
                    ..
                }
            ),
            "a close-flatten order older than its dedicated timeout must be cancelling, got {order:?}"
        );
    }

    #[tokio::test]
    async fn stale_extended_hours_cancellations_run_concurrently() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL", "MSFT"], OperationMode::Enabled);
        let order_placer: Arc<dyn OrderPlacer> = Arc::new(CoordinatedCancelOrderPlacer {
            barrier: Arc::new(Barrier::new(2)),
        });
        let (ctx, position) = build_ctx_with_order_placer(
            pool.clone(),
            apalis_pool,
            cfg,
            Duration::from_secs(60),
            MockExecutor::new().with_market_session(MarketSession::Extended),
            order_placer,
        )
        .await;

        let mut order_ids = Vec::new();
        for ticker in ["AAPL", "MSFT"] {
            let symbol = Symbol::new(ticker).unwrap();
            accumulate_position(
                &position,
                &symbol,
                FractionalShares::new(float!(2.0)),
                Direction::Buy,
            )
            .await;

            let offchain_order_id = OffchainOrderId::new();
            claim_position(&ctx, &symbol, offchain_order_id).await;
            record_extended_hours_order_at(
                &ctx,
                &symbol,
                offchain_order_id,
                chrono::Utc::now() - chrono::Duration::seconds(301),
            )
            .await;
            order_ids.push(offchain_order_id);
        }

        tokio::time::timeout(
            Duration::from_secs(5),
            ctx.request_extended_hours_reprice_timeout_cancellations(),
        )
        .await
        .expect("both cancellation requests should reach the broker concurrently");

        for offchain_order_id in order_ids {
            assert!(matches!(
                ctx.offchain_order
                    .load(&offchain_order_id)
                    .await
                    .unwrap()
                    .unwrap(),
                OffchainOrder::Cancelling {
                    reason: CancellationReason::ExtendedHoursRepriceTimeout,
                    ..
                }
            ));
        }
    }

    #[tokio::test]
    async fn fresh_extended_hours_order_is_not_cancelled_for_reprice() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool,
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Extended)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-eh-1"),
                }),
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
        record_extended_hours_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(240),
        )
        .await;

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
                    market_session: MarketSession::Extended,
                    ..
                }
            ),
            "fresh extended-hours order must stay live, got: {order:?}"
        );
    }

    #[test]
    fn live_extended_hours_order_becomes_stale_at_exact_timeout() {
        let placed_at = chrono::DateTime::<chrono::Utc>::UNIX_EPOCH;
        let timeout = chrono::Duration::seconds(300);
        let order = OffchainOrder::Submitted {
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
            requested_shares: None,
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            executor_order_id: ExecutorOrderId::new("broker-eh-1"),
            placed_at,
            submitted_at: placed_at,
            market_session: MarketSession::Extended,
            close_flatten: false,
            reference_price: None,
        };

        assert!(!live_extended_hours_order_is_stale(
            &order,
            placed_at + timeout - chrono::Duration::nanoseconds(1),
            timeout,
        ));
        assert!(live_extended_hours_order_is_stale(
            &order,
            placed_at + timeout,
            timeout,
        ));
    }

    #[tokio::test]
    async fn close_to_extended_hours_close_cancels_pre_window_order_for_flattening() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let now = chrono::Utc::now();
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool,
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Extended)
                .with_extended_session_closes_at(now + chrono::Duration::seconds(300))
                .with_post_close_gap(st0x_execution::PostCloseGap::MultiDayClosure)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-eh-1"),
                }),
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
        record_extended_hours_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            now - chrono::Duration::seconds(901),
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        let OffchainOrder::Cancelling { reason, .. } = order else {
            panic!("close-to-close extended-hours order must be cancelling, got: {order:?}");
        };
        assert_eq!(reason, CancellationReason::ExtendedHoursCloseFlatten);
    }

    #[tokio::test]
    async fn close_window_replacement_order_is_not_cancelled_again_for_flattening() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let now = chrono::Utc::now();
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool,
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Extended)
                .with_extended_session_closes_at(now + chrono::Duration::seconds(300))
                .with_post_close_gap(st0x_execution::PostCloseGap::MultiDayClosure)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-eh-1"),
                }),
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
        record_extended_hours_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            now + chrono::Duration::seconds(1),
        )
        .await;

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
                    market_session: MarketSession::Extended,
                    ..
                }
            ),
            "replacement hedge placed inside the close window must stay live, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn ordinary_weekday_close_does_not_cancel_order_for_flattening() {
        // The discriminating twin of
        // `close_to_extended_hours_close_cancels_pre_window_order_for_flattening`:
        // the order is backdated identically (901s, predating the would-be
        // window), the tick sits inside that window, and ONLY the gap
        // classification differs. The reprice timeout is raised above the
        // backdating so the flatten sweep is the only candidate canceller --
        // an order this stale would otherwise be repriced (correctly, and
        // with its own reason) on an ordinary evening, masking whether the
        // flatten sweep declined because of the gap.
        let (pool, apalis_pool) = setup_test_pools().await;
        let mut cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        cfg.extended_hours_reprice_timeout_secs = Some(NonZeroU64::new(3_600).unwrap());
        let now = chrono::Utc::now();
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool,
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Extended)
                .with_extended_session_closes_at(now + chrono::Duration::seconds(300))
                .with_post_close_gap(st0x_execution::PostCloseGap::OrdinaryOvernight)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-eh-1"),
                }),
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
        record_extended_hours_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            now - chrono::Duration::seconds(901),
        )
        .await;

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
                    market_session: MarketSession::Extended,
                    ..
                }
            ),
            "ordinary weekday close must not activate close flattening, got: {order:?}"
        );
    }

    /// The RAI-1953 suppression pin: enabling overnight counter-trading for
    /// a symbol must not weaken the Friday/holiday protection. Identical to
    /// `close_to_extended_hours_close_cancels_pre_window_order_for_flattening`
    /// except the symbol also opts into overnight -- the flatten cancel must
    /// fire exactly as for an extended-only symbol.
    #[tokio::test]
    async fn flatten_cancel_still_fires_for_an_overnight_enabled_symbol() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let mut cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        cfg.assets
            .equities
            .symbols
            .get_mut(&Symbol::new("AAPL").unwrap())
            .unwrap()
            .overnight_counter_trading = OperationMode::Enabled;
        cfg.overnight_max_quote_age_secs = Some(NonZeroU64::new(30).unwrap());
        cfg.overnight_slippage_bps = Some(100);
        cfg.overnight_reprice_timeout_secs = Some(NonZeroU64::new(300).unwrap());
        let now = chrono::Utc::now();
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool,
            cfg,
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Extended)
                .with_extended_session_closes_at(now + chrono::Duration::seconds(300))
                .with_post_close_gap(st0x_execution::PostCloseGap::MultiDayClosure)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-eh-1"),
                }),
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
        record_extended_hours_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            now - chrono::Duration::seconds(901),
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        let OffchainOrder::Cancelling { reason, .. } = order else {
            panic!("the overnight opt-in must not suppress close flattening, got: {order:?}");
        };
        assert_eq!(reason, CancellationReason::ExtendedHoursCloseFlatten);
    }

    /// The RAI-1953 dilution pin: inside a flatten window, a buy for a
    /// symbol with BOTH session flags on must still route through the
    /// extended/flatten machinery, never the overnight branch. The overnight
    /// quote is priced absurdly high ($10,000 ask): if the overnight branch
    /// stole the buy, its crossed preflight would demand ~$20,200 against
    /// $250 of cash and block. The extended path preflights the mark's
    /// ramped cross (at most $104 x 2 = $208), which the cash covers, so the
    /// hedge enqueues.
    #[tokio::test]
    async fn flatten_window_buy_ignores_the_overnight_feed() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let mut cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        cfg.assets
            .equities
            .symbols
            .get_mut(&Symbol::new("AAPL").unwrap())
            .unwrap()
            .overnight_counter_trading = OperationMode::Enabled;
        cfg.overnight_max_quote_age_secs = Some(NonZeroU64::new(30).unwrap());
        cfg.overnight_slippage_bps = Some(100);
        cfg.overnight_reprice_timeout_secs = Some(NonZeroU64::new(300).unwrap());
        let now = chrono::Utc::now();
        let executor = MockExecutor::new()
            .with_market_session(MarketSession::Extended)
            .with_extended_session_closes_at(now + chrono::Duration::seconds(300))
            .with_post_close_gap(st0x_execution::PostCloseGap::MultiDayClosure)
            .with_position_mark(Positive::new(Usd::new(float!(100.0))).unwrap())
            .with_overnight_quote(indicative_quote_at("9999", "10000", now))
            .with_inventory(Inventory {
                positions: Vec::new(),
                usd_balance_cents: 25_000,
                cash_buying_power_cents: Some(25_000),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            });
        let (ctx, position) = build_ctx_with_executor(
            pool,
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            executor,
        )
        .await;

        let aapl = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &aapl,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            1,
            "the flatten-window buy must preflight the extended reference, not the \
             overnight quote"
        );
        let job_bytes: Vec<u8> =
            sqlx_apalis::query_scalar("SELECT job FROM Jobs WHERE job_type = ?")
                .bind(hedge_job_type())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let job: PlaceHedge = serde_json::from_slice(&job_bytes).unwrap();
        assert_eq!(
            job.market_session,
            MarketSession::Extended,
            "the job must carry the Extended session so placement uses the flatten pricing"
        );
    }

    /// The RAI-1953 long-closure sequence, driving the SPEC's unreachability
    /// argument tick by tick: a Thursday-night overnight order survives into
    /// Friday, the 04:00 boundary sweep converges it on the FIRST Extended
    /// tick (hours before any flatten window), the flatten window then finds
    /// no overnight order and re-hedges the released exposure through the
    /// extended machinery, and the Closed 20:00 places nothing. Each phase
    /// rebuilds the ctx over the same pools (the mock session is fixed at
    /// construction), so every transition doubles as restart coverage.
    /// Throughout: never two live orders, exactly one replacement hedge.
    #[tokio::test]
    async fn thursday_overnight_order_converges_before_the_friday_flatten() {
        fn friday_cfg() -> Ctx {
            let mut cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
            cfg.assets
                .equities
                .symbols
                .get_mut(&Symbol::new("AAPL").unwrap())
                .unwrap()
                .overnight_counter_trading = OperationMode::Enabled;
            cfg.overnight_max_quote_age_secs = Some(NonZeroU64::new(30).unwrap());
            cfg.overnight_slippage_bps = Some(100);
            cfg.overnight_reprice_timeout_secs = Some(NonZeroU64::new(300).unwrap());
            cfg
        }

        let (pool, apalis_pool) = setup_test_pools().await;
        let now = chrono::Utc::now();

        // Phase 1: Friday pre-market. The extended close is hours away, so
        // no flatten window is active; the boundary sweep alone converges
        // the Thursday-night survivor.
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            friday_cfg(),
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Extended)
                .with_extended_session_closes_at(now + chrono::Duration::hours(8))
                .with_post_close_gap(st0x_execution::PostCloseGap::MultiDayClosure)
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-ovn-1"),
                }),
        )
        .await;

        let aapl = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &aapl,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;

        let offchain_order_id = OffchainOrderId::new();
        claim_position(&ctx, &aapl, offchain_order_id).await;
        record_overnight_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            now - chrono::Duration::hours(10),
        )
        .await;

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
                OffchainOrder::Cancelling {
                    reason: CancellationReason::PreMarketOpenReplacement,
                    ..
                }
            ),
            "the first Friday Extended tick must converge the Thursday survivor, got: {order:?}"
        );
        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            0,
            "no replacement may exist while the overnight order is still live"
        );

        // The poller confirms the broker cancellation.
        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::ConfirmCancellation {
                    filled_shares: FractionalShares::ZERO,
                    cancelled_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
        drop(ctx);

        // Phase 2: the Friday flatten window. No overnight order remains for
        // it to worry about; the released exposure re-hedges through the
        // extended/flatten machinery (mark-priced, ramped cross).
        let (ctx, _) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            friday_cfg(),
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_session(MarketSession::Extended)
                .with_extended_session_closes_at(
                    chrono::Utc::now() + chrono::Duration::seconds(300),
                )
                .with_post_close_gap(st0x_execution::PostCloseGap::MultiDayClosure)
                .with_position_mark(Positive::new(Usd::new(float!(100.0))).unwrap())
                .with_inventory(Inventory {
                    positions: Vec::new(),
                    usd_balance_cents: 25_000,
                    cash_buying_power_cents: Some(25_000),
                    alpaca_usdc: None,
                    cash_withdrawable_cents: None,
                }),
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let order = ctx
            .offchain_order
            .load(&offchain_order_id)
            .await
            .unwrap()
            .expect("order should exist");
        assert!(
            matches!(order, OffchainOrder::Cancelled { .. }),
            "the overnight order must be terminal before any replacement, got: {order:?}"
        );
        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            1,
            "the flatten-window scan must re-hedge the released exposure exactly once"
        );
        let job_bytes: Vec<u8> =
            sqlx_apalis::query_scalar("SELECT job FROM Jobs WHERE job_type = ?")
                .bind(hedge_job_type())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let job: PlaceHedge = serde_json::from_slice(&job_bytes).unwrap();
        assert_eq!(job.market_session, MarketSession::Extended);
        drop(ctx);

        // Phase 3: Friday 20:00 classifies Closed -- no overnight session
        // before a long gap, and the scan places nothing new.
        let (ctx, _) = build_ctx_with_executor(
            pool,
            apalis_pool.clone(),
            friday_cfg(),
            Duration::from_secs(60),
            MockExecutor::new()
                .with_market_closed()
                .with_order_status(OrderState::Submitted {
                    order_id: ExecutorOrderId::new("broker-ovn-1"),
                }),
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            1,
            "the Closed Friday evening must not create another hedge"
        );
    }

    #[tokio::test]
    async fn close_flatten_inventory_block_is_signalled_without_enqueueing() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let executor = MockExecutor::new()
            .with_market_session(MarketSession::Extended)
            .with_extended_session_closes_at(chrono::Utc::now() + chrono::Duration::seconds(300))
            .with_post_close_gap(st0x_execution::PostCloseGap::MultiDayClosure)
            .with_inventory(Inventory {
                positions: Vec::new(),
                usd_balance_cents: 100_000,
                cash_buying_power_cents: Some(100_000),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            });
        let (ctx, position) = build_ctx_with_executor(
            pool,
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            executor,
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(count_jobs(&apalis_pool, &hedge_job_type()).await, 0);
        let rendered = metrics_handle.render();
        assert!(rendered.contains("close_flatten_blocked_total{"));
        assert!(rendered.contains("reason=\"insufficient_equity\""));
        assert!(rendered.contains("symbol=\"AAPL\""));
    }

    #[tokio::test]
    async fn close_flatten_buying_power_block_is_signalled_without_enqueueing() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let executor = MockExecutor::new()
            .with_market_session(MarketSession::Extended)
            .with_extended_session_closes_at(chrono::Utc::now() + chrono::Duration::seconds(300))
            .with_post_close_gap(st0x_execution::PostCloseGap::MultiDayClosure)
            .with_latest_quote(
                st0x_execution::LatestQuote::new(
                    Positive::new(Usd::new(float!(99.0))).unwrap(),
                    Positive::new(Usd::new(float!(100.0))).unwrap(),
                )
                .unwrap(),
            )
            .with_inventory(Inventory {
                positions: Vec::new(),
                usd_balance_cents: 0,
                cash_buying_power_cents: Some(0),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            });
        let (ctx, position) = build_ctx_with_executor(
            pool,
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            executor,
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(count_jobs(&apalis_pool, &hedge_job_type()).await, 0);
        let rendered = metrics_handle.render();
        assert!(rendered.contains("close_flatten_blocked_total{"));
        assert!(rendered.contains("reason=\"insufficient_buying_power\""));
        assert!(rendered.contains("symbol=\"AAPL\""));
    }

    /// Regression test for the preflight/placement price mismatch: the
    /// close-flatten placement path (`select_order_kind_for_current_session`)
    /// prices a buy off the current ask, not the latest trade price. Before
    /// this fix the cash preflight checked the (stale, lower) trade price,
    /// so a widening extended-hours spread could pass preflight while the
    /// order actually submitted needed far more buying power than was
    /// checked. Here the mock's trade price ($10, `with_preflight_price`)
    /// would pass easily; the ask ($1,000, `with_latest_quote`) must be what
    /// actually gets checked and rejected.
    #[tokio::test]
    async fn close_flatten_buy_preflight_uses_ask_not_stale_trade_price() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let executor = MockExecutor::new()
            .with_market_session(MarketSession::Extended)
            .with_extended_session_closes_at(chrono::Utc::now() + chrono::Duration::seconds(300))
            .with_post_close_gap(st0x_execution::PostCloseGap::MultiDayClosure)
            .with_preflight_price(float!(10.0))
            .with_latest_quote(
                st0x_execution::LatestQuote::new(
                    Positive::new(Usd::new(float!(999.0))).unwrap(),
                    Positive::new(Usd::new(float!(1_000.0))).unwrap(),
                )
                .unwrap(),
            )
            .with_inventory(Inventory {
                positions: Vec::new(),
                // Covers the trade-price estimate (~$20.20) but nowhere near
                // the ask-price estimate (~$2,020.00).
                usd_balance_cents: 2_100,
                cash_buying_power_cents: Some(2_100),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            });
        let (ctx, position) = build_ctx_with_executor(
            pool,
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            executor,
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            0,
            "the ask-priced preflight must block a buy the stale trade-price preflight would have allowed"
        );
        let rendered = metrics_handle.render();
        assert!(rendered.contains("close_flatten_blocked_total{"));
        assert!(rendered.contains("reason=\"insufficient_buying_power\""));
        assert!(rendered.contains("symbol=\"AAPL\""));
    }

    /// Companion to the block test above: when cash covers the ask-priced
    /// estimate too, the close-flatten buy must still be enqueued normally.
    #[tokio::test]
    async fn close_flatten_buy_preflight_allows_when_cash_covers_ask_price() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let executor = MockExecutor::new()
            .with_market_session(MarketSession::Extended)
            .with_extended_session_closes_at(chrono::Utc::now() + chrono::Duration::seconds(300))
            .with_post_close_gap(st0x_execution::PostCloseGap::MultiDayClosure)
            .with_preflight_price(float!(10.0))
            .with_latest_quote(
                st0x_execution::LatestQuote::new(
                    Positive::new(Usd::new(float!(99.0))).unwrap(),
                    Positive::new(Usd::new(float!(100.0))).unwrap(),
                )
                .unwrap(),
            )
            .with_inventory(Inventory {
                positions: Vec::new(),
                usd_balance_cents: 100_000,
                cash_buying_power_cents: Some(100_000),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            });
        let (ctx, position) = build_ctx_with_executor(
            pool,
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            executor,
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            1,
            "sufficient cash at the ask price must still enqueue the close-flatten buy"
        );
    }

    #[tokio::test]
    async fn close_flatten_buy_without_quote_fails_closed_before_enqueue() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let executor = MockExecutor::new()
            .with_market_session(MarketSession::Extended)
            .with_extended_session_closes_at(chrono::Utc::now() + chrono::Duration::seconds(300))
            .with_post_close_gap(st0x_execution::PostCloseGap::MultiDayClosure)
            .with_inventory(Inventory {
                positions: Vec::new(),
                usd_balance_cents: 100_000,
                cash_buying_power_cents: Some(100_000),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            });
        let (ctx, position) = build_ctx_with_executor(
            pool,
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            executor,
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            0,
            "a missing close-flatten quote must block enqueue instead of using latest-trade preflight"
        );
    }

    /// The daily extended-hours path, not the flatten window: an ordinary
    /// overnight buy with no optional primary provider is preflighted against
    /// the mark crossed by `counter_trade_slippage_bps`, the exact price the
    /// placement will use.
    /// Cash here covers the bare mark ($200.00) but not the crossed one
    /// ($202.00), so this fails if the resolver skips the mark for the delayed
    /// quote or trade price, and equally if the cross is dropped.
    #[tokio::test]
    async fn ordinary_extended_hours_buy_preflights_the_crossed_mark() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let executor = MockExecutor::new()
            .with_market_session(MarketSession::Extended)
            .with_extended_session_closes_at(chrono::Utc::now() + chrono::Duration::seconds(300))
            .with_post_close_gap(st0x_execution::PostCloseGap::OrdinaryOvernight)
            .with_preflight_price(float!(10.0))
            .with_position_mark(Positive::new(Usd::new(float!(100.0))).unwrap())
            .with_latest_quote(
                st0x_execution::LatestQuote::new(
                    Positive::new(Usd::new(float!(1.0))).unwrap(),
                    Positive::new(Usd::new(float!(2.0))).unwrap(),
                )
                .unwrap(),
            )
            .with_inventory(Inventory {
                positions: Vec::new(),
                usd_balance_cents: 20_100,
                cash_buying_power_cents: Some(20_100),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            });
        let (ctx, position) = build_ctx_with_executor(
            pool,
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            executor,
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            0,
            "cash covering only the un-crossed mark must block the buy"
        );
        let rendered = metrics_handle.render();
        assert!(
            !rendered.contains("close_flatten_blocked_total{"),
            "an ordinary overnight block is not a close-flatten block, in:\n{rendered}"
        );
    }

    /// Companion to the block above: the same daily path must still enqueue
    /// once cash covers the crossed mark, or every extended-hours buy would be
    /// silently dropped.
    #[tokio::test]
    async fn ordinary_extended_hours_buy_enqueues_when_cash_covers_the_crossed_mark() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let executor = MockExecutor::new()
            .with_market_session(MarketSession::Extended)
            .with_extended_session_closes_at(chrono::Utc::now() + chrono::Duration::seconds(300))
            .with_post_close_gap(st0x_execution::PostCloseGap::OrdinaryOvernight)
            .with_position_mark(Positive::new(Usd::new(float!(100.0))).unwrap())
            .with_inventory(Inventory {
                positions: Vec::new(),
                usd_balance_cents: 20_500,
                cash_buying_power_cents: Some(20_500),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            });
        let (ctx, position) = build_ctx_with_executor(
            pool,
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            executor,
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            1,
            "an ordinary extended-hours buy with sufficient cash must still be enqueued"
        );
    }

    /// Overnight enabled with both knobs present, extended hours deliberately
    /// disabled so no scan-side overnight test can pass by leaning on the
    /// extended-hours flag or its close-flatten machinery.
    fn overnight_ctx(symbols: &[&str]) -> Ctx {
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
                    extended_hours_counter_trading: OperationMode::Disabled,
                    overnight_counter_trading: OperationMode::Enabled,
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
            overnight_max_quote_age_secs: Some(NonZeroU64::new(30).unwrap()),
            overnight_slippage_bps: Some(100),
            overnight_reprice_timeout_secs: Some(NonZeroU64::new(300).unwrap()),
            ..create_test_ctx_with_order_owner(address!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ))
        }
    }

    fn indicative_quote_at(
        bid: &str,
        ask: &str,
        at: chrono::DateTime<chrono::Utc>,
    ) -> IndicativeQuote {
        IndicativeQuote {
            quote: LatestQuote::new(
                Positive::new(Usd::new(float!(bid))).unwrap(),
                Positive::new(Usd::new(float!(ask))).unwrap(),
            )
            .unwrap(),
            at,
        }
    }

    /// An overnight buy is preflighted against the indicative ask ($100.00)
    /// crossed by `overnight_slippage_bps` (100 bps -> $101.00), the exact
    /// price the placement will use. Cash covers the bare ask ($200.00 for
    /// two shares) but not the crossed one ($202.00), so this fails if the
    /// scan preflights the un-crossed reference or a non-indicative source.
    #[tokio::test]
    async fn overnight_buy_preflights_the_crossed_indicative_ask() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = overnight_ctx(&["AAPL"]);
        let executor = MockExecutor::new()
            .with_market_session(MarketSession::Overnight)
            .with_overnight_quote(indicative_quote_at("99.50", "100.00", chrono::Utc::now()))
            .with_inventory(Inventory {
                positions: Vec::new(),
                usd_balance_cents: 20_100,
                cash_buying_power_cents: Some(20_100),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            });
        let (ctx, position) = build_ctx_with_executor(
            pool,
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            executor,
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            0,
            "cash covering only the un-crossed indicative ask must block the buy"
        );
        let rendered = metrics_handle.render();
        assert!(
            !rendered.contains("close_flatten_blocked_total{"),
            "an overnight block is not a close-flatten block, in:\n{rendered}"
        );
    }

    /// Companion to the block above: the same overnight path must still
    /// enqueue once cash covers the crossed ask, or every overnight buy would
    /// be silently dropped.
    #[tokio::test]
    async fn overnight_buy_enqueues_when_cash_covers_the_crossed_ask() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = overnight_ctx(&["AAPL"]);
        let executor = MockExecutor::new()
            .with_market_session(MarketSession::Overnight)
            .with_overnight_quote(indicative_quote_at("99.50", "100.00", chrono::Utc::now()))
            .with_inventory(Inventory {
                positions: Vec::new(),
                usd_balance_cents: 20_500,
                cash_buying_power_cents: Some(20_500),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            });
        let (ctx, position) = build_ctx_with_executor(
            pool,
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            executor,
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            1,
            "an overnight buy with sufficient cash for the crossed ask must be enqueued"
        );
    }

    /// A stale indicative quote must block the enqueue with its own counted
    /// reason: pricing an overnight buy from a quote older than
    /// `overnight_max_quote_age_secs` is exactly what the RAI-1947 contract
    /// forbids, and there is no fallback source to try.
    #[tokio::test]
    async fn overnight_buy_with_a_stale_quote_blocks_enqueue() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = overnight_ctx(&["AAPL"]);
        let executor = MockExecutor::new()
            .with_market_session(MarketSession::Overnight)
            .with_overnight_quote(indicative_quote_at(
                "99.50",
                "100.00",
                chrono::Utc::now() - chrono::Duration::seconds(120),
            ))
            .with_inventory(Inventory {
                positions: Vec::new(),
                usd_balance_cents: 20_500,
                cash_buying_power_cents: Some(20_500),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            });
        let (mut ctx, position) = build_ctx_with_executor(
            pool,
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            executor,
        )
        .await;
        let notifier = Arc::new(CapturingNotifier::default());
        ctx.notifier = notifier.clone();
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            0,
            "a stale indicative quote must block the overnight buy enqueue"
        );
        let rendered = metrics_handle.render();
        assert!(
            rendered.contains("reason=\"overnight_unpriceable\"")
                && rendered.contains("session=\"overnight\""),
            "the blocked scan tick must be counted with its cause and session, in:\n{rendered}"
        );
        assert_eq!(
            notifier.messages(),
            Vec::<String>::new(),
            "staleness is a freshness race retried next scan, never an operator page"
        );
    }

    /// The hedge job defers rather than errors when the overnight feed is
    /// unpriceable, so it never dead-letters for it -- this scan-side page is
    /// the only push signal for a symbol whose feed access is gone. It must
    /// fire for a non-retryable classification (an entitlement rejection) and
    /// stay deduped across scans under `(symbol, OvernightQuoteFetch)`.
    #[tokio::test]
    async fn a_failing_overnight_quote_lookup_pages_with_its_own_reason() {
        struct EntitlementFailingPlacer;

        #[async_trait]
        impl OrderPlacer for EntitlementFailingPlacer {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("a skipped buy must never reach the broker")
            }

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("a skipped buy must never reach the broker")
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                Ok(CancellationOutcome::Requested)
            }

            async fn fetch_latest_overnight_quote(
                &self,
                _symbol: &Symbol,
            ) -> Result<IndicativeQuote, Box<dyn std::error::Error + Send + Sync>> {
                // Wrapped the way the production executor surfaces it, so the
                // paging predicate classifies the same chain it will in prod.
                Err(Box::new(AlpacaBrokerApiError::LatestQuote(Box::new(
                    AlpacaMarketDataError::Entitlement {
                        status: StatusCode::FORBIDDEN,
                        body: "subscription does not permit querying overnight feed".to_string(),
                    },
                ))))
            }
        }

        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = overnight_ctx(&["AAPL"]);
        let executor = MockExecutor::new()
            .with_market_session(MarketSession::Overnight)
            .with_inventory(Inventory {
                positions: Vec::new(),
                usd_balance_cents: 100_000,
                cash_buying_power_cents: Some(100_000),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            });
        let (mut ctx, position) = build_ctx_with_order_placer(
            pool,
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            executor,
            Arc::new(EntitlementFailingPlacer),
        )
        .await;
        let notifier = Arc::new(CapturingNotifier::default());
        ctx.notifier = notifier.clone();
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            0,
            "a buy with no overnight reference price must fail closed"
        );
        let rendered = metrics_handle.render();
        assert!(
            rendered.contains("reason=\"overnight_unpriceable\""),
            "the skip must name its cause, in:\n{rendered}"
        );

        let expected_page = "Hedge for AAPL skipped: the overnight indicative quote fetch \
             failed with a non-retryable classification, leaving no reference price. The \
             scan keeps skipping it, so the symbol carries a standing delta until the feed \
             access is fixed."
            .to_string();
        assert_eq!(notifier.messages(), vec![expected_page.clone()]);

        // The scan re-skips this buy every tick, so the page must be deduped
        // by the same `(symbol, reason)` key mechanism the hedge job's
        // dead-letter uses.
        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(notifier.messages(), vec![expected_page]);
    }

    /// Overnight sells keep the ordinary inventory preflight: no indicative
    /// quote is needed (the mock has none to serve), because price cannot
    /// constrain an equity-backed sell reservation.
    #[tokio::test]
    async fn overnight_sell_enqueues_with_the_ordinary_preflight() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = overnight_ctx(&["AAPL"]);
        let executor = MockExecutor::new()
            .with_market_session(MarketSession::Overnight)
            .with_inventory(Inventory {
                positions: vec![st0x_execution::EquityPosition {
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: FractionalShares::new(float!(10.0)),
                    market_value: None,
                }],
                usd_balance_cents: 1_000_000,
                cash_buying_power_cents: Some(1_000_000),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            });
        let (ctx, position) = build_ctx_with_executor(
            pool,
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            executor,
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            1,
            "an overnight sell must enqueue through the ordinary preflight without \
             touching the indicative feed"
        );
    }

    fn quote_fetch_error(status: StatusCode) -> ReferencePriceError {
        ReferencePriceError::QuoteFetch(Box::new(AlpacaBrokerApiError::LatestQuote(Box::new(
            AlpacaMarketDataError::ApiError {
                status,
                body: "test response".to_string(),
                retry_after: None,
            },
        ))))
    }

    #[test]
    fn scan_pages_only_non_retryable_reference_price_failures() {
        assert!(!should_page_reference_price_failure(
            &quote_fetch_error(StatusCode::TOO_MANY_REQUESTS),
            SupportedExecutor::AlpacaBrokerApi,
        ));
        assert!(!should_page_reference_price_failure(
            &quote_fetch_error(StatusCode::SERVICE_UNAVAILABLE),
            SupportedExecutor::AlpacaBrokerApi,
        ));
        assert!(should_page_reference_price_failure(
            &quote_fetch_error(StatusCode::FORBIDDEN),
            SupportedExecutor::AlpacaBrokerApi,
        ));
        assert!(should_page_reference_price_failure(
            &ReferencePriceError::Unavailable,
            SupportedExecutor::AlpacaBrokerApi,
        ));
        assert!(!should_page_reference_price_failure(
            &ReferencePriceError::Unavailable,
            SupportedExecutor::DryRun,
        ));
    }

    #[tokio::test]
    async fn dry_run_without_a_reference_source_skips_without_paging() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let (mut ctx, position) = build_ctx_with_executor(
            pool,
            apalis_pool,
            cfg,
            Duration::from_secs(60),
            MockExecutor::new().with_market_session(MarketSession::Extended),
        )
        .await;
        let notifier = Arc::new(CapturingNotifier::default());
        ctx.notifier = notifier.clone();
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert!(
            notifier.messages().is_empty(),
            "DryRun intentionally has no live reference-price provider, so absence is not an incident"
        );
    }

    #[test]
    fn hedge_scan_skip_reason_labels_are_stable() {
        assert_eq!(
            [
                HedgeScanSkipReason::MarketSessionCheck,
                HedgeScanSkipReason::ReferencePriceUnavailable,
                HedgeScanSkipReason::MarkFetchFailed,
                HedgeScanSkipReason::QuoteFetchFailed,
                HedgeScanSkipReason::SlippageCalculation,
                HedgeScanSkipReason::OvernightIneligible,
                HedgeScanSkipReason::OvernightUnpriceable,
            ]
            .map(HedgeScanSkipReason::metric_label),
            [
                "market_session_check",
                "reference_price_unavailable",
                "mark_fetch_failed",
                "quote_fetch_failed",
                "slippage_calculation",
                "overnight_ineligible",
                "overnight_unpriceable",
            ]
        );
    }

    /// The overnight paging rule matches the extended one on classification:
    /// an entitlement rejection (`Permanent`) and an unclassified failure
    /// page; rate-limited and transient failures wait for the next scan; a
    /// stale quote is a freshness race, never an incident.
    #[test]
    fn scan_pages_only_non_retryable_overnight_quote_failures() {
        // Wrapped the way the production executor surfaces feed failures:
        // `AlpacaBrokerApiError::LatestQuote(market data error)`, which is
        // the chain the permanence/backpressure probes classify.
        let entitlement = OvernightReferenceError::QuoteFetch(Box::new(
            AlpacaBrokerApiError::LatestQuote(Box::new(AlpacaMarketDataError::Entitlement {
                status: StatusCode::FORBIDDEN,
                body: "subscription does not permit querying overnight feed".to_string(),
            })),
        ));
        assert!(should_page_overnight_reference_failure(&entitlement));

        let unclassified = OvernightReferenceError::QuoteFetch("market data endpoint down".into());
        assert!(should_page_overnight_reference_failure(&unclassified));

        let rate_limited = OvernightReferenceError::QuoteFetch(Box::new(
            AlpacaBrokerApiError::LatestQuote(Box::new(AlpacaMarketDataError::ApiError {
                status: StatusCode::TOO_MANY_REQUESTS,
                body: "test response".to_string(),
                retry_after: None,
            })),
        ));
        assert!(!should_page_overnight_reference_failure(&rate_limited));

        let stale = OvernightReferenceError::Stale {
            age: Duration::from_secs(120),
            max_age: Duration::from_secs(30),
        };
        assert!(!should_page_overnight_reference_failure(&stale));
    }

    /// The scan short-circuits before a `PlaceHedge` job exists, so
    /// `hedge_dead_lettered_total` can never fire for a buy dropped here.
    /// `hedge_scan_skipped_total` is the counter left, and it must name the leg
    /// that failed rather than collapse every cause into one label. The
    /// operator page must still fire: the symbol carries a standing delta that
    /// no later job will report.
    #[tokio::test]
    async fn a_failing_quote_lookup_skips_the_buy_with_its_own_reason() {
        struct QuoteFailingPlacer;

        #[async_trait]
        impl OrderPlacer for QuoteFailingPlacer {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("a skipped buy must never reach the broker")
            }

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("a skipped buy must never reach the broker")
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                Ok(CancellationOutcome::Requested)
            }

            async fn fetch_latest_quote(
                &self,
                _symbol: &Symbol,
            ) -> Result<Option<st0x_execution::LatestQuote>, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("market data endpoint down".into())
            }
        }

        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let executor = MockExecutor::new()
            .with_market_session(MarketSession::Extended)
            .with_extended_session_closes_at(chrono::Utc::now() + chrono::Duration::seconds(300))
            .with_post_close_gap(st0x_execution::PostCloseGap::OrdinaryOvernight)
            .with_inventory(Inventory {
                positions: Vec::new(),
                usd_balance_cents: 100_000,
                cash_buying_power_cents: Some(100_000),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            });
        let (mut ctx, position) = build_ctx_with_order_placer(
            pool,
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            executor,
            Arc::new(QuoteFailingPlacer),
        )
        .await;
        let notifier = Arc::new(CapturingNotifier::default());
        ctx.notifier = notifier.clone();
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            0,
            "a buy with no reference price must fail closed"
        );
        let rendered = metrics_handle.render();
        assert!(
            rendered.contains("hedge_scan_skipped_total{"),
            "a scan-time skip must be counted, in:\n{rendered}"
        );
        assert!(
            rendered.contains("reason=\"quote_fetch_failed\""),
            "the skip must name the leg that failed, in:\n{rendered}"
        );
        assert!(
            rendered.contains("session=\"extended\""),
            "the skip must name the session it was scanned in, in:\n{rendered}"
        );
        assert!(
            rendered.contains("symbol=\"AAPL\""),
            "the skip must name the symbol carrying the standing delta, in:\n{rendered}"
        );

        let expected_page = "Hedge for AAPL skipped: limit_quote_fetch failure left no \
             reference price to preflight against. The scan keeps skipping it, so the \
             symbol carries a standing delta until the market-data failure is fixed."
            .to_string();
        assert_eq!(notifier.messages(), vec![expected_page.clone()]);

        // The scan re-skips this buy every tick, so the page must be deduped
        // by the same `(symbol, reason)` key the hedge job's dead-letter uses.
        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(notifier.messages(), vec![expected_page]);
    }

    #[tokio::test]
    async fn a_cross_failure_pages_once_at_the_used_width() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let executor = MockExecutor::new()
            .with_market_session(MarketSession::Extended)
            .with_position_mark(
                Positive::new(Usd::new(Float::max_positive_value().unwrap())).unwrap(),
            );
        let (mut ctx, _) =
            build_ctx_with_executor(pool, apalis_pool, cfg, Duration::from_secs(60), executor)
                .await;
        ctx.close_flatten_ramp = CloseFlattenCrossRamp::new(9_999, 9_999).unwrap();
        let notifier = Arc::new(CapturingNotifier::default());
        ctx.notifier = notifier.clone();
        let symbol = Symbol::new("AAPL").unwrap();
        let order = MarketOrder {
            symbol: symbol.clone(),
            shares: Positive::new(FractionalShares::new(float!(1.0))).unwrap(),
            direction: Direction::Buy,
            client_order_id: ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
        };

        for _ in 0..2 {
            let result = ctx
                .preflight_extended_hours_buy(order.clone(), None)
                .await
                .unwrap();
            assert!(result.is_none());
        }

        assert_eq!(
            notifier.messages(),
            vec![
                "Hedge for AAPL skipped: the reference price could not be crossed at 9999 bps. \
                 The scan keeps skipping the symbol while the cross stays this wide, so it \
                 carries a standing delta."
                    .to_string()
            ]
        );
        assert!(ctx.alerted_dead_letters.lock().await.contains(&(
            symbol,
            DeadLetterReason::SymbolScoped(SymbolScopedReason::SlippageCalculation)
        )));
    }

    #[tokio::test]
    async fn close_flatten_window_is_resolved_once_per_scan() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL", "MSFT"], OperationMode::Enabled);
        let executor = MockExecutor::new()
            .with_market_session(MarketSession::Extended)
            .with_extended_session_closes_at(chrono::Utc::now() + chrono::Duration::seconds(300))
            .with_post_close_gap(st0x_execution::PostCloseGap::MultiDayClosure)
            .with_latest_quote(
                st0x_execution::LatestQuote::new(
                    Positive::new(Usd::new(float!(99.0))).unwrap(),
                    Positive::new(Usd::new(float!(100.0))).unwrap(),
                )
                .unwrap(),
            )
            .with_inventory(Inventory {
                positions: Vec::new(),
                usd_balance_cents: 100_000,
                cash_buying_power_cents: Some(100_000),
                alpaca_usdc: None,
                cash_withdrawable_cents: None,
            });
        let executor_probe = executor.clone();
        let (ctx, position) = build_ctx_with_executor(
            pool,
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            executor,
        )
        .await;

        for symbol in ["AAPL", "MSFT"] {
            accumulate_position(
                &position,
                &Symbol::new(symbol).unwrap(),
                FractionalShares::new(float!(2.0)),
                Direction::Sell,
            )
            .await;
        }

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(count_jobs(&apalis_pool, &hedge_job_type()).await, 2);
        assert_eq!(
            executor_probe.market_session_status_call_count(),
            1,
            "one scan must reuse one close-flatten session-status lookup across all symbols"
        );
    }

    /// Regression test for the gate that scopes the close-flatten window
    /// check to the `Extended` session. Before this fix, `Direction::Buy`
    /// preflights on an extended-hours-enabled symbol called
    /// `market_session_status` (a fresh calendar HTTP round trip)
    /// unconditionally, including during ordinary `Regular` hours -- and a
    /// transient failure from that call would skip the hedge enqueue
    /// entirely (fail-closed). Here `market_session_status` is configured to
    /// error, but the session is `Regular`, so the close-flatten window
    /// check must never be reached and the buy must enqueue normally.
    #[tokio::test]
    async fn regular_session_buy_preflight_skips_close_flatten_check_even_on_calendar_failure() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let executor = MockExecutor::new()
            .with_market_session(MarketSession::Regular)
            .with_market_session_status_failure("calendar endpoint unavailable");
        let (ctx, position) = build_ctx_with_executor(
            pool,
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            executor,
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            1,
            "a Regular-session buy preflight must not consult (or fail closed on) the \
             close-flatten window's calendar status"
        );
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
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool,
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
        record_extended_hours_order_at(
            &ctx,
            &aapl,
            offchain_order_id,
            chrono::Utc::now() - chrono::Duration::seconds(301),
        )
        .await;

        // First scan after the restart: market already Regular.
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
                OffchainOrder::Cancelling {
                    reason: CancellationReason::MarketOpenReplacement,
                    ..
                }
            ),
            "restart catch-up must classify a stale live order as a regular-open replacement, got: {order:?}"
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
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Disabled);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool,
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
                    filled_shares: FractionalShares::ZERO,
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
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Disabled);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
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
                    filled_shares: FractionalShares::new(float!(0.5)),
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
        assert_eq!(
            count_jobs(&apalis_pool, &hedge_job_type()).await,
            1,
            "the remaining executable 1.5 shares must be enqueued for a fresh hedge"
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
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool,
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
        let rendered = metrics_handle.render();
        assert!(
            rendered.contains("hedge_cancellations_requested_total{")
                && rendered.contains("reason=\"market_open_replacement\"")
                && rendered.contains("session=\"extended\""),
            "the market-open cancel request must be counted with its reason and the \
             cancelled order's session, in:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn missing_order_aggregate_is_cleared_without_blocking_others() {
        // AAPL's position is claimed but its offchain-order aggregate does not
        // exist. The cancel sweep must still cancel TSLA's live extended-hours
        // order on this tick, then the shared orphan recovery clears AAPL's
        // missing claim so the position can be retried by the normal hedge
        // path instead of remaining stuck.
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL", "TSLA"], OperationMode::Enabled);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool,
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

        let aapl_position = ctx
            .position_projection
            .load(&aapl)
            .await
            .unwrap()
            .expect("AAPL position should exist");
        assert_eq!(
            aapl_position.pending_offchain_order_id, None,
            "missing offchain-order aggregate must clear the pending claim"
        );
        assert_eq!(
            aapl_position.last_failed_offchain_order_id,
            Some(aapl_order_id),
            "missing offchain-order aggregate must leave a failure anchor for retry"
        );
    }

    #[tokio::test]
    async fn extended_hours_disabled_does_not_cancel_live_extended_hours_order() {
        // With extended-hours counter-trading disabled for the asset, the
        // cancel-and-replace pass must not touch it: a live extended-hours
        // order is left untouched even when the scan observes regular hours.
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Disabled);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool,
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
                    market_session: MarketSession::Extended,
                    ..
                }
            ),
            "with extended hours disabled the cancel pass must not touch the order, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn cancel_sweep_skips_orders_placed_by_different_executor() {
        // The cancel sweep must only dispatch cancellations through the
        // currently-configured executor. An extended-hours order placed by a
        // different executor (AlpacaBrokerApi) while the context runs DryRun
        // must be left untouched: routing the cancellation through the wrong
        // broker would mis-target. Mirrors the guard in PollOrderStatus and
        // recover_submitted_offchain_orders.
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Enabled);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool,
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

        // Claim the position with AlpacaBrokerApi executor (different from ctx's DryRun).
        ctx.position
            .send(
                &aapl,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares: Positive::new(FractionalShares::new(float!(1))).unwrap(),
                    direction: Direction::Sell,
                    executor: SupportedExecutor::AlpacaBrokerApi,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();

        // Record a live extended-hours limit order with AlpacaBrokerApi executor.
        let shares = Positive::new(FractionalShares::new(float!(1))).unwrap();
        let limit_price = Positive::new(Usd::new(float!(195.25))).unwrap();
        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: aapl.clone(),
                    shares,
                    direction: Direction::Sell,
                    executor: SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: ClientOrderId::from_uuid(offchain_order_id.as_uuid()),
                    kind: CounterTradeOrderKind::ExtendedHoursLimit {
                        limit_price,
                        close_flatten: false,
                        reference_price: None,
                    },
                },
            )
            .await
            .unwrap();
        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("alpaca-eh-1"),
                    placed_shares: shares,
                    submitted_at: chrono::Utc::now(),
                    is_extended_hours: true,
                    limit_price: Some(limit_price),
                },
            )
            .await
            .unwrap();

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
                    market_session: MarketSession::Extended,
                    ..
                }
            ),
            "cancel sweep must skip an order placed by a different executor, got: {order:?}"
        );
    }

    #[tokio::test]
    async fn cancel_sweep_only_touches_symbols_with_extended_hours_enabled() {
        // Per-asset granularity: with AAPL extended-hours enabled and TSLA
        // disabled, a regular-hours sweep must cancel AAPL's live
        // extended-hours order while leaving TSLA's untouched.
        let (pool, apalis_pool) = setup_test_pools().await;
        let mut cfg = dry_run_ctx(&["AAPL", "TSLA"], OperationMode::Enabled);
        let tsla = Symbol::new("TSLA").unwrap();
        cfg.assets
            .equities
            .symbols
            .get_mut(&tsla)
            .unwrap()
            .extended_hours_counter_trading = OperationMode::Disabled;
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool,
            cfg,
            Duration::from_secs(60),
            regular_session_executor(),
        )
        .await;

        let aapl = Symbol::new("AAPL").unwrap();
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
        record_extended_hours_order(&ctx, &aapl, aapl_order_id).await;

        let tsla_order_id = OffchainOrderId::new();
        claim_position(&ctx, &tsla, tsla_order_id).await;
        record_extended_hours_order(&ctx, &tsla, tsla_order_id).await;

        CheckPositions::default().perform(&ctx).await.unwrap();

        let aapl_order = ctx
            .offchain_order
            .load(&aapl_order_id)
            .await
            .unwrap()
            .expect("AAPL order should exist");
        assert!(
            matches!(aapl_order, OffchainOrder::Cancelling { .. }),
            "the enabled symbol's extended-hours order must be cancelled, got: {aapl_order:?}"
        );

        let tsla_order = ctx
            .offchain_order
            .load(&tsla_order_id)
            .await
            .unwrap()
            .expect("TSLA order should exist");
        assert!(
            matches!(
                tsla_order,
                OffchainOrder::Submitted {
                    market_session: MarketSession::Extended,
                    ..
                }
            ),
            "the disabled symbol's extended-hours order must be left untouched, got: {tsla_order:?}"
        );
    }

    #[tokio::test]
    async fn skips_trading_disabled_symbols_without_blocking_others() {
        let (pool, apalis_pool) = setup_test_pools().await;
        // RKLB is intentionally absent from the trading config -- the scan
        // must skip it without aborting the rest of the loop.
        let cfg = dry_run_ctx(&["AAPL", "TSLA"], OperationMode::Disabled);
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

        CheckPositions::default().perform(&ctx).await.unwrap();

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

    async fn live_poll_job_count(
        apalis_pool: &apalis_sqlite::SqlitePool,
        offchain_order_id: OffchainOrderId,
    ) -> i64 {
        sqlx_apalis::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM Jobs \
             WHERE job_type = ? \
               AND json_extract(CAST(job AS TEXT), '$.offchain_order_id') = ? \
               AND status IN ('Pending', 'Queued', 'Running')",
        )
        .bind(poll_status_job_type())
        .bind(offchain_order_id.to_string())
        .fetch_one(apalis_pool)
        .await
        .unwrap()
    }

    /// Simulates a single-concurrency apalis worker draining the oldest live
    /// `PollOrderStatus` row for one order: dequeue, run `perform`, ack
    /// (`Done`). Mirrors `drain_pending_equity_jobs`'s pattern
    /// (`src/rebalancing/trigger/equity.rs`) rather than calling `perform`
    /// with no row bookkeeping at all, so an unfixed
    /// `recover_submitted_offchain_orders` forking an extra independent chain
    /// each tick shows up as an extra never-drained row, exactly like the
    /// single `concurrency(1)` worker in production under-draining a growing
    /// backlog.
    async fn drain_one_poll_job_for_order(
        apalis_pool: &apalis_sqlite::SqlitePool,
        poll_ctx: &PollOrderStatusCtx<MockExecutor>,
        offchain_order_id: OffchainOrderId,
    ) {
        let row: Option<(String, Vec<u8>)> = sqlx_apalis::query_as(
            "SELECT id, job FROM Jobs \
             WHERE job_type = ? \
               AND json_extract(CAST(job AS TEXT), '$.offchain_order_id') = ? \
               AND status IN ('Pending', 'Queued', 'Running') \
             ORDER BY run_at ASC LIMIT 1",
        )
        .bind(poll_status_job_type())
        .bind(offchain_order_id.to_string())
        .fetch_optional(apalis_pool)
        .await
        .unwrap();

        let Some((id, payload)) = row else {
            return;
        };

        let job: PollOrderStatus =
            serde_json::from_slice(&payload).expect("deserialize PollOrderStatus payload");
        job.perform(poll_ctx).await.unwrap();

        sqlx_apalis::query("UPDATE Jobs SET status = 'Done' WHERE id = ?")
            .bind(&id)
            .execute(apalis_pool)
            .await
            .expect("mark drained PollOrderStatus job done");
    }

    /// Reproduces the incident mechanism: `CheckPositions`'s per-tick
    /// recovery unconditionally re-pushed a `PollOrderStatus` job
    /// for every still-open order, forking an independent, self-perpetuating
    /// poll chain on top of whatever chain(s) already existed for that order.
    /// A single-concurrency worker (simulated here by draining exactly one
    /// due row per tick) cannot keep up, so the live-row population grows
    /// without bound the longer the order stays open.
    #[tokio::test]
    async fn check_positions_recovery_does_not_multiply_poll_jobs_for_a_long_open_order() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let cfg = dry_run_ctx(&["AAPL"], OperationMode::Disabled);
        let executor = MockExecutor::new().with_order_status(OrderState::Pending);
        let (ctx, position) = build_ctx_with_executor(
            pool.clone(),
            apalis_pool.clone(),
            cfg,
            Duration::from_secs(60),
            executor.clone(),
        )
        .await;

        let symbol = Symbol::new("AAPL").unwrap();
        accumulate_position(
            &position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let offchain_order_id = OffchainOrderId::new();
        let shares = Positive::new(FractionalShares::new(float!(2.0))).unwrap();
        position
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
        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares,
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    client_order_id: ClientOrderId::from_uuid(offchain_order_id.as_uuid()),
                    kind: CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();
        ctx.offchain_order
            .send(
                &offchain_order_id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("test-accept"),
                    placed_shares: shares,
                    submitted_at: chrono::Utc::now(),
                    is_extended_hours: false,
                    limit_price: None,
                },
            )
            .await
            .unwrap();

        let poll_ctx = PollOrderStatusCtx {
            executor: executor.clone(),
            offchain_order_projection: ctx.offchain_order_projection.clone(),
            offchain_order_store: ctx.offchain_order.clone(),
            position_store: position.clone(),
            poll_status_queue: PollOrderStatusJobQueue::new(&apalis_pool),
            reconcile_queue: ReconcileOrderFillJobQueue::new(&apalis_pool),
            rejection_queue: HandleOrderRejectionJobQueue::new(&apalis_pool),
            poll_interval: Duration::from_secs(15),
        };

        for _ in 0..5 {
            CheckPositions::default().perform(&ctx).await.unwrap();
            drain_one_poll_job_for_order(&apalis_pool, &poll_ctx, offchain_order_id).await;
        }

        assert_eq!(
            live_poll_job_count(&apalis_pool, offchain_order_id).await,
            1,
            "a long-open order must have exactly one live PollOrderStatus job \
             regardless of how many CheckPositions ticks fire while it stays open"
        );
    }
}
