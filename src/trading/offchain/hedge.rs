//! Durable hedge placement job.
//!
//! [`PlaceHedge`] is an apalis-backed [`Job`] that places an offsetting
//! broker order for an accumulated position. The position monitor enqueues
//! these; the apalis worker processes them with retry semantics.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::U256;
use metrics::{counter, histogram};
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use st0x_float_macro::float;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use st0x_config::{AssetsConfig, ExecutionThreshold};
use st0x_event_sorcery::{AggregateError, LifecycleError, Store};
use st0x_execution::{
    Backpressure, ClientOrderId, CounterTradePreflight, CounterTradeSkipReason, Direction,
    EligibilitySnapshots, FractionalShares, MarketOrder, MarketSession, OvernightEligibilityError,
    OvernightOrderShape, Permanence, Positive, PostCloseGap, SupportedExecutor, Symbol, Usd,
    validate_overnight_eligibility,
};

use crate::alerts::Notifier;
use crate::conductor::job::{
    BACKPRESSURE_RESCHEDULE_LIMIT, BackpressureOutcome, BackpressureStreak, Job, JobQueue, Label,
    advance_backpressure, apply_backpressure_step, find_backpressure, find_permanence,
};
#[cfg(test)]
use crate::offchain::order::PollOrderStatus;
use crate::offchain::order::{
    CounterTradeOrderKind, OffchainOrder, OffchainOrderId, OffchainOrderPlacement, OrderPlacer,
    PollOrderStatusJobQueue, client_order_id_for_placement,
    finalize_cancelled_position_or_log_unpriced, place_offchain_order_at_broker,
    push_poll_job_if_absent, session_metric_label,
};
use crate::position::{AnchorDisposition, Position, PositionCommand, PositionError};
use crate::trading::offchain::close_flatten::{CloseFlattenCrossRamp, CloseFlattenPolicy};
use crate::trading::onchain::trade_accountant::{
    ClaimedHedgeOrderKindCause, DeadLetterReason, ErrorScope, SymbolScopedReason,
    TradeAccountingError,
};

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
pub(crate) fn apply_slippage(
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
    /// This job's own queue, so a classified broker rate-limit (429) can
    /// reschedule itself (RAI-1494) instead of consuming the terminal retry
    /// budget. Previously missing -- `HedgeCtx` held `poll_status_queue` for
    /// `recover_pending_poll_status` but no handle to its own job type.
    pub(crate) hedge_queue: HedgeJobQueue,
    /// Per-symbol asset config. Gates the extended-hours limit path: only a
    /// symbol with `extended_hours_counter_trading = enabled` may place a limit
    /// order during an Extended session. A disabled symbol skips (the
    /// regular-open cancel-and-replace sweep is keyed off the same per-symbol
    /// flag, so an extended order for a disabled symbol would be orphaned).
    pub(crate) assets: AssetsConfig,
    /// Validated once at construction (`conductor/builder.rs`) instead of
    /// re-parsed from a raw `u64` on every hedge job -- the window is fixed
    /// for the process lifetime, so re-validating it per job just threads an
    /// always-succeeds-in-practice `Result` through the hot placement path.
    pub(crate) close_flatten_policy: CloseFlattenPolicy,
    pub(crate) close_flatten_ramp: CloseFlattenCrossRamp,
    /// The per-symbol eligibility store the conductor's 19:55 ET sync
    /// task writes. The overnight order-kind selection reads it fail
    /// closed: a missing or stale snapshot defers the hedge with no
    /// broker call.
    pub(crate) overnight_eligibility: EligibilitySnapshots,
    /// `Some` whenever any asset enables overnight counter-trading
    /// (startup validation enforces it); an enabled symbol reaching the
    /// overnight path with `None` here is a wiring bug and defers loudly.
    pub(crate) overnight_max_quote_age: Option<Duration>,
    /// Same presence contract as `overnight_max_quote_age`.
    pub(crate) overnight_slippage_bps: Option<u16>,
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
    /// Fed to [`push_poll_job_if_absent`] before every
    /// `PollOrderStatus` push in this module (`recover_pending_poll_status`'s
    /// re-push and `route_placement_outcome`'s push), so a push against an
    /// order that already has a live poll job is skipped instead of forking a
    /// new self-perpetuating chain.
    pub(crate) poll_interval: Duration,
    /// Pages the operator when a hedge is abandoned. Dead-lettering keeps the
    /// process alive, so without this an abandoned symbol would accumulate a
    /// standing delta with no push signal -- only a counter nobody is
    /// watching. The same `Arc<dyn Notifier>` the supervised-worker fail-stop
    /// alert uses, so both share one delivery channel and one config section.
    pub(crate) notifier: Arc<dyn Notifier>,
    /// The `(symbol, reason)` pairs reserved for delivery or already delivered
    /// in this process.
    /// `CheckPositions` re-enqueues an abandoned hedge every scan, so an
    /// un-deduped page would repeat for as long as the underlying failure
    /// lasts. A symbol's entries are dropped again as soon as one of its
    /// hedges reaches the broker ([`route_placement_outcome`]), so the set
    /// suppresses repeats of a *standing* failure rather than latching for the
    /// process lifetime -- a feed regression that recurs next session pages
    /// again. In-process only: a restart re-pages, which is the right
    /// behaviour for a condition that survived a restart.
    pub(crate) alerted_dead_letters: Arc<Mutex<HashSet<(Symbol, DeadLetterReason)>>>,
}

/// Pages the operator once per `(symbol, reason)` for a hedge this process
/// abandoned, mirroring the USDC rebalancer's dead-letter alert. Delivery
/// failure is logged and swallowed: the abandonment already happened, and the
/// counter plus `error!` remain.
///
/// The pair is reserved before delivery so the hedge and position-scan workers
/// cannot both send it. A failed or timed-out delivery releases the reservation
/// for the next scan; a successful one keeps it latched until the symbol places.
///
/// Takes the notifier and the dedup set rather than a `HedgeCtx` because the
/// position scan pages through the same mechanism for a buy it drops before a
/// hedge job exists (`position_check.rs`). Both share one set, so a symbol
/// paged by either path is silenced by the other until one of its hedges
/// reaches the broker.
pub(crate) async fn alert_dead_letter(
    notifier: &dyn Notifier,
    alerted_dead_letters: &Mutex<HashSet<(Symbol, DeadLetterReason)>>,
    symbol: &Symbol,
    reason: DeadLetterReason,
    message: &str,
) {
    let key = (symbol.clone(), reason);
    if !alerted_dead_letters.lock().await.insert(key.clone()) {
        return;
    }

    match tokio::time::timeout(DEAD_LETTER_ALERT_TIMEOUT, notifier.notify(message)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            alerted_dead_letters.lock().await.remove(&key);
            warn!(
                target: "hedge", ?error, %symbol,
                "Failed to deliver hedge dead-letter alert; the next scan re-attempts it"
            );
        }
        Err(_elapsed) => {
            alerted_dead_letters.lock().await.remove(&key);
            warn!(
                target: "hedge",
                %symbol,
                timeout_secs = DEAD_LETTER_ALERT_TIMEOUT.as_secs(),
                "Timed out delivering hedge dead-letter alert; the next scan re-attempts it"
            );
        }
    }
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
    /// Count of consecutive broker rate-limit (429) reschedules leading up to
    /// this attempt (RAI-1494). `#[serde(default)]` so a row enqueued under
    /// the pre-this-change payload shape still deserializes to `0` instead of
    /// crashing the poll stream's `sqlx::Decode`.
    ///
    /// A 429 before the position claim (for example, during the
    /// extended-hours price lookup) simply retries that read. A 429 from the
    /// broker placement happens after the position claim, but the durable
    /// offchain order remains `Pending`; the successor hits
    /// `PositionError::PendingExecution`, enters
    /// `recover_pending_poll_status`, and safely re-drives the placement with
    /// the same deterministic broker `client_order_id`.
    #[serde(default)]
    pub(crate) backpressure_streak: BackpressureStreak,
    /// Consecutive symbol-scoped *transient* failures (a market-data 5xx, a
    /// transport error) already re-driven for this hedge. Bounded by
    /// [`TRANSIENT_RESCHEDULE_LIMIT`], past which the symbol is dead-lettered
    /// rather than propagated into the supervised worker's fail-stop.
    /// `#[serde(default)]` so a row enqueued under the previous payload shape
    /// still deserializes to `0` instead of crashing the poll stream's decode.
    #[serde(default)]
    pub(crate) transient_streak: TransientFailureStreak,
}

/// Durable count of consecutive symbol-scoped transient re-drives leading up to
/// a hedge attempt (RAI-1690). Distinct at the type level from
/// [`BackpressureStreak`], which counts broker rate-limiting, so the two cannot
/// be swapped at the one construction site that sets both.
/// `#[serde(transparent)]` keeps the wire format an unadorned integer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct TransientFailureStreak(pub(crate) u32);

/// How many times a symbol-scoped transient failure is re-driven before the
/// symbol is abandoned. Mirrors the supervised worker's own
/// `RetryPolicy::retries(3)` budget and its 1s/2s/4s backoff, so the retry
/// cadence a transient failure gets is unchanged -- only the terminal action
/// differs: one symbol's sustained market-data outage dead-letters that symbol
/// instead of failing the worker, and with it hedging for every other symbol.
const TRANSIENT_RESCHEDULE_LIMIT: u32 = 3;

/// Delay before the first transient re-drive; doubles with each consecutive
/// transient failure, matching the supervised worker's retry backoff.
const TRANSIENT_RESCHEDULE_BASE: Duration = Duration::from_secs(1);

/// Keeps a stalled alert channel from serialising every other symbol behind a
/// dead-letter on the concurrency-one hedge worker.
const DEAD_LETTER_ALERT_TIMEOUT: Duration = Duration::from_secs(1);

fn default_market_session() -> MarketSession {
    MarketSession::Regular
}

#[derive(Clone, Copy)]
enum SubmittedPricePreflight {
    Required,
    SkipForIdempotentRecovery,
}

async fn select_order_kind_for_current_session(
    ctx: &HedgeCtx,
    symbol: &Symbol,
    shares: Positive<FractionalShares>,
    direction: Direction,
    enqueued_session: MarketSession,
    submitted_price_preflight: SubmittedPricePreflight,
) -> Result<Option<CounterTradeOrderKind>, TradeAccountingError> {
    let status = ctx
        .order_placer
        .market_session_status()
        .await
        .map_err(|source| TradeAccountingError::MarketSessionCheck {
            symbol: symbol.clone(),
            source,
        })?;
    let current_session = status.session;

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
        MarketSession::Overnight => {
            select_overnight_order_kind(ctx, symbol, shares, direction, submitted_price_preflight)
                .await
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

            let now = chrono::Utc::now();
            let close_flatten_window = ctx.close_flatten_policy.active_window(status, now);
            let close_flatten_active = close_flatten_window.is_some();

            if close_flatten_active {
                counter!(
                    "close_flatten_attempts_total",
                    "symbol" => symbol.to_string(),
                    "direction" => direction_label(direction),
                    "reason" => post_close_gap_label(status.post_close_gap)
                )
                .increment(1);
            }

            let reference = resolve_extended_hours_reference_price(
                ctx.order_placer.as_ref(),
                symbol,
                direction,
            )
            .await
            .inspect_err(|error| {
                if close_flatten_active {
                    record_close_flatten_block(symbol, CloseFlattenBlockReason::from(error));
                }
            })
            .map_err(|error| error.into_trade_accounting_error(symbol))?;

            counter!(
                "hedge_price_source_total",
                "symbol" => symbol.to_string(),
                "path" => if close_flatten_active { "close_flatten" } else { "ordinary_extended" },
                "source" => reference.source.metric_label()
            )
            .increment(1);

            let cross_bps = ctx.close_flatten_ramp.cross_bps(close_flatten_window, now);

            let limit_price = apply_slippage(reference.price.inner(), direction, cross_bps)
                .map_err(TradeAccountingError::SlippageCalculation)?;

            // A fresh job's scan-time preflight approved this buy against an
            // earlier reference -- potentially minutes stale by perform().
            // Re-check cash sufficiency against the exact submitted limit.
            // A Pending recovery deliberately skips this gate: the original
            // broker request is idempotently re-driven by client order ID and
            // may already have reserved the cash that this account-wide check
            // would otherwise reject, stranding the claim without a poller.
            // Sells are unaffected by price, so only buys need this.
            if direction == Direction::Buy
                && matches!(submitted_price_preflight, SubmittedPricePreflight::Required)
                && !extended_hours_preflight_at_submitted_price(
                    ctx,
                    symbol,
                    shares,
                    direction,
                    limit_price,
                    close_flatten_active,
                )
                .await?
            {
                return Ok(None);
            }

            // Counted only once the attempt has cleared every gate that can
            // still abort it here, so a preflight-blocked buy is not reported
            // as a priced placement.
            if let Some(window) = close_flatten_window {
                counter!(
                    "close_flatten_placements_total",
                    "symbol" => symbol.to_string(),
                    "direction" => direction_label(direction),
                    "cross_bucket" => cross_bucket_label(cross_bps)
                )
                .increment(1);
                debug!(
                    target: "hedge",
                    %symbol,
                    cross_bps,
                    window_started_at = %window.started_at,
                    "Close flatten cross ramped for this attempt"
                );
            }

            info!(
                target: "hedge",
                %symbol,
                %limit_price,
                direction = ?direction,
                close_flatten_active,
                "Extended hours: placing limit order"
            );

            Ok(Some(CounterTradeOrderKind::ExtendedHoursLimit {
                limit_price,
                close_flatten: close_flatten_active,
                reference_price: Some(reference.price),
            }))
        }
    }
}

/// Re-checks buying power for an extended-hours buy against the exact limit
/// price it is about to submit at, closing the staleness window between the
/// scan-time preflight's reference and this job's fresh mark or quote. Returns
/// `true` if the order should proceed, `false` if it should be skipped -- mirroring
/// `CheckPositions::preflight_and_clamp_shares`'s "bool: proceed vs skip"
/// contract, since a rejection here is a routine outcome of a moved price, not
/// an error. Only a rejection inside close-flatten mode contributes to that
/// mode's blocked-attempt metric.
async fn extended_hours_preflight_at_submitted_price(
    ctx: &HedgeCtx,
    symbol: &Symbol,
    shares: Positive<FractionalShares>,
    direction: Direction,
    limit_price: Positive<Usd>,
    close_flatten_active: bool,
) -> Result<bool, TradeAccountingError> {
    let order = MarketOrder {
        symbol: symbol.clone(),
        shares,
        direction,
        // Preflight only; this id is never sent to the broker. Use a fresh
        // value so callers cannot mistake it for a real key.
        client_order_id: ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
    };

    let preflight = ctx
        .order_placer
        .preflight_counter_trade_at_price(order, limit_price)
        .await
        .map_err(
            |source| TradeAccountingError::CloseFlattenPreflightAtPrice {
                symbol: symbol.clone(),
                source,
            },
        )?;

    match preflight {
        CounterTradePreflight::Allowed { .. } => Ok(true),
        CounterTradePreflight::Skipped(reason) => {
            if close_flatten_active {
                record_close_flatten_block(symbol, CloseFlattenBlockReason::from(&reason));
            }
            warn!(
                target: "hedge",
                %symbol, %reason, %limit_price, close_flatten_active,
                "Extended-hours hedge blocked at submission time: the exact limit no longer \
                 passes the scan-time preflight"
            );
            Ok(false)
        }
    }
}

/// Which source supplied the price a session limit order was derived from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReferencePriceSource {
    /// An optional current bid/ask market-data quote. The preferred source when
    /// a provider is wired.
    PrimaryQuote,
    /// The broker's position mark: the required fallback when the optional
    /// primary quote is absent or fails.
    Mark,
    /// A `delayed_sip` quote, used only when neither the optional primary quote
    /// nor the mark can supply a reference. A real NBBO, fifteen minutes stale.
    DelayedSipQuote,
    /// The indicative overnight feed: the ONLY permissible source during
    /// the overnight session -- there is no fallback chain (RAI-1947
    /// contract).
    OvernightQuote,
}

impl ReferencePriceSource {
    const fn metric_label(self) -> &'static str {
        match self {
            Self::PrimaryQuote => "primary_quote",
            Self::Mark => "mark",
            Self::DelayedSipQuote => "delayed_sip_quote",
            Self::OvernightQuote => "overnight_quote",
        }
    }
}

/// The price an extended-hours limit is crossed from, and where it came from.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReferencePrice {
    pub(crate) price: Positive<Usd>,
    source: ReferencePriceSource,
}

/// Failure to establish any reference price. Only reached once *every* source
/// has been tried, because a flatten must not be skipped for want of a price.
#[derive(Debug)]
pub(crate) enum ReferencePriceError {
    /// No source could supply a price, and none of them errored.
    Unavailable,
    /// The mark lookup failed and the quote could not cover for it. Selected
    /// when the mark failure has at least as useful a retry classification as
    /// the quote failure.
    MarkFetch(Box<dyn std::error::Error + Send + Sync>),
    /// The quote lookup failed after the mark was absent, or its failure has a
    /// better retry classification than a failed mark lookup.
    QuoteFetch(Box<dyn std::error::Error + Send + Sync>),
}

impl ReferencePriceError {
    /// The `(symbol, reason)` dedup key this cause would page under had it
    /// been raised inside the hedge job. The scan drops a buy before that job
    /// exists, so it pages under the same key to stay deduped against it.
    pub(crate) const fn dead_letter_reason(&self) -> DeadLetterReason {
        DeadLetterReason::SymbolScoped(match self {
            Self::Unavailable => SymbolScopedReason::LimitQuoteUnavailable,
            Self::MarkFetch(_) => SymbolScopedReason::MarkFetch,
            Self::QuoteFetch(_) => SymbolScopedReason::LimitQuoteFetch,
        })
    }

    fn into_trade_accounting_error(self, symbol: &Symbol) -> TradeAccountingError {
        match self {
            Self::Unavailable => TradeAccountingError::LimitQuoteUnavailable {
                symbol: symbol.clone(),
            },
            Self::MarkFetch(source) => TradeAccountingError::MarkFetch {
                symbol: symbol.clone(),
                source,
            },
            Self::QuoteFetch(source) => TradeAccountingError::LimitQuoteFetch {
                symbol: symbol.clone(),
                source,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CloseFlattenBlockReason {
    ReferencePriceUnavailable,
    MarkFetchFailed,
    QuoteFetchFailed,
    InsufficientEquity,
    InsufficientBuyingPower,
}

impl CloseFlattenBlockReason {
    const fn metric_label(self) -> &'static str {
        match self {
            Self::ReferencePriceUnavailable => "reference_price_unavailable",
            Self::MarkFetchFailed => "mark_fetch_failed",
            Self::QuoteFetchFailed => "quote_fetch_failed",
            Self::InsufficientEquity => "insufficient_equity",
            Self::InsufficientBuyingPower => "insufficient_buying_power",
        }
    }
}

impl From<&ReferencePriceError> for CloseFlattenBlockReason {
    fn from(error: &ReferencePriceError) -> Self {
        match error {
            ReferencePriceError::Unavailable => Self::ReferencePriceUnavailable,
            ReferencePriceError::MarkFetch(_) => Self::MarkFetchFailed,
            ReferencePriceError::QuoteFetch(_) => Self::QuoteFetchFailed,
        }
    }
}

impl From<&CounterTradeSkipReason> for CloseFlattenBlockReason {
    fn from(reason: &CounterTradeSkipReason) -> Self {
        match reason {
            CounterTradeSkipReason::InsufficientEquity { .. } => Self::InsufficientEquity,
            CounterTradeSkipReason::InsufficientBuyingPower { .. } => Self::InsufficientBuyingPower,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ReferencePriceRetryability {
    PermanentOrUnknown,
    Transient,
    Backpressure,
}

fn reference_price_retryability(
    error: &(dyn std::error::Error + Send + Sync + 'static),
) -> ReferencePriceRetryability {
    if find_backpressure(error).is_some() {
        ReferencePriceRetryability::Backpressure
    } else if find_permanence(error) == Some(Permanence::Transient) {
        ReferencePriceRetryability::Transient
    } else {
        ReferencePriceRetryability::PermanentOrUnknown
    }
}

fn prefer_reference_price_failure(
    current: ReferencePriceError,
    candidate: ReferencePriceError,
) -> ReferencePriceError {
    let retryability = |error: &ReferencePriceError| match error {
        ReferencePriceError::MarkFetch(source) | ReferencePriceError::QuoteFetch(source) => {
            reference_price_retryability(source.as_ref())
        }
        ReferencePriceError::Unavailable => ReferencePriceRetryability::PermanentOrUnknown,
    };

    if retryability(&candidate) > retryability(&current) {
        candidate
    } else {
        current
    }
}

/// Resolves the price an extended-hours limit order is crossed from.
///
/// An optional current bid/ask quote comes first. The mark is the required
/// fallback, followed by a `delayed_sip` emergency quote. No primary provider is
/// currently wired, so today's effective order remains mark then delayed SIP
/// (ADR 0019).
///
/// A source error falls through as readily as a missing value does. The
/// position and market-data endpoints are separate services, so one being down
/// says nothing about the others, and flattening before a multi-day gap is
/// mandatory: a worse price always beats no fill. When every leg fails, the
/// error with the best retry path propagates: rate limiting before other
/// transient failures, then permanent or unclassified failures.
pub(crate) async fn resolve_extended_hours_reference_price(
    order_placer: &dyn OrderPlacer,
    symbol: &Symbol,
    direction: Direction,
) -> Result<ReferencePrice, ReferencePriceError> {
    let mut failure = match order_placer.fetch_primary_limit_quote(symbol).await {
        Ok(Some(quote)) => {
            return Ok(ReferencePrice {
                price: match direction {
                    Direction::Buy => quote.ask(),
                    Direction::Sell => quote.bid(),
                },
                source: ReferencePriceSource::PrimaryQuote,
            });
        }
        Ok(None) => None,
        Err(source) => {
            warn!(
                target: "hedge",
                %symbol,
                %source,
                "Primary limit-quote lookup failed; falling back to the broker mark"
            );
            Some(ReferencePriceError::QuoteFetch(source))
        }
    };

    match order_placer.fetch_position_mark(symbol).await {
        Ok(Some(price)) => {
            return Ok(ReferencePrice {
                price,
                source: ReferencePriceSource::Mark,
            });
        }
        Ok(None) => {
            debug!(
                target: "hedge",
                %symbol,
                "No broker mark for symbol; falling back to the emergency delayed quote"
            );
        }
        Err(source) => {
            warn!(
                target: "hedge",
                %symbol,
                %source,
                "Broker mark lookup failed; falling back to the emergency delayed quote"
            );
            let mark_failure = ReferencePriceError::MarkFetch(source);
            failure = Some(match failure {
                Some(primary_failure) => {
                    prefer_reference_price_failure(primary_failure, mark_failure)
                }
                None => mark_failure,
            });
        }
    }

    let quote = match order_placer.fetch_latest_quote(symbol).await {
        Ok(Some(quote)) => quote,
        Ok(None) => {
            return Err(failure.unwrap_or(ReferencePriceError::Unavailable));
        }
        Err(source) => {
            let quote_failure = ReferencePriceError::QuoteFetch(source);
            return Err(match failure {
                Some(previous_failure) => {
                    warn!(
                        target: "hedge",
                        %symbol,
                        error = ?quote_failure,
                        "Delayed-quote fallback also failed; surfacing the failure with the \
                         best retry path"
                    );
                    prefer_reference_price_failure(previous_failure, quote_failure)
                }
                None => quote_failure,
            });
        }
    };

    Ok(ReferencePrice {
        price: match direction {
            Direction::Buy => quote.ask(),
            Direction::Sell => quote.bid(),
        },
        source: ReferencePriceSource::DelayedSipQuote,
    })
}

/// Why an overnight reference price could not be established. Unlike the
/// extended-hours chain, every case defers the hedge: the indicative
/// overnight feed is the only permissible source (RAI-1947 contract), so
/// there is nothing to fall back to.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OvernightReferenceError {
    /// The overnight quote fetch failed or the feed had no quote.
    #[error("overnight quote fetch failed: {0}")]
    QuoteFetch(Box<dyn std::error::Error + Send + Sync>),
    /// The quote's own timestamp is older than the configured bound; a
    /// stale indicative quote must never be priced from.
    #[error(
        "overnight quote is {age:?} old, exceeding the configured maximum \
         of {max_age:?}"
    )]
    Stale {
        age: std::time::Duration,
        max_age: std::time::Duration,
    },
}

/// Records one deferred overnight hedge attempt with its concrete
/// reason, then reports the defer.
fn defer_overnight(symbol: &Symbol, reason: &'static str) -> Option<CounterTradeOrderKind> {
    counter!(
        "hedge_scan_skipped_total",
        "symbol" => symbol.to_string(),
        "session" => session_metric_label(MarketSession::Overnight),
        "reason" => reason
    )
    .increment(1);
    None
}

/// The overnight arm of order-kind selection. Every gate defers with a
/// concrete reason and no broker call: per-symbol opt-in, fail-closed
/// eligibility against the synced snapshot, and a priceable indicative
/// quote. A passing symbol gets an overnight limit crossed from the
/// indicative feed and bounded by `overnight_slippage_bps`.
async fn select_overnight_order_kind(
    ctx: &HedgeCtx,
    symbol: &Symbol,
    shares: Positive<FractionalShares>,
    direction: Direction,
    submitted_price_preflight: SubmittedPricePreflight,
) -> Result<Option<CounterTradeOrderKind>, TradeAccountingError> {
    if !ctx.assets.is_overnight_enabled(symbol) {
        info!(
            target: "hedge",
            %symbol,
            "Overnight session but symbol is not enabled for overnight counter-trading; \
             skipping, CheckPositions will re-enqueue"
        );
        return Ok(defer_overnight(symbol, "overnight_disabled"));
    }

    let now = chrono::Utc::now();
    let snapshot = ctx.overnight_eligibility.get(symbol);
    let shape = match shares.inner().is_whole() {
        Ok(true) => OvernightOrderShape::WholeShares,
        Ok(false) => OvernightOrderShape::Fractional,
        Err(error) => {
            warn!(
                target: "hedge",
                %symbol, ?error,
                "Quantity shape check failed; deferring the overnight hedge fail-closed"
            );
            return Ok(defer_overnight(symbol, "overnight_ineligible"));
        }
    };
    if let Err(error) = validate_overnight_eligibility(symbol, snapshot.as_ref(), shape, now) {
        info!(
            target: "hedge",
            %symbol, %error,
            "Overnight eligibility refused; deferring, CheckPositions will re-enqueue"
        );
        let reason = match &error {
            OvernightEligibilityError::OvernightHalted { .. } => "overnight_halted",
            OvernightEligibilityError::NoSnapshot { .. }
            | OvernightEligibilityError::StaleSnapshot { .. } => "stale_asset_sync",
            OvernightEligibilityError::NotTradable { .. }
            | OvernightEligibilityError::NotOvernightTradable { .. }
            | OvernightEligibilityError::FractionalNotEligible { .. } => "overnight_ineligible",
        };
        return Ok(defer_overnight(symbol, reason));
    }
    // Validation just proved the snapshot present.
    let Some(snapshot) = snapshot else {
        return Ok(defer_overnight(symbol, "stale_asset_sync"));
    };

    // Present whenever any asset is enabled (startup validation); absence
    // here is a wiring bug, so defer loudly rather than silently assume a
    // bound.
    let (Some(max_quote_age), Some(slippage_bps)) =
        (ctx.overnight_max_quote_age, ctx.overnight_slippage_bps)
    else {
        warn!(
            target: "hedge",
            %symbol,
            "Overnight knobs absent despite an enabled symbol; deferring fail-closed"
        );
        return Ok(defer_overnight(symbol, "overnight_ineligible"));
    };

    let reference = match resolve_overnight_reference_price(
        ctx.order_placer.as_ref(),
        symbol,
        direction,
        max_quote_age,
        now,
    )
    .await
    {
        Ok(reference) => reference,
        Err(error) => {
            info!(
                target: "hedge",
                %symbol, %error,
                "Overnight quote unpriceable; deferring, CheckPositions will re-enqueue"
            );
            return Ok(defer_overnight(symbol, "overnight_unpriceable"));
        }
    };

    counter!(
        "hedge_price_source_total",
        "symbol" => symbol.to_string(),
        "path" => "overnight",
        "source" => reference.source.metric_label()
    )
    .increment(1);

    let limit_price = apply_slippage(reference.price.inner(), direction, slippage_bps)
        .map_err(TradeAccountingError::SlippageCalculation)?;

    // Same submitted-price gate as extended hours, minus the
    // close-flatten dimensions (a non-concept overnight): a buy re-checks
    // cash against the exact limit it is about to submit.
    if direction == Direction::Buy
        && matches!(submitted_price_preflight, SubmittedPricePreflight::Required)
        && !overnight_preflight_at_submitted_price(ctx, symbol, shares, direction, limit_price)
            .await?
    {
        return Ok(defer_overnight(symbol, "overnight_preflight_blocked"));
    }

    Ok(Some(CounterTradeOrderKind::OvernightLimit {
        limit_price,
        snapshot,
        reference_price: Some(reference.price),
    }))
}

/// The overnight twin of `extended_hours_preflight_at_submitted_price`,
/// without the close-flatten coupling that path carries.
async fn overnight_preflight_at_submitted_price(
    ctx: &HedgeCtx,
    symbol: &Symbol,
    shares: Positive<FractionalShares>,
    direction: Direction,
    limit_price: Positive<Usd>,
) -> Result<bool, TradeAccountingError> {
    let order = MarketOrder {
        symbol: symbol.clone(),
        shares,
        direction,
        // Preflight only; this id is never sent to the broker. Use a fresh
        // value so callers cannot mistake it for a real key.
        client_order_id: ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
    };

    let preflight = ctx
        .order_placer
        .preflight_counter_trade_at_price(order, limit_price)
        .await
        .map_err(|source| TradeAccountingError::OvernightPreflightAtPrice {
            symbol: symbol.clone(),
            source,
        })?;

    match preflight {
        CounterTradePreflight::Allowed { .. } => Ok(true),
        CounterTradePreflight::Skipped(reason) => {
            warn!(
                target: "hedge",
                %symbol, %reason, %limit_price,
                "Overnight hedge blocked at submission time: the exact limit no longer \
                 passes the preflight"
            );
            Ok(false)
        }
    }
}

/// Resolves the overnight reference price from the indicative feed alone:
/// the ask for buys, the bid for sells. Crossed and non-positive quotes
/// are unrepresentable upstream (`LatestQuote` validates at
/// construction), so the only defer causes here are a failed fetch and
/// staleness against the quote's own timestamp.
pub(crate) async fn resolve_overnight_reference_price(
    order_placer: &dyn OrderPlacer,
    symbol: &Symbol,
    direction: Direction,
    max_quote_age: std::time::Duration,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<ReferencePrice, OvernightReferenceError> {
    let indicative = order_placer
        .fetch_latest_overnight_quote(symbol)
        .await
        .map_err(OvernightReferenceError::QuoteFetch)?;

    // Same skew clamp as the executor-side validator: a quote stamped
    // slightly ahead of our clock has age zero, never an unsigned wrap.
    let age = (now - indicative.at)
        .to_std()
        .unwrap_or(std::time::Duration::ZERO);

    // Recorded before the staleness gate on purpose: a distribution that
    // censored the stale tail would hide exactly the feed lag that makes
    // placements defer.
    histogram!(
        "hedge_quote_age_seconds",
        "symbol" => symbol.to_string(),
        "source" => ReferencePriceSource::OvernightQuote.metric_label()
    )
    .record(age.as_secs_f64());

    if age > max_quote_age {
        return Err(OvernightReferenceError::Stale {
            age,
            max_age: max_quote_age,
        });
    }

    Ok(ReferencePrice {
        price: match direction {
            Direction::Buy => indicative.quote.ask(),
            Direction::Sell => indicative.quote.bid(),
        },
        source: ReferencePriceSource::OvernightQuote,
    })
}

fn direction_label(direction: Direction) -> &'static str {
    match direction {
        Direction::Buy => "buy",
        Direction::Sell => "sell",
    }
}

/// Buckets the ramped cross to the nearest 100 bps below it, so the metric's
/// label cardinality stays at the handful of whole-percent steps between the
/// base and the ceiling rather than one series per distinct basis point.
fn cross_bucket_label(cross_bps: u16) -> String {
    (cross_bps / 100 * 100).to_string()
}

fn post_close_gap_label(post_close_gap: PostCloseGap) -> &'static str {
    match post_close_gap {
        PostCloseGap::OrdinaryOvernight => "ordinary_overnight",
        PostCloseGap::MultiDayClosure => "multi_day_closure",
        PostCloseGap::Unknown => "unknown",
    }
}

fn record_close_flatten_block(symbol: &Symbol, reason: CloseFlattenBlockReason) {
    counter!(
        "close_flatten_blocked_total",
        "symbol" => symbol.to_string(),
        "reason" => reason.metric_label()
    )
    .increment(1);
}

/// What [`recover_pending_poll_status`] found for the claim an earlier attempt
/// may have left behind. The dead-letter path branches on this: only
/// [`ClaimOutcome::NothingClaimed`] is a hedge this process gave up on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimOutcome {
    /// An earlier attempt's order is live at the broker -- it was already
    /// there, or this recovery re-drove it there -- and its poll job is
    /// armed, so the hedge is still in flight.
    Recovered,
    /// The order already filled. The hedge happened, so it is the opposite of
    /// an abandonment even though nothing is outstanding at the broker.
    Completed,
    /// Nothing is outstanding at the broker: no pending order, one that is
    /// already terminal, or a re-drive the broker rejected (which rolled the
    /// position back). Abandoning here abandons a hedge that was never
    /// placed.
    NothingClaimed,
}

/// Recovery path for the `PendingExecution` rejection. A previous attempt for
/// this position already claimed it, but may not have completed the broker
/// placement, so this retry reconciles the pending order's actual state:
///
/// - `Submitted`/`PartiallyFilled`: the order reached the broker but the prior
///   attempt may have failed to enqueue the `PollOrderStatus` job (e.g. the
///   queue push returned a transient error and apalis re-ran us), so re-enqueue
///   it, guarded by [`push_poll_job_if_absent`]: a retry against an
///   order that already has a live poll job (the common case -- this path re-runs
///   whenever `PendingExecution` is hit, not only after a lost push) must
///   skip the push, or it forks a new independent, self-perpetuating poll
///   chain for the same order every time it re-runs.
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
) -> Result<ClaimOutcome, TradeAccountingError> {
    use OffchainOrder::{
        Cancelled, Cancelling, Failed, Filled, PartiallyFilled, Pending, Submitted,
    };
    match ctx.offchain_order.load(&pending_id).await? {
        Some(Submitted { .. } | PartiallyFilled { .. } | Cancelling { .. }) => {
            push_poll_job_if_absent(ctx.poll_status_queue.clone(), pending_id, ctx.poll_interval)
                .await?;
            Ok(ClaimOutcome::Recovered)
        }
        Some(Pending {
            symbol,
            shares,
            direction,
            executor,
            market_session,
            ..
        }) => {
            // Must re-wrap: a symbol-scoped variant escaping here would
            // dead-letter an already-claimed position.
            let Some(order_kind) = select_order_kind_for_current_session(
                ctx,
                &symbol,
                shares,
                direction,
                market_session,
                SubmittedPricePreflight::SkipForIdempotentRecovery,
            )
            .await
            .map_err(|source| TradeAccountingError::ClaimedHedgeOrderKind {
                symbol: symbol.clone(),
                source: ClaimedHedgeOrderKindCause::classify(source),
            })?
            else {
                // The venue closed under the claim, so nothing can be placed
                // until it reopens.
                return Ok(ClaimOutcome::NothingClaimed);
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

            // Read before the outcome is routed (which consumes it): a
            // re-drive the broker rejected lands `Failed` and is rolled back,
            // so it leaves nothing in flight and must not read as recovered.
            let outcome = match &placed {
                Some(Submitted { .. } | PartiallyFilled { .. } | Cancelling { .. }) => {
                    ClaimOutcome::Recovered
                }
                Some(Failed { .. } | Cancelled { .. } | Pending { .. } | Filled { .. }) | None => {
                    ClaimOutcome::NothingClaimed
                }
            };

            route_placement_outcome(ctx, &symbol, pending_id, placed).await?;

            Ok(outcome)
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
            Ok(ClaimOutcome::NothingClaimed)
        }
        Some(Filled { .. }) => Ok(ClaimOutcome::Completed),
        None => Ok(ClaimOutcome::NothingClaimed),
    }
}

/// Routes the result of [`place_offchain_order_at_broker`] to its follow-up,
/// resolving the position claim for every outcome so it can never be left
/// stranded:
///
/// - `Failed`: roll the position back (clear the claim).
/// - `Submitted`/`PartiallyFilled`/`Cancelling`: enqueue a `PollOrderStatus`
///   job, guarded by [`push_poll_job_if_absent`] so a re-entrant call
///   against an order that already has a live poll job is skipped instead of
///   forking a new self-perpetuating chain.
/// - `None` (no order after a successful `Place`): clear the claim, since there
///   is nothing left to track.
/// - `Pending`/`Filled`: surface a retryable error without clearing the claim,
///   since the order may be live at the broker.
///
/// Shared by the primary placement path (a genuinely fresh order, where the
/// guard is always a no-op) and the `Pending` re-drive in
/// [`recover_pending_poll_status`] (where a concurrent recovery attempt for
/// the same `pending_id` may have already advanced the order to `Submitted`
/// and pushed its poll job before this call observes it -- the guard is what
/// makes that race safe), and kept in lockstep with the trade-processing
/// path's `dispatch_post_place_state`, so the placement paths cannot diverge.
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
                        // No broker terminality classification available
                        // here; fail-safe preserves.
                        anchor: AnchorDisposition::Preserve,
                    },
                )
                .await?;
        }

        Some(Submitted { .. } | PartiallyFilled { .. } | Cancelling { .. }) => {
            push_poll_job_if_absent(
                ctx.poll_status_queue.clone(),
                offchain_order_id,
                ctx.poll_interval,
            )
            .await?;

            // This symbol is placing again, so whatever paged for it has
            // resolved: release its alert slots. Without this the dedup set
            // latches for the process lifetime, and a failure that recurs the
            // following session -- the shape an entitlement or feed regression
            // takes -- would accumulate a standing delta with no page at all.
            ctx.alerted_dead_letters
                .lock()
                .await
                .retain(|(alerted, _)| alerted != symbol);
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
                        anchor: AnchorDisposition::Preserve,
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
        match self.perform_body(ctx).await {
            Ok(output) => Ok(output),
            Err(error) => self.handle_place_hedge_error(ctx, error).await,
        }
    }
}

impl PlaceHedge {
    async fn perform_body(&self, ctx: &HedgeCtx) -> Result<(), TradeAccountingError> {
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
            self.shares,
            self.direction,
            self.market_session,
            SubmittedPricePreflight::Required,
        )
        .await?
        else {
            // Not a plain Ok(()): a retry whose first attempt submitted the
            // order but lost the poll enqueue must re-enqueue it here, or
            // the live order sits un-polled (and its fill unrecorded) until
            // the next restart. A stale job may have a different ID from the
            // order that actually owns the position, so recover the live claim
            // under the same submission lock as every broker placement.
            self.recover_actual_pending_order(ctx).await?;
            return Ok(());
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
                recover_pending_poll_status(ctx, pending_id).await?;
                return Ok(());
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

    /// Handles a `perform_body` failure: reschedules with a classified delay
    /// on broker rate-limiting (429) instead of consuming the terminal retry
    /// budget, and hands anything else to [`Self::dead_letter_or_propagate`].
    ///
    /// `PositionCommand::PlaceOffChainOrder` claims the position before the
    /// broker call. A rescheduled successor therefore enters
    /// `recover_pending_poll_status`; when the 429 came from placement, the
    /// offchain order is still `Pending` and that recovery safely re-drives
    /// the idempotent broker call. A 429 before the claim commits (e.g. during
    /// the extended-hours price lookup) is likewise safe to reschedule.
    async fn handle_place_hedge_error(
        &self,
        ctx: &HedgeCtx,
        error: TradeAccountingError,
    ) -> Result<(), TradeAccountingError> {
        let Some(backpressure) = find_backpressure(&error) else {
            return self.dead_letter_or_propagate(ctx, error).await;
        };

        self.handle_backpressure(ctx, backpressure).await
    }

    async fn handle_backpressure(
        &self,
        ctx: &HedgeCtx,
        backpressure: Backpressure,
    ) -> Result<(), TradeAccountingError> {
        let step = advance_backpressure(&backpressure, self.backpressure_streak);
        let mut queue = ctx.hedge_queue.clone();
        let outcome = apply_backpressure_step(step, &mut queue, |next_streak| Self {
            symbol: self.symbol.clone(),
            direction: self.direction,
            shares: self.shares,
            executor: self.executor,
            threshold: self.threshold,
            offchain_order_id: self.offchain_order_id,
            market_session: self.market_session,
            backpressure_streak: next_streak,
            transient_streak: self.transient_streak,
        })
        .await?;

        match outcome {
            BackpressureOutcome::DeadLettered => {
                // Per the RAI-1494 plan's binding M2 decision (applied uniformly
                // to every supervised job): dead-letter instead of propagating
                // `Err` into the shared supervised on-event path.
                let BackpressureStreak(streak) = self.backpressure_streak;
                // Same counter as `dead_letter_or_propagate`'s exit, so
                // `hedge_dead_lettered_total` means "hedges this process gave
                // up on" for every abandonment path, distinguished by `reason`.
                counter!(
                    "hedge_dead_lettered_total",
                    "symbol" => self.symbol.to_string(),
                    "reason" => DeadLetterReason::BackpressureExhausted.metric_label()
                )
                .increment(1);
                error!(
                    target: "hedge",
                    symbol = %self.symbol,
                    offchain_order_id = %self.offchain_order_id,
                    streak,
                    limit = BACKPRESSURE_RESCHEDULE_LIMIT,
                    "PlaceHedge: broker rate-limiting exceeded the reschedule budget; \
                     dead-lettering this hedge instead of opening the circuit breaker \
                     -- treat as a structurally-dead Alpaca integration needing manual \
                     reconciliation"
                );
                alert_dead_letter(
                    ctx.notifier.as_ref(),
                    &ctx.alerted_dead_letters,
                    &self.symbol,
                    DeadLetterReason::BackpressureExhausted,
                    &format!(
                        "Hedge for {} abandoned: broker rate-limiting exceeded the \
                         {BACKPRESSURE_RESCHEDULE_LIMIT}-reschedule budget. The symbol carries \
                         a standing delta until the Alpaca integration is reconciled.",
                        self.symbol
                    ),
                )
                .await;
            }
            BackpressureOutcome::Rescheduled {
                next_streak: BackpressureStreak(streak),
                visible,
            } => {
                if visible {
                    error!(
                        target: "hedge",
                        symbol = %self.symbol,
                        offchain_order_id = %self.offchain_order_id,
                        streak,
                        "PlaceHedge: still rescheduling after sustained broker rate-limiting"
                    );
                }
            }
        }

        Ok(())
    }

    /// Decides what to do with a `perform_body` failure that is not broker
    /// rate-limiting: abandon this hedge, re-drive it, or propagate it into the
    /// normal retry/circuit-breaker path.
    ///
    /// Ordinary process-scoped failures still propagate. A pricing failure
    /// wrapped by `ClaimedHedgeOrderKind` is the narrow exception: it uses the
    /// same bounded budgets as its inner symbol-scoped cause, then leaves the
    /// existing claim for the periodic recovery sweep instead of fail-stopping.
    ///
    /// The underlying cause then decides how quickly the symbol is given up on,
    /// which the variant alone does not say: `MarkFetch`, `LimitQuoteFetch` and
    /// `CloseFlattenPreflightAtPrice` each box an opaque source that carries an
    /// entitlement 403 and a TCP reset alike. A permanent cause is abandoned at
    /// once, since re-asking cannot change the answer; a transient one is
    /// re-driven on this job's own bounded budget first, because abandoning it
    /// immediately would trade a one-second retry for a full `CheckPositions`
    /// interval of unhedged exposure. Either way the symbol is abandoned rather
    /// than the worker failed: a sustained per-symbol market-data outage must
    /// not stop hedging for every other symbol (RAI-1690).
    async fn dead_letter_or_propagate(
        &self,
        ctx: &HedgeCtx,
        error: TradeAccountingError,
    ) -> Result<(), TradeAccountingError> {
        if matches!(&error, TradeAccountingError::ClaimedHedgeOrderKind { .. }) {
            return self.handle_claimed_order_kind_failure(ctx, error).await;
        }

        let reason = match error.scope() {
            ErrorScope::ProcessScoped => return Err(error),
            ErrorScope::SymbolScoped { reason } => reason,
        };

        match find_permanence(&error) {
            Some(Permanence::Transient) => {
                let TransientFailureStreak(streak) = self.transient_streak;

                if streak < TRANSIENT_RESCHEDULE_LIMIT {
                    return self.redrive_transient_failure(ctx, streak, &error).await;
                }

                self.abandon_or_recover(ctx, reason, &error).await
            }

            // `None` means no broker failure anywhere in the chain: the
            // outcome was decided locally (no quote to price against, a
            // slippage floor), and re-asking cannot change it.
            Some(Permanence::Permanent) | None => {
                self.abandon_or_recover(ctx, reason, &error).await
            }
        }
    }

    async fn handle_claimed_order_kind_failure(
        &self,
        ctx: &HedgeCtx,
        error: TradeAccountingError,
    ) -> Result<(), TradeAccountingError> {
        let TradeAccountingError::ClaimedHedgeOrderKind { symbol, source } = error else {
            return Err(error);
        };
        let (reason, source) = match source {
            ClaimedHedgeOrderKindCause::SymbolScoped { reason, source } => (reason, source),
            source @ ClaimedHedgeOrderKindCause::ProcessScoped { .. } => {
                return Err(TradeAccountingError::ClaimedHedgeOrderKind { symbol, source });
            }
        };

        if let Some(backpressure) = find_backpressure(source.as_ref()) {
            return self.handle_backpressure(ctx, backpressure).await;
        }

        if find_permanence(source.as_ref()) == Some(Permanence::Transient) {
            let TransientFailureStreak(streak) = self.transient_streak;

            if streak < TRANSIENT_RESCHEDULE_LIMIT {
                return self
                    .redrive_transient_failure(ctx, streak, source.as_ref())
                    .await;
            }
        }

        self.record_abandoned_hedge(ctx, reason, source.as_ref())
            .await;

        Ok(())
    }

    /// Re-drives a transient symbol-scoped failure on this job's own durable
    /// budget: pushes a successor carrying the incremented streak and returns
    /// `Ok(())`, so the failure never reaches the supervised worker's retry
    /// budget, whose exhaustion stops the process.
    async fn redrive_transient_failure(
        &self,
        ctx: &HedgeCtx,
        streak: u32,
        error: &TradeAccountingError,
    ) -> Result<(), TradeAccountingError> {
        let next_streak = streak.saturating_add(1);
        let delay = TRANSIENT_RESCHEDULE_BASE.saturating_mul(2u32.saturating_pow(streak));

        ctx.hedge_queue
            .clone()
            .push_with_delay(
                Self {
                    transient_streak: TransientFailureStreak(next_streak),
                    ..self.clone()
                },
                delay,
            )
            .await?;

        warn!(
            target: "hedge",
            symbol = %self.symbol,
            offchain_order_id = %self.offchain_order_id,
            streak = next_streak,
            limit = TRANSIENT_RESCHEDULE_LIMIT,
            ?error,
            "PlaceHedge: transient symbol-scoped failure; re-driving this hedge instead of \
             spending the worker's retry budget, whose exhaustion would stop every symbol"
        );

        Ok(())
    }

    /// Recovers the order that currently owns this symbol's position claim.
    ///
    /// Recovery can re-drive a `Pending` order through the broker, so it must
    /// hold ADR 0014's submission lock. The job's own ID is only the fallback
    /// for a prior attempt whose position claim was already cleared.
    async fn recover_actual_pending_order(
        &self,
        ctx: &HedgeCtx,
    ) -> Result<ClaimOutcome, TradeAccountingError> {
        let _submission_guard = ctx.counter_trade_submission_lock.lock().await;
        let pending_id = ctx
            .position
            .load(&self.symbol)
            .await?
            .and_then(|position| position.pending_offchain_order_id)
            .unwrap_or(self.offchain_order_id);

        recover_pending_poll_status(ctx, pending_id).await
    }

    /// Resolves whatever an earlier attempt left claimed, then abandons this
    /// hedge only if nothing is outstanding at the broker.
    ///
    /// An earlier attempt may have claimed the position, and a hedge that
    /// recovery re-drove to the broker -- or one that already filled -- is not
    /// one this process gave up on. A pricing failure during that recovery is
    /// handled on its own bounded budget and deliberately leaves the claim for
    /// the periodic recovery sweep.
    async fn abandon_or_recover(
        &self,
        ctx: &HedgeCtx,
        reason: SymbolScopedReason,
        error: &TradeAccountingError,
    ) -> Result<(), TradeAccountingError> {
        // ADR 0014: the recovery below can re-drive an earlier attempt's
        // `Pending` order all the way through `place_offchain_order_at_broker`,
        // so it must hold the same submission lock every other placement path
        // takes -- `perform_body`'s guard was released before this handler ran.
        // Taken here rather than inside `recover_pending_poll_status`, whose
        // other call site already runs under the guard and would deadlock on a
        // non-reentrant mutex.
        let recovery = self.recover_actual_pending_order(ctx).await;
        let outcome = match recovery {
            Ok(outcome) => outcome,
            Err(error @ TradeAccountingError::ClaimedHedgeOrderKind { .. }) => {
                return self.handle_claimed_order_kind_failure(ctx, error).await;
            }
            Err(error) => return Err(error),
        };

        match outcome {
            ClaimOutcome::Recovered => {
                info!(
                    target: "hedge",
                    symbol = %self.symbol,
                    offchain_order_id = %self.offchain_order_id,
                    ?error,
                    "PlaceHedge: symbol-scoped failure, but an earlier attempt's order is live \
                     at the broker; the hedge is still in flight, so it is not counted or \
                     paged as abandoned"
                );

                Ok(())
            }

            ClaimOutcome::Completed => {
                info!(
                    target: "hedge",
                    symbol = %self.symbol,
                    offchain_order_id = %self.offchain_order_id,
                    ?error,
                    "PlaceHedge: symbol-scoped failure against an order that already filled; \
                     the hedge happened, so it is not counted or paged as abandoned"
                );

                Ok(())
            }

            ClaimOutcome::NothingClaimed => {
                self.record_abandoned_hedge(ctx, reason, error).await;

                Ok(())
            }
        }
    }

    /// Counts and pages a hedge this process gave up on, after
    /// [`Self::abandon_or_recover`] has established that nothing is
    /// outstanding at the broker.
    async fn record_abandoned_hedge(
        &self,
        ctx: &HedgeCtx,
        reason: SymbolScopedReason,
        error: &TradeAccountingError,
    ) {
        let reason = DeadLetterReason::SymbolScoped(reason);
        counter!(
            "hedge_dead_lettered_total",
            "symbol" => self.symbol.to_string(),
            "reason" => reason.metric_label()
        )
        .increment(1);
        error!(
            target: "hedge",
            symbol = %self.symbol,
            offchain_order_id = %self.offchain_order_id,
            reason = reason.metric_label(),
            ?error,
            "PlaceHedge: symbol-scoped failure the symbol cannot get past; dead-lettering \
             this hedge instead of exiting the process -- CheckPositions will re-enqueue on \
             its next scan"
        );
        alert_dead_letter(
            ctx.notifier.as_ref(),
            &ctx.alerted_dead_letters,
            &self.symbol,
            reason,
            &format!(
                "Hedge for {} abandoned: {} failure did not clear. CheckPositions keeps \
                 re-enqueueing it, so the symbol carries a standing delta until the \
                 market-data failure is fixed.",
                self.symbol,
                reason.metric_label()
            ),
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use std::any::type_name;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};

    use alloy::primitives::{Address, TxHash};
    use proptest::prelude::*;
    use tokio::sync::Notify;
    use uuid::Uuid;

    use st0x_config::{EquitiesConfig, EquityAssetConfig, ExecutionThreshold, OperationMode};
    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::{
        ClientOrderId, Direction, ExecutorOrderId, FractionalShares, IndicativeQuote, LatestQuote,
        MockExecutor, Positive, SupportedExecutor, Symbol,
    };
    use st0x_finance::Usd;
    use st0x_float_macro::float;

    use super::*;
    use crate::conductor::job::Job;
    use crate::offchain::order::{
        OffchainOrder, OffchainOrderCommand, OrderPlacementResult, OrderPlacer,
    };
    use crate::position::{AnchorDisposition, Position, PositionCommand, TradeId};
    use crate::test_utils::TEST_POLL_INTERVAL;

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
                        vault_ids: Vec::new(),
                        trading: OperationMode::Disabled,
                        rebalancing: OperationMode::Disabled,
                        wrapped_equity_recovery: OperationMode::Disabled,
                        extended_hours_counter_trading,
                        overnight_counter_trading: OperationMode::Disabled,
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

    /// Succeeds like [`succeeding_order_placer`], but records every
    /// `client_order_id` submitted with `place_market_order`, letting a test
    /// assert on the key the real placement path actually derived.
    fn capturing_order_placer() -> (Arc<dyn OrderPlacer>, Arc<StdMutex<Vec<ClientOrderId>>>) {
        struct CapturingPlacer {
            captured_client_order_ids: Arc<StdMutex<Vec<ClientOrderId>>>,
        }

        #[async_trait::async_trait]
        impl OrderPlacer for CapturingPlacer {
            async fn place_market_order(
                &self,
                order: st0x_execution::MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                self.captured_client_order_ids
                    .lock()
                    .unwrap()
                    .push(order.client_order_id);
                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("test-order-123"),
                    placed_shares: order.shares,
                    is_extended_hours: false,
                    limit_price: None,
                })
            }

            async fn place_limit_order(
                &self,
                _order: st0x_execution::LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("regular-session test must not place a limit order".into())
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &st0x_execution::ExecutorOrderId,
            ) -> Result<st0x_execution::CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(st0x_execution::CancellationOutcome::Requested)
            }
        }

        let captured_client_order_ids = Arc::new(StdMutex::new(Vec::new()));
        (
            Arc::new(CapturingPlacer {
                captured_client_order_ids: captured_client_order_ids.clone(),
            }),
            captured_client_order_ids,
        )
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

    /// Captures every delivered message, and can be switched to fail delivery
    /// so a test can exercise the notifier error path this module swallows.
    /// Local rather than shared, matching the per-module failing doubles in
    /// `rebalancing::usdc::job` and `conductor`.
    #[derive(Default)]
    struct FlakyNotifier {
        delivered: StdMutex<Vec<String>>,
        failing: AtomicBool,
    }

    impl FlakyNotifier {
        fn messages(&self) -> Vec<String> {
            self.delivered.lock().unwrap().clone()
        }

        fn fail_delivery(&self, failing: bool) {
            self.failing.store(failing, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl crate::alerts::Notifier for FlakyNotifier {
        async fn notify(&self, message: &str) -> Result<(), crate::alerts::NotifierError> {
            if self.failing.load(Ordering::SeqCst) {
                return Err(crate::alerts::NotifierError::ApiError {
                    status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
                    body: "injected delivery failure".to_string(),
                });
            }

            self.delivered.lock().unwrap().push(message.to_string());
            Ok(())
        }
    }

    #[derive(Default)]
    struct PausingNotifier {
        calls: AtomicUsize,
        started: Notify,
        release: Notify,
    }

    #[async_trait::async_trait]
    impl crate::alerts::Notifier for PausingNotifier {
        async fn notify(&self, _message: &str) -> Result<(), crate::alerts::NotifierError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            self.release.notified().await;
            Ok(())
        }
    }

    #[derive(Default)]
    struct HangingNotifier {
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::alerts::Notifier for HangingNotifier {
        async fn notify(&self, _message: &str) -> Result<(), crate::alerts::NotifierError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            std::future::pending().await
        }
    }

    struct TestInfra {
        ctx: HedgeCtx,
        apalis_pool: apalis_sqlite::SqlitePool,
        position_projection: Arc<st0x_event_sorcery::Projection<Position>>,
        offchain_order_projection: Arc<st0x_event_sorcery::Projection<OffchainOrder>>,
        /// The same instance `ctx.notifier` points at, so a test can read
        /// back the operator pages a dead-letter delivered.
        notifier: Arc<FlakyNotifier>,
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

        let notifier = Arc::new(FlakyNotifier::default());

        let ctx = HedgeCtx {
            position: position.clone(),
            offchain_order,
            poll_status_queue: PollOrderStatusJobQueue::new(&apalis_pool),
            hedge_queue: HedgeJobQueue::new(&apalis_pool),
            // The placer doubles as the session source; the default stubs
            // report a Regular session, so these ctxs exercise the regular
            // market-order path. AAPL is enabled for extended hours so the
            // Regular path's session gate is not what skips them.
            order_placer,
            assets: extended_hours_assets("AAPL", true),
            close_flatten_policy: CloseFlattenPolicy::from_secs(900).unwrap(),
            close_flatten_ramp: CloseFlattenCrossRamp::new(100, 400).unwrap(),
            overnight_eligibility: EligibilitySnapshots::default(),
            overnight_max_quote_age: Some(std::time::Duration::from_secs(30)),
            overnight_slippage_bps: Some(150),
            counter_trade_submission_lock: Arc::new(Mutex::new(())),
            poll_interval: TEST_POLL_INTERVAL,
            notifier: notifier.clone(),
            alerted_dead_letters: Arc::new(Mutex::new(HashSet::new())),
        };

        TestInfra {
            ctx,
            apalis_pool,
            position_projection,
            offchain_order_projection,
            notifier,
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
                    block_number: None,
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
            backpressure_streak: BackpressureStreak::default(),
            transient_streak: TransientFailureStreak::default(),
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

    #[test]
    fn place_hedge_payload_without_backpressure_streak_deserializes_to_zero() {
        let symbol = Symbol::new("AAPL").unwrap();
        let offchain_order_id = OffchainOrderId::new();
        let payload = serde_json::json!({
            "symbol": symbol,
            "direction": Direction::Sell,
            "shares": Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
            "executor": SupportedExecutor::DryRun,
            "threshold": ExecutionThreshold::whole_share(),
            "offchain_order_id": offchain_order_id,
            "market_session": MarketSession::Regular,
        });

        let job: PlaceHedge = serde_json::from_value(payload).unwrap();

        assert_eq!(job.backpressure_streak, BackpressureStreak::default());
        assert_eq!(
            job.transient_streak,
            TransientFailureStreak::default(),
            "a row enqueued before the transient budget existed must decode as an unspent \
             budget rather than fail the poll stream"
        );
    }

    fn place_hedge_job_type() -> &'static str {
        std::any::type_name::<PlaceHedge>()
    }

    async fn successor_backpressure_streak(apalis_pool: &apalis_sqlite::SqlitePool) -> i64 {
        let streaks: Vec<i64> = sqlx_apalis::query_scalar(
            "SELECT json_extract(CAST(job AS TEXT), '$.backpressure_streak') FROM Jobs \
             WHERE job_type = ?",
        )
        .bind(place_hedge_job_type())
        .fetch_all(apalis_pool)
        .await
        .unwrap();

        assert_eq!(
            streaks.len(),
            1,
            "a 429 must enqueue exactly one PlaceHedge successor"
        );
        streaks.into_iter().next().unwrap()
    }

    fn alpaca_429(retry_after_millis: u64) -> TradeAccountingError {
        TradeAccountingError::AlpacaBrokerApi(st0x_execution::AlpacaBrokerApiError::ApiError {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            alpaca_code: None,
            message: "rate limited".to_string(),
            retry_after: Some(Duration::from_millis(retry_after_millis)),
        })
    }

    /// Exercises the shared handler directly; end-to-end placement
    /// backpressure is covered separately below.
    #[tokio::test]
    async fn place_hedge_429_reschedules_with_incremented_streak_and_does_not_propagate_err() {
        let TestInfra {
            ctx, apalis_pool, ..
        } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let job = hedge_job(&symbol, 2.0, Direction::Sell);

        job.handle_place_hedge_error(&ctx, alpaca_429(1))
            .await
            .unwrap();

        assert_eq!(successor_backpressure_streak(&apalis_pool).await, 1);
    }

    #[tokio::test]
    async fn place_hedge_429_past_reschedule_limit_dead_letters_without_propagating_err() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let TestInfra {
            ctx,
            apalis_pool,
            notifier,
            ..
        } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let mut job = hedge_job(&symbol, 2.0, Direction::Sell);
        job.backpressure_streak = BackpressureStreak(BACKPRESSURE_RESCHEDULE_LIMIT);

        job.handle_place_hedge_error(&ctx, alpaca_429(1))
            .await
            .unwrap();

        let job_count: i64 =
            sqlx_apalis::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(place_hedge_job_type())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        assert_eq!(
            job_count, 0,
            "an exhausted backpressure streak must dead-letter, not reschedule"
        );

        let rendered = metrics_handle.render();
        assert_eq!(
            dead_letter_count(&rendered, &symbol, DeadLetterReason::BackpressureExhausted),
            1,
            "an exhausted streak must be counted on the shared dead-letter counter, \
             in:\n{rendered}"
        );

        assert_eq!(
            notifier.messages(),
            vec![format!(
                "Hedge for AAPL abandoned: broker rate-limiting exceeded the \
                 {BACKPRESSURE_RESCHEDULE_LIMIT}-reschedule budget. The symbol carries a \
                 standing delta until the Alpaca integration is reconciled."
            )]
        );
    }

    /// A close-flatten session status whose extended session closes exactly
    /// one policy window from now, so `now` sits at the ramp's start and the
    /// cross is the base band. Any nearer close would put a price assertion on
    /// a moving ramp, so every close-flatten placer shares this one status
    /// rather than repeating the literal.
    fn ramp_start_session_status() -> st0x_execution::MarketSessionStatus {
        st0x_execution::MarketSessionStatus {
            session: MarketSession::Extended,
            extended_session_closes_at: Some(chrono::Utc::now() + chrono::TimeDelta::seconds(900)),
            post_close_gap: st0x_execution::PostCloseGap::MultiDayClosure,
        }
    }

    fn dead_letter_page(symbol: &str, reason: &str) -> String {
        format!(
            "Hedge for {symbol} abandoned: {reason} failure did not clear. CheckPositions \
             keeps re-enqueueing it, so the symbol carries a standing delta until the \
             market-data failure is fixed."
        )
    }

    #[tokio::test]
    async fn dead_letter_pages_the_operator_once_per_symbol_and_reason() {
        let TestInfra { ctx, notifier, .. } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let job = hedge_job(&symbol, 2.0, Direction::Sell);

        for _ in 0..2 {
            job.handle_place_hedge_error(
                &ctx,
                quote_fetch_failure(&symbol, reqwest::StatusCode::FORBIDDEN),
            )
            .await
            .unwrap_or_else(|error| panic!("expected a 403 quote fetch to dead-letter: {error:?}"));
        }

        assert_eq!(
            notifier.messages(),
            vec![dead_letter_page("AAPL", "limit_quote_fetch")]
        );

        job.handle_place_hedge_error(
            &ctx,
            TradeAccountingError::LimitQuoteUnavailable {
                symbol: symbol.clone(),
            },
        )
        .await
        .unwrap_or_else(|error| panic!("expected an unavailable price to dead-letter: {error:?}"));

        // One shared 403 abandons every symbol it touches, so the symbol half
        // of the dedup key is what keeps the other symbols visible.
        let other_symbol = Symbol::new("TSLA").unwrap();
        let other_job = hedge_job(&other_symbol, 2.0, Direction::Sell);
        other_job
            .handle_place_hedge_error(
                &ctx,
                quote_fetch_failure(&other_symbol, reqwest::StatusCode::FORBIDDEN),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("expected a second symbol's 403 quote fetch to dead-letter: {error:?}")
            });

        assert_eq!(
            notifier.messages(),
            vec![
                dead_letter_page("AAPL", "limit_quote_fetch"),
                dead_letter_page("AAPL", "limit_quote_unavailable"),
                dead_letter_page("TSLA", "limit_quote_fetch"),
            ],
            "a second reason for the same symbol, and the same reason for a second symbol, are \
             each a separate thing to fix and must page"
        );
    }

    /// Delivery failure must not consume the pair's one alert slot: the
    /// abandonment is still recorded, and the page is re-attempted on the next
    /// `CheckPositions` scan. Latching on a failed send would leave the
    /// operator with no push signal at all for that symbol.
    #[tokio::test]
    async fn dead_letter_page_that_fails_to_deliver_is_retried_on_the_next_scan() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let TestInfra { ctx, notifier, .. } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let job = hedge_job(&symbol, 2.0, Direction::Sell);
        let reason = DeadLetterReason::SymbolScoped(SymbolScopedReason::LimitQuoteFetch);

        notifier.fail_delivery(true);
        job.handle_place_hedge_error(
            &ctx,
            quote_fetch_failure(&symbol, reqwest::StatusCode::FORBIDDEN),
        )
        .await
        .unwrap_or_else(|error| panic!("a failed page must not abort the dead-letter: {error:?}"));

        assert_eq!(
            notifier.messages(),
            Vec::<String>::new(),
            "the injected failure must have dropped the page"
        );
        let rendered = metrics_handle.render();
        assert_eq!(
            dead_letter_count(&rendered, &symbol, reason),
            1,
            "the abandonment is counted whether or not its page was delivered, in:\n{rendered}"
        );

        notifier.fail_delivery(false);
        job.handle_place_hedge_error(
            &ctx,
            quote_fetch_failure(&symbol, reqwest::StatusCode::FORBIDDEN),
        )
        .await
        .unwrap_or_else(|error| panic!("expected a 403 quote fetch to dead-letter: {error:?}"));

        assert_eq!(
            notifier.messages(),
            vec![dead_letter_page("AAPL", "limit_quote_fetch")]
        );
    }

    #[tokio::test]
    async fn concurrent_dead_letter_pages_reserve_one_delivery_slot() {
        let notifier = PausingNotifier::default();
        let alerted = Mutex::new(HashSet::new());
        let symbol = Symbol::new("AAPL").unwrap();
        let reason = DeadLetterReason::SymbolScoped(SymbolScopedReason::LimitQuoteFetch);

        tokio::join!(
            alert_dead_letter(&notifier, &alerted, &symbol, reason, "first"),
            async {
                notifier.started.notified().await;
                alert_dead_letter(&notifier, &alerted, &symbol, reason, "second").await;
                notifier.release.notify_one();
            }
        );

        assert_eq!(
            notifier.calls.load(Ordering::SeqCst),
            1,
            "the scan and hedge workers must not deliver the same page concurrently"
        );
        assert!(
            alerted.lock().await.contains(&(symbol, reason)),
            "a successful delivery must keep the reserved slot latched"
        );
    }

    #[tokio::test]
    async fn an_in_flight_page_cannot_relatch_a_symbol_after_it_recovers() {
        let notifier = PausingNotifier::default();
        let alerted = Mutex::new(HashSet::new());
        let symbol = Symbol::new("AAPL").unwrap();
        let reason = DeadLetterReason::SymbolScoped(SymbolScopedReason::LimitQuoteFetch);
        let key = (symbol.clone(), reason);

        tokio::join!(
            alert_dead_letter(&notifier, &alerted, &symbol, reason, "page"),
            async {
                notifier.started.notified().await;
                assert!(
                    alerted.lock().await.remove(&key),
                    "the in-flight delivery must have reserved its slot before notifying"
                );
                notifier.release.notify_one();
            }
        );

        assert!(
            alerted.lock().await.is_empty(),
            "a delivery that completes after recovery must not reinsert a stale slot"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_hung_dead_letter_notifier_is_bounded_and_retried() {
        let notifier = HangingNotifier::default();
        let alerted = Mutex::new(HashSet::new());
        let symbol = Symbol::new("AAPL").unwrap();
        let reason = DeadLetterReason::SymbolScoped(SymbolScopedReason::LimitQuoteFetch);

        for _ in 0..2 {
            alert_dead_letter(&notifier, &alerted, &symbol, reason, "page").await;
        }

        assert_eq!(
            notifier.calls.load(Ordering::SeqCst),
            2,
            "timing out must release the slot so the next scan can retry"
        );
        assert!(
            alerted.lock().await.is_empty(),
            "a delivery that never completed must not suppress the next page"
        );
    }

    /// The dedup set must not latch for the life of the process: a failure
    /// that recurs a session later -- the shape an entitlement or feed
    /// regression takes -- would otherwise accumulate a standing delta with no
    /// push signal at all, since the bot runs for weeks between deploys. A
    /// hedge reaching the broker is what says the condition resolved.
    #[tokio::test]
    async fn a_symbol_that_hedges_again_pages_on_its_next_abandonment() {
        let TestInfra { ctx, notifier, .. } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let abandoned = hedge_job(&symbol, 2.0, Direction::Sell);

        abandoned
            .handle_place_hedge_error(
                &ctx,
                quote_fetch_failure(&symbol, reqwest::StatusCode::FORBIDDEN),
            )
            .await
            .unwrap_or_else(|error| panic!("expected a 403 quote fetch to dead-letter: {error:?}"));

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;
        let recovered = hedge_job(&symbol, 2.0, Direction::Sell);
        recovered
            .perform(&ctx)
            .await
            .expect("the recovered symbol must place");
        let alerts_after_recovery = ctx.alerted_dead_letters.lock().await.clone();
        assert!(
            alerts_after_recovery.is_empty(),
            "a successful placement must release every alert for the symbol, still latched: \
             {alerts_after_recovery:?}"
        );
        ctx.offchain_order
            .send(
                &recovered.offchain_order_id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.0)),
                    filled_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
        ctx.position
            .send(
                &symbol,
                PositionCommand::CompleteOffChainOrder {
                    offchain_order_id: recovered.offchain_order_id,
                    shares_filled: recovered.shares,
                    direction: recovered.direction,
                    executor_order_id: ExecutorOrderId::new("test-order-123"),
                    price: Usd::new(float!(150.0)),
                    broker_timestamp: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        abandoned
            .handle_place_hedge_error(
                &ctx,
                quote_fetch_failure(&symbol, reqwest::StatusCode::FORBIDDEN),
            )
            .await
            .unwrap_or_else(|error| panic!("expected a 403 quote fetch to dead-letter: {error:?}"));

        assert_eq!(
            notifier.messages(),
            vec![
                dead_letter_page("AAPL", "limit_quote_fetch"),
                dead_letter_page("AAPL", "limit_quote_fetch"),
            ],
            "a failure that returns after the symbol recovered is a new thing to fix and \
             must page again"
        );
    }

    #[tokio::test]
    async fn place_hedge_non_backpressure_error_propagates_unchanged() {
        let TestInfra { ctx, .. } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let job = hedge_job(&symbol, 2.0, Direction::Sell);
        let error =
            TradeAccountingError::AlpacaBrokerApi(st0x_execution::AlpacaBrokerApiError::ApiError {
                status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
                alpaca_code: None,
                message: "boom".to_string(),
                retry_after: None,
            });

        let result = job.handle_place_hedge_error(&ctx, error).await;
        let Err(TradeAccountingError::AlpacaBrokerApi(
            st0x_execution::AlpacaBrokerApiError::ApiError { status, .. },
        )) = result
        else {
            panic!("expected the non-backpressure error to propagate unchanged");
        };
        assert_eq!(status, reqwest::StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn place_hedge_market_session_check_error_propagates_unchanged() {
        let TestInfra { ctx, .. } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let job = hedge_job(&symbol, 2.0, Direction::Sell);
        let error = TradeAccountingError::MarketSessionCheck {
            symbol: symbol.clone(),
            source: "broker calendar endpoint down".into(),
        };

        let result = job.handle_place_hedge_error(&ctx, error).await;

        assert!(
            matches!(result, Err(TradeAccountingError::MarketSessionCheck { .. })),
            "expected MarketSessionCheck to propagate, got: {result:?}"
        );
    }

    /// Reads the `hedge_dead_lettered_total` sample for exactly this
    /// `symbol`/`reason` pair. Matching the counter's own series, rather than
    /// a substring of the whole render, is what makes an assertion fail when
    /// the label is wrong instead of passing because some other metric
    /// happens to carry the same symbol.
    /// Reads the value of the single `metric` sample carrying every label in
    /// `labels`. Matching the series, rather than a substring of the whole
    /// render, is what makes an assertion fail when a label is wrong instead
    /// of passing because some other metric carries the same symbol.
    fn counter_value(rendered: &str, metric: &str, labels: &[(&str, &str)]) -> u64 {
        let prefix = format!("{metric}{{");
        let Some(sample) = rendered.lines().find(|line| {
            line.starts_with(&prefix)
                && labels
                    .iter()
                    .all(|(name, value)| line.contains(&format!("{name}=\"{value}\"")))
        }) else {
            return 0;
        };

        let (_, value) = sample
            .rsplit_once(' ')
            .unwrap_or_else(|| panic!("malformed Prometheus sample line: {sample}"));
        value
            .parse()
            .unwrap_or_else(|error| panic!("unparseable counter value in {sample}: {error}"))
    }

    /// The bucket is a label dashboards group by, so it is a wire format:
    /// pinned to literals rather than derived from the same expression the
    /// implementation uses, which would make any change self-consistent.
    #[test]
    fn cross_bucket_labels_are_stable() {
        assert_eq!(cross_bucket_label(100), "100");
        assert_eq!(cross_bucket_label(175), "100");
        assert_eq!(cross_bucket_label(199), "100");
        assert_eq!(cross_bucket_label(200), "200");
        assert_eq!(cross_bucket_label(400), "400");
    }

    /// The `source` label is likewise a wire format, and the only signal
    /// telling an operator whether the delayed-quote fallback is a corner case
    /// or load-bearing.
    #[test]
    fn reference_price_source_metric_labels_are_stable() {
        assert_eq!(
            ReferencePriceSource::PrimaryQuote.metric_label(),
            "primary_quote"
        );
        assert_eq!(ReferencePriceSource::Mark.metric_label(), "mark");
        assert_eq!(
            ReferencePriceSource::DelayedSipQuote.metric_label(),
            "delayed_sip_quote"
        );
    }

    #[test]
    fn close_flatten_block_reason_metric_labels_are_stable() {
        assert_eq!(
            CloseFlattenBlockReason::ReferencePriceUnavailable.metric_label(),
            "reference_price_unavailable"
        );
        assert_eq!(
            CloseFlattenBlockReason::MarkFetchFailed.metric_label(),
            "mark_fetch_failed"
        );
        assert_eq!(
            CloseFlattenBlockReason::QuoteFetchFailed.metric_label(),
            "quote_fetch_failed"
        );
        assert_eq!(
            CloseFlattenBlockReason::InsufficientEquity.metric_label(),
            "insufficient_equity"
        );
        assert_eq!(
            CloseFlattenBlockReason::InsufficientBuyingPower.metric_label(),
            "insufficient_buying_power"
        );
    }

    fn dead_letter_count(rendered: &str, symbol: &Symbol, reason: DeadLetterReason) -> u64 {
        let label = reason.metric_label();
        let Some(sample) = rendered.lines().find(|line| {
            line.starts_with("hedge_dead_lettered_total{")
                && line.contains(&format!("reason=\"{label}\""))
                && line.contains(&format!("symbol=\"{symbol}\""))
        }) else {
            return 0;
        };

        let (_, value) = sample
            .rsplit_once(' ')
            .unwrap_or_else(|| panic!("malformed Prometheus sample line: {sample}"));
        value
            .parse()
            .unwrap_or_else(|error| panic!("unparseable counter value in {sample}: {error}"))
    }

    /// Every `SymbolScopedReason`, and therefore every symbol-scoped
    /// `TradeAccountingError` variant, listed once. The list is hand-written
    /// because the enum carries no iteration, but [`sample_symbol_scoped_error`]
    /// matches it exhaustively, so a new variant cannot be added without the
    /// compiler pointing at this pair.
    const EVERY_SYMBOL_SCOPED_REASON: [SymbolScopedReason; 6] = [
        SymbolScopedReason::MarkFetch,
        SymbolScopedReason::LimitQuoteFetch,
        SymbolScopedReason::LimitQuoteUnavailable,
        SymbolScopedReason::SlippageCalculation,
        SymbolScopedReason::CloseFlattenPreflightAtPrice,
        SymbolScopedReason::OvernightPreflightAtPrice,
    ];

    /// Builds the error variant `scope()` classifies as `reason`, with a cause
    /// carrying no broker classification so every one of them is abandoned
    /// rather than re-driven.
    fn sample_symbol_scoped_error(
        reason: SymbolScopedReason,
        symbol: &Symbol,
    ) -> TradeAccountingError {
        match reason {
            SymbolScopedReason::MarkFetch => TradeAccountingError::MarkFetch {
                symbol: symbol.clone(),
                source: "positions endpoint unavailable".into(),
            },
            SymbolScopedReason::LimitQuoteFetch => TradeAccountingError::LimitQuoteFetch {
                symbol: symbol.clone(),
                source: "quote endpoint unavailable".into(),
            },
            SymbolScopedReason::LimitQuoteUnavailable => {
                TradeAccountingError::LimitQuoteUnavailable {
                    symbol: symbol.clone(),
                }
            }
            SymbolScopedReason::SlippageCalculation => TradeAccountingError::SlippageCalculation(
                apply_slippage(Usd::new(float!(0.50)), Direction::Sell, 9999).expect_err(
                    "max slippage on a sub-dollar sell must floor to a non-positive price",
                ),
            ),
            SymbolScopedReason::CloseFlattenPreflightAtPrice => {
                TradeAccountingError::CloseFlattenPreflightAtPrice {
                    symbol: symbol.clone(),
                    source: "preflight endpoint unavailable".into(),
                }
            }
            SymbolScopedReason::OvernightPreflightAtPrice => {
                TradeAccountingError::OvernightPreflightAtPrice {
                    symbol: symbol.clone(),
                    source: "preflight endpoint unavailable".into(),
                }
            }
        }
    }

    /// Every symbol-scoped `TradeAccountingError` variant must
    /// dead-letter -- returning `Ok(())` instead of propagating -- and must
    /// record its own `reason` label, so the skip is visible in both logs and
    /// `hedge_dead_lettered_total` rather than silently stopping the process.
    #[tokio::test]
    async fn place_hedge_dead_letters_every_symbol_scoped_variant_with_its_metric_reason() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let TestInfra { ctx, .. } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let job = hedge_job(&symbol, 2.0, Direction::Sell);

        for expected_reason in EVERY_SYMBOL_SCOPED_REASON {
            let error = sample_symbol_scoped_error(expected_reason, &symbol);
            let reason = DeadLetterReason::SymbolScoped(expected_reason);
            let label = reason.metric_label();
            let result = job.handle_place_hedge_error(&ctx, error).await;
            result.unwrap_or_else(|error| {
                panic!("expected {label} to dead-letter, got Err: {error:?}")
            });

            let rendered = metrics_handle.render();
            assert_eq!(
                dead_letter_count(&rendered, &symbol, reason),
                1,
                "expected exactly one hedge_dead_lettered_total{{symbol=\"AAPL\",\
                 reason=\"{label}\"}} in:\n{rendered}"
            );
        }
    }

    /// Wraps a market-data status the way the real Alpaca executor does:
    /// `LimitQuoteFetch` boxes a `dyn Error`, which is the broker error,
    /// which boxes the market-data error carrying the status.
    fn quote_fetch_failure(symbol: &Symbol, status: reqwest::StatusCode) -> TradeAccountingError {
        TradeAccountingError::LimitQuoteFetch {
            symbol: symbol.clone(),
            source: Box::new(st0x_execution::AlpacaBrokerApiError::LatestQuote(Box::new(
                st0x_execution::AlpacaMarketDataError::ApiError {
                    status,
                    body: "quote lookup rejected".to_string(),
                    retry_after: None,
                },
            ))),
        }
    }

    fn claimed_quote_fetch_failure(
        symbol: &Symbol,
        status: reqwest::StatusCode,
    ) -> TradeAccountingError {
        TradeAccountingError::ClaimedHedgeOrderKind {
            symbol: symbol.clone(),
            source: ClaimedHedgeOrderKindCause::classify(quote_fetch_failure(symbol, status)),
        }
    }

    #[tokio::test]
    async fn place_hedge_dead_letters_a_symbol_scoped_403() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let TestInfra { ctx, .. } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let job = hedge_job(&symbol, 2.0, Direction::Sell);

        job.handle_place_hedge_error(
            &ctx,
            quote_fetch_failure(&symbol, reqwest::StatusCode::FORBIDDEN),
        )
        .await
        .unwrap_or_else(|error| panic!("expected a 403 quote fetch to dead-letter: {error:?}"));

        let rendered = metrics_handle.render();
        assert_eq!(
            dead_letter_count(
                &rendered,
                &symbol,
                DeadLetterReason::SymbolScoped(SymbolScopedReason::LimitQuoteFetch)
            ),
            1,
            "expected the 403 to be counted as a dead-letter, in:\n{rendered}"
        );
    }

    /// Reads the `transient_streak` every enqueued `PlaceHedge` successor
    /// carries, so a test can pin both that a re-drive happened and which
    /// budget it spent.
    async fn successor_transient_streaks(apalis_pool: &apalis_sqlite::SqlitePool) -> Vec<i64> {
        sqlx_apalis::query_scalar(
            "SELECT json_extract(CAST(job AS TEXT), '$.transient_streak') FROM Jobs \
             WHERE job_type = ?",
        )
        .bind(place_hedge_job_type())
        .fetch_all(apalis_pool)
        .await
        .unwrap()
    }

    /// Same variant, transient cause: a 5xx clears on its own, so abandoning
    /// the attempt at once would trade a one-second retry for a full
    /// CheckPositions interval of unhedged exposure. It must NOT propagate
    /// either -- the supervised worker stops the whole process when its retry
    /// budget runs out, which is the outage this isolation exists to prevent
    /// (RAI-1690) -- so it is re-driven on the job's own durable budget.
    #[tokio::test]
    async fn place_hedge_redrives_a_symbol_scoped_5xx_instead_of_propagating() {
        let TestInfra {
            ctx, apalis_pool, ..
        } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let job = hedge_job(&symbol, 2.0, Direction::Sell);

        job.handle_place_hedge_error(
            &ctx,
            quote_fetch_failure(&symbol, reqwest::StatusCode::INTERNAL_SERVER_ERROR),
        )
        .await
        .unwrap_or_else(|error| panic!("a transient 5xx must not reach the worker: {error:?}"));

        assert_eq!(
            successor_transient_streaks(&apalis_pool).await,
            vec![1],
            "a transient symbol-scoped failure must enqueue exactly one successor, \
             carrying the incremented transient streak"
        );
    }

    #[tokio::test]
    async fn place_hedge_redrives_a_claimed_symbol_scoped_5xx_instead_of_fail_stopping() {
        let TestInfra {
            ctx, apalis_pool, ..
        } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let job = hedge_job(&symbol, 2.0, Direction::Sell);

        job.handle_place_hedge_error(
            &ctx,
            claimed_quote_fetch_failure(&symbol, reqwest::StatusCode::INTERNAL_SERVER_ERROR),
        )
        .await
        .unwrap_or_else(|error| {
            panic!("a transient claimed-path failure must not reach the worker: {error:?}")
        });

        assert_eq!(
            successor_transient_streaks(&apalis_pool).await,
            vec![1],
            "the claimed path must use the same durable transient budget"
        );
    }

    /// A real connection failure, not a synthesised status: the classification
    /// must reach the same "re-drive it" answer for a transport error as for a
    /// 5xx, since that is the case that fires on every extended-hours hedge
    /// when the network blips.
    #[tokio::test]
    async fn place_hedge_redrives_a_symbol_scoped_transport_failure() {
        let TestInfra {
            ctx, apalis_pool, ..
        } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let job = hedge_job(&symbol, 2.0, Direction::Sell);

        // Port 1 is reserved and never listening, so this is a genuine
        // connect failure carried in a real `reqwest::Error`.
        let transport = reqwest::Client::new()
            .get("http://127.0.0.1:1/v2/stocks/AAPL/trades/latest")
            .send()
            .await
            .expect_err("connecting to a closed port must fail");

        job.handle_place_hedge_error(
            &ctx,
            TradeAccountingError::LimitQuoteFetch {
                symbol: symbol.clone(),
                source: Box::new(st0x_execution::AlpacaBrokerApiError::LatestQuote(Box::new(
                    st0x_execution::AlpacaMarketDataError::Http(transport),
                ))),
            },
        )
        .await
        .unwrap_or_else(|error| panic!("a transport failure must not reach the worker: {error:?}"));

        assert_eq!(
            successor_transient_streaks(&apalis_pool).await,
            vec![1],
            "a transport failure must be re-driven on the job's own budget"
        );
    }

    /// The whole point of the durable budget: a market-data outage that
    /// outlives it abandons the SYMBOL, with a counter and a page, rather than
    /// propagating an `Err` that stops the supervised worker and with it every
    /// other symbol's hedging (RAI-1690).
    #[tokio::test]
    async fn a_transient_failure_past_the_redrive_budget_dead_letters_the_symbol() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let TestInfra {
            ctx,
            apalis_pool,
            notifier,
            ..
        } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let job = PlaceHedge {
            transient_streak: TransientFailureStreak(TRANSIENT_RESCHEDULE_LIMIT),
            ..hedge_job(&symbol, 2.0, Direction::Sell)
        };

        job.handle_place_hedge_error(
            &ctx,
            quote_fetch_failure(&symbol, reqwest::StatusCode::INTERNAL_SERVER_ERROR),
        )
        .await
        .unwrap_or_else(|error| {
            panic!("an exhausted transient budget must dead-letter, not propagate: {error:?}")
        });

        assert_eq!(
            successor_transient_streaks(&apalis_pool).await,
            Vec::<i64>::new(),
            "an exhausted budget must stop re-driving"
        );

        let rendered = metrics_handle.render();
        assert_eq!(
            dead_letter_count(
                &rendered,
                &symbol,
                DeadLetterReason::SymbolScoped(SymbolScopedReason::LimitQuoteFetch)
            ),
            1,
            "a sustained transient failure must be counted on the shared dead-letter \
             counter, in:\n{rendered}"
        );
        assert_eq!(
            notifier.messages(),
            vec![dead_letter_page("AAPL", "limit_quote_fetch")],
            "the operator must be paged for a symbol abandoned to a sustained outage"
        );
    }

    /// A permanent cause must not spend the re-drive budget: re-asking a 403
    /// entitlement failure cannot change the answer, so the symbol is
    /// abandoned on the first attempt.
    #[tokio::test]
    async fn a_permanent_failure_dead_letters_without_spending_the_redrive_budget() {
        let TestInfra {
            ctx, apalis_pool, ..
        } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let job = hedge_job(&symbol, 2.0, Direction::Sell);

        job.handle_place_hedge_error(
            &ctx,
            quote_fetch_failure(&symbol, reqwest::StatusCode::FORBIDDEN),
        )
        .await
        .unwrap_or_else(|error| panic!("expected a 403 quote fetch to dead-letter: {error:?}"));

        assert_eq!(
            successor_transient_streaks(&apalis_pool).await,
            Vec::<i64>::new(),
            "a permanent failure must dead-letter immediately, not re-drive"
        );
    }

    /// The dead-letter must not walk away from a claim an EARLIER attempt
    /// left behind: `perform_body` re-runs the pre-claim pricing lookup on
    /// every apalis attempt, so a retry can fail there while attempt 1's
    /// order is still `Pending` at the broker. Abandoning it silently would
    /// leave that order unplaced and the symbol blocked behind the claim
    /// until a restart. A re-drive that reaches the broker is the opposite of
    /// an abandonment, so it must neither count nor page as one.
    #[tokio::test]
    async fn dead_letter_resolves_a_claim_left_by_an_earlier_attempt() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let TestInfra {
            ctx,
            offchain_order_projection,
            notifier,
            ..
        } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let job = hedge_job(&symbol, 2.0, Direction::Sell);

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        // Attempt 1's state: the position is claimed and the order recorded,
        // but the broker outcome commit was lost, so it is still `Pending`.
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

        job.handle_place_hedge_error(
            &ctx,
            TradeAccountingError::LimitQuoteUnavailable {
                symbol: symbol.clone(),
            },
        )
        .await
        .unwrap_or_else(|error| panic!("expected the dead-letter to succeed: {error:?}"));

        let order = offchain_order_projection
            .load(&job.offchain_order_id)
            .await
            .unwrap()
            .expect("the prior attempt's order must still exist");
        assert!(
            matches!(order, OffchainOrder::Submitted { .. }),
            "the dead-letter must re-drive the prior attempt's pending order rather than \
             abandon it, got {order:?}"
        );

        let rendered = metrics_handle.render();
        assert_eq!(
            dead_letter_count(
                &rendered,
                &symbol,
                DeadLetterReason::SymbolScoped(SymbolScopedReason::LimitQuoteUnavailable)
            ),
            0,
            "a hedge re-driven to the broker was not given up on, in:\n{rendered}"
        );
        assert_eq!(
            notifier.messages(),
            Vec::<String>::new(),
            "paging an abandonment that did not happen burns the pair's one alert slot"
        );
    }

    /// CheckPositions can enqueue more than one hedge before the first job
    /// claims the position. A stale job must recover the order that actually
    /// owns the position, not assume its own independently-minted ID is the
    /// claim. Otherwise it reports a standing delta while another hedge is
    /// already live at the broker.
    #[tokio::test]
    async fn stale_job_does_not_dead_letter_a_different_live_claim() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let TestInfra {
            ctx,
            apalis_pool,
            notifier,
            ..
        } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let stale_job = hedge_job(&symbol, 2.0, Direction::Sell);
        let claimed_order_id = OffchainOrderId::new();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        ctx.position
            .send(
                &symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id: claimed_order_id,
                    shares: stale_job.shares,
                    direction: stale_job.direction,
                    executor: stale_job.executor,
                    threshold: stale_job.threshold,
                },
            )
            .await
            .unwrap();
        ctx.offchain_order
            .send(
                &claimed_order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares: stale_job.shares,
                    direction: stale_job.direction,
                    executor: stale_job.executor,
                    client_order_id: ClientOrderId::from_uuid(claimed_order_id.as_uuid()),
                    kind: CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();
        ctx.offchain_order
            .send(
                &claimed_order_id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("different-live-claim"),
                    placed_shares: stale_job.shares,
                    submitted_at: chrono::Utc::now(),
                    is_extended_hours: false,
                    limit_price: None,
                },
            )
            .await
            .unwrap();

        stale_job
            .handle_place_hedge_error(
                &ctx,
                TradeAccountingError::LimitQuoteUnavailable {
                    symbol: symbol.clone(),
                },
            )
            .await
            .unwrap_or_else(|error| panic!("the live claim must be recovered: {error:?}"));

        let live_poll_jobs: i64 = sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status IN ('Pending', 'Queued', \
             'Running')",
        )
        .bind(type_name::<PollOrderStatus>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        assert_eq!(
            live_poll_jobs, 1,
            "the stale job must re-arm polling for the order that owns the position"
        );

        let rendered = metrics_handle.render();
        assert_eq!(
            dead_letter_count(
                &rendered,
                &symbol,
                DeadLetterReason::SymbolScoped(SymbolScopedReason::LimitQuoteUnavailable)
            ),
            0,
            "a symbol with a live hedge must not be reported as abandoned, in:\n{rendered}"
        );
        assert_eq!(
            notifier.messages(),
            Vec::<String>::new(),
            "a symbol with a live hedge must not page a standing delta"
        );
    }

    /// ADR 0014: the claim recovery the dead-letter path runs can re-drive a
    /// `Pending` order through the broker, so it must serialize on the same
    /// submission lock as every other placement. `perform_body`'s guard is
    /// already released by the time the error handler runs, so without a guard
    /// here this is the one unlocked path that can submit -- and the inline
    /// counter-trade path driving the same order could then interleave, landing
    /// a live broker order permanently `Failed`.
    #[tokio::test]
    async fn dead_letter_recovery_blocks_while_the_submission_lock_is_held() {
        let TestInfra {
            ctx,
            offchain_order_projection,
            ..
        } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let job = hedge_job(&symbol, 2.0, Direction::Sell);

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        // An earlier attempt's state: claimed, recorded, and still `Pending`
        // because its broker outcome commit was lost -- the case whose
        // recovery re-drives the broker call.
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

        let guard = ctx.counter_trade_submission_lock.clone().lock_owned().await;
        let blocked = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            job.handle_place_hedge_error(
                &ctx,
                TradeAccountingError::LimitQuoteUnavailable {
                    symbol: symbol.clone(),
                },
            ),
        )
        .await;
        blocked.unwrap_err();

        let order = offchain_order_projection
            .load(&job.offchain_order_id)
            .await
            .unwrap()
            .expect("the earlier attempt's order must still exist");
        assert!(
            matches!(order, OffchainOrder::Pending { .. }),
            "no re-drive may reach the broker while the submission lock is held, got {order:?}"
        );

        drop(guard);
        job.handle_place_hedge_error(
            &ctx,
            TradeAccountingError::LimitQuoteUnavailable {
                symbol: symbol.clone(),
            },
        )
        .await
        .unwrap_or_else(|error| panic!("expected the dead-letter to succeed: {error:?}"));

        let order = offchain_order_projection
            .load(&job.offchain_order_id)
            .await
            .unwrap()
            .expect("the earlier attempt's order must still exist");
        assert!(
            matches!(order, OffchainOrder::Submitted { .. }),
            "the re-drive proceeds once the lock is released, got {order:?}"
        );
    }

    /// A hedge that already filled is the opposite of one this process gave up
    /// on, so the dead-letter path must neither count nor page it -- both
    /// would tell an operator a symbol carries a standing delta it does not.
    #[tokio::test]
    async fn a_filled_order_is_not_reported_as_an_abandoned_hedge() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let TestInfra { ctx, notifier, .. } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let job = hedge_job(&symbol, 2.0, Direction::Sell);

        // Drive this job's own order to Filled: the shape a crash between
        // placement and the outcome commit leaves once boot recovery polls it.
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
        ctx.offchain_order
            .send(
                &job.offchain_order_id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("filled-order-1"),
                    placed_shares: job.shares,
                    submitted_at: chrono::Utc::now(),
                    is_extended_hours: false,
                    limit_price: None,
                },
            )
            .await
            .unwrap();
        ctx.offchain_order
            .send(
                &job.offchain_order_id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(150.0)),
                    filled_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();

        job.handle_place_hedge_error(
            &ctx,
            TradeAccountingError::LimitQuoteUnavailable {
                symbol: symbol.clone(),
            },
        )
        .await
        .unwrap_or_else(|error| panic!("expected the dead-letter path to succeed: {error:?}"));

        let rendered = metrics_handle.render();
        assert_eq!(
            dead_letter_count(
                &rendered,
                &symbol,
                DeadLetterReason::SymbolScoped(SymbolScopedReason::LimitQuoteUnavailable)
            ),
            0,
            "a hedge that filled was not given up on, in:\n{rendered}"
        );
        assert_eq!(
            notifier.messages(),
            Vec::<String>::new(),
            "a false abandonment page burns the pair's one alert slot"
        );
    }

    /// Encodes the safety invariant the dead-letter path relies on: a
    /// symbol-scoped failure is raised before the position is claimed, so
    /// dead-lettering it leaves nothing outstanding and the next attempt
    /// places from a clean slate. The second `perform` is what pins that: if
    /// the dead-letter had claimed the position, it would hit
    /// `PositionError::PendingExecution` and recover an order that does not
    /// exist, leaving nothing `Submitted`.
    #[tokio::test]
    async fn symbol_scoped_dead_letter_never_claims_the_position() {
        struct QuoteFailingPlacer {
            /// Close-flatten (quote-priced) while set; flipped to a Regular
            /// session so the second attempt takes the plain market path.
            extended_session: AtomicBool,
        }

        #[async_trait::async_trait]
        impl OrderPlacer for QuoteFailingPlacer {
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
                _order: st0x_execution::LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("place_limit_order must not be called; the quote fetch fails first".into())
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<st0x_execution::CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(st0x_execution::CancellationOutcome::Requested)
            }

            async fn fetch_latest_quote(
                &self,
                _symbol: &Symbol,
            ) -> Result<Option<st0x_execution::LatestQuote>, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("quote endpoint unavailable".into())
            }

            async fn market_session_status(
                &self,
            ) -> Result<st0x_execution::MarketSessionStatus, Box<dyn std::error::Error + Send + Sync>>
            {
                if !self.extended_session.load(Ordering::SeqCst) {
                    return Ok(st0x_execution::MarketSessionStatus {
                        session: MarketSession::Regular,
                        extended_session_closes_at: None,
                        post_close_gap: st0x_execution::PostCloseGap::OrdinaryOvernight,
                    });
                }

                Ok(ramp_start_session_status())
            }
        }

        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let placer = Arc::new(QuoteFailingPlacer {
            extended_session: AtomicBool::new(true),
        });
        let TestInfra {
            ctx,
            position_projection,
            offchain_order_projection,
            ..
        } = create_hedge_ctx_with(placer.clone(), extended_hours_assets("AAPL", true)).await;
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
        result.unwrap_or_else(|error| {
            panic!("expected the close-flatten quote failure to dead-letter, got Err: {error:?}")
        });

        // Without this, an `Ok(())` from any earlier skip inside
        // `select_order_kind_for_current_session` would satisfy the
        // assertions below just as well as the failure this test names.
        let rendered = metrics_handle.render();
        assert_eq!(
            dead_letter_count(
                &rendered,
                &symbol,
                DeadLetterReason::SymbolScoped(SymbolScopedReason::LimitQuoteFetch)
            ),
            1,
            "expected the quote fetch failure to dead-letter, in:\n{rendered}"
        );

        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id, None,
            "a symbol-scoped failure raised before the claim must never leave the \
             position claimed"
        );

        placer.extended_session.store(false, Ordering::SeqCst);
        let retry = job.perform(&ctx).await;
        retry.unwrap_or_else(|error| {
            panic!("expected the retry after a dead-letter to place cleanly, got Err: {error:?}")
        });

        let order = offchain_order_projection
            .load(&job.offchain_order_id)
            .await
            .unwrap()
            .expect("the retry must record its offchain order");
        assert!(
            matches!(order, OffchainOrder::Submitted { .. }),
            "the retry after a dead-letter must claim and submit, not recover a \
             phantom pending order, got {order:?}"
        );
    }

    #[tokio::test]
    async fn permanent_pricing_failure_on_the_claimed_recovery_path_dead_letters() {
        struct PriceFetchFailingPlacer;

        #[async_trait::async_trait]
        impl OrderPlacer for PriceFetchFailingPlacer {
            async fn place_market_order(
                &self,
                _order: st0x_execution::MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("place_market_order must not be called during an extended session".into())
            }

            async fn place_limit_order(
                &self,
                _order: st0x_execution::LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("place_limit_order must not be called; the recovery lookup fails first".into())
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<st0x_execution::CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(st0x_execution::CancellationOutcome::Requested)
            }

            async fn fetch_position_mark(
                &self,
                _symbol: &Symbol,
            ) -> Result<Option<Positive<Usd>>, Box<dyn std::error::Error + Send + Sync>>
            {
                Err(Box::new(st0x_execution::AlpacaBrokerApiError::LatestTrade(
                    Box::new(st0x_execution::AlpacaMarketDataError::ApiError {
                        status: reqwest::StatusCode::FORBIDDEN,
                        body: "market data entitlement rejected".to_string(),
                        retry_after: None,
                    }),
                )))
            }

            async fn market_session_status(
                &self,
            ) -> Result<st0x_execution::MarketSessionStatus, Box<dyn std::error::Error + Send + Sync>>
            {
                // Extended with an ordinary overnight gap: the limit price
                // comes from the latest trade, not a close-flatten quote.
                Ok(st0x_execution::MarketSessionStatus {
                    session: MarketSession::Extended,
                    extended_session_closes_at: Some(
                        chrono::Utc::now() + chrono::TimeDelta::minutes(5),
                    ),
                    post_close_gap: st0x_execution::PostCloseGap::OrdinaryOvernight,
                })
            }
        }

        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let TestInfra {
            ctx,
            position_projection,
            notifier,
            ..
        } = create_hedge_ctx_with(
            Arc::new(PriceFetchFailingPlacer),
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

        // Seed the claimed-but-unsubmitted state a prior attempt leaves
        // behind when its broker outcome commit is lost.
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

        job.perform(&ctx).await.unwrap_or_else(|error| {
            panic!("a permanent claimed-path pricing failure must not fail-stop: {error:?}")
        });

        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id,
            Some(job.offchain_order_id),
            "the existing claim must survive so the retry can re-drive it"
        );
        let rendered = metrics_handle.render();
        assert_eq!(
            dead_letter_count(
                &rendered,
                &symbol,
                DeadLetterReason::SymbolScoped(SymbolScopedReason::MarkFetch)
            ),
            1,
            "the claimed-path abandonment must stay visible while its claim awaits recovery, \
             in:\n{rendered}"
        );
        assert_eq!(
            notifier.messages(),
            vec![dead_letter_page("AAPL", "mark_fetch")],
            "leaving the claim for the recovery sweep must still page the standing delta"
        );
    }

    /// `OrderPlacer` whose extended-hours price lookup fails with a classified
    /// broker rate-limit (429) wrapped exactly as the real Alpaca executor
    /// produces it: `AlpacaBrokerApiError::LatestTrade(AlpacaMarketDataError)`,
    /// a two-hop boxed source (`TradeAccountingError::MarkFetch` boxes
    /// a `dyn Error`, which itself wraps the market-data error). Used to pin
    /// the extended-hours reschedule path `handle_place_hedge_error`'s doc
    /// comment claims handles (RAI-1494).
    fn rate_limited_price_fetch_placer() -> Arc<dyn OrderPlacer> {
        struct RateLimitedPricePlacer;

        #[async_trait::async_trait]
        impl OrderPlacer for RateLimitedPricePlacer {
            async fn place_market_order(
                &self,
                _order: st0x_execution::MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err(
                    "place_market_order must not be called when the price fetch is rate-limited"
                        .into(),
                )
            }

            async fn place_limit_order(
                &self,
                _order: st0x_execution::LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err(
                    "place_limit_order must not be called when the price fetch is rate-limited"
                        .into(),
                )
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &st0x_execution::ExecutorOrderId,
            ) -> Result<st0x_execution::CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(st0x_execution::CancellationOutcome::Requested)
            }

            async fn fetch_position_mark(
                &self,
                _symbol: &Symbol,
            ) -> Result<
                Option<st0x_execution::Positive<Usd>>,
                Box<dyn std::error::Error + Send + Sync>,
            > {
                Err(Box::new(st0x_execution::AlpacaBrokerApiError::LatestTrade(
                    Box::new(st0x_execution::AlpacaMarketDataError::ApiError {
                        status: reqwest::StatusCode::TOO_MANY_REQUESTS,
                        body: "rate limited".to_string(),
                        retry_after: Some(Duration::from_millis(1)),
                    }),
                )))
            }

            async fn market_session(
                &self,
            ) -> Result<MarketSession, Box<dyn std::error::Error + Send + Sync>> {
                Ok(MarketSession::Extended)
            }
        }

        Arc::new(RateLimitedPricePlacer)
    }

    #[derive(Default)]
    struct RateLimitedPlacementState {
        attempts: AtomicUsize,
        client_order_ids: StdMutex<Vec<ClientOrderId>>,
    }

    /// Rate-limits the first broker placement, then accepts the successor's
    /// idempotent retry. The shared state lets the test prove both calls used
    /// the same broker `client_order_id`.
    fn rate_limited_once_order_placer() -> (Arc<dyn OrderPlacer>, Arc<RateLimitedPlacementState>) {
        struct RateLimitedOncePlacer {
            state: Arc<RateLimitedPlacementState>,
        }

        #[async_trait::async_trait]
        impl OrderPlacer for RateLimitedOncePlacer {
            async fn place_market_order(
                &self,
                order: st0x_execution::MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                self.state
                    .client_order_ids
                    .lock()
                    .unwrap()
                    .push(order.client_order_id);
                let attempt = self.state.attempts.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    return Err(Box::new(st0x_execution::AlpacaBrokerApiError::ApiError {
                        status: reqwest::StatusCode::TOO_MANY_REQUESTS,
                        alpaca_code: None,
                        message: "rate limited".to_string(),
                        retry_after: Some(Duration::from_millis(1)),
                    }));
                }

                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("accepted-after-rate-limit"),
                    placed_shares: order.shares,
                    is_extended_hours: false,
                    limit_price: None,
                })
            }

            async fn place_limit_order(
                &self,
                _order: st0x_execution::LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("regular-session test must not place a limit order".into())
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<st0x_execution::CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(st0x_execution::CancellationOutcome::Requested)
            }
        }

        let state = Arc::new(RateLimitedPlacementState::default());
        (
            Arc::new(RateLimitedOncePlacer {
                state: state.clone(),
            }),
            state,
        )
    }

    #[tokio::test]
    async fn place_hedge_extended_hours_price_fetch_429_reschedules_through_perform_without_claiming_position()
     {
        let TestInfra {
            ctx,
            apalis_pool,
            position_projection,
            ..
        } = create_hedge_ctx_with(
            rate_limited_price_fetch_placer(),
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
            backpressure_streak: BackpressureStreak::default(),
            transient_streak: TransientFailureStreak::default(),
        };

        // Driven through the REAL `Job::perform` entry point (not
        // `handle_place_hedge_error` called directly): the two-hop boxed 429
        // must reschedule (`Ok(())`), not propagate as `Err`.
        job.perform(&ctx).await.unwrap();

        assert_eq!(successor_backpressure_streak(&apalis_pool).await, 1);

        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id, None,
            "a 429 raised before the position claim must not claim the position"
        );
    }

    #[tokio::test]
    async fn place_hedge_placement_429_retries_same_client_order_id_and_submits() {
        let (placer, placement_state) = rate_limited_once_order_placer();
        let TestInfra {
            ctx,
            apalis_pool,
            offchain_order_projection,
            ..
        } = create_hedge_ctx(placer).await;
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

        assert_eq!(placement_state.attempts.load(Ordering::SeqCst), 1);
        assert_eq!(successor_backpressure_streak(&apalis_pool).await, 1);
        let pending = offchain_order_projection
            .load(&job.offchain_order_id)
            .await
            .unwrap()
            .expect("rate-limited placement must retain its offchain order");
        assert!(
            matches!(pending, OffchainOrder::Pending { .. }),
            "a placement 429 must leave the durable order Pending, got {pending:?}"
        );

        let (successor_id, successor_payload): (String, Vec<u8>) = sqlx_apalis::query_as(
            "SELECT id, job FROM Jobs WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(place_hedge_job_type())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        let successor: PlaceHedge =
            serde_json::from_slice(&successor_payload).expect("deserialize PlaceHedge successor");
        sqlx_apalis::query("UPDATE Jobs SET status = 'Running' WHERE id = ?")
            .bind(&successor_id)
            .execute(&apalis_pool)
            .await
            .unwrap();

        successor.perform(&ctx).await.unwrap();

        sqlx_apalis::query("UPDATE Jobs SET status = 'Done' WHERE id = ?")
            .bind(&successor_id)
            .execute(&apalis_pool)
            .await
            .unwrap();
        assert_eq!(placement_state.attempts.load(Ordering::SeqCst), 2);
        let client_order_ids = placement_state.client_order_ids.lock().unwrap().clone();
        let expected_client_order_id = ClientOrderId::from_uuid(job.offchain_order_id.as_uuid());
        assert_eq!(
            client_order_ids,
            [expected_client_order_id.clone(), expected_client_order_id,],
            "the retry must reuse the first attempt's broker idempotency key"
        );

        let submitted = offchain_order_projection
            .load(&job.offchain_order_id)
            .await
            .unwrap()
            .expect("successful retry must retain its offchain order");
        assert!(
            matches!(submitted, OffchainOrder::Submitted { .. }),
            "the successful retry must advance the order to Submitted, got {submitted:?}"
        );
        let poll_jobs: i64 =
            sqlx_apalis::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(type_name::<PollOrderStatus>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        assert_eq!(
            poll_jobs, 1,
            "the successful retry must enqueue exactly one PollOrderStatus job"
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
            close_flatten: false,
            reference_price: None,
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

    /// `route_placement_outcome`'s `Submitted`/`PartiallyFilled`/`Cancelling`
    /// arm shares the same `push_poll_job_if_absent` guard as
    /// `dispatch_post_place_state` and `recover_pending_poll_status`.
    /// A re-entrant call against an order that already has a live poll job
    /// (the shape of the race between `recover_pending_poll_status`'s `Pending`
    /// re-drive and a concurrent recovery attempt for the same order) must
    /// skip the push rather than forking a second independent,
    /// self-perpetuating poll chain.
    #[tokio::test]
    async fn route_placement_outcome_skips_duplicate_push_when_poll_job_already_live() {
        let TestInfra {
            ctx, apalis_pool, ..
        } = create_hedge_ctx(succeeding_order_placer()).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let offchain_order_id = OffchainOrderId::new();

        let submitted = OffchainOrder::Submitted {
            symbol: symbol.clone(),
            shares: Positive::new(FractionalShares::new(float!(1.0))).unwrap(),
            requested_shares: Some(Positive::new(FractionalShares::new(float!(1.0))).unwrap()),
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            executor_order_id: ExecutorOrderId::new("ORD_ROUTE_GUARD"),
            placed_at: chrono::Utc::now(),
            submitted_at: chrono::Utc::now(),
            market_session: MarketSession::Regular,
            close_flatten: false,
            reference_price: None,
        };

        // First call: no live poll job yet, so the guard is a no-op and the
        // push goes through.
        route_placement_outcome(&ctx, &symbol, offchain_order_id, Some(submitted.clone()))
            .await
            .unwrap();

        let poll_jobs_after_first: i64 =
            sqlx_apalis::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(type_name::<PollOrderStatus>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        assert_eq!(
            poll_jobs_after_first, 1,
            "the first call must push exactly one PollOrderStatus job"
        );

        // Second call against the same order (simulating a concurrent recovery
        // attempt observing the order already Submitted with its poll job
        // already live) must skip the push rather than forking a duplicate
        // chain.
        route_placement_outcome(&ctx, &symbol, offchain_order_id, Some(submitted))
            .await
            .unwrap();

        let poll_jobs_after_second: i64 =
            sqlx_apalis::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(type_name::<PollOrderStatus>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        assert_eq!(
            poll_jobs_after_second, 1,
            "a re-entrant call against an order with a still-live poll job must not push a \
             duplicate"
        );
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
            market_session: MarketSession::Regular,
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
    async fn placement_failure_without_executor_id_preserves_the_anchor() {
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

        let failed = OffchainOrder::Failed {
            market_session: MarketSession::Regular,
            symbol: symbol.clone(),
            shares,
            requested_shares: Some(shares),
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            retained_fill: None,
            filled_shares: None,
            executor_order_id: None,
            error: "broker unreachable".to_string(),
            placed_at: chrono::Utc::now(),
            failed_at: chrono::Utc::now(),
        };

        route_placement_outcome(&ctx, &symbol, offchain_order_id, Some(failed))
            .await
            .unwrap();

        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.last_failed_offchain_order_id,
            Some(offchain_order_id),
            "no broker order id is the lost-2xx window the anchor exists \
             for; the failed order's own id must be stashed"
        );
    }

    #[tokio::test]
    async fn poll_failed_retry_with_executor_id_preserves_the_anchor() {
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

        let first_order_id = OffchainOrderId::new();
        ctx.position
            .send(
                &symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id: first_order_id,
                    shares,
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();
        ctx.position
            .send(
                &symbol,
                PositionCommand::FailOffChainOrder {
                    offchain_order_id: first_order_id,
                    error: "first attempt lost in flight".to_string(),
                    anchor: AnchorDisposition::Preserve,
                },
            )
            .await
            .unwrap();

        let second_order_id = OffchainOrderId::new();
        ctx.position
            .send(
                &symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id: second_order_id,
                    shares,
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();

        let failed = OffchainOrder::Failed {
            market_session: MarketSession::Regular,
            symbol: symbol.clone(),
            shares,
            requested_shares: Some(shares),
            direction: Direction::Sell,
            executor: SupportedExecutor::DryRun,
            retained_fill: None,
            filled_shares: Some(FractionalShares::ZERO),
            executor_order_id: Some(ExecutorOrderId::new("expired-order")),
            error: "expired".to_string(),
            placed_at: chrono::Utc::now(),
            failed_at: chrono::Utc::now(),
        };

        route_placement_outcome(&ctx, &symbol, second_order_id, Some(failed))
            .await
            .unwrap();

        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.last_failed_offchain_order_id,
            Some(first_order_id),
            "route_placement_outcome has no broker-terminality classification \
             to derive from, so it always preserves; a broker order id alone \
             is not evidence of terminality, and Preserve keeps the original \
             anchor across the retry chain"
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

            async fn fetch_position_mark(
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

    fn close_flatten_order_placer(bid: Float, ask: Float) -> Arc<dyn OrderPlacer> {
        close_flatten_order_placer_with_counter(bid, ask, Arc::new(AtomicUsize::new(0)))
    }

    fn close_flatten_order_placer_with_counter(
        bid: Float,
        ask: Float,
        quote_calls: Arc<AtomicUsize>,
    ) -> Arc<dyn OrderPlacer> {
        struct CloseFlattenPlacer {
            quote: st0x_execution::LatestQuote,
            quote_calls: Arc<AtomicUsize>,
        }

        #[async_trait::async_trait]
        impl OrderPlacer for CloseFlattenPlacer {
            async fn place_market_order(
                &self,
                _order: st0x_execution::MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("close flatten must not place a market order".into())
            }

            async fn place_limit_order(
                &self,
                order: st0x_execution::LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("close-flatten-order"),
                    placed_shares: order.shares,
                    is_extended_hours: true,
                    limit_price: Some(order.limit_price),
                })
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<st0x_execution::CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(st0x_execution::CancellationOutcome::Requested)
            }

            async fn fetch_position_mark(
                &self,
                _symbol: &Symbol,
            ) -> Result<Option<Positive<Usd>>, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("close flatten must not fall back to latest trade".into())
            }

            async fn fetch_latest_quote(
                &self,
                _symbol: &Symbol,
            ) -> Result<Option<st0x_execution::LatestQuote>, Box<dyn std::error::Error + Send + Sync>>
            {
                self.quote_calls.fetch_add(1, Ordering::SeqCst);
                Ok(Some(self.quote))
            }

            async fn market_session_status(
                &self,
            ) -> Result<st0x_execution::MarketSessionStatus, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(ramp_start_session_status())
            }
        }

        Arc::new(CloseFlattenPlacer {
            quote: st0x_execution::LatestQuote::new(
                Positive::new(Usd::new(bid)).unwrap(),
                Positive::new(Usd::new(ask)).unwrap(),
            )
            .unwrap(),
            quote_calls,
        })
    }

    /// A close-flatten placer whose ask is wide enough that the
    /// perform-time re-preflight must reject the buy -- and whose
    /// `place_limit_order` panics if reached, proving the order is never
    /// submitted. Models the scan-time-approved-but-perform-time-stale
    /// scenario: `preflight_counter_trade_at_price` always rejects,
    /// regardless of the order it's asked to check, standing in for an ask
    /// that moved past the scan-time preflight's approved price by the time
    /// this job reached perform().
    fn close_flatten_preflight_rejecting_placer(bid: Float, ask: Float) -> Arc<dyn OrderPlacer> {
        struct RejectingPreflightPlacer {
            quote: st0x_execution::LatestQuote,
        }

        #[async_trait::async_trait]
        impl OrderPlacer for RejectingPreflightPlacer {
            async fn place_market_order(
                &self,
                _order: st0x_execution::MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("close flatten must not place a market order");
            }

            async fn place_limit_order(
                &self,
                _order: st0x_execution::LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("a perform-time preflight rejection must abort before the broker is called");
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<st0x_execution::CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(st0x_execution::CancellationOutcome::Requested)
            }

            async fn fetch_latest_quote(
                &self,
                _symbol: &Symbol,
            ) -> Result<Option<st0x_execution::LatestQuote>, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(Some(self.quote))
            }

            async fn preflight_counter_trade_at_price(
                &self,
                _order: st0x_execution::MarketOrder,
                _reference_price: Positive<Usd>,
            ) -> Result<CounterTradePreflight, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(CounterTradePreflight::Skipped(
                    st0x_execution::CounterTradeSkipReason::InsufficientBuyingPower {
                        estimated_cost_cents: 200_000,
                        available_buying_power_cents: 100,
                    },
                ))
            }

            async fn market_session_status(
                &self,
            ) -> Result<st0x_execution::MarketSessionStatus, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(ramp_start_session_status())
            }
        }

        Arc::new(RejectingPreflightPlacer {
            quote: st0x_execution::LatestQuote::new(
                Positive::new(Usd::new(bid)).unwrap(),
                Positive::new(Usd::new(ask)).unwrap(),
            )
            .unwrap(),
        })
    }

    fn extended_preflight_rejecting_placer(
        mark: Float,
        allow_limit_placement: bool,
    ) -> Arc<dyn OrderPlacer> {
        struct RejectingPreflightPlacer {
            mark: Positive<Usd>,
            allow_limit_placement: bool,
        }

        #[async_trait::async_trait]
        impl OrderPlacer for RejectingPreflightPlacer {
            async fn place_market_order(
                &self,
                _order: st0x_execution::MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("an extended-hours hedge must not place a market order");
            }

            async fn place_limit_order(
                &self,
                order: st0x_execution::LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                assert!(
                    self.allow_limit_placement,
                    "a fresh perform-time preflight rejection must abort before the broker is called"
                );
                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("recovered-live-order"),
                    placed_shares: order.shares,
                    is_extended_hours: true,
                    limit_price: Some(order.limit_price),
                })
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<st0x_execution::CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(st0x_execution::CancellationOutcome::Requested)
            }

            async fn fetch_position_mark(
                &self,
                _symbol: &Symbol,
            ) -> Result<Option<Positive<Usd>>, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(Some(self.mark))
            }

            async fn preflight_counter_trade_at_price(
                &self,
                _order: st0x_execution::MarketOrder,
                reference_price: Positive<Usd>,
            ) -> Result<CounterTradePreflight, Box<dyn std::error::Error + Send + Sync>>
            {
                assert!(
                    reference_price.inner().inner().eq(float!(101.00)).unwrap(),
                    "the preflight must receive the fresh 100 USD mark crossed by 100 bps"
                );
                Ok(CounterTradePreflight::Skipped(
                    st0x_execution::CounterTradeSkipReason::InsufficientBuyingPower {
                        estimated_cost_cents: 20_200,
                        available_buying_power_cents: 20_000,
                    },
                ))
            }

            async fn market_session_status(
                &self,
            ) -> Result<st0x_execution::MarketSessionStatus, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(st0x_execution::MarketSessionStatus {
                    session: MarketSession::Extended,
                    extended_session_closes_at: None,
                    post_close_gap: st0x_execution::PostCloseGap::OrdinaryOvernight,
                })
            }
        }

        Arc::new(RejectingPreflightPlacer {
            mark: Positive::new(Usd::new(mark)).unwrap(),
            allow_limit_placement,
        })
    }

    fn ordinary_extended_preflight_rejecting_placer(mark: Float) -> Arc<dyn OrderPlacer> {
        extended_preflight_rejecting_placer(mark, false)
    }

    fn pending_recovery_preflight_rejecting_placer(mark: Float) -> Arc<dyn OrderPlacer> {
        extended_preflight_rejecting_placer(mark, true)
    }

    /// Regression test for the TOCTOU between the scan-time close-flatten
    /// preflight (`CheckPositions::preflight_and_clamp_shares`, which checks
    /// one quote) and the perform-time submission (which fetches its own,
    /// possibly-later quote). Before this fix, `select_order_kind_for_current_session`
    /// applied slippage to the fresh quote and handed the resulting limit
    /// straight to `place_offchain_order_at_broker` with no cash re-check --
    /// so an ask that widened between the two quote fetches could produce a
    /// submitted limit needing more buying power than was ever preflighted.
    /// Here the perform-time preflight always rejects, standing in for that
    /// widened ask; the placer panics if the broker is reached, proving
    /// perform() aborts before submission and the position stays unclaimed
    /// for a later retry.
    #[tokio::test]
    async fn close_flatten_buy_perform_time_preflight_blocks_stale_scan_approval() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let TestInfra {
            ctx,
            position_projection,
            offchain_order_projection,
            ..
        } = create_hedge_ctx_with(
            close_flatten_preflight_rejecting_placer(float!(99.00), float!(1_000.00)),
            extended_hours_assets("AAPL", true),
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;

        let job = PlaceHedge {
            market_session: MarketSession::Extended,
            ..hedge_job(&symbol, 2.0, Direction::Buy)
        };

        job.perform(&ctx).await.unwrap();

        let all_orders = offchain_order_projection.load_all().await.unwrap();
        assert_eq!(
            all_orders.len(),
            0,
            "a perform-time preflight rejection must not submit an order"
        );

        let position_after = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should still exist");
        assert_eq!(
            position_after.pending_offchain_order_id, None,
            "a rejected buy must not claim the position, so a later re-hedge attempt can retry"
        );

        let rendered = metrics_handle.render();
        assert!(rendered.contains("close_flatten_blocked_total{"));
        assert!(rendered.contains("reason=\"insufficient_buying_power\""));
        assert!(rendered.contains("symbol=\"AAPL\""));
    }

    /// Ordinary extended-hours buys have the same gap between the scan's
    /// buying-power snapshot and the fresh mark used at perform time. The
    /// exact crossed limit must be re-preflighted even when no close-flatten
    /// window is active.
    #[tokio::test]
    async fn ordinary_extended_buy_rechecks_the_fresh_limit_before_submission() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let TestInfra {
            ctx,
            position_projection,
            offchain_order_projection,
            ..
        } = create_hedge_ctx_with(
            ordinary_extended_preflight_rejecting_placer(float!(100.00)),
            extended_hours_assets("AAPL", true),
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;

        PlaceHedge {
            market_session: MarketSession::Extended,
            ..hedge_job(&symbol, 2.0, Direction::Buy)
        }
        .perform(&ctx)
        .await
        .unwrap();

        assert_eq!(
            offchain_order_projection.load_all().await.unwrap(),
            Vec::new(),
            "a moved ordinary-extended price must be rejected before broker submission"
        );
        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should remain available for a later retry");
        assert_eq!(position.pending_offchain_order_id, None);

        assert!(
            !metrics_handle
                .render()
                .contains("close_flatten_blocked_total{"),
            "an ordinary extended-hours rejection must not inflate close-flatten metrics"
        );
    }

    /// A perform-time preflight skip happens before this job can claim the
    /// position, but a different queued job may already own it. The skip path
    /// must recover that live order under the submission lock rather than
    /// looking up this stale job's unrelated ID and leaving the broker order
    /// unpolled.
    #[tokio::test]
    async fn preflight_skip_recovers_a_different_live_claim() {
        let TestInfra {
            ctx,
            apalis_pool,
            offchain_order_projection,
            position_projection,
            ..
        } = create_hedge_ctx_with(
            ordinary_extended_preflight_rejecting_placer(float!(100.00)),
            extended_hours_assets("AAPL", true),
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();
        let stale_job = PlaceHedge {
            market_session: MarketSession::Extended,
            ..hedge_job(&symbol, 2.0, Direction::Buy)
        };
        let claimed_order_id = OffchainOrderId::new();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;
        ctx.position
            .send(
                &symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id: claimed_order_id,
                    shares: stale_job.shares,
                    direction: stale_job.direction,
                    executor: stale_job.executor,
                    threshold: stale_job.threshold,
                },
            )
            .await
            .unwrap();
        ctx.offchain_order
            .send(
                &claimed_order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares: stale_job.shares,
                    direction: stale_job.direction,
                    executor: stale_job.executor,
                    client_order_id: ClientOrderId::from_uuid(claimed_order_id.as_uuid()),
                    kind: CounterTradeOrderKind::ExtendedHoursLimit {
                        limit_price: Positive::new(Usd::new(float!(101.00))).unwrap(),
                        close_flatten: false,
                        reference_price: None,
                    },
                },
            )
            .await
            .unwrap();
        ctx.offchain_order
            .send(
                &claimed_order_id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("preflight-skip-live-claim"),
                    placed_shares: stale_job.shares,
                    submitted_at: chrono::Utc::now(),
                    is_extended_hours: true,
                    limit_price: Some(Positive::new(Usd::new(float!(101.00))).unwrap()),
                },
            )
            .await
            .unwrap();

        stale_job.perform(&ctx).await.unwrap();

        let live_poll_jobs: i64 = sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status IN ('Pending', 'Queued', \
             'Running')",
        )
        .bind(type_name::<PollOrderStatus>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        assert_eq!(
            live_poll_jobs, 1,
            "the skipped stale job must re-arm polling for the live claim"
        );
        assert_eq!(
            offchain_order_projection
                .load(&stale_job.offchain_order_id)
                .await
                .unwrap(),
            None,
            "the skipped job must not create its own order"
        );
        assert_eq!(
            position_projection
                .load(&symbol)
                .await
                .unwrap()
                .and_then(|position| position.pending_offchain_order_id),
            Some(claimed_order_id),
            "recovery must preserve the order that owns the position"
        );
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

        let notifier = Arc::new(FlakyNotifier::default());

        let ctx = HedgeCtx {
            position: position.clone(),
            offchain_order,
            poll_status_queue: PollOrderStatusJobQueue::new(&apalis_pool),
            hedge_queue: HedgeJobQueue::new(&apalis_pool),
            order_placer: placer,
            assets: extended_hours_assets("AAPL", true),
            close_flatten_policy: CloseFlattenPolicy::from_secs(900).unwrap(),
            close_flatten_ramp: CloseFlattenCrossRamp::new(100, 400).unwrap(),
            overnight_eligibility: EligibilitySnapshots::default(),
            overnight_max_quote_age: Some(std::time::Duration::from_secs(30)),
            overnight_slippage_bps: Some(150),
            counter_trade_submission_lock: Arc::new(tokio::sync::Mutex::new(())),
            poll_interval: TEST_POLL_INTERVAL,
            notifier: notifier.clone(),
            alerted_dead_letters: Arc::new(Mutex::new(HashSet::new())),
        };

        TestInfra {
            ctx,
            apalis_pool,
            position_projection,
            offchain_order_projection,
            notifier,
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

    #[tokio::test]
    async fn close_flatten_buy_uses_ask_plus_slippage() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let TestInfra {
            ctx,
            offchain_order_projection,
            ..
        } = create_hedge_ctx_with(
            close_flatten_order_placer(float!(99.00), float!(100.01)),
            extended_hours_assets("AAPL", true),
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();
        let selected_kind = select_order_kind_for_current_session(
            &ctx,
            &symbol,
            Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
            Direction::Buy,
            MarketSession::Extended,
            SubmittedPricePreflight::Required,
        )
        .await
        .unwrap()
        .unwrap();
        let CounterTradeOrderKind::ExtendedHoursLimit {
            limit_price,
            close_flatten,
            ..
        } = selected_kind
        else {
            panic!("close flatten must select an extended-hours limit");
        };
        assert!(limit_price.inner().inner().eq(float!(101.02)).unwrap());
        assert!(
            close_flatten,
            "the flag every close-flatten outcome metric is attributed by must be set here, \
             or close_flatten_outcomes_total stays permanently empty"
        );

        // Read before the `perform` below prices a second attempt, so these
        // are the counts for exactly one placement.
        let rendered = metrics_handle.render();
        assert_eq!(
            counter_value(
                &rendered,
                "close_flatten_placements_total",
                &[
                    ("symbol", "AAPL"),
                    ("direction", "buy"),
                    ("cross_bucket", "100"),
                ],
            ),
            1,
            "the fill-rate denominator must count this placement at its cross bucket, \
             in:\n{rendered}"
        );
        assert_eq!(
            counter_value(
                &rendered,
                "hedge_price_source_total",
                &[
                    ("symbol", "AAPL"),
                    ("path", "close_flatten"),
                    ("source", "delayed_sip_quote"),
                ],
            ),
            1,
            "a close-flatten limit priced off the quote fallback must say so, in:\n{rendered}"
        );

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;
        let job = PlaceHedge {
            symbol: symbol.clone(),
            direction: Direction::Buy,
            shares: Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
            executor: SupportedExecutor::DryRun,
            threshold: ExecutionThreshold::whole_share(),
            offchain_order_id: OffchainOrderId::new(),
            market_session: MarketSession::Extended,
            backpressure_streak: BackpressureStreak::default(),
            transient_streak: TransientFailureStreak::default(),
        };

        job.perform(&ctx).await.unwrap();

        let order = offchain_order_projection
            .load(&job.offchain_order_id)
            .await
            .unwrap()
            .expect("new exposure inside the window must place an order");
        let OffchainOrder::Submitted {
            market_session: MarketSession::Extended,
            ..
        } = order
        else {
            panic!("new close-window exposure must place an extended-hours limit");
        };
        let rendered = metrics_handle.render();
        assert!(rendered.contains("close_flatten_attempts_total{"));
        assert!(rendered.contains("symbol=\"AAPL\""));
        assert!(rendered.contains("reason=\"multi_day_closure\""));
    }

    #[tokio::test]
    async fn close_flatten_sell_uses_bid_minus_slippage() {
        let TestInfra { ctx, .. } = create_hedge_ctx_with(
            close_flatten_order_placer(float!(100.01), float!(101.00)),
            extended_hours_assets("AAPL", true),
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();

        let kind = select_order_kind_for_current_session(
            &ctx,
            &symbol,
            Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
            Direction::Sell,
            MarketSession::Extended,
            SubmittedPricePreflight::Required,
        )
        .await
        .unwrap()
        .unwrap();

        let CounterTradeOrderKind::ExtendedHoursLimit {
            limit_price,
            close_flatten,
            ..
        } = kind
        else {
            panic!("close flatten must select an extended-hours limit");
        };
        assert!(limit_price.inner().inner().eq(float!(99.00)).unwrap());
        assert!(
            close_flatten,
            "a sell inside the window is close-flatten too"
        );
    }

    #[tokio::test]
    async fn each_close_flatten_reprice_attempt_fetches_a_fresh_quote() {
        let quote_calls = Arc::new(AtomicUsize::new(0));
        let TestInfra { ctx, .. } = create_hedge_ctx_with(
            close_flatten_order_placer_with_counter(
                float!(99.00),
                float!(100.00),
                quote_calls.clone(),
            ),
            extended_hours_assets("AAPL", true),
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();

        for _ in 0..2 {
            select_order_kind_for_current_session(
                &ctx,
                &symbol,
                Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
                Direction::Buy,
                MarketSession::Extended,
                SubmittedPricePreflight::Required,
            )
            .await
            .unwrap()
            .expect("close-flatten pricing should produce a limit order");
        }

        assert_eq!(quote_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn close_flatten_surfaces_the_mark_error_when_every_source_fails() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        struct QuoteFailingPlacer;

        #[async_trait::async_trait]
        impl OrderPlacer for QuoteFailingPlacer {
            async fn place_market_order(
                &self,
                _order: st0x_execution::MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("placement must not run after quote failure".into())
            }

            async fn place_limit_order(
                &self,
                _order: st0x_execution::LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("placement must not run after quote failure".into())
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<st0x_execution::CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(st0x_execution::CancellationOutcome::Requested)
            }

            async fn fetch_position_mark(
                &self,
                _symbol: &Symbol,
            ) -> Result<Option<Positive<Usd>>, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("positions endpoint down".into())
            }

            async fn fetch_latest_quote(
                &self,
                _symbol: &Symbol,
            ) -> Result<Option<st0x_execution::LatestQuote>, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("quote endpoint unavailable".into())
            }

            async fn market_session_status(
                &self,
            ) -> Result<st0x_execution::MarketSessionStatus, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(ramp_start_session_status())
            }
        }

        let TestInfra { ctx, .. } = create_hedge_ctx_with(
            Arc::new(QuoteFailingPlacer),
            extended_hours_assets("AAPL", true),
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();

        let result = select_order_kind_for_current_session(
            &ctx,
            &symbol,
            Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
            Direction::Buy,
            MarketSession::Extended,
            SubmittedPricePreflight::Required,
        )
        .await;

        // No optional primary provider is wired by this placer. The mark error
        // therefore remains the preferred failure when the emergency quote has
        // the same retry classification: collapsing it into a generic "no
        // price" would strip a 429 or 5xx classification and dead-letter a
        // hedge that should have been retried.
        assert!(
            matches!(result, Err(TradeAccountingError::MarkFetch { .. })),
            "expected the mark error to survive the fallback chain, got {result:?}"
        );
        let rendered = metrics_handle.render();
        assert!(rendered.contains("close_flatten_blocked_total{"));
        assert!(rendered.contains("symbol=\"AAPL\""));
        assert!(rendered.contains("reason=\"mark_fetch_failed\""));
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
            backpressure_streak: BackpressureStreak::default(),
            transient_streak: TransientFailureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
            transient_streak: TransientFailureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
            transient_streak: TransientFailureStreak::default(),
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

    /// Overnight defers exactly like Closed until automated overnight
    /// counter-trading ships: a job performing during the overnight session
    /// must skip without claiming the position or placing any order, even
    /// with extended hours enabled for the symbol.
    #[tokio::test]
    async fn job_skips_without_claiming_during_overnight_session() {
        let TestInfra {
            ctx,
            position_projection,
            offchain_order_projection,
            ..
        } = create_hedge_ctx_with(
            market_session_overriding_placer(MarketSession::Overnight),
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
            // Stale serialized session: enqueued during extended hours.
            market_session: MarketSession::Extended,
            backpressure_streak: BackpressureStreak::default(),
            transient_streak: TransientFailureStreak::default(),
        };

        job.perform(&ctx)
            .await
            .expect("a job performing during the overnight session must skip");

        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.pending_offchain_order_id, None,
            "An overnight recheck must not claim the position"
        );

        let order = offchain_order_projection
            .load(&job.offchain_order_id)
            .await
            .unwrap();
        assert_eq!(
            order, None,
            "No order may be submitted during the overnight session until overnight \
             counter-trading ships, got: {order:?}"
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

            async fn fetch_position_mark(
                &self,
                _symbol: &Symbol,
            ) -> Result<
                Option<st0x_execution::Positive<Usd>>,
                Box<dyn std::error::Error + Send + Sync>,
            > {
                Err("positions endpoint down".into())
            }

            async fn market_session(
                &self,
            ) -> Result<MarketSession, Box<dyn std::error::Error + Send + Sync>> {
                Ok(MarketSession::Extended)
            }
        }

        Arc::new(FailingPricePlacer)
    }

    /// Records the limit price a hedge actually reached the broker with, so a
    /// test can assert which reference the placement was derived from.
    #[derive(Clone)]
    struct RecordingLimitPlacer {
        primary_quote: Option<Result<st0x_execution::LatestQuote, ()>>,
        mark: Option<Result<Positive<Usd>, ()>>,
        quote: Option<st0x_execution::LatestQuote>,
        placed: Arc<std::sync::Mutex<Vec<Usd>>>,
    }

    #[async_trait::async_trait]
    impl OrderPlacer for RecordingLimitPlacer {
        async fn place_market_order(
            &self,
            _order: st0x_execution::MarketOrder,
        ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
            Err("extended hours must not place a market order".into())
        }

        async fn place_limit_order(
            &self,
            order: st0x_execution::LimitOrder,
        ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>> {
            self.placed.lock().unwrap().push(order.limit_price.inner());
            Ok(OrderPlacementResult {
                executor_order_id: ExecutorOrderId::new("recorded-order"),
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

        async fn fetch_position_mark(
            &self,
            _symbol: &Symbol,
        ) -> Result<Option<st0x_execution::Positive<Usd>>, Box<dyn std::error::Error + Send + Sync>>
        {
            match self.mark {
                Some(Ok(mark)) => Ok(Some(mark)),
                Some(Err(())) => Err("positions endpoint down".into()),
                None => Ok(None),
            }
        }

        async fn fetch_primary_limit_quote(
            &self,
            _symbol: &Symbol,
        ) -> Result<Option<st0x_execution::LatestQuote>, Box<dyn std::error::Error + Send + Sync>>
        {
            match self.primary_quote {
                Some(Ok(quote)) => Ok(Some(quote)),
                Some(Err(())) => Err("primary quote endpoint down".into()),
                None => Ok(None),
            }
        }

        async fn fetch_latest_quote(
            &self,
            _symbol: &Symbol,
        ) -> Result<Option<st0x_execution::LatestQuote>, Box<dyn std::error::Error + Send + Sync>>
        {
            Ok(self.quote)
        }

        async fn market_session(
            &self,
        ) -> Result<MarketSession, Box<dyn std::error::Error + Send + Sync>> {
            Ok(MarketSession::Extended)
        }
    }

    fn usd(value: &str) -> Positive<Usd> {
        Positive::new(Usd::new(float!(value))).unwrap()
    }

    /// The whole point of ADR 0019's fallback chain: a broker that cannot serve
    /// a mark must not stop the hedge, because flattening before a multi-day gap
    /// is mandatory. The delayed quote takes over and the order still goes out.
    #[tokio::test]
    async fn a_failed_mark_falls_through_to_the_quote_and_still_places() {
        let submitted_prices = Arc::new(std::sync::Mutex::new(Vec::new()));
        let placer = RecordingLimitPlacer {
            primary_quote: None,
            mark: Some(Err(())),
            quote: Some(st0x_execution::LatestQuote::new(usd("99.00"), usd("101.00")).unwrap()),
            placed: submitted_prices.clone(),
        };

        let TestInfra { ctx, .. } =
            create_hedge_ctx_with(Arc::new(placer), extended_hours_assets("AAPL", true)).await;
        let symbol = Symbol::new("AAPL").unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        PlaceHedge {
            symbol: symbol.clone(),
            direction: Direction::Sell,
            shares: Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
            executor: SupportedExecutor::DryRun,
            threshold: ExecutionThreshold::whole_share(),
            offchain_order_id: OffchainOrderId::new(),
            market_session: MarketSession::Extended,
            backpressure_streak: BackpressureStreak::default(),
            transient_streak: TransientFailureStreak::default(),
        }
        .perform(&ctx)
        .await
        .expect("a failed mark must fall through to the quote, not abandon the hedge");

        let submitted = submitted_prices.lock().unwrap().clone();
        assert_eq!(
            submitted.len(),
            1,
            "the hedge must still reach the broker, got: {submitted:?}"
        );
        // Sell off the bid (99.00), crossed down by the 100 bps base band.
        assert_eq!(submitted[0], Usd::new(float!("98.01")));
    }

    /// With the mark merely absent rather than broken, a quote failure is the
    /// only thing that went wrong, so its own error surfaces and stays
    /// classifiable. This is the leg that keeps `LimitQuoteFetch` reachable.
    #[tokio::test]
    async fn an_absent_mark_surfaces_the_quote_error() {
        struct QuoteOnlyFailurePlacer;

        #[async_trait::async_trait]
        impl OrderPlacer for QuoteOnlyFailurePlacer {
            async fn place_market_order(
                &self,
                _order: st0x_execution::MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("placement must not run without a price".into())
            }

            async fn place_limit_order(
                &self,
                _order: st0x_execution::LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("placement must not run without a price".into())
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<st0x_execution::CancellationOutcome, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(st0x_execution::CancellationOutcome::Requested)
            }

            async fn fetch_position_mark(
                &self,
                _symbol: &Symbol,
            ) -> Result<Option<Positive<Usd>>, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(None)
            }

            async fn fetch_latest_quote(
                &self,
                _symbol: &Symbol,
            ) -> Result<Option<st0x_execution::LatestQuote>, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("quote endpoint unavailable".into())
            }

            async fn market_session(
                &self,
            ) -> Result<MarketSession, Box<dyn std::error::Error + Send + Sync>> {
                Ok(MarketSession::Extended)
            }
        }

        let TestInfra { ctx, .. } = create_hedge_ctx_with(
            Arc::new(QuoteOnlyFailurePlacer),
            extended_hours_assets("AAPL", true),
        )
        .await;

        let result = select_order_kind_for_current_session(
            &ctx,
            &Symbol::new("AAPL").unwrap(),
            Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
            Direction::Sell,
            MarketSession::Extended,
            SubmittedPricePreflight::Required,
        )
        .await;

        assert!(
            matches!(result, Err(TradeAccountingError::LimitQuoteFetch { .. })),
            "expected the quote error to surface when the mark was merely absent, got {result:?}"
        );
    }

    fn classified_market_data_failure(
        status: reqwest::StatusCode,
    ) -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(st0x_execution::AlpacaBrokerApiError::LatestQuote(Box::new(
            st0x_execution::AlpacaMarketDataError::ApiError {
                status,
                body: "classified test failure".to_string(),
                retry_after: None,
            },
        )))
    }

    #[test]
    fn both_reference_failures_surface_the_one_with_the_best_retry_path() {
        let transient = prefer_reference_price_failure(
            ReferencePriceError::MarkFetch(classified_market_data_failure(
                reqwest::StatusCode::FORBIDDEN,
            )),
            ReferencePriceError::QuoteFetch(classified_market_data_failure(
                reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            )),
        );
        let ReferencePriceError::QuoteFetch(source) = transient else {
            panic!("a transient quote failure must outrank a permanent mark failure")
        };
        assert_eq!(
            find_permanence(source.as_ref()),
            Some(Permanence::Transient)
        );

        let backpressure = prefer_reference_price_failure(
            ReferencePriceError::MarkFetch(classified_market_data_failure(
                reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            )),
            ReferencePriceError::QuoteFetch(classified_market_data_failure(
                reqwest::StatusCode::TOO_MANY_REQUESTS,
            )),
        );
        let ReferencePriceError::QuoteFetch(source) = backpressure else {
            panic!("a rate-limited quote failure must outrank a transient mark failure")
        };
        assert!(
            find_backpressure(source.as_ref()).is_some(),
            "the selected error must preserve the quote endpoint's 429 backoff"
        );
    }

    /// The mirror of the close-flatten selection tests: an ordinary
    /// extended-hours limit must NOT carry the close-flatten flag, or every
    /// outcome it reaches is attributed to a flatten that never happened, and
    /// its price source is reported on the ordinary path.
    #[tokio::test]
    async fn an_ordinary_extended_limit_is_not_flagged_as_close_flatten() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let placer = RecordingLimitPlacer {
            primary_quote: None,
            mark: Some(Ok(usd("110.00"))),
            quote: None,
            placed: Arc::new(std::sync::Mutex::new(Vec::new())),
        };

        let TestInfra { ctx, .. } =
            create_hedge_ctx_with(Arc::new(placer), extended_hours_assets("AAPL", true)).await;
        let symbol = Symbol::new("AAPL").unwrap();

        let kind = select_order_kind_for_current_session(
            &ctx,
            &symbol,
            Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
            Direction::Sell,
            MarketSession::Extended,
            SubmittedPricePreflight::Required,
        )
        .await
        .unwrap()
        .unwrap();

        let CounterTradeOrderKind::ExtendedHoursLimit { close_flatten, .. } = kind else {
            panic!("an extended session must select an extended-hours limit");
        };
        assert!(
            !close_flatten,
            "an ordinary overnight gap is not a close-flatten window"
        );

        let rendered = metrics_handle.render();
        assert_eq!(
            counter_value(
                &rendered,
                "hedge_price_source_total",
                &[
                    ("symbol", "AAPL"),
                    ("path", "ordinary_extended"),
                    ("source", "mark"),
                ],
            ),
            1,
            "the daily extended path must report the mark as its source, in:\n{rendered}"
        );
        assert_eq!(
            counter_value(
                &rendered,
                "close_flatten_placements_total",
                &[("symbol", "AAPL")],
            ),
            0,
            "an ordinary extended placement must not inflate the flatten denominator, \
             in:\n{rendered}"
        );
    }

    /// The mark is preferred over the quote whenever both are available: it is
    /// live where the delayed quote is fifteen minutes stale.
    #[tokio::test]
    async fn the_mark_is_preferred_over_the_delayed_quote() {
        let submitted_prices = Arc::new(std::sync::Mutex::new(Vec::new()));
        let placer = RecordingLimitPlacer {
            primary_quote: None,
            mark: Some(Ok(usd("110.00"))),
            quote: Some(st0x_execution::LatestQuote::new(usd("99.00"), usd("101.00")).unwrap()),
            placed: submitted_prices.clone(),
        };

        let TestInfra { ctx, .. } =
            create_hedge_ctx_with(Arc::new(placer), extended_hours_assets("AAPL", true)).await;
        let symbol = Symbol::new("AAPL").unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        PlaceHedge {
            symbol: symbol.clone(),
            direction: Direction::Sell,
            shares: Positive::new(FractionalShares::new(float!(2.0))).unwrap(),
            executor: SupportedExecutor::DryRun,
            threshold: ExecutionThreshold::whole_share(),
            offchain_order_id: OffchainOrderId::new(),
            market_session: MarketSession::Extended,
            backpressure_streak: BackpressureStreak::default(),
            transient_streak: TransientFailureStreak::default(),
        }
        .perform(&ctx)
        .await
        .expect("a live mark must price the hedge");

        let submitted = submitted_prices.lock().unwrap().clone();
        // 110.00 mark crossed down by 100 bps, not the 99.00 quote bid.
        assert_eq!(submitted[0], Usd::new(float!("108.90")));
    }

    #[tokio::test]
    async fn a_primary_quote_is_preferred_over_the_mark_and_delayed_quote() {
        let placer = RecordingLimitPlacer {
            primary_quote: Some(Ok(st0x_execution::LatestQuote::new(
                usd("99.00"),
                usd("101.00"),
            )
            .unwrap())),
            mark: Some(Ok(usd("110.00"))),
            quote: Some(st0x_execution::LatestQuote::new(usd("79.00"), usd("81.00")).unwrap()),
            placed: Arc::new(std::sync::Mutex::new(Vec::new())),
        };
        let symbol = Symbol::new("AAPL").unwrap();

        let buy_reference =
            resolve_extended_hours_reference_price(&placer, &symbol, Direction::Buy)
                .await
                .expect("a primary quote should resolve the reference");
        let sell_reference =
            resolve_extended_hours_reference_price(&placer, &symbol, Direction::Sell)
                .await
                .expect("a primary quote should resolve the reference");

        assert_eq!(buy_reference.source, ReferencePriceSource::PrimaryQuote);
        assert_eq!(buy_reference.price, usd("101.00"));
        assert_eq!(sell_reference.source, ReferencePriceSource::PrimaryQuote);
        assert_eq!(sell_reference.price, usd("99.00"));
    }

    #[tokio::test]
    async fn a_failed_primary_quote_falls_back_to_the_mark() {
        let placer = RecordingLimitPlacer {
            primary_quote: Some(Err(())),
            mark: Some(Ok(usd("110.00"))),
            quote: Some(st0x_execution::LatestQuote::new(usd("79.00"), usd("81.00")).unwrap()),
            placed: Arc::new(std::sync::Mutex::new(Vec::new())),
        };
        let symbol = Symbol::new("AAPL").unwrap();

        let reference = resolve_extended_hours_reference_price(&placer, &symbol, Direction::Sell)
            .await
            .expect("a primary quote failure must not suppress the mark fallback");

        assert_eq!(reference.source, ReferencePriceSource::Mark);
        assert_eq!(reference.price, usd("110.00"));
    }

    fn overnight_quote(bid: &str, ask: &str, at: chrono::DateTime<chrono::Utc>) -> IndicativeQuote {
        IndicativeQuote {
            quote: LatestQuote::new(usd(bid), usd(ask)).unwrap(),
            at,
        }
    }

    #[tokio::test]
    async fn overnight_reference_prices_buys_from_the_ask_and_sells_from_the_bid() {
        let now = chrono::Utc::now();
        let placer = crate::offchain::order::ExecutorOrderPlacer(
            MockExecutor::new().with_overnight_quote(overnight_quote("24.10", "24.30", now)),
        );
        let symbol = Symbol::new("RKLB").unwrap();
        let max_age = std::time::Duration::from_secs(30);

        let buy = resolve_overnight_reference_price(&placer, &symbol, Direction::Buy, max_age, now)
            .await
            .unwrap();
        let sell =
            resolve_overnight_reference_price(&placer, &symbol, Direction::Sell, max_age, now)
                .await
                .unwrap();

        assert_eq!(buy.price, usd("24.30"));
        assert_eq!(buy.source, ReferencePriceSource::OvernightQuote);
        assert_eq!(sell.price, usd("24.10"));
        assert_eq!(sell.source, ReferencePriceSource::OvernightQuote);
    }

    #[tokio::test]
    async fn overnight_reference_defers_on_a_stale_quote() {
        let handle = crate::metrics::setup().expect("install Prometheus recorder");
        let now = chrono::Utc::now();
        let stale_at = now - chrono::Duration::seconds(45);
        let placer = crate::offchain::order::ExecutorOrderPlacer(
            MockExecutor::new().with_overnight_quote(overnight_quote("24.10", "24.30", stale_at)),
        );
        let symbol = Symbol::new("RKLB").unwrap();
        let max_age = std::time::Duration::from_secs(30);

        let error =
            resolve_overnight_reference_price(&placer, &symbol, Direction::Buy, max_age, now)
                .await
                .unwrap_err();

        assert!(
            matches!(
                error,
                OvernightReferenceError::Stale { age, max_age }
                    if age == std::time::Duration::from_secs(45)
                        && max_age == std::time::Duration::from_secs(30)
            ),
            "expected the exact staleness, got {error:?}"
        );
        let rendered = handle.render();
        assert!(
            rendered.contains(
                "hedge_quote_age_seconds_count{symbol=\"RKLB\",source=\"overnight_quote\"} 1"
            ),
            "a stale quote must still record its age sample -- the stale tail is the \
             point of the distribution, got:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn overnight_reference_tolerates_broker_clock_ahead_of_ours() {
        // A quote stamped slightly in the future (broker clock skew) has
        // age zero, never a huge unsigned wrap -- the same clamp the
        // executor-side validator applies.
        let handle = crate::metrics::setup().expect("install Prometheus recorder");
        let now = chrono::Utc::now();
        let future_at = now + chrono::Duration::seconds(5);
        let placer = crate::offchain::order::ExecutorOrderPlacer(
            MockExecutor::new().with_overnight_quote(overnight_quote("24.10", "24.30", future_at)),
        );
        let symbol = Symbol::new("RKLB").unwrap();

        let reference = resolve_overnight_reference_price(
            &placer,
            &symbol,
            Direction::Buy,
            std::time::Duration::from_secs(30),
            now,
        )
        .await
        .unwrap();

        assert_eq!(reference.price, usd("24.30"));
        let rendered = handle.render();
        assert!(
            rendered.contains(
                "hedge_quote_age_seconds_count{symbol=\"RKLB\",source=\"overnight_quote\"} 1"
            ),
            "a successful resolution must record one quote-age sample, got:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn overnight_reference_defers_when_the_quote_fetch_fails() {
        // No fallback chain: a failed overnight quote fetch defers the
        // hedge rather than pricing from a mark or delayed print.
        let handle = crate::metrics::setup().expect("install Prometheus recorder");
        let placer = crate::offchain::order::ExecutorOrderPlacer(MockExecutor::new());
        let symbol = Symbol::new("RKLB").unwrap();

        let error = resolve_overnight_reference_price(
            &placer,
            &symbol,
            Direction::Buy,
            std::time::Duration::from_secs(30),
            chrono::Utc::now(),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(error, OvernightReferenceError::QuoteFetch(_)),
            "expected QuoteFetch, got {error:?}"
        );
        let rendered = handle.render();
        assert!(
            !rendered.contains("hedge_quote_age_seconds_count{"),
            "a failed fetch has no quote to age -- nothing may be recorded, got:\n{rendered}"
        );
    }

    /// One equity with trading enabled and the overnight flag as given.
    fn overnight_assets(symbol: &str, enabled: bool) -> AssetsConfig {
        let overnight_counter_trading = if enabled {
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
                        vault_ids: Vec::new(),
                        trading: OperationMode::Enabled,
                        rebalancing: OperationMode::Disabled,
                        wrapped_equity_recovery: OperationMode::Disabled,
                        extended_hours_counter_trading: OperationMode::Disabled,
                        overnight_counter_trading,
                        operational_limit: None,
                    },
                ))
                .collect(),
            },
            cash: None,
        }
    }

    fn eligible_details() -> st0x_execution::AssetDetails {
        st0x_execution::AssetDetails {
            status: st0x_execution::alpaca_broker_api::AssetStatus::Active,
            tradable: true,
            fractionable: Some(true),
            fractional_eh_enabled: Some(true),
            overnight_tradable: Some(true),
            overnight_halted: Some(false),
        }
    }

    #[tokio::test]
    async fn overnight_session_defers_a_disabled_symbol_with_no_broker_call() {
        let handle = crate::metrics::setup().expect("install Prometheus recorder");
        let placer = Arc::new(crate::offchain::order::ExecutorOrderPlacer(
            MockExecutor::new().with_market_session(MarketSession::Overnight),
        ));
        let infra = create_hedge_ctx_with(placer, overnight_assets("RKLB", false)).await;
        let symbol = Symbol::new("RKLB").unwrap();

        let kind = select_order_kind_for_current_session(
            &infra.ctx,
            &symbol,
            Positive::new(FractionalShares::new(float!(1))).unwrap(),
            Direction::Buy,
            MarketSession::Overnight,
            SubmittedPricePreflight::Required,
        )
        .await
        .unwrap();

        assert!(kind.is_none(), "a disabled symbol must defer, got {kind:?}");
        let rendered = handle.render();
        assert!(
            rendered.contains("hedge_scan_skipped_total{")
                && rendered.contains("session=\"overnight\""),
            "the defer must be counted with the overnight session label, in:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn overnight_session_defers_without_an_eligibility_snapshot() {
        // Enabled but never synced: fail closed with no broker call.
        let placer = Arc::new(crate::offchain::order::ExecutorOrderPlacer(
            MockExecutor::new().with_market_session(MarketSession::Overnight),
        ));
        let infra = create_hedge_ctx_with(placer, overnight_assets("RKLB", true)).await;
        let symbol = Symbol::new("RKLB").unwrap();

        let kind = select_order_kind_for_current_session(
            &infra.ctx,
            &symbol,
            Positive::new(FractionalShares::new(float!(1))).unwrap(),
            Direction::Buy,
            MarketSession::Overnight,
            SubmittedPricePreflight::Required,
        )
        .await
        .unwrap();

        assert!(
            kind.is_none(),
            "an unsynced symbol must defer fail-closed, got {kind:?}"
        );
    }

    #[tokio::test]
    async fn overnight_session_selects_a_slippage_bounded_limit_for_an_eligible_symbol() {
        // Ask 24.30 with the fixture's 150 bps bound: 24.30 * 1.015 =
        // 24.6645, buy-rounded up to the 24.67 tick.
        let now = chrono::Utc::now();
        let placer = Arc::new(crate::offchain::order::ExecutorOrderPlacer(
            MockExecutor::new()
                .with_market_session(MarketSession::Overnight)
                .with_overnight_quote(overnight_quote("24.10", "24.30", now)),
        ));
        let infra = create_hedge_ctx_with(placer, overnight_assets("RKLB", true)).await;
        let symbol = Symbol::new("RKLB").unwrap();
        infra.ctx.overnight_eligibility.record(
            symbol.clone(),
            st0x_execution::EligibilitySnapshot {
                synced_at: now,
                details: eligible_details(),
            },
        );

        let kind = select_order_kind_for_current_session(
            &infra.ctx,
            &symbol,
            Positive::new(FractionalShares::new(float!(1))).unwrap(),
            Direction::Buy,
            MarketSession::Overnight,
            SubmittedPricePreflight::SkipForIdempotentRecovery,
        )
        .await
        .unwrap();

        let Some(CounterTradeOrderKind::OvernightLimit {
            limit_price,
            snapshot,
            reference_price,
        }) = kind
        else {
            panic!("expected an overnight limit, got {kind:?}");
        };
        assert_eq!(limit_price, usd("24.67"));
        assert_eq!(snapshot.details, eligible_details());
        assert_eq!(
            reference_price,
            Some(usd("24.30")),
            "the pre-slippage indicative ask must ride the kind for the audit trail"
        );
    }

    #[tokio::test]
    async fn price_fetch_failure_during_extended_session_does_not_claim_position() {
        // order_placer is Some but the latest-trade-price lookup fails (e.g.
        // market data endpoint down during pre-market). The error must surface
        // BEFORE the position is claimed: a regression that claims the
        // position first would leave a dangling pending_offchain_order_id with
        // no actual order, silently blocking all future hedging of the symbol.
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
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
            backpressure_streak: BackpressureStreak::default(),
            transient_streak: TransientFailureStreak::default(),
        };

        let result = job.perform(&ctx).await;
        result.unwrap_or_else(|error| {
            panic!("expected the price-fetch failure to dead-letter, got Err: {error:?}")
        });

        // Without this, an `Ok(())` from any earlier skip inside
        // `select_order_kind_for_current_session` would satisfy the
        // assertions below just as well as the failure this test names.
        let rendered = metrics_handle.render();
        assert_eq!(
            dead_letter_count(
                &rendered,
                &symbol,
                DeadLetterReason::SymbolScoped(SymbolScopedReason::MarkFetch)
            ),
            1,
            "expected the exhausted reference chain to dead-letter, in:\n{rendered}"
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
        // clock endpoint down). The error must be MarketSessionCheck -- not a
        // reference-price error, which would point operators at the wrong
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
            backpressure_streak: BackpressureStreak::default(),
            transient_streak: TransientFailureStreak::default(),
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
            backpressure_streak: BackpressureStreak::default(),
            transient_streak: TransientFailureStreak::default(),
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

            async fn fetch_position_mark(
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

        let notifier = Arc::new(FlakyNotifier::default());

        let ctx = HedgeCtx {
            position: position.clone(),
            offchain_order,
            poll_status_queue: PollOrderStatusJobQueue::new(&apalis_pool),
            hedge_queue: HedgeJobQueue::new(&apalis_pool),
            order_placer: placer,
            assets,
            close_flatten_policy: CloseFlattenPolicy::from_secs(900).unwrap(),
            close_flatten_ramp: CloseFlattenCrossRamp::new(100, 400).unwrap(),
            overnight_eligibility: EligibilitySnapshots::default(),
            overnight_max_quote_age: Some(std::time::Duration::from_secs(30)),
            overnight_slippage_bps: Some(150),
            counter_trade_submission_lock: Arc::new(tokio::sync::Mutex::new(())),
            poll_interval: TEST_POLL_INTERVAL,
            notifier: notifier.clone(),
            alerted_dead_letters: Arc::new(Mutex::new(HashSet::new())),
        };

        TestInfra {
            ctx,
            apalis_pool,
            position_projection,
            offchain_order_projection,
            notifier,
        }
    }

    #[tokio::test]
    async fn retry_against_a_still_live_poll_job_does_not_fork_a_duplicate() {
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
        // the first run set the pending id, and the offchain order is still
        // `Submitted` with its poll job still live. `recover_pending_poll_status`
        // must observe that via `reconcile_and_check_live_poll_job` and skip
        // the push -- every apalis retry of this same job would otherwise
        // fork its own independent, self-perpetuating poll chain for the same
        // order.
        job.perform(&ctx).await.unwrap();

        let poll_jobs_after_retry: i64 =
            sqlx_apalis::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(type_name::<PollOrderStatus>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        assert_eq!(
            poll_jobs_after_retry, 1,
            "retry must not re-enqueue PollOrderStatus while the first job for this order is \
             still live"
        );
    }

    /// Sibling of the test above, pinning the push branch of
    /// `recover_pending_poll_status`'s `Submitted` arm: when the first poll job
    /// is no longer live (its doc comment's motivating case is a lost enqueue,
    /// but a completed/superseded row collapses to the same "not live" guard
    /// outcome), a retry must re-enqueue a replacement rather than silently
    /// returning `Ok(())` and leaving the order un-polled.
    #[tokio::test]
    async fn retry_against_no_longer_live_poll_job_re_enqueues_a_replacement() {
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

        // Simulate the pushed job no longer being live (e.g. it already ran
        // to completion) so `reconcile_and_check_live_poll_job` observes no
        // live row for the retry to skip against.
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Done', done_at = strftime('%s', 'now') WHERE job_type = ?",
        )
        .bind(type_name::<PollOrderStatus>())
        .execute(&apalis_pool)
        .await
        .unwrap();

        // Retry the same job. Position rejects with PendingExecution again,
        // and this time `reconcile_and_check_live_poll_job` finds no live
        // row, so `recover_pending_poll_status` must push a replacement.
        job.perform(&ctx).await.unwrap();

        let live_poll_jobs: i64 = sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status IN ('Pending', 'Queued', \
             'Running')",
        )
        .bind(type_name::<PollOrderStatus>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        assert_eq!(
            live_poll_jobs, 1,
            "retry against a no-longer-live poll job must re-enqueue exactly one replacement"
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

    #[tokio::test]
    async fn pending_extended_buy_recovery_bypasses_reserved_buying_power_preflight() {
        let TestInfra {
            ctx,
            apalis_pool,
            offchain_order_projection,
            ..
        } = create_hedge_ctx_with(
            pending_recovery_preflight_rejecting_placer(float!(100.00)),
            extended_hours_assets("AAPL", true),
        )
        .await;
        let symbol = Symbol::new("AAPL").unwrap();
        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Sell,
        )
        .await;
        let job = PlaceHedge {
            market_session: MarketSession::Extended,
            ..hedge_job(&symbol, 2.0, Direction::Buy)
        };
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
                    kind: CounterTradeOrderKind::ExtendedHoursLimit {
                        limit_price: Positive::new(Usd::new(float!(101.00))).unwrap(),
                        close_flatten: false,
                        reference_price: None,
                    },
                },
            )
            .await
            .unwrap();

        job.perform(&ctx).await.unwrap();

        assert!(matches!(
            offchain_order_projection
                .load(&job.offchain_order_id)
                .await
                .unwrap()
                .unwrap(),
            OffchainOrder::Submitted {
                market_session: MarketSession::Extended,
                ..
            }
        ));
        let poll_jobs: i64 =
            sqlx_apalis::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(type_name::<PollOrderStatus>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        assert_eq!(
            poll_jobs, 1,
            "the idempotent recovery must re-arm polling even when reserved cash makes a fresh preflight reject"
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
                    filled_shares: None,
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

    #[tokio::test]
    async fn client_order_id_for_placement_derives_fresh_key_after_release() {
        let (placer, captured_client_order_ids) = capturing_order_placer();
        let TestInfra {
            ctx,
            position_projection,
            ..
        } = create_hedge_ctx(placer).await;
        let symbol = Symbol::new("AAPL").unwrap();
        let shares = Positive::new(FractionalShares::new(float!(2.0))).unwrap();

        fill_position(
            &ctx.position,
            &symbol,
            FractionalShares::new(float!(2.0)),
            Direction::Buy,
        )
        .await;

        let expired_order_id = OffchainOrderId::new();
        ctx.position
            .send(
                &symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id: expired_order_id,
                    shares,
                    direction: Direction::Sell,
                    executor: SupportedExecutor::DryRun,
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();
        ctx.position
            .send(
                &symbol,
                PositionCommand::FailOffChainOrder {
                    offchain_order_id: expired_order_id,
                    error: "expired".to_string(),
                    anchor: AnchorDisposition::Release,
                },
            )
            .await
            .unwrap();

        // Drive `PlaceHedge::perform_body`, not a hand re-derivation: this proves
        // the live wiring reads the cleared anchor.
        let job = hedge_job(&symbol, 2.0, Direction::Sell);
        job.perform(&ctx).await.unwrap();

        let position = position_projection
            .load(&symbol)
            .await
            .unwrap()
            .expect("position should exist");
        assert_eq!(
            position.last_failed_offchain_order_id, None,
            "a successful placement must not leave a stale anchor behind"
        );

        let captured = captured_client_order_ids.lock().unwrap().clone();
        assert_eq!(
            captured,
            [ClientOrderId::from_uuid(job.offchain_order_id.as_uuid())],
            "after a Release failure clears the anchor, the real placement \
             path must derive a fresh client_order_id from its own id, not \
             the dead expired order's key"
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
