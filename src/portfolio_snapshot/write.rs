//! Periodic daily portfolio capture as a durable, self-rescheduling job.
//!
//! [`PortfolioSnapshotJob`] wakes once per Eastern Time day (delay-to-next-
//! ET-midnight, decision 12 in the RAI-1457 plan), reads the live
//! [`crate::inventory::InventoryView`], resolves each equity balance's USD
//! mark from the `Position` aggregate, and issues a `Capture` command against
//! the `PortfolioSnapshot` aggregate. Idempotency for a given ET day is the
//! aggregate's job (see `crate::portfolio_snapshot`'s module doc); this job
//! only decides *when* to try and what to skip when the view is not yet
//! trustworthy.
//!
//! The job carries an explicit `target_et_day` rather than deriving the
//! captured day from the wall clock at run time: a job only ever fires when
//! [`next_capture`] schedules it, shortly after that day's ET midnight, so a
//! restart at an arbitrary time of day can never capture an in-progress or
//! stale-hydrated view as a permanent daily snapshot (round-2 fix for the
//! original always-immediate bootstrap).
//!
//! **Scheduling semantics (round-3 fix, see SPEC.md "Portfolio Capital and
//! Return Tracking"):** one snapshot per ET day, labeled by the ET day it is
//! captured on, taken at/after that day's `CAPTURE_BUFFER` boundary using
//! same-day inventory ([`hydration_gap`] guarantees COMPLETE -- every
//! required slot has a row this run; [`freshness_gap`] additionally
//! guarantees FRESH -- every required slot was observed by a poll on or after
//! the TARGET ET day's midnight, day-scoped rather than merely "observed at
//! some point this process run", closing both the restart-stale hole AND the
//! long-running-process hole presence alone cannot see; see both functions'
//! docs). In steady state this fires once a day, right after the
//! boundary. If a restart or a stuck hydration retry causes `perform` to
//! run after its `target_et_day` has already passed, it CATCHES UP to a
//! fresh reading for the CURRENT ET day rather than either back-dating a live
//! read under the stale day's id or leaving that day's capital sample
//! permanently absent: for a roughly delta-neutral book, deployed capital
//! moves slowly and is dominated by deliberate flows, so a same-day reading
//! remains a valid proxy for the missed day -- but only within a BOUNDED
//! lateness window, `[boundary, boundary + MAX_FRESHNESS_DEFER]` (see
//! [`capture_lateness_exceeded`]). That window is anchored to the target
//! day's boundary alone, never to process start, which makes it
//! restart-proof: a same-day restart after the window has already elapsed
//! cannot reopen it and recapture an already-abandoned day off-boundary. Past
//! the cap the day is a gap. A snapshot is never labeled with a day other
//! than the one its balances were actually observed on. A day the bot is down
//! across entirely (no wake before the next boundary) is simply absent from
//! the series -- coverage is sparse by design, not backfilled.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use apalis::prelude::Status;
use chrono::{DateTime, Datelike, Days, NaiveDate, TimeZone, Utc};
use chrono_tz::America::New_York;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, warn};

use st0x_event_sorcery::{AggregateError, LifecycleError, Projection, SendError, Store};
use st0x_execution::{FractionalShares, Symbol};
use st0x_float_macro::float;
use st0x_wrapper::{RatioError, UnderlyingPerWrapped, Wrapper, WrapperError};

use super::{
    PortfolioBalanceRowWithMark, PortfolioSnapshot, PortfolioSnapshotCommand, PortfolioSnapshotId,
};
use crate::conductor::job::{Job, JobQueue, Label, QueuePushError};
use crate::inventory::{
    BroadcastingInventory, PollFreshness, PortfolioAsset, PortfolioBalanceRow, PortfolioLocation,
};
use crate::position::Position;

pub(crate) type PortfolioSnapshotJobQueue = JobQueue<PortfolioSnapshotJob>;

/// USDC is treated as par with USD, matching the reporting-currency
/// assumption used elsewhere in `/pnl` (decision 7).
const USDC_PAR: rain_math_float::Float = float!(1);

/// Buffer added past ET midnight before capturing, so the last poll tick for
/// the closing day has time to land in the read model.
const CAPTURE_BUFFER: chrono::Duration = chrono::Duration::minutes(5);

/// Retry backoff when the completeness gate fails (an incomplete
/// `InventoryView`, expected during startup hydration).
const HYDRATION_RETRY_BACKOFF: Duration = Duration::from_secs(5 * 60);

/// Retry backoff when a job wakes before its own `target_et_day`'s boundary
/// (a premature scheduler tick). Reuses the same short duration as
/// [`HYDRATION_RETRY_BACKOFF`]: both cases want an uneventful, short retry
/// rather than any change to `target_et_day`.
const EARLY_WAKE_BACKOFF: Duration = HYDRATION_RETRY_BACKOFF;

/// Bounds how long a capture may be deferred past its boundary before
/// [`capture_lateness_exceeded`] gives up and abandons the day entirely
/// (livelock escape hatch), independent of whether [`freshness_gap`] would
/// otherwise still be blocking it: a venue that never polls this run -- or a
/// persistent `poll_inflight_equity` outage, which aborts the whole poll tick
/// before any slot stamps (`crate::inventory::polling`) -- would otherwise
/// block capture forever. ~6h: long enough to ride out a startup hydration
/// retry loop or a temporary venue outage, short enough that a genuinely
/// stuck freshness signal does not silently swallow the whole trading day's
/// capital sample.
const MAX_FRESHNESS_DEFER: chrono::Duration = chrono::Duration::hours(6);

/// Shared dependencies for the [`PortfolioSnapshotJob`].
pub(crate) struct PortfolioSnapshotCtx {
    pub(crate) inventory: Arc<BroadcastingInventory>,
    pub(crate) position_projection: Arc<Projection<Position>>,
    pub(crate) portfolio_snapshot: Arc<Store<PortfolioSnapshot>>,
    /// Resolves each configured equity's live vault ratio so wrapped onchain
    /// balances (MarketMaking, BaseWalletWrapped) can be valued in
    /// underlying-equivalent units before being persisted (see
    /// [`convert_wrapped_equity_rows`]). `None` only when no wallet is
    /// configured at all -- the bot can then never hold onchain wrapped
    /// equity in the first place, so no row would ever need conversion.
    pub(crate) wrapper: Option<Arc<dyn Wrapper>>,
    /// Reused from `configured_inventory_vaults` (`src/conductor/builder.rs`)
    /// rather than recomputed, so the completeness gate and the live
    /// inventory poller always agree on what "fully hydrated" means.
    pub(crate) configured_equity_symbols: HashSet<Symbol>,
    pub(crate) usdc_tracking_enabled: bool,
    /// Mirrors whether `[wallet_polling]` is configured (the same source the
    /// live inventory poller reads, `crate::inventory::WalletPollingCtx`):
    /// when `true`, [`hydration_gap`] also requires the wallet-transit
    /// locations (Ethereum wallet, Base wallet unwrapped/wrapped) to have
    /// been polled at least once before capture.
    pub(crate) wallet_polling_enabled: bool,
    /// Ephemeral "observed this run" freshness signal, stamped by the live
    /// inventory poller on every successful sub-poll
    /// (`crate::inventory::polling`). Wired from the same clone the poller
    /// got (`crate::conductor::builder`), so [`freshness_gap`] sees exactly
    /// what this process has actually polled -- closing the restart-stale
    /// hole [`hydration_gap`] alone cannot see (see that function's doc).
    pub(crate) poll_freshness: PollFreshness,
    pub(crate) queue: PortfolioSnapshotJobQueue,
}

/// Errors surfaced by [`PortfolioSnapshotJob::perform`]. Distinct from
/// [`super::PortfolioSnapshotError`] (the aggregate's own command-rejection
/// type) -- `AlreadyCaptured` is matched and consumed before it would ever
/// reach this enum (decision 11).
#[derive(Debug, Error)]
pub(crate) enum PortfolioSnapshotJobError {
    #[error("failed to enqueue follow-up portfolio snapshot job: {0}")]
    Enqueue(#[from] QueuePushError),
    #[error("failed to capture portfolio snapshot: {0}")]
    Capture(#[from] SendError<PortfolioSnapshot>),
    #[error("failed to load position for portfolio snapshot mark: {0}")]
    PositionProjection(#[from] st0x_event_sorcery::ProjectionError<Position>),
    #[error("apalis database error: {0}")]
    ApalisDatabase(#[from] sqlx_apalis::Error),
    #[error("failed to resolve vault ratio for portfolio snapshot: {0}")]
    VaultRatio(#[from] WrapperError),
    #[error("failed to convert wrapped equity balance to underlying units: {0}")]
    WrappedEquityConversion(#[from] RatioError),
    #[error(
        "wrapped equity balance present for {symbol} at {location} but no wallet/Wrapper is \
         configured to resolve its vault ratio"
    )]
    MissingWrapper {
        symbol: Symbol,
        location: PortfolioLocation,
    },
}

/// A durable, self-rescheduling job that captures one daily portfolio
/// snapshot per ET day. Every input other than `target_et_day` is read fresh
/// from the inventory view, the position projection, and the wall clock at
/// run time, mirroring [`crate::position_check::CheckPositions`]'s shape.
/// `target_et_day` is the one piece of state the job carries: the ET day it
/// was scheduled to capture, fixed at enqueue time by [`next_capture`] so a
/// hydration retry or a restart can never silently substitute a different
/// day.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PortfolioSnapshotJob {
    pub(crate) target_et_day: NaiveDate,
}

impl Job<PortfolioSnapshotCtx> for PortfolioSnapshotJob {
    type Output = ();
    type Error = PortfolioSnapshotJobError;

    const WORKER_NAME: &'static str = "portfolio-snapshot-worker";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind =
        crate::conductor::job::JobKind::PortfolioSnapshot;

    fn label(&self) -> Label {
        Label::new("PortfolioSnapshot")
    }

    async fn perform(&self, ctx: &PortfolioSnapshotCtx) -> Result<Self::Output, Self::Error> {
        self.perform_at(ctx, Utc::now()).await
    }
}

impl PortfolioSnapshotJob {
    /// The full body of [`Job::perform`], with `now` taken as a parameter
    /// instead of read from the wall clock, so tests can deterministically
    /// exercise time-dependent branches (the freshness-defer escape hatch in
    /// particular) without racing real time. `perform` itself is a thin
    /// `Utc::now()` -> here delegation.
    async fn perform_at(
        &self,
        ctx: &PortfolioSnapshotCtx,
        now: DateTime<Utc>,
    ) -> Result<(), PortfolioSnapshotJobError> {
        let today = et_day(now);

        if self.target_et_day > today {
            // Woke before this job's own boundary (e.g. a slightly early
            // scheduler tick): wait for it rather than capturing under a day
            // that has not started yet. `target_et_day` is unchanged, so the
            // next attempt re-evaluates the same comparison.
            return ctx.reschedule(self.target_et_day, EARLY_WAKE_BACKOFF).await;
        }

        if self.target_et_day < today {
            // The target day already passed: hydration stuck across a
            // midnight boundary, or the process woke up late. Catch up to a
            // fresh CURRENT-day reading instead of either back-dating a live
            // read under the stale id or leaving the missed day's sample
            // permanently absent (see this module's doc comment).
            warn!(
                missed_target_et_day = %self.target_et_day,
                catching_up_to_et_day = %today,
                "Portfolio snapshot capture missed its target ET day; catching up to a fresh \
                 same-day reading instead of leaving that day's capital sample absent"
            );
        }
        let target_et_day = today;

        // Resolved once here and threaded through the lower-bound gate and
        // both capped retry backoffs below, instead of each re-deriving it
        // from `target_et_day` independently: `target_et_day` never changes
        // within a single `perform_at` call, so re-resolving it per-check
        // bought nothing but a repeated `et_midnight`/timezone-offset
        // lookup. `capture_lateness_exceeded` below still resolves its own
        // (identical) boundary rather than taking this one, purely so it
        // stays callable with a plain `NaiveDate` for tests -- see its doc.
        let boundary = capture_window_boundary(target_et_day);

        // Gate the CAPTURE itself on a boundary-anchored capture WINDOW,
        // independent of freshness, BEFORE the freshness check below. Lower
        // bound first: a tick landing before target_et_day's boundary (e.g.
        // a hydration/early-wake retry straddling midnight) waits for the
        // window to open rather than capturing off-boundary early. A `None`
        // boundary (DST-ambiguous midnight) never gates here -- structurally
        // guaranteed by `if let Some`, not a runtime choice -- falling
        // through instead to `exceeds_lateness_cap`'s own `None`
        // handling below, and then to `freshness_gap`'s.
        if let Some(boundary_instant) = boundary
            && now < boundary_instant
        {
            let delay = duration_until_or_backoff(
                boundary_instant,
                now,
                target_et_day,
                "capture-window wait",
            );
            return ctx.reschedule(target_et_day, delay).await;
        }

        // Upper bound: a tick landing more than MAX_FRESHNESS_DEFER past the
        // boundary abandons the day even if freshness happens to be fully
        // passing (a >6h-late sample is a worse proxy than a gap, see this
        // module's doc comment).
        if capture_lateness_exceeded(target_et_day, now) {
            // Re-derive the freshness gap purely for diagnostics: it did not
            // gate this abandonment (the window check above runs
            // unconditionally, before freshness is ever consulted), but
            // naming the specific unfresh slot -- when one exists -- lets ops
            // tell a genuine venue-freshness failure apart from a purely
            // late-but-healthy tick from this one log line.
            if let Some(gap) = freshness_gap(ctx, target_et_day) {
                error!(
                    %target_et_day,
                    max_freshness_defer = ?MAX_FRESHNESS_DEFER,
                    %gap,
                    "Portfolio snapshot capture is past its boundary-anchored lateness cap; \
                     abandoning today's capture rather than persisting an off-boundary sample \
                     (still not fresh, named above)"
                );
            } else {
                error!(
                    %target_et_day,
                    max_freshness_defer = ?MAX_FRESHNESS_DEFER,
                    "Portfolio snapshot capture is past its boundary-anchored lateness cap; \
                     abandoning today's capture even though freshness is fully passing -- the \
                     tick simply arrived too late"
                );
            }
            let (delay, next_target_et_day) = next_capture_delay(now);
            return ctx.reschedule(next_target_et_day, delay).await;
        }

        // Freshness is checked BEFORE the inventory view is read: the live
        // poller always updates the view and only then stamps
        // `poll_freshness` (see each `observe()` call site's own comment in
        // `crate::inventory::polling`), so checking freshness first and
        // reading `rows` only after it passes guarantees the captured rows
        // can never predate the freshness confirmation that gated them. The
        // reverse order (read `rows` first, check freshness second) would let
        // a poll tick land in the gap between the two reads, stamping
        // freshness for a view that has since moved past the already-read
        // `rows` -- see this module's `PortfolioSnapshotCtx::poll_freshness`
        // doc.
        if let Some(gap) = freshness_gap(ctx, target_et_day) {
            warn!(
                %gap,
                %target_et_day,
                "Portfolio snapshot capture skipped this tick: inventory view is not fresh \
                 yet this run; retrying shortly"
            );
            // Capped to the remaining time before the lateness cap so the
            // final retry lands at or before the last valid capture instant,
            // never overshoots it via the fixed backoff (which would abandon
            // the day on the next tick even if freshness recovers in the
            // skipped interval).
            return ctx
                .reschedule(target_et_day, capped_retry_backoff(boundary, now))
                .await;
        }

        let rows = {
            let view = ctx.inventory.read().await;
            view.to_portfolio_snapshot_rows()
        };

        if let Some(gap) = hydration_gap(&rows, ctx) {
            warn!(
                %gap,
                %target_et_day,
                "Portfolio snapshot capture skipped this tick: inventory view not fully \
                 hydrated yet; retrying shortly"
            );
            // target_et_day (today), not self.target_et_day: a hydration
            // retry that crosses midnight must keep catching up to the
            // current day, never resume retrying under a day that has since
            // passed. Capped the same way as the freshness-gap retry above,
            // for the same reason.
            return ctx
                .reschedule(target_et_day, capped_retry_backoff(boundary, now))
                .await;
        }

        let rows = convert_wrapped_equity_rows(rows, ctx.wrapper.as_deref()).await?;
        let marked_rows = resolve_marks(&ctx.position_projection, now, rows).await?;

        match ctx
            .portfolio_snapshot
            .send(
                &PortfolioSnapshotId(target_et_day),
                PortfolioSnapshotCommand::Capture {
                    captured_at: now,
                    rows: marked_rows,
                },
            )
            .await
        {
            // Both the success and the AlreadyCaptured-rejection cases log
            // from inside the aggregate's own `initialize`/`transition`
            // handlers (mod.rs), not here: the handler has full state and
            // keeps command-execution logs centralized (AGENTS.md "Log in
            // command handlers, not callers"). AlreadyCaptured is expected,
            // not a failure (decision 11): a scheduling bug that fires more
            // than once a day is still visible via the handler's own info
            // log naming the day, just not treated as retryable.
            Ok(())
            | Err(AggregateError::UserError(LifecycleError::Apply(
                super::PortfolioSnapshotError::AlreadyCaptured,
            ))) => {}
            // Every other SendError, including AggregateConflict, is a real
            // failure: propagate so apalis retries. A retried Capture against
            // a now-Live aggregate simply resolves to AlreadyCaptured above
            // on the next attempt.
            Err(error) => return Err(PortfolioSnapshotJobError::Capture(error)),
        }

        let (delay, next_target_et_day) = next_capture_delay(now);
        ctx.reschedule(next_target_et_day, delay).await
    }
}

impl PortfolioSnapshotCtx {
    /// Enqueues the follow-up job for `target_et_day` after `delay`. Used
    /// both for hydration retries (same `target_et_day`, short backoff) and
    /// for the next day's capture (the next `target_et_day`, computed by
    /// [`next_capture_delay`]).
    async fn reschedule(
        &self,
        target_et_day: NaiveDate,
        delay: Duration,
    ) -> Result<(), PortfolioSnapshotJobError> {
        let mut queue = self.queue.clone();
        queue
            .push_with_delay(PortfolioSnapshotJob { target_et_day }, delay)
            .await?;
        Ok(())
    }
}

/// The ET calendar day `now` falls on. DST-safe by construction: chrono_tz
/// resolves the correct EST/EDT offset for the given instant, so no manual
/// transition handling is needed. `pub(crate)` so integration tests (e.g.
/// `api.rs`'s `/pnl` DST-boundary coverage) can derive the same day a real
/// capture would, instead of assuming it.
pub(crate) fn et_day(now: DateTime<Utc>) -> NaiveDate {
    now.with_timezone(&New_York).date_naive()
}

/// The ET midnight instant (00:00:00 ET) for `day`, converted to UTC. `None`
/// only on the vanishingly rare ET calendar day whose local midnight falls
/// inside a DST spring-forward gap, where that local time is skipped
/// entirely and `TimeZone::with_ymd_and_hms` has no single unambiguous
/// answer.
fn et_midnight(day: NaiveDate) -> Option<DateTime<Utc>> {
    New_York
        .with_ymd_and_hms(day.year(), day.month(), day.day(), 0, 0, 0)
        .single()
        .map(|midnight| midnight.with_timezone(&Utc))
}

/// The next capture instant at or after `now`, and the ET day it is labeled
/// with. A capture that fires at 00:0X ET on day X is labeled `et_day = X`:
/// because the job only ever wakes shortly after an ET midnight (never
/// immediately on bootstrap), "day X" always means the freshly-hydrated view
/// taken right after X's midnight, never an in-progress or stale mid-day
/// read (round-2 fix for the original always-immediate bootstrap).
fn next_capture(now: DateTime<Utc>) -> (DateTime<Utc>, NaiveDate) {
    let today = et_day(now);

    if let Some(midnight) = et_midnight(today) {
        let candidate = midnight + CAPTURE_BUFFER;
        if candidate > now {
            return (candidate, today);
        }
    } else {
        warn!(
            %today,
            "Ambiguous ET midnight while scheduling next portfolio snapshot capture; \
             retrying shortly instead"
        );
        return (now + CAPTURE_BUFFER, today);
    }

    let tomorrow = today + Days::new(1);
    let Some(midnight) = et_midnight(tomorrow) else {
        warn!(
            %tomorrow,
            "Ambiguous ET midnight while scheduling next portfolio snapshot capture; \
             retrying shortly instead"
        );
        return (now + CAPTURE_BUFFER, tomorrow);
    };

    (midnight + CAPTURE_BUFFER, tomorrow)
}

/// Converts an already-resolved future `target` instant into a
/// `push_with_delay` delay relative to `now`, falling back to
/// [`HYDRATION_RETRY_BACKOFF`] (and logging a warning naming `context`) if
/// the subtraction is not positive -- e.g. `now` caught up to or passed
/// `target` between when it was computed and when this runs. Shared by every
/// site that turns such an instant into a delay: `perform_at`'s lower-bound
/// gate, [`next_capture_delay`], and [`first_capture`]'s before-boundary
/// branch, so a future change to the fallback behavior (a different backoff,
/// an added metric) only needs to happen once.
fn duration_until_or_backoff(
    target: DateTime<Utc>,
    now: DateTime<Utc>,
    target_et_day: NaiveDate,
    context: &str,
) -> Duration {
    (target - now).to_std().unwrap_or_else(|error| {
        warn!(
            %error,
            %target_et_day,
            %context,
            "Computed portfolio snapshot delay was not positive; retrying shortly instead"
        );
        HYDRATION_RETRY_BACKOFF
    })
}

/// [`next_capture`]'s instant converted to a `push_with_delay` delay,
/// alongside the target ET day it belongs to.
fn next_capture_delay(now: DateTime<Utc>) -> (Duration, NaiveDate) {
    let (target, target_et_day) = next_capture(now);
    // `next_capture` only ever returns instants at or after `now` by
    // construction, so `duration_until_or_backoff`'s fallback below guards
    // the std-duration conversion itself (e.g. clock adjustments between the
    // two `DateTime::now()` reads it is derived from), not a reachable
    // scheduling bug.
    let delay = duration_until_or_backoff(target, now, target_et_day, "capture delay");
    (delay, target_et_day)
}

/// Enumerates every `(location, asset)` slot the daily portfolio snapshot
/// capture requires to have been observed before it may capture, driven by
/// `configured_equity_symbols`, `usdc_tracking_enabled`, and
/// `wallet_polling_enabled`. Shared by both [`hydration_gap`] (PRESENCE --
/// the slot has a row in the live `InventoryView`) and [`freshness_gap`]
/// (FRESHNESS -- the slot has been observed by a poll THIS process run,
/// membership) so the two gates can never diverge on what "complete" means.
fn required_slots(
    ctx: &PortfolioSnapshotCtx,
) -> impl Iterator<Item = (PortfolioLocation, PortfolioAsset)> + '_ {
    let equity_pairs = ctx.configured_equity_symbols.iter().flat_map(|symbol| {
        [PortfolioLocation::MarketMaking, PortfolioLocation::Hedging]
            .into_iter()
            .map(move |location| (location, PortfolioAsset::Equity(symbol.clone())))
    });

    let usdc_pairs = ctx
        .usdc_tracking_enabled
        .then_some([PortfolioLocation::MarketMaking, PortfolioLocation::Hedging])
        .into_iter()
        .flatten()
        .map(|location| (location, PortfolioAsset::Usdc));

    let wallet_usdc_pairs = ctx
        .wallet_polling_enabled
        .then_some([
            PortfolioLocation::EthereumWallet,
            PortfolioLocation::BaseWalletUnwrapped,
        ])
        .into_iter()
        .flatten()
        .map(|location| (location, PortfolioAsset::Usdc));

    let wallet_equity_pairs = ctx
        .wallet_polling_enabled
        .then(|| ctx.configured_equity_symbols.iter())
        .into_iter()
        .flatten()
        .flat_map(|symbol| {
            [
                PortfolioLocation::BaseWalletUnwrapped,
                PortfolioLocation::BaseWalletWrapped,
            ]
            .into_iter()
            .map(move |location| (location, PortfolioAsset::Equity(symbol.clone())))
        });

    equity_pairs
        .chain(usdc_pairs)
        .chain(wallet_usdc_pairs)
        .chain(wallet_equity_pairs)
}

/// Checks that every REQUIRED `(location, asset)` balance ([`required_slots`])
/// has been observed at least once this run -- i.e. is PRESENT in `rows`
/// (`InventoryView::to_portfolio_snapshot_rows` only emits a row for a slot
/// once it has actually been polled, distinguishing "never polled" from
/// "polled to a genuine zero balance"; see that function's doc comment). A
/// partially-hydrated view is just as dangerous as an empty one: once
/// captured, a day can never be amended (the aggregate's `transition()`
/// unconditionally rejects a second `Capture`). Returns a human-readable
/// description of the first gap found, or `None` if fully hydrated.
///
/// This is a PRESENCE gate, not a FRESHNESS gate -- it does not verify the
/// observation happened recently, only that it happened at all this run
/// (including via startup hydration replaying a persisted
/// `InventorySnapshot`'s original readings). [`freshness_gap`] is the
/// poll-driven freshness signal that closes the restart-stale hole this gate
/// alone cannot see; both run in `perform`, FRESHNESS first (it only needs
/// `ctx`, not `rows`, so it gates before the inventory view is even read) and
/// PRESENCE second, against the rows read after that freshness check passed
/// -- never the reverse, so a captured row can never predate the freshness
/// confirmation that gated it.
fn hydration_gap(rows: &[PortfolioBalanceRow], ctx: &PortfolioSnapshotCtx) -> Option<String> {
    let present: HashSet<(PortfolioLocation, &PortfolioAsset)> =
        rows.iter().map(|row| (row.location, &row.asset)).collect();

    required_slots(ctx).find_map(|(location, asset)| {
        if present.contains(&(location, &asset)) {
            None
        } else {
            Some(format!("{asset} not yet polled at {location}"))
        }
    })
}

/// Checks that every REQUIRED `(location, asset)` slot ([`required_slots`])
/// has been observed by a successful poll on or after `target_et_day`'s ET
/// midnight (`PollFreshness::observed_since` -- see
/// `crate::inventory::freshness`). Closes two holes [`hydration_gap`] alone
/// cannot see: (1) the restart-stale hole -- after a restart the inventory
/// view hydrates from persisted `InventorySnapshot` state, so presence passes
/// immediately even though the current process has not polled anything yet;
/// (2) a slot observed once early in a long-running process's life staying
/// "fresh" for every later day's capture even after the venue that fed it
/// stops polling entirely -- day-scoping, not plain run-scoped membership, is
/// what closes this second hole (see `crate::inventory::freshness`'s module
/// doc). Returns a human-readable description of the first un-observed slot,
/// or `None` if every required slot has been freshly polled since that day's
/// midnight. A DST-ambiguous midnight (`et_midnight` returns `None`) is
/// treated as a gap: retry rather than guessing a floor.
fn freshness_gap(ctx: &PortfolioSnapshotCtx, target_et_day: NaiveDate) -> Option<String> {
    let Some(floor) = et_midnight(target_et_day) else {
        warn!(
            %target_et_day,
            "Ambiguous ET midnight while checking portfolio snapshot freshness; treating as not \
             yet fresh"
        );
        return Some(format!(
            "ET midnight for {target_et_day} is ambiguous (DST transition)"
        ));
    };

    required_slots(ctx).find_map(|(location, asset)| {
        if ctx.poll_freshness.observed_since(location, &asset, floor) {
            None
        } else {
            Some(format!(
                "{asset} not observed by a poll on/after {target_et_day} at {location}"
            ))
        }
    })
}

/// The capture window's lower boundary for `target_et_day`, resolved from
/// `et_midnight` (`None` only on a DST-ambiguous midnight -- see that
/// function's doc). `perform_at` resolves it exactly once per call, into a
/// `boundary` local it threads through its own lower-bound gate and both
/// [`capped_retry_backoff`] retries, instead of each re-deriving it
/// separately; [`first_capture`] resolves it once the same way for its own
/// bootstrap-only checks. [`capture_lateness_exceeded`] resolves it a second
/// time within the same `perform_at` call, purely so it stays callable with
/// a plain `NaiveDate` for tests that want to pin the resolve-then-check
/// combination end to end.
fn capture_window_boundary(target_et_day: NaiveDate) -> Option<DateTime<Utc>> {
    et_midnight(target_et_day).map(|midnight| midnight + CAPTURE_BUFFER)
}

/// Whether `now` exceeds a lateness cap computed from an already-resolved
/// capture-window boundary, [`MAX_FRESHNESS_DEFER`] past it. Split out from
/// [`capture_lateness_exceeded`] as a pure function of an already-resolved
/// `Option<DateTime<Utc>>` (rather than a `NaiveDate`) purely so the
/// DST-ambiguous `None` arm can be pinned by a direct unit test: no real US
/// Eastern calendar date makes `et_midnight` actually return `None` (see
/// that function's doc), so the only way to exercise this arm at all is to
/// hand it an unresolved boundary directly instead of a real date.
fn exceeds_lateness_cap(boundary: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    boundary.is_some_and(|boundary| now > boundary + MAX_FRESHNESS_DEFER)
}

/// Whether `target_et_day`'s capture is past its boundary-anchored lateness
/// cap: true once `now` is more than [`MAX_FRESHNESS_DEFER`] past that day's
/// capture boundary -- see this module's `perform_at` for the caller.
/// Anchored to the boundary ALONE, never to any process-start or restart
/// time, so the cap is
/// restart-proof: a same-day restart after the window has already elapsed
/// cannot reopen it and recapture an already-abandoned day off-boundary (the
/// bug this boundary-anchoring closes -- an earlier
/// `PollFreshness::created_at`-based anchor reset on every restart, so an
/// abandoned day's window re-opened on each restart). A DST-ambiguous
/// boundary (`et_midnight` returns `None`) MUST NOT skip the cap, so this
/// returns `false` in that case ([`exceeds_lateness_cap`]'s `None` handling)
/// -- mirroring `perform_at`'s lower-bound gate, which likewise never blocks
/// on an unresolved boundary, and [`freshness_gap`]'s own `None` handling,
/// which independently treats an ambiguous boundary as a gap -- so the
/// combined effect is always defer-not-capture, never an uncapped or
/// off-boundary capture.
fn capture_lateness_exceeded(target_et_day: NaiveDate, now: DateTime<Utc>) -> bool {
    exceeds_lateness_cap(capture_window_boundary(target_et_day), now)
}

/// Caps a hydration/freshness retry delay to the time remaining before an
/// already-resolved capture-window `boundary`'s lateness cap (see
/// [`capture_lateness_exceeded`]), so the LAST retry before abandonment lands
/// at or before the final valid capture instant instead of jumping past it
/// via the fixed [`HYDRATION_RETRY_BACKOFF`] -- otherwise a fixed-backoff
/// retry scheduled near the end of the window can land its next tick past
/// the cap, abandoning the day even if the missing signal would have become
/// fresh inside the skipped interval. Takes the already-resolved
/// `Option<DateTime<Utc>>` (mirroring [`exceeds_lateness_cap`]) rather than a
/// `NaiveDate` so callers that already hold a resolved `boundary` -- both
/// `perform_at`'s two capped retries and [`first_capture`]'s past-boundary
/// branch -- never re-derive it via a second [`capture_window_boundary`]
/// call.
///
/// Two cases fall back to the ordinary fixed backoff instead of shrinking
/// it: a DST-ambiguous boundary (`None`) has no cap to bound against --
/// `capture_lateness_exceeded` never abandons in that case either; and `now`
/// already past the cap has nothing left to protect (the window is already
/// closed, so there is no overshoot to prevent) -- collapsing to a
/// near-immediate delay there would only defeat bootstrap's own "never
/// enqueue a truly-immediate capture" invariant
/// (`bootstrap_portfolio_snapshot`'s doc) for no benefit, since a job that
/// fires 5 minutes from now abandons via `capture_lateness_exceeded` exactly
/// as surely as one that fires immediately.
fn capped_retry_backoff(boundary: Option<DateTime<Utc>>, now: DateTime<Utc>) -> Duration {
    let Some(cap) = boundary.map(|boundary| boundary + MAX_FRESHNESS_DEFER) else {
        return HYDRATION_RETRY_BACKOFF;
    };

    (cap - now)
        .to_std()
        .map_or(HYDRATION_RETRY_BACKOFF, |remaining| {
            remaining.min(HYDRATION_RETRY_BACKOFF)
        })
}

/// Converts MarketMaking-venue and BaseWalletWrapped-transit equity balances
/// from wrapped ERC-4626 vault-share units to underlying-equivalent units via
/// the live vault ratio, mirroring `InventoryView::check_equity_imbalance`'s
/// conversion (`src/inventory/view.rs`). Every other row (Hedging, USDC
/// anywhere, BaseWalletUnwrapped, EthereumWallet) is already denominated in
/// underlying units and is left untouched.
///
/// `evaluate_day` (`read.rs`) later multiplies each row's balance directly by
/// `Position.last_price`'s price, an UNDERLYING share price -- so a wrapped
/// balance persisted without this conversion would be valued as if 1 wrapped
/// share == 1 underlying share, silently wrong once a vault's ratio departs
/// from 1:1 (dividends, splits, NAV accrual).
///
/// The ratio is resolved once per distinct symbol that actually needs
/// conversion (not once per `configured_equity_symbols`), so a symbol with no
/// MarketMaking/BaseWalletWrapped row this tick costs no RPC call.
async fn convert_wrapped_equity_rows(
    mut rows: Vec<PortfolioBalanceRow>,
    wrapper: Option<&dyn Wrapper>,
) -> Result<Vec<PortfolioBalanceRow>, PortfolioSnapshotJobError> {
    let mut ratios: HashMap<Symbol, UnderlyingPerWrapped> = HashMap::new();

    for row in &mut rows {
        let PortfolioAsset::Equity(symbol) = &row.asset else {
            continue;
        };
        if !matches!(
            row.location,
            PortfolioLocation::MarketMaking | PortfolioLocation::BaseWalletWrapped
        ) {
            continue;
        }

        let ratio = if let Some(ratio) = ratios.get(symbol) {
            *ratio
        } else {
            let wrapper = wrapper.ok_or_else(|| PortfolioSnapshotJobError::MissingWrapper {
                symbol: symbol.clone(),
                location: row.location,
            })?;
            let ratio = wrapper.get_ratio_for_symbol(symbol).await?;
            ratios.insert(symbol.clone(), ratio);
            ratio
        };

        row.available = ratio
            .to_underlying_fractional(FractionalShares::new(row.available))?
            .into();
        row.inflight = ratio
            .to_underlying_fractional(FractionalShares::new(row.inflight))?
            .into();
    }

    Ok(rows)
}

/// Resolves each row's USD mark (decision 7): USDC is par, equity is
/// `Position.last_price`'s price as of capture time. A symbol with a balance
/// but no observed price yet gets `usd_mark: None, mark_captured_at: None` --
/// never a fabricated zero.
///
/// `mark_captured_at` for an equity row is `Position.last_price`'s
/// `observed_at`, a dedicated price-observation timestamp set only by the
/// events that also set the price (`OnChainOrderFilled`, priced
/// `ManualPositionAdjusted`) -- unlike `last_updated`, which advances on every event including
/// price-less ones (offchain order placement/fill/cancel, threshold updates).
/// `read.rs`'s staleness guard (`is_stale_mark`) therefore keys off how
/// recently the price itself was actually observed, not how recently the
/// position was last touched for any reason; see SPEC.md "Portfolio Capital
/// and Return Tracking".
async fn resolve_marks(
    position_projection: &Projection<Position>,
    captured_at: DateTime<Utc>,
    rows: Vec<PortfolioBalanceRow>,
) -> Result<Vec<PortfolioBalanceRowWithMark>, PortfolioSnapshotJobError> {
    let mut marked_rows = Vec::with_capacity(rows.len());

    for row in rows {
        let (usd_mark, mark_captured_at) = match &row.asset {
            PortfolioAsset::Usdc => (Some(USDC_PAR), Some(captured_at)),
            PortfolioAsset::Equity(symbol) => match position_projection.load(symbol).await? {
                Some(position) => position.last_price.map_or((None, None), |observation| {
                    (Some(observation.price), Some(observation.observed_at))
                }),
                None => (None, None),
            },
        };

        marked_rows.push(PortfolioBalanceRowWithMark {
            row,
            usd_mark,
            mark_captured_at,
        });
    }

    Ok(marked_rows)
}

/// The delay and ET day for the very FIRST job [`bootstrap_portfolio_snapshot`]
/// pushes. Unlike [`next_capture`] (which always looks forward to the next
/// boundary, appropriate right after a successful capture), this TARGETS
/// `today` even when `now` is already past today's boundary (round-3 fix: a
/// prior version deferred to `next_capture_delay(now)` unconditionally, which
/// silently skipped "today" on any restart after the boundary). Targeting
/// today does not by itself guarantee CAPTURING it, though: `perform_at`'s
/// window gate ([`capture_lateness_exceeded`]) still abandons today's target
/// once `now` is more than `MAX_FRESHNESS_DEFER` past the boundary, so a
/// sufficiently late restart (e.g. an afternoon deploy) still re-queues today
/// but the capture that runs immediately bails and defers to the next ET day
/// -- only a restart within that bounded window actually lands today's
/// snapshot.
fn first_capture(now: DateTime<Utc>) -> (Duration, NaiveDate) {
    let today = et_day(now);

    let Some(boundary) = capture_window_boundary(today) else {
        warn!(
            %today,
            "Ambiguous ET midnight while bootstrapping today's portfolio snapshot capture; \
             retrying shortly instead"
        );
        return (HYDRATION_RETRY_BACKOFF, today);
    };

    if now >= boundary {
        // Already past today's boundary: schedule the first attempt almost
        // immediately (a short backoff, giving the inventory poller a moment
        // to warm up) rather than waiting for tomorrow -- `perform` itself
        // still gates on `hydration_gap` before it ever captures anything.
        // Capped to the remaining time before the lateness cap, same as the
        // in-run retries `perform_at` schedules: a bootstrap landing close to
        // the cap (but still inside it) must not burn its one shot at the
        // window on a fixed backoff that would land past it. Already past
        // the cap, `capped_retry_backoff` falls back to the ordinary fixed
        // backoff -- there is no window left to protect, and the warm-up
        // delay above still applies.
        return (capped_retry_backoff(Some(boundary), now), today);
    }

    let delay = duration_until_or_backoff(boundary, now, today, "bootstrap delay");
    (delay, today)
}

/// Removes any non-terminal [`PortfolioSnapshotJob`] rows and pushes a fresh
/// one targeting today (via [`first_capture`]), so a restart never silently
/// skips queuing an attempt at the current ET day. Whether that attempt
/// actually captures today depends on `perform_at`'s boundary-anchored
/// lateness cap ([`capture_lateness_exceeded`]) -- a sufficiently late
/// restart still leaves today a gap, by design (see this module's doc
/// comment). Each capture re-enqueues itself with a delay, so a
/// still-scheduled job from a previous run remains in the queue across
/// restarts. Without the purge, the number of concurrent capture loops would
/// grow by one with every restart. Mirrors `bootstrap_check_positions`'s
/// purge-then-push shape.
pub(crate) async fn bootstrap_portfolio_snapshot(
    apalis_pool: &apalis_sqlite::SqlitePool,
    queue: &PortfolioSnapshotJobQueue,
) -> Result<(), PortfolioSnapshotJobError> {
    purge_pending_portfolio_snapshot_jobs(apalis_pool).await?;
    let (delay, target_et_day) = first_capture(Utc::now());
    queue
        .clone()
        .push_with_delay(PortfolioSnapshotJob { target_et_day }, delay)
        .await?;
    Ok(())
}

/// Removes every non-terminal [`PortfolioSnapshotJob`] row: `Pending`,
/// `Queued` (apalis reserved the row for a worker but no `Running` lock has
/// landed yet -- reachable whenever the process crashes between
/// `fetch_next` and `lock`), `Running`, and any `Failed` row still within
/// its retry budget. Matches the sibling `bootstrap_check_positions` purge's
/// coverage of apalis's non-terminal statuses.
async fn purge_pending_portfolio_snapshot_jobs(
    apalis_pool: &apalis_sqlite::SqlitePool,
) -> Result<u64, sqlx_apalis::Error> {
    let job_type = std::any::type_name::<PortfolioSnapshotJob>();
    let deleted = sqlx_apalis::query(
        "DELETE FROM Jobs WHERE job_type = ? AND (status IN (?, ?, ?) \
         OR (status = ? AND attempts < max_attempts))",
    )
    .bind(job_type)
    .bind(Status::Pending.to_string())
    .bind(Status::Queued.to_string())
    .bind(Status::Running.to_string())
    .bind(Status::Failed.to_string())
    .execute(apalis_pool)
    .await?
    .rows_affected();

    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use alloy::primitives::{TxHash, U256};
    use chrono::TimeZone;
    use sqlx::SqlitePool;
    use tokio::sync::broadcast;
    use tokio::time::timeout;

    use st0x_config::ExecutionThreshold;
    use st0x_dto::Statement;
    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::{Direction, FractionalShares, HasZero};
    use st0x_finance::Usdc;
    use st0x_wrapper::MockWrapper;

    use crate::inventory::view::{InFlightCashLocation, InFlightEquityLocation};
    use crate::inventory::{Inventory, InventoryView, Operator, Venue};
    use crate::portfolio_snapshot::read::{DayCapital, DayExclusionReason};
    use crate::portfolio_snapshot::{EtDayRange, PortfolioSnapshotProjection, load_portfolio_days};
    use crate::position::{PositionCommand, TradeId};
    use crate::test_utils::setup_test_pools;

    use super::*;

    fn aapl() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    fn broadcasting(inventory: InventoryView) -> Arc<BroadcastingInventory> {
        let (sender, _receiver) = broadcast::channel::<Statement>(16);
        Arc::new(BroadcastingInventory::new(inventory, sender))
    }

    async fn build_ctx(
        pool: SqlitePool,
        apalis_pool: apalis_sqlite::SqlitePool,
        inventory: InventoryView,
        configured_equity_symbols: HashSet<Symbol>,
        usdc_tracking_enabled: bool,
        wallet_polling_enabled: bool,
        wrapper: Option<Arc<dyn Wrapper>>,
    ) -> (PortfolioSnapshotCtx, Arc<Store<Position>>) {
        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let portfolio_snapshot = StoreBuilder::<PortfolioSnapshot>::new(pool.clone())
            .with(Arc::new(PortfolioSnapshotProjection::new(pool.clone())))
            .build(())
            .await
            .unwrap();

        let queue = PortfolioSnapshotJobQueue::new(&apalis_pool);

        let ctx = PortfolioSnapshotCtx {
            inventory: broadcasting(inventory),
            position_projection,
            portfolio_snapshot,
            wrapper,
            configured_equity_symbols,
            usdc_tracking_enabled,
            wallet_polling_enabled,
            // Always starts empty, mirroring a fresh process boot: callers
            // that need the freshness gate to pass call
            // `mark_all_required_fresh` explicitly, the same way they mutate
            // `ctx.inventory` to simulate a poll landing.
            poll_freshness: PollFreshness::new(),
            queue,
        };

        (ctx, position)
    }

    /// Observes every `(location, asset)` slot [`required_slots`] would name
    /// for `ctx`, onto `ctx.poll_freshness` -- the test-side stand-in for
    /// what a real poll tick (`crate::inventory::polling`) would stamp,
    /// without needing a fully wired `InventoryPollingService` in every
    /// `write.rs` test. Reuses the same enumeration `freshness_gap` itself
    /// walks, so a test marking "all required slots fresh" can never drift
    /// from what the gate actually requires.
    fn mark_all_required_fresh(ctx: &PortfolioSnapshotCtx) {
        for (location, asset) in required_slots(ctx) {
            ctx.poll_freshness.observe(location, asset);
        }
    }

    /// A view with equity and USDC balances present at both venues for
    /// `symbol` -- `hydration_gap` is a PRESENCE gate, so `with_equity`/
    /// `with_usdc` alone are sufficient; this helper exists purely so callers
    /// can pass one `InventoryView` around instead of chaining both builders
    /// inline at every call site. Callers that also need the FRESHNESS gate
    /// to pass must additionally call `mark_all_required_fresh`.
    fn freshly_polled_view(
        symbol: Symbol,
        onchain: FractionalShares,
        offchain: FractionalShares,
        onchain_usdc: Usdc,
        offchain_usdc: Usdc,
    ) -> InventoryView {
        InventoryView::default()
            .with_equity(symbol, onchain, offchain)
            .with_usdc(onchain_usdc, offchain_usdc)
    }

    /// Convenience wrapper: full hydration for one configured symbol (AAPL)
    /// plus USDC, both venues.
    async fn build_fully_hydrated_ctx(
        pool: SqlitePool,
        apalis_pool: apalis_sqlite::SqlitePool,
    ) -> (PortfolioSnapshotCtx, Arc<Store<Position>>) {
        let view = freshly_polled_view(
            aapl(),
            FractionalShares::new(float!(10)),
            FractionalShares::new(float!(5)),
            Usdc::new(float!(1000)),
            Usdc::new(float!(500)),
        );

        let (ctx, position) = build_ctx(
            pool,
            apalis_pool,
            view,
            HashSet::from([aapl()]),
            true,
            false,
            Some(Arc::new(MockWrapper::new())),
        )
        .await;

        mark_all_required_fresh(&ctx);

        (ctx, position)
    }

    async fn portfolio_snapshot_job_count(apalis_pool: &apalis_sqlite::SqlitePool) -> i64 {
        sqlx_apalis::query_scalar::<_, i64>("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
            .bind(std::any::type_name::<PortfolioSnapshotJob>())
            .fetch_one(apalis_pool)
            .await
            .unwrap()
    }

    async fn portfolio_snapshot_row_count(pool: &SqlitePool, et_day: &str) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM portfolio_snapshot WHERE et_day = ?")
            .bind(et_day)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// A job targeting today's ET day -- the common case for tests that
    /// exercise the capture logic itself, not the day-scheduling logic.
    fn job_for_today() -> PortfolioSnapshotJob {
        PortfolioSnapshotJob {
            target_et_day: et_day(Utc::now()),
        }
    }

    /// A deterministic instant just past today's capture boundary, safely
    /// inside `capture_lateness_exceeded`'s window regardless of what real
    /// wall-clock time of day the test suite happens to run at. Ordinary
    /// capture-behavior tests (not specifically exercising the lateness cap
    /// itself) call `perform_at` with this instead of `perform`'s internal
    /// `Utc::now()`, so a CI run late in the ET day cannot spuriously trip
    /// the boundary-anchored lateness cap this module's escape hatch
    /// introduces.
    fn safe_capture_now() -> DateTime<Utc> {
        let today = et_day(Utc::now());
        et_midnight(today).unwrap() + CAPTURE_BUFFER + chrono::Duration::minutes(1)
    }

    #[tokio::test]
    async fn first_capture_of_an_et_day_sends_capture_and_succeeds() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool).await;

        job_for_today()
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();

        let et_day = et_day(Utc::now()).to_string();
        assert!(portfolio_snapshot_row_count(&pool, &et_day).await > 0);
    }

    #[tokio::test]
    async fn second_capture_same_et_day_is_a_no_op_and_still_reschedules() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool.clone()).await;

        job_for_today()
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();
        job_for_today()
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();

        let et_day = et_day(Utc::now()).to_string();
        // Still exactly the rows from the first capture -- the second
        // Capture was rejected before it ever reached the reactor.
        let first_count = portfolio_snapshot_row_count(&pool, &et_day).await;
        assert!(first_count > 0);

        let run_at_count: i64 =
            sqlx_apalis::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        assert_eq!(
            run_at_count, 2,
            "each perform() call reschedules exactly one follow-up job"
        );
    }

    /// The catch-all `Err(error) => return Err(...)` arm (write.rs, distinct
    /// from the expected `AlreadyCaptured` no-op): a genuine `Capture`
    /// failure must propagate so apalis retries, never be swallowed. Forces
    /// a real `SendError` by closing the pool backing the `PortfolioSnapshot`
    /// event store before `perform()` runs. Uses a USDC-only view (no
    /// configured equity symbols) so `resolve_marks` never touches
    /// `position_projection`, isolating the failure to the `Capture` write
    /// under test.
    #[tokio::test]
    async fn genuine_send_failure_propagates_as_err_not_swallowed() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let view =
            InventoryView::default().with_usdc(Usdc::new(float!(1000)), Usdc::new(float!(500)));
        let (ctx, _position) = build_ctx(
            pool.clone(),
            apalis_pool.clone(),
            view,
            HashSet::new(),
            true,
            false,
            None,
        )
        .await;
        mark_all_required_fresh(&ctx);

        pool.close().await;

        let error = job_for_today()
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap_err();

        assert!(
            matches!(error, PortfolioSnapshotJobError::Capture(_)),
            "a genuine send failure must surface as PortfolioSnapshotJobError::Capture, got \
             {error:?}"
        );

        let run_at_count: i64 =
            sqlx_apalis::query_scalar("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        assert_eq!(
            run_at_count, 0,
            "perform() must return before the reschedule call when Capture genuinely fails, so \
             apalis's own retry machinery drives the retry instead"
        );
    }

    #[test]
    fn est_boundary_04_59_and_05_01_utc_route_to_distinct_et_days() {
        let before_midnight = Utc.with_ymd_and_hms(2026, 1, 15, 4, 59, 0).unwrap();
        let after_midnight = Utc.with_ymd_and_hms(2026, 1, 15, 5, 1, 0).unwrap();

        assert_ne!(et_day(before_midnight), et_day(after_midnight));
        assert_eq!(
            et_day(before_midnight),
            NaiveDate::from_ymd_opt(2026, 1, 14).unwrap()
        );
        assert_eq!(
            et_day(after_midnight),
            NaiveDate::from_ymd_opt(2026, 1, 15).unwrap()
        );
    }

    #[test]
    fn edt_boundary_03_59_and_04_01_utc_route_to_distinct_et_days() {
        let before_midnight = Utc.with_ymd_and_hms(2026, 7, 15, 3, 59, 0).unwrap();
        let after_midnight = Utc.with_ymd_and_hms(2026, 7, 15, 4, 1, 0).unwrap();

        assert_ne!(et_day(before_midnight), et_day(after_midnight));
        assert_eq!(
            et_day(before_midnight),
            NaiveDate::from_ymd_opt(2026, 7, 14).unwrap()
        );
        assert_eq!(
            et_day(after_midnight),
            NaiveDate::from_ymd_opt(2026, 7, 15).unwrap()
        );
    }

    /// The DST spring-forward gap (2026: 2am ET on March 8, second Sunday of
    /// March) is 02:00-03:00 local, never midnight -- so `et_midnight` for
    /// this calendar day is an ordinary, unambiguous EST instant, not the
    /// `None` branch. Locks in that assumption so a future change to the
    /// transition-detection logic that wrongly treats midnight itself as
    /// ambiguous is caught here, not first noticed on a real DST day.
    #[test]
    fn et_midnight_on_the_spring_forward_date_is_an_unambiguous_est_instant() {
        let spring_forward_day = NaiveDate::from_ymd_opt(2026, 3, 8).unwrap();

        let midnight = et_midnight(spring_forward_day).unwrap();

        assert_eq!(midnight, Utc.with_ymd_and_hms(2026, 3, 8, 5, 0, 0).unwrap());
    }

    /// Complements the spring-forward test: the DST fall-back ambiguous
    /// window (2026: 1am-2am ET on November 1, first Sunday of November) is
    /// also never midnight, so `et_midnight` for this calendar day is an
    /// ordinary, unambiguous EDT instant.
    #[test]
    fn et_midnight_on_the_fall_back_date_is_an_unambiguous_edt_instant() {
        let fall_back_day = NaiveDate::from_ymd_opt(2026, 11, 1).unwrap();

        let midnight = et_midnight(fall_back_day).unwrap();

        assert_eq!(
            midnight,
            Utc.with_ymd_and_hms(2026, 11, 1, 4, 0, 0).unwrap()
        );
    }

    /// `freshness_gap`'s DST-ambiguous branch (`et_midnight` returning
    /// `None`) is never reached on a real spring-forward day -- this locks in
    /// that a target day right on the transition still gates and passes
    /// normally, not via the "treating as not yet fresh" ambiguous-midnight
    /// arm.
    #[tokio::test]
    async fn freshness_gap_passes_normally_on_the_spring_forward_et_day() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_ctx(
            pool,
            apalis_pool,
            InventoryView::default(),
            HashSet::from([aapl()]),
            true,
            false,
            None,
        )
        .await;
        mark_all_required_fresh(&ctx);

        let target_et_day = NaiveDate::from_ymd_opt(2026, 3, 8).unwrap();
        assert_eq!(
            freshness_gap(&ctx, target_et_day),
            None,
            "spring-forward midnight is unambiguous, so a freshly-observed slot must pass \
             normally, not hit the DST-ambiguous gap branch"
        );
    }

    /// `capture_lateness_exceeded`'s DST-ambiguous branch (returns `false`,
    /// never skipping the cap) is never reached on a real fall-back day --
    /// this locks in that ordinary within-window/past-window behavior holds
    /// on that day, not the ambiguous-midnight arm.
    #[test]
    fn capture_lateness_exceeded_behaves_normally_on_the_fall_back_et_day() {
        let target_et_day = NaiveDate::from_ymd_opt(2026, 11, 1).unwrap();
        let boundary = et_midnight(target_et_day).unwrap() + CAPTURE_BUFFER;

        assert!(
            !capture_lateness_exceeded(
                target_et_day,
                boundary + MAX_FRESHNESS_DEFER - chrono::Duration::minutes(1),
            ),
            "fall-back midnight is unambiguous, so ordinary within-cap behavior must hold, not \
             the DST-ambiguous never-exceeded branch"
        );
        assert!(
            capture_lateness_exceeded(
                target_et_day,
                boundary + MAX_FRESHNESS_DEFER + chrono::Duration::minutes(1),
            ),
            "fall-back midnight is unambiguous, so the day must still exceed the cap past the \
             lateness window"
        );
    }

    /// `now` just after today's boundary, well inside the lateness window
    /// (round-3 fix scenario (b)): `first_capture` must target TODAY, not
    /// tomorrow -- the exact bug this fix closes -- with the ordinary fixed
    /// backoff, since there is far more than `HYDRATION_RETRY_BACKOFF` of
    /// window left to retry within.
    #[test]
    fn first_capture_after_todays_boundary_targets_today_almost_immediately() {
        let today = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let midnight = et_midnight(today).unwrap();
        let now = midnight + CAPTURE_BUFFER + chrono::Duration::minutes(1);

        let (delay, target_et_day) = first_capture(now);

        assert_eq!(target_et_day, today, "bootstrap must never skip today");
        assert_eq!(delay, HYDRATION_RETRY_BACKOFF);
    }

    /// `now` close to (round-3-fix regression, FIX #2): a bootstrap landing
    /// with less than `HYDRATION_RETRY_BACKOFF` of window left before the
    /// lateness cap must not schedule its first attempt past the cap -- the
    /// delay is capped to the remaining time instead of the fixed backoff.
    #[test]
    fn first_capture_close_to_the_cap_uses_the_capped_backoff_not_the_fixed_one() {
        let today = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let boundary = et_midnight(today).unwrap() + CAPTURE_BUFFER;
        let cap = boundary + MAX_FRESHNESS_DEFER;
        let now = cap - chrono::Duration::minutes(2);

        let (delay, target_et_day) = first_capture(now);

        assert_eq!(target_et_day, today);
        assert_eq!(
            delay,
            Duration::from_secs(2 * 60),
            "the first attempt must be scheduled for exactly the remaining window, not the \
             longer fixed HYDRATION_RETRY_BACKOFF that would overshoot the cap"
        );
    }

    /// `now` already past the cap entirely: `first_capture` still targets
    /// today (bootstrap never itself decides to abandon -- that is
    /// `perform_at`'s job), and the delay stays the ordinary fixed
    /// `HYDRATION_RETRY_BACKOFF` rather than collapsing to near-zero -- there
    /// is no window left to protect at this point, and collapsing it would
    /// defeat bootstrap's own "never enqueue a truly-immediate capture"
    /// invariant for no benefit (the job abandons via
    /// `capture_lateness_exceeded` on its first tick regardless of whether
    /// that tick is immediate or 5 minutes out).
    #[test]
    fn first_capture_past_the_cap_still_uses_the_ordinary_fixed_backoff() {
        let today = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let midnight = et_midnight(today).unwrap();
        let now = midnight + CAPTURE_BUFFER + MAX_FRESHNESS_DEFER + chrono::Duration::hours(3);

        let (delay, target_et_day) = first_capture(now);

        assert_eq!(target_et_day, today, "bootstrap still targets today");
        assert_eq!(delay, HYDRATION_RETRY_BACKOFF);
    }

    /// `now` before today's boundary (round-3 fix scenario (c)):
    /// `first_capture` still targets today, just waits for the boundary
    /// rather than capturing early.
    #[test]
    fn first_capture_before_todays_boundary_targets_today_at_the_boundary() {
        let today = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let midnight = et_midnight(today).unwrap();
        let now = midnight + chrono::Duration::minutes(1);

        let (delay, target_et_day) = first_capture(now);

        assert_eq!(target_et_day, today);
        let boundary = midnight + CAPTURE_BUFFER;
        let expected_delay = (boundary - now).to_std().unwrap();
        assert_eq!(delay, expected_delay);
    }

    #[tokio::test]
    async fn empty_inventory_view_fails_gate_and_never_calls_send() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_ctx(
            pool.clone(),
            apalis_pool,
            InventoryView::default(),
            HashSet::from([aapl()]),
            true,
            false,
            None,
        )
        .await;

        job_for_today()
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();

        let et_day = et_day(Utc::now()).to_string();
        assert_eq!(portfolio_snapshot_row_count(&pool, &et_day).await, 0);
    }

    /// Regression for the round-2 blocker: the configured equity symbol is
    /// polled at MarketMaking but NOT at Hedging (USDC tracking disabled so
    /// only the equity gap is under test) -- the original "rows non-empty"
    /// check would have wrongly let this through and permanently locked in
    /// an incomplete day.
    #[tokio::test]
    async fn equity_polled_at_only_one_venue_fails_gate_and_never_calls_send() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let partial_view = InventoryView::default()
            .update_equity(
                &aapl(),
                Inventory::available(
                    Venue::MarketMaking,
                    Operator::Add,
                    FractionalShares::new(float!(10)),
                ),
                Utc::now(),
            )
            .unwrap();
        let (ctx, _position) = build_ctx(
            pool.clone(),
            apalis_pool,
            partial_view,
            HashSet::from([aapl()]),
            false,
            false,
            None,
        )
        .await;

        job_for_today()
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();

        let et_day = et_day(Utc::now()).to_string();
        assert_eq!(portfolio_snapshot_row_count(&pool, &et_day).await, 0);
    }

    /// Same regression, USDC side: polled at only one venue must also fail
    /// the gate (no equity symbols configured, isolating the USDC gap).
    #[tokio::test]
    async fn usdc_polled_at_only_one_venue_fails_gate_and_never_calls_send() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let partial_view = InventoryView::default()
            .update_usdc(
                Inventory::available(Venue::MarketMaking, Operator::Add, Usdc::new(float!(1000))),
                Utc::now(),
            )
            .unwrap();
        let (ctx, _position) = build_ctx(
            pool.clone(),
            apalis_pool,
            partial_view,
            HashSet::new(),
            true,
            false,
            None,
        )
        .await;

        job_for_today()
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();

        let et_day = et_day(Utc::now()).to_string();
        assert_eq!(portfolio_snapshot_row_count(&pool, &et_day).await, 0);
    }

    #[tokio::test]
    async fn subsequent_tick_after_view_becomes_fully_hydrated_succeeds() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_ctx(
            pool.clone(),
            apalis_pool,
            InventoryView::default(),
            HashSet::from([aapl()]),
            true,
            false,
            Some(Arc::new(MockWrapper::new())),
        )
        .await;

        job_for_today()
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();
        let et_day_string = et_day(Utc::now()).to_string();
        assert_eq!(portfolio_snapshot_row_count(&pool, &et_day_string).await, 0);

        {
            let mut view = ctx.inventory.write().await;
            *view = freshly_polled_view(
                aapl(),
                FractionalShares::new(float!(10)),
                FractionalShares::new(float!(5)),
                Usdc::new(float!(1000)),
                Usdc::new(float!(500)),
            );
        }
        // Mirrors what a real poll tick would additionally stamp alongside
        // the view update above: presence alone (the view mutation) is not
        // enough once the freshness gate is in play.
        mark_all_required_fresh(&ctx);

        job_for_today()
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();
        assert!(portfolio_snapshot_row_count(&pool, &et_day_string).await > 0);
    }

    #[tokio::test]
    async fn symbol_with_balance_but_no_price_yet_is_included_with_null_mark() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool).await;

        job_for_today()
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();

        let et_day = et_day(Utc::now()).to_string();
        let stored: Option<String> = sqlx::query_scalar(
            "SELECT usd_mark FROM portfolio_snapshot \
             WHERE et_day = ? AND asset = 'AAPL' AND location = 'market_making'",
        )
        .bind(&et_day)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(stored, None, "AAPL has never filled, so no mark is known");
    }

    /// Distinct from the "never touched" case above: the Position aggregate
    /// IS initialized for AAPL, but no price-carrying event has landed on it
    /// yet -- the inner `None` arm of `resolve_marks`'s equity match, not the
    /// outer one.
    #[tokio::test]
    async fn symbol_with_position_initialized_but_no_price_yet_is_included_with_null_mark() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool).await;

        position
            .send(
                &aapl(),
                PositionCommand::ManuallyAdjustPosition {
                    symbol: aapl(),
                    target_net: FractionalShares::ZERO,
                    reason: "test setup: initialize without a price".to_owned(),
                    threshold: ExecutionThreshold::whole_share(),
                    expected_net: None,
                    price_usdc: None,
                },
            )
            .await
            .unwrap();

        job_for_today()
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();

        let et_day = et_day(Utc::now()).to_string();
        let stored: Option<String> = sqlx::query_scalar(
            "SELECT usd_mark FROM portfolio_snapshot \
             WHERE et_day = ? AND asset = 'AAPL' AND location = 'market_making'",
        )
        .bind(&et_day)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(
            stored, None,
            "Position was initialized but no price-carrying event has landed on it yet"
        );
    }

    /// The one branch of `resolve_marks` that reads a real fill-derived price
    /// out of `Position` and pairs it with `position.last_price`'s
    /// `observed_at`: this must persist the position's price-observation
    /// time, never `last_updated` (which a trailing non-price event
    /// advances) nor the job's own `captured_at`. A trailing
    /// `UpdateThreshold` after the fill diverges `last_updated` from the
    /// price's `observed_at` so a regression that reverted to `last_updated`
    /// would be caught, not accidentally pass by coincidence of all three
    /// timestamps lining up.
    #[tokio::test]
    async fn equity_position_with_known_price_persists_its_price_observation_time() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool).await;

        // Deliberately far from "now" so a regression that swapped in
        // `last_updated` or the job's own `captured_at` would be unmistakable
        // in the assertion below, rather than accidentally matching by
        // coincidence.
        let block_timestamp = Utc.with_ymd_and_hms(2026, 7, 10, 12, 0, 0).unwrap();
        position
            .send(
                &aapl(),
                PositionCommand::AcknowledgeOnChainFillAt {
                    symbol: aapl(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::ZERO,
                        log_index: 0,
                    },
                    amount: FractionalShares::new(float!(1)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp,
                    seen_at: block_timestamp,
                },
            )
            .await
            .unwrap();

        // Trailing non-price event: advances last_updated to ~now without
        // touching last_price.
        position
            .send(
                &aapl(),
                PositionCommand::UpdateThreshold {
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();

        job_for_today()
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();

        let et_day = et_day(Utc::now()).to_string();
        let (usd_mark, mark_captured_at): (Option<String>, Option<String>) = sqlx::query_as(
            "SELECT usd_mark, mark_captured_at FROM portfolio_snapshot \
             WHERE et_day = ? AND asset = 'AAPL' AND location = 'market_making'",
        )
        .bind(&et_day)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(usd_mark.as_deref(), Some("150"));
        assert_eq!(
            mark_captured_at.as_deref(),
            Some(block_timestamp.to_rfc3339().as_str()),
            "mark_captured_at must be Position.last_price's observed_at, not last_updated \
             (which the trailing UpdateThreshold advanced past it)"
        );
    }

    /// The ticket's regression: a position whose price is genuinely stale
    /// (over `MARK_STALENESS_THRESHOLD_DAYS` old) but was touched again today
    /// by an unrelated non-price event must still have its day excluded.
    /// Before this change `mark_captured_at` was `Position.last_updated`,
    /// which the trailing `UpdateThreshold` below would have refreshed to
    /// "now" -- passing the staleness gate despite the price being over a
    /// week old.
    #[tokio::test]
    async fn stale_price_with_recent_non_price_touch_excludes_the_day() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool).await;

        let block_timestamp = Utc::now() - chrono::Duration::days(10);
        position
            .send(
                &aapl(),
                PositionCommand::AcknowledgeOnChainFillAt {
                    symbol: aapl(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::ZERO,
                        log_index: 0,
                    },
                    amount: FractionalShares::new(float!(1)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp,
                    seen_at: block_timestamp,
                },
            )
            .await
            .unwrap();

        // Recent, unrelated non-price touch: advances last_updated to ~now
        // without touching last_price.
        position
            .send(
                &aapl(),
                PositionCommand::UpdateThreshold {
                    threshold: ExecutionThreshold::whole_share(),
                },
            )
            .await
            .unwrap();

        job_for_today()
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();

        let today = et_day(Utc::now());
        let days = load_portfolio_days(
            &pool,
            EtDayRange {
                from: Some(today),
                to: Some(today),
            },
        )
        .await
        .unwrap();

        assert_eq!(days.len(), 1);
        assert_eq!(
            days[0].capital,
            DayCapital::Excluded(DayExclusionReason::StaleMark(
                PortfolioAsset::Equity(aapl()),
                block_timestamp
            )),
            "a stale price must exclude the day even though the position was touched today"
        );
    }

    /// `resolve_marks` must key staleness off `block_timestamp` (the
    /// economic price time), not `seen_at` (when the bot detected the fill).
    /// A fill seen recently but whose `block_timestamp` is old simulates
    /// catch-up backfill after downtime -- using `seen_at` here would read as
    /// fresh and reintroduce the false-fresh bug this field exists to close.
    #[tokio::test]
    async fn stale_block_timestamp_with_recent_seen_at_excludes_the_day() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool).await;

        let block_timestamp = Utc::now() - chrono::Duration::days(10);
        let seen_at = Utc::now();
        position
            .send(
                &aapl(),
                PositionCommand::AcknowledgeOnChainFillAt {
                    symbol: aapl(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        tx_hash: TxHash::ZERO,
                        log_index: 0,
                    },
                    amount: FractionalShares::new(float!(1)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp,
                    seen_at,
                },
            )
            .await
            .unwrap();

        job_for_today()
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();

        let today = et_day(Utc::now());
        let days = load_portfolio_days(
            &pool,
            EtDayRange {
                from: Some(today),
                to: Some(today),
            },
        )
        .await
        .unwrap();

        assert_eq!(days.len(), 1);
        assert_eq!(
            days[0].capital,
            DayCapital::Excluded(DayExclusionReason::StaleMark(
                PortfolioAsset::Equity(aapl()),
                block_timestamp
            )),
            "staleness must key off block_timestamp, not the recent seen_at"
        );
    }

    #[tokio::test]
    async fn reschedules_at_next_et_midnight_plus_buffer_after_capture() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool.clone()).await;

        let now = safe_capture_now();
        // `reschedule` (via apalis's `push_with_delay`) anchors `run_at` to
        // the REAL wall clock at insertion time, not `now` -- only the
        // delay's LENGTH is derived from `now` (see
        // `perform_at_abandons_target_et_day_once_the_defer_window_is_exhausted`
        // for the same pattern).
        let real_before = Utc::now();
        job_for_today().perform_at(&ctx, now).await.unwrap();

        let run_at: i64 =
            sqlx_apalis::query_scalar("SELECT run_at FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();

        let (expected_delay, expected_target_et_day) = next_capture_delay(now);
        let expected = i64::try_from(expected_delay.as_secs()).unwrap() + real_before.timestamp();
        assert!(
            (run_at - expected).abs() <= 5,
            "expected run_at near {expected}, got {run_at}"
        );
        // The reschedule after a successful capture always targets the day
        // AFTER the one just captured -- `job_for_today()` captured today,
        // so the follow-up must never target today again.
        assert_eq!(expected_target_et_day, et_day(now) + Days::new(1));
    }

    #[tokio::test]
    async fn reschedules_with_short_backoff_after_failed_completeness_gate() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_ctx(
            pool.clone(),
            apalis_pool.clone(),
            InventoryView::default(),
            HashSet::from([aapl()]),
            true,
            false,
            None,
        )
        .await;

        let before = Utc::now();
        let job = job_for_today();
        job.clone()
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();

        let run_at: i64 =
            sqlx_apalis::query_scalar("SELECT run_at FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();

        let expected =
            before.timestamp() + i64::try_from(HYDRATION_RETRY_BACKOFF.as_secs()).unwrap();
        assert!(
            (run_at - expected).abs() <= 5,
            "expected run_at near {expected}, got {run_at}"
        );

        let rescheduled_job: Vec<u8> =
            sqlx_apalis::query_scalar("SELECT job FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let rescheduled: PortfolioSnapshotJob = serde_json::from_slice(&rescheduled_job).unwrap();
        assert_eq!(
            rescheduled.target_et_day, job.target_et_day,
            "a hydration retry must keep the same target_et_day, never recompute it"
        );
    }

    /// Round-3 fix scenario (a): a job whose `target_et_day` is yesterday
    /// (hydration stuck past midnight, or a very late wake) must capture
    /// TODAY's fresh balances under TODAY's id -- never back-date a live read
    /// under yesterday's stale id. `build_fully_hydrated_ctx` marks every
    /// required slot fresh, so this also doubles as the "catch-up
    /// succeeds once fresh" half of the catch-up+freshness coverage -- the
    /// complementary "catch-up is gated by freshness" half is the next test.
    #[tokio::test]
    async fn perform_past_its_target_day_catches_up_and_captures_under_today() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool).await;

        let today = et_day(Utc::now());
        let stale_job = PortfolioSnapshotJob {
            target_et_day: today - Days::new(1),
        };

        stale_job
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();

        assert_eq!(
            portfolio_snapshot_row_count(&pool, &(today - Days::new(1)).to_string()).await,
            0,
            "the stale target day must never be captured"
        );
        assert!(
            portfolio_snapshot_row_count(&pool, &today.to_string()).await > 0,
            "the catch-up must be labeled with today, the day the balances were observed on"
        );
    }

    /// Complements the test above: the catch-up rebind to today must still be
    /// gated by FRESHNESS, not just presence -- a fully-hydrated view
    /// (presence passes) with an empty `PollFreshness` (nothing observed by a
    /// poll this run) must never capture, even under the rebound `today` id.
    #[tokio::test]
    async fn catch_up_capture_is_gated_by_freshness_even_when_presence_gate_passes() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let view = freshly_polled_view(
            aapl(),
            FractionalShares::new(float!(10)),
            FractionalShares::new(float!(5)),
            Usdc::new(float!(1000)),
            Usdc::new(float!(500)),
        );
        let (ctx, _position) = build_ctx(
            pool.clone(),
            apalis_pool,
            view,
            HashSet::from([aapl()]),
            true,
            false,
            Some(Arc::new(MockWrapper::new())),
        )
        .await;
        // Deliberately no `mark_all_required_fresh`: presence passes (the
        // view is fully hydrated) but nothing has been observed by a poll
        // this run.

        let today = et_day(Utc::now());
        let stale_job = PortfolioSnapshotJob {
            target_et_day: today - Days::new(1),
        };

        stale_job
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();

        assert_eq!(
            portfolio_snapshot_row_count(&pool, &today.to_string()).await,
            0,
            "the catch-up rebind to today must still be gated by freshness, not just presence"
        );
    }

    /// Compounds the round-3 catch-up fix with a hydration-gap failure: a
    /// job whose `target_et_day` has already passed (yesterday) AND whose
    /// inventory is still incompletely hydrated must reschedule under TODAY
    /// (the local `target_et_day` `perform` computes), never resume
    /// retrying under the stale `self.target_et_day`. Neither the stale day
    /// nor today may be captured -- the completeness gate fails before any
    /// `Capture` is attempted.
    #[tokio::test]
    async fn hydration_retry_compounded_with_stale_target_day_never_captures_under_stale_id() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_ctx(
            pool.clone(),
            apalis_pool.clone(),
            InventoryView::default(),
            HashSet::from([aapl()]),
            true,
            false,
            None,
        )
        .await;

        let today = et_day(Utc::now());
        let stale_job = PortfolioSnapshotJob {
            target_et_day: today - Days::new(1),
        };

        stale_job
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();

        assert_eq!(
            portfolio_snapshot_row_count(&pool, &(today - Days::new(1)).to_string()).await,
            0,
            "the stale target day must never be captured"
        );
        assert_eq!(
            portfolio_snapshot_row_count(&pool, &today.to_string()).await,
            0,
            "an incomplete view must not capture today either"
        );

        let rescheduled_job: Vec<u8> =
            sqlx_apalis::query_scalar("SELECT job FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let rescheduled: PortfolioSnapshotJob = serde_json::from_slice(&rescheduled_job).unwrap();
        assert_eq!(
            rescheduled.target_et_day, today,
            "a hydration-gap retry on a stale job must reschedule under today, never resume \
             retrying under the day that has already passed"
        );
    }

    /// A job that wakes before its own `target_et_day`'s boundary must wait
    /// for it rather than capturing early: `target_et_day` stays unchanged on
    /// the rescheduled follow-up.
    #[tokio::test]
    async fn perform_before_its_target_day_reschedules_without_capturing() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool.clone()).await;

        let future_day = et_day(Utc::now()) + Days::new(1);
        let early_job = PortfolioSnapshotJob {
            target_et_day: future_day,
        };

        early_job
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();

        assert_eq!(
            portfolio_snapshot_row_count(&pool, &future_day.to_string()).await,
            0
        );

        let rescheduled_job: Vec<u8> =
            sqlx_apalis::query_scalar("SELECT job FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let rescheduled: PortfolioSnapshotJob = serde_json::from_slice(&rescheduled_job).unwrap();
        assert_eq!(rescheduled.target_et_day, future_day);
    }

    #[tokio::test]
    async fn bootstrap_purges_stale_pending_row_and_leaves_exactly_one() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let queue = PortfolioSnapshotJobQueue::new(&apalis_pool);

        sqlx_apalis::query(
            "INSERT INTO Jobs (job, id, job_type, status, attempts, max_attempts) \
             VALUES (?, ?, ?, ?, 0, 25)",
        )
        .bind("{}")
        .bind("stale-pending")
        .bind(std::any::type_name::<PortfolioSnapshotJob>())
        .bind(Status::Pending.to_string())
        .execute(&apalis_pool)
        .await
        .unwrap();

        let before = Utc::now();
        // `first_capture(before)` names the same delay `bootstrap_portfolio_snapshot`
        // will compute internally (from a slightly later `Utc::now()`), so it is
        // used below as the expected floor instead of a hardcoded duration: FIX #2
        // in the RAI-1496 pass-2 review, `first_capture` now caps that delay to the
        // time remaining before the lateness cap, so a fixed
        // `HYDRATION_RETRY_BACKOFF`-sized floor would flake if this test happened
        // to run within that capped window.
        let (expected_delay, _) = first_capture(before);

        bootstrap_portfolio_snapshot(&apalis_pool, &queue)
            .await
            .unwrap();

        assert_eq!(portfolio_snapshot_job_count(&apalis_pool).await, 1);

        // Round-2 fix: bootstrap must never enqueue a capture sooner than
        // `first_capture` itself would schedule it for -- ordinarily
        // `HYDRATION_RETRY_BACKOFF`, giving the inventory poller a moment to
        // warm up, but possibly shorter near the boundary-anchored lateness
        // cap (round-3 fix's catch-up-to-today path, further capped by FIX
        // #2). `run_at` is derived from a `Utc::now()` at or after `before`,
        // and `first_capture`'s own delay only ever grows or holds steady as
        // `now` advances (it either tracks a fixed boundary/cap instant or
        // adds a constant backoff), so `expected_delay` computed from
        // `before` is a valid lower bound with a small tolerance for the
        // sub-second gap between the two `Utc::now()` reads.
        let run_at: i64 =
            sqlx_apalis::query_scalar("SELECT run_at FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let expected_floor =
            before.timestamp() + i64::try_from(expected_delay.as_secs()).unwrap() - 1;
        assert!(
            run_at >= expected_floor,
            "bootstrap must not schedule sooner than first_capture's own delay: run_at \
             {run_at}, expected floor {expected_floor}"
        );

        let job_bytes: Vec<u8> =
            sqlx_apalis::query_scalar("SELECT job FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let job: PortfolioSnapshotJob = serde_json::from_slice(&job_bytes).unwrap();
        assert_eq!(
            job.target_et_day,
            et_day(before),
            "bootstrap must target today, never skip it"
        );
    }

    #[tokio::test]
    async fn purge_removes_pending_queued_running_and_retryable_failed_but_keeps_terminal() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_type = std::any::type_name::<PortfolioSnapshotJob>();

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
            job_type,
            "pending-1",
            &Status::Pending.to_string(),
            0,
        )
        .await;
        insert(
            &apalis_pool,
            job_type,
            "queued-1",
            &Status::Queued.to_string(),
            0,
        )
        .await;
        insert(
            &apalis_pool,
            job_type,
            "running-1",
            &Status::Running.to_string(),
            0,
        )
        .await;
        insert(
            &apalis_pool,
            job_type,
            "failed-retryable",
            &Status::Failed.to_string(),
            3,
        )
        .await;
        insert(
            &apalis_pool,
            job_type,
            "failed-exhausted",
            &Status::Failed.to_string(),
            25,
        )
        .await;
        insert(
            &apalis_pool,
            job_type,
            "done-1",
            &Status::Done.to_string(),
            1,
        )
        .await;
        insert(
            &apalis_pool,
            job_type,
            "killed-1",
            &Status::Killed.to_string(),
            1,
        )
        .await;

        let deleted = purge_pending_portfolio_snapshot_jobs(&apalis_pool)
            .await
            .unwrap();
        assert_eq!(deleted, 4);

        let remaining: Vec<String> =
            sqlx_apalis::query_scalar("SELECT id FROM Jobs WHERE job_type = ?")
                .bind(job_type)
                .fetch_all(&apalis_pool)
                .await
                .unwrap();
        assert_eq!(remaining.len(), 3);
        assert!(remaining.contains(&"failed-exhausted".to_string()));
        assert!(remaining.contains(&"done-1".to_string()));
        assert!(remaining.contains(&"killed-1".to_string()));
    }

    /// With wallet polling configured (`wallet_polling_enabled: true`), the
    /// wallet-transit locations must themselves have been polled at least
    /// once before capture, even when every venue balance is present. Before
    /// the first successful wallet poll, `inflight_cash`/`inflight_equity`
    /// are simply empty -- indistinguishable from "no balance" unless the
    /// gate explicitly requires them.
    #[tokio::test]
    async fn wallet_transit_not_yet_polled_fails_gate_when_wallet_polling_enabled() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let view = freshly_polled_view(
            aapl(),
            FractionalShares::new(float!(10)),
            FractionalShares::new(float!(5)),
            Usdc::new(float!(1000)),
            Usdc::new(float!(500)),
        );
        let (ctx, _position) = build_ctx(
            pool.clone(),
            apalis_pool,
            view,
            HashSet::from([aapl()]),
            true,
            true,
            Some(Arc::new(MockWrapper::new())),
        )
        .await;

        job_for_today()
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();

        let et_day = et_day(Utc::now()).to_string();
        assert_eq!(
            portfolio_snapshot_row_count(&pool, &et_day).await,
            0,
            "wallet polling is enabled but no wallet-transit balance has ever been polled"
        );
    }

    /// `convert_wrapped_equity_rows`'s `MissingWrapper` fail-fast guard: a
    /// nonzero MarketMaking equity balance (a WRAPPED ERC-4626 vault-share
    /// balance) requires a live vault ratio to value in underlying-equivalent
    /// units. With no `Wrapper` configured, capture must fail outright --
    /// never silently persist the wrapped balance as if it were already
    /// underlying-denominated (a 1:1 valuation that is only correct by
    /// coincidence, and silently wrong once a vault's ratio departs from
    /// 1:1).
    #[tokio::test]
    async fn missing_wrapper_with_nonzero_market_making_balance_fails_fast() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let view = freshly_polled_view(
            aapl(),
            FractionalShares::new(float!(10)),
            FractionalShares::new(float!(5)),
            Usdc::new(float!(1000)),
            Usdc::new(float!(500)),
        );
        let (ctx, _position) = build_ctx(
            pool.clone(),
            apalis_pool,
            view,
            HashSet::from([aapl()]),
            true,
            false,
            None,
        )
        .await;
        mark_all_required_fresh(&ctx);

        let error = job_for_today()
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap_err();

        match error {
            PortfolioSnapshotJobError::MissingWrapper { symbol, location } => {
                assert_eq!(symbol, aapl());
                assert_eq!(location, PortfolioLocation::MarketMaking);
            }
            other => panic!("expected MissingWrapper, got {other:?}"),
        }

        let et_day = et_day(Utc::now()).to_string();
        assert_eq!(
            portfolio_snapshot_row_count(&pool, &et_day).await,
            0,
            "a failed capture must never persist a partial or incorrectly-valued row"
        );
    }

    /// Onchain MarketMaking equity and BaseWalletWrapped-transit equity are
    /// WRAPPED ERC-4626 vault shares, not underlying shares. With a non-1:1
    /// ratio (1.5, simulating vault NAV accrual from dividends/splits), both
    /// must be persisted as underlying-EQUIVALENT balances (raw * ratio), the
    /// same conversion `InventoryView::check_equity_imbalance` already
    /// applies. Hedging (already underlying) and BaseWalletUnwrapped
    /// (already underlying) must be persisted unconverted.
    #[tokio::test]
    async fn wrapped_equity_rows_are_valued_as_underlying_equivalent_with_non_1_to_1_ratio() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let now = Utc::now();

        let wrapped_market_making = FractionalShares::new(float!(10));
        let unwrapped_offchain = FractionalShares::new(float!(5));
        let wrapped_transit = FractionalShares::new(float!(2));
        let unwrapped_transit = FractionalShares::new(float!(3));

        let view = InventoryView::default()
            .with_equity(aapl(), wrapped_market_making, unwrapped_offchain)
            .record_equity_snapshot_watermarks(Venue::MarketMaking, [&aapl()], now)
            .record_equity_snapshot_watermarks(Venue::Hedging, [&aapl()], now)
            .set_inflight_equity_at_location(
                InFlightEquityLocation::BaseWalletWrapped,
                &BTreeMap::from([(aapl(), wrapped_transit)]),
                now,
                now,
            )
            .set_inflight_equity_at_location(
                InFlightEquityLocation::BaseWalletUnwrapped,
                &BTreeMap::from([(aapl(), unwrapped_transit)]),
                now,
                now,
            )
            .set_inflight_cash(InFlightCashLocation::EthereumWallet, Usdc::ZERO, now, now)
            .set_inflight_cash(InFlightCashLocation::BaseWallet, Usdc::ZERO, now, now);

        let ratio_1_5 = U256::from(1_500_000_000_000_000_000u64);
        let (ctx, _position) = build_ctx(
            pool.clone(),
            apalis_pool,
            view,
            HashSet::from([aapl()]),
            false,
            true,
            Some(Arc::new(MockWrapper::with_ratio(ratio_1_5))),
        )
        .await;
        mark_all_required_fresh(&ctx);

        job_for_today()
            .perform_at(&ctx, safe_capture_now())
            .await
            .unwrap();

        let et_day = et_day(Utc::now()).to_string();

        async fn available_balance(pool: &SqlitePool, et_day: &str, location: &str) -> String {
            sqlx::query_scalar(
                "SELECT available_balance FROM portfolio_snapshot \
                 WHERE et_day = ? AND asset = 'AAPL' AND location = ?",
            )
            .bind(et_day)
            .bind(location)
            .fetch_one(pool)
            .await
            .unwrap()
        }

        async fn inflight_balance(pool: &SqlitePool, et_day: &str, location: &str) -> String {
            sqlx::query_scalar(
                "SELECT inflight_balance FROM portfolio_snapshot \
                 WHERE et_day = ? AND asset = 'AAPL' AND location = ?",
            )
            .bind(et_day)
            .bind(location)
            .fetch_one(pool)
            .await
            .unwrap()
        }

        assert_eq!(
            available_balance(&pool, &et_day, "market_making").await,
            "15",
            "10 wrapped shares * 1.5 ratio = 15 underlying-equivalent shares"
        );
        assert_eq!(
            available_balance(&pool, &et_day, "hedging").await,
            "5",
            "offchain balance is already underlying units and must stay unconverted"
        );
        assert_eq!(
            inflight_balance(&pool, &et_day, "base_wallet_wrapped").await,
            "3",
            "2 wrapped shares * 1.5 ratio = 3 underlying-equivalent shares"
        );
        assert_eq!(
            inflight_balance(&pool, &et_day, "base_wallet_unwrapped").await,
            "3",
            "unwrapped transit balance is already underlying units and must stay unconverted"
        );
    }

    /// Proves `perform_at` checks `freshness_gap` before it ever reads
    /// `ctx.inventory` (the fix for the restart-stale race: reading `rows`
    /// first would let a poll tick land in the gap between the read and the
    /// freshness check, stamping freshness for a view that has since moved
    /// past the already-captured `rows`). Holds the inventory's write lock
    /// for the whole call: if `perform_at` regressed to reading the view
    /// before rejecting on an unmet freshness gate, the read would deadlock
    /// against this held write guard (same task, non-reentrant
    /// `tokio::sync::RwLock`) and the `timeout` below would fire instead of
    /// `perform_at` returning promptly.
    #[tokio::test]
    async fn freshness_gate_is_checked_before_the_inventory_view_is_read() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_ctx(
            pool,
            apalis_pool,
            InventoryView::default(),
            HashSet::from([aapl()]),
            true,
            false,
            None,
        )
        .await;
        // Deliberately no `mark_all_required_fresh`: freshness_gap must fail
        // and the job must reschedule without ever touching ctx.inventory.

        let _held_write_guard = ctx.inventory.write().await;

        let perform_result = timeout(
            Duration::from_millis(200),
            job_for_today().perform_at(&ctx, safe_capture_now()),
        )
        .await
        .expect(
            "perform_at must reject on the freshness gate before reading ctx.inventory; \
                 timed out waiting for the held write lock, meaning the view was read first",
        );
        perform_result.unwrap();
    }

    // freshness_gap and its escape hatch.

    #[tokio::test]
    async fn freshness_gap_blocks_when_a_required_slot_was_never_observed_since_target_et_day() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_ctx(
            pool,
            apalis_pool,
            InventoryView::default(),
            HashSet::from([aapl()]),
            true,
            false,
            None,
        )
        .await;

        // Deterministic: with a single configured symbol, `required_slots`
        // always yields (MarketMaking, Equity(AAPL)) first.
        let target_et_day = et_day(Utc::now());
        assert_eq!(
            freshness_gap(&ctx, target_et_day),
            Some(format!(
                "AAPL not observed by a poll on/after {target_et_day} at market_making"
            ))
        );
    }

    #[tokio::test]
    async fn freshness_gap_passes_when_every_required_slot_was_observed_on_or_after_target_et_day()
    {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_ctx(
            pool,
            apalis_pool,
            InventoryView::default(),
            HashSet::from([aapl()]),
            true,
            false,
            None,
        )
        .await;

        mark_all_required_fresh(&ctx);
        // Re-observing an already-fresh slot (simulating a second poll tick
        // of an unchanged balance) must not un-freshen it -- the core
        // regression this module exists to fix: a static book
        // must stay fresh across ticks, unlike the reverted
        // InventorySnapshot-watermark-based attempt.
        mark_all_required_fresh(&ctx);

        assert_eq!(freshness_gap(&ctx, et_day(Utc::now())), None);
    }

    /// Proves the `(location, asset)` key does not collapse the two
    /// co-located rows at `BaseWalletUnwrapped`: USDC observed, equity not,
    /// must still block on the equity slot specifically.
    #[tokio::test]
    async fn base_wallet_unwrapped_equity_and_usdc_freshness_are_tracked_independently() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_ctx(
            pool,
            apalis_pool,
            InventoryView::default(),
            HashSet::from([aapl()]),
            false,
            true,
            None,
        )
        .await;

        for (location, asset) in required_slots(&ctx) {
            if location == PortfolioLocation::BaseWalletUnwrapped
                && asset == PortfolioAsset::Equity(aapl())
            {
                continue;
            }
            ctx.poll_freshness.observe(location, asset);
        }

        let target_et_day = et_day(Utc::now());
        assert_eq!(
            freshness_gap(&ctx, target_et_day),
            Some(format!(
                "AAPL not observed by a poll on/after {target_et_day} at base_wallet_unwrapped"
            ))
        );
        assert!(
            ctx.poll_freshness.observed_since(
                PortfolioLocation::BaseWalletUnwrapped,
                &PortfolioAsset::Usdc,
                DateTime::<Utc>::MIN_UTC
            ),
            "the USDC leg at the same location must remain observed"
        );
    }

    #[test]
    fn capture_lateness_exceeded_false_within_the_window() {
        let target_et_day = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let boundary = et_midnight(target_et_day).unwrap() + CAPTURE_BUFFER;
        let now = boundary + MAX_FRESHNESS_DEFER - chrono::Duration::minutes(1);

        assert!(!capture_lateness_exceeded(target_et_day, now));
    }

    #[test]
    fn capture_lateness_exceeded_true_past_the_window() {
        let target_et_day = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let boundary = et_midnight(target_et_day).unwrap() + CAPTURE_BUFFER;
        let now = boundary + MAX_FRESHNESS_DEFER + chrono::Duration::minutes(1);

        assert!(capture_lateness_exceeded(target_et_day, now));
    }

    /// Pins the strict inequality (`now > cap`, never `>=`): exactly at the
    /// cap is still within the window, one second past it is not.
    #[test]
    fn capture_lateness_exceeded_false_exactly_at_the_cap_true_one_second_past() {
        let target_et_day = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let boundary = et_midnight(target_et_day).unwrap() + CAPTURE_BUFFER;
        let cap = boundary + MAX_FRESHNESS_DEFER;

        assert!(
            !capture_lateness_exceeded(target_et_day, cap),
            "exactly at the cap must still be within the window (strictly greater-than triggers \
             abandonment)"
        );
        assert!(
            capture_lateness_exceeded(target_et_day, cap + chrono::Duration::seconds(1)),
            "one second past the cap must exceed it"
        );
    }

    /// `exceeds_lateness_cap`'s DST-ambiguous `None` arm, pinned directly:
    /// no real US Eastern calendar date makes `et_midnight` actually return
    /// `None` (see that function's doc), so this is exercised against the
    /// already-resolved `Option<DateTime<Utc>>` boundary rather than through
    /// a `NaiveDate`, mirroring the existing fall-back/spring-forward tests'
    /// intent for `capture_lateness_exceeded`, which is built on top of it.
    /// An unresolved boundary MUST NOT exceed the cap for ANY `now`, so
    /// `capture_lateness_exceeded` never abandons on it, falling through
    /// instead to `perform_at`'s lower-bound gate (also structurally a
    /// no-op on `None`, via `if let Some`) and then to `freshness_gap`'s own
    /// independent `None` handling.
    #[test]
    fn exceeds_lateness_cap_is_false_when_the_boundary_is_unresolved() {
        let far_past = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
        let far_future = Utc.with_ymd_and_hms(2100, 1, 1, 0, 0, 0).unwrap();

        for now in [far_past, Utc::now(), far_future] {
            assert!(
                !exceeds_lateness_cap(None, now),
                "an unresolved boundary must never exceed the cap for now = {now}"
            );
        }
    }

    /// `capped_retry_backoff`'s DST-ambiguous `None` arm, pinned directly the
    /// same way [`exceeds_lateness_cap_is_false_when_the_boundary_is_unresolved`]
    /// pins `exceeds_lateness_cap`'s: no real US Eastern calendar date makes
    /// `et_midnight` actually return `None` (see that function's doc), so
    /// this is exercised against an already-unresolved `Option<DateTime<Utc>>`
    /// boundary directly rather than through a `NaiveDate`. An unresolved
    /// boundary has no cap to shrink the retry against, so this must fall
    /// back to the ordinary fixed [`HYDRATION_RETRY_BACKOFF`] rather than
    /// panicking or computing a nonsensical duration, for any `now`.
    #[test]
    fn capped_retry_backoff_is_the_fixed_backoff_when_the_boundary_is_unresolved() {
        let far_past = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
        let far_future = Utc.with_ymd_and_hms(2100, 1, 1, 0, 0, 0).unwrap();

        for now in [far_past, Utc::now(), far_future] {
            assert_eq!(
                capped_retry_backoff(None, now),
                HYDRATION_RETRY_BACKOFF,
                "an unresolved boundary must fall back to the ordinary fixed backoff for now = \
                 {now}"
            );
        }
    }

    /// Deterministic coverage of `perform_at`'s "keep deferring" branch
    /// (finding: the prior single wall-clock-dependent test could not tell
    /// which of the two escape-hatch branches it exercised on a given CI
    /// run). `now` is pinned inside the defer window, so the freshness-gap
    /// failure must reschedule under the SAME `target_et_day` with the short
    /// hydration-retry backoff, never capture.
    #[tokio::test]
    async fn perform_at_keeps_deferring_target_et_day_when_freshness_gap_fails_within_the_defer_window()
     {
        let (pool, apalis_pool) = setup_test_pools().await;
        let view = freshly_polled_view(
            aapl(),
            FractionalShares::new(float!(10)),
            FractionalShares::new(float!(5)),
            Usdc::new(float!(1000)),
            Usdc::new(float!(500)),
        );
        let (ctx, _position) = build_ctx(
            pool.clone(),
            apalis_pool.clone(),
            view,
            HashSet::from([aapl()]),
            true,
            false,
            Some(Arc::new(MockWrapper::new())),
        )
        .await;
        // Deliberately no `mark_all_required_fresh`: presence passes (the
        // view is fully hydrated) but nothing has been observed by a poll
        // this run -- exactly the restart-stale hole freshness_gap closes.

        let target_et_day = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let boundary = et_midnight(target_et_day).unwrap() + CAPTURE_BUFFER;
        let now = boundary + chrono::Duration::minutes(1);

        let job = PortfolioSnapshotJob { target_et_day };
        job.perform_at(&ctx, now).await.unwrap();

        assert_eq!(
            portfolio_snapshot_row_count(&pool, &target_et_day.to_string()).await,
            0,
            "freshness gap must block capture even though presence-only hydration_gap passes"
        );

        let rescheduled_job: Vec<u8> =
            sqlx_apalis::query_scalar("SELECT job FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let rescheduled: PortfolioSnapshotJob = serde_json::from_slice(&rescheduled_job).unwrap();
        assert_eq!(
            rescheduled.target_et_day, target_et_day,
            "within the lateness window, the retry must keep targeting the same day"
        );
    }

    /// FIX (RAI-1496 follow-up): the freshness-gap retry delay must be capped
    /// to the remaining time before the lateness cap, not the fixed
    /// `HYDRATION_RETRY_BACKOFF` -- otherwise the fixed 5-minute jump can
    /// land past the cap and abandon the day even if freshness recovers
    /// inside the skipped interval. Simulates a two-tick sequence: the first
    /// tick has 2 minutes left before the cap (less than the fixed backoff)
    /// and is not yet fresh, so its retry must be capped to exactly those 2
    /// minutes rather than the full 5; the second tick lands exactly at the
    /// cap with freshness now passing, and must still capture (the cap
    /// allows exactly-at-cap, per `capture_lateness_exceeded`'s strict `>`).
    #[tokio::test]
    async fn freshness_retry_delay_is_capped_so_the_final_retry_lands_at_the_cap_and_still_captures()
     {
        let (pool, apalis_pool) = setup_test_pools().await;
        let view = freshly_polled_view(
            aapl(),
            FractionalShares::new(float!(10)),
            FractionalShares::new(float!(5)),
            Usdc::new(float!(1000)),
            Usdc::new(float!(500)),
        );
        let (ctx, _position) = build_ctx(
            pool.clone(),
            apalis_pool.clone(),
            view,
            HashSet::from([aapl()]),
            true,
            false,
            Some(Arc::new(MockWrapper::new())),
        )
        .await;
        // Deliberately no `mark_all_required_fresh` yet: presence passes but
        // freshness has not, simulating a slot not yet re-polled this run.

        let target_et_day = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let boundary = et_midnight(target_et_day).unwrap() + CAPTURE_BUFFER;
        let cap = boundary + MAX_FRESHNESS_DEFER;
        let first_tick_now = cap - chrono::Duration::minutes(2);

        let real_before = Utc::now();
        let job = PortfolioSnapshotJob { target_et_day };
        job.perform_at(&ctx, first_tick_now).await.unwrap();

        assert_eq!(
            portfolio_snapshot_row_count(&pool, &target_et_day.to_string()).await,
            0,
            "not fresh yet: must not capture on the first tick"
        );

        let run_at: i64 =
            sqlx_apalis::query_scalar("SELECT run_at FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let expected_run_at = real_before.timestamp() + 2 * 60;
        assert!(
            (run_at - expected_run_at).abs() <= 5,
            "the retry must be capped to the 2 minutes remaining before the cap, not the full \
             5-minute HYDRATION_RETRY_BACKOFF that would overshoot it: expected run_at near \
             {expected_run_at}, got {run_at}"
        );

        // Freshness recovers inside the interval the fixed backoff would
        // have skipped over. The rescheduled job's second tick lands exactly
        // at the cap.
        mark_all_required_fresh(&ctx);
        let rescheduled = PortfolioSnapshotJob { target_et_day };
        rescheduled.perform_at(&ctx, cap).await.unwrap();

        assert!(
            portfolio_snapshot_row_count(&pool, &target_et_day.to_string()).await > 0,
            "the final retry at the cap must still capture now that freshness has recovered"
        );
    }

    /// The LOWER bound of the capture window must be enforced too, not just
    /// the upper cap: a tick landing one second before target_et_day's
    /// boundary -- e.g. a hydration or early-wake retry that straddles
    /// midnight and lands in the first few minutes of the new day -- must
    /// wait for the window to open rather than capturing off-boundary early,
    /// even when the view is fully fresh and hydrated (fresh does not mean
    /// eligible: the window has not opened yet).
    #[tokio::test]
    async fn perform_at_reschedules_without_capturing_one_second_before_the_window_opens() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool.clone()).await;

        let target_et_day = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let boundary = et_midnight(target_et_day).unwrap() + CAPTURE_BUFFER;
        let now = boundary - chrono::Duration::seconds(1);

        let real_before = Utc::now();
        let job = PortfolioSnapshotJob { target_et_day };
        job.perform_at(&ctx, now).await.unwrap();

        assert_eq!(
            portfolio_snapshot_row_count(&pool, &target_et_day.to_string()).await,
            0,
            "a tick before the window opens must never capture, even when fully fresh and \
             hydrated"
        );

        let rescheduled_job: Vec<u8> =
            sqlx_apalis::query_scalar("SELECT job FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let rescheduled: PortfolioSnapshotJob = serde_json::from_slice(&rescheduled_job).unwrap();
        assert_eq!(
            rescheduled.target_et_day, target_et_day,
            "the retry must keep targeting the same day, waiting for its own window to open"
        );

        let run_at: i64 =
            sqlx_apalis::query_scalar("SELECT run_at FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let expected_run_at = real_before.timestamp() + 1;
        assert!(
            (run_at - expected_run_at).abs() <= 5,
            "expected run_at near {expected_run_at} (almost exactly at the boundary, one second \
             after `now`), got {run_at}"
        );
    }

    /// Boundary case (folded from the critique): `now` exactly at the
    /// boundary-anchored cap must still be treated as within the window --
    /// `capture_lateness_exceeded` uses a strict `>`, so a fresh capture at
    /// exactly `boundary + MAX_FRESHNESS_DEFER` must still succeed.
    #[tokio::test]
    async fn perform_at_captures_when_now_is_exactly_at_the_lateness_cap() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool).await;

        let target_et_day = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let boundary = et_midnight(target_et_day).unwrap() + CAPTURE_BUFFER;
        let cap = boundary + MAX_FRESHNESS_DEFER;

        let job = PortfolioSnapshotJob { target_et_day };
        job.perform_at(&ctx, cap).await.unwrap();

        assert!(
            portfolio_snapshot_row_count(&pool, &target_et_day.to_string()).await > 0,
            "exactly at the cap must still be within the window and capture when fresh"
        );
    }

    /// The ticket's core regression (RAI-1496 resurrection prevention): even
    /// when freshness genuinely passes -- simulating a same-day restart that
    /// re-established fresh polls after a prior abandonment, or a scheduler
    /// tick simply arriving very late -- a tick past the boundary-anchored
    /// cap must still abandon the day rather than capture it off-boundary.
    /// Under the reverted `PollFreshness::created_at`-anchored design, this
    /// exact scenario (fresh signal, `now` past the old anchor) would have
    /// captured; boundary-anchoring closes that hole because the cap no
    /// longer depends on when this process (or its `PollFreshness`) started.
    #[tokio::test]
    async fn perform_at_abandons_even_when_fresh_one_second_past_the_cap() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool.clone()).await;

        let target_et_day = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let boundary = et_midnight(target_et_day).unwrap() + CAPTURE_BUFFER;
        let now = boundary + MAX_FRESHNESS_DEFER + chrono::Duration::seconds(1);

        let job = PortfolioSnapshotJob { target_et_day };
        job.perform_at(&ctx, now).await.unwrap();

        assert_eq!(
            portfolio_snapshot_row_count(&pool, &target_et_day.to_string()).await,
            0,
            "a fresh-but-past-cap tick must abandon, never capture off-boundary"
        );

        let rescheduled_job: Vec<u8> =
            sqlx_apalis::query_scalar("SELECT job FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let rescheduled: PortfolioSnapshotJob = serde_json::from_slice(&rescheduled_job).unwrap();
        assert_eq!(
            rescheduled.target_et_day,
            target_et_day + Days::new(1),
            "past the cap the day must be abandoned by advancing to the NEXT capture day, never \
             resurrected under the same target_et_day"
        );
    }

    /// Deterministic coverage of `perform_at`'s "abandon the day" branch: the
    /// actual livelock-escape-hatch behavior this feature introduces. `now`
    /// is pinned past `MAX_FRESHNESS_DEFER`, so the job must give up on
    /// `target_et_day`, never capture stale data, and reschedule under the
    /// NEXT capture day computed by `next_capture_delay` -- not a short
    /// tight-loop retry of today.
    #[tokio::test]
    async fn perform_at_abandons_target_et_day_once_the_defer_window_is_exhausted() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let view = freshly_polled_view(
            aapl(),
            FractionalShares::new(float!(10)),
            FractionalShares::new(float!(5)),
            Usdc::new(float!(1000)),
            Usdc::new(float!(500)),
        );
        let (ctx, _position) = build_ctx(
            pool.clone(),
            apalis_pool.clone(),
            view,
            HashSet::from([aapl()]),
            true,
            false,
            Some(Arc::new(MockWrapper::new())),
        )
        .await;
        // Deliberately no `mark_all_required_fresh`.

        let target_et_day = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let boundary = et_midnight(target_et_day).unwrap() + CAPTURE_BUFFER;
        let now = boundary + MAX_FRESHNESS_DEFER + chrono::Duration::minutes(1);

        let job = PortfolioSnapshotJob { target_et_day };
        // `reschedule` (via apalis's `push_with_delay`) anchors `run_at` to
        // the REAL wall clock at insertion time, not the fictitious injected
        // `now` -- only the delay's LENGTH is derived from `now`.
        let real_before = Utc::now();
        job.perform_at(&ctx, now).await.unwrap();

        assert_eq!(
            portfolio_snapshot_row_count(&pool, &target_et_day.to_string()).await,
            0,
            "the abandoned day must never be captured with stale data"
        );

        let (expected_delay, expected_next_target_et_day) = next_capture_delay(now);
        assert_eq!(
            expected_next_target_et_day,
            target_et_day + Days::new(1),
            "the abandon branch must advance to TOMORROW, not merely some other day"
        );
        assert!(
            expected_delay > HYDRATION_RETRY_BACKOFF,
            "no tight loop: the reschedule delay must be to tomorrow's boundary, not a short \
             retry backoff that re-hammers today"
        );

        let rescheduled_job: Vec<u8> =
            sqlx_apalis::query_scalar("SELECT job FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let rescheduled: PortfolioSnapshotJob = serde_json::from_slice(&rescheduled_job).unwrap();
        assert_eq!(
            rescheduled.target_et_day, expected_next_target_et_day,
            "past the defer window, the job must abandon target_et_day and advance to the next \
             capture day computed by next_capture_delay, never resume retrying the abandoned day"
        );

        let run_at: i64 =
            sqlx_apalis::query_scalar("SELECT run_at FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let expected_run_at =
            real_before.timestamp() + i64::try_from(expected_delay.as_secs()).unwrap();
        assert!(
            (run_at - expected_run_at).abs() <= 5,
            "expected run_at near {expected_run_at}, got {run_at}"
        );
    }

    /// Catch-up + cap (folded from the plan): a job whose `target_et_day` has
    /// already passed rebinds to TODAY (the existing catch-up path), and the
    /// boundary-anchored cap check that follows is computed against TODAY's
    /// boundary, not the stale target's. Here `now` is past today's own cap,
    /// so the catch-up rebind must still abandon today (never resurrect
    /// yesterday, never capture today off-boundary either) and reschedule to
    /// the day after today -- not a tight loop re-hammering today or
    /// yesterday.
    #[tokio::test]
    async fn catch_up_rebind_is_still_gated_by_todays_lateness_cap() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool.clone()).await;

        let yesterday = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let today = yesterday + Days::new(1);
        let today_boundary = et_midnight(today).unwrap() + CAPTURE_BUFFER;
        let now = today_boundary + MAX_FRESHNESS_DEFER + chrono::Duration::minutes(1);

        let stale_job = PortfolioSnapshotJob {
            target_et_day: yesterday,
        };
        stale_job.perform_at(&ctx, now).await.unwrap();

        assert_eq!(
            portfolio_snapshot_row_count(&pool, &yesterday.to_string()).await,
            0,
            "the stale target day must never be captured"
        );
        assert_eq!(
            portfolio_snapshot_row_count(&pool, &today.to_string()).await,
            0,
            "today's catch-up rebind must still respect today's own lateness cap"
        );

        let rescheduled_job: Vec<u8> =
            sqlx_apalis::query_scalar("SELECT job FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let rescheduled: PortfolioSnapshotJob = serde_json::from_slice(&rescheduled_job).unwrap();
        assert_eq!(
            rescheduled.target_et_day,
            today + Days::new(1),
            "abandon must advance to the day AFTER today, never resume retrying today or \
             yesterday (no tight loop)"
        );
    }

    /// Catch-up + LOWER bound (the finding's exact scenario): a hydration or
    /// early-wake retry that straddles midnight rebinds a stale job to TODAY
    /// in the same tick that lands `now` inside `[midnight, boundary)` --
    /// i.e. the rebind happens in the ungated first few minutes of the new
    /// day. Even fully fresh and hydrated, this must wait for today's window
    /// to open, never capture off-boundary just because the rebind itself
    /// landed after midnight.
    #[tokio::test]
    async fn catch_up_rebind_still_waits_for_todays_window_when_now_is_before_the_boundary() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool.clone()).await;

        let yesterday = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let today = yesterday + Days::new(1);
        let today_boundary = et_midnight(today).unwrap() + CAPTURE_BUFFER;
        let now = today_boundary - chrono::Duration::seconds(1);

        let stale_job = PortfolioSnapshotJob {
            target_et_day: yesterday,
        };
        stale_job.perform_at(&ctx, now).await.unwrap();

        assert_eq!(
            portfolio_snapshot_row_count(&pool, &yesterday.to_string()).await,
            0,
            "the stale target day must never be captured"
        );
        assert_eq!(
            portfolio_snapshot_row_count(&pool, &today.to_string()).await,
            0,
            "the catch-up rebind must still wait for today's own window to open, not capture \
             one second early"
        );

        let rescheduled_job: Vec<u8> =
            sqlx_apalis::query_scalar("SELECT job FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        let rescheduled: PortfolioSnapshotJob = serde_json::from_slice(&rescheduled_job).unwrap();
        assert_eq!(
            rescheduled.target_et_day, today,
            "the rebind to today stands -- it just waits for the window, never resumes \
             retrying under yesterday's stale id"
        );
    }
}
