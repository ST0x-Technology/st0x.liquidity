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
//! required slot has been polled at least once this run; see that
//! function's doc for the presence-vs-freshness tradeoff accepted for v1).
//! In steady state this fires once a day, right after the
//! boundary. If a restart or a stuck hydration retry causes `perform` to
//! run after its `target_et_day` has already passed, it CATCHES UP to a
//! fresh reading for the CURRENT ET day rather than either back-dating a live
//! read under the stale day's id or leaving that day's capital sample
//! permanently absent: for a roughly delta-neutral book, deployed capital
//! moves slowly and is dominated by deliberate flows, so a same-day reading
//! remains a valid proxy for the missed day. A snapshot is never labeled with
//! a day other than the one its balances were actually observed on. A day the
//! bot is down across entirely (no wake before the next boundary) is simply
//! absent from the series -- coverage is sparse by design, not backfilled.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use apalis::prelude::Status;
use chrono::{DateTime, Datelike, Days, NaiveDate, TimeZone, Utc};
use chrono_tz::America::New_York;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

use st0x_event_sorcery::{AggregateError, LifecycleError, Projection, SendError, Store};
use st0x_execution::{FractionalShares, Symbol};
use st0x_float_macro::float;
use st0x_wrapper::{RatioError, UnderlyingPerWrapped, Wrapper, WrapperError};

use super::{
    PortfolioBalanceRowWithMark, PortfolioSnapshot, PortfolioSnapshotCommand, PortfolioSnapshotId,
};
use crate::conductor::job::{Job, JobQueue, Label, QueuePushError};
use crate::inventory::{
    BroadcastingInventory, PortfolioAsset, PortfolioBalanceRow, PortfolioLocation,
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
        let now = Utc::now();
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
            // passed.
            return ctx.reschedule(target_et_day, HYDRATION_RETRY_BACKOFF).await;
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

/// [`next_capture`]'s instant converted to a `push_with_delay` delay,
/// alongside the target ET day it belongs to.
fn next_capture_delay(now: DateTime<Utc>) -> (Duration, NaiveDate) {
    let (target, target_et_day) = next_capture(now);
    let delay = (target - now).to_std().unwrap_or_else(|error| {
        // `next_capture` only ever returns instants at or after `now` by
        // construction, so this branch guards the std-duration conversion
        // itself (e.g. clock adjustments between the two `DateTime::now()`
        // reads it is derived from), not a reachable scheduling bug.
        warn!(
            %error,
            %target_et_day,
            "Computed portfolio snapshot capture delay was not positive; retrying shortly \
             instead"
        );
        HYDRATION_RETRY_BACKOFF
    });
    (delay, target_et_day)
}

/// Checks that every REQUIRED `(location, asset)` balance has been observed
/// at least once this run -- i.e. is PRESENT in `rows`
/// (`InventoryView::to_portfolio_snapshot_rows` only emits a row for a slot
/// once it has actually been polled, distinguishing "never polled" from
/// "polled to a genuine zero balance"; see that function's doc comment). A
/// partially-hydrated view is just as dangerous as an empty one: once
/// captured, a day can never be amended (the aggregate's `transition()`
/// unconditionally rejects a second `Capture`). Returns a human-readable
/// description of the first gap found, or `None` if fully hydrated.
///
/// **v1 accepted limitation** (see SPEC.md "Portfolio Capital and Return
/// Tracking"): this is a PRESENCE gate, not a FRESHNESS gate -- it does not
/// verify the observation happened recently, only that it happened at all
/// this run (including via startup hydration replaying a persisted
/// `InventorySnapshot`'s original readings). An earlier revision attempted a
/// time-window freshness gate on top of presence, but `InventorySnapshot`
/// suppresses events (and their watermarks) whenever a poll observes an
/// unchanged balance, so the watermark freezes on a static book and the
/// freshness window would eventually block every future capture, not just a
/// stale-hydrated one. In steady state this job only wakes shortly after the
/// ET-midnight `CAPTURE_BUFFER` boundary, by which point the poller has
/// already polled fresh this run, so the residual risk is narrow: a restart
/// that hydrates stale data from a persisted snapshot and then captures
/// before the poller gets a chance to poll again. A robust poll-driven
/// freshness signal (distinct from `InventorySnapshot`'s change-suppressed
/// events) is a tracked follow-up, accepted as out of scope for v1.
fn hydration_gap(rows: &[PortfolioBalanceRow], ctx: &PortfolioSnapshotCtx) -> Option<String> {
    let present: HashSet<(PortfolioLocation, &PortfolioAsset)> =
        rows.iter().map(|row| (row.location, &row.asset)).collect();

    let is_polled = |location: PortfolioLocation, asset: &PortfolioAsset| -> bool {
        present.contains(&(location, asset))
    };

    for symbol in &ctx.configured_equity_symbols {
        for location in [PortfolioLocation::MarketMaking, PortfolioLocation::Hedging] {
            let asset = PortfolioAsset::Equity(symbol.clone());
            if !is_polled(location, &asset) {
                return Some(format!("{symbol} not yet polled at {location}"));
            }
        }
    }

    if ctx.usdc_tracking_enabled {
        for location in [PortfolioLocation::MarketMaking, PortfolioLocation::Hedging] {
            if !is_polled(location, &PortfolioAsset::Usdc) {
                return Some(format!("USDC not yet polled at {location}"));
            }
        }
    }

    if ctx.wallet_polling_enabled {
        for location in [
            PortfolioLocation::EthereumWallet,
            PortfolioLocation::BaseWalletUnwrapped,
        ] {
            if !is_polled(location, &PortfolioAsset::Usdc) {
                return Some(format!(
                    "USDC wallet-transit balance not yet polled at {location}"
                ));
            }
        }

        for symbol in &ctx.configured_equity_symbols {
            for location in [
                PortfolioLocation::BaseWalletUnwrapped,
                PortfolioLocation::BaseWalletWrapped,
            ] {
                let asset = PortfolioAsset::Equity(symbol.clone());
                if !is_polled(location, &asset) {
                    return Some(format!(
                        "{symbol} wallet-transit balance not yet polled at {location}"
                    ));
                }
            }
        }
    }

    None
}

/// Converts MarketMaking-venue and BaseWalletWrapped-transit equity balances
/// from wrapped ERC-4626 vault-share units to underlying-equivalent units via
/// the live vault ratio, mirroring `InventoryView::check_equity_imbalance`'s
/// conversion (`src/inventory/view.rs`). Every other row (Hedging, USDC
/// anywhere, BaseWalletUnwrapped, EthereumWallet) is already denominated in
/// underlying units and is left untouched.
///
/// `evaluate_day` (`read.rs`) later multiplies each row's balance directly by
/// `Position.last_price_usdc`, an UNDERLYING share price -- so a wrapped
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
/// `Position.last_price_usdc` as of capture time. A symbol with a balance but
/// no observed price yet gets `usd_mark: None, mark_captured_at: None` --
/// never a fabricated zero.
///
/// `mark_captured_at` for an equity row is `Position.last_updated`, NOT a
/// dedicated price-observation timestamp -- a precise one is a deferred
/// follow-up. `last_updated` is the aggregate's last-touched time, advanced by
/// several non-price events too (offchain order placement/fill/cancel,
/// threshold updates), so it is only an UPPER BOUND on how recently
/// `last_price_usdc` was actually observed: a symbol whose price hasn't moved
/// in weeks but was otherwise recently touched still reports a "fresh"
/// `mark_captured_at`. `read.rs`'s staleness guard (`is_stale_mark`) therefore
/// reliably EXCLUDES a day once the position itself has gone stale, but is not
/// guaranteed to exclude every day with a genuinely stale price -- a stale
/// `last_price_usdc` can still pass the guard if its position was recently
/// touched for an unrelated (non-price) reason. Known and accepted for this
/// iteration; see SPEC.md "Portfolio Capital and Return Tracking".
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
                Some(position) => match position.last_price_usdc {
                    Some(mark) => (Some(mark), position.last_updated),
                    None => (None, None),
                },
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
/// boundary, appropriate right after a successful capture), this targets
/// `today` even when `now` is already past today's boundary -- e.g. a restart
/// at 9am ET -- so a boot never skips the current ET day's snapshot (round-3
/// fix: a prior version deferred to `next_capture_delay(now)` unconditionally,
/// which silently skipped "today" on any restart after the boundary).
fn first_capture(now: DateTime<Utc>) -> (Duration, NaiveDate) {
    let today = et_day(now);

    let Some(midnight) = et_midnight(today) else {
        warn!(
            %today,
            "Ambiguous ET midnight while bootstrapping today's portfolio snapshot capture; \
             retrying shortly instead"
        );
        return (HYDRATION_RETRY_BACKOFF, today);
    };

    let boundary = midnight + CAPTURE_BUFFER;
    if now >= boundary {
        // Already past today's boundary: schedule almost immediately (a
        // short backoff, giving the inventory poller a moment to warm up)
        // rather than waiting for tomorrow -- `perform` itself still gates
        // on `hydration_gap` before it ever captures anything.
        return (HYDRATION_RETRY_BACKOFF, today);
    }

    let delay = (boundary - now).to_std().unwrap_or_else(|error| {
        warn!(
            %error,
            %today,
            "Computed bootstrap portfolio snapshot delay was not positive; retrying shortly \
             instead"
        );
        HYDRATION_RETRY_BACKOFF
    });
    (delay, today)
}

/// Removes any non-terminal [`PortfolioSnapshotJob`] rows and pushes a fresh
/// one targeting today (via [`first_capture`]), so a restart never leaves the
/// current ET day permanently uncaptured. Each capture re-enqueues itself
/// with a delay, so a still-scheduled job from a previous run remains in the
/// queue across restarts. Without the purge, the number of concurrent capture
/// loops would grow by one with every restart. Mirrors
/// `bootstrap_check_positions`'s purge-then-push shape.
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

    use st0x_config::ExecutionThreshold;
    use st0x_dto::Statement;
    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::{Direction, FractionalShares, HasZero};
    use st0x_finance::Usdc;
    use st0x_wrapper::MockWrapper;

    use crate::inventory::view::{InFlightCashLocation, InFlightEquityLocation};
    use crate::inventory::{Inventory, InventoryView, Operator, Venue};
    use crate::portfolio_snapshot::PortfolioSnapshotProjection;
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
            queue,
        };

        (ctx, position)
    }

    /// A view with equity and USDC balances present at both venues for
    /// `symbol` -- `hydration_gap` is a PRESENCE gate (v1, see its doc
    /// comment), so `with_equity`/`with_usdc` alone are sufficient; this
    /// helper exists purely so callers can pass one `InventoryView` around
    /// instead of chaining both builders inline at every call site.
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

        build_ctx(
            pool,
            apalis_pool,
            view,
            HashSet::from([aapl()]),
            true,
            false,
            Some(Arc::new(MockWrapper::new())),
        )
        .await
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

    #[tokio::test]
    async fn first_capture_of_an_et_day_sends_capture_and_succeeds() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool).await;

        job_for_today().perform(&ctx).await.unwrap();

        let et_day = et_day(Utc::now()).to_string();
        assert!(portfolio_snapshot_row_count(&pool, &et_day).await > 0);
    }

    #[tokio::test]
    async fn second_capture_same_et_day_is_a_no_op_and_still_reschedules() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool.clone()).await;

        job_for_today().perform(&ctx).await.unwrap();
        job_for_today().perform(&ctx).await.unwrap();

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

        pool.close().await;

        let error = job_for_today().perform(&ctx).await.unwrap_err();

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

    /// `now` just after today's boundary (round-3 fix scenario (b)):
    /// `first_capture` must target TODAY, not tomorrow -- the exact bug this
    /// fix closes.
    #[test]
    fn first_capture_after_todays_boundary_targets_today_almost_immediately() {
        let today = NaiveDate::from_ymd_opt(2026, 7, 15).unwrap();
        let midnight = et_midnight(today).unwrap();
        let now = midnight + CAPTURE_BUFFER + chrono::Duration::hours(9);

        let (delay, target_et_day) = first_capture(now);

        assert_eq!(target_et_day, today, "bootstrap must never skip today");
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

        job_for_today().perform(&ctx).await.unwrap();

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

        job_for_today().perform(&ctx).await.unwrap();

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

        job_for_today().perform(&ctx).await.unwrap();

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

        job_for_today().perform(&ctx).await.unwrap();
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

        job_for_today().perform(&ctx).await.unwrap();
        assert!(portfolio_snapshot_row_count(&pool, &et_day_string).await > 0);
    }

    #[tokio::test]
    async fn symbol_with_balance_but_no_price_yet_is_included_with_null_mark() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool).await;

        job_for_today().perform(&ctx).await.unwrap();

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

        job_for_today().perform(&ctx).await.unwrap();

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
    /// out of `Position` and pairs it with `position.last_updated`: this must
    /// persist the position's OWN `last_updated`, never the job's `captured_at`
    /// (an easy one-line regression, since both are `DateTime<Utc>` and
    /// `captured_at` is in scope at that line).
    #[tokio::test]
    async fn equity_position_with_known_price_persists_that_price_and_its_own_last_updated() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool).await;

        // Deliberately far from "now" so a regression that swapped in the
        // job's own `captured_at` would be unmistakable in the assertion
        // below, rather than accidentally matching by coincidence.
        let seen_at = Utc.with_ymd_and_hms(2026, 7, 10, 12, 0, 0).unwrap();
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
                    block_timestamp: seen_at,
                    seen_at,
                },
            )
            .await
            .unwrap();

        job_for_today().perform(&ctx).await.unwrap();

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
            Some(seen_at.to_rfc3339().as_str()),
            "mark_captured_at must be Position.last_updated, not the job's own captured_at"
        );
    }

    #[tokio::test]
    async fn reschedules_at_next_et_midnight_plus_buffer_after_capture() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool.clone()).await;

        let before = Utc::now();
        job_for_today().perform(&ctx).await.unwrap();

        let run_at: i64 =
            sqlx_apalis::query_scalar("SELECT run_at FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();

        let (expected_delay, expected_target_et_day) = next_capture_delay(before);
        let expected = i64::try_from(expected_delay.as_secs()).unwrap() + before.timestamp();
        assert!(
            (run_at - expected).abs() <= 5,
            "expected run_at near {expected}, got {run_at}"
        );
        // The reschedule after a successful capture always targets the day
        // AFTER the one just captured -- `job_for_today()` captured today,
        // so the follow-up must never target today again.
        assert_eq!(expected_target_et_day, et_day(before) + Days::new(1));
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
        job.clone().perform(&ctx).await.unwrap();

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
    /// under yesterday's stale id.
    #[tokio::test]
    async fn perform_past_its_target_day_catches_up_and_captures_under_today() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (ctx, _position) = build_fully_hydrated_ctx(pool.clone(), apalis_pool).await;

        let today = et_day(Utc::now());
        let stale_job = PortfolioSnapshotJob {
            target_et_day: today - Days::new(1),
        };

        stale_job.perform(&ctx).await.unwrap();

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

        stale_job.perform(&ctx).await.unwrap();

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

        early_job.perform(&ctx).await.unwrap();

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
        bootstrap_portfolio_snapshot(&apalis_pool, &queue)
            .await
            .unwrap();

        assert_eq!(portfolio_snapshot_job_count(&apalis_pool).await, 1);

        // Round-2 fix: bootstrap must never enqueue a truly-immediate
        // (zero-delay) capture -- it always waits at least
        // HYDRATION_RETRY_BACKOFF, giving the inventory poller a moment to
        // warm up, even when (round-3 fix) it is catching up to today rather
        // than deferring to tomorrow's boundary.
        let run_at: i64 =
            sqlx_apalis::query_scalar("SELECT run_at FROM Jobs WHERE job_type = ? LIMIT 1")
                .bind(std::any::type_name::<PortfolioSnapshotJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        assert!(
            run_at > before.timestamp() + 60,
            "bootstrap must not schedule an immediate capture: run_at {run_at}, now {}",
            before.timestamp()
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

        job_for_today().perform(&ctx).await.unwrap();

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

        let error = job_for_today().perform(&ctx).await.unwrap_err();

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

        job_for_today().perform(&ctx).await.unwrap();

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
}
