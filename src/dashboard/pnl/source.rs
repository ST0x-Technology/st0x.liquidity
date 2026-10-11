//! Ledger and broker-backed source loading for backend PnL reports.
//!
//! Replay inputs come from the typed, append-only `pnl_*` ledger tables
//! maintained by [`super::ledger::PnlLedger`] (ADR 0018) -- never from the
//! `events` table. Freshness is guaranteed by running the ledger's
//! `catch_up()` before resolving the `asOfRowid` watermark. A live request
//! runs it outside the replay admission permit; a background caller that
//! waits for its permit runs it after the wait (see
//! [`PnlReportAdmission::admit_before_catch_up`]).
use chrono::{DateTime, Days, NaiveDate, NaiveTime, Utc};
use chrono_tz::America::New_York;
use rain_math_float::Float;
use sqlx::{QueryBuilder, Sqlite, SqlitePool};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::task;

use st0x_execution::alpaca_broker_api::{AccountActivitiesQuery, AccountActivity};
use st0x_execution::{AlpacaBrokerApiCtx, AlpacaBrokerApiError};
use st0x_float_serde::format_float;

use crate::portfolio_snapshot::{
    CAPTURE_BUFFER, EtDayRange, capital_summary, evaluate_portfolio_days, load_portfolio_day_rows,
};

use super::builder::{PnlReplay, replay_pnl_rows, summarize_pnl_window};
use super::ledger::{
    CCTP_FEE_SOURCE, DIRECTION_BUY_TEXT, DIRECTION_SELL_TEXT, LedgerHead, PnlLedger,
    TOKENIZATION_FEE_SOURCE,
};
use super::query::{PnlError, PnlQuery};
use super::response::{PnlAvailableRange, PnlCapitalSummary, PnlResponse};
use super::state::{
    BotGasCostRow, CostLedgerRow, CostSource, Direction, ManualAdjustmentRow, OffchainFillRow,
    OffchainPlacementRow, OnchainFillRow, PositionLedgerRow, PositionViewRow,
};
use super::{
    ATTRIBUTION_WARNING, BASELINE_WARNING, CAPITAL_AVAILABLE_NOTE, CAPITAL_UNAVAILABLE_NOTE,
    COST_WARNING, SYMBOL_FILTERED_CAPITAL_WARNING,
};

pub(crate) const MAX_CONCURRENT_PNL_REPORTS: usize = 2;

/// The replay permits one caller's reports share, and what a report does
/// when every permit is taken.
#[derive(Clone)]
pub(crate) struct PnlReportAdmission {
    permits: Arc<Semaphore>,
    when_full: WhenFull,
}

/// What a report does when every permit of its admission is taken.
#[derive(Clone, Copy, Debug)]
enum WhenFull {
    /// Fail with [`PnlError::ReplayAdmission`]: a live `/pnl` request sheds
    /// load instead of queueing.
    Reject,
    /// Wait for a permit: a background caller whose reports may outlive the
    /// wait that started them.
    Wait,
}

impl PnlReportAdmission {
    fn new() -> Self {
        Self::with_permits(MAX_CONCURRENT_PNL_REPORTS)
    }

    /// An admission of its own, so a caller that is not a live `/pnl`
    /// request never takes one of the live request permits. A report it
    /// admits fails when every permit is taken.
    pub(crate) fn with_permits(permits: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(permits)),
            when_full: WhenFull::Reject,
        }
    }

    /// Like [`Self::with_permits`], but a report waits for a permit instead
    /// of failing. The permits are FIFO, so reports run in the order they
    /// asked.
    pub(crate) fn queued(permits: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(permits)),
            when_full: WhenFull::Wait,
        }
    }

    fn try_acquire(&self) -> Result<OwnedSemaphorePermit, TryAcquireError> {
        self.permits.clone().try_acquire_owned()
    }

    /// A permit under this admission's [`WhenFull`] rule.
    pub(crate) async fn admit(&self) -> Result<OwnedSemaphorePermit, PnlError> {
        match self.when_full {
            WhenFull::Reject => acquire_pnl_report_permit(self),
            // The semaphore is never closed; a closed one would surface as
            // the admission error rather than hang.
            WhenFull::Wait => self
                .permits
                .clone()
                .acquire_owned()
                .await
                .map_err(|_closed| PnlError::ReplayAdmission(TryAcquireError::Closed)),
        }
    }

    /// The permit a report takes before its ledger catch-up, if any.
    ///
    /// A waiting admission takes its permit first: the wait can last as long
    /// as the reports queued ahead, and a head read before it would leave the
    /// replay behind the live `position_view` and every event ingested during
    /// the wait. Its permits are the caller's own, so holding one through the
    /// catch-up takes no live `/pnl` slot. A rejecting admission returns
    /// `None` and admits after the catch-up, so a live request does not hold
    /// a replay slot through async I/O.
    pub(super) async fn admit_before_catch_up(
        &self,
    ) -> Result<Option<OwnedSemaphorePermit>, PnlError> {
        match self.when_full {
            WhenFull::Reject => Ok(None),
            WhenFull::Wait => self.admit().await.map(Some),
        }
    }
}

pub(crate) fn pnl_report_admission() -> PnlReportAdmission {
    PnlReportAdmission::new()
}

pub(crate) fn acquire_pnl_report_permit(
    admission: &PnlReportAdmission,
) -> Result<OwnedSemaphorePermit, PnlError> {
    Ok(admission.try_acquire()?)
}

pub(super) async fn run_pnl_replay_with_permit<T, F>(
    permit: OwnedSemaphorePermit,
    replay: F,
) -> Result<(T, OwnedSemaphorePermit), PnlError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, PnlError> + Send + 'static,
{
    let (result, permit) = task::spawn_blocking(move || (replay(), permit)).await?;

    Ok((result?, permit))
}

#[cfg(test)]
pub(super) async fn run_pnl_replay<T, F>(replay: F) -> Result<T, PnlError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, PnlError> + Send + 'static,
{
    let admission = pnl_report_admission();
    let permit = acquire_pnl_report_permit(&admission)?;
    run_pnl_replay_with_permit(permit, replay)
        .await
        .map(|(result, _permit)| result)
}

/// Deadline for bringing the PnL ledger current before one report.
/// Catch-up work is proportional to the un-ingested backlog, not to the
/// request, and concurrent callers serialize on the ledger's internal mutex
/// before any admission control -- so past this deadline the report sheds
/// instead of queueing behind ingestion. The boot-path catch-up stays
/// unbounded: first-deploy backfill may legitimately exceed any request
/// deadline. Cancellation is safe because each ingest batch commits its rows
/// and checkpoint atomically; an elapsed deadline only rolls back the
/// in-flight batch.
pub(crate) const PNL_CATCH_UP_TIMEOUT: Duration = Duration::from_secs(10);

/// What one report reads besides the query.
pub(crate) struct PnlReportDeps<'a> {
    pub(crate) pool: &'a SqlitePool,
    pub(crate) ledger: &'a PnlLedger,
    pub(crate) broker: &'a AlpacaBrokerApiCtx,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PnlReportError {
    #[error(transparent)]
    Report(#[from] PnlError),
    #[error("PnL ledger catch-up exceeded its {:?} deadline", PNL_CATCH_UP_TIMEOUT)]
    CatchUpTimeout,
    #[error("failed to fetch Alpaca account activities")]
    Activities(#[source] Box<AlpacaBrokerApiError>),
}

/// One PnL report, the way `GET /pnl` builds it: validate the query, bring
/// the ledger current within [`PNL_CATCH_UP_TIMEOUT`], check `asOfRowid`
/// against the head, take a permit from `admission`, fetch the Alpaca
/// activities for the window and replay. Every caller runs its own catch-up,
/// so each report sees the events ingested before it. A waiting admission
/// takes its permit before the catch-up instead (see
/// [`PnlReportAdmission::admit_before_catch_up`]).
///
/// Nothing here logs a failure: each caller logs the error it gets back,
/// so one failure writes one log line.
#[expect(
    clippy::significant_drop_tightening,
    reason = "the permit moves into the blocking replay, which hands it back or releases it"
)]
pub(crate) async fn run_pnl_report(
    deps: &PnlReportDeps<'_>,
    query: &PnlQuery,
    admission: &PnlReportAdmission,
) -> Result<PnlResponse, PnlReportError> {
    let after = query.activity_after()?;
    let until = query.activity_until()?;
    query.symbol_filter(&mut Vec::new())?;

    let early_permit = admission.admit_before_catch_up().await?;
    let head = tokio::time::timeout(PNL_CATCH_UP_TIMEOUT, deps.ledger.catch_up())
        .await
        .map_err(|_elapsed| PnlReportError::CatchUpTimeout)?
        .map_err(PnlError::Ledger)?;
    validate_pnl_snapshot_rowid(head, query)?;
    let permit = match early_permit {
        Some(permit) => permit,
        None => admission.admit().await?,
    };

    let activities = deps
        .broker
        .fetch_account_activities(&AccountActivitiesQuery::pnl(after, until))
        .await
        .map_err(|error| PnlReportError::Activities(Box::new(error)))?;

    Ok(
        build_pnl_report_with_permit(deps.pool, query, activities, Utc::now(), permit, head)
            .await?,
    )
}

/// One report or its failure per window, each with its window's key, in the
/// windows' order.
pub(crate) type PnlWindowReports<K> = Vec<(K, Result<PnlResponse, PnlReportError>)>;

/// One report per window, each the way [`run_pnl_report`] builds it for
/// `base` with the window's dates, but over one ledger catch-up, one
/// watermark and one replay. The replay reads no date (see
/// [`replay_pnl_rows`]), so the windows share it, and `windows` picks each
/// window's key and `(fromDate, toDate)` from that replay's range of days
/// with fills, so the dates and the figures come from the same head. Each
/// window still fetches its own Alpaca activities and builds its own summary
/// and capital, one window after another. The list returns after the last
/// window, so one window's slow fetch delays every window's report.
/// A waiting admission takes its permit before the catch-up, as
/// [`run_pnl_report`] does (see [`PnlReportAdmission::admit_before_catch_up`]).
///
/// The outer error is a failure before the replay ends, which fails every
/// window. Each window's own failure is its entry in the list, which holds
/// one entry per window `windows` returned, in order. Like
/// [`run_pnl_report`], nothing here logs a failure.
#[expect(
    clippy::significant_drop_tightening,
    reason = "the permit moves into the blocking replay, which hands it back or releases it"
)]
pub(crate) async fn run_pnl_window_reports<K: Send>(
    deps: &PnlReportDeps<'_>,
    base: &PnlQuery,
    windows: impl FnOnce(&PnlAvailableRange) -> Vec<(K, NaiveDate, NaiveDate)> + Send,
    admission: &PnlReportAdmission,
) -> Result<PnlWindowReports<K>, PnlReportError> {
    base.symbol_filter(&mut Vec::new())?;

    let early_permit = admission.admit_before_catch_up().await?;
    let head = tokio::time::timeout(PNL_CATCH_UP_TIMEOUT, deps.ledger.catch_up())
        .await
        .map_err(|_elapsed| PnlReportError::CatchUpTimeout)?
        .map_err(PnlError::Ledger)?;
    validate_pnl_snapshot_rowid(head, base)?;
    let permit = match early_permit {
        Some(permit) => permit,
        None => admission.admit().await?,
    };

    let (shared, permit) = load_and_replay_pnl(deps.pool, base, head, permit).await?;
    drop(permit);

    let windows = windows(&shared.replay.available_range);
    let mut reports = Vec::with_capacity(windows.len());
    for (key, from, to) in windows {
        let query = PnlQuery {
            from_date: Some(from.to_string()),
            to_date: Some(to.to_string()),
            ..base.clone()
        };
        let report = pnl_window_report(deps, &shared, &query, admission).await;
        reports.push((key, report));
    }

    Ok(reports)
}

/// One window of [`run_pnl_window_reports`]: the same steps as
/// [`run_pnl_report`] after its replay, in the same order. Each window takes
/// its own permit after its Alpaca fetch, so a window that fails gives it
/// back.
async fn pnl_window_report(
    deps: &PnlReportDeps<'_>,
    shared: &SharedPnlReplay,
    query: &PnlQuery,
    admission: &PnlReportAdmission,
) -> Result<PnlResponse, PnlReportError> {
    let activities = deps
        .broker
        .fetch_account_activities(&AccountActivitiesQuery::pnl(
            query.activity_after()?,
            query.activity_until()?,
        ))
        .await
        .map_err(|error| PnlReportError::Activities(Box::new(error)))?;

    let permit = admission.admit().await?;

    Ok(summarize_pnl_report(deps.pool, shared, query, activities, Utc::now(), permit).await?)
}

#[cfg(test)]
pub(crate) async fn build_pnl_report(
    pool: &SqlitePool,
    query: &PnlQuery,
    alpaca_activities: Vec<AccountActivity>,
    now: DateTime<Utc>,
) -> Result<PnlResponse, PnlError> {
    let ledger = super::ledger::PnlLedger::new(pool.clone());
    let head = ledger.catch_up().await?;
    let admission = pnl_report_admission();
    let permit = acquire_pnl_report_permit(&admission)?;
    build_pnl_report_with_permit(pool, query, alpaca_activities, now, permit, head).await
}

/// `head` is the event-log head returned by the ledger's `catch_up()`, which
/// the caller MUST have run before building the report: the resolved
/// `asOfRowid` watermark is only meaningful once the ledger contains
/// everything at or below it. A live request runs it before acquiring the
/// replay permit, as freshness is async I/O and must not burn a live
/// blocking-replay slot; a waiting admission runs it after its permit (see
/// [`PnlReportAdmission::admit_before_catch_up`]).
///
/// The report is [`load_and_replay_pnl`] and then [`summarize_pnl_report`]
/// for its one window, the same two steps every window of
/// [`run_pnl_window_reports`] takes.
pub(crate) async fn build_pnl_report_with_permit(
    pool: &SqlitePool,
    query: &PnlQuery,
    alpaca_activities: Vec<AccountActivity>,
    now: DateTime<Utc>,
    permit: OwnedSemaphorePermit,
    head: LedgerHead,
) -> Result<PnlResponse, PnlError> {
    let (shared, permit) = load_and_replay_pnl(pool, query, head, permit).await?;

    summarize_pnl_report(pool, &shared, query, alpaca_activities, now, permit).await
}

/// A replay at one resolved watermark, and what every window summarized from
/// it shares.
struct SharedPnlReplay {
    replay: Arc<PnlReplay>,
    cost_rows: Arc<Vec<CostLedgerRow>>,
    bot_gas_rows: Arc<Vec<BotGasCostRow>>,
    symbols: BTreeSet<String>,
    /// The warnings every report starts with.
    warnings: Vec<String>,
    resolved_rowid: ResolvedRowid,
}

/// The first half of every report: `query`'s starting warnings and symbol
/// filter, its watermark resolved against `head`, the ledger rows at or
/// below that watermark, and the replay over them on a blocking thread under
/// `permit`, which comes back with the replay. Nothing here reads the
/// query's dates, so every window over the same query shares the result.
async fn load_and_replay_pnl(
    pool: &SqlitePool,
    query: &PnlQuery,
    head: LedgerHead,
    permit: OwnedSemaphorePermit,
) -> Result<(SharedPnlReplay, OwnedSemaphorePermit), PnlError> {
    let mut warnings = vec![
        ATTRIBUTION_WARNING.to_owned(),
        BASELINE_WARNING.to_owned(),
        COST_WARNING.to_owned(),
    ];
    let symbols = query.symbol_filter(&mut warnings)?;
    let resolved_rowid = resolve_as_of_rowid(query, head)?;

    let event_rows = load_position_rows(pool, &symbols, resolved_rowid.resolved).await?;
    let position_rows = load_position_view(pool).await?;
    let cost_rows = load_cost_rows(pool, resolved_rowid.resolved).await?;
    let bot_gas_rows = load_bot_gas_rows(pool, resolved_rowid.resolved).await?;

    let replay_symbols = symbols.clone();
    let (replay, permit) = run_pnl_replay_with_permit(permit, move || {
        replay_pnl_rows(event_rows, &position_rows, &replay_symbols)
    })
    .await?;

    let shared = SharedPnlReplay {
        replay: Arc::new(replay),
        cost_rows: Arc::new(cost_rows),
        bot_gas_rows: Arc::new(bot_gas_rows),
        symbols,
        warnings,
        resolved_rowid,
    };

    Ok((shared, permit))
}

/// The second half of every report: one window's summary of `shared` for
/// `query`'s dates on a blocking thread under `permit`, then its capital.
async fn summarize_pnl_report(
    pool: &SqlitePool,
    shared: &SharedPnlReplay,
    query: &PnlQuery,
    alpaca_activities: Vec<AccountActivity>,
    now: DateTime<Utc>,
    permit: OwnedSemaphorePermit,
) -> Result<PnlResponse, PnlError> {
    let effective_query = PnlQuery {
        as_of_rowid: Some(shared.resolved_rowid.resolved),
        ..query.clone()
    };
    let replay = Arc::clone(&shared.replay);
    let cost_rows = Arc::clone(&shared.cost_rows);
    let bot_gas_rows = Arc::clone(&shared.bot_gas_rows);
    let symbols = shared.symbols.clone();
    let warnings = shared.warnings.clone();
    let ((mut response, daily_net_realized_pnl_usd), permit) =
        run_pnl_replay_with_permit(permit, move || {
            summarize_pnl_window(
                &replay,
                &cost_rows,
                &bot_gas_rows,
                &alpaca_activities,
                &effective_query,
                &symbols,
                warnings,
            )
        })
        .await?;

    apply_capital_summary(
        pool,
        query,
        &shared.resolved_rowid,
        &shared.symbols,
        &daily_net_realized_pnl_usd,
        &mut response,
        now,
        permit,
    )
    .await?;

    Ok(response)
}

/// Populates `response.capital` and its accompanying warnings. Symbol-filtered
/// queries omit capital entirely because a symbol-scoped slice of
/// whole-portfolio capital is not a meaningful denominator. `symbols` is the
/// same parsed filter set the PnL body itself was scoped by
/// (`query.symbol_filter`), not the raw `query.symbol` string, so an
/// empty/whitespace-only `symbol=` param (which `symbol_filter` treats as no
/// filter at all) does not suppress capital while the PnL stays
/// whole-portfolio. Capital is never watermarked to `as_of_rowid` -- it always
/// reflects the live `portfolio_snapshot` table, so a non-current
/// `as_of_rowid` gets an explicit caveat rather than a different figure.
async fn apply_capital_summary(
    pool: &SqlitePool,
    query: &PnlQuery,
    resolved_rowid: &ResolvedRowid,
    symbols: &BTreeSet<String>,
    daily_net_realized_pnl_usd: &BTreeMap<NaiveDate, Float>,
    response: &mut PnlResponse,
    now: DateTime<Utc>,
    permit: OwnedSemaphorePermit,
) -> Result<(), PnlError> {
    if resolved_rowid.resolved != resolved_rowid.max {
        response.warnings.push(format!(
            "Capital and return-on-capital figures reflect the current portfolio snapshot \
             table, not a historical view as of rowid {}: daily snapshots are not watermarked \
             to event rowids.",
            resolved_rowid.resolved
        ));
        // A past as_of_rowid asks for a historical view the snapshot table
        // cannot provide. Leave response.capital at its default (both fields
        // None) rather than silently substituting the live snapshot's current
        // capital for a requested historical one.
        return Ok(());
    }

    if !symbols.is_empty() {
        response
            .warnings
            .push(SYMBOL_FILTERED_CAPITAL_WARNING.to_owned());
        response.warnings.push(CAPITAL_UNAVAILABLE_NOTE.to_owned());
        return Ok(());
    }

    let et_day_range = complete_capital_range(
        pool,
        query.et_day_range()?,
        daily_net_realized_pnl_usd,
        latest_capture_day(now)?,
    )
    .await?;
    let day_rows = load_portfolio_day_rows(pool, et_day_range).await?;
    let daily_net_realized_pnl_usd = daily_net_realized_pnl_usd.clone();
    let (((capital_summary, capital_warnings), capital_note), _permit) =
        run_pnl_replay_with_permit(permit, move || {
            let days = evaluate_portfolio_days(day_rows)?;
            let capital = capital_summary(&days, &daily_net_realized_pnl_usd)?;
            let capital_note = if capital.average_deployed_capital_usd.is_some() {
                CAPITAL_AVAILABLE_NOTE
            } else {
                CAPITAL_UNAVAILABLE_NOTE
            };
            let capital_response = PnlCapitalSummary {
                average_deployed_capital_usd: capital
                    .average_deployed_capital_usd
                    .as_ref()
                    .map(format_float)
                    .transpose()?,
                annualized_return_pct: capital
                    .annualized_return_pct
                    .as_ref()
                    .map(format_float)
                    .transpose()?,
                coverage_days: capital.coverage_days,
                sample_days: capital.sample_days,
                first_snapshot_day: capital.first_snapshot_day.map(|day| day.to_string()),
                last_snapshot_day: capital.last_snapshot_day.map(|day| day.to_string()),
                excluded_days: capital
                    .excluded_days
                    .into_iter()
                    .map(|day| super::response::PnlCapitalExcludedDay {
                        et_day: day.et_day.to_string(),
                        kind: day.reason.kind(),
                        reason: day.reason.describe(),
                    })
                    .collect(),
            };

            Ok(((capital_response, capital.warnings), capital_note))
        })
        .await?;

    response.warnings.extend(capital_warnings);
    response.warnings.push(capital_note.to_owned());
    response.capital = capital_summary;

    Ok(())
}

pub(crate) fn latest_capture_day(now: DateTime<Utc>) -> Result<NaiveDate, PnlError> {
    let now_et = now.with_timezone(&New_York);
    let day = now_et.date_naive();
    if now_et.time() < NaiveTime::MIN + CAPTURE_BUFFER {
        day.checked_sub_days(Days::new(1))
            .ok_or_else(|| PnlError::InvalidDate {
                field: "reportThrough",
                value: day.to_string(),
            })
    } else {
        Ok(day)
    }
}

async fn complete_capital_range(
    pool: &SqlitePool,
    mut range: EtDayRange,
    daily_net_realized_pnl_usd: &BTreeMap<NaiveDate, Float>,
    report_through: NaiveDate,
) -> Result<EtDayRange, PnlError> {
    if range.from.is_none() {
        let first_snapshot: Option<String> =
            sqlx::query_scalar("SELECT MIN(et_day) FROM portfolio_snapshot")
                .fetch_one(pool)
                .await?;
        let first_snapshot = first_snapshot
            .map(|day| {
                NaiveDate::parse_from_str(&day, "%Y-%m-%d").map_err(|_| PnlError::InvalidDate {
                    field: "portfolioSnapshotDay",
                    value: day,
                })
            })
            .transpose()?;
        let first_pnl = daily_net_realized_pnl_usd.keys().next().copied();
        range.from = first_snapshot.into_iter().chain(first_pnl).min();
    }
    if range.to.is_none() {
        range.to = Some(report_through);
    }

    Ok(range)
}

/// Validates a user-supplied `asOfRowid` against the ledger head returned by
/// `catch_up()`.
pub(crate) fn validate_pnl_snapshot_rowid(
    LedgerHead(head): LedgerHead,
    query: &PnlQuery,
) -> Result<(), PnlError> {
    query
        .as_of_rowid
        .map_or(Ok(()), |as_of_rowid| check_as_of_rowid(as_of_rowid, head))
}

/// The effective `as_of_rowid` alongside the current head, so callers can
/// tell whether the resolved value is the live head (needed for the capital
/// as-of-rowid caveat). A named pair rather than a bare `(i64, i64)`
/// prevents the two rowids from being transposed at a call site.
struct ResolvedRowid {
    resolved: i64,
    max: i64,
}

/// Resolves the effective `as_of_rowid` against the caught-up ledger head.
fn resolve_as_of_rowid(
    query: &PnlQuery,
    LedgerHead(head): LedgerHead,
) -> Result<ResolvedRowid, PnlError> {
    if let Some(as_of_rowid) = query.as_of_rowid {
        check_as_of_rowid(as_of_rowid, head)?;
        return Ok(ResolvedRowid {
            resolved: as_of_rowid,
            max: head,
        });
    }

    Ok(ResolvedRowid {
        resolved: head,
        max: head,
    })
}

fn check_as_of_rowid(as_of_rowid: i64, max_rowid: i64) -> Result<(), PnlError> {
    if as_of_rowid < 0 || as_of_rowid > max_rowid {
        return Err(PnlError::InvalidSnapshotRowid { value: as_of_rowid });
    }

    Ok(())
}

fn push_symbol_filter(query: &mut QueryBuilder<Sqlite>, symbols: &BTreeSet<String>) {
    if symbols.is_empty() {
        return;
    }

    query.push(" AND symbol IN (");
    let mut separated = query.separated(", ");
    for symbol in symbols {
        separated.push_bind(symbol.clone());
    }
    separated.push_unseparated(")");
}

fn ledger_direction(
    table: &'static str,
    rowid: i64,
    direction: &str,
) -> Result<Direction, PnlError> {
    match direction {
        DIRECTION_BUY_TEXT => Ok(Direction::Buy),
        DIRECTION_SELL_TEXT => Ok(Direction::Sell),
        _ => Err(PnlError::InvalidLedgerRow {
            table,
            rowid,
            reason: "unknown direction",
        }),
    }
}

/// Loads all four position row kinds from the ledger at or below the
/// watermark, merged in global rowid order -- the same stream shape the raw
/// events query produced, already typed.
async fn load_position_rows(
    pool: &SqlitePool,
    symbols: &BTreeSet<String>,
    as_of_rowid: i64,
) -> Result<Vec<PositionLedgerRow>, PnlError> {
    let mut rows = Vec::new();

    let mut onchain = QueryBuilder::<Sqlite>::new(
        "SELECT event_rowid, symbol, tx_hash, log_index, shares, direction, price_usd, \
         executed_at, \
         CASE WHEN underlying_per_wrapped_event_rowid <= ",
    );
    onchain.push_bind(as_of_rowid);
    onchain.push(
        " THEN underlying_per_wrapped_fixed18 ELSE NULL END \
         FROM pnl_onchain_fill \
         WHERE event_rowid <= ",
    );
    onchain.push_bind(as_of_rowid);
    push_symbol_filter(&mut onchain, symbols);
    for (
        event_rowid,
        symbol,
        tx_hash,
        log_index,
        shares,
        direction,
        price_usd,
        executed_at,
        underlying_per_wrapped_fixed18,
    ) in onchain
        .build_query_as::<(
            i64,
            String,
            String,
            i64,
            String,
            String,
            String,
            String,
            Option<String>,
        )>()
        .fetch_all(pool)
        .await?
    {
        rows.push(PositionLedgerRow::OnchainFill(OnchainFillRow {
            event_rowid,
            symbol,
            tx_hash,
            log_index,
            shares,
            direction: ledger_direction("pnl_onchain_fill", event_rowid, &direction)?,
            price_usd,
            executed_at,
            underlying_per_wrapped_fixed18,
        }));
    }

    let mut offchain = QueryBuilder::<Sqlite>::new(
        "SELECT event_rowid, symbol, offchain_order_id, shares, direction, price_usd, \
         executed_at FROM pnl_offchain_fill WHERE event_rowid <= ",
    );
    offchain.push_bind(as_of_rowid);
    push_symbol_filter(&mut offchain, symbols);
    for (event_rowid, symbol, offchain_order_id, shares, direction, price_usd, executed_at) in
        offchain
            .build_query_as::<(i64, String, String, String, String, String, String)>()
            .fetch_all(pool)
            .await?
    {
        rows.push(PositionLedgerRow::OffchainFill(OffchainFillRow {
            event_rowid,
            symbol,
            offchain_order_id,
            shares,
            direction: ledger_direction("pnl_offchain_fill", event_rowid, &direction)?,
            price_usd,
            executed_at,
        }));
    }

    let mut placements = QueryBuilder::<Sqlite>::new(
        "SELECT event_rowid, symbol, offchain_order_id, placed_at \
         FROM pnl_offchain_placement WHERE event_rowid <= ",
    );
    placements.push_bind(as_of_rowid);
    push_symbol_filter(&mut placements, symbols);
    for (event_rowid, symbol, offchain_order_id, placed_at) in placements
        .build_query_as::<(i64, String, String, String)>()
        .fetch_all(pool)
        .await?
    {
        rows.push(PositionLedgerRow::OffchainPlacement(OffchainPlacementRow {
            event_rowid,
            symbol,
            offchain_order_id,
            placed_at,
        }));
    }

    let mut adjustments = QueryBuilder::<Sqlite>::new(
        "SELECT event_rowid, symbol, target_net, price_usd, adjusted_at \
         FROM pnl_manual_adjustment WHERE event_rowid <= ",
    );
    adjustments.push_bind(as_of_rowid);
    push_symbol_filter(&mut adjustments, symbols);
    for (event_rowid, symbol, target_net, price_usd, adjusted_at) in adjustments
        .build_query_as::<(i64, String, String, Option<String>, String)>()
        .fetch_all(pool)
        .await?
    {
        rows.push(PositionLedgerRow::ManualAdjustment(ManualAdjustmentRow {
            event_rowid,
            symbol,
            target_net,
            price_usd,
            adjusted_at,
        }));
    }

    rows.sort_by_key(PositionLedgerRow::event_rowid);
    Ok(rows)
}

async fn load_position_view(pool: &SqlitePool) -> Result<Vec<PositionViewRow>, PnlError> {
    let rows = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT symbol, net_position \
         FROM position_view \
         WHERE symbol IS NOT NULL \
         ORDER BY symbol ASC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(symbol, net_position)| PositionViewRow {
            symbol,
            net_position,
        })
        .collect())
}

async fn load_cost_rows(
    pool: &SqlitePool,
    as_of_rowid: i64,
) -> Result<Vec<CostLedgerRow>, PnlError> {
    let rows = sqlx::query_as::<_, (i64, String, String, Option<String>, Option<String>, String)>(
        "SELECT event_rowid, source, aggregate_id, symbol, amount_usd, occurred_at \
         FROM pnl_cost_entry \
         WHERE event_rowid <= ? \
         ORDER BY event_rowid ASC",
    )
    .bind(as_of_rowid)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(
            |(event_rowid, source, aggregate_id, symbol, amount_usd, occurred_at)| {
                let source = match source.as_str() {
                    TOKENIZATION_FEE_SOURCE => CostSource::TokenizationFee,
                    CCTP_FEE_SOURCE => CostSource::CctpFee,
                    _ => {
                        return Err(PnlError::InvalidLedgerRow {
                            table: "pnl_cost_entry",
                            rowid: event_rowid,
                            reason: "unknown cost source",
                        });
                    }
                };

                Ok(CostLedgerRow {
                    event_rowid,
                    source,
                    aggregate_id,
                    symbol,
                    amount_usd,
                    occurred_at,
                })
            },
        )
        .collect()
}

async fn load_bot_gas_rows(
    pool: &SqlitePool,
    as_of_rowid: i64,
) -> Result<Vec<BotGasCostRow>, PnlError> {
    let rows = sqlx::query_as::<_, (i64, String, String, String, String, Option<String>, String)>(
        "SELECT event_rowid, chain, tx_hash, usd_cost, operation_category, symbol, occurred_at \
         FROM pnl_bot_gas_cost \
         WHERE event_rowid <= ? \
         ORDER BY event_rowid ASC",
    )
    .bind(as_of_rowid)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(rowid, chain, tx_hash, usd_cost, operation_category, symbol, occurred_at)| {
                BotGasCostRow {
                    rowid,
                    chain,
                    tx_hash,
                    usd_cost,
                    operation_category,
                    symbol,
                    occurred_at,
                }
            },
        )
        .collect())
}
