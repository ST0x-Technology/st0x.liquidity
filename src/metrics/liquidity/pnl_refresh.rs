//! `liq-pnl-refresh`: every 5 minutes, one PnL report per window through
//! the same path as `GET /pnl`, published as that window's family.
//!
//! The windows copy the exporter sidecar. Every window ends on the last day
//! with fills (`availableRange.lastDate`), and every start is clamped to the
//! first day with fills. A cold range cache costs one probe report whose
//! figures are not published.
//!
//! The task has its own one-permit admission, so it never takes a live
//! `/pnl` permit. A report waits for that permit instead of failing, so
//! reports queue in the order the cycle started them and run one at a
//! time. Each report takes the permit first and then runs its own ledger
//! catch-up, as each of the exporter's `/pnl` calls does, so its head is
//! read after its wait in the queue.
//!
//! Each window's report runs as its own task. The cycle waits for it until
//! the window's deadline ([`PNL_WINDOW_DEADLINE`]) and then moves on without
//! cancelling it: the replay runs on a blocking thread that cannot be
//! stopped, and the report it would cut off is valid work. A report that
//! misses its deadline publishes its window when it ends, and the window
//! starts no new report until then, so two reports of one window never race
//! to publish. Every report is bounded anyway: the catch-up has its own
//! deadline, the Alpaca calls have HTTP timeouts, and the replay is finite.
//!
//! The deadline frees the cycle, not the reports: the reports stay serial
//! behind the one permit, so a slow report still delays every report queued
//! behind it, and those windows publish no sooner than they would in a
//! sequential loop. What the deadline buys is that the cycle keeps starting
//! windows, in rotation, instead of stopping at the slow one. A window's
//! `metrics_refresh_duration_seconds` includes its wait for the permit.
//!
//! The cycle starts no window after its budget ([`PNL_WINDOW_BUDGET`]), and
//! each cycle starts one window later in [`PnlWindowKey::REFRESH_ORDER`], so
//! the windows a slow cycle skips change from one cycle to the next.
//!
//! A window whose report fails (including a ledger catch-up past its
//! deadline), is skipped, or is still running keeps its last samples. The
//! exporter drops such a window until its next cycle; keeping it means a
//! reader in that gap sees the last value, and the window's
//! `liq_collector_last_success_ts_seconds` shows its age.
//!
//! The samples are built on a blocking thread too: the long windows cost
//! tens of thousands of `Float` operations, which would hold an async worker
//! for up to about a second.
//!
//! Each window replays the ledger on its own, as each of the exporter's
//! calls did: 6 replays and 6 Alpaca fetches per cycle. One replay shared by
//! the 6 windows would cost about a sixth, but needs the report split into a
//! replay step and a per-window summary step, a change to the PnL code path
//! that wants its own parity test.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use chrono::{Datelike, Days, NaiveDate};
use metrics::histogram;
use sqlx::SqlitePool;
use task_supervisor::{SupervisedTask, TaskResult};
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{info, warn};

use st0x_config::{BrokerCtx, Ctx};
use st0x_execution::AlpacaBrokerApiCtx;

use super::pnl::{PnlWindowKey, pnl_samples};
use super::{LIQ_FAMILIES, LiqFamilies, LiqFamily};
use crate::dashboard::pnl::{
    PnlLedger, PnlQuery, PnlReportAdmission, PnlReportDeps, PnlReportError, PnlResponse,
    run_pnl_report,
};

/// The exporter's PnL cadence.
const PNL_REFRESH_PERIOD: Duration = Duration::from_secs(300);

/// Wall-clock budget for one cycle's windows. A window that would start
/// after it keeps its last samples until the next cycle.
const PNL_WINDOW_BUDGET: Duration = Duration::from_secs(120);

/// How long the cycle waits for one window's report before it starts the
/// next window. A quarter of the budget, so one slow report cannot keep
/// every later window from starting. The report keeps running past it.
const PNL_WINDOW_DEADLINE: Duration = Duration::from_secs(30);

/// Reports the task runs at once: one, so the live `/pnl` permits stay free.
const PNL_METRICS_REPORTS: usize = 1;

/// Where the task gets one PnL report. The live source is
/// [`LivePnlReports`]; tests count and fail calls.
pub(crate) trait PnlReports: Clone + Send + Sync + 'static {
    fn report(
        &self,
        query: PnlQuery,
    ) -> impl Future<Output = Result<PnlResponse, PnlReportError>> + Send;
}

/// Reports through [`run_pnl_report`] with the task's own admission.
#[derive(Clone)]
pub(crate) struct LivePnlReports {
    pool: SqlitePool,
    ledger: Arc<PnlLedger>,
    broker: AlpacaBrokerApiCtx,
    admission: PnlReportAdmission,
}

impl LivePnlReports {
    pub(crate) fn new(ctx: &Ctx, pool: SqlitePool, ledger: Arc<PnlLedger>) -> Self {
        let BrokerCtx::AlpacaBrokerApi(broker) = &ctx.broker;

        Self {
            pool,
            ledger,
            broker: broker.clone(),
            admission: PnlReportAdmission::queued(PNL_METRICS_REPORTS),
        }
    }
}

impl PnlReports for LivePnlReports {
    async fn report(&self, query: PnlQuery) -> Result<PnlResponse, PnlReportError> {
        let deps = PnlReportDeps {
            pool: &self.pool,
            ledger: &self.ledger,
            broker: &self.broker,
        };

        run_pnl_report(&deps, &query, &self.admission).await
    }
}

/// The first and last days with fills, from a report's `availableRange`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FillDates {
    first: NaiveDate,
    last: NaiveDate,
}

impl FillDates {
    /// `None` when the report has no fills. Unparseable dates are logged
    /// and treated the same way.
    fn of(report: &PnlResponse) -> Option<Self> {
        let range = &report.available_range;
        let (Some(first), Some(last)) = (&range.first_date, &range.last_date) else {
            return None;
        };

        match (parse_day(first), parse_day(last)) {
            (Ok(first), Ok(last)) => Some(Self { first, last }),
            (Err(error), _) | (_, Err(error)) => {
                warn!(%first, %last, %error, "Ignored a PnL available range that does not parse");
                None
            }
        }
    }
}

fn parse_day(day: &str) -> Result<NaiveDate, chrono::ParseError> {
    NaiveDate::parse_from_str(day, "%Y-%m-%d")
}

/// The `(fromDate, toDate)` of one window: it ends on the last day with
/// fills and starts no earlier than the first. `None` only when the date
/// arithmetic leaves the calendar.
pub(crate) fn window_dates(
    window: PnlWindowKey,
    dates: FillDates,
) -> Option<(NaiveDate, NaiveDate)> {
    let anchor = dates.last;
    let start = match window {
        PnlWindowKey::OneDay => Some(anchor),
        PnlWindowKey::OneWeek => anchor.checked_sub_days(Days::new(6)),
        PnlWindowKey::OneMonth => anchor.checked_sub_days(Days::new(30)),
        PnlWindowKey::YearToDate => NaiveDate::from_ymd_opt(anchor.year(), 1, 1),
        PnlWindowKey::OneYear => anchor.checked_sub_days(Days::new(364)),
        PnlWindowKey::All => Some(dates.first),
    }?;

    Some((start.max(dates.first), anchor))
}

/// [`PnlWindowKey::REFRESH_ORDER`] starting at its `first` entry, wrapping
/// around.
fn cycle_order(first: usize) -> [PnlWindowKey; 6] {
    let mut order = PnlWindowKey::REFRESH_ORDER;
    let shift = first % order.len();
    order.rotate_left(shift);
    order
}

/// One window's running report: the fill dates it carried, once it ends.
type WindowReport = JoinHandle<Option<FillDates>>;

/// Every window report from its start until the task collects it, a report
/// the cycle is still waiting for too. Shared by every clone of the task, so
/// a supervisor restart, even one during the cycle's wait, still sees them
/// and does not start a second report for the same window.
#[derive(Clone, Default)]
struct RunningReports(Arc<Mutex<BTreeMap<PnlWindowKey, WindowReport>>>);

impl RunningReports {
    fn contains(&self, window: PnlWindowKey) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&window)
    }

    fn insert(&self, window: PnlWindowKey, report: WindowReport) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(window, report);
    }

    fn remove(&self, window: PnlWindowKey) -> Option<WindowReport> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&window)
    }

    /// Removes and returns every report that has ended.
    fn take_finished(&self) -> Vec<(PnlWindowKey, WindowReport)> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extract_if(.., |_, report| report.is_finished())
            .collect()
    }
}

/// Every 5 minutes: refreshes each PnL window family.
#[derive(Clone)]
pub(crate) struct LiqPnlRefresh<Reports> {
    reports: Reports,
    families: &'static LiqFamilies,
    dates: Option<FillDates>,
    /// Where the next cycle starts in [`PnlWindowKey::REFRESH_ORDER`].
    first_window: usize,
    running: RunningReports,
}

impl LiqPnlRefresh<LivePnlReports> {
    /// The task as the bot runs it: live reports into the process-wide
    /// store.
    pub(crate) fn live(ctx: &Ctx, pool: SqlitePool, ledger: Arc<PnlLedger>) -> Self {
        Self::new(LivePnlReports::new(ctx, pool, ledger), &LIQ_FAMILIES)
    }
}

impl<Reports: PnlReports> LiqPnlRefresh<Reports> {
    pub(crate) fn new(reports: Reports, families: &'static LiqFamilies) -> Self {
        Self {
            reports,
            families,
            dates: None,
            first_window: 0,
            running: RunningReports::default(),
        }
    }

    async fn refresh_once(&mut self) {
        for (window, report) in self.running.take_finished() {
            self.remember_window(window, report.await);
        }

        let dates = match self.dates {
            Some(dates) => dates,
            None => match self.probe().await {
                Some(dates) => dates,
                None => return,
            },
        };

        let order = cycle_order(self.first_window);
        self.first_window = (self.first_window + 1) % order.len();

        let budget_end = Instant::now() + PNL_WINDOW_BUDGET;
        for window in order {
            if self.running.contains(window) {
                warn!(
                    window = window.label(),
                    "Kept the last liq_ PnL window: its previous report is still running"
                );
                continue;
            }

            if Instant::now() >= budget_end {
                warn!(
                    window = window.label(),
                    budget = ?PNL_WINDOW_BUDGET,
                    "Kept the last liq_ PnL window: the cycle's budget is spent"
                );
                continue;
            }

            self.start_window(window, dates, budget_end).await;
        }
    }

    /// One report with no dates, only to learn the range of days with
    /// fills; its figures are not any window's.
    async fn probe(&mut self) -> Option<FillDates> {
        let query = PnlQuery {
            limit: Some(1),
            ..PnlQuery::default()
        };

        match self.reports.report(query).await {
            Ok(report) => {
                self.remember_dates(&report);
                if self.dates.is_none() {
                    info!("No PnL fills yet; no liq_ PnL window to publish");
                }
                self.dates
            }
            Err(error) => {
                warn!(%error, "PnL range probe failed; no liq_ PnL window this cycle");
                None
            }
        }
    }

    /// Starts `window`'s report and waits for it until its deadline or the
    /// end of the budget, whichever comes first. The report is in `running`
    /// before the first wait, and one still running at the deadline stays
    /// there, not cancelled.
    async fn start_window(&mut self, window: PnlWindowKey, dates: FillDates, budget_end: Instant) {
        let Some((from, to)) = window_dates(window, dates) else {
            warn!(
                window = window.label(),
                ?dates,
                "Kept the last liq_ PnL window: no date range"
            );
            return;
        };
        let query = PnlQuery {
            limit: Some(1),
            from_date: Some(from.to_string()),
            to_date: Some(to.to_string()),
            ..PnlQuery::default()
        };

        let (done, ended) = oneshot::channel();
        let reports = self.reports.clone();
        let families = self.families;
        let report = tokio::spawn(async move {
            let dates = refresh_window(reports, families, window, query).await;
            // Nobody listens once the cycle stopped waiting.
            let _ = done.send(());
            dates
        });
        self.running.insert(window, report);
        let deadline = budget_end.min(Instant::now() + PNL_WINDOW_DEADLINE);

        // A report that panics drops `done`, which also ends the wait; its
        // join error is logged below.
        match tokio::time::timeout_at(deadline, ended).await {
            Ok(_sent_or_dropped) => {
                if let Some(report) = self.running.remove(window) {
                    self.remember_window(window, report.await);
                }
            }
            Err(_elapsed) => {
                warn!(
                    window = window.label(),
                    deadline = ?PNL_WINDOW_DEADLINE,
                    "A liq_ PnL window report is past its deadline; it keeps running and \
                     publishes its window when it ends"
                );
            }
        }
    }

    /// Takes the fill dates a window's report carried. The report already
    /// logged its own failure, so only a task that ended without a result
    /// (a panic) is logged here.
    fn remember_window(
        &mut self,
        window: PnlWindowKey,
        joined: Result<Option<FillDates>, JoinError>,
    ) {
        match joined {
            Ok(Some(dates)) => self.remember(dates),
            Ok(None) => {}
            Err(error) => {
                warn!(
                    window = window.label(),
                    %error,
                    "Kept the last liq_ PnL window: its report task ended without a result"
                );
            }
        }
    }

    /// Every report refreshes the range for the next cycle.
    fn remember_dates(&mut self, report: &PnlResponse) {
        if let Some(dates) = FillDates::of(report) {
            self.remember(dates);
        }
    }

    /// Merges `dates` into the known range: the earliest first day and the
    /// latest last day. A report that missed its deadline is collected after
    /// newer reports and carries the range it read earlier, so it must not
    /// move either end back.
    fn remember(&mut self, dates: FillDates) {
        self.dates = Some(self.dates.map_or(dates, |known| FillDates {
            first: known.first.min(dates.first),
            last: known.last.max(dates.last),
        }));
    }
}

/// One window's report, samples and publish. Runs as its own task, so the
/// cycle can stop waiting for it without cancelling it. Returns the fill
/// dates the report carried, for the next cycle.
async fn refresh_window<Reports: PnlReports>(
    reports: Reports,
    families: &'static LiqFamilies,
    window: PnlWindowKey,
    query: PnlQuery,
) -> Option<FillDates> {
    // Every attempt is timed, a failed one too, so a slow window that keeps
    // failing still shows in the duration's tail. The samples are inside
    // the timing, as they cost real time on the long windows, and so is the
    // wait for the task's permit behind the reports queued ahead.
    let started = Instant::now();
    let dates = match reports.report(query).await {
        Ok(report) => {
            let dates = FillDates::of(&report);
            publish_window(families, window, report).await;
            dates
        }
        Err(error) => {
            warn!(window = window.label(), %error, "Kept the last liq_ PnL window: report failed");
            None
        }
    };
    histogram!("metrics_refresh_duration_seconds", "collector" => window.collector())
        .record(started.elapsed().as_secs_f64());

    dates
}

/// Builds `report`'s samples on a blocking thread and replaces the window's
/// family with them. A failure keeps the window's last samples.
async fn publish_window(families: &'static LiqFamilies, window: PnlWindowKey, report: PnlResponse) {
    let built = tokio::task::spawn_blocking(move || pnl_samples(&report, window)).await;

    match built {
        Ok(Ok(samples)) => {
            families.replace(LiqFamily::Pnl(window), samples, SystemTime::now());
        }
        Ok(Err(error)) => {
            warn!(window = window.label(), %error, "Kept the last liq_ PnL window: samples failed");
        }
        Err(error) => {
            warn!(
                window = window.label(),
                %error,
                "Kept the last liq_ PnL window: the samples task ended without a result"
            );
        }
    }
}

impl<Reports: PnlReports> SupervisedTask for LiqPnlRefresh<Reports> {
    async fn run(&mut self) -> TaskResult {
        info!("liq_ PnL refresh started");

        let mut interval = tokio::time::interval(PNL_REFRESH_PERIOD);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            self.refresh_once().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::liquidity::inventory::tests::{leaked_families, rendered_store};
    use crate::metrics::liquidity::pnl::tests::{day_bucket, day_symbol, pnl_fixture, report};
    use crate::metrics::liquidity::tests::series;
    use crate::metrics::liquidity::{LiqMetric, LiqSample};

    /// The `(fromDate, toDate)` of each report the task asked for.
    type Calls = Arc<Mutex<Vec<(Option<String>, Option<String>)>>>;

    /// Builds the report for a query's `fromDate`.
    type Answer = dyn Fn(Option<&str>) -> Result<PnlResponse, PnlReportError> + Send + Sync;

    /// Answers every report with `answer(fromDate)` after `delay`, or after
    /// the `slow` delay for the report whose `fromDate` it names. Like the
    /// live source, a report holds the one permit of a queued admission
    /// while it runs, so reports run one at a time in the order they asked.
    #[derive(Clone)]
    struct FakeReports {
        calls: Calls,
        delay: Duration,
        slow: Option<(&'static str, Duration)>,
        answer: Arc<Answer>,
        admission: PnlReportAdmission,
    }

    impl FakeReports {
        fn new(
            delay: Duration,
            answer: impl Fn(Option<&str>) -> Result<PnlResponse, PnlReportError> + Send + Sync + 'static,
        ) -> Self {
            Self {
                calls: Arc::default(),
                delay,
                slow: None,
                answer: Arc::new(answer),
                admission: PnlReportAdmission::queued(PNL_METRICS_REPORTS),
            }
        }

        fn calls(&self) -> Vec<(Option<String>, Option<String>)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl PnlReports for FakeReports {
        async fn report(&self, query: PnlQuery) -> Result<PnlResponse, PnlReportError> {
            assert_eq!(query.limit, Some(1));
            self.calls
                .lock()
                .unwrap()
                .push((query.from_date.clone(), query.to_date.clone()));
            let _permit = self.admission.admit().await.unwrap();
            let delay = match self.slow {
                Some((from, delay)) if query.from_date.as_deref() == Some(from) => delay,
                _ => self.delay,
            };
            tokio::time::sleep(delay).await;
            (self.answer)(query.from_date.as_deref())
        }
    }

    fn day(text: &str) -> NaiveDate {
        parse_day(text).unwrap()
    }

    fn dates(first: &str, last: &str) -> FillDates {
        FillDates {
            first: day(first),
            last: day(last),
        }
    }

    fn query_dates(from: &str, to: &str) -> (Option<String>, Option<String>) {
        (Some(from.to_string()), Some(to.to_string()))
    }

    fn collector(families: &'static LiqFamilies, window: PnlWindowKey) -> Option<f64> {
        rendered_store(families)
            .get(&series(
                "liq_collector_last_success_ts_seconds",
                &[("collector", window.collector())],
            ))
            .copied()
    }

    fn total(families: &'static LiqFamilies, window: PnlWindowKey) -> Option<f64> {
        rendered_store(families)
            .get(&series(
                "liq_pnl_summary_usd",
                &[("window", window.label()), ("stream", "total")],
            ))
            .copied()
    }

    /// A stored window family worth 99 published at Unix time 100, so a
    /// test can tell a kept window from a refreshed one.
    fn seed_window(families: &'static LiqFamilies, window: PnlWindowKey) {
        families.replace(
            LiqFamily::Pnl(window),
            vec![
                LiqSample::new(
                    LiqMetric::PnlSummaryUsd,
                    vec![
                        ("stream", "total".to_string()),
                        ("window", window.label().to_string()),
                    ],
                    99.0,
                )
                .unwrap(),
            ],
            SystemTime::UNIX_EPOCH + Duration::from_secs(100),
        );
    }

    #[test]
    fn windows_end_on_the_last_fill_day_and_start_no_earlier_than_the_first() {
        let windows = |fill_dates: FillDates| -> BTreeMap<&str, (NaiveDate, NaiveDate)> {
            PnlWindowKey::REFRESH_ORDER
                .into_iter()
                .map(|window| (window.label(), window_dates(window, fill_dates).unwrap()))
                .collect()
        };

        let anchor = day("2026-03-03");
        assert_eq!(
            windows(dates("2026-02-20", "2026-03-03")),
            BTreeMap::from([
                ("1d", (anchor, anchor)),
                ("1w", (day("2026-02-25"), anchor)),
                ("1m", (day("2026-02-20"), anchor)),
                ("ytd", (day("2026-02-20"), anchor)),
                ("1y", (day("2026-02-20"), anchor)),
                ("all", (day("2026-02-20"), anchor)),
            ])
        );
        assert_eq!(
            windows(dates("2024-06-01", "2026-03-03")),
            BTreeMap::from([
                ("1d", (anchor, anchor)),
                ("1w", (day("2026-02-25"), anchor)),
                ("1m", (day("2026-02-01"), anchor)),
                ("ytd", (day("2026-01-01"), anchor)),
                ("1y", (day("2025-03-04"), anchor)),
                ("all", (day("2024-06-01"), anchor)),
            ])
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_cold_cycle_probes_once_then_reports_each_window_in_order() {
        let families = leaked_families();
        let reports = FakeReports::new(Duration::from_secs(1), |_| Ok(pnl_fixture()));
        let mut task = LiqPnlRefresh::new(reports.clone(), families);

        task.refresh_once().await;

        let to = "2026-03-03";
        assert_eq!(
            reports.calls(),
            [
                (None, None),
                query_dates("2026-02-25", to),
                query_dates(to, to),
                query_dates("2026-02-20", to),
                query_dates("2026-02-20", to),
                query_dates("2026-02-20", to),
                query_dates("2026-02-20", to),
            ]
        );
        for window in PnlWindowKey::REFRESH_ORDER {
            assert_eq!(total(families, window), Some(11.5), "{window:?}");
        }

        task.refresh_once().await;
        assert_eq!(reports.calls().len(), 13, "a warm cycle skips the probe");
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_window_keeps_its_samples_and_timestamp_while_the_others_refresh() {
        let families = leaked_families();
        seed_window(families, PnlWindowKey::OneDay);
        let reports = FakeReports::new(Duration::ZERO, |from| {
            if from == Some("2026-03-03") {
                Err(PnlReportError::CatchUpTimeout)
            } else {
                Ok(pnl_fixture())
            }
        });
        let mut task = LiqPnlRefresh::new(reports, families);
        task.dates = Some(dates("2026-02-20", "2026-03-03"));

        task.refresh_once().await;

        assert_eq!(total(families, PnlWindowKey::OneDay), Some(99.0));
        assert_eq!(collector(families, PnlWindowKey::OneDay), Some(100.0));
        assert_eq!(total(families, PnlWindowKey::OneWeek), Some(11.5));
        assert!(collector(families, PnlWindowKey::OneWeek) > Some(100.0));
    }

    /// A report whose fill range is wide enough that every window asks for
    /// its own `fromDate`, so the calls name the windows.
    fn wide_range_report() -> PnlResponse {
        report((Some("2024-06-01"), Some("2026-03-03")), Vec::new())
    }

    /// The `fromDate` each window asks for under [`wide_range_report`].
    fn wide_range_from(window: PnlWindowKey) -> &'static str {
        match window {
            PnlWindowKey::OneDay => "2026-03-03",
            PnlWindowKey::OneWeek => "2026-02-25",
            PnlWindowKey::OneMonth => "2026-02-01",
            PnlWindowKey::YearToDate => "2026-01-01",
            PnlWindowKey::OneYear => "2025-03-04",
            PnlWindowKey::All => "2024-06-01",
        }
    }

    fn froms(calls: &[(Option<String>, Option<String>)]) -> Vec<&str> {
        calls
            .iter()
            .map(|(from, _)| from.as_deref().unwrap())
            .collect()
    }

    fn refreshed(families: &'static LiqFamilies, window: PnlWindowKey) -> bool {
        collector(families, window) > Some(100.0)
    }

    #[test]
    fn each_cycle_order_starts_one_window_later_and_wraps() {
        assert_eq!(cycle_order(0), PnlWindowKey::REFRESH_ORDER);
        assert_eq!(
            cycle_order(1),
            [
                PnlWindowKey::OneDay,
                PnlWindowKey::All,
                PnlWindowKey::OneMonth,
                PnlWindowKey::YearToDate,
                PnlWindowKey::OneYear,
                PnlWindowKey::OneWeek,
            ]
        );
        assert_eq!(
            cycle_order(5),
            [
                PnlWindowKey::OneYear,
                PnlWindowKey::OneWeek,
                PnlWindowKey::OneDay,
                PnlWindowKey::All,
                PnlWindowKey::OneMonth,
                PnlWindowKey::YearToDate,
            ]
        );
        assert_eq!(cycle_order(6), PnlWindowKey::REFRESH_ORDER);
    }

    #[tokio::test(start_paused = true)]
    async fn each_cycle_starts_one_window_later() {
        let families = leaked_families();
        let reports = FakeReports::new(Duration::ZERO, |_| Ok(wide_range_report()));
        let mut task = LiqPnlRefresh::new(reports.clone(), families);
        task.dates = Some(dates("2024-06-01", "2026-03-03"));

        task.refresh_once().await;
        task.refresh_once().await;

        let expected: Vec<&str> = cycle_order(0)
            .into_iter()
            .chain(cycle_order(1))
            .map(wide_range_from)
            .collect();
        assert_eq!(froms(&reports.calls()), expected);
    }

    #[tokio::test(start_paused = true)]
    async fn windows_past_the_budget_keep_their_samples_and_the_skipped_ones_rotate() {
        let families = leaked_families();
        for window in PnlWindowKey::REFRESH_ORDER {
            seed_window(families, window);
        }
        let reports = FakeReports::new(Duration::from_secs(50), |_| Ok(wide_range_report()));
        let mut task = LiqPnlRefresh::new(reports.clone(), families);
        task.dates = Some(dates("2024-06-01", "2026-03-03"));

        task.refresh_once().await;

        // Each report takes 50 s and the cycle waits 30 s for each, so 1w,
        // 1d, all and 1m start at 0 s, 30 s, 60 s and 90 s; ytd would start
        // at 120 s, the end of the budget. The reports run one at a time
        // behind the one permit, so they end at 50 s, 100 s, 150 s and 200 s.
        let first_cycle: Vec<&str> = [
            PnlWindowKey::OneWeek,
            PnlWindowKey::OneDay,
            PnlWindowKey::All,
            PnlWindowKey::OneMonth,
        ]
        .map(wide_range_from)
        .to_vec();
        assert_eq!(froms(&reports.calls()), first_cycle);

        // At 141 s only the first two have published.
        tokio::time::sleep(Duration::from_secs(21)).await;
        for window in PnlWindowKey::REFRESH_ORDER {
            let published = [PnlWindowKey::OneWeek, PnlWindowKey::OneDay].contains(&window);
            assert_eq!(refreshed(families, window), published, "{window:?}");
        }

        // At 201 s all four have published, and the skipped ones kept theirs.
        tokio::time::sleep(Duration::from_secs(60)).await;
        for window in PnlWindowKey::REFRESH_ORDER {
            let skipped = [PnlWindowKey::YearToDate, PnlWindowKey::OneYear].contains(&window);
            assert_eq!(refreshed(families, window), !skipped, "{window:?}");
        }
        assert_eq!(total(families, PnlWindowKey::OneYear), Some(99.0));

        // The next cycle starts at 1d, so ytd starts this time and 1y and 1w
        // are the ones past the budget.
        task.refresh_once().await;
        let second_cycle: Vec<&str> = [
            PnlWindowKey::OneDay,
            PnlWindowKey::All,
            PnlWindowKey::OneMonth,
            PnlWindowKey::YearToDate,
        ]
        .map(wide_range_from)
        .to_vec();
        assert_eq!(froms(&reports.calls()[4..]), second_cycle);
    }

    /// The deadline frees the cycle, not the reports: the windows started
    /// after a slow one wait for its permit and publish only after it.
    #[tokio::test(start_paused = true)]
    async fn a_slow_window_does_not_hold_the_cycle_but_the_windows_behind_it_wait_for_it() {
        let families = leaked_families();
        seed_window(families, PnlWindowKey::OneWeek);
        let reports = FakeReports {
            slow: Some(("2026-02-25", Duration::from_secs(100))),
            ..FakeReports::new(Duration::from_secs(1), |_| Ok(pnl_fixture()))
        };
        let mut task = LiqPnlRefresh::new(reports.clone(), families);
        task.dates = Some(dates("2026-02-20", "2026-03-03"));
        let started = Instant::now();

        // 1d, all and 1m start at 30 s, 60 s and 90 s, all before 1w ends.
        let before_one_week_ends = async {
            tokio::time::sleep(Duration::from_secs(99)).await;
            assert_eq!(reports.calls().len(), 4);
            for window in PnlWindowKey::REFRESH_ORDER {
                let kept = (window == PnlWindowKey::OneWeek).then_some(99.0);
                assert_eq!(total(families, window), kept, "{window:?}");
            }
        };
        tokio::join!(task.refresh_once(), before_one_week_ends);

        // 1w ends at 100 s, then the queued 1d, all and 1m take 1 s each;
        // 1m ends inside its wait, so ytd and 1y follow at 103 s.
        assert_eq!(started.elapsed(), Duration::from_secs(105));
        assert_eq!(reports.calls().len(), 6);
        for window in PnlWindowKey::REFRESH_ORDER {
            assert_eq!(total(families, window), Some(11.5), "{window:?}");
        }
        assert!(refreshed(families, PnlWindowKey::OneWeek));
    }

    #[tokio::test(start_paused = true)]
    async fn a_window_whose_report_is_still_running_starts_no_second_report() {
        let families = leaked_families();
        let reports = FakeReports {
            slow: Some(("2026-02-25", Duration::from_secs(400))),
            ..FakeReports::new(Duration::ZERO, |_| Ok(wide_range_report()))
        };
        let mut task = LiqPnlRefresh::new(reports.clone(), families);
        task.dates = Some(dates("2024-06-01", "2026-03-03"));
        let one_week_reports = |reports: &FakeReports| {
            froms(&reports.calls())
                .into_iter()
                .filter(|from| *from == "2026-02-25")
                .count()
        };

        // 1w holds the permit until 400 s, so 1d, all and 1m, started at
        // 30 s, 60 s and 90 s, queue behind it, and the cycle ends at 120 s.
        task.refresh_once().await;
        assert_eq!(reports.calls().len(), 4);

        // At 320 s 1d, all and 1m are still queued and are skipped as well;
        // ytd and 1y queue behind them, and the cycle ends at 380 s.
        tokio::time::sleep(Duration::from_secs(200)).await;
        task.refresh_once().await;

        assert_eq!(one_week_reports(&reports), 1);
        assert_eq!(reports.calls().len(), 6);
        assert!(!refreshed(families, PnlWindowKey::OneWeek));

        // The first report ends at 400 s; the next cycle starts a new one.
        tokio::time::sleep(Duration::from_secs(200)).await;
        assert!(refreshed(families, PnlWindowKey::OneWeek));
        task.refresh_once().await;
        assert_eq!(one_week_reports(&reports), 2);
    }

    /// A supervisor restart drops the cycle while it waits for a report;
    /// the restarted task still sees that report and starts no second one
    /// for its window.
    #[tokio::test(start_paused = true)]
    async fn a_restart_during_the_wait_still_sees_the_running_report() {
        let families = leaked_families();
        let reports = FakeReports {
            slow: Some(("2026-02-25", Duration::from_secs(400))),
            ..FakeReports::new(Duration::ZERO, |_| Ok(wide_range_report()))
        };
        let mut task = LiqPnlRefresh::new(reports.clone(), families);
        task.dates = Some(dates("2024-06-01", "2026-03-03"));
        let mut restarted = task.clone();

        let dropped = tokio::time::timeout(Duration::from_secs(10), task.refresh_once()).await;
        assert!(dropped.is_err());
        restarted.refresh_once().await;

        let one_week_reports = froms(&reports.calls())
            .into_iter()
            .filter(|from| *from == "2026-02-25")
            .count();
        assert_eq!(one_week_reports, 1);
    }

    /// A report that misses its deadline still moves the next cycle's range
    /// once it ends, and a report with an earlier last day does not move it
    /// back.
    #[tokio::test(start_paused = true)]
    async fn a_late_report_moves_the_range_once_it_ends() {
        let families = leaked_families();
        let reports = FakeReports {
            slow: Some(("2026-02-25", Duration::from_secs(100))),
            ..FakeReports::new(Duration::ZERO, |from| {
                if from == Some("2026-02-25") {
                    Ok(report((Some("2026-02-20"), Some("2026-03-04")), Vec::new()))
                } else {
                    Ok(pnl_fixture())
                }
            })
        };
        let mut task = LiqPnlRefresh::new(reports.clone(), families);
        task.dates = Some(dates("2026-02-20", "2026-03-03"));

        // 1w ends at 100 s, past its deadline; the windows queued behind it
        // end then too, inside the cycle, and keep the range on the 3rd.
        task.refresh_once().await;
        assert_eq!(task.dates, Some(dates("2026-02-20", "2026-03-03")));

        tokio::time::sleep(Duration::from_secs(100)).await;
        task.refresh_once().await;

        // The second cycle's reports end on the 3rd, an earlier day than
        // the late report's, so they do not move the range back.
        assert_eq!(task.dates, Some(dates("2026-02-20", "2026-03-04")));
        assert_eq!(
            reports.calls()[6],
            query_dates("2026-03-04", "2026-03-04"),
            "the second cycle starts at 1d with the late report's range"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn without_fills_nothing_is_published_and_the_next_cycle_probes_again() {
        let families = leaked_families();
        let reports = FakeReports::new(Duration::ZERO, |_| Ok(report((None, None), Vec::new())));
        let mut task = LiqPnlRefresh::new(reports.clone(), families);

        task.refresh_once().await;
        task.refresh_once().await;

        assert_eq!(reports.calls(), [(None, None), (None, None)]);
        assert_eq!(rendered_store(families), BTreeMap::new());
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_probe_publishes_nothing() {
        let families = leaked_families();
        let reports = FakeReports::new(Duration::ZERO, |_| Err(PnlReportError::CatchUpTimeout));
        let mut task = LiqPnlRefresh::new(reports.clone(), families);

        task.refresh_once().await;

        assert_eq!(reports.calls(), [(None, None)]);
        assert_eq!(task.dates, None);
        assert_eq!(rendered_store(families), BTreeMap::new());
    }

    /// Each report refreshes the range, so a new fill day moves every window
    /// of the next cycle.
    #[tokio::test(start_paused = true)]
    async fn a_report_with_a_later_fill_day_moves_the_next_cycle() {
        let families = leaked_families();
        let later = report(
            (Some("2026-02-20"), Some("2026-03-04")),
            vec![day_bucket(
                "2026-03-04",
                vec![day_symbol("RKLB", ["1", "0", "0", "0", "0", "1"])],
            )],
        );
        let reports = FakeReports::new(Duration::ZERO, move |_| Ok(later.clone()));
        let mut task = LiqPnlRefresh::new(reports.clone(), families);
        task.dates = Some(dates("2026-02-20", "2026-03-03"));

        task.refresh_once().await;
        assert_eq!(task.dates, Some(dates("2026-02-20", "2026-03-04")));

        task.refresh_once().await;
        // The second cycle starts one window later: 1d, then all.
        assert_eq!(
            reports.calls()[6..8],
            [
                query_dates("2026-03-04", "2026-03-04"),
                query_dates("2026-02-20", "2026-03-04"),
            ]
        );
    }

    /// A late report with the same last day but a later first day, as one
    /// that read the ledger before older history was ingested would carry,
    /// does not move the first day forward; one with an earlier last day
    /// does not move the last day back.
    #[test]
    fn remembered_ranges_merge_to_the_earliest_first_and_latest_last_day() {
        let families = leaked_families();
        let reports = FakeReports::new(Duration::ZERO, |_| Ok(pnl_fixture()));
        let mut task = LiqPnlRefresh::new(reports, families);
        task.dates = Some(dates("2026-02-19", "2026-03-03"));

        task.remember(dates("2026-02-20", "2026-03-03"));
        assert_eq!(task.dates, Some(dates("2026-02-19", "2026-03-03")));

        task.remember(dates("2026-02-20", "2026-03-02"));
        assert_eq!(task.dates, Some(dates("2026-02-19", "2026-03-03")));

        task.remember(dates("2026-02-18", "2026-03-04"));
        assert_eq!(task.dates, Some(dates("2026-02-18", "2026-03-04")));
    }

    #[test]
    fn an_unparseable_range_is_not_remembered() {
        assert_eq!(
            FillDates::of(&report(
                (Some("2026-02-30"), Some("2026-03-03")),
                Vec::new()
            )),
            None
        );
        assert_eq!(
            FillDates::of(&report((Some("2026-02-20"), None), Vec::new())),
            None
        );
    }
}
