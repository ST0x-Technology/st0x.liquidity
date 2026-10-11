//! `liq-pnl-refresh`: every 5 minutes, one PnL report per window through
//! the same code as `GET /pnl`, published as that window's family.
//!
//! The windows copy the exporter sidecar. Every window ends on the last day
//! with fills (`availableRange.lastDate`), and every start is clamped to the
//! first day with fills. Both days come from the cycle's own replay, so a
//! fill on a new day moves every window in the cycle that replays it. The
//! exporter took them from an earlier report, so its windows lagged a new
//! fill day by one cycle.
//!
//! The six windows share one ledger catch-up and one replay (see
//! [`run_pnl_window_reports`]): the replay reads no date, so the window
//! dates are picked from its range after it ends, and only the summary, the
//! Alpaca activities and the capital are built per window. The
//! exporter made six `/pnl` calls, so six replays, per cycle. The windows
//! publish together after the shared report has built all six, so one
//! window's slow Alpaca activities fetch delays every window. Each window's
//! `metrics_refresh_duration_seconds` sample runs from the start of the
//! shared report to that window's publish, so later windows in
//! [`PnlWindowKey::REFRESH_ORDER`] always show a longer duration.
//!
//! The task has its own one-permit admission, so it never takes a live
//! `/pnl` permit. A report waits for that permit instead of failing, and
//! takes it before its ledger catch-up, so the head it replays to is read
//! after the wait.
//!
//! The cycle's report runs as its own task. The cycle waits for it until
//! the budget ([`PNL_CYCLE_BUDGET`]) and then moves on without cancelling
//! it: the replay runs on a blocking thread that cannot be stopped, and the
//! report it would cut off is valid work. A report past the budget
//! publishes its windows when it ends, and no cycle starts a new report
//! until then, so two reports never race to publish one window and the
//! replay work never exceeds one report at a time. Every
//! report is bounded anyway: the catch-up has its own deadline, the Alpaca
//! calls have HTTP timeouts, and the replay is finite.
//!
//! A window whose report fails (including a ledger catch-up past its
//! deadline, which fails every window) or whose cycle's report is still
//! running keeps its last samples. The exporter drops such a window until
//! its next cycle; keeping it means a reader in that gap sees the last
//! value, and the window's `liq_collector_last_success_ts_seconds` shows its
//! age.
//!
//! The samples are built on a blocking thread too: the long windows cost
//! tens of thousands of `Float` operations, which would hold an async worker
//! for up to about a second.

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
    PnlAvailableRange, PnlLedger, PnlQuery, PnlReportAdmission, PnlReportDeps, PnlReportError,
    PnlResponse, PnlWindowReports, run_pnl_window_reports,
};

/// The exporter's PnL cadence.
const PNL_REFRESH_PERIOD: Duration = Duration::from_secs(300);

/// How long a cycle waits for its report. A report still running then is
/// not cancelled: it publishes its windows when it ends.
const PNL_CYCLE_BUDGET: Duration = Duration::from_secs(120);

/// Reports the task runs at once: one, so the live `/pnl` permits stay free.
const PNL_METRICS_REPORTS: usize = 1;

/// Where the task gets its PnL reports. The live source is
/// [`LivePnlReports`]; tests count and fail calls.
pub(crate) trait PnlReports: Clone + Send + Sync + 'static {
    /// One report per window [`cycle_windows`] picks from the shared
    /// replay's range, one entry per window, in refresh order. Empty when
    /// there are no fills. The outer error fails every window.
    fn window_reports(
        &self,
    ) -> impl Future<Output = Result<PnlWindowReports<PnlWindowKey>, PnlReportError>> + Send;
}

/// Reports through [`run_pnl_window_reports`] with the task's own admission.
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

    fn deps(&self) -> PnlReportDeps<'_> {
        PnlReportDeps {
            pool: &self.pool,
            ledger: &self.ledger,
            broker: &self.broker,
        }
    }
}

impl PnlReports for LivePnlReports {
    async fn window_reports(&self) -> Result<PnlWindowReports<PnlWindowKey>, PnlReportError> {
        let base = PnlQuery {
            limit: Some(1),
            ..PnlQuery::default()
        };

        run_pnl_window_reports(&self.deps(), &base, cycle_windows, &self.admission).await
    }
}

/// Each window's `(fromDate, toDate)` over a replay's range of days with
/// fills, in refresh order. Empty when there are no fills.
pub(crate) fn cycle_windows(
    range: &PnlAvailableRange,
) -> Vec<(PnlWindowKey, NaiveDate, NaiveDate)> {
    let Some(dates) = FillDates::of(range) else {
        info!("No PnL fills yet; no liq_ PnL window to publish");
        return Vec::new();
    };

    PnlWindowKey::REFRESH_ORDER
        .into_iter()
        .filter_map(|window| {
            let Some((from, to)) = window_dates(window, dates) else {
                warn!(
                    window = window.label(),
                    ?dates,
                    "Kept the last liq_ PnL window: no date range"
                );
                return None;
            };
            Some((window, from, to))
        })
        .collect()
}

/// The first and last days with fills, from a replay's `availableRange`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FillDates {
    first: NaiveDate,
    last: NaiveDate,
}

impl FillDates {
    /// `None` when the range has no fills. Unparseable dates are logged
    /// and treated the same way.
    fn of(range: &PnlAvailableRange) -> Option<Self> {
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

/// A cycle's running report.
type CycleReport = JoinHandle<()>;

/// The cycle's report from its start until the task collects it, a report
/// the cycle is still waiting for too. Shared by every clone of the task, so
/// a supervisor restart, even one during the cycle's wait, still sees it and
/// does not start a second report beside it.
#[derive(Clone, Default)]
struct RunningReport(Arc<Mutex<Option<CycleReport>>>);

impl RunningReport {
    fn is_running(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    fn set(&self, report: CycleReport) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(report);
    }

    fn take(&self) -> Option<CycleReport> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take()
    }

    /// Removes and returns the report if it has ended.
    fn take_finished(&self) -> Option<CycleReport> {
        let mut running = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if running.as_ref().is_some_and(JoinHandle::is_finished) {
            running.take()
        } else {
            None
        }
    }
}

/// Every 5 minutes: refreshes each PnL window family.
#[derive(Clone)]
pub(crate) struct LiqPnlRefresh<Reports> {
    reports: Reports,
    families: &'static LiqFamilies,
    running: RunningReport,
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
            running: RunningReport::default(),
        }
    }

    async fn refresh_once(&self) {
        if let Some(report) = self.running.take_finished() {
            log_report_end(report.await);
        }
        if self.running.is_running() {
            warn!("Kept every liq_ PnL window: the last cycle's report is still running");
            return;
        }

        let (done, ended) = oneshot::channel();
        let reports = self.reports.clone();
        let families = self.families;
        let report = tokio::spawn(async move {
            refresh_windows(reports, families).await;
            // Nobody listens once the cycle stopped waiting.
            let _ = done.send(());
        });
        self.running.set(report);

        // A report that panics drops `done`, which also ends the wait; its
        // join error is logged when it is collected.
        match tokio::time::timeout(PNL_CYCLE_BUDGET, ended).await {
            Ok(_sent_or_dropped) => {
                if let Some(report) = self.running.take() {
                    log_report_end(report.await);
                }
            }
            Err(_elapsed) => {
                warn!(
                    budget = ?PNL_CYCLE_BUDGET,
                    "The liq_ PnL report is past the cycle's budget; it keeps running and \
                     publishes its windows when it ends"
                );
            }
        }
    }
}

/// The report already logged its own failures, so only a task that ended
/// without a result (a panic) is logged here.
fn log_report_end(joined: Result<(), JoinError>) {
    if let Err(error) = joined {
        warn!(%error, "Kept every liq_ PnL window: the report task ended without a result");
    }
}

/// One cycle's shared report, then each window's samples and publish. Runs
/// as its own task, so the cycle can stop waiting for it without cancelling
/// it.
async fn refresh_windows<Reports: PnlReports>(reports: Reports, families: &'static LiqFamilies) {
    // Every window is timed from the start of the shared report to its
    // publish, a failed one too, so a slow report that keeps failing still
    // shows in the duration's tail.
    let started = Instant::now();
    let record = |window: PnlWindowKey| {
        histogram!("metrics_refresh_duration_seconds", "collector" => window.collector())
            .record(started.elapsed().as_secs_f64());
    };

    let window_reports = match reports.window_reports().await {
        Ok(window_reports) => window_reports,
        Err(error) => {
            warn!(%error, "Kept every liq_ PnL window: the shared report failed");
            for window in PnlWindowKey::REFRESH_ORDER {
                record(window);
            }
            return;
        }
    };

    for (window, report) in window_reports {
        match report {
            Ok(report) => publish_window(families, window, report).await,
            Err(error) => {
                warn!(window = window.label(), %error, "Kept the last liq_ PnL window: report failed");
            }
        }
        record(window);
    }
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
    use std::collections::BTreeMap;

    use super::*;
    use crate::metrics::liquidity::inventory::tests::{leaked_families, rendered_store};
    use crate::metrics::liquidity::pnl::tests::{pnl_fixture, report};
    use crate::metrics::liquidity::tests::series;
    use crate::metrics::liquidity::{LiqMetric, LiqSample};

    /// The `(fromDate, toDate)` of each window report the task got.
    type Calls = Arc<Mutex<Vec<(String, String)>>>;

    /// Builds the report for a window's `fromDate`.
    type Answer = dyn Fn(&str) -> Result<PnlResponse, PnlReportError> + Send + Sync;

    /// Picks each cycle's windows from `range` with [`cycle_windows`], as
    /// the live source does from its replay, and answers each with
    /// `answer(fromDate)`. A cycle's window reports take one `delay`
    /// together, like one shared replay, and record one call per window.
    /// With `fail_windows` the shared report fails as a whole.
    #[derive(Clone)]
    struct FakeReports {
        calls: Calls,
        delay: Duration,
        fail_windows: bool,
        range: Arc<Mutex<PnlAvailableRange>>,
        answer: Arc<Answer>,
    }

    impl FakeReports {
        fn new(
            delay: Duration,
            answer: impl Fn(&str) -> Result<PnlResponse, PnlReportError> + Send + Sync + 'static,
        ) -> Self {
            Self {
                calls: Arc::default(),
                delay,
                fail_windows: false,
                range: Arc::new(Mutex::new(pnl_fixture().available_range)),
                answer: Arc::new(answer),
            }
        }

        fn calls(&self) -> Vec<(String, String)> {
            self.calls.lock().unwrap().clone()
        }

        fn set_range(&self, first: Option<&str>, last: Option<&str>) {
            *self.range.lock().unwrap() = report((first, last), Vec::new()).available_range;
        }
    }

    impl PnlReports for FakeReports {
        async fn window_reports(&self) -> Result<PnlWindowReports<PnlWindowKey>, PnlReportError> {
            let range = self.range.lock().unwrap().clone();
            let windows = cycle_windows(&range);
            self.calls.lock().unwrap().extend(
                windows
                    .iter()
                    .map(|(_, from, to)| (from.to_string(), to.to_string())),
            );
            tokio::time::sleep(self.delay).await;
            if self.fail_windows {
                return Err(PnlReportError::CatchUpTimeout);
            }

            Ok(windows
                .into_iter()
                .map(|(window, from, _)| (window, (self.answer)(&from.to_string())))
                .collect())
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

    fn query_dates(from: &str, to: &str) -> (String, String) {
        (from.to_string(), to.to_string())
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

    #[test]
    fn cycle_windows_follow_the_refresh_order() {
        let range = pnl_fixture().available_range;

        let windows: Vec<PnlWindowKey> = cycle_windows(&range)
            .into_iter()
            .map(|(window, _, _)| window)
            .collect();

        assert_eq!(windows, PnlWindowKey::REFRESH_ORDER);
    }

    #[tokio::test(start_paused = true)]
    async fn a_first_cycle_reports_each_window_in_order_without_a_probe() {
        let families = leaked_families();
        let reports = FakeReports::new(Duration::from_secs(1), |_| Ok(pnl_fixture()));
        let task = LiqPnlRefresh::new(reports.clone(), families);

        task.refresh_once().await;

        let to = "2026-03-03";
        assert_eq!(
            reports.calls(),
            [
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
        assert_eq!(reports.calls().len(), 12);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_window_keeps_its_samples_and_timestamp_while_the_others_refresh() {
        let families = leaked_families();
        seed_window(families, PnlWindowKey::OneDay);
        let reports = FakeReports::new(Duration::ZERO, |from| {
            if from == "2026-03-03" {
                Err(PnlReportError::CatchUpTimeout)
            } else {
                Ok(pnl_fixture())
            }
        });
        let task = LiqPnlRefresh::new(reports, families);

        task.refresh_once().await;

        assert_eq!(total(families, PnlWindowKey::OneDay), Some(99.0));
        assert_eq!(collector(families, PnlWindowKey::OneDay), Some(100.0));
        assert_eq!(total(families, PnlWindowKey::OneWeek), Some(11.5));
        assert!(collector(families, PnlWindowKey::OneWeek) > Some(100.0));
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_shared_report_keeps_every_window() {
        let families = leaked_families();
        for window in PnlWindowKey::REFRESH_ORDER {
            seed_window(families, window);
        }
        let reports = FakeReports {
            fail_windows: true,
            ..FakeReports::new(Duration::ZERO, |_| Ok(pnl_fixture()))
        };
        let task = LiqPnlRefresh::new(reports.clone(), families);

        task.refresh_once().await;

        assert_eq!(reports.calls().len(), 6);
        for window in PnlWindowKey::REFRESH_ORDER {
            assert_eq!(total(families, window), Some(99.0), "{window:?}");
            assert_eq!(collector(families, window), Some(100.0), "{window:?}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_report_past_the_budget_publishes_when_it_ends_and_holds_back_the_next_cycle() {
        let families = leaked_families();
        for window in PnlWindowKey::REFRESH_ORDER {
            seed_window(families, window);
        }
        let reports = FakeReports::new(Duration::from_secs(400), |_| Ok(pnl_fixture()));
        let task = LiqPnlRefresh::new(reports.clone(), families);
        let started = Instant::now();

        task.refresh_once().await;

        assert_eq!(started.elapsed(), PNL_CYCLE_BUDGET);
        assert_eq!(total(families, PnlWindowKey::OneWeek), Some(99.0));

        // At 300 s the report still runs, so the cycle starts none.
        tokio::time::sleep(Duration::from_secs(180)).await;
        task.refresh_once().await;
        assert_eq!(reports.calls().len(), 6);

        // It ends at 400 s and publishes every window; the next cycle
        // starts a new report.
        tokio::time::sleep(Duration::from_secs(101)).await;
        for window in PnlWindowKey::REFRESH_ORDER {
            assert_eq!(total(families, window), Some(11.5), "{window:?}");
        }
        task.refresh_once().await;
        assert_eq!(reports.calls().len(), 12);
    }

    /// A supervisor restart drops the cycle while it waits for its report;
    /// the restarted task still sees that report, starts no second one, and
    /// collects it once it ends.
    #[tokio::test(start_paused = true)]
    async fn a_restart_during_the_wait_still_sees_the_running_report() {
        let families = leaked_families();
        let reports = FakeReports::new(Duration::from_secs(400), |_| Ok(pnl_fixture()));
        let task = LiqPnlRefresh::new(reports.clone(), families);
        let restarted = task.clone();

        let dropped = tokio::time::timeout(Duration::from_secs(10), task.refresh_once()).await;
        assert!(dropped.is_err());
        restarted.refresh_once().await;
        assert_eq!(reports.calls().len(), 6);

        // The dropped cycle's report ends at 400 s and publishes every
        // window; the restarted task collects it and starts the next.
        tokio::time::sleep(Duration::from_secs(391)).await;
        for window in PnlWindowKey::REFRESH_ORDER {
            assert_eq!(total(families, window), Some(11.5), "{window:?}");
        }
        restarted.refresh_once().await;
        assert_eq!(reports.calls().len(), 12);
    }

    /// The windows take their dates from the cycle's own replay, so the
    /// cycle that first replays a fill on a new day already ends every
    /// window on that day.
    #[tokio::test(start_paused = true)]
    async fn a_new_fill_day_moves_the_windows_of_the_cycle_that_replays_it() {
        let families = leaked_families();
        let reports = FakeReports::new(Duration::ZERO, |_| Ok(pnl_fixture()));
        let task = LiqPnlRefresh::new(reports.clone(), families);

        task.refresh_once().await;
        reports.set_range(Some("2026-02-20"), Some("2026-03-04"));
        task.refresh_once().await;

        assert_eq!(
            reports.calls()[6..8],
            [
                query_dates("2026-02-26", "2026-03-04"),
                query_dates("2026-03-04", "2026-03-04"),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn without_fills_nothing_is_published() {
        let families = leaked_families();
        let reports = FakeReports::new(Duration::ZERO, |_| Ok(pnl_fixture()));
        reports.set_range(None, None);
        let task = LiqPnlRefresh::new(reports.clone(), families);

        task.refresh_once().await;
        task.refresh_once().await;

        assert_eq!(reports.calls(), Vec::<(String, String)>::new());
        assert_eq!(rendered_store(families), BTreeMap::new());
    }

    #[test]
    fn an_unparseable_range_has_no_windows() {
        let range = |first: Option<&str>, last: Option<&str>| {
            report((first, last), Vec::new()).available_range
        };

        assert_eq!(
            cycle_windows(&range(Some("2026-02-30"), Some("2026-03-03"))),
            Vec::new()
        );
        assert_eq!(cycle_windows(&range(Some("2026-02-20"), None)), Vec::new());
    }
}
