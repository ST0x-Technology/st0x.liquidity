//! `liq-pnl-refresh`: every 5 minutes, one PnL report per window through
//! the same path as `GET /pnl`, published as that window's family.
//!
//! The windows and their order copy the exporter sidecar. Every window ends
//! on the last day with fills (`availableRange.lastDate`), and every start is
//! clamped to the first day with fills. A cold range cache costs one probe
//! report whose figures are not published.
//!
//! The task has its own one-permit admission, so it never takes a live
//! `/pnl` permit. Each report runs its own ledger catch-up, as each of the
//! exporter's `/pnl` calls does.
//!
//! A window whose report fails (including a ledger catch-up past its
//! deadline) or does not fit in the cycle budget keeps its last samples. The
//! exporter drops such a window until its next cycle; keeping it means a
//! reader in that gap sees the last value, and the window's
//! `liq_collector_last_success_ts_seconds` shows its age.
//!
//! A report is never cancelled from here: its replay runs on a blocking
//! thread that keeps the task's permit until it ends, so a cancelled report
//! would make the next windows fail admission. Every report is bounded
//! anyway: the catch-up has its own deadline, the Alpaca calls have HTTP
//! timeouts, and the replay is finite.
//!
//! Each window replays the ledger on its own, as each of the exporter's
//! calls did: 6 replays and 6 Alpaca fetches per cycle. One replay shared by
//! the 6 windows would cost about a sixth, but needs the report split into a
//! replay step and a per-window summary step, a change to the PnL code path
//! that wants its own parity test.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chrono::{Datelike, Days, NaiveDate};
use metrics::histogram;
use sqlx::SqlitePool;
use task_supervisor::{SupervisedTask, TaskResult};
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
            admission: PnlReportAdmission::with_permits(PNL_METRICS_REPORTS),
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

/// Every 5 minutes: refreshes each PnL window family.
#[derive(Clone)]
pub(crate) struct LiqPnlRefresh<Reports> {
    reports: Reports,
    families: &'static LiqFamilies,
    dates: Option<FillDates>,
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
        }
    }

    async fn refresh_once(&mut self) {
        let dates = match self.dates {
            Some(dates) => dates,
            None => match self.probe().await {
                Some(dates) => dates,
                None => return,
            },
        };

        let deadline = Instant::now() + PNL_WINDOW_BUDGET;
        for window in PnlWindowKey::REFRESH_ORDER {
            if Instant::now() > deadline {
                warn!(
                    window = window.label(),
                    budget = ?PNL_WINDOW_BUDGET,
                    "Kept the last liq_ PnL window: the cycle's budget is spent"
                );
                continue;
            }

            self.refresh_window(window, dates).await;
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

    async fn refresh_window(&mut self, window: PnlWindowKey, dates: FillDates) {
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

        // Every attempt is timed, a failed one too, so a slow window that
        // keeps failing still shows in the duration's tail.
        let started = Instant::now();
        let outcome = self.reports.report(query).await;
        histogram!("metrics_refresh_duration_seconds", "collector" => window.collector())
            .record(started.elapsed().as_secs_f64());
        let report = match outcome {
            Ok(report) => report,
            Err(error) => {
                warn!(window = window.label(), %error, "Kept the last liq_ PnL window: report failed");
                return;
            }
        };

        self.remember_dates(&report);
        match pnl_samples(&report, window) {
            Ok(samples) => {
                self.families
                    .replace(LiqFamily::Pnl(window), samples, SystemTime::now());
            }
            Err(error) => {
                warn!(window = window.label(), %error, "Kept the last liq_ PnL window: samples failed");
            }
        }
    }

    /// Every report refreshes the range for the next cycle.
    fn remember_dates(&mut self, report: &PnlResponse) {
        if let Some(dates) = FillDates::of(report) {
            self.dates = Some(dates);
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
    use std::sync::Mutex;

    use super::*;
    use crate::metrics::liquidity::inventory::tests::{leaked_families, rendered_store};
    use crate::metrics::liquidity::pnl::tests::{day_bucket, day_symbol, pnl_fixture, report};
    use crate::metrics::liquidity::tests::series;
    use crate::metrics::liquidity::{LiqMetric, LiqSample};

    /// The `(fromDate, toDate)` of each report the task asked for.
    type Calls = Arc<Mutex<Vec<(Option<String>, Option<String>)>>>;

    /// Builds the report for a query's `fromDate`.
    type Answer = dyn Fn(Option<&str>) -> Result<PnlResponse, PnlReportError> + Send + Sync;

    /// Answers every report with `answer(fromDate)` after `delay`.
    #[derive(Clone)]
    struct FakeReports {
        calls: Calls,
        delay: Duration,
        answer: Arc<Answer>,
    }

    impl FakeReports {
        fn new(
            delay: Duration,
            answer: impl Fn(Option<&str>) -> Result<PnlResponse, PnlReportError> + Send + Sync + 'static,
        ) -> Self {
            Self {
                calls: Arc::default(),
                delay,
                answer: Arc::new(answer),
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
            tokio::time::sleep(self.delay).await;
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

    #[tokio::test(start_paused = true)]
    async fn windows_past_the_budget_keep_their_samples() {
        let families = leaked_families();
        for window in PnlWindowKey::REFRESH_ORDER {
            seed_window(families, window);
        }
        let reports = FakeReports::new(Duration::from_secs(50), |_| Ok(pnl_fixture()));
        let mut task = LiqPnlRefresh::new(reports.clone(), families);
        task.dates = Some(dates("2026-02-20", "2026-03-03"));

        task.refresh_once().await;

        // 1w, 1d and all start at 0 s, 50 s and 100 s; 1m would start at
        // 150 s, past the 120 s budget.
        assert_eq!(reports.calls().len(), 3);
        let refreshed = [
            PnlWindowKey::OneWeek,
            PnlWindowKey::OneDay,
            PnlWindowKey::All,
        ];
        for window in PnlWindowKey::REFRESH_ORDER {
            let expected = if refreshed.contains(&window) {
                11.5
            } else {
                99.0
            };
            assert_eq!(total(families, window), Some(expected), "{window:?}");
        }
        assert_eq!(collector(families, PnlWindowKey::OneYear), Some(100.0));
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
        assert_eq!(
            reports.calls()[6..8],
            [
                query_dates("2026-02-26", "2026-03-04"),
                query_dates("2026-03-04", "2026-03-04"),
            ]
        );
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
