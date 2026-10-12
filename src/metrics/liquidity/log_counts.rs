//! Error and warning counts over the last 24 hours, for
//! `liq_reliability_log_count_24h` and `liq_log_target_count_24h`.
//!
//! The counting layer in `st0x_config` forwards every event the file log
//! writes to [`LogCounts`], which keeps one-minute buckets per level and
//! target. The exporter counted the same events by reading the files back
//! through `/performance/reliability`; counting as they happen avoids that
//! scan every minute and its 50,000-entry cap.
//!
//! The counter is active only when file logging is, like the endpoint. It
//! records when it was activated, before the subscriber is installed, so
//! every file event of this process is later than that instant. Once per
//! process, in the background, the buckets are seeded from the files with
//! only the entries before that instant, so an event of this process is not
//! counted twice and the previous process's events survive a restart. Until
//! the seed is done the log names are not published.
//!
//! Only this process's events are counted live. Lines another process writes
//! into the same files after activation, such as an operator's `st0x-cli`
//! run with the production config, are read by the endpoint but not counted
//! here until the next restart seeds them.
//!
//! A bucket covers a whole minute, so the 24-hour window can differ from the
//! endpoint's exact interval by the events in its first minute. An event is
//! counted when it is logged, so a line the lossy non-blocking file writer
//! then drops (a full queue, a failed disk write) is counted here but not by
//! the endpoint.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use chrono::{DateTime, Utc};
use tokio::task::JoinError;
use tracing::{Level, error, warn};

use st0x_config::LogEventSink;
use st0x_dto::CountedLogLevel;

use crate::api::{LogFilter, visit_matching_entries};

/// The window both log names count over.
const LOG_COUNT_WINDOW: chrono::Duration = chrono::Duration::hours(24);

const SECONDS_PER_MINUTE: i64 = 60;

/// The process-wide counter, set once when file logging is configured.
static LOG_COUNTS: OnceLock<Arc<LogCounts>> = OnceLock::new();

/// Activates counting for this process.
///
/// Returns the sink to pass to the tracing setup. Call it only when file logging is configured, before the
/// subscriber is installed. A second call returns the same counter.
pub fn activate_log_counts() -> Arc<dyn LogEventSink> {
    LOG_COUNTS
        .get_or_init(|| Arc::new(LogCounts::new(Utc::now())))
        .clone()
}

/// The active counter, if this process activated one.
pub(crate) fn active_log_counts() -> Option<Arc<LogCounts>> {
    LOG_COUNTS.get().cloned()
}

/// Counts per minute, per level and target. Minutes are Unix minutes.
type Buckets = BTreeMap<String, BTreeMap<i64, u64>>;

pub(crate) struct LogCounts {
    activated_at: DateTime<Utc>,
    /// Indexed by [`level_index`].
    buckets: Mutex<[Buckets; 2]>,
    seed: Mutex<SeedState>,
}

/// The one-time seed from the files. It runs in the background, so a long
/// scan never delays a collector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SeedState {
    NotStarted { failed_attempts: u32 },
    Running { failed_attempts: u32 },
    Done,
}

/// Seed attempts before a scan that keeps failing is accepted with what it
/// read, as the endpoint shows what it can read. A persistent error would
/// otherwise keep the log names absent until a restart.
const MAX_SEED_ATTEMPTS: u32 = 3;

#[derive(Debug, thiserror::Error)]
pub(crate) enum SeedError {
    #[error("failed to read the log files")]
    Read(#[from] std::io::Error),
    #[error("the log scan task failed")]
    Task(#[from] JoinError),
}

/// What one scan read, and the first read error that cut it short.
struct Scan {
    seed: [Buckets; 2],
    error: Option<std::io::Error>,
}

/// Error and warning counts over the window.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct LogWindow {
    pub(crate) errors: u64,
    pub(crate) warnings: u64,
    /// Every level and target with a positive count.
    pub(crate) targets: Vec<(CountedLogLevel, String, u64)>,
}

impl LogEventSink for LogCounts {
    fn record(&self, level: Level, target: &str) {
        self.record_at(level, target, Utc::now());
    }
}

impl LogCounts {
    pub(crate) fn new(activated_at: DateTime<Utc>) -> Self {
        Self {
            activated_at,
            buckets: Mutex::new([Buckets::new(), Buckets::new()]),
            seed: Mutex::new(SeedState::NotStarted { failed_attempts: 0 }),
        }
    }

    fn record_at(&self, level: Level, target: &str, at: DateTime<Utc>) {
        let Some(level) = counted_level(level) else {
            return;
        };
        metrics::counter!(
            "log_events_total",
            "level" => level_label(level),
            "target" => target.to_string(),
        )
        .increment(1);

        let minute = unix_minute(at);
        let oldest_kept = unix_minute(at - LOG_COUNT_WINDOW);
        let mut buckets = self.buckets.lock().unwrap_or_else(PoisonError::into_inner);
        let by_target = &mut buckets[level_index(level)];

        if let Some(minutes) = by_target.get_mut(target) {
            *minutes.entry(minute).or_default() += 1;
            // Keeps a key bounded even if no window is read for a while.
            // Minutes are sorted, so only the oldest can have expired.
            while let Some(oldest) = minutes.first_entry()
                && *oldest.key() < oldest_kept
            {
                oldest.remove();
            }
        } else {
            by_target.insert(target.to_string(), BTreeMap::from([(minute, 1)]));
        }
        drop(buckets);
    }

    /// The counts of the minutes that overlap the 24 hours before `now`.
    /// Older minutes are dropped, and a target with none left is forgotten.
    pub(crate) fn window(&self, now: DateTime<Utc>) -> LogWindow {
        let oldest_kept = unix_minute(now - LOG_COUNT_WINDOW);
        let mut buckets = self.buckets.lock().unwrap_or_else(PoisonError::into_inner);
        let mut window = LogWindow::default();

        for level in [CountedLogLevel::Error, CountedLogLevel::Warn] {
            let by_target = &mut buckets[level_index(level)];
            by_target.retain(|_, minutes| {
                minutes.retain(|bucket, _| *bucket >= oldest_kept);
                !minutes.is_empty()
            });

            for (target, minutes) in by_target.iter() {
                let count: u64 = minutes.values().sum();
                match level {
                    CountedLogLevel::Error => window.errors += count,
                    CountedLogLevel::Warn => window.warnings += count,
                }
                window.targets.push((level, target.clone(), count));
            }
        }
        drop(buckets);

        window
    }

    /// Whether the seed is done. If it has not started, or the last attempt
    /// failed, it starts in the background and this returns false. The
    /// state is per process, so a restarted refresh task neither repeats a
    /// finished seed nor starts a second one.
    pub(crate) fn seeded_or_start(self: &Arc<Self>, log_dir: &str) -> bool {
        let mut state = self.seed.lock().unwrap_or_else(PoisonError::into_inner);
        let failed_attempts = match *state {
            SeedState::Done => return true,
            SeedState::Running { .. } => return false,
            SeedState::NotStarted { failed_attempts } => failed_attempts,
        };
        *state = SeedState::Running { failed_attempts };
        drop(state);

        let counts = Arc::clone(self);
        let log_dir = log_dir.to_string();
        let last_attempt = failed_attempts + 1 >= MAX_SEED_ATTEMPTS;
        tokio::spawn(async move {
            let next = match counts.seed(&log_dir, last_attempt).await {
                Ok(()) => SeedState::Done,
                Err(error) if last_attempt => {
                    error!(%error, "Log counts seeded without the previous process's entries");
                    SeedState::Done
                }
                Err(error) => {
                    warn!(%error, "Log counts not seeded; retrying on the next refresh");
                    SeedState::NotStarted {
                        failed_attempts: failed_attempts + 1,
                    }
                }
            };
            *counts.seed.lock().unwrap_or_else(PoisonError::into_inner) = next;
        });

        false
    }

    /// Adds the entries the previous processes wrote in the 24 hours before
    /// activation. A scan that hit a read error adds nothing, unless
    /// `accept_partial` (the last attempt), when it adds what it read.
    async fn seed(&self, log_dir: &str, accept_partial: bool) -> Result<(), SeedError> {
        let activated_at = self.activated_at;
        let log_dir = log_dir.to_string();
        let Scan { seed, error } =
            tokio::task::spawn_blocking(move || scan_previous_entries(&log_dir, activated_at))
                .await?;

        match error {
            None => self.merge(seed),
            Some(error) if accept_partial => {
                self.merge(seed);
                error!(%error, "Log counts seeded from the log files that could be read");
            }
            Some(error) => return Err(error.into()),
        }
        Ok(())
    }

    fn merge(&self, seed: [Buckets; 2]) {
        let mut buckets = self.buckets.lock().unwrap_or_else(PoisonError::into_inner);

        for (live, seeded) in buckets.iter_mut().zip(seed) {
            for (target, minutes) in seeded {
                let live_minutes = live.entry(target).or_default();
                for (minute, count) in minutes {
                    *live_minutes.entry(minute).or_default() += count;
                }
            }
        }
    }
}

/// Error and warning entries in `log_dir` from the 24 hours before
/// `activated_at`, strictly before it, bucketed like live events. An entry
/// without a timestamp is skipped, as the endpoint skips it.
fn scan_previous_entries(log_dir: &str, activated_at: DateTime<Utc>) -> Scan {
    let filter = LogFilter {
        search_lower: None,
        levels: Some(vec![
            level_label(CountedLogLevel::Error).to_string(),
            level_label(CountedLogLevel::Warn).to_string(),
        ]),
        targets: None,
        since: Some(activated_at - LOG_COUNT_WINDOW),
        until: Some(activated_at),
    };
    let mut seed = [Buckets::new(), Buckets::new()];

    let read = visit_matching_entries(log_dir, &filter, |entry| {
        let Some(at) = entry["timestamp"]
            .as_str()
            .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
            .map(|parsed| parsed.with_timezone(&Utc))
        else {
            return;
        };
        if at >= activated_at {
            return;
        }

        let level = match entry["level"].as_str() {
            Some(raw) if raw.eq_ignore_ascii_case("ERROR") => CountedLogLevel::Error,
            Some(raw) if raw.eq_ignore_ascii_case("WARN") => CountedLogLevel::Warn,
            _ => return,
        };
        let target = entry["target"].as_str().unwrap_or("unknown");

        *seed[level_index(level)]
            .entry(target.to_string())
            .or_default()
            .entry(unix_minute(at))
            .or_default() += 1;
    });

    Scan {
        seed,
        error: read.err(),
    }
}

const fn counted_level(level: Level) -> Option<CountedLogLevel> {
    match level {
        Level::ERROR => Some(CountedLogLevel::Error),
        Level::WARN => Some(CountedLogLevel::Warn),
        _ => None,
    }
}

const fn level_index(level: CountedLogLevel) -> usize {
    match level {
        CountedLogLevel::Error => 0,
        CountedLogLevel::Warn => 1,
    }
}

/// The level as the log files and the `CountedLogLevel` wire name spell it.
pub(crate) const fn level_label(level: CountedLogLevel) -> &'static str {
    match level {
        CountedLogLevel::Error => "ERROR",
        CountedLogLevel::Warn => "WARN",
    }
}

fn unix_minute(at: DateTime<Utc>) -> i64 {
    at.timestamp().div_euclid(SECONDS_PER_MINUTE)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::metrics::liquidity::performance::tests::at;

    const ACTIVATED: &str = "2026-03-02T12:00:30Z";

    fn counts() -> LogCounts {
        LogCounts::new(at(ACTIVATED))
    }

    fn window(errors: u64, warnings: u64, targets: &[(CountedLogLevel, &str, u64)]) -> LogWindow {
        LogWindow {
            errors,
            warnings,
            targets: targets
                .iter()
                .map(|(level, target, count)| (*level, (*target).to_string(), *count))
                .collect(),
        }
    }

    #[test]
    fn level_labels_are_the_wire_names() {
        for level in [CountedLogLevel::Error, CountedLogLevel::Warn] {
            assert_eq!(
                serde_json::to_value(level).unwrap(),
                serde_json::Value::from(level_label(level))
            );
        }
    }

    #[test]
    fn counts_errors_and_warnings_per_target_and_ignores_other_levels() {
        let counts = counts();
        let now = at("2026-03-02T12:05:00Z");

        counts.record_at(Level::ERROR, "hedge", now);
        counts.record_at(Level::ERROR, "hedge", now);
        counts.record_at(Level::WARN, "hedge", now);
        counts.record_at(Level::WARN, "inventory", now);
        counts.record_at(Level::INFO, "hedge", now);
        counts.record_at(Level::DEBUG, "hedge", now);

        assert_eq!(
            counts.window(now),
            window(
                2,
                2,
                &[
                    (CountedLogLevel::Error, "hedge", 2),
                    (CountedLogLevel::Warn, "hedge", 1),
                    (CountedLogLevel::Warn, "inventory", 1),
                ]
            )
        );
    }

    /// An event 1441 minutes old has left the window; one in the current
    /// minute has not.
    #[test]
    fn only_the_last_24_hours_count() {
        let counts = counts();
        let now = at("2026-03-03T12:00:10Z");

        counts.record_at(Level::ERROR, "hedge", now - chrono::Duration::minutes(1441));
        counts.record_at(Level::ERROR, "hedge", now);

        assert_eq!(
            counts.window(now),
            window(1, 0, &[(CountedLogLevel::Error, "hedge", 1)])
        );
    }

    /// The window cannot split a minute: two events in the minute that
    /// holds the 24-hour cutoff, one on each side of it, count together and
    /// leave together. This is the tolerance against the endpoint's exact
    /// interval.
    #[test]
    fn the_boundary_minute_counts_or_leaves_as_a_whole() {
        let counts = counts();
        let now = at("2026-03-03T12:00:30Z");
        counts.record_at(Level::WARN, "hedge", at("2026-03-02T12:00:10Z"));
        counts.record_at(Level::WARN, "hedge", at("2026-03-02T12:00:50Z"));

        assert_eq!(counts.window(now).warnings, 2);
        assert_eq!(
            counts.window(now + chrono::Duration::seconds(30)).warnings,
            0
        );
    }

    #[test]
    fn a_target_without_events_in_the_window_disappears() {
        let counts = counts();
        let start = at("2026-03-02T12:00:00Z");
        counts.record_at(Level::ERROR, "bridge", start);
        counts.record_at(Level::ERROR, "hedge", start + chrono::Duration::hours(12));

        assert_eq!(
            counts.window(start + chrono::Duration::hours(25)),
            window(1, 0, &[(CountedLogLevel::Error, "hedge", 1)])
        );
        assert_eq!(
            counts.window(start + chrono::Duration::hours(37)),
            LogWindow::default()
        );
    }

    fn write_log(dir: &std::path::Path, name: &str, lines: &[(&str, &str, &str)]) {
        let mut file = std::fs::File::create(dir.join(name)).unwrap();
        for (timestamp, level, target) in lines {
            writeln!(
                file,
                r#"{{"timestamp":"{timestamp}","level":"{level}","fields":{{"message":"m"}},"target":"{target}"}}"#
            )
            .unwrap();
        }
    }

    /// The seed takes the previous processes' entries, strictly before
    /// activation; the events this process logged after activation are
    /// already counted live, so nothing is counted twice.
    #[tokio::test]
    async fn the_seed_adds_only_entries_before_activation() {
        let dir = tempfile::tempdir().unwrap();
        write_log(
            dir.path(),
            "st0x-hedge.log.2026-03-02",
            &[
                ("2026-03-01T11:00:00Z", "ERROR", "hedge"),
                ("2026-03-02T11:59:00Z", "ERROR", "hedge"),
                ("2026-03-02T12:00:00Z", "WARN", "inventory"),
                ("2026-03-02T12:00:00Z", "INFO", "hedge"),
                ("2026-03-02T12:00:30Z", "ERROR", "hedge"),
                ("2026-03-02T12:00:40Z", "ERROR", "hedge"),
            ],
        );
        let counts = counts();
        counts.record_at(Level::ERROR, "hedge", at("2026-03-02T12:00:30Z"));
        counts.record_at(Level::ERROR, "hedge", at("2026-03-02T12:00:40Z"));

        counts
            .seed(dir.path().to_str().unwrap(), false)
            .await
            .unwrap();

        assert_eq!(
            counts.window(at("2026-03-02T12:01:00Z")),
            window(
                3,
                1,
                &[
                    (CountedLogLevel::Error, "hedge", 3),
                    (CountedLogLevel::Warn, "inventory", 1),
                ]
            )
        );
    }

    async fn wait_until_seeded(counts: &Arc<LogCounts>, log_dir: &str) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !counts.seeded_or_start(log_dir) {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    /// The seed runs in the background once per process: a restarted
    /// refresh task finds it done and does not add the files again.
    #[tokio::test]
    async fn the_background_seed_runs_once() {
        let dir = tempfile::tempdir().unwrap();
        write_log(
            dir.path(),
            "st0x-hedge.log.2026-03-02",
            &[("2026-03-02T11:59:00Z", "ERROR", "hedge")],
        );
        let counts = Arc::new(counts());
        let log_dir = dir.path().to_str().unwrap();

        assert!(
            !counts.seeded_or_start(log_dir),
            "the first call only starts it"
        );
        wait_until_seeded(&counts, log_dir).await;
        assert!(counts.seeded_or_start(log_dir));

        assert_eq!(counts.window(at("2026-03-02T12:01:00Z")).errors, 1);
    }

    #[tokio::test]
    async fn a_missing_log_directory_seeds_nothing() {
        let counts = counts();

        counts.seed("/nonexistent/log/dir", false).await.unwrap();

        assert_eq!(
            counts.window(at("2026-03-02T12:01:00Z")),
            LogWindow::default()
        );
    }

    async fn wait_for_state(counts: &LogCounts, expected: SeedState) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while *counts.seed.lock().unwrap() != expected {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    /// A directory that matches the log file name opens but cannot be read.
    /// The scan stops on it instead of retrying the read forever; an early
    /// attempt adds nothing, and the last one keeps what the other files
    /// gave.
    #[tokio::test]
    async fn a_read_error_adds_nothing_until_the_last_attempt() {
        let dir = tempfile::tempdir().unwrap();
        write_log(
            dir.path(),
            "st0x-hedge.log.2026-03-02",
            &[("2026-03-02T11:59:00Z", "ERROR", "hedge")],
        );
        std::fs::create_dir(dir.path().join("st0x-hedge.log.backup")).unwrap();
        let log_dir = dir.path().to_str().unwrap();
        let counts = counts();

        assert!(matches!(
            counts.seed(log_dir, false).await,
            Err(SeedError::Read(_))
        ));
        assert_eq!(
            counts.window(at("2026-03-02T12:01:00Z")),
            LogWindow::default()
        );

        counts.seed(log_dir, true).await.unwrap();
        assert_eq!(counts.window(at("2026-03-02T12:01:00Z")).errors, 1);
    }

    /// A seed that keeps failing is retried on the next refreshes, then
    /// accepted with what it read, so the log names do not stay absent until
    /// a restart.
    #[tokio::test]
    async fn a_failing_seed_is_retried_then_accepted() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let not_a_directory = file.path().to_str().unwrap();
        let counts = Arc::new(counts());

        for failed_attempts in 1..MAX_SEED_ATTEMPTS {
            assert!(!counts.seeded_or_start(not_a_directory));
            wait_for_state(&counts, SeedState::NotStarted { failed_attempts }).await;
        }
        assert!(!counts.seeded_or_start(not_a_directory));
        wait_for_state(&counts, SeedState::Done).await;

        assert!(counts.seeded_or_start(not_a_directory));
    }

    #[test]
    fn every_counted_event_increments_log_events_total() {
        let recorder = crate::metrics::local_recorder();
        let handle = recorder.handle();
        let counts = counts();
        let now = at("2026-03-02T12:05:00Z");

        metrics::with_local_recorder(&recorder, || {
            counts.record_at(Level::ERROR, "hedge", now);
            counts.record_at(Level::ERROR, "hedge", now);
            counts.record_at(Level::WARN, "inventory", now);
            counts.record_at(Level::INFO, "hedge", now);
        });

        let rendered = crate::metrics::liquidity::tests::parse_exposition(&handle.render());
        let total = |level: &str, target: &str| {
            rendered
                .get(&crate::metrics::liquidity::tests::series(
                    "log_events_total",
                    &[("level", level), ("target", target)],
                ))
                .copied()
        };
        assert_eq!(total("ERROR", "hedge"), Some(2.0));
        assert_eq!(total("WARN", "inventory"), Some(1.0));
        assert_eq!(total("INFO", "hedge"), None);
    }
}
