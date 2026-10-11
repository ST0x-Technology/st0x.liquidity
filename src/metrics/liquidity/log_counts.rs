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
//! is activated before the subscriber is installed, and it then records the
//! length of every log file, so every byte this process writes lies past
//! those lengths. Once per process, in the background, the buckets are
//! seeded from the files, each read only up to its recorded length. So an
//! event of this process is not counted twice, the previous processes'
//! events survive a restart, and neither depends on the wall clock: a clock
//! that steps back after the start cannot make an entry of this process read
//! as an earlier one. Until the seed is done the log names are not
//! published.
//!
//! Only this process's events are counted live. Lines another process writes
//! into the same files after activation, such as an operator's `st0x-cli`
//! run with the production config, are read by the endpoint but not counted
//! here until the next restart seeds them.
//!
//! Time is bounded on both sides, as the endpoint bounds it. The seed reads
//! no entry dated after activation, such as one a previous process logged
//! while its clock ran ahead, and a window counts no minute after its own:
//! such a minute is kept and counts once the clock reaches it.
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

use crate::api::{LogFileExtent, LogFilter, log_file_extents, visit_extent_entries};

/// The window both log names count over.
const LOG_COUNT_WINDOW: chrono::Duration = chrono::Duration::hours(24);

const SECONDS_PER_MINUTE: i64 = 60;

/// The process-wide counter, set once when file logging is configured.
static LOG_COUNTS: OnceLock<Arc<LogCounts>> = OnceLock::new();

/// Activates counting for this process and records the lengths of the log
/// files in `log_dir`, which bound the seed.
///
/// Returns the sink to pass to the tracing setup. Call it only when file
/// logging is configured, before the subscriber is installed. A second call
/// returns the same counter.
pub fn activate_log_counts(log_dir: &str) -> Arc<dyn LogEventSink> {
    LOG_COUNTS
        .get_or_init(|| Arc::new(LogCounts::activate(log_dir, Utc::now())))
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
    /// The log files and their lengths at activation: what the previous
    /// processes wrote. The seed reads these bytes and no others.
    previous_files: Vec<LogFileExtent>,
    /// Why the listing at activation missed files, if it did. No subscriber
    /// exists then, so the seed logs it. A later listing would also see this
    /// process's bytes, so the miss is final.
    listing_error: Option<std::io::Error>,
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
    /// A counter activated at `now`, with the lengths the log files in
    /// `log_dir` have now. A missing directory has no previous files.
    pub(crate) fn activate(log_dir: &str, now: DateTime<Utc>) -> Self {
        let (previous_files, listing_error) = log_file_extents(log_dir, &seed_filter(now));

        Self {
            activated_at: now,
            previous_files,
            listing_error,
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
    /// Minutes after `now`'s minute, from a clock that stepped back, are kept
    /// but not counted, as the endpoint's upper bound skips their lines; a
    /// target with only such minutes is not listed.
    pub(crate) fn window(&self, now: DateTime<Utc>) -> LogWindow {
        let oldest_kept = unix_minute(now - LOG_COUNT_WINDOW);
        let newest_counted = unix_minute(now);
        let mut buckets = self.buckets.lock().unwrap_or_else(PoisonError::into_inner);
        let mut window = LogWindow::default();

        for level in [CountedLogLevel::Error, CountedLogLevel::Warn] {
            let by_target = &mut buckets[level_index(level)];
            by_target.retain(|_, minutes| {
                minutes.retain(|bucket, _| *bucket >= oldest_kept);
                !minutes.is_empty()
            });

            for (target, minutes) in by_target.iter() {
                let count: u64 = minutes
                    .range(..=newest_counted)
                    .map(|(_, count)| count)
                    .sum();
                if count == 0 {
                    continue;
                }
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
    pub(crate) fn seeded_or_start(self: &Arc<Self>) -> bool {
        let mut state = self.seed.lock().unwrap_or_else(PoisonError::into_inner);
        let failed_attempts = match *state {
            SeedState::Done => return true,
            SeedState::Running { .. } => return false,
            SeedState::NotStarted { failed_attempts } => failed_attempts,
        };
        *state = SeedState::Running { failed_attempts };
        drop(state);

        let counts = Arc::clone(self);
        let last_attempt = failed_attempts + 1 >= MAX_SEED_ATTEMPTS;
        tokio::spawn(async move {
            let next = match counts.seed(last_attempt).await {
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
    /// `accept_partial` (the last attempt), when it adds what it read. A
    /// retry cannot recover files the listing at activation missed, so the
    /// seed then adds the files it has and logs the miss.
    async fn seed(&self, accept_partial: bool) -> Result<(), SeedError> {
        let filter = seed_filter(self.activated_at);
        let previous_files = self.previous_files.clone();
        let Scan { seed, error } =
            tokio::task::spawn_blocking(move || scan_previous_entries(&previous_files, &filter))
                .await?;

        match error {
            None => self.merge(seed),
            Some(error) if accept_partial => {
                self.merge(seed);
                error!(%error, "Log counts seeded from the log files that could be read");
            }
            Some(error) => return Err(error.into()),
        }

        if let Some(error) = &self.listing_error {
            error!(%error, "Log counts seeded without the log files the start could not list");
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

/// The error and warning entries from the 24 hours before activation. The
/// recorded file lengths, not the upper time bound, separate the previous
/// processes' entries from this process's; the bound only drops entries a
/// previous process dated after activation, while its clock ran ahead.
fn seed_filter(activated_at: DateTime<Utc>) -> LogFilter {
    LogFilter {
        search_lower: None,
        levels: Some(vec![
            level_label(CountedLogLevel::Error).to_string(),
            level_label(CountedLogLevel::Warn).to_string(),
        ]),
        targets: None,
        since: Some(activated_at - LOG_COUNT_WINDOW),
        until: Some(activated_at),
    }
}

/// The entries of `previous_files` that pass `filter`, each file read up to
/// its recorded length, bucketed like live events. An entry without a
/// timestamp is skipped, as the endpoint skips it.
fn scan_previous_entries(previous_files: &[LogFileExtent], filter: &LogFilter) -> Scan {
    let mut seed = [Buckets::new(), Buckets::new()];

    let read = visit_extent_entries(previous_files, filter, |entry| {
        let Some(at) = entry["timestamp"]
            .as_str()
            .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
            .map(|parsed| parsed.with_timezone(&Utc))
        else {
            return;
        };

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

    /// A counter without previous log files.
    fn counts() -> LogCounts {
        LogCounts::activate("/nonexistent/log/dir", at(ACTIVATED))
    }

    fn counts_in(dir: &std::path::Path) -> LogCounts {
        LogCounts::activate(dir.to_str().unwrap(), at(ACTIVATED))
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

    /// A minute after `now`, from a clock that stepped back after the event,
    /// is not counted yet; it counts once the clock reaches it.
    #[test]
    fn a_minute_after_now_counts_once_the_clock_reaches_it() {
        let counts = counts();
        let now = at("2026-03-02T12:05:00Z");
        let ahead = now + chrono::Duration::minutes(10);
        counts.record_at(Level::ERROR, "hedge", now);
        counts.record_at(Level::ERROR, "bridge", ahead);

        assert_eq!(
            counts.window(now),
            window(1, 0, &[(CountedLogLevel::Error, "hedge", 1)])
        );
        assert_eq!(
            counts.window(ahead),
            window(
                2,
                0,
                &[
                    (CountedLogLevel::Error, "bridge", 1),
                    (CountedLogLevel::Error, "hedge", 1),
                ]
            )
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

    /// Appends the lines to the file, creating it if needed.
    fn write_log(dir: &std::path::Path, name: &str, lines: &[(&str, &str, &str)]) {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(name))
            .unwrap();
        for (timestamp, level, target) in lines {
            writeln!(
                file,
                r#"{{"timestamp":"{timestamp}","level":"{level}","fields":{{"message":"m"}},"target":"{target}"}}"#
            )
            .unwrap();
        }
    }

    /// The seed takes the previous processes' entries from the 24 hours
    /// before activation, and only the bytes the files held at activation.
    /// The lines this process writes after it are counted live, so nothing
    /// is counted twice, even when a clock stepped back gives them a time
    /// before activation, and a file created after activation is not read.
    #[tokio::test]
    async fn the_seed_reads_only_what_the_files_held_at_activation() {
        let dir = tempfile::tempdir().unwrap();
        write_log(
            dir.path(),
            "st0x-hedge.log.2026-03-02",
            &[
                ("2026-03-01T11:00:00Z", "ERROR", "hedge"),
                ("2026-03-02T11:59:00Z", "ERROR", "hedge"),
                ("2026-03-02T12:00:00Z", "WARN", "inventory"),
                ("2026-03-02T12:00:00Z", "INFO", "hedge"),
            ],
        );
        let counts = counts_in(dir.path());

        let stepped_back = [
            ("2026-03-02T11:59:40Z", "ERROR", "hedge"),
            ("2026-03-02T11:59:50Z", "WARN", "inventory"),
        ];
        write_log(dir.path(), "st0x-hedge.log.2026-03-02", &stepped_back[..1]);
        write_log(dir.path(), "st0x-hedge.log.2026-03-03", &stepped_back[1..]);
        counts.record_at(Level::ERROR, "hedge", at(stepped_back[0].0));
        counts.record_at(Level::WARN, "inventory", at(stepped_back[1].0));

        counts.seed(false).await.unwrap();

        assert_eq!(
            counts.window(at("2026-03-02T12:01:00Z")),
            window(
                2,
                2,
                &[
                    (CountedLogLevel::Error, "hedge", 2),
                    (CountedLogLevel::Warn, "inventory", 2),
                ]
            )
        );
    }

    /// A line another process was writing at activation is cut by the
    /// recorded length. The cut part does not parse, so the seed skips it
    /// instead of reading past the length; the lines before it count.
    #[tokio::test]
    async fn a_line_cut_by_the_recorded_length_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let name = "st0x-hedge.log.2026-03-02";
        write_log(
            dir.path(),
            name,
            &[("2026-03-02T11:58:00Z", "ERROR", "hedge")],
        );
        let append = |bytes: &[u8]| {
            std::fs::OpenOptions::new()
                .append(true)
                .open(dir.path().join(name))
                .unwrap()
                .write_all(bytes)
                .unwrap();
        };
        append(br#"{"timestamp":"2026-03-02T11:59:00Z","level":"ERROR","#);
        let counts = counts_in(dir.path());
        append(b"\"target\":\"bridge\"}\n");

        counts.seed(false).await.unwrap();

        assert_eq!(
            counts.window(at("2026-03-02T12:01:00Z")),
            window(1, 0, &[(CountedLogLevel::Error, "hedge", 1)])
        );
    }

    /// A previous process whose clock ran ahead dated an entry after this
    /// activation. The seed leaves it out, so it never counts, even once
    /// the clock passes its time.
    #[tokio::test]
    async fn the_seed_skips_entries_dated_after_activation() {
        let dir = tempfile::tempdir().unwrap();
        write_log(
            dir.path(),
            "st0x-hedge.log.2026-03-02",
            &[
                ("2026-03-02T11:59:00Z", "ERROR", "hedge"),
                ("2026-03-02T12:30:00Z", "ERROR", "bridge"),
            ],
        );
        let counts = counts_in(dir.path());

        counts.seed(false).await.unwrap();

        assert_eq!(
            counts.window(at("2026-03-02T12:31:00Z")),
            window(1, 0, &[(CountedLogLevel::Error, "hedge", 1)])
        );
    }

    async fn wait_until_seeded(counts: &Arc<LogCounts>) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !counts.seeded_or_start() {
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
        let counts = Arc::new(counts_in(dir.path()));

        assert!(!counts.seeded_or_start(), "the first call only starts it");
        wait_until_seeded(&counts).await;
        assert!(counts.seeded_or_start());

        assert_eq!(counts.window(at("2026-03-02T12:01:00Z")).errors, 1);
    }

    #[tokio::test]
    async fn a_missing_log_directory_seeds_nothing() {
        let counts = counts();

        counts.seed(false).await.unwrap();

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
        let counts = counts_in(dir.path());

        assert!(matches!(counts.seed(false).await, Err(SeedError::Read(_))));
        assert_eq!(
            counts.window(at("2026-03-02T12:01:00Z")),
            LogWindow::default()
        );

        counts.seed(true).await.unwrap();
        assert_eq!(counts.window(at("2026-03-02T12:01:00Z")).errors, 1);
    }

    /// A seed that keeps failing, here on an unreadable file, is retried
    /// on the next refreshes, then accepted with what it read, so the log
    /// names do not stay absent until a restart.
    #[tokio::test]
    async fn a_failing_seed_is_retried_then_accepted() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("st0x-hedge.log.backup")).unwrap();
        let counts = Arc::new(counts_in(dir.path()));

        for failed_attempts in 1..MAX_SEED_ATTEMPTS {
            assert!(!counts.seeded_or_start());
            wait_for_state(&counts, SeedState::NotStarted { failed_attempts }).await;
        }
        assert!(!counts.seeded_or_start());
        wait_for_state(&counts, SeedState::Done).await;

        assert!(counts.seeded_or_start());
    }

    /// A listing that failed at activation cannot heal on a retry, so the
    /// first attempt seeds what it has instead of failing.
    #[tokio::test]
    async fn a_failed_listing_seeds_on_the_first_attempt() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let counts = Arc::new(counts_in(file.path()));
        assert!(counts.listing_error.is_some());

        assert!(!counts.seeded_or_start());
        wait_for_state(&counts, SeedState::Done).await;
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
