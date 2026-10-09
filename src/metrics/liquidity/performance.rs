//! The performance families: hedge latencies, reliability, infrastructure
//! and rebalance timings.
//!
//! Each builder takes the report the matching `/performance/*` endpoint
//! serves, so the series hold the numbers the exporter read from those
//! endpoints. `liq-performance-refresh` builds them every 60 seconds over the
//! last 24 hours, and the rebalance family every fifth cycle over the last 30
//! days, like the exporter.
//!
//! The infrastructure series carry a `chain` label per hedged chain. The
//! exporter published them without labels, from a report shape the bot no
//! longer serves.

use tracing::warn;

use st0x_dto::{
    EquityTimings, FailureEventCount, HedgeLatencies, InfraReport, JobQueueHealth, LatencyStats,
    RebalanceTimings,
};

use super::log_counts::{LogWindow, level_label};
use super::{
    LiqMetric, LiqSample, count_value, integer_value, push_sample, signed_integer_value,
    strip_prefix, timestamp_value, wire_name,
};

/// `liq_hedge_latency_ms`, its sample counts, and the open exposures.
pub(crate) fn latency_samples(report: &HedgeLatencies) -> Vec<LiqSample> {
    let stages = &report.summary.stages;
    let mut samples = Vec::new();

    for (stage, stats) in [
        ("detection", &stages.detection),
        ("decision", &stages.decision),
        ("submission", &stages.submission),
        ("execution", &stages.execution),
        ("exposure_window", &stages.exposure_window),
    ] {
        let Some(stats) = stats else {
            continue;
        };
        let labels = vec![("stage", stage.to_string())];

        push_quantiles(&mut samples, LiqMetric::HedgeLatencyMs, &labels, stats);
        push_sample(
            &mut samples,
            LiqMetric::HedgeLatencyMsSamples,
            labels,
            count_value(stats.sample_count),
        );
    }

    for exposure in &report.open_exposures {
        let labels = vec![("symbol", strip_prefix(exposure.symbol.as_str()).to_string())];

        push_sample(
            &mut samples,
            LiqMetric::OpenExposureFillCount,
            labels.clone(),
            count_value(exposure.fill_count),
        );
        push_sample(
            &mut samples,
            LiqMetric::OpenExposureOldestTsSeconds,
            labels,
            timestamp_value(exposure.oldest_fill_block_timestamp),
        );
    }

    samples
}

/// Log counts and failure events in the window, and the instantaneous job
/// queue counts. With log counts, both level rows are published, 0
/// included, and a target row only with a positive count. Without them (the
/// seed is not done) the log names are absent.
pub(crate) fn reliability_samples(
    logs: Option<&LogWindow>,
    failure_events: &[FailureEventCount],
    job_queues: &[JobQueueHealth],
) -> Vec<LiqSample> {
    let mut samples = Vec::new();

    if let Some(logs) = logs {
        push_log_counts(&mut samples, logs);
    }

    for event in failure_events {
        push_sample(
            &mut samples,
            LiqMetric::FailureEventCount24h,
            vec![("event_type", event.event_type.as_str().to_string())],
            count_value(event.count),
        );
    }

    for queue in job_queues {
        for (state, count) in [
            ("pending", queue.pending),
            ("running", queue.running),
            ("done", queue.done),
            ("failed", queue.failed),
            ("awaiting_retry", queue.awaiting_retry),
            ("killed", queue.killed),
            ("retried", queue.retried),
        ] {
            push_sample(
                &mut samples,
                LiqMetric::JobQueue,
                vec![
                    ("job_type", queue.job_type.clone()),
                    ("state", state.to_string()),
                ],
                count_value(count),
            );
        }
    }

    samples
}

fn push_log_counts(samples: &mut Vec<LiqSample>, logs: &LogWindow) {
    for (level, count) in [("error", logs.errors), ("warning", logs.warnings)] {
        push_sample(
            samples,
            LiqMetric::ReliabilityLogCount24h,
            vec![("level", level.to_string())],
            integer_value(count),
        );
    }
    for (level, target, count) in &logs.targets {
        push_sample(
            samples,
            LiqMetric::LogTargetCount24h,
            vec![
                ("level", level_label(*level).to_string()),
                ("target", target.clone()),
            ],
            integer_value(*count),
        );
    }
}

/// Block lag and poll health per hedged chain, and dependency call stats.
pub(crate) fn infra_samples(report: &InfraReport) -> Vec<LiqSample> {
    let mut samples = Vec::new();

    for lag in &report.monitor.block_lag {
        let Some(chain) = label_value(&lag.chain) else {
            continue;
        };
        let labels = vec![("chain", chain)];

        if let Some(blocks) = lag.current_lag_blocks {
            push_sample(
                &mut samples,
                LiqMetric::BlockLagBlocks,
                labels.clone(),
                signed_integer_value(blocks),
            );
        }
        if let Some(sampled_at) = lag.current_lag_sampled_at {
            push_sample(
                &mut samples,
                LiqMetric::BlockLagSampledTsSeconds,
                labels,
                timestamp_value(sampled_at),
            );
        }
    }

    for poll in &report.monitor.poll {
        let Some(chain) = label_value(&poll.chain) else {
            continue;
        };
        let labels = vec![("chain", chain)];

        for (metric, count) in [
            (LiqMetric::PollCycles24h, poll.cycles),
            (LiqMetric::PollErrors24h, poll.errors),
            (LiqMetric::PollSkippedTicks24h, poll.skipped_ticks),
        ] {
            push_sample(&mut samples, metric, labels.clone(), count_value(count));
        }
        if let Some(duration) = &poll.duration {
            push_quantiles(&mut samples, LiqMetric::PollDurationMs, &labels, duration);
        }
    }

    for dependency in &report.dependencies {
        let Some(name) = label_value(&dependency.dependency) else {
            continue;
        };
        let labels = vec![
            ("dependency", name),
            ("operation", dependency.operation.clone()),
        ];

        push_sample(
            &mut samples,
            LiqMetric::DependencyCalls24h,
            labels.clone(),
            count_value(dependency.calls),
        );
        push_sample(
            &mut samples,
            LiqMetric::DependencyErrors24h,
            labels.clone(),
            count_value(dependency.errors),
        );
        if let Some(latency) = &dependency.latency {
            push_quantiles(
                &mut samples,
                LiqMetric::DependencyLatencyMs,
                &labels,
                latency,
            );
        }
    }

    samples
}

/// Stage percentiles of USDC and equity rebalances, and the latest CCTP
/// attestation. Equity rebalances have no attestation.
pub(crate) fn rebalance_samples(usdc: &RebalanceTimings, equity: &EquityTimings) -> Vec<LiqSample> {
    let mut samples = Vec::new();

    for entry in &usdc.stage_summary {
        push_stage(&mut samples, "usdc", &entry.stage, &entry.stats);
    }
    if let Some(last) = usdc.attestation_trend.last() {
        push_sample(
            &mut samples,
            LiqMetric::AttestationLastMs,
            vec![("kind", "usdc".to_string())],
            signed_integer_value(last.duration_ms),
        );
    }

    for entry in &equity.stage_summary {
        push_stage(&mut samples, "equity", &entry.stage, &entry.stats);
    }

    samples
}

fn push_stage<Stage: serde::Serialize + std::fmt::Debug>(
    samples: &mut Vec<LiqSample>,
    kind: &str,
    stage: &Stage,
    stats: &LatencyStats,
) {
    let Some(stage) = label_value(stage) else {
        return;
    };

    push_quantiles(
        samples,
        LiqMetric::RebalanceStageMs,
        &[("kind", kind.to_string()), ("stage", stage)],
        stats,
    );
}

/// One sample per exporter quantile label, from the nearest-rank stats.
fn push_quantiles(
    samples: &mut Vec<LiqSample>,
    metric: LiqMetric,
    labels: &[(&'static str, String)],
    stats: &LatencyStats,
) {
    for (quantile, millis) in [
        ("p50", stats.p50_ms),
        ("p90", stats.p90_ms),
        ("p95", stats.p95_ms),
        ("p99", stats.p99_ms),
        ("max", stats.max_ms),
    ] {
        let mut quantile_labels = labels.to_vec();
        quantile_labels.push(("quantile", quantile.to_string()));
        push_sample(
            samples,
            metric,
            quantile_labels,
            signed_integer_value(millis),
        );
    }
}

/// The wire name of an enum label value, or `None` after logging.
fn label_value<T: serde::Serialize + std::fmt::Debug>(value: &T) -> Option<String> {
    wire_name(value)
        .inspect_err(|error| warn!(?value, %error, "Skipped liq_ samples with no label value"))
        .ok()
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;

    use chrono::{DateTime, Utc};
    use serde_json::Value;

    use st0x_dto::{
        AttestationSample, ChainBlockLag, ChainName, ChainPollHealth, CountedLogLevel,
        DependencyName, DependencyStats, EquityStageName, EquityStageStats, FailureEventType,
        LatencySummary, LogTargetCount, LogVolumeBucket, MonitorTelemetry, OpenExposureReport,
        RebalanceStageName, RebalanceStageStats, ReliabilityReport, StageLatencies,
    };
    use st0x_finance::Symbol;

    use super::*;
    use crate::metrics::liquidity::LiqFamily;
    use crate::metrics::liquidity::inventory::tests::{golden_for, render_family};
    use crate::metrics::liquidity::tests::{SeriesKey, series};

    pub(crate) fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn stats(p50_ms: i64, max_ms: i64, sample_count: usize) -> LatencyStats {
        LatencyStats {
            p50_ms,
            p90_ms: p50_ms + 10,
            p95_ms: p50_ms + 20,
            p99_ms: p50_ms + 30,
            max_ms,
            sample_count,
        }
    }

    fn assert_fixture<T: serde::Serialize>(dto: &T, fixture_json: &str) {
        let fixture: Value = serde_json::from_str(fixture_json).unwrap();
        assert_eq!(serde_json::to_value(dto).unwrap(), fixture);
    }

    pub(crate) fn latencies_fixture() -> HedgeLatencies {
        HedgeLatencies {
            summary: LatencySummary {
                fill_count: 7,
                stages: StageLatencies {
                    detection: Some(stats(1_200, 4_000, 7)),
                    decision: Some(stats(-12, 40, 6)),
                    submission: None,
                    execution: Some(stats(850, 9_100, 5)),
                    exposure_window: Some(stats(3_400, 61_000, 5)),
                },
            },
            buckets: Vec::new(),
            cycles: Vec::new(),
            total_cycles: 3,
            open_exposures: vec![
                OpenExposureReport {
                    symbol: Symbol::new("tAAPL").unwrap(),
                    fill_count: 2,
                    oldest_fill_block_timestamp: at("2026-03-01T12:00:00Z"),
                },
                OpenExposureReport {
                    symbol: Symbol::new("RKLB").unwrap(),
                    fill_count: 1,
                    oldest_fill_block_timestamp: at("2026-03-01T12:30:15.250Z"),
                },
            ],
        }
    }

    pub(crate) fn reliability_fixture() -> ReliabilityReport {
        ReliabilityReport {
            log_buckets: vec![LogVolumeBucket {
                start: at("2026-03-01T00:00:00Z"),
                errors: 3,
                warnings: 5,
            }],
            log_targets: vec![
                LogTargetCount {
                    target: "inventory".to_string(),
                    level: CountedLogLevel::Warn,
                    count: 5,
                    sparkline: vec![5],
                },
                LogTargetCount {
                    target: "hedge".to_string(),
                    level: CountedLogLevel::Error,
                    count: 3,
                    sparkline: vec![3],
                },
            ],
            failure_events: vec![
                FailureEventCount {
                    event_type: FailureEventType::OffchainOrderFailed,
                    count: 2,
                    last_at: at("2026-03-01T09:00:00Z"),
                },
                FailureEventCount {
                    event_type: FailureEventType::AttestationTimedOut,
                    count: 1,
                    last_at: at("2026-03-01T10:00:00Z"),
                },
            ],
            job_queues: vec![JobQueueHealth {
                job_type: "PlaceHedge".to_string(),
                pending: 1,
                running: 0,
                done: 40,
                failed: 2,
                awaiting_retry: 1,
                killed: 0,
                retried: 3,
                oldest_pending_run_at: Some(at("2026-03-01T11:00:00Z")),
            }],
            log_entries_truncated: false,
        }
    }

    /// The exporter fails on a non-empty per-chain `poll` list, so the
    /// golden case leaves it empty and pins only the dependency series; the
    /// per-chain series have their own test.
    fn infra_fixture() -> InfraReport {
        InfraReport {
            monitor: MonitorTelemetry {
                enabled_chains: vec![ChainName::Base, ChainName::Robinhood],
                block_lag: vec![
                    ChainBlockLag {
                        chain: ChainName::Base,
                        current_lag_blocks: Some(3),
                        current_lag_sampled_at: Some(at("2026-03-01T12:00:05Z")),
                        points: Vec::new(),
                    },
                    ChainBlockLag {
                        chain: ChainName::Robinhood,
                        current_lag_blocks: None,
                        current_lag_sampled_at: None,
                        points: Vec::new(),
                    },
                ],
                poll: Vec::new(),
            },
            dependencies: vec![
                DependencyStats {
                    dependency: DependencyName::Broker,
                    operation: "get_order_status".to_string(),
                    calls: 30,
                    errors: 0,
                    latency: None,
                    buckets: Vec::new(),
                },
                DependencyStats {
                    dependency: DependencyName::Rpc,
                    operation: "eth_getLogs".to_string(),
                    calls: 120,
                    errors: 2,
                    latency: Some(stats(95, 2_300, 120)),
                    buckets: Vec::new(),
                },
            ],
        }
    }

    fn rebalance_fixture() -> (RebalanceTimings, EquityTimings) {
        (
            RebalanceTimings {
                operations: Vec::new(),
                total_operations: 2,
                skipped_operations: 0,
                stage_summary: vec![
                    RebalanceStageStats {
                        stage: RebalanceStageName::Conversion,
                        stats: stats(2_000, 2_500, 2),
                    },
                    RebalanceStageStats {
                        stage: RebalanceStageName::Attestation,
                        stats: stats(52_000, 60_000, 2),
                    },
                ],
                attestation_trend: vec![
                    AttestationSample {
                        burned_at: at("2026-02-20T08:00:00Z"),
                        duration_ms: 60_000,
                    },
                    AttestationSample {
                        burned_at: at("2026-02-27T08:00:00Z"),
                        duration_ms: 45_000,
                    },
                ],
            },
            EquityTimings {
                operations: Vec::new(),
                total_operations: 1,
                skipped_operations: 1,
                stage_summary: vec![EquityStageStats {
                    stage: EquityStageName::MintAcceptance,
                    stats: stats(30_000, 30_050, 1),
                }],
            },
        )
    }

    #[test]
    fn latencies_match_the_exporter_golden() {
        let report = latencies_fixture();
        assert_fixture(&report, include_str!("testdata/latencies.json"));

        assert_eq!(
            render_family(LiqFamily::Latencies, latency_samples(&report)),
            golden_for(
                LiqFamily::Latencies,
                include_str!("testdata/latencies.prom")
            )
        );
    }

    #[test]
    fn reliability_matches_the_exporter_golden() {
        let report = reliability_fixture();
        assert_fixture(&report, include_str!("testdata/reliability.json"));

        assert_eq!(
            render_family(
                LiqFamily::Reliability,
                reliability_samples(
                    Some(&log_window_from_dto(&report)),
                    &report.failure_events,
                    &report.job_queues,
                ),
            ),
            golden_for(
                LiqFamily::Reliability,
                include_str!("testdata/reliability.prom")
            )
        );
    }

    /// The goldens feed the exporter the endpoint's report, so the same
    /// fixture reaches the builder through this adapter.
    fn log_window_from_dto(report: &ReliabilityReport) -> LogWindow {
        let total = |count: fn(&LogVolumeBucket) -> usize| {
            report
                .log_buckets
                .iter()
                .map(count)
                .sum::<usize>()
                .try_into()
                .unwrap()
        };

        LogWindow {
            errors: total(|bucket| bucket.errors),
            warnings: total(|bucket| bucket.warnings),
            targets: report
                .log_targets
                .iter()
                .map(|target| {
                    (
                        target.level,
                        target.target.clone(),
                        target.count.try_into().unwrap(),
                    )
                })
                .collect(),
        }
    }

    /// Without file logging nothing is counted: both level rows read 0 and
    /// there are no target rows, as the endpoint reports.
    #[test]
    fn without_log_counts_both_levels_read_zero() {
        let rendered = render_family(
            LiqFamily::Reliability,
            reliability_samples(Some(&LogWindow::default()), &[], &[]),
        );

        assert_eq!(
            rendered,
            BTreeMap::from([
                (
                    series("liq_reliability_log_count_24h", &[("level", "error")]),
                    0.0
                ),
                (
                    series("liq_reliability_log_count_24h", &[("level", "warning")]),
                    0.0
                ),
            ])
        );
    }

    /// Before the seed is done the log names are absent rather than low;
    /// the rest of the family still publishes.
    #[test]
    fn without_seeded_log_counts_the_log_names_are_absent() {
        let report = reliability_fixture();

        let rendered = render_family(
            LiqFamily::Reliability,
            reliability_samples(None, &report.failure_events, &report.job_queues),
        );

        assert!(
            rendered
                .keys()
                .all(|(name, _)| name != "liq_reliability_log_count_24h"
                    && name != "liq_log_target_count_24h"),
            "{rendered:?}"
        );
        assert_eq!(
            rendered.get(&series(
                "liq_failure_event_count_24h",
                &[("event_type", "OffchainOrderEvent::Failed")]
            )),
            Some(&2.0)
        );
        assert_eq!(rendered.len(), 9);
    }

    #[test]
    fn infra_dependencies_match_the_exporter_golden() {
        let report = infra_fixture();
        assert_fixture(&report, include_str!("testdata/infra.json"));

        let dependency_names = [
            "liq_dependency_calls_24h",
            "liq_dependency_errors_24h",
            "liq_dependency_latency_ms",
        ];
        let dependencies: BTreeMap<SeriesKey, f64> =
            render_family(LiqFamily::Infra, infra_samples(&report))
                .into_iter()
                .filter(|((name, _), _)| dependency_names.contains(&name.as_str()))
                .collect();

        assert_eq!(
            dependencies,
            golden_for(LiqFamily::Infra, include_str!("testdata/infra.prom"))
        );
    }

    #[test]
    fn rebalances_match_the_exporter_golden() {
        let (usdc, equity) = rebalance_fixture();
        assert_fixture(
            &serde_json::json!({ "usdc": usdc, "equity": equity }),
            include_str!("testdata/rebalance.json"),
        );

        assert_eq!(
            render_family(LiqFamily::Rebalances, rebalance_samples(&usdc, &equity)),
            golden_for(
                LiqFamily::Rebalances,
                include_str!("testdata/rebalance.prom")
            )
        );
    }

    /// The exporter has no working baseline for these (it fails on the
    /// per-chain report), so the expectation is written by hand: one series
    /// per hedged chain, and nothing for a value not sampled yet.
    #[test]
    fn infra_publishes_block_lag_and_poll_health_per_chain() {
        let mut report = infra_fixture();
        report.dependencies.clear();
        report.monitor.poll = vec![
            ChainPollHealth {
                chain: ChainName::Base,
                cycles: 17_280,
                errors: 4,
                skipped_ticks: 1,
                duration: Some(stats(180, 2_400, 17_280)),
            },
            ChainPollHealth {
                chain: ChainName::Robinhood,
                cycles: 0,
                errors: 0,
                skipped_ticks: 0,
                duration: None,
            },
        ];

        let base = [("chain", "base")];
        let robinhood = [("chain", "robinhood")];
        let duration = |quantile| {
            series(
                "liq_poll_duration_ms",
                &[("chain", "base"), ("quantile", quantile)],
            )
        };
        assert_eq!(
            render_family(LiqFamily::Infra, infra_samples(&report)),
            BTreeMap::from([
                (series("liq_block_lag_blocks", &base), 3.0),
                (
                    series("liq_block_lag_sampled_ts_seconds", &base),
                    1_772_366_405.0
                ),
                (series("liq_poll_cycles_24h", &base), 17_280.0),
                (series("liq_poll_errors_24h", &base), 4.0),
                (series("liq_poll_skipped_ticks_24h", &base), 1.0),
                (duration("p50"), 180.0),
                (duration("p90"), 190.0),
                (duration("p95"), 200.0),
                (duration("p99"), 210.0),
                (duration("max"), 2_400.0),
                (series("liq_poll_cycles_24h", &robinhood), 0.0),
                (series("liq_poll_errors_24h", &robinhood), 0.0),
                (series("liq_poll_skipped_ticks_24h", &robinhood), 0.0),
            ])
        );
    }

    #[test]
    fn an_empty_attestation_trend_publishes_no_attestation() {
        let (mut usdc, equity) = rebalance_fixture();
        usdc.attestation_trend.clear();

        let rendered = render_family(LiqFamily::Rebalances, rebalance_samples(&usdc, &equity));

        assert!(
            rendered
                .keys()
                .all(|(name, _)| name != "liq_attestation_last_ms"),
            "{rendered:?}"
        );
    }

    #[test]
    fn a_stage_without_samples_publishes_nothing() {
        let mut report = latencies_fixture();
        report.summary.stages = StageLatencies {
            detection: None,
            decision: None,
            submission: None,
            execution: None,
            exposure_window: None,
        };
        report.open_exposures.clear();

        assert_eq!(latency_samples(&report), Vec::new());
    }
}
