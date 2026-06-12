//! Performance tab DTOs: hedge latency pipeline and rebalance timing metrics.

use chrono::{DateTime, Utc};
use serde::Serialize;
use std::str::FromStr;
use strum::VariantArray;
use thiserror::Error;
use ts_rs::TS;
use uuid::Uuid;

use st0x_finance::{Symbol, Usdc};

use crate::UsdcBridgeDirection;

/// Response of `GET /performance/latencies`.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct HedgeLatencies {
    pub summary: LatencySummary,
    /// Per-stage percentiles bucketed over time, oldest bucket first.
    pub buckets: Vec<LatencyBucket>,
    /// Most recent hedge cycles (waterfall rows), newest first.
    pub cycles: Vec<HedgeCycleReport>,
    /// Total cycles in range before the `cycles` cap was applied.
    #[ts(type = "number")]
    pub total_cycles: usize,
    /// Symbols currently carrying fills not yet covered by a hedge.
    pub open_exposures: Vec<OpenExposureReport>,
}

/// Whole-range latency aggregates.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct LatencySummary {
    #[ts(type = "number")]
    pub fill_count: usize,
    pub stages: StageLatencies,
}

/// Percentiles for each stage of the hedge pipeline. A stage is `null` when
/// no sample in the window reached it.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct StageLatencies {
    pub detection: Option<LatencyStats>,
    pub decision: Option<LatencyStats>,
    pub submission: Option<LatencyStats>,
    pub execution: Option<LatencyStats>,
    pub exposure_window: Option<LatencyStats>,
}

/// Nearest-rank percentiles over a set of latency samples, in milliseconds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct LatencyStats {
    #[ts(type = "number")]
    pub p50_ms: i64,
    #[ts(type = "number")]
    pub p90_ms: i64,
    #[ts(type = "number")]
    pub p95_ms: i64,
    #[ts(type = "number")]
    pub p99_ms: i64,
    #[ts(type = "number")]
    pub max_ms: i64,
    #[ts(type = "number")]
    pub sample_count: usize,
}

/// Per-stage percentiles within one time bucket.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct LatencyBucket {
    pub start: DateTime<Utc>,
    pub stages: StageLatencies,
}

/// One hedge order's trip through the pipeline: a waterfall row.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct HedgeCycleReport {
    #[ts(type = "string")]
    pub symbol: Symbol,
    #[ts(type = "string")]
    pub offchain_order_id: Uuid,
    pub placed_at: DateTime<Utc>,
    #[ts(type = "number")]
    pub covered_fill_count: usize,
    pub earliest_fill_block_timestamp: Option<DateTime<Utc>>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub status: HedgeCycleStatus,
    /// Broker fill or failure time, depending on `status`.
    pub completed_at: Option<DateTime<Utc>>,
    #[ts(type = "number | null")]
    pub decision_ms: Option<i64>,
    #[ts(type = "number | null")]
    pub submission_ms: Option<i64>,
    #[ts(type = "number | null")]
    pub execution_ms: Option<i64>,
    #[ts(type = "number | null")]
    pub exposure_window_ms: Option<i64>,
}

/// Terminal state of a hedge cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum HedgeCycleStatus {
    Pending,
    Filled,
    Failed,
}

/// Fills observed after a symbol's most recent hedge placement: exposure the
/// system carries unhedged right now.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct OpenExposureReport {
    #[ts(type = "string")]
    pub symbol: Symbol,
    #[ts(type = "number")]
    pub fill_count: usize,
    pub oldest_fill_block_timestamp: DateTime<Utc>,
}

/// Response of `GET /performance/rebalances`.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RebalanceTimings {
    /// Most recent operations (stage breakdown rows), newest first.
    pub operations: Vec<RebalanceOperationTiming>,
    /// Total operations in range before the `operations` cap was applied.
    #[ts(type = "number")]
    pub total_operations: usize,
    /// Operations dropped because their stored timing JSON failed to
    /// deserialize. They are excluded from every field above; this count
    /// surfaces the gap so a malformed read-model row is not silently invisible.
    #[ts(type = "number")]
    pub skipped_operations: u32,
    /// Percentiles per stage across all operations in range.
    ///
    /// SPARSE: a stage with no completed (`Succeeded`) samples in range is
    /// omitted entirely rather than emitted with an empty/zero entry. Consumers
    /// must look stages up by name and tolerate absence, not index positionally.
    pub stage_summary: Vec<RebalanceStageStats>,
    /// CCTP attestation duration over time, oldest first.
    pub attestation_trend: Vec<AttestationSample>,
}

/// One USDC rebalance operation's per-stage timing breakdown.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RebalanceOperationTiming {
    #[ts(type = "string")]
    pub operation_id: Uuid,
    pub direction: Option<UsdcBridgeDirection>,
    #[ts(type = "string | null")]
    pub amount: Option<Usdc>,
    /// Genuine operation start (the first-phase conversion or withdrawal event).
    /// `null` when the read model first observed the operation mid-stream
    /// (e.g. after a deploy), in which case its start time is unknown and
    /// `total_ms` is unmeasured.
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub status: RebalanceTimingStatus,
    pub stages: Vec<RebalanceStageTiming>,
    /// Genuine start to terminal success, when both endpoints are known.
    /// `null` when the operation is unfinished, or when `started_at` is unknown
    /// (mid-stream first observation), or when completion was an out-of-band
    /// `OperatorReconciled` (whose manual-response window must not pollute
    /// round-trip latency metrics).
    #[ts(type = "number | null")]
    pub total_ms: Option<i64>,
}

/// Where a rebalance operation currently stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum RebalanceTimingStatus {
    InProgress,
    Completed,
    Failed,
}

/// Timing of one stage within a rebalance operation.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RebalanceStageTiming {
    pub stage: RebalanceStageName,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    #[ts(type = "number | null")]
    pub duration_ms: Option<i64>,
    pub outcome: StageOutcome,
}

/// How a rebalance stage run ended.
///
/// Only `Succeeded` runs carry a meaningful `duration_ms` for percentile
/// aggregation. `Failed` runs are timed but excluded from latency stats so a
/// failure does not contaminate the metric. `Unmeasured` runs demonstrably
/// happened but have no measurable duration (e.g. a burn with no submitting
/// intent, or a mint discovered after the fact by recovery); they carry a
/// `null` `duration_ms` and never enter percentiles. An in-progress stage that
/// has not yet ended also reports `Unmeasured` until it closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum StageOutcome {
    Succeeded,
    Failed,
    Unmeasured,
}

/// Stages of the USDC rebalance pipeline, in flow order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum RebalanceStageName {
    Conversion,
    Withdrawal,
    Burn,
    Attestation,
    Mint,
    Deposit,
}

/// Percentiles for one rebalance stage.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RebalanceStageStats {
    pub stage: RebalanceStageName,
    pub stats: LatencyStats,
}

/// One completed CCTP attestation: burn time and how long Circle took.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct AttestationSample {
    pub burned_at: DateTime<Utc>,
    #[ts(type = "number")]
    pub duration_ms: i64,
}

/// Response of `GET /performance/reliability`.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ReliabilityReport {
    /// Error/warning log volume over time, oldest bucket first.
    pub log_buckets: Vec<LogVolumeBucket>,
    /// Error/warning counts per log target, highest count first.
    pub log_targets: Vec<LogTargetCount>,
    /// Money-at-risk lifecycle failure events in range, by event type.
    pub failure_events: Vec<FailureEventCount>,
    /// Current job queue health per job type. This is an instantaneous
    /// snapshot of the queue, NOT windowed by the report range: counts span
    /// all rows currently in the `Jobs` table regardless of `from`/`to`, so a
    /// short range shows all-time `done`/`failed` next to windowed failures
    /// and logs. Queue types with no rows are absent rather than zero.
    pub job_queues: Vec<JobQueueHealth>,
    /// `true` when the error/warning log scan hit its entry cap, so
    /// `log_buckets` and `log_targets` undercount the noisiest windows. Lets
    /// the dashboard render an honest partial-data state. Failure events are
    /// aggregated in SQL and are never capped.
    pub log_entries_truncated: bool,
}

/// Error and warning counts within one time bucket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct LogVolumeBucket {
    pub start: DateTime<Utc>,
    #[ts(type = "number")]
    pub errors: usize,
    #[ts(type = "number")]
    pub warnings: usize,
}

/// Log level counted by the reliability report. Only errors and warnings are
/// aggregated; other levels never reach the report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, TS)]
#[serde(rename_all = "UPPERCASE")]
pub enum CountedLogLevel {
    Error,
    Warn,
}

/// How often one log target emitted at one level.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct LogTargetCount {
    pub target: String,
    pub level: CountedLogLevel,
    #[ts(type = "number")]
    pub count: usize,
    /// Per-bucket counts aligned with the report's `log_buckets`.
    #[ts(type = "number[]")]
    pub sparkline: Vec<usize>,
}

/// Money-at-risk lifecycle failure event types surfaced by the reliability
/// report.
///
/// Serialized exactly as the event store's `event_type` discriminator
/// (`AggregateEvent::Variant`), so the wire format matches what operators
/// see in the database and logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, TS, VariantArray)]
pub enum FailureEventType {
    #[serde(rename = "OffchainOrderEvent::Failed")]
    OffchainOrderFailed,
    #[serde(rename = "UsdcRebalanceEvent::ConversionFailed")]
    ConversionFailed,
    #[serde(rename = "UsdcRebalanceEvent::WithdrawalFailed")]
    WithdrawalFailed,
    #[serde(rename = "UsdcRebalanceEvent::BridgingFailed")]
    BridgingFailed,
    #[serde(rename = "UsdcRebalanceEvent::DepositFailed")]
    DepositFailed,
    #[serde(rename = "UsdcRebalanceEvent::AttestationTimedOut")]
    AttestationTimedOut,
    #[serde(rename = "EquityRedemptionEvent::TransferFailed")]
    RedemptionTransferFailed,
    #[serde(rename = "EquityRedemptionEvent::DetectionFailed")]
    RedemptionDetectionFailed,
    #[serde(rename = "EquityRedemptionEvent::RedemptionRejected")]
    RedemptionRejected,
    #[serde(rename = "TokenizedEquityMintEvent::MintRejected")]
    MintRejected,
    #[serde(rename = "TokenizedEquityMintEvent::MintAcceptanceFailed")]
    MintAcceptanceFailed,
    #[serde(rename = "TokenizedEquityMintEvent::WrappingFailed")]
    WrappingFailed,
    #[serde(rename = "TokenizedEquityMintEvent::RaindexDepositFailed")]
    RaindexDepositFailed,
}

impl FailureEventType {
    /// Every failure event type, derived by macro so the list itself cannot
    /// drift from the enum. Used for `FromStr` exhaustiveness and coverage
    /// checks; unrecognized rows are filtered in Rust (skip-and-warn), NOT
    /// via a SQL IN(...) allowlist.
    pub const ALL: &'static [Self] = <Self as VariantArray>::VARIANTS;

    /// The event store's `event_type` discriminator for this failure. Must
    /// match the `#[serde(rename)]` strings; a unit test pins the two
    /// together.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OffchainOrderFailed => "OffchainOrderEvent::Failed",
            Self::ConversionFailed => "UsdcRebalanceEvent::ConversionFailed",
            Self::WithdrawalFailed => "UsdcRebalanceEvent::WithdrawalFailed",
            Self::BridgingFailed => "UsdcRebalanceEvent::BridgingFailed",
            Self::DepositFailed => "UsdcRebalanceEvent::DepositFailed",
            Self::AttestationTimedOut => "UsdcRebalanceEvent::AttestationTimedOut",
            Self::RedemptionTransferFailed => "EquityRedemptionEvent::TransferFailed",
            Self::RedemptionDetectionFailed => "EquityRedemptionEvent::DetectionFailed",
            Self::RedemptionRejected => "EquityRedemptionEvent::RedemptionRejected",
            Self::MintRejected => "TokenizedEquityMintEvent::MintRejected",
            Self::MintAcceptanceFailed => "TokenizedEquityMintEvent::MintAcceptanceFailed",
            Self::WrappingFailed => "TokenizedEquityMintEvent::WrappingFailed",
            Self::RaindexDepositFailed => "TokenizedEquityMintEvent::RaindexDepositFailed",
        }
    }
}

/// An `event_type` string that does not name a known failure event type.
/// Carries no payload (per the no-opaque-String-errors rule); callers
/// already hold the offending input and log it themselves.
#[derive(Debug, Error)]
#[error("unknown failure event type")]
pub struct UnknownFailureEventType;

impl FromStr for FailureEventType {
    type Err = UnknownFailureEventType;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .iter()
            .copied()
            .find(|event_type| event_type.as_str() == input)
            .ok_or(UnknownFailureEventType)
    }
}

/// Occurrences of one lifecycle failure event type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct FailureEventCount {
    pub event_type: FailureEventType,
    #[ts(type = "number")]
    pub count: usize,
    pub last_at: DateTime<Utc>,
}

/// Snapshot of one apalis job queue's health.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct JobQueueHealth {
    pub job_type: String,
    #[ts(type = "number")]
    pub pending: usize,
    #[ts(type = "number")]
    pub running: usize,
    #[ts(type = "number")]
    pub done: usize,
    /// Terminal failures from retry exhaustion. apalis writes status `Killed`
    /// (not `Failed`) once a job exhausts `max_attempts`, carrying
    /// `attempts >= max_attempts`, so this counts `Killed AND attempts >=
    /// max_attempts`. Disjoint from `awaiting_retry` and `killed`.
    #[ts(type = "number")]
    pub failed: usize,
    /// Failed jobs apalis will retry (status `Failed`, attempts <
    /// max_attempts): live backlog, also included in `oldest_pending_run_at`.
    #[ts(type = "number")]
    pub awaiting_retry: usize,
    /// Jobs killed before exhausting retries (status `Killed`, attempts <
    /// max_attempts), e.g. an explicit non-retryable abort. Disjoint from the
    /// exhaustion-driven `failed` count.
    #[ts(type = "number")]
    pub killed: usize,
    /// Jobs that needed more than one attempt.
    #[ts(type = "number")]
    pub retried: usize,
    pub oldest_pending_run_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn hedge_cycle_report_serializes_camel_case() {
        let report = HedgeCycleReport {
            symbol: Symbol::new("AAPL").unwrap(),
            offchain_order_id: "11111111-2222-3333-4444-555555555555".parse().unwrap(),
            placed_at: DateTime::from_timestamp(1_750_000_000, 0).unwrap(),
            covered_fill_count: 3,
            earliest_fill_block_timestamp: DateTime::from_timestamp(1_749_999_990, 0),
            submitted_at: DateTime::from_timestamp(1_750_000_001, 0),
            status: HedgeCycleStatus::Filled,
            completed_at: DateTime::from_timestamp(1_750_000_003, 0),
            decision_ms: Some(2_000),
            submission_ms: Some(1_000),
            execution_ms: Some(2_000),
            exposure_window_ms: Some(13_000),
        };

        let json = serde_json::to_value(&report).expect("serialization should succeed");
        assert_eq!(json["symbol"], json!("AAPL"));
        assert_eq!(json["coveredFillCount"], json!(3));
        assert_eq!(json["status"], json!("filled"));
        assert_eq!(json["decisionMs"], json!(2000));
        assert_eq!(json["exposureWindowMs"], json!(13000));
        assert_eq!(json["placedAt"], json!("2025-06-15T15:06:40Z"));
    }

    #[test]
    fn hedge_cycle_report_serializes_null_optional_fields() {
        let report = HedgeCycleReport {
            symbol: Symbol::new("AAPL").unwrap(),
            offchain_order_id: "11111111-2222-3333-4444-555555555555".parse().unwrap(),
            placed_at: DateTime::from_timestamp(1_750_000_000, 0).unwrap(),
            covered_fill_count: 0,
            earliest_fill_block_timestamp: None,
            submitted_at: None,
            status: HedgeCycleStatus::Pending,
            completed_at: None,
            decision_ms: None,
            submission_ms: None,
            execution_ms: None,
            exposure_window_ms: None,
        };

        let json = serde_json::to_value(&report).expect("serialization should succeed");
        assert_eq!(json["earliestFillBlockTimestamp"], json!(null));
        assert_eq!(json["submittedAt"], json!(null));
        assert_eq!(json["completedAt"], json!(null));
        assert_eq!(json["decisionMs"], json!(null));
        assert_eq!(json["submissionMs"], json!(null));
        assert_eq!(json["executionMs"], json!(null));
        assert_eq!(json["exposureWindowMs"], json!(null));
        assert_eq!(json["status"], json!("pending"));
    }

    #[test]
    fn rebalance_operation_timing_serializes_camel_case() {
        let operation = RebalanceOperationTiming {
            operation_id: "11111111-2222-3333-4444-555555555555".parse().unwrap(),
            direction: Some(UsdcBridgeDirection::AlpacaToBase),
            amount: Some(Usdc::new(st0x_float_macro::float!(1000))),
            started_at: DateTime::from_timestamp(1_750_000_000, 0),
            completed_at: DateTime::from_timestamp(1_750_000_730, 0),
            status: RebalanceTimingStatus::Completed,
            stages: vec![RebalanceStageTiming {
                stage: RebalanceStageName::Attestation,
                started_at: DateTime::from_timestamp(1_750_000_070, 0).unwrap(),
                ended_at: DateTime::from_timestamp(1_750_000_670, 0),
                duration_ms: Some(600_000),
                outcome: StageOutcome::Succeeded,
            }],
            total_ms: Some(730_000),
        };

        let json = serde_json::to_value(&operation).expect("serialization should succeed");
        assert_eq!(
            json["operationId"],
            json!("11111111-2222-3333-4444-555555555555")
        );
        assert_eq!(json["direction"], json!("alpaca_to_base"));
        assert_eq!(json["amount"], json!("1000"));
        assert_eq!(json["startedAt"], json!("2025-06-15T15:06:40Z"));
        assert_eq!(json["completedAt"], json!("2025-06-15T15:18:50Z"));
        assert_eq!(json["status"], json!("completed"));
        assert_eq!(json["totalMs"], json!(730_000));
        assert_eq!(json["stages"][0]["stage"], json!("attestation"));
        assert_eq!(
            json["stages"][0]["startedAt"],
            json!("2025-06-15T15:07:50Z")
        );
        assert_eq!(json["stages"][0]["endedAt"], json!("2025-06-15T15:17:50Z"));
        assert_eq!(json["stages"][0]["durationMs"], json!(600_000));
        assert_eq!(json["stages"][0]["outcome"], json!("succeeded"));
    }

    #[test]
    fn rebalance_operation_timing_serializes_null_optional_fields() {
        let operation = RebalanceOperationTiming {
            operation_id: "22222222-3333-4444-5555-666666666666".parse().unwrap(),
            direction: None,
            amount: None,
            started_at: None,
            completed_at: None,
            status: RebalanceTimingStatus::InProgress,
            stages: Vec::new(),
            total_ms: None,
        };

        let json = serde_json::to_value(&operation).expect("serialization should succeed");
        assert_eq!(json["direction"], json!(null));
        assert_eq!(json["amount"], json!(null));
        assert_eq!(json["startedAt"], json!(null));
        assert_eq!(json["completedAt"], json!(null));
        assert_eq!(json["totalMs"], json!(null));
        assert_eq!(json["status"], json!("in_progress"));
    }

    #[test]
    fn stage_outcome_serializes_snake_case() {
        assert_eq!(
            serde_json::to_value(StageOutcome::Succeeded).unwrap(),
            json!("succeeded")
        );
        assert_eq!(
            serde_json::to_value(StageOutcome::Failed).unwrap(),
            json!("failed")
        );
        assert_eq!(
            serde_json::to_value(StageOutcome::Unmeasured).unwrap(),
            json!("unmeasured")
        );
    }

    #[test]
    // Each variant is pinned to an INDEPENDENT string literal (not `as_str()`),
    // so a copy-paste bug that corrupts both the serde rename and the matching
    // `as_str` arm for one variant is still caught -- comparing serde output
    // against `as_str()` would test the crate against itself and pass. Scope
    // note: this pins serde output only; the binding against the live event
    // store discriminators is failure_event_types_match_domain_event_names in
    // src/performance/reliability.rs, which samples every variant.
    fn failure_event_type_serializes_to_literal_discriminator() {
        use FailureEventType::*;

        let serialize = |event_type| serde_json::to_value::<FailureEventType>(event_type).unwrap();

        assert_eq!(
            serialize(OffchainOrderFailed),
            json!("OffchainOrderEvent::Failed")
        );
        assert_eq!(
            serialize(ConversionFailed),
            json!("UsdcRebalanceEvent::ConversionFailed")
        );
        assert_eq!(
            serialize(WithdrawalFailed),
            json!("UsdcRebalanceEvent::WithdrawalFailed")
        );
        assert_eq!(
            serialize(BridgingFailed),
            json!("UsdcRebalanceEvent::BridgingFailed")
        );
        assert_eq!(
            serialize(DepositFailed),
            json!("UsdcRebalanceEvent::DepositFailed")
        );
        assert_eq!(
            serialize(AttestationTimedOut),
            json!("UsdcRebalanceEvent::AttestationTimedOut")
        );
        assert_eq!(
            serialize(RedemptionTransferFailed),
            json!("EquityRedemptionEvent::TransferFailed")
        );
        assert_eq!(
            serialize(RedemptionDetectionFailed),
            json!("EquityRedemptionEvent::DetectionFailed")
        );
        assert_eq!(
            serialize(RedemptionRejected),
            json!("EquityRedemptionEvent::RedemptionRejected")
        );
        assert_eq!(
            serialize(MintRejected),
            json!("TokenizedEquityMintEvent::MintRejected")
        );
        assert_eq!(
            serialize(MintAcceptanceFailed),
            json!("TokenizedEquityMintEvent::MintAcceptanceFailed")
        );
        assert_eq!(
            serialize(WrappingFailed),
            json!("TokenizedEquityMintEvent::WrappingFailed")
        );
        assert_eq!(
            serialize(RaindexDepositFailed),
            json!("TokenizedEquityMintEvent::RaindexDepositFailed")
        );
    }

    #[test]
    fn failure_event_type_round_trips_through_from_str() {
        for event_type in FailureEventType::ALL.iter().copied() {
            assert_eq!(
                FailureEventType::from_str(event_type.as_str()).unwrap(),
                event_type
            );
        }
    }

    #[test]
    fn failure_event_type_rejects_unknown_strings() {
        assert!(matches!(
            FailureEventType::from_str("PositionEvent::Opened"),
            Err(UnknownFailureEventType)
        ));
    }

    #[test]
    fn counted_log_level_serializes_uppercase() {
        assert_eq!(
            serde_json::to_value(CountedLogLevel::Error).unwrap(),
            json!("ERROR")
        );
        assert_eq!(
            serde_json::to_value(CountedLogLevel::Warn).unwrap(),
            json!("WARN")
        );
    }

    #[test]
    fn log_target_count_serializes_camel_case() {
        let row = LogTargetCount {
            target: "hedge".to_string(),
            level: CountedLogLevel::Warn,
            count: 2,
            sparkline: vec![1, 1],
        };

        let json = serde_json::to_value(&row).expect("serialization should succeed");
        assert_eq!(json["target"], json!("hedge"));
        assert_eq!(json["level"], json!("WARN"));
        assert_eq!(json["count"], json!(2));
        assert_eq!(json["sparkline"], json!([1, 1]));
    }

    #[test]
    fn failure_event_count_serializes_event_type_discriminator() {
        let row = FailureEventCount {
            event_type: FailureEventType::OffchainOrderFailed,
            count: 3,
            last_at: DateTime::from_timestamp(1_750_000_000, 0).unwrap(),
        };

        let json = serde_json::to_value(&row).expect("serialization should succeed");
        assert_eq!(json["eventType"], json!("OffchainOrderEvent::Failed"));
        assert_eq!(json["count"], json!(3));
        assert_eq!(json["lastAt"], json!("2025-06-15T15:06:40Z"));
    }

    #[test]
    fn stage_latencies_serialize_null_for_unreached_stages() {
        let stages = StageLatencies {
            detection: Some(LatencyStats {
                p50_ms: 10,
                p90_ms: 20,
                p95_ms: 25,
                p99_ms: 30,
                max_ms: 31,
                sample_count: 5,
            }),
            decision: None,
            submission: None,
            execution: None,
            exposure_window: None,
        };

        let json = serde_json::to_value(&stages).expect("serialization should succeed");
        assert_eq!(json["detection"]["p50Ms"], json!(10));
        assert_eq!(json["detection"]["sampleCount"], json!(5));
        assert_eq!(json["decision"], json!(null));
        assert_eq!(json["exposureWindow"], json!(null));
    }
}
