//! Performance tab DTOs: hedge latency pipeline metrics.

use chrono::{DateTime, Utc};
use serde::Serialize;
use ts_rs::TS;
use uuid::Uuid;

use st0x_finance::Symbol;

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
