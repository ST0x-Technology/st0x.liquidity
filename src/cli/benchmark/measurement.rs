//! Round-trip phase and measurement types for benchmark observations.

use chrono::{DateTime, Utc};
use serde::Serialize;
use std::fmt::Display;
use std::time::Duration;

use crate::threshold::Usd;
use crate::tokenization::TokenizationRequestStatus;

/// Phase of a tokenization round trip being measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RoundTripPhase {
    Mint,
    Redeem,
}

impl Display for RoundTripPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mint => write!(f, "mint"),
            Self::Redeem => write!(f, "redeem"),
        }
    }
}

/// Timed observation of one phase (mint or redeem) of a round trip.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Measurement {
    pub(crate) trip: usize,
    #[serde(serialize_with = "serialize_display")]
    pub(crate) phase: RoundTripPhase,
    #[serde(serialize_with = "serialize_utc")]
    pub(crate) started_at: DateTime<Utc>,
    #[serde(serialize_with = "serialize_utc")]
    pub(crate) completed_at: DateTime<Utc>,
    #[serde(rename = "duration_secs", serialize_with = "serialize_duration")]
    pub(crate) duration: Duration,
    pub(crate) fees: Option<Usd>,
    #[serde(serialize_with = "serialize_display")]
    pub(crate) status: TokenizationRequestStatus,
}

fn serialize_display<T: Display, S: serde::Serializer>(
    value: &T,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&value.to_string())
}

fn serialize_utc<S: serde::Serializer>(
    dt: &DateTime<Utc>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
}

fn serialize_duration<S: serde::Serializer>(
    duration: &Duration,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&format!("{:.1}", duration.as_secs_f64()))
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    use super::*;

    fn make_measurement(phase: RoundTripPhase, duration_secs: f64) -> Measurement {
        let started_at = Utc.with_ymd_and_hms(2026, 2, 27, 18, 30, 0).unwrap();
        let duration = Duration::from_secs_f64(duration_secs);
        let completed_at = started_at + chrono::Duration::from_std(duration).unwrap();

        Measurement {
            trip: 1,
            phase,
            started_at,
            completed_at,
            duration,
            fees: None,
            status: TokenizationRequestStatus::Completed,
        }
    }

    #[test]
    fn round_trip_phase_display_mint() {
        assert_eq!(RoundTripPhase::Mint.to_string(), "mint");
    }

    #[test]
    fn round_trip_phase_display_redeem() {
        assert_eq!(RoundTripPhase::Redeem.to_string(), "redeem");
    }

    #[test]
    fn measurement_serializes_phase_via_display() {
        let measurement = make_measurement(RoundTripPhase::Mint, 10.0);
        let json = serde_json::to_value(&measurement).unwrap();

        assert_eq!(json["phase"], "mint");
    }

    #[test]
    fn measurement_serializes_status_via_display() {
        let measurement = make_measurement(RoundTripPhase::Redeem, 10.0);
        let json = serde_json::to_value(&measurement).unwrap();

        assert_eq!(json["status"], "completed");
    }

    #[test]
    fn measurement_serializes_utc_timestamps_without_fractional_seconds() {
        let measurement = make_measurement(RoundTripPhase::Mint, 75.0);
        let json = serde_json::to_value(&measurement).unwrap();

        assert_eq!(json["started_at"], "2026-02-27T18:30:00Z");
        assert_eq!(json["completed_at"], "2026-02-27T18:31:15Z");
    }

    #[test]
    fn measurement_serializes_duration_as_one_decimal() {
        let measurement = make_measurement(RoundTripPhase::Mint, 75.26);
        let json = serde_json::to_value(&measurement).unwrap();

        assert_eq!(json["duration_secs"], "75.3");
    }

    #[test]
    fn measurement_renames_duration_field_to_duration_secs() {
        let measurement = make_measurement(RoundTripPhase::Mint, 10.0);
        let json = serde_json::to_value(&measurement).unwrap();

        assert!(
            json.get("duration").is_none(),
            "raw 'duration' field should not exist"
        );
        assert!(
            json.get("duration_secs").is_some(),
            "'duration_secs' should exist"
        );
    }

    #[test]
    fn measurement_serializes_fees_none_as_null() {
        let measurement = make_measurement(RoundTripPhase::Mint, 10.0);
        let json = serde_json::to_value(&measurement).unwrap();

        assert!(json["fees"].is_null());
    }

    #[test]
    fn measurement_serializes_fees_some_as_value() {
        let mut measurement = make_measurement(RoundTripPhase::Mint, 10.0);
        measurement.fees = Some(Usd(dec!(1.50)));
        let json = serde_json::to_value(&measurement).unwrap();

        assert_eq!(json["fees"], "1.50");
    }
}
