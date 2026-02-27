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
