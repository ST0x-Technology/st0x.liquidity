//! Threshold configuration for inventory imbalance detection.
//!
//! Lives here transitionally; once Phase 3 extracts the rebalance
//! domain, this type should move alongside the inventory and trigger
//! logic that consumes it.

use std::sync::LazyLock;

use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use st0x_float_macro::float;

static EXACT_ONE: LazyLock<Float> = LazyLock::new(|| float!(1));

/// Threshold configuration for imbalance detection.
///
/// Invariants enforced by [`ImbalanceThreshold::new`]:
/// - `target` must be in `[0.0, 1.0]`
/// - `deviation` must be `>= 0`
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(try_from = "RawImbalanceThreshold", deny_unknown_fields)]
pub struct ImbalanceThreshold {
    /// Target ratio of onchain to total (e.g., 0.5 for 50/50 split).
    #[serde(
        serialize_with = "st0x_float_serde::serialize_float_as_string",
        deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
    )]
    pub target: Float,
    /// Deviation from target that triggers rebalancing.
    #[serde(
        serialize_with = "st0x_float_serde::serialize_float_as_string",
        deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string"
    )]
    pub deviation: Float,
}

/// Error returned when [`ImbalanceThreshold`] is constructed with
/// out-of-range values.
#[derive(Debug, Clone, thiserror::Error)]
pub enum InvalidImbalanceThreshold {
    #[error(
        "target must be between 0.0 and 1.0 inclusive, \
         got {target:?}"
    )]
    TargetOutOfRange { target: Float },
    #[error("deviation must be >= 0, got {deviation:?}")]
    NegativeDeviation { deviation: Float },
}

impl ImbalanceThreshold {
    /// Creates a new threshold with validated parameters.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidImbalanceThreshold`] if `target` is not in
    /// `[0.0, 1.0]` or `deviation` is negative.
    pub fn new(target: Float, deviation: Float) -> Result<Self, InvalidImbalanceThreshold> {
        let zero =
            Float::zero().map_err(|_| InvalidImbalanceThreshold::TargetOutOfRange { target })?;

        if target
            .lt(zero)
            .map_err(|_| InvalidImbalanceThreshold::TargetOutOfRange { target })?
            || target
                .gt(*EXACT_ONE)
                .map_err(|_| InvalidImbalanceThreshold::TargetOutOfRange { target })?
        {
            return Err(InvalidImbalanceThreshold::TargetOutOfRange { target });
        }

        if deviation
            .lt(zero)
            .map_err(|_| InvalidImbalanceThreshold::NegativeDeviation { deviation })?
        {
            return Err(InvalidImbalanceThreshold::NegativeDeviation { deviation });
        }

        Ok(Self { target, deviation })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawImbalanceThreshold {
    #[serde(deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string")]
    target: Float,
    #[serde(deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string")]
    deviation: Float,
}

impl TryFrom<RawImbalanceThreshold> for ImbalanceThreshold {
    type Error = InvalidImbalanceThreshold;

    fn try_from(raw: RawImbalanceThreshold) -> Result<Self, Self::Error> {
        Self::new(raw.target, raw.deviation)
    }
}
