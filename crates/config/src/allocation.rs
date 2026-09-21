//! Equity allocation across chains: the per-chain target shares, the broker
//! floor and the sizing rules the allocation planner reads.

use rain_math_float::{Float, FloatError};
use serde::{Deserialize, Deserializer};
use std::collections::BTreeMap;
use std::sync::LazyLock;
use std::time::Duration;

use st0x_evm::Chain;
use st0x_execution::Positive;
use st0x_finance::Usdc;
use st0x_float_macro::float;
use st0x_float_serde::{deserialize_float_from_number_or_string, format_float_with_fallback};

static EXACT_ONE: LazyLock<Float> = LazyLock::new(|| float!(1));

/// The fraction of a symbol's total inventory that belongs at one venue,
/// in `[0, 1]`.
#[derive(Debug, Clone, Copy)]
pub struct TargetShare(Float);

impl TargetShare {
    /// # Errors
    ///
    /// Returns [`AllocationConfigError::TargetShareOutOfRange`] outside `[0, 1]`.
    pub fn new(value: Float) -> Result<Self, AllocationConfigError> {
        let out_of_range = value.lt(Float::zero()?)? || value.gt(*EXACT_ONE)?;

        if out_of_range {
            return Err(AllocationConfigError::TargetShareOutOfRange { value });
        }

        Ok(Self(value))
    }

    pub fn inner(self) -> Float {
        self.0
    }
}

impl<'de> Deserialize<'de> for TargetShare {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = deserialize_float_from_number_or_string(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// How far a chain's actual share may drift from its target before the
/// planner acts, as a non-negative fraction of the total.
#[derive(Debug, Clone, Copy)]
pub struct DeviationBand(Float);

impl DeviationBand {
    /// # Errors
    ///
    /// Returns [`AllocationConfigError::NegativeDeviationBand`] below zero.
    pub fn new(value: Float) -> Result<Self, AllocationConfigError> {
        if value.lt(Float::zero()?)? {
            return Err(AllocationConfigError::NegativeDeviationBand { value });
        }

        Ok(Self(value))
    }

    pub fn inner(self) -> Float {
        self.0
    }
}

impl<'de> Deserialize<'de> for DeviationBand {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = deserialize_float_from_number_or_string(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// The `[rebalancing.allocation]` table.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllocationConfig {
    /// Default target share of every equity's total inventory per chain. A
    /// chain's equity can override its entry with `target_share`.
    pub targets: BTreeMap<Chain, TargetShare>,
    /// The share of every equity's total that stays at the broker: a mint
    /// never takes the broker's available shares below it.
    pub alpaca_floor: TargetShare,
    /// The band around each chain's target inside which no operation runs.
    pub deviation: DeviationBand,
    /// The smallest transfer worth its gas, valued at the last hedge price. A
    /// chain's trading table can override it with its own `min_operation_usd`.
    pub min_operation_usd: Positive<Usdc>,
    /// How long a `(symbol, chain)` pair is not re-planned after an operation.
    pub cooldown_secs: u64,
}

/// [`AllocationConfig`] after validation, with durations resolved.
#[derive(Debug, Clone)]
pub struct AllocationCtx {
    pub targets: BTreeMap<Chain, TargetShare>,
    pub alpaca_floor: TargetShare,
    pub deviation: DeviationBand,
    pub min_operation_usd: Positive<Usdc>,
    pub cooldown: Duration,
}

impl AllocationCtx {
    /// # Errors
    ///
    /// Returns [`AllocationConfigError::ZeroCooldown`] for a zero cooldown.
    pub fn new(config: &AllocationConfig) -> Result<Self, AllocationConfigError> {
        if config.cooldown_secs == 0 {
            return Err(AllocationConfigError::ZeroCooldown);
        }

        Ok(Self {
            targets: config.targets.clone(),
            alpaca_floor: config.alpaca_floor,
            deviation: config.deviation,
            min_operation_usd: config.min_operation_usd,
            cooldown: Duration::from_secs(config.cooldown_secs),
        })
    }
}

/// Why an allocation config was refused.
#[derive(Debug, thiserror::Error)]
pub enum AllocationConfigError {
    #[error(
        "target share must be between 0 and 1 inclusive, got {}",
        format_float_with_fallback(value)
    )]
    TargetShareOutOfRange { value: Float },
    #[error(
        "deviation band must be >= 0, got {}",
        format_float_with_fallback(value)
    )]
    NegativeDeviationBand { value: Float },
    #[error("[rebalancing.allocation] cooldown_secs must be non-zero")]
    ZeroCooldown,
    #[error(transparent)]
    Float(#[from] FloatError),
}
