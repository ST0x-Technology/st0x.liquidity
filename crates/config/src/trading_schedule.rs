//! Explicit coordination settings for broker-close hedging.

use std::collections::HashSet;
use std::num::NonZeroU64;

use serde::Deserialize;
use thiserror::Error;

use crate::assets::{HedgingAssets, OperationMode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradingScheduleMode {
    Observe,
    Enabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradingScheduleEnvironment {
    Staging,
    Production,
}

impl TradingScheduleEnvironment {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Staging => "staging",
            Self::Production => "production",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradingScheduleScope {
    pub id: String,
    pub profile_revision: String,
    pub extended_hours: bool,
    pub assets: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradingScheduleConfig {
    pub mode: TradingScheduleMode,
    pub environment: TradingScheduleEnvironment,
    pub poll_interval_secs: NonZeroU64,
    pub request_timeout_secs: NonZeroU64,
    pub response_freshness_secs: NonZeroU64,
    pub calendar_max_age_secs: NonZeroU64,
    pub evidence_clock_skew_secs: NonZeroU64,
    pub emergency_buffer_secs: NonZeroU64,
    pub scopes: Vec<TradingScheduleScope>,
}

impl TradingScheduleConfig {
    pub(crate) fn validate(
        &self,
        assets: &HedgingAssets,
    ) -> Result<(), TradingScheduleConfigError> {
        let durations = [
            self.poll_interval_secs,
            self.request_timeout_secs,
            self.response_freshness_secs,
            self.calendar_max_age_secs,
            self.evidence_clock_skew_secs,
            self.emergency_buffer_secs,
        ];
        if durations.iter().any(|duration| duration.get() > 86_400)
            || self.request_timeout_secs >= self.poll_interval_secs
            || self.poll_interval_secs >= self.response_freshness_secs
        {
            return Err(TradingScheduleConfigError::Timing);
        }
        let mut scopes = HashSet::new();
        let mut mapped = HashSet::new();
        for scope in &self.scopes {
            if scope.id.trim().is_empty()
                || scope.profile_revision.trim().is_empty()
                || scope.assets.is_empty()
                || !scopes.insert(&scope.id)
            {
                return Err(TradingScheduleConfigError::Scope);
            }
            for asset in &scope.assets {
                let symbol = st0x_execution::Symbol::new(asset.clone())
                    .map_err(TradingScheduleMembershipError::from)?;
                let Some(policy) = assets.equities.symbols.get(&symbol) else {
                    return Err(TradingScheduleMembershipError::Unknown(symbol).into());
                };
                let extended = match policy.extended_hours_counter_trading {
                    OperationMode::Enabled => true,
                    OperationMode::Disabled => false,
                };
                if asset.as_str() != symbol.as_str() {
                    return Err(TradingScheduleMembershipError::NonCanonical(symbol).into());
                }
                if !mapped.insert(symbol.clone()) {
                    return Err(TradingScheduleMembershipError::Duplicate(symbol).into());
                }
                if extended != scope.extended_hours {
                    return Err(TradingScheduleMembershipError::Eligibility(symbol).into());
                }
            }
        }
        if mapped.len() != assets.equities.symbols.len() {
            return Err(TradingScheduleMembershipError::Incomplete.into());
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum TradingScheduleConfigError {
    #[error("schedule durations must be at most one day, with timeout < poll < freshness")]
    Timing,
    #[error("schedule scopes require unique nonempty identities, profiles, and memberships")]
    Scope,
    #[error(transparent)]
    Membership(#[from] TradingScheduleMembershipError),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TradingScheduleMembershipError {
    #[error("schedule membership is missing configured equities")]
    Incomplete,
    #[error("schedule membership contains an empty symbol")]
    EmptySymbol(#[from] st0x_execution::EmptySymbolError),
    #[error("schedule membership names an unconfigured equity: {0}")]
    Unknown(st0x_execution::Symbol),
    #[error("schedule membership must use the canonical equity symbol: {0}")]
    NonCanonical(st0x_execution::Symbol),
    #[error("schedule membership names an equity more than once: {0}")]
    Duplicate(st0x_execution::Symbol),
    #[error("schedule eligibility differs from the equity policy: {0}")]
    Eligibility(st0x_execution::Symbol),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assets::EquityHedgePolicy;

    fn config() -> TradingScheduleConfig {
        toml::from_str(
            r#"
            mode = "observe"
            environment = "staging"
            poll_interval_secs = 5
            request_timeout_secs = 3
            response_freshness_secs = 30
            calendar_max_age_secs = 7200
            evidence_clock_skew_secs = 2
            emergency_buffer_secs = 900
            scopes = []
        "#,
        )
        .unwrap()
    }

    #[test]
    fn rollout_fragments_match_runtime_asset_eligibility() {
        for (runtime, fragment) in [
            (
                include_str!("../../../config/prod/st0x-hedge.toml"),
                include_str!("../../../docs/trading-schedule/prod.toml"),
            ),
            (
                include_str!("../../../config/staging/st0x-hedge.toml"),
                include_str!("../../../docs/trading-schedule/staging.toml"),
            ),
        ] {
            let runtime: toml::Value = toml::from_str(runtime).unwrap();
            let assets: HedgingAssets = runtime["assets"].clone().try_into().unwrap();
            let fragment: toml::Value = toml::from_str(fragment).unwrap();
            let config: TradingScheduleConfig = fragment["pricing"]["trading_schedule"]
                .clone()
                .try_into()
                .unwrap();
            assert_eq!(config.mode, TradingScheduleMode::Observe);
            config.validate(&assets).unwrap();
        }
    }

    #[test]
    fn timeout_must_finish_before_the_next_poll() {
        let mut config = config();
        config.request_timeout_secs = config.poll_interval_secs;
        assert!(matches!(
            config.validate(&HedgingAssets::default()).unwrap_err(),
            TradingScheduleConfigError::Timing
        ));
    }

    #[test]
    fn timing_limits_accept_one_day_but_reject_every_longer_duration() {
        let mut maximum = config();
        maximum.request_timeout_secs = NonZeroU64::new(86_398).unwrap();
        maximum.poll_interval_secs = NonZeroU64::new(86_399).unwrap();
        maximum.response_freshness_secs = NonZeroU64::new(86_400).unwrap();
        maximum.calendar_max_age_secs = maximum.response_freshness_secs;
        maximum.evidence_clock_skew_secs = maximum.response_freshness_secs;
        maximum.emergency_buffer_secs = maximum.response_freshness_secs;
        maximum.validate(&HedgingAssets::default()).unwrap();
        for field in 0..6 {
            let mut invalid = maximum.clone();
            let durations = [
                &mut invalid.request_timeout_secs,
                &mut invalid.poll_interval_secs,
                &mut invalid.response_freshness_secs,
                &mut invalid.calendar_max_age_secs,
                &mut invalid.evidence_clock_skew_secs,
                &mut invalid.emergency_buffer_secs,
            ];
            *durations.into_iter().nth(field).unwrap() = NonZeroU64::new(86_401).unwrap();
            assert!(matches!(
                invalid.validate(&HedgingAssets::default()).unwrap_err(),
                TradingScheduleConfigError::Timing
            ));
        }
    }

    #[test]
    fn polling_must_finish_before_response_freshness_expires() {
        let mut config = config();
        config.poll_interval_secs = config.response_freshness_secs;
        assert!(matches!(
            config.validate(&HedgingAssets::default()).unwrap_err(),
            TradingScheduleConfigError::Timing
        ));
    }

    #[test]
    fn unknown_environment_is_not_accepted() {
        let error = toml::from_str::<TradingScheduleConfig>("environment = 'prod'").unwrap_err();
        assert!(error.message().contains("unknown variant `prod`"));
    }

    #[test]
    fn environment_names_preserve_the_persisted_partition() {
        for (name, expected) in [
            ("staging", TradingScheduleEnvironment::Staging),
            ("production", TradingScheduleEnvironment::Production),
        ] {
            let value = toml::Value::String(name.into());
            let environment: TradingScheduleEnvironment = value.try_into().unwrap();
            assert_eq!(environment, expected);
            assert_eq!(environment.as_str(), name);
        }
    }

    #[test]
    fn all_operational_parameters_are_required() {
        let error = toml::from_str::<TradingScheduleConfig>(
            r#"
            mode = "observe"
            environment = "staging"
        "#,
        )
        .unwrap_err();
        assert!(error.message().contains("poll_interval_secs"));
    }

    fn partition() -> (TradingScheduleConfig, HedgingAssets) {
        let mut config = config();
        let mut assets = HedgingAssets::default();
        for (symbol, extended) in [("AAPL", false), ("NVDA", true)] {
            assets.equities.symbols.insert(
                st0x_execution::Symbol::new(symbol).unwrap(),
                EquityHedgePolicy {
                    extended_hours_counter_trading: if extended {
                        OperationMode::Enabled
                    } else {
                        OperationMode::Disabled
                    },
                },
            );
            config.scopes.push(TradingScheduleScope {
                id: symbol.into(),
                profile_revision: "v1".into(),
                extended_hours: extended,
                assets: vec![symbol.into()],
            });
        }
        (config, assets)
    }

    #[test]
    fn scope_partition_covers_each_equity_with_matching_eligibility() {
        let (config, assets) = partition();
        config.validate(&assets).unwrap();
        for scenario in 0..5 {
            let mut invalid = config.clone();
            match scenario {
                0 => invalid.scopes[0].assets.push("AAPL".into()),
                1 => invalid.scopes[0].assets = vec!["UNKNOWN".into()],
                2 => invalid.scopes[0].extended_hours = true,
                3 => {
                    invalid.scopes.pop();
                }
                4 => invalid.scopes[0].assets = vec![" ".into()],
                _ => unreachable!(),
            }
            let TradingScheduleConfigError::Membership(reason) =
                invalid.validate(&assets).unwrap_err()
            else {
                panic!("expected a membership error");
            };
            let expected = match scenario {
                0 => TradingScheduleMembershipError::Duplicate(
                    st0x_execution::Symbol::new("AAPL").unwrap(),
                ),
                1 => TradingScheduleMembershipError::Unknown(
                    st0x_execution::Symbol::new("UNKNOWN").unwrap(),
                ),
                2 => TradingScheduleMembershipError::Eligibility(
                    st0x_execution::Symbol::new("AAPL").unwrap(),
                ),
                3 => TradingScheduleMembershipError::Incomplete,
                4 => TradingScheduleMembershipError::EmptySymbol(st0x_execution::EmptySymbolError),
                _ => unreachable!(),
            };
            assert_eq!(reason, expected);
        }
    }

    #[test]
    fn whitespace_aliases_cannot_hide_missing_or_unreachable_membership() {
        let (config, assets) = partition();
        for duplicate in [false, true] {
            let mut invalid = config.clone();
            if duplicate {
                invalid.scopes[1].assets = vec![" AAPL ".into()];
                invalid.scopes[1].extended_hours = false;
            } else {
                invalid.scopes[0].assets = vec![" AAPL ".into()];
            }
            assert!(matches!(
                invalid.validate(&assets).unwrap_err(),
                TradingScheduleConfigError::Membership(
                    TradingScheduleMembershipError::NonCanonical(_)
                )
            ));
        }
    }

    #[test]
    fn scope_identity_and_profile_must_be_nonempty_and_unique() {
        let (config, assets) = partition();
        for scenario in 0..4 {
            let mut invalid = config.clone();
            match scenario {
                0 => invalid.scopes[1].id = invalid.scopes[0].id.clone(),
                1 => invalid.scopes[0].id = " ".into(),
                2 => invalid.scopes[0].profile_revision = " ".into(),
                3 => invalid.scopes[0].assets.clear(),
                _ => unreachable!(),
            }
            assert!(matches!(
                invalid.validate(&assets).unwrap_err(),
                TradingScheduleConfigError::Scope
            ));
        }
    }
}
