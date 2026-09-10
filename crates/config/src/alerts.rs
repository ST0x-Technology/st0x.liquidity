//! Operational alerting configuration: low-gas balance monitoring thresholds
//! and intervals.
//!
//! Alerts themselves are emitted as structured ERROR logs (see the binary
//! crate's `alerts` module); delivery to humans happens downstream, via the
//! log pipeline (Cloud Logging -> Grafana alert rules). This section therefore
//! carries no delivery-channel settings and no secrets -- it only gates and
//! tunes the gas monitor.
//!
//! The plaintext `[alerts]` section is required because its thresholds gate
//! fresh transfers. Enabled watched HyperEVM additionally requires a HYPE
//! threshold. The section must fully specify every field -- there are no silent
//! threshold defaults, per the financial-integrity rule.

use std::collections::BTreeMap;

use alloy::primitives::U256;
use alloy::primitives::ruint::ParseError;
use serde::Deserialize;
use thiserror::Error;

use st0x_evm::Chain;

use crate::chain::ChainConfig;
use crate::enablement::ChainLifecycle;
use crate::loader::StartupNotice;

/// Chains monitored whenever alerting is configured.
///
/// Enabled watched HyperEVM additionally requires its own threshold.
pub const LEGACY_GAS_MONITORED_CHAINS: [Chain; 2] = [Chain::Base, Chain::Ethereum];

/// Non-secret alerting settings deserialized from the plaintext config TOML.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertsConfig {
    /// Native-gas balance threshold per chain, as decimal native-token strings (e.g.
    /// `"0.05"`), parsed to wei at load time so a malformed value fails fast.
    ///
    /// Keyed by chain rather than one field per chain: a single global figure
    /// would be simultaneously too low on an expensive chain and too high on a
    /// cheap one, and every monitored chain must state its own.
    pub low_balance_thresholds: BTreeMap<Chain, String>,
    /// Seconds between native-balance polls.
    pub poll_interval: u64,
    /// Minimum seconds between repeated low-balance alerts while the balance
    /// stays below threshold. Bounds alert spam without hiding a persistent
    /// low-balance condition.
    pub realert_interval: u64,
    /// MIGRATION SHIM, removed next release (together with the secrets-file
    /// `[alerts]` shim in the loader): the Telegram delivery fields are
    /// retired, but the pinned Secret Manager config versions still carry
    /// them, and the currently-running build requires `chat_id` -- so the
    /// config cannot drop the fields before this image rolls, and this image
    /// must not reject them when it rolls. Accepted and ignored with a
    /// deprecation warning in [`AlertsCtx::new`].
    pub chat_id: Option<i64>,
    /// MIGRATION SHIM, removed next release: see [`AlertsConfig::chat_id`].
    pub message_thread_id: Option<i64>,
}

/// Runtime alerting context assembled from the `[alerts]` config section.
///
/// Constructed via [`AlertsCtx::new`], which returns `None` when the section
/// is absent and HyperEVM does not require monitoring.
#[derive(Debug, Clone)]
pub struct AlertsCtx {
    /// Low-balance threshold in wei, per monitored chain. Validated at
    /// construction to hold the legacy chains plus enabled watched HyperEVM.
    low_balance_thresholds_wei: BTreeMap<Chain, U256>,
    pub poll_interval: std::time::Duration,
    pub realert_interval: std::time::Duration,
}

impl AlertsCtx {
    /// The low-balance threshold for `chain`, or `None` when no gas monitor
    /// runs on it. Total for every chain in [`LEGACY_GAS_MONITORED_CHAINS`], because
    /// [`Self::new`] refuses a config that omits one.
    pub fn low_balance_threshold_wei(&self, chain: Chain) -> Option<U256> {
        self.low_balance_thresholds_wei.get(&chain).copied()
    }

    /// An alerts context with the given per-chain thresholds, for tests and
    /// fixtures. Production contexts come from [`Self::new`], which is what
    /// validates the threshold map against [`LEGACY_GAS_MONITORED_CHAINS`].
    #[cfg(any(test, feature = "test-support"))]
    pub fn for_test(
        low_balance_thresholds_wei: BTreeMap<Chain, U256>,
        poll_interval: std::time::Duration,
        realert_interval: std::time::Duration,
    ) -> Self {
        Self {
            low_balance_thresholds_wei,
            poll_interval,
            realert_interval,
        }
    }

    pub fn new(
        config: Option<AlertsConfig>,
        chains: &BTreeMap<Chain, ChainConfig>,
        startup_notices: &mut Vec<StartupNotice>,
    ) -> Result<Option<Self>, AlertsAssemblyError> {
        let monitor_hyperevm =
            chains
                .get(&Chain::HyperEvm)
                .is_some_and(|config| match config.lifecycle {
                    ChainLifecycle::Disabled => false,
                    ChainLifecycle::ObserveOnly
                    | ChainLifecycle::Prefunded
                    | ChainLifecycle::Active => config.trading.is_some(),
                });
        let Some(config) = config else {
            if monitor_hyperevm {
                return Err(AlertsAssemblyError::HyperEvmRequiresAlerts);
            }

            startup_notices.push(StartupNotice::info(
                "[alerts] config section absent; the gas monitor will not run",
            ));
            return Ok(None);
        };

        // Migration shim, removed next release: see `AlertsConfig::chat_id`.
        let retired: Vec<&str> = [
            ("chat_id", config.chat_id.is_some()),
            ("message_thread_id", config.message_thread_id.is_some()),
        ]
        .into_iter()
        .filter_map(|(field, present)| present.then_some(field))
        .collect();

        if !retired.is_empty() {
            startup_notices.push(StartupNotice::warning(format!(
                "[alerts] {fields} deprecated and ignored (alerts are structured logs \
                 now); remove from the [alerts] config section (removed next release)",
                fields = retired.join("/"),
            )));
        }

        if config.poll_interval == 0 {
            return Err(AlertsAssemblyError::ZeroInterval {
                field: "poll_interval",
            });
        }

        if config.realert_interval == 0 {
            return Err(AlertsAssemblyError::ZeroInterval {
                field: "realert_interval",
            });
        }

        let monitored_chains: Vec<_> = LEGACY_GAS_MONITORED_CHAINS
            .into_iter()
            .chain(monitor_hyperevm.then_some(Chain::HyperEvm))
            .collect();
        for chain in config.low_balance_thresholds.keys() {
            if !monitored_chains.contains(chain) {
                return Err(AlertsAssemblyError::UnmonitoredChain { chain: *chain });
            }
        }

        let low_balance_thresholds_wei = monitored_chains
            .into_iter()
            .map(|chain| {
                let raw = config
                    .low_balance_thresholds
                    .get(&chain)
                    .ok_or(AlertsAssemblyError::MissingThreshold { chain })?;

                Ok((chain, parse_threshold(chain, raw)?))
            })
            .collect::<Result<BTreeMap<_, _>, AlertsAssemblyError>>()?;

        Ok(Some(Self {
            low_balance_thresholds_wei,
            poll_interval: std::time::Duration::from_secs(config.poll_interval),
            realert_interval: std::time::Duration::from_secs(config.realert_interval),
        }))
    }
}

fn parse_threshold(chain: Chain, value: &str) -> Result<U256, AlertsAssemblyError> {
    let mut digits = value.to_owned();
    let fractional_digits = digits.find('.').map_or(0, |index| {
        digits.remove(index);
        digits.len() - index
    });

    if fractional_digits > 18 {
        return Err(AlertsAssemblyError::ExcessThresholdPrecision { chain });
    }

    let units = format!(
        "{digits:0<width$}",
        width = digits.len() + 18 - fractional_digits
    );
    let threshold = U256::from_str_radix(&units, 10).map_err(|source| {
        AlertsAssemblyError::InvalidThreshold {
            chain,
            value: value.to_owned(),
            source,
        }
    })?;

    if threshold.is_zero() {
        return Err(AlertsAssemblyError::ZeroThreshold { chain });
    }

    Ok(threshold)
}

#[derive(Debug, Error)]
pub enum AlertsAssemblyError {
    #[error("enabled watched hyperevm requires [alerts] for native-gas monitoring")]
    HyperEvmRequiresAlerts,
    #[error("[alerts.low_balance_thresholds] {chain} has more than 18 decimal places")]
    ExcessThresholdPrecision { chain: Chain },
    #[error("[alerts] {field} must be non-zero")]
    ZeroInterval { field: &'static str },
    #[error("[alerts.low_balance_thresholds] {chain} must be greater than zero")]
    ZeroThreshold { chain: Chain },
    #[error(
        "[alerts.low_balance_thresholds] {chain} value {value} is not a valid \
         decimal native-token amount"
    )]
    InvalidThreshold {
        chain: Chain,
        value: String,
        #[source]
        source: ParseError,
    },
    #[error(
        "[alerts.low_balance_thresholds] is missing {chain}; every chain the gas \
         monitor runs on needs its own threshold, because one figure cannot be \
         right for both an expensive chain and a cheap one"
    )]
    MissingThreshold { chain: Chain },
    #[error(
        "[alerts.low_balance_thresholds] configures {chain}, but no gas monitor is \
         selected for it in this configuration, so the threshold would never be read"
    )]
    UnmonitoredChain { chain: Chain },
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U512;
    use proptest::prelude::*;

    use super::*;

    fn hyper_config(lifecycle: &str, watched: bool) -> BTreeMap<Chain, crate::chain::ChainConfig> {
        let trading = if watched {
            r#"[trading]
orderbook = "0x1111111111111111111111111111111111111111"
inventory_mode = "legacy"
inventory_adapters = []
vault_owner = "0x2222222222222222222222222222222222222222"
deployment_block = 1
ingestion_cutoff = "safe"
order_fill_poll_interval_secs = 1
"#
        } else {
            ""
        };
        BTreeMap::from([(
            Chain::HyperEvm,
            toml::from_str(&format!(
                "lifecycle = \"{lifecycle}\"\nrequired_confirmations = 1\n{trading}"
            ))
            .unwrap(),
        )])
    }

    #[test]
    fn watched_hyperevm_requires_alerts_and_its_own_positive_threshold() {
        for lifecycle in ["prefunded", "observe-only", "active"] {
            let chains = hyper_config(lifecycle, true);
            assert!(matches!(
                AlertsCtx::new(None, &chains, &mut Vec::new()),
                Err(AlertsAssemblyError::HyperEvmRequiresAlerts)
            ));
            assert!(matches!(
                AlertsCtx::new(Some(valid_config()), &chains, &mut Vec::new()),
                Err(AlertsAssemblyError::MissingThreshold {
                    chain: Chain::HyperEvm
                })
            ));
            for value in ["0", "bad", "0.125"] {
                let mut config = valid_config();
                config
                    .low_balance_thresholds
                    .insert(Chain::HyperEvm, value.to_owned());
                let result = AlertsCtx::new(Some(config), &chains, &mut Vec::new());
                match value {
                    "0" => assert!(matches!(
                        result,
                        Err(AlertsAssemblyError::ZeroThreshold {
                            chain: Chain::HyperEvm
                        })
                    )),
                    "bad" => assert!(matches!(
                        result,
                        Err(AlertsAssemblyError::InvalidThreshold {
                            chain: Chain::HyperEvm,
                            ..
                        })
                    )),
                    _ => assert_eq!(
                        result
                            .unwrap()
                            .unwrap()
                            .low_balance_threshold_wei(Chain::HyperEvm),
                        Some(U256::from(125_000_000_000_000_000_u64))
                    ),
                }
            }
        }
    }

    #[test]
    fn unwatched_hyperevm_preserves_optional_alerts_and_rejects_unused_thresholds() {
        for chains in [
            BTreeMap::new(),
            hyper_config("disabled", true),
            hyper_config("observe-only", false),
            hyper_config("prefunded", false),
        ] {
            assert!(matches!(
                AlertsCtx::new(None, &chains, &mut Vec::new()),
                Ok(None)
            ));
            let ctx = AlertsCtx::new(Some(valid_config()), &chains, &mut Vec::new())
                .unwrap()
                .unwrap();
            assert_eq!(ctx.low_balance_threshold_wei(Chain::HyperEvm), None);
            let mut config = valid_config();
            config
                .low_balance_thresholds
                .insert(Chain::HyperEvm, "1".to_owned());
            assert!(matches!(
                AlertsCtx::new(Some(config), &chains, &mut Vec::new()),
                Err(AlertsAssemblyError::UnmonitoredChain {
                    chain: Chain::HyperEvm
                })
            ));
        }
    }

    #[test]
    fn native_thresholds_reject_negative_precision_loss_and_overflow() {
        for chain in [Chain::Base, Chain::Ethereum, Chain::HyperEvm] {
            let error = parse_threshold(chain, "1.0000000000000000001").unwrap_err();
            assert!(
                matches!(
                    error,
                    AlertsAssemblyError::ExcessThresholdPrecision { chain: failed_chain }
                        if failed_chain == chain
                ),
                "expected excess precision for {chain}, got {error:?}"
            );
            for value in [
                "-0.1",
                "115792089237316195423570985008687907853269984665640564039457584007913129639935",
                "115792089237316195423570985008687907853269984665640564039457.584007913129639936",
            ] {
                let error = parse_threshold(chain, value).unwrap_err();
                assert!(
                    matches!(
                        error,
                        AlertsAssemblyError::InvalidThreshold { chain: failed_chain, value: ref failed_value, .. }
                            if failed_chain == chain && failed_value == value
                    ),
                    "expected invalid threshold {value} for {chain}, got {error:?}"
                );
            }
            assert_eq!(
                parse_threshold(chain, "0.000000000000000001").unwrap(),
                U256::from(1)
            );
            assert_eq!(parse_threshold(chain, "115792089237316195423570985008687907853269984665640564039457.584007913129639935").unwrap(), U256::MAX);
        }
    }

    proptest! {
        #[test]
        fn positive_native_thresholds_scale_exactly(
            mantissa in 1_u64..=u64::MAX,
            precision in 0_u32..=18,
            leading_zeros in 0_usize..32,
        ) {
            let divisor = 10_u64.pow(precision);
            let value = if precision == 0 {
                mantissa.to_string()
            } else {
                format!(
                    "{}.{:0width$}",
                    mantissa / divisor,
                    mantissa % divisor,
                    width = usize::try_from(precision).unwrap(),
                )
            };
            let value = format!("{}{value}", "0".repeat(leading_zeros));
            let expected = U256::from(mantissa) * U256::from(10_u64.pow(18 - precision));

            for chain in [Chain::Base, Chain::Ethereum, Chain::HyperEvm] {
                prop_assert_eq!(parse_threshold(chain, &value).unwrap(), expected);
            }
        }

        #[test]
        fn zero_native_threshold_forms_are_rejected(
            whole_zeros in 1_usize..32,
            precision in 0_usize..=18,
        ) {
            let mut value = "0".repeat(whole_zeros);
            if precision > 0 {
                value.push('.');
                value.push_str(&"0".repeat(precision));
            }

            for chain in [Chain::Base, Chain::Ethereum, Chain::HyperEvm] {
                let error = parse_threshold(chain, &value).unwrap_err();
                prop_assert!(matches!(error, AlertsAssemblyError::ZeroThreshold { chain: failed } if failed == chain), "{error:?}");
            }
        }

        #[test]
        fn excess_native_threshold_precision_is_rejected(
            whole in any::<u64>(),
            fraction in "[0-9]{19,40}",
        ) {
            let value = format!("{whole}.{fraction}");
            for chain in [Chain::Base, Chain::Ethereum, Chain::HyperEvm] {
                let error = parse_threshold(chain, &value).unwrap_err();
                prop_assert!(matches!(error, AlertsAssemblyError::ExcessThresholdPrecision { chain: failed } if failed == chain), "{error:?}");
            }
        }

        #[test]
        fn negative_native_thresholds_are_rejected(magnitude in any::<u128>()) {
            let value = format!("-{magnitude}");
            for chain in [Chain::Base, Chain::Ethereum, Chain::HyperEvm] {
                let error = parse_threshold(chain, &value).unwrap_err();
                prop_assert!(matches!(error, AlertsAssemblyError::InvalidThreshold { chain: failed, value: ref rejected, .. } if failed == chain && rejected == &value), "{error:?}");
            }
        }

        #[test]
        fn native_thresholds_beyond_u256_are_rejected(excess in 1_u64..=u64::MAX) {
            let units = U512::from(U256::MAX) + U512::from(excess);
            let scale = U512::from(1_000_000_000_000_000_000_u64);
            let value = format!("{}.{:0>18}", units / scale, units % scale);

            for chain in [Chain::Base, Chain::Ethereum, Chain::HyperEvm] {
                let error = parse_threshold(chain, &value).unwrap_err();
                prop_assert!(matches!(error, AlertsAssemblyError::InvalidThreshold { chain: failed, value: ref rejected, .. } if failed == chain && rejected == &value), "{error:?}");
            }
        }
    }

    fn valid_config() -> AlertsConfig {
        AlertsConfig {
            low_balance_thresholds: BTreeMap::from([
                (Chain::Base, "0.05".to_owned()),
                (Chain::Ethereum, "0.01".to_owned()),
            ]),
            poll_interval: 300,
            realert_interval: 3600,
            chat_id: None,
            message_thread_id: None,
        }
    }

    /// The table is required, not defaulted: an `[alerts]` section without it
    /// would parse into an empty map, and a gas monitor with no threshold is
    /// a monitor that never alerts.
    #[test]
    fn config_requires_a_threshold_table() {
        let error = toml::from_str::<AlertsConfig>(
            "
            poll_interval = 300
            realert_interval = 3600
            ",
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("low_balance_thresholds"),
            "a missing threshold table must fail explicitly, got: {error}"
        );
    }

    /// A config still carrying the retired per-chain field names supplies no
    /// thresholds at all. Rejecting them by name is what turns a stale config
    /// into a startup failure instead of a monitor that silently never fires.
    #[test]
    fn config_rejects_the_retired_flat_threshold_fields() {
        let error = toml::from_str::<AlertsConfig>(
            r#"
            base_low_balance_threshold = "0.05"
            ethereum_low_balance_threshold = "0.01"
            poll_interval = 300
            realert_interval = 3600

            [low_balance_thresholds]
            base = "0.05"
            ethereum = "0.01"
            "#,
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("base_low_balance_threshold"),
            "the retired field must be rejected by name, got: {error}"
        );
    }

    #[test]
    fn new_parses_threshold_and_intervals() {
        let ctx = AlertsCtx::new(Some(valid_config()), &BTreeMap::new(), &mut Vec::new())
            .unwrap()
            .unwrap();

        // 0.05 ETH = 5 * 10^16 wei.
        assert_eq!(
            ctx.low_balance_threshold_wei(Chain::Base),
            Some(U256::from(50_000_000_000_000_000_u64))
        );
        assert_eq!(
            ctx.low_balance_threshold_wei(Chain::Ethereum),
            Some(U256::from(10_000_000_000_000_000_u64))
        );
        assert_eq!(ctx.poll_interval, std::time::Duration::from_secs(300));
        assert_eq!(ctx.realert_interval, std::time::Duration::from_secs(3600));
    }

    /// The delivery-channel fields retired with the Telegram transport are
    /// accepted and ignored for one release: the pinned Secret Manager config
    /// versions still carry them (the previous build required `chat_id`), so
    /// rejecting them here would crash-loop the bot at roll time until a
    /// separate config release lands. Removed next release together with the
    /// secrets-file `[alerts]` shim.
    #[test]
    fn config_accepts_and_ignores_the_retired_delivery_channel_fields() {
        let config: AlertsConfig = toml::from_str(
            r#"
            chat_id = -1_001_234_567_890
            message_thread_id = 42
            poll_interval = 300
            realert_interval = 3600

            [low_balance_thresholds]
            base = "0.05"
            ethereum = "0.01"
            "#,
        )
        .unwrap();

        let mut notices = Vec::new();
        let ctx = AlertsCtx::new(Some(config), &BTreeMap::new(), &mut notices)
            .unwrap()
            .unwrap();

        assert_eq!(
            ctx.low_balance_threshold_wei(Chain::Base),
            Some(U256::from(50_000_000_000_000_000_u64)),
            "the live fields must still load normally alongside the ignored ones"
        );
        assert_eq!(notices.len(), 1, "exactly one deprecation notice");
        assert!(
            notices[0]
                .message
                .contains("chat_id/message_thread_id deprecated"),
            "the notice must name exactly the retired fields seen, got: {}",
            notices[0].message
        );
    }

    #[test]
    fn new_returns_none_when_config_absent() {
        let mut notices = Vec::new();
        let ctx = AlertsCtx::new(None, &BTreeMap::new(), &mut notices).unwrap();

        assert!(ctx.is_none(), "absent [alerts] config must yield None");
        assert_eq!(
            notices.len(),
            1,
            "the absent section must be noticed, not silently skipped"
        );
        assert!(
            notices[0].message.contains("gas monitor will not run"),
            "the notice must say what the absence means, got: {}",
            notices[0].message
        );
    }

    #[test]
    fn new_fails_fast_on_bad_base_threshold() {
        let mut config = valid_config();
        config
            .low_balance_thresholds
            .insert(Chain::Base, "not-a-number".to_owned());

        let error = AlertsCtx::new(Some(config), &BTreeMap::new(), &mut Vec::new()).unwrap_err();

        assert!(
            matches!(
                error,
                AlertsAssemblyError::InvalidThreshold {
                    chain: Chain::Base,
                    ref value,
                    ..
                } if value == "not-a-number"
            ),
            "expected InvalidThreshold naming Base and the offending value, got: {error}"
        );
    }

    #[test]
    fn new_fails_fast_on_bad_ethereum_threshold() {
        let mut config = valid_config();
        config
            .low_balance_thresholds
            .insert(Chain::Ethereum, "not-a-number".to_owned());

        let error = AlertsCtx::new(Some(config), &BTreeMap::new(), &mut Vec::new()).unwrap_err();

        assert!(
            matches!(
                error,
                AlertsAssemblyError::InvalidThreshold {
                    chain: Chain::Ethereum,
                    ref value,
                    ..
                } if value == "not-a-number"
            ),
            "expected InvalidThreshold naming Ethereum and the offending value, got: {error}"
        );
    }

    /// A monitored chain with no threshold has no balance to compare against.
    /// Substituting one would either never alert (zero) or alert at the wrong
    /// balance, so the config is refused instead.
    #[test]
    fn new_rejects_a_monitored_chain_without_a_threshold() {
        for chain in [Chain::Base, Chain::Ethereum] {
            let mut config = valid_config();
            config.low_balance_thresholds.remove(&chain);
            let error =
                AlertsCtx::new(Some(config), &BTreeMap::new(), &mut Vec::new()).unwrap_err();
            assert!(
                matches!(error, AlertsAssemblyError::MissingThreshold { chain: missing } if missing == chain),
                "expected missing threshold for {chain}, got: {error}"
            );
        }
    }

    /// A threshold for a chain no monitor runs on would never be read. Taking
    /// it silently would make a misspelled or premature key look configured.
    #[test]
    fn new_rejects_a_threshold_for_an_unmonitored_chain() {
        let mut config = valid_config();
        config
            .low_balance_thresholds
            .insert(Chain::HyperEvm, "0.05".to_owned());

        let error = AlertsCtx::new(Some(config), &BTreeMap::new(), &mut Vec::new()).unwrap_err();

        assert!(
            matches!(
                error,
                AlertsAssemblyError::UnmonitoredChain {
                    chain: Chain::HyperEvm
                }
            ),
            "expected UnmonitoredChain for HyperEVM, got: {error}"
        );
    }

    #[test]
    fn thresholds_parse_from_a_chain_keyed_table() {
        let config: AlertsConfig = toml::from_str(
            r#"
            poll_interval = 300
            realert_interval = 3600

            [low_balance_thresholds]
            base = "0.05"
            ethereum = "0.01"
            "#,
        )
        .unwrap();

        assert_eq!(
            config
                .low_balance_thresholds
                .get(&Chain::Base)
                .map(String::as_str),
            Some("0.05")
        );
        assert_eq!(
            config
                .low_balance_thresholds
                .get(&Chain::Ethereum)
                .map(String::as_str),
            Some("0.01")
        );
    }

    #[test]
    fn new_rejects_zero_base_threshold() {
        let mut config = valid_config();
        config
            .low_balance_thresholds
            .insert(Chain::Base, "0".to_owned());

        let error = AlertsCtx::new(Some(config), &BTreeMap::new(), &mut Vec::new()).unwrap_err();
        assert!(
            matches!(
                error,
                AlertsAssemblyError::ZeroThreshold { chain: Chain::Base }
            ),
            "expected ZeroThreshold for Base, got: {error}"
        );
    }

    #[test]
    fn new_rejects_zero_ethereum_threshold() {
        let mut config = valid_config();
        config
            .low_balance_thresholds
            .insert(Chain::Ethereum, "0".to_owned());

        let error = AlertsCtx::new(Some(config), &BTreeMap::new(), &mut Vec::new()).unwrap_err();
        assert!(
            matches!(
                error,
                AlertsAssemblyError::ZeroThreshold {
                    chain: Chain::Ethereum
                }
            ),
            "expected ZeroThreshold for Ethereum, got: {error}"
        );
    }

    #[test]
    fn new_rejects_zero_poll_interval() {
        let mut config = valid_config();
        config.poll_interval = 0;

        let error = AlertsCtx::new(Some(config), &BTreeMap::new(), &mut Vec::new()).unwrap_err();
        assert!(
            matches!(
                error,
                AlertsAssemblyError::ZeroInterval {
                    field: "poll_interval"
                }
            ),
            "expected ZeroInterval for poll_interval, got: {error}"
        );
    }

    #[test]
    fn new_rejects_zero_realert_interval() {
        let mut config = valid_config();
        config.realert_interval = 0;

        let error = AlertsCtx::new(Some(config), &BTreeMap::new(), &mut Vec::new()).unwrap_err();
        assert!(
            matches!(
                error,
                AlertsAssemblyError::ZeroInterval {
                    field: "realert_interval"
                }
            ),
            "expected ZeroInterval for realert_interval, got: {error}"
        );
    }
}
