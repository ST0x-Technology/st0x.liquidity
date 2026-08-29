//! Operational alerting configuration: low-gas balance monitoring thresholds
//! and intervals.
//!
//! Alerts themselves are emitted as structured ERROR logs (see the binary
//! crate's `alerts` module); delivery to humans happens downstream, via the
//! log pipeline (Cloud Logging -> Grafana alert rules). This section therefore
//! carries no delivery-channel settings and no secrets -- it only gates and
//! tunes the gas monitor.
//!
//! Like [`crate::telemetry`], this is an OPTIONAL section in the plaintext
//! config (`[alerts]`). When absent the loader yields `None` and no gas
//! monitor is spawned. When present, the section must fully specify every
//! field -- there are no silent threshold defaults, per the financial-integrity
//! rule.

use std::collections::BTreeMap;

use alloy::primitives::U256;
use alloy::primitives::utils::{UnitsError, parse_ether};
use serde::Deserialize;
use thiserror::Error;
use tracing::warn;

use st0x_evm::Chain;

/// The chains this binary runs a gas monitor on.
///
/// A threshold is required for each and rejected for any other chain, so a
/// misspelled key fails startup instead of silently monitoring nothing.
pub const GAS_MONITORED_CHAINS: [Chain; 2] = [Chain::Base, Chain::Ethereum];

/// Non-secret alerting settings deserialized from the plaintext config TOML.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertsConfig {
    /// Native-gas balance threshold per chain, as decimal-ETH strings (e.g.
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
/// is absent.
#[derive(Debug, Clone)]
pub struct AlertsCtx {
    /// Low-balance threshold in wei, per monitored chain. Validated at
    /// construction to hold exactly [`GAS_MONITORED_CHAINS`].
    low_balance_thresholds_wei: BTreeMap<Chain, U256>,
    pub poll_interval: std::time::Duration,
    pub realert_interval: std::time::Duration,
}

impl AlertsCtx {
    /// The low-balance threshold for `chain`, or `None` when no gas monitor
    /// runs on it. Total for every chain in [`GAS_MONITORED_CHAINS`], because
    /// [`Self::new`] refuses a config that omits one.
    pub fn low_balance_threshold_wei(&self, chain: Chain) -> Option<U256> {
        self.low_balance_thresholds_wei.get(&chain).copied()
    }

    /// An alerts context with the given per-chain thresholds, for tests and
    /// fixtures. Production contexts come from [`Self::new`], which is what
    /// validates the threshold map against [`GAS_MONITORED_CHAINS`].
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

    pub fn new(config: Option<AlertsConfig>) -> Result<Option<Self>, AlertsAssemblyError> {
        let Some(config) = config else {
            return Ok(None);
        };

        // Migration shim, removed next release: see `AlertsConfig::chat_id`.
        if config.chat_id.is_some() || config.message_thread_id.is_some() {
            warn!(
                "[alerts] chat_id/message_thread_id are deprecated and ignored (alerts \
                 are structured logs now); remove them from the [alerts] config section"
            );
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

        for chain in config.low_balance_thresholds.keys() {
            if !GAS_MONITORED_CHAINS.contains(chain) {
                return Err(AlertsAssemblyError::UnmonitoredChain { chain: *chain });
            }
        }

        let low_balance_thresholds_wei = GAS_MONITORED_CHAINS
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
    let threshold = parse_ether(value).map_err(|source| AlertsAssemblyError::InvalidThreshold {
        chain,
        value: value.to_owned(),
        source,
    })?;

    if threshold.is_zero() {
        return Err(AlertsAssemblyError::ZeroThreshold { chain });
    }

    Ok(threshold)
}

#[derive(Debug, Error)]
pub enum AlertsAssemblyError {
    #[error("[alerts] {field} must be non-zero")]
    ZeroInterval { field: &'static str },
    #[error("[alerts.low_balance_thresholds] {chain} must be greater than zero")]
    ZeroThreshold { chain: Chain },
    #[error(
        "[alerts.low_balance_thresholds] {chain} value {value} is not a valid \
         decimal-ETH amount"
    )]
    InvalidThreshold {
        chain: Chain,
        value: String,
        #[source]
        source: UnitsError,
    },
    #[error(
        "[alerts.low_balance_thresholds] is missing {chain}; every chain the gas \
         monitor runs on needs its own threshold, because one figure cannot be \
         right for both an expensive chain and a cheap one"
    )]
    MissingThreshold { chain: Chain },
    #[error(
        "[alerts.low_balance_thresholds] configures {chain}, which this binary runs \
         no gas monitor on, so the threshold would never be read"
    )]
    UnmonitoredChain { chain: Chain },
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let ctx = AlertsCtx::new(Some(valid_config())).unwrap().unwrap();

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

        let ctx = AlertsCtx::new(Some(config)).unwrap().unwrap();

        assert_eq!(
            ctx.low_balance_threshold_wei(Chain::Base),
            Some(U256::from(50_000_000_000_000_000_u64)),
            "the live fields must still load normally alongside the ignored ones"
        );
    }

    #[test]
    fn new_returns_none_when_config_absent() {
        let ctx = AlertsCtx::new(None).unwrap();
        assert!(ctx.is_none(), "absent [alerts] config must yield None");
    }

    #[test]
    fn new_fails_fast_on_bad_base_threshold() {
        let mut config = valid_config();
        config
            .low_balance_thresholds
            .insert(Chain::Base, "not-a-number".to_owned());

        let error = AlertsCtx::new(Some(config)).unwrap_err();

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

        let error = AlertsCtx::new(Some(config)).unwrap_err();

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
        let mut config = valid_config();
        config.low_balance_thresholds.remove(&Chain::Ethereum);

        let error = AlertsCtx::new(Some(config)).unwrap_err();

        assert!(
            matches!(
                error,
                AlertsAssemblyError::MissingThreshold {
                    chain: Chain::Ethereum
                }
            ),
            "expected MissingThreshold for Ethereum, got: {error}"
        );
    }

    /// A threshold for a chain no monitor runs on would never be read. Taking
    /// it silently would make a misspelled or premature key look configured.
    #[test]
    fn new_rejects_a_threshold_for_an_unmonitored_chain() {
        let mut config = valid_config();
        config
            .low_balance_thresholds
            .insert(Chain::HyperEvm, "0.05".to_owned());

        let error = AlertsCtx::new(Some(config)).unwrap_err();

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

        let error = AlertsCtx::new(Some(config)).unwrap_err();
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

        let error = AlertsCtx::new(Some(config)).unwrap_err();
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

        let error = AlertsCtx::new(Some(config)).unwrap_err();
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

        let error = AlertsCtx::new(Some(config)).unwrap_err();
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
