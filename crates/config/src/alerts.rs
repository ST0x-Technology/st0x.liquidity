//! Operational alerting configuration: low-gas balance monitoring and the
//! Telegram channel used to deliver alerts.
//!
//! Like [`crate::telemetry`], this is an OPTIONAL section split across the
//! plaintext config (`[alerts]`) and the encrypted secrets TOML (the Telegram
//! `bot_token`). When neither is present the loader yields `None` and no gas
//! monitor is spawned. When present, the section must fully specify every
//! field -- there are no silent threshold defaults, per the financial-integrity
//! rule.

use std::collections::BTreeMap;

use alloy::primitives::U256;
use alloy::primitives::utils::{UnitsError, parse_ether};
use serde::Deserialize;
use thiserror::Error;

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
    /// Telegram chat id alerts are delivered to.
    pub chat_id: i64,
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
    /// Optional forum topic (`message_thread_id`) to deliver alerts into. When
    /// omitted, alerts post to the group's default (General) topic. Only
    /// meaningful for forum-enabled supergroups; a missing field is a valid
    /// distinct state, not a silent default.
    pub message_thread_id: Option<i64>,
}

/// Secret alerting credentials deserialized from the encrypted secrets TOML.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertsSecrets {
    /// Telegram bot token used to authenticate `sendMessage` calls.
    pub bot_token: String,
}

impl std::fmt::Debug for AlertsSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlertsSecrets")
            .field("bot_token", &"[REDACTED]")
            .finish()
    }
}

/// Runtime alerting context assembled from config + secrets.
///
/// Constructed via [`AlertsCtx::new`], which returns `None` when both the
/// config section and the secret are absent.
#[derive(Clone)]
pub struct AlertsCtx {
    pub chat_id: i64,
    pub bot_token: String,
    /// Low-balance threshold in wei, per monitored chain. Validated at
    /// construction to hold exactly [`GAS_MONITORED_CHAINS`].
    low_balance_thresholds_wei: BTreeMap<Chain, U256>,
    pub poll_interval: std::time::Duration,
    pub realert_interval: std::time::Duration,
    /// Forum topic to deliver alerts into, or `None` for the default topic.
    pub message_thread_id: Option<i64>,
}

impl std::fmt::Debug for AlertsCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlertsCtx")
            .field("chat_id", &self.chat_id)
            .field("bot_token", &"[REDACTED]")
            .field(
                "low_balance_thresholds_wei",
                &self.low_balance_thresholds_wei,
            )
            .field("poll_interval", &self.poll_interval)
            .field("realert_interval", &self.realert_interval)
            .field("message_thread_id", &self.message_thread_id)
            .finish()
    }
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
        chat_id: i64,
        low_balance_thresholds_wei: BTreeMap<Chain, U256>,
        poll_interval: std::time::Duration,
        realert_interval: std::time::Duration,
    ) -> Self {
        Self {
            chat_id,
            bot_token: "test-token".to_owned(),
            low_balance_thresholds_wei,
            poll_interval,
            realert_interval,
            message_thread_id: None,
        }
    }

    pub fn new(
        config: Option<AlertsConfig>,
        secrets: Option<AlertsSecrets>,
    ) -> Result<Option<Self>, AlertsAssemblyError> {
        match (config, secrets) {
            (Some(config), Some(secrets)) => {
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
                    chat_id: config.chat_id,
                    bot_token: secrets.bot_token,
                    low_balance_thresholds_wei,
                    poll_interval: std::time::Duration::from_secs(config.poll_interval),
                    realert_interval: std::time::Duration::from_secs(config.realert_interval),
                    message_thread_id: config.message_thread_id,
                }))
            }
            (None, None) => Ok(None),
            (Some(_), None) => Err(AlertsAssemblyError::SecretsMissing),
            (None, Some(_)) => Err(AlertsAssemblyError::ConfigMissing),
        }
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
    #[error("[alerts] config present but [alerts] secrets (bot_token) missing")]
    SecretsMissing,
    #[error("[alerts] secrets (bot_token) present but [alerts] config missing")]
    ConfigMissing,
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
            chat_id: -1_001_234_567_890,
            low_balance_thresholds: BTreeMap::from([
                (Chain::Base, "0.05".to_owned()),
                (Chain::Ethereum, "0.01".to_owned()),
            ]),
            poll_interval: 300,
            realert_interval: 3600,
            message_thread_id: None,
        }
    }

    fn valid_secrets() -> AlertsSecrets {
        AlertsSecrets {
            bot_token: "123:abc".to_owned(),
        }
    }

    /// The table is required, not defaulted: an `[alerts]` section without it
    /// would parse into an empty map, and a gas monitor with no threshold is
    /// a monitor that never alerts.
    #[test]
    fn config_requires_a_threshold_table() {
        let error = toml::from_str::<AlertsConfig>(
            "
            chat_id = 1
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
            chat_id = 1
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
        let ctx = AlertsCtx::new(Some(valid_config()), Some(valid_secrets()))
            .unwrap()
            .unwrap();

        assert_eq!(ctx.chat_id, -1_001_234_567_890);
        assert_eq!(ctx.bot_token, "123:abc");
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
        assert_eq!(ctx.message_thread_id, None);
    }

    #[test]
    fn new_carries_message_thread_id() {
        let mut config = valid_config();
        config.message_thread_id = Some(42);

        let ctx = AlertsCtx::new(Some(config), Some(valid_secrets()))
            .unwrap()
            .unwrap();

        assert_eq!(ctx.message_thread_id, Some(42));
    }

    #[test]
    fn new_returns_none_when_both_absent() {
        let ctx = AlertsCtx::new(None, None).unwrap();
        assert!(
            ctx.is_none(),
            "absent alerts config/secrets must yield None"
        );
    }

    #[test]
    fn new_fails_fast_on_bad_base_threshold() {
        let mut config = valid_config();
        config
            .low_balance_thresholds
            .insert(Chain::Base, "not-a-number".to_owned());

        let error = AlertsCtx::new(Some(config), Some(valid_secrets())).unwrap_err();

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

        let error = AlertsCtx::new(Some(config), Some(valid_secrets())).unwrap_err();

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

        let error = AlertsCtx::new(Some(config), Some(valid_secrets())).unwrap_err();

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

        let error = AlertsCtx::new(Some(config), Some(valid_secrets())).unwrap_err();

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
            chat_id = 1
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
    fn new_rejects_config_without_secrets() {
        let error = AlertsCtx::new(Some(valid_config()), None).unwrap_err();
        assert!(
            matches!(error, AlertsAssemblyError::SecretsMissing),
            "expected SecretsMissing, got: {error}"
        );
    }

    #[test]
    fn new_rejects_secrets_without_config() {
        let error = AlertsCtx::new(None, Some(valid_secrets())).unwrap_err();
        assert!(
            matches!(error, AlertsAssemblyError::ConfigMissing),
            "expected ConfigMissing, got: {error}"
        );
    }

    #[test]
    fn new_rejects_zero_base_threshold() {
        let mut config = valid_config();
        config
            .low_balance_thresholds
            .insert(Chain::Base, "0".to_owned());

        let error = AlertsCtx::new(Some(config), Some(valid_secrets())).unwrap_err();
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

        let error = AlertsCtx::new(Some(config), Some(valid_secrets())).unwrap_err();
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

        let error = AlertsCtx::new(Some(config), Some(valid_secrets())).unwrap_err();
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

        let error = AlertsCtx::new(Some(config), Some(valid_secrets())).unwrap_err();
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
