//! Operational alerting configuration: low-gas balance monitoring and the
//! Telegram channel used to deliver alerts.
//!
//! Like [`crate::telemetry`], this is an OPTIONAL section split across the
//! plaintext config (`[alerts]`) and the encrypted secrets TOML (the Telegram
//! `bot_token`). When neither is present the loader yields `None` and no gas
//! monitor is spawned. When present, the section must fully specify every
//! field -- there are no silent threshold defaults, per the financial-integrity
//! rule.

use alloy::primitives::U256;
use alloy::primitives::utils::{UnitsError, parse_ether};
use serde::Deserialize;
use thiserror::Error;

/// Non-secret alerting settings deserialized from the plaintext config TOML.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertsConfig {
    /// Telegram chat id alerts are delivered to.
    pub chat_id: i64,
    /// Native-ETH balance threshold, expressed as a decimal-ETH string (e.g.
    /// `"0.05"`). Parsed to wei at load time so a malformed value fails fast.
    pub low_balance_threshold: String,
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
    /// Low-balance threshold in wei, parsed from the decimal-ETH config string.
    pub low_balance_threshold_wei: U256,
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
            .field("low_balance_threshold_wei", &self.low_balance_threshold_wei)
            .field("poll_interval", &self.poll_interval)
            .field("realert_interval", &self.realert_interval)
            .field("message_thread_id", &self.message_thread_id)
            .finish()
    }
}

impl AlertsCtx {
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

                let low_balance_threshold_wei = parse_ether(&config.low_balance_threshold)
                    .map_err(|source| AlertsAssemblyError::InvalidThreshold {
                        value: config.low_balance_threshold.clone(),
                        source,
                    })?;

                if low_balance_threshold_wei.is_zero() {
                    return Err(AlertsAssemblyError::ZeroThreshold);
                }

                Ok(Some(Self {
                    chat_id: config.chat_id,
                    bot_token: secrets.bot_token,
                    low_balance_threshold_wei,
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

#[derive(Debug, Error)]
pub enum AlertsAssemblyError {
    #[error("[alerts] config present but [alerts] secrets (bot_token) missing")]
    SecretsMissing,
    #[error("[alerts] secrets (bot_token) present but [alerts] config missing")]
    ConfigMissing,
    #[error("[alerts] {field} must be non-zero")]
    ZeroInterval { field: &'static str },
    #[error("[alerts] low_balance_threshold must be greater than zero")]
    ZeroThreshold,
    #[error("[alerts] low_balance_threshold {value} is not a valid decimal-ETH amount")]
    InvalidThreshold {
        value: String,
        #[source]
        source: UnitsError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> AlertsConfig {
        AlertsConfig {
            chat_id: -1_001_234_567_890,
            low_balance_threshold: "0.05".to_owned(),
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

    #[test]
    fn new_parses_threshold_and_intervals() {
        let ctx = AlertsCtx::new(Some(valid_config()), Some(valid_secrets()))
            .unwrap()
            .unwrap();

        assert_eq!(ctx.chat_id, -1_001_234_567_890);
        assert_eq!(ctx.bot_token, "123:abc");
        // 0.05 ETH = 5 * 10^16 wei.
        assert_eq!(
            ctx.low_balance_threshold_wei,
            U256::from(50_000_000_000_000_000_u64)
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
    fn new_fails_fast_on_bad_threshold() {
        let mut config = valid_config();
        config.low_balance_threshold = "not-a-number".to_owned();

        let error = AlertsCtx::new(Some(config), Some(valid_secrets())).unwrap_err();

        assert!(
            matches!(error, AlertsAssemblyError::InvalidThreshold { ref value, .. } if value == "not-a-number"),
            "expected InvalidThreshold carrying the offending value, got: {error}"
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
    fn new_rejects_zero_threshold() {
        let mut config = valid_config();
        config.low_balance_threshold = "0".to_owned();

        let error = AlertsCtx::new(Some(config), Some(valid_secrets())).unwrap_err();
        assert!(
            matches!(error, AlertsAssemblyError::ZeroThreshold),
            "expected ZeroThreshold, got: {error}"
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
