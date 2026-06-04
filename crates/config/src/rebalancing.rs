//! Rebalancing configuration: parsed schema and validated runtime context.

#[cfg(feature = "test-support")]
use alloy::primitives::Address;
use std::sync::LazyLock;
use std::time::Duration;

use rain_math_float::Float;
use serde::Deserialize;
use st0x_finance::Usdc;
use st0x_float_macro::float;

use crate::ImbalanceThreshold;

/// Minimum USDC amount for Alpaca withdrawals.
///
/// Alpaca requires $50 USD minimum, but due to USDC/USD spread
/// (~17bps observed in live tests), we use $51 to ensure we always meet
/// the minimum after conversion slippage.
pub static ALPACA_MINIMUM_WITHDRAWAL: LazyLock<Usdc> = LazyLock::new(|| Usdc::new(float!(51)));

/// Error type for rebalancing configuration validation.
#[derive(Debug, thiserror::Error)]
pub enum RebalancingCtxError {
    #[error("rebalancing requires alpaca-broker-api broker type")]
    NotAlpacaBroker,
    #[error("rebalancing transfer_timeout_secs must be non-zero")]
    ZeroTransferTimeout,
    #[error("rebalancing transfer_attempt_timeout_secs must be non-zero")]
    ZeroTransferAttemptTimeout,
    #[error("invalid wallet config: {0}")]
    WalletConfig(#[from] toml::de::Error),
    #[error(transparent)]
    Evm(#[from] st0x_evm::EvmError),
}

/// USDC rebalancing configuration with explicit enable/disable.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum UsdcRebalancing {
    Enabled {
        #[serde(deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string")]
        target: Float,
        #[serde(deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string")]
        deviation: Float,
    },
    Disabled,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebalancingConfig {
    pub equity: ImbalanceThreshold,
    pub usdc: UsdcRebalancing,
    pub transfer_timeout_secs: u64,
    /// Per-attempt wall-clock bound for a single Base->Alpaca transfer job
    /// attempt. A hung RPC is aborted after this so the attempt fails and
    /// retries rather than wedging forever. Distinct from
    /// `transfer_timeout_secs`, which is the whole-transfer stall reaper.
    pub transfer_attempt_timeout_secs: u64,
}

/// Runtime configuration for rebalancing operations.
///
/// Constructed from `RebalancingConfig` after the parsed schema has been
/// validated. Wallet construction has moved to [`st0x_evm`]; this type
/// holds only the rebalancing-specific trigger thresholds.
#[derive(Clone)]
pub struct RebalancingCtx {
    pub equity: ImbalanceThreshold,
    pub usdc: Option<ImbalanceThreshold>,
    pub transfer_timeout: Duration,
    /// Per-attempt wall-clock bound for a single Base->Alpaca transfer job
    /// attempt (hung-RPC backstop). See
    /// [`RebalancingConfig::transfer_attempt_timeout_secs`].
    pub transfer_attempt_timeout: Duration,
    /// Circle attestation/fee API base URL (test-only override).
    #[cfg(feature = "test-support")]
    pub circle_api_base: String,
    /// `TokenMessengerV2` contract address (test-only override).
    #[cfg(feature = "test-support")]
    pub token_messenger: Address,
    /// `MessageTransmitterV2` contract address (test-only override).
    #[cfg(feature = "test-support")]
    pub message_transmitter: Address,
}

impl RebalancingCtx {
    /// Construct from config. Validates only rebalancing-specific
    /// trigger thresholds; wallet construction lives elsewhere.
    pub fn new(config: &RebalancingConfig) -> Result<Self, RebalancingCtxError> {
        if config.transfer_timeout_secs == 0 {
            return Err(RebalancingCtxError::ZeroTransferTimeout);
        }

        if config.transfer_attempt_timeout_secs == 0 {
            return Err(RebalancingCtxError::ZeroTransferAttemptTimeout);
        }

        let usdc = match config.usdc {
            UsdcRebalancing::Enabled { target, deviation } => {
                Some(ImbalanceThreshold { target, deviation })
            }
            UsdcRebalancing::Disabled => None,
        };

        Ok(Self {
            equity: config.equity,
            usdc,
            transfer_timeout: Duration::from_secs(config.transfer_timeout_secs),
            transfer_attempt_timeout: Duration::from_secs(config.transfer_attempt_timeout_secs),
            #[cfg(feature = "test-support")]
            circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
            #[cfg(feature = "test-support")]
            token_messenger: st0x_bridge::cctp::TOKEN_MESSENGER_V2,
            #[cfg(feature = "test-support")]
            message_transmitter: st0x_bridge::cctp::MESSAGE_TRANSMITTER_V2,
        })
    }
}

#[cfg(any(test, feature = "test-support"))]
#[bon::bon]
impl RebalancingCtx {
    /// Test constructor that creates a `RebalancingCtx` with stub wallets.
    ///
    /// The wallets panic on `send` -- use only in tests that don't submit
    /// transactions through the rebalancing wallet.
    #[builder]
    pub fn stub(
        equity: ImbalanceThreshold,
        usdc: Option<ImbalanceThreshold>,
        #[builder(default = Duration::from_secs(30 * 60))] transfer_timeout: Duration,
        #[builder(default = Duration::from_secs(60 * 60))] transfer_attempt_timeout: Duration,
    ) -> Self {
        Self {
            equity,
            usdc,
            transfer_timeout,
            transfer_attempt_timeout,
            #[cfg(feature = "test-support")]
            circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
            #[cfg(feature = "test-support")]
            token_messenger: st0x_bridge::cctp::TOKEN_MESSENGER_V2,
            #[cfg(feature = "test-support")]
            message_transmitter: st0x_bridge::cctp::MESSAGE_TRANSMITTER_V2,
        }
    }
}

#[cfg(feature = "test-support")]
#[bon::bon]
impl RebalancingCtx {
    /// Test constructor that accepts pre-built wallets for e2e tests
    /// that need real onchain interaction (e.g. with Anvil forks).
    #[builder]
    pub fn with_wallets(
        equity: ImbalanceThreshold,
        usdc: UsdcRebalancing,
        #[builder(default = Duration::from_secs(30 * 60))] transfer_timeout: Duration,
        #[builder(default = Duration::from_secs(60 * 60))] transfer_attempt_timeout: Duration,
    ) -> Self {
        let usdc = match usdc {
            UsdcRebalancing::Enabled { target, deviation } => {
                Some(ImbalanceThreshold { target, deviation })
            }
            UsdcRebalancing::Disabled => None,
        };

        Self {
            equity,
            usdc,
            transfer_timeout,
            transfer_attempt_timeout,
            circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
            token_messenger: st0x_bridge::cctp::TOKEN_MESSENGER_V2,
            message_transmitter: st0x_bridge::cctp::MESSAGE_TRANSMITTER_V2,
        }
    }

    /// Sets the Circle API base URL override (for e2e tests with local
    /// CCTP contracts and a mock attestation server).
    #[must_use]
    pub fn with_circle_api_base(mut self, base_url: String) -> Self {
        self.circle_api_base = base_url;
        self
    }

    /// Sets the CCTP contract address overrides (for e2e tests with
    /// locally deployed CCTP contracts).
    #[must_use]
    pub fn with_cctp_addresses(
        mut self,
        token_messenger: Address,
        message_transmitter: Address,
    ) -> Self {
        self.token_messenger = token_messenger;
        self.message_transmitter = message_transmitter;
        self
    }
}

impl std::fmt::Debug for RebalancingCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RebalancingCtx")
            .field("equity", &self.equity)
            .field("usdc", &self.usdc)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use st0x_float_macro::float;

    fn valid_rebalancing_config_toml() -> &'static str {
        r#"
            transfer_timeout_secs = 1800
            transfer_attempt_timeout_secs = 3600

            [equity]
            target = "0.5"
            deviation = "0.2"

            [usdc]
            mode = "enabled"
            target = "0.5"
            deviation = "0.3"
        "#
    }

    #[test]
    fn deserialize_config_succeeds() {
        let config: RebalancingConfig = toml::from_str(valid_rebalancing_config_toml()).unwrap();

        assert!(config.equity.target.eq(float!(0.5)).unwrap());
        assert!(config.equity.deviation.eq(float!(0.2)).unwrap());

        let UsdcRebalancing::Enabled { target, deviation } = config.usdc else {
            panic!("expected UsdcRebalancing::Enabled");
        };
        assert!(target.eq(float!(0.5)).unwrap());
        assert!(deviation.eq(float!(0.3)).unwrap());
        assert_eq!(config.transfer_timeout_secs, 1800);
        assert_eq!(config.transfer_attempt_timeout_secs, 3600);
    }

    #[test]
    fn deserialize_with_custom_thresholds() {
        let config: RebalancingConfig = toml::from_str(
            r#"
            transfer_timeout_secs = 1800
            transfer_attempt_timeout_secs = 3600

            [equity]
            target = "0.6"
            deviation = "0.1"

            [usdc]
            mode = "enabled"
            target = "0.4"
            deviation = "0.15"
        "#,
        )
        .unwrap();

        assert!(config.equity.target.eq(float!(0.6)).unwrap());
        assert!(config.equity.deviation.eq(float!(0.1)).unwrap());

        let UsdcRebalancing::Enabled { target, deviation } = config.usdc else {
            panic!("expected UsdcRebalancing::Enabled");
        };
        assert!(target.eq(float!(0.4)).unwrap());
        assert!(deviation.eq(float!(0.15)).unwrap());
    }

    #[test]
    fn deserialize_missing_transfer_timeout_secs_fails() {
        let toml_str = r#"
            [equity]
            target = "0.5"
            deviation = "0.2"

            [usdc]
            mode = "enabled"
            target = "0.5"
            deviation = "0.3"
        "#;

        let error = toml::from_str::<RebalancingConfig>(toml_str).unwrap_err();
        assert!(
            error.message().contains("transfer_timeout_secs"),
            "Expected missing transfer_timeout_secs error, got: {error}"
        );
    }

    #[test]
    fn deserialize_missing_equity_fails() {
        let toml_str = r#"
            transfer_timeout_secs = 1800

            [usdc]
            mode = "enabled"
            target = "0.5"
            deviation = "0.3"
        "#;

        let error = toml::from_str::<RebalancingConfig>(toml_str).unwrap_err();
        assert!(
            error.message().contains("equity"),
            "Expected missing equity error, got: {error}"
        );
    }

    #[test]
    fn deserialize_missing_usdc_fails() {
        let toml_str = r#"
            transfer_timeout_secs = 1800

            [equity]
            target = "0.5"
            deviation = "0.2"
        "#;

        let error = toml::from_str::<RebalancingConfig>(toml_str).unwrap_err();
        assert!(
            error.message().contains("usdc"),
            "Expected missing usdc error, got: {error}"
        );
    }

    #[test]
    fn deserialize_missing_transfer_attempt_timeout_secs_fails() {
        let toml_str = r#"
            transfer_timeout_secs = 1800

            [equity]
            target = "0.5"
            deviation = "0.2"

            [usdc]
            mode = "enabled"
            target = "0.5"
            deviation = "0.3"
        "#;

        let error = toml::from_str::<RebalancingConfig>(toml_str).unwrap_err();
        assert!(
            error.message().contains("transfer_attempt_timeout_secs"),
            "Expected missing transfer_attempt_timeout_secs error, got: {error}"
        );
    }

    #[test]
    fn zero_transfer_attempt_timeout_secs_fails_validation() {
        let config: RebalancingConfig = toml::from_str(
            r#"
            transfer_timeout_secs = 1800
            transfer_attempt_timeout_secs = 0

            [equity]
            target = "0.5"
            deviation = "0.2"

            [usdc]
            mode = "enabled"
            target = "0.5"
            deviation = "0.3"
        "#,
        )
        .unwrap();

        let error = RebalancingCtx::new(&config).unwrap_err();
        assert!(matches!(
            error,
            RebalancingCtxError::ZeroTransferAttemptTimeout
        ));
    }
}
