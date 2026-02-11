use alloy::primitives::{Address, FixedBytes};
use alloy::signers::local::PrivateKeySigner;
use clap::Parser;
use rust_decimal::Decimal;
use serde::Deserialize;
use sqlx::SqlitePool;
use std::path::{Path, PathBuf};
use tracing::Level;
use url::Url;

use st0x_execution::{
    AlpacaBrokerApiCtx, AlpacaBrokerApiMode, AlpacaTradingApiCtx, AlpacaTradingApiMode,
    FractionalShares, Positive, SchwabCtx, SupportedExecutor,
};

use crate::offchain::order_poller::OrderPollerConfig;
use crate::onchain::{EvmConfig, EvmCtx, EvmSecrets};
use crate::rebalancing::{
    RebalancingConfig, RebalancingCtx, RebalancingCtxError, RebalancingSecrets,
};
use crate::telemetry::{TelemetryConfig, TelemetryCtx, TelemetrySecrets};
use crate::threshold::{ExecutionThreshold, InvalidThresholdError, Usdc};

#[derive(Parser, Debug)]
pub struct Env {
    /// Path to plaintext TOML configuration file
    #[clap(long)]
    pub config: PathBuf,
    /// Path to encrypted TOML secrets file
    #[clap(long)]
    pub secrets: PathBuf,
}

/// Non-secret settings deserialized from the plaintext config TOML.
#[derive(Deserialize)]
struct Config {
    database_url: String,
    log_level: Option<LogLevel>,
    server_port: Option<u16>,
    evm: EvmConfig,
    order_polling_interval: Option<u64>,
    order_polling_max_jitter: Option<u64>,
    #[serde(rename = "hyperdx")]
    telemetry: Option<TelemetryConfig>,
    rebalancing: Option<RebalancingConfig>,
}

/// Secret credentials deserialized from the encrypted secrets TOML.
#[derive(Deserialize)]
struct Secrets {
    evm: EvmSecrets,
    broker: BrokerSecrets,
    #[serde(rename = "hyperdx")]
    telemetry: Option<TelemetrySecrets>,
    rebalancing: Option<RebalancingSecrets>,
}

/// Broker type tag and all broker credentials.
/// Deserialized from the `[broker]` section of the secrets TOML.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum BrokerSecrets {
    Schwab {
        app_key: String,
        app_secret: String,
        redirect_uri: Option<Url>,
        base_url: Option<Url>,
        account_index: Option<usize>,
        encryption_key: FixedBytes<32>,
    },
    AlpacaTradingApi {
        api_key: String,
        api_secret: String,
        trading_mode: Option<AlpacaTradingApiMode>,
    },
    AlpacaBrokerApi {
        api_key: String,
        api_secret: String,
        account_id: String,
        mode: Option<AlpacaBrokerApiMode>,
    },
    DryRun,
}

// ===== Runtime types (assembled from Config + Secrets) =====

/// Combined runtime context for the server. Assembled from plaintext config,
/// encrypted secrets, and derived runtime state.
#[derive(Debug, Clone)]
pub struct Ctx {
    pub(crate) database_url: String,
    pub log_level: LogLevel,
    pub(crate) server_port: u16,
    pub(crate) evm: EvmCtx,
    pub(crate) order_polling_interval: u64,
    pub(crate) order_polling_max_jitter: u64,
    pub(crate) broker: BrokerConfig,
    pub(crate) telemetry: Option<TelemetryCtx>,
    pub(crate) rebalancing: Option<RebalancingCtx>,
    pub(crate) execution_threshold: ExecutionThreshold,
}

/// Runtime broker configuration assembled from `BrokerSecrets`.
#[derive(Debug, Clone)]
pub enum BrokerConfig {
    Schwab(SchwabAuth),
    AlpacaTradingApi(AlpacaTradingApiCtx),
    AlpacaBrokerApi(AlpacaBrokerApiCtx),
    DryRun,
}

impl BrokerConfig {
    pub fn to_supported_executor(&self) -> SupportedExecutor {
        match self {
            Self::Schwab(_) => SupportedExecutor::Schwab,
            Self::AlpacaTradingApi(_) => SupportedExecutor::AlpacaTradingApi,
            Self::AlpacaBrokerApi(_) => SupportedExecutor::AlpacaBrokerApi,
            Self::DryRun => SupportedExecutor::DryRun,
        }
    }
}

/// Schwab auth credentials used by the main crate for token management
/// and executor construction.
#[derive(Debug, Clone)]
pub struct SchwabAuth {
    pub(crate) app_key: String,
    pub(crate) app_secret: String,
    pub(crate) redirect_uri: Option<Url>,
    pub(crate) base_url: Option<Url>,
    pub(crate) account_index: Option<usize>,
    pub(crate) encryption_key: FixedBytes<32>,
}

impl SchwabAuth {
    pub(crate) fn to_schwab_ctx(&self, pool: SqlitePool) -> SchwabCtx {
        SchwabCtx {
            app_key: self.app_key.clone(),
            app_secret: self.app_secret.clone(),
            redirect_uri: self.redirect_uri.clone(),
            base_url: self.base_url.clone(),
            account_index: self.account_index,
            encryption_key: self.encryption_key,
            pool,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl From<LogLevel> for Level {
    fn from(log_level: LogLevel) -> Self {
        match log_level {
            LogLevel::Trace => Self::TRACE,
            LogLevel::Debug => Self::DEBUG,
            LogLevel::Info => Self::INFO,
            LogLevel::Warn => Self::WARN,
            LogLevel::Error => Self::ERROR,
        }
    }
}

impl From<&LogLevel> for Level {
    fn from(log_level: &LogLevel) -> Self {
        match log_level {
            LogLevel::Trace => Self::TRACE,
            LogLevel::Debug => Self::DEBUG,
            LogLevel::Info => Self::INFO,
            LogLevel::Warn => Self::WARN,
            LogLevel::Error => Self::ERROR,
        }
    }
}

pub(crate) trait HasSqlite {
    async fn get_sqlite_pool(&self) -> Result<SqlitePool, sqlx::Error>;
}

pub(crate) async fn configure_sqlite_pool(database_url: &str) -> Result<SqlitePool, sqlx::Error> {
    let pool = SqlitePool::connect(database_url).await?;

    // SQLite Concurrency Configuration:
    //
    // WAL Mode: Allows concurrent readers but only ONE writer at a time across
    // all processes. When both main bot and reporter try to write simultaneously,
    // one will block until the other completes. This is a fundamental SQLite
    // limitation.
    sqlx::query("PRAGMA journal_mode = WAL")
        .execute(&pool)
        .await?;

    // Busy Timeout: 10 seconds - when a write is blocked by another process,
    // SQLite will wait up to 10 seconds before failing with "database is locked".
    // This prevents immediate failures when main bot and reporter write concurrently.
    //
    // CRITICAL: Reporter must keep transactions SHORT (single INSERT per trade)
    // to avoid blocking mission-critical main bot operations.
    //
    // Future: This limitation will be eliminated when migrating to Kafka +
    // Elasticsearch with CQRS pattern for separate read/write paths.
    sqlx::query("PRAGMA busy_timeout = 10000")
        .execute(&pool)
        .await?;

    Ok(pool)
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error(transparent)]
    Rebalancing(#[from] RebalancingCtxError),
    #[error("ORDER_OWNER required when rebalancing is disabled")]
    MissingOrderOwner,
    #[error("failed to derive address from EVM_PRIVATE_KEY")]
    PrivateKeyDerivation(#[source] alloy::signers::k256::ecdsa::Error),
    #[error("failed to read config file")]
    Io(#[from] std::io::Error),
    #[error("failed to parse TOML")]
    Toml(#[from] toml::de::Error),
    #[error(transparent)]
    InvalidThreshold(#[from] InvalidThresholdError),
    #[error(transparent)]
    InvalidShares(#[from] st0x_execution::InvalidSharesError),
    #[error("telemetry config present in config but telemetry secrets missing")]
    TelemetrySecretsMissing,
    #[error("telemetry secrets present but telemetry config missing in config")]
    TelemetryConfigMissing,
    #[error("rebalancing config present in config but rebalancing secrets missing")]
    RebalancingSecretsMissing,
    #[error("rebalancing secrets present but rebalancing config missing in config")]
    RebalancingConfigMissing,
}

#[cfg(test)]
impl ConfigError {
    fn kind(&self) -> &'static str {
        match self {
            Self::Rebalancing(_) => "rebalancing configuration error",
            Self::MissingOrderOwner => "ORDER_OWNER required when rebalancing is disabled",
            Self::PrivateKeyDerivation(_) => "failed to derive address from EVM_PRIVATE_KEY",
            Self::Io(_) => "failed to read config file",
            Self::Toml(_) => "failed to parse TOML",
            Self::InvalidThreshold(_) => "invalid execution threshold",
            Self::InvalidShares(_) => "invalid shares value",
            Self::TelemetrySecretsMissing => "telemetry secrets missing",
            Self::TelemetryConfigMissing => "telemetry config missing",
            Self::RebalancingSecretsMissing => "rebalancing secrets missing",
            Self::RebalancingConfigMissing => "rebalancing config missing",
        }
    }
}


impl Ctx {
    pub fn load_files(config: &Path, secrets: &Path) -> Result<Self, ConfigError> {
        let config_str = std::fs::read_to_string(config)?;
        let secrets_str = std::fs::read_to_string(secrets)?;
        Self::from_toml(&config_str, &secrets_str)
    }

    pub fn from_toml(config_toml: &str, secrets_toml: &str) -> Result<Self, ConfigError> {
        let config: Config = toml::from_str(config_toml)?;
        let secrets: Secrets = toml::from_str(secrets_toml)?;

        let broker = assemble_broker(secrets.broker);
        let evm = assemble_evm(config.evm, secrets.evm);
        let telemetry = assemble_telemetry(config.telemetry, secrets.telemetry)?;

        // Execution threshold is determined by broker capabilities:
        // - Schwab API doesn't support fractional shares, so use 1 whole share threshold
        // - Alpaca requires $1 minimum for fractional trading. We use $2 to provide buffer
        //   for slippage, fees, and price discrepancies that could push fills below $1.
        // - DryRun uses shares threshold for testing
        let execution_threshold = derive_execution_threshold(&broker)?;

        // Rebalancing requires both config and secrets, plus an AlpacaBrokerApi broker.
        let rebalancing =
            assemble_rebalancing(config.rebalancing, secrets.rebalancing, &broker)?;

        let log_level = config.log_level.unwrap_or(LogLevel::Debug);

        if rebalancing.is_none() && evm.order_owner.is_none() {
            return Err(ConfigError::MissingOrderOwner);
        }

        Ok(Self {
            database_url: config.database_url,
            log_level,
            server_port: config.server_port.unwrap_or(8080),
            evm,
            order_polling_interval: config.order_polling_interval.unwrap_or(15),
            order_polling_max_jitter: config.order_polling_max_jitter.unwrap_or(5),
            broker,
            telemetry,
            rebalancing,
            execution_threshold,
        })
    }

    pub async fn get_sqlite_pool(&self) -> Result<SqlitePool, sqlx::Error> {
        configure_sqlite_pool(&self.database_url).await
    }

    pub(crate) fn rebalancing_config(&self) -> Option<&RebalancingCtx> {
        self.rebalancing.as_ref()
    }

    pub const fn get_order_poller_config(&self) -> OrderPollerConfig {
        OrderPollerConfig {
            polling_interval: std::time::Duration::from_secs(self.order_polling_interval),
            max_jitter: std::time::Duration::from_secs(self.order_polling_max_jitter),
        }
    }

    pub(crate) fn order_owner(&self) -> Result<Address, ConfigError> {
        match (&self.rebalancing, self.evm.order_owner) {
            (Some(r), _) => {
                let signer = PrivateKeySigner::from_bytes(&r.evm_private_key)
                    .map_err(ConfigError::PrivateKeyDerivation)?;
                Ok(signer.address())
            }
            (None, Some(addr)) => Ok(addr),
            (None, None) => Err(ConfigError::MissingOrderOwner),
        }
    }
}

#[derive(Parser, Debug)]
pub struct Env {
    /// Path to TOML configuration file
    #[clap(long)]
    pub config_file: PathBuf,
}

pub fn setup_tracing(log_level: &LogLevel) {
    let level: Level = log_level.into();
    let default_filter = format!("st0x_hedge={level},st0x_execution={level}");

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| default_filter.into()),
        )
        .init();
}

#[cfg(test)]
pub(crate) mod tests {
    use alloy::primitives::{Address, FixedBytes, address};
    use st0x_execution::schwab::{SchwabAuthConfig, SchwabConfig};
    use st0x_execution::{MockExecutorConfig, TryIntoExecutor};
    use tracing_test::traced_test;

    use super::*;
    use crate::onchain::EvmConfig;
    use crate::threshold::ExecutionThreshold;

    const TEST_ENCRYPTION_KEY: FixedBytes<32> = FixedBytes::ZERO;

    pub fn create_test_config_with_order_owner(order_owner: Address) -> Config {
        Config {
            database_url: ":memory:".to_owned(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            evm: EvmConfig {
                ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1111111111111111111111111111111111111111"),
                order_owner: Some(order_owner),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            broker: BrokerConfig::Schwab(SchwabAuthConfig {
                app_key: "test_key".to_owned(),
                app_secret: "test_secret".to_owned(),
                redirect_uri: None,
                base_url: Some(url::Url::parse("https://test.com").unwrap()),
                account_index: None,
                encryption_key: TEST_ENCRYPTION_KEY,
            }),
            hyperdx: None,
            rebalancing: None,
            execution_threshold: ExecutionThreshold::whole_share(),
        }
    }

    pub fn create_test_config() -> Config {
        create_test_config_with_order_owner(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
    }

    fn minimal_toml_with_broker(broker_section: &str) -> String {
        format!(
            r#"
            database_url = ":memory:"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
            {broker_section}
            "#
        )
    }

    fn dry_run_toml() -> String {
        minimal_toml_with_broker(
            r#"
            [broker]
            type = "dry-run"
            "#,
        )
    }

    fn schwab_toml() -> String {
        minimal_toml_with_broker(
            r#"
            [broker]
            type = "schwab"
            app_key = "test_key"
            app_secret = "test_secret"
            encryption_key = "0x0000000000000000000000000000000000000000000000000000000000000000"
            "#,
        )
    }

    fn schwab_toml_without_order_owner() -> &'static str {
        r#"
            database_url = ":memory:"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            orderbook = "0x1111111111111111111111111111111111111111"
            deployment_block = 1
            [broker]
            type = "schwab"
            app_key = "test_key"
            app_secret = "test_secret"
            encryption_key = "0x0000000000000000000000000000000000000000000000000000000000000000"
            "#
    }

    fn example_toml() -> &'static str {
        include_str!("../example.toml")
    }

    fn example_toml_with_private_key(evm_private_key: &str) -> String {
        example_toml().replacen(
            "evm_private_key = \"0x0000000000000000000000000000000000000000000000000000000000000001\"",
            &format!("evm_private_key = \"{evm_private_key}\""),
            1,
        )
    }

    #[test]
    fn test_log_level_from_conversion() {
        let level: Level = LogLevel::Trace.into();
        assert_eq!(Level::TRACE, level);

        let level: Level = LogLevel::Debug.into();
        assert_eq!(Level::DEBUG, level);

        let level: Level = LogLevel::Info.into();
        assert_eq!(Level::INFO, level);

        let level: Level = LogLevel::Warn.into();
        assert_eq!(Level::WARN, level);

        let level: Level = LogLevel::Error.into();
        assert_eq!(Level::ERROR, level);

        let log_level = LogLevel::Debug;
        let level: Level = (&log_level).into();
        assert_eq!(level, Level::DEBUG);
    }

    #[tokio::test]
    async fn test_config_sqlite_pool_creation() {
        let config = create_test_config();
        let pool_result = config.get_sqlite_pool().await;
        assert!(pool_result.is_ok());
    }

    #[tokio::test]
    async fn test_get_broker_types() {
        let config = create_test_config();
        let pool = crate::test_utils::setup_test_db().await;

        let BrokerConfig::Schwab(schwab_auth) = &config.broker else {
            panic!("Expected Schwab broker config");
        };
        let schwab_config = SchwabConfig {
            auth: schwab_auth.clone(),
            pool: pool.clone(),
        };
        let schwab_result = schwab_config.try_into_executor().await;
        assert!(schwab_result.is_err());

        let test_executor = MockExecutorConfig.try_into_executor().await.unwrap();
        assert!(format!("{test_executor:?}").contains("MockExecutor"));
    }

    #[test]
    fn dry_run_broker_does_not_require_any_credentials() {
        let config = Config::load(&dry_run_toml()).unwrap();
        assert!(matches!(config.broker, BrokerConfig::DryRun));
    }

    #[test]
    fn rebalancing_absent_means_none() {
        let config = Config::load(&dry_run_toml()).unwrap();
        assert!(config.rebalancing.is_none());
    }

    #[test]
    fn defaults_applied_when_optional_fields_omitted() {
        let config = Config::load(&dry_run_toml()).unwrap();
        assert!(matches!(config.log_level, LogLevel::Debug));
        assert_eq!(config.server_port, 8080);
        assert_eq!(config.order_polling_interval, 15);
        assert_eq!(config.order_polling_max_jitter, 5);
    }

    #[test]
    fn optional_fields_override_defaults() {
        let toml = r#"
            database_url = ":memory:"
            log_level = "warn"
            server_port = 9090
            order_polling_interval = 30
            order_polling_max_jitter = 10
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
            [broker]
            type = "dry-run"
        "#;

        let config = Config::load(toml).unwrap();
        assert!(matches!(config.log_level, LogLevel::Warn));
        assert_eq!(config.server_port, 9090);
        assert_eq!(config.order_polling_interval, 30);
        assert_eq!(config.order_polling_max_jitter, 10);
    }

    #[test]
    fn rebalancing_with_schwab_fails() {
        let toml = r#"
            database_url = ":memory:"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            orderbook = "0x1111111111111111111111111111111111111111"
            deployment_block = 1
            [broker]
            type = "schwab"
            app_key = "test_key"
            app_secret = "test_secret"
            encryption_key = "0x0000000000000000000000000000000000000000000000000000000000000000"
            [rebalancing]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ethereum_rpc_url = "https://mainnet.infura.io"
            evm_private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            usdc_vault_id = "0x0000000000000000000000000000000000000000000000000000000000000001"
            [rebalancing.equity_threshold]
            target = "0.5"
            deviation = "0.2"
            [rebalancing.usdc_threshold]
            target = "0.5"
            deviation = "0.3"
        "#;

        let result = Config::load(toml);
        assert!(
            matches!(result, Err(ConfigError::Toml(_))),
            "Expected Toml error for rebalancing with non-Alpaca broker, got {result:?}"
        );
    }

    #[test]
    fn schwab_without_order_owner_fails() {
        let result = Config::load(schwab_toml_without_order_owner());
        assert!(
            matches!(result, Err(ConfigError::MissingOrderOwner)),
            "Expected MissingOrderOwner error, got {result:?}"
        );
    }

    #[test]
    fn schwab_with_order_owner_succeeds() {
        let config = Config::load(&schwab_toml()).unwrap();
        let order_owner = config.order_owner().unwrap();
        assert_eq!(
            order_owner,
            address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
    }

    #[test]
    fn rebalancing_derives_order_owner_from_private_key() {
        let config = Config::load(example_toml()).unwrap();

        let order_owner = config.order_owner().unwrap();
        assert_eq!(
            order_owner,
            address!("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")
        );
    }

    #[test]
    fn rebalancing_ignores_evm_order_owner_field() {
        let toml = example_toml().replace(
            "deployment_block = 1",
            "order_owner = \"0xcccccccccccccccccccccccccccccccccccccccc\"\ndeployment_block = 1",
        );

        let config = Config::load(&toml).unwrap();
        let order_owner = config.order_owner().unwrap();
        assert_eq!(
            order_owner,
            address!("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"),
            "order_owner() should derive from evm_private_key when rebalancing is enabled"
        );
    }

    #[test]
    fn rebalancing_with_invalid_private_key_fails() {
        let config = Config::load(&example_toml_with_private_key(
            "0x0000000000000000000000000000000000000000000000000000000000000000",
        ))
        .unwrap();

        let result = config.order_owner();
        assert!(
            matches!(result, Err(ConfigError::PrivateKeyDerivation(_))),
            "Expected PrivateKeyDerivation error for zero private key, got {result:?}"
        );
    }

    #[test]
    fn hyperdx_config_constructed_from_toml() {
        let toml = r#"
            database_url = ":memory:"
            log_level = "warn"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
            [broker]
            type = "dry-run"
            [hyperdx]
            api_key = "test-api-key"
            service_name = "test-service"
        "#;

        let config = Config::load(toml).unwrap();
        let hyperdx = config.hyperdx.as_ref().expect("hyperdx should be Some");
        assert_eq!(hyperdx.api_key, "test-api-key");
        assert_eq!(hyperdx.service_name, "test-service");
    }

    #[test]
    fn default_execution_threshold_is_one_share_for_dry_run() {
        let config = Config::load(&dry_run_toml()).unwrap();
        assert_eq!(
            config.execution_threshold,
            ExecutionThreshold::shares(Positive::<FractionalShares>::ONE)
        );
    }

    #[test]
    fn schwab_executor_uses_shares_threshold() {
        let config = Config::load(&schwab_toml()).unwrap();
        assert_eq!(
            config.execution_threshold,
            ExecutionThreshold::shares(Positive::<FractionalShares>::ONE)
        );
    }

    #[test]
    fn alpaca_trading_api_executor_uses_dollar_threshold() {
        let toml = minimal_toml_with_broker(
            r#"
            [broker]
            type = "alpaca-trading-api"
            api_key = "test-key"
            api_secret = "test-secret"
            "#,
        );

        let config = Config::load(&toml).unwrap();
        let expected = ExecutionThreshold::dollar_value(Usdc(Decimal::TWO)).unwrap();
        assert_eq!(config.execution_threshold, expected);
    }

    #[test]
    fn alpaca_broker_api_executor_uses_dollar_threshold() {
        let toml = minimal_toml_with_broker(
            r#"
            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "test-account-id"
            "#,
        );

        let config = Config::load(&toml).unwrap();
        let expected = ExecutionThreshold::dollar_value(Usdc(Decimal::TWO)).unwrap();
        assert_eq!(config.execution_threshold, expected);
    }

    #[test]
    fn config_error_kind_rebalancing() {
        let err = ConfigError::Rebalancing(RebalancingConfigError::NotAlpacaBroker);
        assert_eq!(err.kind(), "rebalancing configuration error");
    }

    #[test]
    fn config_error_kind_invalid_threshold() {
        let err = ConfigError::InvalidThreshold(InvalidThresholdError::ZeroDollarValue);
        assert_eq!(err.kind(), "invalid execution threshold");
    }

    #[traced_test]
    #[test]
    fn rebalancing_with_schwab_logs_error_kind() {
        let toml = r#"
            database_url = ":memory:"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
            [broker]
            type = "schwab"
            app_key = "test_key"
            app_secret = "test_secret"
            encryption_key = "0x0000000000000000000000000000000000000000000000000000000000000000"
            [rebalancing]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ethereum_rpc_url = "https://mainnet.infura.io"
            evm_private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            usdc_vault_id = "0x0000000000000000000000000000000000000000000000000000000000000001"
            [rebalancing.equity_threshold]
            target = "0.5"
            deviation = "0.2"
            [rebalancing.usdc_threshold]
            target = "0.5"
            deviation = "0.3"
        "#;

        let result = Config::load(toml);
        assert!(result.is_err());
    }

    #[test]
    fn rebalancing_config_returns_some_when_present() {
        let config = Config::load(example_toml()).unwrap();
        assert!(
            config.rebalancing_config().is_some(),
            "rebalancing_config() should return Some when rebalancing is configured"
        );
    }

    #[test]
    fn rebalancing_config_returns_none_when_absent() {
        let config = Config::load(&dry_run_toml()).unwrap();
        assert!(
            config.rebalancing_config().is_none(),
            "rebalancing_config() should return None when rebalancing is not configured"
        );
    }
}
