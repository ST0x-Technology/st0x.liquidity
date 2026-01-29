use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use clap::Parser;
use serde::Deserialize;
use sqlx::SqlitePool;
use std::path::PathBuf;
use tracing::Level;

use crate::offchain::order_poller::OrderPollerConfig;
use crate::onchain::EvmConfig;
use crate::rebalancing::{RebalancingConfig, RebalancingConfigError};
use crate::telemetry::{HyperDxConfig, HyperDxTomlConfig};
use st0x_execution::SupportedExecutor;
use st0x_execution::alpaca_broker_api::AlpacaBrokerApiAuthConfig;
use st0x_execution::alpaca_trading_api::AlpacaTradingApiAuthConfig;
use st0x_execution::schwab::SchwabAuthConfig;

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum BrokerConfig {
    Schwab(SchwabAuthConfig),
    AlpacaTradingApi(AlpacaTradingApiAuthConfig),
    AlpacaBrokerApi(AlpacaBrokerApiAuthConfig),
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

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error(transparent)]
    Rebalancing(#[from] RebalancingConfigError),
    #[error("ORDER_OWNER required when rebalancing is disabled")]
    MissingOrderOwner,
    #[error("failed to derive address from EVM_PRIVATE_KEY")]
    PrivateKeyDerivation(#[source] alloy::signers::k256::ecdsa::Error),
    #[error("failed to read config file")]
    Io(#[from] std::io::Error),
    #[error("failed to parse config file")]
    Toml(#[from] toml::de::Error),
}

#[derive(Debug, Clone)]
pub struct Config {
    pub(crate) database_url: String,
    pub log_level: LogLevel,
    pub(crate) server_port: u16,
    pub(crate) evm: EvmConfig,
    pub(crate) order_polling_interval: u64,
    pub(crate) order_polling_max_jitter: u64,
    pub(crate) broker: BrokerConfig,
    pub hyperdx: Option<HyperDxConfig>,
    pub(crate) rebalancing: Option<RebalancingConfig>,
}

impl<'de> Deserialize<'de> for Config {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct ConfigFields {
            database_url: String,
            log_level: Option<LogLevel>,
            server_port: Option<u16>,
            evm: EvmConfig,
            order_polling_interval: Option<u64>,
            order_polling_max_jitter: Option<u64>,
            broker: BrokerConfig,
            hyperdx: Option<HyperDxTomlConfig>,
            rebalancing: Option<RebalancingConfig>,
        }

        let fields = ConfigFields::deserialize(deserializer)?;

        let log_level = fields.log_level.unwrap_or(LogLevel::Debug);
        let log_level_tracing: Level = (&log_level).into();

        let hyperdx = fields
            .hyperdx
            .map(|h| HyperDxConfig::from_toml(h, log_level_tracing));

        Ok(Self {
            database_url: fields.database_url,
            log_level,
            server_port: fields.server_port.unwrap_or(8080),
            evm: fields.evm,
            order_polling_interval: fields.order_polling_interval.unwrap_or(15),
            order_polling_max_jitter: fields.order_polling_max_jitter.unwrap_or(5),
            broker: fields.broker,
            hyperdx,
            rebalancing: fields.rebalancing,
        })
    }
}

impl Config {
    pub fn load_file(path: &std::path::Path) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path)?;
        Self::load(&contents)
    }

    pub fn load(toml_str: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(toml_str)?;

        if config.rebalancing.is_some()
            && !matches!(config.broker, BrokerConfig::AlpacaBrokerApi(_))
        {
            return Err(RebalancingConfigError::NotAlpacaBroker.into());
        }

        if config.rebalancing.is_none() && config.evm.order_owner.is_none() {
            return Err(ConfigError::MissingOrderOwner);
        }

        Ok(config)
    }

    pub async fn get_sqlite_pool(&self) -> Result<SqlitePool, sqlx::Error> {
        configure_sqlite_pool(&self.database_url).await
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
pub mod tests {
    use super::*;
    use crate::onchain::EvmConfig;
    use alloy::primitives::{Address, FixedBytes, address};
    use st0x_execution::schwab::{SchwabAuthConfig, SchwabConfig};
    use st0x_execution::{MockExecutorConfig, TryIntoExecutor};

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
            alpaca_account_id = "904837e3-3b76-47ec-b432-046db621571b"
            [rebalancing.equity_threshold]
            target = "0.5"
            deviation = "0.2"
            [rebalancing.usdc_threshold]
            target = "0.5"
            deviation = "0.3"
        "#;

        let result = Config::load(toml);
        assert!(
            matches!(
                result,
                Err(ConfigError::Rebalancing(
                    RebalancingConfigError::NotAlpacaBroker
                ))
            ),
            "Expected NotAlpacaBroker error, got {result:?}"
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
        "#;

        let config = Config::load(toml).unwrap();
        let hyperdx = config.hyperdx.as_ref().expect("hyperdx should be Some");
        assert_eq!(hyperdx.api_key, "test-api-key");
        assert_eq!(hyperdx.service_name, "st0x-hedge");
        assert_eq!(hyperdx.log_level, Level::WARN);
    }
}
