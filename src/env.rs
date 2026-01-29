use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use clap::Parser;
use sqlx::SqlitePool;
use tracing::Level;

use crate::offchain::order_poller::OrderPollerConfig;
use crate::onchain::EvmEnv;
use crate::rebalancing::{RebalancingConfig, RebalancingConfigError};
use crate::telemetry::HyperDxConfig;
use st0x_execution::SupportedExecutor;
use st0x_execution::alpaca_broker_api::AlpacaBrokerApiAuthConfig;
use st0x_execution::alpaca_trading_api::AlpacaTradingApiAuthConfig;
use st0x_execution::schwab::SchwabAuthConfig;

// Dummy program name required by clap when parsing from environment variables.
// clap's try_parse_from expects argv[0] to be the program name, but we only
// care about environment variables, so this is just a placeholder.
const DUMMY_PROGRAM_NAME: &[&str] = &["server"];

#[derive(Debug, Clone)]
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

#[derive(clap::ValueEnum, Debug, Clone)]
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
    #[error(transparent)]
    Clap(#[from] clap::Error),
    #[error("ORDER_OWNER required when rebalancing is disabled")]
    MissingOrderOwner,
    #[error("failed to derive address from EVM_PRIVATE_KEY")]
    PrivateKeyDerivation(#[source] alloy::signers::k256::ecdsa::Error),
}

#[derive(Debug, Clone)]
pub struct Config {
    pub(crate) database_url: String,
    pub log_level: LogLevel,
    pub(crate) server_port: u16,
    pub(crate) evm: EvmEnv,
    pub(crate) order_polling_interval: u64,
    pub(crate) order_polling_max_jitter: u64,
    pub(crate) broker: BrokerConfig,
    pub hyperdx: Option<HyperDxConfig>,
    pub(crate) rebalancing: Option<RebalancingConfig>,
}

#[derive(Parser, Debug, Clone)]
pub struct Env {
    #[clap(long = "db", env)]
    database_url: String,
    #[clap(long, env, default_value = "debug")]
    log_level: LogLevel,
    #[clap(long, env, default_value = "8080")]
    server_port: u16,
    #[clap(flatten)]
    pub(crate) evm: EvmEnv,
    /// Interval in seconds between order status polling checks
    #[clap(long, env, default_value = "15")]
    order_polling_interval: u64,
    /// Maximum jitter in seconds for order polling to prevent thundering herd
    #[clap(long, env, default_value = "5")]
    order_polling_max_jitter: u64,
    /// Executor to use for trading (schwab, alpaca-trading-api, alpaca-broker-api, or dry-run)
    #[clap(long, env)]
    executor: SupportedExecutor,
    /// HyperDX API key for observability (optional)
    #[clap(long, env)]
    hyperdx_api_key: Option<String>,
    /// Service name for HyperDX traces (only used when hyperdx_api_key is set)
    #[clap(long, env, default_value = "st0x-hedge")]
    hyperdx_service_name: String,
    /// Enable rebalancing operations (requires Alpaca Broker API)
    #[clap(long, env, default_value = "false", action = clap::ArgAction::Set)]
    rebalancing_enabled: bool,
}

impl Env {
    pub fn into_config(self) -> Result<Config, ConfigError> {
        let broker = match self.executor {
            SupportedExecutor::Schwab => {
                let schwab_auth = SchwabAuthConfig::try_parse_from(DUMMY_PROGRAM_NAME)?;
                BrokerConfig::Schwab(schwab_auth)
            }
            SupportedExecutor::AlpacaTradingApi => {
                let alpaca_auth = AlpacaTradingApiAuthConfig::try_parse_from(DUMMY_PROGRAM_NAME)?;
                BrokerConfig::AlpacaTradingApi(alpaca_auth)
            }
            SupportedExecutor::AlpacaBrokerApi => {
                let alpaca_auth = AlpacaBrokerApiAuthConfig::try_parse_from(DUMMY_PROGRAM_NAME)?;
                BrokerConfig::AlpacaBrokerApi(alpaca_auth)
            }
            SupportedExecutor::DryRun => BrokerConfig::DryRun,
        };

        let log_level_tracing: Level = (&self.log_level).into();
        let hyperdx = self.hyperdx_api_key.map(|api_key| HyperDxConfig {
            api_key,
            service_name: self.hyperdx_service_name,
            log_level: log_level_tracing,
        });

        let rebalancing = if self.rebalancing_enabled {
            if !matches!(broker, BrokerConfig::AlpacaBrokerApi(_)) {
                return Err(RebalancingConfigError::NotAlpacaBroker.into());
            }
            Some(RebalancingConfig::from_env()?)
        } else {
            None
        };

        if rebalancing.is_none() && self.evm.order_owner.is_none() {
            return Err(ConfigError::MissingOrderOwner);
        }

        Ok(Config {
            database_url: self.database_url,
            log_level: self.log_level,
            server_port: self.server_port,
            evm: self.evm,
            order_polling_interval: self.order_polling_interval,
            order_polling_max_jitter: self.order_polling_max_jitter,
            broker,
            hyperdx,
            rebalancing,
        })
    }
}

impl Config {
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
    use crate::onchain::EvmEnv;
    use alloy::primitives::{Address, FixedBytes, address};
    use rust_decimal::Decimal;
    use st0x_execution::schwab::{SchwabAuthConfig, SchwabConfig};
    use st0x_execution::{MockExecutorConfig, TryIntoExecutor};

    const TEST_ENCRYPTION_KEY: FixedBytes<32> = FixedBytes::ZERO;

    pub fn create_test_config_with_order_owner(order_owner: Address) -> Config {
        Config {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            evm: EvmEnv {
                ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1111111111111111111111111111111111111111"),
                order_owner: Some(order_owner),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            broker: BrokerConfig::Schwab(SchwabAuthConfig {
                app_key: "test_key".to_string(),
                app_secret: "test_secret".to_string(),
                redirect_uri: "https://127.0.0.1".to_string(),
                base_url: "https://test.com".to_string(),
                account_index: 0,
                encryption_key: TEST_ENCRYPTION_KEY,
            }),
            hyperdx: None,
            rebalancing: None,
        }
    }

    pub fn create_test_config() -> Config {
        create_test_config_with_order_owner(address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
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

        // Test reference conversion
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

        // SchwabBroker creation should fail without valid tokens
        let BrokerConfig::Schwab(schwab_auth) = &config.broker else {
            panic!("Expected Schwab broker config");
        };
        let schwab_config = SchwabConfig {
            auth: schwab_auth.clone(),
            pool: pool.clone(),
        };
        let schwab_result = schwab_config.try_into_executor().await;
        assert!(schwab_result.is_err());

        // MockExecutor should always work
        let test_executor = MockExecutorConfig.try_into_executor().await.unwrap();
        assert!(format!("{test_executor:?}").contains("MockExecutor"));
    }

    #[test]
    fn test_config_construction() {
        let config = create_test_config();
        assert_eq!(config.database_url, ":memory:");
        assert!(matches!(config.log_level, LogLevel::Debug));
        assert!(matches!(config.broker, BrokerConfig::Schwab(_)));
        assert_eq!(config.evm.deployment_block, 1);
    }

    #[test]
    fn test_dry_run_broker_does_not_require_any_credentials() {
        // Explicitly unset REBALANCING_ENABLED to avoid env var pollution
        temp_env::with_vars([("REBALANCING_ENABLED", None::<&str>)], || {
            let args = vec![
                "test",
                "--db",
                ":memory:",
                "--ws-rpc-url",
                "ws://localhost:8545",
                "--orderbook",
                "0x1111111111111111111111111111111111111111",
                "--order-owner",
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "--deployment-block",
                "1",
                "--executor",
                "dry-run",
            ];

            let env = Env::try_parse_from(args).unwrap();
            let config = env.into_config().unwrap();
            assert!(matches!(config.broker, BrokerConfig::DryRun));
        });
    }

    #[test]
    fn rebalancing_disabled_by_default() {
        // Explicitly unset REBALANCING_ENABLED to avoid env var pollution
        temp_env::with_vars([("REBALANCING_ENABLED", None::<&str>)], || {
            let args = vec![
                "test",
                "--db",
                ":memory:",
                "--ws-rpc-url",
                "ws://localhost:8545",
                "--orderbook",
                "0x1111111111111111111111111111111111111111",
                "--order-owner",
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "--deployment-block",
                "1",
                "--executor",
                "dry-run",
            ];

            let env = Env::try_parse_from(args).unwrap();
            let config = env.into_config().unwrap();
            assert!(config.rebalancing.is_none());
        });
    }

    #[test]
    fn rebalancing_enabled_with_schwab_fails() {
        temp_env::with_vars(
            [
                ("SCHWAB_APP_KEY", Some("test_key")),
                ("SCHWAB_APP_SECRET", Some("test_secret")),
                ("SCHWAB_REDIRECT_URI", Some("https://127.0.0.1")),
                ("SCHWAB_BASE_URL", Some("https://test.com")),
                ("SCHWAB_ACCOUNT_INDEX", Some("0")),
                (
                    "ENCRYPTION_KEY",
                    Some("0000000000000000000000000000000000000000000000000000000000000000"),
                ),
            ],
            || {
                let args = vec![
                    "test",
                    "--db",
                    ":memory:",
                    "--ws-rpc-url",
                    "ws://localhost:8545",
                    "--orderbook",
                    "0x1111111111111111111111111111111111111111",
                    "--order-owner",
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "--deployment-block",
                    "1",
                    "--executor",
                    "schwab",
                    "--rebalancing-enabled",
                    "true",
                ];

                let env = Env::try_parse_from(args).unwrap();
                let result = env.into_config();
                assert!(
                    matches!(
                        result,
                        Err(ConfigError::Rebalancing(
                            RebalancingConfigError::NotAlpacaBroker
                        ))
                    ),
                    "Expected NotAlpacaBroker error, got {result:?}"
                );
            },
        );
    }

    #[test]
    fn rebalancing_enabled_missing_redemption_wallet_fails() {
        temp_env::with_vars(
            [
                ("ALPACA_BROKER_API_KEY", Some("test_key")),
                ("ALPACA_BROKER_API_SECRET", Some("test_secret")),
                (
                    "ALPACA_ACCOUNT_ID",
                    Some("904837e3-3b76-47ec-b432-046db621571b"),
                ),
                ("ETHEREUM_RPC_URL", Some("https://mainnet.infura.io")),
                (
                    "EVM_PRIVATE_KEY",
                    Some("0x0000000000000000000000000000000000000000000000000000000000000001"),
                ),
                ("BASE_RPC_URL", Some("https://base.example.com")),
                (
                    "BASE_ORDERBOOK",
                    Some("0x2222222222222222222222222222222222222222"),
                ),
                (
                    "USDC_VAULT_ID",
                    Some("0x0000000000000000000000000000000000000000000000000000000000000001"),
                ),
                // Explicitly unset to avoid env pollution
                ("REDEMPTION_WALLET", None),
            ],
            || {
                let args = vec![
                    "test",
                    "--db",
                    ":memory:",
                    "--ws-rpc-url",
                    "ws://localhost:8545",
                    "--orderbook",
                    "0x1111111111111111111111111111111111111111",
                    "--order-owner",
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "--deployment-block",
                    "1",
                    "--executor",
                    "alpaca-broker-api",
                    "--rebalancing-enabled",
                    "true",
                ];

                let env = Env::try_parse_from(args).unwrap();
                let result = env.into_config();
                assert!(
                    matches!(
                        result,
                        Err(ConfigError::Rebalancing(RebalancingConfigError::Clap(_)))
                    ),
                    "Expected clap error for missing redemption_wallet, got {result:?}"
                );
            },
        );
    }

    #[test]
    fn rebalancing_enabled_missing_ethereum_rpc_url_fails() {
        temp_env::with_vars(
            [
                ("ALPACA_BROKER_API_KEY", Some("test_key")),
                ("ALPACA_BROKER_API_SECRET", Some("test_secret")),
                (
                    "ALPACA_ACCOUNT_ID",
                    Some("904837e3-3b76-47ec-b432-046db621571b"),
                ),
                (
                    "REDEMPTION_WALLET",
                    Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                ),
                (
                    "EVM_PRIVATE_KEY",
                    Some("0x0000000000000000000000000000000000000000000000000000000000000001"),
                ),
                ("BASE_RPC_URL", Some("https://base.example.com")),
                (
                    "BASE_ORDERBOOK",
                    Some("0x2222222222222222222222222222222222222222"),
                ),
                (
                    "USDC_VAULT_ID",
                    Some("0x0000000000000000000000000000000000000000000000000000000000000001"),
                ),
                // Explicitly unset to avoid env pollution
                ("ETHEREUM_RPC_URL", None),
            ],
            || {
                let args = vec![
                    "test",
                    "--db",
                    ":memory:",
                    "--ws-rpc-url",
                    "ws://localhost:8545",
                    "--orderbook",
                    "0x1111111111111111111111111111111111111111",
                    "--order-owner",
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "--deployment-block",
                    "1",
                    "--executor",
                    "alpaca-broker-api",
                    "--rebalancing-enabled",
                    "true",
                ];

                let env = Env::try_parse_from(args).unwrap();
                let result = env.into_config();
                assert!(
                    matches!(
                        result,
                        Err(ConfigError::Rebalancing(RebalancingConfigError::Clap(_)))
                    ),
                    "Expected clap error for missing ethereum_rpc_url, got {result:?}"
                );
            },
        );
    }

    #[test]
    fn rebalancing_enabled_missing_evm_private_key_fails() {
        temp_env::with_vars(
            [
                ("ALPACA_BROKER_API_KEY", Some("test_key")),
                ("ALPACA_BROKER_API_SECRET", Some("test_secret")),
                (
                    "ALPACA_ACCOUNT_ID",
                    Some("904837e3-3b76-47ec-b432-046db621571b"),
                ),
                (
                    "REDEMPTION_WALLET",
                    Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                ),
                ("ETHEREUM_RPC_URL", Some("https://mainnet.infura.io")),
                ("BASE_RPC_URL", Some("https://base.example.com")),
                (
                    "BASE_ORDERBOOK",
                    Some("0x2222222222222222222222222222222222222222"),
                ),
                (
                    "USDC_VAULT_ID",
                    Some("0x0000000000000000000000000000000000000000000000000000000000000001"),
                ),
                // Explicitly unset to avoid env pollution
                ("EVM_PRIVATE_KEY", None),
            ],
            || {
                let args = vec![
                    "test",
                    "--db",
                    ":memory:",
                    "--ws-rpc-url",
                    "ws://localhost:8545",
                    "--orderbook",
                    "0x1111111111111111111111111111111111111111",
                    "--order-owner",
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "--deployment-block",
                    "1",
                    "--executor",
                    "alpaca-broker-api",
                    "--rebalancing-enabled",
                    "true",
                ];

                let env = Env::try_parse_from(args).unwrap();
                let result = env.into_config();
                assert!(
                    matches!(
                        result,
                        Err(ConfigError::Rebalancing(RebalancingConfigError::Clap(_)))
                    ),
                    "Expected clap error for missing evm_private_key, got {result:?}"
                );
            },
        );
    }

    #[test]
    fn rebalancing_enabled_with_alpaca_and_all_fields_succeeds() {
        temp_env::with_vars(
            [
                ("ALPACA_BROKER_API_KEY", Some("test_key")),
                ("ALPACA_BROKER_API_SECRET", Some("test_secret")),
                (
                    "ALPACA_ACCOUNT_ID",
                    Some("904837e3-3b76-47ec-b432-046db621571b"),
                ),
                (
                    "REDEMPTION_WALLET",
                    Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                ),
                ("ETHEREUM_RPC_URL", Some("https://mainnet.infura.io")),
                (
                    "EVM_PRIVATE_KEY",
                    Some("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
                ),
                ("BASE_RPC_URL", Some("https://base.example.com")),
                (
                    "BASE_ORDERBOOK",
                    Some("0x2222222222222222222222222222222222222222"),
                ),
                (
                    "USDC_VAULT_ID",
                    Some("0x0000000000000000000000000000000000000000000000000000000000000001"),
                ),
            ],
            || {
                let args = vec![
                    "test",
                    "--db",
                    ":memory:",
                    "--ws-rpc-url",
                    "ws://localhost:8545",
                    "--orderbook",
                    "0x1111111111111111111111111111111111111111",
                    "--order-owner",
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "--deployment-block",
                    "1",
                    "--executor",
                    "alpaca-broker-api",
                    "--rebalancing-enabled",
                    "true",
                ];

                let env = Env::try_parse_from(args).unwrap();
                let config = env.into_config().unwrap();
                let rebalancing = config.rebalancing.expect("rebalancing should be Some");

                assert_eq!(
                    rebalancing.redemption_wallet,
                    address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
                );
                assert_eq!(
                    rebalancing.ethereum_rpc_url.as_str(),
                    "https://mainnet.infura.io/"
                );
            },
        );
    }

    #[test]
    fn rebalancing_uses_default_threshold_values() {
        temp_env::with_vars(
            [
                ("ALPACA_BROKER_API_KEY", Some("test_key")),
                ("ALPACA_BROKER_API_SECRET", Some("test_secret")),
                (
                    "ALPACA_ACCOUNT_ID",
                    Some("904837e3-3b76-47ec-b432-046db621571b"),
                ),
                (
                    "REDEMPTION_WALLET",
                    Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                ),
                ("ETHEREUM_RPC_URL", Some("https://mainnet.infura.io")),
                (
                    "EVM_PRIVATE_KEY",
                    Some("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
                ),
                ("BASE_RPC_URL", Some("https://base.example.com")),
                (
                    "BASE_ORDERBOOK",
                    Some("0x2222222222222222222222222222222222222222"),
                ),
                (
                    "USDC_VAULT_ID",
                    Some("0x0000000000000000000000000000000000000000000000000000000000000001"),
                ),
            ],
            || {
                let args = vec![
                    "test",
                    "--db",
                    ":memory:",
                    "--ws-rpc-url",
                    "ws://localhost:8545",
                    "--orderbook",
                    "0x1111111111111111111111111111111111111111",
                    "--order-owner",
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "--deployment-block",
                    "1",
                    "--executor",
                    "alpaca-broker-api",
                    "--rebalancing-enabled",
                    "true",
                ];

                let env = Env::try_parse_from(args).unwrap();
                let config = env.into_config().unwrap();
                let rebalancing = config.rebalancing.expect("rebalancing should be Some");

                assert_eq!(rebalancing.equity_threshold.target, Decimal::new(5, 1));
                assert_eq!(rebalancing.equity_threshold.deviation, Decimal::new(2, 1));
                assert_eq!(rebalancing.usdc_threshold.target, Decimal::new(5, 1));
                assert_eq!(rebalancing.usdc_threshold.deviation, Decimal::new(3, 1));
            },
        );
    }

    #[test]
    fn rebalancing_custom_threshold_values() {
        temp_env::with_vars(
            [
                ("ALPACA_BROKER_API_KEY", Some("test_key")),
                ("ALPACA_BROKER_API_SECRET", Some("test_secret")),
                (
                    "ALPACA_ACCOUNT_ID",
                    Some("904837e3-3b76-47ec-b432-046db621571b"),
                ),
                (
                    "REDEMPTION_WALLET",
                    Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                ),
                ("ETHEREUM_RPC_URL", Some("https://mainnet.infura.io")),
                (
                    "EVM_PRIVATE_KEY",
                    Some("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
                ),
                ("BASE_RPC_URL", Some("https://base.example.com")),
                (
                    "BASE_ORDERBOOK",
                    Some("0x2222222222222222222222222222222222222222"),
                ),
                (
                    "USDC_VAULT_ID",
                    Some("0x0000000000000000000000000000000000000000000000000000000000000001"),
                ),
                ("EQUITY_TARGET_RATIO", Some("0.6")),
                ("EQUITY_DEVIATION", Some("0.15")),
                ("USDC_TARGET_RATIO", Some("0.4")),
                ("USDC_DEVIATION", Some("0.25")),
            ],
            || {
                let args = vec![
                    "test",
                    "--db",
                    ":memory:",
                    "--ws-rpc-url",
                    "ws://localhost:8545",
                    "--orderbook",
                    "0x1111111111111111111111111111111111111111",
                    "--order-owner",
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "--deployment-block",
                    "1",
                    "--executor",
                    "alpaca-broker-api",
                    "--rebalancing-enabled",
                    "true",
                ];

                let env = Env::try_parse_from(args).unwrap();
                let config = env.into_config().unwrap();
                let rebalancing = config.rebalancing.expect("rebalancing should be Some");

                assert_eq!(rebalancing.equity_threshold.target, Decimal::new(6, 1));
                assert_eq!(rebalancing.equity_threshold.deviation, Decimal::new(15, 2));
                assert_eq!(rebalancing.usdc_threshold.target, Decimal::new(4, 1));
                assert_eq!(rebalancing.usdc_threshold.deviation, Decimal::new(25, 2));
            },
        );
    }

    #[test]
    fn schwab_without_order_owner_fails_with_missing_order_owner_error() {
        temp_env::with_vars(
            [
                ("SCHWAB_APP_KEY", Some("test_key")),
                ("SCHWAB_APP_SECRET", Some("test_secret")),
                ("SCHWAB_REDIRECT_URI", Some("https://127.0.0.1")),
                ("SCHWAB_BASE_URL", Some("https://test.com")),
                ("SCHWAB_ACCOUNT_INDEX", Some("0")),
                (
                    "ENCRYPTION_KEY",
                    Some("0000000000000000000000000000000000000000000000000000000000000000"),
                ),
                // Explicitly unset ORDER_OWNER and REBALANCING_ENABLED
                ("ORDER_OWNER", None::<&str>),
                ("REBALANCING_ENABLED", None::<&str>),
            ],
            || {
                let args = vec![
                    "test",
                    "--db",
                    ":memory:",
                    "--ws-rpc-url",
                    "ws://localhost:8545",
                    "--orderbook",
                    "0x1111111111111111111111111111111111111111",
                    // No --order-owner argument
                    "--deployment-block",
                    "1",
                    "--executor",
                    "schwab",
                ];

                let env = Env::try_parse_from(args).unwrap();
                let result = env.into_config();
                assert!(
                    matches!(result, Err(ConfigError::MissingOrderOwner)),
                    "Expected MissingOrderOwner error, got {result:?}"
                );
            },
        );
    }

    #[test]
    fn schwab_with_order_owner_succeeds() {
        temp_env::with_vars(
            [
                ("SCHWAB_APP_KEY", Some("test_key")),
                ("SCHWAB_APP_SECRET", Some("test_secret")),
                ("SCHWAB_REDIRECT_URI", Some("https://127.0.0.1")),
                ("SCHWAB_BASE_URL", Some("https://test.com")),
                ("SCHWAB_ACCOUNT_INDEX", Some("0")),
                (
                    "ENCRYPTION_KEY",
                    Some("0000000000000000000000000000000000000000000000000000000000000000"),
                ),
                ("REBALANCING_ENABLED", None::<&str>),
            ],
            || {
                let args = vec![
                    "test",
                    "--db",
                    ":memory:",
                    "--ws-rpc-url",
                    "ws://localhost:8545",
                    "--orderbook",
                    "0x1111111111111111111111111111111111111111",
                    "--order-owner",
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "--deployment-block",
                    "1",
                    "--executor",
                    "schwab",
                ];

                let env = Env::try_parse_from(args).unwrap();
                let config = env.into_config().unwrap();
                let order_owner = config.order_owner().unwrap();
                assert_eq!(
                    order_owner,
                    address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                );
            },
        );
    }

    #[test]
    fn alpaca_rebalancing_without_order_owner_derives_from_private_key() {
        temp_env::with_vars(
            [
                ("ALPACA_BROKER_API_KEY", Some("test_key")),
                ("ALPACA_BROKER_API_SECRET", Some("test_secret")),
                (
                    "ALPACA_ACCOUNT_ID",
                    Some("904837e3-3b76-47ec-b432-046db621571b"),
                ),
                (
                    "REDEMPTION_WALLET",
                    Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                ),
                ("ETHEREUM_RPC_URL", Some("https://mainnet.infura.io")),
                // Private key whose address is 0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf
                (
                    "EVM_PRIVATE_KEY",
                    Some("0x0000000000000000000000000000000000000000000000000000000000000001"),
                ),
                ("BASE_RPC_URL", Some("https://base.example.com")),
                (
                    "BASE_ORDERBOOK",
                    Some("0x2222222222222222222222222222222222222222"),
                ),
                (
                    "USDC_VAULT_ID",
                    Some("0x0000000000000000000000000000000000000000000000000000000000000001"),
                ),
                // No ORDER_OWNER - should derive from EVM_PRIVATE_KEY
                ("ORDER_OWNER", None::<&str>),
            ],
            || {
                let args = vec![
                    "test",
                    "--db",
                    ":memory:",
                    "--ws-rpc-url",
                    "ws://localhost:8545",
                    "--orderbook",
                    "0x1111111111111111111111111111111111111111",
                    // No --order-owner argument
                    "--deployment-block",
                    "1",
                    "--executor",
                    "alpaca-broker-api",
                    "--rebalancing-enabled",
                    "true",
                ];

                let env = Env::try_parse_from(args).unwrap();
                let config = env.into_config().unwrap();
                let order_owner = config.order_owner().unwrap();
                // Address derived from private key 0x...0001
                assert_eq!(
                    order_owner,
                    address!("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")
                );
            },
        );
    }

    #[test]
    fn alpaca_rebalancing_ignores_order_owner_env_var() {
        temp_env::with_vars(
            [
                ("ALPACA_BROKER_API_KEY", Some("test_key")),
                ("ALPACA_BROKER_API_SECRET", Some("test_secret")),
                (
                    "ALPACA_ACCOUNT_ID",
                    Some("904837e3-3b76-47ec-b432-046db621571b"),
                ),
                (
                    "REDEMPTION_WALLET",
                    Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                ),
                ("ETHEREUM_RPC_URL", Some("https://mainnet.infura.io")),
                (
                    "EVM_PRIVATE_KEY",
                    Some("0x0000000000000000000000000000000000000000000000000000000000000001"),
                ),
                ("BASE_RPC_URL", Some("https://base.example.com")),
                (
                    "BASE_ORDERBOOK",
                    Some("0x2222222222222222222222222222222222222222"),
                ),
                (
                    "USDC_VAULT_ID",
                    Some("0x0000000000000000000000000000000000000000000000000000000000000001"),
                ),
            ],
            || {
                let args = vec![
                    "test",
                    "--db",
                    ":memory:",
                    "--ws-rpc-url",
                    "ws://localhost:8545",
                    "--orderbook",
                    "0x1111111111111111111111111111111111111111",
                    // Set ORDER_OWNER explicitly - should be ignored when rebalancing enabled
                    "--order-owner",
                    "0xcccccccccccccccccccccccccccccccccccccccc",
                    "--deployment-block",
                    "1",
                    "--executor",
                    "alpaca-broker-api",
                    "--rebalancing-enabled",
                    "true",
                ];

                let env = Env::try_parse_from(args).unwrap();
                let config = env.into_config().unwrap();
                let order_owner = config.order_owner().unwrap();
                // Should return derived address, NOT the env var value
                assert_eq!(
                    order_owner,
                    address!("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"),
                    "Expected derived address, not env var. order_owner() should derive from EVM_PRIVATE_KEY when rebalancing is enabled."
                );
            },
        );
    }

    #[test]
    fn alpaca_rebalancing_with_invalid_private_key_fails_with_derivation_error() {
        temp_env::with_vars(
            [
                ("ALPACA_BROKER_API_KEY", Some("test_key")),
                ("ALPACA_BROKER_API_SECRET", Some("test_secret")),
                (
                    "ALPACA_ACCOUNT_ID",
                    Some("904837e3-3b76-47ec-b432-046db621571b"),
                ),
                (
                    "REDEMPTION_WALLET",
                    Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                ),
                ("ETHEREUM_RPC_URL", Some("https://mainnet.infura.io")),
                // Zero private key is invalid for secp256k1
                (
                    "EVM_PRIVATE_KEY",
                    Some("0x0000000000000000000000000000000000000000000000000000000000000000"),
                ),
                ("BASE_RPC_URL", Some("https://base.example.com")),
                (
                    "BASE_ORDERBOOK",
                    Some("0x2222222222222222222222222222222222222222"),
                ),
                (
                    "USDC_VAULT_ID",
                    Some("0x0000000000000000000000000000000000000000000000000000000000000001"),
                ),
                ("ORDER_OWNER", None::<&str>),
            ],
            || {
                let args = vec![
                    "test",
                    "--db",
                    ":memory:",
                    "--ws-rpc-url",
                    "ws://localhost:8545",
                    "--orderbook",
                    "0x1111111111111111111111111111111111111111",
                    "--deployment-block",
                    "1",
                    "--executor",
                    "alpaca-broker-api",
                    "--rebalancing-enabled",
                    "true",
                ];

                let env = Env::try_parse_from(args).unwrap();
                let config = env.into_config().unwrap();
                let result = config.order_owner();
                assert!(
                    matches!(result, Err(ConfigError::PrivateKeyDerivation(_))),
                    "Expected PrivateKeyDerivation error for zero private key, got {result:?}"
                );
            },
        );
    }
}
