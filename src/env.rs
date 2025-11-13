use clap::Parser;
use sqlx::SqlitePool;
use tracing::Level;

use crate::offchain::order_poller::OrderPollerConfig;
use crate::onchain::EvmEnv;
use crate::telemetry::HyperDxConfig;
use st0x_broker::SupportedBroker;
use st0x_broker::alpaca::AlpacaAuthEnv;
use st0x_broker::schwab::SchwabAuthEnv;

// Dummy program name required by clap when parsing from environment variables.
// clap's try_parse_from expects argv[0] to be the program name, but we only
// care about environment variables, so this is just a placeholder.
const DUMMY_PROGRAM_NAME: &[&str] = &["server"];

#[derive(Debug, Clone)]
pub enum BrokerConfig {
    Schwab(SchwabAuthEnv),
    Alpaca(AlpacaAuthEnv),
    DryRun,
}

impl BrokerConfig {
    pub fn to_supported_broker(&self) -> SupportedBroker {
        match self {
            Self::Schwab(_) => SupportedBroker::Schwab,
            Self::Alpaca(_) => SupportedBroker::Alpaca,
            Self::DryRun => SupportedBroker::DryRun,
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
    /// Broker to use for trading (required: schwab, alpaca, or dry-run)
    #[clap(long, env)]
    broker: SupportedBroker,
    /// HyperDX API key for observability (optional)
    #[clap(long, env)]
    hyperdx_api_key: Option<String>,
    /// Service name for HyperDX traces (only used when hyperdx_api_key is set)
    #[clap(long, env, default_value = "st0x-hedge")]
    hyperdx_service_name: String,
}

impl Env {
    pub fn into_config(self) -> Result<Config, clap::Error> {
        let broker = match self.broker {
            SupportedBroker::Schwab => {
                let schwab_auth = SchwabAuthEnv::try_parse_from(DUMMY_PROGRAM_NAME)?;
                BrokerConfig::Schwab(schwab_auth)
            }
            SupportedBroker::Alpaca => {
                let alpaca_auth = AlpacaAuthEnv::try_parse_from(DUMMY_PROGRAM_NAME)?;
                BrokerConfig::Alpaca(alpaca_auth)
            }
            SupportedBroker::DryRun => BrokerConfig::DryRun,
        };

        let log_level_tracing: Level = (&self.log_level).into();
        let hyperdx = self.hyperdx_api_key.map(|api_key| HyperDxConfig {
            api_key,
            service_name: self.hyperdx_service_name,
            log_level: log_level_tracing,
        });

        Ok(Config {
            database_url: self.database_url,
            log_level: self.log_level,
            server_port: self.server_port,
            evm: self.evm,
            order_polling_interval: self.order_polling_interval,
            order_polling_max_jitter: self.order_polling_max_jitter,
            broker,
            hyperdx,
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
}

pub fn setup_tracing(log_level: &LogLevel) {
    let level: Level = log_level.into();
    let default_filter = format!("st0x_hedge={level},st0x_broker={level}");

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
    use alloy::primitives::{FixedBytes, address};
    use st0x_broker::schwab::{SchwabAuthEnv, SchwabConfig};
    use st0x_broker::{MockBrokerConfig, TryIntoBroker};

    const TEST_ENCRYPTION_KEY: FixedBytes<32> = FixedBytes::ZERO;

    pub fn create_test_config_with_order_owner(order_owner: alloy::primitives::Address) -> Config {
        Config {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            evm: EvmEnv {
                ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1111111111111111111111111111111111111111"),
                order_owner,
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            broker: BrokerConfig::Schwab(SchwabAuthEnv {
                schwab_app_key: "test_key".to_string(),
                schwab_app_secret: "test_secret".to_string(),
                schwab_redirect_uri: "https://127.0.0.1".to_string(),
                schwab_base_url: "https://test.com".to_string(),
                schwab_account_index: 0,
                encryption_key: TEST_ENCRYPTION_KEY,
            }),
            hyperdx: None,
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
        let schwab_result = schwab_config.try_into_broker().await;
        assert!(schwab_result.is_err());

        // MockBroker should always work
        let test_broker = MockBrokerConfig.try_into_broker().await.unwrap();
        assert!(format!("{test_broker:?}").contains("MockBroker"));
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
            "--broker",
            "dry-run",
        ];

        let env = Env::try_parse_from(args).unwrap();
        let config = env.into_config().unwrap();
        assert!(matches!(config.broker, BrokerConfig::DryRun));
    }
}
