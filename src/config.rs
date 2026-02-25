//! Application configuration loading and validation.
//!
//! Reads plaintext config and encrypted secrets from separate TOML files,
//! validates compatibility, and assembles the runtime [`Ctx`] that the rest
//! of the application consumes.

use alloy::primitives::{Address, FixedBytes};
use clap::Parser;
use rust_decimal::Decimal;
use serde::Deserialize;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use std::path::{Path, PathBuf};
use tracing::Level;
use url::Url;

use st0x_execution::{
    AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, AlpacaTradingApiCtx,
    AlpacaTradingApiMode, FractionalShares, Positive, SchwabCtx, SupportedExecutor, TimeInForce,
};

use crate::offchain::order_poller::OrderPollerCtx;
use crate::onchain::{EvmConfig, EvmCtx, EvmSecrets};
use crate::rebalancing::trigger::UsdcRebalancing;
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

/// Configurable caps on operation sizes for safe deployment.
///
/// Required in config -- operators must explicitly choose
/// between conservative limits or uncapped mode.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub(crate) enum OperationalLimits {
    Enabled {
        max_amount: Positive<Usdc>,
        max_shares: Positive<FractionalShares>,
    },
    Disabled,
}

/// Non-secret settings deserialized from the plaintext config TOML.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    database_url: String,
    log_level: Option<LogLevel>,
    server_port: Option<u16>,
    evm: EvmConfig,
    operational_limits: OperationalLimits,
    order_polling_interval: Option<u64>,
    order_polling_max_jitter: Option<u64>,
    #[serde(rename = "hyperdx")]
    telemetry: Option<TelemetryConfig>,
    rebalancing: Option<RebalancingConfig>,
}

/// Secret credentials deserialized from the encrypted secrets TOML.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
#[allow(clippy::large_enum_variant)] // isn't relevant for a brief startup step
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
        account_id: AlpacaAccountId,
        mode: Option<AlpacaBrokerApiMode>,
    },
    DryRun,
}

/// Encodes the two mutually exclusive operating modes at the type level.
///
/// `Standalone`: order_owner comes from config, no rebalancing.
/// `Rebalancing`: order_owner resolved from Fireblocks at runtime.
#[derive(Clone, Debug)]
pub(crate) enum TradingMode {
    Standalone { order_owner: Address },
    Rebalancing(Box<RebalancingCtx>),
}

/// Combined runtime context for the server. Assembled from plaintext config,
/// encrypted secrets, and derived runtime state.
#[derive(Clone)]
pub struct Ctx {
    pub(crate) database_url: String,
    pub log_level: LogLevel,
    pub(crate) server_port: u16,
    pub(crate) operational_limits: OperationalLimits,
    pub(crate) evm: EvmCtx,
    pub(crate) order_polling_interval: u64,
    pub(crate) order_polling_max_jitter: u64,
    pub(crate) broker: BrokerCtx,
    pub telemetry: Option<TelemetryCtx>,
    pub(crate) trading_mode: TradingMode,
    pub(crate) execution_threshold: ExecutionThreshold,
}

/// Runtime broker configuration assembled from `BrokerSecrets`.
#[derive(Clone)]
pub enum BrokerCtx {
    Schwab(SchwabAuth),
    AlpacaTradingApi(AlpacaTradingApiCtx),
    AlpacaBrokerApi(AlpacaBrokerApiCtx),
    DryRun,
}

impl BrokerCtx {
    pub(crate) fn to_supported_executor(&self) -> SupportedExecutor {
        match self {
            Self::Schwab(_) => SupportedExecutor::Schwab,
            Self::AlpacaTradingApi(_) => SupportedExecutor::AlpacaTradingApi,
            Self::AlpacaBrokerApi(_) => SupportedExecutor::AlpacaBrokerApi,
            Self::DryRun => SupportedExecutor::DryRun,
        }
    }

    fn execution_threshold(&self) -> Result<ExecutionThreshold, CtxError> {
        match self {
            Self::Schwab(_) | Self::DryRun => Ok(ExecutionThreshold::shares(
                Positive::<FractionalShares>::ONE,
            )),
            Self::AlpacaTradingApi(_) | Self::AlpacaBrokerApi(_) => {
                Ok(ExecutionThreshold::dollar_value(Usdc(Decimal::TWO))?)
            }
        }
    }
}

impl From<BrokerSecrets> for BrokerCtx {
    fn from(secrets: BrokerSecrets) -> Self {
        match secrets {
            BrokerSecrets::Schwab {
                app_key,
                app_secret,
                redirect_uri,
                base_url,
                account_index,
                encryption_key,
            } => Self::Schwab(SchwabAuth {
                app_key,
                app_secret,
                redirect_uri,
                base_url,
                account_index,
                encryption_key,
            }),

            BrokerSecrets::AlpacaTradingApi {
                api_key,
                api_secret,
                trading_mode,
            } => Self::AlpacaTradingApi(AlpacaTradingApiCtx {
                api_key,
                api_secret,
                trading_mode,
            }),

            BrokerSecrets::AlpacaBrokerApi {
                api_key,
                api_secret,
                account_id,
                mode,
            } => Self::AlpacaBrokerApi(AlpacaBrokerApiCtx {
                api_key,
                api_secret,
                account_id,
                mode,
                asset_cache_ttl: std::time::Duration::from_secs(3600),
                time_in_force: TimeInForce::default(),
            }),

            BrokerSecrets::DryRun => Self::DryRun,
        }
    }
}

/// Schwab auth credentials used by the main crate for token management
/// and executor construction.
#[derive(Clone)]
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

impl std::fmt::Debug for SchwabAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchwabAuth")
            .field("app_key", &"[REDACTED]")
            .field("app_secret", &"[REDACTED]")
            .field("redirect_uri", &self.redirect_uri)
            .field("base_url", &self.base_url)
            .field("account_index", &self.account_index)
            .field("encryption_key", &"[REDACTED]")
            .finish()
    }
}

impl std::fmt::Debug for BrokerCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Schwab(auth) => f.debug_tuple("Schwab").field(auth).finish(),
            Self::AlpacaTradingApi(ctx) => f.debug_tuple("AlpacaTradingApi").field(ctx).finish(),
            Self::AlpacaBrokerApi(ctx) => f.debug_tuple("AlpacaBrokerApi").field(ctx).finish(),
            Self::DryRun => write!(f, "DryRun"),
        }
    }
}

impl std::fmt::Debug for Ctx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ctx")
            .field("database_url", &self.database_url)
            .field("log_level", &self.log_level)
            .field("server_port", &self.server_port)
            .field("operational_limits", &self.operational_limits)
            .field("evm", &self.evm)
            .field("order_polling_interval", &self.order_polling_interval)
            .field("order_polling_max_jitter", &self.order_polling_max_jitter)
            .field("broker", &self.broker)
            .field("telemetry", &self.telemetry)
            .field("trading_mode", &self.trading_mode)
            .field("execution_threshold", &self.execution_threshold)
            .finish()
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

impl Ctx {
    pub async fn load_files(config_path: &Path, secrets_path: &Path) -> Result<Self, CtxError> {
        let config_str = tokio::fs::read_to_string(config_path).await?;
        let secrets_str = tokio::fs::read_to_string(secrets_path).await?;

        let config: Config =
            toml::from_str(&config_str).map_err(|source| CtxError::ConfigToml {
                path: config_path.to_path_buf(),
                source,
            })?;
        let secrets: Secrets =
            toml::from_str(&secrets_str).map_err(|source| CtxError::SecretsToml {
                path: secrets_path.to_path_buf(),
                source,
            })?;

        let broker = BrokerCtx::from(secrets.broker);
        let telemetry = TelemetryCtx::new(config.telemetry, secrets.telemetry)?;

        // Execution threshold is determined by broker capabilities:
        // - Schwab API doesn't support fractional shares, so use 1 whole share threshold
        // - Alpaca requires $1 minimum for fractional trading. We use $2 to provide buffer
        //   for slippage, fees, and price discrepancies that could push fills below $1.
        // - DryRun uses shares threshold for testing
        let execution_threshold = broker.execution_threshold()?;

        let evm = EvmCtx::new(&config.evm, secrets.evm);

        let trading_mode = match (
            config.rebalancing,
            secrets.rebalancing,
            config.evm.order_owner,
        ) {
            (Some(rebalancing_config), Some(rebalancing_secrets), None) => {
                let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &broker else {
                    return Err(RebalancingCtxError::NotAlpacaBroker.into());
                };
                TradingMode::Rebalancing(Box::new(
                    RebalancingCtx::new(
                        rebalancing_config,
                        rebalancing_secrets,
                        alpaca_auth.clone(),
                    )
                    .await?,
                ))
            }
            (Some(_), Some(_), Some(configured)) => {
                return Err(CtxError::OrderOwnerConflictsWithFireblocks { configured });
            }
            (None, None, Some(order_owner)) => TradingMode::Standalone { order_owner },
            (None, None, None) => return Err(CtxError::MissingOrderOwner),
            (Some(_), None, _) => return Err(CtxError::RebalancingSecretsMissing),
            (None, Some(_), _) => return Err(CtxError::RebalancingConfigMissing),
        };

        let log_level = config.log_level.unwrap_or(LogLevel::Debug);

        let usdc = match &trading_mode {
            TradingMode::Rebalancing(ctx) => Some(&ctx.usdc),
            TradingMode::Standalone { .. } => None,
        };
        let usdc_rebalancing_enabled = match usdc {
            Some(UsdcRebalancing::Enabled { .. }) => true,
            Some(UsdcRebalancing::Disabled) | None => false,
        };

        if let OperationalLimits::Enabled { max_amount, .. } = &config.operational_limits
            && usdc_rebalancing_enabled
        {
            let minimum = crate::rebalancing::trigger::ALPACA_MINIMUM_WITHDRAWAL;
            if max_amount.inner() < minimum {
                return Err(CtxError::OperationalLimitBelowMinimumWithdrawal {
                    configured: max_amount.inner(),
                    minimum,
                });
            }
        }

        Ok(Self {
            database_url: config.database_url,
            log_level,
            server_port: config.server_port.unwrap_or(8080),
            operational_limits: config.operational_limits,
            evm,
            order_polling_interval: config.order_polling_interval.unwrap_or(15),
            order_polling_max_jitter: config.order_polling_max_jitter.unwrap_or(5),
            broker,
            telemetry,
            trading_mode,
            execution_threshold,
        })
    }

    pub async fn get_sqlite_pool(&self) -> Result<SqlitePool, sqlx::Error> {
        configure_sqlite_pool(&self.database_url).await
    }

    pub(crate) fn rebalancing_ctx(&self) -> Result<&RebalancingCtx, CtxError> {
        match &self.trading_mode {
            TradingMode::Rebalancing(ctx) => Ok(ctx),
            TradingMode::Standalone { .. } => Err(CtxError::NotRebalancing),
        }
    }

    pub(crate) const fn get_order_poller_ctx(&self) -> OrderPollerCtx {
        OrderPollerCtx {
            polling_interval: std::time::Duration::from_secs(self.order_polling_interval),
            max_jitter: std::time::Duration::from_secs(self.order_polling_max_jitter),
        }
    }

    /// Returns the wallet address that owns orders on the orderbook.
    ///
    /// In `Standalone` mode this is the statically-configured order owner.
    /// In `Rebalancing` mode this is the Fireblocks wallet address resolved
    /// during async construction.
    pub(crate) fn order_owner(&self) -> Address {
        match &self.trading_mode {
            TradingMode::Standalone { order_owner } => *order_owner,
            TradingMode::Rebalancing(ctx) => ctx.base_wallet().address(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CtxError {
    #[error(transparent)]
    Rebalancing(#[from] RebalancingCtxError),
    #[error("ORDER_OWNER required when rebalancing is disabled")]
    MissingOrderOwner,
    #[error("failed to read config file")]
    Io(#[from] std::io::Error),
    #[error("failed to parse config {path}")]
    ConfigToml {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("failed to parse secrets {path}")]
    SecretsToml {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error(transparent)]
    InvalidThreshold(#[from] InvalidThresholdError),
    #[error(transparent)]
    Telemetry(#[from] crate::telemetry::TelemetryAssemblyError),
    #[error("operation requires rebalancing mode")]
    NotRebalancing,
    #[error("rebalancing config present in config but rebalancing secrets missing")]
    RebalancingSecretsMissing,
    #[error("rebalancing secrets present but rebalancing config missing in config")]
    RebalancingConfigMissing,
    #[error(
        "order_owner {configured} must not be set when Fireblocks \
         rebalancing is enabled (address is derived from vault)"
    )]
    OrderOwnerConflictsWithFireblocks { configured: Address },
    #[error(
        "operational_limits max_amount {configured} is below Alpaca's \
         minimum withdrawal of {minimum}"
    )]
    OperationalLimitBelowMinimumWithdrawal { configured: Usdc, minimum: Usdc },
}

#[cfg(test)]
impl CtxError {
    fn kind(&self) -> &'static str {
        match self {
            Self::Rebalancing(_) => "rebalancing configuration error",
            Self::NotRebalancing => "operation requires rebalancing mode",
            Self::MissingOrderOwner => "ORDER_OWNER required when rebalancing is disabled",
            Self::Io(_) => "failed to read config file",
            Self::ConfigToml { .. } => "failed to parse config",
            Self::SecretsToml { .. } => "failed to parse secrets",
            Self::InvalidThreshold(_) => "invalid execution threshold",
            Self::Telemetry(_) => "telemetry assembly error",
            Self::RebalancingSecretsMissing => "rebalancing secrets missing",
            Self::RebalancingConfigMissing => "rebalancing config missing",
            Self::OrderOwnerConflictsWithFireblocks { .. } => {
                "order_owner conflicts with fireblocks"
            }
            Self::OperationalLimitBelowMinimumWithdrawal { .. } => {
                "operational limit below minimum withdrawal"
            }
        }
    }
}

pub(crate) async fn configure_sqlite_pool(database_url: &str) -> Result<SqlitePool, sqlx::Error> {
    // PRAGMAs are set via SqliteConnectOptions so they apply to every
    // connection the pool opens, not just the first one.
    //
    // WAL Mode: Allows concurrent readers but only ONE writer at a time
    // across all processes. When both main bot and reporter try to write
    // simultaneously, one will block until the other completes.
    //
    // Busy Timeout: 10 seconds - when a write is blocked by another
    // process, SQLite will wait up to 10 seconds before failing with
    // "database is locked". Reporter must keep transactions SHORT
    // (single INSERT per trade) to avoid blocking the main bot.
    let options: SqliteConnectOptions = database_url
        .parse::<SqliteConnectOptions>()?
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(std::time::Duration::from_secs(10));

    SqlitePool::connect_with(options).await
}

#[cfg(test)]
pub(crate) mod tests {
    use alloy::primitives::{Address, FixedBytes, address};
    use rust_decimal_macros::dec;
    use std::io::Write;
    use tempfile::NamedTempFile;

    use st0x_execution::{MockExecutor, MockExecutorCtx, TryIntoExecutor};

    use super::*;
    use crate::onchain::EvmCtx;
    use crate::threshold::ExecutionThreshold;

    const TEST_ENCRYPTION_KEY: FixedBytes<32> = FixedBytes::ZERO;

    fn toml_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file
    }

    pub fn create_test_ctx_with_order_owner(order_owner: Address) -> Ctx {
        Ctx {
            database_url: ":memory:".to_owned(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            operational_limits: OperationalLimits::Disabled,
            evm: EvmCtx {
                ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1111111111111111111111111111111111111111"),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            broker: BrokerCtx::Schwab(SchwabAuth {
                app_key: "test_key".to_owned(),
                app_secret: "test_secret".to_owned(),
                redirect_uri: None,
                base_url: Some(url::Url::parse("https://test.com").unwrap()),
                account_index: None,
                encryption_key: TEST_ENCRYPTION_KEY,
            }),
            telemetry: None,
            trading_mode: TradingMode::Standalone { order_owner },
            execution_threshold: ExecutionThreshold::whole_share(),
        }
    }

    fn minimal_config_toml() -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(
            br#"
            database_url = ":memory:"
            [operational_limits]
            mode = "disabled"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
        "#,
        )
        .unwrap();
        file
    }

    fn dry_run_secrets_toml() -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(
            br#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "dry-run"
        "#,
        )
        .unwrap();
        file
    }

    fn schwab_secrets_toml() -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(
            br#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "schwab"
            app_key = "test_key"
            app_secret = "test_secret"
            encryption_key = "0x0000000000000000000000000000000000000000000000000000000000000000"
        "#,
        )
        .unwrap();
        file
    }

    fn minimal_config_toml_without_order_owner() -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(
            br#"
            database_url = ":memory:"
            [operational_limits]
            mode = "disabled"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            deployment_block = 1
        "#,
        )
        .unwrap();
        file
    }

    fn example_config_toml() -> &'static Path {
        Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/example.config.toml"))
    }

    fn example_secrets_toml() -> &'static Path {
        Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/example.secrets.toml"))
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
    async fn test_ctx_sqlite_pool_creation() {
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        ctx.get_sqlite_pool().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_broker_types() {
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let pool = crate::test_utils::setup_test_db().await;

        let BrokerCtx::Schwab(schwab_auth) = &ctx.broker else {
            panic!("Expected Schwab broker ctx");
        };
        let schwab_ctx = schwab_auth.to_schwab_ctx(pool.clone());
        schwab_ctx.try_into_executor().await.unwrap_err();

        // MockExecutorCtx implements TryIntoExecutor, which produces a
        // MockExecutor via the Executor trait's associated Ctx type.
        // The type annotation verifies the correct executor type is
        // produced; .unwrap() verifies construction succeeds.
        let _: MockExecutor = MockExecutorCtx.try_into_executor().await.unwrap();
    }

    #[tokio::test]
    async fn dry_run_broker_does_not_require_any_credentials() {
        let config = minimal_config_toml();
        let secrets = dry_run_secrets_toml();
        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();
        assert!(matches!(ctx.broker, BrokerCtx::DryRun));
    }

    #[tokio::test]
    async fn standalone_mode_when_no_rebalancing() {
        let config = minimal_config_toml();
        let secrets = dry_run_secrets_toml();
        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();
        assert!(matches!(ctx.trading_mode, TradingMode::Standalone { .. }));
    }

    #[tokio::test]
    async fn defaults_applied_when_optional_fields_omitted() {
        let config = minimal_config_toml();
        let secrets = dry_run_secrets_toml();
        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();
        assert!(matches!(ctx.log_level, LogLevel::Debug));
        assert_eq!(ctx.server_port, 8080);
        assert_eq!(ctx.order_polling_interval, 15);
        assert_eq!(ctx.order_polling_max_jitter, 5);
    }

    #[tokio::test]
    async fn operational_limits_enabled_parses_correctly() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            [operational_limits]
            mode = "enabled"
            max_amount = "100"
            max_shares = "2"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
        "#,
        );
        let secrets = dry_run_secrets_toml();

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();
        let OperationalLimits::Enabled {
            max_amount,
            max_shares,
        } = ctx.operational_limits
        else {
            panic!("Expected Enabled, got {:?}", ctx.operational_limits);
        };
        assert_eq!(max_amount.inner(), Usdc(dec!(100)));
        assert_eq!(max_shares.inner(), FractionalShares::new(dec!(2)));
    }

    #[tokio::test]
    async fn operational_limits_missing_fails() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
        "#,
        );
        let secrets = dry_run_secrets_toml();

        let result = Ctx::load_files(config.path(), secrets.path()).await;
        let error = result.unwrap_err();
        assert!(
            matches!(error, CtxError::ConfigToml { .. }),
            "Missing operational_limits should be a config parse error, got {error:?}"
        );
    }

    #[tokio::test]
    async fn standalone_mode_skips_usdc_withdrawal_minimum_check() {
        // max_amount below ALPACA_MINIMUM_WITHDRAWAL is fine in standalone
        // mode because USDC rebalancing transfers don't apply.
        let config = toml_file(
            r#"
            database_url = ":memory:"
            [operational_limits]
            mode = "enabled"
            max_amount = "50"
            max_shares = "2"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
        "#,
        );
        let secrets = dry_run_secrets_toml();

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();
        let OperationalLimits::Enabled { max_amount, .. } = ctx.operational_limits else {
            panic!("Expected Enabled");
        };
        assert_eq!(max_amount.inner(), Usdc(dec!(50)));
    }

    #[tokio::test]
    async fn optional_fields_override_defaults() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            log_level = "warn"
            server_port = 9090
            order_polling_interval = 30
            order_polling_max_jitter = 10
            [operational_limits]
            mode = "disabled"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
        "#,
        );
        let secrets = dry_run_secrets_toml();

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();
        assert!(matches!(ctx.log_level, LogLevel::Warn));
        assert_eq!(ctx.server_port, 9090);
        assert_eq!(ctx.order_polling_interval, 30);
        assert_eq!(ctx.order_polling_max_jitter, 10);
    }

    #[tokio::test]
    async fn rebalancing_with_schwab_fails() {
        let secrets = toml_file(
            r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "schwab"
            app_key = "test_key"
            app_secret = "test_secret"
            encryption_key = "0x0000000000000000000000000000000000000000000000000000000000000000"

            [rebalancing]
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"
            fireblocks_api_user_id = "test-user"
            fireblocks_secret_path = "/tmp/test.key"
        "#,
        );

        let config = toml_file(
            r#"
            database_url = ":memory:"
            [operational_limits]
            mode = "disabled"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            deployment_block = 1
            [rebalancing]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            usdc_vault_id = "0x0000000000000000000000000000000000000000000000000000000000000001"
            fireblocks_vault_account_id = "0"
            fireblocks_environment = "sandbox"

            [rebalancing.fireblocks_chain_asset_ids]
            1 = "ETH"
            8453 = "BASECHAIN_ETH"

            [rebalancing.equities]

            [rebalancing.equity]
            target = "0.5"
            deviation = "0.2"

            [rebalancing.usdc]
            mode = "disabled"
        "#,
        );

        let result = Ctx::load_files(config.path(), secrets.path()).await;
        assert!(
            matches!(
                result,
                Err(CtxError::Rebalancing(RebalancingCtxError::NotAlpacaBroker))
            ),
            "Expected NotAlpacaBroker error for rebalancing with Schwab broker, got {result:?}"
        );
    }

    #[tokio::test]
    async fn schwab_without_order_owner_fails() {
        let config = minimal_config_toml_without_order_owner();
        let secrets = schwab_secrets_toml();
        let result = Ctx::load_files(config.path(), secrets.path()).await;
        assert!(
            matches!(result, Err(CtxError::MissingOrderOwner)),
            "Expected MissingOrderOwner error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn schwab_with_order_owner_succeeds() {
        let config = minimal_config_toml();
        let secrets = schwab_secrets_toml();
        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();
        assert_eq!(
            ctx.order_owner(),
            address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
    }

    #[tokio::test]
    async fn rebalancing_with_missing_secret_file_fails() {
        let error = Ctx::load_files(example_config_toml(), example_secrets_toml())
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                CtxError::Rebalancing(RebalancingCtxError::FireblocksSecretRead(_))
            ),
            "Expected FireblocksSecretRead IO error for non-existent secret file, got {error:?}"
        );
    }

    #[tokio::test]
    async fn rebalancing_with_order_owner_configured_fails() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            [operational_limits]
            mode = "disabled"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xcccccccccccccccccccccccccccccccccccccccc"
            deployment_block = 1
            [hyperdx]
            service_name = "st0x-hedge"
            [rebalancing]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            usdc_vault_id = "0xfedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
            fireblocks_vault_account_id = "0"
            fireblocks_environment = "sandbox"
            [rebalancing.fireblocks_chain_asset_ids]
            1 = "ETH"
            [rebalancing.equities.tEXAMPLE]
            unwrapped = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            wrapped = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            [rebalancing.equity]
            target = "0.5"
            deviation = "0.2"
            [rebalancing.usdc]
            mode = "disabled"
        "#,
        );

        let error = Ctx::load_files(config.path(), example_secrets_toml())
            .await
            .unwrap_err();
        assert!(
            matches!(error, CtxError::OrderOwnerConflictsWithFireblocks { .. }),
            "Expected OrderOwnerConflictsWithFireblocks, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn telemetry_ctx_assembled_from_config_and_secrets() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            [operational_limits]
            mode = "disabled"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
            [hyperdx]
            service_name = "test-service"
        "#,
        );

        let secrets = toml_file(
            r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "dry-run"
            [hyperdx]
            api_key = "test-api-key"
        "#,
        );

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();
        let telemetry = ctx.telemetry.as_ref().expect("telemetry should be Some");
        assert_eq!(telemetry.api_key, "test-api-key");
        assert_eq!(telemetry.service_name, "test-service");
    }

    #[tokio::test]
    async fn telemetry_config_without_secrets_fails() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            [operational_limits]
            mode = "disabled"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
            [hyperdx]
            service_name = "test-service"
        "#,
        );
        let secrets = dry_run_secrets_toml();

        let result = Ctx::load_files(config.path(), secrets.path()).await;
        assert!(
            matches!(
                result,
                Err(CtxError::Telemetry(
                    crate::telemetry::TelemetryAssemblyError::SecretsMissing
                ))
            ),
            "Expected TelemetryAssemblyError::SecretsMissing error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn telemetry_secrets_without_config_fails() {
        let config = minimal_config_toml();
        let secrets = toml_file(
            r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "dry-run"
            [hyperdx]
            api_key = "test-api-key"
        "#,
        );

        let result = Ctx::load_files(config.path(), secrets.path()).await;
        assert!(
            matches!(
                result,
                Err(CtxError::Telemetry(
                    crate::telemetry::TelemetryAssemblyError::ConfigMissing
                ))
            ),
            "Expected TelemetryAssemblyError::ConfigMissing error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn rebalancing_ctx_without_secrets_fails() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            [operational_limits]
            mode = "disabled"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            deployment_block = 1
            [rebalancing]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            usdc_vault_id = "0x0000000000000000000000000000000000000000000000000000000000000001"
            fireblocks_vault_account_id = "0"
            fireblocks_environment = "sandbox"

            [rebalancing.fireblocks_chain_asset_ids]
            1 = "ETH"
            8453 = "BASECHAIN_ETH"

            [rebalancing.equities]

            [rebalancing.equity]
            target = "0.5"
            deviation = "0.2"

            [rebalancing.usdc]
            mode = "disabled"
        "#,
        );

        let secrets = toml_file(
            r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
        "#,
        );

        let result = Ctx::load_files(config.path(), secrets.path()).await;
        assert!(
            matches!(result, Err(CtxError::RebalancingSecretsMissing)),
            "Expected RebalancingSecretsMissing error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn default_execution_threshold_is_one_share_for_dry_run() {
        let config = minimal_config_toml();
        let secrets = dry_run_secrets_toml();
        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();
        assert_eq!(
            ctx.execution_threshold,
            ExecutionThreshold::shares(Positive::<FractionalShares>::ONE)
        );
    }

    #[tokio::test]
    async fn schwab_executor_uses_shares_threshold() {
        let config = minimal_config_toml();
        let secrets = schwab_secrets_toml();
        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();
        assert_eq!(
            ctx.execution_threshold,
            ExecutionThreshold::shares(Positive::<FractionalShares>::ONE)
        );
    }

    #[tokio::test]
    async fn alpaca_trading_api_executor_uses_dollar_threshold() {
        let config = minimal_config_toml();
        let secrets = toml_file(
            r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "alpaca-trading-api"
            api_key = "test-key"
            api_secret = "test-secret"
        "#,
        );

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();
        let expected = ExecutionThreshold::dollar_value(Usdc(Decimal::TWO)).unwrap();
        assert_eq!(ctx.execution_threshold, expected);
    }

    #[tokio::test]
    async fn alpaca_broker_api_executor_uses_dollar_threshold() {
        let config = minimal_config_toml();
        let secrets = toml_file(
            r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
        "#,
        );

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();
        let expected = ExecutionThreshold::dollar_value(Usdc(Decimal::TWO)).unwrap();
        assert_eq!(ctx.execution_threshold, expected);
    }

    #[test]
    fn config_error_kind_rebalancing() {
        let err = CtxError::Rebalancing(RebalancingCtxError::NotAlpacaBroker);
        assert_eq!(err.kind(), "rebalancing configuration error");
    }

    #[test]
    fn config_error_kind_invalid_threshold() {
        let err = CtxError::InvalidThreshold(InvalidThresholdError::ZeroDollarValue);
        assert_eq!(err.kind(), "invalid execution threshold");
    }

    #[tokio::test]
    async fn rebalancing_with_schwab_logs_error_kind() {
        let secrets = toml_file(
            r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "schwab"
            app_key = "test_key"
            app_secret = "test_secret"
            encryption_key = "0x0000000000000000000000000000000000000000000000000000000000000000"
            [rebalancing]
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"
            fireblocks_api_user_id = "test-user"
            fireblocks_secret_path = "/tmp/test.key"
        "#,
        );

        let config = toml_file(
            r#"
            database_url = ":memory:"
            [operational_limits]
            mode = "disabled"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
            [rebalancing]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            usdc_vault_id = "0x0000000000000000000000000000000000000000000000000000000000000001"
            fireblocks_vault_account_id = "0"
            fireblocks_environment = "sandbox"
            [rebalancing.fireblocks_chain_asset_ids]
            1 = "ETH"
            8453 = "BASECHAIN_ETH"
            [rebalancing.equities]
            [rebalancing.equity]
            target = "0.5"
            deviation = "0.2"
            [rebalancing.usdc]
            mode = "enabled"
            target = "0.5"
            deviation = "0.3"
        "#,
        );

        Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn rebalancing_ctx_returns_err_when_standalone() {
        let config = minimal_config_toml();
        let secrets = dry_run_secrets_toml();
        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();
        let error = ctx.rebalancing_ctx().unwrap_err();
        assert!(matches!(error, CtxError::NotRebalancing));
    }

    #[test]
    fn server_config_toml_is_valid() {
        let config_str = include_str!("../config/st0x-hedge.toml");
        toml::from_str::<Config>(config_str).unwrap();
    }

    #[tokio::test]
    async fn unknown_config_fields_rejected() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            bogus_field = "should fail"
            [operational_limits]
            mode = "disabled"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
        "#,
        );
        let secrets = dry_run_secrets_toml();

        let err = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();
        assert!(
            matches!(err, CtxError::ConfigToml { .. }),
            "Expected config parse error for unknown field, got {err:?}"
        );
    }

    #[tokio::test]
    async fn unknown_secrets_fields_rejected() {
        let config = minimal_config_toml();
        let secrets = toml_file(
            r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            extra_secret = "should fail"
            [broker]
            type = "dry-run"
        "#,
        );

        let err = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();
        assert!(
            matches!(err, CtxError::SecretsToml { .. }),
            "Expected secrets parse error for unknown field, got {err:?}"
        );
    }

    #[tokio::test]
    async fn unknown_broker_secrets_fields_rejected() {
        let config = minimal_config_toml();
        let secrets = toml_file(
            r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "alpaca-broker-api"
            api_key = "key"
            api_secret = "secret"
            account_id = "id"
            unknown_field = "should fail"
        "#,
        );

        let err = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();
        assert!(
            matches!(err, CtxError::SecretsToml { .. }),
            "Expected secrets parse error for unknown broker field, got {err:?}"
        );
    }
}
