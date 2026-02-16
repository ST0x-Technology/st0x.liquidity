//! Application configuration loading and validation.
//!
//! Reads plaintext config and encrypted secrets from separate TOML files,
//! validates compatibility, and assembles the runtime [`Ctx`] that the rest
//! of the application consumes.

use alloy::primitives::{Address, FixedBytes};
use alloy::signers::local::PrivateKeySigner;
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
#[serde(deny_unknown_fields)]
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

/// Combined runtime context for the server. Assembled from plaintext config,
/// encrypted secrets, and derived runtime state.
#[derive(Clone)]
pub struct Ctx {
    pub(crate) database_url: String,
    pub log_level: LogLevel,
    pub(crate) server_port: u16,
    pub(crate) evm: EvmCtx,
    pub(crate) order_polling_interval: u64,
    pub(crate) order_polling_max_jitter: u64,
    pub(crate) broker: BrokerCtx,
    pub telemetry: Option<TelemetryCtx>,
    pub(crate) rebalancing: Option<RebalancingCtx>,
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
            .field("evm", &self.evm)
            .field("order_polling_interval", &self.order_polling_interval)
            .field("order_polling_max_jitter", &self.order_polling_max_jitter)
            .field("broker", &self.broker)
            .field("telemetry", &self.telemetry)
            .field("rebalancing", &self.rebalancing.is_some())
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
    pub fn load_files(config: &Path, secrets: &Path) -> Result<Self, CtxError> {
        let config_str = std::fs::read_to_string(config)?;
        let secrets_str = std::fs::read_to_string(secrets)?;
        Self::from_toml(&config_str, &secrets_str)
    }

    pub fn from_toml(config_toml: &str, secrets_toml: &str) -> Result<Self, CtxError> {
        let config: Config = toml::from_str(config_toml)?;
        let secrets: Secrets = toml::from_str(secrets_toml)?;

        let broker = BrokerCtx::from(secrets.broker);
        let evm = EvmCtx::new(&config.evm, secrets.evm);
        let telemetry = TelemetryCtx::new(config.telemetry, secrets.telemetry)?;

        // Execution threshold is determined by broker capabilities:
        // - Schwab API doesn't support fractional shares, so use 1 whole share threshold
        // - Alpaca requires $1 minimum for fractional trading. We use $2 to provide buffer
        //   for slippage, fees, and price discrepancies that could push fills below $1.
        // - DryRun uses shares threshold for testing
        let execution_threshold = broker.execution_threshold()?;

        // Rebalancing requires both config and secrets, plus an AlpacaBrokerApi broker.
        let rebalancing = match (config.rebalancing, secrets.rebalancing) {
            (Some(rebalancing_config), Some(rebalancing_secrets)) => {
                let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &broker else {
                    return Err(RebalancingCtxError::NotAlpacaBroker.into());
                };
                Some(RebalancingCtx::new(
                    rebalancing_config,
                    rebalancing_secrets,
                    alpaca_auth.clone(),
                )?)
            }
            (None, None) => None,
            (Some(_), None) => return Err(CtxError::RebalancingSecretsMissing),
            (None, Some(_)) => return Err(CtxError::RebalancingConfigMissing),
        };

        let log_level = config.log_level.unwrap_or(LogLevel::Debug);

        if let (Some(rebalancing_ctx), Some(configured_owner)) = (&rebalancing, evm.order_owner) {
            let derived = PrivateKeySigner::from_bytes(&rebalancing_ctx.evm_private_key)?.address();
            if derived != configured_owner {
                return Err(CtxError::OrderOwnerMismatch {
                    configured: configured_owner,
                    derived,
                });
            }
        }

        if rebalancing.is_none() && evm.order_owner.is_none() {
            return Err(CtxError::MissingOrderOwner);
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

    pub(crate) fn rebalancing_ctx(&self) -> Option<&RebalancingCtx> {
        self.rebalancing.as_ref()
    }

    pub(crate) const fn get_order_poller_ctx(&self) -> OrderPollerCtx {
        OrderPollerCtx {
            polling_interval: std::time::Duration::from_secs(self.order_polling_interval),
            max_jitter: std::time::Duration::from_secs(self.order_polling_max_jitter),
        }
    }

    pub(crate) fn order_owner(&self) -> Result<Address, CtxError> {
        match (&self.rebalancing, self.evm.order_owner) {
            (Some(r), _) => {
                let signer = PrivateKeySigner::from_bytes(&r.evm_private_key)?;
                Ok(signer.address())
            }
            (None, Some(addr)) => Ok(addr),
            (None, None) => Err(CtxError::MissingOrderOwner),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CtxError {
    #[error(transparent)]
    Rebalancing(#[from] RebalancingCtxError),
    #[error("ORDER_OWNER required when rebalancing is disabled")]
    MissingOrderOwner,
    #[error("failed to derive address from EVM_PRIVATE_KEY")]
    PrivateKeyDerivation(#[from] alloy::signers::k256::ecdsa::Error),
    #[error("failed to read config file")]
    Io(#[from] std::io::Error),
    #[error("failed to parse TOML")]
    Toml(#[from] toml::de::Error),
    #[error(transparent)]
    InvalidThreshold(#[from] InvalidThresholdError),
    #[error(transparent)]
    Telemetry(#[from] crate::telemetry::TelemetryAssemblyError),
    #[error("rebalancing config present in config but rebalancing secrets missing")]
    RebalancingSecretsMissing,
    #[error("rebalancing secrets present but rebalancing config missing in config")]
    RebalancingConfigMissing,
    #[error(
        "order_owner {configured} does not match market maker wallet \
         {derived} derived from evm_private_key"
    )]
    OrderOwnerMismatch {
        configured: Address,
        derived: Address,
    },
}

#[cfg(test)]
impl CtxError {
    fn kind(&self) -> &'static str {
        match self {
            Self::Rebalancing(_) => "rebalancing configuration error",
            Self::MissingOrderOwner => "ORDER_OWNER required when rebalancing is disabled",
            Self::PrivateKeyDerivation(_) => "failed to derive address from EVM_PRIVATE_KEY",
            Self::Io(_) => "failed to read config file",
            Self::Toml(_) => "failed to parse TOML",
            Self::InvalidThreshold(_) => "invalid execution threshold",
            Self::Telemetry(_) => "telemetry assembly error",
            Self::RebalancingSecretsMissing => "rebalancing secrets missing",
            Self::RebalancingConfigMissing => "rebalancing config missing",
            Self::OrderOwnerMismatch { .. } => "order_owner mismatch",
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
    use tracing_test::traced_test;

    use st0x_execution::{MockExecutor, MockExecutorCtx, TryIntoExecutor};

    use super::*;
    use crate::onchain::EvmCtx;
    use crate::threshold::ExecutionThreshold;

    const TEST_ENCRYPTION_KEY: FixedBytes<32> = FixedBytes::ZERO;

    pub fn create_test_ctx_with_order_owner(order_owner: Address) -> Ctx {
        Ctx {
            database_url: ":memory:".to_owned(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            evm: EvmCtx {
                ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1111111111111111111111111111111111111111"),
                order_owner: Some(order_owner),
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
            rebalancing: None,
            execution_threshold: ExecutionThreshold::whole_share(),
        }
    }

    fn minimal_config_toml() -> &'static str {
        r#"
            database_url = ":memory:"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
        "#
    }

    fn dry_run_secrets_toml() -> &'static str {
        r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "dry-run"
        "#
    }

    fn schwab_secrets_toml() -> &'static str {
        r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "schwab"
            app_key = "test_key"
            app_secret = "test_secret"
            encryption_key = "0x0000000000000000000000000000000000000000000000000000000000000000"
        "#
    }

    fn minimal_config_toml_without_order_owner() -> &'static str {
        r#"
            database_url = ":memory:"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            deployment_block = 1
        "#
    }

    fn example_config_toml() -> &'static str {
        include_str!("../example.config.toml")
    }

    fn example_secrets_toml() -> &'static str {
        include_str!("../example.secrets.toml")
    }

    fn example_secrets_with_private_key(evm_private_key: &str) -> String {
        example_secrets_toml().replacen(
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

    #[test]
    fn dry_run_broker_does_not_require_any_credentials() {
        let ctx = Ctx::from_toml(minimal_config_toml(), dry_run_secrets_toml()).unwrap();
        assert!(matches!(ctx.broker, BrokerCtx::DryRun));
    }

    #[test]
    fn rebalancing_absent_means_none() {
        let ctx = Ctx::from_toml(minimal_config_toml(), dry_run_secrets_toml()).unwrap();
        assert!(ctx.rebalancing.is_none());
    }

    #[test]
    fn defaults_applied_when_optional_fields_omitted() {
        let ctx = Ctx::from_toml(minimal_config_toml(), dry_run_secrets_toml()).unwrap();
        assert!(matches!(ctx.log_level, LogLevel::Debug));
        assert_eq!(ctx.server_port, 8080);
        assert_eq!(ctx.order_polling_interval, 15);
        assert_eq!(ctx.order_polling_max_jitter, 5);
    }

    #[test]
    fn optional_fields_override_defaults() {
        let config = r#"
            database_url = ":memory:"
            log_level = "warn"
            server_port = 9090
            order_polling_interval = 30
            order_polling_max_jitter = 10
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
        "#;

        let ctx = Ctx::from_toml(config, dry_run_secrets_toml()).unwrap();
        assert!(matches!(ctx.log_level, LogLevel::Warn));
        assert_eq!(ctx.server_port, 9090);
        assert_eq!(ctx.order_polling_interval, 30);
        assert_eq!(ctx.order_polling_max_jitter, 10);
    }

    #[test]
    fn rebalancing_with_schwab_fails() {
        let secrets = r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "schwab"
            app_key = "test_key"
            app_secret = "test_secret"
            encryption_key = "0x0000000000000000000000000000000000000000000000000000000000000000"

            [rebalancing]
            ethereum_rpc_url = "https://mainnet.infura.io"
            evm_private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#;

        let config = r#"
            database_url = ":memory:"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            deployment_block = 1
            [rebalancing]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            usdc_vault_id = "0x0000000000000000000000000000000000000000000000000000000000000001"

            [rebalancing.equities]

            [rebalancing.equity]
            target = "0.5"
            deviation = "0.2"

            [rebalancing.usdc]
            mode = "disabled"
        "#;

        let result = Ctx::from_toml(config, secrets);
        assert!(
            matches!(
                result,
                Err(CtxError::Rebalancing(RebalancingCtxError::NotAlpacaBroker))
            ),
            "Expected NotAlpacaBroker error for rebalancing with Schwab broker, got {result:?}"
        );
    }

    #[test]
    fn schwab_without_order_owner_fails() {
        let result = Ctx::from_toml(
            minimal_config_toml_without_order_owner(),
            schwab_secrets_toml(),
        );
        assert!(
            matches!(result, Err(CtxError::MissingOrderOwner)),
            "Expected MissingOrderOwner error, got {result:?}"
        );
    }

    #[test]
    fn schwab_with_order_owner_succeeds() {
        let ctx = Ctx::from_toml(minimal_config_toml(), schwab_secrets_toml()).unwrap();
        let order_owner = ctx.order_owner().unwrap();
        assert_eq!(
            order_owner,
            address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
    }

    #[test]
    fn rebalancing_derives_order_owner_from_private_key() {
        let ctx = Ctx::from_toml(example_config_toml(), example_secrets_toml()).unwrap();

        let order_owner = ctx.order_owner().unwrap();
        assert_eq!(
            order_owner,
            address!("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf")
        );
    }

    #[test]
    fn rebalancing_with_mismatched_order_owner_fails() {
        let config = example_config_toml().replace(
            "deployment_block = 1",
            "order_owner = \"0xcccccccccccccccccccccccccccccccccccccccc\"\ndeployment_block = 1",
        );

        let error = Ctx::from_toml(&config, example_secrets_toml()).unwrap_err();
        assert!(
            matches!(error, CtxError::OrderOwnerMismatch { .. }),
            "Expected OrderOwnerMismatch, got: {error:?}"
        );
    }

    #[test]
    fn rebalancing_with_matching_order_owner_succeeds() {
        // Private key 0x01 derives to 0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf
        let config = example_config_toml().replace(
            "deployment_block = 1",
            "order_owner = \"0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf\"\ndeployment_block = 1",
        );

        let ctx = Ctx::from_toml(&config, example_secrets_toml()).unwrap();
        let order_owner = ctx.order_owner().unwrap();
        assert_eq!(
            order_owner,
            address!("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"),
        );
    }

    #[test]
    fn rebalancing_with_invalid_private_key_fails() {
        let secrets = example_secrets_with_private_key(
            "0x0000000000000000000000000000000000000000000000000000000000000000",
        );

        let error = Ctx::from_toml(example_config_toml(), &secrets).unwrap_err();
        assert!(
            matches!(error, CtxError::Rebalancing(_)),
            "Expected Rebalancing error for zero private key, got {error:?}"
        );
    }

    #[test]
    fn telemetry_ctx_assembled_from_config_and_secrets() {
        let config = r#"
            database_url = ":memory:"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
            [hyperdx]
            service_name = "test-service"
        "#;

        let secrets = r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "dry-run"
            [hyperdx]
            api_key = "test-api-key"
        "#;

        let ctx = Ctx::from_toml(config, secrets).unwrap();
        let telemetry = ctx.telemetry.as_ref().expect("telemetry should be Some");
        assert_eq!(telemetry.api_key, "test-api-key");
        assert_eq!(telemetry.service_name, "test-service");
    }

    #[test]
    fn telemetry_config_without_secrets_fails() {
        let config = r#"
            database_url = ":memory:"

            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
            [hyperdx]
            service_name = "test-service"
        "#;

        let result = Ctx::from_toml(config, dry_run_secrets_toml());
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

    #[test]
    fn telemetry_secrets_without_config_fails() {
        let secrets = r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "dry-run"
            [hyperdx]
            api_key = "test-api-key"
        "#;

        let result = Ctx::from_toml(minimal_config_toml(), secrets);
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

    #[test]
    fn rebalancing_ctx_without_secrets_fails() {
        let config = r#"
            database_url = ":memory:"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            deployment_block = 1
            [rebalancing]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            usdc_vault_id = "0x0000000000000000000000000000000000000000000000000000000000000001"

            [rebalancing.equities]

            [rebalancing.equity]
            target = "0.5"
            deviation = "0.2"

            [rebalancing.usdc]
            mode = "disabled"
        "#;

        let secrets = r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
        "#;

        let result = Ctx::from_toml(config, secrets);
        assert!(
            matches!(result, Err(CtxError::RebalancingSecretsMissing)),
            "Expected RebalancingSecretsMissing error, got {result:?}"
        );
    }

    #[test]
    fn default_execution_threshold_is_one_share_for_dry_run() {
        let ctx = Ctx::from_toml(minimal_config_toml(), dry_run_secrets_toml()).unwrap();
        assert_eq!(
            ctx.execution_threshold,
            ExecutionThreshold::shares(Positive::<FractionalShares>::ONE)
        );
    }

    #[test]
    fn schwab_executor_uses_shares_threshold() {
        let ctx = Ctx::from_toml(minimal_config_toml(), schwab_secrets_toml()).unwrap();
        assert_eq!(
            ctx.execution_threshold,
            ExecutionThreshold::shares(Positive::<FractionalShares>::ONE)
        );
    }

    #[test]
    fn alpaca_trading_api_executor_uses_dollar_threshold() {
        let secrets = r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "alpaca-trading-api"
            api_key = "test-key"
            api_secret = "test-secret"
        "#;

        let ctx = Ctx::from_toml(minimal_config_toml(), secrets).unwrap();
        let expected = ExecutionThreshold::dollar_value(Usdc(Decimal::TWO)).unwrap();
        assert_eq!(ctx.execution_threshold, expected);
    }

    #[test]
    fn alpaca_broker_api_executor_uses_dollar_threshold() {
        let secrets = r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
        "#;

        let ctx = Ctx::from_toml(minimal_config_toml(), secrets).unwrap();
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

    #[traced_test]
    #[test]
    fn rebalancing_with_schwab_logs_error_kind() {
        let secrets = r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "schwab"
            app_key = "test_key"
            app_secret = "test_secret"
            encryption_key = "0x0000000000000000000000000000000000000000000000000000000000000000"
            [rebalancing]
            ethereum_rpc_url = "https://mainnet.infura.io"
            evm_private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#;

        let config = r#"
            database_url = ":memory:"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
            [rebalancing]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            usdc_vault_id = "0x0000000000000000000000000000000000000000000000000000000000000001"
            [rebalancing.equities]
            [rebalancing.equity]
            target = "0.5"
            deviation = "0.2"
            [rebalancing.usdc]
            mode = "enabled"
            target = "0.5"
            deviation = "0.3"
        "#;

        Ctx::from_toml(config, secrets).unwrap_err();
    }

    #[test]
    fn rebalancing_ctx_returns_some_when_present() {
        let ctx = Ctx::from_toml(example_config_toml(), example_secrets_toml()).unwrap();
        assert!(
            ctx.rebalancing_ctx().is_some(),
            "rebalancing_ctx() should return Some when rebalancing is configured"
        );
    }

    #[test]
    fn rebalancing_ctx_returns_none_when_absent() {
        let ctx = Ctx::from_toml(minimal_config_toml(), dry_run_secrets_toml()).unwrap();
        assert!(
            ctx.rebalancing_ctx().is_none(),
            "rebalancing_ctx() should return None when rebalancing is not configured"
        );
    }

    #[test]
    fn example_files_load_successfully() {
        let ctx = Ctx::from_toml(example_config_toml(), example_secrets_toml()).unwrap();
        assert!(matches!(ctx.broker, BrokerCtx::AlpacaBrokerApi(_)));
        assert!(ctx.rebalancing.is_some());
        assert!(ctx.telemetry.is_some());
    }

    #[test]
    fn server_config_toml_is_valid() {
        let config_str = include_str!("../config/server.toml");
        toml::from_str::<Config>(config_str).unwrap();
    }

    #[test]
    fn unknown_config_fields_rejected() {
        let config = r#"
            database_url = ":memory:"
            bogus_field = "should fail"
            [evm]
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
        "#;

        let err = Ctx::from_toml(config, dry_run_secrets_toml()).unwrap_err();
        assert!(
            matches!(err, CtxError::Toml(_)),
            "Expected TOML parse error for unknown field, got {err:?}"
        );
    }

    #[test]
    fn unknown_secrets_fields_rejected() {
        let secrets = r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            extra_secret = "should fail"
            [broker]
            type = "dry-run"
        "#;

        let err = Ctx::from_toml(minimal_config_toml(), secrets).unwrap_err();
        assert!(
            matches!(err, CtxError::Toml(_)),
            "Expected TOML parse error for unknown field, got {err:?}"
        );
    }

    #[test]
    fn unknown_broker_secrets_fields_rejected() {
        let secrets = r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"
            [broker]
            type = "alpaca-broker-api"
            api_key = "key"
            api_secret = "secret"
            account_id = "id"
            unknown_field = "should fail"
        "#;

        let err = Ctx::from_toml(minimal_config_toml(), secrets).unwrap_err();
        assert!(
            matches!(err, CtxError::Toml(_)),
            "Expected TOML parse error for unknown broker field, got {err:?}"
        );
    }

    #[test]
    fn rebalancing_fails_when_market_maker_wallet_equals_redemption_wallet() {
        // Private key 0x01 derives to 0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf
        let config = example_config_toml().replacen(
            "redemption_wallet = \"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\"",
            "redemption_wallet = \"0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf\"",
            1,
        );

        let error = Ctx::from_toml(&config, example_secrets_toml()).unwrap_err();

        assert!(
            matches!(error, CtxError::Rebalancing(_)),
            "Expected Rebalancing error for matching wallets, got {error:?}"
        );
        assert!(
            error.to_string().contains("must be different addresses"),
            "Expected error about different addresses, got: {error}"
        );
    }
}
