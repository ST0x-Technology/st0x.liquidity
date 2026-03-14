//! Application configuration loading and validation.
//!
//! Reads plaintext config and encrypted secrets from separate TOML files,
//! validates compatibility, and assembles the runtime [`Ctx`] that the rest
//! of the application consumes.

use alloy::primitives::{Address, B256, FixedBytes};
use clap::Parser;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::Level;
use url::Url;

use st0x_execution::{
    AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, AlpacaTradingApiCtx,
    AlpacaTradingApiMode, FractionalShares, Positive, SchwabCtx, SupportedExecutor, Symbol,
    TimeInForce,
};
use st0x_finance::Usdc;

use crate::offchain::order_poller::OrderPollerCtx;
use crate::onchain::{EvmConfig, EvmCtx, EvmSecrets};
use crate::rebalancing::{
    RebalancingConfig, RebalancingCtx, RebalancingCtxError, RebalancingSecrets,
};
use crate::telemetry::{TelemetryConfig, TelemetryCtx, TelemetrySecrets};
use crate::threshold::{ExecutionThreshold, InvalidThresholdError};

#[derive(Parser, Debug)]
pub struct Env {
    /// Path to plaintext TOML configuration file
    #[clap(long)]
    pub config: PathBuf,
    /// Path to encrypted TOML secrets file
    #[clap(long)]
    pub secrets: PathBuf,
}

/// Whether a per-asset operation (trading or rebalancing) is active.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OperationMode {
    Enabled,
    Disabled,
}

/// Per-equity asset configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EquityAssetConfig {
    pub tokenized_equity: Address,
    pub tokenized_equity_derivative: Address,
    #[serde(default, deserialize_with = "deserialize_padded_b256")]
    pub vault_id: Option<B256>,
    pub trading: OperationMode,
    pub rebalancing: OperationMode,
    pub operational_limit: Option<Positive<FractionalShares>>,
}

/// Cash asset (USDC) configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CashAssetConfig {
    #[serde(default, deserialize_with = "deserialize_padded_b256")]
    pub vault_id: Option<B256>,
    pub rebalancing: OperationMode,
    pub operational_limit: Option<Positive<Usdc>>,
}

/// Equity assets configuration with an optional global operational limit.
///
/// Uses `#[serde(flatten)]` so that per-symbol tables live alongside the
/// `operational_limit` key under `[assets.equities]` in the TOML.
/// `deny_unknown_fields` is intentionally absent because it is
/// incompatible with `flatten`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct EquitiesConfig {
    pub operational_limit: Option<Positive<FractionalShares>>,
    #[serde(flatten)]
    pub symbols: HashMap<Symbol, EquityAssetConfig>,
}

/// Top-level assets configuration containing equities and cash.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssetsConfig {
    pub equities: EquitiesConfig,
    pub cash: Option<CashAssetConfig>,
}

/// Deserializes a hex string (possibly short, e.g. `"0xfab"`) into a
/// left-padded `B256`. Missing values deserialize as `None`.
fn deserialize_padded_b256<'de, D>(deserializer: D) -> Result<Option<B256>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Some(hex_str) = Option::<String>::deserialize(deserializer)? else {
        return Ok(None);
    };

    let stripped = hex_str
        .strip_prefix("0x")
        .or_else(|| hex_str.strip_prefix("0X"))
        .unwrap_or(&hex_str);

    if stripped.len() > 64 {
        return Err(serde::de::Error::custom(format!(
            "hex string too long for B256: {stripped}"
        )));
    }

    if stripped.is_empty() {
        return Err(serde::de::Error::custom("empty hex string for B256"));
    }

    let padded = format!("{stripped:0>64}");
    padded
        .parse::<B256>()
        .map(Some)
        .map_err(serde::de::Error::custom)
}
/// Non-secret settings deserialized from the plaintext config TOML.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    database_url: String,
    log_level: Option<LogLevel>,
    server_port: Option<u16>,
    raindex: EvmConfig,
    order_polling_interval: Option<u64>,
    order_polling_max_jitter: Option<u64>,
    position_check_interval: Option<u64>,
    inventory_poll_interval: Option<u64>,
    #[serde(rename = "hyperdx")]
    telemetry: Option<TelemetryConfig>,
    rebalancing: Option<RebalancingConfig>,
    assets: AssetsConfig,
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
/// `Rebalancing`: order_owner resolved from the wallet at runtime.
#[derive(Clone, Debug)]
pub enum TradingMode {
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
    pub(crate) evm: EvmCtx,
    pub(crate) order_polling_interval: u64,
    pub(crate) order_polling_max_jitter: u64,
    pub(crate) position_check_interval: u64,
    pub(crate) inventory_poll_interval: u64,
    pub(crate) broker: BrokerCtx,
    pub telemetry: Option<TelemetryCtx>,
    pub trading_mode: TradingMode,
    pub execution_threshold: ExecutionThreshold,
    pub(crate) assets: AssetsConfig,
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
                Ok(ExecutionThreshold::dollar_value(Usdc::new(Decimal::TWO))?)
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
            .field("position_check_interval", &self.position_check_interval)
            .field("inventory_poll_interval", &self.inventory_poll_interval)
            .field("broker", &self.broker)
            .field("telemetry", &self.telemetry)
            .field("trading_mode", &self.trading_mode)
            .field("execution_threshold", &self.execution_threshold)
            .field("assets", &self.assets)
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
        let config_str = tokio::fs::read_to_string(config_path)
            .await
            .map_err(|source| CtxError::ConfigIo {
                path: config_path.to_path_buf(),
                source,
            })?;
        let secrets_str = tokio::fs::read_to_string(secrets_path)
            .await
            .map_err(|source| CtxError::SecretsIo {
                path: secrets_path.to_path_buf(),
                source,
            })?;

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

        let evm = EvmCtx::new(&config.raindex, secrets.evm);

        let trading_mode = match (
            config.rebalancing,
            secrets.rebalancing,
            config.raindex.order_owner,
        ) {
            (Some(rebalancing_config), Some(rebalancing_secrets), None) => {
                let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &broker else {
                    return Err(RebalancingCtxError::NotAlpacaBroker.into());
                };

                let minimum = crate::rebalancing::trigger::ALPACA_MINIMUM_WITHDRAWAL;

                if let Some(cash) = &config.assets.cash
                    && cash.rebalancing == OperationMode::Enabled
                    && let Some(cash_limit) = &cash.operational_limit
                    && cash_limit.inner() < minimum
                {
                    return Err(CtxError::CashOperationalLimitBelowMinimumWithdrawal {
                        configured: cash_limit.inner(),
                        minimum,
                    });
                }

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
                return Err(CtxError::OrderOwnerConflictsWithRebalancing { configured });
            }
            (None, None, Some(order_owner)) => TradingMode::Standalone { order_owner },
            (None, None, None) => return Err(CtxError::MissingOrderOwner),
            (Some(_), None, _) => return Err(CtxError::RebalancingSecretsMissing),
            (None, Some(_), _) => return Err(CtxError::RebalancingConfigMissing),
        };

        let log_level = config.log_level.unwrap_or(LogLevel::Debug);

        let position_check_interval = config.position_check_interval.unwrap_or(60);
        if position_check_interval == 0 {
            return Err(CtxError::ZeroPollingInterval {
                field: "position_check_interval",
            });
        }

        let inventory_poll_interval = config.inventory_poll_interval.unwrap_or(60);
        if inventory_poll_interval == 0 {
            return Err(CtxError::ZeroPollingInterval {
                field: "inventory_poll_interval",
            });
        }

        Ok(Self {
            database_url: config.database_url,
            log_level,
            server_port: config.server_port.unwrap_or(8080),
            evm,
            order_polling_interval: config.order_polling_interval.unwrap_or(15),
            order_polling_max_jitter: config.order_polling_max_jitter.unwrap_or(5),
            position_check_interval,
            inventory_poll_interval,
            broker,
            telemetry,
            trading_mode,
            execution_threshold,
            assets: config.assets,
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
    /// In `Rebalancing` mode this is the wallet address resolved during
    /// async construction.
    pub(crate) fn order_owner(&self) -> Address {
        match &self.trading_mode {
            TradingMode::Standalone { order_owner } => *order_owner,
            TradingMode::Rebalancing(ctx) => ctx.base_wallet().address(),
        }
    }

    /// Returns whether trading is enabled for the given equity.
    ///
    /// Fail-closed: assets not present in the config are treated as
    /// trading-disabled.
    pub(crate) fn is_trading_enabled(&self, symbol: &Symbol) -> bool {
        self.assets
            .equities
            .symbols
            .get(symbol)
            .is_some_and(|config| config.trading == OperationMode::Enabled)
    }

    /// Returns whether rebalancing is enabled for the given equity.
    ///
    /// Assets not present in the config are treated as rebalancing-disabled
    /// by default.
    pub(crate) fn is_rebalancing_enabled(&self, symbol: &Symbol) -> bool {
        self.assets
            .equities
            .symbols
            .get(symbol)
            .is_some_and(|config| config.rebalancing == OperationMode::Enabled)
    }
}

/// Test-only constructor for `Ctx` that internalizes fields e2e tests
/// don't need to control (log level, operational limits, EVM wrapping,
/// polling intervals). This keeps `Ctx` fields `pub(crate)` while
/// providing a stable construction API for the e2e test crate.
#[cfg(any(test, feature = "test-support"))]
#[bon::bon]
impl Ctx {
    #[builder]
    pub fn for_test(
        database_url: String,
        ws_rpc_url: Url,
        orderbook: Address,
        deployment_block: u64,
        broker: BrokerCtx,
        trading_mode: TradingMode,
        assets: AssetsConfig,
        #[builder(default = 2)] inventory_poll_interval: u64,
        #[builder(default = 0)] server_port: u16,
        execution_threshold_override: Option<ExecutionThreshold>,
    ) -> Result<Self, CtxError> {
        let execution_threshold = match execution_threshold_override {
            Some(threshold) => threshold,
            None => broker.execution_threshold()?,
        };

        Ok(Self {
            database_url,
            log_level: LogLevel::Debug,
            server_port,
            evm: EvmCtx {
                ws_rpc_url,
                orderbook,
                deployment_block,
            },
            order_polling_interval: 1,
            order_polling_max_jitter: 0,
            position_check_interval: 2,
            inventory_poll_interval,
            broker,
            telemetry: None,
            trading_mode,
            execution_threshold,
            assets,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CtxError {
    #[error(transparent)]
    Rebalancing(Box<RebalancingCtxError>),
    #[error("ORDER_OWNER required when rebalancing is disabled")]
    MissingOrderOwner,
    #[error("failed to read config file {path}")]
    ConfigIo {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to read secrets file {path}")]
    SecretsIo {
        path: PathBuf,
        source: std::io::Error,
    },
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
        "order_owner {configured} must not be set when rebalancing \
         is enabled (address comes from the configured wallet)"
    )]
    OrderOwnerConflictsWithRebalancing { configured: Address },
    #[error(
        "assets.cash operational_limit {configured} is below Alpaca's \
         minimum withdrawal of {minimum}"
    )]
    CashOperationalLimitBelowMinimumWithdrawal { configured: Usdc, minimum: Usdc },
    #[error(
        "assets.cash.vault_id is required for rebalancing \
         but not configured"
    )]
    MissingCashVaultId,
    #[error(
        "assets.equities.{symbol}.vault_id is required when \
         rebalancing is enabled but not configured"
    )]
    MissingEquityVaultId { symbol: Symbol },
    #[error("{field} polling interval must be non-zero")]
    ZeroPollingInterval { field: &'static str },
}

impl From<RebalancingCtxError> for CtxError {
    fn from(error: RebalancingCtxError) -> Self {
        Self::Rebalancing(Box::new(error))
    }
}

#[cfg(test)]
impl CtxError {
    fn kind(&self) -> &'static str {
        match self {
            Self::Rebalancing(_) => "rebalancing configuration error",
            Self::NotRebalancing => "operation requires rebalancing mode",
            Self::MissingOrderOwner => "ORDER_OWNER required when rebalancing is disabled",
            Self::ConfigIo { .. } => "failed to read config file",
            Self::SecretsIo { .. } => "failed to read secrets file",
            Self::ConfigToml { .. } => "failed to parse config",
            Self::SecretsToml { .. } => "failed to parse secrets",
            Self::InvalidThreshold(_) => "invalid execution threshold",
            Self::Telemetry(_) => "telemetry assembly error",
            Self::RebalancingSecretsMissing => "rebalancing secrets missing",
            Self::RebalancingConfigMissing => "rebalancing config missing",
            Self::OrderOwnerConflictsWithRebalancing { .. } => {
                "order_owner conflicts with rebalancing"
            }
            Self::CashOperationalLimitBelowMinimumWithdrawal { .. } => {
                "cash operational limit below minimum withdrawal"
            }
            Self::MissingCashVaultId => "missing cash vault_id",
            Self::MissingEquityVaultId { .. } => "missing equity vault_id",
            Self::ZeroPollingInterval { .. } => "zero polling interval",
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
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(std::time::Duration::from_secs(10));

    SqlitePool::connect_with(options).await
}

#[cfg(test)]
pub(crate) mod tests {
    use alloy::primitives::{Address, FixedBytes, address};
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
            evm: EvmCtx {
                ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1111111111111111111111111111111111111111"),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            position_check_interval: 60,
            inventory_poll_interval: 60,
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
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },
        }
    }

    fn minimal_config_toml() -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(
            br#"
            database_url = ":memory:"

            [assets.equities]

            [raindex]
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

            [assets.equities]

            [raindex]
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
        assert_eq!(ctx.position_check_interval, 60);
        assert_eq!(ctx.inventory_poll_interval, 60);
    }

    #[tokio::test]
    async fn rebalancing_with_low_cash_operational_limit_fails() {
        let secrets = toml_file(
            r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test_key"
            api_secret = "test_secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
            mode = "sandbox"

            [rebalancing]
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"

            [rebalancing.wallet]
            type = "private-key"
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#,
        );

        let config = toml_file(
            r#"
            database_url = ":memory:"

            [assets.equities]

            [assets.cash]
            rebalancing = "enabled"
            operational_limit = 5

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
            deployment_block = 1

            [rebalancing]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

            [rebalancing.wallet]
            type = "private-key"
            address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

            [rebalancing.equity]
            target = "0.5"
            deviation = "0.2"

            [rebalancing.usdc]
            mode = "enabled"
            target = "0.5"
            deviation = "0.3"
        "#,
        );

        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                CtxError::CashOperationalLimitBelowMinimumWithdrawal { .. }
            ),
            "Expected CashOperationalLimitBelowMinimumWithdrawal for \
             operational_limit=5, got: {error:?}"
        );
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
            position_check_interval = 120
            inventory_poll_interval = 90

            [assets.equities]

            [raindex]
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
        assert_eq!(ctx.position_check_interval, 120);
        assert_eq!(ctx.inventory_poll_interval, 90);
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

            [rebalancing.wallet]
            type = "private-key"
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#,
        );

        let config = toml_file(
            r#"
            database_url = ":memory:"

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
            deployment_block = 1

            [rebalancing]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

            [rebalancing.wallet]
            type = "private-key"
            address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

            [rebalancing.equity]
            target = "0.5"
            deviation = "0.2"

            [rebalancing.usdc]
            mode = "enabled"
            target = "0.5"
            deviation = "0.3"
        "#,
        );

        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                CtxError::Rebalancing(ref inner) if matches!(**inner, RebalancingCtxError::NotAlpacaBroker)
            ),
            "Expected NotAlpacaBroker error for rebalancing with Schwab broker, got {error:?}"
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
    async fn example_config_and_secrets_parse_successfully() {
        let ctx = Ctx::load_files(example_config_toml(), example_secrets_toml())
            .await
            .unwrap();

        // Example configs enable rebalancing with a private-key wallet.
        assert!(matches!(ctx.trading_mode, TradingMode::Rebalancing(_)));

        // In rebalancing mode, order_owner is derived from the wallet key.
        // The example key 0x0123...cdef derives to this address.
        assert_eq!(
            ctx.order_owner(),
            address!("0xfcad0b19bb29d4674531d6f115237e16afce377c")
        );
    }

    #[tokio::test]
    async fn rebalancing_with_order_owner_configured_fails() {
        let config = toml_file(
            r#"
            database_url = ":memory:"

            [assets.equities.EXAMPLE]
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            trading = "enabled"
            rebalancing = "disabled"

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xcccccccccccccccccccccccccccccccccccccccc"
            deployment_block = 1

            [hyperdx]
            service_name = "st0x-hedge"

            [rebalancing]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

            [rebalancing.wallet]
            type = "private-key"
            address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

            [rebalancing.equity]
            target = "0.5"
            deviation = "0.2"

            [rebalancing.usdc]
            mode = "enabled"
            target = "0.5"
            deviation = "0.3"
        "#,
        );

        let secrets = toml_file(
            r#"
            hyperdx.api_key = "test-key"

            [evm]
            ws_rpc_url = "ws://localhost:8545"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test_key"
            api_secret = "test_secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
            mode = "sandbox"

            [rebalancing]
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"

            [rebalancing.wallet]
            type = "private-key"
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#,
        );
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();
        assert!(
            matches!(error, CtxError::OrderOwnerConflictsWithRebalancing { .. }),
            "Expected OrderOwnerConflictsWithRebalancing, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn telemetry_ctx_assembled_from_config_and_secrets() {
        let config = toml_file(
            r#"
            database_url = ":memory:"

            [assets.equities]

            [raindex]
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

            [assets.equities]

            [raindex]
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

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
            deployment_block = 1

            [rebalancing]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

            [rebalancing.wallet]
            type = "private-key"
            address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

            [rebalancing.equity]
            target = "0.5"
            deviation = "0.2"

            [rebalancing.usdc]
            mode = "enabled"
            target = "0.5"
            deviation = "0.3"
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
        let expected = ExecutionThreshold::dollar_value(Usdc::new(Decimal::TWO)).unwrap();
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
        let expected = ExecutionThreshold::dollar_value(Usdc::new(Decimal::TWO)).unwrap();
        assert_eq!(ctx.execution_threshold, expected);
    }

    #[test]
    fn config_error_kind_rebalancing() {
        let err = CtxError::Rebalancing(Box::new(RebalancingCtxError::NotAlpacaBroker));
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

            [rebalancing.wallet]
            type = "private-key"
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#,
        );

        let config = toml_file(
            r#"
            database_url = ":memory:"

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
            deployment_block = 1

            [rebalancing]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

            [rebalancing.wallet]
            type = "private-key"
            address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

            [rebalancing.equity]
            target = "0.5"
            deviation = "0.2"

            [rebalancing.usdc]
            mode = "enabled"
            target = "0.5"
            deviation = "0.3"
        "#,
        );

        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                CtxError::Rebalancing(ref inner) if matches!(**inner, RebalancingCtxError::NotAlpacaBroker)
            ),
            "expected NotAlpacaBroker, got: {error:?}"
        );
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
        let config: Config = toml::from_str(config_str).unwrap();

        let global_limit = config
            .assets
            .equities
            .operational_limit
            .map(Positive::inner);

        for (symbol, equity) in &config.assets.equities.symbols {
            if equity.rebalancing == OperationMode::Enabled
                && let Some(limit) = &equity.operational_limit
                && let Some(global) = global_limit
            {
                assert!(
                    limit.inner() < global,
                    "{symbol}: per-asset operational_limit ({}) must be \
                     stricter than global equities operational_limit ({global}) \
                     to provide meaningful per-asset safety",
                    limit.inner()
                );
            }
        }
    }

    #[test]
    fn example_config_toml_is_valid() {
        let config_str = include_str!("../example.config.toml");
        let _: Config = toml::from_str(config_str).unwrap();
    }

    #[test]
    fn example_secrets_toml_is_valid() {
        let secrets_str = include_str!("../example.secrets.toml");
        let _: Secrets = toml::from_str(secrets_str).unwrap();
    }

    #[test]
    fn e2e_config_toml_is_valid() {
        let config_str = include_str!("../e2e/config.toml");
        let _: Config = toml::from_str(config_str).unwrap();
    }

    #[test]
    fn e2e_secrets_toml_is_valid() {
        let secrets_str = include_str!("../e2e/secrets.toml");
        let _: Secrets = toml::from_str(secrets_str).unwrap();
    }

    #[test]
    fn all_repo_config_tomls_are_valid() {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut config_paths: Vec<PathBuf> = std::fs::read_dir(repo_root.join("config"))
            .unwrap()
            .filter_map(std::result::Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "toml"))
            .collect();

        config_paths.push(repo_root.join("example.config.toml"));
        config_paths.push(repo_root.join("e2e/config.toml"));

        for path in config_paths {
            let contents = std::fs::read_to_string(&path).unwrap_or_else(|error| {
                panic!("Failed to read config {path:?}: {error}");
            });
            toml::from_str::<Config>(&contents).unwrap_or_else(|error| {
                panic!("Invalid config {path:?}: {error}");
            });
        }
    }

    #[test]
    fn all_repo_secrets_tomls_are_valid() {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let secret_paths = [
            repo_root.join("example.secrets.toml"),
            repo_root.join("e2e/secrets.toml"),
        ];

        for path in secret_paths {
            let contents = std::fs::read_to_string(&path).unwrap_or_else(|error| {
                panic!("Failed to read secrets {path:?}: {error}");
            });
            toml::from_str::<Secrets>(&contents).unwrap_or_else(|error| {
                panic!("Invalid secrets {path:?}: {error}");
            });
        }
    }

    #[tokio::test]
    async fn unknown_config_fields_rejected() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            bogus_field = "should fail"

            [raindex]
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
    async fn unknown_assets_fields_rejected() {
        let config = toml_file(
            r#"
            database_url = ":memory:"

            [assets]
            bogus_field = "should fail"

            [assets.equities]

            [raindex]
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
            "Expected config parse error for unknown assets field, got {err:?}"
        );
    }

    #[tokio::test]
    async fn unknown_equity_fields_rejected() {
        let config = toml_file(
            r#"
            database_url = ":memory:"

            [assets.equities.AAPL]
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            trading = "enabled"
            rebalancing = "disabled"
            bogus_field = "should fail"

            [raindex]
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
            "Expected config parse error for unknown equity field, got {err:?}"
        );
    }

    #[tokio::test]
    async fn unknown_cash_fields_rejected() {
        let config = toml_file(
            r#"
            database_url = ":memory:"

            [assets.cash]
            rebalancing = "disabled"
            bogus_field = "should fail"

            [raindex]
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
            "Expected config parse error for unknown cash field, got {err:?}"
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
    async fn parse_error_display_includes_file_path() {
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

        let secrets_path = secrets.path().to_path_buf();
        let err = Ctx::load_files(config.path(), &secrets_path)
            .await
            .unwrap_err();
        let display = err.to_string();
        assert!(
            display.contains(&secrets_path.display().to_string()),
            "Error display must include the file path so operators can \
             identify which file failed to parse. Got: {display}"
        );

        let source = std::error::Error::source(&err).unwrap();
        let source_display = source.to_string();
        assert!(
            source_display.contains("extra_secret"),
            "Error source must contain the TOML parse details. Got: {source_display}"
        );
    }

    #[tokio::test]
    async fn config_parse_error_display_includes_file_path() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            bogus_field = "should fail"

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
            order_owner = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            deployment_block = 1
        "#,
        );
        let secrets = dry_run_secrets_toml();

        let config_path = config.path().to_path_buf();
        let err = Ctx::load_files(&config_path, secrets.path())
            .await
            .unwrap_err();
        let display = err.to_string();
        assert!(
            display.contains(&config_path.display().to_string()),
            "Config error display must include the file path so operators \
             can identify which file failed to parse. Got: {display}"
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

    #[test]
    fn assets_config_parses_equities_and_cash() {
        let toml_str = r#"
            [equities.RKLB]
            tokenized_equity = "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
            tokenized_equity_derivative = "0xf4f8c66085910d583c01f3b4e44bf731d4e2c565"
            vault_id = "0xfab"
            trading = "disabled"
            rebalancing = "enabled"
            operational_limit = 5

            [equities.SPYM]
            tokenized_equity = "0x8fdf41116f755771bfe0747d5f8c3711d5debfbb"
            tokenized_equity_derivative = "0x31c2c14134e6e3b7ef9478297f199331133fc2d8"
            trading = "disabled"
            rebalancing = "disabled"

            [cash]
            vault_id = "0x0000000000000000000000000000000000000000000000000000000000000fab"
            rebalancing = "disabled"
            operational_limit = 100
        "#;

        let config: AssetsConfig = toml::from_str(toml_str).unwrap();

        assert_eq!(config.equities.symbols.len(), 2);

        let rklb = &config.equities.symbols[&Symbol::new("RKLB").unwrap()];
        assert_eq!(
            rklb.tokenized_equity,
            "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
                .parse::<Address>()
                .unwrap()
        );
        assert_eq!(rklb.trading, OperationMode::Disabled);
        assert_eq!(rklb.rebalancing, OperationMode::Enabled);
        assert!(rklb.vault_id.is_some());
        assert!(rklb.operational_limit.is_some());

        let cash = config.cash.unwrap();
        assert_eq!(cash.rebalancing, OperationMode::Disabled);
        assert!(cash.vault_id.is_some());
    }

    #[test]
    fn short_vault_id_left_pads_to_b256() {
        let toml_str = r#"
            [equities.RKLB]
            tokenized_equity = "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
            tokenized_equity_derivative = "0xf4f8c66085910d583c01f3b4e44bf731d4e2c565"
            vault_id = "0xfab"
            trading = "disabled"
            rebalancing = "enabled"
        "#;

        let config: AssetsConfig = toml::from_str(toml_str).unwrap();
        let rklb = &config.equities.symbols[&Symbol::new("RKLB").unwrap()];
        let expected: B256 = "0000000000000000000000000000000000000000000000000000000000000fab"
            .parse()
            .unwrap();
        assert_eq!(rklb.vault_id.unwrap(), expected);
    }

    #[test]
    fn equity_missing_trading_field_rejects() {
        let toml_str = r#"
            [equities.AAPL]
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            rebalancing = "disabled"
        "#;

        let result = toml::from_str::<AssetsConfig>(toml_str);
        assert!(
            result.is_err(),
            "Expected error for missing trading field, got {result:?}"
        );
    }

    #[test]
    fn equity_missing_rebalancing_field_rejects() {
        let toml_str = r#"
            [equities.AAPL]
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            trading = "enabled"
        "#;

        let result = toml::from_str::<AssetsConfig>(toml_str);
        assert!(
            result.is_err(),
            "Expected error for missing rebalancing field, got {result:?}"
        );
    }

    #[test]
    fn per_asset_operational_limits_parsed_independently() {
        let toml_str = r#"
            [equities.AAPL]
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            trading = "enabled"
            rebalancing = "disabled"
            operational_limit = 10

            [equities.TSLA]
            tokenized_equity = "0xcccccccccccccccccccccccccccccccccccccccc"
            tokenized_equity_derivative = "0xdddddddddddddddddddddddddddddddddddddddd"
            trading = "enabled"
            rebalancing = "disabled"
        "#;

        let config: AssetsConfig = toml::from_str(toml_str).unwrap();
        let aapl = &config.equities.symbols[&Symbol::new("AAPL").unwrap()];
        let tsla = &config.equities.symbols[&Symbol::new("TSLA").unwrap()];
        assert!(
            aapl.operational_limit.is_some(),
            "AAPL should have an operational limit"
        );
        assert!(
            tsla.operational_limit.is_none(),
            "TSLA should not have an operational limit"
        );
    }

    #[test]
    fn is_trading_enabled_returns_configured_value() {
        let mut symbols = HashMap::new();
        symbols.insert(
            Symbol::new("RKLB").unwrap(),
            EquityAssetConfig {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                vault_id: None,
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Enabled,
                operational_limit: None,
            },
        );

        let ctx = Ctx {
            assets: AssetsConfig {
                equities: EquitiesConfig {
                    operational_limit: None,
                    symbols,
                },
                cash: None,
            },
            ..create_test_ctx_with_order_owner(address!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ))
        };

        assert!(
            !ctx.is_trading_enabled(&Symbol::new("RKLB").unwrap()),
            "RKLB trading should be disabled"
        );
    }

    #[test]
    fn is_trading_disabled_for_unknown_assets() {
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));

        assert!(
            !ctx.is_trading_enabled(&Symbol::new("UNKNOWN").unwrap()),
            "Unknown assets should default to trading disabled (fail-closed)"
        );
    }

    #[test]
    fn is_rebalancing_enabled_returns_configured_value() {
        let mut symbols = HashMap::new();
        symbols.insert(
            Symbol::new("RKLB").unwrap(),
            EquityAssetConfig {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                vault_id: None,
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Enabled,
                operational_limit: None,
            },
        );

        let ctx = Ctx {
            assets: AssetsConfig {
                equities: EquitiesConfig {
                    operational_limit: None,
                    symbols,
                },
                cash: None,
            },
            ..create_test_ctx_with_order_owner(address!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ))
        };

        assert!(
            ctx.is_rebalancing_enabled(&Symbol::new("RKLB").unwrap()),
            "RKLB rebalancing should be enabled"
        );
    }

    #[test]
    fn is_rebalancing_enabled_defaults_to_false_for_unknown() {
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));

        assert!(
            !ctx.is_rebalancing_enabled(&Symbol::new("UNKNOWN").unwrap()),
            "Unknown assets should default to rebalancing disabled"
        );
    }

    #[test]
    fn base_symbol_config_keys_fix_lookup_bug() {
        // Config keys use base symbols (SPYM not tSPYM).
        // is_trading_enabled uses Symbol directly, which matches
        // base symbol keys. This verifies the bug fix.
        let mut symbols = HashMap::new();
        symbols.insert(
            Symbol::new("SPYM").unwrap(),
            EquityAssetConfig {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                vault_id: None,
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Disabled,
                operational_limit: None,
            },
        );

        let ctx = Ctx {
            assets: AssetsConfig {
                equities: EquitiesConfig {
                    operational_limit: None,
                    symbols,
                },
                cash: None,
            },
            ..create_test_ctx_with_order_owner(address!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ))
        };

        // The lookup uses base symbol "SPYM" which matches the config key
        assert!(
            !ctx.is_trading_enabled(&Symbol::new("SPYM").unwrap()),
            "SPYM trading should be disabled per config"
        );
    }

    mod proptests {
        use proptest::prelude::*;

        use super::*;

        /// Generates a valid hex digit string of length 1..=64.
        fn arb_hex_digits() -> impl Strategy<Value = String> {
            prop::collection::vec(
                prop::sample::select(vec![
                    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
                ]),
                1..=64,
            )
            .prop_map(|chars| chars.into_iter().collect::<String>())
        }

        proptest! {
            /// Arbitrary short hex strings left-pad to correct B256.
            ///
            /// The padded result should equal the hex digits
            /// zero-filled on the left to 64 chars.
            #[test]
            fn padded_b256_roundtrip(hex_digits in arb_hex_digits()) {
                let toml_str = format!(
                    r#"vault_id = "0x{hex_digits}""#,
                );

                #[derive(Deserialize)]
                #[serde(rename_all = "snake_case")]
                struct Wrapper {
                    #[serde(deserialize_with = "deserialize_padded_b256")]
                    vault_id: Option<B256>,
                }

                let wrapper: Wrapper = toml::from_str(&toml_str).unwrap();
                let parsed = wrapper.vault_id.unwrap();

                let expected_hex = format!("{hex_digits:0>64}");
                let expected: B256 = expected_hex.parse().unwrap();
                prop_assert_eq!(parsed, expected);
            }

            /// Invalid hex characters must produce a deserialization error.
            #[test]
            fn padded_b256_rejects_invalid_hex(
                bad_char in "[g-zG-Z!@#$%^&*]",
                prefix in arb_hex_digits(),
            ) {
                let hex_str = format!("{prefix}{bad_char}");
                let toml_str = format!(
                    r#"vault_id = "0x{hex_str}""#,
                );

                #[derive(Debug, Deserialize)]
                #[serde(rename_all = "snake_case")]
                struct Wrapper {
                    #[serde(deserialize_with = "deserialize_padded_b256")]
                    vault_id: Option<B256>,
                }

                let result = toml::from_str::<Wrapper>(&toml_str);
                prop_assert!(
                    result.is_err(),
                    "Expected error for invalid hex '{hex_str}', got {result:?}. \
                     Parsed vault_id: {:?}",
                    result.as_ref().ok().map(|w| w.vault_id),
                );
            }

            /// OperationMode serializes and deserializes as lowercase strings.
            #[test]
            fn operation_mode_serde_roundtrip(enabled in any::<bool>()) {
                #[derive(Debug, PartialEq, Serialize, Deserialize)]
                struct Wrapper {
                    mode: OperationMode,
                }

                let wrapper = Wrapper {
                    mode: if enabled {
                        OperationMode::Enabled
                    } else {
                        OperationMode::Disabled
                    },
                };

                let serialized = toml::to_string(&wrapper).unwrap();
                let deserialized: Wrapper = toml::from_str(&serialized).unwrap();
                prop_assert_eq!(wrapper, deserialized);
            }

            /// OperationMode rejects strings that are not "enabled" or
            /// "disabled".
            #[test]
            fn operation_mode_rejects_invalid_strings(
                invalid in "[a-z]{3,10}"
                    .prop_filter("must not be a valid mode", |value| {
                        value != "enabled" && value != "disabled"
                    })
            ) {
                let toml_str = format!(r#"mode = "{invalid}""#);

                #[derive(Debug, Deserialize)]
                struct Wrapper {
                    #[allow(dead_code)]
                    mode: OperationMode,
                }

                let result = toml::from_str::<Wrapper>(&toml_str);
                prop_assert!(
                    result.is_err(),
                    "Expected error for invalid mode '{invalid}', got {result:?}"
                );
            }

            /// EquityAssetConfig parses when all required fields are present.
            #[test]
            fn equity_asset_config_parses_with_addresses(
                share_byte in any::<u8>(),
                derivative_byte in any::<u8>(),
                trading_enabled in any::<bool>(),
                rebalancing_enabled in any::<bool>(),
            ) {
                let trading = if trading_enabled { "enabled" } else { "disabled" };
                let rebalancing = if rebalancing_enabled { "enabled" } else { "disabled" };
                let toml_str = format!(
                    r#"
                    tokenized_equity = "0x{share_byte:02x}{:0>38}"
                    tokenized_equity_derivative = "0x{derivative_byte:02x}{:0>38}"
                    trading = "{trading}"
                    rebalancing = "{rebalancing}"
                    "#,
                    "", "",
                );

                let result = toml::from_str::<EquityAssetConfig>(&toml_str);
                prop_assert!(
                    result.is_ok(),
                    "Expected successful parse, got {result:?}"
                );

                let config = result.unwrap();
                let expected_trading = if trading_enabled {
                    OperationMode::Enabled
                } else {
                    OperationMode::Disabled
                };
                let expected_rebalancing = if rebalancing_enabled {
                    OperationMode::Enabled
                } else {
                    OperationMode::Disabled
                };
                prop_assert_eq!(config.trading, expected_trading);
                prop_assert_eq!(config.rebalancing, expected_rebalancing);
            }
        }

        #[test]
        fn cash_asset_config_parses_without_token_addresses() {
            let toml_str = r#"
                vault_id = "0xfab"
                rebalancing = "disabled"
                operational_limit = 100
            "#;

            let config: CashAssetConfig = toml::from_str(toml_str).unwrap();
            assert_eq!(config.rebalancing, OperationMode::Disabled);
            assert!(config.vault_id.is_some());
        }

        #[test]
        fn equity_asset_config_rejects_missing_tokenized_equity() {
            let toml_str = r#"
                tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            "#;

            let result = toml::from_str::<EquityAssetConfig>(toml_str);
            assert!(
                result.is_err(),
                "Expected error for missing tokenized_equity, got {result:?}"
            );
        }

        #[test]
        fn equity_asset_config_rejects_missing_tokenized_equity_derivative() {
            let toml_str = r#"
                tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            "#;

            let result = toml::from_str::<EquityAssetConfig>(toml_str);
            assert!(
                result.is_err(),
                "Expected error for missing tokenized_equity_derivative, got {result:?}"
            );
        }

        #[test]
        fn padded_b256_rejects_empty_hex() {
            let toml_str = r#"vault_id = "0x""#;

            #[derive(Debug, Deserialize)]
            #[serde(rename_all = "snake_case")]
            struct Wrapper {
                #[serde(deserialize_with = "deserialize_padded_b256")]
                #[allow(dead_code)]
                vault_id: Option<B256>,
            }

            let result = toml::from_str::<Wrapper>(toml_str);
            assert!(
                result.is_err(),
                "Expected error for empty hex string, got {result:?}"
            );
        }

        #[test]
        fn padded_b256_rejects_too_long_hex() {
            let long_hex = "a".repeat(65);
            let toml_str = format!(r#"vault_id = "0x{long_hex}""#);

            #[derive(Debug, Deserialize)]
            #[serde(rename_all = "snake_case")]
            struct Wrapper {
                #[serde(deserialize_with = "deserialize_padded_b256")]
                #[allow(dead_code)]
                vault_id: Option<B256>,
            }

            let result = toml::from_str::<Wrapper>(&toml_str);
            assert!(
                result.is_err(),
                "Expected error for too-long hex string, got {result:?}"
            );
        }

        #[test]
        fn operation_mode_enabled_from_string() {
            #[derive(Debug, Deserialize)]
            struct Wrapper {
                mode: OperationMode,
            }

            let wrapper: Wrapper = toml::from_str(r#"mode = "enabled""#).unwrap();
            assert_eq!(wrapper.mode, OperationMode::Enabled);
        }

        #[test]
        fn operation_mode_disabled_from_string() {
            #[derive(Debug, Deserialize)]
            struct Wrapper {
                mode: OperationMode,
            }

            let wrapper: Wrapper = toml::from_str(r#"mode = "disabled""#).unwrap();
            assert_eq!(wrapper.mode, OperationMode::Disabled);
        }
    }

    #[test]
    fn broker_type_tag_uses_kebab_case() {
        let variants = [
            ("dry-run", "DryRun"),
            ("schwab", "Schwab"),
            ("alpaca-trading-api", "AlpacaTradingApi"),
            ("alpaca-broker-api", "AlpacaBrokerApi"),
        ];

        for (kebab_value, variant_name) in variants {
            let toml_str = format!(
                r#"
                [evm]
                ws_rpc_url = "ws://localhost:8545"

                [broker]
                type = "{kebab_value}"
                "#,
            );

            // Only dry-run and schwab parse without extra fields;
            // alpaca variants need credentials but the tag itself
            // must be accepted before field validation runs.
            let result = toml::from_str::<Secrets>(&toml_str);
            match result {
                Ok(_) => {}
                Err(error) => {
                    let msg = error.to_string();
                    assert!(
                        !msg.contains("unknown variant"),
                        "Broker type tag \"{kebab_value}\" ({variant_name}) \
                         was rejected as unknown variant. BrokerSecrets must \
                         use rename_all = \"kebab-case\". Error: {msg}"
                    );
                }
            }
        }
    }

    #[test]
    fn secrets_evm_section_uses_evm_not_raindex() {
        // The secrets TOML section for EVM RPC URLs must be [evm], not
        // [raindex]. These are generic EVM secrets (RPC endpoints), not
        // Raindex-specific. The config TOML correctly uses [raindex] because
        // those fields (orderbook, deployment_block) are Raindex-specific.
        let with_evm = r#"
            [evm]
            ws_rpc_url = "ws://localhost:8545"

            [broker]
            type = "dry-run"
        "#;

        let result = toml::from_str::<Secrets>(with_evm);
        assert!(
            result.is_ok(),
            "Secrets TOML with [evm] section should parse successfully, \
             but got error: {}",
            result.err().unwrap()
        );

        let with_raindex = r#"
            [raindex]
            ws_rpc_url = "ws://localhost:8545"

            [broker]
            type = "dry-run"
        "#;

        let result = toml::from_str::<Secrets>(with_raindex);
        assert!(
            result.is_err(),
            "Secrets TOML with [raindex] section should be rejected \
             (deny_unknown_fields); the correct section name is [evm]"
        );
    }

    #[test]
    fn broker_type_tag_rejects_snake_case() {
        let snake_values = ["dry_run", "alpaca_trading_api", "alpaca_broker_api"];

        for snake_value in snake_values {
            let toml_str = format!(
                r#"
                [evm]
                ws_rpc_url = "ws://localhost:8545"

                [broker]
                type = "{snake_value}"
                "#,
            );

            let result = toml::from_str::<Secrets>(&toml_str);
            assert!(
                result.is_err(),
                "Snake_case broker type \"{snake_value}\" should be rejected"
            );
            let error = result.err().unwrap();
            assert!(
                error.to_string().contains("unknown variant"),
                "Snake_case broker type \"{snake_value}\" should be rejected \
                 as unknown variant (kebab-case required), but got: {error}"
            );
        }
    }
}
