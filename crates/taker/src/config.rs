//! Application configuration loading and validation.
//!
//! Reads plaintext config and encrypted secrets from separate TOML files,
//! validates compatibility, and assembles the runtime [`Ctx`] that the rest
//! of the application consumes.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use alloy::primitives::Address;
use clap::Parser;
use serde::Deserialize;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use tracing::Level;
use url::Url;

use st0x_execution::{AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, Symbol};
use st0x_shared::EquityTokenAddresses;

/// CLI arguments for the taker binary.
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
    evm: EvmConfig,
    order_taker: OrderTakerConfig,
    tokenization: TokenizationConfig,
    hold_pool: HoldPoolConfig,
    #[serde(default)]
    equities: HashMap<Symbol, EquityTokenConfig>,
}

/// EVM-related plaintext configuration.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EvmConfig {
    /// Raindex orderbook contract address.
    orderbook: Address,
    /// USDC contract address on the target chain.
    usdc_address: Address,
    /// Block to start scanning for historical events on first run.
    deployment_block: u64,
    /// Address of the liquidity bot's order owner. Orders from this address
    /// are ignored since they are managed by the hedging bot.
    excluded_owner: Address,
}

/// Order taker strategy configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct OrderTakerConfig {
    /// Minimum profit margin after all costs (in basis points).
    min_profit_margin_bps: u32,
    /// Maximum position size per symbol (in shares).
    max_position_per_symbol: f64,
    /// Maximum total capital at risk (in USDC).
    max_total_exposure_usdc: f64,
    /// Gas price ceiling (in gwei). Don't take if gas exceeds this.
    max_gas_price_gwei: u64,
    /// Polling interval for re-evaluating tracked orders (seconds).
    evaluation_interval_secs: u64,
}

/// Tokenization configuration (plaintext portion).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenizationConfig {
    /// Alpaca account ID for tokenization operations.
    alpaca_account_id: AlpacaAccountId,
    /// Bot's wallet address (receives minted tokens).
    wallet_address: Address,
    /// Alpaca redemption wallet (tokens sent here to redeem).
    redemption_wallet: Address,
}

/// Hold pool configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct HoldPoolConfig {
    /// How long to hold minted tokens before redeeming (seconds).
    retention_window_secs: u64,
    /// Maximum value to hold in the pool across all symbols (USDC equivalent).
    max_pool_value_usdc: f64,
    /// Per-symbol cap (in shares).
    max_per_symbol: f64,
}

/// Per-equity token address configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct EquityTokenConfig {
    /// wtToken ERC-4626 vault contract address.
    wrapped_address: Address,
    /// tToken ERC20 contract address.
    unwrapped_address: Address,
}

/// Secret credentials deserialized from the encrypted secrets TOML.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Secrets {
    evm: EvmSecrets,
    alpaca: AlpacaSecrets,
}

/// EVM-related secrets.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EvmSecrets {
    ws_rpc_url: Url,
    private_key: String,
}

/// Alpaca Broker API credentials. Used for both share trading (Executor)
/// and tokenization (Tokenizer).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AlpacaSecrets {
    api_key: String,
    api_secret: String,
    #[serde(default)]
    mode: Option<AlpacaBrokerApiMode>,
}

/// Log level configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
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

/// Runtime EVM context assembled from config and secrets.
#[derive(Clone)]
pub(crate) struct EvmCtx {
    pub(crate) ws_rpc_url: Url,
    pub(crate) private_key: String,
    pub(crate) orderbook: Address,
    pub(crate) usdc_address: Address,
    pub(crate) deployment_block: u64,
    pub(crate) excluded_owner: Address,
}

impl std::fmt::Debug for EvmCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvmCtx")
            .field("ws_rpc_url", &"[REDACTED]")
            .field("private_key", &"[REDACTED]")
            .field("orderbook", &self.orderbook)
            .field("usdc_address", &self.usdc_address)
            .field("deployment_block", &self.deployment_block)
            .field("excluded_owner", &self.excluded_owner)
            .finish()
    }
}

/// Runtime order taker strategy context.
#[derive(Debug, Clone)]
pub(crate) struct OrderTakerCtx {
    pub(crate) min_profit_margin_bps: u32,
    pub(crate) max_position_per_symbol: f64,
    pub(crate) max_total_exposure_usdc: f64,
    pub(crate) max_gas_price_gwei: u64,
    pub(crate) evaluation_interval_secs: u64,
}

/// Runtime tokenization context assembled from config and secrets.
#[derive(Clone)]
pub(crate) struct TokenizationCtx {
    pub(crate) alpaca_account_id: AlpacaAccountId,
    pub(crate) wallet_address: Address,
    pub(crate) redemption_wallet: Address,
}

impl std::fmt::Debug for TokenizationCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenizationCtx")
            .field("alpaca_account_id", &self.alpaca_account_id)
            .field("wallet_address", &self.wallet_address)
            .field("redemption_wallet", &self.redemption_wallet)
            .finish()
    }
}

/// Runtime hold pool context.
#[derive(Debug, Clone)]
pub(crate) struct HoldPoolCtx {
    pub(crate) retention_window_secs: u64,
    pub(crate) max_pool_value_usdc: f64,
    pub(crate) max_per_symbol: f64,
}

/// Runtime Alpaca broker context.
#[derive(Clone)]
pub(crate) struct AlpacaCtx {
    pub(crate) broker: AlpacaBrokerApiCtx,
}

impl std::fmt::Debug for AlpacaCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaCtx")
            .field("broker", &"[REDACTED]")
            .finish()
    }
}

/// Combined runtime context for the taker bot. Assembled from plaintext
/// config, encrypted secrets, and derived runtime state.
#[derive(Clone)]
pub struct Ctx {
    pub(crate) database_url: String,
    pub log_level: LogLevel,
    pub(crate) evm: EvmCtx,
    pub(crate) order_taker: OrderTakerCtx,
    pub(crate) tokenization: TokenizationCtx,
    pub(crate) hold_pool: HoldPoolCtx,
    pub(crate) alpaca: AlpacaCtx,
    pub(crate) equities: HashMap<Symbol, EquityTokenAddresses>,
}

impl std::fmt::Debug for Ctx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ctx")
            .field("database_url", &self.database_url)
            .field("log_level", &self.log_level)
            .field("evm", &self.evm)
            .field("order_taker", &self.order_taker)
            .field("tokenization", &self.tokenization)
            .field("hold_pool", &self.hold_pool)
            .field("alpaca", &self.alpaca)
            .field("equities", &self.equities)
            .finish()
    }
}

/// Errors during context assembly.
#[derive(Debug, thiserror::Error)]
pub enum CtxError {
    #[error("Failed to read config file at {path}: {source}")]
    ConfigIo {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("Failed to read secrets file at {path}: {source}")]
    SecretsIo {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("Failed to parse config TOML at {path}: {source}")]
    ConfigToml {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("Failed to parse secrets TOML at {path}: {source}")]
    SecretsToml {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("evaluation_interval_secs must be greater than zero")]
    ZeroEvaluationInterval,
    #[error("retention_window_secs must be greater than zero")]
    ZeroRetentionWindow,
    #[error("No equity tokens configured")]
    NoEquities,
}

impl Ctx {
    /// Loads and validates config and secrets from TOML files,
    /// assembling the runtime context.
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

        Self::from_parsed(config, secrets)
    }

    /// Assembles a `Ctx` from already-parsed config and secrets.
    /// Separated from `load_files` so unit tests can validate
    /// without touching the filesystem.
    fn from_parsed(config: Config, secrets: Secrets) -> Result<Self, CtxError> {
        if config.order_taker.evaluation_interval_secs == 0 {
            return Err(CtxError::ZeroEvaluationInterval);
        }

        if config.hold_pool.retention_window_secs == 0 {
            return Err(CtxError::ZeroRetentionWindow);
        }

        if config.equities.is_empty() {
            return Err(CtxError::NoEquities);
        }

        let equities = config
            .equities
            .into_iter()
            .map(|(symbol, equity)| {
                let addresses = EquityTokenAddresses {
                    wrapped: equity.wrapped_address,
                    unwrapped: equity.unwrapped_address,
                    enabled: true,
                };
                (symbol, addresses)
            })
            .collect();

        let evm = EvmCtx {
            ws_rpc_url: secrets.evm.ws_rpc_url,
            private_key: secrets.evm.private_key,
            orderbook: config.evm.orderbook,
            usdc_address: config.evm.usdc_address,
            deployment_block: config.evm.deployment_block,
            excluded_owner: config.evm.excluded_owner,
        };

        let order_taker = OrderTakerCtx {
            min_profit_margin_bps: config.order_taker.min_profit_margin_bps,
            max_position_per_symbol: config.order_taker.max_position_per_symbol,
            max_total_exposure_usdc: config.order_taker.max_total_exposure_usdc,
            max_gas_price_gwei: config.order_taker.max_gas_price_gwei,
            evaluation_interval_secs: config.order_taker.evaluation_interval_secs,
        };

        let tokenization = TokenizationCtx {
            alpaca_account_id: config.tokenization.alpaca_account_id,
            wallet_address: config.tokenization.wallet_address,
            redemption_wallet: config.tokenization.redemption_wallet,
        };

        let hold_pool = HoldPoolCtx {
            retention_window_secs: config.hold_pool.retention_window_secs,
            max_pool_value_usdc: config.hold_pool.max_pool_value_usdc,
            max_per_symbol: config.hold_pool.max_per_symbol,
        };

        let alpaca = AlpacaCtx {
            broker: AlpacaBrokerApiCtx {
                api_key: secrets.alpaca.api_key,
                api_secret: secrets.alpaca.api_secret,
                account_id: config.tokenization.alpaca_account_id,
                mode: secrets.alpaca.mode,
                asset_cache_ttl: std::time::Duration::from_secs(3600),
                time_in_force: st0x_execution::TimeInForce::default(),
            },
        };

        let log_level = config.log_level.unwrap_or(LogLevel::Debug);

        Ok(Self {
            database_url: config.database_url,
            log_level,
            evm,
            order_taker,
            tokenization,
            hold_pool,
            alpaca,
            equities,
        })
    }

    /// Creates a SQLite connection pool from the configured database URL.
    pub async fn get_sqlite_pool(&self) -> Result<SqlitePool, sqlx::Error> {
        configure_sqlite_pool(&self.database_url).await
    }
}

/// Configures a SQLite connection pool with WAL mode and busy timeout.
async fn configure_sqlite_pool(database_url: &str) -> Result<SqlitePool, sqlx::Error> {
    let options: SqliteConnectOptions = database_url
        .parse::<SqliteConnectOptions>()?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(std::time::Duration::from_secs(10));

    SqlitePool::connect_with(options).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config_toml() -> String {
        r#"
database_url = "sqlite://taker.db"
log_level = "debug"

[evm]
orderbook = "0x1111111111111111111111111111111111111111"
usdc_address = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
deployment_block = 100
excluded_owner = "0x2222222222222222222222222222222222222222"

[order_taker]
min_profit_margin_bps = 50
max_position_per_symbol = 100.0
max_total_exposure_usdc = 50000.0
max_gas_price_gwei = 10
evaluation_interval_secs = 10

[tokenization]
alpaca_account_id = "550e8400-e29b-41d4-a716-446655440000"
wallet_address = "0x3333333333333333333333333333333333333333"
redemption_wallet = "0x4444444444444444444444444444444444444444"

[hold_pool]
retention_window_secs = 3600
max_pool_value_usdc = 10000.0
max_per_symbol = 50.0

[equities.AAPL]
wrapped_address = "0x5555555555555555555555555555555555555555"
unwrapped_address = "0x6666666666666666666666666666666666666666"
"#
        .to_string()
    }

    fn valid_secrets_toml() -> String {
        r#"
[evm]
ws_rpc_url = "ws://localhost:8545"
private_key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

[alpaca]
api_key = "test_key"
api_secret = "test_secret"
mode = "sandbox"
"#
        .to_string()
    }

    fn parse_config_and_secrets(config_toml: &str, secrets_toml: &str) -> Result<Ctx, CtxError> {
        let config: Config =
            toml::from_str(config_toml).map_err(|source| CtxError::ConfigToml {
                path: PathBuf::from("test"),
                source,
            })?;
        let secrets: Secrets =
            toml::from_str(secrets_toml).map_err(|source| CtxError::SecretsToml {
                path: PathBuf::from("test"),
                source,
            })?;

        Ctx::from_parsed(config, secrets)
    }

    #[test]
    fn valid_config_roundtrip() {
        let ctx = parse_config_and_secrets(&valid_config_toml(), &valid_secrets_toml()).unwrap();

        assert_eq!(ctx.database_url, "sqlite://taker.db");
        assert_eq!(ctx.evm.deployment_block, 100);
        assert_eq!(ctx.order_taker.min_profit_margin_bps, 50);
        assert_eq!(ctx.order_taker.evaluation_interval_secs, 10);
        assert_eq!(ctx.hold_pool.retention_window_secs, 3600);
        assert_eq!(ctx.equities.len(), 1);

        let aapl = ctx.equities.get(&Symbol::new("AAPL").unwrap()).unwrap();
        assert_eq!(
            aapl.wrapped.to_string(),
            "0x5555555555555555555555555555555555555555"
        );
    }

    #[test]
    fn zero_evaluation_interval_rejected() {
        let config = valid_config_toml().replace(
            "evaluation_interval_secs = 10",
            "evaluation_interval_secs = 0",
        );

        let error = parse_config_and_secrets(&config, &valid_secrets_toml()).unwrap_err();
        assert!(matches!(error, CtxError::ZeroEvaluationInterval));
    }

    #[test]
    fn zero_retention_window_rejected() {
        let config = valid_config_toml()
            .replace("retention_window_secs = 3600", "retention_window_secs = 0");

        let error = parse_config_and_secrets(&config, &valid_secrets_toml()).unwrap_err();
        assert!(matches!(error, CtxError::ZeroRetentionWindow));
    }

    #[test]
    fn empty_equities_rejected() {
        let config = r#"
database_url = "sqlite://taker.db"

[evm]
orderbook = "0x1111111111111111111111111111111111111111"
usdc_address = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
deployment_block = 100
excluded_owner = "0x2222222222222222222222222222222222222222"

[order_taker]
min_profit_margin_bps = 50
max_position_per_symbol = 100.0
max_total_exposure_usdc = 50000.0
max_gas_price_gwei = 10
evaluation_interval_secs = 10

[tokenization]
alpaca_account_id = "550e8400-e29b-41d4-a716-446655440000"
wallet_address = "0x3333333333333333333333333333333333333333"
redemption_wallet = "0x4444444444444444444444444444444444444444"

[hold_pool]
retention_window_secs = 3600
max_pool_value_usdc = 10000.0
max_per_symbol = 50.0
"#;

        let error = parse_config_and_secrets(config, &valid_secrets_toml()).unwrap_err();
        assert!(matches!(error, CtxError::NoEquities));
    }

    #[test]
    fn unknown_config_field_rejected() {
        let config = valid_config_toml() + "\nunknown_field = true\n";

        let error = parse_config_and_secrets(&config, &valid_secrets_toml()).unwrap_err();
        assert!(matches!(error, CtxError::ConfigToml { .. }));
    }

    #[test]
    fn unknown_secrets_field_rejected() {
        let secrets = valid_secrets_toml() + "\nunknown_field = true\n";

        let error = parse_config_and_secrets(&valid_config_toml(), &secrets).unwrap_err();
        assert!(matches!(error, CtxError::SecretsToml { .. }));
    }

    #[test]
    fn multiple_equities_parsed() {
        let config = valid_config_toml()
            + r#"
[equities.TSLA]
wrapped_address = "0x7777777777777777777777777777777777777777"
unwrapped_address = "0x8888888888888888888888888888888888888888"
"#;

        let ctx = parse_config_and_secrets(&config, &valid_secrets_toml()).unwrap();
        assert_eq!(ctx.equities.len(), 2);
        assert!(ctx.equities.contains_key(&Symbol::new("AAPL").unwrap()));
        assert!(ctx.equities.contains_key(&Symbol::new("TSLA").unwrap()));
    }

    #[test]
    fn default_log_level_is_debug() {
        let config = valid_config_toml().replace("log_level = \"debug\"\n", "");

        let ctx = parse_config_and_secrets(&config, &valid_secrets_toml()).unwrap();
        assert!(matches!(ctx.log_level, LogLevel::Debug));
    }

    #[test]
    fn alpaca_mode_optional() {
        let secrets = valid_secrets_toml().replace("mode = \"sandbox\"\n", "");

        let ctx = parse_config_and_secrets(&valid_config_toml(), &secrets).unwrap();
        assert!(ctx.alpaca.broker.mode.is_none());
    }
}
