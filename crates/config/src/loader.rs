//! Application configuration loading and validation.
//!
//! Reads plaintext config and encrypted secrets from separate TOML files,
//! validates compatibility, and assembles the runtime [`Ctx`] that the rest
//! of the application consumes.

use alloy::primitives::{Address, B256};
use clap::Parser;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode};
use st0x_execution::{
    AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, FractionalShares, Positive,
    SupportedExecutor, Symbol, TimeInForce,
};
use st0x_finance::{Usd, Usdc};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use tracing::{Level, warn};

use url::Url;

use crate::{
    AlertsConfig, AlertsCtx, AlertsSecrets, EvmConfig, EvmCtx, EvmSecrets, ExecutionThreshold,
    InvalidThresholdError, RebalancingConfig, RebalancingCtx, RebalancingCtxError, TelemetryConfig,
    TelemetryCtx,
};
use st0x_float_macro::float;

/// Alpaca minimum execution threshold: $2.
static ALPACA_MIN_DOLLARS: LazyLock<Usdc> = LazyLock::new(|| Usdc::new(float!(2)));

/// Dry-run minimum execution threshold: 1 share.
static DRY_RUN_MIN_SHARES: LazyLock<Positive<FractionalShares>> = LazyLock::new(|| {
    Positive::new(FractionalShares::new(float!(1))).unwrap_or_else(|_| unreachable!())
});
const MIN_COUNTER_TRADE_SLIPPAGE_BPS: u16 = 1;
const MAX_EXTENDED_HOURS_REPRICE_TIMEOUT_SECS: u64 =
    chrono::TimeDelta::MAX.num_seconds().unsigned_abs();
/// Slippage must be strictly less than 100%: 10_000 bps (exactly 100%) zeroes a
/// sell-side limit price and fails `Positive::new` at runtime.
///
/// NOTE: this bound only rules out the exact-100% zero. It does NOT guarantee a
/// positive sell price for every symbol: a near-100% slippage on a sub-dollar
/// reference still floors to $0.0000 and fails `Positive::new` (fail-fast, not
/// silent). Such a value is a gross misconfiguration; the bound exists to reject
/// the degenerate exact-zero case, not to validate operationally sane slippage.
const MAX_COUNTER_TRADE_SLIPPAGE_BPS: u16 = 9_999;

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
    /// Pyth price feed ID for this equity, used to record a reference price for
    /// trade enrichment via a historic `getPriceUnsafe` call.
    ///
    /// Optional: when absent, trade enrichment is skipped for this symbol
    /// (logged at debug level) but hedging is unaffected. The feed an order
    /// reads can change over time (oracle migrations), so this is the current
    /// feed per symbol and must be refreshed if the order migrates to a
    /// different feed.
    #[serde(default, deserialize_with = "deserialize_feed_id")]
    pub pyth_feed_id: Option<B256>,
    #[serde(
        default,
        alias = "vault_id",
        deserialize_with = "deserialize_vault_ids"
    )]
    pub vault_ids: Vec<B256>,
    pub trading: OperationMode,
    pub rebalancing: OperationMode,
    pub wrapped_equity_recovery: OperationMode,
    /// When enabled, counter-trades for this equity may be placed during
    /// extended (pre-/after-market) sessions as limit orders, instead of
    /// waiting for the regular open. Must be explicitly configured.
    pub extended_hours_counter_trading: OperationMode,
    pub operational_limit: Option<Positive<FractionalShares>>,
}

/// Cash asset (USDC) configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CashAssetConfig {
    #[serde(
        default,
        alias = "vault_id",
        deserialize_with = "deserialize_vault_ids"
    )]
    pub vault_ids: Vec<B256>,
    pub rebalancing: OperationMode,
    pub operational_limit: Option<Positive<Usdc>>,
    /// USD amount subtracted from offchain cash to compute available balance.
    /// Prevents the system from rebalancing funds that should remain untouched.
    pub reserved: Option<Positive<Usd>>,
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

/// Parses a hex string (possibly short, e.g. `"0xfab"`) into a
/// left-padded `B256`.
fn parse_padded_b256(hex_str: &str) -> Result<B256, String> {
    let stripped = hex_str
        .strip_prefix("0x")
        .or_else(|| hex_str.strip_prefix("0X"))
        .unwrap_or(hex_str);

    if stripped.len() > 64 {
        return Err(format!("hex string too long for B256: {stripped}"));
    }

    if stripped.is_empty() {
        return Err("empty hex string for B256".to_string());
    }

    let padded = format!("{stripped:0>64}");
    padded.parse::<B256>().map_err(|err| err.to_string())
}

/// Deserializes a Pyth feed ID from a full 32-byte hex string.
///
/// Only invoked when the key is present (the field is `#[serde(default)]`), so a
/// present-but-malformed value is rejected while an absent one yields `None`.
/// Unlike vault IDs, feed IDs are not left-padded: a feed ID is always the full
/// 32 bytes, so a short value is a configuration mistake and is rejected.
fn deserialize_feed_id<'de, D>(deserializer: D) -> Result<Option<B256>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;

    if raw.is_empty() {
        return Err(serde::de::Error::custom(
            "pyth_feed_id must not be empty; omit the field to disable enrichment",
        ));
    }

    raw.parse::<B256>()
        .map(Some)
        .map_err(|err| serde::de::Error::custom(format!("invalid pyth_feed_id `{raw}`: {err}")))
}

/// Deserializes vault IDs from either a single hex string or an array of hex
/// strings. Each value is left-padded to a full `B256`.
///
/// Accepts both `vault_id = "0xfab"` (single) and
/// `vault_ids = ["0xfab", "0xfab2"]` (multiple) in TOML.
fn deserialize_vault_ids<'de, D>(deserializer: D) -> Result<Vec<B256>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }

    let raw = OneOrMany::deserialize(deserializer)?;

    let hex_strings = match raw {
        OneOrMany::One(single) => vec![single],
        OneOrMany::Many(many) => many,
    };

    hex_strings
        .into_iter()
        .map(|hex_str| parse_padded_b256(&hex_str).map_err(serde::de::Error::custom))
        .collect()
}

/// Non-secret settings deserialized from the plaintext config TOML.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    database_url: String,
    log_level: Option<LogLevel>,
    log_dir: Option<String>,
    server_port: u16,
    board_port: u16,
    raindex: EvmConfig,
    order_polling_interval: Option<u64>,
    order_polling_max_jitter: Option<u64>,
    position_check_interval: Option<u64>,
    inventory_poll_interval: Option<u64>,
    order_fill_poll_interval: Option<u64>,
    apalis_finished_job_cleanup_interval_secs: u64,
    telemetry: Option<TelemetryConfig>,
    alerts: Option<AlertsConfig>,
    rebalancing: Option<RebalancingConfig>,
    tokenization: Option<TokenizationConfig>,
    wallet: Option<toml::Value>,
    broker: Option<BrokerConfig>,
    assets: AssetsConfig,
    rest_api: Option<RestApiUrlConfig>,
}

/// Plaintext REST API settings (URL only). Credentials live in secrets.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestApiUrlConfig {
    url: String,
}

/// Secret REST API credentials from the encrypted secrets TOML.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestApiSecrets {
    key_id: String,
    key_secret: String,
}

/// TOML shape for `[issuance]` in the encrypted secrets file. The `api_key`
/// stays a `String` at this layer so a malformed value never surfaces in a
/// `toml::de::Error` (whose `Display` echoes the offending source line).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IssuanceSecretsToml {
    base_url: Url,
    api_key: String,
}

/// Validated issuance secrets with the API key parsed into a [`B256`].
struct IssuanceSecrets {
    base_url: Url,
    api_key: B256,
}

impl IssuanceSecrets {
    fn try_from_toml(raw: IssuanceSecretsToml) -> Result<Self, IssuanceApiKeyError> {
        let api_key = raw
            .api_key
            .parse::<B256>()
            .map_err(|_| IssuanceApiKeyError::NotThirtyTwoByteHex)?;

        Ok(Self {
            base_url: raw.base_url,
            api_key,
        })
    }
}

/// Issuance internal API key: a 32-byte secret transmitted as a bare
/// 64-character lowercase hex string in the `X-API-KEY` header.
///
/// Generated with `openssl rand -hex 32`.
#[derive(Clone)]
pub struct IssuanceApiKey(B256);

impl IssuanceApiKey {
    /// The bare lowercase hex form (no `0x`) sent as the `X-API-KEY` header
    /// value, matching issuance's `openssl rand -hex 32` secret.
    ///
    /// Returns the raw secret as a `String`, which carries no redaction: never
    /// log, store, or forward the result beyond the immediate header write.
    #[must_use]
    pub fn header_value(&self) -> String {
        alloy::hex::encode(self.0)
    }
}

/// Failure parsing an issuance `api_key` into a [`B256`]. Value-free by
/// construction: the raw key must never appear in an error message, log, or
/// `validate-config` output.
#[derive(Debug, thiserror::Error)]
pub enum IssuanceApiKeyError {
    #[error("issuance api_key must be 32 bytes of hex (64 hex chars, optional 0x prefix)")]
    NotThirtyTwoByteHex,
}

impl std::fmt::Debug for IssuanceApiKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("IssuanceApiKey(<redacted>)")
    }
}

/// Runtime context for reaching issuance's internal status API, assembled from
/// secrets (`base_url`, `api_key`). The conductor constructs the typed issuance
/// client from these.
#[derive(Clone)]
pub struct IssuanceStatusCtx {
    pub base_url: Url,
    pub api_key: IssuanceApiKey,
}

impl std::fmt::Debug for IssuanceStatusCtx {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IssuanceStatusCtx")
            .field("base_url", &self.base_url.as_str())
            .field("api_key", &self.api_key)
            .finish()
    }
}

/// Combined REST API runtime context assembled from config + secrets.
/// When absent from config, features that depend on it (e.g., the Orders
/// dashboard tab) are gracefully disabled.
#[derive(Clone)]
pub struct RestApiCtx {
    pub url: String,
    pub key_id: Option<String>,
    pub key_secret: Option<String>,
    pub http_client: reqwest::Client,
}

impl std::fmt::Debug for RestApiCtx {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RestApiCtx")
            .field("url", &self.url)
            .field("key_id", &self.key_id.as_deref().map(|_| "<redacted>"))
            .field(
                "key_secret",
                &self.key_secret.as_deref().map(|_| "<redacted>"),
            )
            .field("http_client", &"reqwest::Client")
            .finish()
    }
}

impl RestApiCtx {
    fn new(
        url: String,
        key_id: Option<String>,
        key_secret: Option<String>,
    ) -> Result<Self, reqwest::Error> {
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()?;

        Ok(Self {
            url,
            key_id,
            key_secret,
            http_client,
        })
    }

    /// Creates a REST API context without authentication. Used for testing
    /// and environments where the API does not require credentials.
    #[allow(clippy::expect_used)]
    pub fn unauthenticated(url: String) -> Self {
        Self::new(url, None, None)
            .expect("reqwest client with default TLS should never fail to build")
    }
}

/// Tokenization settings for Alpaca equity mint/redeem operations.
///
/// Required when `[rebalancing]` is configured. Also usable in standalone
/// mode for CLI commands that send tokens (`alpaca-redeem`, `transfer-equity`).
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenizationConfig {
    /// Alpaca's issuer wallet — ERC-20 token transfers for redemption
    /// are sent to this address.
    redemption_wallet: Address,
}

/// Non-secret broker settings from the plaintext config TOML.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BrokerConfig {
    counter_trade_slippage_bps: Option<u16>,
    extended_hours_reprice_timeout_secs: Option<u64>,
    extended_hours_close_flatten_window_secs: Option<u64>,
    travel_rule: Option<TravelRuleConfig>,
}

/// Alpaca Travel Rule beneficiary identity, required for whitelist
/// creation, effective 2026-03-27.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TravelRuleConfig {
    pub beneficiary_entity_name: String,
}

impl TravelRuleConfig {
    /// Validates that beneficiary name fields are not blank or placeholder
    /// values, and returns a normalized copy with trimmed whitespace.
    fn validated(self) -> Result<Self, CtxError> {
        let trimmed = self.beneficiary_entity_name.trim();

        if trimmed.is_empty() {
            return Err(CtxError::InvalidTravelRule {
                field: "beneficiary_entity_name",
                reason: "must not be blank",
            });
        }

        if trimmed.eq_ignore_ascii_case("PLACEHOLDER") {
            return Err(CtxError::InvalidTravelRule {
                field: "beneficiary_entity_name",
                reason: "must be set to a real value, not a placeholder",
            });
        }

        Ok(Self {
            beneficiary_entity_name: trimmed.to_owned(),
        })
    }
}

impl BrokerConfig {
    fn counter_trade_slippage_bps(&self) -> Result<u16, CtxError> {
        let configured = self
            .counter_trade_slippage_bps
            .ok_or(CtxError::MissingCounterTradeSlippageBps)?;

        if !(MIN_COUNTER_TRADE_SLIPPAGE_BPS..=MAX_COUNTER_TRADE_SLIPPAGE_BPS).contains(&configured)
        {
            return Err(CtxError::CounterTradeSlippageBpsOutOfRange {
                configured,
                min: MIN_COUNTER_TRADE_SLIPPAGE_BPS,
                max: MAX_COUNTER_TRADE_SLIPPAGE_BPS,
            });
        }

        Ok(configured)
    }

    fn extended_hours_reprice_timeout_secs(&self) -> Result<u64, CtxError> {
        let configured = self
            .extended_hours_reprice_timeout_secs
            .ok_or(CtxError::MissingExtendedHoursRepriceTimeout)?;

        if configured == 0 {
            return Err(CtxError::ZeroPollingInterval {
                field: "broker.extended_hours_reprice_timeout_secs",
            });
        }

        if configured > MAX_EXTENDED_HOURS_REPRICE_TIMEOUT_SECS {
            return Err(CtxError::ExtendedHoursRepriceTimeoutOutOfRange {
                configured,
                max: MAX_EXTENDED_HOURS_REPRICE_TIMEOUT_SECS,
            });
        }

        Ok(configured)
    }

    fn extended_hours_close_flatten_window_secs(&self) -> Result<u64, CtxError> {
        let configured = self
            .extended_hours_close_flatten_window_secs
            .ok_or(CtxError::MissingExtendedHoursCloseFlattenWindow)?;

        if configured == 0 {
            return Err(CtxError::ZeroPollingInterval {
                field: "broker.extended_hours_close_flatten_window_secs",
            });
        }

        Ok(configured)
    }
}

impl std::fmt::Debug for TravelRuleConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TravelRuleConfig")
            .field("beneficiary_entity_name", &"<redacted>")
            .finish()
    }
}

/// Secret credentials deserialized from the encrypted secrets TOML.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Secrets {
    evm: EvmSecrets,
    broker: BrokerSecrets,
    alerts: Option<AlertsSecrets>,
    wallet: Option<toml::Value>,
    rest_api: Option<RestApiSecrets>,
    issuance: Option<IssuanceSecretsToml>,
}

/// Broker type tag and all broker credentials.
/// Deserialized from the `[broker]` section of the secrets TOML.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
#[allow(clippy::large_enum_variant)] // isn't relevant for a brief startup step
enum BrokerSecrets {
    AlpacaBrokerApi {
        api_key: String,
        api_secret: String,
        account_id: AlpacaAccountId,
        mode: Option<AlpacaBrokerApiMode>,
    },
    DryRun,
}

/// Encodes the two operating modes at the type level.
///
/// `Standalone`: hedging only, no automatic rebalancing.
/// `Rebalancing`: hedging + automatic inventory rebalancing.
///
/// In both modes, `order_owner` is derived from `[wallet].address`.
#[derive(Clone, Debug)]
pub enum TradingMode {
    Standalone,
    Rebalancing(Box<RebalancingCtx>),
}

/// Combined runtime context for the server. Assembled from plaintext config,
/// encrypted secrets, and derived runtime state.
#[derive(Clone)]
pub struct Ctx {
    pub database_url: String,
    pub log_level: LogLevel,
    pub log_dir: Option<String>,
    pub server_port: u16,
    pub board_port: u16,
    pub evm: EvmCtx,
    pub order_polling_interval: u64,
    pub order_polling_max_jitter: u64,
    pub position_check_interval: u64,
    pub inventory_poll_interval: u64,
    /// Interval (seconds) between continuous `eth_getLogs` polls for orderbook
    /// fills. Each tick enqueues a backfill range over the unprocessed blocks
    /// (capped at the chain's latest finalized block).
    pub order_fill_poll_interval: u64,
    /// Maximum age (seconds) for a live extended-hours limit hedge before it is
    /// cancelled so the next scan can place a fresh marketable limit.
    pub extended_hours_reprice_timeout_secs: u64,
    /// Window (seconds) before a long-gap extended-session close during which
    /// the bot repeatedly cancels, refreshes, and replaces executable residual
    /// exposure with quote-crossing limits.
    pub extended_hours_close_flatten_window_secs: u64,
    pub apalis_finished_job_cleanup_interval_secs: u64,
    pub broker: BrokerCtx,
    pub telemetry: Option<TelemetryCtx>,
    /// Optional gas-balance alerting context. `Some` when both `[alerts]`
    /// config and the Telegram `bot_token` secret are present.
    pub alerts: Option<AlertsCtx>,
    pub trading_mode: TradingMode,
    /// The onchain address that owns orders on the orderbook.
    /// Always derived from the configured `[wallet]` address.
    pub order_owner: Address,
    pub wallet: Option<crate::wallet::OnchainWalletCtx>,
    /// Non-secret wallet metadata for the dashboard config dialog.
    pub wallet_meta: Option<WalletMeta>,
    pub execution_threshold: ExecutionThreshold,
    pub assets: AssetsConfig,
    pub travel_rule: Option<TravelRuleConfig>,
    pub rest_api: Option<RestApiCtx>,
    pub issuance: IssuanceStatusCtx,
    /// Alpaca redemption wallet from `[tokenization]`.
    /// `Some` when the config includes a `[tokenization]` section.
    pub redemption_wallet: Option<Address>,
}

/// Runtime broker configuration assembled from `BrokerSecrets`.
#[derive(Clone)]
pub enum BrokerCtx {
    AlpacaBrokerApi(AlpacaBrokerApiCtx),
    DryRun,
}

impl BrokerCtx {
    pub fn to_supported_executor(&self) -> SupportedExecutor {
        match self {
            Self::AlpacaBrokerApi(_) => SupportedExecutor::AlpacaBrokerApi,
            Self::DryRun => SupportedExecutor::DryRun,
        }
    }

    fn execution_threshold(&self) -> Result<ExecutionThreshold, CtxError> {
        match self {
            Self::AlpacaBrokerApi(_) => Ok(ExecutionThreshold::dollar_value(*ALPACA_MIN_DOLLARS)?),
            Self::DryRun => Ok(ExecutionThreshold::shares(*DRY_RUN_MIN_SHARES)),
        }
    }
}

impl BrokerCtx {
    fn from_parts(
        secrets: BrokerSecrets,
        broker_config: Option<&BrokerConfig>,
    ) -> Result<Self, CtxError> {
        match secrets {
            BrokerSecrets::AlpacaBrokerApi {
                api_key,
                api_secret,
                account_id,
                mode,
            } => Ok(Self::AlpacaBrokerApi(AlpacaBrokerApiCtx {
                api_key,
                api_secret,
                account_id,
                mode,
                asset_cache_ttl: std::time::Duration::from_secs(3600),
                time_in_force: TimeInForce::default(),
                counter_trade_slippage_bps: broker_config
                    .ok_or(CtxError::MissingCounterTradeSlippageBps)?
                    .counter_trade_slippage_bps()?,
            })),

            BrokerSecrets::DryRun => Ok(Self::DryRun),
        }
    }
}

impl std::fmt::Debug for BrokerCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlpacaBrokerApi(ctx) => f.debug_tuple("AlpacaBrokerApi").field(ctx).finish(),
            Self::DryRun => write!(f, "DryRun"),
        }
    }
}

impl std::fmt::Debug for Ctx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("Ctx");
        debug_struct
            .field("database_url", &self.database_url)
            .field("log_level", &self.log_level)
            .field("log_dir", &self.log_dir)
            .field("server_port", &self.server_port)
            .field("board_port", &self.board_port)
            .field("evm", &self.evm)
            .field("order_polling_interval", &self.order_polling_interval)
            .field("order_polling_max_jitter", &self.order_polling_max_jitter)
            .field("position_check_interval", &self.position_check_interval)
            .field("inventory_poll_interval", &self.inventory_poll_interval)
            .field("order_fill_poll_interval", &self.order_fill_poll_interval)
            .field(
                "extended_hours_reprice_timeout_secs",
                &self.extended_hours_reprice_timeout_secs,
            )
            .field(
                "extended_hours_close_flatten_window_secs",
                &self.extended_hours_close_flatten_window_secs,
            )
            .field(
                "apalis_finished_job_cleanup_interval_secs",
                &self.apalis_finished_job_cleanup_interval_secs,
            )
            .field("broker", &self.broker)
            .field("telemetry", &self.telemetry)
            .field("alerts", &self.alerts)
            .field("trading_mode", &self.trading_mode)
            .field("order_owner", &self.order_owner)
            .field("wallet_configured", &self.wallet.is_some())
            .field("wallet_meta", &self.wallet_meta)
            .field("execution_threshold", &self.execution_threshold)
            .field("assets", &self.assets)
            .field("travel_rule_configured", &self.travel_rule.is_some())
            .field("redemption_wallet", &self.redemption_wallet)
            .field("rest_api", &self.rest_api)
            .field("issuance", &self.issuance);

        debug_struct.finish()
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

/// Intermediate result from [`parse_and_validate`]. Contains everything
/// needed to construct [`Ctx`] after async wallet initialization.
struct ValidatedParts {
    database_url: String,
    log_level: LogLevel,
    log_dir: Option<String>,
    server_port: u16,
    board_port: u16,
    evm: EvmCtx,
    order_polling_interval: u64,
    order_polling_max_jitter: u64,
    position_check_interval: u64,
    inventory_poll_interval: u64,
    order_fill_poll_interval: u64,
    extended_hours_reprice_timeout_secs: u64,
    extended_hours_close_flatten_window_secs: u64,
    apalis_finished_job_cleanup_interval_secs: u64,
    broker: BrokerCtx,
    telemetry: Option<TelemetryCtx>,
    alerts: Option<AlertsCtx>,
    execution_threshold: ExecutionThreshold,
    trading_mode: TradingMode,
    assets: AssetsConfig,
    travel_rule: Option<TravelRuleConfig>,
    rest_api: Option<RestApiCtx>,
    issuance: IssuanceStatusCtx,
    redemption_wallet: Option<Address>,
    /// Wallet construction inputs. Always present — `parse_and_validate`
    /// returns `WalletNotConfigured` when both config and secrets lack
    /// a `[wallet]` section. Actual async wallet construction is deferred
    /// to `load_files`.
    wallet_inputs: WalletInputs,
    /// Non-secret wallet metadata for the dashboard config dialog.
    wallet_meta: WalletMeta,
}

struct WalletInputs {
    config: toml::Value,
    secrets: toml::Value,
    base_rpc_url: Url,
    ethereum_rpc_url: Url,
}

/// Non-secret wallet metadata extracted from the config TOML during
/// parsing. Displayed on the dashboard config dialog.
#[derive(Clone, Debug, Deserialize)]
pub struct WalletMeta {
    pub kind: String,
    pub address: Address,
    pub organization_id: Option<String>,
}

/// Single validation path shared by [`Ctx::load_files`] and
/// [`Ctx::validate_files`]. All config/secrets business-rule checks live
/// here — neither caller duplicates validation logic.
fn parse_and_validate(
    config_str: &str,
    config_path: &Path,
    secrets_str: &str,
    secrets_path: &Path,
) -> Result<ValidatedParts, CtxError> {
    let config: Config = toml::from_str(config_str).map_err(|source| CtxError::ConfigToml {
        path: config_path.to_path_buf(),
        source,
    })?;
    let mut secrets: Secrets =
        toml::from_str(secrets_str).map_err(|source| CtxError::SecretsToml {
            path: secrets_path.to_path_buf(),
            source,
        })?;

    if config.server_port == config.board_port {
        return Err(CtxError::ServerAndBoardPortsMatch {
            port: config.server_port,
        });
    }

    // Extended-hours counter-trading depends on counter-trading itself: the
    // hedge path gates on `trading` before it ever consults the extended-hours
    // session (see `check_execution_readiness`), so
    // `extended_hours_counter_trading = enabled` with `trading = disabled` is a
    // dead configuration that can never execute. Reject it so the invalid
    // combination never loads and never reaches the dashboard.
    for (symbol, equity) in &config.assets.equities.symbols {
        if equity.extended_hours_counter_trading == OperationMode::Enabled
            && equity.trading == OperationMode::Disabled
        {
            return Err(CtxError::ExtendedHoursWithoutCounterTrading {
                symbol: symbol.clone(),
            });
        }
    }

    let broker = BrokerCtx::from_parts(secrets.broker, config.broker.as_ref())?;
    let telemetry = config.telemetry.map(TelemetryCtx::from);
    let alerts = AlertsCtx::new(config.alerts, secrets.alerts)?;

    // Execution threshold is determined by broker capabilities:
    // - Alpaca requires $1 minimum for fractional trading. We use $2 to provide buffer
    //   for slippage, fees, and price discrepancies that could push fills below $1.
    // - DryRun uses shares threshold for testing
    let execution_threshold = broker.execution_threshold()?;

    // Extract RPC URLs before EvmCtx consumes secrets.evm.
    let base_rpc_url = secrets.evm.base.take();
    let ethereum_rpc_url = secrets.evm.ethereum.take();

    let evm = EvmCtx::new(&config.raindex, secrets.evm)?;

    // Validate wallet config/secrets pairing and required RPC URLs.
    // Actual wallet construction (async, connects to RPC) is deferred.
    let (wallet_inputs, wallet_meta) = match (config.wallet, secrets.wallet) {
        (Some(wallet_config), Some(wallet_secrets)) => {
            let Some(base_url) = base_rpc_url else {
                return Err(CtxError::WalletMissingRpcUrl {
                    field: "base_rpc_url",
                });
            };

            let Some(eth_url) = ethereum_rpc_url else {
                return Err(CtxError::WalletMissingRpcUrl {
                    field: "ethereum_rpc_url",
                });
            };

            let wallet_meta = WalletMeta::deserialize(wallet_config.clone()).map_err(|source| {
                CtxError::ConfigToml {
                    path: config_path.to_path_buf(),
                    source,
                }
            })?;

            (
                WalletInputs {
                    config: wallet_config,
                    secrets: wallet_secrets,
                    base_rpc_url: base_url,
                    ethereum_rpc_url: eth_url,
                },
                wallet_meta,
            )
        }
        (Some(_), None) => return Err(CtxError::WalletSecretsMissing),
        (None, Some(_)) => {
            // Wallet secrets present but no [wallet] in config.
            // Common when sharing one secrets file across bot + CLI
            // where the bot config doesn't need a wallet.
            warn!(
                target: "startup",
                "[wallet] secrets present but no [wallet] config section -- \
                 wallet signing will not be available"
            );
            return Err(CtxError::WalletNotConfigured);
        }
        (None, None) => return Err(CtxError::WalletNotConfigured),
    };

    let trading_mode = match config.rebalancing {
        Some(rebalancing_config) => {
            let BrokerCtx::AlpacaBrokerApi(_) = &broker else {
                return Err(RebalancingCtxError::NotAlpacaBroker.into());
            };

            let minimum = *crate::ALPACA_MINIMUM_WITHDRAWAL;

            if let Some(cash) = &config.assets.cash
                && cash.rebalancing == OperationMode::Enabled
                && let Some(cash_limit) = &cash.operational_limit
            {
                let below_minimum = cash_limit.inner().lt(&minimum)?;

                if below_minimum {
                    return Err(CtxError::CashOperationalLimitBelowMinimumWithdrawal {
                        configured: cash_limit.inner(),
                        minimum,
                    });
                }
            }

            TradingMode::Rebalancing(Box::new(RebalancingCtx::new(&rebalancing_config)?))
        }
        None => TradingMode::Standalone,
    };

    let redemption_wallet = config.tokenization.map(|t| t.redemption_wallet);

    if matches!(trading_mode, TradingMode::Rebalancing(_)) && redemption_wallet.is_none() {
        return Err(CtxError::MissingTokenization);
    }

    let log_level = config.log_level.unwrap_or(LogLevel::Debug);

    let order_polling_interval = config.order_polling_interval.unwrap_or(15);
    if order_polling_interval == 0 {
        return Err(CtxError::ZeroPollingInterval {
            field: "order_polling_interval",
        });
    }

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

    let order_fill_poll_interval = config.order_fill_poll_interval.unwrap_or(5);
    if order_fill_poll_interval == 0 {
        return Err(CtxError::ZeroPollingInterval {
            field: "order_fill_poll_interval",
        });
    }

    let ExtendedHoursBrokerWindows {
        reprice_timeout_secs: extended_hours_reprice_timeout_secs,
        close_flatten_window_secs: extended_hours_close_flatten_window_secs,
    } = extended_hours_broker_windows(&broker, config.broker.as_ref())?;

    let apalis_finished_job_cleanup_interval_secs =
        config.apalis_finished_job_cleanup_interval_secs;
    if apalis_finished_job_cleanup_interval_secs == 0 {
        return Err(CtxError::ZeroPollingInterval {
            field: "apalis_finished_job_cleanup_interval_secs",
        });
    }

    let travel_rule = config
        .broker
        .as_ref()
        .and_then(|broker_config| broker_config.travel_rule.as_ref());

    let broker_requires_travel_rule = match &broker {
        BrokerCtx::AlpacaBrokerApi(_) => true,
        BrokerCtx::DryRun => false,
    };

    if broker_requires_travel_rule && travel_rule.is_none() {
        return Err(CtxError::MissingTravelRule);
    }

    let travel_rule = config
        .broker
        .and_then(|broker_config| broker_config.travel_rule)
        .map(TravelRuleConfig::validated)
        .transpose()?;

    Ok(ValidatedParts {
        database_url: config.database_url,
        log_level,
        log_dir: config.log_dir,
        server_port: config.server_port,
        board_port: config.board_port,
        evm,
        order_polling_interval,
        order_polling_max_jitter: config.order_polling_max_jitter.unwrap_or(5),
        position_check_interval,
        inventory_poll_interval,
        order_fill_poll_interval,
        extended_hours_reprice_timeout_secs,
        extended_hours_close_flatten_window_secs,
        apalis_finished_job_cleanup_interval_secs,
        broker,
        telemetry,
        alerts,
        execution_threshold,
        trading_mode,
        assets: config.assets,
        travel_rule,
        rest_api: config
            .rest_api
            .map(|cfg| {
                if secrets.rest_api.is_none() {
                    warn!(
                        target: "startup",
                        "[rest_api] URL configured but no [rest_api] credentials in secrets -- \
                         requests will be unauthenticated"
                    );
                }

                let key_id = secrets.rest_api.as_ref().map(|s| s.key_id.clone());
                let key_secret = secrets.rest_api.map(|s| s.key_secret);
                RestApiCtx::new(cfg.url, key_id, key_secret).map_err(CtxError::RestApiClient)
            })
            .transpose()?,
        issuance: issuance_ctx(
            secrets
                .issuance
                .map(IssuanceSecrets::try_from_toml)
                .transpose()
                .map_err(|source| CtxError::InvalidIssuanceApiKey { source })?,
        )?,
        redemption_wallet,
        wallet_inputs,
        wallet_meta,
    })
}

/// Result of [`extended_hours_broker_windows`]. Both fields are `u64`
/// seconds with distinct meanings -- a named struct (rather than a
/// positional tuple) prevents a future reorder at either the construction or
/// destructuring site from silently swapping which duration feeds which
/// `Ctx` field.
struct ExtendedHoursBrokerWindows {
    reprice_timeout_secs: u64,
    close_flatten_window_secs: u64,
}

fn extended_hours_broker_windows(
    broker: &BrokerCtx,
    broker_config: Option<&BrokerConfig>,
) -> Result<ExtendedHoursBrokerWindows, CtxError> {
    match broker {
        BrokerCtx::AlpacaBrokerApi(_) => {
            let broker_config =
                broker_config.ok_or(CtxError::MissingExtendedHoursRepriceTimeout)?;

            Ok(ExtendedHoursBrokerWindows {
                reprice_timeout_secs: broker_config.extended_hours_reprice_timeout_secs()?,
                close_flatten_window_secs: broker_config
                    .extended_hours_close_flatten_window_secs()?,
            })
        }
        BrokerCtx::DryRun => Ok(ExtendedHoursBrokerWindows {
            reprice_timeout_secs: 0,
            close_flatten_window_secs: 0,
        }),
    }
}

/// Assembles the required issuance status context from secrets.
/// `[issuance]` is mandatory: `base_url` and `api_key` must both be present
/// and the URL must parse -- no silent fallbacks for an endpoint the
/// rebalancing freeze guard depends on.
fn issuance_ctx(secret: Option<IssuanceSecrets>) -> Result<IssuanceStatusCtx, CtxError> {
    let Some(secret) = secret else {
        return Err(CtxError::MissingIssuanceConfig);
    };

    Ok(IssuanceStatusCtx {
        base_url: secret.base_url,
        api_key: IssuanceApiKey(secret.api_key),
    })
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

        let parts = parse_and_validate(&config_str, config_path, &secrets_str, secrets_path)?;

        // Async wallet construction — the only step that requires network
        // access and cannot run in the deploy-time validator.
        let wallet = crate::wallet::OnchainWalletCtx::new(
            parts.wallet_inputs.config,
            parts.wallet_inputs.secrets,
            parts.wallet_inputs.base_rpc_url,
            parts.wallet_inputs.ethereum_rpc_url,
        )
        .await?;

        let order_owner = wallet.base_wallet().address();

        Ok(Self {
            database_url: parts.database_url,
            log_level: parts.log_level,
            log_dir: parts.log_dir,
            server_port: parts.server_port,
            board_port: parts.board_port,
            evm: parts.evm,
            order_polling_interval: parts.order_polling_interval,
            order_polling_max_jitter: parts.order_polling_max_jitter,
            position_check_interval: parts.position_check_interval,
            inventory_poll_interval: parts.inventory_poll_interval,
            order_fill_poll_interval: parts.order_fill_poll_interval,
            extended_hours_reprice_timeout_secs: parts.extended_hours_reprice_timeout_secs,
            extended_hours_close_flatten_window_secs: parts
                .extended_hours_close_flatten_window_secs,
            apalis_finished_job_cleanup_interval_secs: parts
                .apalis_finished_job_cleanup_interval_secs,
            broker: parts.broker,
            telemetry: parts.telemetry,
            alerts: parts.alerts,
            trading_mode: parts.trading_mode,
            order_owner,
            wallet: Some(wallet),
            wallet_meta: Some(parts.wallet_meta),
            execution_threshold: parts.execution_threshold,
            assets: parts.assets,
            travel_rule: parts.travel_rule,
            rest_api: parts.rest_api,
            issuance: parts.issuance,
            redemption_wallet: parts.redemption_wallet,
        })
    }

    /// Validates config and secrets files without constructing runtime objects.
    ///
    /// Calls the same [`parse_and_validate`] function as [`load_files`](Self::load_files),
    /// ensuring identical validation. The only difference is that `load_files`
    /// additionally performs async wallet construction (which connects to RPC
    /// endpoints). Suitable for pre-deploy validation where we want to catch
    /// config errors before restarting the service.
    pub fn validate_files(config_path: &Path, secrets_path: &Path) -> Result<(), CtxError> {
        let config_str =
            std::fs::read_to_string(config_path).map_err(|source| CtxError::ConfigIo {
                path: config_path.to_path_buf(),
                source,
            })?;
        let secrets_str =
            std::fs::read_to_string(secrets_path).map_err(|source| CtxError::SecretsIo {
                path: secrets_path.to_path_buf(),
                source,
            })?;
        parse_and_validate(&config_str, config_path, &secrets_str, secrets_path)?;
        Ok(())
    }

    pub async fn get_sqlite_pool(&self) -> Result<SqlitePool, sqlx::Error> {
        configure_sqlite_pool(&self.database_url).await
    }

    pub fn rebalancing_ctx(&self) -> Result<&RebalancingCtx, CtxError> {
        match &self.trading_mode {
            TradingMode::Rebalancing(ctx) => Ok(ctx),
            TradingMode::Standalone => Err(CtxError::NotRebalancing),
        }
    }

    pub fn wallet(&self) -> Result<&crate::wallet::OnchainWalletCtx, CtxError> {
        self.wallet.as_ref().ok_or(CtxError::WalletNotConfigured)
    }

    /// Returns the redemption wallet from the `[tokenization]` config section.
    pub fn redemption_wallet(&self) -> Result<Address, CtxError> {
        self.redemption_wallet.ok_or(CtxError::MissingTokenization)
    }

    pub const fn order_polling_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.order_polling_interval)
    }

    /// Returns the bot's signing wallet address (the `[wallet]` EOA).
    ///
    /// This is the transaction signer / gas payer, the `spender`-granting
    /// account for ERC20 approvals, and the `operator` that appears in
    /// `RaindexInventory.Operator{Deposit,Withdraw}` events (and the `sender`
    /// in pre-migration `WithdrawV2` events). It is NOT necessarily the address
    /// that owns the Raindex vaults -- see [`Self::vault_owner`].
    ///
    /// Named `order_owner` for historical reasons: before the shared-inventory
    /// migration the signing wallet also owned the orders/vaults, so the two
    /// concepts coincided.
    pub fn order_owner(&self) -> Address {
        self.order_owner
    }

    /// Returns the address that owns the Raindex orders and vaults on-chain.
    ///
    /// Every `vaultBalance2` read, vault-registry entry, and ClearV3/TakeOrderV3
    /// order-owner fill match is scoped by this address. Sourced from the
    /// required `[raindex].vault_owner` config field -- the signing wallet while
    /// the vaults are bot-EOA-owned, flipped to the inventory address when the
    /// shared-inventory migration makes the inventory contract `msg.sender` to
    /// Raindex (and therefore the vault owner).
    pub fn vault_owner(&self) -> Address {
        self.evm.vault_owner
    }

    /// Returns the configured Pyth feed IDs keyed by base equity symbol.
    ///
    /// Built from the `pyth_feed_id` field of each `[assets.equities.*]` entry.
    /// Used to record a reference price for trade enrichment.
    pub fn pyth_feed_ids(&self) -> HashMap<Symbol, B256> {
        self.assets
            .equities
            .symbols
            .iter()
            .filter_map(|(symbol, config)| {
                config.pyth_feed_id.map(|feed_id| (symbol.clone(), feed_id))
            })
            .collect()
    }
}

/// Per-symbol equity config guards. All six live on `AssetsConfig` (the owner
/// of the `[assets.equities]` map) so callers use one convention --
/// `assets.X(symbol)` or `ctx.assets.X(symbol)` -- rather than mixing
/// `ctx.X(symbol)` with `ctx.assets.X(symbol)`. Code that holds only an
/// `&AssetsConfig` (e.g. the accumulator) can reach every guard without a `Ctx`.
impl AssetsConfig {
    /// Returns whether trading is enabled for the given equity.
    ///
    /// Fail-closed: assets not present in the config are treated as
    /// trading-disabled.
    pub fn is_trading_enabled(&self, symbol: &Symbol) -> bool {
        self.equities
            .symbols
            .get(symbol)
            .is_some_and(|config| config.trading == OperationMode::Enabled)
    }

    /// Returns whether rebalancing is enabled for the given equity.
    ///
    /// Assets not present in the config are treated as rebalancing-disabled
    /// by default.
    pub fn is_rebalancing_enabled(&self, symbol: &Symbol) -> bool {
        self.equities
            .symbols
            .get(symbol)
            .is_some_and(|config| config.rebalancing == OperationMode::Enabled)
    }

    /// Returns whether wrapped/unwrapped wallet equity recovery is enabled
    /// for the given equity.
    ///
    /// Independent of `rebalancing`: a symbol may opt into recovery while
    /// keeping automatic rebalancing disabled. Assets not present in the
    /// config are treated as recovery-disabled by default.
    pub fn is_wrapped_equity_recovery_enabled(&self, symbol: &Symbol) -> bool {
        self.equities
            .symbols
            .get(symbol)
            .is_some_and(|config| config.wrapped_equity_recovery == OperationMode::Enabled)
    }

    /// Returns the configured tokenized-equity (minted onchain token) address
    /// for `symbol`, or `None` when the symbol is absent from
    /// `[assets.equities]`. Lets operator commands resolve the token from the
    /// ticker instead of taking an error-prone address argument.
    pub fn tokenized_equity(&self, symbol: &Symbol) -> Option<Address> {
        self.equities
            .symbols
            .get(symbol)
            .map(|config| config.tokenized_equity)
    }

    /// Returns whether extended-hours counter-trading is enabled for the
    /// given equity.
    ///
    /// Fail-closed: assets not present in the config are treated as
    /// disabled.
    pub fn is_extended_hours_enabled(&self, symbol: &Symbol) -> bool {
        self.equities
            .symbols
            .get(symbol)
            .is_some_and(|config| config.extended_hours_counter_trading == OperationMode::Enabled)
    }

    /// Returns whether any configured equity enables extended-hours
    /// counter-trading.
    pub fn any_extended_hours_enabled(&self) -> bool {
        self.equities
            .symbols
            .values()
            .any(|config| config.extended_hours_counter_trading == OperationMode::Enabled)
    }
}

#[cfg(any(test, feature = "test-support"))]
use crate::{IngestionCutoff, InventoryMode};

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
        rpc_url: Url,
        orderbook: Address,
        deployment_block: u64,
        #[builder(default = 0)] required_confirmations: u64,
        broker: BrokerCtx,
        trading_mode: TradingMode,
        order_owner: Address,
        wallet: Option<crate::wallet::OnchainWalletCtx>,
        /// Rebalancing settlement mode. Defaults to `Legacy` (bot-EOA-owned
        /// vaults settling directly against the orderbook), matching every
        /// e2e test that predates the shared-inventory migration. Tests that
        /// need a distinct `RaindexInventory` (e.g. InventoryTrade fills from
        /// a venue adapter) pass `Managed { inventory }` explicitly; the
        /// vault owner then becomes the inventory address (mirroring
        /// `crate::onchain::raindex_contracts`'s production wiring) instead
        /// of `order_owner`.
        #[builder(default = InventoryMode::Legacy)]
        inventory_mode: InventoryMode,
        assets: AssetsConfig,
        #[builder(default = 2)] inventory_poll_interval: u64,
        #[builder(default = 3600)] apalis_finished_job_cleanup_interval_secs: u64,
        #[builder(default = 0)] server_port: u16,
        #[builder(default = 0)] board_port: u16,
        execution_threshold_override: Option<ExecutionThreshold>,
        travel_rule: Option<TravelRuleConfig>,
        rest_api: Option<RestApiCtx>,
        #[builder(default = create_test_issuance_ctx())] issuance: IssuanceStatusCtx,
        redemption_wallet: Option<Address>,
    ) -> Result<Self, CtxError> {
        let execution_threshold = match execution_threshold_override {
            Some(threshold) => threshold,
            None => broker.execution_threshold()?,
        };

        if matches!(trading_mode, TradingMode::Rebalancing(_)) && wallet.is_none() {
            return Err(CtxError::WalletNotConfigured);
        }

        if matches!(trading_mode, TradingMode::Rebalancing(_)) && redemption_wallet.is_none() {
            return Err(CtxError::MissingTokenization);
        }

        // Legacy: tests simulate the pre-migration state where the bot owns
        // the vaults and settles on the orderbook, so the startup
        // OPERATOR_ROLE preflight is skipped (there is no distinct inventory
        // contract) and the vault owner is the bot's own order-owner address.
        // Managed: a distinct RaindexInventory owns the vaults (production
        // wiring in `crate::onchain::raindex_contracts`), so the vault owner
        // becomes the inventory address instead.
        let vault_owner = match inventory_mode {
            InventoryMode::Legacy => order_owner,
            InventoryMode::Managed { inventory } => inventory,
        };

        Ok(Self {
            database_url,
            log_level: LogLevel::Debug,
            log_dir: None,
            server_port,
            board_port,
            evm: EvmCtx {
                rpc_url,
                orderbook,
                inventory: inventory_mode,
                vault_owner,
                deployment_block,
                required_confirmations,
                ingestion_cutoff: IngestionCutoff::Safe,
            },
            order_polling_interval: 1,
            order_polling_max_jitter: 0,
            position_check_interval: 2,
            inventory_poll_interval,
            order_fill_poll_interval: 1,
            extended_hours_reprice_timeout_secs: 300,
            extended_hours_close_flatten_window_secs: 900,
            apalis_finished_job_cleanup_interval_secs,
            broker,
            telemetry: None,
            alerts: None,
            trading_mode,
            order_owner,
            wallet,
            wallet_meta: None,
            execution_threshold,
            assets,
            travel_rule,
            rest_api,
            issuance,
            redemption_wallet,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CtxError {
    #[error(transparent)]
    Rebalancing(Box<RebalancingCtxError>),
    #[error("failed to build REST API HTTP client")]
    RestApiClient(#[source] reqwest::Error),
    #[error("[issuance] section is required in secrets but was not configured")]
    MissingIssuanceConfig,
    #[error("[issuance] api_key is invalid")]
    InvalidIssuanceApiKey {
        #[source]
        source: IssuanceApiKeyError,
    },
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
    #[error("invalid travel rule config: {field} {reason}")]
    InvalidTravelRule {
        field: &'static str,
        reason: &'static str,
    },
    #[error(
        "[broker] counter_trade_slippage_bps is required when using Alpaca \
         Trading API or Alpaca Broker API"
    )]
    MissingCounterTradeSlippageBps,
    #[error(
        "[broker] extended_hours_reprice_timeout_secs is required when using \
         Alpaca Broker API"
    )]
    MissingExtendedHoursRepriceTimeout,
    #[error(
        "[broker] extended_hours_reprice_timeout_secs {configured} is out of range; \
         expected 1..={max}"
    )]
    ExtendedHoursRepriceTimeoutOutOfRange { configured: u64, max: u64 },
    #[error(
        "[broker] extended_hours_close_flatten_window_secs is required when \
         using Alpaca Broker API"
    )]
    MissingExtendedHoursCloseFlattenWindow,
    #[error(
        "[broker] counter_trade_slippage_bps {configured} is out of range; \
         expected {min}..={max}"
    )]
    CounterTradeSlippageBpsOutOfRange { configured: u16, min: u16, max: u16 },
    #[error(transparent)]
    Alerts(#[from] crate::alerts::AlertsAssemblyError),
    #[error(transparent)]
    Evm(#[from] crate::evm::EvmConfigError),
    #[error("operation requires rebalancing mode")]
    NotRebalancing,
    #[error(
        "operation requires [tokenization] config section \
         with redemption_wallet"
    )]
    MissingTokenization,
    #[error(
        "operation requires a configured [wallet] section \
         (base_rpc_url and ethereum_rpc_url in [evm] secrets)"
    )]
    WalletNotConfigured,
    #[error(transparent)]
    Wallet(#[from] crate::wallet::WalletCtxError),
    #[error("[evm] {field} is required when [wallet] is configured")]
    WalletMissingRpcUrl { field: &'static str },
    #[error("[wallet] config present but [wallet] secrets missing")]
    WalletSecretsMissing,
    #[error(
        "assets.cash operational_limit {configured} is below Alpaca's \
         minimum withdrawal of {minimum}"
    )]
    CashOperationalLimitBelowMinimumWithdrawal { configured: Usdc, minimum: Usdc },
    #[error(
        "assets.cash.vault_ids is required for rebalancing \
         but not configured"
    )]
    MissingCashVaultId,
    #[error(
        "assets.equities.{symbol}.vault_ids is required when \
         rebalancing is enabled but not configured"
    )]
    MissingEquityVaultId { symbol: Symbol },
    #[error(
        "assets.equities.{symbol}: extended_hours_counter_trading cannot be \
         enabled when trading is disabled -- extended-hours counter-trades only \
         run while counter-trading is enabled, so this combination can never \
         execute"
    )]
    ExtendedHoursWithoutCounterTrading { symbol: Symbol },
    #[error("{field} polling interval must be non-zero")]
    ZeroPollingInterval { field: &'static str },
    #[error("server_port and board_port must differ; both set to {port}")]
    ServerAndBoardPortsMatch { port: u16 },
    #[error(
        "[broker.travel_rule] is required when using Alpaca Broker API \
         -- Alpaca rejects whitelist requests without it since 2026-03-27"
    )]
    MissingTravelRule,
    #[error("Float comparison failed during config validation: {0}")]
    FloatComparison(#[from] rain_math_float::FloatError),
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
            Self::MissingTokenization => "operation requires tokenization config",
            Self::ConfigIo { .. } => "failed to read config file",
            Self::SecretsIo { .. } => "failed to read secrets file",
            Self::ConfigToml { .. } => "failed to parse config",
            Self::SecretsToml { .. } => "failed to parse secrets",
            Self::InvalidThreshold(_) => "invalid execution threshold",
            Self::MissingCounterTradeSlippageBps => "missing counter trade slippage bps",
            Self::MissingExtendedHoursRepriceTimeout => "missing extended hours reprice timeout",
            Self::ExtendedHoursRepriceTimeoutOutOfRange { .. } => {
                "extended hours reprice timeout out of range"
            }
            Self::MissingExtendedHoursCloseFlattenWindow => {
                "missing extended hours close flatten window"
            }
            Self::CounterTradeSlippageBpsOutOfRange { .. } => {
                "counter trade slippage bps out of range"
            }
            Self::Alerts(_) => "alerts assembly error",
            Self::Evm(_) => "evm configuration error",
            Self::CashOperationalLimitBelowMinimumWithdrawal { .. } => {
                "cash operational limit below minimum withdrawal"
            }
            Self::MissingCashVaultId => "missing cash vault_ids",
            Self::MissingEquityVaultId { .. } => "missing equity vault_ids",
            Self::ExtendedHoursWithoutCounterTrading { .. } => {
                "extended hours enabled without counter-trading"
            }
            Self::ZeroPollingInterval { .. } => "zero polling interval",
            Self::ServerAndBoardPortsMatch { .. } => "server_port and board_port must differ",
            Self::FloatComparison(_) => "float comparison failed",
            Self::InvalidTravelRule { .. } => "invalid travel rule config",
            Self::MissingTravelRule => "missing travel rule config",
            Self::WalletNotConfigured => "wallet not configured",
            Self::Wallet(_) => "wallet construction error",
            Self::WalletMissingRpcUrl { .. } => "wallet missing RPC URL",
            Self::WalletSecretsMissing => "wallet secrets missing",
            Self::RestApiClient(_) => "failed to build REST API HTTP client",
            Self::MissingIssuanceConfig => "missing issuance config",
            Self::InvalidIssuanceApiKey { .. } => "invalid issuance api_key",
        }
    }
}

/// Normalizes database URLs so multiple connection pools (sqlx 0.9 for CQRS,
/// apalis's sqlx 0.8 for workers) address the same in-memory database.
pub fn effective_sqlite_url(database_url: &str) -> String {
    if database_url == ":memory:" {
        "file:st0x-hedge?mode=memory&cache=shared".to_owned()
    } else {
        database_url.to_owned()
    }
}

pub async fn configure_sqlite_pool(database_url: &str) -> Result<SqlitePool, sqlx::Error> {
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
    let options: SqliteConnectOptions = effective_sqlite_url(database_url)
        .parse::<SqliteConnectOptions>()?
        .create_if_missing(true)
        .auto_vacuum(SqliteAutoVacuum::Incremental)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(std::time::Duration::from_secs(10));

    let pool = SqlitePool::connect_with(options).await?;

    // auto_vacuum can only be changed on a newly created database, or by
    // running `PRAGMA auto_vacuum = INCREMENTAL` followed by a full
    // `VACUUM` on an existing one. If the database was created before this
    // setting was added, the pragma in SqliteConnectOptions is silently
    // ignored and incremental_vacuum() calls will be no-ops.
    let auto_vacuum: i32 = sqlx::query_scalar("PRAGMA auto_vacuum")
        .fetch_one(&pool)
        .await?;

    if auto_vacuum != 2 {
        warn!(
            auto_vacuum,
            "Database auto_vacuum mode is not INCREMENTAL (2). \
             Event compaction will not reclaim disk space. \
             Run `PRAGMA auto_vacuum = INCREMENTAL; VACUUM;` \
             once to enable it."
        );
    }

    Ok(pool)
}

#[cfg(any(test, feature = "test-support"))]
#[must_use]
pub fn create_test_issuance_ctx() -> IssuanceStatusCtx {
    IssuanceStatusCtx {
        // Hard-coded literal URL -- parse cannot fail in a test helper.
        #[allow(clippy::unwrap_used)]
        base_url: Url::parse("http://localhost:8000").unwrap(),
        api_key: IssuanceApiKey(B256::repeat_byte(0xab)),
    }
}

/// Issuance status context pointing at an e2e mock server URL.
#[cfg(any(test, feature = "test-support"))]
#[must_use]
pub fn test_issuance_status_ctx(base_url: Url) -> IssuanceStatusCtx {
    IssuanceStatusCtx {
        base_url,
        api_key: create_test_issuance_ctx().api_key,
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn create_test_ctx_with_order_owner(order_owner: Address) -> Ctx {
    Ctx {
        database_url: ":memory:".to_owned(),
        log_level: LogLevel::Debug,
        log_dir: None,
        server_port: 8080,
        board_port: 8081,
        evm: EvmCtx {
            // Hard-coded literal URL — parse cannot fail in a test helper.
            #[allow(clippy::unwrap_used)]
            rpc_url: url::Url::parse("http://localhost:8545").unwrap(),
            orderbook: alloy::primitives::address!("0x1111111111111111111111111111111111111111"),
            // Legacy by default: no distinct inventory, so the OPERATOR_ROLE
            // preflight is skipped. Tests exercising the managed path override
            // `evm.inventory` explicitly.
            inventory: InventoryMode::Legacy,
            vault_owner: order_owner,
            deployment_block: 1,
            required_confirmations: 1,
            ingestion_cutoff: IngestionCutoff::Safe,
        },
        order_polling_interval: 15,
        order_polling_max_jitter: 5,
        position_check_interval: 60,
        inventory_poll_interval: 60,
        order_fill_poll_interval: 5,
        extended_hours_reprice_timeout_secs: 300,
        extended_hours_close_flatten_window_secs: 900,
        apalis_finished_job_cleanup_interval_secs: 3600,
        broker: BrokerCtx::DryRun,
        telemetry: None,
        alerts: None,
        trading_mode: TradingMode::Standalone,
        order_owner,
        wallet: None,
        wallet_meta: None,
        execution_threshold: ExecutionThreshold::whole_share(),
        assets: AssetsConfig {
            equities: EquitiesConfig::default(),
            cash: None,
        },
        travel_rule: None,
        rest_api: None,
        issuance: create_test_issuance_ctx(),
        redemption_wallet: None,
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address, b256};
    use std::io::Write;
    use tempfile::NamedTempFile;

    use st0x_execution::{MockExecutor, MockExecutorCtx, TryIntoExecutor};
    use st0x_float_macro::float;

    use super::*;
    use crate::{ExecutionThreshold, InventoryModeTag};

    fn toml_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file
    }

    /// `Ctx::for_test`'s `vault_owner` derivation mirrors the production
    /// `EvmCtx` wiring: `Legacy` (no distinct inventory contract) resolves to
    /// `order_owner`, `Managed` resolves to the inventory address. A swapped
    /// branch would misattribute which vaults the bot considers its own for
    /// ClearV3/TakeOrderV3 matching and inventory-mode discovery.
    #[test]
    fn for_test_vault_owner_matches_legacy_order_owner() {
        let order_owner = address!("0x1111111111111111111111111111111111111111");

        let ctx = Ctx::for_test()
            .database_url(":memory:".to_owned())
            .rpc_url(url::Url::parse("http://localhost:8545").unwrap())
            .orderbook(address!("0x2222222222222222222222222222222222222222"))
            .deployment_block(1)
            .broker(BrokerCtx::DryRun)
            .trading_mode(TradingMode::Standalone)
            .order_owner(order_owner)
            .assets(AssetsConfig::default())
            .call()
            .unwrap();

        assert_eq!(ctx.evm.inventory, InventoryMode::Legacy);
        assert_eq!(ctx.evm.vault_owner, order_owner);
    }

    #[test]
    fn for_test_vault_owner_matches_managed_inventory_address() {
        let order_owner = address!("0x1111111111111111111111111111111111111111");
        let inventory = address!("0x3333333333333333333333333333333333333333");

        let ctx = Ctx::for_test()
            .database_url(":memory:".to_owned())
            .rpc_url(url::Url::parse("http://localhost:8545").unwrap())
            .orderbook(address!("0x2222222222222222222222222222222222222222"))
            .deployment_block(1)
            .broker(BrokerCtx::DryRun)
            .trading_mode(TradingMode::Standalone)
            .order_owner(order_owner)
            .assets(AssetsConfig::default())
            .inventory_mode(InventoryMode::Managed { inventory })
            .call()
            .unwrap();

        assert_eq!(ctx.evm.inventory, InventoryMode::Managed { inventory });
        assert_eq!(ctx.evm.vault_owner, inventory);
    }

    /// Pins the first line of defense against database contention: every
    /// pooled connection runs WAL (writers never block readers) with a
    /// 10s busy timeout that waits out brief write-lock contention from a
    /// co-located writer instead of failing instantly. Contention beyond the
    /// timeout still surfaces as a retryable SQLITE_BUSY error in the trading
    /// pipeline -- it is not swallowed. If a refactor drops either pragma,
    /// every lock blip from a co-located process becomes an immediate hard
    /// error, and this test catches the drift.
    #[tokio::test]
    async fn configure_sqlite_pool_pins_wal_and_busy_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let database_url = format!("sqlite://{}/config-pin.sqlite", dir.path().display());

        let pool = configure_sqlite_pool(&database_url).await.unwrap();

        let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(journal_mode, "wal");

        let busy_timeout_ms: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(busy_timeout_ms, 10_000);
    }

    fn minimal_config_toml() -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(
            br#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        )
        .unwrap();
        file
    }

    /// Minimal config with `[broker.travel_rule]` included, for tests
    /// that use Alpaca Broker API secrets (which now require travel rule
    /// at startup).
    fn alpaca_config_toml() -> NamedTempFile {
        toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [broker]
            counter_trade_slippage_bps = 100
            extended_hours_reprice_timeout_secs = 300
            extended_hours_close_flatten_window_secs = 900

            [broker.travel_rule]
            beneficiary_entity_name = "Test Entity"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        )
    }

    fn alpaca_trading_config_toml() -> NamedTempFile {
        toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [broker]
            counter_trade_slippage_bps = 100
            extended_hours_reprice_timeout_secs = 300
            extended_hours_close_flatten_window_secs = 900

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        )
    }

    fn dry_run_secrets_toml() -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(
            br#"
            [evm]
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"

            [broker]
            type = "dry-run"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

            [issuance]
            base_url = "http://issuance.test:8000"
            api_key = "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
        "#,
        )
        .unwrap();
        file
    }

    fn unsupported_schwab_secrets_toml() -> NamedTempFile {
        toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"

            [broker]
            type = "schwab"
            app_key = "test_key"
            app_secret = "test_secret"
            encryption_key = "0x0000000000000000000000000000000000000000000000000000000000000000"
        "#,
        )
    }

    fn example_config_toml() -> &'static Path {
        Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../example.config.toml"
        ))
    }

    fn example_secrets_toml() -> &'static Path {
        Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../example.secrets.toml"
        ))
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
        assert!(matches!(ctx.broker, BrokerCtx::DryRun));

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
    async fn load_files_rejects_invalid_orderbook_address() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "not-an-address"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"
        "#,
        );
        let secrets = dry_run_secrets_toml();
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(error, CtxError::ConfigToml { .. }),
            "expected config parse failure for invalid orderbook, got: {error:#}"
        );

        let source = std::error::Error::source(&error).unwrap();
        let source_display = source.to_string();
        assert!(
            source_display.contains("orderbook"),
            "expected parse error to mention orderbook field, got: {source_display}"
        );
        assert!(
            source_display.contains("not-an-address"),
            "expected parse error to mention invalid orderbook value, got: {source_display}"
        );
    }

    #[tokio::test]
    async fn travel_rule_parsed_from_broker_section() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"

            [broker.travel_rule]
            beneficiary_entity_name = "T0 TRADE (BVI) LTD"
        "#,
        );
        let secrets = dry_run_secrets_toml();
        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();

        let travel_rule = ctx.travel_rule.unwrap();
        assert_eq!(travel_rule.beneficiary_entity_name, "T0 TRADE (BVI) LTD");
    }

    #[tokio::test]
    async fn travel_rule_optional_when_broker_section_absent() {
        let config = minimal_config_toml();
        let secrets = dry_run_secrets_toml();
        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();

        assert!(ctx.travel_rule.is_none());
    }

    #[tokio::test]
    async fn travel_rule_rejects_placeholder_entity_name() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"

            [broker.travel_rule]
            beneficiary_entity_name = "PLACEHOLDER"
        "#,
        );
        let secrets = dry_run_secrets_toml();
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                CtxError::InvalidTravelRule {
                    field: "beneficiary_entity_name",
                    ..
                }
            ),
            "expected InvalidTravelRule for entity_name, got: {error}"
        );
    }

    #[tokio::test]
    async fn travel_rule_rejects_blank_entity_name() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"

            [broker.travel_rule]
            beneficiary_entity_name = "   "
        "#,
        );
        let secrets = dry_run_secrets_toml();
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                CtxError::InvalidTravelRule {
                    field: "beneficiary_entity_name",
                    ..
                }
            ),
            "expected InvalidTravelRule for entity_name, got: {error}"
        );
    }

    #[tokio::test]
    async fn alerts_ctx_built_when_section_and_secret_present() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"

            [alerts]
            chat_id = -1_001_234_567_890
            low_balance_threshold = "0.05"
            poll_interval = 300
            realert_interval = 3600
        "#,
        );
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"

            [broker]
            type = "dry-run"

            [alerts]
            bot_token = "123:abc"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

            [issuance]
            base_url = "http://issuance.test:8000"
            api_key = "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
        "#,
        );

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();

        let alerts = ctx.alerts.unwrap();
        assert_eq!(alerts.chat_id, -1_001_234_567_890);
        assert_eq!(
            alerts.low_balance_threshold_wei,
            alloy::primitives::U256::from(50_000_000_000_000_000_u64)
        );
        assert_eq!(alerts.poll_interval, std::time::Duration::from_secs(300));
        assert_eq!(
            alerts.realert_interval,
            std::time::Duration::from_secs(3600)
        );
        assert_eq!(alerts.message_thread_id, None);
    }

    #[tokio::test]
    async fn alerts_ctx_absent_when_section_omitted() {
        let config = minimal_config_toml();
        let secrets = dry_run_secrets_toml();
        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();

        assert!(ctx.alerts.is_none());
    }

    #[tokio::test]
    async fn alerts_config_fails_fast_on_bad_threshold() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"

            [alerts]
            chat_id = 1
            low_balance_threshold = "not-a-number"
            poll_interval = 300
            realert_interval = 3600
        "#,
        );
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"

            [broker]
            type = "dry-run"

            [alerts]
            bot_token = "123:abc"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#,
        );

        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                CtxError::Alerts(crate::alerts::AlertsAssemblyError::InvalidThreshold { .. })
            ),
            "expected Alerts(InvalidThreshold), got: {error}"
        );
    }

    #[tokio::test]
    async fn standalone_mode_when_no_rebalancing() {
        let config = minimal_config_toml();
        let secrets = dry_run_secrets_toml();
        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();
        assert!(matches!(ctx.trading_mode, TradingMode::Standalone));
        assert_eq!(
            ctx.order_owner(),
            address!("0xfcad0b19bb29d4674531d6f115237e16afce377c")
        );
    }

    #[tokio::test]
    async fn defaults_applied_when_optional_fields_omitted() {
        let config = minimal_config_toml();
        let secrets = dry_run_secrets_toml();
        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();
        assert!(matches!(ctx.log_level, LogLevel::Debug));
        assert_eq!(ctx.order_polling_interval, 15);
        assert_eq!(ctx.order_polling_max_jitter, 5);
        assert_eq!(ctx.position_check_interval, 60);
        assert_eq!(ctx.inventory_poll_interval, 60);
        assert_eq!(ctx.order_fill_poll_interval, 5);
    }

    #[tokio::test]
    async fn apalis_finished_job_cleanup_interval_is_required() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"
        "#,
        );
        let secrets = dry_run_secrets_toml();
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(error, CtxError::ConfigToml { .. }),
            "expected config parse failure for missing cleanup interval, got: {error:#}"
        );

        let source = std::error::Error::source(&error).unwrap();
        let source_display = source.to_string();
        assert!(
            source_display.contains("apalis_finished_job_cleanup_interval_secs"),
            "expected parse error to mention cleanup interval field, got: {source_display}"
        );
    }

    #[tokio::test]
    async fn server_port_is_required() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"
        "#,
        );
        let secrets = dry_run_secrets_toml();
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(error, CtxError::ConfigToml { .. }),
            "expected config parse failure for missing server_port, got: {error:#}"
        );

        let source = std::error::Error::source(&error).unwrap();
        let source_display = source.to_string();
        assert!(
            source_display.contains("server_port"),
            "expected parse error to mention server_port, got: {source_display}"
        );
    }

    #[tokio::test]
    async fn board_port_is_required() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"
        "#,
        );
        let secrets = dry_run_secrets_toml();
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(error, CtxError::ConfigToml { .. }),
            "expected config parse failure for missing board_port, got: {error:#}"
        );

        let source = std::error::Error::source(&error).unwrap();
        let source_display = source.to_string();
        assert!(
            source_display.contains("board_port"),
            "expected parse error to mention board_port, got: {source_display}"
        );
    }

    #[tokio::test]
    async fn extended_hours_counter_trading_is_required_per_equity() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities.AAPL]
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"
        "#,
        );
        let secrets = dry_run_secrets_toml();
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(error, CtxError::ConfigToml { .. }),
            "expected config parse failure for missing extended-hours flag, got: {error:#}"
        );

        let source = std::error::Error::source(&error).unwrap();
        let source_display = source.to_string();
        assert!(
            source_display.contains("extended_hours_counter_trading"),
            "expected parse error to mention extended-hours flag, got: {source_display}"
        );
    }

    #[tokio::test]
    async fn apalis_finished_job_cleanup_interval_must_be_non_zero() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 0

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = dry_run_secrets_toml();
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                CtxError::ZeroPollingInterval {
                    field: "apalis_finished_job_cleanup_interval_secs"
                }
            ),
            "expected ZeroPollingInterval for cleanup interval, got: {error:#}"
        );
    }

    #[tokio::test]
    async fn order_fill_poll_interval_must_be_non_zero() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            order_fill_poll_interval = 0

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = dry_run_secrets_toml();
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                CtxError::ZeroPollingInterval {
                    field: "order_fill_poll_interval"
                }
            ),
            "expected ZeroPollingInterval for order fill poll interval, got: {error:#}"
        );
    }

    #[tokio::test]
    async fn server_port_and_board_port_must_differ() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8080
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = dry_run_secrets_toml();
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(error, CtxError::ServerAndBoardPortsMatch { port: 8080 }),
            "expected ServerAndBoardPortsMatch for equal ports, got: {error:#}"
        );
    }

    #[tokio::test]
    async fn rebalancing_with_low_cash_operational_limit_fails() {
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test_key"
            api_secret = "test_secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
            mode = "sandbox"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#,
        );

        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [assets.cash]
            rebalancing = "enabled"
            operational_limit = 5

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [broker]
            counter_trade_slippage_bps = 100
            extended_hours_reprice_timeout_secs = 300
            extended_hours_close_flatten_window_secs = 900

            [tokenization]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

            [rebalancing]
            transfer_timeout_secs = 1800
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [wallet]
            kind = "private-key"
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
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            log_level = "warn"
            server_port = 9090
            order_polling_interval = 30
            order_polling_max_jitter = 10
            position_check_interval = 120
            inventory_poll_interval = 90

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
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
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"

            [broker]
            type = "schwab"
            app_key = "test_key"
            app_secret = "test_secret"
            encryption_key = "0x0000000000000000000000000000000000000000000000000000000000000000"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#,
        );

        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [tokenization]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

            [rebalancing]
            transfer_timeout_secs = 1800
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [wallet]
            kind = "private-key"
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
            matches!(error, CtxError::SecretsToml { .. }),
            "Expected unsupported Schwab broker secrets to fail during parsing, got {error:?}"
        );
    }

    #[tokio::test]
    async fn unsupported_schwab_broker_fails_during_secret_parsing() {
        let config = minimal_config_toml();
        let secrets = unsupported_schwab_secrets_toml();
        let result = Ctx::load_files(config.path(), secrets.path()).await;
        assert!(
            matches!(result, Err(CtxError::SecretsToml { .. })),
            "Expected unsupported Schwab broker secrets to fail during parsing, got {result:?}"
        );
    }

    #[tokio::test]
    async fn unsupported_schwab_broker_with_order_owner_fails_during_secret_parsing() {
        let config = minimal_config_toml();
        let secrets = unsupported_schwab_secrets_toml();
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            "failed to parse secrets",
            "Unsupported Schwab broker should be rejected during secrets parsing"
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
    async fn telemetry_ctx_assembled_from_config() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"

            [telemetry]
            service_name = "test-service"
            environment = "test"
            traces_endpoint = "http://100.0.0.1:10428"
            logs_endpoint = "http://100.0.0.1:9428"
        "#,
        );

        let ctx = Ctx::load_files(config.path(), dry_run_secrets_toml().path())
            .await
            .unwrap();
        let telemetry = ctx.telemetry.as_ref().expect("telemetry should be Some");
        assert_eq!(telemetry.service_name, "test-service");
        assert_eq!(telemetry.environment, "test");
        // `url::Url` normalizes an authority-only URL to carry a trailing-slash
        // root path, so the parsed endpoint gains the `/` the literal omits.
        assert_eq!(
            telemetry.traces_endpoint.as_str(),
            "http://100.0.0.1:10428/"
        );
        assert_eq!(telemetry.logs_endpoint.as_str(), "http://100.0.0.1:9428/");
    }

    #[tokio::test]
    async fn telemetry_absent_when_config_section_missing() {
        let config = minimal_config_toml();
        let ctx = Ctx::load_files(config.path(), dry_run_secrets_toml().path())
            .await
            .unwrap();
        assert!(
            ctx.telemetry.is_none(),
            "expected telemetry None when [telemetry] absent, got: {:?}",
            ctx.telemetry
        );
    }

    #[tokio::test]
    async fn rebalancing_ctx_without_secrets_fails() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [broker]
            counter_trade_slippage_bps = 100
            extended_hours_reprice_timeout_secs = 300
            extended_hours_close_flatten_window_secs = 900

            [tokenization]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

            [rebalancing]
            transfer_timeout_secs = 1800
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

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
            rpc_url = "http://localhost:8545"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
        "#,
        );

        let result = Ctx::load_files(config.path(), secrets.path()).await;
        assert!(
            matches!(result, Err(CtxError::WalletNotConfigured)),
            "Expected WalletNotConfigured error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn rebalancing_without_wallet_config_fails() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [broker]
            counter_trade_slippage_bps = 100
            extended_hours_reprice_timeout_secs = 300
            extended_hours_close_flatten_window_secs = 900

            [broker.travel_rule]
            beneficiary_entity_name = "Test Corp"

            [tokenization]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

            [rebalancing]
            transfer_timeout_secs = 1800
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

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
            rpc_url = "http://localhost:8545"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
            mode = "sandbox"
        "#,
        );

        let result = Ctx::load_files(config.path(), secrets.path()).await;
        assert!(
            matches!(result, Err(CtxError::WalletNotConfigured)),
            "Expected WalletNotConfigured error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn rebalancing_without_tokenization_config_fails() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [wallet]
            kind = "private-key"
            address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

            [broker]
            counter_trade_slippage_bps = 100
            extended_hours_reprice_timeout_secs = 300
            extended_hours_close_flatten_window_secs = 900

            [broker.travel_rule]
            beneficiary_entity_name = "Test Corp"

            [rebalancing]
            transfer_timeout_secs = 1800
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

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
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.example.com"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
            mode = "sandbox"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#,
        );

        let result = Ctx::load_files(config.path(), secrets.path()).await;
        assert!(
            matches!(result, Err(CtxError::MissingTokenization)),
            "Expected MissingTokenization error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn wallet_config_without_wallet_secrets_fails() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"


            [broker]
            counter_trade_slippage_bps = 100
            extended_hours_reprice_timeout_secs = 300
            extended_hours_close_flatten_window_secs = 900

            [broker.travel_rule]
            beneficiary_entity_name = "Test Corp"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );

        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
            mode = "sandbox"
        "#,
        );

        let result = Ctx::load_files(config.path(), secrets.path()).await;
        assert!(
            matches!(result, Err(CtxError::WalletSecretsMissing)),
            "Expected WalletSecretsMissing error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn wallet_without_rpc_urls_fails() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"


            [broker]
            counter_trade_slippage_bps = 100
            extended_hours_reprice_timeout_secs = 300
            extended_hours_close_flatten_window_secs = 900

            [broker.travel_rule]
            beneficiary_entity_name = "Test Corp"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );

        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
            mode = "sandbox"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#,
        );

        let result = Ctx::load_files(config.path(), secrets.path()).await;
        assert!(
            matches!(
                result,
                Err(CtxError::WalletMissingRpcUrl {
                    field: "base_rpc_url"
                })
            ),
            "Expected WalletMissingRpcUrl for base_rpc_url, got {result:?}"
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
            ExecutionThreshold::shares(Positive::new(FractionalShares::new(float!(1))).unwrap())
        );
    }

    #[tokio::test]
    async fn alpaca_broker_api_requires_counter_trade_slippage_config() {
        let config = minimal_config_toml();
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
        "#,
        );

        let err = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();

        assert!(
            matches!(err, CtxError::MissingCounterTradeSlippageBps),
            "Expected MissingCounterTradeSlippageBps, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn alpaca_broker_api_requires_extended_hours_reprice_timeout_config() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [broker]
            counter_trade_slippage_bps = 100

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.example.com"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#,
        );

        let err = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();

        assert!(
            matches!(err, CtxError::MissingExtendedHoursRepriceTimeout),
            "Expected MissingExtendedHoursRepriceTimeout, got: {err:?}"
        );
    }

    #[test]
    fn extended_hours_reprice_timeout_rejects_values_chrono_cannot_represent() {
        let broker = BrokerConfig {
            counter_trade_slippage_bps: Some(100),
            extended_hours_reprice_timeout_secs: Some(u64::MAX),
            extended_hours_close_flatten_window_secs: Some(300),
            travel_rule: None,
        };

        let error = broker.extended_hours_reprice_timeout_secs().unwrap_err();

        assert!(
            matches!(
                error,
                CtxError::ExtendedHoursRepriceTimeoutOutOfRange {
                    configured: u64::MAX,
                    ..
                }
            ),
            "Expected ExtendedHoursRepriceTimeoutOutOfRange, got: {error:?}"
        );
    }

    #[test]
    fn extended_hours_reprice_timeout_accepts_chrono_maximum() {
        let broker = BrokerConfig {
            counter_trade_slippage_bps: Some(100),
            extended_hours_reprice_timeout_secs: Some(MAX_EXTENDED_HOURS_REPRICE_TIMEOUT_SECS),
            extended_hours_close_flatten_window_secs: Some(300),
            travel_rule: None,
        };

        assert_eq!(
            broker.extended_hours_reprice_timeout_secs().unwrap(),
            MAX_EXTENDED_HOURS_REPRICE_TIMEOUT_SECS
        );
    }

    #[tokio::test]
    async fn alpaca_broker_api_requires_extended_hours_close_flatten_window_config() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [broker]
            counter_trade_slippage_bps = 100
            extended_hours_reprice_timeout_secs = 300

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.example.com"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#,
        );

        let err = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();

        assert!(
            matches!(err, CtxError::MissingExtendedHoursCloseFlattenWindow),
            "Expected MissingExtendedHoursCloseFlattenWindow, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn alpaca_broker_api_rejects_zero_extended_hours_close_flatten_window() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [broker]
            counter_trade_slippage_bps = 100
            extended_hours_reprice_timeout_secs = 300
            extended_hours_close_flatten_window_secs = 0

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.example.com"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#,
        );

        let err = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();

        assert!(
            matches!(
                err,
                CtxError::ZeroPollingInterval {
                    field: "broker.extended_hours_close_flatten_window_secs"
                }
            ),
            "Expected ZeroPollingInterval for extended_hours_close_flatten_window_secs, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn alpaca_broker_api_counter_trade_slippage_must_be_positive_and_under_10_000_bps() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [broker]
            counter_trade_slippage_bps = 0
        "#,
        );
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
        "#,
        );

        let err = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                CtxError::CounterTradeSlippageBpsOutOfRange {
                    configured: 0,
                    min: 1,
                    max: 9_999,
                }
            ),
            "Expected CounterTradeSlippageBpsOutOfRange, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn alpaca_broker_api_counter_trade_slippage_rejects_10_000_bps() {
        // 10_000 bps (=100%) zeroes sell-side limit prices and fails
        // Positive::new at runtime. Must be rejected at config load.
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [broker]
            counter_trade_slippage_bps = 10000
            extended_hours_reprice_timeout_secs = 300
            extended_hours_close_flatten_window_secs = 900
        "#,
        );
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
        "#,
        );

        let err = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                CtxError::CounterTradeSlippageBpsOutOfRange {
                    configured: 10_000,
                    min: 1,
                    max: 9_999,
                }
            ),
            "Expected CounterTradeSlippageBpsOutOfRange{{configured: 10000}}, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn alpaca_broker_api_counter_trade_slippage_accepts_9_999_bps() {
        // 9_999 bps is the maximum accepted value (MAX_COUNTER_TRADE_SLIPPAGE_BPS).
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [broker]
            counter_trade_slippage_bps = 9999
            extended_hours_reprice_timeout_secs = 300
            extended_hours_close_flatten_window_secs = 900

            [broker.travel_rule]
            beneficiary_entity_name = "Test Entity"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

            [issuance]
            base_url = "http://issuance.test:8000"
            api_key = "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
        "#,
        );

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();

        let BrokerCtx::AlpacaBrokerApi(broker) = ctx.broker else {
            panic!("expected AlpacaBrokerApi broker");
        };

        assert_eq!(broker.counter_trade_slippage_bps, 9999);
    }

    #[tokio::test]
    async fn alpaca_broker_api_executor_uses_dollar_threshold() {
        let config = alpaca_config_toml();
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

            [issuance]
            base_url = "http://issuance.test:8000"
            api_key = "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
        "#,
        );

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();
        let expected = ExecutionThreshold::dollar_value(Usdc::new(float!(2))).unwrap();
        assert_eq!(ctx.execution_threshold, expected);
    }

    #[tokio::test]
    async fn missing_issuance_section_fails_at_startup() {
        let config = minimal_config_toml();
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"

            [broker]
            type = "dry-run"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#,
        );
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(error, CtxError::MissingIssuanceConfig),
            "expected MissingIssuanceConfig when [issuance] is absent from secrets, got: {error:#}"
        );
    }

    #[tokio::test]
    async fn issuance_secret_without_api_key_fails() {
        let config = minimal_config_toml();
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"

            [broker]
            type = "dry-run"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

            [issuance]
            base_url = "http://issuance.test:8000"
        "#,
        );
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(error, CtxError::SecretsToml { .. }),
            "expected SecretsToml when api_key is absent, got: {error:#}"
        );
    }

    #[tokio::test]
    async fn invalid_issuance_base_url_fails() {
        let config = minimal_config_toml();
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"

            [broker]
            type = "dry-run"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

            [issuance]
            base_url = "not a url"
            api_key = "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
        "#,
        );
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(error, CtxError::SecretsToml { .. }),
            "expected SecretsToml for an unparseable base_url, got: {error:#}"
        );
    }

    #[tokio::test]
    async fn issuance_api_key_must_be_32_bytes() {
        let config = minimal_config_toml();
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"

            [broker]
            type = "dry-run"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

            [issuance]
            base_url = "http://issuance.test:8000"
            api_key = "0xdeadbeef"
        "#,
        );
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(error, CtxError::InvalidIssuanceApiKey { .. }),
            "expected InvalidIssuanceApiKey for a non-32-byte api_key, got: {error:#}"
        );
    }

    #[tokio::test]
    async fn invalid_issuance_api_key_never_echoes_the_raw_secret() {
        // A non-hex key that resembles issuance's >=32-char string keys. If it
        // ever surfaces in the error chain, the secret has leaked.
        let raw_key = "this-is-a-secret-api-key-not-hex-0123456789";
        let config = minimal_config_toml();
        let secrets = toml_file(&format!(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"

            [broker]
            type = "dry-run"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

            [issuance]
            base_url = "http://issuance.test:8000"
            api_key = "{raw_key}"
        "#
        ));
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(error, CtxError::InvalidIssuanceApiKey { .. }),
            "expected InvalidIssuanceApiKey for a non-hex api_key, got: {error:#}"
        );

        // The full error chain (what validate-config and startup logs print)
        // must never contain the raw key.
        let rendered = format!("{error:#}\n{error:?}");
        assert!(
            !rendered.contains(raw_key),
            "the raw api_key leaked into the error output: {rendered}"
        );
    }

    #[test]
    fn test_issuance_status_ctx_uses_supplied_base_url() {
        let base_url = Url::parse("http://127.0.0.1:4242").unwrap();
        let ctx = test_issuance_status_ctx(base_url.clone());
        assert_eq!(ctx.base_url, base_url);
    }

    #[test]
    fn issuance_ctx_assembles_base_url_and_parses_api_key() {
        // Exercise issuance_ctx directly so the assertion does not depend on a
        // wallet feature being enabled for a full Ctx::load_files.
        let ctx = issuance_ctx(Some(IssuanceSecrets {
            base_url: Url::parse("http://issuance.test:8000").unwrap(),
            api_key: "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
                .parse()
                .unwrap(),
        }))
        .expect("a valid issuance secret must assemble the ctx");

        assert_eq!(
            ctx.base_url,
            Url::parse("http://issuance.test:8000").unwrap(),
            "base_url must come from the issuance secrets"
        );
        // The secret is 0x-prefixed; the header value must be the bare 64-char
        // lowercase hex with no 0x prefix (issuance's X-API-KEY contract).
        assert_eq!(
            ctx.api_key.header_value(),
            "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
            "header_value must be bare lowercase hex (no 0x prefix)"
        );
    }

    #[test]
    fn issuance_api_key_header_value_is_bare_lowercase_hex() {
        let bare = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899";
        let with_prefix = format!("0x{bare}");
        let from_bare = IssuanceApiKey(bare.parse().expect("bare hex must parse"));
        let from_prefixed = IssuanceApiKey(with_prefix.parse().expect("0x hex must parse"));

        assert_eq!(
            from_bare.header_value(),
            bare,
            "header_value must round-trip bare lowercase hex"
        );
        assert_eq!(
            from_prefixed.header_value(),
            bare,
            "the 0x prefix must be stripped in the header value"
        );

        let uppercase = IssuanceApiKey(
            "0xAABBCCDDEEFF00112233445566778899AABBCCDDEEFF00112233445566778899"
                .parse()
                .expect("uppercase hex must parse"),
        );
        assert_eq!(
            uppercase.header_value(),
            bare,
            "uppercase hex must normalise to bare lowercase hex"
        );
    }

    #[tokio::test]
    async fn alpaca_broker_without_travel_rule_fails_at_startup() {
        let config = alpaca_trading_config_toml();
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#,
        );

        let err = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(err, CtxError::MissingTravelRule),
            "Expected MissingTravelRule, got: {err:?}"
        );
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
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"

            [broker]
            type = "schwab"
            app_key = "test_key"
            app_secret = "test_secret"
            encryption_key = "0x0000000000000000000000000000000000000000000000000000000000000000"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#,
        );

        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [tokenization]
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

            [rebalancing]
            transfer_timeout_secs = 1800
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [wallet]
            kind = "private-key"
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
            matches!(error, CtxError::SecretsToml { .. }),
            "expected unsupported Schwab broker secrets to fail during parsing, got: {error:?}"
        );
        assert_eq!(error.kind(), "failed to parse secrets");
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
        let config_str = include_str!("../../../config/prod/st0x-hedge.toml");
        let config: Config = toml::from_str(config_str).unwrap();

        let global_limit = config
            .assets
            .equities
            .operational_limit
            .map(Positive::inner);

        let broker = config.broker.expect(
            "prod config must include [broker.travel_rule] — \
             Alpaca rejects whitelist requests without it, effective 2026-03-27",
        );

        broker
            .counter_trade_slippage_bps
            .expect("prod config must set [broker].counter_trade_slippage_bps");
        broker
            .travel_rule
            .expect("prod config must include [broker.travel_rule]")
            .validated()
            .unwrap();

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
    fn s01_issuer_config_toml_is_valid() {
        let config_str = include_str!("../../../config/s01-issuer.toml");
        let config: Config = toml::from_str(config_str).unwrap();

        // The dividend buy leg runs through Alpaca, which rejects whitelist
        // requests without a travel rule -- same constraint as the prod bot.
        let broker = config
            .broker
            .expect("s01-issuer config must include [broker] for the dividend buy leg");
        broker
            .travel_rule
            .expect("s01-issuer config must include [broker.travel_rule]")
            .validated()
            .unwrap();

        // The NAV bump tokenizes + donates the wrapped equity, so the bumped
        // symbol must carry both token addresses the wrap/donate path resolves.
        let sgov = config
            .assets
            .equities
            .symbols
            .get(&Symbol::new("SGOV").unwrap())
            .expect("s01-issuer config must configure the SGOV equity for the NAV bump");
        assert_ne!(sgov.tokenized_equity, Address::ZERO);
        assert_ne!(sgov.tokenized_equity_derivative, Address::ZERO);

        // The bump is funded + signed by the dividend-ops turnkey wallet.
        let wallet = config
            .wallet
            .expect("s01-issuer config must include the [wallet] section");
        let wallet_meta = WalletMeta::deserialize(wallet).unwrap();
        assert_eq!(wallet_meta.kind, "turnkey");
    }

    #[test]
    fn example_config_toml_is_valid() {
        let config_str = include_str!("../../../example.config.toml");
        let _: Config = toml::from_str(config_str).unwrap();
    }

    #[test]
    fn example_secrets_toml_is_valid() {
        let secrets_str = include_str!("../../../example.secrets.toml");
        let _: Secrets = toml::from_str(secrets_str).unwrap();
    }

    #[test]
    fn e2e_config_toml_is_valid() {
        let config_str = include_str!("../../../e2e/config.toml");
        let _: Config = toml::from_str(config_str).unwrap();
    }

    fn equity_config_with_feed_line(feed_line: &str) -> Config {
        let toml_str = format!(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [assets.equities.AAPL]
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            {feed_line}
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"
            extended_hours_counter_trading = "disabled"
        "#
        );

        toml::from_str(&toml_str).unwrap()
    }

    #[test]
    fn equity_pyth_feed_id_parses_when_present() {
        let config = equity_config_with_feed_line(
            r#"pyth_feed_id = "0xfee33f2a978bf32dd6b662b65ba8083c6773b494f8401194ec1870c640860245""#,
        );

        let aapl = config
            .assets
            .equities
            .symbols
            .get(&Symbol::new("AAPL").unwrap())
            .unwrap();

        assert_eq!(
            aapl.pyth_feed_id,
            Some(alloy::primitives::b256!(
                "0xfee33f2a978bf32dd6b662b65ba8083c6773b494f8401194ec1870c640860245"
            ))
        );
    }

    #[test]
    fn equity_pyth_feed_id_is_none_when_absent() {
        let config = equity_config_with_feed_line("");

        let aapl = config
            .assets
            .equities
            .symbols
            .get(&Symbol::new("AAPL").unwrap())
            .unwrap();

        assert_eq!(aapl.pyth_feed_id, None);
    }

    #[test]
    fn equity_pyth_feed_id_rejects_malformed_value() {
        let toml_str = r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [assets.equities.AAPL]
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            pyth_feed_id = "0xdeadbeef"
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"
        "#;

        let Err(error) = toml::from_str::<Config>(toml_str) else {
            panic!("expected malformed pyth_feed_id to fail parsing");
        };

        assert!(
            error
                .to_string()
                .contains("invalid pyth_feed_id `0xdeadbeef`"),
            "expected parse error to name the field and the offending value, got: {error}"
        );
    }

    #[test]
    fn pyth_feed_ids_returns_only_symbols_with_configured_feed() {
        fn equity(pyth_feed_id: Option<B256>) -> EquityAssetConfig {
            EquityAssetConfig {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                pyth_feed_id,
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Disabled,
                operational_limit: None,
            }
        }

        let configured = Symbol::new("COIN").unwrap();
        let unconfigured = Symbol::new("AMZN").unwrap();
        let feed_id = b256!("0xfee33f2a978bf32dd6b662b65ba8083c6773b494f8401194ec1870c640860245");

        let mut ctx = create_test_ctx_with_order_owner(Address::ZERO);
        ctx.assets
            .equities
            .symbols
            .insert(configured.clone(), equity(Some(feed_id)));
        ctx.assets
            .equities
            .symbols
            .insert(unconfigured.clone(), equity(None));

        let feed_ids = ctx.pyth_feed_ids();

        assert_eq!(feed_ids.len(), 1);
        assert_eq!(feed_ids.get(&configured), Some(&feed_id));
        assert_eq!(feed_ids.get(&unconfigured), None);
    }

    #[test]
    fn e2e_secrets_toml_is_valid() {
        let secrets_str = include_str!("../../../e2e/secrets.toml");
        let _: Secrets = toml::from_str(secrets_str).unwrap();
    }

    /// Every `.toml` config checked into the repo: `config/*/`, plus the
    /// `example.config.toml` and `e2e/config.toml` templates.
    fn repo_config_paths() -> Vec<PathBuf> {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap();
        let config_dir = repo_root.join("config");

        // Walk config/ subdirectories (config/prod/, config/staging/) to
        // find all .toml files. A flat read_dir misses these because the
        // direct children are directories, not .toml files.
        let mut config_paths: Vec<PathBuf> = Vec::new();
        for entry in std::fs::read_dir(&config_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();

            if path.is_dir() {
                for sub_entry in std::fs::read_dir(&path).unwrap() {
                    let sub_path = sub_entry.unwrap().path();
                    if sub_path.extension().is_some_and(|ext| ext == "toml") {
                        config_paths.push(sub_path);
                    }
                }
            } else if path.extension().is_some_and(|ext| ext == "toml") {
                config_paths.push(path);
            }
        }

        config_paths.push(repo_root.join("example.config.toml"));
        config_paths.push(repo_root.join("e2e/config.toml"));

        assert!(
            config_paths.len() >= 3,
            "Expected at least 3 config files (prod, staging, example), \
             found {}: {config_paths:?}",
            config_paths.len()
        );

        config_paths
    }

    #[test]
    fn all_repo_config_tomls_are_valid() {
        for path in repo_config_paths() {
            let contents = std::fs::read_to_string(&path).unwrap_or_else(|error| {
                panic!("Failed to read config {path:?}: {error}");
            });
            toml::from_str::<Config>(&contents).unwrap_or_else(|error| {
                panic!("Invalid config {path:?}: {error}");
            });
        }
    }

    /// `vault_owner` is the key every `vaultBalance2` read, vault-registry
    /// entry and order-owner fill match is scoped by, so pointing it at the
    /// wrong address silently routes a deployment at another deployment's
    /// vaults. Both settlement modes pin it exactly: `legacy` vaults are owned
    /// by the bot's own EOA, `managed` vaults are owned by the inventory
    /// contract. Neither invariant is checkable from a single field, so guard
    /// every checked-in config here (staging once shipped prod's wallet
    /// address).
    #[test]
    fn repo_config_vault_owner_matches_settlement_mode() {
        for path in repo_config_paths() {
            let contents = std::fs::read_to_string(&path).unwrap();
            let config: Config = toml::from_str(&contents).unwrap();

            // e2e/config.toml supplies its wallet out of band, so there is no
            // [wallet].address to pin a legacy vault_owner against -- but the
            // mode/inventory consistency checks below don't need the wallet,
            // so they run for every checked-in config.
            let wallet: Option<WalletMeta> = config.wallet.map(|value| value.try_into().unwrap());

            match (config.raindex.inventory_mode, config.raindex.inventory) {
                (InventoryModeTag::Legacy, None) => {
                    if let Some(wallet) = wallet {
                        assert_eq!(
                            config.raindex.vault_owner, wallet.address,
                            "{path:?}: inventory_mode = \"legacy\" means the bot's EOA owns \
                             the vaults, so vault_owner must equal [wallet].address"
                        );
                    }
                }
                (InventoryModeTag::Legacy, Some(inventory)) => {
                    // EvmCtx::new rejects this combination at startup
                    // (LegacyWithInventory); asserting it here fails the
                    // contradiction in CI instead of at the deploy gate.
                    panic!(
                        "{path:?}: inventory_mode = \"legacy\" forbids an inventory address, \
                         found {inventory}"
                    )
                }
                (InventoryModeTag::Managed, Some(inventory)) => assert_eq!(
                    config.raindex.vault_owner, inventory,
                    "{path:?}: inventory_mode = \"managed\" means the inventory contract \
                     owns the vaults, so vault_owner must equal inventory"
                ),
                (InventoryModeTag::Managed, None) => {
                    panic!("{path:?}: inventory_mode = \"managed\" requires an inventory address")
                }
            }
        }
    }

    #[test]
    fn all_repo_secrets_tomls_are_valid() {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap();
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
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            bogus_field = "should fail"

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"
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
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets]
            bogus_field = "should fail"

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"
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
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities.AAPL]
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"
            extended_hours_counter_trading = "disabled"
            bogus_field = "should fail"

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"
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
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.cash]
            rebalancing = "disabled"
            bogus_field = "should fail"

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"
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
            rpc_url = "http://localhost:8545"
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
            rpc_url = "http://localhost:8545"
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
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            bogus_field = "should fail"

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"
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
            rpc_url = "http://localhost:8545"

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
            wrapped_equity_recovery = "disabled"
            extended_hours_counter_trading = "disabled"
            operational_limit = 5

            [equities.SPYM]
            tokenized_equity = "0x8fdf41116f755771bfe0747d5f8c3711d5debfbb"
            tokenized_equity_derivative = "0x31c2c14134e6e3b7ef9478297f199331133fc2d8"
            trading = "disabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"
            extended_hours_counter_trading = "disabled"

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
        assert_eq!(rklb.vault_ids.len(), 1);
        assert!(rklb.operational_limit.is_some());

        let cash = config.cash.unwrap();
        assert_eq!(cash.rebalancing, OperationMode::Disabled);
        assert_eq!(cash.vault_ids.len(), 1);
    }

    #[test]
    fn extended_hours_counter_trading_parses_enabled_and_disabled_from_toml() {
        let toml_str = r#"
            [equities.AAPL]
            tokenized_equity = "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
            tokenized_equity_derivative = "0xf4f8c66085910d583c01f3b4e44bf731d4e2c565"
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"
            extended_hours_counter_trading = "enabled"

            [equities.TSLA]
            tokenized_equity = "0x8fdf41116f755771bfe0747d5f8c3711d5debfbb"
            tokenized_equity_derivative = "0x31c2c14134e6e3b7ef9478297f199331133fc2d8"
            trading = "disabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"
            extended_hours_counter_trading = "disabled"
        "#;
        let config: AssetsConfig = toml::from_str(toml_str).unwrap();
        let aapl = Symbol::new("AAPL").unwrap();
        let tsla = Symbol::new("TSLA").unwrap();
        assert!(config.is_extended_hours_enabled(&aapl));
        assert!(!config.is_extended_hours_enabled(&tsla));
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
            wrapped_equity_recovery = "disabled"
            extended_hours_counter_trading = "disabled"
        "#;

        let config: AssetsConfig = toml::from_str(toml_str).unwrap();
        let rklb = &config.equities.symbols[&Symbol::new("RKLB").unwrap()];
        let expected: B256 = "0000000000000000000000000000000000000000000000000000000000000fab"
            .parse()
            .unwrap();
        assert_eq!(rklb.vault_ids[0], expected);
    }

    #[test]
    fn vault_ids_array_parses_multiple_values() {
        let toml_str = r#"
            [equities.RKLB]
            tokenized_equity = "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
            tokenized_equity_derivative = "0xf4f8c66085910d583c01f3b4e44bf731d4e2c565"
            vault_ids = ["0xfab", "0xfab2"]
            trading = "disabled"
            rebalancing = "enabled"
            wrapped_equity_recovery = "disabled"
            extended_hours_counter_trading = "disabled"
        "#;

        let config: AssetsConfig = toml::from_str(toml_str).unwrap();
        let rklb = &config.equities.symbols[&Symbol::new("RKLB").unwrap()];
        assert_eq!(rklb.vault_ids.len(), 2);

        let expected_1: B256 = "0000000000000000000000000000000000000000000000000000000000000fab"
            .parse()
            .unwrap();
        let expected_2: B256 = "000000000000000000000000000000000000000000000000000000000000fab2"
            .parse()
            .unwrap();
        assert_eq!(rklb.vault_ids[0], expected_1);
        assert_eq!(rklb.vault_ids[1], expected_2);
    }

    #[test]
    fn equity_missing_trading_field_rejects() {
        let toml_str = r#"
            [equities.AAPL]
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"
            extended_hours_counter_trading = "disabled"
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
            wrapped_equity_recovery = "disabled"
            extended_hours_counter_trading = "disabled"
        "#;

        let result = toml::from_str::<AssetsConfig>(toml_str);
        assert!(
            result.is_err(),
            "Expected error for missing rebalancing field, got {result:?}"
        );
    }

    #[test]
    fn equity_missing_wrapped_equity_recovery_field_rejects() {
        let toml_str = r#"
            [equities.AAPL]
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            trading = "enabled"
            rebalancing = "disabled"
            extended_hours_counter_trading = "disabled"
        "#;

        let result = toml::from_str::<AssetsConfig>(toml_str);
        assert!(
            result.is_err(),
            "Expected error for missing wrapped_equity_recovery field, got {result:?}"
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
            wrapped_equity_recovery = "disabled"
            extended_hours_counter_trading = "disabled"
            operational_limit = 10

            [equities.TSLA]
            tokenized_equity = "0xcccccccccccccccccccccccccccccccccccccccc"
            tokenized_equity_derivative = "0xdddddddddddddddddddddddddddddddddddddddd"
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"
            extended_hours_counter_trading = "disabled"
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
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Enabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Disabled,
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
            !ctx.assets.is_trading_enabled(&Symbol::new("RKLB").unwrap()),
            "RKLB trading should be disabled"
        );
    }

    #[test]
    fn is_trading_disabled_for_unknown_assets() {
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));

        assert!(
            !ctx.assets
                .is_trading_enabled(&Symbol::new("UNKNOWN").unwrap()),
            "Unknown assets should default to trading disabled (fail-closed)"
        );
    }

    #[test]
    fn is_extended_hours_enabled_returns_configured_value() {
        let mut symbols = HashMap::new();
        symbols.insert(
            Symbol::new("AAPL").unwrap(),
            EquityAssetConfig {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Enabled,
                operational_limit: None,
            },
        );
        symbols.insert(
            Symbol::new("TSLA").unwrap(),
            EquityAssetConfig {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Disabled,
                operational_limit: None,
            },
        );

        let assets = AssetsConfig {
            equities: EquitiesConfig {
                operational_limit: None,
                symbols,
            },
            cash: None,
        };

        assert!(
            assets.is_extended_hours_enabled(&Symbol::new("AAPL").unwrap()),
            "AAPL extended-hours counter-trading should be enabled"
        );
        assert!(
            !assets.is_extended_hours_enabled(&Symbol::new("TSLA").unwrap()),
            "TSLA extended-hours counter-trading should be disabled"
        );
        assert!(
            !assets.is_extended_hours_enabled(&Symbol::new("UNKNOWN").unwrap()),
            "Unknown assets should default to disabled (fail-closed)"
        );
        assert!(
            assets.any_extended_hours_enabled(),
            "At least one equity enables extended-hours counter-trading"
        );
        assert!(
            !AssetsConfig::default().any_extended_hours_enabled(),
            "No configured equities means no extended-hours counter-trading"
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
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Enabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Disabled,
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
            ctx.assets
                .is_rebalancing_enabled(&Symbol::new("RKLB").unwrap()),
            "RKLB rebalancing should be enabled"
        );
    }

    #[test]
    fn is_rebalancing_enabled_defaults_to_false_for_unknown() {
        let ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));

        assert!(
            !ctx.assets
                .is_rebalancing_enabled(&Symbol::new("UNKNOWN").unwrap()),
            "Unknown assets should default to rebalancing disabled"
        );
    }

    #[test]
    fn is_wrapped_equity_recovery_enabled_is_independent_of_rebalancing() {
        let mut symbols = HashMap::new();
        symbols.insert(
            Symbol::new("AAPL").unwrap(),
            EquityAssetConfig {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Enabled,
                extended_hours_counter_trading: OperationMode::Disabled,
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

        let aapl = Symbol::new("AAPL").unwrap();
        assert!(
            ctx.assets.is_wrapped_equity_recovery_enabled(&aapl),
            "Recovery should follow wrapped_equity_recovery config"
        );
        assert!(
            !ctx.assets.is_rebalancing_enabled(&aapl),
            "Recovery-enabled symbol must not imply rebalancing is enabled"
        );
        assert!(
            !ctx.assets
                .is_wrapped_equity_recovery_enabled(&Symbol::new("UNKNOWN").unwrap()),
            "Unknown assets should default to recovery disabled"
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
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Disabled,
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
            !ctx.assets.is_trading_enabled(&Symbol::new("SPYM").unwrap()),
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
                let hex_str = format!("0x{hex_digits}");
                let parsed = parse_padded_b256(&hex_str).unwrap();

                let expected_hex = format!("{hex_digits:0>64}");
                let expected: B256 = expected_hex.parse().unwrap();
                prop_assert_eq!(parsed, expected);
            }

            /// Invalid hex characters must produce a parse error.
            #[test]
            fn padded_b256_rejects_invalid_hex(
                bad_char in "[g-zG-Z!@#$%^&*]",
                prefix in arb_hex_digits(),
            ) {
                let hex_str = format!("0x{prefix}{bad_char}");
                let result = parse_padded_b256(&hex_str);
                prop_assert!(
                    result.is_err(),
                    "Expected error for invalid hex '{hex_str}', got {result:?}",
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
                #[derive(Debug, Deserialize)]
                struct Wrapper {
                    #[allow(dead_code)]
                    mode: OperationMode,
                }

                let toml_str = format!(r#"mode = "{invalid}""#);

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
                    wrapped_equity_recovery = "disabled"
                    extended_hours_counter_trading = "disabled"
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
            assert_eq!(config.vault_ids.len(), 1);
        }

        #[test]
        fn cash_reserved_parses_positive_usd() {
            let toml_str = r#"
                rebalancing = "enabled"
                reserved = 5000.00
            "#;

            let config: CashAssetConfig = toml::from_str(toml_str).unwrap();
            let reserved = config.reserved.unwrap();
            assert!(
                reserved.inner().eq(&Usd::new(float!(5000))).unwrap(),
                "Expected $5000 reserved, got {reserved}"
            );
        }

        #[test]
        fn cash_reserved_absent_is_none() {
            let toml_str = r#"
                rebalancing = "enabled"
            "#;

            let config: CashAssetConfig = toml::from_str(toml_str).unwrap();
            assert!(config.reserved.is_none());
        }

        #[test]
        fn cash_reserved_rejects_zero() {
            let toml_str = r#"
                rebalancing = "enabled"
                reserved = 0
            "#;

            let result = toml::from_str::<CashAssetConfig>(toml_str);
            assert!(
                result.is_err(),
                "Expected error for zero reserved, got {result:?}"
            );
        }

        #[test]
        fn cash_reserved_rejects_negative() {
            let toml_str = r#"
                rebalancing = "enabled"
                reserved = -100
            "#;

            let result = toml::from_str::<CashAssetConfig>(toml_str);
            assert!(
                result.is_err(),
                "Expected error for negative reserved, got {result:?}"
            );
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
            let result = parse_padded_b256("0x");
            assert!(
                result.is_err(),
                "Expected error for empty hex string, got {result:?}"
            );
        }

        #[test]
        fn padded_b256_rejects_too_long_hex() {
            let long_hex = "a".repeat(65);
            let hex_str = format!("0x{long_hex}");
            let result = parse_padded_b256(&hex_str);
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
            ("alpaca-broker-api", "AlpacaBrokerApi"),
        ];

        for (kebab_value, variant_name) in variants {
            let toml_str = format!(
                r#"
                [evm]
                rpc_url = "http://localhost:8545"

                [broker]
                type = "{kebab_value}"
                "#,
            );

            // Only dry-run parses without extra fields;
            // alpaca broker needs credentials but the tag itself
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
            rpc_url = "http://localhost:8545"

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
            rpc_url = "http://localhost:8545"

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
        let snake_values = ["dry_run", "alpaca_broker_api"];

        for snake_value in snake_values {
            let toml_str = format!(
                r#"
                [evm]
                rpc_url = "http://localhost:8545"

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

    #[test]
    fn validate_files_accepts_valid_config_and_secrets() {
        let config = minimal_config_toml();
        let secrets = dry_run_secrets_toml();
        Ctx::validate_files(config.path(), secrets.path()).unwrap();
    }

    #[test]
    fn validate_files_accepts_example_config_and_secrets() {
        Ctx::validate_files(example_config_toml(), example_secrets_toml()).unwrap();
    }

    #[test]
    fn validate_files_rejects_extended_hours_without_counter_trading() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities.AAPL]
            tokenized_equity = "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
            tokenized_equity_derivative = "0xf4f8c66085910d583c01f3b4e44bf731d4e2c565"
            trading = "disabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"
            extended_hours_counter_trading = "enabled"

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "legacy"
            vault_owner = "0x0000000000000000000000000000000000000001"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = dry_run_secrets_toml();

        let error = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();
        assert!(
            matches!(
                error,
                CtxError::ExtendedHoursWithoutCounterTrading { ref symbol }
                    if *symbol == "AAPL"
            ),
            "Expected ExtendedHoursWithoutCounterTrading for AAPL, got {error:?}"
        );
    }

    #[test]
    fn validate_files_accepts_extended_hours_with_counter_trading() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities.AAPL]
            tokenized_equity = "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
            tokenized_equity_derivative = "0xf4f8c66085910d583c01f3b4e44bf731d4e2c565"
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"
            extended_hours_counter_trading = "enabled"

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "legacy"
            vault_owner = "0x0000000000000000000000000000000000000001"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = dry_run_secrets_toml();

        Ctx::validate_files(config.path(), secrets.path()).unwrap();
    }

    #[test]
    fn validate_files_rejects_invalid_config_toml() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            bogus_field = "should fail"

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"
        "#,
        );
        let secrets = dry_run_secrets_toml();

        let error = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();
        assert!(
            matches!(error, CtxError::ConfigToml { .. }),
            "Expected config parse error for unknown field, got {error:?}"
        );
    }

    #[test]
    fn validate_files_rejects_invalid_secrets_toml() {
        let config = minimal_config_toml();
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"
            extra_secret = "should fail"

            [broker]
            type = "dry-run"
        "#,
        );

        let error = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();
        assert!(
            matches!(error, CtxError::SecretsToml { .. }),
            "Expected secrets parse error for unknown field, got {error:?}"
        );
    }

    #[test]
    fn validate_files_rejects_missing_wallet() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"
        "#,
        );
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"

            [broker]
            type = "dry-run"
        "#,
        );

        let error = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();
        assert!(
            matches!(error, CtxError::WalletNotConfigured),
            "Expected WalletNotConfigured, got {error:?}"
        );
    }

    #[test]
    fn validate_files_rejects_wallet_config_without_secrets() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"


            [broker]
            counter_trade_slippage_bps = 100
            extended_hours_reprice_timeout_secs = 300
            extended_hours_close_flatten_window_secs = 900

            [broker.travel_rule]
            beneficiary_entity_name = "Test Corp"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
            mode = "sandbox"
        "#,
        );

        let error = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();
        assert!(
            matches!(error, CtxError::WalletSecretsMissing),
            "Expected WalletSecretsMissing, got {error:?}"
        );
    }

    #[test]
    fn validate_files_rejects_wallet_without_rpc_urls() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"


            [broker]
            counter_trade_slippage_bps = 100
            extended_hours_reprice_timeout_secs = 300
            extended_hours_close_flatten_window_secs = 900

            [broker.travel_rule]
            beneficiary_entity_name = "Test Corp"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
            mode = "sandbox"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#,
        );

        let error = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();
        assert!(
            matches!(
                error,
                CtxError::WalletMissingRpcUrl {
                    field: "base_rpc_url"
                }
            ),
            "Expected WalletMissingRpcUrl for base_rpc_url, got {error:?}"
        );
    }

    #[test]
    fn validate_files_rejects_alpaca_without_travel_rule() {
        let config = alpaca_trading_config_toml();
        let secrets = toml_file(
            r#"
            [evm]
            rpc_url = "http://localhost:8545"
            base_rpc_url = "https://base.example.com"
            ethereum_rpc_url = "https://mainnet.infura.io"

            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#,
        );

        let error = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();
        assert!(
            matches!(error, CtxError::MissingTravelRule),
            "Expected MissingTravelRule, got {error:?}"
        );
    }

    #[test]
    fn validate_files_rejects_placeholder_travel_rule() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"

            [broker.travel_rule]
            beneficiary_entity_name = "PLACEHOLDER"
        "#,
        );
        let secrets = dry_run_secrets_toml();

        let error = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();
        assert!(
            matches!(
                error,
                CtxError::InvalidTravelRule {
                    field: "beneficiary_entity_name",
                    ..
                }
            ),
            "Expected InvalidTravelRule for entity_name, got {error:?}"
        );
    }

    #[test]
    fn validate_files_rejects_zero_polling_interval() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            position_check_interval = 0

            [assets.equities]

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
           inventory_mode = "managed"
           inventory = "0x2222222222222222222222222222222222222222"
           vault_owner = "0x3333333333333333333333333333333333333333"

            deployment_block = 1
            required_confirmations = 3
            ingestion_cutoff = "safe"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = dry_run_secrets_toml();

        let error = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();
        assert!(
            matches!(error, CtxError::ZeroPollingInterval { .. }),
            "Expected ZeroPollingInterval, got {error:?}"
        );
    }
}
