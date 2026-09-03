//! Application configuration loading and validation.
//!
//! Reads plaintext config and encrypted secrets from separate TOML files,
//! validates compatibility, and assembles the runtime [`Ctx`] that the rest
//! of the application consumes.

use alloy::primitives::{Address, B256};
use clap::Parser;
use serde::Deserialize;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode};
use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use tracing::{Level, info, warn};
use url::Url;

use st0x_evm::Chain;
use st0x_execution::{
    AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, AlpacaBrokerAuth,
    DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS, FractionalShares, Positive, SupportedExecutor,
    Symbol, TimeInForce,
};
use st0x_finance::Usdc;
use st0x_float_macro::float;

#[cfg(any(test, feature = "test-support"))]
use crate::InventoryAdapters;
#[cfg(any(test, feature = "test-support"))]
use crate::chain::TradingChain;
use crate::pricing::PricingSecrets;
use crate::wallet::{SigningChain, SigningChains};
use crate::{
    AlertsConfig, AlertsCtx, BotGasValuationConfig, ChainConfig, ChainEquityAsset, ChainRegistry,
    ChainSecrets, ExecutionThreshold, HedgingAssets, InvalidThresholdError, OperationMode,
    OrchestratorConfig, PricingConfig, PricingCtx, PricingCtxError, RebalancingConfig,
    RebalancingCtx, RebalancingCtxError, TelemetryConfig, TelemetryCtx,
};

/// Alpaca minimum execution threshold: $2.
static ALPACA_MIN_DOLLARS: LazyLock<Usdc> = LazyLock::new(|| Usdc::new(float!(2)));

/// Dry-run minimum execution threshold: 1 share.
static DRY_RUN_MIN_SHARES: LazyLock<Positive<FractionalShares>> = LazyLock::new(|| {
    Positive::new(FractionalShares::new(float!(1))).unwrap_or_else(|_| unreachable!())
});
const MIN_COUNTER_TRADE_SLIPPAGE_BPS: u16 = 1;
const MAX_EXTENDED_HOURS_REPRICE_TIMEOUT_SECS: u64 =
    chrono::TimeDelta::MAX.num_seconds().unsigned_abs();
/// Same bound as the reprice timeout: `CloseFlattenPolicy::from_secs` builds
/// a `chrono::Duration` from this value, which fails with an opaque
/// `OutOfRangeError` past `chrono::TimeDelta::MAX`. Bounding it here at
/// config-load time gives a clear, actionable error instead.
const MAX_EXTENDED_HOURS_CLOSE_FLATTEN_WINDOW_SECS: u64 =
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

/// A migration/deprecation notice produced while parsing config + secrets.
///
/// Parsing runs before any tracing subscriber exists (the server and CLI
/// configure their subscriber FROM the parsed [`Ctx`]), so a `warn!` emitted
/// during parsing dispatches to `NoSubscriber` and silently vanishes. The
/// parse collects notices instead; the binaries emit them the moment logging
/// is up ([`Ctx::emit_startup_notices`]), and `validate-config` prints them
/// to stderr.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupNotice {
    pub level: StartupNoticeLevel,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupNoticeLevel {
    Info,
    Warn,
}

impl StartupNotice {
    pub(crate) fn info(message: impl Into<String>) -> Self {
        Self {
            level: StartupNoticeLevel::Info,
            message: message.into(),
        }
    }

    pub(crate) fn warning(message: impl Into<String>) -> Self {
        Self {
            level: StartupNoticeLevel::Warn,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for StartupNotice {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let level = match self.level {
            StartupNoticeLevel::Info => "info",
            StartupNoticeLevel::Warn => "warning",
        };
        write!(formatter, "{level}: {message}", message = self.message)
    }
}

/// Candidate-config symbol sets consumed by the deploy-time database verifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentSymbolPolicy {
    configured: BTreeSet<Symbol>,
    retired: BTreeSet<Symbol>,
}

impl DeploymentSymbolPolicy {
    pub fn new(
        configured: impl IntoIterator<Item = Symbol>,
        retired: impl IntoIterator<Item = Symbol>,
    ) -> Result<Self, CtxError> {
        let configured = configured.into_iter().collect::<BTreeSet<_>>();
        let mut retired_symbols = BTreeSet::new();

        for symbol in retired {
            if !retired_symbols.insert(symbol.clone()) {
                return Err(CtxError::DuplicateRetiredSymbol { symbol });
            }
            if configured.contains(&symbol) {
                return Err(CtxError::ConfiguredSymbolMarkedRetired { symbol });
            }
        }

        Ok(Self {
            configured,
            retired: retired_symbols,
        })
    }

    pub fn configured(&self) -> &BTreeSet<Symbol> {
        &self.configured
    }

    pub fn retired(&self) -> &BTreeSet<Symbol> {
        &self.retired
    }
}

#[derive(Deserialize)]
struct DeploymentConfig {
    assets: HedgingAssets,
}

/// Reads only the public asset section needed by the deploy-time symbol gate.
///
/// The full config/secrets validation remains the responsibility of
/// `validate-config`; this loader performs no network or secret access.
pub fn load_deployment_symbol_policy(
    config_path: &Path,
) -> Result<DeploymentSymbolPolicy, CtxError> {
    let config_str = std::fs::read_to_string(config_path).map_err(|source| CtxError::ConfigIo {
        path: config_path.to_path_buf(),
        source,
    })?;
    let config: DeploymentConfig =
        toml::from_str(&config_str).map_err(|source| CtxError::ConfigToml {
            path: config_path.to_path_buf(),
            source,
        })?;

    DeploymentSymbolPolicy::new(
        config.assets.equities.symbols.into_keys(),
        config.assets.equities.retired_symbols,
    )
}

/// Validated, network-free inputs required by the deploy-time Turnkey approval
/// policy coverage check.
#[cfg(feature = "wallet-turnkey")]
#[derive(Clone, Debug)]
pub struct TurnkeyApprovalPolicyInputs {
    pub organization_id: st0x_evm::turnkey::TurnkeyOrganizationId,
    pub kms_api_key: Option<st0x_evm::turnkey::TurnkeyKmsApiKey>,
    pub api_private_key: Option<st0x_evm::turnkey::TurnkeyApiPrivateKey>,
    pub wallet_address: Address,
    pub orderbook: Address,
    pub assets: crate::ChainAssets,
}

/// Non-secret settings deserialized from the plaintext config TOML.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    database_url: String,
    log_level: Option<LogLevel>,
    log_dir: Option<String>,
    log_format: Option<LogFormat>,
    log_query_url_template: Option<String>,
    server_port: u16,
    board_port: u16,
    /// One table per chain the bot acts on. Replaces the single unnamed
    /// `[raindex]` section, which could only ever describe one chain.
    chains: BTreeMap<Chain, ChainConfig>,
    order_polling_interval: Option<u64>,
    order_polling_max_jitter: Option<u64>,
    position_check_interval: Option<u64>,
    inventory_poll_interval: Option<u64>,
    inventory_divergence_threshold: NonZeroU32,
    hedge_order_gate_reconciliation_timeout_secs: NonZeroU64,
    order_fill_poll_interval: Option<u64>,
    apalis_finished_job_cleanup_interval_secs: u64,
    telemetry: Option<TelemetryConfig>,
    alerts: Option<AlertsConfig>,
    pricing: Option<PricingConfig>,
    rebalancing: Option<RebalancingConfig>,
    wallet: Option<toml::Value>,
    broker: Option<BrokerConfig>,
    #[serde(default)]
    assets: HedgingAssets,
    rest_api: Option<RestApiUrlConfig>,
    /// Non-secret issuance settings (`base_url`). The issuance `api_key`
    /// stays in the secrets file.
    issuance: Option<IssuanceConfig>,
    ops_api: Option<OpsApiConfig>,
    /// ETH/USD valuation source for bot-gas cost recording. See
    /// [`Ctx::bot_gas_valuation`] for when this is required.
    bot_gas_valuation: Option<BotGasValuationConfig>,
    /// Per-network ST0xOrchestrator contract addresses. See
    /// [`Ctx::orchestrator`].
    orchestrator: Option<OrchestratorConfig>,
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
    /// MIGRATION SHIM, removed next release: `base_url` is not a secret and
    /// now lives in the config file's `[issuance]` section. The deprecated
    /// secrets-file copy is still accepted for one release because deployed
    /// secret versions carry it; [`issuance_ctx`] warns when it is used and
    /// refuses a value that conflicts with the config one.
    base_url: Option<Url>,
    api_key: String,
}

/// Non-secret issuance settings from the plaintext config: where the
/// internal status API lives. The `api_key` stays in the secrets file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct IssuanceConfig {
    base_url: Url,
}

/// Validated issuance secrets with the API key parsed into a [`B256`].
struct IssuanceSecrets {
    /// Deprecated location; see [`IssuanceSecretsToml::base_url`].
    base_url: Option<Url>,
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

/// IAP audiences for the role-gated ops API paths.
///
/// Each audience names the load balancer backend service that fronts one role
/// prefix (`terraform/staging-liquidity/ops-api-iap.tf`, published as the
/// `ops_api_audiences` output). IAP binds the token it mints to the backend
/// that admitted the caller, so pinning the audience per prefix is what makes
/// a read-tier token useless against the write path.
///
/// Not a secret: these name IAM-gated backends and are worthless without an
/// identity Google will sign for. Absent from config means the role-gated
/// routes are not mounted at all, which is the correct posture for any
/// deployment that has no load balancer in front of it.
#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OpsApiConfig {
    /// Audience of the backend serving `/liquidity-read/*`.
    pub read_audience: String,
    /// Audience of the backend serving `/liquidity-write/*`.
    pub write_audience: String,
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

/// Non-secret broker settings from the plaintext config TOML.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BrokerConfig {
    /// Which executor backs hedging. This is the broker's identity, not a
    /// credential, so it lives here; only actual credentials (the legacy
    /// `api_key`/`api_secret` pair) stay in the secrets file. `Option` for
    /// one release while the deprecated secrets-file copy of the identity is
    /// still accepted; [`resolve_broker`] requires it from one of the two.
    #[serde(rename = "type")]
    kind: Option<BrokerKind>,
    /// Alpaca environment (`production`/`sandbox`). Identity, not secret.
    mode: Option<AlpacaBrokerApiMode>,
    /// Alpaca account identifier. Identity, not secret.
    account_id: Option<AlpacaAccountId>,
    /// KMS broker only: the BrokerDash credential the KMS key signs for.
    client_id: Option<String>,
    /// KMS broker only: fully-qualified Cloud KMS key version resource name.
    kms_key_version: Option<String>,
    counter_trade_slippage_bps: Option<u16>,
    extended_hours_reprice_timeout_secs: Option<u64>,
    close_flatten_reprice_timeout_secs: Option<u64>,
    extended_hours_close_flatten_window_secs: Option<u64>,
    travel_rule: Option<TravelRuleConfig>,
    close_flatten_cross_max_bps: Option<u16>,
}

/// The broker identity tag: which executor backs hedging, minus its
/// credentials. Mirrors the (deprecated) `type` tag of the secrets-file
/// `[broker]` table so the two locations can be cross-checked during the
/// migration release.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum BrokerKind {
    AlpacaBrokerApi,
    AlpacaBrokerApiKms,
    DryRun,
}

impl BrokerKind {
    /// The kebab-case tag as written in the TOML, for error messages.
    fn as_str(self) -> &'static str {
        match self {
            Self::AlpacaBrokerApi => "alpaca-broker-api",
            Self::AlpacaBrokerApiKms => "alpaca-broker-api-kms",
            Self::DryRun => "dry-run",
        }
    }
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

    fn extended_hours_reprice_timeout_secs(&self) -> Result<NonZeroU64, CtxError> {
        let configured = self
            .extended_hours_reprice_timeout_secs
            .ok_or(CtxError::MissingExtendedHoursRepriceTimeout)?;

        NonZeroU64::new(configured)
            .filter(|timeout| timeout.get() <= MAX_EXTENDED_HOURS_REPRICE_TIMEOUT_SECS)
            .ok_or(CtxError::ExtendedHoursRepriceTimeoutOutOfRange {
                configured,
                max: MAX_EXTENDED_HOURS_REPRICE_TIMEOUT_SECS,
            })
    }

    fn close_flatten_reprice_timeout_secs(&self) -> Result<u64, CtxError> {
        let configured = self
            .close_flatten_reprice_timeout_secs
            .ok_or(CtxError::MissingCloseFlattenRepriceTimeout)?;

        if configured == 0 || configured > MAX_EXTENDED_HOURS_REPRICE_TIMEOUT_SECS {
            return Err(CtxError::CloseFlattenRepriceTimeoutOutOfRange {
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

        if configured == 0 || configured > MAX_EXTENDED_HOURS_CLOSE_FLATTEN_WINDOW_SECS {
            return Err(CtxError::ExtendedHoursCloseFlattenWindowOutOfRange {
                configured,
                max: MAX_EXTENDED_HOURS_CLOSE_FLATTEN_WINDOW_SECS,
            });
        }

        Ok(configured)
    }

    /// The cross a close-flatten hedge reaches at the extended-session close.
    ///
    /// Placement ramps linearly from `ramp_base_bps` at the start of the window
    /// to this value at the close, so it must be at least the base or the ramp
    /// would run backwards (ADR 0019). The caller supplies the effective
    /// runtime base from [`BrokerCtx::counter_trade_slippage_bps`], not the raw
    /// configured field: DryRun uses the executor default, so validating against
    /// the configured value would admit an inverted ramp.
    fn close_flatten_cross_max_bps(&self, ramp_base_bps: u16) -> Result<u16, CtxError> {
        let configured = self
            .close_flatten_cross_max_bps
            .ok_or(CtxError::MissingCloseFlattenCrossMaxBps)?;

        if configured < ramp_base_bps || configured > MAX_COUNTER_TRADE_SLIPPAGE_BPS {
            return Err(CtxError::CloseFlattenCrossMaxBpsOutOfRange {
                configured,
                min: ramp_base_bps,
                max: MAX_COUNTER_TRADE_SLIPPAGE_BPS,
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
    chains: BTreeMap<Chain, ChainSecrets>,
    /// `Option` because the KMS and dry-run brokers carry no credentials at
    /// all: their identity lives in the config file's `[broker]` section and
    /// the secrets file then has no `[broker]` table. Only the legacy
    /// alpaca-broker-api variant still needs one, for its
    /// `api_key`/`api_secret` credential pair.
    broker: Option<BrokerSecrets>,
    /// MIGRATION SHIM, removed next release: the Telegram alert transport is
    /// retired (alerts are structured logs now), but currently-deployed
    /// secret versions still carry an `[alerts]` table with `bot_token`.
    /// Accept and ignore it for one release so those secrets keep parsing
    /// under `deny_unknown_fields` at rollout; `parse_and_validate` warns
    /// when it is present.
    alerts: Option<toml::Value>,
    pricing: Option<PricingSecrets>,
    wallet: Option<toml::Value>,
    rest_api: Option<RestApiSecrets>,
    issuance: Option<IssuanceSecretsToml>,
}

/// The `[broker]` table of the secrets TOML.
///
/// Only the legacy alpaca-broker-api credential pair belongs here. Every
/// other field is identity, whose home is now the config file's `[broker]`
/// section; the copies below are MIGRATION SHIMS, removed next release,
/// accepted because deployed secret versions still carry the identity here.
/// [`resolve_broker`] warns when identity comes from this table and refuses
/// values that conflict with the config file's.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
#[allow(clippy::large_enum_variant)] // isn't relevant for a brief startup step
enum BrokerSecrets {
    AlpacaBrokerApi {
        api_key: String,
        api_secret: String,
        /// MIGRATION SHIM, removed next release: moved to config `[broker]`.
        account_id: Option<AlpacaAccountId>,
        /// MIGRATION SHIM, removed next release: moved to config `[broker]`.
        mode: Option<AlpacaBrokerApiMode>,
    },
    /// Keyless variant: no stored credential, so the entire variant is a
    /// MIGRATION SHIM in the secrets file (removed next release) -- its
    /// fields' home is the config `[broker]` section. The client id names
    /// the BrokerDash credential whose public half is the named Cloud KMS
    /// key; the bot signs an RFC 7523 client assertion per token request
    /// (see st0x-execution's `kms_jwt`).
    AlpacaBrokerApiKms {
        client_id: String,
        kms_key_version: String,
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
    pub log_format: LogFormat,
    /// Log query link printed by CLI transfer commands, with `{id}`
    /// substituted. `None` prints nothing.
    pub log_query_url_template: Option<LogQueryUrlTemplate>,
    pub server_port: u16,
    pub board_port: u16,
    /// Every chain the bot acts on. Read the trading chain out of it with
    /// [`ChainRegistry::sole_trading`].
    pub chains: ChainRegistry,
    pub order_polling_interval: u64,
    pub order_polling_max_jitter: u64,
    pub position_check_interval: u64,
    pub inventory_poll_interval: u64,
    /// Consecutive offchain polls that must diverge from the inventory view's
    /// Hedging balance for a symbol before the poller escalates a forced
    /// snapshot reconciliation. Required and nonzero: a missing value must
    /// fail config parsing rather than silently defaulting.
    pub inventory_divergence_threshold: NonZeroU32,
    /// Maximum duration (seconds) the inventory poller may wait for durable
    /// Position state while holding the inventory write lock to reconcile
    /// hedge-order gates. Required and nonzero.
    pub hedge_order_gate_reconciliation_timeout_secs: NonZeroU64,
    /// Interval (seconds) between continuous `eth_getLogs` polls for orderbook
    /// fills. Each tick enqueues a backfill range over the unprocessed blocks
    /// (capped at the chain's latest finalized block).
    pub order_fill_poll_interval: u64,
    /// Maximum age (seconds) for a live extended-hours limit hedge before it is
    /// cancelled so the next scan can place a fresh marketable limit. `None`
    /// is valid only for DryRun with no extended-hours-enabled assets; loaded
    /// Alpaca and extended-hours-enabled DryRun contexts always contain `Some`.
    pub extended_hours_reprice_timeout_secs: Option<NonZeroU64>,
    /// Maximum age (seconds) for a close-flatten limit hedge before it is
    /// cancelled and repriced further along the widening cross ramp.
    pub close_flatten_reprice_timeout_secs: u64,
    /// Window (seconds) before a long-gap extended-session close during which
    /// the bot repeatedly cancels, refreshes, and replaces executable residual
    /// exposure with quote-crossing limits.
    pub extended_hours_close_flatten_window_secs: u64,
    /// Cross a close-flatten hedge ramps to at the extended-session close,
    /// starting from `counter_trade_slippage_bps` at the window's start.
    pub close_flatten_cross_max_bps: u16,
    pub apalis_finished_job_cleanup_interval_secs: u64,
    pub broker: BrokerCtx,
    pub telemetry: Option<TelemetryCtx>,
    /// Gas-balance alerting and transfer-readiness context. Optional in
    /// standalone mode and required in rebalancing mode.
    pub alerts: Option<AlertsCtx>,
    /// Notices collected during parsing, before any tracing subscriber
    /// existed. Emit via [`Ctx::emit_startup_notices`] once logging is up.
    pub startup_notices: Vec<StartupNotice>,
    /// Live reference prices used exclusively by dashboard USD valuations.
    pub pricing: Option<PricingCtx>,
    pub trading_mode: TradingMode,
    /// The onchain address that owns orders on the orderbook.
    /// Always derived from the configured `[wallet]` address.
    pub order_owner: Address,
    pub wallet: Option<crate::wallet::OnchainWalletCtx>,
    /// Non-secret wallet metadata for the dashboard config dialog.
    pub wallet_meta: Option<WalletMeta>,
    pub execution_threshold: ExecutionThreshold,
    /// What the bot hedges and how. Where each symbol is listed on-chain
    /// lives on the chain registry, not here.
    pub assets: HedgingAssets,
    pub travel_rule: Option<TravelRuleConfig>,
    pub rest_api: Option<RestApiCtx>,
    /// IAP audiences per ops-API role. `None` leaves the role-gated routes
    /// unmounted.
    pub ops_api: Option<OpsApiConfig>,
    pub issuance: IssuanceStatusCtx,
    /// Alpaca redemption wallet from `[chains.<name>.trading].redemption_wallet`.
    /// `Some` when the config includes a `[chains.<name>.trading].redemption_wallet` section.
    pub redemption_wallet: Option<Address>,
    /// ETH/USD valuation source for bot-gas cost recording (ADR 0020).
    /// Bot-gas cost recording only runs on rebalancing paths (vault
    /// deposit/withdraw, wrap/unwrap, CCTP burn/mint, USDC transfer), so this
    /// is required (validated at startup) whenever `[rebalancing]` is
    /// configured (`TradingMode::Rebalancing`) and otherwise optional --
    /// including in Standalone mode, where an operator may still configure it
    /// even though no rebalancing path will ever enqueue to it.
    pub bot_gas_valuation: Option<BotGasValuationConfig>,
    /// Per-network ST0xOrchestrator contract addresses from
    /// `[orchestrator.addresses]`, needed to sign `MintAuthV1` recipient
    /// authorizations for orchestrator-mode mints. `Some` when the config
    /// includes an `[orchestrator]` section. Optional so the bot runs
    /// unchanged while every asset is vault-direct; a mint that discovers an
    /// orchestrator-mode asset with no address for its chain must fail
    /// loudly, never guess an address.
    pub orchestrator: Option<OrchestratorConfig>,
}

/// Runtime broker configuration assembled from the config file's `[broker]`
/// identity plus the secrets file's credentials (see `resolve_broker` in
/// this module).
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

    /// Returns the slippage band the runtime uses as the base of extended-hours
    /// counter-trade pricing.
    #[must_use]
    pub fn counter_trade_slippage_bps(&self) -> u16 {
        match self {
            Self::AlpacaBrokerApi(ctx) => ctx.counter_trade_slippage_bps,
            Self::DryRun => DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
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
    fn alpaca_ctx(
        auth: AlpacaBrokerAuth,
        account_id: AlpacaAccountId,
        mode: Option<AlpacaBrokerApiMode>,
        broker_config: Option<&BrokerConfig>,
    ) -> Result<Self, CtxError> {
        // Unwrap the section once: a per-field `ok_or` would make the
        // error reported for a wholly missing `[broker]` depend on
        // field declaration order, and every arm after the first
        // would be unreachable.
        let broker_config = broker_config.ok_or(CtxError::MissingCounterTradeSlippageBps)?;

        Ok(Self::AlpacaBrokerApi(AlpacaBrokerApiCtx {
            auth,
            account_id,
            mode,
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
            counter_trade_slippage_bps: broker_config.counter_trade_slippage_bps()?,
        }))
    }
}

/// The legacy Alpaca credential pair: the only genuinely secret part of the
/// broker configuration, and therefore the only part that stays in the
/// secrets file.
struct AlpacaCredentials {
    api_key: String,
    api_secret: String,
}

/// The secrets-file `[broker]` table flattened into identity fields plus the
/// credential pair, so [`resolve_broker`] can merge the (deprecated)
/// identity copies field-by-field with the config file's.
struct SecretsBrokerParts {
    kind: BrokerKind,
    mode: Option<AlpacaBrokerApiMode>,
    account_id: Option<AlpacaAccountId>,
    client_id: Option<String>,
    kms_key_version: Option<String>,
    credentials: Option<AlpacaCredentials>,
}

impl SecretsBrokerParts {
    /// The deprecation notice for what this secrets `[broker]` table must
    /// shed before next release, naming exactly what was seen -- or `None`
    /// when the table is already in its end-state shape (legacy tag +
    /// credential pair only).
    ///
    /// For the keyless and dry-run brokers the table itself is the
    /// deprecated artifact: their identity's home is the config file and
    /// they have no credentials, so even a tag-only `type = "dry-run"`
    /// table must go.
    fn deprecation_notice(&self) -> Option<StartupNotice> {
        match self.kind {
            BrokerKind::AlpacaBrokerApiKms | BrokerKind::DryRun => {
                Some(StartupNotice::warning(format!(
                    "the secrets file's [broker] table (type = \"{kind}\") is deprecated: \
                     this broker type has no credentials, so declare its identity in the \
                     config file's [broker] section and remove the table from the secrets \
                     file (removed next release)",
                    kind = self.kind.as_str(),
                )))
            }
            BrokerKind::AlpacaBrokerApi => {
                let seen: Vec<&str> = [
                    ("account_id", self.account_id.is_some()),
                    ("mode", self.mode.is_some()),
                ]
                .into_iter()
                .filter_map(|(field, present)| present.then_some(field))
                .collect();

                if seen.is_empty() {
                    return None;
                }

                Some(StartupNotice::warning(format!(
                    "[broker] {fields} in the secrets file are deprecated and move to the \
                     config file's [broker] section; only type/api_key/api_secret stay in \
                     the secrets file (removed next release)",
                    fields = seen.join("/"),
                )))
            }
        }
    }
}

impl From<BrokerSecrets> for SecretsBrokerParts {
    fn from(secrets: BrokerSecrets) -> Self {
        match secrets {
            BrokerSecrets::AlpacaBrokerApi {
                api_key,
                api_secret,
                account_id,
                mode,
            } => Self {
                kind: BrokerKind::AlpacaBrokerApi,
                mode,
                account_id,
                client_id: None,
                kms_key_version: None,
                credentials: Some(AlpacaCredentials {
                    api_key,
                    api_secret,
                }),
            },
            BrokerSecrets::AlpacaBrokerApiKms {
                client_id,
                kms_key_version,
                account_id,
                mode,
            } => Self {
                kind: BrokerKind::AlpacaBrokerApiKms,
                mode,
                account_id: Some(account_id),
                client_id: Some(client_id),
                kms_key_version: Some(kms_key_version),
                credentials: None,
            },
            BrokerSecrets::DryRun => Self {
                kind: BrokerKind::DryRun,
                mode: None,
                account_id: None,
                client_id: None,
                kms_key_version: None,
                credentials: None,
            },
        }
    }
}

/// Merges one broker-identity field from its two possible locations. Both
/// present and equal is tolerated (the migration window where the config
/// release has landed but the secret still carries the old copy); both
/// present and different is refused rather than silently picking one.
fn merge_broker_field<Value: PartialEq>(
    field: &'static str,
    config_value: Option<Value>,
    secrets_value: Option<Value>,
) -> Result<Option<Value>, CtxError> {
    match (config_value, secrets_value) {
        (Some(from_config), Some(from_secrets)) => {
            if from_config == from_secrets {
                Ok(Some(from_config))
            } else {
                Err(CtxError::BrokerIdentityConflict { field })
            }
        }
        (Some(value), None) | (None, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

/// Refuses identity fields that have no meaning for the resolved broker
/// type: a set-but-never-read field is a misconfiguration that should fail
/// startup, not be silently ignored.
fn refuse_broker_fields_not_for_kind(
    kind: BrokerKind,
    fields: &[(&'static str, bool)],
) -> Result<(), CtxError> {
    for &(field, present) in fields {
        if present {
            return Err(CtxError::BrokerFieldNotForKind {
                field,
                kind: kind.as_str(),
            });
        }
    }

    Ok(())
}

/// Assembles the broker from its two halves: identity in the config file
/// (preferred home) and credentials in the secrets file. The deprecated
/// identity copies in the secrets file are still accepted for one release
/// (see [`BrokerSecrets`]); a field set differently in both places is
/// refused.
/// The merged broker identity, ready for per-kind validation. Exists so the
/// `match` arms in [`resolve_broker`] destructure it EXHAUSTIVELY: adding an
/// identity field breaks compilation in every arm instead of being silently
/// ignored for some broker type.
struct ResolvedIdentity {
    mode: Option<AlpacaBrokerApiMode>,
    account_id: Option<AlpacaAccountId>,
    client_id: Option<String>,
    kms_key_version: Option<String>,
    credentials: Option<AlpacaCredentials>,
}

fn resolve_broker(
    broker_config: Option<&BrokerConfig>,
    secrets: Option<BrokerSecrets>,
    startup_notices: &mut Vec<StartupNotice>,
) -> Result<BrokerCtx, CtxError> {
    let secrets_parts = secrets.map(SecretsBrokerParts::from);

    // Migration shim, removed next release: see `BrokerSecrets`.
    if let Some(notice) = secrets_parts
        .as_ref()
        .and_then(SecretsBrokerParts::deprecation_notice)
    {
        startup_notices.push(notice);
    }

    let kind = merge_broker_field(
        "type",
        broker_config.and_then(|config| config.kind),
        secrets_parts.as_ref().map(|parts| parts.kind),
    )?
    .ok_or(CtxError::MissingBrokerType)?;

    let SecretsBrokerParts {
        kind: _,
        mode: secrets_mode,
        account_id: secrets_account_id,
        client_id: secrets_client_id,
        kms_key_version: secrets_kms_key_version,
        credentials,
    } = secrets_parts.unwrap_or(SecretsBrokerParts {
        kind,
        mode: None,
        account_id: None,
        client_id: None,
        kms_key_version: None,
        credentials: None,
    });

    let mode = merge_broker_field(
        "mode",
        broker_config.and_then(|config| config.mode.clone()),
        secrets_mode,
    )?;
    let account_id = merge_broker_field(
        "account_id",
        broker_config.and_then(|config| config.account_id),
        secrets_account_id,
    )?;
    let client_id = merge_broker_field(
        "client_id",
        broker_config.and_then(|config| config.client_id.clone()),
        secrets_client_id,
    )?;
    let kms_key_version = merge_broker_field(
        "kms_key_version",
        broker_config.and_then(|config| config.kms_key_version.clone()),
        secrets_kms_key_version,
    )?;

    let identity = ResolvedIdentity {
        mode,
        account_id,
        client_id,
        kms_key_version,
        credentials,
    };

    // Every arm destructures `ResolvedIdentity` exhaustively (no `..`), so a
    // newly added identity field fails to compile until each broker type
    // decides whether to require, allow, or refuse it.
    match kind {
        BrokerKind::DryRun => {
            let ResolvedIdentity {
                mode,
                account_id,
                client_id,
                kms_key_version,
                credentials,
            } = identity;

            refuse_broker_fields_not_for_kind(
                kind,
                &[
                    ("mode", mode.is_some()),
                    ("account_id", account_id.is_some()),
                    ("client_id", client_id.is_some()),
                    ("kms_key_version", kms_key_version.is_some()),
                    ("api_key/api_secret", credentials.is_some()),
                ],
            )?;

            Ok(BrokerCtx::DryRun)
        }
        BrokerKind::AlpacaBrokerApi => {
            let ResolvedIdentity {
                mode,
                account_id,
                client_id,
                kms_key_version,
                credentials,
            } = identity;

            refuse_broker_fields_not_for_kind(
                kind,
                &[
                    ("client_id", client_id.is_some()),
                    ("kms_key_version", kms_key_version.is_some()),
                ],
            )?;

            let AlpacaCredentials {
                api_key,
                api_secret,
            } = credentials.ok_or(CtxError::MissingBrokerCredentials)?;
            let account_id = account_id.ok_or(CtxError::MissingBrokerField {
                field: "account_id",
            })?;

            BrokerCtx::alpaca_ctx(
                AlpacaBrokerAuth::Basic {
                    api_key,
                    api_secret,
                },
                account_id,
                mode,
                broker_config,
            )
        }
        BrokerKind::AlpacaBrokerApiKms => {
            let ResolvedIdentity {
                mode,
                account_id,
                client_id,
                kms_key_version,
                credentials,
            } = identity;

            refuse_broker_fields_not_for_kind(
                kind,
                &[("api_key/api_secret", credentials.is_some())],
            )?;

            // The keyless mint targets the LIVE authx token endpoint; a
            // sandbox (or mock) broker mode paired with it would silently
            // mint production bearer tokens. Fail loud until a sandbox
            // authx URL is wired through. Exhaustive so a future mode
            // forces an explicit keyless decision.
            match &mode {
                Some(AlpacaBrokerApiMode::Production) => {}
                None | Some(_) => {
                    return Err(CtxError::KmsBrokerRequiresProductionMode);
                }
            }

            let client_id = client_id.ok_or(CtxError::MissingBrokerField { field: "client_id" })?;
            let kms_key_version = kms_key_version.ok_or(CtxError::MissingBrokerField {
                field: "kms_key_version",
            })?;
            let account_id = account_id.ok_or(CtxError::MissingBrokerField {
                field: "account_id",
            })?;

            BrokerCtx::alpaca_ctx(
                AlpacaBrokerAuth::KmsJwt {
                    client_id,
                    kms_key_version,
                },
                account_id,
                mode,
                broker_config,
            )
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
            .field("log_format", &self.log_format)
            .field("log_query_url_template", &self.log_query_url_template)
            .field("server_port", &self.server_port)
            .field("board_port", &self.board_port)
            .field("chains", &self.chains)
            .field("order_polling_interval", &self.order_polling_interval)
            .field("order_polling_max_jitter", &self.order_polling_max_jitter)
            .field("position_check_interval", &self.position_check_interval)
            .field("inventory_poll_interval", &self.inventory_poll_interval)
            .field(
                "inventory_divergence_threshold",
                &self.inventory_divergence_threshold,
            )
            .field(
                "hedge_order_gate_reconciliation_timeout_secs",
                &self.hedge_order_gate_reconciliation_timeout_secs,
            )
            .field("order_fill_poll_interval", &self.order_fill_poll_interval)
            .field(
                "extended_hours_reprice_timeout_secs",
                &self.extended_hours_reprice_timeout_secs,
            )
            .field(
                "close_flatten_reprice_timeout_secs",
                &self.close_flatten_reprice_timeout_secs,
            )
            .field(
                "extended_hours_close_flatten_window_secs",
                &self.extended_hours_close_flatten_window_secs,
            )
            .field(
                "close_flatten_cross_max_bps",
                &self.close_flatten_cross_max_bps,
            )
            .field(
                "apalis_finished_job_cleanup_interval_secs",
                &self.apalis_finished_job_cleanup_interval_secs,
            )
            .field("broker", &self.broker)
            .field("telemetry", &self.telemetry)
            .field("alerts", &self.alerts)
            .field("startup_notices", &self.startup_notices)
            .field("pricing", &self.pricing)
            .field("trading_mode", &self.trading_mode)
            .field("order_owner", &self.order_owner)
            .field("wallet_configured", &self.wallet.is_some())
            .field("wallet_meta", &self.wallet_meta)
            .field("execution_threshold", &self.execution_threshold)
            .field("assets", &self.assets)
            .field("travel_rule_configured", &self.travel_rule.is_some())
            .field("redemption_wallet", &self.redemption_wallet)
            .field("rest_api", &self.rest_api)
            .field("ops_api", &self.ops_api)
            .field("issuance", &self.issuance)
            .field("bot_gas_valuation", &self.bot_gas_valuation)
            .field("orchestrator", &self.orchestrator);

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

/// Console log output format. `text` is the human readable format; `json`
/// emits one JSON object per line, the same shape as the rolling file
/// layer, so a log shipper can parse journald output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Text,
    Json,
}

/// Placeholder in `log_query_url_template` replaced with the transfer or
/// aggregate id.
const LOG_QUERY_ID_PLACEHOLDER: &str = "{id}";

/// Log query link template printed by CLI transfer commands.
///
/// Construction proves the template contains the `{id}` placeholder and
/// parses as a URL once the placeholder is substituted, so consumers can
/// call [`Self::substitute`] without re-validating either half.
#[derive(Clone, Debug)]
pub struct LogQueryUrlTemplate(String);

impl LogQueryUrlTemplate {
    /// Validates that `template` carries the `{id}` placeholder and parses as
    /// a URL once the placeholder is substituted.
    pub fn parse(template: String) -> Result<Self, CtxError> {
        if !template.contains(LOG_QUERY_ID_PLACEHOLDER) {
            return Err(CtxError::LogQueryUrlTemplateMissingIdPlaceholder);
        }

        let sample = template.replace(LOG_QUERY_ID_PLACEHOLDER, "sample-id");
        if let Err(source) = Url::parse(&sample) {
            return Err(CtxError::LogQueryUrlTemplateNotAUrl { source });
        }

        Ok(Self(template))
    }

    /// The template with `{id}` replaced by `id`.
    #[must_use]
    pub fn substitute(&self, id: &str) -> String {
        let Self(template) = self;
        template.replace(LOG_QUERY_ID_PLACEHOLDER, id)
    }
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
    log_format: LogFormat,
    log_query_url_template: Option<LogQueryUrlTemplate>,
    server_port: u16,
    board_port: u16,
    chains: ChainRegistry,
    order_polling_interval: u64,
    order_polling_max_jitter: u64,
    position_check_interval: u64,
    inventory_poll_interval: u64,
    inventory_divergence_threshold: NonZeroU32,
    hedge_order_gate_reconciliation_timeout_secs: NonZeroU64,
    order_fill_poll_interval: u64,
    extended_hours_reprice_timeout_secs: Option<NonZeroU64>,
    close_flatten_reprice_timeout_secs: u64,
    extended_hours_close_flatten_window_secs: u64,
    close_flatten_cross_max_bps: u16,
    apalis_finished_job_cleanup_interval_secs: u64,
    broker: BrokerCtx,
    telemetry: Option<TelemetryCtx>,
    alerts: Option<AlertsCtx>,
    startup_notices: Vec<StartupNotice>,
    pricing: Option<PricingCtx>,
    execution_threshold: ExecutionThreshold,
    trading_mode: TradingMode,
    assets: HedgingAssets,
    travel_rule: Option<TravelRuleConfig>,
    rest_api: Option<RestApiCtx>,
    ops_api: Option<OpsApiConfig>,
    issuance: IssuanceStatusCtx,
    redemption_wallet: Option<Address>,
    bot_gas_valuation: Option<BotGasValuationConfig>,
    orchestrator: Option<OrchestratorConfig>,
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
    /// Endpoint and confirmation depth per chain the wallet signs on.
    signing_chains: SigningChains,
}

/// Non-secret wallet metadata extracted from the config TOML during
/// parsing. Displayed on the dashboard config dialog.
#[derive(Clone, Debug, Deserialize)]
pub struct WalletMeta {
    pub kind: String,
    pub address: Address,
    pub organization_id: Option<String>,
}

/// Validates the config/secrets pairing and RPC prerequisites for wallet
/// construction without connecting to any chain.
///
/// The wallet signs on every chain the bot holds funds on, including the ones
/// it does not trade on, so each of those needs a `[chains.<name>]` entry
/// carrying its `rpc_url`.
fn validate_wallet_inputs(
    wallet_config: Option<toml::Value>,
    wallet_secrets: Option<toml::Value>,
    chains: &ChainRegistry,
    config_path: &Path,
) -> Result<(WalletInputs, WalletMeta), CtxError> {
    match (wallet_config, wallet_secrets) {
        (Some(wallet_config), wallet_secrets) => {
            // Credential-bearing backends require wallet secrets. A
            // KMS-stamped Turnkey wallet is the sole exception because its
            // credential is an IAM-gated KMS key in plaintext config.
            let wallet_secrets = match wallet_secrets {
                Some(wallet_secrets) => wallet_secrets,
                None if wallet_config.get("kms_api_key").is_some() => {
                    toml::Value::Table(toml::map::Map::new())
                }
                None => return Err(CtxError::WalletSecretsMissing),
            };

            // A disabled chain is absent from the registry, so this lookup
            // covers both "never configured" and "configured but disabled".
            // Neither can stand while the wallet signs on that chain.
            let signing_chain = |chain: Chain| -> Result<SigningChain, CtxError> {
                let rpc_url = chains
                    .rpc_url(chain)
                    .ok_or(CtxError::WalletMissingChain { chain })?;
                crate::wallet::require_secure_wallet_rpc_url(rpc_url, chain)?;
                let required_confirmations = chains
                    .required_confirmations(chain)
                    .ok_or(CtxError::WalletMissingChain { chain })?;

                Ok(SigningChain {
                    rpc_url: rpc_url.clone(),
                    required_confirmations,
                })
            };
            let signing_chains = SigningChains {
                base: signing_chain(Chain::Base)?,
                ethereum: signing_chain(Chain::Ethereum)?,
                hyperevm: signing_chain(Chain::HyperEvm)?,
            };

            let wallet_meta = WalletMeta::deserialize(wallet_config.clone()).map_err(|source| {
                CtxError::ConfigToml {
                    path: config_path.to_path_buf(),
                    source,
                }
            })?;

            Ok((
                WalletInputs {
                    config: wallet_config,
                    secrets: wallet_secrets,
                    signing_chains,
                },
                wallet_meta,
            ))
        }
        // Covers secrets-without-config too, which used to warn before
        // erroring: parsing runs before any tracing subscriber exists, so
        // that warn! was silently dropped anyway. The returned error is the
        // operator signal.
        (None, _) => Err(CtxError::WalletNotConfigured),
    }
}

/// Rejects an asset table that contradicts the chain tables it depends on.
///
/// The two halves have to agree: `[chains.<name>.trading.assets.equities]` says what the bot hedges,
/// and each `[chains.<name>.trading.assets.equities]` says where those symbols
/// are listed. A symbol in one and not the other is a misconfiguration in both
/// directions -- a listing nothing will hedge, or a hedge for something that
/// trades nowhere -- so neither is silently tolerated.
fn validate_asset_tables(
    hedging: &HedgingAssets,
    chains: &BTreeMap<Chain, ChainConfig>,
) -> Result<(), CtxError> {
    DeploymentSymbolPolicy::new(
        hedging.equities.symbols.keys().cloned(),
        hedging.equities.retired_symbols.clone(),
    )?;

    let listings = |symbol: &Symbol| -> Vec<&ChainEquityAsset> {
        chains
            .values()
            .filter_map(|config| config.trading.as_ref())
            .filter_map(|trading| trading.assets.equities.symbols.get(symbol))
            .collect()
    };

    for (chain, config) in chains {
        let Some(trading) = &config.trading else {
            continue;
        };

        for symbol in trading.assets.equities.symbols.keys() {
            if !hedging.equities.symbols.contains_key(symbol) {
                return Err(CtxError::ListedSymbolIsNotHedged {
                    chain: *chain,
                    symbol: symbol.clone(),
                });
            }
        }
    }

    for (symbol, policy) in &hedging.equities.symbols {
        let listed = listings(symbol);

        if listed.is_empty() {
            return Err(CtxError::HedgedSymbolIsNotListed {
                symbol: symbol.clone(),
            });
        }

        // The hedge path gates on `trading` before it consults the
        // extended-hours session, so enabling extended-hours counter-trading
        // for a symbol that trades nowhere creates a dead configuration that
        // can never execute.
        if policy.extended_hours_counter_trading == OperationMode::Enabled
            && listed
                .iter()
                .all(|asset| asset.trading == OperationMode::Disabled)
        {
            return Err(CtxError::ExtendedHoursWithoutCounterTrading {
                symbol: symbol.clone(),
            });
        }
    }

    Ok(())
}

/// The poll cadences with defaults applied, each rejected at zero -- a zero
/// interval would spin the corresponding poller in a hot loop.
struct PollingIntervals {
    order_polling_interval: u64,
    position_check_interval: u64,
    inventory_poll_interval: u64,
    order_fill_poll_interval: u64,
    apalis_finished_job_cleanup_interval_secs: u64,
}

fn validated_polling_intervals(config: &Config) -> Result<PollingIntervals, CtxError> {
    let intervals = PollingIntervals {
        order_polling_interval: config.order_polling_interval.unwrap_or(15),
        position_check_interval: config.position_check_interval.unwrap_or(60),
        inventory_poll_interval: config.inventory_poll_interval.unwrap_or(60),
        order_fill_poll_interval: config.order_fill_poll_interval.unwrap_or(5),
        apalis_finished_job_cleanup_interval_secs: config.apalis_finished_job_cleanup_interval_secs,
    };

    for (value, field) in [
        (intervals.order_polling_interval, "order_polling_interval"),
        (intervals.position_check_interval, "position_check_interval"),
        (intervals.inventory_poll_interval, "inventory_poll_interval"),
        (
            intervals.order_fill_poll_interval,
            "order_fill_poll_interval",
        ),
        (
            intervals.apalis_finished_job_cleanup_interval_secs,
            "apalis_finished_job_cleanup_interval_secs",
        ),
    ] {
        if value == 0 {
            return Err(CtxError::ZeroPollingInterval { field });
        }
    }

    Ok(intervals)
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
    let secrets: Secrets = toml::from_str(secrets_str).map_err(|source| CtxError::SecretsToml {
        path: secrets_path.to_path_buf(),
        source,
    })?;

    if config.server_port == config.board_port {
        return Err(CtxError::ServerAndBoardPortsMatch {
            port: config.server_port,
        });
    }

    // The audiences are what keep the read and write tiers apart: each role
    // prefix's verifier pins the audience IAP minted for that prefix's
    // backend. Equal (or blank) audiences would make a read-tier assertion
    // verify on the write path, silently collapsing the tiers, so a config
    // that says that is refused outright.
    if let Some(ops_api) = &config.ops_api {
        if ops_api.read_audience.trim().is_empty() || ops_api.write_audience.trim().is_empty() {
            return Err(CtxError::OpsApiAudienceBlank);
        }
        // The verifier pins the audience byte for byte (jsonwebtoken compares
        // `aud` exactly), so a copy-pasted trailing space would pass a
        // trimmed-only validation here and then 401 every real token at
        // runtime with no startup signal. Refuse padding outright.
        if ops_api.read_audience.trim() != ops_api.read_audience
            || ops_api.write_audience.trim() != ops_api.write_audience
        {
            return Err(CtxError::OpsApiAudiencePadded);
        }
        if ops_api.read_audience == ops_api.write_audience {
            return Err(CtxError::OpsApiAudiencesEqual);
        }
    }

    validate_asset_tables(&config.assets, &config.chains)?;
    let polling_intervals = validated_polling_intervals(&config)?;

    // Collected instead of warn!ed: no tracing subscriber exists yet (the
    // binaries build theirs from the parsed Ctx), so a warn! here would
    // dispatch to NoSubscriber and vanish. See `StartupNotice`.
    let mut startup_notices = Vec::new();

    let broker = resolve_broker(config.broker.as_ref(), secrets.broker, &mut startup_notices)?;
    let telemetry = config.telemetry.map(TelemetryCtx::from);

    // Migration shim, removed next release: see the `Secrets::alerts` field.
    if secrets.alerts.is_some() {
        startup_notices.push(StartupNotice::warning(
            "[alerts] in the secrets file is deprecated and ignored (alerts are \
             structured logs now); remove [alerts] from the secrets file",
        ));
    }

    let alerts = AlertsCtx::new(config.alerts, &mut startup_notices)?;
    let pricing = PricingCtx::assemble(
        config.pricing,
        secrets.pricing,
        !config.assets.equities.symbols.is_empty(),
    )?;

    // Execution threshold is determined by broker capabilities:
    // - Alpaca requires $1 minimum for fractional trading. We use $2 to provide buffer
    //   for slippage, fees, and price discrepancies that could push fills below $1.
    // - DryRun uses shares threshold for testing
    let execution_threshold = broker.execution_threshold()?;

    let chains = ChainRegistry::new(&config.chains, secrets.chains)?;
    let (wallet_inputs, wallet_meta) =
        validate_wallet_inputs(config.wallet, secrets.wallet, &chains, config_path)?;

    let trading_mode = match config.rebalancing {
        Some(rebalancing_config) => {
            let BrokerCtx::AlpacaBrokerApi(_) = &broker else {
                return Err(RebalancingCtxError::NotAlpacaBroker.into());
            };

            let minimum = *crate::ALPACA_TO_BASE_MINIMUM_TRANSFER;

            for chain_config in config.chains.values() {
                let Some(cash) = chain_config
                    .trading
                    .as_ref()
                    .and_then(|trading| trading.assets.cash.as_ref())
                else {
                    continue;
                };

                if cash.rebalancing == OperationMode::Enabled
                    && let Some(cash_limit) = &cash.operational_limit
                    && cash_limit.inner().lt(&minimum)?
                {
                    return Err(CtxError::CashOperationalLimitBelowMinimumTransfer {
                        configured: cash_limit.inner(),
                        minimum,
                    });
                }
            }

            TradingMode::Rebalancing(Box::new(RebalancingCtx::new(&rebalancing_config)?))
        }
        None => TradingMode::Standalone,
    };

    let redemption_wallet = chains.sole_trading().redemption_wallet;

    if matches!(trading_mode, TradingMode::Rebalancing(_)) && redemption_wallet.is_none() {
        return Err(CtxError::MissingTokenization);
    }

    // See `Ctx::bot_gas_valuation` doc for why this is required only in
    // Rebalancing mode.
    if matches!(trading_mode, TradingMode::Rebalancing(_)) && config.bot_gas_valuation.is_none() {
        return Err(CtxError::MissingBotGasValuation);
    }

    match (&trading_mode, &alerts) {
        (TradingMode::Rebalancing(_), None) => {
            return Err(CtxError::MissingAlertsForRebalancing);
        }
        (TradingMode::Rebalancing(_), Some(_)) | (TradingMode::Standalone, _) => {}
    }

    let log_level = config.log_level.unwrap_or(LogLevel::Debug);
    let log_format = config.log_format.unwrap_or(LogFormat::Text);
    let log_query_url_template = config
        .log_query_url_template
        .map(LogQueryUrlTemplate::parse)
        .transpose()?;

    let ExtendedHoursBrokerWindows {
        reprice_timeout_secs: extended_hours_reprice_timeout_secs,
        close_flatten_reprice_timeout_secs,
        close_flatten_window_secs: extended_hours_close_flatten_window_secs,
        close_flatten_cross_max_bps,
    } = extended_hours_broker_windows(&broker, config.broker.as_ref(), &config.assets)?;

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
        log_format,
        log_query_url_template,
        server_port: config.server_port,
        board_port: config.board_port,
        chains,
        order_polling_interval: polling_intervals.order_polling_interval,
        order_polling_max_jitter: config.order_polling_max_jitter.unwrap_or(5),
        position_check_interval: polling_intervals.position_check_interval,
        inventory_poll_interval: polling_intervals.inventory_poll_interval,
        inventory_divergence_threshold: config.inventory_divergence_threshold,
        hedge_order_gate_reconciliation_timeout_secs: config
            .hedge_order_gate_reconciliation_timeout_secs,
        order_fill_poll_interval: polling_intervals.order_fill_poll_interval,
        extended_hours_reprice_timeout_secs,
        close_flatten_reprice_timeout_secs,
        extended_hours_close_flatten_window_secs,
        close_flatten_cross_max_bps,
        apalis_finished_job_cleanup_interval_secs: polling_intervals
            .apalis_finished_job_cleanup_interval_secs,
        broker,
        telemetry,
        alerts,
        pricing,
        execution_threshold,
        trading_mode,
        assets: config.assets,
        travel_rule,
        rest_api: config
            .rest_api
            .map(|cfg| {
                if secrets.rest_api.is_none() {
                    startup_notices.push(StartupNotice::warning(
                        "[rest_api] URL configured but no [rest_api] credentials in secrets \
                         -- requests will be unauthenticated",
                    ));
                }

                let key_id = secrets.rest_api.as_ref().map(|s| s.key_id.clone());
                let key_secret = secrets.rest_api.map(|s| s.key_secret);
                RestApiCtx::new(cfg.url, key_id, key_secret).map_err(CtxError::RestApiClient)
            })
            .transpose()?,
        issuance: issuance_ctx(config.issuance, secrets.issuance, &mut startup_notices)?,
        ops_api: config.ops_api,
        startup_notices,
        redemption_wallet,
        bot_gas_valuation: config.bot_gas_valuation,
        orchestrator: config.orchestrator,
        wallet_inputs,
        wallet_meta,
    })
}

/// Result of [`extended_hours_broker_windows`]. The fields have distinct
/// meanings -- a named struct (rather than a
/// positional tuple) prevents a future reorder at either the construction or
/// destructuring site from silently swapping which duration feeds which
/// `Ctx` field.
struct ExtendedHoursBrokerWindows {
    reprice_timeout_secs: Option<NonZeroU64>,
    close_flatten_reprice_timeout_secs: u64,
    close_flatten_window_secs: u64,
    close_flatten_cross_max_bps: u16,
}

/// Resolves both extended-hours windows, requiring validated config values
/// whenever they can actually be consulted at runtime: always for Alpaca,
/// and for DryRun whenever any asset has extended hours enabled. DryRun is a
/// real runtime mode (staging, CLI dry-run), not test-only -- the reprice
/// sweep and the close-flatten policy both consult these windows on every
/// `CheckPositions` tick, so neither may silently default to 0 while
/// extended hours is live.
fn extended_hours_broker_windows(
    broker: &BrokerCtx,
    broker_config: Option<&BrokerConfig>,
    assets: &HedgingAssets,
) -> Result<ExtendedHoursBrokerWindows, CtxError> {
    let requires_configured_windows = match broker {
        BrokerCtx::AlpacaBrokerApi(_) => true,
        BrokerCtx::DryRun => assets.any_extended_hours_enabled(),
    };

    if !requires_configured_windows {
        return Ok(ExtendedHoursBrokerWindows {
            reprice_timeout_secs: None,
            close_flatten_reprice_timeout_secs: 0,
            close_flatten_window_secs: 0,
            close_flatten_cross_max_bps: broker.counter_trade_slippage_bps(),
        });
    }

    let broker_config = broker_config.ok_or(CtxError::MissingExtendedHoursRepriceTimeout)?;

    Ok(ExtendedHoursBrokerWindows {
        reprice_timeout_secs: Some(broker_config.extended_hours_reprice_timeout_secs()?),
        close_flatten_reprice_timeout_secs: broker_config.close_flatten_reprice_timeout_secs()?,
        close_flatten_window_secs: broker_config.extended_hours_close_flatten_window_secs()?,
        close_flatten_cross_max_bps: broker_config
            .close_flatten_cross_max_bps(broker.counter_trade_slippage_bps())?,
    })
}

/// Assembles the required issuance status context: `api_key` from the
/// secrets file, `base_url` from the config file (preferred) or its
/// deprecated secrets-file copy. Both must be present somewhere and the URL
/// must parse -- no silent fallbacks for an endpoint the rebalancing freeze
/// guard depends on. A `base_url` set differently in both files is refused
/// rather than silently resolved.
fn issuance_ctx(
    config: Option<IssuanceConfig>,
    secrets: Option<IssuanceSecretsToml>,
    startup_notices: &mut Vec<StartupNotice>,
) -> Result<IssuanceStatusCtx, CtxError> {
    let Some(secrets) = secrets else {
        return Err(CtxError::MissingIssuanceConfig);
    };
    let secrets = IssuanceSecrets::try_from_toml(secrets)
        .map_err(|source| CtxError::InvalidIssuanceApiKey { source })?;

    if secrets.base_url.is_some() {
        // Migration shim, removed next release: see `IssuanceSecretsToml`.
        startup_notices.push(StartupNotice::warning(
            "[issuance] base_url in the secrets file is deprecated and moves to the \
             config file; the secrets [issuance] section keeps only api_key \
             (removed next release)",
        ));
    }

    let base_url = match (config.map(|config| config.base_url), secrets.base_url) {
        (Some(from_config), Some(from_secrets)) => {
            if from_config == from_secrets {
                from_config
            } else {
                return Err(CtxError::IssuanceBaseUrlConflict);
            }
        }
        (Some(base_url), None) | (None, Some(base_url)) => base_url,
        (None, None) => return Err(CtxError::MissingIssuanceBaseUrl),
    };

    Ok(IssuanceStatusCtx {
        base_url,
        api_key: IssuanceApiKey(secrets.api_key),
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
            parts.wallet_inputs.signing_chains,
        )
        .await?;

        let order_owner = wallet.base_wallet().address();

        Ok(Self {
            database_url: parts.database_url,
            log_level: parts.log_level,
            log_dir: parts.log_dir,
            log_format: parts.log_format,
            log_query_url_template: parts.log_query_url_template,
            server_port: parts.server_port,
            board_port: parts.board_port,
            chains: parts.chains,
            order_polling_interval: parts.order_polling_interval,
            order_polling_max_jitter: parts.order_polling_max_jitter,
            position_check_interval: parts.position_check_interval,
            inventory_poll_interval: parts.inventory_poll_interval,
            inventory_divergence_threshold: parts.inventory_divergence_threshold,
            hedge_order_gate_reconciliation_timeout_secs: parts
                .hedge_order_gate_reconciliation_timeout_secs,
            order_fill_poll_interval: parts.order_fill_poll_interval,
            extended_hours_reprice_timeout_secs: parts.extended_hours_reprice_timeout_secs,
            close_flatten_reprice_timeout_secs: parts.close_flatten_reprice_timeout_secs,
            extended_hours_close_flatten_window_secs: parts
                .extended_hours_close_flatten_window_secs,
            close_flatten_cross_max_bps: parts.close_flatten_cross_max_bps,
            apalis_finished_job_cleanup_interval_secs: parts
                .apalis_finished_job_cleanup_interval_secs,
            broker: parts.broker,
            telemetry: parts.telemetry,
            alerts: parts.alerts,
            startup_notices: parts.startup_notices,
            pricing: parts.pricing,
            trading_mode: parts.trading_mode,
            order_owner,
            wallet: Some(wallet),
            wallet_meta: Some(parts.wallet_meta),
            execution_threshold: parts.execution_threshold,
            assets: parts.assets,
            travel_rule: parts.travel_rule,
            rest_api: parts.rest_api,
            ops_api: parts.ops_api,
            issuance: parts.issuance,
            redemption_wallet: parts.redemption_wallet,
            bot_gas_valuation: parts.bot_gas_valuation,
            orchestrator: parts.orchestrator,
        })
    }

    /// Validates config and secrets files without constructing runtime objects.
    ///
    /// Calls the same `parse_and_validate` function (private to this module) as
    /// [`load_files`](Self::load_files), ensuring identical validation. The only
    /// difference is that `load_files` additionally performs async wallet
    /// construction (which connects to RPC endpoints). Suitable for pre-deploy
    /// validation where we want to catch config errors before restarting the
    /// service.
    pub fn validate_files(
        config_path: &Path,
        secrets_path: &Path,
    ) -> Result<Vec<StartupNotice>, CtxError> {
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
        let parts = parse_and_validate(&config_str, config_path, &secrets_str, secrets_path)?;
        Ok(parts.startup_notices)
    }

    /// Emits the notices collected during parsing. Call once, right after
    /// the tracing subscriber is installed -- during parsing itself there
    /// was no subscriber, so logging there would have vanished.
    pub fn emit_startup_notices(&self) {
        for notice in &self.startup_notices {
            let message = notice.message.as_str();
            match notice.level {
                StartupNoticeLevel::Info => info!(target: "startup", "{message}"),
                StartupNoticeLevel::Warn => warn!(target: "startup", "{message}"),
            }
        }
    }

    /// Loads deploy-time Turnkey policy inputs without constructing wallets or
    /// connecting to the configured RPC endpoints.
    #[cfg(feature = "wallet-turnkey")]
    pub fn load_turnkey_approval_policy_inputs(
        config_path: &Path,
        secrets_path: &Path,
    ) -> Result<Option<TurnkeyApprovalPolicyInputs>, CtxError> {
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
        let parts = parse_and_validate(&config_str, config_path, &secrets_str, secrets_path)?;

        if parts.wallet_meta.kind != "turnkey" {
            return Ok(None);
        }

        let st0x_evm::turnkey::TurnkeySettings {
            address: wallet_address,
            organization_id,
            kms_api_key,
        } = st0x_evm::turnkey::TurnkeySettings::deserialize(parts.wallet_inputs.config).map_err(
            |source| CtxError::ConfigToml {
                path: config_path.to_path_buf(),
                source,
            },
        )?;
        let st0x_evm::turnkey::TurnkeyCredentials { api_private_key } =
            st0x_evm::turnkey::TurnkeyCredentials::deserialize(parts.wallet_inputs.secrets)
                .map_err(|source| CtxError::SecretsToml {
                    path: secrets_path.to_path_buf(),
                    source,
                })?;

        Ok(Some(TurnkeyApprovalPolicyInputs {
            organization_id,
            kms_api_key,
            api_private_key,
            wallet_address,
            orderbook: parts.chains.sole_trading().orderbook,
            assets: parts.chains.sole_trading().assets.clone(),
        }))
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

    /// Returns the redemption wallet from the `[chains.<name>.trading].redemption_wallet` config section.
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
    /// required `[chains.<name>.trading].vault_owner` config field -- the signing wallet while
    /// the vaults are bot-EOA-owned, flipped to the inventory address when the
    /// shared-inventory migration makes the inventory contract `msg.sender` to
    /// Raindex (and therefore the vault owner).
    pub fn vault_owner(&self) -> Address {
        self.chains.sole_trading().vault_owner
    }
}

/// Per-symbol equity config guards. All six live on `ChainAssets` (the owner
/// of the `[chains.<name>.trading.assets.equities]` map) so callers use one convention --
/// `assets.X(symbol)` or `ctx.assets.X(symbol)` -- rather than mixing
/// `ctx.X(symbol)` with `ctx.assets.X(symbol)`. Code that holds only an
/// `&ChainAssets` (e.g. the accumulator) can reach every guard without a `Ctx`.
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
        #[builder(default = InventoryAdapters::default())] inventory_adapters: InventoryAdapters,
        /// What the trading chain lists.
        assets: crate::ChainAssets,
        /// How those symbols hedge. Omitted by most fixtures, which care about
        /// the chain listing; when absent every listed symbol gets an
        /// extended-hours-disabled policy, matching what the config validator
        /// requires of a real file.
        hedging: Option<HedgingAssets>,
        #[builder(default = 2)] inventory_poll_interval: u64,
        #[builder(default = const { NonZeroU32::new(10).unwrap() })]
        inventory_divergence_threshold: NonZeroU32,
        #[builder(default = const { NonZeroU64::new(10).unwrap() })]
        hedge_order_gate_reconciliation_timeout_secs: NonZeroU64,
        #[builder(default = 3600)] apalis_finished_job_cleanup_interval_secs: u64,
        #[builder(default = 0)] server_port: u16,
        #[builder(default = 0)] board_port: u16,
        execution_threshold_override: Option<ExecutionThreshold>,
        travel_rule: Option<TravelRuleConfig>,
        rest_api: Option<RestApiCtx>,
        ops_api: Option<OpsApiConfig>,
        #[builder(default = create_test_issuance_ctx())] issuance: IssuanceStatusCtx,
        redemption_wallet: Option<Address>,
        /// Gas thresholds used by rebalancing admission and monitors. Fixtures
        /// that enable rebalancing must provide this just like production.
        alerts: Option<AlertsCtx>,
        bot_gas_valuation: Option<BotGasValuationConfig>,
        orchestrator: Option<OrchestratorConfig>,
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

        if matches!(trading_mode, TradingMode::Rebalancing(_)) && bot_gas_valuation.is_none() {
            return Err(CtxError::MissingBotGasValuation);
        }

        match (&trading_mode, &alerts) {
            (TradingMode::Rebalancing(_), None) => {
                return Err(CtxError::MissingAlertsForRebalancing);
            }
            (TradingMode::Rebalancing(_), Some(_)) | (TradingMode::Standalone, _) => {}
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

        let hedging = hedging.unwrap_or_else(|| HedgingAssets {
            equities: crate::HedgedEquities {
                retired_symbols: Vec::new(),
                symbols: assets
                    .equities
                    .symbols
                    .keys()
                    .map(|symbol| {
                        (
                            symbol.clone(),
                            crate::EquityHedgePolicy {
                                extended_hours_counter_trading: OperationMode::Disabled,
                            },
                        )
                    })
                    .collect(),
            },
            cash: None,
        });

        Ok(Self {
            database_url,
            log_level: LogLevel::Debug,
            log_dir: None,
            log_format: LogFormat::Text,
            log_query_url_template: None,
            server_port,
            board_port,
            chains: ChainRegistry::single_trading_chain(TradingChain {
                chain: Chain::Base,
                rpc_url,
                required_confirmations,
                orderbook,
                inventory: inventory_mode,
                inventory_adapters,
                vault_owner,
                deployment_block,
                ingestion_cutoff: IngestionCutoff::Safe,
                redemption_wallet,
                assets,
            }),
            order_polling_interval: 1,
            order_polling_max_jitter: 0,
            position_check_interval: 2,
            inventory_poll_interval,
            inventory_divergence_threshold,
            hedge_order_gate_reconciliation_timeout_secs,
            order_fill_poll_interval: 1,
            extended_hours_reprice_timeout_secs: NonZeroU64::new(300),
            close_flatten_reprice_timeout_secs: 60,
            extended_hours_close_flatten_window_secs: 900,
            close_flatten_cross_max_bps: 400,
            apalis_finished_job_cleanup_interval_secs,
            broker,
            telemetry: None,
            alerts,
            startup_notices: Vec::new(),
            pricing: None,
            trading_mode,
            order_owner,
            wallet,
            wallet_meta: None,
            execution_threshold,
            assets: hedging,
            travel_rule,
            rest_api,
            ops_api,
            issuance,
            redemption_wallet,
            bot_gas_valuation,
            orchestrator,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CtxError {
    #[error(transparent)]
    Rebalancing(Box<RebalancingCtxError>),
    #[error(transparent)]
    Pricing(#[from] PricingCtxError),
    #[error("log_query_url_template must contain the {{id}} placeholder")]
    LogQueryUrlTemplateMissingIdPlaceholder,
    #[error(
        "[ops_api] audiences must not be blank: each role prefix's verifier pins the \
         audience IAP mints for that prefix's backend, and a blank pin verifies nothing"
    )]
    OpsApiAudienceBlank,
    #[error(
        "[ops_api] audiences must not carry leading or trailing whitespace: the verifier \
         pins the audience byte for byte, so a padded value would reject every real IAP \
         token at runtime instead of failing here"
    )]
    OpsApiAudiencePadded,
    #[error(
        "[ops_api] read_audience and write_audience must differ: equal audiences let a \
         read-tier IAP assertion pass the write verifier, collapsing the role tiers"
    )]
    OpsApiAudiencesEqual,
    #[error(
        "[broker] type = \"alpaca-broker-api-kms\" requires mode = \"production\": the \
         keyless token mint targets the live authx.alpaca.markets endpoint, so a sandbox or \
         mock broker mode would silently mint production bearer tokens"
    )]
    KmsBrokerRequiresProductionMode,
    #[error(
        "[broker] type must be set in the config file's [broker] section (or, \
         deprecated, in the secrets file's [broker] table)"
    )]
    MissingBrokerType,
    #[error(
        "[broker] {field} is set to different values in the config and secrets \
         files; remove the deprecated secrets-file copy"
    )]
    BrokerIdentityConflict { field: &'static str },
    #[error("[broker] {field} does not apply to broker type \"{kind}\"")]
    BrokerFieldNotForKind {
        field: &'static str,
        kind: &'static str,
    },
    #[error("[broker] {field} is required for this broker type but was not configured")]
    MissingBrokerField { field: &'static str },
    #[error(
        "[broker] type = \"alpaca-broker-api\" requires api_key and api_secret in \
         the secrets file's [broker] table"
    )]
    MissingBrokerCredentials,
    #[error(
        "[issuance] base_url is set to different values in the config and secrets \
         files; remove the deprecated secrets-file copy"
    )]
    IssuanceBaseUrlConflict,
    #[error(
        "[issuance] base_url must be set in the config file (or, deprecated, in \
         the secrets file's [issuance] section)"
    )]
    MissingIssuanceBaseUrl,
    #[error("log_query_url_template is not a valid URL")]
    LogQueryUrlTemplateNotAUrl {
        #[source]
        source: url::ParseError,
    },
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
    #[error("duplicate symbol {symbol} in [assets.equities].retired_symbols")]
    DuplicateRetiredSymbol { symbol: Symbol },
    #[error(
        "symbol {symbol} cannot be both configured under [assets.equities] and \
         listed in retired_symbols"
    )]
    ConfiguredSymbolMarkedRetired { symbol: Symbol },
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
        "[broker] close_flatten_cross_max_bps is required when using Alpaca \
         Broker API, or when using DryRun with extended-hours counter-trading \
         enabled for any asset"
    )]
    MissingCloseFlattenCrossMaxBps,
    #[error(
        "[broker] close_flatten_cross_max_bps {configured} is out of range; \
         expected {min}..={max}, where the minimum is the effective runtime \
         counter-trade slippage base"
    )]
    CloseFlattenCrossMaxBpsOutOfRange { configured: u16, min: u16, max: u16 },
    #[error(
        "[broker] extended_hours_reprice_timeout_secs is required when using \
         Alpaca Broker API, or when using DryRun with extended-hours \
         counter-trading enabled for any asset"
    )]
    MissingExtendedHoursRepriceTimeout,
    #[error(
        "[broker] extended_hours_reprice_timeout_secs {configured} is out of range; \
         expected 1..={max}"
    )]
    ExtendedHoursRepriceTimeoutOutOfRange { configured: u64, max: u64 },
    #[error(
        "[broker] close_flatten_reprice_timeout_secs is required when using \
         Alpaca Broker API, or when using DryRun with extended-hours \
         counter-trading enabled for any asset"
    )]
    MissingCloseFlattenRepriceTimeout,
    #[error(
        "[broker] close_flatten_reprice_timeout_secs {configured} is out of range; \
         expected 1..={max}"
    )]
    CloseFlattenRepriceTimeoutOutOfRange { configured: u64, max: u64 },
    #[error(
        "[broker] extended_hours_close_flatten_window_secs is required when \
         using Alpaca Broker API, or when using DryRun with extended-hours \
         counter-trading enabled for any asset"
    )]
    MissingExtendedHoursCloseFlattenWindow,
    #[error(
        "[broker] extended_hours_close_flatten_window_secs {configured} is out of range; \
         expected 1..={max}"
    )]
    ExtendedHoursCloseFlattenWindowOutOfRange { configured: u64, max: u64 },
    #[error(
        "[broker] counter_trade_slippage_bps {configured} is out of range; \
         expected {min}..={max}"
    )]
    CounterTradeSlippageBpsOutOfRange { configured: u16, min: u16, max: u16 },
    #[error(transparent)]
    Alerts(#[from] crate::alerts::AlertsAssemblyError),
    #[error(transparent)]
    Chain(#[from] crate::chain::ChainConfigError),
    #[error(transparent)]
    ChainRegistry(#[from] crate::chain::ChainRegistryError),
    #[error("operation requires rebalancing mode")]
    NotRebalancing,
    #[error(
        "operation requires the trading chain's redemption_wallet \
         ([chains.<name>.trading].redemption_wallet)"
    )]
    MissingTokenization,
    #[error(
        "[bot_gas_valuation] section is required when rebalancing is enabled \
         (see ADR 0020)"
    )]
    MissingBotGasValuation,
    #[error(
        "[alerts] section is required when rebalancing is enabled so every fresh \
         transfer can verify its signing wallets have enough native gas"
    )]
    MissingAlertsForRebalancing,
    #[error(
        "operation requires a configured [wallet] section, and a [chains.<name>] \
         entry supplying an rpc_url for every chain it signs on"
    )]
    WalletNotConfigured,
    #[error(transparent)]
    Wallet(#[from] crate::wallet::WalletCtxError),
    #[error(
        "[wallet] is configured but no rpc_url is available for {chain}: this build \
         constructs a signer for every chain it may hold funds on, so {chain} needs a \
         [chains.{chain}] table with a matching secrets entry, and cannot be \
         lifecycle = \"disabled\""
    )]
    WalletMissingChain { chain: Chain },
    #[error("[wallet] config present but [wallet] secrets missing")]
    WalletSecretsMissing,
    #[error(
        "[chains.<name>.trading.assets.cash] operational_limit {configured} is below the \
         smallest Alpaca-to-Base transfer that can complete, {minimum}"
    )]
    CashOperationalLimitBelowMinimumTransfer { configured: Usdc, minimum: Usdc },
    #[error(
        "vault_ids in [chains.<name>.trading.assets.cash] is required for rebalancing \
         but not configured"
    )]
    MissingCashVaultId,
    #[error(
        "vault_ids in [chains.<name>.trading.assets.equities.{symbol}] is required when \
         rebalancing is enabled but not configured"
    )]
    MissingEquityVaultId { symbol: Symbol },
    #[error(
        "[chains.{chain}.trading.assets.equities.{symbol}] is listed on chain but absent \
         from [chains.<name>.trading.assets.equities], so nothing would hedge its fills"
    )]
    ListedSymbolIsNotHedged { chain: Chain, symbol: Symbol },
    #[error(
        "[assets.equities.{symbol}] is hedged but listed under no chain's \
         [chains.base.assets.equities], so it can never trade"
    )]
    HedgedSymbolIsNotListed { symbol: Symbol },
    #[error(
        "[assets.equities.{symbol}] enables extended_hours_counter_trading, but every \
         [chains.<name>.trading.assets.equities.{symbol}] listing has trading disabled \
         -- extended-hours counter-trades only run while counter-trading is enabled, \
         so this combination can never execute"
    )]
    ExtendedHoursWithoutCounterTrading { symbol: Symbol },
    #[error("{field} must be non-zero")]
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
            Self::Pricing(_) => "pricing configuration error",
            Self::NotRebalancing => "operation requires rebalancing mode",
            Self::MissingTokenization => "operation requires tokenization config",
            Self::MissingBotGasValuation => "missing bot gas valuation config",
            Self::MissingAlertsForRebalancing => "missing rebalancing gas thresholds",
            Self::ConfigIo { .. } => "failed to read config file",
            Self::SecretsIo { .. } => "failed to read secrets file",
            Self::ConfigToml { .. } => "failed to parse config",
            Self::DuplicateRetiredSymbol { .. } => "duplicate retired symbol",
            Self::ConfiguredSymbolMarkedRetired { .. } => "configured symbol marked retired",
            Self::SecretsToml { .. } => "failed to parse secrets",
            Self::InvalidThreshold(_) => "invalid execution threshold",
            Self::MissingCounterTradeSlippageBps => "missing counter trade slippage bps",
            Self::KmsBrokerRequiresProductionMode => "kms broker auth requires production mode",
            Self::MissingBrokerType => "missing broker type",
            Self::BrokerIdentityConflict { .. } => "broker identity conflict",
            Self::BrokerFieldNotForKind { .. } => "broker field not for kind",
            Self::MissingBrokerField { .. } => "missing broker field",
            Self::MissingBrokerCredentials => "missing broker credentials",
            Self::IssuanceBaseUrlConflict => "issuance base_url conflict",
            Self::MissingIssuanceBaseUrl => "missing issuance base_url",
            Self::MissingCloseFlattenCrossMaxBps => "missing close flatten cross max bps",
            Self::CloseFlattenCrossMaxBpsOutOfRange { .. } => {
                "close flatten cross max bps out of range"
            }
            Self::MissingExtendedHoursRepriceTimeout => "missing extended hours reprice timeout",
            Self::ExtendedHoursRepriceTimeoutOutOfRange { .. } => {
                "extended hours reprice timeout out of range"
            }
            Self::MissingCloseFlattenRepriceTimeout => "missing close flatten reprice timeout",
            Self::CloseFlattenRepriceTimeoutOutOfRange { .. } => {
                "close flatten reprice timeout out of range"
            }
            Self::MissingExtendedHoursCloseFlattenWindow => {
                "missing extended hours close flatten window"
            }
            Self::ExtendedHoursCloseFlattenWindowOutOfRange { .. } => {
                "extended hours close flatten window out of range"
            }
            Self::CounterTradeSlippageBpsOutOfRange { .. } => {
                "counter trade slippage bps out of range"
            }
            Self::Alerts(_) => "alerts assembly error",
            Self::Chain(_) => "chain configuration error",
            Self::ChainRegistry(_) => "chain registry error",
            Self::CashOperationalLimitBelowMinimumTransfer { .. } => {
                "cash operational limit below minimum transfer"
            }
            Self::MissingCashVaultId => "missing cash vault_ids",
            Self::ListedSymbolIsNotHedged { .. } => "listed symbol has no hedging policy",
            Self::HedgedSymbolIsNotListed { .. } => "hedged symbol is listed on no chain",
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
            Self::WalletMissingChain { .. } => "wallet missing a chain entry",
            Self::WalletSecretsMissing => "wallet secrets missing",
            Self::RestApiClient(_) => "failed to build REST API HTTP client",
            Self::MissingIssuanceConfig => "missing issuance config",
            Self::InvalidIssuanceApiKey { .. } => "invalid issuance api_key",
            Self::LogQueryUrlTemplateMissingIdPlaceholder => {
                "log_query_url_template missing {id} placeholder"
            }
            Self::LogQueryUrlTemplateNotAUrl { .. } => "log_query_url_template is not a valid URL",
            Self::OpsApiAudienceBlank => "[ops_api] audience is blank",
            Self::OpsApiAudiencePadded => "[ops_api] audience has surrounding whitespace",
            Self::OpsApiAudiencesEqual => "[ops_api] audiences are equal",
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
        log_format: LogFormat::Text,
        log_query_url_template: None,
        server_port: 8080,
        board_port: 8081,
        chains: ChainRegistry::single_trading_chain(TradingChain {
            chain: Chain::Base,
            // Hard-coded literal URL — parse cannot fail in a test helper.
            #[allow(clippy::unwrap_used)]
            rpc_url: url::Url::parse("http://localhost:8545").unwrap(),
            required_confirmations: 1,
            orderbook: alloy::primitives::address!("0x1111111111111111111111111111111111111111"),
            // Legacy by default: no distinct inventory, so the OPERATOR_ROLE
            // preflight is skipped. Tests exercising the managed path override
            // the trading chain's `inventory` explicitly.
            inventory: InventoryMode::Legacy,
            inventory_adapters: InventoryAdapters::default(),
            vault_owner: order_owner,
            deployment_block: 1,
            ingestion_cutoff: IngestionCutoff::Safe,
            redemption_wallet: None,
            assets: crate::ChainAssets::default(),
        }),
        order_polling_interval: 15,
        order_polling_max_jitter: 5,
        position_check_interval: 60,
        inventory_poll_interval: 60,
        inventory_divergence_threshold: NonZeroU32::MIN,
        hedge_order_gate_reconciliation_timeout_secs: NonZeroU64::MIN,
        order_fill_poll_interval: 5,
        extended_hours_reprice_timeout_secs: NonZeroU64::new(300),
        close_flatten_reprice_timeout_secs: 60,
        extended_hours_close_flatten_window_secs: 900,
        close_flatten_cross_max_bps: 400,
        apalis_finished_job_cleanup_interval_secs: 3600,
        broker: BrokerCtx::DryRun,
        telemetry: None,
        alerts: None,
        startup_notices: Vec::new(),
        pricing: None,
        trading_mode: TradingMode::Standalone,
        order_owner,
        wallet: None,
        wallet_meta: None,
        execution_threshold: ExecutionThreshold::whole_share(),
        assets: HedgingAssets::default(),
        travel_rule: None,
        rest_api: None,
        ops_api: None,
        issuance: create_test_issuance_ctx(),
        redemption_wallet: None,
        bot_gas_valuation: None,
        orchestrator: None,
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address};
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
            .assets(crate::ChainAssets::default())
            .call()
            .unwrap();

        assert_eq!(ctx.chains.sole_trading().inventory, InventoryMode::Legacy);
        assert_eq!(ctx.chains.sole_trading().vault_owner, order_owner);
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
            .assets(crate::ChainAssets::default())
            .inventory_mode(InventoryMode::Managed { inventory })
            .call()
            .unwrap();

        assert_eq!(
            ctx.chains.sole_trading().inventory,
            InventoryMode::Managed { inventory }
        );
        assert_eq!(ctx.chains.sole_trading().vault_owner, inventory);
    }

    #[test]
    fn for_test_rebalancing_requires_alert_thresholds() {
        let rebalancing = RebalancingCtx::stub()
            .equity(crate::ImbalanceThreshold::new(float!(0.5), float!(0.1)).unwrap())
            .call();

        let result = Ctx::for_test()
            .database_url(":memory:".to_owned())
            .rpc_url(url::Url::parse("http://localhost:8545").unwrap())
            .orderbook(address!("0x2222222222222222222222222222222222222222"))
            .deployment_block(1)
            .broker(BrokerCtx::DryRun)
            .trading_mode(TradingMode::Rebalancing(Box::new(rebalancing)))
            .order_owner(address!("0x1111111111111111111111111111111111111111"))
            .wallet(crate::OnchainWalletCtx::stub())
            .assets(crate::ChainAssets::default())
            .redemption_wallet(Address::with_last_byte(3))
            .bot_gas_valuation(BotGasValuationConfig {
                chainlink_feed: Address::with_last_byte(4),
            })
            .call();

        assert!(matches!(result, Err(CtxError::MissingAlertsForRebalancing)));
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
        file.write_all(minimal_config_toml_bytes()).unwrap();
        file
    }

    /// The minimal config's raw bytes, so a test can vary a single line of it
    /// rather than restating the whole file.
    fn minimal_config_toml_bytes() -> &'static [u8] {
        br#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#
    }

    /// The enablement predicate has to run on the real load path, not just as
    /// a unit. HyperEVM is the case that matters: the chain exists, a signer
    /// is built for it, and nothing else is -- so a config raising it to
    /// "active" reads as reasonable and must still be refused.
    #[tokio::test]
    async fn hyperevm_cannot_be_raised_to_active() {
        let config = toml_file(
            &String::from_utf8_lossy(minimal_config_toml_bytes()).replace(
                "[chains.hyperevm]\n            lifecycle = \"observe-only\"",
                "[chains.hyperevm]\n            lifecycle = \"active\"",
            ),
        );
        let secrets = dry_run_secrets_toml();

        let error = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();
        let message = error.to_string();

        assert!(
            message.contains("hyperevm") && message.contains("gas valuation"),
            "expected the predicate to name hyperevm and its missing capability, got: {message}"
        );
    }

    /// A disabled chain is dropped from the registry, so a wallet that signs
    /// on it finds no endpoint. This build constructs a signer for all three
    /// chains, so disabling one is not yet expressible; the refusal names the
    /// chain rather than failing later inside wallet construction.
    #[tokio::test]
    async fn a_signing_chain_cannot_be_disabled() {
        let config = toml_file(
            &String::from_utf8_lossy(minimal_config_toml_bytes()).replace(
                "[chains.hyperevm]\n            lifecycle = \"observe-only\"",
                "[chains.hyperevm]\n            lifecycle = \"disabled\"",
            ),
        );
        // Its secrets entry goes too: a disabled chain leaves the registry, so
        // keeping the entry would be refused as unconfigured instead.
        let secrets = toml_file(
            r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "http://localhost:8545"

            [broker]
            type = "dry-run"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            "#,
        );

        let error = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();

        assert!(
            matches!(
                error,
                CtxError::WalletMissingChain {
                    chain: Chain::HyperEvm
                }
            ),
            "expected WalletMissingChain for hyperevm, got: {error:?}"
        );
    }

    /// Every chain disabled is not a quiet no-op: the bot would act on nothing
    /// at all, which is a misconfiguration rather than a valid idle state.
    #[tokio::test]
    async fn every_chain_disabled_is_refused() {
        let config = toml_file(
            &String::from_utf8_lossy(minimal_config_toml_bytes())
                .replace("lifecycle = \"active\"", "lifecycle = \"disabled\"")
                .replace("lifecycle = \"observe-only\"", "lifecycle = \"disabled\""),
        );
        let secrets = dry_run_secrets_toml();

        let error = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();

        assert!(
            error.to_string().contains("no chain at all"),
            "expected the all-disabled refusal, got: {error}"
        );
    }

    /// Pyth enrichment was removed (#1265): a config still carrying a feed id
    /// must fail loudly rather than silently ignoring the stale key.
    #[tokio::test]
    async fn equity_pyth_feed_id_is_rejected_as_unknown_configuration() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.base.trading.assets.equities.AAPL]
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            pyth_feed_id = "0xfee33f2a978bf32dd6b662b65ba8083c6773b494f8401194ec1870c640860245"
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [assets.equities]
            retired_symbols = []

            [assets.equities.AAPL]
            extended_hours_counter_trading = "disabled"
        "#,
        );
        let secrets = dry_run_secrets_toml();

        let error = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();
        let source = std::error::Error::source(&error)
            .map(ToString::to_string)
            .unwrap_or_default();

        assert!(
            source.contains("pyth_feed_id"),
            "a stale pyth_feed_id key must be rejected by name, got: {source}"
        );
    }

    fn alerts_config_toml(base_threshold: &str, ethereum_threshold: &str) -> NamedTempFile {
        toml_file(&format!(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"

            [alerts]
            poll_interval = 300
            realert_interval = 3600

            [alerts.low_balance_thresholds]
            base = "{base_threshold}"
            ethereum = "{ethereum_threshold}"
            "#,
        ))
    }

    /// Minimal config with `[broker.travel_rule]` included, for tests
    /// that use Alpaca Broker API secrets (which now require travel rule
    /// at startup). `close_flatten_cross_max_bps` is a parameter because it is
    /// the one key a test may need to vary -- or omit, via `None` -- without
    /// restating the whole file.
    fn alpaca_config_toml(close_flatten_cross_max_bps: Option<u16>) -> NamedTempFile {
        let cross_max_line = close_flatten_cross_max_bps
            .map(|bps| format!("close_flatten_cross_max_bps = {bps}"))
            .unwrap_or_default();

        toml_file(&format!(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            counter_trade_slippage_bps = 100
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
            extended_hours_close_flatten_window_secs = 900
            {cross_max_line}

            [broker.travel_rule]
            beneficiary_entity_name = "Test Entity"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#
        ))
    }

    /// The Alpaca Broker API secrets every `alpaca_config_toml` test pairs
    /// with: a complete `[evm]`/`[broker]`/`[wallet]`/`[issuance]` set, so a
    /// test asserting on one config key does not also depend on which
    /// secrets check runs first.
    fn alpaca_secrets_toml() -> NamedTempFile {
        toml_file(
            r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.infura.io"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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
        )
    }

    fn alpaca_trading_config_toml() -> NamedTempFile {
        toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            counter_trade_slippage_bps = 100
            close_flatten_cross_max_bps = 400
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.infura.io"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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

    fn dry_run_pricing_secrets_toml() -> NamedTempFile {
        let mut file = dry_run_secrets_toml();
        file.write_all(
            br#"

            [pricing]
            api_key = "pricing-oracle-test-key"
        "#,
        )
        .unwrap();
        file
    }

    fn equity_pricing_config_toml(include_pricing: bool) -> NamedTempFile {
        let pricing = if include_pricing {
            r#"
                [pricing]
                ws_url = "wss://pricing.test/ws"
                "#
        } else {
            ""
        };

        toml_file(&format!(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [assets.equities]
            retired_symbols = []

            [assets.equities.AAPL]
            extended_hours_counter_trading = "disabled"

            [chains.base.trading.assets.equities.AAPL]
            tokenized_equity = "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
            tokenized_equity_derivative = "0xf4f8c66085910d583c01f3b4e44bf731d4e2c565"
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"

            {pricing}

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
            "#,
        ))
    }

    fn unsupported_schwab_secrets_toml() -> NamedTempFile {
        toml_file(
            r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "http://localhost:8545"

            [chains.hyperevm]
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
        assert_eq!(ctx.extended_hours_reprice_timeout_secs, None);
        assert_eq!(
            ctx.close_flatten_cross_max_bps,
            ctx.broker.counter_trade_slippage_bps(),
            "an inactive DryRun ramp must still have a valid base-equal ceiling"
        );
    }

    #[tokio::test]
    async fn load_files_rejects_invalid_orderbook_address() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "not-an-address"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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

    /// The secrets file deliberately still carries the retired `[alerts]`
    /// table (bot_token): deployed secret versions do at rollout, and the
    /// migration shim must accept and ignore it rather than fail the strict
    /// parse. The shim -- and this fixture's `[alerts]` block -- go away next
    /// release.
    #[tokio::test]
    async fn alerts_ctx_built_when_section_present() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"

            [alerts]
            poll_interval = 300
            realert_interval = 3600

            [alerts.low_balance_thresholds]
            base = "0.05"
            ethereum = "0.01"
        "#,
        );
        let secrets = toml_file(
            r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.infura.io"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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
        assert_eq!(
            alerts.low_balance_threshold_wei(Chain::Base),
            Some(alloy::primitives::U256::from(50_000_000_000_000_000_u64))
        );
        assert_eq!(
            alerts.low_balance_threshold_wei(Chain::Ethereum),
            Some(alloy::primitives::U256::from(10_000_000_000_000_000_u64))
        );
        assert_eq!(alerts.poll_interval, std::time::Duration::from_secs(300));
        assert_eq!(
            alerts.realert_interval,
            std::time::Duration::from_secs(3600)
        );
        assert!(
            ctx.startup_notices.iter().any(|notice| {
                notice.level == StartupNoticeLevel::Warn
                    && notice.message.contains("[alerts] in the secrets file")
            }),
            "the deprecated secrets [alerts] table must produce a collected \
             startup notice, got: {:?}",
            ctx.startup_notices
        );
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
    async fn alerts_config_fails_fast_on_bad_thresholds() {
        let secrets = toml_file(
            r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.infura.io"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


            [broker]
            type = "dry-run"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "#,
        );

        for (base_threshold, ethereum_threshold, expected_chain) in [
            ("not-a-number", "0.01", Chain::Base),
            ("0.05", "not-a-number", Chain::Ethereum),
        ] {
            let config = alerts_config_toml(base_threshold, ethereum_threshold);
            let error = Ctx::load_files(config.path(), secrets.path())
                .await
                .unwrap_err();

            assert!(
                matches!(
                    &error,
                    CtxError::Alerts(
                        crate::alerts::AlertsAssemblyError::InvalidThreshold { chain, .. }
                    ) if *chain == expected_chain
                ),
                "expected Alerts(InvalidThreshold) for {expected_chain}, got: {error}"
            );
        }
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
        assert!(matches!(ctx.log_format, LogFormat::Text));
        assert_eq!(ctx.order_polling_interval, 15);
        assert_eq!(ctx.order_polling_max_jitter, 5);
        assert_eq!(ctx.position_check_interval, 60);
        assert_eq!(ctx.inventory_poll_interval, 60);
        assert_eq!(ctx.hedge_order_gate_reconciliation_timeout_secs.get(), 10);
        assert_eq!(ctx.order_fill_poll_interval, 5);
    }

    #[tokio::test]
    async fn apalis_finished_job_cleanup_interval_is_required() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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
    async fn inventory_divergence_threshold_is_required() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1
        "#,
        );
        let secrets = dry_run_secrets_toml();
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(error, CtxError::ConfigToml { .. }),
            "expected config parse failure for missing divergence threshold, got: {error:#}"
        );

        let source = std::error::Error::source(&error).unwrap();
        let source_display = source.to_string();
        assert!(
            source_display.contains("inventory_divergence_threshold"),
            "expected parse error to mention the threshold field, got: {source_display}"
        );
    }

    #[tokio::test]
    async fn hedge_order_gate_reconciliation_timeout_is_required() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10

            [assets.equities]
            retired_symbols = []

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
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
            "expected config parse failure for missing reconciliation timeout, got: {error:#}"
        );

        let source = std::error::Error::source(&error).unwrap();
        let source_display = source.to_string();
        assert!(
            source_display.contains("hedge_order_gate_reconciliation_timeout_secs"),
            "expected parse error to mention the reconciliation timeout field, got: \
             {source_display}"
        );
    }

    #[tokio::test]
    async fn zero_hedge_order_gate_reconciliation_timeout_is_rejected() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 0

            [assets.equities]
            retired_symbols = []

            [raindex]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
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
            "expected config parse failure for zero reconciliation timeout, got: {error:#}"
        );

        let source = std::error::Error::source(&error).unwrap();
        let source_display = source.to_string();
        assert!(
            source_display.contains("hedge_order_gate_reconciliation_timeout_secs"),
            "expected parse error to mention the reconciliation timeout field, got: \
             {source_display}"
        );
    }

    #[tokio::test]
    async fn zero_inventory_divergence_threshold_is_rejected() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 0
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1
        "#,
        );
        let secrets = dry_run_secrets_toml();
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(error, CtxError::ConfigToml { .. }),
            "expected config parse failure for zero divergence threshold, got: {error:#}"
        );

        let source = std::error::Error::source(&error).unwrap();
        let source_display = source.to_string();
        assert!(
            source_display.contains("nonzero"),
            "expected parse error to reject the zero value, got: {source_display}"
        );
    }

    #[tokio::test]
    async fn server_port_is_required() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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

    /// The flag is still required per equity, but it is no longer a field on
    /// the chain listing, so a missing one is caught by the cross-table check
    /// rather than by serde: a symbol listed on a chain with no hedging policy
    /// has nothing to hedge its fills.
    #[tokio::test]
    async fn a_listed_equity_without_a_hedging_policy_is_rejected() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities.AAPL]
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

        "#,
        );
        let secrets = dry_run_secrets_toml();
        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                CtxError::ListedSymbolIsNotHedged {
                    chain: Chain::Base,
                    ref symbol,
                } if *symbol == Symbol::new("AAPL").unwrap()
            ),
            "a listed symbol with no [chains.<name>.trading.assets.equities] entry must be refused, got: {error:#}"
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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10
            order_fill_poll_interval = 0

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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

    /// Equal audiences would let a read-tier IAP assertion pass the write
    /// verifier, and a blank audience pins nothing at all -- both are refused
    /// at load so the collapse cannot reach the verifiers.
    #[tokio::test]
    async fn ops_api_audiences_must_differ_and_be_non_blank() {
        for (read, write, expect) in [
            (
                "/projects/1/global/backendServices/11",
                "/projects/1/global/backendServices/11",
                "equal",
            ),
            ("", "/projects/1/global/backendServices/22", "blank"),
            ("/projects/1/global/backendServices/11", "  ", "blank"),
            (
                "/projects/1/global/backendServices/11",
                "/projects/1/global/backendServices/22 ",
                "padded",
            ),
        ] {
            let config = toml_file(&format!(
                r#"
                database_url = ":memory:"
                server_port = 8080
                board_port = 8081
                apalis_finished_job_cleanup_interval_secs = 3600
                inventory_divergence_threshold = 10
                hedge_order_gate_reconciliation_timeout_secs = 10

                [ops_api]
                read_audience = "{read}"
                write_audience = "{write}"

                [chains.base.trading.assets.equities]

                [chains.base]
                lifecycle = "active"
                required_confirmations = 3

                [chains.base.trading]
                orderbook = "0x1111111111111111111111111111111111111111"
                inventory_mode = "managed"
                inventory_adapters = []
                inventory = "0x2222222222222222222222222222222222222222"
                vault_owner = "0x3333333333333333333333333333333333333333"
                deployment_block = 1
                ingestion_cutoff = "safe"

                [chains.ethereum]
                lifecycle = "active"
                required_confirmations = 12

                [chains.hyperevm]
                lifecycle = "observe-only"
                required_confirmations = 1

                [wallet]
                kind = "private-key"
                address = "0x0000000000000000000000000000000000000001"
            "#
            ));
            let secrets = dry_run_secrets_toml();
            let error = Ctx::load_files(config.path(), secrets.path())
                .await
                .unwrap_err();

            let matched = match expect {
                "equal" => matches!(error, CtxError::OpsApiAudiencesEqual),
                "blank" => matches!(error, CtxError::OpsApiAudienceBlank),
                "padded" => matches!(error, CtxError::OpsApiAudiencePadded),
                other => unreachable!("unknown expectation {other}"),
            };
            assert!(matched, "expected {expect} audience error, got: {error:#}");
        }
    }

    #[tokio::test]
    // $52 is the sharp case rather than a trivially small limit: it clears
    // Alpaca's $51 withdrawal minimum, but every transfer it caps converts to
    // less than that, so rebalancing configured this way could never complete
    // a transfer.
    async fn rebalancing_with_low_cash_operational_limit_fails() {
        let secrets = toml_file(
            r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.infura.io"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base.trading.assets.cash]
            rebalancing = "enabled"
            operational_limit = 52

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            counter_trade_slippage_bps = 100
            close_flatten_cross_max_bps = 400
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
            extended_hours_close_flatten_window_secs = 900
            [rebalancing]
            transfer_timeout_secs = 1800
            inventory_staleness_bound_secs = 300
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
        let CtxError::CashOperationalLimitBelowMinimumTransfer {
            configured,
            minimum,
        } = error
        else {
            panic!(
                "Expected CashOperationalLimitBelowMinimumTransfer for \
                 operational_limit=52, got: {error:?}"
            );
        };
        assert_eq!(configured, Usdc::new(float!(52)));
        assert_eq!(minimum, Usdc::new(float!(53)));
    }

    #[tokio::test]
    async fn optional_fields_override_defaults() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10
            log_level = "warn"
            log_format = "json"
            log_query_url_template = "https://logs.example/query?id={id}"
            server_port = 9090
            order_polling_interval = 30
            order_polling_max_jitter = 10
            position_check_interval = 120
            inventory_poll_interval = 90

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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
        assert!(matches!(ctx.log_format, LogFormat::Json));
        assert_eq!(
            ctx.log_query_url_template
                .expect("template is configured")
                .substitute("abc-123"),
            "https://logs.example/query?id=abc-123"
        );
        assert_eq!(ctx.server_port, 9090);
        assert_eq!(ctx.order_polling_interval, 30);
        assert_eq!(ctx.order_polling_max_jitter, 10);
        assert_eq!(ctx.position_check_interval, 120);
        assert_eq!(ctx.inventory_poll_interval, 90);
    }

    /// A template without the `{id}` placeholder can never carry the id it
    /// exists to link, so startup refuses it instead of printing dead links.
    #[tokio::test]
    async fn log_query_url_template_without_id_placeholder_fails() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10
            server_port = 9090
            log_query_url_template = "https://logs.example/query?id=missing"

            [assets.equities]
            retired_symbols = []

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1
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
            matches!(error, CtxError::LogQueryUrlTemplateMissingIdPlaceholder),
            "expected LogQueryUrlTemplateMissingIdPlaceholder, got: {error:?}"
        );
    }

    /// A template that is not a URL would print a dead link on every transfer
    /// command, so startup refuses it even when the placeholder is present.
    #[tokio::test]
    async fn log_query_url_template_that_is_not_a_url_fails() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10
            server_port = 9090
            log_query_url_template = "not a url {id}"

            [assets.equities]
            retired_symbols = []

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1
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
            matches!(error, CtxError::LogQueryUrlTemplateNotAUrl { .. }),
            "expected LogQueryUrlTemplateNotAUrl, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn rebalancing_with_schwab_fails() {
        let secrets = toml_file(
            r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.infura.io"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1
            [rebalancing]
            transfer_timeout_secs = 1800
            inventory_staleness_bound_secs = 300
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

    /// Config with the broker identity in the config file's `[broker]`
    /// section (its new home) and `[issuance].base_url` in config.
    /// `identity_lines` is spliced in above the tuning fields.
    fn broker_identity_config_toml(identity_lines: &str) -> NamedTempFile {
        toml_file(&format!(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [issuance]
            base_url = "http://issuance.test:8000"

            [broker]
            {identity_lines}
            counter_trade_slippage_bps = 100
            close_flatten_cross_max_bps = 400
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
            extended_hours_close_flatten_window_secs = 900

            [broker.travel_rule]
            beneficiary_entity_name = "Acme Corp"

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
            "#,
        ))
    }

    /// New-shape secrets carrying only actual secrets. `broker_section` and
    /// `issuance_section` are complete TOML tables (or empty).
    fn secrets_only_secrets_toml(broker_section: &str, issuance_section: &str) -> NamedTempFile {
        toml_file(&format!(
            r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.infura.io"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

            {broker_section}

            {issuance_section}
            "#,
        ))
    }

    const CREDENTIALS_ONLY_BROKER: &str = r#"[broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret""#;

    const API_KEY_ONLY_ISSUANCE: &str = r#"[issuance]
            api_key = "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899""#;

    #[tokio::test]
    async fn legacy_broker_identity_resolves_from_config_with_credentials_only_secrets() {
        let config = broker_identity_config_toml(
            r#"type = "alpaca-broker-api"
            mode = "sandbox"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef""#,
        );
        let secrets = secrets_only_secrets_toml(CREDENTIALS_ONLY_BROKER, API_KEY_ONLY_ISSUANCE);

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();

        let BrokerCtx::AlpacaBrokerApi(alpaca) = ctx.broker else {
            panic!("expected the Alpaca broker, got DryRun");
        };
        assert_eq!(
            alpaca.account_id,
            "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
                .parse::<AlpacaAccountId>()
                .unwrap()
        );
        assert!(
            matches!(
                alpaca.auth,
                AlpacaBrokerAuth::Basic { ref api_key, ref api_secret }
                    if api_key == "test-key" && api_secret == "test-secret"
            ),
            "credentials must come from the secrets file"
        );
        assert_eq!(
            ctx.issuance.base_url.as_str(),
            "http://issuance.test:8000/",
            "issuance base_url must come from the config file"
        );
    }

    #[tokio::test]
    async fn kms_broker_fully_in_config_needs_no_broker_secrets() {
        let config = broker_identity_config_toml(
            r#"type = "alpaca-broker-api-kms"
            mode = "production"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
            client_id = "CKEXAMPLE"
            kms_key_version = "projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1""#,
        );
        let secrets = secrets_only_secrets_toml("", API_KEY_ONLY_ISSUANCE);

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();

        let BrokerCtx::AlpacaBrokerApi(alpaca) = ctx.broker else {
            panic!("expected the Alpaca broker, got DryRun");
        };
        assert!(
            matches!(
                alpaca.auth,
                AlpacaBrokerAuth::KmsJwt { ref client_id, ref kms_key_version }
                    if client_id == "CKEXAMPLE"
                        && kms_key_version
                            == "projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1"
            ),
            "the keyless broker must assemble entirely from the config file"
        );
    }

    /// The migration window: the config release has landed (identity in
    /// config) while the deployed secret still carries the old copy. Equal
    /// values are tolerated; see the conflict test for differing ones.
    #[tokio::test]
    async fn duplicated_equal_broker_identity_is_tolerated() {
        let config = broker_identity_config_toml(
            r#"type = "alpaca-broker-api"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef""#,
        );
        let secrets = alpaca_secrets_toml();

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();

        let BrokerCtx::AlpacaBrokerApi(alpaca) = ctx.broker else {
            panic!("expected the Alpaca broker, got DryRun");
        };
        assert_eq!(
            alpaca.account_id,
            "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
                .parse::<AlpacaAccountId>()
                .unwrap()
        );
        assert!(
            ctx.startup_notices.iter().any(|notice| {
                notice.level == StartupNoticeLevel::Warn
                    && notice
                        .message
                        .contains("[broker] account_id in the secrets file")
            }),
            "the deprecated identity copy must produce a notice naming exactly \
             the fields seen, got: {:?}",
            ctx.startup_notices
        );
    }

    /// A field set differently in both files must fail startup rather than
    /// silently prefer either copy: whichever file the operator believed
    /// they changed, half their intent would be discarded.
    #[tokio::test]
    async fn conflicting_broker_account_id_is_refused() {
        let config = broker_identity_config_toml(
            r#"type = "alpaca-broker-api"
            account_id = "aaaaaaaa-eeee-aaaa-dddd-beeeeeeeeeef""#,
        );
        let secrets = alpaca_secrets_toml();

        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                CtxError::BrokerIdentityConflict {
                    field: "account_id"
                }
            ),
            "expected BrokerIdentityConflict for account_id, got: {error}"
        );
    }

    #[tokio::test]
    async fn conflicting_broker_type_is_refused() {
        let config = broker_identity_config_toml(r#"type = "dry-run""#);
        let secrets = alpaca_secrets_toml();

        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(error, CtxError::BrokerIdentityConflict { field: "type" }),
            "expected BrokerIdentityConflict for type, got: {error}"
        );
    }

    #[tokio::test]
    async fn missing_broker_type_everywhere_is_refused() {
        let config = alpaca_trading_config_toml();
        let secrets = secrets_only_secrets_toml("", API_KEY_ONLY_ISSUANCE);

        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(error, CtxError::MissingBrokerType),
            "expected MissingBrokerType, got: {error}"
        );
    }

    #[tokio::test]
    async fn dry_run_broker_resolves_from_config_alone() {
        let config = broker_identity_config_toml(r#"type = "dry-run""#);
        let secrets = secrets_only_secrets_toml("", API_KEY_ONLY_ISSUANCE);

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();

        assert!(
            matches!(ctx.broker, BrokerCtx::DryRun),
            "expected DryRun, got: {:?}",
            ctx.broker
        );
    }

    /// A set-but-never-read identity field is a misconfiguration (probably a
    /// stale copy from a broker-type switch) and must fail startup instead
    /// of being silently ignored.
    #[tokio::test]
    async fn broker_field_not_applicable_to_kind_is_refused() {
        let config = broker_identity_config_toml(
            r#"type = "dry-run"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef""#,
        );
        let secrets = secrets_only_secrets_toml("", API_KEY_ONLY_ISSUANCE);

        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                CtxError::BrokerFieldNotForKind {
                    field: "account_id",
                    kind: "dry-run"
                }
            ),
            "expected BrokerFieldNotForKind for account_id, got: {error}"
        );
    }

    #[tokio::test]
    async fn legacy_broker_without_credentials_is_refused() {
        let config = broker_identity_config_toml(
            r#"type = "alpaca-broker-api"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef""#,
        );
        let secrets = secrets_only_secrets_toml("", API_KEY_ONLY_ISSUANCE);

        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(error, CtxError::MissingBrokerCredentials),
            "expected MissingBrokerCredentials, got: {error}"
        );
    }

    #[tokio::test]
    async fn issuance_base_url_conflict_is_refused() {
        let config = broker_identity_config_toml(r#"type = "dry-run""#);
        let secrets = secrets_only_secrets_toml(
            "",
            r#"[issuance]
            base_url = "http://issuance.elsewhere:8000"
            api_key = "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899""#,
        );

        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(error, CtxError::IssuanceBaseUrlConflict),
            "expected IssuanceBaseUrlConflict, got: {error}"
        );
    }

    /// The migration window for issuance: both files naming the same
    /// base_url is tolerated until the shim is removed.
    #[tokio::test]
    async fn issuance_base_url_duplicated_equal_is_tolerated() {
        let config = broker_identity_config_toml(r#"type = "dry-run""#);
        let secrets = secrets_only_secrets_toml(
            "",
            r#"[issuance]
            base_url = "http://issuance.test:8000"
            api_key = "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899""#,
        );

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();

        assert_eq!(ctx.issuance.base_url.as_str(), "http://issuance.test:8000/");
    }

    #[tokio::test]
    async fn issuance_base_url_missing_everywhere_is_refused() {
        let config = toml_file(&String::from_utf8_lossy(minimal_config_toml_bytes()));
        let secrets = secrets_only_secrets_toml(
            r#"[broker]
            type = "dry-run""#,
            API_KEY_ONLY_ISSUANCE,
        );

        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(
            matches!(error, CtxError::MissingIssuanceBaseUrl),
            "expected MissingIssuanceBaseUrl, got: {error}"
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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            counter_trade_slippage_bps = 100
            close_flatten_cross_max_bps = 400
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
            extended_hours_close_flatten_window_secs = 900
            [rebalancing]
            transfer_timeout_secs = 1800
            inventory_staleness_bound_secs = 300
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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "http://localhost:8545"

            [chains.hyperevm]
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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            counter_trade_slippage_bps = 100
            close_flatten_cross_max_bps = 400
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
            extended_hours_close_flatten_window_secs = 900

            [broker.travel_rule]
            beneficiary_entity_name = "Test Corp"
            [rebalancing]
            transfer_timeout_secs = 1800
            inventory_staleness_bound_secs = 300
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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "http://localhost:8545"

            [chains.hyperevm]
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
    async fn rebalancing_without_a_redemption_wallet_fails() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [wallet]
            kind = "private-key"
            address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

            [alerts]
            poll_interval = 300
            realert_interval = 3600

            [alerts.low_balance_thresholds]
            base = "0.05"
            ethereum = "0.05"

            [broker]
            counter_trade_slippage_bps = 100
            close_flatten_cross_max_bps = 400
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
            extended_hours_close_flatten_window_secs = 900

            [broker.travel_rule]
            beneficiary_entity_name = "Test Corp"

            [rebalancing]
            transfer_timeout_secs = 1800
            inventory_staleness_bound_secs = 300
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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.example.com"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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

    /// [`parse_and_validate`] never triggers the async wallet-key construction
    /// gated by the `wallet-private-key`/`wallet-turnkey` cargo features (that
    /// happens later, in `Ctx::load_files`), so it is used directly here
    /// rather than `Ctx::load_files` to keep this test feature-independent.
    fn rebalancing_toml_with_bot_gas_valuation(bot_gas_valuation_section: &str) -> String {
        format!(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [wallet]
            kind = "private-key"
            address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

            [alerts]
            poll_interval = 300
            realert_interval = 3600

            [alerts.low_balance_thresholds]
            base = "0.05"
            ethereum = "0.05"

            [broker]
            counter_trade_slippage_bps = 100
            close_flatten_cross_max_bps = 400
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
            extended_hours_close_flatten_window_secs = 900

            [broker.travel_rule]
            beneficiary_entity_name = "Test Corp"
            {bot_gas_valuation_section}

            [rebalancing]
            transfer_timeout_secs = 1800
            inventory_staleness_bound_secs = 300
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
            "#
        )
    }

    fn rebalancing_secrets_toml() -> NamedTempFile {
        toml_file(
            r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.example.com"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


            [broker]
            type = "alpaca-broker-api"
            api_key = "test-key"
            api_secret = "test-secret"
            account_id = "dddddddd-eeee-aaaa-dddd-beeeeeeeeeef"
            mode = "sandbox"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

            [issuance]
            base_url = "http://issuance.test:8000"
            api_key = "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
        "#,
        )
    }

    #[test]
    fn rebalancing_without_bot_gas_valuation_config_fails() {
        let config_str = rebalancing_toml_with_bot_gas_valuation("");
        let secrets = rebalancing_secrets_toml();
        let secrets_str = std::fs::read_to_string(secrets.path()).unwrap();

        let result = parse_and_validate(
            &config_str,
            Path::new("config.toml"),
            &secrets_str,
            Path::new("secrets.toml"),
        );

        assert!(
            matches!(result, Err(CtxError::MissingBotGasValuation)),
            "Expected MissingBotGasValuation error"
        );
    }

    #[test]
    fn rebalancing_accepts_configured_bot_gas_valuation() {
        let config_str = rebalancing_toml_with_bot_gas_valuation(
            r#"[bot_gas_valuation]
            chainlink_feed = "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70""#,
        );
        let secrets = rebalancing_secrets_toml();
        let secrets_str = std::fs::read_to_string(secrets.path()).unwrap();

        let parts = parse_and_validate(
            &config_str,
            Path::new("config.toml"),
            &secrets_str,
            Path::new("secrets.toml"),
        )
        .unwrap();

        let bot_gas_valuation = parts
            .bot_gas_valuation
            .expect("bot_gas_valuation should be Some when configured");
        assert_eq!(
            bot_gas_valuation.chainlink_feed,
            address!("0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70")
        );
    }

    #[test]
    fn rebalancing_without_alert_thresholds_fails() {
        let config_str = rebalancing_toml_with_bot_gas_valuation(
            r#"[bot_gas_valuation]
            chainlink_feed = "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70""#,
        )
        .replace(
            r#"            [alerts]
            poll_interval = 300
            realert_interval = 3600

            [alerts.low_balance_thresholds]
            base = "0.05"
            ethereum = "0.05"

"#,
            "",
        );
        let secrets = rebalancing_secrets_toml();
        let secrets_str = std::fs::read_to_string(secrets.path()).unwrap();

        let result = parse_and_validate(
            &config_str,
            Path::new("config.toml"),
            &secrets_str,
            Path::new("secrets.toml"),
        );

        assert!(matches!(result, Err(CtxError::MissingAlertsForRebalancing)));
    }

    /// The full bot-gas section plus the section under test -- the splice
    /// hole in [`rebalancing_toml_with_bot_gas_valuation`] is plain TOML
    /// text, and section order is insignificant, so appending
    /// `[orchestrator]` there exercises the complete real config shape.
    fn bot_gas_and_orchestrator_sections(orchestrator_section: &str) -> String {
        format!(
            r#"[bot_gas_valuation]
            chainlink_feed = "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70"

            {orchestrator_section}"#
        )
    }

    #[test]
    fn orchestrator_section_flows_into_parts() {
        let config_str =
            rebalancing_toml_with_bot_gas_valuation(&bot_gas_and_orchestrator_sections(
                r#"[orchestrator.addresses]
            base = "0x4444444444444444444444444444444444444444""#,
            ));
        let secrets = rebalancing_secrets_toml();
        let secrets_str = std::fs::read_to_string(secrets.path()).unwrap();

        let parts = parse_and_validate(
            &config_str,
            Path::new("config.toml"),
            &secrets_str,
            Path::new("secrets.toml"),
        )
        .unwrap();

        let orchestrator = parts
            .orchestrator
            .expect("orchestrator should be Some when configured");
        assert_eq!(
            orchestrator.addresses.get(Chain::Base),
            Some(address!("0x4444444444444444444444444444444444444444"))
        );
    }

    /// Absence of `[orchestrator]` is the dark default: every asset is
    /// vault-direct and the bot must run unchanged without the section.
    #[test]
    fn missing_orchestrator_section_parses_as_none() {
        let config_str =
            rebalancing_toml_with_bot_gas_valuation(&bot_gas_and_orchestrator_sections(""));
        let secrets = rebalancing_secrets_toml();
        let secrets_str = std::fs::read_to_string(secrets.path()).unwrap();

        let parts = parse_and_validate(
            &config_str,
            Path::new("config.toml"),
            &secrets_str,
            Path::new("secrets.toml"),
        )
        .unwrap();

        assert_eq!(parts.orchestrator, None);
    }

    /// A zero orchestrator address is a placeholder that slipped through;
    /// it must fail the whole config parse (and thus `validate-config`)
    /// even while no asset is orchestrator-mode.
    #[test]
    fn zero_orchestrator_address_fails_config_parse() {
        let config_str =
            rebalancing_toml_with_bot_gas_valuation(&bot_gas_and_orchestrator_sections(
                r#"[orchestrator.addresses]
            base = "0x0000000000000000000000000000000000000000""#,
            ));
        let secrets = rebalancing_secrets_toml();
        let secrets_str = std::fs::read_to_string(secrets.path()).unwrap();

        let result = parse_and_validate(
            &config_str,
            Path::new("config.toml"),
            &secrets_str,
            Path::new("secrets.toml"),
        );

        let source = match result {
            Err(CtxError::ConfigToml { source, .. }) => source,
            Err(other) => panic!("expected ConfigToml error, got {other:?}"),
            Ok(_) => panic!("zero orchestrator address must fail config parse"),
        };
        assert!(
            source
                .to_string()
                .contains("base must not be the zero address"),
            "expected zero-address rejection, got: {source}"
        );
    }

    #[test]
    fn standalone_mode_does_not_require_bot_gas_valuation() {
        let config_str = r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [wallet]
            kind = "private-key"
            address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        "#;
        let secrets_str = r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.example.com"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


            [broker]
            type = "dry-run"

            [wallet]
            private_key = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

            [issuance]
            base_url = "http://issuance.test:8000"
            api_key = "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
        "#;

        let parts = parse_and_validate(
            config_str,
            Path::new("config.toml"),
            secrets_str,
            Path::new("secrets.toml"),
        )
        .unwrap();

        assert!(
            parts.bot_gas_valuation.is_none(),
            "Standalone mode should not require [bot_gas_valuation]"
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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            counter_trade_slippage_bps = 100
            close_flatten_cross_max_bps = 400
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "http://localhost:8545"

            [chains.hyperevm]
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

    /// A KMS-stamped Turnkey wallet ([wallet].kms_api_key in config)
    /// must pass config/secrets pairing WITHOUT a [wallet] secrets
    /// section — the credential is an IAM-gated KMS key, not stored
    /// material. Construction proceeds to the (deferred) wallet build,
    /// which fails on the unreachable RPC here — the assertion is only
    /// that the pairing gate no longer rejects the shape.
    #[tokio::test]
    async fn kms_turnkey_wallet_needs_no_wallet_secrets() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            counter_trade_slippage_bps = 100
            extended_hours_reprice_timeout_secs = 300
            extended_hours_close_flatten_window_secs = 900

            [broker.travel_rule]
            beneficiary_entity_name = "Test Corp"

            [wallet]
            kind = "turnkey"
            address = "0x0000000000000000000000000000000000000001"
            organization_id = "org-test"
            kms_api_key = "projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1"
        "#,
        );

        let secrets = toml_file(
            r#"
            [chains.base]
            rpc_url = "http://localhost:1"

            [chains.ethereum]
            rpc_url = "http://localhost:1"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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
            !matches!(result, Err(CtxError::WalletSecretsMissing)),
            "KMS-stamped wallet must not require [wallet] secrets, got {result:?}"
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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            counter_trade_slippage_bps = 100
            close_flatten_cross_max_bps = 400
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
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
        let error = result.unwrap_err();
        let detail = std::error::Error::source(&error)
            .map(std::string::ToString::to_string)
            .unwrap_or_default();
        assert!(
            detail.contains("chains"),
            "a secrets file supplying no chain endpoints must fail naming the \
             missing table, got: {detail}"
        );
    }

    #[tokio::test]
    async fn wallet_without_hyperevm_rpc_url_fails() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            counter_trade_slippage_bps = 100
            close_flatten_cross_max_bps = 400
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.example.com"


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
                Err(CtxError::ChainRegistry(
                    crate::chain::ChainRegistryError::MissingSecrets {
                        chain: Chain::HyperEvm
                    }
                ))
            ),
            "a chain described in the config but absent from secrets must be \
             refused by the registry pairing check, got {result:?}"
        );
    }

    #[test]
    fn wallet_rpc_url_rejects_routable_http() {
        let url = Url::parse("http://mainnet.example.com").unwrap();

        let result = crate::wallet::require_secure_wallet_rpc_url(&url, Chain::HyperEvm);

        assert!(
            matches!(
                result,
                Err(crate::wallet::WalletCtxError::InsecureRpcUrl {
                    chain: Chain::HyperEvm
                })
            ),
            "routable http must be rejected, got {result:?}"
        );
    }

    #[test]
    fn wallet_rpc_url_rejects_non_http_loopback_schemes() {
        for url in ["ftp://localhost:8545", "ws://127.0.0.1:8545"] {
            let parsed = Url::parse(url).unwrap();

            let result = crate::wallet::require_secure_wallet_rpc_url(&parsed, Chain::Base);

            assert!(
                matches!(
                    result,
                    Err(crate::wallet::WalletCtxError::InsecureRpcUrl { chain: Chain::Base })
                ),
                "{url} must be rejected, got {result:?}"
            );
        }
    }

    #[test]
    fn wallet_rpc_url_allows_https_and_loopback_http() {
        for url in [
            "https://rpc.hyperliquid.xyz/evm",
            "http://localhost:8545",
            "http://127.0.0.1:8545",
            "http://[::1]:8545",
        ] {
            let parsed = Url::parse(url).unwrap();
            crate::wallet::require_secure_wallet_rpc_url(&parsed, Chain::Base).unwrap();
        }
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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "http://localhost:8545"

            [chains.hyperevm]
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
    async fn alpaca_broker_api_requires_close_flatten_cross_max_bps() {
        let config = alpaca_config_toml(None);
        let secrets = alpaca_secrets_toml();

        let err = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();

        assert!(
            matches!(err, CtxError::MissingCloseFlattenCrossMaxBps),
            "Expected MissingCloseFlattenCrossMaxBps, got: {err:?}"
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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            counter_trade_slippage_bps = 100
            close_flatten_cross_max_bps = 400

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = toml_file(
            r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.example.com"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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
            kind: None,
            mode: None,
            account_id: None,
            client_id: None,
            kms_key_version: None,
            counter_trade_slippage_bps: Some(100),
            extended_hours_reprice_timeout_secs: Some(u64::MAX),
            close_flatten_reprice_timeout_secs: Some(60),
            extended_hours_close_flatten_window_secs: Some(300),
            travel_rule: None,
            close_flatten_cross_max_bps: None,
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
    fn extended_hours_reprice_timeout_rejects_zero() {
        let broker = BrokerConfig {
            kind: None,
            mode: None,
            account_id: None,
            client_id: None,
            kms_key_version: None,
            counter_trade_slippage_bps: Some(100),
            extended_hours_reprice_timeout_secs: Some(0),
            close_flatten_reprice_timeout_secs: Some(60),
            extended_hours_close_flatten_window_secs: Some(300),
            travel_rule: None,
            close_flatten_cross_max_bps: None,
        };

        let error = broker.extended_hours_reprice_timeout_secs().unwrap_err();

        assert!(
            matches!(
                error,
                CtxError::ExtendedHoursRepriceTimeoutOutOfRange { configured: 0, .. }
            ),
            "Expected ExtendedHoursRepriceTimeoutOutOfRange, got: {error:?}"
        );
    }

    #[test]
    fn extended_hours_reprice_timeout_accepts_chrono_maximum() {
        let broker = BrokerConfig {
            kind: None,
            mode: None,
            account_id: None,
            client_id: None,
            kms_key_version: None,
            counter_trade_slippage_bps: Some(100),
            extended_hours_reprice_timeout_secs: Some(MAX_EXTENDED_HOURS_REPRICE_TIMEOUT_SECS),
            close_flatten_reprice_timeout_secs: Some(60),
            extended_hours_close_flatten_window_secs: Some(300),
            travel_rule: None,
            close_flatten_cross_max_bps: None,
        };

        assert_eq!(
            broker.extended_hours_reprice_timeout_secs().unwrap(),
            NonZeroU64::new(MAX_EXTENDED_HOURS_REPRICE_TIMEOUT_SECS).unwrap()
        );
    }

    #[test]
    fn close_flatten_reprice_timeout_is_required_and_rejects_zero() {
        let missing = BrokerConfig {
            kind: None,
            mode: None,
            account_id: None,
            client_id: None,
            kms_key_version: None,
            counter_trade_slippage_bps: Some(100),
            extended_hours_reprice_timeout_secs: Some(300),
            close_flatten_reprice_timeout_secs: None,
            extended_hours_close_flatten_window_secs: Some(300),
            travel_rule: None,
            close_flatten_cross_max_bps: None,
        };
        assert!(matches!(
            missing.close_flatten_reprice_timeout_secs(),
            Err(CtxError::MissingCloseFlattenRepriceTimeout)
        ));

        let zero = BrokerConfig {
            close_flatten_reprice_timeout_secs: Some(0),
            ..missing
        };
        assert!(matches!(
            zero.close_flatten_reprice_timeout_secs(),
            Err(CtxError::CloseFlattenRepriceTimeoutOutOfRange { configured: 0, .. })
        ));
    }

    #[test]
    fn extended_hours_close_flatten_window_rejects_values_chrono_cannot_represent() {
        let broker = BrokerConfig {
            kind: None,
            mode: None,
            account_id: None,
            client_id: None,
            kms_key_version: None,
            counter_trade_slippage_bps: Some(100),
            extended_hours_reprice_timeout_secs: Some(300),
            close_flatten_reprice_timeout_secs: Some(60),
            extended_hours_close_flatten_window_secs: Some(u64::MAX),
            travel_rule: None,
            close_flatten_cross_max_bps: None,
        };

        let error = broker
            .extended_hours_close_flatten_window_secs()
            .unwrap_err();

        assert!(
            matches!(
                error,
                CtxError::ExtendedHoursCloseFlattenWindowOutOfRange {
                    configured: u64::MAX,
                    ..
                }
            ),
            "Expected ExtendedHoursCloseFlattenWindowOutOfRange, got: {error:?}"
        );
    }

    #[test]
    fn extended_hours_close_flatten_window_accepts_chrono_maximum() {
        let broker = BrokerConfig {
            kind: None,
            mode: None,
            account_id: None,
            client_id: None,
            kms_key_version: None,
            counter_trade_slippage_bps: Some(100),
            extended_hours_reprice_timeout_secs: Some(300),
            close_flatten_reprice_timeout_secs: Some(60),
            extended_hours_close_flatten_window_secs: Some(
                MAX_EXTENDED_HOURS_CLOSE_FLATTEN_WINDOW_SECS,
            ),
            travel_rule: None,
            close_flatten_cross_max_bps: None,
        };

        assert_eq!(
            broker.extended_hours_close_flatten_window_secs().unwrap(),
            MAX_EXTENDED_HOURS_CLOSE_FLATTEN_WINDOW_SECS
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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            counter_trade_slippage_bps = 100
            close_flatten_cross_max_bps = 400
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = toml_file(
            r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.example.com"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            counter_trade_slippage_bps = 100
            close_flatten_cross_max_bps = 400
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
            extended_hours_close_flatten_window_secs = 0

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = toml_file(
            r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.example.com"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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
                CtxError::ExtendedHoursCloseFlattenWindowOutOfRange { configured: 0, .. }
            ),
            "Expected ExtendedHoursCloseFlattenWindowOutOfRange, got: {err:?}"
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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            counter_trade_slippage_bps = 0
        "#,
        );
        let secrets = toml_file(
            r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "http://localhost:8545"

            [chains.hyperevm]
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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            counter_trade_slippage_bps = 10000
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
            extended_hours_close_flatten_window_secs = 900
            close_flatten_cross_max_bps = 400
        "#,
        );
        let secrets = toml_file(
            r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "http://localhost:8545"

            [chains.hyperevm]
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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            counter_trade_slippage_bps = 9999
            close_flatten_cross_max_bps = 9999
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.infura.io"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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

        assert_eq!(ctx.broker.counter_trade_slippage_bps(), 9999);

        let BrokerCtx::AlpacaBrokerApi(broker) = &ctx.broker else {
            panic!("expected AlpacaBrokerApi broker");
        };

        assert_eq!(broker.counter_trade_slippage_bps, 9999);
    }

    #[tokio::test]
    async fn close_flatten_cross_max_bps_accepts_the_slippage_base_as_its_floor() {
        let config = alpaca_config_toml(Some(100));
        let secrets = alpaca_secrets_toml();

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();

        assert_eq!(ctx.broker.counter_trade_slippage_bps(), 100);
        assert_eq!(ctx.close_flatten_cross_max_bps, 100);
    }

    /// The ramp runs from `counter_trade_slippage_bps` up to this ceiling, so a
    /// ceiling below the base would run it backwards and price a close-flatten
    /// hedge *less* aggressively as the close approached.
    #[tokio::test]
    async fn close_flatten_cross_max_bps_below_the_slippage_base_is_rejected() {
        let config = alpaca_config_toml(Some(50));
        let secrets = alpaca_secrets_toml();

        let err = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();

        let CtxError::CloseFlattenCrossMaxBpsOutOfRange {
            configured,
            min,
            max,
        } = err
        else {
            panic!("expected CloseFlattenCrossMaxBpsOutOfRange, got: {err:?}");
        };
        assert_eq!((configured, min, max), (50, 100, 9_999));
    }

    /// The ceiling shares `counter_trade_slippage_bps`'s upper bound: a cross of
    /// 100% or more is a typo, not a tuning choice.
    #[tokio::test]
    async fn close_flatten_cross_max_bps_above_the_slippage_ceiling_is_rejected() {
        let config = alpaca_config_toml(Some(10_000));
        let secrets = alpaca_secrets_toml();

        let err = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();

        let CtxError::CloseFlattenCrossMaxBpsOutOfRange {
            configured,
            min,
            max,
        } = err
        else {
            panic!("expected CloseFlattenCrossMaxBpsOutOfRange, got: {err:?}");
        };
        assert_eq!((configured, min, max), (10_000, 100, 9_999));
    }

    #[tokio::test]
    async fn alpaca_broker_api_executor_uses_dollar_threshold() {
        let config = alpaca_config_toml(Some(400));
        let secrets = alpaca_secrets_toml();

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();
        let expected = ExecutionThreshold::dollar_value(Usdc::new(float!(2))).unwrap();
        assert_eq!(ctx.execution_threshold, expected);
        assert_eq!(
            ctx.extended_hours_reprice_timeout_secs,
            NonZeroU64::new(300)
        );
        assert_eq!(ctx.close_flatten_cross_max_bps, 400);
    }

    #[tokio::test]
    async fn missing_issuance_section_fails_at_startup() {
        let config = minimal_config_toml();
        let secrets = toml_file(
            r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.infura.io"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.infura.io"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.infura.io"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.infura.io"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.infura.io"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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
        // wallet feature being enabled for a full Ctx::load_files. The
        // base_url rides in the deprecated secrets location here, which the
        // migration shim must still honor (and notice).
        let mut notices = Vec::new();
        let ctx = issuance_ctx(
            None,
            Some(IssuanceSecretsToml {
                base_url: Some(Url::parse("http://issuance.test:8000").unwrap()),
                api_key: "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
                    .to_owned(),
            }),
            &mut notices,
        )
        .expect("a valid issuance secret must assemble the ctx");

        assert_eq!(
            ctx.base_url,
            Url::parse("http://issuance.test:8000").unwrap(),
            "base_url must come from the (deprecated) issuance secrets location"
        );
        assert_eq!(
            notices.len(),
            1,
            "the deprecated base_url location must produce a notice"
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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.infura.io"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.infura.io"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"
            redemption_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1
            [rebalancing]
            transfer_timeout_secs = 1800
            inventory_staleness_bound_secs = 300
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

        let base = config
            .chains
            .get(&Chain::Base)
            .and_then(|chain| chain.trading.as_ref())
            .expect("prod config must describe Base as a trading chain");
        let global_limit = base.assets.equities.operational_limit.map(Positive::inner);

        let broker = config.broker.expect(
            "prod config must include [broker.travel_rule] — \
             Alpaca rejects whitelist requests without it, effective 2026-03-27",
        );

        broker
            .counter_trade_slippage_bps
            .expect("prod config must set [broker].counter_trade_slippage_bps");
        broker
            .close_flatten_cross_max_bps
            .expect("prod config must set [broker].close_flatten_cross_max_bps");
        broker
            .extended_hours_reprice_timeout_secs
            .expect("prod config must set [broker].extended_hours_reprice_timeout_secs");
        broker
            .close_flatten_reprice_timeout_secs
            .expect("prod config must set [broker].close_flatten_reprice_timeout_secs");
        broker
            .extended_hours_close_flatten_window_secs
            .expect("prod config must set [broker].extended_hours_close_flatten_window_secs");
        broker
            .travel_rule
            .expect("prod config must include [broker.travel_rule]")
            .validated()
            .unwrap();

        for (symbol, equity) in &base.assets.equities.symbols {
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
    fn staging_config_toml_is_valid() {
        let config_str = include_str!("../../../config/staging/st0x-hedge.toml");
        let config: Config = toml::from_str(config_str).unwrap();

        let broker = config
            .broker
            .expect("staging config must include the [broker] section");

        broker
            .counter_trade_slippage_bps
            .expect("staging config must set [broker].counter_trade_slippage_bps");
        broker
            .close_flatten_cross_max_bps
            .expect("staging config must set [broker].close_flatten_cross_max_bps");
        broker
            .extended_hours_reprice_timeout_secs
            .expect("staging config must set [broker].extended_hours_reprice_timeout_secs");
        broker
            .close_flatten_reprice_timeout_secs
            .expect("staging config must set [broker].close_flatten_reprice_timeout_secs");
        broker
            .extended_hours_close_flatten_window_secs
            .expect("staging config must set [broker].extended_hours_close_flatten_window_secs");
        broker
            .travel_rule
            .expect("staging config must include [broker.travel_rule]")
            .validated()
            .unwrap();
    }

    #[test]
    fn staging_gcp_config_toml_is_valid() {
        let config_str = include_str!("../../../config/staging-gcp/st0x-hedge.toml");
        let config: Config = toml::from_str(config_str).unwrap();

        let broker = config
            .broker
            .expect("staging-gcp config must include the [broker] section");

        broker
            .counter_trade_slippage_bps
            .expect("staging-gcp config must set [broker].counter_trade_slippage_bps");
        broker
            .close_flatten_cross_max_bps
            .expect("staging-gcp config must set [broker].close_flatten_cross_max_bps");
        broker
            .extended_hours_reprice_timeout_secs
            .expect("staging-gcp config must set [broker].extended_hours_reprice_timeout_secs");
        broker
            .close_flatten_reprice_timeout_secs
            .expect("staging-gcp config must set [broker].close_flatten_reprice_timeout_secs");
        broker.extended_hours_close_flatten_window_secs.expect(
            "staging-gcp config must set [broker].extended_hours_close_flatten_window_secs",
        );
        broker
            .travel_rule
            .expect("staging-gcp config must include [broker.travel_rule]")
            .validated()
            .unwrap();
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
            .close_flatten_cross_max_bps
            .expect("s01-issuer config must set [broker].close_flatten_cross_max_bps");
        broker
            .extended_hours_reprice_timeout_secs
            .expect("s01-issuer config must set [broker].extended_hours_reprice_timeout_secs");
        broker
            .close_flatten_reprice_timeout_secs
            .expect("s01-issuer config must set [broker].close_flatten_reprice_timeout_secs");
        broker
            .extended_hours_close_flatten_window_secs
            .expect("s01-issuer config must set [broker].extended_hours_close_flatten_window_secs");
        broker
            .travel_rule
            .expect("s01-issuer config must include [broker.travel_rule]")
            .validated()
            .unwrap();

        // The NAV bump tokenizes + donates the wrapped equity, so the bumped
        // symbol must carry both token addresses the wrap/donate path resolves.
        let sgov = config
            .chains
            .get(&Chain::Base)
            .and_then(|chain| chain.trading.as_ref())
            .expect("s01-issuer config must describe Base as a trading chain")
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

    #[test]
    fn e2e_secrets_toml_is_valid() {
        let secrets_str = include_str!("../../../e2e/secrets.toml");
        let _: Secrets = toml::from_str(secrets_str).unwrap();
    }

    #[test]
    fn duplicate_retired_symbols_are_rejected() {
        let error = DeploymentSymbolPolicy::new(
            [],
            [Symbol::new("QSEP").unwrap(), Symbol::new("QSEP").unwrap()],
        )
        .unwrap_err();

        assert!(matches!(error, CtxError::DuplicateRetiredSymbol { .. }));
    }

    #[test]
    fn configured_symbol_cannot_also_be_retired() {
        let qsep = Symbol::new("QSEP").unwrap();
        let error = DeploymentSymbolPolicy::new([qsep.clone()], [qsep]).unwrap_err();

        assert!(matches!(
            error,
            CtxError::ConfiguredSymbolMarkedRetired { .. }
        ));
    }

    #[test]
    fn deployment_symbol_policy_reads_only_plaintext_asset_config() {
        let config = toml_file(
            r#"
                unrelated_top_level_key = "ignored by the narrow deploy loader"

                [assets.equities]
                retired_symbols = ["QSEP"]

                [assets.equities.AAPL]
                extended_hours_counter_trading = "disabled"
            "#,
        );

        let policy = load_deployment_symbol_policy(config.path()).unwrap();

        assert_eq!(
            policy.configured(),
            &BTreeSet::from([Symbol::new("AAPL").unwrap()])
        );
        assert_eq!(
            policy.retired(),
            &BTreeSet::from([Symbol::new("QSEP").unwrap()])
        );
    }

    #[test]
    fn deployment_symbol_policy_loader_rejects_duplicate_retired_symbols() {
        let config = toml_file(
            r#"
                [assets.equities]
                retired_symbols = ["QSEP", "QSEP"]
            "#,
        );

        let error = load_deployment_symbol_policy(config.path()).unwrap_err();

        assert!(matches!(error, CtxError::DuplicateRetiredSymbol { .. }));
    }

    #[test]
    fn deployment_symbol_policy_loader_rejects_configured_retired_symbol() {
        let config = toml_file(
            r#"
                [assets.equities]
                retired_symbols = ["QSEP"]

                [assets.equities.QSEP]
                extended_hours_counter_trading = "disabled"
            "#,
        );

        let error = load_deployment_symbol_policy(config.path()).unwrap_err();

        assert!(matches!(
            error,
            CtxError::ConfiguredSymbolMarkedRetired { .. }
        ));
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

    /// `parse_and_validate` rejects a config with `[rebalancing]` but no
    /// `[bot_gas_valuation]` (`CtxError::MissingBotGasValuation`). But
    /// `all_repo_config_tomls_are_valid` only exercises `toml::from_str`,
    /// a structural parse that succeeds either way since the field is
    /// `Option` -- so a future edit that drops or renames
    /// `[bot_gas_valuation]` from a checked-in `[rebalancing]` config would
    /// stay green there and only fail at the `validate-config` deploy gate
    /// or at bot startup. Guard the cross-field invariant here instead, the
    /// same way `repo_config_vault_owner_matches_settlement_mode` guards its
    /// own cross-field invariant.
    #[test]
    fn repo_config_rebalancing_requires_bot_gas_valuation() {
        for path in repo_config_paths() {
            let contents = std::fs::read_to_string(&path).unwrap();
            let config: Config = toml::from_str(&contents).unwrap();

            assert!(
                config.rebalancing.is_none() || config.bot_gas_valuation.is_some(),
                "{path:?}: [rebalancing] is configured but [bot_gas_valuation] is \
                 missing -- parse_and_validate rejects this combination at startup"
            );
        }
    }

    #[test]
    fn repo_config_rebalancing_requires_alert_thresholds() {
        for path in repo_config_paths() {
            let contents = std::fs::read_to_string(&path).unwrap();
            let config: Config = toml::from_str(&contents).unwrap();

            if config.rebalancing.is_none() {
                continue;
            }

            let alerts = AlertsCtx::new(config.alerts, &mut Vec::new()).unwrap_or_else(|error| {
                panic!("{path:?}: invalid [alerts] gas thresholds: {error}")
            });

            assert!(
                alerts.is_some(),
                "{path:?}: [rebalancing] is configured but [alerts] is missing -- \
                 fresh transfers would have no native-gas safety threshold"
            );
        }
    }

    /// Every checked-in config's `[bot_gas_valuation].chainlink_feed` must be
    /// Chainlink's standard ETH/USD proxy on Base, not just a syntactically
    /// valid address: `BotGasValuationConfig` accepts any `Address`, so a
    /// typo or a copy of a feed from another network parses and passes
    /// `repo_config_rebalancing_requires_bot_gas_valuation` cleanly, then
    /// dead-letters every Base bot-gas cost fact as an opaque RPC failure at
    /// runtime (ADR 0020: contract-call errors are classified transient, so a
    /// wrong address burns the full redrive budget per receipt before
    /// dead-lettering, rather than failing fast).
    ///
    /// Source: Chainlink's Base mainnet ETH/USD feed registry,
    /// https://data.chain.link/feeds/base/base/eth-usd.
    const BASE_CHAINLINK_ETH_USD_FEED: Address =
        address!("0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70");

    #[test]
    fn repo_config_bot_gas_valuation_chainlink_feed_matches_base_deployment() {
        for path in repo_config_paths() {
            let contents = std::fs::read_to_string(&path).unwrap();
            let config: Config = toml::from_str(&contents).unwrap();

            let Some(bot_gas_valuation) = config.bot_gas_valuation else {
                continue;
            };

            assert_eq!(
                bot_gas_valuation.chainlink_feed, BASE_CHAINLINK_ETH_USD_FEED,
                "{path:?}: [bot_gas_valuation].chainlink_feed does not match Chainlink's \
                 standard ETH/USD proxy on Base -- a wrong address dead-letters every \
                 Base bot-gas cost fact as an opaque contract-call failure"
            );
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

            for (chain, chain_config) in &config.chains {
                let Some(trading) = &chain_config.trading else {
                    continue;
                };

                match (trading.inventory_mode, trading.inventory) {
                    (InventoryModeTag::Legacy, None) => {
                        if let Some(wallet) = &wallet {
                            assert_eq!(
                                trading.vault_owner, wallet.address,
                                "{path:?} [chains.{chain}]: inventory_mode = \"legacy\" means \
                                 the bot's EOA owns the vaults, so vault_owner must equal \
                                 [wallet].address"
                            );
                        }
                    }
                    (InventoryModeTag::Legacy, Some(inventory)) => {
                        // TradingChain::new rejects this combination at
                        // startup (LegacyWithInventory); asserting it here
                        // fails the contradiction in CI instead of at the
                        // deploy gate.
                        panic!(
                            "{path:?} [chains.{chain}]: inventory_mode = \"legacy\" forbids an \
                             inventory address, found {inventory}"
                        )
                    }
                    (InventoryModeTag::Managed, Some(inventory)) => assert_eq!(
                        trading.vault_owner, inventory,
                        "{path:?} [chains.{chain}]: inventory_mode = \"managed\" means the \
                         inventory contract owns the vaults, so vault_owner must equal inventory"
                    ),
                    (InventoryModeTag::Managed, None) => panic!(
                        "{path:?} [chains.{chain}]: inventory_mode = \"managed\" requires an \
                         inventory address"
                    ),
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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10
            bogus_field = "should fail"

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [assets]
            bogus_field = "should fail"

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [assets.equities]
            retired_symbols = []

            [assets.equities.AAPL]
            extended_hours_counter_trading = "disabled"

            [chains.base.trading.assets.equities.AAPL]
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"
            bogus_field = "should fail"

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.cash]
            rebalancing = "disabled"
            bogus_field = "should fail"

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "http://localhost:8545"

            [chains.hyperevm]
            rpc_url = "http://localhost:8545"

            extra_secret = "surprise"

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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "http://localhost:8545"

            [chains.hyperevm]
            rpc_url = "http://localhost:8545"

            extra_secret = "surprise"

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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10
            bogus_field = "should fail"

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "http://localhost:8545"

            [chains.hyperevm]
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
    fn broker_type_tag_uses_kebab_case() {
        let variants = [
            ("dry-run", "DryRun"),
            ("alpaca-broker-api", "AlpacaBrokerApi"),
            ("alpaca-broker-api-kms", "AlpacaBrokerApiKms"),
        ];

        for (kebab_value, variant_name) in variants {
            let toml_str = format!(
                r#"
                [chains.base]
                rpc_url = "http://localhost:8545"

                [chains.ethereum]
                rpc_url = "http://localhost:8545"

                [chains.hyperevm]
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

    /// Both files now key RPC endpoints by chain. The retired flat `[evm]`
    /// section must be rejected rather than silently ignored: a secrets file
    /// still carrying it supplies no endpoint for any chain, and the registry
    /// would fail later with a missing-secrets error that does not say why.
    #[test]
    fn secrets_reject_the_retired_flat_evm_section() {
        let per_chain = r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "http://localhost:8545"

            [chains.hyperevm]
            rpc_url = "http://localhost:8545"

            [broker]
            type = "dry-run"
        "#;

        let secrets = toml::from_str::<Secrets>(per_chain).unwrap();
        assert_eq!(secrets.chains.len(), 3);
        assert_eq!(
            secrets.chains[&Chain::Base].rpc_url.as_str(),
            "http://localhost:8545/"
        );

        let flat = r#"
            [evm]
            rpc_url = "http://localhost:8545"

            [broker]
            type = "dry-run"
        "#;

        let Err(error) = toml::from_str::<Secrets>(flat) else {
            panic!("the retired [evm] section must be rejected")
        };
        assert!(
            error.to_string().contains("evm"),
            "expected an unknown-field error naming [evm], got: {error}"
        );
    }

    #[test]
    fn broker_type_tag_rejects_snake_case() {
        let snake_values = ["dry_run", "alpaca_broker_api"];

        for snake_value in snake_values {
            let toml_str = format!(
                r#"
                [chains.base]
                rpc_url = "http://localhost:8545"

                [chains.ethereum]
                rpc_url = "http://localhost:8545"

                [chains.hyperevm]
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

    #[tokio::test]
    async fn load_files_assembles_pricing_for_configured_equities() {
        let config = equity_pricing_config_toml(true);
        let secrets = dry_run_pricing_secrets_toml();

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();

        let pricing = ctx.pricing.expect("pricing context should be assembled");
        assert_eq!(pricing.ws_url.as_str(), "wss://pricing.test/ws");
        assert!(!format!("{pricing:?}").contains("pricing-oracle-test-key"));
    }

    #[tokio::test]
    async fn load_files_requires_pricing_config_for_configured_equities() {
        let config = equity_pricing_config_toml(false);
        let secrets = dry_run_pricing_secrets_toml();

        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            CtxError::Pricing(PricingCtxError::MissingConfig)
        ));
    }

    #[tokio::test]
    async fn load_files_requires_pricing_secrets_for_configured_equities() {
        let config = equity_pricing_config_toml(true);
        let secrets = dry_run_secrets_toml();

        let error = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            CtxError::Pricing(PricingCtxError::MissingSecrets)
        ));
    }

    #[cfg(feature = "wallet-turnkey")]
    #[test]
    fn load_turnkey_approval_policy_inputs_extracts_validated_deploy_inputs() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [assets.equities]
            retired_symbols = []

            [assets.equities.AAPL]
            extended_hours_counter_trading = "disabled"

            [chains.base.trading.assets.equities.AAPL]
            tokenized_equity = "0x4444444444444444444444444444444444444444"
            tokenized_equity_derivative = "0x5555555555555555555555555555555555555555"
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"

            [pricing]
            ws_url = "wss://pricing.test/ws"

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [wallet]
            kind = "turnkey"
            address = "0x6666666666666666666666666666666666666666"
            organization_id = "org-test"
            "#,
        );
        let secrets = toml_file(
            r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://ethereum.example.com"

            [chains.hyperevm]
            rpc_url = "https://hyperevm.example.com"


            [broker]
            type = "dry-run"

            [wallet]
            api_private_key = "secret-p256-key"

            [issuance]
            base_url = "http://issuance.test:8000"
            api_key = "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"

            [pricing]
            api_key = "pricing-oracle-test-key"
            "#,
        );

        let inputs = Ctx::load_turnkey_approval_policy_inputs(config.path(), secrets.path())
            .unwrap()
            .unwrap();

        assert_eq!(inputs.organization_id.as_str(), "org-test");
        assert_eq!(
            inputs.wallet_address,
            address!("0x6666666666666666666666666666666666666666")
        );
        assert_eq!(
            inputs.orderbook,
            address!("0x1111111111111111111111111111111111111111")
        );
        assert!(
            inputs
                .assets
                .is_trading_enabled(&Symbol::new("AAPL").unwrap())
        );
        assert!(inputs.kms_api_key.is_none());
        assert!(inputs.api_private_key.is_some());
        assert!(!format!("{inputs:?}").contains("secret-p256-key"));
    }

    #[cfg(feature = "wallet-turnkey")]
    #[test]
    fn load_turnkey_approval_policy_inputs_skips_non_turnkey_wallet() {
        let config = minimal_config_toml();
        let secrets = dry_run_secrets_toml();

        let inputs =
            Ctx::load_turnkey_approval_policy_inputs(config.path(), secrets.path()).unwrap();

        assert!(inputs.is_none());
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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [assets.equities]
            retired_symbols = []

            [assets.equities.AAPL]
            extended_hours_counter_trading = "enabled"

            [chains.base.trading.assets.equities.AAPL]
            tokenized_equity = "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
            tokenized_equity_derivative = "0xf4f8c66085910d583c01f3b4e44bf731d4e2c565"
            trading = "disabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"

            [pricing]
            ws_url = "wss://pricing.test/ws"

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "legacy"
            inventory_adapters = []
            vault_owner = "0x0000000000000000000000000000000000000001"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = dry_run_pricing_secrets_toml();

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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [assets.equities]
            retired_symbols = []

            [assets.equities.AAPL]
            extended_hours_counter_trading = "enabled"

            [chains.base.trading.assets.equities.AAPL]
            tokenized_equity = "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
            tokenized_equity_derivative = "0xf4f8c66085910d583c01f3b4e44bf731d4e2c565"
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"

            [pricing]
            ws_url = "wss://pricing.test/ws"

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "legacy"
            inventory_adapters = []
            vault_owner = "0x0000000000000000000000000000000000000001"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
            extended_hours_close_flatten_window_secs = 900
            close_flatten_cross_max_bps = 400

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = dry_run_pricing_secrets_toml();

        Ctx::validate_files(config.path(), secrets.path()).unwrap();
    }

    #[test]
    fn dry_run_broker_requires_extended_hours_reprice_timeout_when_extended_hours_enabled() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [assets.equities]
            retired_symbols = []

            [assets.equities.AAPL]
            extended_hours_counter_trading = "enabled"

            [chains.base.trading.assets.equities.AAPL]
            tokenized_equity = "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
            tokenized_equity_derivative = "0xf4f8c66085910d583c01f3b4e44bf731d4e2c565"
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"

            [pricing]
            ws_url = "wss://pricing.test/ws"

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "legacy"
            inventory_adapters = []
            vault_owner = "0x0000000000000000000000000000000000000001"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = dry_run_pricing_secrets_toml();

        let error = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();

        assert!(
            matches!(error, CtxError::MissingExtendedHoursRepriceTimeout),
            "Expected MissingExtendedHoursRepriceTimeout for DryRun broker with extended \
             hours enabled and no configured timeout, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn dry_run_broker_honors_configured_extended_hours_reprice_timeout() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [assets.equities]
            retired_symbols = []

            [assets.equities.AAPL]
            extended_hours_counter_trading = "enabled"

            [chains.base.trading.assets.equities.AAPL]
            tokenized_equity = "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
            tokenized_equity_derivative = "0xf4f8c66085910d583c01f3b4e44bf731d4e2c565"
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"

            [pricing]
            ws_url = "wss://pricing.test/ws"

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "legacy"
            inventory_adapters = []
            vault_owner = "0x0000000000000000000000000000000000000001"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
            extended_hours_close_flatten_window_secs = 900
            close_flatten_cross_max_bps = 400

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = dry_run_pricing_secrets_toml();

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();

        assert_eq!(
            ctx.extended_hours_reprice_timeout_secs,
            NonZeroU64::new(300)
        );
        assert_eq!(ctx.close_flatten_reprice_timeout_secs, 60);
        assert_eq!(ctx.extended_hours_close_flatten_window_secs, 900);
    }

    /// DryRun config with extended hours enabled, so the close-flatten keys are
    /// required. `counter_trade_slippage_bps` is a parameter because DryRun
    /// never reads it: the ramp base is the executor default, and validation
    /// must be checked against the base the runtime actually uses.
    fn dry_run_extended_hours_config_toml(
        counter_trade_slippage_bps: Option<u16>,
        close_flatten_cross_max_bps: u16,
    ) -> NamedTempFile {
        let slippage_line = counter_trade_slippage_bps
            .map(|bps| format!("counter_trade_slippage_bps = {bps}"))
            .unwrap_or_default();

        toml_file(&format!(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [assets.equities]
            retired_symbols = []

            [assets.equities.AAPL]
            extended_hours_counter_trading = "enabled"

            [chains.base.trading.assets.equities.AAPL]
            tokenized_equity = "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
            tokenized_equity_derivative = "0xf4f8c66085910d583c01f3b4e44bf731d4e2c565"
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"

            [pricing]
            ws_url = "wss://pricing.test/ws"

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "legacy"
            inventory_adapters = []
            vault_owner = "0x0000000000000000000000000000000000000001"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            {slippage_line}
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
            extended_hours_close_flatten_window_secs = 900
            close_flatten_cross_max_bps = {close_flatten_cross_max_bps}

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#
        ))
    }

    /// A DryRun ceiling equal to a configured base still runs the ramp
    /// backwards, because DryRun builds the ramp from the executor default
    /// instead of the configured value.
    #[test]
    fn dry_run_close_flatten_cross_max_bps_below_the_executor_default_is_rejected() {
        let config = dry_run_extended_hours_config_toml(Some(50), 50);
        let secrets = dry_run_pricing_secrets_toml();

        let err = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();
        let message = err.to_string();

        let CtxError::CloseFlattenCrossMaxBpsOutOfRange {
            configured,
            min,
            max,
        } = err
        else {
            panic!("expected CloseFlattenCrossMaxBpsOutOfRange, got: {err:?}");
        };
        assert_eq!(
            (configured, min, max),
            (50, DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS, 9_999)
        );
        assert!(message.contains("minimum is the effective runtime counter-trade slippage base"));
    }

    /// The mirror case: a ceiling at the executor default is accepted even
    /// though the configured base sits below it, since the configured base is
    /// dead weight under DryRun.
    #[tokio::test]
    async fn dry_run_close_flatten_cross_max_bps_accepts_the_executor_default_as_its_floor() {
        let config =
            dry_run_extended_hours_config_toml(Some(50), DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS);
        let secrets = dry_run_pricing_secrets_toml();

        let ctx = Ctx::load_files(config.path(), secrets.path())
            .await
            .unwrap();

        assert_eq!(
            ctx.close_flatten_cross_max_bps,
            DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS
        );
        assert_eq!(
            ctx.broker.counter_trade_slippage_bps(),
            DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS
        );
    }

    #[test]
    fn dry_run_broker_requires_extended_hours_close_flatten_window_when_extended_hours_enabled() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            apalis_finished_job_cleanup_interval_secs = 3600
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [assets.equities]
            retired_symbols = []

            [assets.equities.AAPL]
            extended_hours_counter_trading = "enabled"

            [chains.base.trading.assets.equities.AAPL]
            tokenized_equity = "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
            tokenized_equity_derivative = "0xf4f8c66085910d583c01f3b4e44bf731d4e2c565"
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"

            [pricing]
            ws_url = "wss://pricing.test/ws"

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "legacy"
            inventory_adapters = []
            vault_owner = "0x0000000000000000000000000000000000000001"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60

            [wallet]
            kind = "private-key"
            address = "0x0000000000000000000000000000000000000001"
        "#,
        );
        let secrets = dry_run_pricing_secrets_toml();

        let error = Ctx::validate_files(config.path(), secrets.path()).unwrap_err();

        assert!(
            matches!(error, CtxError::MissingExtendedHoursCloseFlattenWindow),
            "Expected MissingExtendedHoursCloseFlattenWindow for DryRun broker with \
             extended hours enabled and no configured close-flatten window, got: {error:?}"
        );
    }

    #[test]
    fn validate_files_rejects_invalid_config_toml() {
        let config = toml_file(
            r#"
            database_url = ":memory:"
            server_port = 8080
            board_port = 8081
            bogus_field = "should fail"

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "http://localhost:8545"

            [chains.hyperevm]
            rpc_url = "http://localhost:8545"

            extra_secret = "surprise"

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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1
        "#,
        );
        let secrets = toml_file(
            r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "http://localhost:8545"

            [chains.hyperevm]
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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            counter_trade_slippage_bps = 100
            close_flatten_cross_max_bps = 400
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
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
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "http://localhost:8545"

            [chains.hyperevm]
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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

            [broker]
            counter_trade_slippage_bps = 100
            close_flatten_cross_max_bps = 400
            extended_hours_reprice_timeout_secs = 300
            close_flatten_reprice_timeout_secs = 60
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
            matches!(error, CtxError::SecretsToml { .. }),
            "a secrets file supplying no chain endpoints must fail to parse, \
             got {error:?}"
        );
    }

    #[test]
    fn validate_files_rejects_alpaca_without_travel_rule() {
        let config = alpaca_trading_config_toml();
        let secrets = toml_file(
            r#"
            [chains.base]
            rpc_url = "http://localhost:8545"

            [chains.ethereum]
            rpc_url = "https://mainnet.infura.io"

            [chains.hyperevm]
            rpc_url = "https://rpc.hyperliquid.xyz/evm"


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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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
            inventory_divergence_threshold = 10
            hedge_order_gate_reconciliation_timeout_secs = 10
            position_check_interval = 0

            [chains.base.trading.assets.equities]

            [chains.base]
            lifecycle = "active"
            required_confirmations = 3

            [chains.base.trading]
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "managed"
            inventory_adapters = []
            inventory = "0x2222222222222222222222222222222222222222"
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            ingestion_cutoff = "safe"

            [chains.ethereum]
            lifecycle = "active"
            required_confirmations = 12

            [chains.hyperevm]
            lifecycle = "observe-only"
            required_confirmations = 1

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
