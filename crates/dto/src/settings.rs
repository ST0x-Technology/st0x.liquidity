//! Operational settings DTOs for the dashboard overview.

use serde::Serialize;
use ts_rs::TS;

use st0x_finance::{Symbol, Usd};

/// Operational settings shown on the dashboard overview.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    pub equity_target: f64,
    pub equity_deviation: f64,
    pub usdc_target: Option<f64>,
    pub usdc_deviation: Option<f64>,
    #[ts(as = "Option<String>")]
    pub cash_reserved: Option<Usd>,
    pub execution_threshold: String,
    pub assets: Vec<AssetSettings>,

    // Wallet
    pub wallet: Option<WalletSettings>,

    // Operational config
    pub log_level: String,
    pub server_port: u16,
    pub orderbook: String,
    #[ts(type = "number")]
    pub deployment_block: u64,
    pub trading_mode: String,
    pub broker: String,
    #[ts(type = "number")]
    pub order_polling_interval: u64,
    #[ts(type = "number")]
    pub inventory_poll_interval: u64,
}

/// Wallet provider settings shown in the config dialog.
///
/// Exposes only non-secret metadata: wallet kind, address, and
/// backend-specific identifiers (e.g., Turnkey organization ID).
/// The private key / API key is never included.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WalletSettings {
    /// Signing backend: `"turnkey"` or `"private-key"`.
    pub kind: String,
    /// Hex-encoded Ethereum address (e.g., `"0xabcd…"`).
    pub address: String,
    /// Turnkey organization ID (present only for `kind = "turnkey"`).
    pub organization_id: Option<String>,
}

/// Per-asset operational settings.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct AssetSettings {
    #[ts(type = "string")]
    pub symbol: Symbol,
    pub trading: bool,
    pub rebalancing: bool,
    pub extended_hours: bool,
    pub operational_limit: Option<String>,
}
