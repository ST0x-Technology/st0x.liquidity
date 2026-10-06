//! Operational settings DTOs for the dashboard overview.

use serde::Serialize;
use ts_rs::TS;

use st0x_finance::{Symbol, Usd};

use crate::ChainName;

/// Operational settings shown on the dashboard overview.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    /// The primary chain's chain-level default target share; absent when
    /// only per-symbol overrides are configured.
    pub equity_target: Option<f64>,
    pub equity_deviation: f64,
    pub usdc_target: Option<f64>,
    pub usdc_deviation: Option<f64>,
    /// Every `[rebalancing.usdc.corridors.<chain>]` table's band, in chain
    /// order, whatever the USDC mode. `usdc_target` and `usdc_deviation`
    /// above stay the single band the dashboard shows.
    pub usdc_corridors: Vec<UsdcCorridorSettings>,
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
    pub broker: String,
    #[ts(type = "number")]
    pub order_polling_interval: u64,
    #[ts(type = "number")]
    pub inventory_poll_interval: u64,
}

/// One USDC corridor's trigger band: the chain vault's target share of (that
/// vault's USDC + Alpaca cash) and the deviation inside which nothing moves.
#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct UsdcCorridorSettings {
    pub chain: ChainName,
    pub target: f64,
    pub deviation: f64,
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
    pub counter_trading: CounterTrading,
    /// Whether the bot starts new rebalancing operations for this asset.
    /// A paused listing (`rebalancing = "paused"`) reads `false`, like a
    /// disabled one, although it still finishes and recovers work under way.
    pub rebalancing: bool,
    pub operational_limit: Option<String>,
}

/// Counter-trading (hedging) configuration for an asset.
///
/// Extended-hours counter-trading only makes sense when counter-trading is
/// enabled: the hedge path gates on counter-trading before it ever consults the
/// extended-hours session, so extended hours without counter-trading is a dead
/// configuration that can never execute. Nesting `extended_hours` under
/// `Enabled` makes that invalid combination unrepresentable on the wire, so the
/// dashboard can never receive a contradictory pair.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase", tag = "status")]
pub enum CounterTrading {
    Disabled,
    #[serde(rename_all = "camelCase")]
    Enabled {
        extended_hours: bool,
    },
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use st0x_finance::Symbol;

    use super::{AssetSettings, CounterTrading, UsdcCorridorSettings};
    use crate::ChainName;

    #[test]
    fn counter_trading_disabled_serializes_with_status_tag_only() {
        let value = serde_json::to_value(CounterTrading::Disabled).unwrap();
        assert_eq!(value, json!({ "status": "disabled" }));
    }

    #[test]
    fn counter_trading_enabled_serializes_extended_hours_flag() {
        let value = serde_json::to_value(CounterTrading::Enabled {
            extended_hours: true,
        })
        .unwrap();
        assert_eq!(value, json!({ "status": "enabled", "extendedHours": true }));
    }

    #[test]
    fn counter_trading_enabled_without_extended_hours_serializes_false_flag() {
        let value = serde_json::to_value(CounterTrading::Enabled {
            extended_hours: false,
        })
        .unwrap();
        assert_eq!(
            value,
            json!({ "status": "enabled", "extendedHours": false })
        );
    }

    #[test]
    fn asset_settings_nests_counter_trading_under_status() {
        let settings = AssetSettings {
            symbol: Symbol::new("AAPL").unwrap(),
            counter_trading: CounterTrading::Enabled {
                extended_hours: true,
            },
            rebalancing: false,
            operational_limit: None,
        };

        let value = serde_json::to_value(&settings).unwrap();
        assert_eq!(value["symbol"], json!("AAPL"));
        assert_eq!(
            value["counterTrading"],
            json!({ "status": "enabled", "extendedHours": true })
        );
        assert_eq!(value["rebalancing"], json!(false));
        assert_eq!(value["operationalLimit"], json!(null));
    }

    #[test]
    fn usdc_corridor_settings_serialize_chain_by_wire_name_and_band_as_numbers() {
        let corridors = vec![
            UsdcCorridorSettings {
                chain: ChainName::Base,
                target: 0.5,
                deviation: 0.3,
            },
            UsdcCorridorSettings {
                chain: ChainName::HyperEvm,
                target: 0.25,
                deviation: 0.125,
            },
        ];

        let value = serde_json::to_value(&corridors).unwrap();

        assert_eq!(
            value,
            json!([
                { "chain": "base", "target": 0.5, "deviation": 0.3 },
                { "chain": "hyperevm", "target": 0.25, "deviation": 0.125 },
            ])
        );
    }
}
