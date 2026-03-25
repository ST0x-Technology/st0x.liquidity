//! Inventory DTOs for per-symbol and USDC balance snapshots.
//!
//! Represents the current distribution of assets across onchain and
//! offchain venues, split by availability (available vs in-flight).

use chrono::{DateTime, Utc};
use serde::Serialize;
use ts_rs::TS;

use st0x_finance::{FractionalShares, HasZero, Symbol, Usdc};

/// Per-symbol equity balances split by venue and availability.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SymbolInventory {
    #[ts(type = "string")]
    pub symbol: Symbol,
    #[ts(type = "string")]
    pub onchain_available: FractionalShares,
    #[ts(type = "string")]
    pub onchain_inflight: FractionalShares,
    #[ts(type = "string")]
    pub offchain_available: FractionalShares,
    #[ts(type = "string")]
    pub offchain_inflight: FractionalShares,
}

/// Onchain and offchain USDC balances split by availability.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct UsdcInventory {
    #[ts(type = "string")]
    pub onchain_available: Usdc,
    #[ts(type = "string")]
    pub onchain_inflight: Usdc,
    #[ts(type = "string")]
    pub offchain_available: Usdc,
    #[ts(type = "string")]
    pub offchain_inflight: Usdc,
}

/// Full inventory snapshot across all symbols and USDC.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Inventory {
    pub per_symbol: Vec<SymbolInventory>,
    pub usdc: UsdcInventory,
}

impl Inventory {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            per_symbol: Vec::new(),
            usdc: UsdcInventory {
                onchain_available: Usdc::ZERO,
                onchain_inflight: Usdc::ZERO,
                offchain_available: Usdc::ZERO,
                offchain_inflight: Usdc::ZERO,
            },
        }
    }
}

/// Point-in-time snapshot of the current inventory state broadcast to clients.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct InventorySnapshot {
    pub inventory: Inventory,
    pub fetched_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use st0x_float_macro::float;

    use super::*;

    #[test]
    fn inventory_with_inflight_serializes_correctly() {
        let inventory = Inventory {
            per_symbol: vec![SymbolInventory {
                symbol: Symbol::new("TSLA").unwrap(),
                onchain_available: FractionalShares::new(float!(50)),
                onchain_inflight: FractionalShares::new(float!(5)),
                offchain_available: FractionalShares::new(float!(45)),
                offchain_inflight: FractionalShares::ZERO,
            }],
            usdc: UsdcInventory {
                onchain_available: Usdc::new(float!(10000)),
                onchain_inflight: Usdc::ZERO,
                offchain_available: Usdc::new(float!(5000)),
                offchain_inflight: Usdc::new(float!(500)),
            },
        };

        let json = serde_json::to_value(&inventory).expect("serialization should succeed");

        let symbol = &json["perSymbol"][0];
        assert_eq!(symbol["symbol"], json!("TSLA"));
        assert_eq!(symbol["onchainAvailable"], json!("50"));
        assert_eq!(symbol["onchainInflight"], json!("5"));
        assert_eq!(symbol["offchainAvailable"], json!("45"));
        assert_eq!(symbol["offchainInflight"], json!("0"));

        let usdc = &json["usdc"];
        assert_eq!(usdc["onchainAvailable"], json!("10000"));
        assert_eq!(usdc["onchainInflight"], json!("0"));
        assert_eq!(usdc["offchainAvailable"], json!("5000"));
        assert_eq!(usdc["offchainInflight"], json!("500"));
    }
}
