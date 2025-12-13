use rust_decimal::Decimal;
use serde::Serialize;
use ts_rs::TS;

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
pub(crate) struct SymbolInventory {
    pub(super) symbol: String,
    #[ts(type = "string")]
    pub(super) onchain: Decimal,
    #[ts(type = "string")]
    pub(super) offchain: Decimal,
    #[ts(type = "string")]
    pub(super) net: Decimal,
}

#[derive(Debug, Clone, Copy, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
pub(crate) struct UsdcInventory {
    #[ts(type = "string")]
    pub(super) onchain: Decimal,
    #[ts(type = "string")]
    pub(super) offchain: Decimal,
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(crate) struct Inventory {
    pub(super) per_symbol: Vec<SymbolInventory>,
    pub(super) usdc: UsdcInventory,
}

impl Inventory {
    pub(crate) fn empty() -> Self {
        Self {
            per_symbol: Vec::new(),
            usdc: UsdcInventory {
                onchain: Decimal::ZERO,
                offchain: Decimal::ZERO,
            },
        }
    }
}
