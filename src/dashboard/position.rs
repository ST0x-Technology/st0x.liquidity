use rust_decimal::Decimal;
use serde::Serialize;
use ts_rs::TS;

/// Position information for a symbol.
///
/// Note: Fields will be expanded when position tracking is implemented.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(crate) struct Position {
    pub(super) symbol: String,
    #[ts(type = "string")]
    pub(super) net: Decimal,
}
