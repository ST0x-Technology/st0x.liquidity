use serde::Serialize;
use ts_rs::TS;

/// Placeholder for rebalance operation tracking.
///
/// Note: Full type structure will be added when rebalance panel is implemented.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(crate) struct RebalanceOperation {
    pub(super) id: String,
}
