use serde::Serialize;
use ts_rs::TS;

/// Placeholder for trade information.
///
/// Note: Full type structure will be added when trades panel is implemented.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(crate) struct Trade {
    pub(super) id: String,
}
