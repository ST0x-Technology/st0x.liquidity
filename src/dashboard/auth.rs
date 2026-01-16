use serde::Serialize;
use ts_rs::TS;

/// Authentication status for broker connections.
///
/// Note: Additional variants (Valid, ExpiringSoon, Expired) will be added
/// when auth status monitoring is implemented.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum AuthStatus {
    NotConfigured,
}
