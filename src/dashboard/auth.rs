use chrono::{DateTime, Utc};
use serde::Serialize;
use ts_rs::TS;

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum AuthStatus {
    Valid { expires_at: DateTime<Utc> },
    ExpiringSoon { expires_at: DateTime<Utc> },
    Expired,
    NotConfigured,
}
