use chrono::{DateTime, Utc};
use serde::Serialize;
use ts_rs::TS;

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
pub(crate) struct EventStoreEntry {
    pub(super) aggregate_type: String,
    pub(super) aggregate_id: String,
    #[ts(type = "number")]
    pub(super) sequence: u64,
    pub(super) event_type: String,
    pub(super) timestamp: DateTime<Utc>,
}
