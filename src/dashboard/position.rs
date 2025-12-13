use rust_decimal::Decimal;
use serde::Serialize;
use ts_rs::TS;

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(tag = "status", rename_all = "lowercase")]
pub(crate) enum Position {
    Empty {
        symbol: String,
    },
    Active {
        symbol: String,
        #[ts(type = "string")]
        net: Decimal,
        #[serde(skip_serializing_if = "Option::is_none")]
        pending_execution_id: Option<i64>,
    },
}
