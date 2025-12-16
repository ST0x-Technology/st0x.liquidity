use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use ts_rs::TS;

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(crate) struct SpreadSummary {
    pub(super) symbol: String,
    #[ts(type = "string")]
    pub(super) last_buy_price: Decimal,
    #[ts(type = "string")]
    pub(super) last_sell_price: Decimal,
    #[ts(type = "string")]
    pub(super) pyth_price: Decimal,
    #[ts(type = "string")]
    pub(super) spread_bps: Decimal,
    pub(super) updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(crate) struct SpreadUpdate {
    pub(super) symbol: String,
    pub(super) timestamp: DateTime<Utc>,
    #[ts(type = "string | null")]
    pub(super) buy_price: Option<Decimal>,
    #[ts(type = "string | null")]
    pub(super) sell_price: Option<Decimal>,
    #[ts(type = "string")]
    pub(super) pyth_price: Decimal,
}
