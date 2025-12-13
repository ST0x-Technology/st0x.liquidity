use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use ts_rs::TS;

#[derive(Debug, Clone, Copy, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "lowercase")]
pub(crate) enum Direction {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "lowercase")]
pub(crate) enum OffchainTradeStatus {
    Pending,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(crate) struct OnchainTrade {
    pub(super) tx_hash: String,
    #[ts(type = "number")]
    pub(super) log_index: u64,
    pub(super) symbol: String,
    #[ts(type = "string")]
    pub(super) amount: Decimal,
    pub(super) direction: Direction,
    #[ts(type = "string")]
    pub(super) price_usdc: Decimal,
    pub(super) timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(crate) struct OffchainTrade {
    #[ts(type = "number")]
    pub(super) id: i64,
    pub(super) symbol: String,
    #[ts(type = "number")]
    pub(super) shares: u32,
    pub(super) direction: Direction,
    pub(super) order_id: Option<String>,
    #[ts(type = "string | null")]
    pub(super) price_cents: Option<Decimal>,
    pub(super) status: OffchainTradeStatus,
    pub(super) executed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(tag = "type", content = "data", rename_all = "lowercase")]
pub(crate) enum Trade {
    Onchain(OnchainTrade),
    Offchain(OffchainTrade),
}
