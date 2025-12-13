use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use ts_rs::TS;

#[derive(Debug, Clone, Copy, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "snake_case")]
pub(crate) enum UsdcDirection {
    AlpacaToBase,
    BaseToAlpaca,
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(tag = "type", rename_all = "lowercase")]
pub(crate) enum RebalanceOperationType {
    Mint {
        id: String,
        symbol: String,
        #[ts(type = "string")]
        amount: Decimal,
    },
    Redeem {
        id: String,
        symbol: String,
        #[ts(type = "string")]
        amount: Decimal,
    },
    Usdc {
        id: String,
        direction: UsdcDirection,
        #[ts(type = "string")]
        amount: Decimal,
    },
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum RebalanceStatus {
    InProgress {
        started_at: DateTime<Utc>,
    },
    Completed {
        started_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
    },
    Failed {
        started_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(crate) struct RebalanceOperation {
    #[serde(flatten)]
    pub(super) operation_type: RebalanceOperationType,
    #[serde(flatten)]
    pub(super) status: RebalanceStatus,
}
