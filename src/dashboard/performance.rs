use rust_decimal::Decimal;
use serde::Serialize;
use ts_rs::TS;

#[derive(Debug, Clone, Copy, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
pub(crate) struct PnL {
    #[ts(type = "string")]
    pub(super) absolute: Decimal,
    #[ts(type = "string")]
    pub(super) percent: Decimal,
}

#[derive(Debug, Clone, Copy, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(rename_all = "camelCase")]
pub(crate) struct TimeframeMetrics {
    #[ts(type = "string")]
    pub(super) aum: Decimal,
    pub(super) pnl: PnL,
    #[ts(type = "string")]
    pub(super) volume: Decimal,
    #[ts(type = "number")]
    pub(super) trade_count: u64,
    #[ts(type = "string | null")]
    pub(super) sharpe_ratio: Option<Decimal>,
    #[ts(type = "string | null")]
    pub(super) sortino_ratio: Option<Decimal>,
    #[ts(type = "string")]
    pub(super) max_drawdown: Decimal,
    #[ts(type = "number | null")]
    pub(super) hedge_lag_ms: Option<u64>,
    #[ts(type = "string")]
    pub(super) uptime_percent: Decimal,
}

impl TimeframeMetrics {
    pub(crate) fn zero() -> Self {
        Self {
            aum: Decimal::ZERO,
            pnl: PnL {
                absolute: Decimal::ZERO,
                percent: Decimal::ZERO,
            },
            volume: Decimal::ZERO,
            trade_count: 0,
            sharpe_ratio: None,
            sortino_ratio: None,
            max_drawdown: Decimal::ZERO,
            hedge_lag_ms: None,
            uptime_percent: Decimal::ONE_HUNDRED,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
pub(crate) struct PerformanceMetrics {
    #[serde(rename = "1h")]
    pub(super) one_hour: TimeframeMetrics,
    #[serde(rename = "1d")]
    pub(super) one_day: TimeframeMetrics,
    #[serde(rename = "1w")]
    pub(super) one_week: TimeframeMetrics,
    #[serde(rename = "1m")]
    pub(super) one_month: TimeframeMetrics,
    pub(super) all: TimeframeMetrics,
}

impl PerformanceMetrics {
    pub(crate) fn zero() -> Self {
        let zero = TimeframeMetrics::zero();
        Self {
            one_hour: zero,
            one_day: zero,
            one_week: zero,
            one_month: zero,
            all: zero,
        }
    }
}
