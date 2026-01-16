use serde::Serialize;
use ts_rs::TS;

/// Circuit breaker status for the trading system.
///
/// Note: Tripped variant will be added when circuit breaker logic is implemented.
#[derive(Debug, Clone, Serialize, TS)]
#[ts(export, export_to = "../dashboard/src/lib/api/")]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum CircuitBreakerStatus {
    Active,
}
