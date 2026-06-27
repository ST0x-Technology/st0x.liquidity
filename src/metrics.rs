//! Prometheus metrics export for the st0x-hedge service.

use std::sync::{Mutex, OnceLock};

use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
static INIT: Mutex<()> = Mutex::new(());

pub(crate) fn setup() -> Result<PrometheusHandle, BuildError> {
    if let Some(handle) = HANDLE.get() {
        return Ok(handle.clone());
    }
    let _guard = INIT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(handle) = HANDLE.get() {
        return Ok(handle.clone());
    }
    let handle = PrometheusBuilder::new().install_recorder()?;

    metrics::describe_counter!(
        "hedge_trades_total",
        "Hedge orders placed, by symbol and direction"
    );
    metrics::describe_histogram!(
        "hedge_fill_latency_seconds",
        metrics::Unit::Seconds,
        "Wall-clock time from order placement to confirmed fill, by symbol"
    );
    metrics::describe_gauge!(
        "position_shares",
        "Net open position in fractional shares, by symbol"
    );
    metrics::describe_counter!(
        "onchain_events_total",
        "ClearV3 and TakeOrderV3 events received from Raindex, by event_type"
    );
    metrics::describe_counter!(
        "broker_errors_total",
        "Broker API errors, by symbol and kind"
    );

    let _ = HANDLE.set(handle.clone());
    Ok(handle)
}

pub(crate) async fn endpoint(
    axum::extract::State(state): axum::extract::State<crate::AppState>,
) -> String {
    state.metrics_handle.render()
}
