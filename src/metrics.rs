//! Prometheus metrics export for the st0x-hedge service.

use std::sync::OnceLock;

use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

pub(crate) fn setup() -> Result<PrometheusHandle, BuildError> {
    HANDLE.get_or_try_init(|| {
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

        Ok(handle)
    })
    .cloned()
}

pub(crate) async fn endpoint(
    axum::extract::State(state): axum::extract::State<crate::AppState>,
) -> String {
    state.metrics_handle.render()
}
