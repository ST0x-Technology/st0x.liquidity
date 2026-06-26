//! Prometheus metrics export for the st0x-hedge service.
//!
//! Call [`setup`] once at startup before any `counter!`/`gauge!`/`histogram!`
//! invocations. The returned [`PrometheusHandle`] is managed by Rocket so the
//! `/metrics` endpoint can render the current state on demand.

use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};

pub(crate) fn setup() -> Result<PrometheusHandle, BuildError> {
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
        "ClearV2 and TakeOrderV2 events received from Raindex, by event_type"
    );
    metrics::describe_counter!(
        "broker_errors_total",
        "Broker API errors, by symbol and kind"
    );

    Ok(handle)
}

#[rocket::get("/metrics")]
pub(crate) fn endpoint(handle: &rocket::State<PrometheusHandle>) -> String {
    handle.render()
}

pub(crate) fn routes() -> Vec<rocket::Route> {
    rocket::routes![endpoint]
}
