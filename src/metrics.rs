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
        "ClearV3, TakeOrderV3 and InventoryTrade events received from Raindex, by event_type"
    );
    metrics::describe_counter!(
        "broker_errors_total",
        "Broker API errors, by symbol and kind"
    );
    metrics::describe_counter!(
        "inventory_ambiguous_settlement_total",
        "Inventory settlements quarantined because a tx emitted multiple \
         OperatorDeposits or multiple OperatorWithdraws and could not be safely paired"
    );
    metrics::describe_counter!(
        "inventory_unpaired_settlement_total",
        "Inventory OperatorDeposit/OperatorWithdraw legs with no same-tx counterpart in the \
         batch, by leg"
    );
    metrics::describe_counter!(
        "portfolio_snapshot_unusable_mark_total",
        "Nonzero equity balances captured with a missing or stale USD mark, by symbol and reason"
    );

    let _ = HANDLE.set(handle.clone());
    Ok(handle)
}

pub(crate) async fn endpoint(
    axum::extract::State(state): axum::extract::State<crate::AppState>,
) -> String {
    state.metrics_handle.render()
}

#[cfg(test)]
mod tests {
    use super::*;

    // These tests install the process-global Prometheus recorder. nextest runs
    // each test in its own process, so the install-once recorder is fresh per
    // test and they do not contend over global state.

    #[test]
    fn setup_is_idempotent_across_calls() {
        // The double-checked lock must let a second caller reuse the cached
        // handle rather than failing to reinstall the global recorder.
        let first = setup().expect("first setup installs the recorder");
        let second = setup().expect("second setup returns the cached handle");

        // Both handles point at the same shared registry, so they render
        // identical output -- proving the second call reused the recorder
        // rather than failing to reinstall it.
        assert_eq!(first.render(), second.render());
    }

    #[test]
    fn rendered_output_surfaces_an_incremented_counter() {
        let handle = setup().expect("setup installs the recorder");

        metrics::counter!("hedge_trades_total", "symbol" => "AAPL", "direction" => "buy")
            .increment(1);

        let rendered = handle.render();
        assert!(
            rendered.contains("hedge_trades_total"),
            "an incremented counter must appear in the rendered /metrics output, got:\n{rendered}"
        );
    }
}
