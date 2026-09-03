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
        "Hedge orders placed, by symbol, direction, and session \
         (regular/extended/overnight)"
    );
    metrics::describe_histogram!(
        "hedge_fill_latency_seconds",
        metrics::Unit::Seconds,
        "Wall-clock time from order placement to confirmed fill, by symbol and session"
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
        "Broker API errors, by symbol, kind, and the session the order targeted"
    );
    metrics::describe_counter!(
        "close_flatten_attempts_total",
        "Close-flatten hedge pricing attempts, by symbol, direction, and post-close gap reason"
    );
    metrics::describe_counter!(
        "close_flatten_blocked_total",
        "Close-flatten attempts blocked before submission, by symbol and stable reason"
    );
    metrics::describe_counter!(
        "close_flatten_placements_total",
        "Close-flatten limit orders priced and cleared for submission, by symbol, direction, \
         and the cross applied bucketed to whole percent. Counts attempts, not orders: a \
         retried or re-driven placement of the same hedge counts again, and an attempt the \
         broker later rejects still counts"
    );
    metrics::describe_counter!(
        "close_flatten_outcomes_total",
        "Terminal broker-state dispatches for close-flatten placements, by symbol, direction, \
         and outcome (filled/cancelled/failed). A placement can be observed again until its \
         follow-up job commits the terminal aggregate transition"
    );
    metrics::describe_counter!(
        "hedge_price_source_total",
        "Which reference an extended-hours limit was priced from, by symbol, path \
         (ordinary_extended/close_flatten), and source \
         (primary_quote/mark/delayed_sip_quote). Shows which fallback legs are load-bearing"
    );
    metrics::describe_counter!(
        "hedge_scan_skipped_total",
        "Extended-hours and overnight buys the position scan dropped because a session gate, \
         reference-price resolution, or crossing failed before enqueueing a hedge job, by \
         symbol, session, and cause"
    );
    metrics::describe_counter!(
        "hedge_cancellations_requested_total",
        "Cancel requests issued by the maintenance sweeps against live limit orders, by \
         symbol, session, and reason (reprice timeouts, session-boundary replacements, \
         close flatten, unrequested)"
    );
    metrics::describe_histogram!(
        "hedge_quote_age_seconds",
        metrics::Unit::Seconds,
        "Age of the indicative overnight quote at reference-price resolution, by symbol and \
         source. Stale quotes that defer the hedge are included, so the distribution shows \
         the feed lag placements actually see; only failed fetches record nothing"
    );
    metrics::describe_histogram!(
        "hedge_reference_to_limit_bps",
        "Signed basis-point distance from the reference price to the submitted limit, by \
         symbol and session; positive is adverse (a buy limit above the reference, a sell \
         limit below it). Recorded per successful placement of a limit-kind hedge"
    );
    metrics::describe_histogram!(
        "hedge_reference_to_fill_slippage_bps",
        "Signed basis-point distance from the placement-time reference price to the fill \
         price, by symbol and session; positive is adverse. Orders without a persisted \
         reference (market orders, legacy orders) record nothing"
    );
    metrics::describe_gauge!(
        "asset_sync_last_success_timestamp",
        "Unix timestamp of the last asset-eligibility sync run that refreshed every \
         configured symbol; a partial or failed run leaves it unchanged"
    );
    metrics::describe_counter!(
        "asset_sync_failures_total",
        "Asset-eligibility sync failures, by symbol: one increment per failed symbol per \
         sync run"
    );
    metrics::describe_counter!(
        "hedge_dead_lettered_total",
        "Hedge attempts this process gave up on, by symbol and reason: a permanent or \
         retry-budget-exhausted transient symbol-scoped pricing failure, or broker \
         rate-limiting that outlived the reschedule budget"
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
    metrics::describe_counter!(
        "bot_gas_redrive_total",
        "Bot-gas receipt-cost enqueue failures redriven instead of failing the triggering \
         job, by job"
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

        metrics::counter!(
            "hedge_trades_total",
            "symbol" => "AAPL",
            "direction" => "buy",
            "session" => "regular"
        )
        .increment(1);

        let rendered = handle.render();
        assert!(
            rendered.contains("hedge_trades_total"),
            "an incremented counter must appear in the rendered /metrics output, got:\n{rendered}"
        );
    }
}
