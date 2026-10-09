//! Prometheus metrics export for the st0x-hedge service.
//!
//! Prometheus scrapes this endpoint as job `t0-liquidity` in production and
//! `t0-liquidity-staging` in staging. The body is the `metrics` recorder's
//! output followed by the `liq_*` contract from [`liquidity`]. No `liq_` name
//! may go through the `metrics` macros: the recorder never forgets a label
//! set, and the contract needs whole families replaced at once.

use std::sync::{Mutex, OnceLock};

use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};

pub(crate) mod liquidity;

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
    metrics::describe_gauge!(
        "hedge_floor_shares",
        "Shares every sell hedge and equity mint leaves in the broker account, by symbol; \
         refreshed by every position scan"
    );
    metrics::describe_gauge!(
        "hedge_deficit_shares",
        "Shares the last position scan wanted to hedge but could not place, by symbol; the \
         floor's residual plus any other preflight shortfall"
    );
    metrics::describe_gauge!(
        "registry_applied_generation",
        "Bucket generation of the running registry tables"
    );
    metrics::describe_gauge!(
        "registry_invalid",
        "Latest bucket copy is refused or unusable"
    );
    metrics::describe_counter!(
        "registry_reloads_total",
        "Registry reload outcomes, by result"
    );
    metrics::describe_gauge!(
        "registry_last_reload_timestamp_seconds",
        "Time of the last registry reload outcome, by result"
    );
    metrics::describe_gauge!(
        "registry_carried_forward_symbols",
        "Removed listings retained with admission disabled"
    );
    metrics::describe_gauge!(
        "registry_reload_held_seconds",
        "Age of the deploy registry hold"
    );
    metrics::describe_gauge!(
        "conductor_completion_only_symbols",
        "Listings with services only to complete durable work"
    );
    metrics::describe_counter!(
        "registry_reload_request_errors_total",
        "Accepted copies whose coordinated restart channel closed"
    );
    metrics::describe_counter!(
        "registry_fetch_errors_total",
        "Registry bucket reads that failed"
    );
    metrics::describe_counter!(
        "equity_plan_declined_total",
        "Equity allocation plans that chose no operation, by reason (within_band, \
         floor_capped, below_minimum, no_gas, cooling_down, price_missing, inflight, \
         total_zero, offchain_unpolled, no_polled_chain, chain_unpolled, chain_stale, \
         not_in_registry, redemption_unrecoverable, blocked_by_hedge)"
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
        "Extended-hours buys the position scan dropped because reference-price resolution or \
         crossing failed before enqueueing a hedge job, by symbol and cause"
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
    render_body(&state.metrics_handle, &liquidity::LIQ_FAMILIES)
}

fn render_body(handle: &PrometheusHandle, families: &liquidity::LiqFamilies) -> String {
    let mut body = handle.render();
    families.render_into(&mut body);
    body
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::SystemTime;

    use super::*;
    use crate::metrics::liquidity::settings::health_samples;
    use crate::metrics::liquidity::{LIQ_FAMILIES, LiqFamily};

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

    #[test]
    fn endpoint_body_appends_one_liq_block_per_name_after_the_recorder_output() {
        let handle = setup().expect("setup installs the recorder");
        metrics::counter!("hedge_trades_total", "symbol" => "AAPL", "direction" => "buy")
            .increment(1);
        LIQ_FAMILIES.replace(
            LiqFamily::Health,
            health_samples("0123456789abcdef", SystemTime::UNIX_EPOCH),
            SystemTime::UNIX_EPOCH,
        );

        let body = render_body(&handle, &LIQ_FAMILIES);

        let recorder_end = body
            .find("# HELP liq_")
            .expect("the liq_ blocks are rendered");
        assert!(
            body[..recorder_end].contains("hedge_trades_total"),
            "the recorder output comes first, got:\n{body}"
        );
        assert!(
            !body[..recorder_end].contains("liq_"),
            "the recorder rendered a liq_ name, got:\n{body}"
        );
        for name in [
            "liq_bot_info",
            "liq_bot_start_timestamp_seconds",
            "liq_collector_last_success_ts_seconds",
        ] {
            assert_eq!(
                body.matches(&format!("# HELP {name} ")).count(),
                1,
                "{name} must have exactly one block, got:\n{body}"
            );
            assert_eq!(
                body.matches(&format!("# TYPE {name} gauge\n")).count(),
                1,
                "{name} must be typed as a gauge once, got:\n{body}"
            );
        }
        assert!(
            body.lines()
                .filter(|line| line.starts_with("# TYPE liq_"))
                .all(|line| line.ends_with(" gauge")),
            "every liq_ name is a gauge, got:\n{body}"
        );
        assert!(body.contains("liq_bot_info{git_commit=\"0123456789ab\"} 1\n"));
    }

    /// `liq_` names belong to the family store; a `metrics` macro with one
    /// would put a second, never-forgetting writer on the contract.
    #[test]
    fn no_metrics_macro_uses_a_liq_name() {
        let macros = [
            "counter!(",
            "gauge!(",
            "histogram!(",
            "describe_counter!(",
            "describe_gauge!(",
            "describe_histogram!(",
        ];
        let mut violations = Vec::new();
        let mut pending = vec![Path::new(env!("CARGO_MANIFEST_DIR")).join("src")];

        while let Some(path) = pending.pop() {
            if path.is_dir() {
                pending.extend(
                    std::fs::read_dir(&path)
                        .unwrap()
                        .map(|entry| entry.unwrap().path()),
                );
                continue;
            }
            if path.extension().is_none_or(|extension| extension != "rs") {
                continue;
            }

            let source = std::fs::read_to_string(&path).unwrap();
            for (offset, _) in source.match_indices("\"liq_") {
                let before = source[..offset].trim_end();
                if macros.iter().any(|call| before.ends_with(call)) {
                    violations.push(format!("{}:{offset}", path.display()));
                }
            }
        }

        assert_eq!(violations, Vec::<String>::new());
    }
}
