//! Prometheus metrics export for the st0x-hedge service.
//!
//! Prometheus scrapes this endpoint as job `t0-liquidity` in production and
//! `t0-liquidity-staging` in staging. The body is the `metrics` recorder's
//! output followed by the `liq_*` contract from [`liquidity`]. No `liq_` name
//! may go through the `metrics` macros: the recorder never forgets a label
//! set, and the contract needs whole families replaced at once.

use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use metrics_exporter_prometheus::{BuildError, Matcher, PrometheusBuilder, PrometheusHandle};
use task_supervisor::{SupervisedTask, TaskResult};
use tokio::time::MissedTickBehavior;
use tracing::info;

pub(crate) mod liquidity;

/// Bucket bounds, in seconds, for the duration histograms that render as
/// Prometheus histograms. Without explicit buckets the recorder renders a
/// histogram as a summary, which `histogram_quantile` cannot aggregate across
/// label sets or time.
const DURATION_BUCKETS: [f64; 13] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

/// Duration histograms rendered with [`DURATION_BUCKETS`].
const BUCKETED_HISTOGRAMS: [&str; 3] = [
    "dependency_call_duration_seconds",
    "order_fill_poll_duration_seconds",
    "metrics_refresh_duration_seconds",
];

fn builder() -> Result<PrometheusBuilder, BuildError> {
    // Kept as a function so tests build a local recorder with the same
    // buckets the process-global one has.
    BUCKETED_HISTOGRAMS
        .into_iter()
        .try_fold(PrometheusBuilder::new(), |builder, name| {
            builder.set_buckets_for_metric(Matcher::Full(name.to_string()), &DURATION_BUCKETS)
        })
}

/// How often [`RecorderUpkeep`] drains the recorder's histogram buffers.
/// Each histogram sample stays buffered until a render or an upkeep, so
/// without this the buffers grow with every call while nothing scrapes.
const RECORDER_UPKEEP_PERIOD: Duration = Duration::from_secs(5);

/// What [`RecorderUpkeep`] runs each period, so a test can count the calls
/// that a render would otherwise make invisible.
pub(crate) trait Upkeep {
    fn run_upkeep(&self);
}

impl Upkeep for PrometheusHandle {
    fn run_upkeep(&self) {
        Self::run_upkeep(self);
    }
}

/// Runs the recorder upkeep that `install_recorder` does not start: the
/// HTTP-listener install path would, but this process serves `/metrics`
/// itself.
#[derive(Clone)]
pub(crate) struct RecorderUpkeep<U = PrometheusHandle> {
    pub(crate) handle: U,
}

impl<U: Upkeep + Clone + Send + Sync + 'static> SupervisedTask for RecorderUpkeep<U> {
    async fn run(&mut self) -> TaskResult {
        info!(period = ?RECORDER_UPKEEP_PERIOD, "Metrics recorder upkeep started");

        let mut interval = tokio::time::interval(RECORDER_UPKEEP_PERIOD);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            self.handle.run_upkeep();
        }
    }
}

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
    let handle = builder()?.install_recorder()?;

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
        "dependency_calls_total",
        "External dependency calls, by dependency, operation and outcome (ok/error); counted \
         before the telemetry channel, so a full channel does not hide calls"
    );
    metrics::describe_histogram!(
        "dependency_call_duration_seconds",
        metrics::Unit::Seconds,
        "External dependency call duration, by dependency and operation"
    );
    metrics::describe_counter!(
        "telemetry_samples_dropped_total",
        "Dependency call samples dropped because the telemetry channel was full or closed"
    );
    metrics::describe_counter!(
        "order_fill_poll_cycles_total",
        "Order-fill poll cycles, by chain and outcome (ok/paused/error); paused is a cycle that \
         succeeds but ingests nothing because the cutoff block is unknown or behind the checkpoint"
    );
    metrics::describe_counter!(
        "order_fill_poll_skipped_ticks_total",
        "Order-fill poll ticks dropped because the previous cycle overran, by chain"
    );
    metrics::describe_histogram!(
        "order_fill_poll_duration_seconds",
        metrics::Unit::Seconds,
        "Order-fill poll cycle duration, by chain"
    );
    metrics::describe_gauge!(
        "order_fill_block_lag_blocks",
        "Ingestion cutoff block minus the last processed block at the latest poll that knew \
         both, by chain"
    );
    metrics::describe_gauge!(
        "order_fill_block_lag_sampled_timestamp_seconds",
        "Time of the poll that last set order_fill_block_lag_blocks, by chain; it stops \
         advancing while the lag is unknown"
    );
    metrics::describe_counter!(
        "log_events_total",
        "Error and warning events the file log wrote, by level and target; counted only with \
         file logging, from the recorder's install onwards"
    );
    metrics::describe_histogram!(
        "metrics_refresh_duration_seconds",
        metrics::Unit::Seconds,
        "Time one liq_ collector took to load and build its family, by collector"
    );
    metrics::describe_counter!(
        "bot_gas_redrive_total",
        "Bot-gas receipt-cost enqueue failures redriven instead of failing the triggering \
         job, by job"
    );

    let _ = HANDLE.set(handle.clone());
    Ok(handle)
}

/// A recorder configured like the global one, for tests that install it
/// locally with `metrics::with_local_recorder`.
#[cfg(test)]
pub(crate) fn local_recorder() -> metrics_exporter_prometheus::PrometheusRecorder {
    builder().unwrap().build_recorder()
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::SystemTime;

    use super::*;
    use crate::metrics::liquidity::settings::health_samples;
    use crate::metrics::liquidity::{LIQ_FAMILIES, LiqFamily};

    // These tests install the process-global Prometheus recorder. nextest runs
    // each test in its own process, so the install-once recorder is fresh per
    // test and they do not contend over global state.

    #[derive(Clone, Default)]
    struct CountingUpkeep(Arc<AtomicUsize>);

    impl Upkeep for CountingUpkeep {
        fn run_upkeep(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn recorder_upkeep_runs_once_per_period_and_never_returns() {
        let upkeep = CountingUpkeep::default();
        let calls = Arc::clone(&upkeep.0);
        let mut task = RecorderUpkeep { handle: upkeep };
        let running = tokio::spawn(async move { task.run().await });

        // Ticks fire at 0, 5 and 10 seconds; stop halfway to the next one.
        tokio::time::sleep(RECORDER_UPKEEP_PERIOD * 2 + RECORDER_UPKEEP_PERIOD / 2).await;

        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert!(!running.is_finished(), "the upkeep loop never returns");
        running.abort();
    }

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
