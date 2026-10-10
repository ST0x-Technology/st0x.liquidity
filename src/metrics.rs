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

/// Bucket bounds, in seconds, for `metrics_refresh_duration_seconds`:
/// [`DURATION_BUCKETS`] plus bounds above 60 seconds. A P&L window may use
/// its whole 120-second budget, and a slower run must not fall only into
/// `+Inf`, where `histogram_quantile` cannot place it.
const REFRESH_DURATION_BUCKETS: [f64; 17] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 90.0, 120.0, 180.0,
    300.0,
];

/// Each duration histogram that renders as a Prometheus histogram, with its
/// bucket bounds.
const BUCKETED_HISTOGRAMS: [(&str, &[f64]); 3] = [
    ("dependency_call_duration_seconds", &DURATION_BUCKETS),
    ("order_fill_poll_duration_seconds", &DURATION_BUCKETS),
    (
        "metrics_refresh_duration_seconds",
        &REFRESH_DURATION_BUCKETS,
    ),
];

fn builder() -> Result<PrometheusBuilder, BuildError> {
    // Kept as a function so tests build a local recorder with the same
    // buckets the process-global one has.
    BUCKETED_HISTOGRAMS.into_iter().try_fold(
        PrometheusBuilder::new(),
        |builder, (name, buckets)| {
            builder.set_buckets_for_metric(Matcher::Full(name.to_string()), buckets)
        },
    )
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
         file logging, for this process only"
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
    use std::collections::BTreeSet;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::SystemTime;

    use proc_macro2::{Delimiter, Ident, Literal, TokenStream, TokenTree};

    use super::*;
    use crate::metrics::liquidity::settings::health_samples;
    use crate::metrics::liquidity::tests::{parse_exposition, series};
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

    /// A refresh slower than 60 seconds lands in a finite bucket, so a P&L
    /// window that uses its 120-second budget shows in the quantiles.
    #[test]
    fn the_refresh_histogram_has_buckets_above_sixty_seconds() {
        let recorder = local_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            metrics::histogram!("metrics_refresh_duration_seconds", "collector" => "pnl_all")
                .record(Duration::from_secs(100));
        });

        let rendered = parse_exposition(&handle.render());
        let bucket = |le: &str| {
            rendered
                .get(&series(
                    "metrics_refresh_duration_seconds_bucket",
                    &[("collector", "pnl_all"), ("le", le)],
                ))
                .copied()
        };
        assert_eq!(bucket("60"), Some(0.0));
        assert_eq!(bucket("90"), Some(0.0));
        assert_eq!(bucket("120"), Some(1.0));
        assert_eq!(bucket("300"), Some(1.0));
    }

    /// `liq_` names belong to the family store; a `metrics` macro with one
    /// would put a second, never-forgetting writer on the contract.
    #[test]
    fn no_metrics_macro_uses_a_liq_name() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut violations = Vec::new();

        for path in rust_sources(root) {
            let relative = relative_path(root, &path);
            let source = std::fs::read_to_string(&path).unwrap();
            violations.extend(
                scan_liq_names(&source)
                    .macro_names
                    .into_iter()
                    .map(|name| format!("{relative}: \"{name}\"")),
            );
        }

        assert_eq!(violations, Vec::<String>::new());
    }

    #[test]
    fn liq_macro_scan_sees_names_however_the_call_is_spaced_and_skips_comments() {
        let source = concat!(
            "gauge!(\"liq_plain\").set(1.0);\n",
            "counter!( r\"liq_raw\").increment(1);\n",
            "histogram!(r#\"liq_raw_hashed\"#).record(1.0);\n",
            "gauge!(\"hedge_open_positions\").set(1.0);\n",
            "info!(\"liq_ state refresh started\");\n",
            "metrics::gauge! (\"liq_spaced_paren\").set(1.0);\n",
            "counter ! [\"liq_spaced_bang\"].increment(1);\n",
            "describe_histogram!{ \"liq_braced\" };\n",
            "gauge!(\n    \"liq_next_line\",\n    \"symbol\" => \"AAPL\"\n);\n",
            "gauge!(\"hedge_shares\", \"kind\" => \"liq_label_value\");\n",
            "// gauge!(\"liq_line_comment\");\n",
            "/* outer /* gauge!(\"liq_nested_comment\"); */ */\n",
        );

        assert_eq!(
            scan_liq_names(source).macro_names,
            vec![
                "liq_plain",
                "liq_raw",
                "liq_raw_hashed",
                "liq_spaced_paren",
                "liq_spaced_bang",
                "liq_braced",
                "liq_next_line",
            ]
        );
    }

    /// The catalog that the family store renders. It may hold any `liq_`
    /// name literal.
    const LIQ_CATALOG_FILE: &str = "src/metrics/liquidity.rs";

    /// The only `liq_` literals allowed outside the catalog, each with the
    /// file that holds it: the log targets of the dashboard event lines and
    /// the same targets in the log filter. A literal anywhere else could
    /// reach a `metrics` macro through a helper or a const (such as
    /// `set_shares_gauge` in `position_check`), which
    /// `no_metrics_macro_uses_a_liq_name` cannot see.
    const LIQ_LOG_TARGETS: [(&str, &str); 6] = [
        ("crates/config/src/telemetry.rs", "liq_event"),
        ("crates/config/src/telemetry.rs", "liq_trade"),
        ("crates/config/src/telemetry.rs", "liq_transfer"),
        ("src/dashboard/event_lines.rs", "liq_event"),
        ("src/dashboard/event_lines.rs", "liq_trade"),
        ("src/dashboard/event_lines.rs", "liq_transfer"),
    ];

    #[test]
    fn only_the_catalog_and_the_log_targets_hold_liq_names_in_production_code() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut violations = Vec::new();
        let mut catalog_names = 0_usize;
        let mut log_targets_found = BTreeSet::new();

        for path in rust_sources(root) {
            let relative = relative_path(root, &path);
            let source = std::fs::read_to_string(&path).unwrap();

            for name in scan_liq_names(&source).production_names {
                if relative == LIQ_CATALOG_FILE {
                    catalog_names += 1;
                } else if LIQ_LOG_TARGETS
                    .iter()
                    .any(|&(file, target)| file == relative && target == name)
                {
                    log_targets_found.insert((relative.clone(), name));
                } else {
                    violations.push(format!("{relative}: \"{name}\""));
                }
            }
        }

        assert_eq!(
            violations,
            Vec::<String>::new(),
            "liq_ names belong to the catalog in {LIQ_CATALOG_FILE}"
        );
        assert!(
            catalog_names > 0,
            "the catalog in {LIQ_CATALOG_FILE} must still hold liq_ names"
        );
        let allowed: BTreeSet<(String, String)> = LIQ_LOG_TARGETS
            .iter()
            .map(|&(file, target)| (file.to_string(), target.to_string()))
            .collect();
        assert_eq!(
            log_targets_found, allowed,
            "every allowed log target must still be found where it is allowed"
        );
    }

    #[test]
    fn liq_name_scan_sees_names_behind_consts_and_skips_log_text_and_tests() {
        let source = concat!(
            "const NAME: &str = \"liq_hidden\";\n",
            "fn publish() { set_gauge(NAME); }\n",
            "fn log() { info!(\"liq_ state refresh started\"); }\n",
            "fn prefix(name: &str) -> bool { name.starts_with(\"liq_\") }\n",
            "const RAW: &str = r\"liq_raw\";\n",
            "const RAW_HASHED: &str = r#\"liq_raw_hashed\"#;\n",
            "fn built(kind: &str) -> String { format!(\"liq_{kind}_shares\") }\n",
            "const UPPER: &str = \"liq_Unexpected\";\n",
            "const UNDERSCORE: &str = \"liq__double\";\n",
            "/// Doc text with liq_doc_comment.\n",
            "// const COMMENTED: &str = \"liq_commented\";\n",
            "#[cfg(test)]\n",
            "mod tests { const IN_TEST: &str = \"liq_test_only\"; }\n",
        );

        assert_eq!(
            scan_liq_names(source).production_names,
            vec![
                "liq_hidden",
                "liq_raw",
                "liq_raw_hashed",
                "liq_{kind}_shares",
                "liq_Unexpected",
                "liq__double",
            ]
        );
    }

    #[test]
    fn liq_name_scan_skips_each_test_item_and_reads_the_code_after_it() {
        let source = concat!(
            "/* #[cfg(test)] mod example { */\n",
            "const AFTER_COMMENT: &str = \"liq_after_comment\";\n",
            "const TEXT: &str = \"#[cfg(test)] mod example {\";\n",
            "const AFTER_STRING: &str = \"liq_after_string\";\n",
            "#[cfg(test)]\n",
            "mod integration_tests;\n",
            "const AFTER_DECLARATION: &str = \"liq_after_declaration\";\n",
            "#[cfg(test)]\n",
            "pub(crate) mod tests {\n",
            "    const BRACE: char = '}';\n",
            "    const RAW: &str = r#\"}\"#;\n",
            "    // }\n",
            "    /* outer /* inner */ { */\n",
            "    const IN_TEST: &str = \"liq_test_only\";\n",
            "    fn record() { gauge!(\"liq_in_test\").set(1.0); }\n",
            "}\n",
            "#[cfg(test)]\n",
            "const TEST_CONST: Named = Named { name: \"liq_test_const\" };\n",
            "#[cfg(test)]\n",
            "#[allow(dead_code)]\n",
            "pub(crate) fn test_helper() -> &'static str { \"liq_test_fn\" }\n",
            "fn publish() {\n",
            "    #[cfg(test)]\n",
            "    let name = Named { name: \"liq_test_statement\" };\n",
            "    set_gauge(\"liq_in_function\");\n",
            "}\n",
            "const AFTER_TESTS: &str = \"liq_after_tests\";\n",
        );

        let scan = scan_liq_names(source);

        assert_eq!(
            scan.production_names,
            vec![
                "liq_after_comment",
                "liq_after_string",
                "liq_after_declaration",
                "liq_in_function",
                "liq_after_tests",
            ]
        );
        assert_eq!(scan.macro_names, vec!["liq_in_test"]);
    }

    #[test]
    fn liq_name_scan_skips_a_file_marked_as_test_only() {
        let source = concat!(
            "#![cfg(test)]\n",
            "const IN_TEST: &str = \"liq_test_only\";\n",
        );

        assert_eq!(
            scan_liq_names(source).production_names,
            Vec::<String>::new()
        );
    }

    /// Every `.rs` file under `src/` and `crates/`, sorted.
    fn rust_sources(root: &Path) -> Vec<PathBuf> {
        let mut sources = Vec::new();
        let mut pending = vec![root.join("src"), root.join("crates")];

        while let Some(path) = pending.pop() {
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name == "target") {
                    continue;
                }
                pending.extend(
                    std::fs::read_dir(&path)
                        .unwrap()
                        .map(|entry| entry.unwrap().path()),
                );
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                sources.push(path);
            }
        }

        sources.sort();
        sources
    }

    /// `path` relative to `root`, with `/` separators.
    fn relative_path(root: &Path, path: &Path) -> String {
        path.strip_prefix(root)
            .unwrap()
            .components()
            .map(|component| component.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("/")
    }

    /// What one token walk over a Rust source file finds.
    #[derive(Debug, Default)]
    struct LiqScan {
        /// `liq_` name literals outside `#[cfg(test)]` items, in order.
        production_names: Vec<String>,
        /// `liq_` literals passed as the first argument of a `metrics`
        /// macro, test code included, in order.
        macro_names: Vec<String>,
    }

    /// Lexes `source` with `proc-macro2`, so comments (nested ones too) are
    /// dropped, plain and raw strings are literals, and whitespace between
    /// a macro name, its `!` and its arguments does not matter.
    fn scan_liq_names(source: &str) -> LiqScan {
        let tokens: Vec<TokenTree> = source.parse::<TokenStream>().unwrap().into_iter().collect();
        let mut scan = LiqScan::default();
        walk_tokens(&tokens, false, &mut scan);
        scan
    }

    /// Records the `liq_` literals of `tokens`, groups included, into
    /// `scan`. `in_test` marks tokens inside a `#[cfg(test)]` item.
    fn walk_tokens(tokens: &[TokenTree], in_test: bool, scan: &mut LiqScan) {
        let mut index = 0;

        while let Some(token) = tokens.get(index) {
            let is_hash = is_punct(Some(token), '#');

            if is_hash && is_punct(tokens.get(index + 1), '!') && is_cfg_test(tokens.get(index + 2))
            {
                walk_tokens(&tokens[index + 3..], true, scan);
                return;
            }
            if is_hash && is_cfg_test(tokens.get(index + 1)) {
                let end = attributed_item_end(tokens, index + 2);
                walk_tokens(&tokens[index + 2..end], true, scan);
                index = end;
                continue;
            }

            match token {
                TokenTree::Group(group) => {
                    let inner: Vec<TokenTree> = group.stream().into_iter().collect();
                    walk_tokens(&inner, in_test, scan);
                }
                TokenTree::Literal(literal) if !in_test => {
                    if let Some(value) = string_literal_value(literal)
                        && is_liq_name(&value)
                    {
                        scan.production_names.push(value);
                    }
                }
                TokenTree::Ident(ident) => {
                    if let Some(value) = metrics_macro_liq_name(ident, &tokens[index + 1..]) {
                        scan.macro_names.push(value);
                    }
                }
                TokenTree::Literal(_) | TokenTree::Punct(_) => {}
            }
            index += 1;
        }
    }

    /// The `liq_` literal that `ident` passes as the first argument when
    /// it names a `metrics` macro and `after` holds its `!` and arguments.
    /// The `describe_` macros end with the same names, so they match too.
    fn metrics_macro_liq_name(ident: &Ident, after: &[TokenTree]) -> Option<String> {
        const MACROS: [&str; 3] = ["counter", "gauge", "histogram"];

        let name = ident.to_string();
        if !MACROS.iter().any(|call| name.ends_with(call)) || !is_punct(after.first(), '!') {
            return None;
        }
        let Some(TokenTree::Group(arguments)) = after.get(1) else {
            return None;
        };
        let Some(TokenTree::Literal(first)) = arguments.stream().into_iter().next() else {
            return None;
        };

        string_literal_value(&first).filter(|value| value.starts_with("liq_"))
    }

    /// Index just past the item that starts at `start`, the item an outer
    /// `#[cfg(test)]` applies to. Further outer attributes come first. A
    /// `const`, `static`, `let`, `use` or `type` ends at its `;`. Another
    /// item (`fn`, `mod`, `impl` and so on) ends at its `;` or its body.
    /// A field, variant or statement also ends at a `,`.
    fn attributed_item_end(tokens: &[TokenTree], start: usize) -> usize {
        const ITEMS: [&str; 8] = [
            "fn",
            "mod",
            "impl",
            "struct",
            "enum",
            "union",
            "trait",
            "macro_rules",
        ];
        const DECLARATIONS: [&str; 5] = ["const", "static", "let", "use", "type"];

        let mut item_start = start;
        while is_punct(tokens.get(item_start), '#')
            && matches!(
                tokens.get(item_start + 1),
                Some(TokenTree::Group(group)) if group.delimiter() == Delimiter::Bracket
            )
        {
            item_start += 2;
        }

        let leading_words: Vec<String> = tokens[item_start..]
            .iter()
            .filter(|token| {
                !matches!(
                    token,
                    TokenTree::Group(group) if group.delimiter() == Delimiter::Parenthesis
                )
            })
            .map_while(|token| match token {
                TokenTree::Ident(ident) => Some(ident.to_string()),
                _ => None,
            })
            .collect();
        let has_any = |keywords: &[&str]| {
            leading_words
                .iter()
                .any(|word| keywords.contains(&word.as_str()))
        };
        let is_item = has_any(ITEMS.as_slice());
        let is_declaration = !is_item && has_any(DECLARATIONS.as_slice());

        tokens[item_start..]
            .iter()
            .position(|token| match token {
                TokenTree::Punct(punct) => {
                    punct.as_char() == ';'
                        || (!is_item && !is_declaration && punct.as_char() == ',')
                }
                TokenTree::Group(group) => !is_declaration && group.delimiter() == Delimiter::Brace,
                TokenTree::Ident(_) | TokenTree::Literal(_) => false,
            })
            .map_or(tokens.len(), |offset| item_start + offset + 1)
    }

    fn is_punct(token: Option<&TokenTree>, character: char) -> bool {
        matches!(token, Some(TokenTree::Punct(punct)) if punct.as_char() == character)
    }

    /// Whether `token` is the `[cfg(test)]` part of an attribute.
    fn is_cfg_test(token: Option<&TokenTree>) -> bool {
        matches!(
            token,
            Some(TokenTree::Group(group))
                if group.delimiter() == Delimiter::Bracket
                    && group.stream().to_string().replace(' ', "") == "cfg(test)"
        )
    }

    /// The text of a plain or raw string literal, escapes left as written.
    /// `None` for any other literal.
    fn string_literal_value(literal: &Literal) -> Option<String> {
        let text = literal.to_string();

        text.trim_start_matches('r')
            .trim_matches('#')
            .strip_prefix('"')?
            .strip_suffix('"')
            .map(str::to_string)
    }

    /// Whether a string literal's text names a `liq_` series: `liq_`
    /// followed by an ASCII letter, digit or `_`, or by `{` (a name built
    /// with `format!`). Log text such as `"liq_ state refresh started"` and
    /// the bare prefix `"liq_"` are not names.
    fn is_liq_name(text: &str) -> bool {
        text.strip_prefix("liq_")
            .and_then(|rest| rest.chars().next())
            .is_some_and(|next| next.is_ascii_alphanumeric() || next == '_' || next == '{')
    }
}
