//! Periodic `liq_*` refresh tasks. They run under a non-escalating
//! supervisor: a metrics failure never stops trading.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chrono::Utc;
use sqlx::SqlitePool;
use task_supervisor::{SupervisedTask, TaskResult};
use tokio::time::{Instant, MissedTickBehavior, timeout};
use tracing::{info, warn};

use st0x_config::ChainRegistry;
use st0x_dto::InfraReport;

use super::performance::{infra_samples, latency_samples, rebalance_samples, reliability_samples};
use super::prices::{load_positions, price_samples};
use super::{LiqFamilies, LiqFamily, LiqSample};
use crate::dashboard::equity_price::EquityPriceStore;
use crate::inventory::BroadcastingInventory;
use crate::performance::equity_timing::load_equity_timings;
use crate::performance::infra::{load_dependency_stats, load_monitor_telemetry};
use crate::performance::rebalance::load_rebalance_timings;
use crate::performance::reliability::{load_failure_events, load_job_queue_health};
use crate::performance::{
    PerformanceError, ReportRange, hedge_latency_report, load_hedge_performance,
};

/// The exporter's cadence and the scrape interval.
const STATE_REFRESH_PERIOD: Duration = Duration::from_secs(60);

/// Longest a positions load may take before the cycle keeps the last prices.
const POSITION_LOAD_TIMEOUT: Duration = Duration::from_secs(30);

/// Every 60 seconds: republishes the inventory series, which covers quiet
/// periods, and refreshes the prices and exposure series.
#[derive(Clone)]
pub(crate) struct LiqStateRefresh {
    pub(crate) inventory: Arc<BroadcastingInventory>,
    pub(crate) equity_prices: EquityPriceStore,
    pub(crate) pool: SqlitePool,
    pub(crate) families: &'static LiqFamilies,
}

impl LiqStateRefresh {
    async fn refresh_once(&self) {
        self.inventory.republish_liq_metrics().await;
        self.refresh_prices().await;
    }

    /// A failed or slow positions load keeps the last published prices, so
    /// exposure never drops to a shorter list.
    async fn refresh_prices(&self) {
        let positions = match timeout(POSITION_LOAD_TIMEOUT, load_positions(&self.pool)).await {
            Ok(Ok(positions)) => positions,
            Ok(Err(error)) => {
                warn!(
                    ?error,
                    "Kept the last liq_ prices: positions failed to load"
                );
                return;
            }
            Err(_) => {
                warn!(
                    timeout = ?POSITION_LOAD_TIMEOUT,
                    "Kept the last liq_ prices: positions took too long to load"
                );
                return;
            }
        };

        let live_prices = self.equity_prices.live_prices(Utc::now()).await;
        self.families.replace(
            LiqFamily::Prices,
            price_samples(&positions, &live_prices),
            SystemTime::now(),
        );
    }
}

/// The exporter's cadence for the performance collectors.
const PERFORMANCE_REFRESH_PERIOD: Duration = Duration::from_secs(60);

/// The rebalance family refreshes on every fifth performance cycle, like the
/// exporter's slow cycle.
const REBALANCE_EVERY_CYCLES: u64 = 5;

/// The window of the 24-hour performance series.
const PERFORMANCE_WINDOW: chrono::Duration = chrono::Duration::hours(24);

/// The window of the rebalance timings.
const REBALANCE_WINDOW: chrono::Duration = chrono::Duration::days(30);

/// Longest one collector may take before the cycle keeps its last family.
const COLLECTOR_TIMEOUT: Duration = Duration::from_secs(30);

/// A collector slower than this is logged: the loaders replay history and
/// the exporter runs the same loaders while both publish.
const SLOW_COLLECTOR: Duration = Duration::from_secs(10);

/// Every 60 seconds: the latencies, reliability and infra families over the
/// last 24 hours, and every fifth cycle the rebalances over the last 30
/// days. Each family comes from the loaders its `/performance/*` endpoint
/// uses. A collector that fails or times out keeps its last family, and its
/// `liq_collector_last_success_ts_seconds` stops advancing.
#[derive(Clone)]
pub(crate) struct LiqPerformanceRefresh {
    pub(crate) pool: SqlitePool,
    pub(crate) chains: ChainRegistry,
    pub(crate) families: &'static LiqFamilies,
}

impl LiqPerformanceRefresh {
    async fn refresh_once(&self, cycle: u64) {
        let now = Utc::now();
        let day = ReportRange {
            from: now - PERFORMANCE_WINDOW,
            to: now,
        };

        self.collect(LiqFamily::Latencies, async {
            let performances = load_hedge_performance(&self.pool, &day).await?;
            Ok(latency_samples(&hedge_latency_report(&performances, &day)))
        })
        .await;

        self.collect(LiqFamily::Reliability, async {
            let (failure_events, job_queues) = tokio::try_join!(
                load_failure_events(&self.pool, &day),
                load_job_queue_health(&self.pool),
            )?;
            Ok(reliability_samples(&failure_events, &job_queues))
        })
        .await;

        self.collect(LiqFamily::Infra, async {
            let (monitor, dependencies) = tokio::try_join!(
                load_monitor_telemetry(&self.pool, &day, &self.chains),
                load_dependency_stats(&self.pool, &day),
            )?;
            Ok(infra_samples(&InfraReport {
                monitor,
                dependencies,
            }))
        })
        .await;

        if cycle.is_multiple_of(REBALANCE_EVERY_CYCLES) {
            let month = ReportRange {
                from: now - REBALANCE_WINDOW,
                to: now,
            };
            self.collect(LiqFamily::Rebalances, async {
                let (usdc, equity) = tokio::try_join!(
                    load_rebalance_timings(&self.pool, &month),
                    load_equity_timings(&self.pool, &month),
                )?;
                Ok(rebalance_samples(&usdc, &equity))
            })
            .await;
        }
    }

    /// Publishes `family` from `build`, or keeps the last one after logging
    /// why. The duration goes to `metrics_refresh_duration_seconds`.
    async fn collect(
        &self,
        family: LiqFamily,
        build: impl Future<Output = Result<Vec<LiqSample>, PerformanceError>>,
    ) {
        let collector = family.collector();
        let started = Instant::now();
        let result = timeout(COLLECTOR_TIMEOUT, build).await;
        let elapsed = started.elapsed();

        metrics::histogram!("metrics_refresh_duration_seconds", "collector" => collector)
            .record(elapsed);
        if elapsed > SLOW_COLLECTOR {
            warn!(collector, ?elapsed, "A liq_ collector was slow");
        }

        match result {
            Ok(Ok(samples)) => self.families.replace(family, samples, SystemTime::now()),
            Ok(Err(error)) => {
                warn!(
                    collector,
                    ?error,
                    "Kept the last liq_ family: its loader failed"
                );
            }
            Err(_) => warn!(
                collector,
                timeout = ?COLLECTOR_TIMEOUT,
                "Kept the last liq_ family: its loader took too long"
            ),
        }
    }
}

impl SupervisedTask for LiqPerformanceRefresh {
    async fn run(&mut self) -> TaskResult {
        info!("liq_ performance refresh started");

        let mut interval = tokio::time::interval(PERFORMANCE_REFRESH_PERIOD);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut cycle: u64 = 0;
        loop {
            interval.tick().await;
            self.refresh_once(cycle).await;
            cycle = cycle.wrapping_add(1);
        }
    }
}

impl SupervisedTask for LiqStateRefresh {
    async fn run(&mut self) -> TaskResult {
        info!("liq_ state refresh started");

        let mut interval = tokio::time::interval(STATE_REFRESH_PERIOD);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            self.refresh_once().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;
    use tokio::sync::broadcast;

    use st0x_config::create_test_ctx_with_order_owner;

    use st0x_execution::{FractionalShares, Symbol};
    use st0x_float_macro::float;

    use super::*;
    use crate::inventory::InventoryView;
    use crate::metrics::liquidity::LiqMetric;
    use crate::metrics::liquidity::inventory::tests::{leaked_families, publisher, rendered_store};
    use crate::metrics::liquidity::tests::{parse_exposition, series};
    use crate::test_utils::setup_test_db;

    fn performance(families: &'static LiqFamilies, pool: SqlitePool) -> LiqPerformanceRefresh {
        LiqPerformanceRefresh {
            pool,
            chains: create_test_ctx_with_order_owner(Address::ZERO).chains,
            families,
        }
    }

    fn performance_collectors(families: &'static LiqFamilies) -> Vec<Option<f64>> {
        ["latencies", "reliability", "infra", "rebalances"]
            .into_iter()
            .map(|name| collector(families, name))
            .collect()
    }

    /// On an empty database every family publishes: the counts are zero
    /// where the loaders report zero, and absent where they report nothing.
    #[tokio::test]
    async fn the_first_performance_cycle_publishes_every_family() {
        let families = leaked_families();
        let task = performance(families, setup_test_db().await);
        let recorder = crate::metrics::local_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);

        task.refresh_once(0).await;

        let durations = parse_exposition(&handle.render());
        for collector in ["latencies", "reliability", "infra", "rebalances"] {
            assert_eq!(
                durations.get(&series(
                    "metrics_refresh_duration_seconds_count",
                    &[("collector", collector)],
                )),
                Some(&1.0),
                "{collector}"
            );
        }
        assert!(
            performance_collectors(families).iter().all(Option::is_some),
            "{:?}",
            performance_collectors(families)
        );
        let rendered = rendered_store(families);
        let base = [("chain", "base")];
        assert_eq!(
            rendered.get(&series("liq_poll_cycles_24h", &base)),
            Some(&0.0)
        );
        assert_eq!(rendered.get(&series("liq_block_lag_blocks", &base)), None);
    }

    #[tokio::test]
    async fn rebalances_refresh_only_every_fifth_cycle() {
        let families = leaked_families();
        let task = performance(families, setup_test_db().await);

        task.refresh_once(1).await;
        assert_eq!(collector(families, "rebalances"), None);
        assert!(collector(families, "latencies").is_some());

        task.refresh_once(5).await;
        assert!(collector(families, "rebalances").is_some());
    }

    #[tokio::test]
    async fn a_failing_loader_keeps_the_last_family_and_its_timestamp() {
        let families = leaked_families();
        let task = performance(families, setup_test_db().await);
        let previous = vec![
            LiqSample::new(
                LiqMetric::JobQueue,
                vec![
                    ("job_type", "PlaceHedge".to_string()),
                    ("state", "pending".to_string()),
                ],
                4.0,
            )
            .unwrap(),
        ];
        families.replace(
            LiqFamily::Reliability,
            previous,
            SystemTime::UNIX_EPOCH + Duration::from_secs(100),
        );
        task.pool.close().await;

        task.refresh_once(0).await;

        assert_eq!(
            rendered_store(families).get(&series(
                "liq_job_queue",
                &[("job_type", "PlaceHedge"), ("state", "pending")]
            )),
            Some(&4.0)
        );
        assert_eq!(
            performance_collectors(families),
            [None, Some(100.0), None, None]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_collector_that_times_out_keeps_the_last_family() {
        let families = leaked_families();
        // The pending collector never reads the pool; a lazy one does no I/O
        // while time is paused.
        let task = performance(
            families,
            SqlitePool::connect_lazy("sqlite::memory:").unwrap(),
        );
        families.replace(
            LiqFamily::Latencies,
            Vec::new(),
            SystemTime::UNIX_EPOCH + Duration::from_secs(100),
        );

        task.collect(LiqFamily::Latencies, std::future::pending())
            .await;

        assert_eq!(collector(families, "latencies"), Some(100.0));
    }

    async fn refresh(
        families: &'static LiqFamilies,
        equity_prices: EquityPriceStore,
    ) -> LiqStateRefresh {
        let (sender, _) = broadcast::channel(1);
        let view = InventoryView::default().with_equity(
            Symbol::new("AAPL").unwrap(),
            FractionalShares::new(float!(7)),
            FractionalShares::ZERO,
        );

        LiqStateRefresh {
            inventory: Arc::new(
                BroadcastingInventory::new(view, sender)
                    .publishing_liq_metrics(publisher(families)),
            ),
            equity_prices,
            pool: setup_test_db().await,
            families,
        }
    }

    fn collector(families: &'static LiqFamilies, name: &str) -> Option<f64> {
        rendered_store(families)
            .get(&series(
                "liq_collector_last_success_ts_seconds",
                &[("collector", name)],
            ))
            .copied()
    }

    async fn run_first_tick(task: LiqStateRefresh, families: &'static LiqFamilies) {
        let mut task = task;
        let running = tokio::spawn(async move { task.run().await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while collector(families, "prices").is_none() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        running.abort();
    }

    fn aapl_onchain(families: &'static LiqFamilies) -> Option<f64> {
        rendered_store(families)
            .get(&series(
                "liq_equity_onchain_available",
                &[("symbol", "AAPL")],
            ))
            .copied()
    }

    /// The first tick runs at once and puts back the inventory of the last
    /// write, here after the stored family was lost.
    #[tokio::test]
    async fn the_first_tick_republishes_the_inventory_and_publishes_prices() {
        let families = leaked_families();
        let task = refresh(families, EquityPriceStore::new([])).await;
        task.inventory.start_publishing_liq_metrics().await;
        families.replace(LiqFamily::Inventory, Vec::new(), SystemTime::UNIX_EPOCH);
        assert_eq!(aapl_onchain(families), None);

        run_first_tick(task, families).await;

        assert_eq!(aapl_onchain(families), Some(7.0));
    }

    /// Before boot restores the view there is nothing to publish: an empty
    /// view would read as zero balances.
    #[tokio::test]
    async fn the_inventory_is_not_published_before_the_start() {
        let families = leaked_families();
        let task = refresh(families, EquityPriceStore::new([])).await;

        run_first_tick(task, families).await;

        assert_eq!(collector(families, "inventory"), None);
    }

    #[tokio::test]
    async fn a_failed_positions_load_keeps_the_last_prices() {
        let families = leaked_families();
        let task = refresh(
            families,
            EquityPriceStore::with_live_mark(Symbol::new("AAPL").unwrap(), float!(2.5)),
        )
        .await;
        let previous = vec![
            crate::metrics::liquidity::LiqSample::new(
                crate::metrics::liquidity::LiqMetric::PositionLastPriceUsd,
                vec![("symbol", "AAPL".to_string())],
                2.0,
            )
            .unwrap(),
        ];
        families.replace(
            LiqFamily::Prices,
            previous,
            SystemTime::UNIX_EPOCH + Duration::from_secs(100),
        );
        task.pool.close().await;

        task.refresh_prices().await;

        assert_eq!(
            rendered_store(families).get(&series(
                "liq_position_last_price_usd",
                &[("symbol", "AAPL")]
            )),
            Some(&2.0)
        );
        assert_eq!(collector(families, "prices"), Some(100.0));
    }
}
