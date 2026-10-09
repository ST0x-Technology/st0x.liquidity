//! Periodic `liq_*` refresh tasks. They run under a non-escalating
//! supervisor: a metrics failure never stops trading.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chrono::Utc;
use sqlx::SqlitePool;
use task_supervisor::{SupervisedTask, TaskResult};
use tokio::time::{MissedTickBehavior, timeout};
use tracing::{info, warn};

use super::prices::{load_positions, price_samples};
use super::{LiqFamilies, LiqFamily};
use crate::dashboard::equity_price::EquityPriceStore;
use crate::inventory::BroadcastingInventory;

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
    use tokio::sync::broadcast;

    use st0x_execution::{FractionalShares, Symbol};
    use st0x_float_macro::float;

    use super::*;
    use crate::inventory::InventoryView;
    use crate::metrics::liquidity::inventory::tests::{leaked_families, publisher, rendered_store};
    use crate::metrics::liquidity::tests::series;
    use crate::test_utils::setup_test_db;

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
