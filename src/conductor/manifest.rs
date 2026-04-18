//! Query processor manifest with compile-time wiring guarantees.
//!
//! This module enumerates ALL query processors that must be wired to
//! CQRS frameworks. The exhaustive destructuring in
//! [`QueryManifest::build_frameworks()`] ensures that adding a new
//! query to `QueryManifest` forces you to wire it.
//!
//! # Adding a new query processor
//!
//! 1. Add field to [`QueryManifest`]
//! 2. Create it in [`QueryManifest::new()`]
//! 3. Wire it in [`QueryManifest::build()`] -
//!    destructuring forces you to handle it
//! 4. Add output to [`BuiltFrameworks`] if needed

use sqlx::SqlitePool;
use std::sync::Arc;

use st0x_event_sorcery::{Projection, Store, StoreBuilder};

use crate::dashboard::Broadcaster;
use crate::equity_redemption::EquityRedemption;
use crate::inventory::InventorySnapshot;
use crate::position::Position;
use crate::rebalancing::RebalancingTrigger;
use crate::rebalancing::equity::EquityTransferServices;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::UsdcRebalance;

/// All query processors that must be created and wired when
/// rebalancing is enabled.
///
/// Exhaustive destructuring in [`Self::build()`]
/// ensures every field is handled.
pub(super) struct QueryManifest {
    rebalancing_trigger: Arc<RebalancingTrigger>,
    broadcaster: Arc<Broadcaster>,
}

/// Built CQRS frameworks from the wiring process.
pub(super) struct BuiltFrameworks {
    pub(super) position: Arc<Store<Position>>,
    pub(super) position_projection: Arc<Projection<Position>>,
    pub(super) mint: Arc<Store<TokenizedEquityMint>>,
    pub(super) redemption: Arc<Store<EquityRedemption>>,
    pub(super) usdc: Arc<Store<UsdcRebalance>>,
    pub(super) snapshot: Arc<Store<InventorySnapshot>>,
}

impl QueryManifest {
    pub(super) fn new(
        rebalancing_trigger: Arc<RebalancingTrigger>,
        broadcaster: Arc<Broadcaster>,
    ) -> Self {
        Self {
            rebalancing_trigger,
            broadcaster,
        }
    }

    /// Builds all CQRS frameworks, wiring query processors to each.
    ///
    /// Destructures `self` to ensure every field is handled. If you
    /// add a new query to the manifest, this method won't compile
    /// until you wire it.
    pub(super) async fn build(
        self,
        pool: SqlitePool,
        services: EquityTransferServices,
    ) -> anyhow::Result<BuiltFrameworks> {
        let Self {
            rebalancing_trigger,
            broadcaster,
        } = self;

        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .with(rebalancing_trigger.clone())
            .build(())
            .await?;

        let mint = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
            .with(rebalancing_trigger.clone())
            .with(broadcaster.clone())
            .build(services.clone())
            .await?;

        let redemption = StoreBuilder::<EquityRedemption>::new(pool.clone())
            .with(rebalancing_trigger.clone())
            .with(broadcaster.clone())
            .build(services)
            .await?;

        let usdc = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .with(rebalancing_trigger.clone())
            .with(broadcaster)
            .build(())
            .await?;

        // The trigger owns the snapshot projection internally, so it's
        // the sole subscriber here.
        let snapshot = StoreBuilder::<InventorySnapshot>::new(pool.clone())
            .with(rebalancing_trigger)
            .build(())
            .await?;

        Ok(BuiltFrameworks {
            position,
            position_projection,
            mint,
            redemption,
            usdc,
            snapshot,
        })
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;
    use std::collections::{BTreeMap, HashSet};
    use std::time::Duration;
    use tokio::sync::{broadcast, mpsc};

    use st0x_event_sorcery::test_store;
    use st0x_execution::{FractionalShares, Symbol};
    use st0x_float_macro::float;

    use super::*;
    use crate::config::{AssetsConfig, EquitiesConfig};
    use crate::inventory::snapshot::{InventorySnapshotCommand, InventorySnapshotId};
    use crate::inventory::{BroadcastingInventory, ImbalanceThreshold, InventoryView, Venue};
    use crate::onchain::mock::MockRaindex;
    use crate::rebalancing::RebalancingTriggerConfig;
    use crate::test_utils::setup_test_db;
    use crate::tokenization::mock::MockTokenizer;
    use crate::wrapper::mock::MockWrapper;

    fn test_trigger_config() -> RebalancingTriggerConfig {
        RebalancingTriggerConfig {
            equity: ImbalanceThreshold {
                target: float!(0.5),
                deviation: float!(0.2),
            },
            usdc: Some(ImbalanceThreshold {
                target: float!(0.6),
                deviation: float!(0.15),
            }),
            transfer_timeout: Duration::from_secs(30 * 60),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },

            disabled_assets: HashSet::new(),
        }
    }

    #[tokio::test]
    async fn build_frameworks_produces_working_stores() {
        let pool = setup_test_db().await;
        let (operation_sender, _operation_receiver) = mpsc::channel(10);
        let (event_sender, _event_receiver) = broadcast::channel(10);

        let vault_registry = Arc::new(test_store(pool.clone(), ()));

        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender.clone(),
        ));

        let rebalancing_trigger = Arc::new(RebalancingTrigger::new(
            test_trigger_config(),
            vault_registry,
            Address::ZERO,
            Address::ZERO,
            inventory.clone(),
            operation_sender,
            Arc::new(MockWrapper::new()),
        ));

        let broadcaster = Arc::new(Broadcaster::new(event_sender, pool.clone()));
        let manifest = QueryManifest::new(rebalancing_trigger, broadcaster);

        let services = EquityTransferServices {
            raindex: Arc::new(MockRaindex::new()),
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: Arc::new(MockWrapper::new()),
        };

        let frameworks = manifest.build(pool, services).await.unwrap();

        // Verify stores are usable by checking that loading a
        // nonexistent position returns None
        let result = frameworks
            .position_projection
            .load(&Symbol::new("AAPL").unwrap())
            .await
            .unwrap();
        assert!(result.is_none());
    }

    /// Live snapshot commands dispatched through the built store must
    /// reach `BroadcastingInventory` via the trigger's internal
    /// projection. Historical replay on restart is out of scope —
    /// `InventorySnapshot` is non-projected, so `StoreBuilder::build`
    /// does not `catch_up` reactor subscribers.
    #[tokio::test]
    async fn build_frameworks_dispatches_live_snapshot_events_to_view() {
        let pool = setup_test_db().await;
        let (operation_sender, _operation_receiver) = mpsc::channel(10);
        let (event_sender, _event_receiver) = broadcast::channel(10);

        let vault_registry = Arc::new(test_store(pool.clone(), ()));

        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender.clone(),
        ));

        let rebalancing_trigger = Arc::new(RebalancingTrigger::new(
            test_trigger_config(),
            vault_registry,
            Address::ZERO,
            Address::ZERO,
            inventory.clone(),
            operation_sender,
            Arc::new(MockWrapper::new()),
        ));
        let broadcaster = Arc::new(Broadcaster::new(event_sender, pool.clone()));
        let manifest = QueryManifest::new(rebalancing_trigger, broadcaster);
        let services = EquityTransferServices {
            raindex: Arc::new(MockRaindex::new()),
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: Arc::new(MockWrapper::new()),
        };
        let built = manifest.build(pool, services).await.unwrap();

        // Dispatch a live snapshot command through the built store and
        // verify it lands in the shared BroadcastingInventory via the
        // trigger's internal projection.
        let id = InventorySnapshotId {
            orderbook: Address::repeat_byte(0xAB),
            owner: Address::repeat_byte(0xCD),
        };
        let mut balances = BTreeMap::new();
        balances.insert(
            Symbol::new("RKLB").unwrap(),
            FractionalShares::new(float!(7)),
        );
        built
            .snapshot
            .send(&id, InventorySnapshotCommand::OnchainEquity { balances })
            .await
            .unwrap();

        let symbol = Symbol::new("RKLB").unwrap();
        let available = inventory
            .read()
            .await
            .equity_available(&symbol, Venue::MarketMaking);
        assert_eq!(
            available,
            Some(FractionalShares::new(float!(7))),
            "manifest build must wire the InventorySnapshot store so that \
             live commands dispatch through the trigger's projection into \
             BroadcastingInventory",
        );
    }
}
