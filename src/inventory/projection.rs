//! Folds [`InventorySnapshotEvent`]s into the in-memory
//! [`BroadcastingInventory`] that feeds the dashboard WS.
//!
//! When rebalancing is enabled the projection is owned by
//! `RebalancingTrigger`, which calls [`Self::apply`] before its
//! threshold checks so rebalancing decisions only run against a
//! successfully-folded view. When rebalancing is disabled the
//! projection is registered directly as the sole subscriber on the
//! `InventorySnapshot` store so the dashboard still updates.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tracing::{debug, warn};

use st0x_event_sorcery::{EntityList, Reactor, deps};

use super::{
    BroadcastingInventory, InventorySnapshot, InventoryViewError, snapshot::InventorySnapshotEvent,
};

pub(crate) struct InventoryProjection {
    inventory: Arc<BroadcastingInventory>,
}

impl InventoryProjection {
    pub(crate) fn new(inventory: Arc<BroadcastingInventory>) -> Self {
        Self { inventory }
    }

    pub(crate) async fn apply(
        &self,
        event: &InventorySnapshotEvent,
    ) -> Result<(), InventoryProjectionError> {
        let now = Utc::now();

        let mut inventory = self.inventory.write().await;
        let updated = match inventory.clone().apply_snapshot_event(event, now) {
            Ok(updated) => {
                debug!("Applied inventory snapshot event");
                updated
            }
            Err(error) => {
                warn!(
                    ?error,
                    "Inventory snapshot apply failed; force-applying to existing view",
                );
                // Recover against the existing view, not a reset, so
                // unrelated venues survive a single failing event.
                let reason = Arc::new(error);
                let forced = inventory
                    .clone()
                    .force_apply_snapshot_event(event, now, reason)?;
                debug!("Force-applied inventory snapshot after recovery");
                forced
            }
        };
        *inventory = updated;
        drop(inventory);
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InventoryProjectionError {
    #[error(transparent)]
    View(#[from] InventoryViewError),
}

deps!(InventoryProjection, [InventorySnapshot]);

#[async_trait]
impl Reactor for InventoryProjection {
    type Error = InventoryProjectionError;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|_id, event| async move { self.apply(&event).await })
            .exhaustive()
            .await
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;
    use std::collections::BTreeMap;

    use chrono::Utc;
    use tokio::sync::broadcast;

    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::{FractionalShares, Symbol};
    use st0x_finance::Usdc;
    use st0x_float_macro::float;

    use super::*;
    use crate::inventory::snapshot::{
        InventorySnapshot, InventorySnapshotCommand, InventorySnapshotId,
    };
    use crate::inventory::{InventoryView, Venue};
    use crate::test_utils::setup_test_db;

    fn shares(amount: i64) -> FractionalShares {
        FractionalShares::new(float!(&amount.to_string()))
    }

    fn symbol(name: &str) -> Symbol {
        Symbol::new(name).unwrap()
    }

    fn make_projection() -> (InventoryProjection, Arc<BroadcastingInventory>) {
        let (sender, _receiver) = broadcast::channel(10);
        let inventory = Arc::new(BroadcastingInventory::new(InventoryView::default(), sender));
        let projection = InventoryProjection::new(inventory.clone());
        (projection, inventory)
    }

    async fn read_equity_available(
        inventory: &BroadcastingInventory,
        symbol: &Symbol,
        venue: Venue,
    ) -> Option<FractionalShares> {
        let view = inventory.read().await;
        view.equity_available(symbol, venue)
    }

    async fn read_usdc_available(inventory: &BroadcastingInventory, venue: Venue) -> Option<Usdc> {
        let view = inventory.read().await;
        view.usdc_available(venue)
    }

    #[tokio::test]
    async fn apply_onchain_equity_updates_view() {
        let (projection, inventory) = make_projection();

        let mut balances = BTreeMap::new();
        balances.insert(symbol("RKLB"), shares(2));
        let event = InventorySnapshotEvent::OnchainEquity {
            balances,
            fetched_at: Utc::now(),
        };

        projection.apply(&event).await.unwrap();

        assert_eq!(
            read_equity_available(&inventory, &symbol("RKLB"), Venue::MarketMaking).await,
            Some(shares(2)),
            "onchain equity available should reflect snapshot balance",
        );
    }

    #[tokio::test]
    async fn apply_offchain_usd_converts_and_updates_view() {
        let (projection, inventory) = make_projection();

        let event = InventorySnapshotEvent::OffchainUsd {
            usd_balance_cents: 12345,
            fetched_at: Utc::now(),
        };

        projection.apply(&event).await.unwrap();

        assert_eq!(
            read_usdc_available(&inventory, Venue::Hedging).await,
            Some(Usdc::from_cents(12345).unwrap()),
            "offchain usdc available should reflect converted cents",
        );
    }

    #[tokio::test]
    async fn apply_wallet_read_events_are_noops_on_view() {
        let (projection, inventory) = make_projection();

        let event = InventorySnapshotEvent::EthereumUsdc {
            usdc_balance: Usdc::new(float!(100)),
            fetched_at: Utc::now(),
        };

        projection.apply(&event).await.unwrap();

        assert_eq!(
            read_usdc_available(&inventory, Venue::MarketMaking).await,
            None,
            "EthereumUsdc should not touch tracked inventory slots",
        );
        assert_eq!(read_usdc_available(&inventory, Venue::Hedging).await, None,);
    }

    /// Force-apply must leave untouched venue slots alone while
    /// overwriting the event's targeted slot. Tested at the view level
    /// because the only `InventoryViewError` reachable from
    /// `apply_snapshot_event` (`UsdBalanceConversion`) fails identically
    /// in `force_apply_snapshot_event`, so the projection's recovery
    /// branch is not reachable via a real event.
    #[tokio::test]
    async fn force_apply_preserves_unrelated_state_and_overwrites_target() {
        let seed = InventoryView::default()
            .apply_snapshot_event(
                &InventorySnapshotEvent::OffchainUsd {
                    usd_balance_cents: 500_000,
                    fetched_at: Utc::now(),
                },
                Utc::now(),
            )
            .unwrap();

        let reason = Arc::new(InventoryViewError::UsdBalanceConversion(-1));
        let forced = seed
            .force_apply_snapshot_event(
                &InventorySnapshotEvent::OnchainUsdc {
                    usdc_balance: Usdc::from_cents(2_000_000).unwrap(),
                    fetched_at: Utc::now(),
                },
                Utc::now(),
                reason,
            )
            .unwrap();

        assert_eq!(
            forced.usdc_available(Venue::Hedging),
            Some(Usdc::from_cents(500_000).unwrap()),
            "force recovery must not wipe unrelated-venue state",
        );
        assert_eq!(
            forced.usdc_available(Venue::MarketMaking),
            Some(Usdc::from_cents(2_000_000).unwrap()),
            "force recovery must overwrite the targeted venue slot",
        );
    }

    /// Applying an event targeting one venue must not wipe an unrelated
    /// venue's previously-seeded balance.
    #[tokio::test]
    async fn apply_preserves_unrelated_venue_state() {
        let (projection, inventory) = make_projection();

        projection
            .apply(&InventorySnapshotEvent::OffchainUsd {
                usd_balance_cents: 500_000,
                fetched_at: Utc::now(),
            })
            .await
            .unwrap();

        projection
            .apply(&InventorySnapshotEvent::OnchainUsdc {
                usdc_balance: Usdc::from_cents(2_000_000).unwrap(),
                fetched_at: Utc::now(),
            })
            .await
            .unwrap();

        assert_eq!(
            read_usdc_available(&inventory, Venue::Hedging).await,
            Some(Usdc::from_cents(500_000).unwrap()),
            "hedging USDC must survive a subsequent unrelated onchain event",
        );
        assert_eq!(
            read_usdc_available(&inventory, Venue::MarketMaking).await,
            Some(Usdc::from_cents(2_000_000).unwrap()),
            "market-making USDC should reflect the onchain event",
        );
    }

    /// When rebalancing is disabled the projection is the sole
    /// subscriber on the snapshot store and must still propagate events
    /// to `BroadcastingInventory` so the dashboard reflects live balances.
    #[tokio::test]
    async fn snapshot_command_through_store_updates_view_without_rebalancing() {
        let pool = setup_test_db().await;
        let (sender, _receiver) = broadcast::channel(10);
        let inventory = Arc::new(BroadcastingInventory::new(InventoryView::default(), sender));

        // Mirror the non-rebalancing branch of `Conductor::start`: the
        // projection is the only subscriber on the snapshot store.
        let projection = Arc::new(InventoryProjection::new(inventory.clone()));
        let snapshot = StoreBuilder::<InventorySnapshot>::new(pool)
            .with(projection)
            .build(())
            .await
            .unwrap();

        let id = InventorySnapshotId {
            orderbook: Address::repeat_byte(0xAB),
            owner: Address::repeat_byte(0xCD),
        };

        let mut balances = BTreeMap::new();
        balances.insert(symbol("RKLB"), shares(42));
        snapshot
            .send(&id, InventorySnapshotCommand::OnchainEquity { balances })
            .await
            .unwrap();

        assert_eq!(
            read_equity_available(&inventory, &symbol("RKLB"), Venue::MarketMaking).await,
            Some(shares(42)),
            "snapshot dispatched through store must reach BroadcastingInventory \
             via the projection even when no RebalancingTrigger is registered",
        );
    }
}
