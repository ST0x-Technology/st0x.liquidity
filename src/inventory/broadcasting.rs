//! Thread-safe inventory wrapper that publishes snapshots on mutation.
//!
//! [`BroadcastingInventory`] wraps an [`InventoryView`] behind an [`RwLock`].
//! When a [`BroadcastingWriteGuard`] is dropped, it sends a
//! [`Statement::InventorySnapshot`] to connected dashboard clients and, when
//! enabled, publishes the `liq_*` inventory series.

use std::ops::{Deref, DerefMut};

use chrono::Utc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, broadcast};
use tracing::{debug, warn};

use st0x_dto::{InventorySnapshot, Statement};

use super::InventoryView;
use crate::metrics::liquidity::inventory::{InventoryPublisher, InventoryRead};

/// Thread-safe inventory that publishes a snapshot whenever the write guard
/// is released.
pub(crate) struct BroadcastingInventory {
    view: RwLock<InventoryView>,
    sender: broadcast::Sender<Statement>,
    liq_metrics: Option<InventoryPublisher>,
}

impl BroadcastingInventory {
    pub(crate) fn new(view: InventoryView, sender: broadcast::Sender<Statement>) -> Self {
        Self {
            view: RwLock::new(view),
            sender,
            liq_metrics: None,
        }
    }

    /// Also publishes the `liq_*` inventory series on every guard release.
    pub(crate) fn publishing_liq_metrics(self, publisher: InventoryPublisher) -> Self {
        Self {
            liq_metrics: Some(publisher),
            ..self
        }
    }

    /// Starts the `liq_*` inventory publish, once boot has restored the
    /// view, and publishes the restored view at once. Writes before this
    /// (boot recovery seeding stranded redemptions into an empty view)
    /// publish nothing.
    pub(crate) async fn start_publishing_liq_metrics(&self) {
        let Some(publisher) = &self.liq_metrics else {
            warn!(target: "inventory", "No liq_ inventory publisher attached; nothing to start");
            return;
        };

        let read = publisher.start(&*self.view.read().await);
        publisher.publish(read);
    }

    pub(crate) async fn read(&self) -> RwLockReadGuard<'_, InventoryView> {
        self.view.read().await
    }

    /// Publishes the `liq_*` inventory series again from the current view.
    /// Covers quiet periods and any write path that skipped the publish; a
    /// write that lands meanwhile carries a newer generation and wins.
    pub(crate) async fn republish_liq_metrics(&self) {
        let Some(publisher) = &self.liq_metrics else {
            warn!(target: "inventory", "No liq_ inventory publisher attached; skipped the republish");
            return;
        };

        let Some(read) = publisher.read_current(&*self.view.read().await) else {
            debug!(
                target: "inventory",
                "Skipped the liq_ inventory republish: boot has not restored the inventory yet"
            );
            return;
        };
        publisher.publish(read);
    }

    pub(crate) async fn write(&self) -> BroadcastingWriteGuard<'_> {
        BroadcastingWriteGuard {
            guard: self.view.write().await,
            sender: &self.sender,
            liq_metrics: PendingPublish {
                publisher: self.liq_metrics.as_ref(),
                read: None,
            },
        }
    }

    /// Write access that skips the dashboard broadcast and the `liq_*`
    /// publish on drop.
    ///
    /// For mutations of state `to_dto()` does not expose (the offchain-order
    /// gate): publishing those would push a byte-identical snapshot per
    /// order placement and termination and rebuild unchanged `liq_*`
    /// series. Any mutation the dashboard or the series can observe must use
    /// [`Self::write`] instead.
    pub(crate) async fn write_without_broadcast(&self) -> RwLockWriteGuard<'_, InventoryView> {
        self.view.write().await
    }
}

/// Write guard that publishes the current inventory on drop.
///
/// Field order matters: `drop` runs first, with the lock held, and reads
/// the view into `liq_metrics`; then `guard` drops and releases the lock;
/// then `liq_metrics` drops and builds and stores the samples, so other
/// writers do not wait for the build.
pub(crate) struct BroadcastingWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, InventoryView>,
    sender: &'a broadcast::Sender<Statement>,
    liq_metrics: PendingPublish<'a>,
}

/// A `liq_*` publish read under the write lock and stored on drop, after the
/// lock is released.
struct PendingPublish<'a> {
    publisher: Option<&'a InventoryPublisher>,
    read: Option<InventoryRead>,
}

impl Drop for PendingPublish<'_> {
    fn drop(&mut self) {
        if let (Some(publisher), Some(read)) = (self.publisher, self.read.take()) {
            publisher.publish(read);
        }
    }
}

impl Deref for BroadcastingWriteGuard<'_> {
    type Target = InventoryView;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for BroadcastingWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl Drop for BroadcastingWriteGuard<'_> {
    fn drop(&mut self) {
        if let Some(publisher) = self.liq_metrics.publisher
            && publisher.started()
        {
            self.liq_metrics.read = Some(publisher.read_changed(&self.guard));
        }

        // Sent while the lock is held, so clients see snapshots in write
        // order.
        if self.sender.receiver_count() == 0 {
            return;
        }

        let snapshot = InventorySnapshot {
            inventory: self.guard.to_dto(),
            fetched_at: Utc::now(),
        };

        if let Err(error) = self
            .sender
            .send(Statement::InventorySnapshot(Box::new(snapshot)))
        {
            warn!(target: "inventory", %error, "Failed to broadcast inventory snapshot");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex, OnceLock, Weak};

    use st0x_execution::{FractionalShares, Symbol};
    use st0x_float_macro::float;

    use super::*;
    use crate::inventory::Venue;
    use crate::metrics::liquidity::inventory::tests::{leaked_families, publisher, rendered_store};
    use crate::metrics::liquidity::tests::series;
    use crate::offchain::order::OffchainOrderId;

    fn aapl_onchain(families: &'static crate::metrics::liquidity::LiqFamilies) -> Option<f64> {
        rendered_store(families)
            .get(&series(
                "liq_equity_onchain_available",
                &[("symbol", "AAPL")],
            ))
            .copied()
    }

    async fn add_aapl(inventory: &BroadcastingInventory, shares: FractionalShares) {
        let mut guard = inventory.write().await;
        let aapl = Symbol::new("AAPL").unwrap();
        let current = guard
            .equity_available(&aapl, Venue::MarketMaking)
            .unwrap_or(FractionalShares::ZERO);
        let next = FractionalShares::new((current.inner() + shares.inner()).unwrap());
        *guard = std::mem::take(&mut *guard).with_equity(aapl, next, FractionalShares::ZERO);
    }

    #[tokio::test]
    async fn a_write_publishes_the_inventory_series_with_no_dashboard_clients() {
        let families = leaked_families();
        let (sender, receiver) = broadcast::channel(16);
        drop(receiver);
        let inventory = BroadcastingInventory::new(InventoryView::default(), sender)
            .publishing_liq_metrics(publisher(families));
        inventory.start_publishing_liq_metrics().await;

        add_aapl(&inventory, FractionalShares::new(float!(10))).await;
        assert_eq!(aapl_onchain(families), Some(10.0));

        add_aapl(&inventory, FractionalShares::new(float!(2.5))).await;
        assert_eq!(aapl_onchain(families), Some(12.5));
    }

    #[tokio::test]
    async fn write_without_broadcast_does_not_publish_the_inventory_series() {
        let families = leaked_families();
        let (sender, _receiver) = broadcast::channel(16);
        let inventory = BroadcastingInventory::new(InventoryView::default(), sender)
            .publishing_liq_metrics(publisher(families));
        inventory.start_publishing_liq_metrics().await;
        let started = rendered_store(families);

        {
            let mut guard = inventory.write_without_broadcast().await;
            *guard = std::mem::take(&mut *guard).with_equity(
                Symbol::new("AAPL").unwrap(),
                FractionalShares::new(float!(1)),
                FractionalShares::ZERO,
            );
        }

        assert_eq!(rendered_store(families), started);
    }

    /// Boot recovery writes into the empty view before hydration; those
    /// writes publish nothing, and the start publishes the restored view.
    #[tokio::test]
    async fn writes_before_the_start_publish_nothing() {
        let families = leaked_families();
        let (sender, _receiver) = broadcast::channel(16);
        let inventory = BroadcastingInventory::new(InventoryView::default(), sender)
            .publishing_liq_metrics(publisher(families));

        add_aapl(&inventory, FractionalShares::new(float!(3))).await;
        assert_eq!(rendered_store(families), BTreeMap::new());

        inventory.start_publishing_liq_metrics().await;
        assert_eq!(aapl_onchain(families), Some(3.0));
    }

    /// The samples are built after the write lock is released, so another
    /// writer never waits for them.
    #[tokio::test]
    async fn the_inventory_series_are_built_after_the_write_lock_is_released() {
        let families = leaked_families();
        let cell: Arc<OnceLock<Weak<BroadcastingInventory>>> = Arc::new(OnceLock::new());
        let lock_free_at_publish = Arc::new(Mutex::new(Vec::new()));
        let hook = {
            let cell = Arc::clone(&cell);
            let lock_free_at_publish = Arc::clone(&lock_free_at_publish);
            move || {
                let inventory = cell.get().unwrap().upgrade().unwrap();
                let lock_free = inventory.view.try_write().is_ok();
                lock_free_at_publish.lock().unwrap().push(lock_free);
            }
        };
        let (sender, _receiver) = broadcast::channel(16);
        let inventory = Arc::new(
            BroadcastingInventory::new(InventoryView::default(), sender)
                .publishing_liq_metrics(publisher(families).before_publish(hook)),
        );
        cell.set(Arc::downgrade(&inventory)).unwrap();
        inventory.start_publishing_liq_metrics().await;
        lock_free_at_publish.lock().unwrap().clear();

        add_aapl(&inventory, FractionalShares::new(float!(1))).await;

        assert_eq!(*lock_free_at_publish.lock().unwrap(), [true]);
        assert_eq!(aapl_onchain(families), Some(1.0));
    }

    /// Concurrent writers: dashboard clients see snapshots in write order,
    /// and the stored series end at the last write.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writes_reach_clients_in_order_and_the_store_ends_at_the_last() {
        let families = leaked_families();
        let (sender, mut receiver) = broadcast::channel(256);
        let inventory = Arc::new(
            BroadcastingInventory::new(InventoryView::default(), sender)
                .publishing_liq_metrics(publisher(families)),
        );
        inventory.start_publishing_liq_metrics().await;

        let writers: Vec<_> = (0..20)
            .map(|_| {
                let inventory = Arc::clone(&inventory);
                tokio::spawn(async move {
                    for _ in 0..5 {
                        add_aapl(&inventory, FractionalShares::new(float!(1))).await;
                    }
                })
            })
            .collect();
        for writer in writers {
            writer.await.unwrap();
        }

        let mut seen = Vec::new();
        while let Ok(Statement::InventorySnapshot(snapshot)) = receiver.try_recv() {
            let onchain: f64 = snapshot.inventory.per_symbol[0]
                .onchain_available
                .to_string()
                .parse()
                .unwrap();
            seen.push(onchain);
        }

        let expected: Vec<f64> = (1..=100).map(f64::from).collect();
        assert_eq!(seen, expected);
        assert_eq!(aapl_onchain(families), Some(100.0));
    }

    fn create_broadcasting_inventory() -> (BroadcastingInventory, broadcast::Receiver<Statement>) {
        let (sender, receiver) = broadcast::channel(16);
        (
            BroadcastingInventory::new(InventoryView::default(), sender),
            receiver,
        )
    }

    #[tokio::test]
    async fn read_returns_default_inventory() {
        let (inventory, _receiver) = create_broadcasting_inventory();
        let dto = inventory.read().await.to_dto();

        assert!(dto.per_symbol.is_empty());
    }

    #[tokio::test]
    async fn write_guard_allows_mutation() {
        let (inventory, _receiver) = create_broadcasting_inventory();

        let symbol = Symbol::new("AAPL").unwrap();
        let onchain = FractionalShares::new(float!(10.0));
        let offchain = FractionalShares::new(float!(5.0));

        {
            let mut guard = inventory.write().await;
            *guard = std::mem::take(&mut *guard).with_equity(symbol.clone(), onchain, offchain);
        }

        let dto = inventory.read().await.to_dto();

        assert_eq!(dto.per_symbol.len(), 1);
        assert_eq!(dto.per_symbol[0].symbol, symbol);
        assert_eq!(dto.per_symbol[0].onchain_available, onchain);
        assert_eq!(dto.per_symbol[0].offchain_available, offchain);
    }

    #[tokio::test]
    async fn write_without_broadcast_stays_silent() {
        let (inventory, mut receiver) = create_broadcasting_inventory();

        let symbol = Symbol::new("AAPL").unwrap();
        {
            let mut guard = inventory.write_without_broadcast().await;
            guard.mark_offchain_order_pending(symbol.clone(), OffchainOrderId::new());
        }
        assert!(
            matches!(
                receiver.try_recv(),
                Err(broadcast::error::TryRecvError::Empty)
            ),
            "a gate-only mutation must not broadcast a dashboard snapshot"
        );

        // The broadcasting path still fires for observable mutations.
        drop(inventory.write().await);
        let statement = receiver.try_recv().unwrap();
        assert!(
            matches!(statement, Statement::InventorySnapshot(_)),
            "the broadcasting write path must emit a snapshot on drop"
        );
    }

    #[tokio::test]
    async fn dropping_write_guard_broadcasts_snapshot() {
        let (inventory, mut receiver) = create_broadcasting_inventory();

        let symbol = Symbol::new("TSLA").unwrap();
        let onchain = FractionalShares::new(float!(100.0));
        let offchain = FractionalShares::new(float!(50.0));

        {
            let mut guard = inventory.write().await;
            *guard = std::mem::take(&mut *guard).with_equity(symbol.clone(), onchain, offchain);
        }

        let msg = receiver.recv().await.unwrap();

        match msg {
            Statement::InventorySnapshot(snapshot) => {
                assert_eq!(snapshot.inventory.per_symbol.len(), 1);
                assert_eq!(snapshot.inventory.per_symbol[0].symbol, symbol);
                assert_eq!(snapshot.inventory.per_symbol[0].onchain_available, onchain);
                assert_eq!(
                    snapshot.inventory.per_symbol[0].offchain_available,
                    offchain
                );
            }
            other => panic!("expected Snapshot, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn each_write_broadcasts_independently() {
        let (inventory, mut receiver) = create_broadcasting_inventory();

        let aapl = Symbol::new("AAPL").unwrap();
        let tsla = Symbol::new("TSLA").unwrap();

        {
            let mut guard = inventory.write().await;
            *guard = std::mem::take(&mut *guard).with_equity(
                aapl.clone(),
                FractionalShares::new(float!(10.0)),
                FractionalShares::new(float!(5.0)),
            );
        }

        {
            let mut guard = inventory.write().await;
            *guard = std::mem::take(&mut *guard).with_equity(
                tsla.clone(),
                FractionalShares::new(float!(20.0)),
                FractionalShares::new(float!(15.0)),
            );
        }

        let first = receiver.recv().await.unwrap();
        let second = receiver.recv().await.unwrap();

        match first {
            Statement::InventorySnapshot(snapshot) => {
                assert_eq!(snapshot.inventory.per_symbol.len(), 1);
                assert_eq!(snapshot.inventory.per_symbol[0].symbol, aapl);
            }
            other => panic!("expected Snapshot, got {other:?}"),
        }

        match second {
            Statement::InventorySnapshot(snapshot) => {
                assert_eq!(snapshot.inventory.per_symbol.len(), 2);
            }
            other => panic!("expected Snapshot, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn write_completes_cleanly_with_no_receivers() {
        let (inventory, receiver) = create_broadcasting_inventory();
        drop(receiver);

        let symbol = Symbol::new("AAPL").unwrap();
        let onchain = FractionalShares::new(float!(10.0));
        let offchain = FractionalShares::new(float!(5.0));

        {
            let mut guard = inventory.write().await;
            *guard = std::mem::take(&mut *guard).with_equity(symbol.clone(), onchain, offchain);
        }

        let dto = inventory.read().await.to_dto();

        assert_eq!(dto.per_symbol.len(), 1);
        assert_eq!(dto.per_symbol[0].symbol, symbol);
        assert_eq!(dto.per_symbol[0].onchain_available, onchain);
        assert_eq!(dto.per_symbol[0].offchain_available, offchain);
    }
}
