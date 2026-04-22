//! Thread-safe inventory wrapper that broadcasts snapshots on mutation.
//!
//! [`BroadcastingInventory`] wraps an [`InventoryView`] behind an [`RwLock`]
//! and holds a [`broadcast::Sender`]. When a [`BroadcastingWriteGuard`] is
//! dropped, it automatically sends a [`Statement::InventorySnapshot`] with the
//! current inventory state to all connected dashboard clients.

use chrono::Utc;
use std::ops::{Deref, DerefMut};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, broadcast};
use tracing::warn;

use st0x_dto::{InventorySnapshot, Statement};

use super::InventoryView;

/// Thread-safe inventory that broadcasts a snapshot to dashboard clients
/// whenever the write guard is released.
pub(crate) struct BroadcastingInventory {
    view: RwLock<InventoryView>,
    sender: broadcast::Sender<Statement>,
}

impl BroadcastingInventory {
    pub(crate) fn new(view: InventoryView, sender: broadcast::Sender<Statement>) -> Self {
        Self {
            view: RwLock::new(view),
            sender,
        }
    }

    pub(crate) async fn read(&self) -> RwLockReadGuard<'_, InventoryView> {
        self.view.read().await
    }

    pub(crate) async fn write(&self) -> BroadcastingWriteGuard<'_> {
        BroadcastingWriteGuard {
            guard: self.view.write().await,
            sender: &self.sender,
        }
    }
}

/// Write guard that broadcasts the current inventory snapshot on drop.
pub(crate) struct BroadcastingWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, InventoryView>,
    sender: &'a broadcast::Sender<Statement>,
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
        let snapshot = InventorySnapshot {
            inventory: self.guard.to_dto(),
            fetched_at: Utc::now(),
        };

        if self.sender.receiver_count() == 0 {
            return;
        }

        if let Err(error) = self
            .sender
            .send(Statement::InventorySnapshot(Box::new(snapshot)))
        {
            warn!("Failed to broadcast inventory snapshot: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use st0x_execution::{FractionalShares, Symbol};
    use st0x_float_macro::float;

    use super::*;

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
