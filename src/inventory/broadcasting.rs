//! Thread-safe inventory wrapper with read/write locking.
//!
//! When a [`BroadcastingWriteGuard`] is dropped, the current inventory state
//! is serialized to a DTO and broadcast to all connected WebSocket clients.

use std::ops::{Deref, DerefMut};

use tokio::sync::broadcast;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::warn;

use st0x_dto::{Concern, Statement};

use super::InventoryView;

/// Wraps `RwLock<InventoryView>` for concurrent access to inventory state.
///
/// When a write guard is dropped, the updated inventory is automatically
/// broadcast to all connected dashboard clients via [`Statement`].
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

    /// Creates an inventory that discards broadcasts. For tests that don't
    /// need to observe WebSocket messages.
    #[cfg(test)]
    pub(crate) fn new_without_broadcast(view: InventoryView) -> Self {
        let (sender, _) = broadcast::channel(1);

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

pub(crate) struct BroadcastingWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, InventoryView>,
    sender: &'a broadcast::Sender<Statement>,
}

impl Drop for BroadcastingWriteGuard<'_> {
    fn drop(&mut self) {
        let statement = Statement {
            id: "inventory".to_string(),
            statement: Concern::Inventory,
        };

        if let Err(error) = self.sender.send(statement) {
            warn!("Failed to broadcast inventory update (no receivers): {error}");
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

#[cfg(test)]
mod tests {
    use st0x_execution::{FractionalShares, Symbol};
    use st0x_float_macro::float;

    use super::*;

    fn create_broadcasting_inventory() -> (BroadcastingInventory, broadcast::Receiver<Statement>) {
        let (sender, receiver) = broadcast::channel(16);
        let inventory = BroadcastingInventory::new(InventoryView::default(), sender);
        (inventory, receiver)
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
    async fn write_guard_broadcasts_inventory_update_on_drop() {
        let (inventory, mut receiver) = create_broadcasting_inventory();

        let symbol = Symbol::new("TSLA").unwrap();
        let onchain = FractionalShares::new(float!(25.0));
        let offchain = FractionalShares::new(float!(15.0));

        {
            let mut guard = inventory.write().await;
            *guard = std::mem::take(&mut *guard).with_equity(symbol.clone(), onchain, offchain);
        }

        let msg = receiver.recv().await.expect("should receive broadcast");

        assert_eq!(msg.id, "inventory");
        assert!(matches!(msg.statement, Concern::Inventory));
    }

    #[tokio::test]
    async fn each_write_produces_a_broadcast() {
        let (inventory, mut receiver) = create_broadcasting_inventory();

        {
            let mut guard = inventory.write().await;
            let symbol = Symbol::new("AAPL").unwrap();
            *guard = std::mem::take(&mut *guard).with_equity(
                symbol,
                FractionalShares::new(float!(1.0)),
                FractionalShares::ZERO,
            );
        }

        {
            let mut guard = inventory.write().await;
            let symbol = Symbol::new("TSLA").unwrap();
            *guard = std::mem::take(&mut *guard).with_equity(
                symbol,
                FractionalShares::new(float!(2.0)),
                FractionalShares::ZERO,
            );
        }

        let msg1 = receiver.recv().await.expect("first broadcast");
        let msg2 = receiver.recv().await.expect("second broadcast");

        assert_eq!(msg1.id, "inventory");
        assert!(matches!(msg1.statement, Concern::Inventory));

        assert_eq!(msg2.id, "inventory");
        assert!(matches!(msg2.statement, Concern::Inventory));
    }
}
