//! Broadcasting wrapper for inventory that sends WebSocket updates on mutation.

use std::ops::{Deref, DerefMut};

use chrono::Utc;
use tokio::sync::broadcast;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::trace;

use st0x_dto::{InventorySnapshot, ServerMessage};

use super::InventoryView;

/// Wraps `RwLock<InventoryView>` and broadcasts an `InventoryUpdate`
/// message whenever a write guard is dropped.
pub(crate) struct BroadcastingInventory {
    view: RwLock<InventoryView>,
    sender: broadcast::Sender<ServerMessage>,
}

impl BroadcastingInventory {
    pub(crate) fn new(view: InventoryView, sender: broadcast::Sender<ServerMessage>) -> Self {
        Self {
            view: RwLock::new(view),
            sender,
        }
    }

    pub(crate) async fn read(&self) -> RwLockReadGuard<'_, InventoryView> {
        self.view.read().await
    }

    /// Acquire a write guard that broadcasts the updated inventory on drop.
    pub(crate) async fn write(&self) -> BroadcastingWriteGuard<'_> {
        BroadcastingWriteGuard {
            guard: self.view.write().await,
            sender: &self.sender,
        }
    }
}

/// Write guard that broadcasts an `InventoryUpdate` when dropped.
pub(crate) struct BroadcastingWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, InventoryView>,
    sender: &'a broadcast::Sender<ServerMessage>,
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
        let message = ServerMessage::InventoryUpdate(Box::new(snapshot));

        match self.sender.send(message) {
            Ok(receiver_count) => {
                trace!(receiver_count, "Broadcast inventory update");
            }
            Err(_) => {
                trace!("No active subscribers for inventory update");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_broadcasting_inventory() -> BroadcastingInventory {
        let (sender, _) = broadcast::channel(16);
        BroadcastingInventory::new(InventoryView::default(), sender)
    }

    #[tokio::test]
    async fn read_returns_default_inventory() {
        let inventory = create_broadcasting_inventory();
        let dto = inventory.read().await.to_dto();

        assert!(dto.per_symbol.is_empty());
    }

    #[tokio::test]
    async fn write_guard_broadcasts_on_drop() {
        let (sender, mut receiver) = broadcast::channel(16);
        let inventory = BroadcastingInventory::new(InventoryView::default(), sender);

        {
            let _guard = inventory.write().await;
        }

        let message = receiver.recv().await.unwrap();

        assert!(matches!(message, ServerMessage::InventoryUpdate(_)));
    }

    #[tokio::test]
    async fn write_guard_allows_mutation_and_broadcasts() {
        let (sender, mut receiver) = broadcast::channel(16);
        let inventory = BroadcastingInventory::new(InventoryView::default(), sender);

        {
            let _guard = inventory.write().await;
        }

        let message = receiver.recv().await.unwrap();

        assert!(matches!(message, ServerMessage::InventoryUpdate(_)));
    }
}
