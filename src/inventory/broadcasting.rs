//! Thread-safe inventory wrapper.
//!
//! [`BroadcastingInventory`] wraps an [`InventoryView`] behind an [`RwLock`].

use std::ops::{Deref, DerefMut};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::InventoryView;

/// Thread-safe inventory wrapper.
pub(crate) struct BroadcastingInventory {
    view: RwLock<InventoryView>,
}

impl BroadcastingInventory {
    pub(crate) fn new(view: InventoryView) -> Self {
        Self {
            view: RwLock::new(view),
        }
    }

    /// Alias for [`Self::new`] — retained for call-site compatibility during
    /// the broadcasting removal migration.
    pub(crate) fn new_without_broadcast(view: InventoryView) -> Self {
        Self::new(view)
    }

    pub(crate) async fn read(&self) -> RwLockReadGuard<'_, InventoryView> {
        self.view.read().await
    }

    pub(crate) async fn write(&self) -> BroadcastingWriteGuard<'_> {
        BroadcastingWriteGuard {
            guard: self.view.write().await,
        }
    }
}

/// Write guard for [`BroadcastingInventory`].
pub(crate) struct BroadcastingWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, InventoryView>,
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

    #[tokio::test]
    async fn read_returns_default_inventory() {
        let inventory = BroadcastingInventory::new(InventoryView::default());
        let dto = inventory.read().await.to_dto();

        assert!(dto.per_symbol.is_empty());
    }

    #[tokio::test]
    async fn write_guard_allows_mutation() {
        let inventory = BroadcastingInventory::new(InventoryView::default());

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
