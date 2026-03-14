//! Thread-safe inventory wrapper with read/write locking.

use std::ops::{Deref, DerefMut};

use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::InventoryView;

/// Wraps `RwLock<InventoryView>` for concurrent access to inventory state.
pub(crate) struct BroadcastingInventory {
    view: RwLock<InventoryView>,
}

impl BroadcastingInventory {
    pub(crate) fn new(view: InventoryView) -> Self {
        Self {
            view: RwLock::new(view),
        }
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
    use rust_decimal_macros::dec;

    use st0x_execution::{FractionalShares, Symbol};

    use super::*;

    fn create_broadcasting_inventory() -> BroadcastingInventory {
        BroadcastingInventory::new(InventoryView::default())
    }

    #[tokio::test]
    async fn read_returns_default_inventory() {
        let inventory = create_broadcasting_inventory();
        let dto = inventory.read().await.to_dto();

        assert!(dto.per_symbol.is_empty());
    }

    #[tokio::test]
    async fn write_guard_allows_mutation() {
        let inventory = create_broadcasting_inventory();

        let symbol = Symbol::new("AAPL").unwrap();
        let onchain = FractionalShares::new(dec!(10.0));
        let offchain = FractionalShares::new(dec!(5.0));

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
