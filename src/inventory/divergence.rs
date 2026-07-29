//! Recovery for a view that persistently diverges from the broker.
//!
//! Three mechanisms can leave the view holding a balance the broker does
//! not report, with no ordinary poll able to correct it: the snapshot
//! aggregate emits no event for an unchanged poll, the view's staleness
//! guards skip the events that do arrive, and failed transfer cleanups
//! stamp a fresh `last_rebalancing` that arms those guards again. The
//! poller detects this state by comparing each fetched broker position
//! against the view's Hedging balance across consecutive polls. Once the
//! configured threshold is reached it escalates a forced reconcile
//! through the `InventorySnapshot` aggregate.

use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::{Arc, RwLock};
use tracing::warn;

use st0x_execution::{FractionalShares, Symbol};

use super::BroadcastingInventory;

/// Symbols with a detected but unresolved offchain snapshot divergence.
///
/// The inventory poller writes: it engages a symbol on the first confirmed
/// divergence and releases it when a poll matches again or an escalation
/// verifiably healed the view. The equity rebalancing trigger reads: it
/// skips firing mints and redemptions for engaged symbols. Gating starts
/// at the first diverging poll because a transfer sized off a diverged
/// balance fails at the broker and marks the symbol busy, which freezes
/// the divergence counter. The cost on a transient mismatch is at most
/// one poll interval of delayed rebalancing.
#[derive(Debug, Default)]
pub(crate) struct InventoryDivergenceGate {
    symbols: RwLock<HashSet<Symbol>>,
}

impl InventoryDivergenceGate {
    pub(crate) fn engage(&self, symbol: &Symbol) {
        self.write_symbols().insert(symbol.clone());
    }

    pub(crate) fn release(&self, symbol: &Symbol) {
        self.write_symbols().remove(symbol);
    }

    pub(crate) fn is_engaged(&self, symbol: &Symbol) -> bool {
        self.read_symbols().contains(symbol)
    }

    fn read_symbols(&self) -> std::sync::RwLockReadGuard<'_, HashSet<Symbol>> {
        self.symbols.read().unwrap_or_else(|poisoned| {
            warn!(
                target: "inventory",
                "Divergence gate lock was poisoned; recovering state"
            );
            poisoned.into_inner()
        })
    }

    fn write_symbols(&self) -> std::sync::RwLockWriteGuard<'_, HashSet<Symbol>> {
        self.symbols.write().unwrap_or_else(|poisoned| {
            warn!(
                target: "inventory",
                "Divergence gate lock was poisoned; recovering state"
            );
            poisoned.into_inner()
        })
    }
}

/// Everything the poller needs to detect and escalate divergences: a read
/// handle on the live view, the confirmation threshold, and the transfer
/// suppression gate shared with the trigger.
pub(crate) struct InventoryDivergenceRecoveryCtx {
    pub(crate) inventory: Arc<BroadcastingInventory>,
    pub(crate) threshold: NonZeroU32,
    pub(crate) gate: Arc<InventoryDivergenceGate>,
}

/// Witness for forcing a broker snapshot over the view's balance.
///
/// [`Inventory::force_on_snapshot`] takes the triggering error as a witness
/// to prevent blind usage; this type records what the poller observed
/// before the escalation fired.
///
/// [`Inventory::force_on_snapshot`]: super::Inventory::force_on_snapshot
#[derive(Debug, thiserror::Error)]
#[error(
    "offchain snapshot diverged from the inventory view for {polls} \
     consecutive polls: symbol {symbol}, ledger {ledger_value:?}, \
     broker {broker_value}"
)]
pub(crate) struct PersistentBrokerDivergence {
    pub(crate) symbol: Symbol,
    /// Available balance the view held at the Hedging venue; `None` when
    /// the venue was never initialized.
    pub(crate) ledger_value: Option<FractionalShares>,
    pub(crate) broker_value: FractionalShares,
    pub(crate) polls: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbol(ticker: &str) -> Symbol {
        Symbol::new(ticker).unwrap()
    }

    #[test]
    fn gate_engages_and_releases_symbols_independently() {
        let gate = InventoryDivergenceGate::default();
        let aapl = symbol("AAPL");
        let tsla = symbol("TSLA");

        gate.engage(&aapl);

        assert!(gate.is_engaged(&aapl));
        assert!(!gate.is_engaged(&tsla));

        gate.release(&aapl);

        assert!(!gate.is_engaged(&aapl));
    }

    #[test]
    fn gate_engage_is_idempotent() {
        let gate = InventoryDivergenceGate::default();
        let aapl = symbol("AAPL");

        gate.engage(&aapl);
        gate.engage(&aapl);
        gate.release(&aapl);

        assert!(!gate.is_engaged(&aapl));
    }
}
