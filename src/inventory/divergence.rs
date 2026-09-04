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
use st0x_finance::Usdc;

use super::{BroadcastingInventory, Venue};

/// Venue-keyed symbols and venues with a detected but unresolved snapshot
/// divergence.
///
/// The inventory poller writes: it engages a `(venue, symbol)` pair on the
/// first confirmed divergence at that venue and releases it when a poll
/// matches again or an escalation verifiably healed the view. Gating
/// starts at the first diverging poll because a transfer sized off a
/// diverged balance fails and marks the symbol busy, which freezes the
/// divergence counter. The cost on a transient mismatch is at most one
/// poll interval of delayed rebalancing.
///
/// The rebalancing trigger reads venue-agnostically: a mint, redemption or
/// bridge moves the balance at *both* venues, so a divergence at either
/// one makes the transfer unsafe to size. Detection stays venue-keyed
/// because the venues diverge and heal independently -- a matching poll at
/// one venue must not lift suppression the other venue still needs.
#[derive(Debug, Default)]
pub(crate) struct InventoryDivergenceGate {
    symbols: RwLock<HashSet<(Venue, Symbol)>>,
    /// Venues with a detected but unresolved cash divergence. A set of
    /// venues, not a set of symbols: a venue's cash balance is one number.
    /// While any venue is engaged, the USDC rebalancing trigger skips
    /// dispatch -- a bridge sized off a diverged cash balance moves the
    /// wrong amount and marks the venue busy, freezing the very counter
    /// that resolves the divergence.
    cash: RwLock<HashSet<Venue>>,
}

impl InventoryDivergenceGate {
    pub(crate) fn engage(&self, venue: Venue, symbol: &Symbol) {
        write_recovering(&self.symbols).insert((venue, symbol.clone()));
    }

    pub(crate) fn release(&self, venue: Venue, symbol: &Symbol) {
        write_recovering(&self.symbols).remove(&(venue, symbol.clone()));
    }

    /// Whether any venue holds an unresolved equity divergence for `symbol`.
    pub(crate) fn is_engaged(&self, symbol: &Symbol) -> bool {
        read_recovering(&self.symbols)
            .iter()
            .any(|(_, engaged)| engaged == symbol)
    }

    pub(crate) fn engage_cash(&self, venue: Venue) {
        write_recovering(&self.cash).insert(venue);
    }

    pub(crate) fn release_cash(&self, venue: Venue) {
        write_recovering(&self.cash).remove(&venue);
    }

    /// Whether any venue holds an unresolved cash divergence.
    pub(crate) fn is_cash_engaged(&self) -> bool {
        !read_recovering(&self.cash).is_empty()
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
/// before the escalation fired. Only `Debug` is needed: the force path
/// logs the witness with debug formatting and never propagates it.
///
/// [`Inventory::force_on_snapshot`]: super::Inventory::force_on_snapshot
pub(crate) struct PersistentBrokerDivergence {
    pub(crate) symbol: Symbol,
    /// Available balance the view held at the Hedging venue; `None` when
    /// the venue was never initialized.
    pub(crate) ledger_value: Option<FractionalShares>,
    pub(crate) broker_value: FractionalShares,
    pub(crate) polls: u32,
}

// Hand-written: the fields are read only through `Debug` logging, and a
// derived impl is exempt from liveness analysis, so `derive(Debug)` would
// flag every field as dead code.
impl std::fmt::Debug for PersistentBrokerDivergence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            symbol,
            ledger_value,
            broker_value,
            polls,
        } = self;

        formatter
            .debug_struct("PersistentBrokerDivergence")
            .field("symbol", symbol)
            .field("ledger_value", ledger_value)
            .field("broker_value", broker_value)
            .field("polls", polls)
            .finish()
    }
}

/// The venue-level cash twin of [`PersistentBrokerDivergence`]: witness for
/// forcing the broker's available cash over the view's Hedging USDC.
pub(crate) struct PersistentBrokerCashDivergence {
    /// Hedging USDC the view held; `None` when the venue was never
    /// initialized.
    pub(crate) ledger_usdc: Option<Usdc>,
    pub(crate) broker_usd_cents: i64,
    pub(crate) polls: u32,
}

// Hand-written for the same reason as `PersistentBrokerDivergence`.
impl std::fmt::Debug for PersistentBrokerCashDivergence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            ledger_usdc,
            broker_usd_cents,
            polls,
        } = self;

        formatter
            .debug_struct("PersistentBrokerCashDivergence")
            .field("ledger_usdc", ledger_usdc)
            .field("broker_usd_cents", broker_usd_cents)
            .field("polls", polls)
            .finish()
    }
}

/// Read a gate set, recovering the contents a panicking writer poisoned.
/// Suppression must stay readable: the sets are plain memberships that a
/// panic cannot leave half-written, and refusing to read them would let
/// dispatch fire against a balance known to be diverged.
fn read_recovering<Contents>(lock: &RwLock<Contents>) -> std::sync::RwLockReadGuard<'_, Contents> {
    lock.read().unwrap_or_else(|poisoned| {
        warn!(
            target: "inventory",
            "Divergence gate lock was poisoned; recovering state"
        );
        poisoned.into_inner()
    })
}

/// The write twin of [`read_recovering`], recovering for the same reason.
fn write_recovering<Contents>(
    lock: &RwLock<Contents>,
) -> std::sync::RwLockWriteGuard<'_, Contents> {
    lock.write().unwrap_or_else(|poisoned| {
        warn!(
            target: "inventory",
            "Divergence gate lock was poisoned; recovering state"
        );
        poisoned.into_inner()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The venues diverge and heal independently, so a release at one venue
    /// must not lift suppression the other venue still needs.
    #[test]
    fn onchain_and_offchain_divergence_gates_release_independently() {
        let spym = Symbol::new("SPYM").unwrap();
        let gate = InventoryDivergenceGate::default();

        gate.engage(Venue::Hedging, &spym);
        gate.engage(Venue::MarketMaking, &spym);
        assert!(gate.is_engaged(&spym));

        gate.release(Venue::Hedging, &spym);
        assert!(
            gate.is_engaged(&spym),
            "the still-diverging MarketMaking venue must keep dispatch suppressed"
        );

        gate.release(Venue::MarketMaking, &spym);
        assert!(
            !gate.is_engaged(&spym),
            "releasing the last engaged venue lifts suppression"
        );

        gate.engage_cash(Venue::Hedging);
        gate.engage_cash(Venue::MarketMaking);
        gate.release_cash(Venue::Hedging);
        assert!(
            gate.is_cash_engaged(),
            "the still-diverging MarketMaking cash balance must keep dispatch suppressed"
        );

        gate.release_cash(Venue::MarketMaking);
        assert!(
            !gate.is_cash_engaged(),
            "releasing the last engaged venue lifts cash suppression"
        );
    }

    #[test]
    #[tracing_test::traced_test]
    fn engaged_symbol_remains_readable_after_writer_panic() {
        let spym = Symbol::new("SPYM").unwrap();
        let gate = InventoryDivergenceGate::default();
        gate.engage(Venue::Hedging, &spym);

        std::thread::scope(|scope| {
            let panic_result = scope
                .spawn(|| {
                    let _guard = gate.symbols.write().unwrap();
                    panic!("poison divergence gate for test");
                })
                .join();
            assert!(panic_result.is_err());
        });

        assert!(
            gate.is_engaged(&spym),
            "poison recovery must preserve the suppression membership"
        );
        assert!(logs_contain(
            "Divergence gate lock was poisoned; recovering state"
        ));
    }
}
