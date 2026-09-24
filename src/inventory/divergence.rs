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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;

use st0x_evm::Chain;
use st0x_execution::{FractionalShares, Symbol};
use st0x_finance::Usdc;

use super::{BroadcastingInventory, InventoryScope};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub(crate) struct ReconciliationGeneration(u64);

impl ReconciliationGeneration {
    #[cfg(test)]
    pub(crate) const fn for_test(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug)]
struct ReconciliationRequest {
    generation: ReconciliationGeneration,
    requested_at: DateTime<Utc>,
    minimum_block: Option<u64>,
}

/// Inventory scopes with a detected but unresolved snapshot
/// divergence.
///
/// The inventory poller writes: it engages a symbol at a scope on the first
/// confirmed divergence there and releases it when a poll matches again or
/// an escalation verifiably healed the view. MarketMaking scopes include
/// their chain, so one chain healing cannot release another. Gating starts
/// at the first diverging poll because a transfer sized off a
/// diverged balance fails and marks the symbol busy, which freezes the
/// divergence counter. The cost on a transient mismatch is at most one
/// poll interval of delayed rebalancing.
///
/// The rebalancing trigger reads venue-agnostically: a mint, redemption or
/// bridge moves the balance at *both* venues, so a divergence at either
/// one makes the transfer unsafe to size. Detection stays scope-keyed
/// because the scopes diverge and heal independently -- a matching poll at
/// one scope must not lift suppression another venue or chain still needs.
#[derive(Debug, Default)]
pub(crate) struct InventoryDivergenceGate {
    symbols: RwLock<HashMap<Symbol, HashSet<InventoryScope>>>,
    /// Explicit venue snapshots required after inventory bookkeeping was
    /// deferred. These are separate from broker-divergence detection so a
    /// matching offchain poll cannot accidentally clear an onchain repair.
    pending_offchain_equity: RwLock<HashMap<Symbol, ReconciliationRequest>>,
    pending_onchain_equity: RwLock<HashMap<(Chain, Symbol), ReconciliationRequest>>,
    pending_onchain_cash: RwLock<HashMap<Chain, ReconciliationRequest>>,
    next_reconciliation_generation: AtomicU64,
    /// Scopes with a detected but unresolved cash divergence. A set of
    /// scopes, not symbols: each scope's cash balance is one number.
    /// While any scope is engaged, the USDC rebalancing trigger skips
    /// dispatch -- a bridge sized off a diverged cash balance moves the
    /// wrong amount and marks the venue busy, freezing the very counter
    /// that resolves the divergence.
    cash: RwLock<HashSet<InventoryScope>>,
}

impl InventoryDivergenceGate {
    pub(crate) fn engage(&self, scope: InventoryScope, symbol: &Symbol) {
        write_recovering(&self.symbols)
            .entry(symbol.clone())
            .or_default()
            .insert(scope);
    }

    pub(crate) fn release(&self, scope: InventoryScope, symbol: &Symbol) {
        let mut symbols = write_recovering(&self.symbols);
        let Some(scopes) = symbols.get_mut(symbol) else {
            return;
        };

        scopes.remove(&scope);
        if scopes.is_empty() {
            symbols.remove(symbol);
        }
    }

    /// Whether any scope holds an unresolved equity divergence for `symbol`.
    /// A symbol is present only while at least one scope is engaged, so
    /// membership is one lookup.
    pub(crate) fn is_engaged(&self, symbol: &Symbol) -> bool {
        read_recovering(&self.symbols).contains_key(symbol)
            || self.read_pending_offchain_equity().contains_key(symbol)
            || self
                .read_pending_onchain_equity()
                .keys()
                .any(|(_, pending_symbol)| pending_symbol == symbol)
    }

    pub(crate) fn engage_cash(&self, scope: InventoryScope) {
        write_recovering(&self.cash).insert(scope);
    }

    pub(crate) fn release_cash(&self, scope: InventoryScope) {
        write_recovering(&self.cash).remove(&scope);
    }

    /// Whether any scope holds an unresolved cash divergence.
    pub(crate) fn is_cash_engaged(&self) -> bool {
        !read_recovering(&self.cash).is_empty() || !self.read_pending_onchain_cash().is_empty()
    }

    pub(crate) fn request_offchain_equity_reconcile(
        &self,
        symbol: &Symbol,
    ) -> ReconciliationGeneration {
        let request = self.new_reconciliation_request(None);
        self.write_pending_offchain_equity()
            .insert(symbol.clone(), request);
        request.generation
    }

    #[cfg(test)]
    pub(crate) fn pending_offchain_equity_reconciles(&self) -> Vec<Symbol> {
        self.read_pending_offchain_equity()
            .keys()
            .cloned()
            .collect()
    }

    pub(crate) fn claim_pending_offchain_equity_reconciles(
        &self,
    ) -> Vec<(Symbol, ReconciliationGeneration)> {
        self.read_pending_offchain_equity()
            .iter()
            .map(|(symbol, request)| (symbol.clone(), request.generation))
            .collect()
    }

    pub(crate) fn accepts_offchain_equity_reconcile(
        &self,
        symbol: &Symbol,
        generation: ReconciliationGeneration,
        fetched_at: DateTime<Utc>,
    ) -> bool {
        self.read_pending_offchain_equity()
            .get(symbol)
            .is_some_and(|request| {
                request.generation == generation && fetched_at > request.requested_at
            })
    }
    /// Symbols owned by an explicit reconciliation request must never be
    /// mutated by the ordinary snapshot emitted from the same poll. The
    /// generation-bound event that follows is the only event allowed to apply
    /// and resolve that request; otherwise the ordinary event can advance the
    /// watermark and make its paired reconcile event reject itself.
    pub(crate) fn protected_offchain_equity_symbols(&self) -> BTreeSet<Symbol> {
        self.read_pending_offchain_equity()
            .keys()
            .cloned()
            .collect()
    }

    pub(crate) fn resolve_offchain_equity_reconcile(
        &self,
        symbol: &Symbol,
        generation: ReconciliationGeneration,
    ) {
        let mut pending = self.write_pending_offchain_equity();
        if pending
            .get(symbol)
            .is_some_and(|request| request.generation == generation)
        {
            pending.remove(symbol);
        }
    }

    pub(crate) fn request_onchain_equity_reconcile(
        &self,
        chain: Chain,
        symbol: &Symbol,
        minimum_block: Option<u64>,
    ) -> ReconciliationGeneration {
        let request = self.new_reconciliation_request(minimum_block);
        self.write_pending_onchain_equity()
            .insert((chain, symbol.clone()), request);
        request.generation
    }

    pub(crate) fn claim_pending_onchain_equity_reconciles(
        &self,
        chain: Chain,
    ) -> BTreeMap<Symbol, ReconciliationGeneration> {
        self.read_pending_onchain_equity()
            .iter()
            .filter(|((pending_chain, _), _)| *pending_chain == chain)
            .map(|((_, symbol), request)| (symbol.clone(), request.generation))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn has_pending_onchain_equity_reconcile(&self, chain: Chain) -> bool {
        self.read_pending_onchain_equity()
            .keys()
            .any(|(pending_chain, _)| *pending_chain == chain)
    }

    pub(crate) fn accepts_onchain_equity_reconcile(
        &self,
        chain: Chain,
        symbol: &Symbol,
        generation: ReconciliationGeneration,
        fetched_at: DateTime<Utc>,
        block_number: Option<u64>,
    ) -> bool {
        self.read_pending_onchain_equity()
            .get(&(chain, symbol.clone()))
            .is_some_and(|request| {
                Self::request_accepts_snapshot(request, generation, fetched_at, block_number)
            })
    }
    pub(crate) fn protected_onchain_equity_symbols(
        &self,
        chain: Chain,
        fetched_at: DateTime<Utc>,
        block_number: Option<u64>,
    ) -> BTreeSet<Symbol> {
        self.read_pending_onchain_equity()
            .iter()
            .filter(|((pending_chain, _), request)| {
                *pending_chain == chain
                    && !Self::request_is_covered_by_snapshot(request, fetched_at, block_number)
            })
            .map(|((_, symbol), _)| symbol.clone())
            .collect()
    }

    pub(crate) fn resolve_onchain_equity_reconcile(
        &self,
        chain: Chain,
        symbol: &Symbol,
        generation: ReconciliationGeneration,
    ) {
        let key = (chain, symbol.clone());
        let mut pending = self.write_pending_onchain_equity();
        if pending
            .get(&key)
            .is_some_and(|request| request.generation == generation)
        {
            pending.remove(&key);
        }
    }

    pub(crate) fn request_onchain_cash_reconcile(
        &self,
        chain: Chain,
        minimum_block: Option<u64>,
    ) -> ReconciliationGeneration {
        let request = self.new_reconciliation_request(minimum_block);
        self.write_pending_onchain_cash().insert(chain, request);
        request.generation
    }

    pub(crate) fn claim_pending_onchain_cash_reconcile(
        &self,
        chain: Chain,
    ) -> Option<ReconciliationGeneration> {
        self.read_pending_onchain_cash()
            .get(&chain)
            .map(|request| request.generation)
    }

    pub(crate) fn accepts_onchain_cash_reconcile(
        &self,
        chain: Chain,
        generation: ReconciliationGeneration,
        fetched_at: DateTime<Utc>,
        block_number: Option<u64>,
    ) -> bool {
        self.read_pending_onchain_cash()
            .get(&chain)
            .is_some_and(|request| {
                Self::request_accepts_snapshot(request, generation, fetched_at, block_number)
            })
    }
    pub(crate) fn protects_onchain_cash_snapshot(
        &self,
        chain: Chain,
        fetched_at: DateTime<Utc>,
        block_number: Option<u64>,
    ) -> bool {
        self.read_pending_onchain_cash()
            .get(&chain)
            .is_some_and(|request| {
                !Self::request_is_covered_by_snapshot(request, fetched_at, block_number)
            })
    }

    pub(crate) fn resolve_onchain_cash_reconcile(
        &self,
        chain: Chain,
        generation: ReconciliationGeneration,
    ) {
        let mut pending = self.write_pending_onchain_cash();
        if pending
            .get(&chain)
            .is_some_and(|request| request.generation == generation)
        {
            pending.remove(&chain);
        }
    }

    fn new_reconciliation_request(&self, minimum_block: Option<u64>) -> ReconciliationRequest {
        let generation = self
            .next_reconciliation_generation
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_add(1);
        ReconciliationRequest {
            generation: ReconciliationGeneration(generation),
            requested_at: Utc::now(),
            minimum_block,
        }
    }

    fn request_accepts_snapshot(
        request: &ReconciliationRequest,
        generation: ReconciliationGeneration,
        fetched_at: DateTime<Utc>,
        block_number: Option<u64>,
    ) -> bool {
        request.generation == generation
            && Self::request_is_covered_by_snapshot(request, fetched_at, block_number)
    }

    fn request_is_covered_by_snapshot(
        request: &ReconciliationRequest,
        fetched_at: DateTime<Utc>,
        block_number: Option<u64>,
    ) -> bool {
        request.minimum_block.map_or_else(
            || fetched_at > request.requested_at,
            |minimum_block| block_number.is_some_and(|block_number| block_number >= minimum_block),
        )
    }

    fn read_pending_offchain_equity(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, HashMap<Symbol, ReconciliationRequest>> {
        self.pending_offchain_equity
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write_pending_offchain_equity(
        &self,
    ) -> std::sync::RwLockWriteGuard<'_, HashMap<Symbol, ReconciliationRequest>> {
        self.pending_offchain_equity
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn read_pending_onchain_equity(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, HashMap<(Chain, Symbol), ReconciliationRequest>> {
        self.pending_onchain_equity
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write_pending_onchain_equity(
        &self,
    ) -> std::sync::RwLockWriteGuard<'_, HashMap<(Chain, Symbol), ReconciliationRequest>> {
        self.pending_onchain_equity
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn read_pending_onchain_cash(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, HashMap<Chain, ReconciliationRequest>> {
        self.pending_onchain_cash
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write_pending_onchain_cash(
        &self,
    ) -> std::sync::RwLockWriteGuard<'_, HashMap<Chain, ReconciliationRequest>> {
        self.pending_onchain_cash
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
    use st0x_evm::Chain;

    use super::*;

    /// Inventory scopes diverge and heal independently, so one release must
    /// not lift suppression another venue or chain still needs.
    #[test]
    fn onchain_and_offchain_divergence_gates_release_independently() {
        let spym = Symbol::new("SPYM").unwrap();
        let gate = InventoryDivergenceGate::default();

        gate.engage(InventoryScope::Hedging, &spym);
        gate.engage(InventoryScope::MarketMaking(Chain::Base), &spym);
        gate.engage(InventoryScope::MarketMaking(Chain::Robinhood), &spym);
        assert!(gate.is_engaged(&spym));

        gate.release(InventoryScope::Hedging, &spym);
        assert!(
            gate.is_engaged(&spym),
            "the still-diverging MarketMaking venue must keep dispatch suppressed"
        );

        gate.release(InventoryScope::MarketMaking(Chain::Base), &spym);
        assert!(
            gate.is_engaged(&spym),
            "releasing Base must not release Robinhood's equity divergence"
        );

        gate.release(InventoryScope::MarketMaking(Chain::Robinhood), &spym);
        assert!(
            !gate.is_engaged(&spym),
            "releasing the last engaged venue lifts suppression"
        );

        gate.engage_cash(InventoryScope::Hedging);
        gate.engage_cash(InventoryScope::MarketMaking(Chain::Base));
        gate.engage_cash(InventoryScope::MarketMaking(Chain::Robinhood));
        gate.release_cash(InventoryScope::Hedging);
        assert!(
            gate.is_cash_engaged(),
            "the still-diverging MarketMaking cash balance must keep dispatch suppressed"
        );

        gate.release_cash(InventoryScope::MarketMaking(Chain::Base));
        assert!(
            gate.is_cash_engaged(),
            "releasing Base must not release Robinhood's cash divergence"
        );

        gate.release_cash(InventoryScope::MarketMaking(Chain::Robinhood));
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
        gate.engage(InventoryScope::Hedging, &spym);

        std::thread::scope(|scope| {
            let panic_payload = scope
                .spawn(|| {
                    let _guard = gate.symbols.write().unwrap();
                    panic!("poison divergence gate for test");
                })
                .join()
                .expect_err("the test writer must poison the divergence gate");
            assert_eq!(
                panic_payload.downcast_ref::<&str>().copied(),
                Some("poison divergence gate for test")
            );
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
