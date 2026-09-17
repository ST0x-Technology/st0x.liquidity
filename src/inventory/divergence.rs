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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;

use st0x_evm::Chain;
use st0x_execution::{FractionalShares, Symbol};
use st0x_finance::Usdc;

use super::BroadcastingInventory;
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
    /// Explicit venue snapshots required after inventory bookkeeping was
    /// deferred. These are separate from broker-divergence detection so a
    /// matching offchain poll cannot accidentally clear an onchain repair.
    pending_offchain_equity: RwLock<HashMap<Symbol, ReconciliationRequest>>,
    pending_onchain_equity: RwLock<HashMap<(Chain, Symbol), ReconciliationRequest>>,
    pending_onchain_cash: RwLock<HashMap<Chain, ReconciliationRequest>>,
    next_reconciliation_generation: AtomicU64,
    /// Venue-level flag for a detected but unresolved `OffchainUsd`
    /// divergence. One flag, not a set: the Hedging cash balance is one
    /// number.
    cash: AtomicBool,
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
            || self.read_pending_offchain_equity().contains_key(symbol)
            || self
                .read_pending_onchain_equity()
                .keys()
                .any(|(_, pending_symbol)| pending_symbol == symbol)
    }

    pub(crate) fn engage_cash(&self) {
        self.cash.store(true, Ordering::SeqCst);
    }

    pub(crate) fn release_cash(&self) {
        self.cash.store(false, Ordering::SeqCst);
    }

    pub(crate) fn is_cash_engaged(&self) -> bool {
        self.cash.load(Ordering::SeqCst) || !self.read_pending_onchain_cash().is_empty()
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
    pub(crate) fn protected_offchain_equity_symbols(
        &self,
        fetched_at: DateTime<Utc>,
    ) -> BTreeSet<Symbol> {
        self.read_pending_offchain_equity()
            .iter()
            .filter(|(_, request)| fetched_at <= request.requested_at)
            .map(|(symbol, _)| symbol.clone())
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
