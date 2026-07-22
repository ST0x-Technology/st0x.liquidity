//! Tracks nonces this wallet itself has assigned to transactions it has
//! broadcast but not yet seen confirmed or proven dropped.
//!
//! [`ResettableNonceManager`](crate::nonce::ResettableNonceManager) caches
//! the *next* nonce to assign; it says nothing about which nonces are
//! currently occupied by transactions this wallet is still waiting on.
//! [`InFlightNonces`] fills that gap: `submit::send_with_recovery` records
//! an entry the moment a submission is accepted onto the node, and
//! `await_receipt` releases it once the wait resolves definitively (mined,
//! or proven dropped from both the receipt lookup and the mempool). This
//! lets a later "replacement transaction underpriced" rejection be checked
//! against direct proof of ownership instead of an inferred heuristic.
//!
//! A `HashSet<TxHash>` is kept per nonce, not a single hash, because a
//! fee-bumped replacement resubmits the *same* nonce under a *new* hash --
//! only one of the two can ever mine, but both must be recognized as
//! "ours" while either is outstanding.

use alloy::primitives::{Address, TxHash};
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// The answer to "does this wallet own the transaction currently occupying
/// `nonce`?", as far as this process's own bookkeeping can tell.
///
/// Not yet consulted by any production code path in this crate: PR1 only
/// wires up recording and releasing entries. A follow-up change replaces
/// `submit.rs`'s inferred nonce-ownership heuristics with lookups against
/// this, which is when this type and [`InFlightNonces::ownership`] gain a
/// production caller. Until then, only this module's own tests construct it,
/// hence `#[cfg(test)]`.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NonceOwnership {
    /// This nonce currently has an entry: it's a nonce this wallet itself
    /// assigned to a transaction it has not yet seen confirmed or proven
    /// dropped.
    Ours,
    /// This address has at least one recorded entry (so the tracker is
    /// primed for it), and this specific nonce is not among them.
    NotOurs,
    /// No entry has ever been recorded for this address in this process's
    /// lifetime -- cold start or just after a restart. The tracker has no
    /// opinion; the caller must fall back to pre-existing heuristics.
    Unknown,
}

/// Clone-able handle around the wallet's own record of in-flight nonces.
///
/// Same sharing model as `ResettableNonceManager`: cloned handles share the
/// same underlying map, so every clone of a wallet contends over one
/// record. Never removes an address key once recorded -- see
/// [`ownership`](Self::ownership)'s doc comment for why `Unknown` and
/// `NotOurs` must stay distinguishable even after every in-flight entry for
/// an address has been released.
#[derive(Clone, Debug, Default)]
pub(crate) struct InFlightNonces {
    nonces: Arc<DashMap<Address, HashMap<u64, HashSet<TxHash>>>>,
}

impl InFlightNonces {
    /// Records that `tx_hash` was just broadcast at `nonce` for `address`.
    ///
    /// Callers must only call this after a successful broadcast, never
    /// speculatively before one: the specific attempt that was rejected
    /// "replacement transaction underpriced" or "nonce too low" was, by
    /// definition, never accepted onto the node, and recording it would
    /// make the ownership check circular (the just-rejected attempt would
    /// look like proof it owns the nonce that rejected it).
    pub(crate) fn record(&self, address: Address, nonce: u64, tx_hash: TxHash) {
        self.nonces
            .entry(address)
            .or_default()
            .entry(nonce)
            .or_default()
            .insert(tx_hash);
    }

    /// Removes `tx_hash` from whichever nonce's entry contains it. If that
    /// leaves the nonce's set empty, the nonce entry itself is dropped --
    /// but the address key is left in place, even if this was the address's
    /// last remaining entry, so a later query still distinguishes "primed,
    /// zero in flight" ([`NonceOwnership::NotOurs`]) from "never primed"
    /// ([`NonceOwnership::Unknown`]).
    ///
    /// A no-op if `tx_hash` was never recorded, or if `address` has no
    /// entries at all.
    pub(crate) fn release(&self, address: Address, tx_hash: TxHash) {
        let Some(mut per_nonce) = self.nonces.get_mut(&address) else {
            return;
        };

        per_nonce.retain(|_nonce, tx_hashes| {
            tx_hashes.remove(&tx_hash);
            !tx_hashes.is_empty()
        });
    }

    /// Whether this wallet's own bookkeeping recognizes `nonce` as
    /// currently occupied by a transaction it broadcast itself.
    ///
    /// Returns [`NonceOwnership::Unknown`] rather than
    /// [`NonceOwnership::NotOurs`] whenever `address` has never had an
    /// entry recorded (including right after every one of its entries was
    /// later released): only [`record`](Self::record) ever inserts the
    /// address key, and [`release`](Self::release) never removes it, so
    /// presence of the key alone answers "has this address ever been
    /// primed", independent of whether it currently holds zero or more
    /// nonces.
    ///
    /// `#[cfg(test)]`: see [`NonceOwnership`]'s doc comment for why this has
    /// no production caller yet.
    #[cfg(test)]
    pub(crate) fn ownership(&self, address: Address, nonce: u64) -> NonceOwnership {
        let Some(per_nonce) = self.nonces.get(&address) else {
            return NonceOwnership::Unknown;
        };

        if per_nonce.contains_key(&nonce) {
            NonceOwnership::Ours
        } else {
            NonceOwnership::NotOurs
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;

    use super::*;

    const ADDRESS: Address = address!("00000000000000000000000000000000000000a9");
    const OTHER_ADDRESS: Address = address!("00000000000000000000000000000000000000b7");
    const NONCE: u64 = 42;
    const OTHER_NONCE: u64 = 43;

    #[test]
    fn recording_then_querying_returns_ours() {
        let in_flight = InFlightNonces::default();
        let tx_hash = TxHash::repeat_byte(0x11);

        in_flight.record(ADDRESS, NONCE, tx_hash);

        assert_eq!(in_flight.ownership(ADDRESS, NONCE), NonceOwnership::Ours);
    }

    #[test]
    fn querying_an_address_with_zero_recorded_history_returns_unknown() {
        let in_flight = InFlightNonces::default();

        assert_eq!(in_flight.ownership(ADDRESS, NONCE), NonceOwnership::Unknown);
    }

    #[test]
    fn querying_a_recorded_address_for_a_never_recorded_nonce_returns_not_ours() {
        let in_flight = InFlightNonces::default();
        let tx_hash = TxHash::repeat_byte(0x22);

        in_flight.record(ADDRESS, NONCE, tx_hash);

        assert_eq!(
            in_flight.ownership(ADDRESS, OTHER_NONCE),
            NonceOwnership::NotOurs,
            "a primed address must answer NotOurs for a nonce it never recorded, \
             not Unknown"
        );
    }

    #[test]
    fn releasing_the_only_tx_hash_for_a_nonce_leaves_the_address_queryable_as_not_ours() {
        let in_flight = InFlightNonces::default();
        let tx_hash = TxHash::repeat_byte(0x33);

        in_flight.record(ADDRESS, NONCE, tx_hash);
        in_flight.release(ADDRESS, tx_hash);

        assert_eq!(
            in_flight.ownership(ADDRESS, NONCE),
            NonceOwnership::NotOurs,
            "releasing the wallet's only in-flight entry must not regress the \
             address back to Unknown -- it legitimately knows it now has zero \
             nonces in flight, which is a NotOurs answer for any nonce"
        );
    }

    #[test]
    fn two_tx_hashes_recorded_under_the_same_nonce_stay_ours_until_both_are_released() {
        let in_flight = InFlightNonces::default();
        let original_hash = TxHash::repeat_byte(0x44);
        let fee_bumped_hash = TxHash::repeat_byte(0x55);

        in_flight.record(ADDRESS, NONCE, original_hash);
        in_flight.record(ADDRESS, NONCE, fee_bumped_hash);

        in_flight.release(ADDRESS, original_hash);
        assert_eq!(
            in_flight.ownership(ADDRESS, NONCE),
            NonceOwnership::Ours,
            "the nonce is still occupied by the fee-bumped replacement, which \
             has not been released yet"
        );

        in_flight.release(ADDRESS, fee_bumped_hash);
        assert_eq!(
            in_flight.ownership(ADDRESS, NONCE),
            NonceOwnership::NotOurs,
            "releasing the second (and last) tx hash must finally clear the nonce"
        );
    }

    #[test]
    fn releasing_a_never_recorded_tx_hash_is_a_no_op() {
        let in_flight = InFlightNonces::default();
        let recorded_hash = TxHash::repeat_byte(0x66);
        let never_recorded_hash = TxHash::repeat_byte(0x77);

        in_flight.record(ADDRESS, NONCE, recorded_hash);
        in_flight.release(ADDRESS, never_recorded_hash);

        assert_eq!(
            in_flight.ownership(ADDRESS, NONCE),
            NonceOwnership::Ours,
            "releasing a hash that was never recorded must not disturb an \
             unrelated recorded entry"
        );

        in_flight.release(OTHER_ADDRESS, never_recorded_hash);
        assert_eq!(
            in_flight.ownership(OTHER_ADDRESS, NONCE),
            NonceOwnership::Unknown,
            "releasing a hash for an address with no entries at all must not \
             fabricate an entry for it"
        );
    }

    #[test]
    fn cloned_handles_share_the_same_underlying_map() {
        let in_flight_a = InFlightNonces::default();
        let in_flight_b = in_flight_a.clone();
        let tx_hash = TxHash::repeat_byte(0x88);

        in_flight_a.record(ADDRESS, NONCE, tx_hash);

        assert_eq!(
            in_flight_b.ownership(ADDRESS, NONCE),
            NonceOwnership::Ours,
            "a clone must see entries recorded through a different handle"
        );

        in_flight_b.release(ADDRESS, tx_hash);

        assert_eq!(
            in_flight_a.ownership(ADDRESS, NONCE),
            NonceOwnership::NotOurs,
            "a release through one clone must be visible through another"
        );
    }
}
