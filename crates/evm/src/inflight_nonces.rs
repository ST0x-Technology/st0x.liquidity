//! Tracks nonces this wallet itself has assigned to transactions it has
//! broadcast but not yet seen confirmed or proven dropped.
//!
//! [`ResettableNonceManager`](crate::nonce::ResettableNonceManager) owns the
//! allocator's coherent set of prepared and broadcast-but-unconfirmed nonces.
//! [`InFlightNonces`] adds transaction hashes to that ownership: a record is
//! created when a submission is accepted or restored from durable state, and
//! `await_receipt` releases it only when the wait resolves definitively. A
//! mined transaction clears every competing hash recorded at its nonce, while
//! a transaction proven dropped from both the receipt lookup and the mempool
//! clears only that hash. This both protects allocation and lets later
//! "replacement transaction underpriced" rejections use direct ownership
//! evidence instead of an inferred heuristic.
//!
//! A `HashSet<TxHash>` is kept per nonce, not a single hash, because a
//! fee-bumped replacement resubmits the *same* nonce under a *new* hash. Only
//! one can mine, but both must remain recognized as ours until confirmation or
//! a definitive drop resolves them. Entries are never released by age:
//! elapsed time does not prove a transaction can no longer mine.
//!
//! ## What this tracker can and cannot prove
//!
//! A nonce this process recorded is provably this wallet's own: it was
//! assigned and broadcast by this tracker's own [`record`](InFlightNonces::record)
//! call. An *unrecorded* nonce, however, is not provably a co-signer's: after
//! a restart, this wallet can have its own transactions still pending from
//! before the restart, and this tracker starts empty with no way to tell
//! those apart from a genuinely foreign nonce. Proving that distinction
//! soundly requires durable ownership evidence across restarts. Callers restore
//! such evidence before recovery when the aggregate persisted an exact prepared
//! transaction. For every other unrecorded nonce,
//! [`ownership`](InFlightNonces::ownership) answers
//! [`NonceOwnership::Unknown`], never a proven-foreign answer.

use alloy::primitives::{Address, TxHash};
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::trace;

use crate::nonce::ResettableNonceManager;

/// The answer to "does this wallet own the transaction currently occupying
/// `nonce`?", as far as this process's own bookkeeping can tell.
///
/// Consulted by `submit.rs`'s `resubmit_with_bumped_fee` and
/// `retry_after_nonce_too_low` in place of the inferred `latest_nonce()`
/// heuristics those functions used to rely on exclusively: direct proof from
/// this wallet's own record beats an inferred read whenever the record has
/// one, and both functions fall back to the pre-existing heuristic only for
/// [`NonceOwnership::Unknown`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NonceOwnership {
    /// This nonce currently has an entry: it's a nonce this wallet itself
    /// assigned to a transaction it has not yet seen confirmed or proven
    /// dropped.
    Ours,
    /// The tracker cannot prove this nonce is this wallet's own. This covers
    /// both a nonce that genuinely belongs to a co-signer sharing this
    /// wallet, and one of this wallet's own pre-restart transactions this
    /// process never recorded -- the tracker cannot currently tell those
    /// apart (see the module doc), so it never answers a proven-foreign
    /// verdict. The caller must fall back to a pre-existing heuristic.
    Unknown,
}

/// One address's in-flight bookkeeping: the nonces this process has
/// recorded for it.
#[derive(Debug, Default)]
struct AddressRecord {
    /// Nonces this process has recorded as its own, each with every
    /// still-outstanding transaction hash broadcast at it.
    per_nonce: HashMap<u64, HashSet<TxHash>>,
}

/// Clone-able handle around the wallet's own record of in-flight nonces.
///
/// Cloned handles share both the hash record and the nonce manager's occupancy
/// set, so allocation and ownership evidence cannot diverge.
#[derive(Clone, Debug)]
pub(crate) struct InFlightNonces {
    nonces: Arc<DashMap<Address, AddressRecord>>,
    nonce_manager: ResettableNonceManager,
}

impl Default for InFlightNonces {
    fn default() -> Self {
        Self::new(ResettableNonceManager::default())
    }
}

impl InFlightNonces {
    pub(crate) fn new(nonce_manager: ResettableNonceManager) -> Self {
        Self {
            nonces: Arc::new(DashMap::new()),
            nonce_manager,
        }
    }

    /// Records that `tx_hash` occupies `nonce` for `address`.
    ///
    /// Fresh submissions call this only after acceptance. Durable recovery may
    /// also restore an exact persisted transaction before workers begin.
    pub(crate) fn record(&self, address: Address, nonce: u64, tx_hash: TxHash) {
        self.nonce_manager.occupy_nonce(address, nonce);
        self.nonces
            .entry(address)
            .or_default()
            .per_nonce
            .entry(nonce)
            .or_default()
            .insert(tx_hash);
    }

    /// Removes a proven-dropped `tx_hash` from whichever nonce's entry
    /// contains it. If that leaves the nonce's set empty, the nonce entry
    /// itself is dropped. Other hashes at the same nonce remain in flight
    /// because one of them may still confirm.
    ///
    /// A no-op if `tx_hash` was never recorded, or if `address` has no
    /// entries at all.
    pub(crate) fn release_hash(&self, address: Address, tx_hash: TxHash) {
        let Some(mut record) = self.nonces.get_mut(&address) else {
            trace!(
                %address, %tx_hash,
                "In-flight release for an address with no recorded entries -- \
                 expected if this process never recorded this hash (e.g. a \
                 transfer submitted by another party); would also be the \
                 observable symptom of record() and release() disagreeing on \
                 address"
            );
            return;
        };

        let mut released_nonce = None;
        record.per_nonce.retain(|nonce, tx_hashes| {
            tx_hashes.remove(&tx_hash);
            let retained = !tx_hashes.is_empty();
            if !retained {
                released_nonce = Some(*nonce);
            }
            retained
        });
        if let Some(nonce) = released_nonce {
            self.nonce_manager.release_occupied_nonce(address, nonce);
        }
    }

    /// Removes the entire nonce entry containing confirmed `tx_hash`.
    ///
    /// Once one transaction at a nonce is mined, every fee-bumped predecessor
    /// or replacement recorded at that same nonce is obsolete. A no-op if
    /// `tx_hash` was never recorded, or if `address` has no entries.
    pub(crate) fn release_nonce_for_hash(&self, address: Address, tx_hash: TxHash) {
        let Some(mut record) = self.nonces.get_mut(&address) else {
            trace!(
                %address, %tx_hash,
                "Confirmed in-flight transaction belongs to an address with no \
                 recorded entries -- expected if this process never recorded \
                 this hash"
            );
            return;
        };

        let confirmed_nonce = record
            .per_nonce
            .iter()
            .find_map(|(nonce, tx_hashes)| tx_hashes.contains(&tx_hash).then_some(*nonce));
        if let Some(nonce) = confirmed_nonce {
            record.per_nonce.remove(&nonce);
            self.nonce_manager.release_occupied_nonce(address, nonce);
        }
    }

    /// Whether this wallet's own bookkeeping recognizes `nonce` as
    /// currently occupied by a transaction it broadcast itself. See
    /// [`NonceOwnership`] and the module doc for what each answer proves.
    pub(crate) fn ownership(&self, address: Address, nonce: u64) -> NonceOwnership {
        self.nonces
            .get(&address)
            .filter(|record| record.per_nonce.contains_key(&nonce))
            .map_or(NonceOwnership::Unknown, |_| NonceOwnership::Ours)
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
    fn recording_alone_leaves_a_different_nonce_unknown() {
        // Recording one nonce must not, by itself, make a different nonce
        // provably foreign -- this tracker never answers a proven-foreign
        // verdict at all (see the module doc), only `Ours` or `Unknown`.
        let in_flight = InFlightNonces::default();
        let tx_hash = TxHash::repeat_byte(0x22);

        in_flight.record(ADDRESS, NONCE, tx_hash);

        assert_eq!(
            in_flight.ownership(ADDRESS, OTHER_NONCE),
            NonceOwnership::Unknown,
            "an unrecorded nonce cannot be proven foreign -- it could be this \
             wallet's own pre-restart transaction that this process never \
             touched"
        );
    }

    #[test]
    fn releasing_the_only_tx_hash_for_a_nonce_leaves_it_queryable_again() {
        let in_flight = InFlightNonces::default();
        let tx_hash = TxHash::repeat_byte(0x33);

        in_flight.record(ADDRESS, NONCE, tx_hash);
        in_flight.release_hash(ADDRESS, tx_hash);

        assert_eq!(
            in_flight.ownership(ADDRESS, NONCE),
            NonceOwnership::Unknown,
            "releasing the wallet's only in-flight entry leaves this nonce \
             unproven either way"
        );
    }

    #[test]
    fn two_tx_hashes_recorded_under_the_same_nonce_stay_ours_until_both_are_released() {
        let in_flight = InFlightNonces::default();
        let original_hash = TxHash::repeat_byte(0x44);
        let fee_bumped_hash = TxHash::repeat_byte(0x55);

        in_flight.record(ADDRESS, NONCE, original_hash);
        in_flight.record(ADDRESS, NONCE, fee_bumped_hash);

        in_flight.release_hash(ADDRESS, original_hash);
        assert_eq!(
            in_flight.ownership(ADDRESS, NONCE),
            NonceOwnership::Ours,
            "the nonce is still occupied by the fee-bumped replacement, which \
             has not been released yet"
        );

        in_flight.release_hash(ADDRESS, fee_bumped_hash);
        assert_eq!(
            in_flight.ownership(ADDRESS, NONCE),
            NonceOwnership::Unknown,
            "releasing the second (and last) tx hash must finally clear the \
             nonce; it is not provably foreign either"
        );
    }

    #[test]
    fn releasing_a_never_recorded_tx_hash_is_a_no_op() {
        let in_flight = InFlightNonces::default();
        let recorded_hash = TxHash::repeat_byte(0x66);
        let never_recorded_hash = TxHash::repeat_byte(0x77);

        in_flight.record(ADDRESS, NONCE, recorded_hash);
        in_flight.release_hash(ADDRESS, never_recorded_hash);

        assert_eq!(
            in_flight.ownership(ADDRESS, NONCE),
            NonceOwnership::Ours,
            "releasing a hash that was never recorded must not disturb an \
             unrelated recorded entry"
        );

        in_flight.release_hash(OTHER_ADDRESS, never_recorded_hash);
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

        in_flight_b.release_hash(ADDRESS, tx_hash);

        assert_eq!(
            in_flight_a.ownership(ADDRESS, NONCE),
            NonceOwnership::Unknown,
            "a release through one clone must be visible through another"
        );
    }

    #[test]
    fn releasing_confirmed_nonce_clears_all_hashes_at_that_nonce_only() {
        let in_flight = InFlightNonces::default();
        let original_hash = TxHash::repeat_byte(0x89);
        let confirmed_replacement_hash = TxHash::repeat_byte(0x8a);
        let other_nonce_hash = TxHash::repeat_byte(0x8b);

        in_flight.record(ADDRESS, NONCE, original_hash);
        in_flight.record(ADDRESS, NONCE, confirmed_replacement_hash);
        in_flight.record(ADDRESS, OTHER_NONCE, other_nonce_hash);

        in_flight.release_nonce_for_hash(ADDRESS, confirmed_replacement_hash);

        assert_eq!(
            in_flight.ownership(ADDRESS, NONCE),
            NonceOwnership::Unknown,
            "confirming one replacement must clear every competing hash at \
             that nonce"
        );
        assert_eq!(
            in_flight.ownership(ADDRESS, OTHER_NONCE),
            NonceOwnership::Ours,
            "confirming one nonce must not disturb a different in-flight nonce"
        );
    }
}
