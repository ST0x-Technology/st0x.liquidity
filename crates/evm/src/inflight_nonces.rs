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
//! A `HashMap<TxHash, Instant>` is kept per nonce, not a single hash,
//! because a fee-bumped replacement resubmits the *same* nonce under a
//! *new* hash -- only one of the two can ever mine, but both must be
//! recognized as "ours" while either is outstanding. The `Instant` records
//! when the entry was recorded, purely to bound this tracker's memory
//! growth -- see [`MAX_IN_FLIGHT_AGE`].
//!
//! ## What this tracker can and cannot prove
//!
//! A nonce this process recorded is provably this wallet's own: it was
//! assigned and broadcast by this tracker's own [`record`](InFlightNonces::record)
//! call. An *unrecorded* nonce, however, is not provably a co-signer's: after
//! a restart, this wallet can have its own transactions still pending from
//! before the restart, and this tracker starts empty with no way to tell
//! those apart from a genuinely foreign nonce. Proving that distinction
//! soundly would require persisting ownership evidence across restarts,
//! which this tracker does not currently do (tracked as a follow-up). Until
//! then, [`ownership`](InFlightNonces::ownership) answers
//! [`NonceOwnership::Unknown`], never a proven-foreign answer, for any
//! nonce it has not itself recorded.

use alloy::primitives::{Address, TxHash};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::trace;

/// Upper bound on how long this tracker keeps an in-flight entry before
/// pruning it, so the map cannot grow without bound over a long-running
/// process.
///
/// Age, not the wait outcome, is what triggers reclaiming: a
/// `ReceiptTimeout` proves nothing about whether the transaction will still
/// mine (see `submit::release_in_flight_after_wait`'s doc comment), so an
/// entry left behind by a timed-out wait must not be reclaimed the moment
/// the wait gives up -- doing so could let a still-live transaction be
/// evicted by a later fee bump. This bound is deliberately far larger than
/// any receipt-wait timeout (`RECEIPT_TIMEOUT` / `CONFIRMATION_TIMEOUT` in
/// `lib.rs`, 5 and 30 minutes) so it essentially never fires against a
/// transaction still genuinely being waited on; it exists purely so an
/// entry that never gets a definitive outcome (a caller that stopped
/// polling, a persistently degraded RPC) does not accumulate forever.
const MAX_IN_FLIGHT_AGE: Duration = Duration::from_secs(24 * 60 * 60);

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
    /// Nonces this process has recorded as its own, each with the set of
    /// still-outstanding transaction hashes broadcast at it (more than one
    /// only while a fee-bumped replacement and its predecessor are both
    /// unresolved) and when each was recorded.
    per_nonce: HashMap<u64, HashMap<TxHash, Instant>>,
}

/// Drops in-flight entries older than [`MAX_IN_FLIGHT_AGE`] from `per_nonce`,
/// dropping a nonce's own entry too once its last transaction hash is
/// pruned. Called from both [`InFlightNonces::record`] and
/// [`InFlightNonces::ownership`] so growth is bounded without needing a
/// separate background sweep task.
fn prune_stale(per_nonce: &mut HashMap<u64, HashMap<TxHash, Instant>>) {
    let now = Instant::now();
    per_nonce.retain(|_nonce, tx_hashes| {
        tx_hashes
            .retain(|_tx_hash, submitted_at| now.duration_since(*submitted_at) < MAX_IN_FLIGHT_AGE);
        !tx_hashes.is_empty()
    });
}

/// Clone-able handle around the wallet's own record of in-flight nonces.
///
/// Same sharing model as `ResettableNonceManager`: cloned handles share the
/// same underlying map, so every clone of a wallet contends over one
/// record.
#[derive(Clone, Debug, Default)]
pub(crate) struct InFlightNonces {
    nonces: Arc<DashMap<Address, AddressRecord>>,
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
        let mut record = self.nonces.entry(address).or_default();
        prune_stale(&mut record.per_nonce);
        record
            .per_nonce
            .entry(nonce)
            .or_default()
            .insert(tx_hash, Instant::now());
    }

    /// Removes `tx_hash` from whichever nonce's entry contains it. If that
    /// leaves the nonce's set empty, the nonce entry itself is dropped.
    ///
    /// A no-op if `tx_hash` was never recorded, or if `address` has no
    /// entries at all.
    pub(crate) fn release(&self, address: Address, tx_hash: TxHash) {
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

        record.per_nonce.retain(|_nonce, tx_hashes| {
            tx_hashes.remove(&tx_hash);
            !tx_hashes.is_empty()
        });
    }

    /// Whether this wallet's own bookkeeping recognizes `nonce` as
    /// currently occupied by a transaction it broadcast itself. See
    /// [`NonceOwnership`] and the module doc for what each answer proves.
    pub(crate) fn ownership(&self, address: Address, nonce: u64) -> NonceOwnership {
        let Some(mut record) = self.nonces.get_mut(&address) else {
            return NonceOwnership::Unknown;
        };

        prune_stale(&mut record.per_nonce);

        if record.per_nonce.contains_key(&nonce) {
            return NonceOwnership::Ours;
        }

        NonceOwnership::Unknown
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
        in_flight.release(ADDRESS, tx_hash);

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
            NonceOwnership::Unknown,
            "a release through one clone must be visible through another"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stale_entries_are_pruned_after_the_age_bound() {
        let in_flight = InFlightNonces::default();
        let tx_hash = TxHash::repeat_byte(0x99);

        in_flight.record(ADDRESS, NONCE, tx_hash);
        assert_eq!(in_flight.ownership(ADDRESS, NONCE), NonceOwnership::Ours);

        tokio::time::advance(MAX_IN_FLIGHT_AGE + Duration::from_secs(1)).await;

        assert_eq!(
            in_flight.ownership(ADDRESS, NONCE),
            NonceOwnership::Unknown,
            "an entry older than the age bound must be pruned rather than \
             answer Ours forever for a transaction this tracker never saw \
             resolved"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_fresh_recording_survives_pruning_triggered_by_a_stale_sibling() {
        let in_flight = InFlightNonces::default();
        let stale_hash = TxHash::repeat_byte(0xaa);

        in_flight.record(ADDRESS, NONCE, stale_hash);
        tokio::time::advance(MAX_IN_FLIGHT_AGE + Duration::from_secs(1)).await;

        let fresh_hash = TxHash::repeat_byte(0xbb);
        in_flight.record(ADDRESS, OTHER_NONCE, fresh_hash);

        assert_eq!(
            in_flight.ownership(ADDRESS, NONCE),
            NonceOwnership::Unknown,
            "the stale sibling must be pruned"
        );
        assert_eq!(
            in_flight.ownership(ADDRESS, OTHER_NONCE),
            NonceOwnership::Ours,
            "pruning a stale entry must not disturb a fresh one recorded in \
             the same call"
        );
    }
}
