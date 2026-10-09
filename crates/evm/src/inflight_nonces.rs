//! Tracks nonces this wallet itself has assigned to transactions it has
//! signed or broadcast until confirmation or the configured drop policy resolves them.
//!
//! [`ResettableNonceManager`](crate::nonce::ResettableNonceManager) owns the
//! allocator's coherent set of prepared and broadcast-but-unconfirmed nonces.
//! [`InFlightNonces`] adds transaction hashes to that ownership: a record is
//! created when a prepared transaction is signed, when a generic send is
//! accepted, or when durable state is restored, and `await_receipt` resolves
//! it. A mined transaction clears every competing
//! hash recorded at its nonce. A policy-qualified suspected generic drop clears
//! only that hash and rewinds allocation when it was the final hash. Durable
//! prepared transactions instead remain occupied after a drop because their
//! persisted exact bytes will be rebroadcast.
//!
//! A `HashMap<TxHash, HashRecord>` is kept per nonce, not a single hash,
//! because a fee-bumped replacement resubmits the *same* nonce under a *new*
//! hash. Only one can mine, but every live or durably retryable hash must
//! remain recognized as ours, with drop policy and optional submission boundary.
//! Entries are never released by age: elapsed time
//! does not prove a transaction can no longer mine.
//!
//! ## What this tracker can and cannot prove
//!
//! A nonce this process recorded is provably this wallet's own: it was
//! assigned and signed or broadcast by this tracker's own [`record`](InFlightNonces::record)
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
use std::collections::HashMap;
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
use std::future::Future;
use std::sync::Arc;
use tracing::trace;
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
use tracing::warn;

use crate::TransactionSubmission;
use crate::nonce::ResettableNonceManager;
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
use crate::{EvmError, RECEIPT_POLL_INTERVAL};

/// What a discard of a durable prepared transaction knows about its nonce,
/// which decides whether allocation is rewound onto the freed nonce.
#[cfg(any(feature = "turnkey", feature = "local-signer"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiscardedNonce {
    /// The transaction was prepared but will never be broadcast, so its nonce
    /// is unused and must be refilled before any higher one.
    Unused,
    /// Another transaction from this wallet has mined at the nonce. Rewinding
    /// onto it would sign the next send at a nonce the chain has passed.
    Superseded,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DropPolicy {
    Release,
    RetainForRebroadcast,
}

#[derive(Debug, Clone, Copy)]
struct HashRecord {
    policy: DropPolicy,
    submission_boundary: SubmissionBoundary,
}

#[derive(Debug, Clone, Copy)]
enum SubmissionBoundary {
    Unavailable { highest_observed: Option<u64> },
    Complete(u64),
}

/// One address's in-flight bookkeeping: the nonces this process has
/// recorded for it.
#[derive(Debug, Default)]
struct AddressRecord {
    /// Nonces this process has recorded as its own, each with every
    /// still-outstanding transaction hash and its suspected-drop policy.
    per_nonce: HashMap<u64, HashMap<TxHash, HashRecord>>,
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

    /// Records an accepted generic transaction that may release its nonce once
    /// its final hash is definitively dropped.
    pub(crate) fn record(&self, address: Address, nonce: u64, tx_hash: TxHash) {
        self.record_with_policy(address, nonce, tx_hash, DropPolicy::Release);
    }

    /// Records a durable exact transaction whose nonce remains reserved after
    /// a drop so recovery can rebroadcast the same bytes.
    pub(crate) fn record_durable(&self, address: Address, nonce: u64, tx_hash: TxHash) {
        self.record_with_policy(address, nonce, tx_hash, DropPolicy::RetainForRebroadcast);
    }

    fn record_with_policy(
        &self,
        address: Address,
        nonce: u64,
        tx_hash: TxHash,
        policy: DropPolicy,
    ) {
        match policy {
            DropPolicy::Release => self.nonce_manager.occupy_in_flight_nonce(address, nonce),
            DropPolicy::RetainForRebroadcast => {
                self.nonce_manager.reserve_durable_nonce(address, nonce);
            }
        }
        self.nonces
            .entry(address)
            .or_default()
            .per_nonce
            .entry(nonce)
            .or_default()
            .entry(tx_hash)
            .and_modify(|existing| {
                if policy == DropPolicy::RetainForRebroadcast {
                    existing.policy = policy;
                }
            })
            .or_insert(HashRecord {
                policy,
                submission_boundary: SubmissionBoundary::Unavailable {
                    highest_observed: None,
                },
            });
    }

    pub(crate) fn submission(
        &self,
        address: Address,
        tx_hash: TxHash,
    ) -> Option<TransactionSubmission> {
        self.nonces
            .get(&address)?
            .per_nonce
            .iter()
            .find_map(|(nonce, hashes)| {
                hashes.get(&tx_hash).map(|record| TransactionSubmission {
                    sender: address,
                    nonce: *nonce,
                    submitted_after_block: match record.submission_boundary {
                        SubmissionBoundary::Unavailable { .. } => None,
                        SubmissionBoundary::Complete(block) => Some(block),
                    },
                })
            })
    }

    /// Preserve the highest observed submission boundary across exact rebroadcasts.
    pub(crate) fn record_submission_block(&self, address: Address, tx_hash: TxHash, block: u64) {
        if let Some(mut record) = self.nonces.get_mut(&address) {
            for hashes in record.per_nonce.values_mut() {
                if let Some(hash) = hashes.get_mut(&tx_hash) {
                    let highest = match hash.submission_boundary {
                        SubmissionBoundary::Unavailable { highest_observed } => {
                            highest_observed.map_or(block, |old| old.max(block))
                        }
                        SubmissionBoundary::Complete(old) => old.max(block),
                    };
                    hash.submission_boundary = SubmissionBoundary::Complete(highest);
                    return;
                }
            }
        }
    }

    /// Hide the usable floor before broadcasting, while retaining its monotonic
    /// history. Cancellation or a failed post-read leaves freshness unavailable.
    pub(crate) fn begin_submission_observation(
        &self,
        address: Address,
        tx_hash: TxHash,
        block: u64,
    ) {
        if let Some(mut record) = self.nonces.get_mut(&address) {
            for hashes in record.per_nonce.values_mut() {
                if let Some(hash) = hashes.get_mut(&tx_hash) {
                    let highest = match hash.submission_boundary {
                        SubmissionBoundary::Unavailable { highest_observed } => {
                            highest_observed.map_or(block, |old| old.max(block))
                        }
                        SubmissionBoundary::Complete(old) => old.max(block),
                    };
                    hash.submission_boundary = SubmissionBoundary::Unavailable {
                        highest_observed: Some(highest),
                    };
                    return;
                }
            }
        }
    }

    /// Bracket a successful operation without turning a later read failure into
    /// a submission failure. Unknown boundaries cannot qualify absence as dropped.
    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    pub(crate) async fn observe_submission_boundary(
        &self,
        head: impl Future<Output = Result<u64, EvmError>> + Send,
        address: Address,
        tx_hash: TxHash,
        before_block: u64,
    ) {
        match tokio::time::timeout(RECEIPT_POLL_INTERVAL, head).await {
            Ok(Ok(after_block)) => {
                self.record_submission_block(address, tx_hash, before_block.max(after_block));
            }
            Ok(Err(error)) => {
                warn!(%address, %tx_hash, ?error,
                    "Operation completed but post-operation head is unavailable; retaining ownership without a new drop boundary");
            }
            Err(error) => {
                warn!(%address, %tx_hash, ?error,
                    "Post-submission head observation timed out; retaining ownership with unknown freshness");
            }
        }
    }

    /// Remove only a fresh signed preparation that has not returned or crossed
    /// the broadcast boundary. The preparation guard holds the wallet send lock.
    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    pub(crate) fn discard_unreturned_preparation(
        &self,
        address: Address,
        nonce: u64,
        tx_hash: TxHash,
    ) {
        let Some(mut record) = self.nonces.get_mut(&address) else {
            return;
        };
        let Some(hashes) = record.per_nonce.get_mut(&nonce) else {
            return;
        };
        if hashes.remove(&tx_hash).is_some() && hashes.is_empty() {
            record.per_nonce.remove(&nonce);
            self.nonce_manager.release_occupied_nonce(address, nonce);
        }
    }

    /// Resolves a `tx_hash` classified as suspected dropped by wallet policy.
    ///
    /// Generic hashes are removed. If the hash was the nonce's final entry,
    /// occupancy and the cached allocation are atomically rewound while the
    /// caller holds the wallet send lock. Durable prepared hashes remain
    /// occupied because their exact persisted bytes are still retryable.
    pub(crate) async fn release_hash(&self, address: Address, tx_hash: TxHash) {
        let released_nonce = {
            let Some(mut record) = self.nonces.get_mut(&address) else {
                trace!(
                    %address, %tx_hash,
                    "In-flight release for an address with no recorded entries -- \
                     expected if this process never recorded this hash"
                );
                return;
            };

            let Some((nonce, policy)) = record.per_nonce.iter().find_map(|(nonce, tx_hashes)| {
                tx_hashes
                    .get(&tx_hash)
                    .copied()
                    .map(|record| (*nonce, record.policy))
            }) else {
                return;
            };
            if policy == DropPolicy::RetainForRebroadcast {
                return;
            }

            let Some(tx_hashes) = record.per_nonce.get_mut(&nonce) else {
                return;
            };
            tx_hashes.remove(&tx_hash);
            if tx_hashes.is_empty() {
                record.per_nonce.remove(&nonce);
                Some(nonce)
            } else {
                None
            }
        };

        if let Some(nonce) = released_nonce {
            self.nonce_manager
                .release_nonce_and_rewind(address, nonce)
                .await;
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
            .find_map(|(nonce, tx_hashes)| tx_hashes.contains_key(&tx_hash).then_some(*nonce));
        if let Some(nonce) = confirmed_nonce {
            record.per_nonce.remove(&nonce);
            self.nonce_manager.release_occupied_nonce(address, nonce);
        }
    }

    /// Ownership-checked release for an explicit discard of a durable prepared
    /// transaction. Finds the nonce whose recorded hash set still contains
    /// `tx_hash`, removes that hash regardless of its drop policy, and when it was
    /// the nonce's last hash releases the reservation. A [`Superseded`] release
    /// drops every hash at the nonce, since a mined transaction used it. An [`Unused`] nonce is
    /// also rewound onto so it is reused before any higher one; a [`Superseded`]
    /// nonce leaves allocation untouched, because the chain has already used it.
    ///
    /// [`Unused`]: DiscardedNonce::Unused
    /// [`Superseded`]: DiscardedNonce::Superseded
    ///
    /// Keyed by hash, so it is safe against the two ways a reconcile can be
    /// observed more than once (a sleeping redrive row and the timeout sweep's
    /// enqueue, plus apalis retries): once the first release removes the hash, a
    /// later call finds nothing for `tx_hash` and is a no-op, leaving intact any
    /// different transaction that has since taken the reallocated nonce. Returns
    /// whether this call was the owner that released the reservation.
    ///
    /// Callers must hold the wallet send lock across this operation.
    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    pub(crate) async fn release_durable_by_hash(
        &self,
        address: Address,
        tx_hash: TxHash,
        discarded: DiscardedNonce,
    ) -> bool {
        let released_nonce =
            {
                let Some(mut record) = self.nonces.get_mut(&address) else {
                    return false;
                };
                let Some(nonce) = record.per_nonce.iter().find_map(|(nonce, tx_hashes)| {
                    tx_hashes.contains_key(&tx_hash).then_some(*nonce)
                }) else {
                    return false;
                };
                let Some(tx_hashes) = record.per_nonce.get_mut(&nonce) else {
                    return false;
                };
                tx_hashes.remove(&tx_hash);
                // A mined transaction used the nonce, so no other hash recorded
                // at it (a fee replacement of the same send) can mine either.
                if tx_hashes.is_empty() || discarded == DiscardedNonce::Superseded {
                    record.per_nonce.remove(&nonce);
                    Some(nonce)
                } else {
                    None
                }
            };

        let Some(nonce) = released_nonce else {
            return false;
        };

        match discarded {
            DiscardedNonce::Unused => {
                self.nonce_manager
                    .release_nonce_and_rewind(address, nonce)
                    .await;
            }
            DiscardedNonce::Superseded => {
                self.nonce_manager.release_occupied_nonce(address, nonce);
            }
        }
        true
    }

    /// Whether this wallet's own bookkeeping recognizes `nonce` as
    /// currently occupied by a transaction it signed or broadcast itself. See
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

    #[test]
    fn restored_identity_cannot_invent_submission_floor_and_observations_never_lower_floor() {
        let tracker = InFlightNonces::default();
        let hash = TxHash::random();
        tracker.record_durable(ADDRESS, NONCE, hash);
        assert_eq!(
            tracker
                .submission(ADDRESS, hash)
                .unwrap()
                .submitted_after_block,
            None
        );
        tracker.record_submission_block(ADDRESS, hash, 100);
        tracker.record_durable(ADDRESS, NONCE, hash);
        tracker.record_submission_block(ADDRESS, hash, 200);
        assert_eq!(
            tracker.submission(ADDRESS, hash),
            Some(TransactionSubmission {
                sender: ADDRESS,
                nonce: NONCE,
                submitted_after_block: Some(200),
            })
        );
        tracker.record_submission_block(ADDRESS, hash, 150);
        assert_eq!(
            tracker
                .submission(ADDRESS, hash)
                .unwrap()
                .submitted_after_block,
            Some(200)
        );
        assert_eq!(tracker.submission(OTHER_ADDRESS, hash), None);
        tracker.release_nonce_for_hash(ADDRESS, hash);
        assert_eq!(tracker.submission(ADDRESS, hash), None);
    }
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

    #[tokio::test]
    async fn releasing_the_only_tx_hash_for_a_nonce_leaves_it_queryable_again() {
        let in_flight = InFlightNonces::default();
        let tx_hash = TxHash::repeat_byte(0x33);

        in_flight.record(ADDRESS, NONCE, tx_hash);
        in_flight.release_hash(ADDRESS, tx_hash).await;

        assert_eq!(
            in_flight.ownership(ADDRESS, NONCE),
            NonceOwnership::Unknown,
            "releasing the wallet's only in-flight entry leaves this nonce \
             unproven either way"
        );
    }

    #[tokio::test]
    async fn two_tx_hashes_recorded_under_the_same_nonce_stay_ours_until_both_are_released() {
        let in_flight = InFlightNonces::default();
        let original_hash = TxHash::repeat_byte(0x44);
        let fee_bumped_hash = TxHash::repeat_byte(0x55);

        in_flight.record(ADDRESS, NONCE, original_hash);
        in_flight.record(ADDRESS, NONCE, fee_bumped_hash);

        in_flight.release_hash(ADDRESS, original_hash).await;
        assert_eq!(
            in_flight.ownership(ADDRESS, NONCE),
            NonceOwnership::Ours,
            "the nonce is still occupied by the fee-bumped replacement, which \
             has not been released yet"
        );

        in_flight.release_hash(ADDRESS, fee_bumped_hash).await;
        assert_eq!(
            in_flight.ownership(ADDRESS, NONCE),
            NonceOwnership::Unknown,
            "releasing the second (and last) tx hash must finally clear the \
             nonce; it is not provably foreign either"
        );
    }

    #[tokio::test]
    async fn releasing_a_never_recorded_tx_hash_is_a_no_op() {
        let in_flight = InFlightNonces::default();
        let recorded_hash = TxHash::repeat_byte(0x66);
        let never_recorded_hash = TxHash::repeat_byte(0x77);

        in_flight.record(ADDRESS, NONCE, recorded_hash);
        in_flight.release_hash(ADDRESS, never_recorded_hash).await;

        assert_eq!(
            in_flight.ownership(ADDRESS, NONCE),
            NonceOwnership::Ours,
            "releasing a hash that was never recorded must not disturb an \
             unrelated recorded entry"
        );

        in_flight
            .release_hash(OTHER_ADDRESS, never_recorded_hash)
            .await;
        assert_eq!(
            in_flight.ownership(OTHER_ADDRESS, NONCE),
            NonceOwnership::Unknown,
            "releasing a hash for an address with no entries at all must not \
             fabricate an entry for it"
        );
    }

    #[tokio::test]
    async fn cloned_handles_share_the_same_underlying_map() {
        let in_flight_a = InFlightNonces::default();
        let in_flight_b = in_flight_a.clone();
        let tx_hash = TxHash::repeat_byte(0x88);

        in_flight_a.record(ADDRESS, NONCE, tx_hash);

        assert_eq!(
            in_flight_b.ownership(ADDRESS, NONCE),
            NonceOwnership::Ours,
            "a clone must see entries recorded through a different handle"
        );

        in_flight_b.release_hash(ADDRESS, tx_hash).await;

        assert_eq!(
            in_flight_a.ownership(ADDRESS, NONCE),
            NonceOwnership::Unknown,
            "a release through one clone must be visible through another"
        );
    }

    #[tokio::test]
    async fn durable_hash_remains_owned_after_a_definitive_drop() {
        let in_flight = InFlightNonces::default();
        let tx_hash = TxHash::repeat_byte(0x87);

        in_flight.record_durable(ADDRESS, NONCE, tx_hash);
        in_flight.release_hash(ADDRESS, tx_hash).await;

        assert_eq!(
            in_flight.ownership(ADDRESS, NONCE),
            NonceOwnership::Ours,
            "persisted exact bytes remain retryable and must keep their nonce \
             occupied after a definitive drop"
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

    #[tokio::test]
    async fn release_durable_by_hash_is_ownership_checked_and_reuse_safe() {
        // Reconciling a stuck withdrawal is observed more than once (a sleeping
        // redrive row plus the timeout sweep, plus apalis retries). The first
        // ownership-checked release frees the nonce; a later release for the same
        // withdrawal must be a no-op that leaves intact any different transaction
        // that has since taken the reallocated nonce, so a stale repeat can never
        // strand an unrelated send.
        let manager = ResettableNonceManager::default();
        let in_flight = InFlightNonces::new(manager.clone());
        let stuck_hash = TxHash::repeat_byte(0x91);

        in_flight.record_durable(ADDRESS, NONCE, stuck_hash);
        assert_eq!(in_flight.ownership(ADDRESS, NONCE), NonceOwnership::Ours);

        assert!(
            in_flight
                .release_durable_by_hash(ADDRESS, stuck_hash, DiscardedNonce::Superseded)
                .await,
            "the first release still owns the nonce and must free the reservation"
        );
        assert_eq!(
            in_flight.ownership(ADDRESS, NONCE),
            NonceOwnership::Unknown,
            "the reconciled withdrawal's nonce record must be gone after release"
        );

        // A different transaction now takes the freed, rewound nonce.
        let reused_hash = TxHash::repeat_byte(0x92);
        in_flight.record_durable(ADDRESS, NONCE, reused_hash);

        assert!(
            !in_flight
                .release_durable_by_hash(ADDRESS, stuck_hash, DiscardedNonce::Superseded)
                .await,
            "a repeat release for the already-released withdrawal must be a no-op"
        );
        assert_eq!(
            in_flight.ownership(ADDRESS, NONCE),
            NonceOwnership::Ours,
            "the stale repeat must not disturb the transaction that reused the nonce"
        );
        assert!(
            manager.release_occupied_nonce(ADDRESS, NONCE),
            "the stale repeat must leave the reused transaction's allocator hold \
             intact, not just its in-flight record"
        );
    }

    #[tokio::test]
    async fn a_superseded_release_drops_every_replacement_at_the_nonce() {
        // A send and its fee replacement share a nonce. Once a cancel mines at
        // it, releasing either hash must free the nonce: neither can mine.
        let manager = ResettableNonceManager::default();
        let in_flight = InFlightNonces::new(manager.clone());
        let original = TxHash::repeat_byte(0x94);
        let replacement = TxHash::repeat_byte(0x95);
        in_flight.record_durable(ADDRESS, NONCE, original);
        in_flight.record_durable(ADDRESS, NONCE, replacement);

        assert!(
            in_flight
                .release_durable_by_hash(ADDRESS, original, DiscardedNonce::Superseded)
                .await
        );

        assert_eq!(in_flight.ownership(ADDRESS, NONCE), NonceOwnership::Unknown);
        assert!(
            !in_flight
                .release_durable_by_hash(ADDRESS, replacement, DiscardedNonce::Superseded)
                .await,
            "the replacement's hash went with its nonce"
        );
    }

    #[tokio::test]
    async fn discarding_an_unused_nonce_rewinds_allocation_onto_it() {
        // A persist-failure rollback: the prepared transaction was never
        // broadcast, so its nonce must be refilled before any higher one.
        let manager = ResettableNonceManager::default();
        let in_flight = InFlightNonces::new(manager.clone());
        let tx_hash = TxHash::repeat_byte(0x93);
        manager.set_next_nonce(ADDRESS, NONCE + 3).await;
        manager.reserve_prepared_nonce(ADDRESS, NONCE).await;
        in_flight.record_durable(ADDRESS, NONCE, tx_hash);

        assert!(
            in_flight
                .release_durable_by_hash(ADDRESS, tx_hash, DiscardedNonce::Unused)
                .await
        );

        assert_eq!(manager.peek_next_nonce(ADDRESS).await, Some(NONCE));
    }

    #[tokio::test]
    async fn discarding_a_superseded_nonce_keeps_allocation() {
        // An operator reconcile after a replacement mined at the nonce: the
        // hold is dropped, but rewinding onto a used nonce would make the next
        // send fail with "nonce too low".
        let manager = ResettableNonceManager::default();
        let in_flight = InFlightNonces::new(manager.clone());
        let tx_hash = TxHash::repeat_byte(0x94);
        manager.set_next_nonce(ADDRESS, NONCE + 3).await;
        manager.reserve_prepared_nonce(ADDRESS, NONCE).await;
        in_flight.record_durable(ADDRESS, NONCE, tx_hash);

        assert!(
            in_flight
                .release_durable_by_hash(ADDRESS, tx_hash, DiscardedNonce::Superseded)
                .await
        );

        assert_eq!(manager.peek_next_nonce(ADDRESS).await, Some(NONCE + 3));
        assert!(
            !manager.release_occupied_nonce(ADDRESS, NONCE),
            "the superseded discard must still release the allocator hold"
        );
    }
}
