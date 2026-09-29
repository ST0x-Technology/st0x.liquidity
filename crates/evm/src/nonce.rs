//! Resettable nonce manager for concurrent transaction submission.
//!
//! Alloy's [`CachedNonceManager`] caches nonces locally and never
//! re-fetches from the RPC after initialization. When external
//! processes (e.g. CLI commands) submit transactions from the same
//! wallet address, the cache becomes stale and every subsequent send
//! fails with "nonce too low".
//!
//! [`ResettableNonceManager`] extends `CachedNonceManager` semantics with
//! recovery and ownership hooks:
//!
//! - [`invalidate()`] clears only the cache, forcing the next send to re-fetch
//!   the nonce from the chain via the **latest** (mined) transaction count.
//!   It deliberately retains every prepared or broadcast-but-unconfirmed
//!   nonce.
//! - `set_next_nonce()` (crate-internal) seeds the cache with a known-good
//!   lower bound, skipping the RPC re-fetch. Nonce-too-low recovery then asks
//!   the allocator for the exact reservation-aware nonce and pins that value
//!   onto the retry transaction.
//! - Held nonces carry whether allocation must skip them. A prepared
//!   (not-yet-broadcast) reservation and a durable prepared transaction
//!   retained for exact rebroadcast are skipped so a fresh send never
//!   overwrites those exact bytes. A generic broadcast-but-unconfirmed nonce
//!   is deliberately *not* skipped: allocation must be able to land back on
//!   the wallet's own stuck under-gassed transaction to replace it (see the
//!   `latest` fetch below and `submit::send_with_recovery`), rather than
//!   queue the next send behind it forever. Generic ownership is released
//!   after a definitive receipt or drop; durable prepared ownership survives
//!   drops for exact rebroadcast and is released only by confirmation or an
//!   explicit discard-before-broadcast decision.
//!
//! The cold-cache fetch intentionally uses `latest`, not `pending`.
//! `submit.rs`'s stuck-transaction recovery depends on landing back on a stuck
//! pending nonce and receiving "replacement transaction underpriced"; a
//! `pending` fetch would jump past it and leave the replacement unattempted.
//!
//! [`CachedNonceManager`]: alloy::providers::fillers::CachedNonceManager
//! [`invalidate()`]: ResettableNonceManager::invalidate

use alloy::network::Network;
use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::providers::fillers::NonceManager;
use alloy::transports::TransportResult;
use async_trait::async_trait;
use dashmap::DashMap;
use futures::lock::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::trace;

/// Nonce manager that caches nonces locally and supports cache
/// invalidation and seeding for resilience against external nonce
/// changes.
///
/// Each nonce entry holds the nonce the *next* send from that address should
/// use, or `None` when it must be fetched from the RPC. Held nonces are
/// tracked separately, each tagged with whether allocation must skip it, so
/// cache invalidation can discard stale RPC-derived state without erasing
/// prepared or broadcast-but-unconfirmed ownership.
#[derive(Clone, Debug, Default)]
pub struct ResettableNonceManager {
    nonces: Arc<DashMap<Address, Arc<Mutex<Option<u64>>>>>,
    occupied: Arc<DashMap<Address, BTreeMap<u64, NonceHold>>>,
}

/// Why a nonce is currently held, which decides whether allocation may land
/// back on it after a cache seed or invalidation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NonceHold {
    /// A prepared (not-yet-broadcast) reservation or a durable prepared
    /// transaction retained for exact rebroadcast. Allocation skips it: a
    /// fresh send here would steal or overwrite those exact bytes.
    Reserved,
    /// A generic broadcast-but-unconfirmed send. Allocation may land back on
    /// it so the wallet can replace its own stuck under-gassed transaction
    /// with a fee-bumped resubmit; skipping it would leave the next send
    /// queued behind the stuck one until it mines or the process restarts.
    Replaceable,
}

impl ResettableNonceManager {
    /// Clears cached next-nonce values while retaining every occupied nonce.
    /// The next allocation re-fetches the latest mined nonce, then skips
    /// prepared and broadcast-but-unconfirmed transactions.
    ///
    /// Race note: if a concurrent `get_next_nonce` has already cloned the
    /// per-address `Arc<Mutex>` but not yet locked it, it will write to an
    /// orphaned mutex while a new entry is created for subsequent callers.
    /// The wallet send path closes this window by serializing all nonce
    /// mutations behind its send lock.
    pub fn invalidate(&self) {
        self.nonces.clear();
    }

    /// Seeds the cache so the next send from `address` uses `nonce`,
    /// skipping the RPC re-fetch entirely.
    ///
    /// Used by nonce-too-low recovery to seed the target nonce it
    /// computed (see `submit::retry_after_nonce_too_low`). Subject to the
    /// same concurrency caveat as [`invalidate()`](Self::invalidate): the
    /// wallet send path serializes sends so the seeded value cannot be
    /// clobbered mid-recovery.
    #[cfg(any(feature = "turnkey", feature = "local-signer", test))]
    pub(crate) async fn set_next_nonce(&self, address: Address, nonce: u64) {
        let slot = self.slot(address);
        *slot.lock().await = Some(nonce);
    }

    /// Reserves a prepared transaction's nonce and raises an already warm
    /// cache past it, without lowering a higher allocation.
    ///
    /// A cold cache is deliberately left unseeded: `get_next_nonce` then
    /// fetches the chain's mined `latest` again and skips this reservation.
    /// Seeding `nonce + 1` here would pin allocation below the mined count
    /// whenever earlier sends from the wallet mined before the restart, so
    /// every following prepare would sign at a nonce the chain has already
    /// passed and could never land.
    #[cfg(any(feature = "turnkey", feature = "local-signer", test))]
    pub(crate) async fn reserve_prepared_nonce(&self, address: Address, nonce: u64) {
        let slot = self.slot(address);
        let mut cached = slot.lock().await;
        self.hold_nonce(address, nonce, NonceHold::Reserved);
        if let Some(current) = *cached {
            *cached = Some(current.max(nonce.saturating_add(1)));
        }
    }

    /// Reserves the next nonce that no reservation or in flight send already
    /// holds, for a prepared transaction that cannot raise its fee to escape a
    /// collision.
    ///
    /// A prepared (withdrawal) transaction is signed once at a fixed nonce and
    /// persisted for verbatim rebroadcast, so the nonce it is signed at is the
    /// only nonce it can ever land at. Allocating it through `get_next_nonce`
    /// is unsafe: that path deliberately lands back on a `Replaceable` (generic
    /// broadcast but unconfirmed) hold so the generic send can raise the fee on
    /// its own stuck transaction, but fixed prepared bytes pinned onto that
    /// same nonce become permanently unlandable once the generic transaction
    /// mines. This path instead skips every held nonce, `Reserved` and
    /// `Replaceable` alike.
    ///
    /// The floor and the skip every held step are both required. The floor is
    /// the higher of `pending` and `latest`. `pending` counts this wallet's own
    /// unmined sends the cache may have lost to an `invalidate()`, a
    /// `release_nonce_and_rewind` gap fill, or a restart; `latest` guards against
    /// a lagging node behind a load balancer serving a `pending` below the mined
    /// count, which would sign a fixed prepared nonce the chain has already used.
    /// Skipping every held nonce then steps past occupancy the cache still
    /// tracks, including the `Replaceable` holds `get_next_nonce` would land on.
    /// The cache is only ever raised to `chosen + 1`, never lowered, so a
    /// concurrent prepare that already advanced further is not rewound.
    ///
    /// Callers must hold the wallet send lock across this operation.
    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    pub(crate) async fn reserve_next_unheld_nonce<TProvider, TNetwork>(
        &self,
        provider: &TProvider,
        address: Address,
    ) -> TransportResult<u64>
    where
        TProvider: Provider<TNetwork>,
        TNetwork: Network,
    {
        let slot = self.slot(address);
        let mut cached = slot.lock().await;

        // Floor the candidate at the higher of the wallet's `pending` count and
        // the chain's mined `latest`. `pending` counts this wallet's own unmined
        // sends the cache may have dropped, but a lagging node behind a load
        // balancer can serve a `pending` below the true mined count; flooring at
        // `latest` as well stops a fixed prepared nonce from being signed at a
        // nonce the chain has already used, which a prepared withdrawal cannot
        // recover from the way a generic send does.
        let pending = provider.get_transaction_count(address).pending().await?;
        let latest = provider.get_transaction_count(address).latest().await?;
        let floor = pending.max(latest);
        let mut candidate = cached.map_or(floor, |current| current.max(floor));
        while self.is_held(address, candidate) {
            let advanced = candidate.saturating_add(1);
            if advanced == candidate {
                break;
            }
            candidate = advanced;
        }
        self.hold_nonce(address, candidate, NonceHold::Reserved);
        *cached = Some(candidate.saturating_add(1));
        drop(cached);
        Ok(candidate)
    }

    /// Releases a prepared nonce that will never be broadcast. The cache is
    /// rewound only as far as that nonce; every other prepared or in-flight
    /// nonce remains protected and is skipped by `get_next_nonce`.
    #[cfg(any(feature = "turnkey", feature = "local-signer", test))]
    pub(crate) async fn release_prepared_nonce(&self, address: Address, nonce: u64) {
        self.release_nonce_and_rewind(address, nonce).await;
    }

    /// Releases a nonce that is definitively free and rewinds the allocation
    /// cache so the resulting gap is filled before any higher nonce is used.
    ///
    /// Callers must hold the wallet send lock across this operation.
    #[cfg(any(feature = "turnkey", feature = "local-signer", test))]
    pub(crate) async fn release_nonce_and_rewind(&self, address: Address, nonce: u64) {
        let slot = self.slot(address);
        let mut cached = slot.lock().await;
        if self.release_occupied_nonce(address, nonce) {
            // Only lower a warm cache; never seed a cold one. Seeding could pin
            // allocation below the chain's mined count, the same hazard the
            // prepared reservation path avoids. A genuinely free nonce is still
            // refilled because a cold fetch of `latest` already returns a value
            // at or below it.
            if let Some(current) = *cached {
                *cached = Some(current.min(nonce));
            }
        }
    }

    /// Reserves `nonce` for exact rebroadcast of a durable prepared
    /// transaction. Allocation skips it, exactly like a prepared reservation:
    /// a fresh send here would overwrite bytes that must be rebroadcast
    /// verbatim.
    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    pub(crate) fn reserve_durable_nonce(&self, address: Address, nonce: u64) {
        self.hold_nonce(address, nonce, NonceHold::Reserved);
    }

    /// Marks `nonce` occupied by a generic broadcast-but-unconfirmed send.
    /// Unlike a reservation it stays re-allocatable: the wallet's own stuck
    /// under-gassed transaction at this nonce must remain reachable so
    /// `submit::send_with_recovery` can land back on it and fee-bump it,
    /// instead of queuing the next send behind it.
    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    pub(crate) fn occupy_in_flight_nonce(&self, address: Address, nonce: u64) {
        self.hold_nonce(address, nonce, NonceHold::Replaceable);
    }

    /// Records a hold on `nonce`. A `Reserved` hold is sticky: a later
    /// `Replaceable` hold never downgrades it, so a durable reservation
    /// cannot be silently turned replaceable and overwritten.
    #[cfg(any(feature = "turnkey", feature = "local-signer", test))]
    fn hold_nonce(&self, address: Address, nonce: u64, hold: NonceHold) {
        self.occupied
            .entry(address)
            .or_default()
            .entry(nonce)
            .and_modify(|existing| {
                if hold == NonceHold::Reserved {
                    *existing = NonceHold::Reserved;
                }
            })
            .or_insert(hold);
    }

    /// Releases one definitively resolved nonce.
    #[cfg(any(feature = "turnkey", feature = "local-signer", test))]
    pub(crate) fn release_occupied_nonce(&self, address: Address, nonce: u64) -> bool {
        self.occupied
            .get_mut(&address)
            .is_some_and(|mut occupied| occupied.remove(&nonce).is_some())
    }

    /// The per-address cache slot, created empty on first access.
    fn slot(&self, address: Address) -> Arc<Mutex<Option<u64>>> {
        let entry = self
            .nonces
            .entry(address)
            .or_insert_with(|| Arc::new(Mutex::new(None)));

        Arc::clone(entry.value())
    }

    /// Whether a prepared or durable-rebroadcast reservation currently holds
    /// `nonce`, so allocation must skip it. A generic broadcast-but-
    /// unconfirmed nonce is *not* reserved and stays re-allocatable.
    fn is_reserved(&self, address: Address, nonce: u64) -> bool {
        self.occupied
            .get(&address)
            .is_some_and(|held| matches!(held.get(&nonce), Some(NonceHold::Reserved)))
    }

    /// Whether any hold, `Reserved` or `Replaceable`, currently sits on
    /// `nonce`. Unlike `is_reserved`, which a generic broadcast but unconfirmed
    /// `Replaceable` hold does not satisfy, this treats every occupied nonce as
    /// held: a prepared allocation must skip both kinds because its fixed signed
    /// bytes cannot raise their fee to escape a collision the way a generic send
    /// lands back on its own `Replaceable` nonce to replace it.
    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    fn is_held(&self, address: Address, nonce: u64) -> bool {
        self.occupied
            .get(&address)
            .is_some_and(|held| held.contains_key(&nonce))
    }

    /// The nonce the next send from `address` would use, without assigning
    /// it (i.e. without consuming it the way [`get_next_nonce`] does).
    /// `None` when the cache holds nothing for `address` yet.
    ///
    /// Used by `submit::retry_after_nonce_too_low` to fold the nonce the
    /// triggering base send already consumed into its initial monotonic
    /// floor: by the time recovery starts, this cache holds one past the
    /// nonce that was just rejected as too low, a proven lower bound even
    /// when the rejection itself carries no parseable hint. Also used by
    /// this module's own tests to assert on cache state without consuming
    /// it.
    ///
    /// [`get_next_nonce`]: NonceManager::get_next_nonce
    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    pub(crate) async fn peek_next_nonce(&self, address: Address) -> Option<u64> {
        let slot = self.nonces.get(&address).map(|entry| Arc::clone(&entry))?;

        *slot.lock().await
    }
}

#[async_trait]
impl NonceManager for ResettableNonceManager {
    async fn get_next_nonce<TProvider, TNetwork>(
        &self,
        provider: &TProvider,
        address: Address,
    ) -> TransportResult<u64>
    where
        TProvider: Provider<TNetwork>,
        TNetwork: Network,
    {
        let slot = self.slot(address);
        let mut cached = slot.lock().await;

        let mut next_nonce = if let Some(next_nonce) = *cached {
            trace!(%address, next_nonce, "using cached nonce");
            next_nonce
        } else {
            trace!(%address, "fetching latest nonce from RPC");
            // Explicit rather than relying on alloy's default block tag: the
            // module doc above depends on this re-fetch returning the
            // `latest` (mined) count, not `pending`.
            provider.get_transaction_count(address).latest().await?
        };
        while self.is_reserved(address, next_nonce) {
            let advanced = next_nonce.saturating_add(1);
            if advanced == next_nonce {
                break;
            }
            next_nonce = advanced;
        }

        // Saturate rather than wrap: reaching `u64::MAX` is unreachable in
        // practice, and loudly reusing MAX is safer than wrapping to zero.
        *cached = Some(next_nonce.saturating_add(1));
        drop(cached);

        Ok(next_nonce)
    }
}

#[cfg(test)]
mod tests {
    use alloy::providers::ProviderBuilder;

    use super::*;
    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    use crate::inflight_nonces::InFlightNonces;
    use alloy::node_bindings::Anvil;
    use alloy::primitives::U256;
    use alloy::rpc::types::TransactionRequest;

    #[tokio::test]
    async fn increments_locally_after_first_fetch() {
        let manager = ResettableNonceManager::default();
        let provider = ProviderBuilder::new().connect_anvil();
        let address = Address::ZERO;

        let first = manager.get_next_nonce(&provider, address).await.unwrap();
        assert_eq!(first, 0);

        let second = manager.get_next_nonce(&provider, address).await.unwrap();
        assert_eq!(second, 1);

        let third = manager.get_next_nonce(&provider, address).await.unwrap();
        assert_eq!(third, 2);
    }

    #[tokio::test]
    async fn invalidate_forces_rpc_refetch() {
        let manager = ResettableNonceManager::default();
        let provider = ProviderBuilder::new().connect_anvil();
        let address = Address::ZERO;

        let first = manager.get_next_nonce(&provider, address).await.unwrap();
        assert_eq!(first, 0);

        let second = manager.get_next_nonce(&provider, address).await.unwrap();
        assert_eq!(second, 1);

        manager.invalidate();

        // After invalidation, re-fetches from RPC (still 0 since no
        // real txs were sent on anvil).
        let after_invalidate = manager.get_next_nonce(&provider, address).await.unwrap();
        assert_eq!(after_invalidate, 0);
    }

    #[tokio::test]
    async fn set_next_nonce_overrides_the_cached_value() {
        let manager = ResettableNonceManager::default();
        let provider = ProviderBuilder::new().connect_anvil();
        let address = Address::ZERO;

        assert_eq!(manager.get_next_nonce(&provider, address).await.unwrap(), 0);

        manager.set_next_nonce(address, 13476).await;

        assert_eq!(
            manager.get_next_nonce(&provider, address).await.unwrap(),
            13476,
            "the seeded nonce must be used verbatim, not re-fetched"
        );
        assert_eq!(
            manager.get_next_nonce(&provider, address).await.unwrap(),
            13477,
            "subsequent sends continue from the seeded nonce"
        );
    }

    #[tokio::test]
    async fn set_next_nonce_seeds_an_address_never_fetched_before() {
        let manager = ResettableNonceManager::default();
        let provider = ProviderBuilder::new().connect_anvil();
        let address = Address::ZERO;

        manager.set_next_nonce(address, 9).await;

        assert_eq!(manager.get_next_nonce(&provider, address).await.unwrap(), 9);
    }

    #[tokio::test]
    async fn invalidation_preserves_prepared_nonce_reservations() {
        let manager = ResettableNonceManager::default();
        let provider = ProviderBuilder::new().connect_anvil();
        let address = Address::ZERO;

        let prepared = manager.get_next_nonce(&provider, address).await.unwrap();
        manager.reserve_prepared_nonce(address, prepared).await;
        manager.invalidate();

        assert_eq!(
            manager.get_next_nonce(&provider, address).await.unwrap(),
            prepared + 1
        );
    }

    #[tokio::test]
    async fn releasing_later_preparation_preserves_earlier_reservation() {
        let manager = ResettableNonceManager::default();
        let provider = ProviderBuilder::new().connect_anvil();
        let address = Address::ZERO;

        let earlier = manager.get_next_nonce(&provider, address).await.unwrap();
        manager.reserve_prepared_nonce(address, earlier).await;
        let later = manager.get_next_nonce(&provider, address).await.unwrap();
        manager.reserve_prepared_nonce(address, later).await;
        manager.release_prepared_nonce(address, later).await;

        assert_eq!(
            manager.get_next_nonce(&provider, address).await.unwrap(),
            later,
            "rolling back the later preparation must not expose the earlier reserved nonce"
        );
    }

    #[tokio::test]
    async fn releasing_earlier_preparation_skips_later_reservation_after_gap_is_filled() {
        let manager = ResettableNonceManager::default();
        let provider = ProviderBuilder::new().connect_anvil();
        let address = Address::ZERO;

        let earlier = manager.get_next_nonce(&provider, address).await.unwrap();
        manager.reserve_prepared_nonce(address, earlier).await;
        let later = manager.get_next_nonce(&provider, address).await.unwrap();
        manager.reserve_prepared_nonce(address, later).await;
        manager.release_prepared_nonce(address, earlier).await;

        assert_eq!(
            manager.get_next_nonce(&provider, address).await.unwrap(),
            earlier
        );
        assert_eq!(
            manager.get_next_nonce(&provider, address).await.unwrap(),
            later + 1,
            "the allocator must skip the still-reserved later nonce"
        );
    }

    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    #[tokio::test]
    async fn filling_released_gap_reallocates_generic_in_flight_nonce() {
        let manager = ResettableNonceManager::default();
        let in_flight = InFlightNonces::new(manager.clone());
        let provider = ProviderBuilder::new().connect_anvil();
        let address = Address::ZERO;

        let prepared = manager.get_next_nonce(&provider, address).await.unwrap();
        manager.reserve_prepared_nonce(address, prepared).await;
        let in_flight_nonce = manager.get_next_nonce(&provider, address).await.unwrap();
        in_flight.record(
            address,
            in_flight_nonce,
            alloy::primitives::TxHash::repeat_byte(0x42),
        );
        manager.release_prepared_nonce(address, prepared).await;

        assert_eq!(
            manager.get_next_nonce(&provider, address).await.unwrap(),
            prepared
        );
        assert_eq!(
            manager.get_next_nonce(&provider, address).await.unwrap(),
            in_flight_nonce,
            "a generic broadcast-but-unconfirmed nonce is not skipped: allocation \
             lands back on it so the wallet can replace its own stuck transaction"
        );
    }

    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    #[tokio::test]
    async fn invalidation_reallocates_generic_in_flight_nonce_for_replacement() {
        // Regression: a generic broadcast-but-unconfirmed nonce must remain
        // re-allocatable after invalidation. `submit::send_with_recovery`'s
        // stuck-transaction recovery depends on landing back on the wallet's
        // own stuck under-gassed nonce to get "replacement underpriced" and
        // fee-bump it; skipping it (as a prepared reservation is skipped) left
        // the next send queued behind the stuck one until it mined or the
        // process restarted.
        let manager = ResettableNonceManager::default();
        let in_flight = InFlightNonces::new(manager.clone());
        let provider = ProviderBuilder::new().connect_anvil();
        let address = Address::ZERO;

        let in_flight_nonce = manager.get_next_nonce(&provider, address).await.unwrap();
        in_flight.record(
            address,
            in_flight_nonce,
            alloy::primitives::TxHash::repeat_byte(0x42),
        );
        manager.invalidate();

        assert_eq!(
            manager.get_next_nonce(&provider, address).await.unwrap(),
            in_flight_nonce,
            "an invalidated cache must re-fetch and land back on the wallet's own \
             in-flight nonce, not skip past it"
        );
    }

    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    #[tokio::test]
    async fn final_generic_drop_rewinds_cached_allocation_to_the_released_nonce() {
        let manager = ResettableNonceManager::default();
        let in_flight = InFlightNonces::new(manager.clone());
        let provider = ProviderBuilder::new().connect_anvil();
        let address = Address::ZERO;
        let tx_hash = alloy::primitives::TxHash::repeat_byte(0x43);

        let submitted_nonce = manager.get_next_nonce(&provider, address).await.unwrap();
        in_flight.record(address, submitted_nonce, tx_hash);
        in_flight.release_hash(address, tx_hash).await;

        assert_eq!(
            manager.get_next_nonce(&provider, address).await.unwrap(),
            submitted_nonce,
            "the final generic dropped hash must make its nonce the next \
             allocation instead of leaving a permanent gap"
        );
    }

    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    #[tokio::test]
    async fn durable_drop_retains_ownership_and_skips_the_prepared_nonce() {
        let manager = ResettableNonceManager::default();
        let in_flight = InFlightNonces::new(manager.clone());
        let provider = ProviderBuilder::new().connect_anvil();
        let address = Address::ZERO;
        let tx_hash = alloy::primitives::TxHash::repeat_byte(0x44);

        let submitted_nonce = manager.get_next_nonce(&provider, address).await.unwrap();
        in_flight.record_durable(address, submitted_nonce, tx_hash);
        in_flight.release_hash(address, tx_hash).await;

        assert_eq!(
            manager.get_next_nonce(&provider, address).await.unwrap(),
            submitted_nonce + 1,
            "a dropped durable transaction must keep its nonce occupied until \
             its exact persisted bytes are rebroadcast and confirmed"
        );
    }

    #[tokio::test]
    async fn get_next_nonce_saturates_instead_of_wrapping_at_u64_max() {
        let manager = ResettableNonceManager::default();
        let provider = ProviderBuilder::new().connect_anvil();
        let address = Address::ZERO;

        manager.set_next_nonce(address, u64::MAX).await;

        let first = manager.get_next_nonce(&provider, address).await.unwrap();
        assert_eq!(first, u64::MAX);

        // The cache must saturate at `u64::MAX`, not wrap to 0 -- wrapping
        // would silently target an already-used nonce on the next send.
        let second = manager.get_next_nonce(&provider, address).await.unwrap();
        assert_eq!(
            second,
            u64::MAX,
            "the cache must saturate at u64::MAX rather than wrap to 0"
        );
    }

    #[tokio::test]
    async fn cloned_managers_share_cache() {
        let manager_a = ResettableNonceManager::default();
        let manager_b = manager_a.clone();
        let provider = ProviderBuilder::new().connect_anvil();
        let address = Address::ZERO;

        assert_eq!(
            manager_a.get_next_nonce(&provider, address).await.unwrap(),
            0
        );
        assert_eq!(
            manager_b.get_next_nonce(&provider, address).await.unwrap(),
            1
        );
        assert_eq!(
            manager_a.get_next_nonce(&provider, address).await.unwrap(),
            2
        );

        manager_b.invalidate();

        assert_eq!(
            manager_a.get_next_nonce(&provider, address).await.unwrap(),
            0
        );
    }

    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    #[tokio::test]
    async fn prepared_allocation_skips_generic_in_flight_nonce() {
        // A prepared (withdrawal) transaction is signed at a fixed nonce and
        // can never raise its fee to escape a collision, so its allocation must
        // skip a generic in flight nonce that `get_next_nonce` deliberately
        // lands back on. Signing fixed bytes onto a nonce a live generic send
        // already occupies strands them: once that generic send mines, every
        // rebroadcast is nonce too low.
        let provider = ProviderBuilder::new().connect_anvil();
        let address = Address::ZERO;

        let manager = ResettableNonceManager::default();
        let in_flight = InFlightNonces::new(manager.clone());
        let in_flight_nonce = manager.get_next_nonce(&provider, address).await.unwrap();
        in_flight.record(
            address,
            in_flight_nonce,
            alloy::primitives::TxHash::repeat_byte(0x42),
        );
        manager.invalidate();

        let prepared = manager
            .reserve_next_unheld_nonce(&provider, address)
            .await
            .unwrap();
        assert!(
            prepared > in_flight_nonce,
            "a prepared allocation must skip the generic in flight nonce \
             (prepared={prepared}, in_flight={in_flight_nonce}), not sign fixed \
             bytes onto a nonce a live generic send already holds"
        );

        // Contrast: the generic path in the identical situation still lands
        // back ON its own in flight nonce so it can replace its stuck send, the
        // deliberately unchanged behavior for a `Replaceable` hold.
        let generic_manager = ResettableNonceManager::default();
        let generic_in_flight = InFlightNonces::new(generic_manager.clone());
        let generic_nonce = generic_manager
            .get_next_nonce(&provider, address)
            .await
            .unwrap();
        generic_in_flight.record(
            address,
            generic_nonce,
            alloy::primitives::TxHash::repeat_byte(0x42),
        );
        generic_manager.invalidate();
        assert_eq!(
            generic_manager
                .get_next_nonce(&provider, address)
                .await
                .unwrap(),
            generic_nonce,
            "generic allocation is unchanged: it lands back on its own in \
             flight nonce for replacement"
        );
    }

    #[cfg(any(feature = "turnkey", feature = "local-signer"))]
    #[tokio::test]
    async fn reserve_next_unheld_nonce_floors_at_latest_over_a_lagging_pending() {
        use alloy::primitives::U64;
        use alloy::providers::mock::Asserter;

        // A lagging node behind a load balancer serves `pending` (3) below the
        // mined `latest` (7). The prepared allocation must floor at `latest`, so
        // the reserved nonce is 7, never the stale 3 it could never recover from.
        // `reserve_next_unheld_nonce` reads `pending` first, then `latest`.
        let asserter = Asserter::new();
        asserter.push_success(&U64::from(3));
        asserter.push_success(&U64::from(7));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let manager = ResettableNonceManager::default();

        let reserved = manager
            .reserve_next_unheld_nonce(&provider, Address::ZERO)
            .await
            .unwrap();
        assert_eq!(
            reserved, 7,
            "a lagging pending must not lower the prepared nonce below the mined latest"
        );
    }

    #[tokio::test]
    async fn cold_cache_reserve_does_not_seed_below_mined_latest() {
        // Regression for commit 580e71b6: `reserve_prepared_nonce` must not
        // seed a cold cache. After a restart a durable prepared transaction's
        // nonce N can sit below the chain's mined `latest` (that transaction or
        // a co-signer's transaction mined while this process was down). Seeding
        // `N + 1` would pin every following send below the mined count, where
        // it can never land; a cold fetch of `latest` must win instead.
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());
        let address = anvil.addresses()[0];

        // Mine three real self transfers so the account's mined `latest`
        // advances strictly past both the older prepared nonce N and the buggy
        // `N + 1` seed, so this test fails if the cold cache seed returns.
        for _ in 0..3 {
            provider
                .send_transaction(
                    TransactionRequest::default()
                        .from(address)
                        .to(address)
                        .value(U256::ZERO),
                )
                .await
                .unwrap()
                .get_receipt()
                .await
                .unwrap();
        }
        let mined_latest = provider
            .get_transaction_count(address)
            .latest()
            .await
            .unwrap();
        assert_eq!(
            mined_latest, 3,
            "the three mined self transfers must advance the account's latest"
        );

        // A fresh manager is a cold cache, exactly as after a process restart.
        let manager = ResettableNonceManager::default();
        let older_prepared_nonce = 0;
        manager
            .reserve_prepared_nonce(address, older_prepared_nonce)
            .await;

        assert_eq!(
            manager.get_next_nonce(&provider, address).await.unwrap(),
            mined_latest,
            "a cold cache must re-fetch the mined latest, not seed the older \
             prepared nonce + 1: seeding {older_prepared_nonce} + 1 would \
             strand every following send below the mined count"
        );
    }
}
