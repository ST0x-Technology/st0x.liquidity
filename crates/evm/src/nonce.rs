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
//! - The occupancy set protects both prepared transactions and accepted
//!   in-flight transactions. Allocation skips occupied values after every
//!   cache seed or invalidation, and ownership is released only after a
//!   definitive receipt, drop, or discard-before-broadcast decision.
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
use std::collections::BTreeSet;
use std::sync::Arc;
use tracing::trace;

/// Nonce manager that caches nonces locally and supports cache
/// invalidation and seeding for resilience against external nonce
/// changes.
///
/// Each nonce entry holds the nonce the *next* send from that address should
/// use, or `None` when it must be fetched from the RPC. Occupied nonces are
/// tracked separately so cache invalidation can discard stale RPC-derived
/// state without erasing prepared or broadcast-but-unconfirmed ownership.
#[derive(Clone, Debug, Default)]
pub struct ResettableNonceManager {
    nonces: Arc<DashMap<Address, Arc<Mutex<Option<u64>>>>>,
    occupied: Arc<DashMap<Address, BTreeSet<u64>>>,
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

    /// Reserves a prepared transaction's nonce and raises the cache past it
    /// without lowering an already-higher allocation.
    #[cfg(any(feature = "turnkey", feature = "local-signer", test))]
    pub(crate) async fn reserve_prepared_nonce(&self, address: Address, nonce: u64) {
        let slot = self.slot(address);
        let mut cached = slot.lock().await;
        self.occupy_nonce(address, nonce);
        let next = nonce.saturating_add(1);
        *cached = Some(cached.map_or(next, |current| current.max(next)));
    }

    /// Releases a prepared nonce that will never be broadcast. The cache is
    /// rewound only as far as that nonce; every other prepared or in-flight
    /// nonce remains protected and is skipped by `get_next_nonce`.
    #[cfg(any(feature = "turnkey", feature = "local-signer", test))]
    pub(crate) async fn release_prepared_nonce(&self, address: Address, nonce: u64) {
        let slot = self.slot(address);
        let mut cached = slot.lock().await;
        if self.release_occupied_nonce(address, nonce) {
            *cached = Some(cached.map_or(nonce, |current| current.min(nonce)));
        }
    }

    /// Marks a nonce occupied by a prepared or broadcast-but-unconfirmed
    /// transaction. Ownership is released only after a definitive receipt or
    /// drop decision.
    #[cfg(any(feature = "turnkey", feature = "local-signer", test))]
    pub(crate) fn occupy_nonce(&self, address: Address, nonce: u64) {
        self.occupied.entry(address).or_default().insert(nonce);
    }

    /// Releases one definitively resolved nonce.
    #[cfg(any(feature = "turnkey", feature = "local-signer", test))]
    pub(crate) fn release_occupied_nonce(&self, address: Address, nonce: u64) -> bool {
        self.occupied
            .get_mut(&address)
            .is_some_and(|mut occupied| occupied.remove(&nonce))
    }

    /// The per-address cache slot, created empty on first access.
    fn slot(&self, address: Address) -> Arc<Mutex<Option<u64>>> {
        let entry = self
            .nonces
            .entry(address)
            .or_insert_with(|| Arc::new(Mutex::new(None)));

        Arc::clone(entry.value())
    }

    /// Whether a prepared or broadcast-but-unconfirmed transaction currently
    /// owns `nonce`.
    fn is_occupied(&self, address: Address, nonce: u64) -> bool {
        self.occupied
            .get(&address)
            .is_some_and(|occupied| occupied.contains(&nonce))
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
        while self.is_occupied(address, next_nonce) {
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
    use crate::inflight_nonces::InFlightNonces;

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

    #[tokio::test]
    async fn filling_released_gap_skips_higher_in_flight_nonce() {
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
            in_flight_nonce + 1,
            "after filling the released gap, allocation must skip the higher \
             broadcast-but-unconfirmed nonce"
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
}
