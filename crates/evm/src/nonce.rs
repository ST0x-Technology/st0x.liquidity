//! Resettable nonce manager for concurrent transaction submission.
//!
//! Alloy's [`CachedNonceManager`] caches nonces locally and never
//! re-fetches from the RPC after initialization. When external
//! processes (e.g. CLI commands) submit transactions from the same
//! wallet address, the cache becomes stale and every subsequent send
//! fails with "nonce too low".
//!
//! [`ResettableNonceManager`] behaves identically to
//! `CachedNonceManager` but exposes two recovery hooks:
//!
//! - [`invalidate()`] clears the cache, forcing the next send to
//!   re-fetch the nonce from the chain via the **latest** (mined)
//!   transaction count, not `pending`. `submit.rs`'s stuck-transaction
//!   recovery (failure mode 2) depends on the next send landing back on
//!   the nonce a stuck pending transaction occupies and being rejected
//!   with "replacement transaction underpriced", which is what re-arms
//!   the fee-bump loop; a `pending` re-fetch would instead return the
//!   nonce *after* the stuck one (since `pending` is mempool-aware, see
//!   `submit::TxSubmitter::pending_nonce`'s doc comment), so the send
//!   would be accepted and merely queue behind it, and the replacement
//!   would never happen.
//! - [`set_next_nonce()`] seeds the cache with a known-good nonce,
//!   skipping the re-fetch entirely. Nonce-too-low recovery in
//!   `submit::retry_after_nonce_too_low` uses this to seed a
//!   pending-aware target nonce it computes itself (the higher of the
//!   node's reported next nonce and its own `pending` read), since the
//!   cold-cache re-fetch above intentionally stays on `latest`.
//!
//! [`CachedNonceManager`]: alloy::providers::fillers::CachedNonceManager
//! [`invalidate()`]: ResettableNonceManager::invalidate
//! [`set_next_nonce()`]: ResettableNonceManager::set_next_nonce

use alloy::network::Network;
use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::providers::fillers::NonceManager;
use alloy::transports::TransportResult;
use async_trait::async_trait;
use dashmap::DashMap;
use futures::lock::Mutex;
use std::sync::Arc;
use tracing::trace;

/// Nonce manager that caches nonces locally and supports cache
/// invalidation and seeding for resilience against external nonce
/// changes.
///
/// Each entry holds the nonce the *next* send from that address should
/// use, or `None` when it must be fetched from the RPC.
#[derive(Clone, Debug, Default)]
pub struct ResettableNonceManager {
    nonces: Arc<DashMap<Address, Arc<Mutex<Option<u64>>>>>,
}

impl ResettableNonceManager {
    /// Clears all cached nonces, forcing the next transaction to
    /// re-fetch the latest nonce from the RPC provider.
    ///
    /// Race note: if a concurrent `get_next_nonce` has already cloned the
    /// per-address `Arc<Mutex>` but not yet locked it, it will write to
    /// an orphaned mutex while a new entry is created for subsequent
    /// callers. This can cause a one-time nonce collision under concurrent
    /// sends during invalidation. The wallet send path closes this window
    /// by serializing all sends from one wallet behind a mutex (see
    /// `submit::send_with_recovery`), so only one send assigns a nonce at
    /// a time; this note documents the manager's behavior in isolation.
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

    /// The per-address cache slot, created empty on first access.
    fn slot(&self, address: Address) -> Arc<Mutex<Option<u64>>> {
        let entry = self
            .nonces
            .entry(address)
            .or_insert_with(|| Arc::new(Mutex::new(None)));

        Arc::clone(entry.value())
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

        let next_nonce = if let Some(next_nonce) = *cached {
            trace!(%address, next_nonce, "using cached nonce");
            next_nonce
        } else {
            trace!(%address, "fetching latest nonce from RPC");
            // Explicit rather than relying on alloy's default block tag: the
            // module doc above depends on this re-fetch returning the
            // `latest` (mined) count, not `pending`, so that a stuck
            // pending transaction's nonce is what gets re-targeted. Pinning
            // it here keeps that dependency visible in code rather than
            // resting on an upstream default that a future alloy release
            // could silently change.
            provider.get_transaction_count(address).latest().await?
        };

        // Saturate rather than wrap: reaching `u64::MAX` here means the
        // account's nonce space is exhausted, an unreachable condition in
        // practice (it would take more transactions than any EOA could
        // ever send). Wrapping to 0 would silently target an
        // already-used nonce on the next send; saturating instead leaves
        // every later send targeting `u64::MAX`, which the node will
        // reject loudly rather than corrupting the cache in silence.
        *cached = Some(next_nonce.saturating_add(1));
        drop(cached);

        Ok(next_nonce)
    }
}

#[cfg(test)]
mod tests {
    use alloy::providers::ProviderBuilder;

    use super::*;

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
