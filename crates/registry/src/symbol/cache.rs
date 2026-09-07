//! Thread-safe cache for ERC20 token symbol lookups.
//!
//! Prevents repeated RPC calls by caching the mapping from token addresses
//! to their symbol strings.

use alloy::primitives::Address;
use backon::{ExponentialBuilder, Retryable};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, RwLock},
};
use tokio::sync::{Mutex, RwLock as AsyncRwLock};

use st0x_evm::{Chain, Evm, EvmError, IERC20, OpenChainErrorRegistry};

/// One fetch mutex per (chain, token), so concurrent misses for the same
/// token share a single RPC call without serializing unrelated lookups.
type FetchLocks = Arc<AsyncRwLock<HashMap<(Chain, Address), Arc<Mutex<()>>>>>;

#[derive(Debug, Default, Clone)]
pub struct SymbolCache {
    // Keyed by (chain, address): deterministic deployments make the same
    // address on two chains a different token, so a chain-blind cache would
    // serve one chain's symbol for another chain's contract.
    map: Arc<RwLock<BTreeMap<(Chain, Address), String>>>,
    fetch_locks: FetchLocks,
}

impl SymbolCache {
    /// Resolves the ERC20 symbol for `token`, fetching it onchain on a cache
    /// miss and memoizing the result for subsequent lookups.
    pub async fn resolve_symbol<E: Evm>(
        &self,
        evm: &E,
        chain: Chain,
        token: Address,
    ) -> Result<String, EvmError> {
        if let Some(symbol) = self.cached_symbol(chain, token) {
            return Ok(symbol);
        }

        let fetch_lock = self.fetch_lock_for(chain, token).await;
        let _fetch_guard = fetch_lock.lock().await;

        if let Some(symbol) = self.cached_symbol(chain, token) {
            return Ok(symbol);
        }

        const SYMBOL_FETCH_MAX_RETRIES: usize = 3;

        let symbol: String = (|| async {
            evm.call::<OpenChainErrorRegistry, _>(token, IERC20::symbolCall {})
                .await
        })
        .retry(ExponentialBuilder::new().with_max_times(SYMBOL_FETCH_MAX_RETRIES))
        .await?;

        self.store_symbol(chain, token, symbol.clone());
        Ok(symbol)
    }

    /// Inserts a known symbol without an RPC lookup.
    ///
    /// Useful when callers already know the mapping, for example when seeding
    /// from persisted vault metadata or preloading deterministic test fixtures.
    pub fn preload_symbol(&self, chain: Chain, token: Address, symbol: impl Into<String>) {
        self.store_symbol(chain, token, symbol.into());
    }

    fn cached_symbol(&self, chain: Chain, token: Address) -> Option<String> {
        let read_guard = match self.map.read() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };
        read_guard.get(&(chain, token)).cloned()
    }

    fn store_symbol(&self, chain: Chain, token: Address, symbol: String) {
        match self.map.write() {
            Ok(mut guard) => {
                guard.insert((chain, token), symbol);
            }
            Err(poison) => {
                poison.into_inner().insert((chain, token), symbol);
            }
        }
    }

    async fn fetch_lock_for(&self, chain: Chain, token: Address) -> Arc<Mutex<()>> {
        {
            let fetch_locks = self.fetch_locks.read().await;
            if let Some(fetch_lock) = fetch_locks.get(&(chain, token)) {
                return fetch_lock.clone();
            }
        }

        let mut fetch_locks = self.fetch_locks.write().await;
        fetch_locks
            .entry((chain, token))
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use alloy::providers::{ProviderBuilder, mock::Asserter};
    use alloy::sol_types::SolCall;

    /// Deterministic deployments make the same address a different token per
    /// chain: entries must never leak across chains.
    #[test]
    fn same_address_resolves_independently_per_chain() {
        let cache = SymbolCache::default();
        let token = Address::repeat_byte(0x42);

        cache.preload_symbol(Chain::Base, token, "wtAAPL");
        cache.preload_symbol(Chain::Ethereum, token, "wtCOIN");

        assert_eq!(
            cache.cached_symbol(Chain::Base, token).as_deref(),
            Some("wtAAPL")
        );
        assert_eq!(
            cache.cached_symbol(Chain::Ethereum, token).as_deref(),
            Some("wtCOIN")
        );
        assert_eq!(cache.cached_symbol(Chain::HyperEvm, token), None);
    }

    use st0x_evm::{IERC20::symbolCall, ReadOnlyEvm};

    use super::*;

    #[tokio::test]
    async fn test_symbol_cache_hit() {
        let cache = SymbolCache::default();
        let token = address!("0x1234567890123456789012345678901234567890");

        cache.preload_symbol(Chain::Base, token, "TEST");

        let asserter = Asserter::new();
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));
        assert_eq!(
            cache
                .resolve_symbol(&evm, Chain::Base, token)
                .await
                .unwrap(),
            "TEST"
        );
    }

    #[tokio::test]
    async fn test_symbol_cache_miss_rpc_failure() {
        let cache = SymbolCache::default();
        let token = address!("0x1234567890123456789012345678901234567890");

        let asserter = Asserter::new();
        asserter.push_failure_msg("RPC failure");
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));
        assert!(matches!(
            cache
                .resolve_symbol(&evm, Chain::Base, token)
                .await
                .unwrap_err(),
            EvmError::Contract(_)
        ));
    }

    #[tokio::test]
    async fn concurrent_misses_for_same_token_share_one_rpc_call() {
        let cache = SymbolCache::default();
        let token = address!("0x1234567890123456789012345678901234567890");

        let asserter = Asserter::new();
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"TEST".to_string(),
        ));
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));

        let (first, second, third) = tokio::join!(
            cache.resolve_symbol(&evm, Chain::Base, token),
            cache.resolve_symbol(&evm, Chain::Base, token),
            cache.resolve_symbol(&evm, Chain::Base, token),
        );

        assert_eq!(first.unwrap(), "TEST");
        assert_eq!(second.unwrap(), "TEST");
        assert_eq!(third.unwrap(), "TEST");
    }
}
