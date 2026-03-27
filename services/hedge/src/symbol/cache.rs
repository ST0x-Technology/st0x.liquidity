//! Thread-safe cache for ERC20 token symbol lookups.
//!
//! Prevents repeated RPC calls by caching the mapping from token addresses
//! to their symbol strings.

use alloy::primitives::Address;
use backon::{ExponentialBuilder, Retryable};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use st0x_evm::{Evm, OpenChainErrorRegistry};

use crate::bindings::{IERC20, IOrderBookV6::IOV2};
use crate::onchain::OnChainError;

#[derive(Debug, Default, Clone)]
pub(crate) struct SymbolCache {
    pub(crate) map: Arc<RwLock<BTreeMap<Address, String>>>,
}

impl SymbolCache {
    pub async fn get_io_symbol<E: Evm>(&self, evm: &E, io: &IOV2) -> Result<String, OnChainError> {
        let maybe_symbol = {
            let read_guard = match self.map.read() {
                Ok(guard) => guard,
                Err(poison) => poison.into_inner(),
            };
            read_guard.get(&io.token).cloned()
        };

        if let Some(symbol) = maybe_symbol {
            return Ok(symbol);
        }

        const SYMBOL_FETCH_MAX_RETRIES: usize = 3;

        let token = io.token;
        let symbol: String = (|| async {
            evm.call::<OpenChainErrorRegistry, _>(token, IERC20::symbolCall {})
                .await
        })
        .retry(ExponentialBuilder::new().with_max_times(SYMBOL_FETCH_MAX_RETRIES))
        .await?;

        match self.map.write() {
            Ok(mut guard) => guard.insert(io.token, symbol.clone()),
            Err(poison) => poison.into_inner().insert(io.token, symbol.clone()),
        };

        Ok(symbol)
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, address};
    use alloy::providers::{ProviderBuilder, mock::Asserter};

    use st0x_evm::ReadOnlyEvm;

    use super::*;

    #[tokio::test]
    async fn test_symbol_cache_hit() {
        let cache = SymbolCache::default();
        let address = address!("0x1234567890123456789012345678901234567890");

        cache
            .map
            .write()
            .expect("Test cache lock poisoned")
            .insert(address, "TEST".to_string());

        let io = IOV2 {
            token: address,
            vaultId: B256::ZERO,
        };

        let asserter = Asserter::new();
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));
        assert_eq!(cache.get_io_symbol(&evm, &io).await.unwrap(), "TEST");
    }

    #[tokio::test]
    async fn test_symbol_cache_miss_rpc_failure() {
        let cache = SymbolCache::default();
        let address = address!("0x1234567890123456789012345678901234567890");

        let io = IOV2 {
            token: address,
            vaultId: B256::ZERO,
        };

        let asserter = Asserter::new();
        asserter.push_failure_msg("RPC failure");
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));
        assert!(matches!(
            cache.get_io_symbol(&evm, &io).await.unwrap_err(),
            OnChainError::Evm(_)
        ));
    }
}
