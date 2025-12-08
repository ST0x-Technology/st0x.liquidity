use alloy::{primitives::Address, providers::Provider};
use backon::{ExponentialBuilder, Retryable};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use crate::bindings::{IERC20::IERC20Instance, IOrderBookV5::IOV2};
use crate::error::OnChainError;

#[derive(Debug, Default, Clone)]
pub(crate) struct SymbolCache {
    map: Arc<RwLock<BTreeMap<Address, String>>>,
}

impl SymbolCache {
    pub async fn get_io_symbol<P: Provider>(
        &self,
        provider: P,
        io: &IOV2,
    ) -> Result<String, OnChainError> {
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

        let erc20 = IERC20Instance::new(io.token, provider);
        let symbol = (|| async { erc20.symbol().call().await })
            .retry(ExponentialBuilder::new().with_max_times(SYMBOL_FETCH_MAX_RETRIES))
            .await?;

        match self.map.write() {
            Ok(mut guard) => guard.insert(io.token, symbol.clone()),
            Err(poison) => poison.into_inner().insert(io.token, symbol.clone()),
        };

        Ok(symbol)
    }

    /// Reverse lookup: find token address by symbol.
    pub(crate) fn get_address(&self, symbol: &str) -> Option<Address> {
        let read_guard = match self.map.read() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };

        read_guard
            .iter()
            .find(|(_, s)| s.as_str() == symbol)
            .map(|(addr, _)| *addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;
    use alloy::providers::{ProviderBuilder, mock::Asserter};

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
            vaultId: alloy::primitives::B256::ZERO,
        };

        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let result = cache.get_io_symbol(provider, &io).await.unwrap();
        assert_eq!(result, "TEST");
    }

    #[tokio::test]
    async fn test_symbol_cache_miss_rpc_failure() {
        let cache = SymbolCache::default();
        let address = address!("0x1234567890123456789012345678901234567890");

        let io = IOV2 {
            token: address,
            vaultId: alloy::primitives::B256::ZERO,
        };

        let asserter = Asserter::new();
        asserter.push_failure_msg("RPC failure");
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let result = cache.get_io_symbol(provider, &io).await;
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Alloy(crate::error::AlloyError::GetSymbol(_))
        ));
    }

    #[test]
    fn test_get_address_returns_correct_address() {
        let cache = SymbolCache::default();
        let addr = address!("0x1234567890123456789012345678901234567890");

        cache
            .map
            .write()
            .expect("Test cache lock poisoned")
            .insert(addr, "AAPL".to_string());

        let result = cache.get_address("AAPL");
        assert_eq!(result, Some(addr));
    }

    #[test]
    fn test_get_address_returns_none_for_unknown_symbol() {
        let cache = SymbolCache::default();
        let addr = address!("0x1234567890123456789012345678901234567890");

        cache
            .map
            .write()
            .expect("Test cache lock poisoned")
            .insert(addr, "AAPL".to_string());

        let result = cache.get_address("UNKNOWN");
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_address_works_with_multiple_entries() {
        let cache = SymbolCache::default();
        let aapl_addr = address!("0x1111111111111111111111111111111111111111");
        let msft_addr = address!("0x2222222222222222222222222222222222222222");
        let goog_addr = address!("0x3333333333333333333333333333333333333333");

        {
            let mut guard = cache.map.write().expect("Test cache lock poisoned");
            guard.insert(aapl_addr, "AAPL".to_string());
            guard.insert(msft_addr, "MSFT".to_string());
            guard.insert(goog_addr, "GOOG".to_string());
        }

        assert_eq!(cache.get_address("AAPL"), Some(aapl_addr));
        assert_eq!(cache.get_address("MSFT"), Some(msft_addr));
        assert_eq!(cache.get_address("GOOG"), Some(goog_addr));
        assert_eq!(cache.get_address("TSLA"), None);
    }
}
