use alloy::{primitives::Address, providers::Provider};
use backon::{ExponentialBuilder, Retryable};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use crate::bindings::{IOrderBookV5::IOV2, IERC20::IERC20Instance};
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;
    use alloy::providers::{mock::Asserter, ProviderBuilder};

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
}
