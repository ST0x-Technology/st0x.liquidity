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
    symbols: Arc<RwLock<BTreeMap<Address, String>>>,
    decimals: Arc<RwLock<BTreeMap<Address, u8>>>,
}

impl SymbolCache {
    /// Fetch symbol for IOV2 (V5 token input/output with token and vaultId)
    pub async fn get_iov2_symbol<P: Provider>(
        &self,
        provider: P,
        io: &IOV2,
    ) -> Result<String, OnChainError> {
        self.get_io_symbol_from_token(provider, io.token).await
    }

    /// Fetch symbol for a token address (used for IOV2 tokens that don't have embedded decimals)
    pub async fn get_io_symbol_from_token<P: Provider>(
        &self,
        provider: P,
        token: Address,
    ) -> Result<String, OnChainError> {
        let maybe_symbol = {
            let read_guard = match self.symbols.read() {
                Ok(guard) => guard,
                Err(poison) => poison.into_inner(),
            };
            read_guard.get(&token).cloned()
        };

        if let Some(symbol) = maybe_symbol {
            return Ok(symbol);
        }

        const SYMBOL_FETCH_MAX_RETRIES: usize = 3;

        let erc20 = IERC20Instance::new(token, provider);
        let symbol = (|| async { erc20.symbol().call().await })
            .retry(ExponentialBuilder::new().with_max_times(SYMBOL_FETCH_MAX_RETRIES))
            .await?;

        match self.symbols.write() {
            Ok(mut guard) => guard.insert(token, symbol.clone()),
            Err(poison) => poison.into_inner().insert(token, symbol.clone()),
        };

        Ok(symbol)
    }

    /// Fetch token decimals from cache or blockchain, caching the result for future calls.
    /// This method reduces RPC traffic in the hot ingestion path by caching decimals per token.
    pub async fn get_token_decimals<P: Provider>(
        &self,
        provider: P,
        token: Address,
    ) -> Result<u8, OnChainError> {
        // Check if we already have decimals cached for this token
        let maybe_decimals = {
            let read_guard = match self.decimals.read() {
                Ok(guard) => guard,
                Err(poison) => poison.into_inner(),
            };
            read_guard.get(&token).copied()
        };

        if let Some(decimals) = maybe_decimals {
            return Ok(decimals);
        }

        // Fetch decimals from blockchain with retries
        const DECIMALS_FETCH_MAX_RETRIES: usize = 3;

        let erc20 = IERC20Instance::new(token, provider);
        let decimals = (|| async { erc20.decimals().call().await })
            .retry(ExponentialBuilder::new().with_max_times(DECIMALS_FETCH_MAX_RETRIES))
            .await?;

        // Cache the result for future calls
        match self.decimals.write() {
            Ok(mut guard) => guard.insert(token, decimals),
            Err(poison) => poison.into_inner().insert(token, decimals),
        };

        Ok(decimals)
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
        let token = address!("0x1234567890123456789012345678901234567890");

        cache
            .symbols
            .write()
            .expect("Test cache lock poisoned")
            .insert(token, "TEST".to_string());

        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let result = cache
            .get_io_symbol_from_token(provider, token)
            .await
            .unwrap();
        assert_eq!(result, "TEST");
    }

    #[tokio::test]
    async fn test_symbol_cache_miss_rpc_failure() {
        let cache = SymbolCache::default();
        let token = address!("0x1234567890123456789012345678901234567890");

        let asserter = Asserter::new();
        asserter.push_failure_msg("RPC failure");
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let result = cache.get_io_symbol_from_token(provider, token).await;
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Alloy(crate::error::AlloyError::GetSymbol(_))
        ));
    }

    #[tokio::test]
    async fn test_decimals_cache_hit() {
        let cache = SymbolCache::default();
        let token = address!("0x1234567890123456789012345678901234567890");

        cache
            .decimals
            .write()
            .expect("Test cache lock poisoned")
            .insert(token, 18);

        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let result = cache.get_token_decimals(provider, token).await.unwrap();
        assert_eq!(result, 18);
    }

    #[tokio::test]
    async fn test_decimals_cache_miss_rpc_failure() {
        let cache = SymbolCache::default();
        let token = address!("0x1234567890123456789012345678901234567890");

        let asserter = Asserter::new();
        asserter.push_failure_msg("RPC failure");
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let result = cache.get_token_decimals(provider, token).await;
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Alloy(crate::error::AlloyError::GetSymbol(_))
        ));
    }
}
