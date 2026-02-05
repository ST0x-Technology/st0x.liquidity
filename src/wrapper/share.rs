//! WrapperService implementation for ERC-4626 token wrapping/unwrapping.

use std::collections::HashSet;
use std::sync::RwLock;

use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::Provider;
use alloy::sol_types::SolEvent;
use async_trait::async_trait;
use st0x_execution::{EmptySymbolError, Symbol};

use super::Wrapper;
use crate::bindings::{IERC20, IERC4626};
use crate::vault::{
    RatioCache, UnderlyingPerWrapped, WrappedTokenConfig, WrappedTokenRegistry, WrapperError,
};

/// One unit with 18 decimals for ratio queries.
const RATIO_QUERY_AMOUNT: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);

/// Maximum approval amount (type(uint256).max).
const MAX_APPROVAL: U256 = U256::MAX;

/// Service for managing ERC-4626 token wrapping/unwrapping operations.
///
/// Provides methods for:
/// - Fetching conversion ratios (with caching)
/// - Wrapping tokens (deposit into vault)
/// - Unwrapping tokens (redeem from vault)
/// - Approval management
pub(crate) struct WrapperService<P>
where
    P: Provider + Clone,
{
    provider: P,
    owner: Address,
    cache: RatioCache,
    registry: WrappedTokenRegistry,
    /// Tracks which (unwrapped_token, spender) pairs have been approved.
    approvals: RwLock<HashSet<(Address, Address)>>,
}

impl<P> WrapperService<P>
where
    P: Provider + Clone,
{
    /// Creates a new WrapperService with the hardcoded registry.
    pub(crate) fn new(provider: P, owner: Address) -> Result<Self, EmptySymbolError> {
        Ok(Self {
            provider,
            owner,
            cache: RatioCache::new(),
            registry: WrappedTokenRegistry::hardcoded()?,
            approvals: RwLock::new(HashSet::new()),
        })
    }

    pub(crate) fn with_registry(mut self, registry: WrappedTokenRegistry) -> Self {
        self.registry = registry;
        self
    }

    /// Pre-seeds the ratio cache for a wrapped token address.
    #[cfg(test)]
    pub(crate) fn seed_ratio(&self, wrapped_token: Address, ratio: UnderlyingPerWrapped) {
        self.cache.seed(wrapped_token, ratio);
    }

    /// Fetches the current conversion ratio for a wrapped token.
    ///
    /// Uses a 2-second cache to deduplicate burst calls during rapid event processing.
    async fn get_ratio(&self, wrapped_token: Address) -> Result<UnderlyingPerWrapped, WrapperError> {
        if let Some(ratio) = self.cache.get(&wrapped_token) {
            return Ok(ratio);
        }

        let vault = IERC4626::new(wrapped_token, &self.provider);
        let assets_per_share = vault.convertToAssets(RATIO_QUERY_AMOUNT).call().await?;

        let ratio = UnderlyingPerWrapped::new(assets_per_share)?;
        self.cache.set(wrapped_token, ratio);

        Ok(ratio)
    }

    /// Gets the wrapped token config for a symbol.
    pub(crate) fn get_config_by_symbol(&self, symbol: &Symbol) -> Option<&WrappedTokenConfig> {
        self.registry.get_by_symbol(symbol)
    }

    /// Gets the wrapped token config for a wrapped token address.
    pub(crate) fn get_config_by_wrapped(&self, wrapped: &Address) -> Option<&WrappedTokenConfig> {
        self.registry.get_by_wrapped(wrapped)
    }

    /// Ensures the vault has approval to spend the unwrapped tokens.
    ///
    /// Approves `type(uint256).max` on first call for a given vault.
    /// Subsequent calls are no-ops (tracked in memory).
    pub(crate) async fn ensure_approval(
        &self,
        unwrapped_token: Address,
        spender: Address,
    ) -> Result<(), WrapperError> {
        let key = (unwrapped_token, spender);

        {
            let approvals = match self.approvals.read() {
                Ok(guard) => guard,
                Err(poison) => poison.into_inner(),
            };

            if approvals.contains(&key) {
                return Ok(());
            }
        }

        let token = IERC20::new(unwrapped_token, &self.provider);
        let pending = token.approve(spender, MAX_APPROVAL).send().await?;
        pending.get_receipt().await?;

        {
            let mut approvals = match self.approvals.write() {
                Ok(guard) => guard,
                Err(poison) => poison.into_inner(),
            };

            approvals.insert(key);
        }

        Ok(())
    }
}

#[async_trait]
impl<P> Wrapper for WrapperService<P>
where
    P: Provider + Clone + Send + Sync,
{
    async fn get_ratio_for_symbol(
        &self,
        symbol: &Symbol,
    ) -> Result<UnderlyingPerWrapped, WrapperError> {
        let config = self
            .get_config_by_symbol(symbol)
            .ok_or_else(|| WrapperError::SymbolNotInRegistry(symbol.clone()))?;

        self.get_ratio(config.wrapped_token).await
    }

    async fn to_wrapped(
        &self,
        wrapped_token: Address,
        underlying_amount: U256,
        receiver: Address,
    ) -> Result<(TxHash, U256), WrapperError> {
        let vault = IERC4626::new(wrapped_token, &self.provider);

        let pending = vault.deposit(underlying_amount, receiver).send().await?;
        let receipt = pending.get_receipt().await?;
        let tx_hash = receipt.transaction_hash;

        let actual_shares = receipt
            .inner
            .logs()
            .iter()
            .filter(|log| log.address() == wrapped_token)
            .find_map(|log| {
                IERC4626::Deposit::decode_log(log.as_ref())
                    .ok()
                    .map(|event| event.data.shares)
            })
            .ok_or(WrapperError::MissingDepositEvent)?;

        self.cache.invalidate(&wrapped_token);

        Ok((tx_hash, actual_shares))
    }

    async fn to_underlying(
        &self,
        wrapped_token: Address,
        wrapped_amount: U256,
        receiver: Address,
        owner: Address,
    ) -> Result<(TxHash, U256), WrapperError> {
        let vault = IERC4626::new(wrapped_token, &self.provider);

        let pending = vault.redeem(wrapped_amount, receiver, owner).send().await?;
        let receipt = pending.get_receipt().await?;
        let tx_hash = receipt.transaction_hash;

        let actual_assets = receipt
            .inner
            .logs()
            .iter()
            .filter(|log| log.address() == wrapped_token)
            .find_map(|log| {
                IERC4626::Withdraw::decode_log(log.as_ref())
                    .ok()
                    .map(|event| event.data.assets)
            })
            .ok_or(WrapperError::MissingWithdrawEvent)?;

        self.cache.invalidate(&wrapped_token);

        Ok((tx_hash, actual_assets))
    }

    fn owner(&self) -> Address {
        self.owner
    }
}
