//! ERC-4626 vault operations for wrapped token management.
//!
//! This module provides services for interacting with ERC-4626 tokenized vaults
//! to wrap and unwrap equity tokens. The wrapped tokens accrue value over time
//! due to stock splits and dividend reinvestment.

mod cache;
mod config;
mod ratio;

use std::collections::HashSet;
use std::sync::RwLock;

use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::Provider;

use crate::bindings::{IERC20, IERC4626};

pub(crate) use self::config::{WrappedTokenConfig, WrappedTokenRegistry};
pub(crate) use self::ratio::{VaultRatio, VaultRatioError};

use self::cache::RatioCache;

/// One unit with 18 decimals for ratio queries.
const RATIO_QUERY_AMOUNT: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);

/// Maximum approval amount (type(uint256).max).
const MAX_APPROVAL: U256 = U256::MAX;

#[derive(Debug, thiserror::Error)]
pub(crate) enum VaultError {
    #[error("Contract call failed: {0}")]
    Contract(#[from] alloy::contract::Error),

    #[error("Transaction failed: {0}")]
    Transaction(#[from] alloy::providers::PendingTransactionError),

    #[error("Ratio error: {0}")]
    Ratio(#[from] VaultRatioError),

    #[error("Wrapped token not configured: {0}")]
    TokenNotConfigured(Address),

    #[error("Slippage exceeded: expected {expected}, got {actual}")]
    SlippageExceeded { expected: U256, actual: U256 },
}

/// Service for managing ERC-4626 vault operations.
///
/// Provides methods for:
/// - Fetching conversion ratios (with caching)
/// - Wrapping tokens (deposit into vault)
/// - Unwrapping tokens (redeem from vault)
/// - Approval management
pub(crate) struct VaultService<P>
where
    P: Provider + Clone,
{
    provider: P,
    cache: RatioCache,
    registry: WrappedTokenRegistry,
    /// Tracks which (unwrapped_token, spender) pairs have been approved.
    approvals: RwLock<HashSet<(Address, Address)>>,
}

impl<P> VaultService<P>
where
    P: Provider + Clone,
{
    /// Creates a new VaultService with the given provider and registry.
    pub(crate) fn new(provider: P, registry: WrappedTokenRegistry) -> Self {
        Self {
            provider,
            cache: RatioCache::new(),
            registry,
            approvals: RwLock::new(HashSet::new()),
        }
    }

    /// Fetches the current conversion ratio for a wrapped token.
    ///
    /// Uses a 2-second cache to deduplicate burst calls during rapid event processing.
    /// The ratio represents assets per share (underlying per wrapped).
    pub(crate) async fn get_ratio(&self, wrapped_token: Address) -> Result<VaultRatio, VaultError> {
        // Check cache first
        if let Some(ratio) = self.cache.get(&wrapped_token) {
            return Ok(ratio);
        }

        // Fetch from contract
        let vault = IERC4626::new(wrapped_token, &self.provider);
        let assets_per_share = vault.convertToAssets(RATIO_QUERY_AMOUNT).call().await?;

        let ratio = VaultRatio::new(assets_per_share)?;

        // Cache the result
        self.cache.set(wrapped_token, ratio);

        Ok(ratio)
    }

    /// Wraps tokens by depositing into the ERC-4626 vault.
    ///
    /// # Arguments
    ///
    /// * `wrapped_token` - The vault (wrapped token) address
    /// * `unwrapped_amount` - Amount of underlying tokens to deposit
    /// * `receiver` - Address to receive the wrapped shares
    ///
    /// # Returns
    ///
    /// Transaction hash and the amount of wrapped shares received.
    pub(crate) async fn wrap(
        &self,
        wrapped_token: Address,
        unwrapped_amount: U256,
        receiver: Address,
    ) -> Result<(TxHash, U256), VaultError> {
        let vault = IERC4626::new(wrapped_token, &self.provider);

        // Preview the deposit to get expected shares
        let expected_shares = vault.previewDeposit(unwrapped_amount).call().await?;

        // Execute the deposit
        let pending = vault.deposit(unwrapped_amount, receiver).send().await?;

        let receipt = pending.get_receipt().await?;
        let tx_hash = receipt.transaction_hash;

        // Invalidate cache since vault state changed
        self.cache.invalidate(&wrapped_token);

        Ok((tx_hash, expected_shares))
    }

    /// Unwraps tokens by redeeming from the ERC-4626 vault.
    ///
    /// # Arguments
    ///
    /// * `wrapped_token` - The vault (wrapped token) address
    /// * `wrapped_amount` - Amount of wrapped shares to redeem
    /// * `receiver` - Address to receive the underlying assets
    /// * `owner` - Owner of the wrapped shares
    ///
    /// # Returns
    ///
    /// Transaction hash and the amount of underlying assets received.
    pub(crate) async fn unwrap(
        &self,
        wrapped_token: Address,
        wrapped_amount: U256,
        receiver: Address,
        owner: Address,
    ) -> Result<(TxHash, U256), VaultError> {
        let vault = IERC4626::new(wrapped_token, &self.provider);

        // Preview the redemption to get expected assets
        let expected_assets = vault.previewRedeem(wrapped_amount).call().await?;

        // Execute the redemption
        let pending = vault.redeem(wrapped_amount, receiver, owner).send().await?;

        let receipt = pending.get_receipt().await?;
        let tx_hash = receipt.transaction_hash;

        // Invalidate cache since vault state changed
        self.cache.invalidate(&wrapped_token);

        Ok((tx_hash, expected_assets))
    }

    /// Ensures the vault has approval to spend the unwrapped tokens.
    ///
    /// Approves `type(uint256).max` on first call for a given vault.
    /// Subsequent calls are no-ops (tracked in memory).
    pub(crate) async fn ensure_approval(
        &self,
        unwrapped_token: Address,
        spender: Address,
    ) -> Result<(), VaultError> {
        let key = (unwrapped_token, spender);

        // Check if already approved (in memory)
        {
            let approvals = match self.approvals.read() {
                Ok(guard) => guard,
                Err(poison) => poison.into_inner(),
            };

            if approvals.contains(&key) {
                return Ok(());
            }
        }

        // Approve max amount
        let token = IERC20::new(unwrapped_token, &self.provider);
        let pending = token.approve(spender, MAX_APPROVAL).send().await?;
        pending.get_receipt().await?;

        // Record approval
        {
            let mut approvals = match self.approvals.write() {
                Ok(guard) => guard,
                Err(poison) => poison.into_inner(),
            };

            approvals.insert(key);
        }

        Ok(())
    }

    /// Checks if an address is a configured wrapped token.
    pub(crate) fn is_wrapped(&self, address: &Address) -> bool {
        self.registry.is_wrapped(address)
    }

    /// Checks if an address is a configured unwrapped token.
    pub(crate) fn is_unwrapped(&self, address: &Address) -> bool {
        self.registry.is_unwrapped(address)
    }

    /// Gets the wrapped token config for a symbol.
    pub(crate) fn get_config_by_symbol(
        &self,
        symbol: &st0x_execution::Symbol,
    ) -> Option<&WrappedTokenConfig> {
        self.registry.get_by_symbol(symbol)
    }

    /// Gets the wrapped token config for a wrapped token address.
    pub(crate) fn get_config_by_wrapped(&self, wrapped: &Address) -> Option<&WrappedTokenConfig> {
        self.registry.get_by_wrapped(wrapped)
    }

    /// Gets the wrapped token config for an unwrapped token address.
    pub(crate) fn get_config_by_unwrapped(
        &self,
        unwrapped: &Address,
    ) -> Option<&WrappedTokenConfig> {
        self.registry.get_by_unwrapped(unwrapped)
    }

    /// Returns a reference to the registry.
    pub(crate) fn registry(&self) -> &WrappedTokenRegistry {
        &self.registry
    }

    /// Cleans up expired cache entries.
    pub(crate) fn cleanup_cache(&self) {
        self.cache.cleanup_expired();
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;

    use super::*;

    fn create_test_registry() -> WrappedTokenRegistry {
        let config = WrappedTokenConfig {
            equity_symbol: st0x_execution::Symbol::new("AAPL").unwrap(),
            wrapped_token: address!("1111111111111111111111111111111111111111"),
            unwrapped_token: address!("2222222222222222222222222222222222222222"),
        };

        WrappedTokenRegistry::new(vec![config])
    }

    #[test]
    fn is_wrapped_delegates_to_registry() {
        // This test verifies the delegation without needing a provider
        let registry = create_test_registry();
        let wrapped = address!("1111111111111111111111111111111111111111");
        let unwrapped = address!("2222222222222222222222222222222222222222");
        let unknown = address!("3333333333333333333333333333333333333333");

        assert!(registry.is_wrapped(&wrapped));
        assert!(!registry.is_wrapped(&unwrapped));
        assert!(!registry.is_wrapped(&unknown));
    }

    #[test]
    fn get_config_by_symbol_returns_correct_config() {
        let registry = create_test_registry();
        let symbol = st0x_execution::Symbol::new("AAPL").unwrap();

        let config = registry.get_by_symbol(&symbol);
        assert!(config.is_some());
        assert_eq!(config.unwrap().equity_symbol, symbol);
    }
}
