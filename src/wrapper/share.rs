//! WrapperService implementation for ERC-4626 token wrapping/unwrapping.

use std::collections::HashMap;

use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::Provider;
use alloy::sol_types::SolEvent;
use async_trait::async_trait;
use serde::Deserialize;
use st0x_execution::Symbol;

use super::{UnderlyingPerWrapped, Wrapper, WrapperError};
use crate::bindings::IERC4626;

/// One unit with 18 decimals for ratio queries.
const RATIO_QUERY_AMOUNT: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);

/// Wrapped and unwrapped token addresses for an equity.
#[derive(Debug, Clone, Deserialize)]
pub struct EquityTokenAddresses {
    pub wrapped: Address,
    pub unwrapped: Address,
}

/// Service for managing ERC-4626 token wrapping/unwrapping operations.
pub(crate) struct WrapperService<P>
where
    P: Provider + Clone,
{
    provider: P,
    owner: Address,
    config: HashMap<Symbol, EquityTokenAddresses>,
}

impl<P> WrapperService<P>
where
    P: Provider + Clone,
{
    pub(crate) fn new(
        provider: P,
        owner: Address,
        config: HashMap<Symbol, EquityTokenAddresses>,
    ) -> Self {
        Self {
            provider,
            owner,
            config,
        }
    }

    /// Fetches the current conversion ratio for a wrapped token.
    async fn get_ratio(
        &self,
        wrapped_token: Address,
    ) -> Result<UnderlyingPerWrapped, WrapperError> {
        let vault = IERC4626::new(wrapped_token, &self.provider);
        let assets_per_share = vault.convertToAssets(RATIO_QUERY_AMOUNT).call().await?;

        UnderlyingPerWrapped::new(assets_per_share).map_err(Into::into)
    }

    /// Gets the token addresses for a symbol.
    pub(crate) fn lookup_equity(&self, symbol: &Symbol) -> Option<&EquityTokenAddresses> {
        self.config.get(symbol)
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
        let addresses = self
            .lookup_equity(symbol)
            .ok_or_else(|| WrapperError::SymbolNotConfigured(symbol.clone()))?;

        self.get_ratio(addresses.wrapped).await
    }

    fn lookup_unwrapped(&self, symbol: &Symbol) -> Result<Address, WrapperError> {
        let addresses = self
            .lookup_equity(symbol)
            .ok_or_else(|| WrapperError::SymbolNotConfigured(symbol.clone()))?;

        Ok(addresses.unwrapped)
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

        Ok((tx_hash, actual_assets))
    }

    fn owner(&self) -> Address {
        self.owner
    }
}
