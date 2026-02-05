//! Wrapper trait and implementations for converting between wrapped and underlying tokens.

#[cfg(test)]
pub(crate) mod mock;
mod share;

pub(crate) use share::WrapperService;

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use st0x_execution::Symbol;

use crate::vault::{UnderlyingPerWrapped, WrapperError};

/// Trait for converting between wrapped and underlying tokens.
#[async_trait]
pub(crate) trait Wrapper: Send + Sync {
    /// Gets the conversion ratio for a symbol.
    async fn get_ratio_for_symbol(&self, symbol: &Symbol) -> Result<UnderlyingPerWrapped, WrapperError>;

    /// Converts underlying tokens to wrapped by depositing into the ERC-4626 vault.
    ///
    /// # Arguments
    /// * `wrapped_token` - The ERC-4626 vault address
    /// * `underlying_amount` - Amount of underlying tokens to convert
    /// * `receiver` - Address to receive the wrapped shares
    ///
    /// # Returns
    /// Transaction hash and the amount of wrapped shares received.
    async fn to_wrapped(
        &self,
        wrapped_token: Address,
        underlying_amount: U256,
        receiver: Address,
    ) -> Result<(TxHash, U256), WrapperError>;

    /// Converts wrapped tokens to underlying by redeeming from the ERC-4626 vault.
    ///
    /// # Arguments
    /// * `wrapped_token` - The ERC-4626 vault address
    /// * `wrapped_amount` - Amount of wrapped shares to convert
    /// * `receiver` - Address to receive the underlying tokens
    /// * `owner` - Owner of the wrapped shares
    ///
    /// # Returns
    /// Transaction hash and the amount of underlying tokens received.
    async fn to_underlying(
        &self,
        wrapped_token: Address,
        wrapped_amount: U256,
        receiver: Address,
        owner: Address,
    ) -> Result<(TxHash, U256), WrapperError>;

    /// Returns the owner address for conversion operations.
    fn owner(&self) -> Address;
}
