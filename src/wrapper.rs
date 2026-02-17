//! Token wrapping/unwrapping abstractions for ERC-4626 vaults.

mod ratio;
mod share;

#[cfg(test)]
pub(crate) mod mock;

use alloy::contract::Error as ContractError;
use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::PendingTransactionError;
use async_trait::async_trait;

use st0x_execution::Symbol;

pub(crate) use ratio::{RatioError, UnderlyingPerWrapped};
pub(crate) use share::{EquityTokenAddresses, WrapperService};

#[cfg(test)]
pub(crate) use ratio::RATIO_ONE;

/// Error type for wrapper operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum WrapperError {
    #[error("Symbol not configured: {0}")]
    SymbolNotConfigured(Symbol),
    #[error("Missing Deposit event in transaction receipt")]
    MissingDepositEvent,
    #[error("Missing Withdraw event in transaction receipt")]
    MissingWithdrawEvent,
    #[error("Contract error: {0}")]
    Contract(#[from] ContractError),
    #[error("Pending transaction error: {0}")]
    PendingTransaction(#[from] PendingTransactionError),
    #[error("Ratio error: {0}")]
    Ratio(#[from] RatioError),
}

/// Trait for wrapping and unwrapping tokens via ERC-4626 vaults.
#[async_trait]
pub(crate) trait Wrapper: Send + Sync {
    /// Gets the underlying-per-wrapped ratio for a symbol.
    async fn get_ratio_for_symbol(
        &self,
        symbol: &Symbol,
    ) -> Result<UnderlyingPerWrapped, WrapperError>;

    /// Gets the unwrapped (underlying) token address for a symbol.
    fn lookup_unwrapped(&self, symbol: &Symbol) -> Result<Address, WrapperError>;

    /// Deposits underlying tokens to receive wrapped tokens.
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

    /// Returns the market maker wallet address that owns the wrapped tokens.
    fn owner(&self) -> Address;
}
