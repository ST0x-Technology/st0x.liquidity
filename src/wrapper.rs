//! Token wrapping/unwrapping abstractions for ERC-4626 vaults.

mod ratio;
mod share;

#[cfg(test)]
pub(crate) mod mock;

use alloy::contract::Error as ContractError;
use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;

use st0x_evm::EvmError;
use st0x_execution::Symbol;

pub(crate) use ratio::{RatioError, UnderlyingPerWrapped};
pub(crate) use share::WrapperService;

#[cfg(test)]
pub(crate) use ratio::RATIO_ONE;

/// Simplified equity token addresses for test constructors.
///
/// Maps a symbol to its wrapped/unwrapped token addresses and
/// which operations are enabled.
#[cfg(any(test, feature = "test-support"))]
pub struct EquityTokenAddresses {
    pub wrapped: Address,
    pub unwrapped: Address,
    /// Whether trading is enabled for this asset.
    pub enabled: bool,
    /// Whether rebalancing is enabled. Defaults to `false` when
    /// not specified so hedging-only tests don't need to set it.
    pub rebalancing: bool,
    /// Optional Raindex vault ID for this equity. Required when
    /// rebalancing is enabled.
    pub vault_id: Option<alloy::primitives::B256>,
}

/// Error type for wrapper operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum WrapperError {
    #[error("Symbol not configured: {0}")]
    SymbolNotConfigured(Symbol),
    #[error("Missing Deposit event in transaction receipt")]
    MissingDepositEvent,
    #[error("Missing Withdraw event in transaction receipt")]
    MissingWithdrawEvent,
    #[error("Contract call error: {0}")]
    Evm(#[from] EvmError),
    #[error("Contract view error: {0}")]
    Contract(#[from] ContractError),
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

    /// Gets the tokenized equity (underlying) token address for a symbol.
    fn lookup_tokenized_equity(&self, symbol: &Symbol) -> Result<Address, WrapperError>;

    /// Gets the tokenized equity derivative (ERC-4626 vault) token address for a symbol.
    fn lookup_tokenized_equity_derivative(&self, symbol: &Symbol) -> Result<Address, WrapperError>;

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
