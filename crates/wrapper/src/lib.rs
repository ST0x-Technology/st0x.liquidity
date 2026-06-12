//! Token wrapping/unwrapping abstractions for ERC-4626 vaults.
//!
//! This crate provides the generic [`Wrapper`] trait for wrapping and unwrapping
//! tokenized equity against ERC-4626 vaults, plus the shared domain types
//! ([`WrapperError`], [`UnderlyingPerWrapped`], [`RatioError`], [`WrappedEquity`]).
//!
//! The default (no-feature) build ships only the trait and domain types. The
//! ERC-4626 implementation ([`WrapperService`]) is gated behind the `erc4626`
//! feature, and the test mock (`MockWrapper`) behind the `mock` feature.

use alloy::contract::Error as ContractError;
use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;

use st0x_evm::EvmError;
use st0x_execution::Symbol;

mod ratio;

#[cfg(feature = "erc4626")]
mod service;

#[cfg(feature = "mock")]
mod mock;

pub use ratio::{RATIO_ONE, RatioError, UnderlyingPerWrapped};

#[cfg(feature = "erc4626")]
pub use service::WrapperService;

#[cfg(feature = "mock")]
pub use mock::MockWrapper;

/// The underlying and derivative (ERC-4626 vault) token addresses for a wrappable
/// equity symbol.
///
/// Narrow replacement for the integration crate's view of `EquityAssetConfig`:
/// `st0x-wrapper` only needs the two token addresses, not the full asset config,
/// so it stays independent of `st0x-config`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WrappedEquity {
    /// The tokenized equity (underlying) token.
    pub underlying: Address,
    /// The tokenized equity derivative (ERC-4626 vault / wrapped) token.
    pub derivative: Address,
}

/// Error type for wrapper operations.
#[derive(Debug, thiserror::Error)]
pub enum WrapperError {
    #[error("Symbol not configured: {0}")]
    SymbolNotConfigured(Symbol),
    #[error("Missing Deposit event in transaction receipt")]
    MissingDepositEvent,
    #[error("Missing Withdraw event in transaction receipt")]
    MissingWithdrawEvent,
    /// Preflight rejected an unwrap because the requested shares exceed the vault's
    /// `maxRedeem(owner)`. This signals inventory/balance drift: the bot believes it
    /// holds more wrapped shares than the wallet actually owns. We fail fast here
    /// rather than clamp the amount (clamping would silently mask the drift and
    /// violate the financial-integrity rule that range violations must error, not
    /// cap). Reconciling the underlying inventory drift — and any non-1:1
    /// wrapped-ratio handling — is out of scope and tracked separately.
    #[error(
        "Redeem of {requested} wrapped shares exceeds vault {wrapped_token} \
         maxRedeem of {max_redeem} for owner"
    )]
    RedeemExceedsMax {
        wrapped_token: Address,
        requested: U256,
        max_redeem: U256,
    },
    #[error("Contract call error: {0}")]
    Evm(#[from] EvmError),
    #[error("Contract view error: {0}")]
    Contract(#[from] ContractError),
    #[error("Ratio error: {0}")]
    Ratio(#[from] RatioError),
}

/// Trait for wrapping and unwrapping tokens via ERC-4626 vaults.
#[async_trait]
pub trait Wrapper: Send + Sync {
    /// Gets the underlying-per-wrapped ratio for a symbol.
    async fn get_ratio_for_symbol(
        &self,
        symbol: &Symbol,
    ) -> Result<UnderlyingPerWrapped, WrapperError>;

    /// Gets the tokenized equity (underlying) token address for a symbol.
    fn lookup_underlying(&self, symbol: &Symbol) -> Result<Address, WrapperError>;

    /// Gets the tokenized equity derivative (ERC-4626 vault) token address for a symbol.
    fn lookup_derivative(&self, symbol: &Symbol) -> Result<Address, WrapperError>;

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

    /// Submit an ERC-4626 deposit without waiting for confirmation.
    ///
    /// Handles approval if needed, submits the deposit transaction, and
    /// returns the tx hash immediately. Use [`confirm_wrap`](Wrapper::confirm_wrap)
    /// to wait for confirmation and extract the actual shares minted.
    async fn submit_wrap(
        &self,
        wrapped_token: Address,
        underlying_amount: U256,
        receiver: Address,
    ) -> Result<TxHash, WrapperError>;

    /// Wait for a previously submitted wrap transaction to confirm
    /// and extract the shares minted from the receipt.
    async fn confirm_wrap(
        &self,
        wrapped_token: Address,
        tx_hash: TxHash,
    ) -> Result<U256, WrapperError>;

    /// Submit an ERC-4626 redeem without waiting for confirmation.
    ///
    /// Preflights the vault's `maxRedeem(owner)` before submitting any
    /// transaction: if `wrapped_amount` exceeds `maxRedeem`, returns
    /// [`WrapperError::RedeemExceedsMax`] without sending a tx (no on-chain
    /// revert, no wasted gas). Exceeding `maxRedeem` signals inventory/balance
    /// drift — the bot believes it holds more wrapped shares than the wallet
    /// actually owns — so we fail fast here rather than clamp the amount, per
    /// the financial-integrity rule that range violations must error, not cap.
    ///
    /// On success, returns the tx hash immediately. Use
    /// [`confirm_unwrap`](Wrapper::confirm_unwrap) to wait for
    /// confirmation and extract the actual underlying amount received.
    async fn submit_unwrap(
        &self,
        wrapped_token: Address,
        wrapped_amount: U256,
        receiver: Address,
        owner: Address,
    ) -> Result<TxHash, WrapperError>;

    /// Wait for a previously submitted unwrap transaction to confirm
    /// and extract the underlying amount received from the receipt.
    async fn confirm_unwrap(
        &self,
        wrapped_token: Address,
        tx_hash: TxHash,
    ) -> Result<U256, WrapperError>;

    /// Returns the market maker wallet address that owns the wrapped tokens.
    fn owner(&self) -> Address;
}
