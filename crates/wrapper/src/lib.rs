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

use st0x_evm::{EvmError, NODE_SYNC_MAX_ATTEMPTS};
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

/// Result returned by [`Wrapper::confirm_wrap`]: the shares minted and the block
/// in which the deposit transaction was included.
///
/// The block number is returned so callers can pass it to
/// [`Wrapper::wait_for_block`] before submitting any dependent on-chain write
/// that reads the wrapped token balance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WrapConfirmation {
    /// Actual ERC-4626 shares minted by the deposit.
    pub shares: U256,
    /// Block number of the confirmed deposit transaction.
    pub block: u64,
}

/// Result returned by [`Wrapper::confirm_unwrap`]: the underlying amount
/// received and the block in which the redeem transaction was included.
///
/// The block number is returned so callers can pass it to
/// [`Wrapper::wait_for_block`] before submitting any dependent on-chain write
/// that reads the unwrapped balance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnwrapConfirmation {
    /// Actual underlying tokens returned by the redeem.
    pub assets: U256,
    /// Block number of the confirmed redeem transaction.
    pub block: u64,
}

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
    #[error("transaction receipt for tx {tx_hash} is missing a block number")]
    MissingBlockNumber { tx_hash: TxHash },
}

/// Extracts the number of polling attempts from a `WrapperError` returned by
/// `Wrapper::wait_for_block`.
///
/// Returns the actual attempt count when the error is
/// `WrapperError::Evm(NodeBehindRequiredBlock { attempts, .. })` — the node
/// polled that many times and never caught up. Falls back to
/// [`NODE_SYNC_MAX_ATTEMPTS`] for any other error variant (e.g. a transport
/// error where every poll failed before any block number was observed, consuming
/// the full budget without ever recording a successful tip).
///
/// Both error paths indicate the full polling budget was consumed; the
/// difference is only in what diagnostic information is available. Callers use
/// the extracted attempt count to populate their own `NodeSyncFailed` error
/// variant.
pub fn node_sync_attempts(error: &WrapperError) -> u32 {
    match error {
        WrapperError::Evm(EvmError::NodeBehindRequiredBlock { attempts, .. }) => *attempts,
        // All other Evm sub-variants signal full budget consumption with no
        // recorded attempt count, so fall back to the maximum. The inner
        // EvmError match uses a wildcard here because EvmError has feature-gated
        // variants (turnkey, local-signer) that cannot be enumerated from this
        // crate without mirroring all their cfg gates. The outer WrapperError
        // match is exhaustive, which is where new variants are most likely to
        // carry a meaningful attempt count.
        WrapperError::Evm(_)
        | WrapperError::SymbolNotConfigured(_)
        | WrapperError::MissingDepositEvent
        | WrapperError::MissingWithdrawEvent
        | WrapperError::RedeemExceedsMax { .. }
        | WrapperError::Contract(_)
        | WrapperError::Ratio(_)
        | WrapperError::MissingBlockNumber { .. } => NODE_SYNC_MAX_ATTEMPTS,
    }
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

    /// Wait for a previously submitted wrap transaction to confirm,
    /// extract the shares minted from the receipt, and return the
    /// confirmation block number alongside the shares.
    ///
    /// The block number is returned so callers can pass it to
    /// [`Wrapper::wait_for_block`] before submitting any dependent
    /// on-chain write that reads the wrapped token balance.
    async fn confirm_wrap(
        &self,
        wrapped_token: Address,
        tx_hash: TxHash,
    ) -> Result<WrapConfirmation, WrapperError>;

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

    /// Wait for a previously submitted unwrap transaction to confirm and
    /// extract the underlying amount received and the block it confirmed in.
    ///
    /// The block number is returned alongside the amount so callers can pass
    /// it to [`Wrapper::wait_for_block`] before submitting any dependent
    /// on-chain write that reads the unwrapped balance.
    async fn confirm_unwrap(
        &self,
        wrapped_token: Address,
        tx_hash: TxHash,
    ) -> Result<UnwrapConfirmation, WrapperError>;

    /// Block until the RPC node's tip is at least `block`.
    ///
    /// Load-balanced RPCs may route the next request to a backend that has
    /// not yet indexed the block produced by a just-confirmed transaction.
    /// Call this with the block number returned by [`confirm_unwrap`] before
    /// submitting any dependent on-chain write that reads the unwrapped
    /// token balance.
    ///
    /// # Errors
    ///
    /// Returns [`WrapperError::Evm`] wrapping
    /// [`st0x_evm::EvmError::NodeBehindRequiredBlock`] if the node never
    /// catches up within the retry budget.
    async fn wait_for_block(&self, block: u64) -> Result<(), WrapperError>;

    /// Returns the market maker wallet address that owns the wrapped tokens.
    fn owner(&self) -> Address;
}
