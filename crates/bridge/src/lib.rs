//! Bridge abstraction for cross-chain USDC transfers.
//!
//! This crate provides a generic `Bridge` trait for bridging USDC between chains.
//! The default (no features) build ships only the trait and shared domain types.
//! Enable the `cctp` feature for the Circle CCTP V2 implementation.

use alloy::primitives::{Address, B256, TxHash, U256};
use async_trait::async_trait;

#[cfg(feature = "cctp")]
pub mod cctp;

/// Direction of a bridge transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeDirection {
    /// Bridge USDC from Ethereum to Base
    EthereumToBase,
    /// Bridge USDC from Base to Ethereum
    BaseToEthereum,
}

/// Receipt from burning USDC on the source chain.
#[derive(Debug)]
pub struct BurnReceipt {
    /// Transaction hash of the burn transaction
    pub tx: TxHash,
    /// Amount of USDC burned (in smallest unit, 6 decimals for USDC).
    /// This is the INPUT amount sent to the contract, NOT the amount received
    /// on the destination chain (which is amount minus fee).
    pub amount: U256,
}

/// On-chain status of a broadcast CCTP burn transaction, resolved from its
/// receipt and (when the receipt is absent) the mempool.
///
/// Lets a crash/timeout redrive check the exact recorded burn tx instead of a
/// mempool-blind log scan, so a burn that was broadcast but whose receipt was
/// never awaited is adopted rather than re-burned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BurnTxStatus {
    /// Receipt present and the transaction succeeded: the burn landed.
    MinedSuccess,
    /// Receipt present but the transaction reverted: the burn took no effect.
    MinedReverted,
    /// No receipt yet, but the tx is still visible via `get_transaction_by_hash`
    /// (in the mempool / known to the node): broadcast but not yet mined, so it
    /// may still mine. The caller must NOT re-burn.
    Pending,
    /// No receipt and the tx is absent from the mempool past the grace window
    /// and consecutive-miss threshold. A recorded burn classified `Dropped` is
    /// treated as TERMINAL by the caller (operator-paged for on-chain
    /// verification), NOT auto-reburned: a load-balanced RPC can misclassify a
    /// still-pending burn as dropped, so reburning could double-burn. Only the
    /// operator may reburn, after manually confirming the burn never landed.
    Dropped,
}

/// Receipt from minting USDC on the destination chain.
#[derive(Debug, PartialEq, Eq)]
pub struct MintReceipt {
    /// Transaction hash of the mint transaction
    pub tx: TxHash,
    /// Actual USDC minted to recipient (NET of fees).
    pub amount: U256,
    /// Actual fee collected for this transfer.
    pub fee: U256,
}

/// Attestation data required to mint on the destination chain.
pub trait Attestation: Send + Sync {
    /// Returns the 32-byte CCTP V2 nonce for this attestation.
    fn nonce(&self) -> B256;

    /// Returns the raw attestation signature bytes.
    fn as_bytes(&self) -> &[u8];

    /// Returns the full message envelope bytes. Minting needs the whole
    /// envelope (not just the signature), so a caller persisting an attestation
    /// for an offline resume stores these alongside [`Attestation::as_bytes`]
    /// and later rebuilds via [`Bridge::reconstruct_attestation`].
    fn message_bytes(&self) -> &[u8];
}

/// Generic bridge trait for cross-chain USDC transfers.
///
/// Implementations handle the three-step flow: burn -> poll attestation -> mint.
#[async_trait]
pub trait Bridge: Send + Sync + 'static {
    /// Error type for bridge operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Attestation type returned by polling.
    type Attestation: Attestation;

    /// Burns USDC on the source chain.
    async fn burn(
        &self,
        direction: BridgeDirection,
        amount: U256,
        recipient: Address,
    ) -> Result<BurnReceipt, Self::Error>;

    /// Broadcasts the CCTP burn and returns its tx hash immediately, WITHOUT
    /// awaiting the receipt, so a caller can durably record the broadcast hash
    /// before awaiting confirmation (closing the double-burn window). Re-runs the
    /// standing-allowance check and fee query on every call.
    ///
    /// NOT idempotent: every call that returns `Ok` broadcasts a NEW burn. Once a
    /// `TxHash` is returned the caller MUST persist it and continue via
    /// [`Bridge::confirm_burn`] / [`Bridge::burn_status`] -- never re-invoke
    /// `submit_burn` for the same transfer, or it will burn twice. Re-invocation is
    /// safe only when the call returned `Err` (no hash, so no burn broadcast).
    async fn submit_burn(
        &self,
        direction: BridgeDirection,
        amount: U256,
        recipient: Address,
    ) -> Result<TxHash, Self::Error>;

    /// Awaits the receipt of a burn previously broadcast via
    /// [`Bridge::submit_burn`], decoding a revert, and validates that the burn
    /// emitted the CCTP `MessageSent` event. `amount` is the burned input amount,
    /// returned on the [`BurnReceipt`] for downstream inventory accounting.
    async fn confirm_burn(
        &self,
        direction: BridgeDirection,
        tx_hash: TxHash,
        amount: U256,
    ) -> Result<BurnReceipt, Self::Error>;

    /// Resolves the on-chain status of a broadcast burn tx, for crash-safe
    /// resume. A present receipt classifies as mined success/revert. An absent
    /// receipt consults the mempool: a tx still known to the node is
    /// [`BurnTxStatus::Pending`], and a tx absent from the mempool is only
    /// reported [`BurnTxStatus::Dropped`] after a grace window plus consecutive
    /// misses (mirroring the wallet's `wait_for_receipt` drop policy), so a
    /// still-pending tx is never re-burned.
    async fn burn_status(
        &self,
        direction: BridgeDirection,
        tx_hash: TxHash,
    ) -> Result<BurnTxStatus, Self::Error>;

    /// Polls for an attestation confirming the burn.
    async fn poll_attestation(
        &self,
        direction: BridgeDirection,
        burn_tx: TxHash,
    ) -> Result<Self::Attestation, Self::Error>;

    /// Mints USDC on the destination chain using the attestation.
    async fn mint(
        &self,
        direction: BridgeDirection,
        attestation: &Self::Attestation,
    ) -> Result<MintReceipt, Self::Error>;

    /// Rebuilds an attestation from a persisted message envelope and signature,
    /// so an `Attested` resume mints offline without re-polling the attestation
    /// service. The implementation re-derives and validates any embedded nonce,
    /// so a corrupt or truncated envelope fails here rather than on-chain. This
    /// keeps reconstruction behind the trait, so callers never depend on a
    /// concrete attestation representation.
    fn reconstruct_attestation(
        &self,
        message: Vec<u8>,
        attestation: Vec<u8>,
    ) -> Result<Self::Attestation, Self::Error>;

    /// Scans the burn source chain for an already-submitted burn matching
    /// `(amount, destinationDomain, recipient)` at or after `from_block`, for
    /// crash-safe resume.
    async fn find_recent_burn(
        &self,
        direction: BridgeDirection,
        amount: U256,
        recipient: Address,
        from_block: u64,
    ) -> Result<Option<TxHash>, Self::Error>;

    /// Scans the mint destination chain for an already-submitted mint to
    /// `recipient` strictly after `from_block`, for crash-safe resume. Returns
    /// the receipt so the caller can adopt the existing mint instead of
    /// re-minting, which reverts on the already-used CCTP nonce and otherwise
    /// fails the transfer for USDC that was in fact minted.
    async fn find_recent_mint(
        &self,
        direction: BridgeDirection,
        recipient: Address,
        from_block: u64,
    ) -> Result<Option<MintReceipt>, Self::Error>;

    /// Returns the current head of the mint destination chain for `direction`.
    /// Captured when the attestation is recorded -- before the mint -- so the
    /// crash-safe resume scan in [`Bridge::find_recent_mint`] is bounded to
    /// blocks mined strictly after it.
    async fn destination_block(&self, direction: BridgeDirection) -> Result<u64, Self::Error>;

    /// Returns the current head of the burn source chain for `direction`.
    /// Captured before the burn call so crash-safe resume ([`Bridge::find_recent_burn`])
    /// has a lower-bound block. For `BaseToEthereum` this is Base chain head;
    /// for `EthereumToBase` this is Ethereum chain head.
    async fn source_block(&self, direction: BridgeDirection) -> Result<u64, Self::Error>;
}
