//! Bridge abstraction for cross-chain USDC transfers.
//!
//! This crate provides a generic `Bridge` trait for bridging USDC between chains.
//! The default (no features) build ships only the trait and shared domain types.
//! Enable the `cctp` feature for the Circle CCTP V2 implementation and the
//! `relay` feature for the Relay client and [`SwapBridge`], its on-chain side.

use alloy::primitives::{Address, B256, TxHash, U256};
use async_trait::async_trait;

#[cfg(feature = "relay")]
use st0x_evm::PreparedTransaction;

#[cfg(feature = "cctp")]
pub mod cctp;
#[cfg(any(feature = "cctp", feature = "relay"))]
pub mod corridor;
#[cfg(feature = "relay")]
pub mod relay;

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

    /// Fetches the attestation once, without waiting for it: a burn not
    /// attested yet is a retryable error. For callers bounded by a request
    /// deadline, which must not keep polling after their caller gave up.
    async fn fetch_attestation(
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

    /// Returns the mint that consumed `attestation`'s nonce on the destination
    /// chain, or `None` while that nonce is unused, for crash-safe resume. The
    /// match is by nonce, so another transfer's mint to the same recipient is
    /// never returned.
    ///
    /// The log scan for a consumed nonce starts at the lower of
    /// `scan_from_block` (the [`Bridge::destination_block`] captured before the
    /// mint) less a small margin and a fixed lookback from the head, since a
    /// relayer can mint before that head is captured. `None`, for a transfer
    /// that predates it, scans the lookback alone. A consumed nonce whose mint
    /// is not found in that window is an error, never a scan to genesis. The
    /// error carries a `usedNonces` read at the block below the floor: unused
    /// there means the mint is in the window and the log is lagging; used means
    /// the mint lies below the floor. Only when that read fails does it carry
    /// the floor block's timestamp instead: a floor mined before the transfer
    /// started covers its mint; a later floor may be above it.
    async fn find_attested_mint(
        &self,
        direction: BridgeDirection,
        attestation: &Self::Attestation,
        scan_from_block: Option<u64>,
    ) -> Result<Option<MintReceipt>, Self::Error>;

    /// Returns whether `nonce` is consumed (`usedNonces`) on the mint
    /// destination chain for `direction`: `true` once its mint has landed.
    /// Needs only the nonce, for a resume that has no message envelope yet.
    /// `false` means every read over a probe window found the nonce unused, so
    /// one lagging node cannot hide a landed mint.
    async fn mint_nonce_consumed(
        &self,
        direction: BridgeDirection,
        nonce: B256,
    ) -> Result<bool, Self::Error>;

    /// Returns the current head of the mint destination chain for `direction`.
    /// Captured when the attestation is recorded -- before the mint -- as the
    /// `scan_from_block` of [`Bridge::find_attested_mint`], whose scan starts
    /// at the lower of it less a small margin and a fixed lookback from the head.
    async fn destination_block(&self, direction: BridgeDirection) -> Result<u64, Self::Error>;

    /// Returns the current head of the burn source chain for `direction`.
    /// Captured before the burn call so crash-safe resume ([`Bridge::find_recent_burn`])
    /// has a lower-bound block. For `BaseToEthereum` this is Base chain head;
    /// for `EthereumToBase` this is Ethereum chain head.
    async fn source_block(&self, direction: BridgeDirection) -> Result<u64, Self::Error>;
}

/// Which way a hop moves the stable, relative to the Ethereum hub.
#[cfg(feature = "relay")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HopDirection {
    /// From the corridor chain to the hub.
    ToHub,
    /// From the hub to the corridor chain.
    FromHub,
}

/// What [`SwapBridge::prepare_deposit`] signed, not yet broadcast.
///
/// The caller persists it before broadcasting, so a retry sends the same
/// bytes at the same nonces and never signs a second deposit.
#[cfg(feature = "relay")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreparedSwap {
    Deposit(PreparedSwapDeposit),
    /// Another send from the wallet took the nonce between the approve and
    /// the deposit, so the deposit was discarded. The approve, an allowance
    /// to the pinned depository, goes out alone through
    /// [`SwapBridge::broadcast_approve`] to fill its nonce; the caller then
    /// prepares the deposit again. The caller must persist and broadcast it:
    /// a dropped approve leaves its nonce as a gap that every later send from
    /// the wallet waits behind until restart.
    ApproveOnly {
        approve: PreparedTransaction,
    },
}

/// The approve and deposit of one swap, at consecutive nonces.
#[cfg(feature = "relay")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedSwapDeposit {
    /// `None` when the standing allowance already covers the deposit.
    pub approve: Option<PreparedTransaction>,
    pub deposit: PreparedTransaction,
}

/// A deposit found on the origin chain, from its receipt or a log scan.
#[cfg(feature = "relay")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwapDeposit<OrderId> {
    pub tx: TxHash,
    pub order_id: OrderId,
    /// In the origin stable's smallest unit.
    pub amount: U256,
    pub block: u64,
}

/// What a deposit scan found, and how far it looked.
#[cfg(feature = "relay")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepositScan<OrderId> {
    pub deposits: Vec<SwapDeposit<OrderId>>,
    /// The last block the scan covered, the newest with the origin chain's
    /// confirmations: a deposit mined later is not covered. Never below
    /// `from_block - 1`, so a scan from an unconfirmed block leaves the
    /// caller's cursor where it was.
    pub scanned_to: u64,
}

/// The end of a swap a payment landed on.
#[cfg(feature = "relay")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapSide {
    Origin,
    Destination,
}

/// A fill or refund proven on chain.
#[cfg(feature = "relay")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwapPayment {
    pub tx: TxHash,
    /// A fill is always on the destination; a refund may be on either side.
    pub side: SwapSide,
    /// In the smallest unit of that side's stable.
    pub amount: U256,
}

/// The on-chain side of a swap hop: we deposit on the origin chain and a
/// solver pays us on the destination chain from its own funds, or refunds us.
///
/// Sibling of [`Bridge`], whose burn and mint are both ours to send. Here only
/// the deposit is ours; the payment is a transaction the solver sends, which
/// [`SwapBridge::verify_fill`] and [`SwapBridge::verify_refund`] prove from
/// chain data before anything trusts it.
#[cfg(feature = "relay")]
#[async_trait]
pub trait SwapBridge: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// The accepted quote a deposit funds.
    type Quote: Send + Sync;

    /// The key the deposit and its payment share.
    type OrderId: Copy + Send + Sync;

    /// Signs the quote's approve (when the allowance does not already cover
    /// the deposit) and its deposit on the origin chain, without broadcasting,
    /// or only the approve ([`PreparedSwap::ApproveOnly`]) when another send
    /// split the pair.
    ///
    /// Not cancel-safe: dropped mid-call, it leaves a signed approve's nonce
    /// reserved with nothing to send it, a gap later sends wait behind until
    /// restart. Run it to completion, never under a timeout or `select!`.
    async fn prepare_deposit(
        &self,
        direction: HopDirection,
        quote: &Self::Quote,
    ) -> Result<PreparedSwap, Self::Error>;

    /// Releases the nonces of a prepared swap that was not persisted and so
    /// will never be broadcast, the deposit's before the approve's.
    async fn discard_prepared(&self, direction: HopDirection, prepared: &PreparedSwap);

    /// Reserves the nonces of a persisted prepared swap after a restart. The
    /// caller restores every persisted swap before any other send from the
    /// origin wallet, which would otherwise take a nonce in the pair.
    async fn restore_prepared(&self, direction: HopDirection, prepared: &PreparedSwap);

    /// Broadcasts a prepared pair in nonce order and returns the deposit's
    /// hash. Idempotent: a repeat sends the same bytes.
    async fn broadcast_deposit(
        &self,
        direction: HopDirection,
        prepared: &PreparedSwapDeposit,
    ) -> Result<TxHash, Self::Error>;

    /// Broadcasts the approve of a [`PreparedSwap::ApproveOnly`] and returns
    /// its hash. Idempotent: a repeat sends the same bytes.
    async fn broadcast_approve(
        &self,
        direction: HopDirection,
        approve: &PreparedTransaction,
    ) -> Result<TxHash, Self::Error>;

    /// Waits for the deposit's receipt and checks that it funded `order_id`.
    /// Short of the origin chain's confirmations it fails, to be retried.
    async fn confirm_deposit(
        &self,
        direction: HopDirection,
        order_id: Self::OrderId,
        deposit_tx: TxHash,
    ) -> Result<SwapDeposit<Self::OrderId>, Self::Error>;

    /// Returns the origin chain's head, the floor for a later
    /// [`SwapBridge::find_recent_deposits`].
    async fn origin_block(&self, direction: HopDirection) -> Result<u64, Self::Error>;

    /// Scans the origin chain from `from_block` to its confirmed blocks for
    /// our deposits funding any of `order_ids`.
    async fn find_recent_deposits(
        &self,
        direction: HopDirection,
        order_ids: &[Self::OrderId],
        from_block: u64,
    ) -> Result<DepositScan<Self::OrderId>, Self::Error>;

    /// Proves that the one tx in `txs` paid us at least `minimum_out` of the
    /// destination stable for `order_id`.
    async fn verify_fill(
        &self,
        direction: HopDirection,
        order_id: Self::OrderId,
        minimum_out: U256,
        txs: &[TxHash],
    ) -> Result<SwapPayment, Self::Error>;

    /// Proves that the one tx in `txs` refunded us at most `deposited` for
    /// `order_id`, in the stable of whichever side it landed on.
    async fn verify_refund(
        &self,
        direction: HopDirection,
        order_id: Self::OrderId,
        deposited: U256,
        txs: &[TxHash],
    ) -> Result<SwapPayment, Self::Error>;
}
