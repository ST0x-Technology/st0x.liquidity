//! USDC cross-venue transfers between Alpaca and Base.
//!
//! [`CrossVenueCashTransfer`] drives USDC transfers in both directions through
//! direct `execute_alpaca_to_base` / `execute_base_to_alpaca` entry points,
//! with matching `resume_*` paths for apalis-driven crash recovery. Each
//! transfer handles USD/USDC conversion, withdrawal, CCTP bridging, and deposit.

mod job;
mod manager;

pub(crate) use job::{
    ResumeAlpacaToBase, ResumeBaseToAlpaca, TransferUsdcToHedging, TransferUsdcToHedgingCtx,
    TransferUsdcToHedgingJobQueue, TransferUsdcToMarketMaking, TransferUsdcToMarketMakingCtx,
    TransferUsdcToMarketMakingJobQueue,
};
pub(crate) use manager::{CrossVenueCashTransfer, UsdcSettlementParams, u256_to_usdc};

use std::time::Duration;

use alloy::primitives::TxHash;
use rain_math_float::FloatError;
use thiserror::Error;

use st0x_bridge::cctp::CctpError;
use st0x_event_sorcery::SendError;
use st0x_execution::{
    AlpacaBrokerApiError, AlpacaWalletError, ClientOrderId, InvalidSharesError, NotPositive,
};
use st0x_finance::{Usdc, UsdcConversionError};
use st0x_raindex::RaindexError;

use crate::usdc_rebalance::{RebalanceDirection, UsdcRebalance, UsdcRebalanceId};

#[derive(Debug, Error)]
pub(crate) enum UsdcTransferError {
    #[error("Alpaca wallet error: {0}")]
    AlpacaWallet(#[from] AlpacaWalletError),
    #[error("Alpaca broker API error: {0}")]
    AlpacaBrokerApi(#[from] AlpacaBrokerApiError),
    #[error("CCTP bridge error: {0}")]
    Cctp(#[from] Box<CctpError>),
    /// Emitted by the two-phase burn path (`burn_recording_pending`) when the
    /// burn fails with a revert-class error: EVM execution reverts and pre-flight
    /// rejections that produce no on-chain state change. Safe to redrive via the
    /// scan-or-reburn path in `resume_bridging_submitting` /
    /// `resume_bridging_submitting_ethereum`.
    ///
    /// IMPORTANT: The double-burn safety guarantee does NOT come from this
    /// classification. It comes from `resume_bridging_submitting` /
    /// `resume_bridging_submitting_ethereum` scanning for an existing burn (via
    /// `find_recent_burn`) before attempting a new one, with the scan lower bound
    /// (`from_block`) durably recorded in the `BeginBridging` / `BridgingSubmitting`
    /// event before the burn call. This variant identifies errors where the EVM
    /// produced no lasting state change (post-mining reverts, pre-flight rejections).
    /// The safety guarantee is the scan, not this variant.
    #[error("CCTP burn revert (no on-chain state change): {0}")]
    BurnRevert(Box<CctpError>),
    #[error("Vault error: {0}")]
    Vault(#[from] RaindexError),
    #[error("Aggregate error: {0}")]
    Aggregate(Box<SendError<UsdcRebalance>>),
    #[error("Withdrawal failed with terminal status: {status}")]
    WithdrawalFailed { status: String },
    #[error("Deposit failed with terminal status: {status}")]
    DepositFailed { status: String },
    #[error("USDC conversion error: {0}")]
    UsdcConversion(#[from] UsdcConversionError),
    #[error("Float operation error: {0}")]
    Float(#[from] FloatError),
    #[error("Invalid shares: {0}")]
    InvalidShares(#[from] InvalidSharesError),
    #[error(transparent)]
    NotPositive(#[from] NotPositive<Usdc>),
    #[error(
        "Conversion order {order_id} filled but \
         filled_quantity is missing"
    )]
    MissingFilledQuantity { order_id: ClientOrderId },
    #[error(
        "USDC rebalance {id} conversion could not be completed on resume \
         (order not found at Alpaca or terminally failed); failed for \
         operator reconciliation"
    )]
    ResumeIndeterminateConversion { id: UsdcRebalanceId },
    #[error(
        "USDC rebalance {id} cannot resume from Attested state: no mint scan \
         bound was captured (pre-resume-hardening event); manual reconciliation \
         required"
    )]
    ResumeWithoutMintScanBound { id: UsdcRebalanceId },
    #[error(
        "USDC rebalance {id} attestation polling timed out; retrying until \
         attestation retry deadline"
    )]
    AttestationTimedOut { id: UsdcRebalanceId },
    #[error(
        "USDC rebalance {id} attestation retry deadline elapsed; failed for \
         operator reconciliation"
    )]
    AttestationRetryDeadlineElapsed { id: UsdcRebalanceId },
    #[error(
        "USDC rebalance {id} attestation retry deadline duration {retry_deadline:?} \
         cannot be represented as an absolute timestamp"
    )]
    AttestationRetryDeadlineOverflow {
        id: UsdcRebalanceId,
        retry_deadline: Duration,
    },
    #[error(
        "USDC rebalance {id} recorded cctp_nonce {recorded} does not match nonce \
         {reconstructed} re-derived from the persisted message envelope; failed for \
         operator reconciliation rather than minting against an unverifiable nonce"
    )]
    AttestationNonceMismatch {
        id: UsdcRebalanceId,
        recorded: alloy::primitives::B256,
        reconstructed: alloy::primitives::B256,
    },
    #[error("USDC rebalance {id} cannot resume: aggregate is in terminal failure state")]
    PreviouslyFailedAggregate { id: UsdcRebalanceId },
    #[error(
        "USDC rebalance {id} DepositInitiated has non-onchain deposit ref; \
         BaseToAlpaca always records the mint tx as OnchainTx"
    )]
    DepositRefMustBeOnchain { id: UsdcRebalanceId },
    #[error(
        "USDC rebalance {id} cannot resume via Base->Alpaca entrypoint: \
         aggregate direction is {direction:?}"
    )]
    ResumeDirectionMismatch {
        id: UsdcRebalanceId,
        direction: RebalanceDirection,
    },
    #[error(
        "USDC rebalance {id} adopted a withdrawal that realized {withdrawn} but \
         {requested} was requested; failed for operator reconciliation rather \
         than burning more than was withdrawn"
    )]
    AdoptedWithdrawalAmountMismatch {
        id: UsdcRebalanceId,
        withdrawn: alloy::primitives::U256,
        requested: alloy::primitives::U256,
    },
    #[error(
        "USDC rebalance {id} Withdrawing has non-Alpaca withdrawal ref; \
         AlpacaToBase always records the Alpaca transfer ID"
    )]
    WithdrawalRefMustBeAlpacaId {
        id: crate::usdc_rebalance::UsdcRebalanceId,
    },
    #[error(
        "USDC rebalance {id}: market-maker Ethereum wallet holds zero USDC; \
         any non-zero balance will unblock the CCTP burn \
         (nominal amount: {nominal}); waiting for withdrawal to settle on-chain"
    )]
    WalletUsdcInsufficient { id: UsdcRebalanceId, nominal: Usdc },
    /// The wallet holds MORE USDC than the nominal amount for this rebalance.
    /// The wallet-empty-between-rebalances invariant is broken: ambient or
    /// residual USDC from a prior rebalance is present and cannot be
    /// distinguished from this withdrawal's funds. The aggregate is moved to
    /// `BridgingFailed` for operator reconciliation; no burn is attempted.
    #[error(
        "USDC rebalance {id}: market-maker wallet holds {balance} USDC, \
         which exceeds the nominal {nominal}; ambient/residual USDC detected \
         (wallet-empty invariant broken); failed for operator reconciliation"
    )]
    WalletUsdcAmbientBalance {
        id: UsdcRebalanceId,
        balance: Usdc,
        nominal: Usdc,
    },
    #[error(
        "USDC rebalance {id}: withdrawal tx {tx} has only {actual} confirmations, \
         need {required}; waiting for on-chain settlement"
    )]
    WithdrawalTxUnderconfirmed {
        id: UsdcRebalanceId,
        tx: TxHash,
        required: u64,
        actual: u64,
    },
    /// An RPC call in the settlement phase (confirmation re-check, balance read,
    /// or burn scan) failed transiently. The aggregate is in
    /// `WithdrawalComplete` or `BridgingSubmitting` -- a durable, resumable
    /// state -- so this is safe to delayed-redrive exactly like
    /// `WithdrawalTxUnderconfirmed`.
    #[error(
        "USDC rebalance {id}: settlement-phase RPC check failed transiently; \
         will retry after delay"
    )]
    SettlementCheckTransient {
        id: UsdcRebalanceId,
        #[source]
        source: Box<CctpError>,
    },
    /// The detached submit-and-record burn task (spawned so a cancelling job
    /// timeout cannot drop the broadcast->record critical section) panicked and
    /// failed to join. The burn may or may not have broadcast. TERMINAL and
    /// fail-closed at the job layer: the transfer latches at `BridgingSubmitting`
    /// for operator reconciliation rather than auto-redriving, because a redrive
    /// with no recorded tx would fall to the mempool-blind scan and could reburn a
    /// still-pending burn.
    #[error("USDC rebalance {id}: burn submit-and-record task panicked")]
    BurnRecordTaskFailed { id: UsdcRebalanceId },
    /// The burn was broadcast but `RecordPendingBurn` failed to commit after
    /// retries, so the burn tx hash was NOT durably recorded. TERMINAL and
    /// fail-closed at the job layer: rather than proceeding to confirm (or letting
    /// an Apalis retry treat the burn as un-broadcast and reburn off a
    /// mempool-blind scan), the transfer latches at `BridgingSubmitting` for
    /// operator reconciliation. The broadcast burn hash is in the logs; manual
    /// on-chain verification is required before any further action.
    #[error(
        "USDC rebalance {id}: burn broadcast but RecordPendingBurn could not be \
         committed; failed for operator reconciliation (burn tx {burn_tx})"
    )]
    BurnRecordFailed {
        id: UsdcRebalanceId,
        burn_tx: TxHash,
    },
    /// The CCTP burn submission did not return a usable tx hash: it either timed
    /// out (the broadcast may still have reached the network) or failed with a
    /// non-revert (transport/RPC) error after the request was sent. The burn may
    /// or may not be on-chain, so its fate is INCONCLUSIVE. TERMINAL and
    /// fail-closed at the job layer: the transfer latches at `BridgingSubmitting`
    /// for operator reconciliation rather than auto-reburning, since a reburn
    /// could double-burn if the original submission did land. A clean revert (no
    /// funds moved) is NOT this error -- that stays on the bounded revert-redrive
    /// path.
    #[error(
        "USDC rebalance {id}: CCTP burn submission inconclusive (timed out or \
         non-revert transport error after broadcast); failed for operator \
         reconciliation (verify on-chain before any reburn)"
    )]
    BurnSubmitInconclusive { id: UsdcRebalanceId },
    /// A durably-recorded burn tx was classified `Dropped` (no receipt and absent
    /// from the mempool past the grace window): the recorded burn is not mined and
    /// is no longer in the mempool. TERMINAL: once a burn tx hash is durably
    /// recorded the system NEVER auto-issues a second burn for an ambiguous
    /// "dropped" classification (a load-balanced RPC could misreport a still-pending
    /// burn as dropped, causing a double-burn). The operator must manually verify
    /// on-chain whether the burn landed before any reburn.
    #[error(
        "USDC rebalance {id}: recorded burn tx {burn_tx} not mined and no longer in \
         the mempool; manual on-chain verification required before any reburn"
    )]
    BurnTxDropped {
        id: UsdcRebalanceId,
        burn_tx: TxHash,
    },
}

impl From<SendError<UsdcRebalance>> for UsdcTransferError {
    fn from(error: SendError<UsdcRebalance>) -> Self {
        Self::Aggregate(Box::new(error))
    }
}
