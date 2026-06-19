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
        tx: alloy::primitives::TxHash,
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
}

impl From<SendError<UsdcRebalance>> for UsdcTransferError {
    fn from(error: SendError<UsdcRebalance>) -> Self {
        Self::Aggregate(Box::new(error))
    }
}
