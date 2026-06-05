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
pub(crate) use manager::{CrossVenueCashTransfer, u256_to_usdc};

use std::time::Duration;

use rain_math_float::FloatError;
use thiserror::Error;

use st0x_bridge::cctp::CctpError;
use st0x_event_sorcery::SendError;
use st0x_execution::{AlpacaBrokerApiError, ClientOrderId, InvalidSharesError, NotPositive};
use st0x_finance::{Usdc, UsdcConversionError};
use st0x_raindex::RaindexError;

use crate::alpaca_wallet::AlpacaWalletError;
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
}

impl From<SendError<UsdcRebalance>> for UsdcTransferError {
    fn from(error: SendError<UsdcRebalance>) -> Self {
        Self::Aggregate(Box::new(error))
    }
}
