//! USDC cross-venue transfers between Alpaca and Base.
//!
//! [`CrossVenueCashTransfer`] implements [`CrossVenueTransfer`] in both
//! directions for USDC, handling USD/USDC conversion, withdrawal, CCTP
//! bridging, and deposit.

mod job;
mod manager;
#[cfg(test)]
pub(crate) mod mock;

pub(crate) use job::{
    ResumeBaseToAlpaca, TransferUsdcToHedging, TransferUsdcToHedgingCtx,
    TransferUsdcToHedgingJobQueue,
};
pub(crate) use manager::{CrossVenueCashTransfer, u256_to_usdc};

use rain_math_float::FloatError;
use thiserror::Error;

use st0x_bridge::cctp::CctpError;
use st0x_event_sorcery::SendError;
use st0x_execution::{AlpacaBrokerApiError, InvalidSharesError, NotPositive};
use st0x_finance::Usdc;

use crate::alpaca_wallet::AlpacaWalletError;
use crate::onchain::raindex::RaindexError;
use crate::usdc_rebalance::{RebalanceDirection, UsdcRebalance, UsdcRebalanceId};
use st0x_finance::UsdcConversionError;

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
    MissingFilledQuantity { order_id: uuid::Uuid },
    #[error(
        "USDC rebalance {id} conversion could not be completed on resume \
         (order not found at Alpaca or terminally failed); failed for \
         operator reconciliation"
    )]
    ResumeIndeterminateConversion { id: UsdcRebalanceId },
    #[error(
        "USDC rebalance {id} conversion order is still settling on resume; \
         retrying so apalis re-checks once it reaches a terminal status"
    )]
    ConversionStillSettling { id: UsdcRebalanceId },
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
}

impl From<SendError<UsdcRebalance>> for UsdcTransferError {
    fn from(error: SendError<UsdcRebalance>) -> Self {
        Self::Aggregate(Box::new(error))
    }
}
