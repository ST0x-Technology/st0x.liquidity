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
    BaseToAlpacaTransfer, TransferUsdcToHedging, TransferUsdcToHedgingCtx,
    TransferUsdcToHedgingJobQueue,
};
pub(crate) use manager::{CrossVenueCashTransfer, u256_to_usdc};

use rain_math_float::FloatError;
use thiserror::Error;

use st0x_bridge::cctp::CctpError;
use st0x_event_sorcery::SendError;
use st0x_execution::{AlpacaBrokerApiError, InvalidSharesError, NotPositive};
use st0x_finance::{Usdc, UsdcConversionError};

use crate::alpaca_wallet::AlpacaWalletError;
use crate::onchain::raindex::RaindexError;
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
    MissingFilledQuantity { order_id: uuid::Uuid },
    #[error(
        "Cannot resume rebalance {id}: aggregate direction is {direction:?}, \
         expected BaseToAlpaca"
    )]
    WrongDirectionForResume {
        id: UsdcRebalanceId,
        direction: RebalanceDirection,
    },
    #[error(
        "Cannot safely resume rebalance {id} from Converting state: the \
         post-deposit conversion order may or may not have been placed \
         on Alpaca and cannot be reconciled without manual intervention"
    )]
    ResumeUnsafeConverting { id: UsdcRebalanceId },
}

impl From<SendError<UsdcRebalance>> for UsdcTransferError {
    fn from(error: SendError<UsdcRebalance>) -> Self {
        Self::Aggregate(Box::new(error))
    }
}
