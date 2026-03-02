//! USDC cross-venue transfers between Alpaca and Base.
//!
//! [`CrossVenueCashTransfer`] implements [`CrossVenueTransfer`] in both
//! directions for USDC, handling USD/USDC conversion, withdrawal, CCTP
//! bridging, and deposit.

mod manager;
#[cfg(test)]
pub(crate) mod mock;

pub(crate) use manager::CrossVenueCashTransfer;

use rain_math_float::FloatError;
use thiserror::Error;

use st0x_bridge::cctp::CctpError;
use st0x_event_sorcery::SendError;
use st0x_execution::{AlpacaBrokerApiError, InvalidSharesError};

use crate::alpaca_wallet::AlpacaWalletError;
use crate::onchain::raindex::RaindexError;
use crate::threshold::UsdcConversionError;

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
    Aggregate(Box<SendError<crate::usdc_rebalance::UsdcRebalance>>),
    #[error("Withdrawal failed with terminal status: {status}")]
    WithdrawalFailed { status: String },
    #[error("Deposit failed with terminal status: {status}")]
    DepositFailed { status: String },
    #[error("USDC conversion error: {0}")]
    UsdcConversion(#[from] UsdcConversionError),
    #[error("Float operation error: {0}")]
    Float(#[from] FloatError),
    #[error("Decimal parse error: {0}")]
    DecimalParse(#[from] rust_decimal::Error),
    #[error("Invalid shares: {0}")]
    InvalidShares(#[from] InvalidSharesError),
    #[error(
        "Conversion order {order_id} filled but \
         filled_quantity is missing"
    )]
    MissingFilledQuantity { order_id: uuid::Uuid },
}

impl From<SendError<crate::usdc_rebalance::UsdcRebalance>> for UsdcTransferError {
    fn from(error: SendError<crate::usdc_rebalance::UsdcRebalance>) -> Self {
        Self::Aggregate(Box::new(error))
    }
}
