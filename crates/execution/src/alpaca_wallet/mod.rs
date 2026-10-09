//! Alpaca wallet API types and client supplied by the shared integration crate.

pub use st0x_alpaca::wallet::{
    AlpacaTransferId, AlpacaWalletError, AlpacaWalletService, Network, PollingConfig, TokenSymbol,
    Transfer, TransferStatus, TravelRuleInfo, WhitelistEntry, WhitelistStatus,
    poll_deposit_by_tx_hash_with, poll_transfer_until_complete_with,
};

/// Keeps status polling but returns lookup errors to the outer retry owner immediately.
#[must_use]
pub fn immediate_error_polling_config() -> PollingConfig {
    PollingConfig {
        max_retries: 0,
        ..PollingConfig::default()
    }
}

#[cfg(any(test, feature = "test-support"))]
pub use st0x_alpaca::wallet::AlpacaWalletClient;
