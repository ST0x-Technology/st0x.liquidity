//! Alpaca crypto wallet API client for deposits and withdrawals.
//!
//! This module provides a client for interacting with Alpaca's crypto wallet API,
//! supporting USDC deposits and withdrawals.
//!
//! # Authentication
//!
//! Authentication uses Alpaca API credentials (API key and secret).
//! The client automatically fetches and caches the account ID.
//!
//! # Whitelisting
//!
//! Alpaca requires addresses to be whitelisted before withdrawals. After whitelisting,
//! there is a 24-hour approval period before the address can be used.
//!
//! # Transfer Lifecycle
//!
//! Transfers progress through states: Pending → Processing → Complete/Failed.
//! Use `poll_transfer_until_complete()` to wait for a transfer to reach a terminal state.

mod client;
mod status;
mod transfer;
mod whitelist;

use alloy::primitives::Address;
use rust_decimal::Decimal;
use std::sync::Arc;

use client::AlpacaWalletClient;
use status::PollingConfig;
use transfer::DepositAddress;
use whitelist::WhitelistEntry;

pub(crate) use client::AlpacaWalletError;
pub(crate) use transfer::{Network, TokenSymbol, Transfer, TransferId};

// TODO(#137): Remove dead_code allow when rebalancing orchestration uses this service
#[allow(dead_code)]
/// Service facade for Alpaca crypto wallet operations.
///
/// Provides a high-level API for deposits, withdrawals, and transfer polling.
pub(crate) struct AlpacaWalletService {
    client: Arc<AlpacaWalletClient>,
    polling_config: PollingConfig,
}

// TODO(#137): Remove dead_code allow when rebalancing orchestration uses this service
#[allow(dead_code)]
impl AlpacaWalletService {
    /// Gets the deposit address for an asset and network.
    ///
    /// # Errors
    ///
    /// Returns an error if the API call fails or no wallet is found.
    pub async fn get_deposit_address(
        &self,
        asset: &TokenSymbol,
        network: &Network,
    ) -> Result<DepositAddress, AlpacaWalletError> {
        transfer::get_deposit_address(&self.client, &asset.0, &network.0).await
    }

    /// Initiates a withdrawal to a whitelisted address.
    ///
    /// The address must be whitelisted and approved before this call.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The amount is invalid (zero or negative)
    /// - The address is not whitelisted and approved
    /// - The API call fails
    pub async fn initiate_withdrawal(
        &self,
        amount: Decimal,
        asset: &TokenSymbol,
        to_address: &Address,
    ) -> Result<Transfer, AlpacaWalletError> {
        let network = Network::new("ethereum");

        if !self
            .client
            .is_address_whitelisted_and_approved(to_address, asset, &network)
            .await?
        {
            return Err(AlpacaWalletError::AddressNotWhitelisted {
                address: *to_address,
                asset: asset.clone(),
                network,
            });
        }

        transfer::initiate_withdrawal(&self.client, amount, &asset.0, &to_address.to_string()).await
    }

    /// Gets the current status of a transfer.
    ///
    /// # Errors
    ///
    /// Returns an error if the transfer is not found or the API call fails.
    pub async fn get_transfer_status(
        &self,
        transfer_id: &TransferId,
    ) -> Result<Transfer, AlpacaWalletError> {
        transfer::get_transfer_status(&self.client, transfer_id).await
    }

    /// Polls a transfer until it reaches a terminal state (Complete or Failed).
    ///
    /// This method will retry transient errors and timeout after the configured duration.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The transfer times out
    /// - An invalid status regression is detected
    /// - The API call fails persistently
    pub async fn poll_transfer_until_complete(
        &self,
        transfer_id: &TransferId,
    ) -> Result<Transfer, AlpacaWalletError> {
        status::poll_transfer_status(&self.client, transfer_id, &self.polling_config).await
    }

    /// Whitelists an address for withdrawals.
    ///
    /// After whitelisting, there is a 24-hour approval period before the address can be used.
    ///
    /// # Errors
    ///
    /// Returns an error if the API call fails.
    pub async fn whitelist_address(
        &self,
        address: &Address,
        asset: &TokenSymbol,
        network: &Network,
    ) -> Result<WhitelistEntry, AlpacaWalletError> {
        self.client.whitelist_address(address, asset, network).await
    }

    /// Gets all whitelisted addresses.
    ///
    /// # Errors
    ///
    /// Returns an error if the API call fails.
    pub async fn get_whitelisted_addresses(
        &self,
    ) -> Result<Vec<WhitelistEntry>, AlpacaWalletError> {
        self.client.get_whitelisted_addresses().await
    }
}
