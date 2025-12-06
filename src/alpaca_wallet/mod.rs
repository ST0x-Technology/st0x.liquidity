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

use transfer::DepositAddress;
use whitelist::WhitelistEntry;

pub(crate) use client::AlpacaWalletError;
pub(crate) use status::PollingConfig;
pub(crate) use transfer::{AlpacaTransferId, Network, TokenSymbol, Transfer, TransferStatus};

#[cfg(test)]
pub(crate) use client::{AlpacaWalletClient, create_account_mock};

// TODO(#137): Remove dead_code allow when rebalancing orchestration uses this service
#[allow(dead_code)]
/// Service facade for Alpaca crypto wallet operations.
///
/// Provides a high-level API for deposits, withdrawals, and transfer polling.
pub(crate) struct AlpacaWalletService {
    client: Arc<client::AlpacaWalletClient>,
    polling_config: PollingConfig,
}

// TODO(#137): Remove dead_code allow when rebalancing orchestration uses this service
#[allow(dead_code)]
impl AlpacaWalletService {
    #[cfg(test)]
    pub(crate) fn new_with_client(
        client: client::AlpacaWalletClient,
        polling_config: Option<PollingConfig>,
    ) -> Self {
        Self {
            client: Arc::new(client),
            polling_config: polling_config.unwrap_or_default(),
        }
    }

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
        transfer_id: &AlpacaTransferId,
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
        transfer_id: &AlpacaTransferId,
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

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use httpmock::prelude::*;
    use rust_decimal::Decimal;
    use serde_json::json;
    use std::time::Duration;

    use super::*;
    use client::create_account_mock;

    async fn create_test_service(server: &MockServer) -> AlpacaWalletService {
        let account_mock = create_account_mock(server, "test-account-id");

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key".to_string(),
            "test_secret".to_string(),
        )
        .await
        .unwrap();

        account_mock.assert();

        AlpacaWalletService::new_with_client(client, None)
    }

    #[tokio::test]
    async fn test_get_deposit_address() {
        let server = MockServer::start();
        let service = create_test_service(&server).await;

        let wallet_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/crypto/funding_wallets")
                .query_param("asset", "USDC")
                .query_param("network", "ethereum");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "address": "0x1234567890abcdef1234567890abcdef12345678",
                    "asset": "USDC",
                    "network": "Ethereum"
                }]));
        });

        let asset = TokenSymbol::new("USDC");
        let network = Network::new("ethereum");

        let result = service.get_deposit_address(&asset, &network).await.unwrap();

        assert_eq!(
            result.address,
            address!("0x1234567890abcdef1234567890abcdef12345678")
        );
        wallet_mock.assert();
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_not_whitelisted() {
        let server = MockServer::start();
        let service = create_test_service(&server).await;

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account-id/wallets/whitelists");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let asset = TokenSymbol::new("USDC");
        let to_address = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = service
            .initiate_withdrawal(Decimal::new(100, 0), &asset, &to_address)
            .await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::AddressNotWhitelisted { .. }
        ));
        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_pending_whitelist() {
        let server = MockServer::start();
        let service = create_test_service(&server).await;

        let to_address = address!("0x1234567890abcdef1234567890abcdef12345678");

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account-id/wallets/whitelists");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": "whitelist-123",
                    "address": to_address.to_string(),
                    "asset": "USDC",
                    "chain": "ethereum",
                    "status": "PENDING",
                    "created_at": "2024-01-01T00:00:00Z"
                }]));
        });

        let asset = TokenSymbol::new("USDC");

        let result = service
            .initiate_withdrawal(Decimal::new(100, 0), &asset, &to_address)
            .await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::AddressNotWhitelisted { .. }
        ));
        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_approved() {
        let server = MockServer::start();
        let service = create_test_service(&server).await;

        let to_address = address!("0x1234567890abcdef1234567890abcdef12345678");

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account-id/wallets/whitelists");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": "whitelist-123",
                    "address": to_address.to_string(),
                    "asset": "USDC",
                    "chain": "ethereum",
                    "status": "APPROVED",
                    "created_at": "2024-01-01T00:00:00Z"
                }]));
        });

        let transfer_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account-id/wallets/transfers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "550e8400-e29b-41d4-a716-446655440000",
                    "relationship": "OUTGOING",
                    "amount": "100",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": to_address.to_string(),
                    "status": "PENDING",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": null
                }));
        });

        let asset = TokenSymbol::new("USDC");

        let result = service
            .initiate_withdrawal(Decimal::new(100, 0), &asset, &to_address)
            .await
            .unwrap();

        assert_eq!(result.to, to_address);
        whitelist_mock.assert();
        transfer_mock.assert();
    }

    #[tokio::test]
    async fn test_whitelist_address() {
        let server = MockServer::start();
        let service = create_test_service(&server).await;

        let address = address!("0x1234567890abcdef1234567890abcdef12345678");

        let whitelist_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/accounts/test-account-id/wallets/whitelists");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "whitelist-123",
                    "address": address,
                    "asset": "USDC",
                    "chain": "ethereum",
                    "status": "PENDING",
                    "created_at": "2024-01-01T00:00:00Z"
                }));
        });

        let asset = TokenSymbol::new("USDC");
        let network = Network::new("ethereum");

        let result = service
            .whitelist_address(&address, &asset, &network)
            .await
            .unwrap();

        assert_eq!(result.address, address);
        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_get_whitelisted_addresses() {
        let server = MockServer::start();
        let service = create_test_service(&server).await;

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account-id/wallets/whitelists");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": "whitelist-1",
                        "address": "0x1111111111111111111111111111111111111111",
                        "asset": "USDC",
                        "chain": "ethereum",
                        "status": "APPROVED",
                        "created_at": "2024-01-01T00:00:00Z"
                    },
                    {
                        "id": "whitelist-2",
                        "address": "0x2222222222222222222222222222222222222222",
                        "asset": "USDC",
                        "chain": "polygon",
                        "status": "PENDING",
                        "created_at": "2024-01-02T00:00:00Z"
                    }
                ]));
        });

        let result = service.get_whitelisted_addresses().await.unwrap();

        assert_eq!(result.len(), 2);
        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_poll_transfer_until_complete() {
        let server = MockServer::start();

        let polling_config = PollingConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let account_mock = create_account_mock(&server, "test-account-id");

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key".to_string(),
            "test_secret".to_string(),
        )
        .await
        .unwrap();

        account_mock.assert();

        let service = AlpacaWalletService::new_with_client(client, Some(polling_config));

        let transfer_id = uuid::Uuid::new_v4();

        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/test-account-id/wallets/transfers")
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": transfer_id,
                    "relationship": "OUTGOING",
                    "amount": "100",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "COMPLETE",
                    "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": "0.5"
                }]));
        });

        let tid = transfer::AlpacaTransferId::from(transfer_id);
        let result = service.poll_transfer_until_complete(&tid).await.unwrap();

        assert_eq!(result.status, transfer::TransferStatus::Complete);
        status_mock.assert();
    }
}
