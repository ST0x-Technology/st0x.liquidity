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

use alloy::primitives::{Address, TxHash};
use rust_decimal::Decimal;
use std::sync::Arc;

pub(crate) use client::{AlpacaWalletClient, AlpacaWalletError};
pub(crate) use status::PollingConfig;
pub(crate) use transfer::{
    AlpacaAccountId, AlpacaTransferId, Network, TokenSymbol, Transfer, TransferStatus,
};
pub(crate) use whitelist::WhitelistStatus;

/// Service facade for Alpaca crypto wallet operations.
///
/// Provides a high-level API for deposits, withdrawals, and transfer polling.
pub(crate) struct AlpacaWalletService {
    client: Arc<AlpacaWalletClient>,
    polling_config: PollingConfig,
}

impl AlpacaWalletService {
    pub(crate) fn new(
        base_url: String,
        account_id: AlpacaAccountId,
        api_key: String,
        api_secret: String,
    ) -> Self {
        let client = AlpacaWalletClient::new(base_url, account_id, api_key, api_secret);

        Self {
            client: Arc::new(client),
            polling_config: PollingConfig::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_client(
        client: AlpacaWalletClient,
        polling_config: Option<PollingConfig>,
    ) -> Self {
        Self {
            client: Arc::new(client),
            polling_config: polling_config.unwrap_or_default(),
        }
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
    pub(crate) async fn initiate_withdrawal(
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
    pub(crate) async fn poll_transfer_until_complete(
        &self,
        transfer_id: &AlpacaTransferId,
    ) -> Result<Transfer, AlpacaWalletError> {
        status::poll_transfer_status(&self.client, transfer_id, &self.polling_config).await
    }

    /// Polls for an incoming deposit by its on-chain transaction hash.
    ///
    /// Alpaca auto-detects incoming transfers to their funding wallet addresses.
    /// This method polls until the deposit is detected and reaches a terminal state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The deposit times out (not detected within timeout)
    /// - The API call fails persistently
    pub(crate) async fn poll_deposit_by_tx_hash(
        &self,
        tx_hash: &TxHash,
    ) -> Result<Transfer, AlpacaWalletError> {
        status::poll_deposit_by_tx_hash(&self.client, tx_hash, &self.polling_config).await
    }

    pub(crate) async fn get_wallet_address(
        &self,
        asset: &TokenSymbol,
        network: &Network,
    ) -> Result<Address, AlpacaWalletError> {
        self.client.get_wallet_address(asset, network).await
    }

    pub(crate) async fn create_whitelist_entry(
        &self,
        address: &Address,
        asset: &TokenSymbol,
        network: &Network,
    ) -> Result<whitelist::WhitelistEntry, AlpacaWalletError> {
        self.client
            .create_whitelist_entry(address, asset, network)
            .await
    }

    /// Gets all whitelisted addresses for this account.
    pub(crate) async fn get_whitelisted_addresses(
        &self,
    ) -> Result<Vec<whitelist::WhitelistEntry>, AlpacaWalletError> {
        self.client.get_whitelisted_addresses().await
    }

    /// Lists all transfers for this account.
    pub(crate) async fn list_all_transfers(&self) -> Result<Vec<Transfer>, AlpacaWalletError> {
        transfer::list_all_transfers(&self.client).await
    }

    /// Simulates an incoming wire transfer in sandbox environment.
    ///
    /// This endpoint only works in sandbox (broker-api.sandbox.alpaca.markets).
    /// It immediately credits USD to the account for testing purposes.
    ///
    /// # Arguments
    /// * `amount` - Amount in USD to credit to the account
    /// * `wire_instruction` - Wire routing instruction provided by Alpaca
    ///
    /// # Errors
    /// Returns `SandboxOnlyEndpoint` if called against production API.
    pub(crate) async fn fund_sandbox(
        &self,
        amount: Decimal,
        wire_instruction: &str,
    ) -> Result<(), AlpacaWalletError> {
        self.client.fund_sandbox(amount, wire_instruction).await
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use httpmock::prelude::*;
    use rust_decimal::Decimal;
    use serde_json::json;
    use std::time::Duration;
    use uuid::uuid;

    use super::*;

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    fn create_test_service(server: &MockServer) -> AlpacaWalletService {
        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key".to_string(),
            "test_secret".to_string(),
        );

        AlpacaWalletService::new_with_client(client, None)
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_not_whitelisted() {
        let server = MockServer::start();
        let service = create_test_service(&server);

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/whitelists");
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
        let service = create_test_service(&server);

        let to_address = address!("0x1234567890abcdef1234567890abcdef12345678");

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/whitelists");
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
        let service = create_test_service(&server);

        let to_address = address!("0x1234567890abcdef1234567890abcdef12345678");

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/whitelists");
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
                .path("/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/transfers");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "550e8400-e29b-41d4-a716-446655440000",
                    "direction": "OUTGOING",
                    "amount": "100",
                    "usd_value": "100",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x0000000000000000000000000000000000000001",
                    "to_address": to_address.to_string(),
                    "status": "PENDING",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0",
                    "fees": "0"
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
    async fn test_poll_transfer_until_complete() {
        let server = MockServer::start();

        let polling_config = PollingConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key".to_string(),
            "test_secret".to_string(),
        );

        let service = AlpacaWalletService::new_with_client(client, Some(polling_config));

        let transfer_id = uuid::Uuid::new_v4();

        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/accounts/904837e3-3b76-47ec-b432-046db621571b/wallets/transfers")
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": transfer_id,
                    "direction": "OUTGOING",
                    "amount": "100",
                    "usd_value": "100",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x0000000000000000000000000000000000000001",
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "COMPLETE",
                    "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0.5",
                    "fees": "0"
                }]));
        });

        let tid = transfer::AlpacaTransferId::from(transfer_id);
        let result = service.poll_transfer_until_complete(&tid).await.unwrap();

        assert_eq!(result.status, transfer::TransferStatus::Complete);
        status_mock.assert();
    }

    #[tokio::test]
    async fn test_fund_sandbox_success() {
        let server = MockServer::start();

        // Path includes /sandbox because base_url ends with /sandbox
        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/sandbox/v1/testing/incoming_wires")
                .json_body(json!({
                    "amount": "500",
                    "wire_instructions": "FFC st4P-test-account-id"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({"success": true}));
        });

        let client = AlpacaWalletClient::new(
            format!("{}/sandbox", server.base_url()),
            TEST_ACCOUNT_ID,
            "test_key".to_string(),
            "test_secret".to_string(),
        );

        let service = AlpacaWalletService::new_with_client(client, None);
        let amount = Decimal::new(500, 0);
        let result = service
            .fund_sandbox(amount, "FFC st4P-test-account-id")
            .await;

        assert!(result.is_ok());
        mock.assert();
    }

    #[tokio::test]
    async fn test_fund_sandbox_rejects_production() {
        let server = MockServer::start();
        let service = create_test_service(&server);

        let amount = Decimal::new(500, 0);
        let result = service.fund_sandbox(amount, "FFC st4P-123456").await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::SandboxOnlyEndpoint
        ));
    }
}
