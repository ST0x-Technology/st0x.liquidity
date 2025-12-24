use alloy::primitives::{Address, TxHash, hex::FromHexError};
use reqwest::{Client, Response, StatusCode};
use rust_decimal::Decimal;
use thiserror::Error;
use tracing::debug;

use super::transfer::{AlpacaAccountId, AlpacaTransferId, Network, TokenSymbol, TransferStatus};
use super::whitelist::{WhitelistEntry, WhitelistStatus};

#[derive(Debug, Error)]
pub enum AlpacaWalletError {
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error("API error (status {status}): {message}")]
    ApiError { status: StatusCode, message: String },
    #[error("Failed to parse response")]
    ParseError(#[from] serde_json::Error),
    #[error(transparent)]
    FromHex(#[from] FromHexError),
    #[error("Amount must be positive and non-zero, got: {amount}")]
    InvalidAmount { amount: Decimal },
    #[error("Transfer not found: {transfer_id}")]
    TransferNotFound { transfer_id: AlpacaTransferId },
    #[error("Transfer {transfer_id} timed out after {elapsed:?}")]
    TransferTimeout {
        transfer_id: AlpacaTransferId,
        elapsed: std::time::Duration,
    },
    #[error("Invalid status transition for transfer {transfer_id}: {previous:?} -> {next:?}")]
    InvalidStatusTransition {
        transfer_id: AlpacaTransferId,
        previous: TransferStatus,
        next: TransferStatus,
    },
    #[error("Address {address} is not whitelisted for {asset} on {network}")]
    AddressNotWhitelisted {
        address: Address,
        asset: TokenSymbol,
        network: Network,
    },
    #[error("Deposit with tx hash {tx_hash} not detected after {elapsed:?}")]
    DepositTimeout {
        tx_hash: TxHash,
        elapsed: std::time::Duration,
    },
}

pub struct AlpacaWalletClient {
    client: Client,
    account_id: AlpacaAccountId,
    base_url: String,
    api_key: String,
    api_secret: String,
}

impl AlpacaWalletClient {
    pub(crate) fn new(
        base_url: String,
        account_id: AlpacaAccountId,
        api_key: String,
        api_secret: String,
    ) -> Self {
        Self {
            client: Client::new(),
            account_id,
            base_url,
            api_key,
            api_secret,
        }
    }

    pub(super) async fn get(&self, path: &str) -> Result<Response, AlpacaWalletError> {
        let url = format!("{}{}", self.base_url, path);
        debug!("GET {url}");

        let response = self
            .client
            .get(&url)
            .basic_auth(&self.api_key, Some(&self.api_secret))
            .header("APCA-API-KEY-ID", &self.api_key)
            .header("APCA-API-SECRET-KEY", &self.api_secret)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());

            return Err(AlpacaWalletError::ApiError { status, message });
        }

        Ok(response)
    }

    pub(super) async fn post<T: serde::Serialize + Sync>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<Response, AlpacaWalletError> {
        let url = format!("{}{}", self.base_url, path);

        // Alpaca API requires both Basic auth AND APCA headers for authentication
        let response = self
            .client
            .post(&url)
            .basic_auth(&self.api_key, Some(&self.api_secret))
            .header("APCA-API-KEY-ID", &self.api_key)
            .header("APCA-API-SECRET-KEY", &self.api_secret)
            .json(body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());

            return Err(AlpacaWalletError::ApiError { status, message });
        }

        Ok(response)
    }

    pub(super) fn account_id(&self) -> &AlpacaAccountId {
        &self.account_id
    }

    pub(super) async fn get_whitelisted_addresses(
        &self,
    ) -> Result<Vec<WhitelistEntry>, AlpacaWalletError> {
        let path = format!("/v1/accounts/{}/wallets/whitelists", self.account_id);

        let response = self.get(&path).await?;
        let entries: Vec<WhitelistEntry> = response.json().await?;

        Ok(entries)
    }

    pub(super) async fn is_address_whitelisted_and_approved(
        &self,
        address: &Address,
        asset: &TokenSymbol,
        _network: &Network,
    ) -> Result<bool, AlpacaWalletError> {
        let entries = self.get_whitelisted_addresses().await?;

        // NOTE: Chain comparison disabled due to Alpaca API inconsistency.
        // Request uses "ethereum" but response returns "ETH".
        // TODO: Re-enable once Alpaca fixes the chain field or we normalize values.
        Ok(entries.iter().any(|entry| {
            entry.address == *address
                && entry.asset == *asset
                && entry.status == WhitelistStatus::Approved
        }))
    }

    /// Creates a whitelist entry for a withdrawal address.
    ///
    /// The address will be in PENDING status initially and must be approved
    /// before withdrawals can be made (typically within 24 hours).
    pub(super) async fn create_whitelist_entry(
        &self,
        address: &Address,
        asset: &TokenSymbol,
        _network: &Network,
    ) -> Result<WhitelistEntry, AlpacaWalletError> {
        #[derive(serde::Serialize)]
        struct Request<'a> {
            address: String,
            asset: &'a str,
        }

        let path = format!("/v1/accounts/{}/wallets/whitelists", self.account_id);

        let request = Request {
            address: address.to_string(),
            asset: asset.as_ref(),
        };

        let response = self.post(&path, &request).await?;
        let text = response.text().await?;

        debug!("Whitelist creation response: {text}");

        Ok(serde_json::from_str::<WhitelistEntry>(&text)?)
    }

    /// Gets or creates a wallet deposit address for a specific asset and network.
    ///
    /// Uses GET with account_id in path per Broker API documentation.
    /// If no wallet exists for the account/asset pair, one will be created.
    pub(super) async fn get_wallet_address(
        &self,
        asset: &TokenSymbol,
        network: &Network,
    ) -> Result<Address, AlpacaWalletError> {
        // Response format from Alpaca API documentation
        #[derive(serde::Deserialize)]
        struct Response {
            #[allow(dead_code)]
            asset_id: String,
            address: Address,
            #[allow(dead_code)]
            created_at: String,
        }

        // Broker API endpoint: GET /v1/accounts/{account_id}/wallets?asset=USDC&network=ethereum
        let path = format!(
            "/v1/accounts/{}/wallets?asset={}&network={}",
            self.account_id,
            asset.as_ref(),
            network.as_ref()
        );

        let response = self.get(&path).await?;
        let text = response.text().await?;

        Ok(serde_json::from_str::<Response>(&text)?.address)
    }
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use serde_json::json;
    use uuid::uuid;

    use super::*;

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    #[test]
    fn test_client_construction() {
        let client = AlpacaWalletClient::new(
            "https://broker-api.sandbox.alpaca.markets".to_string(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        assert_eq!(*client.account_id(), TEST_ACCOUNT_ID);
        assert_eq!(client.api_key, "test_key_id");
        assert_eq!(client.api_secret, "test_secret_key");
    }

    #[tokio::test]
    async fn test_get_with_auth_headers() {
        let server = MockServer::start();

        // Basic auth header: base64("test_key_id:test_secret_key") = "dGVzdF9rZXlfaWQ6dGVzdF9zZWNyZXRfa2V5"
        let test_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/test")
                .header(
                    "authorization",
                    "Basic dGVzdF9rZXlfaWQ6dGVzdF9zZWNyZXRfa2V5",
                )
                .header("APCA-API-KEY-ID", "test_key_id")
                .header("APCA-API-SECRET-KEY", "test_secret_key");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({"success": true}));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let response = client.get("/v1/test").await.unwrap();

        assert!(response.status().is_success());
        test_mock.assert();
    }

    #[tokio::test]
    async fn test_get_api_error() {
        let server = MockServer::start();

        let error_mock = server.mock(|when, then| {
            when.method(GET).path("/v1/error");
            then.status(401).json_body(json!({
                "message": "Invalid credentials"
            }));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let result = client.get("/v1/error").await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == StatusCode::UNAUTHORIZED
        ));

        error_mock.assert();
    }

    #[tokio::test]
    async fn test_get_server_error() {
        let server = MockServer::start();

        let error_mock = server.mock(|when, then| {
            when.method(GET).path("/v1/server_error");
            then.status(500).body("Internal Server Error");
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let result = client.get("/v1/server_error").await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == StatusCode::INTERNAL_SERVER_ERROR
        ));

        error_mock.assert();
    }

    #[tokio::test]
    async fn test_post_with_json_body() {
        let server = MockServer::start();

        let test_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/test")
                .header("APCA-API-KEY-ID", "test_key_id")
                .header("APCA-API-SECRET-KEY", "test_secret_key")
                .json_body(json!({"amount": "10.5", "asset": "USDC"}));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({"success": true}));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let body = json!({"amount": "10.5", "asset": "USDC"});
        let response = client.post("/v1/test", &body).await.unwrap();

        assert!(response.status().is_success());
        test_mock.assert();
    }

    #[tokio::test]
    async fn test_post_api_error() {
        let server = MockServer::start();

        let error_mock = server.mock(|when, then| {
            when.method(POST).path("/v1/error");
            then.status(400).json_body(json!({
                "message": "Invalid request"
            }));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let body = json!({"test": "data"});
        let result = client.post("/v1/error", &body).await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == StatusCode::BAD_REQUEST
        ));

        error_mock.assert();
    }

    #[tokio::test]
    async fn test_get_wallet_address_success() {
        let server = MockServer::start();
        let expected_address = "0x42a76C83014e886e639768D84EAF3573b1876844";

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "asset_id": "5d0de74f-827b-41a7-9f74-9c07c08fe55f",
                    "address": expected_address,
                    "created_at": "2025-08-07T08:52:40.656166Z"
                }));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let result = client
            .get_wallet_address(&TokenSymbol::new("USDC"), &Network::new("ethereum"))
            .await
            .unwrap();

        assert_eq!(
            result.to_string().to_lowercase(),
            expected_address.to_lowercase()
        );
        mock.assert();
    }

    #[tokio::test]
    async fn test_get_wallet_address_api_error() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets"));
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({
                    "message": "Invalid asset or network"
                }));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let result = client
            .get_wallet_address(&TokenSymbol::new("INVALID"), &Network::new("ethereum"))
            .await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == StatusCode::BAD_REQUEST
        ));
        mock.assert();
    }

    #[tokio::test]
    async fn test_get_wallet_address_empty_response() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets"));
            then.status(200)
                .header("content-type", "application/json")
                .body("");
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let result = client
            .get_wallet_address(&TokenSymbol::new("USDC"), &Network::new("ethereum"))
            .await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::ParseError(_)
        ));
        mock.assert();
    }
}
