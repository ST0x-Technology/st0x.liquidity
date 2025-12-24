use alloy::primitives::{Address, TxHash, hex::FromHexError};
use reqwest::{Client, Response, StatusCode};
use rust_decimal::Decimal;
use serde::Deserialize;
use thiserror::Error;

use super::transfer::{AlpacaTransferId, Network, TokenSymbol, TransferStatus};
use super::whitelist::{WhitelistEntry, WhitelistStatus};

#[cfg(test)]
use httpmock::prelude::GET;
#[cfg(test)]
use serde_json::json;

#[derive(Debug, Error)]
pub enum AlpacaWalletError {
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error("API error (status {status}): {message}")]
    ApiError { status: StatusCode, message: String },
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
    #[error("Invalid status transition for deposit {tx_hash}: {previous:?} -> {next:?}")]
    InvalidDepositTransition {
        tx_hash: TxHash,
        previous: TransferStatus,
        next: TransferStatus,
    },
}

#[derive(Deserialize)]
struct AccountResponse {
    id: String,
}

pub struct AlpacaWalletClient {
    client: Client,
    account_id: String,
    base_url: String,
    api_key: String,
    api_secret: String,
}

impl AlpacaWalletClient {
    /// Creates a new Alpaca wallet client.
    ///
    /// Fetches the account ID from the Alpaca API during construction.
    pub(crate) async fn new(
        base_url: String,
        api_key: String,
        api_secret: String,
    ) -> Result<Self, AlpacaWalletError> {
        let client = Client::new();

        let account_id = Self::fetch_account_id(&client, &base_url, &api_key, &api_secret).await?;

        Ok(Self {
            client,
            account_id,
            base_url,
            api_key,
            api_secret,
        })
    }

    async fn fetch_account_id(
        client: &Client,
        base_url: &str,
        api_key: &str,
        api_secret: &str,
    ) -> Result<String, AlpacaWalletError> {
        let url = format!("{base_url}/v2/account");

        let response = client
            .get(&url)
            .header("APCA-API-KEY-ID", api_key)
            .header("APCA-API-SECRET-KEY", api_secret)
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

        let account: AccountResponse = response.json().await?;

        Ok(account.id)
    }

    pub(super) async fn get(&self, path: &str) -> Result<Response, AlpacaWalletError> {
        let url = format!("{}{}", self.base_url, path);

        let response = self
            .client
            .get(&url)
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

        let response = self
            .client
            .post(&url)
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

    pub(super) fn account_id(&self) -> &str {
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
        network: &Network,
    ) -> Result<bool, AlpacaWalletError> {
        let entries = self.get_whitelisted_addresses().await?;

        Ok(entries.iter().any(|entry| {
            entry.address == *address
                && entry.asset == *asset
                && entry.chain == *network
                && entry.status == WhitelistStatus::Approved
        }))
    }
}

#[cfg(test)]
pub(crate) fn create_account_mock<'a>(
    server: &'a httpmock::MockServer,
    account_id: &str,
) -> httpmock::Mock<'a> {
    server.mock(|when, then| {
        when.method(GET).path("/v2/account");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!({
                "id": account_id,
                "account_number": "PA1234567890",
                "status": "ACTIVE",
                "currency": "USD",
                "buying_power": "100000.00",
                "regt_buying_power": "100000.00",
                "daytrading_buying_power": "400000.00",
                "non_marginable_buying_power": "100000.00",
                "cash": "100000.00",
                "accrued_fees": "0",
                "pending_transfer_out": "0",
                "pending_transfer_in": "0",
                "portfolio_value": "100000.00",
                "pattern_day_trader": false,
                "trading_blocked": false,
                "transfers_blocked": false,
                "account_blocked": false,
                "created_at": "2020-01-01T00:00:00Z",
                "trade_suspended_by_user": false,
                "multiplier": "4",
                "shorting_enabled": true,
                "equity": "100000.00",
                "last_equity": "100000.00",
                "long_market_value": "0",
                "short_market_value": "0",
                "initial_margin": "0",
                "maintenance_margin": "0",
                "last_maintenance_margin": "0",
                "sma": "0",
                "daytrade_count": 0
            }));
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_client_construction() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let client = AlpacaWalletClient::new(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        assert_eq!(client.account_id(), expected_account_id);
        assert_eq!(client.api_key, "test_key_id");
        assert_eq!(client.api_secret, "test_secret_key");
        account_mock.assert();
    }

    #[tokio::test]
    async fn test_get_with_auth_headers() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let test_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/test")
                .header("APCA-API-KEY-ID", "test_key_id")
                .header("APCA-API-SECRET-KEY", "test_secret_key");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({"success": true}));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let response = client.get("/v1/test").await.unwrap();

        assert!(response.status().is_success());
        account_mock.assert();
        test_mock.assert();
    }

    #[tokio::test]
    async fn test_get_api_error() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let error_mock = server.mock(|when, then| {
            when.method(GET).path("/v1/error");
            then.status(401).json_body(json!({
                "message": "Invalid credentials"
            }));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = client.get("/v1/error").await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == StatusCode::UNAUTHORIZED
        ));

        account_mock.assert();
        error_mock.assert();
    }

    #[tokio::test]
    async fn test_get_server_error() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let error_mock = server.mock(|when, then| {
            when.method(GET).path("/v1/server_error");
            then.status(500).body("Internal Server Error");
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = client.get("/v1/server_error").await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == StatusCode::INTERNAL_SERVER_ERROR
        ));

        account_mock.assert();
        error_mock.assert();
    }

    #[tokio::test]
    async fn test_post_with_json_body() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

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
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let body = json!({"amount": "10.5", "asset": "USDC"});
        let response = client.post("/v1/test", &body).await.unwrap();

        assert!(response.status().is_success());
        account_mock.assert();
        test_mock.assert();
    }

    #[tokio::test]
    async fn test_post_api_error() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let error_mock = server.mock(|when, then| {
            when.method(POST).path("/v1/error");
            then.status(400).json_body(json!({
                "message": "Invalid request"
            }));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let body = json!({"test": "data"});
        let result = client.post("/v1/error", &body).await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == StatusCode::BAD_REQUEST
        ));

        account_mock.assert();
        error_mock.assert();
    }
}
