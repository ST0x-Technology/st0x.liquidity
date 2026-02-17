//! Alpaca Broker API crypto transfer types and operations.
//!
//! Provides `request_withdrawal` and `get_transfer_status`
//! for initiating and tracking crypto transfers. Transfers
//! progress through Queued -> Pending -> Complete/Failed.

use alloy::primitives::{Address, TxHash};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::client::{AlpacaWalletClient, AlpacaWalletError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TokenSymbol(pub(super) String);

impl TokenSymbol {
    pub(crate) fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl From<String> for TokenSymbol {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl AsRef<str> for TokenSymbol {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TokenSymbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AlpacaAccountId(Uuid);

impl AlpacaAccountId {
    pub(crate) const fn new(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl std::fmt::Display for AlpacaAccountId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AlpacaTransferId(Uuid);

impl AlpacaTransferId {
    #[cfg(test)]
    fn new(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl From<Uuid> for AlpacaTransferId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl std::fmt::Display for AlpacaTransferId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub(crate) enum TransferDirection {
    Incoming,
    Outgoing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub(crate) enum TransferStatus {
    Pending,
    Processing,
    Complete,
    Failed,
}

/// Transfer response from Alpaca Crypto Wallets API.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct Transfer {
    pub(crate) id: AlpacaTransferId,
    #[serde(rename = "tx_hash", default)]
    pub(crate) tx: Option<TxHash>,
    pub(crate) direction: TransferDirection,
    #[serde(deserialize_with = "deserialize_decimal_from_string")]
    pub(crate) amount: Decimal,
    #[serde(deserialize_with = "deserialize_decimal_from_string")]
    pub(crate) usd_value: Decimal,
    pub(crate) chain: String,
    pub(crate) asset: TokenSymbol,
    #[serde(rename = "from_address")]
    pub(crate) from: Address,
    #[serde(rename = "to_address")]
    pub(crate) to: Address,
    pub(crate) status: TransferStatus,
    pub(crate) created_at: DateTime<Utc>,
    #[serde(deserialize_with = "deserialize_decimal_from_string")]
    pub(crate) network_fee: Decimal,
    #[serde(deserialize_with = "deserialize_decimal_from_string")]
    pub(crate) fees: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Network(pub(super) String);

impl<'de> serde::Deserialize<'de> for Network {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Ok(Self::new(raw))
    }
}

impl Network {
    pub(crate) fn new(s: impl Into<String>) -> Self {
        Self(s.into().to_lowercase())
    }
}

impl From<String> for Network {
    fn from(s: String) -> Self {
        Self(s.to_lowercase())
    }
}

impl AsRef<str> for Network {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Network {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

fn deserialize_decimal_from_string<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    raw.parse::<Decimal>().map_err(serde::de::Error::custom)
}

fn validate_amount(amount: Decimal) -> Result<(), AlpacaWalletError> {
    if amount <= Decimal::ZERO {
        return Err(AlpacaWalletError::InvalidAmount { amount });
    }

    Ok(())
}

#[derive(Serialize)]
struct WithdrawalRequest {
    amount: String,
    asset: String,
    address: String,
}

pub(super) async fn initiate_withdrawal(
    client: &AlpacaWalletClient,
    amount: Decimal,
    asset: &str,
    address: &str,
) -> Result<Transfer, AlpacaWalletError> {
    validate_amount(amount)?;

    let request = WithdrawalRequest {
        amount: amount.to_string(),
        asset: asset.to_string(),
        address: address.to_string(),
    };

    let path = format!("/v1/accounts/{}/wallets/transfers", client.account_id());

    let response = client.post(&path, &request).await?;

    let transfer: Transfer = response.json().await?;

    Ok(transfer)
}

pub(super) async fn get_transfer_status(
    client: &AlpacaWalletClient,
    transfer_id: &AlpacaTransferId,
) -> Result<Transfer, AlpacaWalletError> {
    // Fetch all transfers and filter by ID on our side.
    // The API's transfer_id query param appears to be unreliable.
    let path = format!("/v1/accounts/{}/wallets/transfers", client.account_id());

    let response = client.get(&path).await?;

    let transfers: Vec<Transfer> = response.json().await?;

    transfers
        .into_iter()
        .find(|transfer| transfer.id == *transfer_id)
        .ok_or_else(|| AlpacaWalletError::TransferNotFound {
            transfer_id: *transfer_id,
        })
}

/// Lists all transfers for the account.
pub(super) async fn list_all_transfers(
    client: &AlpacaWalletClient,
) -> Result<Vec<Transfer>, AlpacaWalletError> {
    let path = format!("/v1/accounts/{}/wallets/transfers", client.account_id());

    let response = client.get(&path).await?;

    Ok(response.json().await?)
}

/// Finds a transfer by its transaction hash.
///
/// Fetches all transfers and filters by tx_hash. Returns the first match
/// or None if no transfer with that tx hash exists.
pub(super) async fn find_transfer_by_tx_hash(
    client: &AlpacaWalletClient,
    tx_hash: &TxHash,
) -> Result<Option<Transfer>, AlpacaWalletError> {
    let path = format!("/v1/accounts/{}/wallets/transfers", client.account_id());

    let response = client.get(&path).await?;

    let transfers: Vec<Transfer> = response.json().await?;

    Ok(transfers
        .into_iter()
        .find(|transfer| transfer.tx.as_ref() == Some(tx_hash)))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::fixed_bytes;
    use httpmock::prelude::*;
    use serde_json::json;
    use std::str::FromStr;
    use uuid::uuid;

    use super::*;

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    #[tokio::test]
    async fn test_initiate_withdrawal_successful() {
        let server = MockServer::start();
        let transfer_id = Uuid::new_v4();
        let withdrawal_mock = server.mock(|when, then| {
            when.method(POST)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"))
                .json_body(json!({
                    "amount": "100.5",
                    "asset": "USDC",
                    "address": "0x1234567890abcdef1234567890abcdef12345678"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": transfer_id,
                    "direction": "OUTGOING",
                    "amount": "100.5",
                    "usd_value": "100.48",
                    "chain": "ETH",
                    "asset": "USDC",
                    "from_address": "0xabcdef1234567890abcdef1234567890abcdef12",
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "PENDING",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0.5",
                    "fees": "0"
                }));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let amount = Decimal::new(1005, 1);
        let transfer = initiate_withdrawal(
            &client,
            amount,
            "USDC",
            "0x1234567890abcdef1234567890abcdef12345678",
        )
        .await
        .unwrap();

        let expected_address =
            Address::from_str("0x1234567890abcdef1234567890abcdef12345678").unwrap();

        assert_eq!(transfer.id, AlpacaTransferId::new(transfer_id));
        assert_eq!(transfer.direction, TransferDirection::Outgoing);
        assert_eq!(transfer.amount, Decimal::new(1005, 1));
        assert_eq!(transfer.asset.as_ref(), "USDC");
        assert_eq!(transfer.to, expected_address);
        assert_eq!(transfer.status, TransferStatus::Pending);
        assert_eq!(transfer.network_fee, Decimal::new(5, 1));

        withdrawal_mock.assert();
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_zero_amount() {
        let server = MockServer::start();
        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let err = initiate_withdrawal(
            &client,
            Decimal::ZERO,
            "USDC",
            "0x1234567890abcdef1234567890abcdef12345678",
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            AlpacaWalletError::InvalidAmount { amount } if amount == Decimal::ZERO
        ));
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_negative_amount() {
        let server = MockServer::start();
        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let err = initiate_withdrawal(
            &client,
            Decimal::new(-100, 0),
            "USDC",
            "0x1234567890abcdef1234567890abcdef12345678",
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            AlpacaWalletError::InvalidAmount { amount } if amount == Decimal::new(-100, 0)
        ));
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_invalid_asset() {
        let server = MockServer::start();
        let withdrawal_mock = server.mock(|when, then| {
            when.method(POST)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({
                    "message": "Invalid asset"
                }));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let error = initiate_withdrawal(
            &client,
            Decimal::new(100, 0),
            "INVALID",
            "0x1234567890abcdef1234567890abcdef12345678",
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            AlpacaWalletError::ApiError { status, .. } if status == 400
        ));

        withdrawal_mock.assert();
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_invalid_address() {
        let server = MockServer::start();
        let withdrawal_mock = server.mock(|when, then| {
            when.method(POST)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({
                    "message": "Invalid address"
                }));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let error = initiate_withdrawal(&client, Decimal::new(100, 0), "USDC", "invalid_address")
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            AlpacaWalletError::ApiError { status, .. } if status == 400
        ));

        withdrawal_mock.assert();
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_api_error() {
        let server = MockServer::start();
        let withdrawal_mock = server.mock(|when, then| {
            when.method(POST)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(500).body("Internal Server Error");
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let error = initiate_withdrawal(
            &client,
            Decimal::new(100, 0),
            "USDC",
            "0x1234567890abcdef1234567890abcdef12345678",
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            AlpacaWalletError::ApiError { status, .. } if status == 500
        ));

        withdrawal_mock.assert();
    }

    #[tokio::test]
    async fn test_get_transfer_status_pending() {
        let server = MockServer::start();
        let transfer_id = Uuid::new_v4();
        let other_transfer_id = Uuid::new_v4();
        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": other_transfer_id,
                        "direction": "INCOMING",
                        "amount": "50.0",
                        "usd_value": "49.99",
                        "chain": "ETH",
                        "asset": "USDC",
                        "from_address": "0x9999999999999999999999999999999999999999",
                        "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                        "status": "COMPLETE",
                        "tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                        "created_at": "2024-01-01T00:00:00Z",
                        "network_fee": "0",
                        "fees": "0"
                    },
                    {
                        "id": transfer_id,
                        "direction": "OUTGOING",
                        "amount": "100.0",
                        "usd_value": "99.98",
                        "chain": "ETH",
                        "asset": "USDC",
                        "from_address": "0xabcdef1234567890abcdef1234567890abcdef12",
                        "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                        "status": "PENDING",
                        "tx_hash": null,
                        "created_at": "2024-01-01T00:00:00Z",
                        "network_fee": "0.5",
                        "fees": "0"
                    }
                ]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let result = get_transfer_status(&client, &AlpacaTransferId::from(transfer_id))
            .await
            .unwrap();

        assert_eq!(result.status, TransferStatus::Pending);
        assert_eq!(result.id, AlpacaTransferId::from(transfer_id));

        status_mock.assert();
    }

    #[tokio::test]
    async fn test_get_transfer_status_processing() {
        let server = MockServer::start();
        let transfer_id = Uuid::new_v4();
        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": transfer_id,
                    "direction": "OUTGOING",
                    "amount": "100.0",
                    "usd_value": "99.98",
                    "chain": "ETH",
                    "asset": "USDC",
                    "from_address": "0xabcdef1234567890abcdef1234567890abcdef12",
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "PROCESSING",
                    "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0.5",
                    "fees": "0"
                }]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let result = get_transfer_status(&client, &AlpacaTransferId::from(transfer_id))
            .await
            .unwrap();

        assert_eq!(result.status, TransferStatus::Processing);
        assert!(result.tx.is_some());

        status_mock.assert();
    }

    #[tokio::test]
    async fn test_get_transfer_status_complete() {
        let server = MockServer::start();
        let transfer_id = Uuid::new_v4();
        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": transfer_id,
                    "direction": "OUTGOING",
                    "amount": "100.0",
                    "usd_value": "99.98",
                    "chain": "ETH",
                    "asset": "USDC",
                    "from_address": "0xabcdef1234567890abcdef1234567890abcdef12",
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "COMPLETE",
                    "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0.5",
                    "fees": "0"
                }]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let result = get_transfer_status(&client, &AlpacaTransferId::from(transfer_id))
            .await
            .unwrap();

        assert_eq!(result.status, TransferStatus::Complete);

        status_mock.assert();
    }

    #[tokio::test]
    async fn test_get_transfer_status_failed() {
        let server = MockServer::start();
        let transfer_id = Uuid::new_v4();
        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": transfer_id,
                    "direction": "OUTGOING",
                    "amount": "100.0",
                    "usd_value": "99.98",
                    "chain": "ETH",
                    "asset": "USDC",
                    "from_address": "0xabcdef1234567890abcdef1234567890abcdef12",
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "FAILED",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0",
                    "fees": "0"
                }]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let result = get_transfer_status(&client, &AlpacaTransferId::from(transfer_id))
            .await
            .unwrap();

        assert_eq!(result.status, TransferStatus::Failed);

        status_mock.assert();
    }

    #[tokio::test]
    async fn test_get_transfer_status_not_found() {
        let server = MockServer::start();
        let transfer_id = Uuid::new_v4();
        let other_transfer_id = Uuid::new_v4();
        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": other_transfer_id,
                    "direction": "INCOMING",
                    "amount": "50.0",
                    "usd_value": "49.99",
                    "chain": "ETH",
                    "asset": "USDC",
                    "from_address": "0x9999999999999999999999999999999999999999",
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "COMPLETE",
                    "tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0",
                    "fees": "0"
                }]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let error = get_transfer_status(&client, &AlpacaTransferId::from(transfer_id))
            .await
            .unwrap_err();

        assert!(matches!(error, AlpacaWalletError::TransferNotFound { .. }));

        status_mock.assert();
    }

    #[tokio::test]
    async fn test_get_transfer_status_api_error() {
        let server = MockServer::start();
        let transfer_id = Uuid::new_v4();
        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(500).body("Internal Server Error");
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let error = get_transfer_status(&client, &AlpacaTransferId::from(transfer_id))
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            AlpacaWalletError::ApiError { status, .. } if status == 500
        ));

        status_mock.assert();
    }

    #[tokio::test]
    async fn test_get_transfer_status_malformed_json() {
        let server = MockServer::start();
        let transfer_id = Uuid::new_v4();
        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .body("not valid json");
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let error = get_transfer_status(&client, &AlpacaTransferId::from(transfer_id))
            .await
            .unwrap_err();

        assert!(matches!(error, AlpacaWalletError::Reqwest(_)));

        status_mock.assert();
    }

    /// Regression test: The Alpaca API ignores the transfer_id query parameter
    /// and returns all transfers. A previous bug blindly took the first
    /// transfer from the response, causing the wrong transfer to be returned.
    /// This test verifies we correctly filter by transfer ID on our side.
    #[tokio::test]
    async fn test_get_transfer_status_finds_correct_transfer_among_many() {
        let server = MockServer::start();
        let target_transfer_id = Uuid::new_v4();

        // Simulate the API returning many transfers in arbitrary order.
        // The target transfer is buried in the middle.
        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": Uuid::new_v4(),
                        "direction": "INCOMING",
                        "amount": "0.01",
                        "usd_value": "0.01",
                        "chain": "ETH",
                        "asset": "USDC",
                        "from_address": "0x1111111111111111111111111111111111111111",
                        "to_address": "0xF48c3Bcb8981ed53DcA2455D7462EAAAC20ee760",
                        "status": "COMPLETE",
                        "tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                        "created_at": "2024-12-19T22:29:36Z",
                        "network_fee": "0",
                        "fees": "0"
                    },
                    {
                        "id": Uuid::new_v4(),
                        "direction": "INCOMING",
                        "amount": "0.01",
                        "usd_value": "0.01",
                        "chain": "ETH",
                        "asset": "USDC",
                        "from_address": "0x2222222222222222222222222222222222222222",
                        "to_address": "0xF48c3Bcb8981ed53DcA2455D7462EAAAC20ee760",
                        "status": "COMPLETE",
                        "tx_hash": "0x2222222222222222222222222222222222222222222222222222222222222222",
                        "created_at": "2024-12-20T10:00:00Z",
                        "network_fee": "0",
                        "fees": "0"
                    },
                    {
                        "id": target_transfer_id,
                        "direction": "OUTGOING",
                        "amount": "100",
                        "usd_value": "99.98",
                        "chain": "ETH",
                        "asset": "USDC",
                        "from_address": "0xA0D2C7210D7e2112A4F7888B8658CB579226dB3B",
                        "to_address": "0x5A379C330c84Af97864507FfeA4c23aEAF3476d9",
                        "status": "PROCESSING",
                        "created_at": "2024-12-26T20:43:29Z",
                        "network_fee": "0.5",
                        "fees": "0"
                    },
                    {
                        "id": Uuid::new_v4(),
                        "direction": "INCOMING",
                        "amount": "0.01",
                        "usd_value": "0.01",
                        "chain": "ETH",
                        "asset": "USDC",
                        "from_address": "0x3333333333333333333333333333333333333333",
                        "to_address": "0xF48c3Bcb8981ed53DcA2455D7462EAAAC20ee760",
                        "status": "COMPLETE",
                        "tx_hash": "0x3333333333333333333333333333333333333333333333333333333333333333",
                        "created_at": "2024-12-21T15:00:00Z",
                        "network_fee": "0",
                        "fees": "0"
                    }
                ]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let transfer = get_transfer_status(&client, &AlpacaTransferId::from(target_transfer_id))
            .await
            .unwrap();

        assert_eq!(
            transfer.id,
            AlpacaTransferId::from(target_transfer_id),
            "Should return the transfer with matching ID, not the first one in the list"
        );
        assert_eq!(
            transfer.amount,
            Decimal::new(100, 0),
            "Should return the 100 USDC withdrawal, not a 0.01 USDC deposit"
        );
        assert_eq!(transfer.direction, TransferDirection::Outgoing);
        assert_eq!(transfer.status, TransferStatus::Processing);

        status_mock.assert();
    }

    #[test]
    fn test_network_normalizes_to_lowercase() {
        let network = Network::new("Ethereum");
        assert_eq!(network.as_ref(), "ethereum");
    }

    #[test]
    fn test_network_from_string_normalizes() {
        let network = Network::from("EtHeReuM".to_string());
        assert_eq!(network.as_ref(), "ethereum");
    }

    #[tokio::test]
    async fn test_find_transfer_by_tx_hash_found() {
        let server = MockServer::start();
        let tx_hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");
        let transfer_id = Uuid::new_v4();

        let transfers_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": Uuid::new_v4(),
                        "direction": "OUTGOING",
                        "amount": "100",
                        "usd_value": "99.98",
                        "chain": "ETH",
                        "asset": "USDC",
                        "from_address": "0xabcdef1234567890abcdef1234567890abcdef12",
                        "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                        "status": "COMPLETE",
                        "tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                        "created_at": "2024-01-01T00:00:00Z",
                        "network_fee": "0",
                        "fees": "0"
                    },
                    {
                        "id": transfer_id,
                        "direction": "INCOMING",
                        "amount": "500",
                        "usd_value": "499.90",
                        "chain": "ETH",
                        "asset": "USDC",
                        "from_address": "0x9999999999999999999999999999999999999999",
                        "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                        "status": "COMPLETE",
                        "tx_hash": tx_hash,
                        "created_at": "2024-01-02T00:00:00Z",
                        "network_fee": "0.5",
                        "fees": "0"
                    }
                ]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let transfer = find_transfer_by_tx_hash(&client, &tx_hash)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(transfer.id, AlpacaTransferId::from(transfer_id));
        assert_eq!(transfer.tx, Some(tx_hash));
        assert_eq!(transfer.status, TransferStatus::Complete);

        transfers_mock.assert();
    }

    #[tokio::test]
    async fn test_find_transfer_by_tx_hash_not_found() {
        let server = MockServer::start();
        let tx_hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");

        let transfers_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": Uuid::new_v4(),
                        "direction": "OUTGOING",
                        "amount": "100",
                        "usd_value": "99.98",
                        "chain": "ETH",
                        "asset": "USDC",
                        "from_address": "0xabcdef1234567890abcdef1234567890abcdef12",
                        "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                        "status": "COMPLETE",
                        "tx_hash": "0x2222222222222222222222222222222222222222222222222222222222222222",
                        "created_at": "2024-01-01T00:00:00Z",
                        "network_fee": "0",
                        "fees": "0"
                    }
                ]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let result = find_transfer_by_tx_hash(&client, &tx_hash).await.unwrap();

        assert!(result.is_none());

        transfers_mock.assert();
    }

    #[tokio::test]
    async fn test_find_transfer_by_tx_hash_empty_list() {
        let server = MockServer::start();
        let tx_hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");

        let transfers_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let result = find_transfer_by_tx_hash(&client, &tx_hash).await.unwrap();

        assert!(result.is_none());

        transfers_mock.assert();
    }

    #[tokio::test]
    async fn test_find_transfer_by_tx_hash_api_error() {
        let server = MockServer::start();
        let tx_hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");

        let transfers_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(500).body("Internal Server Error");
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let error = find_transfer_by_tx_hash(&client, &tx_hash)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            AlpacaWalletError::ApiError { status, .. } if status == 500
        ));

        transfers_mock.assert();
    }
}
