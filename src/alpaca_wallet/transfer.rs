use alloy::primitives::{Address, TxHash};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use super::client::{AlpacaWalletClient, AlpacaWalletError};

fn deserialize_decimal_from_string<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    s.parse::<Decimal>().map_err(serde::de::Error::custom)
}

fn deserialize_optional_decimal_from_string<'de, D>(
    deserializer: D,
) -> Result<Option<Decimal>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| s.parse::<Decimal>().map_err(serde::de::Error::custom))
        .transpose()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TokenSymbol(pub(super) String);

// TODO(#137): Remove dead_code allow when rebalancing orchestration uses this type
#[allow(dead_code)]
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

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct Transfer {
    pub(crate) id: AlpacaTransferId,
    #[serde(rename = "relationship")]
    pub(crate) direction: TransferDirection,
    #[serde(deserialize_with = "deserialize_decimal_from_string")]
    pub(crate) amount: Decimal,
    pub(crate) asset: TokenSymbol,
    #[serde(rename = "from_address")]
    pub(crate) from: Option<Address>,
    #[serde(rename = "to_address")]
    pub(crate) to: Address,
    pub(crate) status: TransferStatus,
    #[serde(rename = "tx_hash")]
    pub(crate) tx: Option<TxHash>,
    pub(crate) created_at: DateTime<Utc>,
    #[serde(
        rename = "network_fee_amount",
        deserialize_with = "deserialize_optional_decimal_from_string"
    )]
    pub(crate) network_fee: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Network(pub(super) String);

impl<'de> serde::Deserialize<'de> for Network {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Self::new(s))
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DepositAddress {
    pub(crate) address: Address,
    pub(crate) asset: TokenSymbol,
    pub(crate) network: Network,
}

#[derive(Deserialize)]
struct FundingWallet {
    address: Address,
    asset: TokenSymbol,
    network: Network,
}

pub(super) async fn get_deposit_address(
    client: &AlpacaWalletClient,
    asset: &str,
    network: &str,
) -> Result<DepositAddress, AlpacaWalletError> {
    let path = format!(
        "/v1/crypto/funding_wallets?asset={}&network={}",
        urlencoding::encode(asset),
        urlencoding::encode(network)
    );

    let response = client.get(&path).await?;

    let wallets: Vec<FundingWallet> = response.json().await?;

    let wallet = wallets
        .first()
        .ok_or_else(|| AlpacaWalletError::NoWalletFound {
            asset: asset.to_string(),
            network: network.to_string(),
        })?;

    Ok(DepositAddress {
        address: wallet.address,
        asset: wallet.asset.clone(),
        network: wallet.network.clone(),
    })
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
    let path = format!(
        "/v1/accounts/{}/wallets/transfers?transfer_id={}",
        client.account_id(),
        transfer_id.0
    );

    let response = client.get(&path).await?;

    let transfers: Vec<Transfer> = response.json().await?;

    transfers
        .into_iter()
        .next()
        .ok_or_else(|| AlpacaWalletError::TransferNotFound {
            transfer_id: *transfer_id,
        })
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
        .find(|t| t.tx.as_ref() == Some(tx_hash)))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::fixed_bytes;
    use httpmock::prelude::*;
    use serde_json::json;
    use std::str::FromStr;

    use super::super::client::create_account_mock;
    use super::*;

    #[tokio::test]
    async fn test_get_deposit_address_successful() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let wallet_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/crypto/funding_wallets")
                .query_param("asset", "USDC")
                .query_param("network", "Ethereum");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "address": "0x1234567890abcdef1234567890abcdef12345678",
                        "asset": "USDC",
                        "network": "Ethereum"
                    }
                ]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let deposit_address = get_deposit_address(&client, "USDC", "Ethereum")
            .await
            .unwrap();

        let expected_address =
            Address::from_str("0x1234567890abcdef1234567890abcdef12345678").unwrap();

        assert_eq!(deposit_address.address, expected_address);
        assert_eq!(deposit_address.asset.as_ref(), "USDC");
        assert_eq!(deposit_address.network.as_ref(), "ethereum");

        account_mock.assert();
        wallet_mock.assert();
    }

    #[tokio::test]
    async fn test_get_deposit_address_invalid_asset() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let wallet_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/crypto/funding_wallets")
                .query_param("asset", "INVALID")
                .query_param("network", "Ethereum");
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({
                    "message": "Invalid asset"
                }));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = get_deposit_address(&client, "INVALID", "Ethereum").await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == 400
        ));

        account_mock.assert();
        wallet_mock.assert();
    }

    #[tokio::test]
    async fn test_get_deposit_address_invalid_network() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let wallet_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/crypto/funding_wallets")
                .query_param("asset", "USDC")
                .query_param("network", "InvalidNetwork");
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({
                    "message": "Invalid network"
                }));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = get_deposit_address(&client, "USDC", "InvalidNetwork").await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == 400
        ));

        account_mock.assert();
        wallet_mock.assert();
    }

    #[tokio::test]
    async fn test_get_deposit_address_api_error() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let wallet_mock = server.mock(|when, then| {
            when.method(GET).path("/v1/crypto/funding_wallets");
            then.status(500).body("Internal Server Error");
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = get_deposit_address(&client, "USDC", "Ethereum").await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == 500
        ));

        account_mock.assert();
        wallet_mock.assert();
    }

    #[tokio::test]
    async fn test_get_deposit_address_malformed_json() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let wallet_mock = server.mock(|when, then| {
            when.method(GET).path("/v1/crypto/funding_wallets");
            then.status(200)
                .header("content-type", "application/json")
                .body("not valid json");
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = get_deposit_address(&client, "USDC", "Ethereum").await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AlpacaWalletError::Reqwest(_)));

        account_mock.assert();
        wallet_mock.assert();
    }

    #[tokio::test]
    async fn test_get_deposit_address_empty_response() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let wallet_mock = server.mock(|when, then| {
            when.method(GET).path("/v1/crypto/funding_wallets");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = get_deposit_address(&client, "USDC", "Ethereum").await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::NoWalletFound { .. }
        ));

        account_mock.assert();
        wallet_mock.assert();
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_successful() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let transfer_id = Uuid::new_v4();
        let withdrawal_mock = server.mock(|when, then| {
            when.method(POST)
                .path(format!(
                    "/v1/accounts/{expected_account_id}/wallets/transfers"
                ))
                .json_body(json!({
                    "amount": "100.5",
                    "asset": "USDC",
                    "address": "0x1234567890abcdef1234567890abcdef12345678"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": transfer_id,
                    "relationship": "OUTGOING",
                    "amount": "100.5",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "PENDING",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": "0.5"
                }));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

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
        assert_eq!(transfer.network_fee, Some(Decimal::new(5, 1)));

        account_mock.assert();
        withdrawal_mock.assert();
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_zero_amount() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = initiate_withdrawal(
            &client,
            Decimal::ZERO,
            "USDC",
            "0x1234567890abcdef1234567890abcdef12345678",
        )
        .await;

        let err = result.unwrap_err();
        assert!(matches!(
            err,
            AlpacaWalletError::InvalidAmount { amount } if amount == Decimal::ZERO
        ));

        account_mock.assert();
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_negative_amount() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = initiate_withdrawal(
            &client,
            Decimal::new(-100, 0),
            "USDC",
            "0x1234567890abcdef1234567890abcdef12345678",
        )
        .await;

        let err = result.unwrap_err();
        assert!(matches!(
            err,
            AlpacaWalletError::InvalidAmount { amount } if amount == Decimal::new(-100, 0)
        ));

        account_mock.assert();
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_invalid_asset() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let withdrawal_mock = server.mock(|when, then| {
            when.method(POST).path(format!(
                "/v1/accounts/{expected_account_id}/wallets/transfers"
            ));
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({
                    "message": "Invalid asset"
                }));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = initiate_withdrawal(
            &client,
            Decimal::new(100, 0),
            "INVALID",
            "0x1234567890abcdef1234567890abcdef12345678",
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == 400
        ));

        account_mock.assert();
        withdrawal_mock.assert();
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_invalid_address() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let withdrawal_mock = server.mock(|when, then| {
            when.method(POST).path(format!(
                "/v1/accounts/{expected_account_id}/wallets/transfers"
            ));
            then.status(400)
                .header("content-type", "application/json")
                .json_body(json!({
                    "message": "Invalid address"
                }));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result =
            initiate_withdrawal(&client, Decimal::new(100, 0), "USDC", "invalid_address").await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == 400
        ));

        account_mock.assert();
        withdrawal_mock.assert();
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_api_error() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let withdrawal_mock = server.mock(|when, then| {
            when.method(POST).path(format!(
                "/v1/accounts/{expected_account_id}/wallets/transfers"
            ));
            then.status(500).body("Internal Server Error");
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = initiate_withdrawal(
            &client,
            Decimal::new(100, 0),
            "USDC",
            "0x1234567890abcdef1234567890abcdef12345678",
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == 500
        ));

        account_mock.assert();
        withdrawal_mock.assert();
    }

    #[tokio::test]
    async fn test_get_transfer_status_pending() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let transfer_id = Uuid::new_v4();
        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{expected_account_id}/wallets/transfers"
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": transfer_id,
                    "relationship": "OUTGOING",
                    "amount": "100.0",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "PENDING",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": "0.5"
                }]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = get_transfer_status(&client, &AlpacaTransferId::from(transfer_id))
            .await
            .unwrap();

        assert_eq!(result.status, TransferStatus::Pending);
        assert_eq!(result.id, AlpacaTransferId::from(transfer_id));

        account_mock.assert();
        status_mock.assert();
    }

    #[tokio::test]
    async fn test_get_transfer_status_processing() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let transfer_id = Uuid::new_v4();
        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{expected_account_id}/wallets/transfers"
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": transfer_id,
                    "relationship": "OUTGOING",
                    "amount": "100.0",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "PROCESSING",
                    "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": "0.5"
                }]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = get_transfer_status(&client, &AlpacaTransferId::from(transfer_id))
            .await
            .unwrap();

        assert_eq!(result.status, TransferStatus::Processing);
        assert!(result.tx.is_some());

        account_mock.assert();
        status_mock.assert();
    }

    #[tokio::test]
    async fn test_get_transfer_status_complete() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let transfer_id = Uuid::new_v4();
        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{expected_account_id}/wallets/transfers"
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": transfer_id,
                    "relationship": "OUTGOING",
                    "amount": "100.0",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "COMPLETE",
                    "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": "0.5"
                }]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = get_transfer_status(&client, &AlpacaTransferId::from(transfer_id))
            .await
            .unwrap();

        assert_eq!(result.status, TransferStatus::Complete);

        account_mock.assert();
        status_mock.assert();
    }

    #[tokio::test]
    async fn test_get_transfer_status_failed() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let transfer_id = Uuid::new_v4();
        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{expected_account_id}/wallets/transfers"
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": transfer_id,
                    "relationship": "OUTGOING",
                    "amount": "100.0",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "FAILED",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": null
                }]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = get_transfer_status(&client, &AlpacaTransferId::from(transfer_id))
            .await
            .unwrap();

        assert_eq!(result.status, TransferStatus::Failed);

        account_mock.assert();
        status_mock.assert();
    }

    #[tokio::test]
    async fn test_get_transfer_status_not_found() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let transfer_id = Uuid::new_v4();
        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{expected_account_id}/wallets/transfers"
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = get_transfer_status(&client, &AlpacaTransferId::from(transfer_id)).await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::TransferNotFound { .. }
        ));

        account_mock.assert();
        status_mock.assert();
    }

    #[tokio::test]
    async fn test_get_transfer_status_api_error() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let transfer_id = Uuid::new_v4();
        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{expected_account_id}/wallets/transfers"
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(500).body("Internal Server Error");
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = get_transfer_status(&client, &AlpacaTransferId::from(transfer_id)).await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == 500
        ));

        account_mock.assert();
        status_mock.assert();
    }

    #[tokio::test]
    async fn test_get_transfer_status_malformed_json() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let transfer_id = Uuid::new_v4();
        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{expected_account_id}/wallets/transfers"
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .body("not valid json");
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = get_transfer_status(&client, &AlpacaTransferId::from(transfer_id)).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AlpacaWalletError::Reqwest(_)));

        account_mock.assert();
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
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let tx_hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");
        let transfer_id = Uuid::new_v4();

        let transfers_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{expected_account_id}/wallets/transfers"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": Uuid::new_v4(),
                        "relationship": "OUTGOING",
                        "amount": "100",
                        "asset": "USDC",
                        "from_address": null,
                        "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                        "status": "COMPLETE",
                        "tx_hash": null,
                        "created_at": "2024-01-01T00:00:00Z",
                        "network_fee_amount": null
                    },
                    {
                        "id": transfer_id,
                        "relationship": "INCOMING",
                        "amount": "500",
                        "asset": "USDC",
                        "from_address": "0x9999999999999999999999999999999999999999",
                        "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                        "status": "COMPLETE",
                        "tx_hash": tx_hash,
                        "created_at": "2024-01-02T00:00:00Z",
                        "network_fee_amount": "0.5"
                    }
                ]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let transfer = find_transfer_by_tx_hash(&client, &tx_hash)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(transfer.id, AlpacaTransferId::from(transfer_id));
        assert_eq!(transfer.tx, Some(tx_hash));
        assert_eq!(transfer.status, TransferStatus::Complete);

        account_mock.assert();
        transfers_mock.assert();
    }

    #[tokio::test]
    async fn test_find_transfer_by_tx_hash_not_found() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let tx_hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");

        let transfers_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{expected_account_id}/wallets/transfers"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": Uuid::new_v4(),
                        "relationship": "OUTGOING",
                        "amount": "100",
                        "asset": "USDC",
                        "from_address": null,
                        "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                        "status": "COMPLETE",
                        "tx_hash": null,
                        "created_at": "2024-01-01T00:00:00Z",
                        "network_fee_amount": null
                    }
                ]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = find_transfer_by_tx_hash(&client, &tx_hash).await.unwrap();

        assert!(result.is_none());

        account_mock.assert();
        transfers_mock.assert();
    }

    #[tokio::test]
    async fn test_find_transfer_by_tx_hash_empty_list() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let tx_hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");

        let transfers_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{expected_account_id}/wallets/transfers"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = find_transfer_by_tx_hash(&client, &tx_hash).await.unwrap();

        assert!(result.is_none());

        account_mock.assert();
        transfers_mock.assert();
    }

    #[tokio::test]
    async fn test_find_transfer_by_tx_hash_api_error() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let tx_hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");

        let transfers_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{expected_account_id}/wallets/transfers"
            ));
            then.status(500).body("Internal Server Error");
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let result = find_transfer_by_tx_hash(&client, &tx_hash).await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == 500
        ));

        account_mock.assert();
        transfers_mock.assert();
    }
}
