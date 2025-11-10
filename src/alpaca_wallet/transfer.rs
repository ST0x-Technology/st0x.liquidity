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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(super) struct Token(String);

impl From<String> for Token {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl AsRef<str> for Token {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub(super) struct TransferId(Uuid);

impl TransferId {
    #[cfg(test)]
    fn new(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl From<Uuid> for TransferId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub(super) enum TransferDirection {
    Incoming,
    Outgoing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub(super) enum TransferStatus {
    Pending,
    Processing,
    Complete,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(super) struct Transfer {
    pub(super) id: TransferId,
    #[serde(rename = "relationship")]
    pub(super) direction: TransferDirection,
    #[serde(deserialize_with = "deserialize_decimal_from_string")]
    pub(super) amount: Decimal,
    pub(super) asset: Token,
    #[serde(rename = "from_address")]
    pub(super) from: Option<Address>,
    #[serde(rename = "to_address")]
    pub(super) to: Address,
    pub(super) status: TransferStatus,
    #[serde(rename = "tx_hash")]
    pub(super) tx: Option<TxHash>,
    pub(super) created_at: DateTime<Utc>,
    #[serde(
        rename = "network_fee_amount",
        deserialize_with = "deserialize_optional_decimal_from_string"
    )]
    pub(super) network_fee: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(super) struct Network(String);

impl From<String> for Network {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl AsRef<str> for Network {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DepositAddress {
    pub(super) address: Address,
    pub(super) asset: Token,
    pub(super) network: Network,
}

#[derive(Deserialize)]
struct FundingWallet {
    address: Address,
    asset: Token,
    network: Network,
}

pub(super) async fn get_deposit_address(
    client: &AlpacaWalletClient,
    asset: &str,
    network: &str,
) -> Result<DepositAddress, AlpacaWalletError> {
    let path = format!(
        "/v1/crypto/funding_wallets?asset={}&network={}",
        asset, network
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
    transfer_id: &TransferId,
) -> Result<Transfer, AlpacaWalletError> {
    let path = format!(
        "/v1/accounts/{}/wallets/transfers?transfer_id={}",
        client.account_id(),
        transfer_id.0
    );

    let response = client.get(&path).await?;

    let mut transfers: Vec<Transfer> = response.json().await?;

    transfers
        .pop()
        .ok_or_else(|| AlpacaWalletError::TransferNotFound {
            transfer_id: transfer_id.0.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;
    use serde_json::json;

    fn create_account_mock<'a>(server: &'a MockServer, account_id: &str) -> httpmock::Mock<'a> {
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
        assert_eq!(deposit_address.network.as_ref(), "Ethereum");

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

        assert!(result.is_err());
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

        assert!(result.is_err());
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

        assert!(result.is_err());
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

        assert!(result.is_err());
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
                    "/v1/accounts/{}/wallets/transfers",
                    expected_account_id
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

        assert_eq!(transfer.id, TransferId::new(transfer_id));
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
                "/v1/accounts/{}/wallets/transfers",
                expected_account_id
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

        assert!(result.is_err());
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
                "/v1/accounts/{}/wallets/transfers",
                expected_account_id
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

        assert!(result.is_err());
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
                "/v1/accounts/{}/wallets/transfers",
                expected_account_id
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

        assert!(result.is_err());
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
                    "/v1/accounts/{}/wallets/transfers",
                    expected_account_id
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

        let result = get_transfer_status(&client, &TransferId::from(transfer_id))
            .await
            .unwrap();

        assert_eq!(result.status, TransferStatus::Pending);
        assert_eq!(result.id, TransferId::from(transfer_id));

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
                    "/v1/accounts/{}/wallets/transfers",
                    expected_account_id
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

        let result = get_transfer_status(&client, &TransferId::from(transfer_id))
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
                    "/v1/accounts/{}/wallets/transfers",
                    expected_account_id
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

        let result = get_transfer_status(&client, &TransferId::from(transfer_id))
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
                    "/v1/accounts/{}/wallets/transfers",
                    expected_account_id
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

        let result = get_transfer_status(&client, &TransferId::from(transfer_id))
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
                    "/v1/accounts/{}/wallets/transfers",
                    expected_account_id
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

        let result = get_transfer_status(&client, &TransferId::from(transfer_id)).await;

        assert!(result.is_err());
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
                    "/v1/accounts/{}/wallets/transfers",
                    expected_account_id
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

        let result = get_transfer_status(&client, &TransferId::from(transfer_id)).await;

        assert!(result.is_err());
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
                    "/v1/accounts/{}/wallets/transfers",
                    expected_account_id
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

        let result = get_transfer_status(&client, &TransferId::from(transfer_id)).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AlpacaWalletError::Reqwest(_)));

        account_mock.assert();
        status_mock.assert();
    }
}
