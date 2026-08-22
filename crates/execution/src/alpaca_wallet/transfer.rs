//! Alpaca Broker API crypto transfer types and operations.
//!
//! Provides `request_withdrawal` and `get_transfer_status`
//! for initiating and tracking crypto transfers. Transfers
//! progress through Queued -> Pending -> Complete/Failed.

use alloy::primitives::{Address, TxHash};
use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use serde::{Deserialize, Deserializer, Serialize};
use tracing::warn;
use uuid::Uuid;

use rain_math_float::Float;
use st0x_finance::Usdc;

use super::client::{AlpacaWalletClient, AlpacaWalletError};
use crate::Positive;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenSymbol(pub String);

impl TokenSymbol {
    pub fn new(s: impl Into<String>) -> Self {
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
pub struct AlpacaTransferId(pub Uuid);

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
pub enum TransferDirection {
    Incoming,
    Outgoing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TransferStatus {
    Pending,
    Processing,
    Complete,
    Failed,
}

impl TransferStatus {
    /// Whether the transfer is still in flight (not yet complete or failed).
    pub fn is_pending(self) -> bool {
        use TransferStatus::*;

        match self {
            Pending | Processing => true,
            Complete | Failed => false,
        }
    }
}

/// Transfer response from Alpaca Crypto Wallets API.
#[derive(Debug, Clone, Deserialize)]
pub struct Transfer {
    pub id: AlpacaTransferId,
    #[serde(rename = "tx_hash", default)]
    pub tx: Option<TxHash>,
    pub direction: TransferDirection,
    #[serde(deserialize_with = "deserialize_float_from_string")]
    pub amount: Float,
    pub chain: String,
    pub asset: TokenSymbol,
    #[serde(rename = "from_address")]
    pub from: Address,
    #[serde(rename = "to_address")]
    pub to: Address,
    pub status: TransferStatus,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Network(String);

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
    pub fn new(s: impl Into<String>) -> Self {
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

fn deserialize_float_from_string<'de, D>(deserializer: D) -> Result<Float, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    Float::parse(raw).map_err(serde::de::Error::custom)
}

#[derive(Serialize)]
struct WithdrawalRequest<'a> {
    #[serde(serialize_with = "st0x_float_serde::serialize_float_as_string")]
    amount: Float,
    asset: &'a TokenSymbol,
    #[serde(serialize_with = "serialize_address_checksummed")]
    address: &'a Address,
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde serialize_with passes &&Address
fn serialize_address_checksummed<S>(address: &&Address, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    // None = standard EIP-55 checksum (no chain-specific EIP-1191 encoding).
    // Fine for now since this system only handles Ethereum mainnet.
    serializer.serialize_str(&address.to_checksum(None))
}

pub(super) async fn initiate_withdrawal(
    client: &AlpacaWalletClient,
    amount: Positive<Usdc>,
    asset: &TokenSymbol,
    address: &Address,
) -> Result<Transfer, AlpacaWalletError> {
    let request = WithdrawalRequest {
        amount: amount.inner().inner(),
        asset,
        address,
    };

    let path = format!("/v1/accounts/{}/wallets/transfers", client.account_id());

    let body = client.post(&path, &request).await?;
    let transfer: Transfer = serde_json::from_str(&body)?;

    Ok(transfer)
}

pub(super) async fn get_transfer_status(
    client: &AlpacaWalletClient,
    transfer_id: &AlpacaTransferId,
) -> Result<Transfer, AlpacaWalletError> {
    // Use the documented by-id endpoint for a single transfer:
    // https://docs.alpaca.markets/us/reference/getcryptofundingtransfer-1.
    // The list endpoint returns an account-wide array and has no documented
    // transfer_id filter, so status polling must not depend on client-side
    // filtering of a potentially capped transfer list.
    let path = format!(
        "/v1/accounts/{}/wallets/transfers/{}",
        client.account_id(),
        transfer_id
    );

    let body = client.get(&path).await.map_err(|error| match error {
        AlpacaWalletError::ApiError { status, .. } if status == StatusCode::NOT_FOUND => {
            AlpacaWalletError::TransferNotFound {
                transfer_id: *transfer_id,
            }
        }
        error => error,
    })?;

    Ok(serde_json::from_str(&body)?)
}

/// Lists all transfers for the account.
pub(super) async fn list_all_transfers(
    client: &AlpacaWalletClient,
) -> Result<Vec<Transfer>, AlpacaWalletError> {
    let path = format!("/v1/accounts/{}/wallets/transfers", client.account_id());

    let body = client.get(&path).await?;

    Ok(serde_json::from_str(&body)?)
}

/// Finds an incoming deposit by its transaction hash with a single query --
/// no polling deadline. Both predicates (hash AND incoming direction) apply
/// to the same search: an outgoing transfer sharing the hash must not
/// shadow the deposit behind it, and can never itself be the deposit a
/// recheck is verifying.
///
/// Scans the account-wide transfer list client-side because the list
/// endpoint has no tx-hash filter and a deposit detected by hash has no
/// Alpaca transfer id to feed the by-id endpoint. The list is potentially
/// capped (see [`get_transfer_status`]), so `Ok(None)` means "not in the
/// list", NOT proof the deposit does not exist: callers must treat it as
/// retryable, never as grounds for an irreversible decision.
///
/// Filters on a chain-neutral key before parsing EVM fields: the
/// account-wide list can carry transfers on other chains whose hashes and
/// addresses do not parse as EVM types, and one such row must not make an
/// Ethereum deposit impossible to recheck. A hash that does not parse as
/// an EVM tx hash can never match the EVM target, so foreign rows are
/// skipped, not errors; only the matched row is parsed as a full
/// [`Transfer`], where a parse failure IS an error.
pub(super) async fn find_deposit_by_tx_hash(
    client: &AlpacaWalletClient,
    tx_hash: &TxHash,
) -> Result<Option<Transfer>, AlpacaWalletError> {
    scan_transfer_list_by_tx_hash(client, tx_hash, Some(TransferDirection::Incoming)).await
}

/// Chain-neutral scan of the account-wide transfer list for a tx-hash match,
/// optionally constrained to one direction. Shared by both hash lookups so a
/// foreign-chain row is skipped in one place instead of failing whichever
/// caller deserializes the full list.
async fn scan_transfer_list_by_tx_hash(
    client: &AlpacaWalletClient,
    tx_hash: &TxHash,
    direction: Option<TransferDirection>,
) -> Result<Option<Transfer>, AlpacaWalletError> {
    let path = format!("/v1/accounts/{}/wallets/transfers", client.account_id());

    let body = client.get(&path).await?;
    let rows: Vec<serde_json::Value> = serde_json::from_str(&body)?;

    rows.into_iter()
        .find(|row| {
            // Log the parse error and the row's transfer id only, never the
            // full row: it is an external payload of arbitrary shape carrying
            // account addresses and amounts that do not belong in logs.
            let key = match TransferListKey::deserialize(row) {
                Ok(key) => key,
                Err(error) => {
                    warn!(
                        %error,
                        row_id = ?row.get("id"),
                        "skipping transfer-list row without a parsable direction"
                    );
                    return false;
                }
            };

            direction.is_none_or(|wanted| key.direction == wanted)
                && key
                    .tx
                    .is_some_and(|raw| raw.parse::<TxHash>().ok().as_ref() == Some(tx_hash))
        })
        .map(serde_json::from_value)
        .transpose()
        .map_err(Into::into)
}

/// Chain-neutral subset of a transfer-list row: only the fields the
/// tx-hash search filters on, with the hash left as a raw string so a
/// non-EVM row deserializes instead of failing the whole scan.
#[derive(Deserialize)]
struct TransferListKey {
    #[serde(rename = "tx_hash", default)]
    tx: Option<String>,
    direction: TransferDirection,
}

/// Finds a transfer by its transaction hash, in either direction.
///
/// Scans the account-wide list through the shared chain-neutral scan, so a
/// foreign-chain row cannot fail the lookup. Returns the first match or
/// None if no transfer with that tx hash exists.
pub(super) async fn find_transfer_by_tx_hash(
    client: &AlpacaWalletClient,
    tx_hash: &TxHash,
) -> Result<Option<Transfer>, AlpacaWalletError> {
    scan_transfer_list_by_tx_hash(client, tx_hash, None).await
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, fixed_bytes};
    use httpmock::prelude::*;
    use serde_json::json;
    use std::str::FromStr;
    use uuid::uuid;

    use crate::AlpacaAccountId;
    use rain_math_float::Float;

    use super::*;
    use st0x_float_macro::float;

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    #[test]
    fn withdrawal_request_serializes_address_as_checksummed() {
        let address = address!("0xbd41F40D91eE4E816Ada1Aa842e94aEb6B6385a6");
        let asset = TokenSymbol::new("USDC");

        let request = WithdrawalRequest {
            amount: float!(100),
            asset: &asset,
            address: &address,
        };

        let json = serde_json::to_value(&request).unwrap();

        assert_eq!(
            json["address"].as_str().unwrap(),
            "0xbd41F40D91eE4E816Ada1Aa842e94aEb6B6385a6",
            "Alpaca requires EIP-55 checksummed addresses for whitelist matching"
        );
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_successful() {
        let server = MockServer::start();
        let transfer_id = Uuid::new_v4();
        let to_address = address!("0x1234567890abcdef1234567890abcdef12345678");

        let withdrawal_mock = server.mock(|when, then| {
            when.method(POST)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"))
                .json_body(json!({
                    "amount": "100.5",
                    "asset": "USDC",
                    "address": to_address.to_checksum(None)
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

        let amount = Positive::new(Usdc::new(float!(100.5))).unwrap();
        let asset = TokenSymbol::new("USDC");

        let transfer = initiate_withdrawal(&client, amount, &asset, &to_address)
            .await
            .unwrap();

        let expected_address =
            Address::from_str("0x1234567890abcdef1234567890abcdef12345678").unwrap();

        assert_eq!(transfer.id, AlpacaTransferId::new(transfer_id));
        assert_eq!(transfer.direction, TransferDirection::Outgoing);
        assert!(transfer.amount.eq(float!(100.5)).unwrap());
        assert_eq!(transfer.asset.as_ref(), "USDC");
        assert_eq!(transfer.to, expected_address);
        assert_eq!(transfer.status, TransferStatus::Pending);

        withdrawal_mock.assert();
    }

    #[test]
    fn test_initiate_withdrawal_zero_amount() {
        let zero = Float::zero().unwrap();
        let error = Positive::new(Usdc::new(zero)).unwrap_err();
        assert_eq!(error.value, Usdc::new(Float::zero().unwrap()));
    }

    #[test]
    fn test_initiate_withdrawal_negative_amount() {
        let error = Positive::new(Usdc::new(float!(-100))).unwrap_err();
        assert_eq!(error.value, Usdc::new(float!(-100)));
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

        let amount = Positive::new(Usdc::new(float!(100))).unwrap();
        let asset = TokenSymbol::new("INVALID");
        let addr = address!("0x1234567890abcdef1234567890abcdef12345678");

        let error = initiate_withdrawal(&client, amount, &asset, &addr)
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

        let amount = Positive::new(Usdc::new(float!(100))).unwrap();
        let asset = TokenSymbol::new("USDC");
        let addr = address!("0x0000000000000000000000000000000000000000");

        let error = initiate_withdrawal(&client, amount, &asset, &addr)
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

        let amount = Positive::new(Usdc::new(float!(100))).unwrap();
        let asset = TokenSymbol::new("USDC");
        let addr = address!("0x1234567890abcdef1234567890abcdef12345678");

        let error = initiate_withdrawal(&client, amount, &asset, &addr)
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
        let status_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers/{transfer_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
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
                }));
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
            when.method(GET).path(format!(
                "/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers/{transfer_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
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
                }));
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
            when.method(GET).path(format!(
                "/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers/{transfer_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
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
                }));
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
            when.method(GET).path(format!(
                "/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers/{transfer_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
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
                }));
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
        let status_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers/{transfer_id}"
            ));
            then.status(404)
                .header("content-type", "application/json")
                .json_body(json!({ "message": "transfer not found" }));
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
            when.method(GET).path(format!(
                "/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers/{transfer_id}"
            ));
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
            when.method(GET).path(format!(
                "/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers/{transfer_id}"
            ));
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

        assert!(matches!(error, AlpacaWalletError::ParseError(_)));

        status_mock.assert();
    }

    /// Regression test: status polling must use Alpaca's by-id endpoint rather
    /// than the list endpoint, so an account-wide response cap cannot hide an
    /// older in-flight withdrawal.
    #[tokio::test]
    async fn test_get_transfer_status_uses_by_id_endpoint() {
        let server = MockServer::start();
        let transfer_id = Uuid::new_v4();

        let status_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers/{transfer_id}"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": transfer_id,
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
                }));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let transfer = get_transfer_status(&client, &AlpacaTransferId::from(transfer_id))
            .await
            .unwrap();

        assert_eq!(
            transfer.id,
            AlpacaTransferId::from(transfer_id),
            "status polling must request the transfer by ID"
        );
        assert!(
            transfer.amount.eq(float!(100)).unwrap(),
            "status polling must parse the by-id transfer payload"
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

    #[test]
    fn pending_and_processing_are_pending_statuses() {
        assert!(TransferStatus::Pending.is_pending());
        assert!(TransferStatus::Processing.is_pending());
    }

    #[test]
    fn complete_and_failed_are_not_pending() {
        assert!(!TransferStatus::Complete.is_pending());
        assert!(!TransferStatus::Failed.is_pending());
    }

    #[test]
    fn malformed_decimal_string_fails_deserialization() {
        let malformed = json!({
            "id": Uuid::new_v4(),
            "direction": "OUTGOING",
            "amount": "not_a_number",
            "usd_value": "100.0",
            "chain": "BASE",
            "asset": "USDC",
            "from_address": Address::ZERO,
            "to_address": Address::ZERO,
            "status": "COMPLETE",
            "created_at": "2025-01-01T00:00:00Z",
            "network_fee": "0.001",
            "fees": "0.0"
        });

        let error = serde_json::from_value::<Transfer>(malformed).unwrap_err();
        assert!(
            error.to_string().contains("Float"),
            "error should indicate Float parse failure: {error}"
        );
    }

    fn transfer_list_entry(tx_hash: TxHash, direction: &str, status: &str) -> serde_json::Value {
        json!({
            "id": Uuid::new_v4(),
            "direction": direction,
            "amount": "500",
            "usd_value": "500",
            "chain": "ethereum",
            "asset": "USDC",
            "from_address": "0x9999999999999999999999999999999999999999",
            "to_address": "0x1234567890abcdef1234567890abcdef12345678",
            "status": status,
            "tx_hash": tx_hash,
            "created_at": "2024-01-01T00:00:00Z",
            "network_fee": "0",
            "fees": "0"
        })
    }

    fn test_wallet_client(server: &MockServer) -> AlpacaWalletClient {
        AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
    }

    const DEPOSIT_TX: TxHash =
        fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");

    /// The single-shot lookup finds a settled incoming deposit without any
    /// polling deadline -- the `transfer recheck` path for a deposit that
    /// completed after the poller gave up.
    #[tokio::test]
    async fn find_deposit_returns_completed_incoming_transfer() {
        let server = MockServer::start();
        let transfers_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([transfer_list_entry(
                    DEPOSIT_TX, "INCOMING", "COMPLETE"
                )]));
        });

        let transfer = find_deposit_by_tx_hash(&test_wallet_client(&server), &DEPOSIT_TX)
            .await
            .unwrap()
            .expect("a settled incoming deposit must be found");

        transfers_mock.assert();
        assert_eq!(transfer.status, TransferStatus::Complete);
        assert_eq!(transfer.tx, Some(DEPOSIT_TX));
        assert_eq!(transfer.direction, TransferDirection::Incoming);
    }

    /// A still-processing deposit is returned with its live status so the
    /// caller can refuse rather than fabricate a success.
    #[tokio::test]
    async fn find_deposit_preserves_non_terminal_status() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([transfer_list_entry(
                    DEPOSIT_TX,
                    "INCOMING",
                    "PROCESSING"
                )]));
        });

        let transfer = find_deposit_by_tx_hash(&test_wallet_client(&server), &DEPOSIT_TX)
            .await
            .unwrap()
            .expect("a detected deposit must be found regardless of status");

        assert_eq!(transfer.status, TransferStatus::Processing);
    }

    /// An outgoing duplicate listed BEFORE the incoming deposit must not
    /// shadow it: both predicates apply to the same search, so the deposit
    /// behind the duplicate is still found.
    #[tokio::test]
    async fn find_deposit_finds_incoming_behind_outgoing_duplicate() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    transfer_list_entry(DEPOSIT_TX, "OUTGOING", "COMPLETE"),
                    transfer_list_entry(DEPOSIT_TX, "INCOMING", "COMPLETE"),
                ]));
        });

        let transfer = find_deposit_by_tx_hash(&test_wallet_client(&server), &DEPOSIT_TX)
            .await
            .unwrap()
            .expect("the incoming deposit behind the outgoing duplicate must be found");

        assert_eq!(transfer.direction, TransferDirection::Incoming);
        assert_eq!(transfer.status, TransferStatus::Complete);
    }

    /// An outgoing transfer with the same hash can never be the deposit a
    /// recheck verifies; it must be filtered out, not returned.
    #[tokio::test]
    async fn find_deposit_filters_outgoing_transfer_with_matching_hash() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([transfer_list_entry(
                    DEPOSIT_TX, "OUTGOING", "COMPLETE"
                )]));
        });

        let found = find_deposit_by_tx_hash(&test_wallet_client(&server), &DEPOSIT_TX)
            .await
            .unwrap();

        assert_eq!(found.map(|transfer| transfer.id), None);
    }

    /// A transfer on another chain -- whose hash and addresses do not parse
    /// as EVM types -- must not make the Ethereum deposit behind it
    /// impossible to find: the search keys on chain-neutral fields first and
    /// only parses the matched row as an EVM transfer.
    #[tokio::test]
    async fn find_deposit_skips_non_evm_rows_in_mixed_chain_list() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": Uuid::new_v4(),
                        "direction": "INCOMING",
                        "amount": "2.5",
                        "usd_value": "500",
                        "chain": "solana",
                        "asset": "SOL",
                        "from_address": "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin",
                        "to_address": "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d",
                        "status": "COMPLETE",
                        "tx_hash": "5wHu1qwD4kKKyN1EEPBLRZ8hUvmCwF9zPSNdPCVBLcNq\
                                    QwR8DXCzB1FLZniqW6cBGXbmMDvBhSf5aG1qNW7Wj2Vt",
                        "created_at": "2024-01-01T00:00:00Z",
                        "network_fee": "0",
                        "fees": "0"
                    },
                    transfer_list_entry(DEPOSIT_TX, "INCOMING", "COMPLETE"),
                ]));
        });

        let transfer = find_deposit_by_tx_hash(&test_wallet_client(&server), &DEPOSIT_TX)
            .await
            .unwrap()
            .expect("the EVM deposit behind the non-EVM row must be found");

        assert_eq!(transfer.tx, Some(DEPOSIT_TX));
        assert_eq!(transfer.direction, TransferDirection::Incoming);
        assert_eq!(transfer.status, TransferStatus::Complete);
    }

    /// A row without a parsable direction (here: the field is absent) must be
    /// skipped with a warning, not hide the matching deposit listed after it.
    #[tokio::test]
    async fn find_deposit_skips_row_without_parsable_direction() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": Uuid::new_v4(),
                        "amount": "500",
                        "usd_value": "500",
                        "chain": "ethereum",
                        "asset": "USDC",
                        "status": "COMPLETE",
                        "tx_hash": DEPOSIT_TX,
                        "created_at": "2024-01-01T00:00:00Z"
                    },
                    transfer_list_entry(DEPOSIT_TX, "INCOMING", "COMPLETE"),
                ]));
        });

        let transfer = find_deposit_by_tx_hash(&test_wallet_client(&server), &DEPOSIT_TX)
            .await
            .unwrap()
            .expect("the deposit behind the directionless row must be found");

        assert_eq!(transfer.tx, Some(DEPOSIT_TX));
        assert_eq!(transfer.direction, TransferDirection::Incoming);
    }

    /// The direction-agnostic hash lookup shares the chain-neutral scan, so a
    /// non-EVM row in the account-wide list must not fail the whole lookup.
    #[tokio::test]
    async fn find_transfer_skips_non_evm_rows_in_mixed_chain_list() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": Uuid::new_v4(),
                        "direction": "OUTGOING",
                        "amount": "2.5",
                        "usd_value": "500",
                        "chain": "solana",
                        "asset": "SOL",
                        "from_address": "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin",
                        "to_address": "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d",
                        "status": "COMPLETE",
                        "tx_hash": "5wHu1qwD4kKKyN1EEPBLRZ8hUvmCwF9zPSNdPCVBLcNq\
                                    QwR8DXCzB1FLZniqW6cBGXbmMDvBhSf5aG1qNW7Wj2Vt",
                        "created_at": "2024-01-01T00:00:00Z",
                        "network_fee": "0",
                        "fees": "0"
                    },
                    transfer_list_entry(DEPOSIT_TX, "OUTGOING", "COMPLETE"),
                ]));
        });

        let transfer = find_transfer_by_tx_hash(&test_wallet_client(&server), &DEPOSIT_TX)
            .await
            .unwrap()
            .expect("the EVM transfer behind the non-EVM row must be found");

        assert_eq!(transfer.tx, Some(DEPOSIT_TX));
        assert_eq!(transfer.direction, TransferDirection::Outgoing);
    }

    #[tokio::test]
    async fn find_deposit_returns_none_when_undetected() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let found = find_deposit_by_tx_hash(&test_wallet_client(&server), &DEPOSIT_TX)
            .await
            .unwrap();

        assert_eq!(found.map(|transfer| transfer.id), None);
    }
}
