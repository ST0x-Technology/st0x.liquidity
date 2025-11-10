use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use super::transfer::{Network, TokenSymbol};

pub(super) const APPROVAL_WAIT_TIME: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub(super) enum WhitelistStatus {
    Pending,
    Approved,
    Rejected,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct WhitelistEntry {
    pub(super) id: String,
    pub(super) address: Address,
    pub(super) asset: TokenSymbol,
    pub(super) chain: Network,
    pub(super) status: WhitelistStatus,
    pub(super) created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::super::client::{AlpacaWalletClient, AlpacaWalletError};
    use super::super::transfer::{Network, Token};
    use super::*;
    use alloy::primitives::Address;
    use httpmock::prelude::*;
    use serde_json::json;
    use std::str::FromStr;

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
    async fn test_whitelist_address_success() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let test_address = "0x1234567890abcdef1234567890abcdef12345678";

        let whitelist_mock = server.mock(|when, then| {
            when.method(POST)
                .path(format!(
                    "/v1/accounts/{}/wallets/whitelists",
                    expected_account_id
                ))
                .json_body(json!({
                    "address": test_address,
                    "asset": "USDC",
                    "chain": "Ethereum"
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "whitelist-123",
                    "address": test_address,
                    "asset": "USDC",
                    "chain": "Ethereum",
                    "status": "PENDING",
                    "created_at": "2024-01-01T00:00:00Z"
                }));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let address = Address(AlloyAddress::from_str(test_address).unwrap());
        let asset = Token("USDC".to_string());
        let network = Network("Ethereum".to_string());

        let result = client
            .whitelist_address(&address, &asset, &network)
            .await
            .unwrap();

        assert_eq!(result.id, "whitelist-123");
        assert_eq!(result.address, address);
        assert_eq!(result.asset, asset);
        assert_eq!(result.chain, network);
        assert_eq!(result.status, WhitelistStatus::Pending);

        account_mock.assert();
        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_get_whitelisted_addresses() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let test_address1 = "0x1234567890abcdef1234567890abcdef12345678";
        let test_address2 = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd";

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{}/wallets/whitelists",
                expected_account_id
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": "whitelist-123",
                        "address": test_address1,
                        "asset": "USDC",
                        "chain": "Ethereum",
                        "status": "APPROVED",
                        "created_at": "2024-01-01T00:00:00Z"
                    },
                    {
                        "id": "whitelist-456",
                        "address": test_address2,
                        "asset": "USDC",
                        "chain": "Polygon",
                        "status": "PENDING",
                        "created_at": "2024-01-02T00:00:00Z"
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

        let result = client.get_whitelisted_addresses().await.unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, "whitelist-123");
        assert_eq!(result[0].status, WhitelistStatus::Approved);
        assert_eq!(result[1].id, "whitelist-456");
        assert_eq!(result[1].status, WhitelistStatus::Pending);

        account_mock.assert();
        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_is_address_whitelisted_and_approved_true() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let test_address = "0x1234567890abcdef1234567890abcdef12345678";

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{}/wallets/whitelists",
                expected_account_id
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": "whitelist-123",
                        "address": test_address,
                        "asset": "USDC",
                        "chain": "Ethereum",
                        "status": "APPROVED",
                        "created_at": "2024-01-01T00:00:00Z"
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

        let address = Address(AlloyAddress::from_str(test_address).unwrap());
        let asset = Token("USDC".to_string());
        let network = Network("Ethereum".to_string());

        let result = client
            .is_address_whitelisted_and_approved(&address, &asset, &network)
            .await
            .unwrap();

        assert!(result);

        account_mock.assert();
        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_is_address_whitelisted_and_approved_pending() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let test_address = "0x1234567890abcdef1234567890abcdef12345678";

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{}/wallets/whitelists",
                expected_account_id
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": "whitelist-123",
                        "address": test_address,
                        "asset": "USDC",
                        "chain": "Ethereum",
                        "status": "PENDING",
                        "created_at": "2024-01-01T00:00:00Z"
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

        let address = Address(AlloyAddress::from_str(test_address).unwrap());
        let asset = Token("USDC".to_string());
        let network = Network("Ethereum".to_string());

        let result = client
            .is_address_whitelisted_and_approved(&address, &asset, &network)
            .await
            .unwrap();

        assert!(!result);

        account_mock.assert();
        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_is_address_whitelisted_and_approved_rejected() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let test_address = "0x1234567890abcdef1234567890abcdef12345678";

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{}/wallets/whitelists",
                expected_account_id
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": "whitelist-123",
                        "address": test_address,
                        "asset": "USDC",
                        "chain": "Ethereum",
                        "status": "REJECTED",
                        "created_at": "2024-01-01T00:00:00Z"
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

        let address = Address(AlloyAddress::from_str(test_address).unwrap());
        let asset = Token("USDC".to_string());
        let network = Network("Ethereum".to_string());

        let result = client
            .is_address_whitelisted_and_approved(&address, &asset, &network)
            .await
            .unwrap();

        assert!(!result);

        account_mock.assert();
        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_is_address_whitelisted_and_approved_not_found() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let test_address = "0x1234567890abcdef1234567890abcdef12345678";
        let other_address = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd";

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{}/wallets/whitelists",
                expected_account_id
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": "whitelist-123",
                        "address": other_address,
                        "asset": "USDC",
                        "chain": "Ethereum",
                        "status": "APPROVED",
                        "created_at": "2024-01-01T00:00:00Z"
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

        let address = Address(AlloyAddress::from_str(test_address).unwrap());
        let asset = Token("USDC".to_string());
        let network = Network("Ethereum".to_string());

        let result = client
            .is_address_whitelisted_and_approved(&address, &asset, &network)
            .await
            .unwrap();

        assert!(!result);

        account_mock.assert();
        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_whitelist_address_api_error() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let test_address = "0x1234567890abcdef1234567890abcdef12345678";

        let whitelist_mock = server.mock(|when, then| {
            when.method(POST).path(format!(
                "/v1/accounts/{}/wallets/whitelists",
                expected_account_id
            ));
            then.status(400).json_body(json!({
                "message": "Invalid address format"
            }));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let address = Address(AlloyAddress::from_str(test_address).unwrap());
        let asset = Token("USDC".to_string());
        let network = Network("Ethereum".to_string());

        let result = client.whitelist_address(&address, &asset, &network).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::ApiError { .. }
        ));

        account_mock.assert();
        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_whitelist_duplicate_address() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let test_address = "0x1234567890abcdef1234567890abcdef12345678";

        let whitelist_mock = server.mock(|when, then| {
            when.method(POST).path(format!(
                "/v1/accounts/{}/wallets/whitelists",
                expected_account_id
            ));
            then.status(409).json_body(json!({
                "message": "Address already whitelisted"
            }));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let address = Address(AlloyAddress::from_str(test_address).unwrap());
        let asset = Token("USDC".to_string());
        let network = Network("Ethereum".to_string());

        let result = client.whitelist_address(&address, &asset, &network).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::ApiError { .. }
        ));

        account_mock.assert();
        whitelist_mock.assert();
    }
}
