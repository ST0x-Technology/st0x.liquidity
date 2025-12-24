use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::transfer::{Network, TokenSymbol};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub(crate) enum WhitelistStatus {
    Pending,
    Approved,
    Rejected,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct WhitelistEntry {
    pub(crate) id: String,
    pub(crate) address: Address,
    pub(crate) asset: TokenSymbol,
    pub(crate) chain: Network,
    pub(crate) status: WhitelistStatus,
    pub(crate) created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use httpmock::prelude::*;
    use serde_json::json;

    use super::super::client::{AlpacaWalletClient, create_account_mock};
    use super::super::transfer::{Network, TokenSymbol};
    use super::*;

    #[tokio::test]
    async fn test_get_whitelisted_addresses() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let test_address1 = "0x1234567890abcdef1234567890abcdef12345678";
        let test_address2 = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd";

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{expected_account_id}/wallets/whitelists"
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

        let client = AlpacaWalletClient::new(
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

        let address = address!("0x1234567890abcdef1234567890abcdef12345678");

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{expected_account_id}/wallets/whitelists"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": "whitelist-123",
                        "address": address,
                        "asset": "USDC",
                        "chain": "Ethereum",
                        "status": "APPROVED",
                        "created_at": "2024-01-01T00:00:00Z"
                    }
                ]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();
        let asset = TokenSymbol::new("USDC");
        let network = Network::new("Ethereum");

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

        let address = address!("0x1234567890abcdef1234567890abcdef12345678");

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{expected_account_id}/wallets/whitelists"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": "whitelist-123",
                        "address": address,
                        "asset": "USDC",
                        "chain": "Ethereum",
                        "status": "PENDING",
                        "created_at": "2024-01-01T00:00:00Z"
                    }
                ]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let asset = TokenSymbol::new("USDC");
        let network = Network::new("Ethereum");

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

        let address = address!("0x1234567890abcdef1234567890abcdef12345678");

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{expected_account_id}/wallets/whitelists"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "id": "whitelist-123",
                        "address": address,
                        "asset": "USDC",
                        "chain": "Ethereum",
                        "status": "REJECTED",
                        "created_at": "2024-01-01T00:00:00Z"
                    }
                ]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let asset = TokenSymbol::new("USDC");
        let network = Network::new("Ethereum");

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

        let other_address = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd";

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{expected_account_id}/wallets/whitelists"
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

        let client = AlpacaWalletClient::new(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let address = address!("0x1234567890abcdef1234567890abcdef12345678");
        let asset = TokenSymbol::new("USDC");
        let network = Network::new("Ethereum");

        let result = client
            .is_address_whitelisted_and_approved(&address, &asset, &network)
            .await
            .unwrap();

        assert!(!result);

        account_mock.assert();
        whitelist_mock.assert();
    }
}
