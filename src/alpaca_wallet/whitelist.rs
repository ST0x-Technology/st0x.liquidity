//! Alpaca Broker API trusted address whitelist management
//! for crypto withdrawals.
//!
//! Addresses must be whitelisted and approved (24h waiting
//! period) before withdrawals can target them.

use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::config::TravelRuleConfig;

use super::transfer::{Network, TokenSymbol};

/// Travel Rule beneficiary info for Alpaca whitelist creation requests.
///
/// Alpaca requires this on all `POST /whitelists` calls effective 2026-03-27.
/// We only support self-hosted wallets, so `is_self_hosted` is always `true`.
///
/// Field names use `serde(rename)` to match the Alpaca API's
/// `beneficiary_*` JSON keys while avoiding the `struct_field_names` lint.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct TravelRuleInfo {
    #[serde(rename = "beneficiary_is_self_hosted")]
    is_self_hosted: bool,

    #[serde(rename = "beneficiary_entity_name")]
    entity_name: String,
}

impl TravelRuleInfo {
    pub(crate) fn from_config(config: &TravelRuleConfig) -> Self {
        Self {
            is_self_hosted: true,
            entity_name: config.beneficiary_entity_name.clone(),
        }
    }
}

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
    use uuid::uuid;

    use st0x_execution::AlpacaAccountId;

    use super::super::client::AlpacaWalletClient;
    use super::super::transfer::{Network, TokenSymbol};
    use super::*;

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    #[tokio::test]
    async fn test_get_whitelisted_addresses() {
        let server = MockServer::start();
        let test_address1 = "0x1234567890abcdef1234567890abcdef12345678";
        let test_address2 = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd";

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/whitelists"));
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
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let result = client.get_whitelisted_addresses().await.unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id, "whitelist-123");
        assert_eq!(result[0].status, WhitelistStatus::Approved);
        assert_eq!(result[1].id, "whitelist-456");
        assert_eq!(result[1].status, WhitelistStatus::Pending);

        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_is_address_whitelisted_and_approved_true() {
        let server = MockServer::start();
        let address = address!("0x1234567890abcdef1234567890abcdef12345678");

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/whitelists"));
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
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );
        let asset = TokenSymbol::new("USDC");
        let network = Network::new("Ethereum");

        let result = client
            .is_address_whitelisted_and_approved(&address, &asset, &network)
            .await
            .unwrap();

        assert!(result);

        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_is_address_whitelisted_and_approved_pending() {
        let server = MockServer::start();
        let address = address!("0x1234567890abcdef1234567890abcdef12345678");

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/whitelists"));
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
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let asset = TokenSymbol::new("USDC");
        let network = Network::new("Ethereum");

        let result = client
            .is_address_whitelisted_and_approved(&address, &asset, &network)
            .await
            .unwrap();

        assert!(!result);

        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn test_is_address_whitelisted_and_approved_rejected() {
        let server = MockServer::start();
        let address = address!("0x1234567890abcdef1234567890abcdef12345678");

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/whitelists"));
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
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let asset = TokenSymbol::new("USDC");
        let network = Network::new("Ethereum");

        let result = client
            .is_address_whitelisted_and_approved(&address, &asset, &network)
            .await
            .unwrap();

        assert!(!result);

        whitelist_mock.assert();
    }

    #[tokio::test]
    async fn create_whitelist_entry_sends_travel_rule_info() {
        let server = MockServer::start();
        let target = address!("0x1234567890abcdef1234567890abcdef12345678");

        let travel_rule = TravelRuleInfo::from_config(&TravelRuleConfig {
            beneficiary_entity_name: "T0 TRADE (BVI) LTD".to_string(),
        });

        let checksummed = target.to_checksum(None);

        let create_mock = server.mock(|when, then| {
            when.method(POST)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/whitelists"))
                .json_body(json!({
                    "address": checksummed,
                    "asset": "USDC",
                    "travel_rule_info": {
                        "beneficiary_is_self_hosted": true,
                        "beneficiary_entity_name": "T0 TRADE (BVI) LTD"
                    }
                }));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "id": "whitelist-new",
                    "address": target.to_string(),
                    "asset": "USDC",
                    "chain": "Ethereum",
                    "status": "PENDING",
                    "created_at": "2024-01-01T00:00:00Z"
                }));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let asset = TokenSymbol::new("USDC");
        let network = Network::new("Ethereum");

        let entry = client
            .create_whitelist_entry(&target, &asset, &network, &travel_rule)
            .await
            .unwrap();

        assert_eq!(entry.id, "whitelist-new");
        assert_eq!(entry.status, WhitelistStatus::Pending);

        // httpmock asserts the full JSON body matched, confirming
        // travel_rule_info was serialized correctly.
        create_mock.assert();
    }

    #[tokio::test]
    async fn test_is_address_whitelisted_and_approved_not_found() {
        let server = MockServer::start();
        let other_address = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd";

        let whitelist_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/whitelists"));
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
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let address = address!("0x1234567890abcdef1234567890abcdef12345678");
        let asset = TokenSymbol::new("USDC");
        let network = Network::new("Ethereum");

        let result = client
            .is_address_whitelisted_and_approved(&address, &asset, &network)
            .await
            .unwrap();

        assert!(!result);

        whitelist_mock.assert();
    }

    /// Helper that reads Alpaca sandbox credentials from environment variables.
    /// Returns `None` if any required var is missing, allowing `#[ignore]`
    /// tests to skip gracefully.
    fn sandbox_client() -> Option<AlpacaWalletClient> {
        let api_key = std::env::var("ALPACA_BROKER_API_KEY").ok()?;
        let api_secret = std::env::var("ALPACA_BROKER_API_SECRET").ok()?;
        let account_id_str = std::env::var("ALPACA_BROKER_ACCOUNT_ID").ok()?;
        let account_id =
            AlpacaAccountId::new(account_id_str.parse::<uuid::Uuid>().expect("valid UUID"));

        Some(AlpacaWalletClient::new(
            "https://broker-api.sandbox.alpaca.markets".to_string(),
            account_id,
            api_key,
            api_secret,
        ))
    }

    /// Exercises the Alpaca sandbox wallet API: list, create, patch, delete.
    ///
    /// Combined into a single test to control execution order — parallel
    /// sandbox calls cause 409 "account is being created" errors.
    #[tokio::test]
    #[ignore = "hits real Alpaca sandbox — requires ALPACA_BROKER_* env vars"]
    async fn sandbox_whitelist_operations() {
        let Some(client) = sandbox_client() else {
            eprintln!("skipping: missing ALPACA_BROKER_* env vars");
            return;
        };

        let travel_rule = TravelRuleInfo::from_config(&TravelRuleConfig {
            beneficiary_entity_name: "T0 TRADE (BVI) LTD".to_string(),
        });

        // 1. List existing whitelist entries.
        let entries = client.get_whitelisted_addresses().await.unwrap();
        eprintln!("found {} whitelist entries", entries.len());

        for entry in &entries {
            eprintln!(
                "  {} | {} | {} | {:?}",
                entry.id,
                entry.address,
                entry.asset.as_ref(),
                entry.status
            );
        }

        // 2. Create a new whitelist entry with travel rule info.
        let address = Address::random();
        let asset = TokenSymbol::new("USDC");
        let network = Network::new("ethereum");

        eprintln!("creating whitelist entry for {address}");
        let created = client
            .create_whitelist_entry(&address, &asset, &network, &travel_rule)
            .await
            .unwrap();

        eprintln!("created: id={}, status={:?}", created.id, created.status);
        assert_eq!(created.address, address);

        // 3. Patch travel rule info on the entry we just created.
        eprintln!("patching travel rule on {}", created.id);
        client
            .patch_whitelist_travel_rule(&created.id, &travel_rule)
            .await
            .unwrap();
        eprintln!("patched successfully");

        // 4. Clean up: delete the entry.
        client.delete_whitelist_entry(&created.id).await.unwrap();
        eprintln!("cleaned up: deleted {}", created.id);
    }
}
