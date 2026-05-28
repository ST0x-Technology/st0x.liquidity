//! Alpaca Broker API account asset listing and deposit address lookups.
//!
//! This module covers wallet asset inspection without touching transfer or
//! whitelist behavior.

use alloy::primitives::Address;
use rain_math_float::Float;
use serde::Deserialize;

use st0x_finance::Usdc;

use super::client::{AlpacaWalletClient, AlpacaWalletError};
use super::transfer::{Network, TokenSymbol};

impl AlpacaWalletClient {
    /// Gets or creates a wallet deposit address for a specific asset and network.
    ///
    /// Uses `GET /v1/accounts/{account_id}/wallets?asset=...&network=...` per
    /// the Alpaca Broker API documentation.
    pub(super) async fn get_wallet_address(
        &self,
        asset: &TokenSymbol,
        network: &Network,
    ) -> Result<Address, AlpacaWalletError> {
        #[derive(Deserialize)]
        struct WalletAddressResponse {
            #[allow(dead_code)]
            asset_id: String,
            address: Address,
            #[allow(dead_code)]
            created_at: String,
        }

        let path = format!(
            "/v1/accounts/{}/wallets?asset={}&network={}",
            self.account_id(),
            asset.as_ref(),
            network.as_ref()
        );

        let body = self.get(&path).await?;

        Ok(serde_json::from_str::<WalletAddressResponse>(&body)?.address)
    }

    /// Gets the Alpaca account asset balance for a token and network.
    pub(super) async fn get_asset_balance(
        &self,
        asset: &TokenSymbol,
        network: &Network,
    ) -> Result<Usdc, AlpacaWalletError> {
        #[derive(Deserialize)]
        struct AssetBalanceResponse {
            #[serde(deserialize_with = "deserialize_float")]
            balance: Float,
        }

        fn deserialize_float<'de, D>(deserializer: D) -> Result<Float, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let raw = String::deserialize(deserializer)?;
            Float::parse(raw).map_err(serde::de::Error::custom)
        }

        let path = format!(
            "/v1/accounts/{}/wallets?asset={}&network={}",
            self.account_id(),
            asset.as_ref(),
            network.as_ref()
        );

        let body = self.get(&path).await?;

        Ok(Usdc::new(
            serde_json::from_str::<AssetBalanceResponse>(&body)?.balance,
        ))
    }
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use serde_json::json;
    use uuid::uuid;

    use st0x_execution::AlpacaAccountId;

    use super::*;

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    #[tokio::test]
    async fn get_asset_balance_reads_account_token_balance() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets"))
                .query_param("asset", "USDC")
                .query_param("network", "ethereum");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "asset_id": "5d0de74f-827b-41a7-9f74-9c07c08fe55f",
                    "asset": "USDC",
                    "address": "0x42a76C83014e886e639768D84EAF3573b1876844",
                    "balance": "125.75",
                    "created_at": "2025-08-07T08:52:40.656166Z"
                }));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let balance = client
            .get_asset_balance(&TokenSymbol::new("USDC"), &Network::new("ethereum"))
            .await
            .unwrap();

        assert_eq!(balance, Usdc::new(st0x_float_macro::float!(125.75)));
        mock.assert();
    }

    #[tokio::test]
    async fn test_get_wallet_address_success() {
        let server = MockServer::start();
        let expected_address = "0x42a76C83014e886e639768D84EAF3573b1876844";

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets"))
                .query_param("asset", "USDC")
                .query_param("network", "ethereum");
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
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets"))
                .query_param("asset", "INVALID")
                .query_param("network", "ethereum");
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

        assert!(matches!(
            client
                .get_wallet_address(&TokenSymbol::new("INVALID"), &Network::new("ethereum"))
                .await
                .unwrap_err(),
            AlpacaWalletError::ApiError { status, .. } if status == reqwest::StatusCode::BAD_REQUEST
        ));
        mock.assert();
    }

    #[tokio::test]
    async fn test_get_wallet_address_empty_response() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets"))
                .query_param("asset", "USDC")
                .query_param("network", "ethereum");
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

        assert!(matches!(
            client
                .get_wallet_address(&TokenSymbol::new("USDC"), &Network::new("ethereum"))
                .await
                .unwrap_err(),
            AlpacaWalletError::ParseError(_)
        ));
        mock.assert();
    }
}
