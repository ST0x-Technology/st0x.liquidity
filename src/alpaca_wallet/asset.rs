//! Alpaca Broker API crypto wallet asset listing and deposit address lookups.
//!
//! This module covers wallet asset inspection without touching transfer or
//! whitelist behavior.

use alloy::primitives::Address;
use rust_decimal::Decimal;
use serde::Deserialize;

use super::client::{AlpacaWalletClient, AlpacaWalletError};
use super::serde::deserialize_decimal_from_string;
use super::transfer::{Network, TokenSymbol};

/// A single asset entry returned by Alpaca's crypto wallet listing endpoint.
///
/// The account has one crypto wallet context, but that wallet can hold multiple
/// assets such as USDC or BTC. This type models one asset row from
/// `GET /v1/accounts/{account_id}/wallets`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(super) struct WalletAsset {
    pub(super) asset: TokenSymbol,
    #[serde(alias = "qty", deserialize_with = "deserialize_decimal_from_string")]
    pub(super) balance: Decimal,
}

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

        let response = self.get(&path).await?;
        let text = response.text().await?;

        Ok(serde_json::from_str::<WalletAddressResponse>(&text)?.address)
    }

    /// Lists the existing crypto wallet assets for the account.
    ///
    /// This intentionally calls `GET /wallets` without an `asset` query
    /// parameter. Alpaca's docs say querying by `asset` can create a wallet for
    /// that asset if one does not already exist, while the bare endpoint only
    /// lists existing wallet assets.
    pub(super) async fn list_wallet_assets(&self) -> Result<Vec<WalletAsset>, AlpacaWalletError> {
        let path = format!("/v1/accounts/{}/wallets", self.account_id());

        let response = self.get(&path).await?;
        let text = response.text().await?;

        Ok(serde_json::from_str::<Vec<WalletAsset>>(&text)?)
    }
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use serde_json::json;
    use std::str::FromStr;
    use uuid::uuid;

    use st0x_execution::AlpacaAccountId;

    use super::*;

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

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
    async fn test_list_wallet_assets_returns_assets() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "asset": "USDC",
                        "balance": "1250.75"
                    },
                    {
                        "asset": "BTC",
                        "balance": "0.50"
                    }
                ]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let assets = client.list_wallet_assets().await.unwrap();

        assert_eq!(assets.len(), 2);
        assert_eq!(assets[0].asset, TokenSymbol::new("USDC"));
        assert_eq!(assets[0].balance, Decimal::from_str("1250.75").unwrap());
        assert_eq!(assets[1].asset, TokenSymbol::new("BTC"));
        assert_eq!(assets[1].balance, Decimal::from_str("0.50").unwrap());
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
