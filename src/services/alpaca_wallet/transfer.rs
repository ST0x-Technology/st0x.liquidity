use serde::Deserialize;

use super::client::{AlpacaWalletClient, AlpacaWalletError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DepositAddress {
    pub(super) address: String,
    pub(super) asset: String,
    pub(super) network: String,
}

#[derive(Deserialize)]
struct FundingWallet {
    address: String,
    asset: String,
    network: String,
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

    let wallets: Vec<FundingWallet> = response.json().await.map_err(|e| {
        AlpacaWalletError::InvalidResponse(format!("Failed to parse funding wallet response: {e}"))
    })?;

    let wallet = wallets.first().ok_or_else(|| {
        AlpacaWalletError::InvalidResponse(format!(
            "No funding wallet found for asset {} on network {}",
            asset, network
        ))
    })?;

    Ok(DepositAddress {
        address: wallet.address.clone(),
        asset: wallet.asset.clone(),
        network: wallet.network.clone(),
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

        assert_eq!(
            deposit_address.address,
            "0x1234567890abcdef1234567890abcdef12345678"
        );
        assert_eq!(deposit_address.asset, "USDC");
        assert_eq!(deposit_address.network, "Ethereum");

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
        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::InvalidResponse(_)
        ));

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
            AlpacaWalletError::InvalidResponse(_)
        ));

        account_mock.assert();
        wallet_mock.assert();
    }
}
