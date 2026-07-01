//! Shared Alpaca tokenization HTTP mock fixtures for integration tests.

use std::time::Duration;

use alloy::primitives::{Address, B256, TxHash};
use alloy::providers::ProviderBuilder;
use httpmock::Mock;
use httpmock::prelude::*;
use serde_json::json;
use uuid::uuid;

use st0x_evm::Wallet;
use st0x_evm::local::RawPrivateKeyWallet;
use st0x_execution::{AlpacaAccountId, PollingConfig};
use st0x_tokenization::{AlpacaTokenizationService, IssuerRequestId};

pub(crate) const TEST_ACCOUNT_ID: AlpacaAccountId =
    AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

pub(crate) fn tokenization_mint_path() -> String {
    format!("/v1/accounts/{TEST_ACCOUNT_ID}/tokenization/mint")
}

pub(crate) fn tokenization_requests_path() -> String {
    format!("/v1/accounts/{TEST_ACCOUNT_ID}/tokenization/requests")
}

/// Builds an `AlpacaTokenizationService` pointed at a mock HTTP server, signing
/// onchain transactions with the Anvil-derived wallet and polling on the fast
/// test cadence.
pub(crate) async fn create_test_service_from_mock(
    server: &MockServer,
    anvil_endpoint: &str,
    private_key: &B256,
    redemption_wallet: Address,
) -> AlpacaTokenizationService<impl Wallet> {
    let provider = ProviderBuilder::new()
        .connect(anvil_endpoint)
        .await
        .unwrap();
    let wallet = RawPrivateKeyWallet::new(private_key, provider, 1).unwrap();

    AlpacaTokenizationService::new(
        server.base_url(),
        TEST_ACCOUNT_ID,
        "test_api_key".to_string(),
        "test_api_secret".to_string(),
        wallet,
        Some(redemption_wallet),
    )
    .with_polling_config(PollingConfig {
        interval: Duration::from_millis(10),
        timeout: Duration::from_secs(5),
        max_retries: 3,
        min_retry_delay: Duration::from_millis(10),
        max_retry_delay: Duration::from_millis(100),
    })
}

/// Creates httpmock responses for the Alpaca tokenization API detection and
/// completion polling endpoints, matching the given tx_hash.
pub(crate) fn setup_redemption_mocks(
    server: &MockServer,
    expected_tx_hash: TxHash,
) -> (Mock<'_>, Mock<'_>) {
    let detection_mock = server.mock(|when, then| {
        when.method(GET)
            .path(tokenization_requests_path())
            .query_param("type", "redeem");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!([{
                "tokenization_request_id": "redeem_int_test",
                "type": "redeem",
                "status": "pending",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "qty": "30.0",
                "issuer": "st0x",
                "network": "base",
                "tx_hash": expected_tx_hash,
                "created_at": "2024-01-15T10:30:00Z"
            }]));
    });

    let completion_mock = server.mock(|when, then| {
        when.method(GET).path(tokenization_requests_path());
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!([{
                "tokenization_request_id": "redeem_int_test",
                "type": "redeem",
                "status": "completed",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "qty": "30.0",
                "issuer": "st0x",
                "network": "base",
                "tx_hash": expected_tx_hash,
                "created_at": "2024-01-15T10:30:00Z"
            }]));
    });

    (detection_mock, completion_mock)
}

pub(crate) fn sample_pending_response(
    tokenization_request_id: &str,
    issuer_request_id: &IssuerRequestId,
) -> serde_json::Value {
    json!({
        "tokenization_request_id": tokenization_request_id,
        "type": "mint",
        "status": "pending",
        "underlying_symbol": "AAPL",
        "token_symbol": "tAAPL",
        "qty": "30.0",
        "issuer": "st0x",
        "network": "base",
        "wallet_address": "0x0000000000000000000000000000000000000000",
        "issuer_request_id": issuer_request_id.to_string(),
        "created_at": "2024-01-15T10:30:00Z"
    })
}

pub(crate) fn sample_completed_response(
    tokenization_request_id: &str,
    issuer_request_id: &IssuerRequestId,
    tx_hash: TxHash,
) -> serde_json::Value {
    json!({
        "tokenization_request_id": tokenization_request_id,
        "type": "mint",
        "status": "completed",
        "underlying_symbol": "AAPL",
        "token_symbol": "tAAPL",
        "qty": "30.0",
        "issuer": "st0x",
        "network": "base",
        "wallet_address": "0x0000000000000000000000000000000000000000",
        "issuer_request_id": issuer_request_id.to_string(),
        "tx_hash": tx_hash,
        "created_at": "2024-01-15T10:30:00Z"
    })
}
