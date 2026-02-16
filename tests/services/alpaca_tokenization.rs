//! Mock Alpaca Tokenization API for E2E tests.
//!
//! Provides an HTTP mock server that responds to the endpoints the bot calls
//! when interacting with the Alpaca Tokenization API: listing tokenization
//! requests and requesting mints.

use httpmock::prelude::*;
use serde_json::json;

use super::alpaca_broker::TEST_ACCOUNT_ID;

/// Owns the `MockServer` and pre-configures a happy-path response for the
/// tokenization requests listing (empty array). Scenario methods allow
/// per-test customisation of mints and redemptions.
pub struct AlpacaTokenizationMock {
    server: MockServer,
}

impl AlpacaTokenizationMock {
    /// Starts the mock server with a default empty request list.
    pub async fn start() -> Self {
        let server = MockServer::start_async().await;

        server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{TEST_ACCOUNT_ID}/tokenization/requests"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        Self { server }
    }

    pub fn base_url(&self) -> String {
        self.server.base_url()
    }

    /// Mocks a successful mint request acceptance.
    pub fn mock_mint_accepted(&self, request_id: &str, symbol: &str, qty: &str) {
        let request_id = request_id.to_string();
        let symbol = symbol.to_string();
        let token_symbol = format!("t{symbol}");
        let qty = qty.to_string();

        self.server.mock(|when, then| {
            when.method(POST)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/tokenization/mint"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "tokenization_request_id": request_id,
                    "type": "mint",
                    "status": "pending",
                    "underlying_symbol": symbol,
                    "token_symbol": token_symbol,
                    "qty": qty,
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address":
                        "0x1234567890abcdef1234567890abcdef12345678",
                    "created_at": "2024-01-15T10:30:00Z"
                }));
        });
    }

    /// Mocks the requests list returning a single completed mint.
    pub fn mock_mint_completed(&self, request_id: &str, symbol: &str) {
        let request_id = request_id.to_string();
        let symbol = symbol.to_string();
        let token_symbol = format!("t{symbol}");

        self.server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{TEST_ACCOUNT_ID}/tokenization/requests"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "tokenization_request_id": request_id,
                    "type": "mint",
                    "status": "completed",
                    "underlying_symbol": symbol,
                    "token_symbol": token_symbol,
                    "qty": "50.0",
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address":
                        "0x1234567890abcdef1234567890abcdef12345678",
                    "created_at": "2024-01-15T10:30:00Z"
                }]));
        });
    }

    /// Mocks the requests list returning a pending redemption filtered by type
    /// and status query params.
    pub fn mock_redemption_pending(&self, request_id: &str, symbol: &str, tx_hash: &str) {
        let request_id = request_id.to_string();
        let symbol = symbol.to_string();
        let token_symbol = format!("t{symbol}");
        let tx_hash = tx_hash.to_string();

        self.server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{TEST_ACCOUNT_ID}/tokenization/requests"
                ))
                .query_param("type", "redeem")
                .query_param("status", "pending");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "tokenization_request_id": request_id,
                    "type": "redeem",
                    "status": "pending",
                    "underlying_symbol": symbol,
                    "token_symbol": token_symbol,
                    "qty": "50.0",
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address":
                        "0x1234567890abcdef1234567890abcdef12345678",
                    "created_at": "2024-01-15T10:30:00Z",
                    "transaction_hash": tx_hash
                }]));
        });
    }

    /// Mocks the requests list returning a completed redemption filtered by
    /// type and status query params.
    pub fn mock_redemption_completed(&self, request_id: &str, symbol: &str) {
        let request_id = request_id.to_string();
        let symbol = symbol.to_string();
        let token_symbol = format!("t{symbol}");

        self.server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{TEST_ACCOUNT_ID}/tokenization/requests"
                ))
                .query_param("type", "redeem")
                .query_param("status", "completed");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "tokenization_request_id": request_id,
                    "type": "redeem",
                    "status": "completed",
                    "underlying_symbol": symbol,
                    "token_symbol": token_symbol,
                    "qty": "50.0",
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address":
                        "0x1234567890abcdef1234567890abcdef12345678",
                    "created_at": "2024-01-15T10:30:00Z"
                }]));
        });
    }
}
