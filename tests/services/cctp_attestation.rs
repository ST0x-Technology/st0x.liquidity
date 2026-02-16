//! Mock Circle CCTP Attestation API for E2E tests.
//!
//! Provides an HTTP mock server that responds to the endpoints the bot calls
//! when bridging USDC via CCTP: fee schedule queries and attestation polling.

use httpmock::prelude::*;
use serde_json::json;

/// Owns the `MockServer` and pre-configures a happy-path fee schedule response.
/// Scenario methods allow per-test customisation of attestation outcomes.
pub struct CctpAttestationMock {
    server: MockServer,
}

impl CctpAttestationMock {
    /// Starts the mock server with a default fee schedule endpoint that matches
    /// any source/dest domain pair under `/v2/burn/USDC/fees/`.
    pub async fn start() -> Self {
        let server = MockServer::start_async().await;

        server.mock(|when, then| {
            when.method(GET).path_contains("/v2/burn/USDC/fees/");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {"finalityThreshold": 1000, "minimumFee": 1},
                    {"finalityThreshold": 2000, "minimumFee": 0}
                ]));
        });

        Self { server }
    }

    pub fn base_url(&self) -> String {
        self.server.base_url()
    }

    /// Mocks a completed attestation response for a given transaction hash.
    /// `message_hex` and `attestation_hex` should be "0x"-prefixed hex strings.
    pub fn mock_attestation_complete(
        &self,
        tx_hash: &str,
        message_hex: &str,
        attestation_hex: &str,
    ) {
        let tx_hash = tx_hash.to_string();
        let message_hex = message_hex.to_string();
        let attestation_hex = attestation_hex.to_string();

        self.server.mock(|when, then| {
            when.method(GET)
                .path_contains("/v2/messages/")
                .query_param("transactionHash", &tx_hash);
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "messages": [{
                        "status": "complete",
                        "message": message_hex,
                        "attestation": attestation_hex
                    }]
                }));
        });
    }

    /// Mocks a pending attestation response for a given transaction hash.
    pub fn mock_attestation_pending(&self, tx_hash: &str) {
        let tx_hash = tx_hash.to_string();

        self.server.mock(|when, then| {
            when.method(GET)
                .path_contains("/v2/messages/")
                .query_param("transactionHash", &tx_hash);
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "messages": [{
                        "status": "pending"
                    }]
                }));
        });
    }
}
