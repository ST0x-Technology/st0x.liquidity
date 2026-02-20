//! Mock Circle CCTP Attestation API for E2E tests.
//!
//! Provides an HTTP mock server that responds to the endpoints the bot calls
//! when bridging USDC via CCTP: fee schedule queries and attestation polling.
//!
//! For USDC rebalancing tests, a background watcher monitors forked chains
//! for `MessageSent` events and auto-signs attestations with a test key.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use alloy::primitives::{B256, TxHash, keccak256};
use alloy::providers::Provider;
use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use alloy::sol_types::SolEvent;
use httpmock::prelude::*;
use rand::Rng;
use serde_json::json;
use tokio::task::JoinHandle;

// Minimal ABI for MessageSent event extraction.
sol! {
    #[sol(all_derives = true)]
    #[derive(serde::Serialize, serde::Deserialize)]
    event MessageSent(bytes message);
}

/// Owns the `MockServer` and pre-configures a happy-path fee schedule response.
/// Scenario methods allow per-test customisation of attestation outcomes.
pub struct CctpAttestationMock {
    server: MockServer,
    /// Tracks tx hashes that already have attestation mocks registered.
    seen_tx_hashes: Arc<Mutex<HashSet<TxHash>>>,
}

impl CctpAttestationMock {
    /// Starts the mock server with a default fee schedule endpoint that matches
    /// any source/dest domain pair under `/v2/burn/USDC/fees/`.
    pub async fn start() -> Self {
        let server = MockServer::start_async().await;

        server.mock(|when, then| {
            when.method(GET).path_includes("/v2/burn/USDC/fees/");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {"finalityThreshold": 1000, "minimumFee": 1},
                    {"finalityThreshold": 2000, "minimumFee": 0}
                ]));
        });

        Self {
            server,
            seen_tx_hashes: Arc::new(Mutex::new(HashSet::new())),
        }
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
                .path_includes("/v2/messages/")
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
                .path_includes("/v2/messages/")
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

    /// Starts a background task that monitors both chains for `MessageSent`
    /// events and auto-registers completed attestation mocks.
    ///
    /// The watcher polls recent blocks on both chains, extracts CCTP messages,
    /// signs them with the test attester key, and calls
    /// `mock_attestation_complete` for each new burn transaction.
    pub fn start_watcher<EP, BP>(
        &self,
        ethereum_provider: EP,
        base_provider: BP,
        attester_key: B256,
    ) -> JoinHandle<()>
    where
        EP: Provider + Clone + Send + Sync + 'static,
        BP: Provider + Clone + Send + Sync + 'static,
    {
        let server_url = self.server.base_url();
        let seen = Arc::clone(&self.seen_tx_hashes);

        tokio::spawn(async move {
            let Ok(signer) = PrivateKeySigner::from_bytes(&attester_key) else {
                return;
            };

            // Track the last scanned block for each chain
            let mut eth_last_block = ethereum_provider.get_block_number().await.unwrap_or(0);
            let mut base_last_block = base_provider.get_block_number().await.unwrap_or(0);

            // Create an HTTP client to register attestation responses
            let client = reqwest::Client::new();

            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;

                // Scan Ethereum
                if let Ok(current) = ethereum_provider.get_block_number().await
                    && current > eth_last_block
                {
                    for block_num in (eth_last_block + 1)..=current {
                        scan_block_for_messages(
                            &ethereum_provider,
                            block_num,
                            &signer,
                            &seen,
                            &server_url,
                            &client,
                        )
                        .await;
                    }
                    eth_last_block = current;
                }

                // Scan Base
                if let Ok(current) = base_provider.get_block_number().await
                    && current > base_last_block
                {
                    for block_num in (base_last_block + 1)..=current {
                        scan_block_for_messages(
                            &base_provider,
                            block_num,
                            &signer,
                            &seen,
                            &server_url,
                            &client,
                        )
                        .await;
                    }
                    base_last_block = current;
                }
            }
        })
    }
}

/// Scans a single block for `MessageSent` events and registers attestation
/// mocks for any new ones found.
async fn scan_block_for_messages<P: Provider>(
    provider: &P,
    block_number: u64,
    signer: &PrivateKeySigner,
    seen: &Arc<Mutex<HashSet<TxHash>>>,
    server_url: &str,
    client: &reqwest::Client,
) {
    let Ok(Some(block)) = provider
        .get_block_by_number(block_number.into())
        .full()
        .await
    else {
        return;
    };

    for tx_hash in block.transactions.hashes() {
        // Check if already processed
        {
            let seen_guard = seen.lock().unwrap_or_else(PoisonError::into_inner);
            if seen_guard.contains(&tx_hash) {
                continue;
            }
        }

        let Ok(Some(receipt)) = provider.get_transaction_receipt(tx_hash).await else {
            continue;
        };

        // Look for MessageSent events
        for log in receipt.inner.logs() {
            let Ok(event) = MessageSent::decode_log(log.as_ref()) else {
                continue;
            };
            let message = event.message.to_vec();

            // Sign the message with nonce and finality threshold filled in
            let Some((attestation_bytes, modified_message)) =
                sign_cctp_message(&message, signer).await
            else {
                continue;
            };

            let tx_hash_hex = format!("{tx_hash:#x}");
            let message_hex = format!("0x{}", alloy::hex::encode(&modified_message));
            let attestation_hex = format!("0x{}", alloy::hex::encode(&attestation_bytes));

            // Register the attestation mock via httpmock's API.
            // We post directly to the mock server's management endpoint.
            register_attestation_via_http(
                client,
                server_url,
                &tx_hash_hex,
                &message_hex,
                &attestation_hex,
            )
            .await;

            // Mark as seen
            let mut seen_guard = seen.lock().unwrap_or_else(PoisonError::into_inner);
            seen_guard.insert(tx_hash);
        }
    }
}

/// Signs a CCTP message as the test attester.
///
/// Fills in a random nonce at position 12-44 and sets
/// `finalityThresholdExecuted` to 2000 at position 144-148, mirroring
/// what Circle's attester does in production.
async fn sign_cctp_message(
    message: &[u8],
    signer: &PrivateKeySigner,
) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut modified = message.to_vec();

    // Generate a random nonce at position 12-44
    let mut nonce = [0u8; 32];
    rand::thread_rng().fill(&mut nonce);
    // Ensure nonce is not zero (reserved)
    nonce[0] |= 1;

    const NONCE_INDEX: usize = 12;
    if modified.len() >= NONCE_INDEX + 32 {
        modified[NONCE_INDEX..NONCE_INDEX + 32].copy_from_slice(&nonce);
    }

    // Set finalityThresholdExecuted to FINALIZED (2000)
    const FINALITY_INDEX: usize = 144;
    const FINALITY_FINALIZED: u32 = 2000;
    if modified.len() >= FINALITY_INDEX + 4 {
        modified[FINALITY_INDEX..FINALITY_INDEX + 4]
            .copy_from_slice(&FINALITY_FINALIZED.to_be_bytes());
    }

    let message_hash = keccak256(&modified);
    let signature = signer.sign_hash(&message_hash).await.ok()?;

    Some((signature.as_bytes().to_vec(), modified))
}

/// Registers an attestation response by making a direct httpmock management
/// API call. This is necessary because the watcher runs in a background task
/// that doesn't have access to the `MockServer` handle.
///
/// httpmock supports a management HTTP API at `/__httpmock__/mocks` for
/// dynamically adding mock definitions.
async fn register_attestation_via_http(
    client: &reqwest::Client,
    server_url: &str,
    tx_hash: &str,
    message_hex: &str,
    attestation_hex: &str,
) {
    let body = json!({
        "messages": [{
            "status": "complete",
            "message": message_hex,
            "attestation": attestation_hex
        }]
    });

    let mock_definition = json!({
        "when": {
            "method": "GET",
            "path_contains": "/v2/messages/",
            "query_param": [
                {"key": "transactionHash", "value": tx_hash}
            ]
        },
        "then": {
            "status": 200,
            "headers": {"content-type": "application/json"},
            "body": body.to_string()
        }
    });

    let url = format!("{server_url}/__httpmock__/mocks");

    let _response = client.post(&url).json(&mock_definition).send().await;
}
