//! Mock Circle CCTP Attestation API for E2E tests.
//!
//! Provides an HTTP mock server that responds to the endpoints the bot calls
//! when bridging USDC via CCTP: fee schedule queries and attestation polling.
//!
//! For USDC rebalancing tests, a background watcher monitors forked chains
//! for `MessageSent` events and auto-signs attestations with a test key.
//!
//! Uses `respond_with` dynamic closures backed by shared
//! `Arc<Mutex<AttestationState>>` so the watcher can register attestations
//! from a background task without needing the `MockServer` handle.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use alloy::primitives::{B256, keccak256};
use alloy::providers::Provider;
use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use alloy::sol_types::SolEvent;
use httpmock::prelude::*;
use rand::Rng;
use serde_json::json;
use tokio::task::JoinHandle;

sol! {
    #[sol(all_derives = true)]
    #[derive(serde::Serialize, serde::Deserialize)]
    event MessageSent(bytes message);
}

struct AttestationData {
    message_hex: String,
    attestation_hex: String,
}

struct AttestationState {
    /// Completed attestations keyed by transaction hash (0x-prefixed).
    attestations: HashMap<String, AttestationData>,
}

fn lock(state: &Arc<Mutex<AttestationState>>) -> std::sync::MutexGuard<'_, AttestationState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Owns the `MockServer` and pre-configures a happy-path fee schedule response.
/// Scenario methods allow per-test customisation of attestation outcomes.
pub struct CctpAttestationMock {
    server: MockServer,
    state: Arc<Mutex<AttestationState>>,
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

        let state = Arc::new(Mutex::new(AttestationState {
            attestations: HashMap::new(),
        }));

        // Single dynamic handler for all attestation polling requests.
        // Looks up the `transactionHash` query param in shared state.
        let handler_state = Arc::clone(&state);

        server.mock(|when, then| {
            when.method(GET).path_includes("/v2/messages/");
            then.respond_with(move |request: &HttpMockRequest| {
                let uri = request.uri();
                let query = uri.query().unwrap_or("");
                let tx_hash = query
                    .split('&')
                    .find_map(|param| param.strip_prefix("transactionHash="))
                    .unwrap_or("");

                let body = lock(&handler_state).attestations.get(tx_hash).map_or_else(
                    || {
                        json!({
                            "messages": [{
                                "status": "pending"
                            }]
                        })
                    },
                    |data| {
                        json!({
                            "messages": [{
                                "status": "complete",
                                "message": data.message_hex,
                                "attestation": data.attestation_hex
                            }]
                        })
                    },
                );

                let serialized = serde_json::to_vec(&body).unwrap_or_default();

                HttpMockResponse {
                    status: Some(200),
                    headers: Some(vec![(
                        "content-type".to_string(),
                        "application/json".to_string(),
                    )]),
                    body: Some(serialized.into()),
                }
            });
        });

        Self { server, state }
    }

    pub fn base_url(&self) -> String {
        self.server.base_url()
    }

    /// Returns how many unique `MessageSent` attestations the watcher has
    /// processed (useful for e2e test assertions).
    pub fn processed_attestation_count(&self) -> usize {
        lock(&self.state).attestations.len()
    }

    /// Starts a background task that monitors both chains for `MessageSent`
    /// events and auto-registers completed attestation mocks.
    ///
    /// The watcher polls recent blocks on both chains, extracts CCTP messages,
    /// signs them with the test attester key, and stores them in shared state
    /// for the dynamic mock handler to serve.
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
        let state = Arc::clone(&self.state);

        tokio::spawn(async move {
            let Ok(signer) = PrivateKeySigner::from_bytes(&attester_key) else {
                return;
            };

            let mut eth_last_block = ethereum_provider.get_block_number().await.unwrap_or(0);
            let mut base_last_block = base_provider.get_block_number().await.unwrap_or(0);

            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;

                if let Ok(current) = ethereum_provider.get_block_number().await
                    && current > eth_last_block
                {
                    for block_num in (eth_last_block + 1)..=current {
                        scan_block_for_messages(&ethereum_provider, block_num, &signer, &state)
                            .await;
                    }
                    eth_last_block = current;
                }

                if let Ok(current) = base_provider.get_block_number().await
                    && current > base_last_block
                {
                    for block_num in (base_last_block + 1)..=current {
                        scan_block_for_messages(&base_provider, block_num, &signer, &state).await;
                    }
                    base_last_block = current;
                }
            }
        })
    }
}

/// Scans a single block for `MessageSent` events and registers attestation
/// data in shared state for any new ones found.
async fn scan_block_for_messages<P: Provider>(
    provider: &P,
    block_number: u64,
    signer: &PrivateKeySigner,
    state: &Arc<Mutex<AttestationState>>,
) {
    let Ok(Some(block)) = provider
        .get_block_by_number(block_number.into())
        .full()
        .await
    else {
        return;
    };

    for tx_hash in block.transactions.hashes() {
        {
            let state_guard = lock(state);
            let tx_hash_hex = format!("{tx_hash:#x}");
            if state_guard.attestations.contains_key(&tx_hash_hex) {
                continue;
            }
        }

        let Ok(Some(receipt)) = provider.get_transaction_receipt(tx_hash).await else {
            continue;
        };

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

            lock(state).attestations.insert(
                tx_hash_hex,
                AttestationData {
                    message_hex,
                    attestation_hex,
                },
            );
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

    // Generate a random nonce that fits in u64 (upper 24 bytes must be zero
    // for AttestationResponse::new() validation). Only the low 8 bytes are
    // randomized; the high 24 remain zero.
    let mut nonce = [0u8; 32];
    rand::thread_rng().fill(&mut nonce[24..]);
    // Ensure nonce is not zero (reserved)
    nonce[31] |= 1;

    const NONCE_INDEX: usize = 12;
    if modified.len() >= NONCE_INDEX + 32 {
        modified[NONCE_INDEX..NONCE_INDEX + 32].copy_from_slice(&nonce);
    }

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

#[cfg(test)]
mod tests {
    use alloy::primitives::{FixedBytes, keccak256};
    use alloy::signers::local::PrivateKeySigner;
    use serde_json::Value;

    use super::{AttestationData, CctpAttestationMock, lock, sign_cctp_message};

    /// Deterministic test attester key (not used on any real network).
    fn test_signer() -> PrivateKeySigner {
        let key = FixedBytes::repeat_byte(0xAB);
        PrivateKeySigner::from_bytes(&key).unwrap()
    }

    fn zeroed_message(len: usize) -> Vec<u8> {
        vec![0u8; len]
    }

    #[tokio::test]
    async fn sign_cctp_message_injects_nonce_at_correct_offset() {
        let signer = test_signer();
        let message = zeroed_message(200);

        let (_, modified) = sign_cctp_message(&message, &signer).await.unwrap();

        // Nonce is at bytes 12..44. At least some bytes must be non-zero.
        let nonce_region = &modified[12..44];
        assert!(
            nonce_region.iter().any(|byte| *byte != 0),
            "Nonce region (bytes 12..44) should have non-zero bytes after signing",
        );

        // Bit 0 of the last nonce byte is forced on to ensure non-zero nonce.
        // The nonce is 32 bytes at offset 12, so the last byte is at index 43.
        assert_eq!(
            modified[43] & 1,
            1,
            "Low bit of last nonce byte (index 43) should be set",
        );
    }

    #[tokio::test]
    async fn sign_cctp_message_sets_finality_threshold() {
        let signer = test_signer();
        let message = zeroed_message(200);

        let (_, modified) = sign_cctp_message(&message, &signer).await.unwrap();

        let finality_bytes: [u8; 4] = modified[144..148].try_into().unwrap();
        let finality_value = u32::from_be_bytes(finality_bytes);
        assert_eq!(
            finality_value, 2000,
            "Finality threshold at bytes 144..148 should be 2000",
        );
    }

    #[tokio::test]
    async fn sign_cctp_message_produces_valid_signature() {
        let signer = test_signer();
        let message = zeroed_message(200);

        let (attestation_bytes, modified) = sign_cctp_message(&message, &signer).await.unwrap();

        // Recover the signer from the signature and verify it matches
        let message_hash = keccak256(&modified);
        let signature = alloy::primitives::Signature::try_from(attestation_bytes.as_slice())
            .expect("attestation should be a valid 65-byte signature");
        let recovered = signature
            .recover_address_from_prehash(&message_hash)
            .expect("should recover address from prehash");

        assert_eq!(
            recovered,
            signer.address(),
            "Recovered signer address should match test attester",
        );
    }

    #[tokio::test]
    async fn sign_cctp_message_preserves_other_bytes() {
        let signer = test_signer();
        let mut message = zeroed_message(200);
        // Fill with a recognizable pattern
        for (index, byte) in message.iter_mut().enumerate() {
            *byte = u8::try_from(index % 256).unwrap();
        }

        let original = message.clone();
        let (_, modified) = sign_cctp_message(&message, &signer).await.unwrap();

        // Bytes outside nonce (12..44) and finality (144..148) should be unchanged.
        for index in 0..200 {
            let in_nonce_range = (12..44).contains(&index);
            let in_finality_range = (144..148).contains(&index);

            if !in_nonce_range && !in_finality_range {
                assert_eq!(
                    modified[index], original[index],
                    "Byte at index {index} should be preserved \
                     (not in nonce or finality range)",
                );
            }
        }
    }

    #[tokio::test]
    async fn attestation_mock_returns_pending_for_unknown_tx() {
        let mock = CctpAttestationMock::start().await;
        let url = format!(
            "{}/v2/messages/0xabc?transactionHash=0xunknown",
            mock.base_url()
        );

        let response: Value = reqwest::get(&url).await.unwrap().json().await.unwrap();

        let status = response["messages"][0]["status"].as_str().unwrap();
        assert_eq!(status, "pending", "Unknown tx should return pending status");
    }

    #[tokio::test]
    async fn attestation_mock_returns_complete_after_registration() {
        let mock = CctpAttestationMock::start().await;

        let tx_hash = "0xdeadbeef";
        let expected_message = "0xcafebabe";
        let expected_attestation = "0x1234abcd";

        lock(&mock.state).attestations.insert(
            tx_hash.to_string(),
            AttestationData {
                message_hex: expected_message.to_string(),
                attestation_hex: expected_attestation.to_string(),
            },
        );

        let url = format!(
            "{}/v2/messages/0xabc?transactionHash={tx_hash}",
            mock.base_url()
        );
        let response: Value = reqwest::get(&url).await.unwrap().json().await.unwrap();

        let msg = &response["messages"][0];
        assert_eq!(msg["status"].as_str().unwrap(), "complete");
        assert_eq!(msg["message"].as_str().unwrap(), expected_message);
        assert_eq!(msg["attestation"].as_str().unwrap(), expected_attestation);
    }

    #[tokio::test]
    async fn fee_schedule_endpoint_returns_tiers() {
        let mock = CctpAttestationMock::start().await;
        let url = format!("{}/v2/burn/USDC/fees/0/6", mock.base_url());

        let response: Value = reqwest::get(&url).await.unwrap().json().await.unwrap();

        let tiers = response.as_array().unwrap();
        assert_eq!(tiers.len(), 2, "Fee schedule should have two tiers");
        assert_eq!(tiers[0]["finalityThreshold"], 1000);
        assert_eq!(tiers[0]["minimumFee"], 1);
        assert_eq!(tiers[1]["finalityThreshold"], 2000);
        assert_eq!(tiers[1]["minimumFee"], 0);
    }

    #[tokio::test]
    async fn processed_attestation_count_tracks_state() {
        let mock = CctpAttestationMock::start().await;

        assert_eq!(
            mock.processed_attestation_count(),
            0,
            "Initial attestation count should be zero",
        );

        lock(&mock.state).attestations.insert(
            "0xaaa".to_string(),
            AttestationData {
                message_hex: "0x01".to_string(),
                attestation_hex: "0x02".to_string(),
            },
        );

        assert_eq!(
            mock.processed_attestation_count(),
            1,
            "Attestation count should increment after insertion",
        );
    }
}
