//! Autonomous mock Alpaca Tokenization API for E2E tests.
//!
//! Manages tokenization state (mint/redeem requests) and registers endpoints
//! on the broker's `MockServer` since the bot resolves all Alpaca services
//! from a single base URL.
//!
//! Uses `respond_with` dynamic closures backed by shared
//! `Arc<Mutex<TokenizationState>>` so the mock auto-responds based on request
//! content. Mint requests transition from "pending" to "completed" after a
//! configurable number of polls.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use alloy::primitives::{Address, address};
use alloy::providers::Provider;
use alloy::sol;
use alloy::sol_types::SolEvent;
use chrono::Utc;
use httpmock::prelude::*;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::alpaca_broker::TEST_ACCOUNT_ID;

// ERC-20 Transfer event for the redemption watcher.
sol! {
    #[sol(all_derives = true)]
    event Transfer(address indexed from, address indexed to, uint256 value);
}

pub const REDEMPTION_WALLET: Address = address!("0x1234567890123456789012345678901234567890");

/// A mock tokenization request tracked in shared state.
struct MockTokenizationRequest {
    tokenization_request_id: String,
    issuer_request_id: String,
    underlying_symbol: String,
    qty: String,
    wallet_address: String,
    status: String,
    poll_count: usize,
    /// Number of polls before transitioning from "pending" to "completed".
    polls_until_complete: usize,
    /// "mint" or "redeem"
    request_type: String,
    /// Transaction hash (for redemption detection).
    tx_hash: String,
}

struct TokenizationState {
    requests: Vec<MockTokenizationRequest>,
    /// Number of polls before mint requests transition to "completed".
    polls_until_complete: usize,
}

/// Snapshot of a tokenization request, returned by
/// [`AlpacaTokenizationMock::tokenization_requests`].
pub struct MockTokenizationRequestSnapshot {
    pub request_id: String,
    pub request_type: String,
    pub symbol: String,
    pub qty: String,
    pub status: String,
}

/// Owns tokenization state and registers endpoints on the broker's mock
/// server. All endpoints respond dynamically based on `TokenizationState`,
/// which updates as mint/redeem requests are created and polled.
pub struct AlpacaTokenizationMock {
    state: Arc<Mutex<TokenizationState>>,
    redemption_watcher: Option<JoinHandle<()>>,
}

impl AlpacaTokenizationMock {
    /// Creates the tokenization mock and registers mint + request polling
    /// endpoints on the given server.
    pub fn start(server: &MockServer) -> Self {
        let state = Arc::new(Mutex::new(TokenizationState {
            requests: Vec::new(),
            polls_until_complete: 2,
        }));

        register_mint_endpoint(server, &state);
        register_tokenization_requests_with_filter_endpoint(server, &state);

        Self {
            state,
            redemption_watcher: None,
        }
    }

    /// Adds a redemption tokenization request to the mock state.
    ///
    /// Called by test infrastructure when it detects tokens sent to the
    /// redemption wallet, simulating Alpaca's detection of the transfer.
    pub fn add_redemption_request(
        &self,
        tx_hash: &str,
        symbol: &str,
        qty: &str,
        wallet_address: &str,
    ) {
        let mut state = lock(&self.state);
        let polls_until_complete = state.polls_until_complete;

        state.requests.push(MockTokenizationRequest {
            tokenization_request_id: Uuid::new_v4().to_string(),
            issuer_request_id: String::new(),
            underlying_symbol: symbol.to_string(),
            qty: qty.to_string(),
            wallet_address: wallet_address.to_string(),
            status: "pending".to_string(),
            poll_count: 0,
            polls_until_complete,
            request_type: "redeem".to_string(),
            tx_hash: tx_hash.to_string(),
        });
    }

    /// Starts a background task that monitors the Base chain for ERC-20
    /// Transfer events to the `redemption_wallet` address. When detected,
    /// adds a redemption tokenization request to the mock state so the
    /// bot's `poll_for_redemption` call finds a matching entry.
    pub fn start_redemption_watcher<P: Provider + Clone + Send + Sync + 'static>(
        &mut self,
        provider: P,
        redemption_wallet: Address,
    ) -> anyhow::Result<()> {
        let state = self.state.clone();

        let handle = tokio::spawn(async move {
            let mut last_block = provider.get_block_number().await.unwrap_or(0);

            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;

                let Ok(current) = provider.get_block_number().await else {
                    continue;
                };

                if current <= last_block {
                    continue;
                }

                for block_num in (last_block + 1)..=current {
                    let Ok(Some(block)) =
                        provider.get_block_by_number(block_num.into()).full().await
                    else {
                        continue;
                    };

                    for tx_hash in block.transactions.hashes() {
                        let Ok(Some(receipt)) = provider.get_transaction_receipt(tx_hash).await
                        else {
                            continue;
                        };

                        for log in receipt.inner.logs() {
                            if log.topic0() != Some(&Transfer::SIGNATURE_HASH) {
                                continue;
                            }

                            // topic2 = `to` (second indexed parameter)
                            let Some(to_topic) = log.topics().get(2) else {
                                continue;
                            };
                            let to_addr = Address::from_word(*to_topic);

                            if to_addr != redemption_wallet {
                                continue;
                            }

                            let tx_hash_hex = format!("{tx_hash:#x}");
                            let wallet_hex = format!("{redemption_wallet:#x}");

                            let mut guard = state.lock().unwrap_or_else(PoisonError::into_inner);
                            let polls_until_complete = guard.polls_until_complete;

                            guard.requests.push(MockTokenizationRequest {
                                tokenization_request_id: Uuid::new_v4().to_string(),
                                issuer_request_id: String::new(),
                                underlying_symbol: "AAPL".to_string(),
                                qty: "1.0".to_string(),
                                wallet_address: wallet_hex,
                                status: "pending".to_string(),
                                poll_count: 0,
                                polls_until_complete,
                                request_type: "redeem".to_string(),
                                tx_hash: tx_hash_hex,
                            });
                        }
                    }
                }

                last_block = current;
            }
        });
        self.redemption_watcher = Some(handle);
        Ok(())
    }

    /// Returns a snapshot of all tokenization requests (mint + redeem).
    pub fn tokenization_requests(&self) -> Vec<MockTokenizationRequestSnapshot> {
        let state = lock(&self.state);
        state
            .requests
            .iter()
            .map(|request| MockTokenizationRequestSnapshot {
                request_id: request.tokenization_request_id.clone(),
                request_type: request.request_type.clone(),
                symbol: request.underlying_symbol.clone(),
                qty: request.qty.clone(),
                status: request.status.clone(),
            })
            .collect()
    }
}

/// Locks the mutex, recovering from poisoning (a previous holder panicked).
/// Safe for test mocks -- we still want to inspect state after panics.
fn lock(state: &Mutex<TokenizationState>) -> MutexGuard<'_, TokenizationState> {
    match state.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

// ── Endpoint registration ────────────────────────────────────────────

fn register_mint_endpoint(server: &MockServer, state: &Arc<Mutex<TokenizationState>>) {
    let state = Arc::clone(state);

    server.mock(|when, then| {
        when.method(POST)
            .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/tokenization/mint"));
        then.respond_with(move |request: &HttpMockRequest| {
            let body: Value = serde_json::from_slice(request.body().as_ref()).unwrap_or(json!({}));

            let underlying_symbol = body["underlying_symbol"]
                .as_str()
                .unwrap_or("UNKNOWN")
                .to_string();
            let qty = body["qty"].as_str().unwrap_or("0").to_string();
            let wallet_address = body["wallet_address"].as_str().unwrap_or("").to_string();
            let issuer_request_id = body["issuer_request_id"].as_str().unwrap_or("").to_string();

            let tokenization_request_id = Uuid::new_v4().to_string();

            {
                let mut state = lock(&state);
                let polls_until_complete = state.polls_until_complete;

                state.requests.push(MockTokenizationRequest {
                    tokenization_request_id: tokenization_request_id.clone(),
                    issuer_request_id: issuer_request_id.clone(),
                    underlying_symbol: underlying_symbol.clone(),
                    qty: qty.clone(),
                    wallet_address: wallet_address.clone(),
                    status: "pending".to_string(),
                    poll_count: 0,
                    polls_until_complete,
                    request_type: "mint".to_string(),
                    tx_hash: String::new(),
                });
            }

            json_response(
                200,
                &json!({
                    "tokenization_request_id": tokenization_request_id,
                    "type": "mint",
                    "status": "pending",
                    "underlying_symbol": underlying_symbol,
                    "token_symbol": format!("t{underlying_symbol}"),
                    "qty": qty,
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address": wallet_address,
                    "issuer_request_id": issuer_request_id,
                    "tx_hash": "",
                    "created_at": Utc::now().to_rfc3339(),
                }),
            )
        });
    });
}

fn register_tokenization_requests_endpoint(
    server: &MockServer,
    state: &Arc<Mutex<TokenizationState>>,
) {
    let state = Arc::clone(state);

    server.mock(|when, then| {
        when.method(GET).path(format!(
            "/v1/accounts/{TEST_ACCOUNT_ID}/tokenization/requests"
        ));
        then.respond_with(move |_request: &HttpMockRequest| {
            let requests: Vec<Value> = {
                let mut state = lock(&state);

                // Advance each pending request's poll count and transition
                // to "completed" once the threshold is reached.
                for request in &mut state.requests {
                    if request.status == "pending" {
                        request.poll_count += 1;
                        if request.poll_count >= request.polls_until_complete {
                            request.status = "completed".to_string();
                        }
                    }
                }

                state
                    .requests
                    .iter()
                    .map(tokenization_request_to_json)
                    .collect()
            };

            json_response(200, &Value::Array(requests))
        });
    });
}

/// Tokenization requests endpoint with `type` query param filtering.
fn register_tokenization_requests_with_filter_endpoint(
    server: &MockServer,
    state: &Arc<Mutex<TokenizationState>>,
) {
    let state = Arc::clone(state);

    server.mock(|when, then| {
        when.method(GET).path(format!(
            "/v1/accounts/{TEST_ACCOUNT_ID}/tokenization/requests"
        ));
        then.respond_with(move |request: &HttpMockRequest| {
            let uri = request.uri();
            let query = uri.query().unwrap_or("");
            let type_filter = query
                .split('&')
                .find_map(|param| param.strip_prefix("type="))
                .unwrap_or("");

            let requests: Vec<Value> = {
                let mut state = lock(&state);

                // Advance pending requests
                for req in &mut state.requests {
                    if req.status == "pending" {
                        req.poll_count += 1;
                        if req.poll_count >= req.polls_until_complete {
                            req.status = "completed".to_string();
                        }
                    }
                }

                state
                    .requests
                    .iter()
                    .filter(|req| type_filter.is_empty() || req.request_type == type_filter)
                    .map(tokenization_request_to_json)
                    .collect()
            };

            json_response(200, &Value::Array(requests))
        });
    });
}

/// Converts a `MockTokenizationRequest` to its JSON representation.
fn tokenization_request_to_json(request: &MockTokenizationRequest) -> Value {
    // Completed requests include a tx_hash. For mints, generate a fake one
    // if the request didn't already have one (from redemption detection).
    let tx_hash = if request.status == "completed" && request.tx_hash.is_empty() {
        format!("0x{}", "ab".repeat(32))
    } else {
        request.tx_hash.clone()
    };

    json!({
        "tokenization_request_id": request.tokenization_request_id,
        "type": request.request_type,
        "status": request.status,
        "underlying_symbol": request.underlying_symbol,
        "token_symbol": format!("t{}", request.underlying_symbol),
        "qty": request.qty,
        "issuer": "st0x",
        "network": "base",
        "wallet_address": request.wallet_address,
        "issuer_request_id": request.issuer_request_id,
        "tx_hash": tx_hash,
        "created_at": "2025-01-01T00:00:00Z",
    })
}

/// Builds an `HttpMockResponse` with JSON content-type and serialized body.
fn json_response(status: u16, body: &Value) -> HttpMockResponse {
    let serialized = serde_json::to_vec(body).unwrap_or_default();
    HttpMockResponse {
        status: Some(status),
        headers: Some(vec![(
            "content-type".to_string(),
            "application/json".to_string(),
        )]),
        body: Some(serialized.into()),
    }
}
