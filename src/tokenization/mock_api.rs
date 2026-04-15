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

use alloy::primitives::utils::parse_units;
use alloy::primitives::{Address, U256, address};
use alloy::providers::Provider;
use alloy::sol;
use alloy::sol_types::SolEvent;
use chrono::Utc;
use httpmock::prelude::*;
use rain_math_float::Float;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::warn;
use uuid::Uuid;

use st0x_execution::alpaca_broker_api::TEST_ACCOUNT_ID;
use st0x_execution::{FractionalShares, SharesBlockchain};

use crate::bindings::DeployableERC20;
use st0x_float_serde::format_float_with_fallback;

sol! {
    #[sol(all_derives = true)]
    event Transfer(address indexed from, address indexed to, uint256 value);
}

pub const REDEMPTION_WALLET: Address = address!("0x1234567890123456789012345678901234567890");

/// Status of a tokenization request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenizationStatus {
    Pending,
    Completed,
    Failed,
    /// Maps to `"rejected"` in the Alpaca API, which the bot deserializes
    /// as `TokenizationRequestStatus::Rejected`.
    Rejected,
}

impl std::fmt::Display for TokenizationStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(formatter, "pending"),
            Self::Completed => write!(formatter, "completed"),
            Self::Failed => write!(formatter, "failed"),
            Self::Rejected => write!(formatter, "rejected"),
        }
    }
}

/// Type of a tokenization request (mint or redeem).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenizationRequestType {
    Mint,
    Redeem,
}

impl std::fmt::Display for TokenizationRequestType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mint => write!(formatter, "mint"),
            Self::Redeem => write!(formatter, "redeem"),
        }
    }
}

/// A mock tokenization request tracked in shared state.
struct MockTokenizationRequest {
    tokenization_request_id: String,
    issuer_request_id: String,
    underlying_symbol: String,
    quantity: Float,
    wallet_address: Address,
    status: TokenizationStatus,
    poll_count: usize,
    /// Number of polls before transitioning from "pending" to "completed".
    polls_until_complete: usize,
    request_type: TokenizationRequestType,
    /// Transaction hash (for redemption detection).
    tx_hash: String,
    /// Whether the background mint executor still needs to run the onchain
    /// transfer. Set to `true` when `poll_count` crosses the threshold for
    /// mint requests; the mint executor clears it after executing a real
    /// ERC-20 transfer and recording the tx hash.
    needs_mint_execution: bool,
}

/// Controls what happens when a redemption request reaches its poll threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedemptionOutcome {
    /// Auto-complete after `polls_until_complete` polls (default behavior).
    Complete,
    /// Transition to `Rejected` after `polls_until_complete` polls.
    Reject,
}

struct TokenizationState {
    requests: Vec<MockTokenizationRequest>,
    /// Number of polls before mint requests transition to "completed".
    polls_until_complete: usize,
    /// What happens when a redemption request reaches its poll threshold.
    redemption_outcome: RedemptionOutcome,
}

/// Snapshot of a tokenization request, returned by
/// [`AlpacaTokenizationMock::tokenization_requests`].
pub struct MockTokenizationRequestSnapshot {
    pub request_id: String,
    pub request_type: TokenizationRequestType,
    pub symbol: String,
    pub quantity: Float,
    pub status: TokenizationStatus,
}

/// Owns tokenization state and registers endpoints on the broker's mock server.
///
/// All endpoints respond dynamically based on `TokenizationState`,
/// which updates as mint/redeem requests are created and polled.
pub struct AlpacaTokenizationMock {
    state: Arc<Mutex<TokenizationState>>,
    redemption_watcher: Option<JoinHandle<()>>,
    mint_executor: Option<JoinHandle<()>>,
}

impl Drop for AlpacaTokenizationMock {
    fn drop(&mut self) {
        if let Some(handle) = self.redemption_watcher.take() {
            handle.abort();
        }
        if let Some(handle) = self.mint_executor.take() {
            handle.abort();
        }
    }
}

impl AlpacaTokenizationMock {
    /// Creates the tokenization mock and registers mint + request polling
    /// endpoints on the given server.
    pub fn start(server: &MockServer) -> Self {
        let state = Arc::new(Mutex::new(TokenizationState {
            requests: Vec::new(),
            polls_until_complete: 2,
            redemption_outcome: RedemptionOutcome::Complete,
        }));

        register_mint_endpoint(server, &state);
        register_tokenization_requests_with_filter_endpoint(server, &state);

        Self {
            state,
            redemption_watcher: None,
            mint_executor: None,
        }
    }

    /// Starts a background task that monitors the Base chain for ERC-20
    /// Transfer events to the `redemption_wallet` address. When detected,
    /// adds a redemption tokenization request to the mock state so the
    /// bot's `poll_for_redemption` call finds a matching entry.
    ///
    /// `order_owner` is the bot's wallet address, used as the
    /// `wallet_address` on created requests (matching real Alpaca API
    /// behavior where the field identifies the account holder, not the
    /// transfer destination).
    ///
    /// `token_symbols` maps token contract address -> underlying symbol
    /// (e.g. vault_addr -> "AAPL"). The Transfer event's `value` field
    /// (U256 with 18 decimals) is decoded to determine the quantity.
    pub async fn start_redemption_watcher<P: Provider + Clone + Send + Sync + 'static>(
        &mut self,
        provider: P,
        redemption_wallet: Address,
        order_owner: Address,
        token_symbols: HashMap<Address, String>,
    ) -> anyhow::Result<()> {
        let state = self.state.clone();
        let start_block = provider.get_block_number().await?;

        let handle = tokio::spawn(async move {
            let mut last_block = start_block;

            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;

                let Ok(current) = provider.get_block_number().await else {
                    continue;
                };

                if current <= last_block {
                    continue;
                }

                let range_ok = scan_redemption_range(
                    &provider,
                    &state,
                    last_block + 1,
                    current,
                    redemption_wallet,
                    order_owner,
                    &token_symbols,
                )
                .await;

                if range_ok {
                    last_block = current;
                }
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
                request_type: request.request_type,
                symbol: request.underlying_symbol.clone(),
                quantity: request.quantity,
                status: request.status,
            })
            .collect()
    }

    /// Configures what happens when redemption requests reach their poll
    /// threshold. Defaults to [`RedemptionOutcome::Complete`].
    pub fn set_redemption_outcome(&self, outcome: RedemptionOutcome) {
        lock(&self.state).redemption_outcome = outcome;
    }

    /// Configures how many poll cycles a request stays pending before it can
    /// complete or reject.
    pub fn set_polls_until_complete(&self, polls_until_complete: usize) {
        lock(&self.state).polls_until_complete = polls_until_complete;
    }

    /// Injects a pending tokenization request with an arbitrary wallet
    /// address. Used to simulate requests from other conductors sharing
    /// the same Alpaca account.
    ///
    /// Injected requests stay pending indefinitely (`polls_until_complete =
    /// usize::MAX`): they model external requests that the test harness
    /// should observe but never fulfill. This avoids nonce collisions when
    /// the injected wallet matches the bot's signing key.
    pub fn inject_pending_request(
        &self,
        symbol: &str,
        quantity: Float,
        wallet: Address,
        request_type: TokenizationRequestType,
    ) {
        let mut state = lock(&self.state);

        state.requests.push(MockTokenizationRequest {
            tokenization_request_id: Uuid::new_v4().to_string(),
            issuer_request_id: String::new(),
            underlying_symbol: symbol.to_string(),
            quantity,
            wallet_address: wallet,
            status: TokenizationStatus::Pending,
            poll_count: 0,
            polls_until_complete: usize::MAX,
            request_type,
            tx_hash: String::new(),
            needs_mint_execution: false,
        });
    }

    /// Starts a background task that executes real ERC-20 transfers on the
    /// Anvil chain when mint requests reach the "completed" threshold.
    ///
    /// `token_addresses` maps underlying symbol -> token contract address.
    /// When a mint request has `needs_mint_execution`, this task transfers
    /// the requested quantity from the chain owner to the bot's wallet,
    /// records the real tx hash, and marks the request as completed.
    pub fn start_mint_executor<P: Provider + Clone + Send + Sync + 'static>(
        &mut self,
        provider: P,
        token_addresses: HashMap<String, Address>,
    ) {
        let state = self.state.clone();

        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;

                // Collect pending mints that need onchain execution.
                let pending_mints: Vec<(usize, Float, Address, Address)> = {
                    let mut guard = lock(&state);
                    let mut valid = Vec::new();

                    for (idx, req) in guard.requests.iter().enumerate() {
                        if !req.needs_mint_execution {
                            continue;
                        }

                        match token_addresses.get(&req.underlying_symbol) {
                            Some(token_addr) => {
                                valid.push((idx, req.quantity, req.wallet_address, *token_addr));
                            }
                            None => {
                                warn!(
                                    symbol = %req.underlying_symbol,
                                    wallet = %req.wallet_address,
                                    idx,
                                    "unknown symbol in mint request, marking as failed"
                                );
                            }
                        }
                    }

                    // Mark unknown-symbol requests as failed so they don't
                    // spin forever with needs_mint_execution = true.
                    for req in &mut guard.requests {
                        if req.needs_mint_execution
                            && !token_addresses.contains_key(&req.underlying_symbol)
                        {
                            req.needs_mint_execution = false;
                            req.status = TokenizationStatus::Failed;
                        }
                    }

                    drop(guard);
                    valid
                };

                for (idx, quantity, wallet, token_addr) in pending_mints {
                    // Convert decimal quantity to U256 with 18 decimals
                    let quantity_str = format_float_with_fallback(&quantity);
                    let Ok(amount_signed) = parse_units(&quantity_str, 18) else {
                        warn!(%wallet, ?quantity, %token_addr, "failed to parse quantity as 18-decimal units, skipping mint");
                        continue;
                    };
                    let amount: U256 = amount_signed.into();

                    // Execute real ERC-20 transfer on Anvil
                    let token = DeployableERC20::new(token_addr, &provider);
                    let Ok(pending_tx) = token.transfer(wallet, amount).send().await else {
                        warn!(%wallet, %amount, %token_addr, "ERC-20 transfer send failed, skipping mint");
                        continue;
                    };
                    let Ok(receipt) = pending_tx.get_receipt().await else {
                        warn!(%token_addr, "failed to get transfer receipt, skipping mint");
                        continue;
                    };

                    let real_tx_hash = format!("{:#x}", receipt.transaction_hash);

                    let mut guard = lock(&state);
                    if let Some(req) = guard.requests.get_mut(idx) {
                        req.tx_hash = real_tx_hash;
                        if receipt.status() {
                            req.status = TokenizationStatus::Completed;
                        } else {
                            warn!(%token_addr, "mint transfer reverted on-chain");
                            req.status = TokenizationStatus::Failed;
                        }
                        req.needs_mint_execution = false;
                    }
                }
            }
        });
        self.mint_executor = Some(handle);
    }
}

/// Scans a single block for ERC-20 Transfer events to `redemption_wallet`
/// and records them as redemption tokenization requests in mock state.
/// Returns `true` if the block was fully processed, `false` on RPC error.
async fn scan_block_for_redemptions<P: Provider>(
    provider: &P,
    state: &Mutex<TokenizationState>,
    block_num: u64,
    redemption_wallet: Address,
    order_owner: Address,
    token_symbols: &HashMap<Address, String>,
) -> bool {
    let Ok(Some(block)) = provider.get_block_by_number(block_num.into()).full().await else {
        return false;
    };

    for tx_hash in block.transactions.hashes() {
        let Ok(Some(receipt)) = provider.get_transaction_receipt(tx_hash).await else {
            return false;
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

            let Some(symbol) = token_symbols.get(&log.address()) else {
                continue;
            };

            // Decode quantity from Transfer event value (18 decimals)
            let value = U256::from_be_slice(log.data().data.as_ref());
            let Ok(shares) = FractionalShares::from_u256_18_decimals(value) else {
                warn!(
                    %value,
                    "Failed to decode redemption quantity from Transfer event value"
                );
                continue;
            };
            let quantity = shares.inner();

            let tx_hash_hex = format!("{tx_hash:#x}");

            let mut guard = state.lock().unwrap_or_else(PoisonError::into_inner);

            // Skip if already recorded (prevents duplicates on retry
            // after transient RPC failures mid-range).
            if guard
                .requests
                .iter()
                .any(|existing| existing.tx_hash == tx_hash_hex)
            {
                continue;
            }

            let polls_until_complete = guard.polls_until_complete;

            guard.requests.push(MockTokenizationRequest {
                tokenization_request_id: Uuid::new_v4().to_string(),
                issuer_request_id: String::new(),
                underlying_symbol: symbol.clone(),
                quantity,
                // Use the bot's wallet (order_owner), matching real Alpaca
                // API behavior where wallet_address identifies the account
                // holder, not the transfer destination.
                wallet_address: order_owner,
                status: TokenizationStatus::Pending,
                poll_count: 0,
                polls_until_complete,
                request_type: TokenizationRequestType::Redeem,
                tx_hash: tx_hash_hex,
                needs_mint_execution: false,
            });
        }
    }

    true
}

/// Scans a range of blocks for redemptions, returning true only if
/// every block in the range was fully processed.
async fn scan_redemption_range<P: Provider>(
    provider: &P,
    state: &Mutex<TokenizationState>,
    from: u64,
    to: u64,
    redemption_wallet: Address,
    order_owner: Address,
    token_symbols: &HashMap<Address, String>,
) -> bool {
    for block_num in from..=to {
        if !scan_block_for_redemptions(
            provider,
            state,
            block_num,
            redemption_wallet,
            order_owner,
            token_symbols,
        )
        .await
        {
            return false;
        }
    }

    true
}

/// Locks the mutex, recovering from poisoning (a previous holder panicked).
/// Safe for test mocks -- we still want to inspect state after panics.
fn lock(state: &Mutex<TokenizationState>) -> MutexGuard<'_, TokenizationState> {
    match state.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn register_mint_endpoint(server: &MockServer, state: &Arc<Mutex<TokenizationState>>) {
    let state = Arc::clone(state);

    server.mock(|when, then| {
        when.method(POST)
            .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/tokenization/mint"));
        then.respond_with(move |request: &HttpMockRequest| {
            let Ok(body) = serde_json::from_slice::<Value>(request.body().as_ref()) else {
                return json_response(
                    400,
                    &json!({"message": "mint request body is not valid JSON"}),
                );
            };

            let Some(underlying_symbol) = body["underlying_symbol"].as_str() else {
                return json_response(
                    400,
                    &json!({"message": "missing or non-string field: underlying_symbol"}),
                );
            };
            let underlying_symbol = underlying_symbol.to_string();

            let Some(quantity_str) = body["qty"].as_str() else {
                return json_response(400, &json!({"message": "missing or non-string field: qty"}));
            };
            let Ok(quantity) = Float::parse(quantity_str.to_string()) else {
                return json_response(
                    400,
                    &json!({"message": format!("invalid qty: {quantity_str}")}),
                );
            };

            let Some(wallet_address_str) = body["wallet_address"].as_str() else {
                return json_response(
                    400,
                    &json!({"message": "missing or non-string field: wallet_address"}),
                );
            };
            let Ok(wallet_address) = wallet_address_str.parse::<Address>() else {
                return json_response(
                    400,
                    &json!({"message": format!("invalid wallet_address: {wallet_address_str}")}),
                );
            };
            let issuer_request_id = body["issuer_request_id"]
                .as_str()
                .map_or_else(String::new, ToString::to_string);

            let tokenization_request_id = Uuid::new_v4().to_string();

            {
                let mut state = lock(&state);
                let polls_until_complete = state.polls_until_complete;

                state.requests.push(MockTokenizationRequest {
                    tokenization_request_id: tokenization_request_id.clone(),
                    issuer_request_id: issuer_request_id.clone(),
                    underlying_symbol: underlying_symbol.clone(),
                    quantity,
                    wallet_address,
                    status: TokenizationStatus::Pending,
                    poll_count: 0,
                    polls_until_complete,
                    request_type: TokenizationRequestType::Mint,
                    tx_hash: String::new(),
                    needs_mint_execution: false,
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
                    "qty": format_float_with_fallback(&quantity),
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
            let status_filter = query
                .split('&')
                .find_map(|param| param.strip_prefix("status="))
                .unwrap_or("");

            let requests: Vec<Value> = {
                let mut state = lock(&state);

                // Only mutate state (poll_count, status transitions) when no
                // status filter is present. Calls with status= are read-only
                // observers (e.g. list_pending_requests for inflight polling).
                if status_filter.is_empty() {
                    let redemption_outcome = state.redemption_outcome;

                    for req in &mut state.requests {
                        if req.status == TokenizationStatus::Pending && !req.needs_mint_execution {
                            req.poll_count += 1;
                            if req.poll_count >= req.polls_until_complete {
                                if req.request_type == TokenizationRequestType::Mint {
                                    // Mint requests need a real onchain transfer
                                    // before they can report as "completed".
                                    // The background mint executor will handle it.
                                    req.needs_mint_execution = true;
                                } else {
                                    // Redeem requests transition based on the
                                    // configured outcome.
                                    req.status = match redemption_outcome {
                                        RedemptionOutcome::Complete => {
                                            TokenizationStatus::Completed
                                        }
                                        RedemptionOutcome::Reject => TokenizationStatus::Rejected,
                                    };
                                }
                            }
                        }
                    }
                }

                state
                    .requests
                    .iter()
                    .filter(|req| {
                        (type_filter.is_empty() || req.request_type.to_string() == type_filter)
                            && (status_filter.is_empty() || req.status.to_string() == status_filter)
                    })
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
    let tx_hash = if request.status == TokenizationStatus::Completed && request.tx_hash.is_empty() {
        format!("0x{}", "ab".repeat(32))
    } else {
        request.tx_hash.clone()
    };

    json!({
        "tokenization_request_id": request.tokenization_request_id,
        "type": request.request_type.to_string(),
        "status": request.status.to_string(),
        "underlying_symbol": request.underlying_symbol,
        "token_symbol": format!("t{}", request.underlying_symbol),
        "qty": format_float_with_fallback(&request.quantity),
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
