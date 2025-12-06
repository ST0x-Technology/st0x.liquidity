# Implementation Plan: Alpaca Tokenization API Client (Issue #171)

## Overview

Build an Alpaca tokenization API client for mint and redemption operations. This
client is required by MintManager and RedemptionManager to drive the
TokenizedEquityMint and EquityRedemption aggregates.

API reference:
https://docs.alpaca.markets/v1.3/docs/tokenization-guide-for-authorized-participant

## API Endpoints

- `POST /v2/tokenization/mint` - Request mint (shares to tokens)
- `GET /v2/tokenization/requests` - List/poll tokenization requests (mint and
  redeem)

## Workflows

**Mint** (TokenizedEquityMint aggregate):

1. Call `POST /v2/tokenization/mint` with symbol, qty, wallet
2. Receive `tokenization_request_id` immediately
3. Poll `GET /v2/tokenization/requests` until status is `completed` or
   `rejected`
4. Issuer deposits tokens to wallet (detected via onchain events)

**Redemption** (EquityRedemption aggregate):

1. Send tokens to our redemption wallet (onchain tx - handled by
   RedemptionManager)
2. Poll `GET /v2/tokenization/requests?type=redeem` until Alpaca detects the
   transfer and returns `tokenization_request_id`
3. Poll until status is `completed` or `rejected`

**Note**: Redemption is initiated by the onchain token transfer to our issuer
wallet. The API client polls for Alpaca's detection and completion.

## Task 1. Mint Request Endpoint

Create module with working mint request.

### Subtasks

- [ ] Create `src/alpaca_tokenization.rs`
- [ ] Define core types:
  - `TokenizationRequestId(String)` newtype
  - `TokenizationRequestType` enum: `Mint`, `Redeem`
  - `TokenizationRequestStatus` enum: `Pending`, `Completed`, `Rejected`
  - `Issuer` enum: `St0x`
  - `TokenizationRequest` struct with API response fields
  - `MintRequest` struct for POST body
- [ ] Define `AlpacaTokenizationError` enum:
  - `Reqwest(reqwest::Error)`
  - `ApiError { status, message }`
  - `InsufficientPosition { symbol }`
  - `UnsupportedAccount`
  - `InvalidParameters { details }`
- [ ] Define `AlpacaTokenizationClient` struct with reqwest client and auth
- [ ] Implement
      `request_mint(&self, request: MintRequest) -> Result<TokenizationRequest>`:
  - POST to `/v2/tokenization/mint`
  - Handle 200, 403, 422 responses
- [ ] Add module to lib.rs
- [ ] Add test: successful mint request
- [ ] Add test: mint returns 403 insufficient position
- [ ] Add test: mint returns 422 invalid parameters

### Validation

- [ ] Run `cargo test -q`
- [ ] Run `cargo clippy -- -D clippy::all`
- [ ] Run `cargo fmt`

## Task 2. List and Get Requests

Add request listing with filtering by type/status.

### Subtasks

- [ ] Define `ListRequestsParams` struct:
  - `request_type: Option<TokenizationRequestType>`
  - `status: Option<TokenizationRequestStatus>`
  - `underlying_symbol: Option<Symbol>`
- [ ] Implement
      `list_requests(&self, params: ListRequestsParams) -> Result<Vec<TokenizationRequest>>`:
  - GET `/v2/tokenization/requests` with query params
- [ ] Add `RequestNotFound { request_id }` error variant
- [ ] Implement
      `get_request(&self, id: &TokenizationRequestId) -> Result<TokenizationRequest>`:
  - Filter list by ID
  - Return `RequestNotFound` if absent
- [ ] Add test: list all requests
- [ ] Add test: list filtered by type (mint only)
- [ ] Add test: list filtered by type (redeem only)
- [ ] Add test: get single request by ID
- [ ] Add test: get request not found

### Validation

- [ ] Run `cargo test -q`
- [ ] Run `cargo clippy -- -D clippy::all`
- [ ] Run `cargo fmt`

## Task 3. Send Tokens to Redemption Wallet

Implement onchain token transfer to initiate redemption.

### Subtasks

- [ ] Add `redemption_wallet: Address` field to `AlpacaTokenizationClient`
- [ ] Implement
      `send_tokens_for_redemption(&self, token: Address, amount: U256) -> Result<TxHash>`:
  - Transfer ERC20 tokens to redemption wallet
  - Return tx hash for tracking
- [ ] Add `RedemptionTransferFailed { reason }` error variant
- [ ] Add test: successful token transfer to redemption wallet
- [ ] Add test: transfer fails with insufficient balance

### Validation

- [ ] Run `cargo test -q`
- [ ] Run `cargo clippy -- -D clippy::all`
- [ ] Run `cargo fmt`

## Task 4. Redemption Detection

Add methods to detect when Alpaca recognizes a redemption transfer.

### Subtasks

- [ ] Implement
      `find_redemption_by_tx(&self, tx_hash: &TxHash) -> Result<Option<TokenizationRequest>>`:
  - List requests filtered by type=redeem
  - Find matching tx_hash field
  - Return None if not yet detected
- [ ] Add test: find redemption by tx hash
- [ ] Add test: redemption not yet detected returns None

### Validation

- [ ] Run `cargo test -q`
- [ ] Run `cargo clippy -- -D clippy::all`
- [ ] Run `cargo fmt`

## Task 5. Status Polling

Add polling for terminal states with timeout.

### Subtasks

- [ ] Define `PollingConfig` struct:
  - `interval: Duration`
  - `timeout: Duration`
- [ ] Add `PollTimeout { elapsed }` error variant
- [ ] Implement
      `poll_until_terminal(&self, id: &TokenizationRequestId, config: &PollingConfig) -> Result<TokenizationRequest>`:
  - Use `tokio::time::interval()` with `MissedTickBehavior::Skip`
  - Poll `get_request()` each tick
  - Return when status is `Completed` or `Rejected`
  - Return `PollTimeout` if exceeded
- [ ] Implement
      `poll_for_redemption_detection(&self, tx_hash: &TxHash, config: &PollingConfig) -> Result<TokenizationRequest>`:
  - Poll `find_redemption_by_tx()` each tick
  - Return when request appears (Alpaca detected the transfer)
  - Return `PollTimeout` if exceeded
- [ ] Add test: poll mint until completed
- [ ] Add test: poll mint until rejected
- [ ] Add test: poll redemption detection success
- [ ] Add test: poll timeout

### Validation

- [ ] Run `cargo test -q`
- [ ] Run `cargo clippy -- -D clippy::all`
- [ ] Run `cargo fmt`

## Task 6. Service Facade

Create high-level service with default config.

### Subtasks

- [ ] Define `AlpacaTokenizationService` struct:
  - `client: Arc<AlpacaTokenizationClient>`
  - `polling_config: PollingConfig`
- [ ] Implement constructor with default polling config
- [ ] Implement public methods:
  - `request_mint(symbol, qty, wallet) -> Result<TokenizationRequest>`
  - `get_request_status(id) -> Result<TokenizationRequest>`
  - `poll_mint_until_complete(id) -> Result<TokenizationRequest>`
  - `send_for_redemption(token, amount) -> Result<TxHash>`
  - `poll_for_redemption(tx_hash) -> Result<TokenizationRequest>`
  - `poll_redemption_until_complete(id) -> Result<TokenizationRequest>`
- [ ] Re-export public types from module
- [ ] Add integration test: mint -> poll -> completed
- [ ] Add integration test: redemption detected -> poll -> completed

### Validation

- [ ] Run `cargo test -q`
- [ ] Run `cargo clippy -- -D clippy::all`
- [ ] Run `cargo fmt`

## Task 7. Final Validation and Cleanup

### Subtasks

- [ ] Run full test suite
- [ ] Run clippy with all warnings denied
- [ ] Run formatter
- [ ] Review against AGENTS.md guidelines:
  - [ ] Package by feature (single alpaca_tokenization.rs file)
  - [ ] No `#[allow(clippy::*)]` attributes
  - [ ] Proper error handling
  - [ ] Visibility levels are restrictive
- [ ] Delete PLAN.md before creating PR

### Validation

- [ ] All tests pass
- [ ] Zero clippy warnings
- [ ] Code adheres to project standards

## Configuration Requirements

- **Redemption wallet address**: Our own issuer wallet address where users send
  tokens for redemption. Configured in environment/config.

## Success Criteria

1. Client can request mint via `POST /v2/tokenization/mint`
2. Client can list/filter requests via `GET /v2/tokenization/requests`
3. Client can detect when Alpaca recognizes redemption transfers (by tx_hash)
4. Client can poll until terminal state (Completed/Rejected) or timeout
5. Authentication uses Alpaca API key/secret headers
6. Error types cover API responses (200, 403, 422)
7. Tests cover mint and redemption polling flows
