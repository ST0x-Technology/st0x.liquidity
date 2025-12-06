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

- [x] Create `src/alpaca_tokenization.rs`
- [x] Define core types:
  - Reuse `TokenizationRequestId` from `tokenized_equity_mint` module
  - Reuse `IssuerRequestId` from `tokenized_equity_mint` module
  - Reuse `Network` from `alpaca_wallet` module
  - Reuse `TokenizedEquitySymbol` from `onchain::io` module for token_symbol
  - `TokenizationRequestType` enum: `Mint`, `Redeem`
  - `TokenizationRequestStatus` enum: `Pending`, `Completed`, `Rejected`
  - `Issuer(String)` newtype (API accepts string values like "st0x")
  - `TokenizationRequest` struct with API response fields
  - `MintRequest` struct for POST body
- [x] Define `AlpacaTokenizationError` enum:
  - `Reqwest(reqwest::Error)`
  - `ApiError { status, message }`
  - `InsufficientPosition { symbol }`
  - `UnsupportedAccount`
  - `InvalidParameters { details }`
- [x] Define `AlpacaTokenizationClient` struct with reqwest client and auth
- [x] Implement
      `request_mint(&self, request: MintRequest) -> Result<TokenizationRequest>`:
  - POST to `/v2/tokenization/mint`
  - Handle 200, 403, 422 responses
- [x] Add module to lib.rs
- [x] Add test: successful mint request
- [x] Add test: mint returns 403 insufficient position
- [x] Add test: mint returns 422 invalid parameters

### Validation

- [x] Run `cargo test -q`
- [x] Run `cargo clippy -- -D clippy::all`
- [x] Run `cargo fmt`

### Changes Made

- Created `src/alpaca_tokenization.rs` with Alpaca tokenization API client
- Reused existing types: `TokenizationRequestId`, `IssuerRequestId` from
  `tokenized_equity_mint`, `Network` from `alpaca_wallet`,
  `TokenizedEquitySymbol` from `onchain::io`
- Added custom deserializer for `TokenizedEquitySymbol` to handle optional field
- All types are private by default, visibility will be increased as needed
- Uses `r#type` raw identifier syntax for the `type` field
- Uses `serde(rename)` for field name mappings (qty -> quantity, wallet_address
  -> wallet)
- Authentication uses `APCA-API-KEY-ID` and `APCA-API-SECRET-KEY` headers per
  Alpaca docs

## Task 2. List and Get Requests

Add request listing with filtering by type/status.

### Subtasks

- [x] Define `ListRequestsParams` struct:
  - `request_type: Option<TokenizationRequestType>`
  - `status: Option<TokenizationRequestStatus>`
  - `underlying_symbol: Option<Symbol>`
- [x] Implement
      `list_requests(&self, params: ListRequestsParams) -> Result<Vec<TokenizationRequest>>`:
  - GET `/v2/tokenization/requests` with query params
- [x] Add `RequestNotFound { id }` error variant
- [x] Implement
      `get_request(&self, id: &TokenizationRequestId) -> Result<TokenizationRequest>`:
  - Filter list by ID
  - Return `RequestNotFound` if absent
- [x] Add test: list all requests
- [x] Add test: list filtered by type (mint only)
- [x] Add test: list filtered by type (redeem only)
- [x] Add test: get single request by ID
- [x] Add test: get request not found

### Validation

- [x] Run `cargo test -q`
- [x] Run `cargo clippy -- -D clippy::all`
- [x] Run `cargo fmt`

### Changes Made

- Added `ListRequestsParams` struct with optional filters for type, status, and
  symbol
- Implemented `list_requests` method with query parameter building
- Added `RequestNotFound { id }` error variant
- Added `Display` impl for `TokenizationRequestId` in tokenized_equity_mint.rs
- Implemented `get_request` which fetches all requests and filters by ID
- Added helper function `sample_tokenization_request_json` for tests
- All 8 tests pass

## Task 3. Send Tokens to Redemption Wallet

Implement onchain token transfer to initiate redemption.

### Subtasks

- [x] Add `redemption_wallet: Address` field to `AlpacaTokenizationClient`
- [x] Implement
      `send_tokens_for_redemption(&self, token: Address, amount: U256) -> Result<TxHash>`:
  - Transfer ERC20 tokens to redemption wallet
  - Return tx hash for tracking
- [x] Add `RedemptionTransferFailed` and `Transaction` error variants (using
      `#[from]` for automatic error wrapping)
- [x] Add test: successful token transfer to redemption wallet
- [x] Add test: transfer fails with insufficient balance

### Validation

- [x] Run `cargo test -q`
- [x] Run `cargo clippy -- -D clippy::all`
- [x] Run `cargo fmt`

### Changes Made

- Made `AlpacaTokenizationClient` generic over `P: Provider + Clone` and
  `S: Signer + Clone + Sync` to support blockchain operations
- Renamed `client` field to `http_client` and added `provider`, `signer`, and
  `redemption_wallet` fields
- Added `RedemptionTransferFailed(#[from] alloy::contract::Error)` error variant
- Added `Transaction(#[from] alloy::providers::PendingTransactionError)` error
  variant
- Implemented `send_tokens_for_redemption` using `IERC20::transfer`
- Updated all tests to use Anvil for provider/signer setup
- All 10 tests pass

## Task 4. Redemption Detection

Add methods to detect when Alpaca recognizes a redemption transfer.

### Subtasks

- [x] Implement
      `find_redemption_by_tx(&self, tx_hash: &TxHash) -> Result<Option<TokenizationRequest>>`:
  - List requests filtered by type=redeem
  - Find matching tx_hash field
  - Return None if not yet detected
- [x] Add test: find redemption by tx hash
- [x] Add test: redemption not yet detected returns None

### Validation

- [x] Run `cargo test -q`
- [x] Run `cargo clippy -- -D clippy::all`
- [x] Run `cargo fmt`

### Changes Made

- Implemented `find_redemption_by_tx` which lists redeem requests and finds one
  matching the given tx_hash
- Returns `Option<TokenizationRequest>` - `None` if Alpaca hasn't detected the
  transfer yet
- Added helper `sample_redemption_request_json_with_tx` taking `TxHash` directly
- Used `fixed_bytes!` macro with `0x` prefix for test tx hashes
- All 12 tests pass

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
