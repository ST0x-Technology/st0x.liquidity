# Implementation Plan: Alpaca Crypto Wallet Service (#132)

## Overview

Build service layer for Alpaca crypto wallet withdrawals and deposits with
status polling. This establishes a new service abstraction pattern for external
integrations.

## Context

- **Issue**: #132 - Implement Alpaca crypto wallet service
- **Phase**: Phase 2 (external service integrations)
- **Dependencies**: Event sourcing infrastructure exists but isn't fully
  integrated yet (phase 1 in progress). However, service layer can be
  implemented independently.
- **Related Issues**: #133 (Circle CCTP), #134 (Rain OrderBook) - all follow
  same service pattern

## Architecture Decisions

### Service Pattern Design

Unlike the `Broker` trait which uses generics and compile-time selection, the
service layer will use trait objects for runtime flexibility. This is because:

1. Services are external integrations consumed by aggregates, not core trading
   abstractions
2. Multiple services may be used together (Alpaca wallet + Circle CCTP + Rain
   vault)
3. Services don't need the zero-cost abstraction guarantees that brokers require

### Module Organization

Following the broker crate's encapsulation principles:

```
src/
  services/
    mod.rs                    # Service trait definition + re-exports
    alpaca_wallet/
      mod.rs                  # Public API: AlpacaWalletService + config types
      client.rs               # Private: HTTP client wrapper
      transfer.rs             # Private: transfer types and operations
      status.rs               # Private: status polling logic
      whitelist.rs            # Private: address whitelisting
```

### HTTP Client Strategy

The `apca` crate doesn't support crypto wallet operations, so we'll implement
HTTP calls directly:

- Use `reqwest` for HTTP (already in dependencies)
- Reuse `AlpacaAuthEnv` for authentication
- Add account ID retrieval functionality
- Handle Alpaca-specific headers and authentication

## Tasks

### Task 1. Add Account ID Support to Alpaca Broker

Extend the Alpaca broker crate to support account ID retrieval needed by wallet
API.

**Vertical Slice**: Complete working account ID retrieval with tests passing.

**Implementation**:

- [ ] Add `get_account_id()` method to `AlpacaClient` in
      `crates/broker/src/alpaca/auth.rs`
- [ ] Call `/v2/account` endpoint and extract account ID field
- [ ] Cache account ID in `AlpacaClient` after first retrieval
- [ ] Add comprehensive tests with `httpmock`:
  - Successful account ID retrieval from mocked endpoint
  - Caching behavior (second call doesn't hit API)
  - Error handling when endpoint fails
- [ ] Run `cargo test -p st0x-broker -q` to verify tests pass
- [ ] Run `cargo clippy -p st0x-broker -- -D clippy::all` to verify no linting
      errors
- [ ] Run `cargo fmt` to format code

**Design Reasoning**:

- Account ID is required by wallet API endpoints
- Caching prevents repeated API calls
- Keep in broker crate since it's Alpaca-specific infrastructure
- Complete vertical slice ensures broker changes work before moving to services

**Files Modified**:

- `crates/broker/src/alpaca/auth.rs`

**Task Completion Criteria**: ✅ Tests pass in broker crate ✅ No clippy errors
✅ Code formatted

### Task 2. Implement Deposit Address Retrieval (Vertical Slice #1)

Build first complete vertical slice: deposit address retrieval with full
testing.

**Vertical Slice**: Complete working deposit address feature from types to
service with tests passing.

**Implementation**:

- [ ] Create `src/services/mod.rs` with module declaration for `alpaca_wallet`
- [ ] Add `mod services;` to `src/lib.rs` (with `#[allow(dead_code)]` for now)
- [ ] Create `src/services/alpaca_wallet/mod.rs` with private submodules
- [ ] Create `src/services/alpaca_wallet/client.rs`:
  - Define `AlpacaWalletClient` struct with `reqwest::Client`, account ID, base
    URL, credentials
  - Define minimal `AlpacaWalletError` enum: `HttpError`, `ApiError`,
    `InvalidResponse`
  - Implement `new()` using `AlpacaClient::get_account_id()` from Task 1
  - Implement helper for HTTP GET with auth headers (APCA-API-KEY-ID,
    APCA-API-SECRET-KEY)
  - Add comprehensive tests with `httpmock`:
    - Client construction with valid config
    - Authentication headers present in requests
    - Error parsing from API error responses
- [ ] Create `src/services/alpaca_wallet/transfer.rs`:
  - Define `DepositAddress` struct: `address: String`, `asset: String`,
    `network: String`
  - Implement `get_deposit_address()` function:
    - `GET /v1/crypto/funding_wallets?asset={asset}&network={network}`
    - Parse JSON response to extract address
  - Add comprehensive tests with `httpmock`:
    - Successful deposit address retrieval for USDC on Ethereum
    - Invalid asset handling
    - Invalid network handling
    - API error response handling
    - Malformed JSON response handling
- [ ] Update `src/services/alpaca_wallet/mod.rs`:
  - Re-export `AlpacaWalletClient`, `AlpacaWalletError`
  - Keep submodules private
- [ ] Update `src/services/mod.rs`:
  - Re-export `alpaca_wallet` module
- [ ] Run `cargo test -q` to verify all tests pass
- [ ] Run `cargo clippy --all-targets --all-features -- -D clippy::all`
- [ ] Run `cargo fmt`

**Design Reasoning**:

- Deposit address is simplest operation (no state changes)
- Establishes HTTP client pattern for other operations
- Complete vertical slice proves architecture before adding complexity
- Tests ensure financial operations work correctly

**Files Created**:

- `src/services/mod.rs`
- `src/services/alpaca_wallet/mod.rs`
- `src/services/alpaca_wallet/client.rs`
- `src/services/alpaca_wallet/transfer.rs`

**Files Modified**:

- `src/lib.rs`

**Task Completion Criteria**: ✅ All tests pass ✅ No clippy errors ✅ Can
retrieve deposit address with tests proving it works

### Task 3. Implement Withdrawal Initiation (Vertical Slice #2)

Build second vertical slice: withdrawal initiation with types, validation, and
testing.

**Vertical Slice**: Complete working withdrawal feature with full testing.

**Implementation**:

- [ ] Update `src/services/alpaca_wallet/transfer.rs`:
  - Define `TransferId` newtype wrapping `uuid::Uuid`
  - Define `TransferDirection` enum: `Incoming`, `Outgoing`
  - Define `TransferStatus` enum: `Pending`, `Processing`, `Complete`, `Failed`
  - Define `Transfer` struct with all fields (id, direction, amount, asset,
    addresses, status, etc.)
  - Add validation helper for amounts (must be positive, non-zero)
  - Implement `initiate_withdrawal()` function:
    - `POST /v1/accounts/{account_id}/wallets/transfers`
    - Request body: `{ "address": "...", "amount": "...", "asset": "..." }`
    - Parse response to extract transfer ID
  - Add comprehensive tests with `httpmock`:
    - Successful withdrawal initiation
    - Amount validation (reject zero, negative, invalid decimals)
    - Invalid asset handling
    - Invalid address format handling
    - API error responses
- [ ] Update `src/services/alpaca_wallet/client.rs`:
  - Add helper for HTTP POST with JSON body and auth headers
  - Update `AlpacaWalletError` with new variants: `InvalidAmount`,
    `InvalidAsset`
  - Add tests for POST helper method
- [ ] Add `uuid` to dependencies in `Cargo.toml` if not present
- [ ] Add `rust_decimal` usage throughout (already in dependencies)
- [ ] Run `cargo test -q` to verify all tests pass
- [ ] Run `cargo clippy --all-targets --all-features -- -D clippy::all`
- [ ] Run `cargo fmt`

**Design Reasoning**:

- Withdrawal is core functionality needed for rebalancing
- Validation prevents financial errors
- Complete vertical slice with tests ensures correctness
- Build on established HTTP client pattern from Task 2

**Files Modified**:

- `src/services/alpaca_wallet/transfer.rs`
- `src/services/alpaca_wallet/client.rs`
- `Cargo.toml` (if uuid needed)

**Task Completion Criteria**: ✅ All tests pass ✅ No clippy errors ✅ Can
initiate withdrawals with full validation

### Task 4. Implement Transfer Status Query (Vertical Slice #3)

Build third vertical slice: transfer status querying with full testing.

**Vertical Slice**: Complete working transfer status feature with testing.

**Implementation**:

- [ ] Update `src/services/alpaca_wallet/transfer.rs`:
  - Implement `get_transfer_status()` function:
    - `GET /v1/accounts/{account_id}/wallets/transfers?transfer_id={id}`
    - Parse response and map to `TransferStatus` enum
    - Handle API returning different status strings
  - Add comprehensive tests with `httpmock`:
    - Query status for transfers in each state (Pending, Processing, Complete,
      Failed)
    - Invalid transfer ID handling
    - Transfer not found handling
    - Malformed API responses
- [ ] Run `cargo test -q` to verify all tests pass
- [ ] Run `cargo clippy --all-targets --all-features -- -D clippy::all`
- [ ] Run `cargo fmt`

**Design Reasoning**:

- Status querying needed before polling can be implemented
- Complete vertical slice with tests
- Simple addition to existing transfer module

**Files Modified**:

- `src/services/alpaca_wallet/transfer.rs`

**Task Completion Criteria**: ✅ All tests pass ✅ No clippy errors ✅ Can query
transfer status

### Task 5. Implement Status Polling (Vertical Slice #4)

Build fourth vertical slice: status polling with configurable behavior and
testing.

**Vertical Slice**: Complete working polling mechanism with full testing.

**Implementation**:

- [ ] Create `src/services/alpaca_wallet/status.rs`:
  - Define `PollingConfig` struct:
    - `interval: Duration` (default: 10 seconds)
    - `timeout: Duration` (default: 30 minutes)
    - `max_retries: u32` (default: 3)
  - Implement `poll_transfer_status()` function:
    - Loop until status is `Complete` or `Failed`
    - Sleep between polls using `tokio::time::sleep(interval)`
    - Return `Err(TransferTimeout)` if timeout exceeded
    - Retry on transient errors up to `max_retries`
    - Log status transitions using `tracing::info!`
  - Add exponential backoff helper for retries
  - Add comprehensive tests:
    - Successful polling (Processing → Complete) with multiple status checks
    - Failed transfer polling (Processing → Failed)
    - Timeout during polling (mock slow responses)
    - Retry on transient 5xx errors
    - Invalid status regression detection
- [ ] Update `src/services/alpaca_wallet/client.rs`:
  - Add `AlpacaWalletError::TransferTimeout` variant
- [ ] Update `src/services/alpaca_wallet/mod.rs`:
  - Add `mod status;` declaration
- [ ] Run `cargo test -q` to verify all tests pass
- [ ] Run `cargo clippy --all-targets --all-features -- -D clippy::all`
- [ ] Run `cargo fmt`

**Design Reasoning**:

- Polling is critical for async transfer lifecycle
- Configurable behavior prevents hardcoded values
- Comprehensive testing ensures reliability
- Separate module keeps concerns isolated

**Files Created**:

- `src/services/alpaca_wallet/status.rs`

**Files Modified**:

- `src/services/alpaca_wallet/mod.rs`
- `src/services/alpaca_wallet/client.rs`

**Task Completion Criteria**: ✅ All tests pass ✅ No clippy errors ✅ Polling
works with configurable timeouts

### Task 6. Implement Address Whitelisting (Vertical Slice #5)

Build fifth vertical slice: address whitelisting with full testing.

**Vertical Slice**: Complete working whitelisting feature with testing.

**Implementation**:

- [ ] Create `src/services/alpaca_wallet/whitelist.rs`:
  - Define `WhitelistEntry` struct with all fields
  - Define `WhitelistStatus` enum: `Pending`, `Approved`, `Rejected`
  - Define `APPROVAL_WAIT_TIME` constant (24 hours)
  - Implement `whitelist_address()` function:
    - `POST /v1/accounts/{account_id}/wallets/whitelists`
    - Request body: `{ "address": "...", "asset": "..." }`
    - Parse response
  - Implement `get_whitelisted_addresses()` function:
    - `GET /v1/accounts/{account_id}/wallets/whitelists`
    - Parse list response
  - Implement `is_address_whitelisted_and_approved()` helper
  - Add comprehensive tests with `httpmock`:
    - Successful address whitelisting
    - Getting whitelisted addresses list
    - Checking if address is approved (status = Approved)
    - Handling pending status
    - Handling rejected status
    - Duplicate address handling
- [ ] Update `src/services/alpaca_wallet/client.rs`:
  - Add `AlpacaWalletError::AddressNotWhitelisted` variant
- [ ] Update `src/services/alpaca_wallet/mod.rs`:
  - Add `mod whitelist;` declaration
- [ ] Run `cargo test -q` to verify all tests pass
- [ ] Run `cargo clippy --all-targets --all-features -- -D clippy::all`
- [ ] Run `cargo fmt`

**Design Reasoning**:

- Whitelisting is security requirement from Alpaca
- Separate module keeps concerns isolated
- Complete testing ensures withdrawal safety
- Check method prevents withdrawal failures

**Files Created**:

- `src/services/alpaca_wallet/whitelist.rs`

**Files Modified**:

- `src/services/alpaca_wallet/mod.rs`
- `src/services/alpaca_wallet/client.rs`

**Task Completion Criteria**: ✅ All tests pass ✅ No clippy errors ✅ Can
whitelist addresses and check approval status

### Task 7. Implement AlpacaWalletService Facade

Assemble all components into cohesive service with trait abstraction.

**Vertical Slice**: Complete service implementation with full integration tests.

**Implementation**:

- [ ] Update `src/services/mod.rs`:
  - Define `TransferStatus` enum (move from transfer.rs, re-export)
  - Define `TransferId` newtype (move from transfer.rs, re-export)
  - Define simple `WalletService` trait with core methods:
    - `async fn get_deposit_address(&self, asset: &str, network: &str) -> Result<String, Self::Error>`
    - `async fn initiate_withdrawal(&self, address: &str, amount: Decimal, asset: &str) -> Result<TransferId, Self::Error>`
    - `async fn get_transfer_status(&self, transfer_id: &TransferId) -> Result<TransferStatus, Self::Error>`
    - `async fn poll_transfer_until_complete(&self, transfer_id: &TransferId, timeout: Duration) -> Result<TransferStatus, Self::Error>`
  - Add module-level docs explaining service pattern
- [ ] Update `src/services/alpaca_wallet/mod.rs`:
  - Define `AlpacaWalletService` struct:
    - `client: Arc<AlpacaWalletClient>`
    - `polling_config: PollingConfig`
  - Define `AlpacaWalletConfig` struct:
    - `auth_env: AlpacaAuthEnv`
    - `polling_config: Option<PollingConfig>`
  - Implement `WalletService` trait for `AlpacaWalletService`:
    - Delegate each method to appropriate module function
    - `initiate_withdrawal()` checks whitelist before delegating
  - Implement `new()` async constructor:
    - Build `AlpacaWalletClient` from config
    - Set up polling config with defaults
    - Verify account access by calling get_account_id()
  - Add convenience methods:
    - `whitelist_address()` - expose whitelisting
    - `get_whitelisted_addresses()` - list whitelisted addresses
  - Add comprehensive integration tests:
    - Full withdrawal flow: whitelist → initiate → poll → complete
    - Full deposit flow: get address
    - Error: unauthorized credentials
    - Error: invalid amount
    - Error: address not whitelisted
    - Error: timeout during polling
  - Add module-level docs with usage examples
  - Re-export only public API types
- [ ] Run `cargo test -q` to verify all tests pass (including integration tests)
- [ ] Run `cargo clippy --all-targets --all-features -- -D clippy::all`
- [ ] Run `cargo fmt`

**Design Reasoning**:

- Service facade provides clean API for consumers
- Trait abstraction allows future alternative implementations
- Integration tests verify full workflows work end-to-end
- Minimal public API prevents leaky abstractions

**Files Modified**:

- `src/services/mod.rs`
- `src/services/alpaca_wallet/mod.rs`
- `src/services/alpaca_wallet/transfer.rs` (move types to mod.rs)

**Task Completion Criteria**: ✅ All tests pass (including integration tests) ✅
No clippy errors ✅ Service provides complete working API

### Task 8. Add Documentation and Final Polish

Add comprehensive documentation and verify everything is production-ready.

**Vertical Slice**: Fully documented, production-ready service.

**Implementation**:

- [ ] Add doc comments to all public types and methods in:
  - `src/services/mod.rs` (traits and types)
  - `src/services/alpaca_wallet/mod.rs` (service and config)
  - `src/services/alpaca_wallet/client.rs` (error type)
- [ ] Add module-level documentation:
  - `src/services/mod.rs`: Service pattern, when to use services, related issues
  - `src/services/alpaca_wallet/mod.rs`: API overview, authentication,
    whitelisting, transfer lifecycle
- [ ] Add usage examples in doc comments:
  - Getting deposit address example
  - Withdrawal flow example
  - Polling example
- [ ] Document all error variants with clear descriptions
- [ ] Verify Debug implementations redact secrets (API keys)
- [ ] Remove `#[allow(dead_code)]` from `src/lib.rs` services module
- [ ] Run full test suite: `cargo test -q`
- [ ] Run clippy with all checks:
      `cargo clippy --all-targets --all-features -- -D clippy::all`
- [ ] Run formatter: `cargo fmt`
- [ ] Verify no unused imports or dead code warnings
- [ ] Review all public exports for leaky abstractions

**Design Reasoning**:

- Documentation eases maintenance and onboarding
- Examples prevent misuse
- Final checks ensure production quality
- Clean public API review

**Files Modified** (documentation added):

- `src/services/mod.rs`
- `src/services/alpaca_wallet/mod.rs`
- `src/services/alpaca_wallet/client.rs`
- `src/lib.rs`

**Task Completion Criteria**: ✅ All code documented ✅ All tests pass ✅ No
clippy errors ✅ No dead code warnings ✅ Clean public API

## Dependencies Between Tasks

```
Task 1: Account ID (broker crate)
   ↓
Task 2: Deposit Address (HTTP client + transfer basics)
   ↓
Task 3: Withdrawal Initiation (types + validation)
   ↓
Task 4: Transfer Status Query
   ↓
Task 5: Status Polling
   ↓
Task 6: Address Whitelisting
   ↓
Task 7: Service Facade (trait + integration)
   ↓
Task 8: Documentation + Final Polish
```

**Each task is a complete vertical slice with tests passing at the end.**

## Non-Goals

The following are explicitly **out of scope** for this issue:

- Integration with event sourcing aggregates (covered in later issues)
- Rebalancing orchestration (issues #135-139)
- Database migrations for wallet state (if needed, will be separate issue)
- CLI commands for wallet operations (can be added later if needed)
- Support for non-USDC assets (focus on USDC only initially)
- SSE event subscription for transfers (use polling for MVP)

## Success Criteria

✅ Service trait abstraction established for future services ✅
AlpacaWalletService implements WalletService trait ✅ Can retrieve USDC deposit
addresses (Ethereum network) ✅ Can whitelist withdrawal addresses ✅ Can
initiate USDC withdrawals to whitelisted addresses ✅ Status polling correctly
handles all transfer states ✅ Comprehensive tests with mocked Alpaca API
responses ✅ All tests pass (`cargo test -q`) ✅ No clippy warnings
(`cargo clippy`) ✅ Code formatted (`cargo fmt`)

## Testing Strategy

### Unit Tests

- Each module has comprehensive unit tests
- Mock HTTP responses using `httpmock`
- Test all error paths
- Test validation logic

### Integration Tests

- Full workflows (whitelist → withdraw → poll)
- Error scenarios (timeout, unauthorized, etc.)
- Edge cases (invalid amounts, etc.)

### No Live API Testing

- All tests use mocks
- No real withdrawals or deposits
- Testnet support can be added later if needed

## Risk Mitigation

### Financial Integrity

- Use `Decimal` for all amounts (never `f64`)
- Validate amounts are positive and non-zero
- Comprehensive error handling
- Test error paths thoroughly

### Security

- Never log API keys or secrets
- Redact sensitive data in Debug implementations
- Follow broker crate's security patterns

### API Changes

- Document API version in code
- Log unexpected response formats
- Graceful degradation where possible

## Follow-up Work

After this issue is complete:

1. Issue #133: Circle CCTP bridge service (follows same pattern)
2. Issue #134: Rain OrderBook vault service (follows same pattern)
3. Integration with rebalancing aggregates (issues #135-139)
4. Optional: CLI commands for manual wallet operations
5. Optional: SSE event subscription instead of polling
