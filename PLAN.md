# Implementation Plan: Alpaca Crypto Wallet Service (#132)

## Overview

Build service layer for Alpaca crypto wallet withdrawals and deposits with
status polling. This establishes a new service abstraction pattern for external
integrations that will be used by Circle CCTP (#133) and Rain OrderBook (#134)
services.

## Context

The event sourcing infrastructure from phase 1 is still being integrated on
another worktree, but the service layer can be implemented independently since
it will be consumed by higher-level aggregates later.

## Architecture Decisions

**Service Pattern**: Unlike the `Broker` trait which uses generics and
compile-time selection, the service layer will use trait objects for runtime
flexibility. This is because multiple services will be used together (Alpaca
wallet + Circle CCTP + Rain vault) and they don't require the zero-cost
abstraction guarantees that brokers need.

**HTTP Client**: The `apca` crate doesn't support crypto wallet operations, so
we'll implement HTTP calls directly using `reqwest`. We'll reuse `AlpacaAuthEnv`
for authentication and add account ID retrieval functionality.

**Module Organization**: Following the broker crate's encapsulation principles
with private implementation modules and minimal public API surface.

## Task 1. Add Account ID Support to Alpaca Broker

**Design Reasoning**: Account ID is required by wallet API endpoints. We need to
extend the Alpaca broker crate to retrieve and cache it. This task is isolated
to the broker crate to ensure changes work before moving to services.

- [x] Add `get_account_id()` method to `AlpacaClient` in
      `crates/broker/src/alpaca/auth.rs`
- [x] Call `/v2/account` endpoint and extract account ID field
- [x] Cache account ID in `AlpacaClient` after first retrieval to prevent
      repeated API calls
- [x] Add tests with `httpmock`: successful retrieval, caching behavior, error
      handling
- [x] Run `cargo test -p st0x-broker -q`,
      `cargo clippy -p st0x-broker -- -D clippy::all`, `cargo fmt`

## Task 2. Implement Deposit Address Retrieval

**Design Reasoning**: Deposit address retrieval is the simplest operation (no
state changes), making it ideal for establishing the HTTP client pattern and
service architecture. This proves the design before adding complexity.

- [x] Create `src/alpaca_wallet/mod.rs` with private submodules
- [x] Add `mod alpaca_wallet;` to `src/lib.rs`
- [x] Create `src/alpaca_wallet/client.rs`:
  - [x] Define `AlpacaWalletClient` struct with `reqwest::Client`, account ID,
        base URL, credentials
  - [x] Define minimal `AlpacaWalletError` enum: `HttpError`, `ApiError`,
        `InvalidResponse`
  - [x] Implement `new()` using `AlpacaClient::get_account_id()` from Task 1
  - [x] Implement helper for HTTP GET with auth headers (APCA-API-KEY-ID,
        APCA-API-SECRET-KEY)
  - [x] Add tests: client construction, auth headers, error parsing
- [x] Create `src/alpaca_wallet/transfer.rs`:
  - [x] Define `DepositAddress` struct: `address: String`, `asset: String`,
        `network: String`
  - [x] Implement `get_deposit_address()` function calling
        `GET /v1/crypto/funding_wallets?asset={asset}&network={network}`
  - [x] Add tests: successful retrieval for USDC/Ethereum, invalid
        asset/network, API errors, malformed JSON
- [x] Update `src/alpaca_wallet/mod.rs` to re-export `AlpacaWalletClient`,
      `AlpacaWalletError` (keep submodules private)
- [x] Run `cargo test -q`,
      `cargo clippy --all-targets --all-features -- -D clippy::all`, `cargo fmt`

## Task 3. Implement Withdrawal Initiation

**Design Reasoning**: Withdrawal is core functionality needed for rebalancing.
This adds types, validation, and POST support building on the HTTP client
pattern from Task 2. Financial validation prevents errors.

- [x] Update `src/alpaca_wallet/transfer.rs`:
  - [x] Define `TransferId` newtype wrapping `uuid::Uuid`
  - [x] Define `TransferDirection` enum: `Incoming`, `Outgoing`
  - [x] Define `TransferStatus` enum: `Pending`, `Processing`, `Complete`,
        `Failed`
  - [x] Define `Transfer` struct with all fields (id, direction, amount using
        `Decimal`, asset, addresses, status, tx_hash, created_at, network_fee)
  - [x] Add validation helper for amounts (must be positive, non-zero)
  - [x] Implement `initiate_withdrawal()` function calling
        `POST /v1/accounts/{account_id}/wallets/transfers`
  - [x] Add tests: successful withdrawal, amount validation (reject
        zero/negative/invalid), invalid asset/address, API errors
- [x] Update `src/alpaca_wallet/client.rs`:
  - [x] Add helper for HTTP POST with JSON body and auth headers
  - [x] Update `AlpacaWalletError` with variants: `InvalidAmount`,
        `InvalidAsset`
  - [x] Add tests for POST helper
- [x] Add `uuid` to `Cargo.toml` dependencies if not present
- [x] Run `cargo test -q`,
      `cargo clippy --all-targets --all-features -- -D clippy::all`, `cargo fmt`

## Task 4. Implement Transfer Status Query

**Design Reasoning**: Status querying is needed before polling can be
implemented. Simple addition to existing transfer module.

- [x] Update `src/alpaca_wallet/transfer.rs`:
  - [x] Implement `get_transfer_status()` function calling
        `GET /v1/accounts/{account_id}/wallets/transfers?transfer_id={id}`
  - [x] Parse response and map to `TransferStatus` enum, handling different
        status strings from API
  - [x] Add tests: query status for each state (Pending, Processing, Complete,
        Failed), invalid transfer ID, not found, malformed responses
- [x] Run `cargo test -q`,
      `cargo clippy --all-targets --all-features -- -D clippy::all`, `cargo fmt`

## Task 5. Implement Status Polling

**Design Reasoning**: Polling is critical for async transfer lifecycle.
Configurable behavior prevents hardcoded values. Exponential backoff reduces API
load during retries.

- [x] Create `src/alpaca_wallet/status.rs`:
  - [x] Define `PollingConfig` struct: `interval: Duration` (default: 10s),
        `timeout: Duration` (default: 30m), `max_retries: u32` (default: 3)
  - [x] Implement `poll_transfer_status()` function that loops until `Complete`
        or `Failed`
  - [x] Sleep between polls using `tokio::time::sleep(interval)`
  - [x] Return `Err(TransferTimeout)` if timeout exceeded
  - [x] Retry on transient errors up to `max_retries` with exponential backoff
  - [x] Log status transitions using `tracing::info!`
  - [x] Handle edge cases: status regression, inconsistent status, network
        failures
  - [x] Add tests: successful polling (Processing → Complete), failed transfer,
        timeout, retry on 5xx errors, invalid status regression
- [x] Update `src/alpaca_wallet/client.rs`: add
      `AlpacaWalletError::TransferTimeout` variant
- [x] Update `src/alpaca_wallet/mod.rs`: add `mod status;` declaration
- [x] Run `cargo test -q`,
      `cargo clippy --all-targets --all-features -- -D clippy::all`, `cargo fmt`

## Task 6. Implement Address Whitelisting

**Design Reasoning**: Whitelisting is a security requirement from Alpaca.
Addresses must be whitelisted and approved (24-hour wait) before withdrawals.
Separate module keeps concerns isolated.

- [ ] Create `src/alpaca_wallet/whitelist.rs`:
  - [ ] Define `WhitelistEntry` struct: id, address, asset, chain, status,
        created_at
  - [ ] Define `WhitelistStatus` enum: `Pending`, `Approved`, `Rejected`
  - [ ] Define `APPROVAL_WAIT_TIME` constant (24 hours)
  - [ ] Implement `whitelist_address()` calling
        `POST /v1/accounts/{account_id}/wallets/whitelists`
  - [ ] Implement `get_whitelisted_addresses()` calling
        `GET /v1/accounts/{account_id}/wallets/whitelists`
  - [ ] Implement `is_address_whitelisted_and_approved()` helper
  - [ ] Add tests: successful whitelisting, getting list, checking approved
        status, pending/rejected handling, duplicates
- [ ] Update `src/alpaca_wallet/client.rs`: add
      `AlpacaWalletError::AddressNotWhitelisted` variant
- [ ] Update `src/alpaca_wallet/mod.rs`: add `mod whitelist;` declaration
- [ ] Run `cargo test -q`,
      `cargo clippy --all-targets --all-features -- -D clippy::all`, `cargo fmt`

## Task 7. Implement AlpacaWalletService Facade

**Design Reasoning**: Service facade provides clean API for consumers. Trait
abstraction allows future alternative implementations. Integration tests verify
full workflows work end-to-end.

- [ ] Update `src/alpaca_wallet/mod.rs`:
  - [ ] Define `AlpacaWalletService` struct: `client: Arc<AlpacaWalletClient>`,
        `polling_config: PollingConfig`
  - [ ] Define `AlpacaWalletConfig` struct: `auth_env: AlpacaAuthEnv`,
        `polling_config: Option<PollingConfig>`
  - [ ] Implement methods delegating to appropriate module functions:
        `get_deposit_address()`, `initiate_withdrawal()`,
        `get_transfer_status()`, `poll_transfer_until_complete()`
  - [ ] In `initiate_withdrawal()`, check whitelist before delegating
  - [ ] Implement `new()` async constructor: build client, set up polling config
        with defaults, verify account access
  - [ ] Add convenience methods: `whitelist_address()`,
        `get_whitelisted_addresses()`
  - [ ] Add integration tests: full withdrawal flow (whitelist → initiate → poll
        → complete), deposit flow, error cases (unauthorized, invalid amount,
        address not whitelisted, timeout)
  - [ ] Add module-level docs with usage examples
  - [ ] Re-export only public API types
- [ ] Run `cargo test -q`,
      `cargo clippy --all-targets --all-features -- -D clippy::all`, `cargo fmt`

## Task 8. Add Documentation and Final Polish

**Design Reasoning**: Documentation eases maintenance and onboarding. Examples
prevent misuse. Final checks ensure production quality.

- [ ] Add doc comments to all public types and methods in
      `src/alpaca_wallet/mod.rs`, `src/alpaca_wallet/client.rs`
- [ ] Add module-level documentation to `src/alpaca_wallet/mod.rs` (API
      overview, authentication, whitelisting process, transfer lifecycle)
- [ ] Add usage examples in doc comments: getting deposit address, withdrawal
      flow, polling
- [ ] Document all error variants with clear descriptions
- [ ] Verify Debug implementations redact secrets (API keys)
- [ ] Run full test suite: `cargo test -q`
- [ ] Run clippy: `cargo clippy --all-targets --all-features -- -D clippy::all`
- [ ] Run formatter: `cargo fmt`
- [ ] Verify no unused imports or dead code warnings
- [ ] Review all public exports for leaky abstractions
