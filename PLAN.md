# Issue #137: Implement Rebalancing Orchestration

## Goal

Build managers to coordinate rebalancing services with aggregates and handle
async operation lifecycles for TokenizedEquityMint, EquityRedemption, and
UsdcRebalance.

## Background

The codebase already has:

1. **Three CQRS-ES aggregates** (fully implemented):
   - `TokenizedEquityMint` - shares -> tokens (Alpaca API -> onchain)
   - `EquityRedemption` - tokens -> shares (onchain -> Alpaca API)
   - `UsdcRebalance` - USDC bridging (Alpaca <-> Base via CCTP)

2. **Service integrations** (fully implemented):
   - `AlpacaTokenizationService` - mint/redemption API calls
   - `AlpacaWalletService` - USDC deposits/withdrawals
   - `CctpBridge` - cross-chain USDC transfers
   - `VaultService` - Rain OrderBook vault operations

3. **Infrastructure** (in place):
   - Event store tables (events, snapshots)
   - View tables for rebalancing
   - Lifecycle wrapper for aggregate state machines
   - `PollingConfig` for async operation polling
   - `backon` crate for retry with backoff

## Design Decisions

### Manager Pattern

Managers coordinate between aggregates and external services by:

1. Receiving triggers (programmatic calls, future InventoryView events)
2. Calling external services (Alpaca, CCTP, vault)
3. Sending commands to aggregates based on service results
4. Handling polling for async operations
5. Implementing retry logic for transient failures

Managers are stateless - all state is persisted in aggregates via the event
store. On restart, managers can resume by querying aggregate state.

### Error Handling

Two categories:

1. **Transient failures** (network, rate limits): Retry with exponential backoff
   using `backon`
2. **Permanent failures** (rejected, invalid state): Send `Fail` command to
   aggregate, transition to terminal `Failed` state

### Module Organization

Per AGENTS.md "Package by Feature": Each manager module is self-contained with
its own error types, config, and retry logic. Shared types live in the parent
`rebalancing` module.

---

## Task 1. MintManager

Implements TokenizedEquityMint orchestration with full workflow.

**Workflow**:

1. Caller invokes `execute_mint(symbol, quantity, wallet)`
2. Manager sends `RequestMint` command to aggregate
3. Manager calls `AlpacaTokenizationService::request_mint()`
4. Manager sends `AcknowledgeAcceptance` with request IDs
5. Manager polls Alpaca until `completed` status
6. Manager sends `ReceiveTokens` when Alpaca reports completion
7. Manager sends `Finalize` to complete

**Implementation**:

```
src/rebalancing/
  mod.rs              # Exports MintManager, common types
  mint.rs             # MintManager struct, workflow, errors, tests
```

### Subtasks

- [x] Create `src/rebalancing/mod.rs` with common error types
- [x] Create `src/rebalancing/mint.rs` with `MintManager` struct
- [x] Add service dependencies: `AlpacaTokenizationService`
- [x] Implement `execute_mint()` async method for full workflow
- [x] Use existing `PollingConfig` for Alpaca status polling
- [x] Use `backon` for retry on transient Alpaca API errors (via service)
- [x] Send `Fail` command on permanent errors (rejected, timeout)
- [x] Write unit tests with mocked `AlpacaTokenizationService`
- [x] Write integration test: happy path with mock HTTP server
- [x] Update `src/lib.rs` to export `rebalancing` module
- [ ] Remove `#[allow(dead_code)]` from `tokenized_equity_mint` and
      `alpaca_tokenization` (deferred to #139 - will be removed when wired up)

### Completion Criteria

- [x] `cargo test -q` passes
- [x] `cargo clippy --all-targets -- -D clippy::all` passes
- [x] `cargo fmt` produces no changes

### Implementation Notes

**Files created:**

- `src/rebalancing/mod.rs` - Module entry point with manager pattern
  documentation
- `src/rebalancing/mint.rs` - `MintManager<P, S>` generic over Provider and
  Signer

**Key design decisions:**

- Manager is stateless, holds `Arc<AlpacaTokenizationService>` reference
- Uses `Lifecycle<TokenizedEquityMint, Never>` wrapper for aggregate operations
- `decimal_to_u256_18_decimals()` helper converts Decimal quantities to onchain
  U256
- Test helpers (`setup_anvil`, `create_test_service_from_mock`,
  `TEST_REDEMPTION_WALLET`) consolidated in `pub(crate) mod tests` within
  `alpaca_tokenization.rs`

**Tests (6 total):**

- 3 unit tests for `decimal_to_u256_18_decimals` conversion
- 3 integration tests: happy path, rejected status, API error handling

---

## Task 2. RedemptionManager

Implements EquityRedemption orchestration with full workflow.

**Workflow**:

1. Caller invokes `execute_redemption(symbol, quantity, token_address)`
2. Manager calls `AlpacaTokenizationService::send_for_redemption()`
3. Manager sends `SendTokens` command with tx_hash
4. Manager polls Alpaca for redemption detection
5. Manager sends `Detect` with tokenization_request_id
6. Manager polls until `completed` status
7. Manager sends `Complete`

**Implementation**:

```
src/rebalancing/
  redemption.rs       # RedemptionManager struct, workflow, errors, tests
```

### Subtasks

- [x] Create `src/rebalancing/redemption.rs` with `RedemptionManager` struct
- [x] Add service dependencies: `AlpacaTokenizationService`, `CqrsFramework`
- [x] Implement `execute_redemption()` async method for full workflow
- [x] Implement token transfer via service's `send_for_redemption()`
- [x] Poll for redemption detection using `poll_for_redemption()`
- [x] Poll for completion using `poll_redemption_until_complete()`
- [x] Send `Fail` command on permanent errors
- [x] Write unit tests with mocked service
- [ ] Remove `#[allow(dead_code)]` from `equity_redemption` (deferred to #139)

### Completion Criteria

- [x] `cargo test -q` passes
- [x] `cargo clippy --all-targets -- -D clippy::all` passes
- [x] `cargo fmt` produces no changes

### Implementation Notes

**Files modified:**

- `src/rebalancing/redemption.rs` - `RedemptionManager<P, S, ES>` generic over
  Provider, Signer, and EventStore
- `src/alpaca_tokenization.rs` - Made `send_for_redemption()`,
  `poll_for_redemption()`, `poll_redemption_until_complete()`, and
  `redemption_wallet()` public

**Key design decisions:**

- Uses `CqrsFramework` with `cqrs.execute(&aggregate_id, command)` pattern
  (consistent with MintManager)
- Manager takes `Arc<CqrsFramework<Lifecycle<EquityRedemption, Never>, ES>>`
- Uses `FractionalShares` type for quantity parameter
- Error type uses `AggregateError<EquityRedemptionError>` to properly wrap CQRS
  errors

**Tests:**

- 1 integration test: send failure scenario (token transfer fails before any
  aggregate commands)

---

## Task 3. UsdcRebalanceManager (AlpacaToBase)

Implements USDC rebalancing from Alpaca to Base (first direction).

**Workflow (AlpacaToBase)**:

1. Caller invokes `execute_alpaca_to_base(amount)`
2. Initiate Alpaca withdrawal -> `Initiate` command
3. Poll Alpaca until complete -> `ConfirmWithdrawal` command
4. Execute CCTP burn on Ethereum -> `InitiateBridging` command
5. Poll Circle API for attestation -> `ReceiveAttestation` command
6. Execute CCTP mint on Base -> `ConfirmBridging` command
7. Deposit to Rain vault -> `InitiateDeposit` command
8. Confirm deposit -> `ConfirmDeposit` command

**Implementation**:

```
src/rebalancing/
  usdc.rs             # UsdcRebalanceManager, errors, tests
```

### Subtasks

- [ ] Create `src/rebalancing/usdc.rs` with `UsdcRebalanceManager` struct
- [ ] Add service dependencies: `AlpacaWalletService`, `CctpBridge`,
      `VaultService`
- [ ] Implement `execute_alpaca_to_base()` async method
- [ ] Implement withdrawal phase: initiate + poll via `AlpacaWalletService`
- [ ] Implement bridging phase: burn + attestation poll + mint via `CctpBridge`
- [ ] Implement deposit phase: vault deposit via `VaultService`
- [ ] Send appropriate `Fail*` command on errors at each phase
- [ ] Write unit tests with mocked services
- [ ] Write integration test: happy path
- [ ] Remove `#[allow(dead_code)]` from `usdc_rebalance`, `alpaca_wallet`,
      `cctp`

### Completion Criteria

- [ ] `cargo test -q` passes
- [ ] `cargo clippy --all-targets -- -D clippy::all` passes
- [ ] `cargo fmt` produces no changes

---

## Task 4. UsdcRebalanceManager (BaseToAlpaca)

Adds reverse direction to UsdcRebalanceManager.

**Workflow (BaseToAlpaca)**:

1. Caller invokes `execute_base_to_alpaca(amount)`
2. Withdraw from Rain vault -> `Initiate` command (with OnchainTx ref)
3. Confirm withdrawal -> `ConfirmWithdrawal` command
4. Execute CCTP burn on Base -> `InitiateBridging` command
5. Poll Circle API for attestation -> `ReceiveAttestation` command
6. Execute CCTP mint on Ethereum -> `ConfirmBridging` command
7. Initiate Alpaca deposit -> `InitiateDeposit` command
8. Poll Alpaca until complete -> `ConfirmDeposit` command

### Subtasks

- [ ] Implement `execute_base_to_alpaca()` async method
- [ ] Implement vault withdrawal phase via `VaultService::withdraw()`
- [ ] Implement bridging phase (Base -> Ethereum direction)
- [ ] Implement Alpaca deposit phase: initiate + poll
- [ ] Write unit tests for reverse direction
- [ ] Write integration test: happy path

### Completion Criteria

- [ ] `cargo test -q` passes
- [ ] `cargo clippy --all-targets -- -D clippy::all` passes
- [ ] `cargo fmt` produces no changes

---

## Task 5. Error Recovery and Edge Cases

Hardens all managers with comprehensive error handling and recovery.

### Subtasks

- [ ] Add timeout handling for all polling loops (configurable max duration)
- [ ] Implement resume-from-state for interrupted operations:
  - Query aggregate state on startup
  - Resume at appropriate workflow step
- [ ] Add structured logging with `tracing::instrument` for all manager methods
- [ ] Write tests: transient failure -> retry succeeds
- [ ] Write tests: permanent failure -> aggregate transitions to Failed
- [ ] Write tests: timeout -> aggregate transitions to Failed
- [ ] Write tests: resume from MintAccepted state
- [ ] Write tests: resume from Bridging state

### Completion Criteria

- [ ] `cargo test -q` passes (including new error case tests)
- [ ] `cargo clippy --all-targets -- -D clippy::all` passes
- [ ] `cargo fmt` produces no changes
- [ ] Log output shows clear state transitions

---

## Out of Scope

- **InventoryView and imbalance detection** - Separate issue for automatic
  trigger based on inventory ratios
- **CLI commands** - Managers are programmatic; CLI integration is separate
- **View materializers** - Event projection handled separately
- **Event store persistence** - Aggregates use in-memory state; persistence is
  separate issue

---

## File Structure After Implementation

```
src/
  rebalancing/
    mod.rs              # Exports managers, common RebalancingError type
    mint.rs             # MintManager + tests
    redemption.rs       # RedemptionManager + tests
    usdc.rs             # UsdcRebalanceManager + tests
  lib.rs                # Exports rebalancing module
```

---

## Success Criteria (Issue #137)

- [ ] Managers coordinate service calls with aggregate state
- [ ] Handle polling for Alpaca transfer status
- [ ] Handle polling for CCTP attestations
- [ ] Retry logic for transient failures
- [ ] Tests cover manager coordination flows
