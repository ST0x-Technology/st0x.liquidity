# Implementation Plan: TokenizedEquityMint and EquityRedemption Aggregates (Issue #135)

## Overview

Build TokenizedEquityMint and EquityRedemption aggregates to manage equity
rebalancing flows for converting between Alpaca shares and onchain tokens. Both
aggregates track async workflows with Alpaca API integration and onchain token
transfers.

## Design Decisions

**Module Organization**: Create separate feature modules following
package-by-feature pattern:

- `src/tokenized_equity_mint/` for TokenizedEquityMint aggregate
- `src/equity_redemption/` for EquityRedemption aggregate
- Each module contains: `mod.rs`, `cmd.rs`, `event.rs`, `view.rs`

**Aggregate Pattern**: Use CQRS-ES with `Aggregate` trait from `cqrs_es` crate.
Implement typestate pattern via enum variants to make invalid states
unrepresentable. Commands validate state transitions, events capture what
happened, apply events to transition between states.

**Type Safety**: Use newtypes for domain concepts:

- Reuse existing: `Symbol` from `st0x_broker`, `Address`, `TxHash`, `U256` from
  `alloy`
- Create new: `IssuerRequestId`, `TokenizationRequestId`, `ReceiptId` for
  Alpaca-specific IDs

**Decimal Precision**: Use `rust_decimal::Decimal` for share quantities to
maintain precision.

**Testing**: Use `cqrs_es::test::TestFramework` for comprehensive unit tests
alongside implementation.

**View Projections**: Implement `View` trait from `cqrs_es` to rebuild state
from events for querying.

## Task 1. Implement TokenizedEquityMint aggregate core types and structure

Create the TokenizedEquityMint aggregate with complete type definitions,
commands, events, and aggregate implementation.

**Subtasks:**

- [ ] Create `src/tokenized_equity_mint/` directory
- [ ] Create `src/tokenized_equity_mint/mod.rs`:
  - Define `IssuerRequestId(String)`, `TokenizationRequestId(String)`,
    `ReceiptId(U256)` newtypes
  - Define `TokenizedEquityMint` enum with states: `NotStarted`,
    `MintRequested`, `MintAccepted`, `TokensReceived`, `Completed`, `Failed`
  - Define `TokenizedEquityMintError` enum with error variants
  - Implement `Default` returning `NotStarted`
  - Export types
- [ ] Create `src/tokenized_equity_mint/cmd.rs`:
  - Define `TokenizedEquityMintCommand` enum with variants: `RequestMint`,
    `AcknowledgeAcceptance`, `ReceiveTokens`, `Finalize`, `Fail`
  - Add derives: `Debug`, `Clone`
- [ ] Create `src/tokenized_equity_mint/event.rs`:
  - Define `TokenizedEquityMintEvent` enum with variants: `MintRequested`,
    `MintAccepted`, `TokensReceived`, `MintCompleted`, `MintFailed`
  - Add derives: `Debug`, `Clone`, `Serialize`, `Deserialize`, `PartialEq`
- [ ] Export module in `src/lib.rs`
- [ ] Run `cargo build` to verify compilation
- [ ] Run `cargo clippy`

**State Definitions:**

```rust
pub(crate) enum TokenizedEquityMint {
    NotStarted,
    MintRequested {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        requested_at: DateTime<Utc>,
    },
    MintAccepted {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        requested_at: DateTime<Utc>,
        accepted_at: DateTime<Utc>,
    },
    TokensReceived {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
        requested_at: DateTime<Utc>,
        accepted_at: DateTime<Utc>,
        received_at: DateTime<Utc>,
    },
    Completed {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
        requested_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
    },
    Failed {
        symbol: Symbol,
        quantity: Decimal,
        reason: String,
        requested_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
}
```

**Error Variants:**

- `NotRequested` - Cannot accept mint: not in requested state
- `NotAccepted` - Cannot receive tokens: mint not accepted
- `TokensNotReceived` - Cannot finalize: tokens not received
- `AlreadyCompleted` - Already completed
- `AlreadyFailed` - Already failed

## Task 2. Implement TokenizedEquityMint aggregate logic with tests

Implement the `Aggregate` trait with business logic and comprehensive tests.

**Subtasks:**

- [ ] In `src/tokenized_equity_mint/mod.rs`, implement `Aggregate` trait:
  - `aggregate_type()` returns "TokenizedEquityMint"
  - `handle()` validates commands and produces events
  - `apply()` transitions state based on events
- [ ] Implement business rules in `handle()`:
  - `RequestMint`: Only from `NotStarted` → emit `MintRequested`
  - `AcknowledgeAcceptance`: Only from `MintRequested` → emit `MintAccepted`
  - `ReceiveTokens`: Only from `MintAccepted` → emit `TokensReceived`
  - `Finalize`: Only from `TokensReceived` → emit `MintCompleted`
  - `Fail`: From any non-terminal state → emit `MintFailed`
- [ ] Implement state transitions in `apply()` for each event
- [ ] Write tests in `#[cfg(test)] mod tests`:
  - `test_request_mint_from_not_started` - Verify initial request works
  - `test_acknowledge_acceptance_after_request` - Verify acceptance transition
  - `test_receive_tokens_after_acceptance` - Verify token receipt
  - `test_finalize_after_tokens_received` - Verify completion
  - `test_complete_mint_flow_end_to_end` - Full happy path
  - `test_cannot_acknowledge_before_request` - Expect `NotRequested` error
  - `test_cannot_receive_tokens_before_acceptance` - Expect `NotAccepted` error
  - `test_cannot_finalize_before_tokens_received` - Expect `TokensNotReceived`
    error
  - `test_fail_from_requested_state` - Verify failure from requested
  - `test_fail_from_accepted_state` - Verify failure from accepted
  - `test_cannot_fail_when_completed` - Expect `AlreadyCompleted` error
  - `test_cannot_fail_when_already_failed` - Expect `AlreadyFailed` error
- [ ] Run `cargo test -q` - all TokenizedEquityMint tests must pass
- [ ] Run `cargo clippy`

**State Transition Rules:**

- `NotStarted` → `RequestMint` → `MintRequested`
- `MintRequested` → `AcknowledgeAcceptance` → `MintAccepted`
- `MintAccepted` → `ReceiveTokens` → `TokensReceived`
- `TokensReceived` → `Finalize` → `Completed`
- Any non-terminal state → `Fail` → `Failed`

## Task 3. Implement TokenizedEquityMint view projection

Create view projection to query aggregate state.

**Subtasks:**

- [ ] Create `src/tokenized_equity_mint/view.rs`
- [ ] Define `MintStatus` enum: `NotStarted`, `Requested`, `Accepted`,
      `TokensReceived`, `Completed`, `Failed`
- [ ] Define `TokenizedEquityMintView` struct with fields:
  - `mint_id: String`
  - `symbol: Option<Symbol>`
  - `quantity: Option<Decimal>`
  - `wallet: Option<Address>`
  - `status: MintStatus`
  - `issuer_request_id: Option<IssuerRequestId>`
  - `tokenization_request_id: Option<TokenizationRequestId>`
  - `tx_hash: Option<TxHash>`
  - `shares_minted: Option<U256>`
  - `requested_at: Option<DateTime<Utc>>`
  - `completed_at: Option<DateTime<Utc>>`
  - `failed_at: Option<DateTime<Utc>>`
  - `failure_reason: Option<String>`
- [ ] Implement `View<TokenizedEquityMint>` trait with `update()` method
- [ ] Update view fields based on each event type
- [ ] Write tests:
  - `test_view_tracks_complete_flow` - Verify view updates through all states
  - `test_view_captures_failure_state` - Verify failure tracking
- [ ] Export view in module
- [ ] Run `cargo test -q` - all tests must pass
- [ ] Run `cargo clippy`

## Task 4. Implement EquityRedemption aggregate core types and structure

Create the EquityRedemption aggregate with complete type definitions, commands,
events, and aggregate implementation.

**Subtasks:**

- [ ] Create `src/equity_redemption/` directory
- [ ] Create `src/equity_redemption/mod.rs`:
  - Import `TokenizationRequestId` from tokenized_equity_mint module
  - Define `EquityRedemption` enum with states: `NotStarted`, `TokensSent`,
    `Pending`, `Completed`, `Failed`
  - Define `EquityRedemptionError` enum with error variants
  - Implement `Default` returning `NotStarted`
  - Export types
- [ ] Create `src/equity_redemption/cmd.rs`:
  - Define `EquityRedemptionCommand` enum with variants: `SendTokens`, `Detect`,
    `Complete`, `Fail`
  - Add derives: `Debug`, `Clone`
- [ ] Create `src/equity_redemption/event.rs`:
  - Define `EquityRedemptionEvent` enum with variants: `TokensSent`, `Detected`,
    `Completed`, `Failed`
  - Add derives: `Debug`, `Clone`, `Serialize`, `Deserialize`, `PartialEq`
- [ ] Export module in `src/lib.rs`
- [ ] Run `cargo build` to verify compilation
- [ ] Run `cargo clippy`

**State Definitions:**

```rust
pub(crate) enum EquityRedemption {
    NotStarted,
    TokensSent {
        symbol: Symbol,
        quantity: Decimal,
        redemption_wallet: Address,
        tx_hash: TxHash,
        sent_at: DateTime<Utc>,
    },
    Pending {
        symbol: Symbol,
        quantity: Decimal,
        tx_hash: TxHash,
        tokenization_request_id: TokenizationRequestId,
        sent_at: DateTime<Utc>,
        detected_at: DateTime<Utc>,
    },
    Completed {
        symbol: Symbol,
        quantity: Decimal,
        tx_hash: TxHash,
        tokenization_request_id: TokenizationRequestId,
        completed_at: DateTime<Utc>,
    },
    Failed {
        symbol: Symbol,
        quantity: Decimal,
        tx_hash: Option<TxHash>,
        tokenization_request_id: Option<TokenizationRequestId>,
        reason: String,
        sent_at: Option<DateTime<Utc>>,
        failed_at: DateTime<Utc>,
    },
}
```

**Error Variants:**

- `TokensNotSent` - Cannot detect redemption: tokens not sent
- `NotPending` - Cannot complete: not in pending state
- `AlreadyCompleted` - Already completed
- `AlreadyFailed` - Already failed

## Task 5. Implement EquityRedemption aggregate logic with tests

Implement the `Aggregate` trait with business logic and comprehensive tests.

**Subtasks:**

- [ ] In `src/equity_redemption/mod.rs`, implement `Aggregate` trait:
  - `aggregate_type()` returns "EquityRedemption"
  - `handle()` validates commands and produces events
  - `apply()` transitions state based on events
- [ ] Implement business rules in `handle()`:
  - `SendTokens`: Only from `NotStarted` → emit `TokensSent`
  - `Detect`: Only from `TokensSent` → emit `Detected`
  - `Complete`: Only from `Pending` → emit `Completed`
  - `Fail`: From any non-terminal state → emit `Failed`
- [ ] Implement state transitions in `apply()` for each event
- [ ] Write tests in `#[cfg(test)] mod tests`:
  - `test_send_tokens_from_not_started` - Verify sending tokens works
  - `test_detect_after_tokens_sent` - Verify detection transition
  - `test_complete_from_pending` - Verify completion
  - `test_complete_redemption_flow_end_to_end` - Full happy path
  - `test_cannot_detect_before_sending_tokens` - Expect `TokensNotSent` error
  - `test_cannot_complete_before_pending` - Expect `NotPending` error
  - `test_fail_from_tokens_sent_state` - Verify failure from sent
  - `test_fail_from_pending_state` - Verify failure from pending
  - `test_cannot_fail_when_completed` - Expect `AlreadyCompleted` error
  - `test_cannot_fail_when_already_failed` - Expect `AlreadyFailed` error
  - `test_failed_state_preserves_optional_context` - Verify optional fields in
    failed state
- [ ] Run `cargo test -q` - all EquityRedemption tests must pass
- [ ] Run `cargo clippy`

**State Transition Rules:**

- `NotStarted` → `SendTokens` → `TokensSent`
- `TokensSent` → `Detect` → `Pending`
- `Pending` → `Complete` → `Completed`
- Any non-terminal state → `Fail` → `Failed`

## Task 6. Implement EquityRedemption view projection

Create view projection to query aggregate state.

**Subtasks:**

- [ ] Create `src/equity_redemption/view.rs`
- [ ] Define `RedemptionStatus` enum: `NotStarted`, `TokensSent`, `Pending`,
      `Completed`, `Failed`
- [ ] Define `EquityRedemptionView` struct with fields:
  - `redemption_id: String`
  - `symbol: Option<Symbol>`
  - `quantity: Option<Decimal>`
  - `redemption_wallet: Option<Address>`
  - `status: RedemptionStatus`
  - `tokenization_request_id: Option<TokenizationRequestId>`
  - `tx_hash: Option<TxHash>`
  - `sent_at: Option<DateTime<Utc>>`
  - `completed_at: Option<DateTime<Utc>>`
  - `failed_at: Option<DateTime<Utc>>`
  - `failure_reason: Option<String>`
- [ ] Implement `View<EquityRedemption>` trait with `update()` method
- [ ] Update view fields based on each event type
- [ ] Write tests:
  - `test_view_tracks_complete_flow` - Verify view updates through all states
  - `test_view_captures_failure_state` - Verify failure tracking
- [ ] Export view in module
- [ ] Run `cargo test -q` - all tests must pass
- [ ] Run `cargo clippy`

## Task 7. Add documentation and final verification

Add comprehensive documentation and verify complete integration.

**Subtasks:**

- [ ] Add module-level doc comment to `src/tokenized_equity_mint/mod.rs`:
  - Purpose: Converting offchain Alpaca shares to onchain tokens
  - State flow description
  - Integration with Alpaca tokenization API
  - Usage example
- [ ] Add module-level doc comment to `src/equity_redemption/mod.rs`:
  - Purpose: Converting onchain tokens to offchain Alpaca shares
  - State flow description
  - Integration with Alpaca redemption API
  - Usage example
- [ ] Document public types with doc comments:
  - `IssuerRequestId` - Alpaca issuer request identifier
  - `TokenizationRequestId` - Alpaca tokenization request identifier
  - `ReceiptId` - Onchain receipt identifier
  - All aggregate states
  - All error variants
- [ ] Run `cargo fmt`
- [ ] Run full test suite: `cargo test -q` - all tests must pass
- [ ] Run clippy:
      `cargo clippy --all-targets --all-features -- -D clippy::all` - no errors
- [ ] Verify integration:
  - Both aggregates follow patterns from `position` and `onchain_trade`
  - Type compatibility with existing domain types
  - Event sourcing audit trail is complete
  - View projections accurately reflect aggregate state

**Documentation Requirements:**

Each module should document:

- The rebalancing flow it handles (mint or redemption)
- State machine transitions
- Alpaca API integration points
- Error handling approach
- Example usage pattern

**Completion Criteria:**

- All 24+ tests pass (12 per aggregate minimum)
- Zero clippy warnings
- Code properly formatted
- Type safety enforced via newtypes and enum variants
- Invalid states unrepresentable
- Complete audit trail via events
- Views rebuild correctly from events
