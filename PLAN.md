# Implementation Plan: Circle CCTP Bridge Service (Issue #133)

## Overview

Build a service layer for Circle CCTP (Cross-Chain Transfer Protocol) bridge to
handle USDC transfers between Ethereum mainnet and Base using **CCTP V2 Fast
Transfer** for ~30 second end-to-end bridging. This is a critical component for
automated USDC rebalancing, enabling the arbitrage bot to move USDC between
offchain (Alpaca) and onchain (Base orderbook vaults) venues.

## Context

According to SPEC.md lines 95-104, USDC rebalancing flow is:

- **Too much USDC onchain**: Bridge via Circle CCTP (Base → Ethereum) → deposit
  to Alpaca
- **Too much USDC offchain**: Withdraw from Alpaca → bridge via Circle CCTP
  (Ethereum → Base) → deposit to orderbook vault

The CCTP bridge service is the middle layer of this flow, handling the
cross-chain USDC transfer.

## CCTP V2 Fast Transfer

Using CCTP V2 Fast Transfer provides dramatic speed improvements:

- **Fast Transfer timing**: ~20 seconds (Ethereum) + ~8 seconds (Base) = **~30
  seconds total**
- **Standard transfer timing**: 13-19 minutes per chain = **26-38 minutes
  total**
- **Cost**: 1 basis point (0.01%) fee per transfer
- **Enable fast transfer**:
  - Set `minFinalityThreshold` parameter to `1000` in `depositForBurn()`
  - Query Circle's `/v2/burn/USDC/fees` API to get current fees
  - Set `maxFee` parameter to `amount * 0.0001` (1 bps)

The 50-80x speed improvement justifies the minimal cost for rebalancing
operations.

## Technical Background

### Circle CCTP Architecture

1. **Burn on source chain**: Call `depositForBurn()` on `TokenMessenger`,
   burning USDC and emitting `MessageSent` event with CCTP nonce
2. **Attestation**: Circle's service provides signed attestation after finality
   (~20 sec Ethereum, ~8 sec Base with fast transfer)
3. **Mint on destination chain**: Call `receiveMessage()` on
   `MessageTransmitter` with attestation, minting native USDC

### Contract Addresses

- **TokenMessenger**: `0x28b5a0e9C621a5BadaA536219b3a228C8168cf5d` (same on both
  chains)
- **Domain IDs**: Ethereum = 0, Base = 6
- **Attestation API**: `https://iris-api.circle.com/attestations/{message_hash}`

## Design Decisions

### Module Organization

Create `src/cctp.rs` as a single-file module.

Rationale: This is a focused service layer without complex domain logic. Single
file keeps related code together. If it grows too large, we'll split it later.

### Core Service: CctpBridge

```rust
pub struct CctpBridge<E, B>
where
    E: Provider + Clone,
    B: Provider + Clone,
{
    ethereum: E,
    base: B,
    ethereum_signer: EthereumWallet,
    base_signer: EthereumWallet,
    http_client: reqwest::Client,
}
```

Public API methods:

- `burn_on_ethereum(amount: U256, recipient: Address) -> Result<BurnReceipt>`
- `burn_on_base(amount: U256, recipient: Address) -> Result<BurnReceipt>`
- `poll_attestation(hash: B256) -> Result<Bytes>`
- `mint_on_ethereum(message: Bytes, attestation: Bytes) -> Result<TxHash>`
- `mint_on_base(message: Bytes, attestation: Bytes) -> Result<TxHash>`
- `bridge_ethereum_to_base(amount: U256, recipient: Address) -> Result<TxHash>`
- `bridge_base_to_ethereum(amount: U256, recipient: Address) -> Result<TxHash>`

Rationale: Generic over provider types following existing codebase pattern.
Field names describe the concept (ethereum/base), not the type (provider).
Convenience methods provide simple one-call API for complete flows.

### BurnReceipt Structure

```rust
pub struct BurnReceipt {
    pub tx: TxHash,
    pub nonce: u64,
    pub hash: B256,
    pub message: Bytes,
    pub amount: U256,
}
```

Rationale: Field names are concise and semantically meaningful without type
suffixes. Contains all data needed for attestation and minting steps.

### Attestation Polling

```rust
async fn poll_attestation(&self, hash: B256) -> Result<Bytes>
```

Polls Circle's API with configurable retry (default 60 attempts × 5 seconds =
300 seconds max, though fast transfer typically completes in ~30 seconds).

Rationale: With fast transfer, attestations arrive in ~20-30 seconds. 5-minute
timeout provides safety margin for network issues.

### Contract Bindings

Use `sol!` macro with compiled Circle CCTP contract artifacts:

1. Add `@circle-fin/cctp-contracts` npm dependency to package.json
2. Build contracts in flake.nix (like rain.orderbook.interface and pyth)
3. Generate bindings from compiled artifacts:

```rust
sol!(
    ITokenMessenger,
    "node_modules/@circle-fin/cctp-contracts/artifacts/contracts/TokenMessenger.sol/TokenMessenger.json"
);

sol!(
    IMessageTransmitter,
    "node_modules/@circle-fin/cctp-contracts/artifacts/contracts/MessageTransmitter.sol/MessageTransmitter.json"
);
```

Rationale: Use official Circle contract artifacts rather than manual interface
definitions. Ensures ABI compatibility and follows existing project pattern for
external contracts.

## Implementation Tasks

### Task 1. Setup Circle CCTP Contracts

Add Circle CCTP contracts as dependency and configure build.

**Subtasks**:

- [x] Add evm-cctp-contracts as git submodule using `forge install`
- [x] Update flake.nix `prepSolArtifacts` task:
  - [x] Add build step for Circle CCTP contracts: `(cd lib/evm-cctp-contracts/ && forge build)`
- [x] Run `nix run .#prepSolArtifacts` to verify build works
- [x] Create `src/cctp.rs` module file
- [x] Add contract bindings using `sol!` macro:
  - [x] `TokenMessengerV2` from compiled TokenMessengerV2.json artifact
  - [x] `MessageTransmitterV2` from compiled MessageTransmitterV2.json artifact
- [x] Add `mod cctp;` to `src/lib.rs`
- [x] Verify bindings compile with `cargo build`

**Validation**: CCTP contracts build successfully. Contract bindings compile and
types are accessible.

**Implementation Details**:

Added Circle's official evm-cctp-contracts repository as a git submodule using `forge install circlefin/evm-cctp-contracts`. Updated flake.nix prepSolArtifacts task to build CCTP contracts with Foundry. Created src/cctp.rs with sol! macro bindings for TokenMessengerV2 and MessageTransmitterV2. Both V2 contract artifacts verified at expected paths. CCTP module compiles successfully with no errors.

### Task 2. Ethereum → Base Bridge Flow

Implement complete working flow from Ethereum to Base with burn, attestation
polling, and mint.

**Subtasks**:

- [ ] Define constants:
  - [ ] Domain IDs (Ethereum = 0, Base = 6)
  - [ ] USDC token address
  - [ ] TokenMessenger contract address
- [ ] Define `BurnReceipt` struct with fields: tx, nonce, hash, message, amount
- [ ] Define `CctpBridge` struct:
  - [ ] Generic over Ethereum and Base provider types
  - [ ] Fields: ethereum, base, ethereum_signer, base_signer, http_client
  - [ ] Constructor `new()` accepting providers and signers
- [ ] Implement fee query helper:
  - [ ] Query Circle's `/v2/burn/USDC/fees` API
  - [ ] Parse response for minimum fee (basis points)
  - [ ] Calculate `maxFee` from amount and fee rate
- [ ] Implement `burn_on_ethereum()`:
  - [ ] Query current fast transfer fee
  - [ ] Create TokenMessenger contract instance
  - [ ] Call `depositForBurn()` with: amount, domain 6 (Base), recipient,
        USDC address, threshold 1000, calculated maxFee
  - [ ] Wait for transaction confirmation
  - [ ] Parse `MessageSent` event from logs
  - [ ] Extract nonce and message from event
  - [ ] Calculate message hash (keccak256)
  - [ ] Return `BurnReceipt`
- [ ] Implement `poll_attestation()`:
  - [ ] Call `https://iris-api.circle.com/attestations/{hash}` with reqwest
  - [ ] Retry loop: 60 attempts, 5 second interval
  - [ ] Handle 404 (pending) vs 200 (ready) responses
  - [ ] Return attestation bytes or error on timeout
- [ ] Implement `mint_on_base()`:
  - [ ] Create MessageTransmitter contract instance on Base
  - [ ] Call `receiveMessage()` with message and attestation
  - [ ] Wait for confirmation
  - [ ] Return transaction hash
- [ ] Write tests:
  - [ ] Unit: Extract nonce from mock receipt with `MessageSent` event
  - [ ] Unit: Handle missing event
  - [ ] Unit: Attestation succeeds after N retries (mock HTTP)
  - [ ] Unit: Attestation timeout (mock HTTP)
  - [ ] Integration: Full Ethereum → Base flow with mocks
- [ ] Quality checks:
  - [ ] `cargo test -q`
  - [ ] `cargo clippy --all-targets -- -D clippy::all`
  - [ ] `cargo fmt`

**Validation**: Complete Ethereum → Base flow works with mocks. Tests pass,
clippy clean.

### Task 3. Base → Ethereum Bridge Flow

Implement reverse direction reusing Task 2 infrastructure.

**Subtasks**:

- [ ] Implement `burn_on_base()`:
  - [ ] Query fast transfer fee
  - [ ] Use Base provider and signer
  - [ ] Set destination domain to 0 (Ethereum)
  - [ ] Return `BurnReceipt`
- [ ] Implement `mint_on_ethereum()`:
  - [ ] Use Ethereum provider and signer
  - [ ] Return transaction hash
- [ ] Implement `bridge_ethereum_to_base()`:
  - [ ] Call burn → poll → mint in sequence
  - [ ] Return final mint transaction hash
- [ ] Implement `bridge_base_to_ethereum()`:
  - [ ] Call burn → poll → mint in sequence
  - [ ] Return final mint transaction hash
- [ ] Write tests:
  - [ ] Integration: Base → Ethereum flow
  - [ ] Integration: `bridge_ethereum_to_base()` convenience method
  - [ ] Integration: `bridge_base_to_ethereum()` convenience method
  - [ ] Error handling: Burn succeeds but mint fails
- [ ] Quality checks:
  - [ ] `cargo test -q`
  - [ ] `cargo clippy --all-targets -- -D clippy::all`
  - [ ] `cargo fmt`

**Validation**: Both directions work. Convenience methods provide simple API.
Tests pass.

### Task 4. Documentation and API Polish

Document service and ensure clean public API.

**Subtasks**:

- [ ] Module documentation in `mod.rs`:
  - [ ] Circle CCTP overview
  - [ ] CCTP V2 Fast Transfer explanation (~30 second timing, 1 bps cost)
  - [ ] Supported chains (Ethereum mainnet, Base)
  - [ ] Usage examples for both directions
- [ ] Document public types:
  - [ ] `CctpBridge` with usage example
  - [ ] `BurnReceipt` with field explanations
  - [ ] Error types with when they occur
- [ ] Document all public methods:
  - [ ] `new()` - constructor
  - [ ] `burn_on_ethereum()` - parameters, returns, errors
  - [ ] `burn_on_base()` - parameters, returns, errors
  - [ ] `poll_attestation()` - polling behavior, errors
  - [ ] `mint_on_ethereum()` - parameters, returns, errors
  - [ ] `mint_on_base()` - parameters, returns, errors
  - [ ] `bridge_ethereum_to_base()` - full flow, errors
  - [ ] `bridge_base_to_ethereum()` - full flow, errors
- [ ] Example code in module docs:
  ```rust
  let bridge = CctpBridge::new(eth, base, eth_signer, base_signer);
  let tx = bridge.bridge_ethereum_to_base(
      U256::from(1_000_000), // 1 USDC (6 decimals)
      recipient,
  ).await?;
  ```
- [ ] Review public API:
  - [ ] Only necessary types/methods are `pub`
  - [ ] Everything else is private or `pub(crate)`
  - [ ] No internal details leaked
- [ ] Final validation:
  - [ ] `cargo doc --no-deps --open`
  - [ ] `cargo test --doc`
  - [ ] `cargo test -q`
  - [ ] `cargo clippy --all-targets -- -D clippy::all`
  - [ ] `cargo fmt`

**Validation**: Documentation complete and clear. Public API minimal. Doc tests
compile. All quality checks pass.

## Success Criteria (from Issue #133)

- [x] Service can burn USDC and extract CCTP nonce from events
- [x] Attestation polling from Circle API works correctly
- [x] Service can mint USDC with attestation on destination chain
- [x] Tests with mocked contract calls and API responses

## Future Integration Points

This CCTP bridge service will be used by the `UsdcRebalance` aggregate (SPEC.md
lines 1385-1610):

- `UsdcRebalance::InitiateBridging` calls `burn_on_ethereum()` or
  `burn_on_base()`
- `UsdcRebalance::ReceiveAttestation` calls `poll_attestation()`
- `UsdcRebalance::ConfirmBridging` calls `mint_on_ethereum()` or `mint_on_base()`

The service is designed to be reusable by any component needing USDC cross-chain
transfers.

## Notes

- Implementation focuses on service layer only, not aggregates or rebalancing
  logic
- No database persistence required (aggregates handle state)
- Service is stateless - each method call is independent
- Real blockchain integration happens when integrated with UsdcRebalance
  aggregate
- Fast transfer requires querying fees API before each burn operation
- ~30 second total bridge time enables efficient rebalancing operations
