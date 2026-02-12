# AGENTS.md

This file provides guidance to AI agents working with the `st0x-bridge` crate.

## General Guidelines

**IMPORTANT**: All agents working in this crate MUST obey the top-level
`@AGENTS.md` at the repository root. This file only contains
bridge-crate-specific additions and clarifications.

## Bridge Crate Scope

This is a **standalone library crate** that provides a generic `Bridge` trait
for cross-chain USDC transfers. When working in this crate:

- **Stay focused on bridge abstractions**: This crate should remain independent
  of the parent application
- **No parent crate dependencies**: Never add dependencies on `st0x-hedge` or
  other workspace members
- **Feature-gated implementations**: The default build ships only the `Bridge`
  trait and shared domain types. CCTP implementation is behind the `cctp`
  feature

## CRITICAL: No Leaky Abstractions

Export lists and visibility levels must be **strictly monitored and
controlled**.

### Encapsulation Requirements

The encapsulation design **pushes consumers towards using the `Bridge` trait**,
not the CCTP implementation directly. Implementation details must remain hidden.

**What to expose (and ONLY what to expose):**

1. **The `Bridge` trait** - The core abstraction (always compiled)
2. **The `Attestation` trait** - For opaque attestation data access
3. **Domain types** - `BridgeDirection`, `BurnReceipt`, `MintReceipt`
4. **Implementation type** - `CctpBridge` (behind `cctp` feature)
5. **Context type** - `CctpCtx` (behind `cctp` feature)

**What must remain private:**

- `Evm` struct and all EVM interaction logic
- Contract addresses (`TOKEN_MESSENGER_V2`, `MESSAGE_TRANSMITTER_V2`)
- Circle API interaction details
- Internal receipt types
- Fee calculation logic
- Attestation polling implementation

### Visibility Level Guidelines

1. **Start with the most restrictive visibility** (private/module-private)
2. **Only increase visibility when required** by external consumers
3. **Prefer `pub(crate)` over `pub`** for crate-internal sharing
4. **Challenge every `pub`** - does this really need to be in the public API?

## Testing Requirements

- **HTTP mocking**: Use `httpmock` for Circle API testing
- **Anvil**: Use `alloy::node_bindings::Anvil` for on-chain integration tests
- **No test utils bloat**: Only add test utilities that are reused across
  multiple test modules

## Architecture Constraints

- **Trait-based design**: All functionality goes through the `Bridge` trait
- **Feature flags**: CCTP implementation behind `cctp` feature flag
- **No runtime selection**: Implementation choice happens at compile time
  through generics
- **Associated types**: Error and Attestation types are associated to enable
  zero-cost abstraction
