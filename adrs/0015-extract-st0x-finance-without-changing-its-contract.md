# ADR 0015: Extract `st0x-finance` without changing its contract

- Status: Proposed
- Date: 2026-07-14
- Linear:
  [RAI-669](https://linear.app/makeitrain/issue/RAI-669/reuse-existing-st0x-finance-crate-from-issuance)

## Context

`st0x-finance` currently lives at `st0x.liquidity/crates/finance`, but its
financial primitives are also required by the planned shared `st0x-alpaca`
client and overlap with types in `st0x.issuance`. Keeping equivalent types and
arithmetic in multiple repositories would let validation, serialization, and
rounding behavior drift.

The crate is not presently extractable by itself. Despite its module
documentation calling it a leaf with no workspace dependencies, its production
code depends on the sibling `st0x-float-macro` and `st0x-float-serde` crates.
All three depend on the same pinned `rain-math-float` implementation. Moving
only `crates/finance` would therefore either leave it coupled to the liquidity
repository or require rewriting numeric behavior during the extraction.

The apparent issuance overlap also hides two different units:

- `st0x_finance::FractionalShares` is a human/broker quantity backed by Rain
  `Float` and serialized as a decimal value.
- `receipt_inventory::Shares` is an on-chain raw `U256` amount of 18-decimal
  ERC-20 vault shares. Its checked integer arithmetic and raw-unit
  representation are part of the receipt-inventory invariant.

Treating these as one type would erase the scale and representation boundary.
The first issuance migration must demonstrate reuse without changing that
invariant or the public `st0x-issuance-dto` wire contract.

## Decision

1. **Create a standalone, submodule-free `st0x-finance` repository containing
   one small Rust workspace.** The workspace contains the existing
   `st0x-finance`, `st0x-float-macro`, and `st0x-float-serde` packages and pins
   `rain-math-float` once. The two float support packages remain implementation
   support for `st0x-finance`; service workflows and Alpaca transport do not
   move into this repository.

2. **Make extraction behavior-preserving.** Preserve the current public
   `st0x-finance` type and method surface, Rain `Float` representation, serde
   encodings, validation, arithmetic, and rounding behavior. Move the existing
   tests with the packages and add consumer-facing contract tests for public
   construction and wire formats before changing either consumer. Do not
   redesign the financial types in the extraction commit.

3. **Keep raw on-chain shares distinct from fractional financial shares.**
   Issuance's `receipt_inventory::Shares(U256)` remains an issuance-owned raw
   unit. Any future conversion to or from `FractionalShares` must be an
   explicit, checked boundary operation that names the decimal scale; no blanket
   `From` conversion will connect the two representations.

4. **Use `UnderlyingSymbol` as the first narrow issuance slice.** Preserve the
   public `st0x_issuance_dto::UnderlyingSymbol` name, constructor behavior,
   error shape, TypeScript/OpenAPI schema, and string wire encoding, while
   delegating its non-empty, trimmed symbol invariant to `st0x_finance::Symbol`.
   `TokenSymbol` remains distinct because it identifies the issued token rather
   than the broker underlying. This proves cross-repo reuse without changing
   quantities, persisted events, or receipt inventory.

5. **Migrate one consumer at a time under a one-version rule.** Tag the
   standalone repository only after its compatibility tests pass. First replace
   liquidity's workspace path dependency with that tag and remove the three
   source packages from liquidity in the same stack. Then migrate the issuance
   symbol slice. Each consumer uses one `st0x-finance` version across its full
   dependency graph; compatibility shims live at the consumer boundary and are
   removed with that consumer's migration.

6. **Keep dependency direction domain-first.** `st0x-alpaca` depends on the
   tagged `st0x-finance` contract; `st0x-finance` never depends on Alpaca or on
   either service. Liquidity and issuance retain their business orchestration,
   telemetry, persistence, and API-specific types.

7. **Pin releases reproducibly.** Cross-repository consumers use an immutable
   git tag and update their Cargo lockfile and crane `outputHashes` entry in the
   same change. A release is not considered consumable until both services can
   resolve it through their normal Nix builds.

## Consequences

### Positive

- Financial primitives and their serialization behavior have one owner and one
  versioned contract across liquidity, issuance, and `st0x-alpaca`.
- The extraction preserves proven arithmetic instead of mixing a repository move
  with a numeric rewrite.
- The raw vault-share invariant remains explicit and cannot be confused with a
  broker-facing fractional quantity.
- The first issuance slice is small, reversible, and exercises a real shared
  invariant without changing persisted financial state.
- Dependency direction remains acyclic: services and adapters depend on domain
  primitives, never the reverse.

### Negative / costs

- The standalone repository contains three packages rather than a visually
  simpler single package.
- Coordinated tags, Cargo lock updates, and Nix hashes add release work for each
  shared-crate change.
- Preserving the existing API postpones desirable cleanup discovered during the
  audit; later breaking changes require their own compatibility decision.
- `UnderlyingSymbol` remains a service-facing wrapper instead of becoming a
  direct type alias because its DTO name, error, and generated schemas are
  already public contracts.

### Neutral

- Liquidity's internal callers continue using the same `st0x_finance` paths;
  only the package source changes.
- Issuance keeps `Quantity`, `TokenSymbol`, and raw receipt-inventory `Shares`
  until a separate audit establishes an exact shared unit and contract.
- `st0x-alpaca` remains a separate repository and release stream.

## Alternatives considered

- **Extract only `crates/finance`.** Rejected because production code depends on
  the two sibling float packages; pointing back into liquidity would leave the
  supposedly shared domain crate owned by a service repository.
- **Inline or rewrite the float macro and serde helpers during extraction.**
  Rejected because it combines a repository-boundary change with changes to
  financial representation and wire behavior.
- **Put the financial types inside `st0x-alpaca`.** Rejected because symbols,
  money, and share quantities are domain primitives used outside Alpaca. It
  would invert the dependency and couple non-Alpaca code to a transport client.
- **Replace issuance's raw `Shares(U256)` with `FractionalShares`.** Rejected
  because the types represent different units and arithmetic domains. An
  implicit replacement risks scaling errors in receipt allocation and burns.
- **Copy the types into each repository.** Rejected because fixes and contract
  changes would continue to drift across services.

## Follow-ups

- RAI-669: create the standalone workspace, move the three packages and tests,
  and establish the compatibility baseline.
- RAI-669: migrate liquidity to the tagged dependency and remove its local
  copies.
- RAI-669: migrate the issuance `UnderlyingSymbol` implementation while keeping
  its public DTO contract intact.
- RAI-1165: implement `st0x-alpaca` against the tagged `Symbol`,
  `FractionalShares`, `Usd`, and `Usdc` types.
- RAI-1166 and RAI-1167: migrate liquidity and issuance Alpaca adapters one
  consumer at a time.
