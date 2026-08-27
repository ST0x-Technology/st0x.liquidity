# Float: Precision-Safe Financial Arithmetic

Reference for the `rain_math_float::Float` type used throughout the codebase for
financial arithmetic.

## Problem

`rust_decimal::Decimal` has a 96-bit mantissa (~28 significant digits). Rain's
onchain `Float` type uses a 224-bit coefficient + 32-bit exponent. When
converting Float or U256 values into Decimal, precision artifacts appear beyond
the token's native precision. For example, `7.5` shares becomes
`7.5000000000000000000000000375`. These artifacts cascade through position
tracking, inventory checks, and rebalancing triggers, causing hard production
failures.

See [#312](https://github.com/ST0x-Technology/st0x.liquidity/issues/312) for the
full incident description.

## Solution: Use `Float` Directly

The codebase uses `rain_math_float::Float` directly for all financial values.

### Key Properties

- **No precision loss** on values originating from onchain Float data.
- **224-bit coefficient** means no truncation artifacts when converting from
  U256 token amounts (18 decimals for ERC-20, 6 for USDC).

## Usage Patterns

### Construction

```rust
// From a decimal string
let value = Float::parse("7.5")?;

// From a fixed-point U256 (e.g., ERC-20 amount with 18 decimals)
let shares = Float::from_fixed_decimal(u256_amount, 18)?;

// From a raw onchain B256 (Float's wire format)
let float_value = Float::from_raw(b256_value);

// Zero constant
let zero = Float::zero()?;
```

### Conversion to Fixed-Point

Two methods exist for converting back to U256 fixed-point:

```rust
// Lossless: fails if precision would be lost
let u256 = value.to_fixed_decimal(18)?;

// Lossy: truncates excess precision, returns (value, lossless)
let (u256, lossless) = value.to_fixed_decimal_lossy(18)?;
```

**When to use which:**

- Use `to_fixed_decimal` (strict) for values that should round-trip exactly
  (e.g., parsing a U256 and converting back).
- Use `to_fixed_decimal_lossy` when the source may have more precision than the
  target (e.g., onchain Float values being written to an ERC-20 with 18
  decimals, or USDC with 6 decimals). This is the common case for production
  code paths.

### Arithmetic

All arithmetic operators return `Result<Float, FloatError>`:

```rust
let sum = (a + b)?;
let difference = (a - b)?;
let product = (a * b)?;
let quotient = (a / b)?;
let negated = (-a)?;
```

## Where `Float` Replaced `Decimal`

| Domain type         | Before                      | After                     |
| ------------------- | --------------------------- | ------------------------- |
| `FractionalShares`  | `FractionalShares(Decimal)` | `FractionalShares(Float)` |
| `Usdc` (threshold)  | `Usdc(Decimal)`             | `Usdc(Float)`             |
| `Usdc` (onchain/io) | `Usdc(Decimal)`             | `Usdc(Float)`             |
| `Usd`               | `Usd(Decimal)`              | `Usd(Float)`              |
| Oracle prices       | `Decimal`                   | `Float`                   |
| Inventory balances  | `Decimal`                   | `Float`                   |
| Position events     | `Decimal`                   | `Float`                   |
| Dashboard DTOs      | `Decimal`                   | `Float`                   |

## Formatting for output

Use `st0x_float_serde::format_float` (or `serialize_float_as_string` in a
`#[serde(serialize_with = ...)]`) for persistence and human-facing output. The
helper falls back to scientific notation when plain formatting rejects an
extreme exponent. Fixed-decimal protocol and integer-conversion boundaries may
call `Float::format_with_scientific(false)` directly when plain decimal notation
is part of the contract and the formatting error is propagated.

## Cost of an operation

`Float` arithmetic is not a plain Rust op. Every `+`/`-`/`*`/`/`, comparison,
and format ABI-encodes a call and executes it in a thread-local revm -- that is
what buys the guarantee that results match the contracts. Measured on this
codebase: **~4 us per op in release, ~140 us in debug**. It is cheap enough for
report-sized workloads, but do not put it inside a hot loop that runs
per-message.

`Float` has no `PartialEq`/`Ord`; comparisons are fallible (`eq`, `lt`, `gt`,
`min`, `max` all return `Result`). It derives `Default`, but **nothing in this
codebase treats `Float::default()` as zero** -- use `float!(0)` from
`st0x-float-macro`, which resolves at compile time and so stays infallible
inside a `Default` impl.

## Do not compute in any other numeric type

Financial values are computed in `Float` everywhere. The former implementation
used `num_decimal::Num` at some boundaries; references to its formatting and
rounding behavior describe legacy persisted data, not an active dependency or an
exception to the `Float` rule.

### Persist values losslessly by default

Persist `Float` values with the shared decimal-string serde helpers. Formatting
does not round, so prices, quantities, positions, snapshots, and other audit
facts retain their full representable precision across serialization.

Round before persistence only when a field declares a fixed-decimal persistence
contract, including both its precision and rounding mode. `src/bot_gas` is one
such compatibility boundary: its USD cost preserves the legacy eight-decimal,
round-half-to-even contract, then persists that already-rounded `Float`
losslessly. Do not copy that rounding into fields without the same declared
contract.

### Equality on a type holding `Float`

`Float` has no `PartialEq`, but `cqrs_es::DomainEvent` requires one on event
types. Hand-write it, routing through `Float`'s fallible comparison:

```rust
impl PartialEq for Usd {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(other.0).unwrap_or(false)
    }
}
```

This fallback is limited to an unavoidable `PartialEq -> bool` adapter: the
trait signature cannot propagate `FloatError`, and persisted domain values are
validated before they reach equality checks. For optional prices use
`crate::position::option_float_eq`, which is `pub(crate)` precisely so event
equality callers reuse the same constrained adapter.

Business predicates and validation must not use `unwrap_or(false)`. Propagate
`FloatError` with `?` where possible. If a persisted, `Serialize`-able error
enum cannot carry `FloatError` (it is not `Clone`/`Eq`/`Serialize`), map the
failure to an explicit serializable domain error variant; do not stringify the
source or collapse an arithmetic failure into a legitimate `false` result.

Presentation boundaries (Prometheus gauges, dashboard DTOs consumed by
JavaScript) convert out of `Float` for display. That is fine; what is not fine
is substituting a fallback value when the conversion fails, which fabricates a
financial number.

### Why not arbitrary-precision rationals

The PnL replay once accumulated in `num_decimal::Num` to stay _exact_. Exact
arithmetic has no width bound: a 67-digit derived price (an on-chain price is
`usdc_amount / equity_amount`, and that division rarely terminates) times an
18-decimal share count produced an 84-digit total. Converting that back into
`Float` for the capital calculation failed, and `/pnl` returned 500 for every
range containing realized PnL. `Float`'s 224-bit coefficient bounds the width at
~67 significant digits, which is far beyond what money needs.
