# ExactDecimal: Precision-Safe Financial Arithmetic

Reference for the `ExactDecimal` type and the migration away from
`rust_decimal::Decimal`.

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

## Solution: `ExactDecimal`

`ExactDecimal` is a newtype wrapping `rain_math_float::Float` that provides
standard Rust trait implementations (`PartialEq`, `Eq`, `Ord`, `Serialize`,
`Deserialize`, arithmetic operators) that `Float` itself cannot offer because
its operations are fallible EVM calls.

```rust
// crates/exact-decimal/src/lib.rs
pub struct ExactDecimal(Float);
```

The type lives in its own crate (`st0x-exact-decimal`) so it can be used by both
`st0x-execution` and `st0x-hedge` without circular dependencies.

### Key Properties

- **No precision loss** on values originating from onchain Float data.
- **224-bit coefficient** means no truncation artifacts when converting from
  U256 token amounts (18 decimals for ERC-20, 6 for USDC).
- **Backward-compatible serde**: serializes as a decimal string (`"1.5"`),
  deserializes from both decimal strings and hex B256 strings (`"0xffff..."`).
- **Trailing zero stripping**: `ExactDecimal` formats without trailing zeros.
  `"500.50"` becomes `"500.5"`, `"1.0"` becomes `"1"`. Code that compares
  formatted output must account for this.

## Usage Patterns

### Construction

```rust
// From a decimal string
let value = ExactDecimal::parse("7.5")?;

// From a fixed-point U256 (e.g., ERC-20 amount with 18 decimals)
let shares = ExactDecimal::from_fixed_decimal(u256_amount, 18)?;

// From a raw onchain B256 (Float's wire format)
let float_value = ExactDecimal::from_raw(b256_value);

// Zero constant (const, no allocation)
let zero = ExactDecimal::zero();
```

### Conversion to Fixed-Point

Two methods exist for converting back to U256 fixed-point:

```rust
// Lossless: fails if precision would be lost
let u256 = value.to_fixed_decimal(18)?;

// Lossy: truncates excess precision, returns whether truncation occurred
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

All arithmetic operators return `Result<ExactDecimal, FloatError>`:

```rust
let sum = (a + b)?;
let difference = (a - b)?;
let product = (a * b)?;
let quotient = (a / b)?;
let negated = (-a)?;
```

### Comparisons

`PartialEq`, `Eq`, `PartialOrd`, and `Ord` are implemented. They delegate to
Float's EVM-based comparison and panic on malformed B256 data (which cannot
occur through valid construction paths). This makes comparisons infallible in
practice:

```rust
if amount > threshold {
    // ...
}
```

### Serde

Serializes as a JSON string: `"1.5"`, `"0"`, `"-3.14"`. Deserialization accepts
both decimal strings (backward compat with existing event payloads that used
`rust_decimal::Decimal`) and hex B256 strings.

## Where `ExactDecimal` Replaced `Decimal`

| Domain type         | Before                      | After                            |
| ------------------- | --------------------------- | -------------------------------- |
| `FractionalShares`  | `FractionalShares(Decimal)` | `FractionalShares(ExactDecimal)` |
| `Usdc` (threshold)  | `Usdc(Decimal)`             | `Usdc(ExactDecimal)`             |
| `Usdc` (onchain/io) | `Usdc(Decimal)`             | `Usdc(ExactDecimal)`             |
| `Dollars`           | `Dollars(Decimal)`          | `Dollars(ExactDecimal)`          |
| Pyth prices         | `Decimal`                   | `ExactDecimal`                   |
| Inventory balances  | `Decimal`                   | `ExactDecimal`                   |
| Position events     | `Decimal`                   | `ExactDecimal`                   |
| Dashboard DTOs      | `Decimal`                   | `ExactDecimal`                   |

## Broker API Boundary

`rust_decimal::Decimal` is still used at the Alpaca and Schwab API boundaries as
a private implementation detail inside `st0x-execution`. The broker APIs expect
`Decimal` values, so conversion happens at the edge:

- `FractionalShares::to_decimal()` -- for outgoing broker requests
- `FractionalShares::from_decimal()` -- for incoming broker responses

These converters live in `st0x-execution` and are not part of the public API.
`rust_decimal` remains as a private dependency of `st0x-execution` only.

## Crate Structure

```
crates/exact-decimal/
  Cargo.toml          # depends on rain-math-float, serde, alloy-primitives
  src/lib.rs          # ExactDecimal type + all trait impls + tests
```

The crate is a workspace member and is imported as `st0x-exact-decimal` by
`st0x-execution` and `st0x-hedge`.
