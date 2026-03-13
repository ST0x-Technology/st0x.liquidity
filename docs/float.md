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
| `Dollars`           | `Dollars(Decimal)`          | `Dollars(Float)`          |
| Pyth prices         | `Decimal`                   | `Float`                   |
| Inventory balances  | `Decimal`                   | `Float`                   |
| Position events     | `Decimal`                   | `Float`                   |
| Dashboard DTOs      | `Decimal`                   | `Float`                   |

## Broker API Boundary

`num_decimal::Num` is still used at the Alpaca API boundary as a private
implementation detail inside `st0x-execution`. The broker API (via the `apca`
crate) expects `Num` values, so conversion happens at the edge via
`Float::format_with_scientific()` and string parsing.

These converters live in `st0x-execution` and are not part of the public API.
`num-decimal` remains as a dependency of `st0x-execution` only.
