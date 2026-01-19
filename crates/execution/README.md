# st0x-execution

Trade execution abstraction for placing orders across different brokerage
platforms. Part of the st0x workspace, sharing the same database schema for
token storage and order tracking.

## Naming

This crate uses "execution" terminology rather than "broker" to avoid confusion
with Alpaca's Broker API (one of the supported integrations). Without this
distinction, we'd end up with awkward naming like "Alpaca Broker API broker" or
ambiguous references to "the Alpaca broker" that could mean either the API type
or an implementation.

The core abstraction is the `Executor` trait - implementations execute trades
against specific brokerage APIs.

## Implementations

- **SchwabExecutor** - Charles Schwab API with OAuth 2.0 and automatic token
  refresh
- **AlpacaTradingApi** - Alpaca Trading API for individual accounts with
  paper/live trading support
- **AlpacaBrokerApi** - Alpaca Broker API for managing customer accounts
- **MockExecutor** - Testing implementation with configurable behavior

## Core Trait

All trade execution goes through the `Executor` trait with associated types:

- `Error` - Implementation-specific error types
- `OrderId` - Brokerage's order identifier format
- `Config` - Configuration needed for initialization

## Domain Types

Type-safe newtypes prevent mixing incompatible values:

- `Symbol(String)` - Stock ticker symbols with validation
- `Shares(u32)` - Share quantities with bounds checking
- `Direction` - Buy/Sell enum
- `OrderState` - Order lifecycle (Pending/Submitted/Filled/Failed)

## Example Usage

```rust
// Write brokerage-agnostic code
async fn execute_trade<E: Executor>(
    executor: &E,
    ticker: &str,
    qty: u32,
) -> Result<(), E::Error> {
    executor.wait_until_market_open().await?;
    let order = MarketOrder {
        symbol: Symbol::new(ticker)?,
        shares: Shares::new(qty)?,
        direction: Direction::Buy,
    };
    executor.place_market_order(order).await?;
    Ok(())
}

// Use with any implementation
let schwab = SchwabExecutor::try_from_config(config).await?;
execute_trade(&schwab, "AAPL", 10).await?;
```

## Crate Structure

```
src/
├── lib.rs                 # Public API: Executor trait, domain types, exports
├── error.rs               # Shared error types
├── order/                 # Shared order types (MarketOrder, OrderPlacement)
├── schwab/                # Charles Schwab implementation
├── alpaca_trading_api/    # Alpaca Trading API implementation
├── alpaca_broker_api/     # Alpaca Broker API implementation
├── mock.rs                # Mock implementation for testing
└── test_utils.rs          # Shared test utilities
```

## Design Principles

- **Trait-driven** - All operations go through the `Executor` trait
- **Minimal public API** - Only expose what's needed for construction and errors
- **No leaky abstractions** - Auth, tokens, HTTP clients remain private
- **Compile-time selection** - Zero-cost abstractions via generics

## Documentation

- **[AGENTS.md](./AGENTS.md)** - Guidelines for adding support for new
  brokerages
- **[Root README](../../README.md)** - Full project documentation
