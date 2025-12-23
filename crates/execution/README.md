# st0x-broker

Broker trait abstraction for executing stock trades across different brokerage
platforms. Part of the st0x workspace, sharing the same database schema for
token storage and order tracking.

## Architecture

### Core Trait

All broker functionality goes through the `Broker` trait with associated types
for broker-specific concerns:

- `Error` - Broker-specific error types
- `OrderId` - Broker's order identifier format
- `Config` - Configuration needed for broker initialization

### Implementations

- **SchwabBroker** - Charles Schwab API with OAuth 2.0 and token refresh
- **AlpacaBroker** - Alpaca Markets API with paper/live trading support
- **MockBroker** - Testing implementation with configurable behavior

### Domain Types

Type-safe newtypes prevent mixing incompatible values:

- `Symbol(String)` - Stock ticker symbols with validation
- `Shares(u32)` - Share quantities with bounds checking
- `Direction` - Buy/Sell enum
- `OrderState` - Order status (Submitted/Filled/Failed)

## Crate Structure

Each broker implementation follows strict encapsulation:

```
src/
├── lib.rs                 # Public API: Broker trait, domain types, broker exports
├── error.rs               # Shared error types (PersistenceError)
├── order/                 # Shared order types (MarketOrder, OrderPlacement, etc.)
│   ├── mod.rs
│   ├── state.rs           # OrderState enum
│   └── status.rs          # OrderStatus tracking
├── schwab/
│   ├── mod.rs             # Public: SchwabBroker, SchwabAuthEnv, SchwabError
│   ├── auth/              # Private: OAuth 2.0 with CQRS pattern
│   │   ├── mod.rs         # SchwabAuth aggregate
│   │   ├── cmd.rs         # SchwabAuthCommand enum
│   │   ├── event.rs       # SchwabAuthEvent enum
│   │   ├── oauth.rs       # OAuth flow implementation
│   │   └── view.rs        # Auth state projection
│   ├── broker.rs          # Private: Broker trait implementation
│   ├── encryption.rs      # Private: Token encryption/decryption
│   ├── market_hours.rs    # Private: Market hours checking via Schwab API
│   ├── order.rs           # Private: Order placement logic
│   ├── order_status.rs    # Private: Order status polling
│   └── tokens.rs          # Private: Token storage/retrieval from database
├── alpaca/
│   ├── mod.rs             # Public: AlpacaBroker, AlpacaAuthEnv, MarketHoursError
│   ├── auth.rs            # Private: API key authentication
│   ├── broker.rs          # Private: Broker trait implementation
│   ├── market_hours.rs    # Private: Market hours via Alpaca Clock API
│   └── order.rs           # Private: Order placement and status checking
├── mock.rs                # MockBroker: Single-file testing implementation
└── test_utils.rs          # Shared test utilities
```

**Export philosophy**: Only expose what's needed for broker construction and
error handling. All implementation details (auth, tokens, HTTP clients, helpers)
remain private.

## Key Principles

- **No leaky abstractions** - Implementation details are strictly private
- **Trait-driven design** - All operations go through the `Broker` trait
- **Minimal public API** - Users work with traits, not implementations
- **Encapsulation enforcement** - Visibility levels push users toward the trait
- **Compile-time broker selection** - Zero-cost abstractions via generics
- **Shared database schema** - Uses parent workspace migrations for persistence

## Example Usage

```rust
// Write broker-agnostic code
async fn execute_trade<B: Broker>(broker: &B, ticker: &str, qty: u32) -> Result<(), B::Error> {
    broker.wait_until_market_open().await?;
    let order = MarketOrder {
        symbol: Symbol::new(ticker)?,
        shares: Shares::new(qty)?,
        direction: Direction::Buy,
    };
    broker.place_market_order(order).await?;
    Ok(())
}

// Use with any broker implementation
let schwab = SchwabBroker::try_from_config(config).await?;
execute_trade(&schwab, "AAPL", 10).await?;
```

## Adding New Brokers

See [AGENTS.md](./AGENTS.md) for detailed guidelines on implementing new broker
integrations. Key requirements:

1. Create private module in `src/your_broker/`
2. Implement `Broker` trait
3. Export only: broker type, config type, error type
4. Keep all internals private (auth, tokens, HTTP, helpers)
5. Update `src/lib.rs` to re-export broker type

Reference `src/schwab/mod.rs` and `src/alpaca/mod.rs` for proper encapsulation
patterns.

## Documentation

- **[AGENTS.md](./AGENTS.md)** - Development guidelines for this crate
- **[Root README](../../README.md)** - Full project documentation
- **[Root AGENTS.md](../../AGENTS.md)** - Project-wide development guidelines
