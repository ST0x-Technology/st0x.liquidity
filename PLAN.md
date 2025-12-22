# Add Alpaca Broker API + Rename Existing for Clarity

## Overview

1. Rename existing Alpaca Trading API broker for clarity
2. Add new Alpaca Broker API broker

**Final naming:**

| API                    | Module           | Struct                | CLI Value        | SupportedBroker |
| ---------------------- | ---------------- | --------------------- | ---------------- | --------------- |
| Trading API (existing) | `alpaca_trading` | `AlpacaTradingBroker` | `alpaca-trading` | `AlpacaTrading` |
| Broker API (new)       | `alpaca_broker`  | `AlpacaBrokerBroker`  | `alpaca-broker`  | `AlpacaBroker`  |

**Key API Differences:**

| Aspect   | Trading API                | Broker API                                 |
| -------- | -------------------------- | ------------------------------------------ |
| Base URL | `paper-api.alpaca.markets` | `broker-api.sandbox.alpaca.markets`        |
| Auth     | APCA headers               | Basic + APCA headers                       |
| Orders   | `/v2/orders`               | `/v1/trading/accounts/{account_id}/orders` |

---

## Task 1. Rename Existing Alpaca Module

Rename `alpaca` → `alpaca_trading` and update all references.

**File renames:**

- `crates/broker/src/alpaca/` → `crates/broker/src/alpaca_trading/`

**Struct renames in broker crate:**

- `AlpacaAuthEnv` → `AlpacaTradingAuthEnv`
- `AlpacaBroker` → `AlpacaTradingBroker`
- `AlpacaClient` → `AlpacaTradingClient`

**Changes in `crates/broker/src/lib.rs`:**

- `pub mod alpaca;` → `pub mod alpaca_trading;`
- `pub use alpaca::AlpacaBroker;` →
  `pub use alpaca_trading::AlpacaTradingBroker;`
- `use alpaca::{AlpacaAuthEnv, ...}` →
  `use alpaca_trading::{AlpacaTradingAuthEnv, ...}`
- `SupportedBroker::Alpaca` → `SupportedBroker::AlpacaTrading`

**Changes in main crate:**

- `src/env.rs`: `BrokerConfig::Alpaca` → `BrokerConfig::AlpacaTrading`
- `src/lib.rs`: Update broker handling
- `src/rebalancing/spawn.rs`: Update imports

**Subtasks:**

- [ ] Rename directory `alpaca` → `alpaca_trading`
- [ ] Rename structs in broker crate files
- [ ] Update `crates/broker/src/lib.rs` exports and SupportedBroker
- [ ] Update `src/env.rs` BrokerConfig enum
- [ ] Update `src/lib.rs` launch logic
- [ ] Update `src/rebalancing/spawn.rs` imports
- [ ] Update any other references
- [ ] Run `cargo test -q` - all tests must pass
- [ ] Run `cargo clippy` - no warnings

---

## Task 2. Implement Alpaca Broker API Configuration and HTTP Client

Create `alpaca_broker` module with configuration types and HTTP client.

**Files to create:**

- `crates/broker/src/alpaca_broker/mod.rs`
- `crates/broker/src/alpaca_broker/auth.rs`

**Types in `auth.rs`:**

```rust
pub enum AlpacaBrokerMode {
    Sandbox,      // broker-api.sandbox.alpaca.markets
    Production,   // broker-api.alpaca.markets
    #[cfg(test)] Mock(String),
}

#[derive(Parser)]
pub struct AlpacaBrokerAuthEnv {
    #[clap(long, env = "ALPACA_BROKER_API_KEY")]
    pub alpaca_broker_api_key: String,

    #[clap(long, env = "ALPACA_BROKER_API_SECRET")]
    pub alpaca_broker_api_secret: String,

    #[clap(long, env = "ALPACA_ACCOUNT_ID")]
    pub alpaca_account_id: String,

    #[clap(long, env = "ALPACA_BROKER_MODE", default_value = "sandbox")]
    pub alpaca_broker_mode: AlpacaBrokerMode,
}

pub(crate) struct AlpacaBrokerClient { ... }
```

**HTTP client requirements:**

- Dual authentication: Basic auth + APCA headers
- `get()` and `post()` methods
- `verify_account()` method calling
  `GET /v1/trading/accounts/{account_id}/account`

**Subtasks:**

- [ ] Create `mod.rs` with private module declarations
- [ ] Create `AlpacaBrokerMode` enum with `base_url()` method
- [ ] Create `AlpacaBrokerAuthEnv` config struct with clap parsing
- [ ] Create `AlpacaBrokerClient` HTTP client with dual auth
- [ ] Create `AlpacaBrokerError` error type
- [ ] Implement Debug that redacts secrets
- [ ] Add tests for config parsing, URL selection, HTTP methods
- [ ] Run `cargo test -p st0x-broker -q` - all tests must pass

---

## Task 3. Implement Market Hours

**File:** `crates/broker/src/alpaca_broker/market_hours.rs`

**Endpoint:** `GET /v1/trading/accounts/{account_id}/clock`

**Response struct:**

```rust
struct ClockResponse {
    timestamp: DateTime<FixedOffset>,
    is_open: bool,
    next_open: DateTime<FixedOffset>,
    next_close: DateTime<FixedOffset>,
}
```

**Subtasks:**

- [ ] Create `ClockResponse` struct
- [ ] Implement `wait_until_market_open()` function
- [ ] Add tests with mocked clock responses (market open, market closed)
- [ ] Run `cargo test -p st0x-broker -q` - all tests must pass

---

## Task 4. Implement Order Operations

**File:** `crates/broker/src/alpaca_broker/order.rs`

**Endpoints:**

- `POST /v1/trading/accounts/{account_id}/orders` - Place order
- `GET /v1/trading/accounts/{account_id}/orders/{order_id}` - Get order status
- `GET /v1/trading/accounts/{account_id}/orders?status=open` - List pending
  orders

**Request/Response structs:**

```rust
struct OrderRequest {
    symbol: String,
    qty: String,
    side: String,      // "buy" or "sell"
    r#type: String,    // "market"
    time_in_force: String, // "day"
}

struct OrderResponse {
    id: String,
    symbol: String,
    qty: String,
    side: String,
    status: String,
    filled_avg_price: Option<String>,
    // ...
}
```

**Subtasks:**

- [ ] Create `OrderRequest` and `OrderResponse` structs
- [ ] Implement `place_market_order()` function
- [ ] Implement `get_order_status()` function
- [ ] Implement `poll_pending_orders()` function
- [ ] Implement `map_alpaca_status()` helper for status mapping
- [ ] Add tests: place buy/sell, get status (pending/filled/rejected), poll
      multiple orders
- [ ] Run `cargo test -p st0x-broker -q` - all tests must pass

---

## Task 5. Implement Broker Trait

**File:** `crates/broker/src/alpaca_broker/broker.rs`

```rust
pub struct AlpacaBrokerBroker {
    client: Arc<AlpacaBrokerClient>,
}

impl Broker for AlpacaBrokerBroker {
    type Error = BrokerError;
    type OrderId = String;  // UUID format
    type Config = AlpacaBrokerAuthEnv;
}
```

**Trait methods:**

- `try_from_config` - Create client, verify account
- `wait_until_market_open` - Delegate to market_hours
- `place_market_order` - Delegate to order
- `get_order_status` - Delegate to order
- `poll_pending_orders` - Delegate to order
- `to_supported_broker` - Return `SupportedBroker::AlpacaBroker`
- `parse_order_id` - Validate UUID format
- `run_broker_maintenance` - Return None (API keys, no refresh)

**Subtasks:**

- [ ] Implement all 8 Broker trait methods
- [ ] Add tests for broker initialization (valid/invalid credentials)
- [ ] Add tests for parse_order_id (valid/invalid UUID)
- [ ] Run `cargo test -p st0x-broker -q` - all tests must pass

---

## Task 6. Update Broker Crate Exports

**Changes to `crates/broker/src/lib.rs`:**

- Add `pub mod alpaca_broker;`
- Add `pub use alpaca_broker::AlpacaBrokerBroker;`
- Add `SupportedBroker::AlpacaBroker` variant
- Update `Display` and `FromStr` for new variant
- Add `TryIntoBroker` impl for `AlpacaBrokerAuthEnv`

**Subtasks:**

- [ ] Add module declaration and export
- [ ] Add `SupportedBroker::AlpacaBroker` variant
- [ ] Update `Display::fmt` to return `"alpaca_broker"`
- [ ] Update `FromStr::from_str` to parse `"alpaca-broker"`
- [ ] Add `TryIntoBroker` implementation
- [ ] Run `cargo test -p st0x-broker -q` - all tests must pass

---

## Task 7. Update Main Crate

**`src/env.rs`:**

- Add import for `AlpacaBrokerAuthEnv`
- Add `BrokerConfig::AlpacaBroker(AlpacaBrokerAuthEnv)`
- Update `into_config()` parsing for `SupportedBroker::AlpacaBroker`
- Update `to_supported_broker()` for new variant

**`src/lib.rs`:**

- Add handling for `BrokerConfig::AlpacaBroker` in launch logic

**Subtasks:**

- [ ] Update `src/env.rs` with new BrokerConfig variant
- [ ] Update `src/lib.rs` launch handling
- [ ] Add test for new broker config parsing
- [ ] Run `cargo test -q` - all tests must pass
- [ ] Run `cargo clippy` - no warnings
- [ ] Run `cargo fmt`

---

## Files Summary

**Renamed:**

- `crates/broker/src/alpaca/` → `crates/broker/src/alpaca_trading/`

**New:**

- `crates/broker/src/alpaca_broker/mod.rs`
- `crates/broker/src/alpaca_broker/auth.rs`
- `crates/broker/src/alpaca_broker/broker.rs`
- `crates/broker/src/alpaca_broker/market_hours.rs`
- `crates/broker/src/alpaca_broker/order.rs`

**Modified:**

- `crates/broker/src/lib.rs`
- `src/env.rs`
- `src/lib.rs`
- `src/rebalancing/spawn.rs`

---

## Environment Variables

**Trading API (existing):**

- `ALPACA_API_KEY`
- `ALPACA_API_SECRET`
- `ALPACA_TRADING_MODE` (paper/live)

**Broker API (new):**

- `ALPACA_BROKER_API_KEY`
- `ALPACA_BROKER_API_SECRET`
- `ALPACA_ACCOUNT_ID`
- `ALPACA_BROKER_MODE` (sandbox/production)

**CLI Usage:**

```bash
# Trading API
--broker alpaca-trading

# Broker API
--broker alpaca-broker
```
