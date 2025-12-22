# Add Alpaca Broker API + Rename for Clarity

## Overview

1. Rename crate `st0x-broker` → `st0x-execution` and trait `Broker` → `Executor`
2. Rename existing Alpaca types to have `TradingApi` suffix
3. Add new Alpaca Broker API implementation

**Final naming:**

| API                    | Module   | Struct             | CLI Value            | SupportedExecutor  |
| ---------------------- | -------- | ------------------ | -------------------- | ------------------ |
| Trading API (existing) | `alpaca` | `AlpacaTradingApi` | `alpaca-trading-api` | `AlpacaTradingApi` |
| Broker API (new)       | `alpaca` | `AlpacaBrokerApi`  | `alpaca-broker-api`  | `AlpacaBrokerApi`  |

**Key API Differences:**

| Aspect   | Trading API                | Broker API                                 |
| -------- | -------------------------- | ------------------------------------------ |
| Base URL | `paper-api.alpaca.markets` | `broker-api.sandbox.alpaca.markets`        |
| Auth     | APCA headers               | Basic + APCA headers                       |
| Orders   | `/v2/orders`               | `/v1/trading/accounts/{account_id}/orders` |

---

## Task 1. Rename Crate and Core Types

Rename the crate and trait to avoid "BrokerBroker" naming confusion.

**Crate rename:**

- `crates/broker/` stays as directory (cargo package name changes)
- `Cargo.toml`: `name = "st0x-broker"` → `name = "st0x-execution"`

**Core type renames in `crates/broker/src/lib.rs`:**

- `Broker` trait → `Executor` trait
- `SupportedBroker` enum → `SupportedExecutor` enum
- `BrokerError` → `ExecutionError`
- `TryIntoBroker` trait → `TryIntoExecutor` trait
- Method: `to_supported_broker()` → `to_supported_executor()`
- Method: `run_broker_maintenance()` → `run_executor_maintenance()`

**Update imports in main crate:**

- `use st0x_broker::` → `use st0x_execution::`
- Update all references to renamed types

**Subtasks:**

- [ ] Update `crates/broker/Cargo.toml` package name
- [ ] Rename trait and types in `crates/broker/src/lib.rs`
- [ ] Update `src/env.rs` imports and type references
- [ ] Update `src/lib.rs` imports and type references
- [ ] Update `src/rebalancing/spawn.rs` imports
- [ ] Update root `Cargo.toml` workspace member reference if needed
- [ ] Run `cargo test -q` - all tests must pass
- [ ] Run `cargo clippy` - no warnings

---

## Task 2. Rename Existing Alpaca Types

Add `TradingApi` suffix to existing Alpaca types for clarity.

**Type renames in `crates/broker/src/alpaca/`:**

- `AlpacaBroker` → `AlpacaTradingApi`
- `AlpacaAuthEnv` → `AlpacaTradingApiAuthEnv`
- `AlpacaClient` → `AlpacaTradingApiClient`
- `AlpacaMode` → `AlpacaTradingApiMode`

**Enum variant renames:**

- `SupportedExecutor::Alpaca` → `SupportedExecutor::AlpacaTradingApi`
- `ExecutorConfig::Alpaca` → `ExecutorConfig::AlpacaTradingApi`

**CLI/Display updates:**

- `"alpaca"` → `"alpaca-trading-api"`

**Subtasks:**

- [ ] Rename types in `crates/broker/src/alpaca/` files
- [ ] Update exports in `crates/broker/src/alpaca/mod.rs`
- [ ] Update `crates/broker/src/lib.rs` exports and enum variants
- [ ] Update `src/env.rs` enum variants
- [ ] Update `src/lib.rs` match arms
- [ ] Update `src/rebalancing/spawn.rs` type references
- [ ] Run `cargo test -q` - all tests must pass
- [ ] Run `cargo clippy` - no warnings

---

## Task 3. Implement Alpaca Broker API Client

Create client infrastructure for Broker API authentication and HTTP requests.

**Files to create in `crates/broker/src/alpaca/`:**

- `broker_api_auth.rs` - Auth config and HTTP client
- `broker_api.rs` - Executor implementation

**Types in `broker_api_auth.rs`:**

```rust
pub enum AlpacaBrokerApiMode {
    Sandbox,      // broker-api.sandbox.alpaca.markets
    Production,   // broker-api.alpaca.markets
    #[cfg(test)] Mock(String),
}

#[derive(Parser)]
pub struct AlpacaBrokerApiAuthEnv {
    #[clap(long, env = "ALPACA_BROKER_API_KEY")]
    pub alpaca_broker_api_key: String,

    #[clap(long, env = "ALPACA_BROKER_API_SECRET")]
    pub alpaca_broker_api_secret: String,

    #[clap(long, env = "ALPACA_ACCOUNT_ID")]
    pub alpaca_account_id: String,

    #[clap(long, env = "ALPACA_BROKER_API_MODE", default_value = "sandbox")]
    pub alpaca_broker_api_mode: AlpacaBrokerApiMode,
}

pub(crate) struct AlpacaBrokerApiClient { ... }
```

**HTTP client requirements:**

- Dual authentication: Basic auth + APCA headers
- `get()` and `post()` methods
- `verify_account()` method calling
  `GET /v1/trading/accounts/{account_id}/account`

**Subtasks:**

- [ ] Create `AlpacaBrokerApiMode` enum with `base_url()` method
- [ ] Create `AlpacaBrokerApiAuthEnv` config struct with clap parsing
- [ ] Create `AlpacaBrokerApiClient` HTTP client with dual auth
- [ ] Create `AlpacaBrokerApiError` error type
- [ ] Implement Debug that redacts secrets
- [ ] Add tests for config parsing, URL selection, HTTP methods
- [ ] Run `cargo test -p st0x-execution -q` - all tests must pass

---

## Task 4. Implement Market Hours

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

- [ ] Create `ClockResponse` struct in `broker_api_auth.rs`
- [ ] Implement `wait_until_market_open()` method on client
- [ ] Add tests with mocked clock responses (market open, market closed)
- [ ] Run `cargo test -p st0x-execution -q` - all tests must pass

---

## Task 5. Implement Order Operations

**Endpoints:**

- `POST /v1/trading/accounts/{account_id}/orders` - Place order
- `GET /v1/trading/accounts/{account_id}/orders/{order_id}` - Get order status
- `GET /v1/trading/accounts/{account_id}/orders?status=open` - List pending

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
}
```

**Subtasks:**

- [ ] Create `OrderRequest` and `OrderResponse` structs
- [ ] Implement `place_market_order()` method
- [ ] Implement `get_order_status()` method
- [ ] Implement `poll_pending_orders()` method
- [ ] Add tests for order operations
- [ ] Run `cargo test -p st0x-execution -q` - all tests must pass

---

## Task 6. Implement Executor Trait

**File:** `crates/broker/src/alpaca/broker_api.rs`

```rust
pub struct AlpacaBrokerApi {
    client: Arc<AlpacaBrokerApiClient>,
}

impl Executor for AlpacaBrokerApi {
    type Error = ExecutionError;
    type OrderId = String;  // UUID format
    type Config = AlpacaBrokerApiAuthEnv;
}
```

**Trait methods:**

- `try_from_config` - Create client, verify account
- `wait_until_market_open` - Delegate to client
- `place_market_order` - Delegate to client
- `get_order_status` - Delegate to client
- `poll_pending_orders` - Delegate to client
- `to_supported_executor` - Return `SupportedExecutor::AlpacaBrokerApi`
- `parse_order_id` - Validate UUID format
- `run_executor_maintenance` - Return None (API keys, no refresh)

**Subtasks:**

- [ ] Implement all 8 Executor trait methods
- [ ] Add tests for executor initialization
- [ ] Add tests for parse_order_id (valid/invalid UUID)
- [ ] Run `cargo test -p st0x-execution -q` - all tests must pass

---

## Task 7. Update Exports and Main Crate Integration

**Changes to `crates/broker/src/lib.rs`:**

- Add `SupportedExecutor::AlpacaBrokerApi` variant
- Update `Display` and `FromStr` for new variant
- Add `TryIntoExecutor` impl for `AlpacaBrokerApiAuthEnv`
- Export `AlpacaBrokerApi` and `AlpacaBrokerApiAuthEnv`

**Changes to `src/env.rs`:**

- Add `ExecutorConfig::AlpacaBrokerApi(AlpacaBrokerApiAuthEnv)`
- Update `into_config()` parsing
- Update `to_supported_executor()`

**Changes to `src/lib.rs`:**

- Add handling for `ExecutorConfig::AlpacaBrokerApi` in launch logic

**Subtasks:**

- [ ] Update `crates/broker/src/alpaca/mod.rs` exports
- [ ] Update `crates/broker/src/lib.rs` with new variant and impl
- [ ] Update `src/env.rs` with new ExecutorConfig variant
- [ ] Update `src/lib.rs` launch handling
- [ ] Run `cargo test -q` - all tests must pass
- [ ] Run `cargo clippy` - no warnings
- [ ] Run `cargo fmt`

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
- `ALPACA_BROKER_API_MODE` (sandbox/production)

**CLI Usage:**

```bash
# Trading API
--executor alpaca-trading-api

# Broker API
--executor alpaca-broker-api
```
