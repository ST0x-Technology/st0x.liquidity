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

## Task 1. Rename Crate and Core Types ✅

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

- [x] Update `crates/broker/Cargo.toml` package name
- [x] Rename trait and types in `crates/broker/src/lib.rs`
- [x] Update `src/env.rs` imports and type references
- [x] Update `src/lib.rs` imports and type references
- [x] Update `src/rebalancing/spawn.rs` imports
- [x] Update root `Cargo.toml` workspace member reference if needed
- [x] Run `cargo test -q` - all tests must pass
- [x] Run `cargo clippy` - no warnings

**Changes made:**

- Renamed crate directory from `crates/broker/` to `crates/execution/`
- Updated all imports from `st0x_broker` to `st0x_execution` across 40+ files
- Renamed `Broker` → `Executor`, `BrokerError` → `ExecutionError`,
  `SupportedBroker` → `SupportedExecutor`
- Updated all trait method names (`to_supported_broker` →
  `to_supported_executor`, etc.)

---

## Task 2. Rename Existing Alpaca Types ✅

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

- [x] Rename types in `crates/broker/src/alpaca/` files
- [x] Update exports in `crates/broker/src/alpaca/mod.rs`
- [x] Update `crates/broker/src/lib.rs` exports and enum variants
- [x] Update `src/env.rs` enum variants
- [x] Update `src/lib.rs` match arms
- [x] Update `src/rebalancing/spawn.rs` type references
- [x] Run `cargo test -q` - all tests must pass
- [x] Run `cargo clippy` - no warnings

**Changes made:**

- Renamed all Alpaca types with `TradingApi` suffix
- Updated CLI enum value from "alpaca" to "alpaca-trading-api"
- Fixed error handling to use proper `#[from]` attributes instead of
  `.map_err()`
- Added specific error variants: `AlpacaOrderCreate`, `AlpacaOrderGet`,
  `AlpacaOrderList`, `ParseFloat`, `ParseInt`, `NotionalOrdersNotSupported`

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

- [x] Create `AlpacaBrokerApiMode` enum with `base_url()` method
- [x] Create `AlpacaBrokerApiAuthEnv` config struct with clap parsing
- [x] Create `AlpacaBrokerApiClient` HTTP client with Basic auth
- [x] Create `AlpacaBrokerApiError` error type
- [x] Implement Debug that redacts secrets
- [x] Add tests for config parsing, URL selection, HTTP methods
- [x] Run `cargo test -p st0x-execution -q` - all tests must pass

**Changes made:**

- Created `crates/execution/src/alpaca_broker_api/mod.rs` with
  `AlpacaBrokerApiError` type
- Created `crates/execution/src/alpaca_broker_api/auth.rs` with all client
  infrastructure
- Used proper types: `Symbol`, `Shares` newtypes, `BrokerOrderStatus` enum,
  `OrderSide` enum, `f64` for prices
- Added serde boundary conversions with `#[serde(rename = "...")]` for API field
  names like `qty`
- Custom deserializers for string-to-type conversions (prices, shares)
- Comprehensive tests for config, URL selection, account verification, clock,
  orders

---

## Task 4. Implement Market Hours ✅

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

- [x] Create `ClockResponse` struct in `broker_api_auth.rs`
- [x] Implement `wait_until_market_open()` function in `market_hours.rs`
- [x] Add tests with mocked clock responses (market open, market closed)
- [x] Run `cargo test -p st0x-execution -q` - all tests must pass

**Changes made:**

- Added `ClockResponse` struct in auth.rs with proper serde deserialization
- Created `market_hours.rs` with `wait_until_market_open()` function
- Added `DurationConversion` error variant to `AlpacaBrokerApiError`
- Added 3 tests for market hours scenarios

---

## Task 5. Implement Order Operations ✅

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

- [x] Create `OrderRequest` and `OrderResponse` structs
- [x] Implement `place_order()` method on client
- [x] Implement `get_order()` method on client
- [x] Implement `list_open_orders()` method on client
- [x] Add tests for order operations
- [x] Run `cargo test -p st0x-execution -q` - all tests must pass

**Changes made:**

- All order operations implemented in auth.rs on AlpacaBrokerApiClient
- `OrderRequest` uses proper types: `Symbol`, `Shares`, `OrderSide` enum
- `OrderResponse` with custom deserializers for string-to-type conversions
- Comprehensive tests for place_order, get_order, list_open_orders

---

## Task 6. Implement Executor Trait ✅

**File:** `crates/execution/src/alpaca_broker_api/executor.rs`

```rust
pub struct AlpacaBrokerApi {
    client: Arc<AlpacaBrokerApiClient>,
}

impl Executor for AlpacaBrokerApi {
    type Error = AlpacaBrokerApiError;
    type OrderId = String;  // UUID format
    type Config = AlpacaBrokerApiAuthEnv;
}
```

**Trait methods:**

- `try_from_config` - Create client, verify account
- `wait_until_market_open` - Delegate to market_hours module
- `place_market_order` - Delegate to order module
- `get_order_status` - Delegate to order module
- `poll_pending_orders` - Delegate to order module
- `to_supported_executor` - Return `SupportedExecutor::AlpacaBrokerApi`
- `parse_order_id` - Validate UUID format
- `run_executor_maintenance` - Return None (API keys, no refresh)

**Subtasks:**

- [x] Implement all 8 Executor trait methods
- [x] Add tests for executor initialization
- [x] Add tests for parse_order_id (valid/invalid UUID)
- [x] Run `cargo test -p st0x-execution -q` - all tests must pass

**Changes made:**

- Created `executor.rs` with `AlpacaBrokerApi` struct implementing `Executor`
  trait
- Created `order.rs` with order operations (place, get, poll) with proper type
  conversions
- Added `SupportedExecutor::AlpacaBrokerApi` variant to lib.rs
- Updated Display and FromStr for the new variant
- Added `TryIntoExecutor` impl for `AlpacaBrokerApiAuthEnv`
- Added error variants: `InvalidOrderId`, `PriceConversion`
- 226 tests passing

---

## Task 7. Update Exports and Main Crate Integration ✅

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

- [x] Update `crates/broker/src/alpaca/mod.rs` exports
- [x] Update `crates/broker/src/lib.rs` with new variant and impl
- [x] Update `src/env.rs` with new ExecutorConfig variant
- [x] Update `src/lib.rs` launch handling
- [x] Run `cargo test -q` - all tests must pass
- [x] Run `cargo clippy` - no warnings
- [x] Run `cargo fmt`

**Changes made:**

- Updated `src/env.rs` with `BrokerConfig::AlpacaBrokerApi` variant
- Updated `src/lib.rs` `run_bot_session()` to handle the new variant
- Updated `src/conductor/mod.rs` renaming `BrokerConfig::Alpaca` to
  `BrokerConfig::AlpacaTradingApi`
- Updated `src/cli.rs` to handle both Alpaca variants in CLI commands
- Fixed price conversion in `order.rs` to use
  `num_traits::ToPrimitive::to_u64()` for safe f64 to u64 conversion
- Refactored `test_database_tracks_different_brokers` to use helper functions
  for clippy compliance
- All 829 tests passing, clippy clean, code formatted

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
