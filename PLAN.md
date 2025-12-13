# Plan: Dashboard Project Setup, Broker Selector & Live Events Panel

Issue: #177

---

## Task 1. Frontend Project Initialization

Initialize the SvelteKit project with all dependencies, configuration, and FP
helpers module.

- [x] Create `dashboard/` directory and initialize SvelteKit project with
      TypeScript and Vite
- [x] Install dependencies: `@tanstack/svelte-query`, `bits-ui`, `tailwindcss`,
      `lightweight-charts`
- [x] Configure shadcn-svelte
- [x] Configure ESLint and Prettier (no semicolons, arrow functions preferred)
- [x] Create `lib/fp.ts` with Result type, `ok()`, `err()`, `match()`, `pipe()`,
      `map`, `flatMap`, `mapErr`
- [x] Write unit tests for FP helpers
- [x] Set up environment variables for broker WebSocket URLs
- [x] Verify: `npm run build` succeeds, `npm run check` passes, FP helper tests
      pass

### Changes Made

- `dashboard/` - SvelteKit project with TypeScript, Vite, Svelte 5
- `dashboard/package.json` - Dependencies: `@tanstack/svelte-query`, `bits-ui`,
  `tailwindcss`, `lightweight-charts`, plus dev tooling
- `dashboard/eslint.config.js` - Strict TypeScript rules with
  `strict-type-checked`, switch exhaustiveness, strict boolean expressions
- `dashboard/.prettierrc` - No semicolons, single quotes, trailing comma none
- `dashboard/src/lib/fp.ts` - FP helpers module:
  - `Result<T, E>` type with `tag: 'ok' | 'err'` discriminant
  - `ok()`, `err()`, `isOk()`, `isErr()` constructors and guards
  - `matchResult()` for pattern matching on Results
  - `matcher<T>()('discriminant')` for generic discriminated union matching
  - `map`, `mapErr`, `flatMap` for Result transformations
  - `unwrap`, `unwrapOr` for extraction
  - `tryCatch`, `tryCatchAsync` for exception handling
  - `pipe()` for left-to-right composition
- `dashboard/src/lib/fp.test.ts` - 25 unit tests covering all FP helpers
- `dashboard/src/lib/env.ts` - `Broker` type and `getWebSocketUrl()` using
  `PUBLIC_SCHWAB_WS_URL` and `PUBLIC_ALPACA_WS_URL` env vars
- shadcn-svelte components: `button`, `badge`, `card`, `select`, `separator`

---

## Task 2. Shared Types with ts-rs

Generate TypeScript types from Rust structs using ts-rs to maintain a single
source of truth for WebSocket message types.

- [x] Add `ts-rs` dependency to main crate: `cargo add ts-rs -F serde-compat`
- [x] Create `src/dashboard/mod.rs` module for dashboard-related types
- [x] Create `src/dashboard/messages.rs` with WebSocket message types:
      `ServerMessage` enum (tagged union with `type` discriminant),
      `InitialState`, `EventStoreEntry`, `Trade`, `Position`, `Inventory`,
      `PerformanceMetrics`, `SpreadSummary`, `RebalanceOperation`,
      `CircuitBreakerStatus`, `AuthStatus`
- [x] Derive `TS` and `Serialize` on all types with `#[ts(export)]`
- [x] Configure ts-rs export path to `dashboard/src/lib/api/` via
      `#[ts(export_to = "../dashboard/src/lib/api/")]`
- [x] Create `src/dashboard/ts.rs` with `export_bindings()` function and test
- [x] Create `src/bin/codegen.rs` binary to generate TypeScript bindings
- [x] Add `dashboard/src/lib/api/` to `.gitignore` (generated files not
      committed)
- [x] Update imports in `websocket.svelte.ts` and test file to use generated
      types
- [x] Verify: `cargo run --bin codegen` generates types,
      `bun run --cwd dashboard check` passes

### Changes Made

- `Cargo.toml` - Added `ts-rs` dependency with `serde-compat` feature
- `src/lib.rs` - Added `pub mod dashboard`
- `src/dashboard/mod.rs` - Module entry point, exports `export_bindings`
- `src/dashboard/messages.rs` - All WebSocket message types with ts-rs derives:
  - `ServerMessage` enum with tagged union serialization
  - `InitialState`, `EventStoreEntry`, `Trade`, `OnchainTrade`, `OffchainTrade`
  - `Position`, `Inventory`, `SymbolInventory`, `UsdcInventory`
  - `PerformanceMetrics`, `TimeframeMetrics`, `PnL`, `Timeframe`
  - `SpreadSummary`, `SpreadUpdate`, `RebalanceOperation`, `RebalanceStatus`
  - `CircuitBreakerStatus`, `AuthStatus`, `Direction`, `OffchainTradeStatus`
  - All types use `pub(super)` visibility (only used within dashboard module)
- `src/dashboard/ts.rs` - `export_bindings()` function and test
- `src/bin/codegen.rs` - Binary that calls `export_bindings()`
- `dashboard/.gitignore` - Added `src/lib/api/` (generated files not committed)
- `dashboard/src/lib/websocket.svelte.ts` - Updated imports to use generated
  files
- `dashboard/src/lib/websocket.svelte.test.ts` - Updated imports

---

## Task 3. Backend WebSocket Endpoint

Add WebSocket support to Rocket server that broadcasts domain events to
connected clients.

- [x] Add `rocket_ws` dependency via `cargo add`
- [x] Create WebSocket endpoint handler in dashboard module
- [x] Implement broadcast channel in Rocket managed state for publishing events
- [x] WebSocket endpoint `/api/ws` accepts connections and adds to broadcast
      channel
- [x] On connect, send `initial` message with stub/empty data for unimplemented
      fields
- [x] Mount WebSocket route in `src/lib.rs`
- [x] Write tests: WebSocket connection, initial message receipt, multiple
      concurrent clients
- [x] Verify: `cargo test`, `cargo clippy`, `cargo fmt`

### Changes Made

- `Cargo.toml` - Added `rocket_ws` dependency
- `src/dashboard/mod.rs` - Added WebSocket handler `ws_stream()` at `/api/ws`:
  - Accepts WebSocket connections
  - Sends initial state message with stub data on connect
  - Subscribes to broadcast channel for future events
  - Uses `tokio::select!` to handle incoming messages and broadcasts
- `src/dashboard/mod.rs` - Added `EventBroadcaster` type wrapping
  `broadcast::Sender<ServerMessage>` for Rocket managed state
- `src/lib.rs` - Mounted WebSocket route and added `EventBroadcaster` to Rocket
  managed state
- Dashboard types refactored to use `Decimal` instead of `f64` for financial
  values
- Fixed `get_symbol_audit_trail` to query by base symbol (strip "0x" suffix)
- Added WebSocket test using `tokio_tungstenite` client
- Fixed cognitive complexity in 5 files by extracting helper functions:
  - `src/rebalancing/rebalancer.rs`
  - `src/conductor/mod.rs`
  - `src/cli.rs`
  - `src/onchain/pyth/mod.rs`
  - `src/reporter/mod.rs`

---

## Task 4. Event Broadcasting Integration

Hook the WebSocket broadcast into the event store so new events are pushed to
connected clients.

- [x] Identify where events are persisted (sqlite-es crate integration point)
- [x] Add broadcast channel sender to the persistence path
- [x] When event is persisted, broadcast EventStoreEntry (excluding payload) to
      all connected clients
- [x] Write integration test: persist event, verify connected WebSocket receives
      broadcast
- [x] Verify: Backend broadcasts events when they are persisted

### Changes Made

- `src/dashboard/event.rs` - Created `EventBroadcaster` implementing cqrs-es
  `Query` trait:
  - Implements `Query` for `Lifecycle<TokenizedEquityMint, Never>`,
    `Lifecycle<EquityRedemption, Never>`, and `Lifecycle<UsdcRebalance, Never>`
  - Broadcasts `EventStoreEntry` (aggregate_type, aggregate_id, sequence,
    event_type, timestamp) to WebSocket clients when events are dispatched
  - Unit tests for broadcaster functionality
- `src/rebalancing/spawn.rs` - Wired `EventBroadcaster` into CQRS framework:
  - Updated `spawn_rebalancer` to accept optional broadcast sender
  - Added `build_mint_queries`, `build_redemption_queries`, `build_usdc_queries`
    helper functions that create query vectors including the broadcaster
  - Updated `into_rebalancer` to pass broadcast sender through
- `src/lib.rs` - Threaded broadcast sender from launch through bot:
  - Create broadcast channel in `launch()` and share between server and bot
  - Pass sender through `spawn_bot_task`, `run`, `run_bot_session`,
    `run_with_broker`
- `src/conductor/mod.rs` - Threaded broadcast sender through conductor:
  - Updated `run_market_hours_loop`, `start_conductor`, `retry_after_failure`,
    `run_conductor_until_market_close`, `handle_market_close`, and
    `Conductor::start` to accept and forward the sender
  - `spawn_rebalancer` now receives `Some(event_sender)` when called
- Dashboard types simplified to only include currently-used variants (full API
  contract types will be added as features are implemented)

---

## Task 5. Frontend App Shell with WebSocket

Build the complete dashboard layout with broker selector and WebSocket
infrastructure connecting to backend.

- [x] Create `lib/websocket.svelte.ts` wrapper with connection state ($state),
      reconnection logic, and TanStack Query cache integration
- [x] Create `lib/stores/broker.svelte.ts` for broker selection state (persisted
      to localStorage)
- [x] Write tests for WebSocket wrapper (connect, disconnect, reconnect, message
      handling)
- [x] Set up TanStack Query provider in `+layout.svelte` with
      `staleTime: Infinity`
- [x] Create header bar component with broker selector dropdown (Schwab/Alpaca),
      circuit breaker placeholder (disabled), WebSocket status indicator
- [x] Create panel grid layout in `+page.svelte` with six panel slots
- [x] Create placeholder panel component showing "Coming soon..."
- [x] Wire broker selector to switch WebSocket URL and clear query cache
- [x] Verify: Switching brokers disconnects/reconnects WebSocket, status
      indicator reflects connection state, layout renders all six panels

### Changes Made

- `dashboard/src/routes/+layout.svelte` - TanStack Query provider with
  `staleTime: Infinity`, favicon link
- `dashboard/src/routes/+page.svelte` - Main page with HeaderBar, 6-panel grid
  layout, WebSocket connection management, broker switching logic
- `dashboard/src/lib/components/header-bar.svelte` - Header with broker selector
  (Select component), Circuit Breaker button (disabled placeholder), connection
  status Badge
- `dashboard/src/lib/components/placeholder-panel.svelte` - Card component
  showing "Coming soon..." for unimplemented panels
- `dashboard/src/lib/websocket.svelte.ts` - WebSocket wrapper with:
  - Connection state management via Svelte 5 `$state`
  - Exponential backoff reconnection (1s base, 30s max)
  - TanStack Query cache integration for `initial` and `event` messages
  - 100-event cap for events list
- `dashboard/src/lib/stores/broker.svelte.ts` - Broker selection store with
  localStorage persistence
- `dashboard/src/lib/env.ts` - `Broker` type and `getWebSocketUrl()` using
  `PUBLIC_SCHWAB_WS_URL` and `PUBLIC_ALPACA_WS_URL` env vars
- `dashboard/src/lib/websocket.svelte.test.ts` - 12 tests covering connection
  lifecycle, message handling, reconnection logic, and edge cases

---

## Task 6. Live Events Panel (Frontend)

Implement the Live Events panel component that displays events from TanStack
Query cache.

- [x] Create LiveEventsPanel component reading from `['events']` query key
- [x] Display columns: aggregate_type, aggregate_id, sequence, event_type,
      timestamp
- [x] Events prepended at top (newest first), capped at 100 entries client-side
- [x] Style with shadcn-svelte table components
- [x] Write component tests verifying event rendering and 100-entry cap
- [x] Verify: Panel displays events from cache, auto-updates when cache changes

### Changes Made

- `dashboard/src/lib/components/live-events-panel.svelte` - Live events panel:
  - Uses TanStack Query `createQuery` to read from `['events']` cache key
  - Displays events in a table with Time, Type, Aggregate, Seq columns
  - Truncates long aggregate IDs with ellipsis (hover for full ID)
  - Shows event count in header
  - Empty state displays "No events yet"
- `dashboard/src/lib/components/ui/table/` - Added shadcn table components
- `dashboard/src/routes/+page.svelte` - Replaced placeholder with
  LiveEventsPanel
- `dashboard/src/lib/env.ts` - Changed default WS URL to port 8080
- `dashboard/src/app.html` - Added `dark` class for dark mode
- The 100-event cap is enforced in websocket.svelte.ts (already implemented in
  Task 5)

---

## Task 7. CI Configuration for Dashboard

Add dashboard checks to CI pipeline.

- [ ] Update CI workflow to install bun
- [ ] Add dashboard build step: `bun run --cwd dashboard build`
- [ ] Add dashboard type check step: `bun run --cwd dashboard check`
- [ ] Add dashboard lint step: `bun run --cwd dashboard lint`
- [ ] Add dashboard test step: `bun run --cwd dashboard test:run`
- [ ] Verify CI passes on a test push

---

## Task 8. End-to-End Verification and Cleanup

Final integration testing and cleanup before PR.

- [ ] Manual end-to-end test: run backend, open dashboard, trigger domain event,
      see it appear in Live Events panel
- [ ] Verify broker selector switches between URLs correctly
- [ ] Run full test suite: `cargo test -q`, `bun run --cwd dashboard test:run`
- [ ] Run linters: `cargo clippy --all-targets -- -D warnings`, `cargo fmt`,
      `bun run --cwd dashboard lint`
- [ ] Delete PLAN.md before creating PR
