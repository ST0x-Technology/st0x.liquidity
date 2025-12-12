# Plan: Dashboard Project Setup, Broker Selector & Live Events Panel

Issue: #177

---

## Task 1. Frontend Project Initialization

Initialize the SvelteKit project with all dependencies, configuration, and FP
helpers module.

- [ ] Create `dashboard/` directory and initialize SvelteKit project with
      TypeScript and Vite
- [ ] Install dependencies: `@tanstack/svelte-query`, `bits-ui`, `tailwindcss`,
      `lightweight-charts`
- [ ] Configure shadcn-svelte
- [ ] Configure ESLint and Prettier (no semicolons, arrow functions preferred)
- [ ] Create `lib/fp.ts` with Result type, `ok()`, `err()`, `match()`, `pipe()`,
      `map`, `flatMap`, `mapErr`
- [ ] Write unit tests for FP helpers
- [ ] Set up environment variables for broker WebSocket URLs
- [ ] Verify: `npm run build` succeeds, `npm run check` passes, FP helper tests
      pass

---

## Task 2. Frontend App Shell with Mock WebSocket

Build the complete dashboard layout with broker selector and WebSocket
infrastructure using mock data.

- [ ] Create `lib/api/messages.ts` with TypeScript types for ServerMessage,
      InitialState, EventStoreEntry matching SPEC.md
- [ ] Create `lib/websocket.svelte.ts` wrapper with connection state ($state),
      reconnection logic, and TanStack Query cache integration
- [ ] Create `lib/stores/broker.svelte.ts` for broker selection state (persisted
      to localStorage)
- [ ] Set up TanStack Query provider in `+layout.svelte` with
      `staleTime: Infinity`
- [ ] Create header bar component with broker selector dropdown (Schwab/Alpaca),
      circuit breaker placeholder (disabled), WebSocket status indicator
- [ ] Create panel grid layout in `+page.svelte` with six panel slots
- [ ] Create placeholder panel component showing "Coming soon..."
- [ ] Wire broker selector to switch WebSocket URL and clear query cache
- [ ] Write tests for WebSocket wrapper (connect, disconnect, reconnect, message
      handling)
- [ ] Verify: Switching brokers disconnects/reconnects WebSocket, status
      indicator reflects connection state, layout renders all six panels

---

## Task 3. Live Events Panel (Frontend)

Implement the Live Events panel component that displays events from TanStack
Query cache.

- [ ] Create LiveEventsPanel component reading from `['events']` query key
- [ ] Display columns: aggregate_type, aggregate_id, sequence, event_type,
      timestamp
- [ ] Events prepended at top (newest first), capped at 100 entries client-side
- [ ] Style with shadcn-svelte table components
- [ ] Write component tests verifying event rendering and 100-entry cap
- [ ] Verify: Panel displays events from cache, auto-updates when cache changes

---

## Task 4. Backend WebSocket Endpoint

Add WebSocket support to Rocket server that broadcasts domain events to
connected clients.

- [ ] Add `rocket_ws` dependency via `cargo add`
- [ ] Create `src/dashboard.rs` module with WebSocket endpoint handler
- [ ] Implement broadcast channel in Rocket managed state for publishing events
- [ ] WebSocket endpoint `/api/ws` accepts connections and adds to broadcast
      channel
- [ ] On connect, send `initial` message with stub/empty data for unimplemented
      fields
- [ ] Create `EventStoreEntry` struct (aggregate_type, aggregate_id, sequence,
      event_type, timestamp - no payload)
- [ ] Mount WebSocket route in `src/lib.rs`
- [ ] Write tests: WebSocket connection, initial message receipt, multiple
      concurrent clients
- [ ] Verify: `cargo test`, `cargo clippy`, `cargo fmt`

---

## Task 5. Event Broadcasting Integration

Hook the WebSocket broadcast into the event store so new events are pushed to
connected clients.

- [ ] Identify where events are persisted (sqlite-es crate integration point)
- [ ] Add broadcast channel sender to the persistence path
- [ ] When event is persisted, broadcast EventStoreEntry (excluding payload) to
      all connected clients
- [ ] Write integration test: persist event, verify connected WebSocket receives
      broadcast
- [ ] Verify: Full vertical slice works - backend broadcasts events, frontend
      displays them in Live Events panel

---

## Task 6. End-to-End Verification and Cleanup

Final integration testing and cleanup before PR.

- [ ] Manual end-to-end test: run backend, open dashboard, trigger domain event,
      see it appear in Live Events panel
- [ ] Verify broker selector switches between URLs correctly
- [ ] Run full test suite: `cargo test -q`, `npm test`
- [ ] Run linters: `cargo clippy --all-targets -- -D warnings`, `cargo fmt`,
      `npm run lint`
- [ ] Delete PLAN.md before creating PR
