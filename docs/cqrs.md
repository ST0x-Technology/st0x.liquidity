# Event Sourcing with st0x-event-sorcery

Quick reference for event-sourcing patterns in this codebase. The
`st0x-event-sorcery` crate provides the primary interface; cqrs-es is an
implementation detail hidden behind it.

## Core Principle: Events Are Immutable

**Events are the source of truth and can NEVER be changed or deleted.**
Everything else -- entities, commands, projections -- can be freely modified
because they're derived from events.

- **Commands**: Can add, remove, or change freely
- **Entities**: Can restructure, add fields, change logic freely
- **Projections**: Can add, drop, restructure freely (just replay from events)
- **Events**: PERMANENT. Think carefully before adding new event types.

## Architecture

```text
Domain type          Adapter             cqrs-es (hidden)
+--------------+     +----------------+  +------------+
| impl         | --> | Lifecycle      |  | Aggregate  |
| EventSourced |     | (blanket impl) |--| trait      |
+--------------+     +----------------+  +------------+
                            |
                     +------+------+
                     | Store       |
                     | (typed IDs, |
                     |  send())    |
                     +-------------+
```

Consumers implement `EventSourced`. `Lifecycle` bridges to cqrs-es
automatically. `Store` provides type-safe command dispatch with strongly-typed
IDs.

## Implementing a New Entity

### 1. Define the Domain Type

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MyEntity {
    // domain state
}
```

### 2. Define Events and Commands

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MyEntityEvent {
    Created { /* fields */ },
    Updated { /* fields */ },
}

impl DomainEvent for MyEntityEvent {
    fn event_type(&self) -> String { /* e.g., "MyEntityEvent::Created" */ }
    fn event_version(&self) -> String { "1.0".to_string() }
}

pub enum MyEntityCommand {
    Create { /* fields */ },
    Update { /* fields */ },
}
```

### 3. Implement EventSourced

```rust
#[async_trait]
impl EventSourced for MyEntity {
    type Id = MyEntityId;       // strongly-typed, Display + FromStr
    type Event = MyEntityEvent;
    type Command = MyEntityCommand;
    type Error = Never;         // or a thiserror type
    type Services = ();         // or Arc<dyn SomeService>

    const AGGREGATE_TYPE: &'static str = "MyEntity";
    const PROJECTION: Option<Table> = Some(Table("my_entity_view"));
    const SCHEMA_VERSION: u64 = 1;

    // Event-side: reconstruct state from events
    fn originate(event: &Self::Event) -> Option<Self> { /* ... */ }
    fn evolve(entity: &Self, event: &Self::Event)
        -> Result<Option<Self>, Self::Error> { /* ... */ }

    // Command-side: process commands to produce events
    async fn initialize(command: Self::Command, services: &Self::Services)
        -> Result<Vec<Self::Event>, Self::Error> { /* ... */ }
    async fn transition(&self, command: Self::Command, services: &Self::Services)
        -> Result<Vec<Self::Event>, Self::Error> { /* ... */ }
}
```

**Method naming conventions:**

| Method       | Purpose                                | Theme         |
| ------------ | -------------------------------------- | ------------- |
| `originate`  | Create initial state from first event  | Evolution     |
| `evolve`     | Derive new state from subsequent event | Evolution     |
| `initialize` | Handle command when no state exists    | State machine |
| `transition` | Handle command against existing state  | State machine |

## Key Types

| Type                   | Purpose                                      |
| ---------------------- | -------------------------------------------- |
| `EventSourced`         | Core trait -- implement on domain types      |
| `Store<Entity>`        | Type-safe command dispatch                   |
| `StoreBuilder<Entity>` | Wires reactors/projections, builds Store     |
| `Projection<Entity>`   | Read-side materialized view                  |
| `Reactor<Entity>`      | Event side-effect handler                    |
| `SendError<Entity>`    | Error from `Store::send()`                   |
| `LifecycleError<E>`    | Errors from event application                |
| `Never`                | Error type for infallible entities           |
| `DomainEvent`          | Trait for event serialization (from cqrs-es) |
| `Table`                | Newtype for projection table name            |

## Sending Commands

```rust
let store: Store<Position> = /* built by StoreBuilder */;

let symbol = Symbol::new("AAPL").unwrap();
store.send(&symbol, PositionCommand::AcknowledgeFill { /* ... */ }).await?;
```

`Store::send()` routes based on lifecycle state:

- Uninitialized -> `Entity::initialize`
- Live -> `Entity::transition`
- Failed -> returns the stored error

## Reading State via Projections

Production code reads entity state through `Projection`, never by loading
aggregates directly:

```rust
let projection: Projection<Position> = Projection::sqlite(pool.clone())?;

// Load by typed ID
let position: Option<Position> = projection.load(&symbol).await?;
```

Projections are materialized views stored in SQLite tables (named by
`PROJECTION` constant). They're automatically updated when events are persisted
through a `Store` that has the projection wired.

### Filtered Queries with Columns

```rust
const STATUS: Column = Column("status");

let pending_orders: Vec<OffchainOrder> = projection
    .load_where(STATUS, "Pending")
    .await?;
```

## Wiring: StoreBuilder

`StoreBuilder` wires projections and reactors to a `Store` at startup. It uses
type-level linked lists (`Cons`/`Nil`) to ensure all required processors are
wired at compile time.

```rust
let projection = Projection::<Position>::sqlite(pool.clone())?;

let (store, (trigger, ())) = StoreBuilder::<Position>::new(pool)
    .with(projection.clone())  // wire the projection
    .wire(trigger)             // wire a reactor (via Unwired)
    .build(services)
    .await?;
```

The `QueryManifest` pattern in `conductor/manifest.rs` ensures exhaustive wiring
by destructuring all processors.

## Reactors

Side-effect handlers that process events one at a time with typed IDs:

```rust
#[async_trait]
impl Reactor<TokenizedEquityMint> for EventBroadcaster {
    async fn react(&self, id: &IssuerRequestId, event: &MintEvent) {
        // broadcast to dashboard, trigger downstream, etc.
    }
}
```

Wire reactors via `Unwired` + `StoreBuilder::wire()`.

## Services Pattern

Inject external dependencies into command handlers:

```rust
type Services = Arc<dyn OrderPlacer>;

async fn transition(
    &self,
    command: Self::Command,
    services: &Self::Services,
) -> Result<Vec<Self::Event>, Self::Error> {
    let result = services.place_order(/* ... */).await?;
    Ok(vec![MyEvent::OrderPlaced { /* ... */ }])
}
```

Pass services when building the `Store`:

```rust
let store = StoreBuilder::<MyEntity>::new(pool)
    .build(services)
    .await?;
```

For entities that don't need services, use `type Services = ()`.

## Schema Versioning

Bump `SCHEMA_VERSION` when the entity's state, event, or projection schema
changes. On startup, the wiring infrastructure (via `StoreBuilder::build()`)
detects version mismatches and automatically clears stale snapshots.

## Testing

### replay -- reconstruct state from events

```rust
use st0x_event_sorcery::replay;

let position = replay::<Position>(vec![
    PositionEvent::Initialized { /* ... */ },
    PositionEvent::FillAcknowledged { /* ... */ },
]).unwrap().unwrap();

assert_eq!(position.net, dec!(100));
```

### TestHarness -- BDD-style command testing

```rust
use st0x_event_sorcery::TestHarness;

TestHarness::<Position>::with(())
    .given(vec![PositionEvent::Initialized { /* ... */ }])
    .when(PositionCommand::AcknowledgeFill { /* ... */ })
    .await
    .then_expect_events(&[PositionEvent::FillAcknowledged { /* ... */ }]);
```

### TestStore -- in-memory command dispatch

```rust
use st0x_event_sorcery::TestStore;

let store = TestStore::<MyEntity>::new(vec![], ());
store.send(&id, MyCommand::Create { /* ... */ }).await.unwrap();

let entity = store.load(&id).await.unwrap().unwrap();
assert_eq!(entity.field, expected);
```

### test_store -- SQLite-backed store without reactors

```rust
use st0x_event_sorcery::test_store;

let store = test_store::<VaultRegistry>(pool.clone(), ());
store.send(&id, command).await.unwrap();
```

Use `test_store` when you need SQLite persistence but don't care about
projections or reactors. If you need projection data visible after commands, use
`StoreBuilder` with the projection wired.

### load_aggregate -- test-only aggregate loading

```rust
use st0x_event_sorcery::load_aggregate;

let entity: Option<Position> = load_aggregate::<Position>(pool, &symbol)
    .await.unwrap();
```

Gated behind `#[cfg(test)]` / `feature = "test-support"`. Bypasses the CQRS
framework (no reactors dispatched). Production code reads through `Projection`.

## Event Upcasters

When you MUST change event structure (e.g., adding required fields to existing
events), use upcasters to transform old events to the new format at load time:

```rust
use cqrs_es::persist::{EventUpcaster, SemanticVersionEventUpcaster};

fn upcast_v1_to_v2(mut payload: Value) -> Value {
    payload["new_field"] = json!("default");
    payload
}

pub fn create_my_upcaster() -> Box<dyn EventUpcaster> {
    Box::new(SemanticVersionEventUpcaster::new(
        "MyAggregate::MyEvent",  // event_type to match
        "2.0",                    // target version
        Box::new(upcast_v1_to_v2),
    ))
}
```

Update `event_version()` in your event enum to return the new version for new
events.

## Forbidden Patterns

1. **NEVER write directly to the `events` table** -- use `Store::send()`
2. **NEVER query the `events` table with raw SQL** -- use framework APIs
3. **NEVER modify or delete events** -- they're immutable historical facts
4. **NEVER implement `Aggregate` directly** -- implement `EventSourced`
5. **NEVER construct `Lifecycle` in application code** -- it's an internal
   adapter
6. **NEVER call `sqlite_cqrs()` or `CqrsFramework::new()` in production code**
   -- use `StoreBuilder`
7. **NEVER create multiple `Store<Entity>` for the same entity type** -- one per
   entity, wired once at startup

## Single Framework Instance Per Entity

Each entity type must have exactly ONE `Store<Entity>` instance, constructed
once in `Conductor::start` via `StoreBuilder`, then shared. Multiple instances
cause silent bugs: events persist but reactors/projections on other instances
never see them.

## cqrs-es / sqlite-es Internals Reference

These details are hidden by st0x-event-sorcery but documented here for debugging
and migration authoring.

### sqlite-es Table Schemas

All three tables are created in the `event_store` migration.

**Events table:**

```sql
CREATE TABLE IF NOT EXISTS events (
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    sequence       BIGINT NOT NULL,
    event_type     TEXT NOT NULL,
    event_version  TEXT NOT NULL,
    payload        JSON NOT NULL,
    metadata       JSON NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id, sequence)
);
```

- **aggregate_type**: From `EventSourced::AGGREGATE_TYPE` (e.g., `"Position"`)
- **aggregate_id**: Caller-provided ID string
- **sequence**: Auto-incremented per aggregate instance (1, 2, 3, ...)
- **event_type**: From `DomainEvent::event_type()` (e.g.,
  `"PositionEvent::Initialized"`)
- **event_version**: From `DomainEvent::event_version()` (e.g., `"1.0"`)
- **payload**: Event serialized via `serde_json::to_value(&event)`
- **metadata**: Arbitrary JSON metadata passed via `execute_with_metadata()`

**NEVER** write to this table directly. Use `CqrsFramework::execute()`.

### Snapshots Table

```sql
CREATE TABLE IF NOT EXISTS snapshots (
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    last_sequence  BIGINT NOT NULL,
    payload        JSON NOT NULL,
    timestamp      TEXT NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id)
);
```

- **payload**: Aggregate state serialized via `serde_json::to_value(&aggregate)`
- **last_sequence**: The event sequence at the time of the snapshot
- **timestamp**: ISO 8601 timestamp of when the snapshot was taken

**Not currently enabled** -- all aggregates use `new_event_store`. To enable,
replace `new_event_store(repo)` with `new_snapshot_store(repo, N)` where `N` is
the snapshot frequency. Must reset snapshots after changing aggregate struct
layout. Deleting snapshots is always safe (replays from events).

```sql
-- Reset snapshots for an aggregate after struct changes
DELETE FROM snapshots WHERE aggregate_type = 'Mint';
```

### View Tables (Projections)

```sql
CREATE TABLE IF NOT EXISTS my_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);
```

- **view_id**: The aggregate ID string
- **version**: Event sequence number (used for optimistic locking)
- **payload**: The view serialized via `serde_json::to_value(&view)`

`SqliteViewRepository` stores views with `serde_json::to_value()` and loads them
with `serde_json::from_value()`.

### Lifecycle Serialization in View Payloads

All our aggregates use `Lifecycle<Entity>` (where `Entity: EventSourced`) as
both the aggregate and its own view (via the blanket `View` impl). Serde's
default externally-tagged enum representation means:

- `Lifecycle::Uninitialized` -> `"Uninitialized"`
- `Lifecycle::Live(data)` -> `{"Live": <data>}`
- `Lifecycle::Failed { error, last_valid_state }` -> `{"Failed": {...}}`

When `T` is a **struct** (e.g., `Position`, `OnChainTrade`), `<data>` is a flat
JSON object: `{"Live": {"symbol": "AAPL", "net": "0", ...}}`. JSON paths:
`$.Live.symbol`, `$.Live.net`.

When `T` is an **enum** (e.g., `OffchainOrder`, `UsdcRebalance`), `<data>` is
another tagged enum: `{"Live": {"Pending": {"symbol": "AAPL", ...}}}`. JSON
paths depend on the active variant and are unsuitable for generated columns. Use
`GenericQuery::load()` and deserialize in Rust instead.

### Generated Columns on Views

SQLite generated columns can extract fields from `payload` for indexing and
querying. Only appropriate for **struct-typed views** where the JSON path is
stable:

```sql
CREATE TABLE IF NOT EXISTS position_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL,
    symbol TEXT GENERATED ALWAYS AS (
        json_extract(payload, '$.Live.symbol')
    ) STORED
);
```

Generated columns on enum-typed views (the path changes per variant) should be
avoided in favor of using native cqrs-es tooling, e.g.`GenericQuery::load()`.

## Event Upcasters

When you MUST change event structure (e.g., adding required fields to existing
events), use upcasters to transform old events to the new format at load time:

```rust
use cqrs_es::persist::{EventUpcaster, SemanticVersionEventUpcaster};

// Transform function: takes old JSON, returns new JSON
fn upcast_v1_to_v2(mut payload: Value) -> Value {
    // Add new field with default value
    payload["new_field"] = json!("default");
    payload
}

// Create upcaster targeting specific event type and version
pub fn create_my_upcaster() -> Box<dyn EventUpcaster> {
    Box::new(SemanticVersionEventUpcaster::new(
        "MyAggregate::MyEvent",  // event_type to match
        "2.0",                    // target version (events < this get upcasted)
        Box::new(upcast_v1_to_v2),
    ))
}
```

Register upcasters on the event store:

```rust
let event_store = PersistedEventStore::new(event_repo)
    .with_upcasters(vec![create_my_upcaster()]);
```

**Update `event_version()` in your event enum** to return the new version for
new events.

## Views and GenericQuery

Views are read-optimized projections built from events. **Never query view
tables directly with raw SQL** -- use `GenericQuery`.

For `EventSourced` entities, `Lifecycle<Entity>` has a blanket `View` impl that
delegates to `originate` and `evolve`, so the entity itself serves as its own
view. Use the `SqliteQuery<Entity>` type alias (defined in `event_sourced.rs`)
for the query type:

```rust
use crate::event_sourced::SqliteQuery;

// SqliteQuery<Position> wraps
// GenericQuery<SqliteViewRepository<Lifecycle<Position>,
//     Lifecycle<Position>>>
let query: Arc<SqliteQuery<Position>> = /* built by CqrsBuilder */;

// Load view by aggregate ID
let view: Option<Lifecycle<Position>> =
    query.load(&symbol.to_string()).await;
```

For custom views (not the entity itself), implement the `View` trait on the
cqrs-es `Aggregate` type (`Lifecycle<Entity>`):

```rust
impl View<Lifecycle<MyEntity>> for MyCustomView {
    fn update(&mut self, event: &EventEnvelope<Lifecycle<MyEntity>>) {
        match &event.payload {
            MyEvent::Created { .. } => { /* update view */ }
            MyEvent::Updated { .. } => { /* update view */ }
        }
    }
}
```

## Re-projecting Views with QueryReplay

When you add a new view or need to rebuild an existing one from events, use
`QueryReplay`:

```rust
use cqrs_es::persist::QueryReplay;

pub async fn replay_my_view(pool: Pool<Sqlite>) -> Result<(), MyError> {
    let view_repo = Arc::new(SqliteViewRepository::<MyView, MyAggregate>::new(
        pool.clone(),
        "my_view".to_string(),
    ));
    let query = GenericQuery::new(view_repo);
    let event_repo = SqliteEventRepository::new(pool);

    let replay = QueryReplay::new(event_repo, query);
    replay.replay_all().await?;

    Ok(())
}
```

This replays ALL events through the view's `update()` method, rebuilding the
entire view from scratch. It's idempotent - running it multiple times produces
the same result.

**Call replay at startup** to ensure views are up-to-date with any schema
changes.

## Services Pattern

Domain types can depend on external services (APIs, blockchain, etc.) via the
`Services` associated type on `EventSourced`:

```rust
#[async_trait]
impl EventSourced for MyEntity {
    type Services = Arc<dyn MyService>;  // or () if none needed

    async fn transition(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let result = services.do_something().await?;
        Ok(vec![MyEvent::SomethingDone { result }])
    }
    // ...
}
```

Pass services when building the `Store`:

```rust
let services: Arc<dyn MyService> = Arc::new(MyServiceImpl::new());
let store: Store<MyEntity> = CqrsBuilder::<MyEntity>::new(pool)
    .build(services)
    .await?;
```

For entities that don't need services, use `type Services = ()`.

## Forbidden Patterns

### CRITICAL: Event Writes Are Strictly Controlled

1. **NEVER write directly to the `events` table** - this is STRICTLY FORBIDDEN:
   - **FORBIDDEN**: Direct INSERT statements into the `events` table
   - **FORBIDDEN**: Manual sequence number management for events
   - **FORBIDDEN**: Bypassing the framework to write events
   - **REQUIRED**: Always use `Store::send()` (or `CqrsFramework::execute()` in
     test code) to emit events through commands
   - **WHY**: Direct writes break aggregate consistency, event ordering, and
     violate the CQRS pattern. Events must be emitted through aggregate commands
     that generate domain events. The framework handles event persistence,
     sequence numbers, aggregate loading, and consistency guarantees.
   - **NOTE**: If you see existing code writing directly to `events` table, that
     code is incorrect and should be refactored to use CqrsFramework

### Other Forbidden Patterns

2. **Never query the `events` table directly with raw SQL** - use `EventStore`
   trait methods or the framework's query API
3. **Never query view tables with raw SQL** - use `GenericQuery::load()`
4. **Never modify or delete events** - they're immutable historical facts
5. **Never worry about changing aggregates/views** - they're just
   interpretations
6. **Never add events you don't need yet** - YAGNI applies especially to events

## Single Framework Instance Per Aggregate

**CRITICAL**: Each entity type must have exactly ONE `Store<Entity>` instance,
constructed once in `Conductor::start` via `CqrsBuilder`, then shared. The
`CqrsBuilder` enforces compile-time query wiring via type-level linked lists
(`Cons`/`Nil`), making it impossible to silently forget wiring a query
processor.

### Why This Matters

Multiple framework instances for the same entity cause **silent production
bugs**: events persist to the database, but query processors registered on OTHER
instances never see them. Views and projections go stale without any warnings
while the application continues operating as if everything worked.

### Rules

- **FORBIDDEN**: Calling `sqlite_cqrs()` or `CqrsFramework::new()` directly in
  the server binary -- use `CqrsBuilder` instead
- **FORBIDDEN**: Creating multiple `Store<Entity>` instances for the same entity
  type in the bot flow
- **REQUIRED**: Wire all query processors via `CqrsBuilder::wire()` before
  calling `build()`
- **ALLOWED**: Direct construction in test code (via `wire::test_cqrs()`), CLI
  code, and migration code (different execution contexts)

### Adding a New Query Processor

1. Wire it via `CqrsBuilder::wire()` in `Conductor::start`
2. Never create a new framework instance just to add a processor
3. The builder tracks wired queries at the type level -- if a query is required
   by the manifest but not wired, it's a compile-time error

## Testing Aggregates

Use `Lifecycle::default()` with `Aggregate::handle()` for command tests. Set up
prior state with `Aggregate::apply()`:

```rust
use cqrs_es::{Aggregate, View};

#[tokio::test]
async fn test_my_command() {
    let mut aggregate = Lifecycle::<MyEntity>::default();

    // Given: apply prior events to set up state
    aggregate.apply(MyEvent::Created { /* ... */ });

    // When: execute command under test
    let events = aggregate
        .handle(MyCommand::Update { /* ... */ }, &())
        .await
        .unwrap();

    // Then: verify emitted events
    assert!(matches!(events[0], MyEvent::Updated { .. }));
}
```

For view tests, use `View::update()` with `EventEnvelope`:

```rust
#[test]
fn test_view_updates() {
    let mut view = Lifecycle::<MyEntity>::default();
    view.update(&make_envelope("id", 1, MyEvent::Created { /* ... */ }));

    let Lifecycle::Live(entity) = view else {
        panic!("Expected Live state");
    };
    assert_eq!(entity.field, expected_value);
}
```

For error cases, verify the exact `LifecycleError` variant:

```rust
assert!(matches!(
    aggregate.handle(command, &()).await,
    Err(LifecycleError::Apply(MyError::SpecificVariant))
));
```
