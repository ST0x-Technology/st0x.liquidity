# CQRS/ES Patterns with cqrs-es

Quick reference for cqrs-es usage patterns in this codebase.

## Core Principle: Events Are Immutable

**Events are the source of truth and can NEVER be changed or deleted.**
Everything else - aggregates, commands, views - can be freely modified because
they're derived from events.

- **Commands**: Can add, remove, or change freely
- **Aggregates**: Can restructure, add fields, change logic freely
- **Views**: Can add, drop, restructure freely (just replay from events)
- **Events**: PERMANENT. Think carefully before adding new event types.

This is the power of event sourcing: unlimited flexibility in how you interpret
historical data, as long as you preserve the raw facts.

## sqlite-es Table Schemas

sqlite-es and cqrs-es mandate specific table schemas. All three tables are
created in the `event_store` migration.

### Events Table

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

- **aggregate_type**: From `Aggregate::aggregate_type()` (e.g., `"Position"`)
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

All our aggregates use `Lifecycle<T, E>` as both the aggregate and its own view
(via the blanket `View` impl). Serde's default externally-tagged enum
representation means:

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
tables directly with raw SQL** - use `GenericQuery`:

```rust
use cqrs_es::persist::GenericQuery;
use sqlite_es::SqliteViewRepository;

// Create view repository and query
let view_repo = Arc::new(SqliteViewRepository::<MyView, MyAggregate>::new(
    pool.clone(),
    "my_view".to_string(),
));
let query = GenericQuery::new(view_repo.clone());

// Load a view by aggregate ID
let view: Option<MyView> = query.load(&aggregate_id).await;
```

Views implement the `View` trait:

```rust
impl View<MyAggregate> for MyView {
    fn update(&mut self, event: &EventEnvelope<MyAggregate>) {
        match &event.payload {
            MyEvent::Created { ... } => { /* update view state */ }
            MyEvent::Updated { ... } => { /* update view state */ }
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

Aggregates can depend on external services (APIs, blockchain, etc.) via the
`Services` associated type:

```rust
#[async_trait]
impl Aggregate for MyAggregate {
    type Command = MyCommand;
    type Event = MyEvent;
    type Error = MyError;
    type Services = Arc<dyn MyService>;  // or () if no services needed

    async fn handle(
        &self,
        command: Self::Command,
        services: &Self::Services,  // injected by framework
    ) -> Result<Vec<Self::Event>, Self::Error> {
        // Use services in command handlers
        let result = services.do_something().await?;
        Ok(vec![MyEvent::SomethingDone { result }])
    }
}
```

Pass services when creating the CQRS framework:

```rust
let services: Arc<dyn MyService> = Arc::new(MyServiceImpl::new());
let cqrs = CqrsFramework::new(event_store, queries, services);
```

For aggregates that don't need services, use `type Services = ()`.

## Forbidden Patterns

### CRITICAL: Event Writes Are Strictly Controlled

1. **NEVER write directly to the `events` table** - this is STRICTLY FORBIDDEN:
   - **FORBIDDEN**: Direct INSERT statements into the `events` table
   - **FORBIDDEN**: Manual sequence number management for events
   - **FORBIDDEN**: Bypassing the CqrsFramework to write events
   - **REQUIRED**: Always use `CqrsFramework::execute()` or
     `CqrsFramework::execute_with_metadata()` to emit events through aggregate
     commands
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

**CRITICAL**: Each aggregate type must have exactly ONE `SqliteCqrs<A>`
instance, constructed once in `Conductor::start`, then shared via `Arc` clones.

### Why This Matters

Multiple `SqliteCqrs<A>` instances for the same aggregate cause **silent
production bugs**: events persist to the database, but query processors
registered on OTHER instances never see them. Views and projections go stale
without any warnings while the application continues operating as if everything
worked.

### Rules

- **FORBIDDEN**: Calling `sqlite_cqrs()` or `CqrsFramework::new()` in the server
  binary outside `Conductor::start`
- **FORBIDDEN**: Creating multiple `SqliteCqrs<A>` instances for the same
  aggregate type in the bot flow
- **REQUIRED**: Add all query processors to the single instance at construction
- **ALLOWED**: Direct construction in tests, CLI, and migration code (different
  execution contexts with intentionally different processor needs)

### Adding a New Query Processor

1. Add it to the query processor vector in `Conductor::start`
2. Never create a new framework instance just to add a processor
3. The framework registers processors at construction time -- the only way to
   ensure all events trigger all required side effects is to have every
   processor on the single instance from the start

## Testing Aggregates

Use the Given-When-Then pattern with in-memory stores:

```rust
use cqrs_es::mem_store::MemStore;

#[tokio::test]
async fn test_my_command() {
    let store = MemStore::<MyAggregate>::default();
    let cqrs = CqrsFramework::new(store, vec![], services);

    // Given: apply prior events
    cqrs.execute(&id, SetupCommand { ... }).await.unwrap();

    // When: execute command under test
    let result = cqrs.execute(&id, CommandUnderTest { ... }).await;

    // Then: verify result
    assert!(result.is_ok());
}
```

To verify aggregate state after executing commands, clone the store before
passing it to the framework and use `load_aggregate`:

```rust
let store = MemStore::<MyAggregate>::default();
let cqrs = CqrsFramework::new(store.clone(), vec![], services);

cqrs.execute(&id, MyCommand { ... }).await.unwrap();

let ctx = store.load_aggregate(&id).await.unwrap();
let aggregate = ctx.aggregate();
// assert on aggregate state
```

**FORBIDDEN**: Calling `Aggregate::handle()` + `Aggregate::apply()` manually in
tests. This bypasses the framework and doesn't test the real execution path. Use
`CqrsFramework::execute()` instead. The only exception is testing the `apply()`
method itself (e.g., verifying corruption detection when applying events to
invalid states).
