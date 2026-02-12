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

## Snapshots

Performance optimization that caches aggregate state to skip replaying old
events. **Not currently enabled** — all aggregates use `new_event_store`.

**Enabling:** Replace `new_event_store(repo)` with `new_snapshot_store(repo, N)`
where `N` is the snapshot frequency (events between snapshots). On load, replays
only events after the last snapshot. On commit, writes a new snapshot when the
event count crosses a frequency boundary. Switching between `new_event_store`
and `new_snapshot_store` is safe **only when** existing snapshots are compatible
with the current aggregate shape — if you've changed the aggregate's struct
layout (fields, variants) since the last snapshot was written, you must reset
snapshots first to avoid deserialization failures.

**Resetting:** Deleting snapshots is safe anytime — the next load replays all
events from the beginning. **Must** reset after changing an aggregate's struct
layout (fields, variants) since the serialized snapshot won't deserialize
against the new shape. Events are unaffected.

```sql
-- Example: reset snapshots for the Mint aggregate
DELETE FROM snapshots WHERE aggregate_type = 'Mint';
```

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

1. **Never query the `events` table directly** - use `EventStore` trait methods
2. **Never query view tables with raw SQL** - use `GenericQuery::load()`
3. **Never modify or delete events** - they're immutable historical facts
4. **Never worry about changing aggregates/views** - they're just
   interpretations
5. **Never add events you don't need yet** - YAGNI applies especially to events

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

Or for more direct testing, use `AggregateContext`:

```rust
let ctx = store.load_aggregate(&id).await.unwrap();
let aggregate = ctx.aggregate();
// assert on aggregate state
```
