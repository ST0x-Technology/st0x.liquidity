# Conductor

Implementation reference for the orchestration layer. Read this before working
on the conductor (startup, supervision, job registration, worker wiring).

For the architectural overview and job processing flows, see
[SPEC.md](../SPEC.md) under "Orchestration Architecture".

## Two-layer architecture

```
task-supervisor (outermost - runs indefinitely, restarts crashed tasks)
  |-- apalis Monitor (event processing worker pool)
  |-- Event receiver (WS -> SqliteStorage)
  |-- Order poller (interval-based)
  |-- Position checker (interval-based)
  |-- Inventory poller (interval-based, optional)
  |-- Executor maintenance (optional, one-shot)
  +-- Rebalancer (optional, one-shot)

apalis (work layer - persistent job queue)
  +-- ProcessOnchainEvent (persistent, event-driven)
```

**task-supervisor** handles anything that must run indefinitely. Each task
implements `SupervisedTask` and is registered with a `SupervisorBuilder`. The
supervisor restarts crashed tasks with exponential backoff.

Cron-like tasks (order poller, position checker, inventory poller) are
`SupervisedTask` implementations with an internal `tokio::time::interval` loop.
Pre-spawned tasks (executor maintenance, rebalancer) are wrapped in
`JoinHandleTask` which waits on the existing `JoinHandle`.

**apalis** handles persistent event processing. `ProcessOnchainEventJob` is the
only apalis job type. It provides retry with backoff, persistence across
restarts, and ordered execution - all backed by the same SQLite database the
rest of the system uses.

The bridge: the event receiver task receives blockchain events from WebSocket
streams and pushes `ProcessOnchainEventJob` instances to `SqliteStorage`. The
apalis Monitor picks them up and processes them.

## task-supervisor API

[task-supervisor](https://github.com/akhercha/task-supervisor) (v0.3.5) is a
Tokio task supervision library.

### SupervisedTask trait

```rust
#[async_trait]
trait SupervisedTask: Clone + Send + 'static {
    async fn run(&mut self) -> Result<(), anyhow::Error>;
}
```

The supervisor clones the task to spawn a fresh instance on restart. Tasks hold
their dependencies as plain struct fields (no DI framework).

### SupervisorBuilder

```rust
SupervisorBuilder::new()
    .with_task("order-poller", order_poller_task)
    .with_task("position-checker", position_checker_task)
    .with_task("apalis-monitor", monitor_task)
    .with_task("event-receiver", receiver_task)
    .with_unlimited_restarts()
    .with_base_restart_delay(Duration::from_secs(5))
    .build()
    .run()  // -> SupervisorHandle
```

Restart delay: `base_delay * 2^min(attempt, max_exponent)`. If a task stays
`Healthy` longer than the stability threshold, its restart counter resets to
zero.

### SupervisorHandle

Returned by `Supervisor::run()`. Cloneable. Auto-shuts down on drop.

- `add_task(name, task)` - add a task at runtime
- `restart(name)` - force restart
- `kill_task(name)` - permanently stop
- `get_task_status(name)` / `get_all_task_statuses()` - query status
- `shutdown()` - terminate everything
- `wait()` - block until supervisor exits

### TaskStatus

`Created -> Healthy -> (Failed -> Healthy)* -> Completed` or `-> Dead` (exceeded
restart limit).

## apalis API

[apalis](https://github.com/apalis-dev/apalis) (v0.7.4) is a tower-based job
processing framework.

### Job trait

Jobs implement a local `Job` trait that bundles each job's dependencies into a
single context type rather than using multiple `Data<T>` extractors:

```rust
trait Job:
    Serialize + DeserializeOwned + Send + Sync + Unpin + Debug + 'static
{
    type Ctx: Clone + Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    fn label(&self) -> Label;
    async fn run(self, ctx: Data<Self::Ctx>) -> Result<(), Self::Error>;
}
```

Each job is a serializable struct. Its `Ctx` associated type holds all
dependencies (CQRS instances, backend handles, caches) as a single struct:

```rust
#[derive(Clone)]
struct ProcessOnchainEventCtx<P, E> {
    pool: SqlitePool,
    ctx: Ctx,
    executor: E,
    provider: P,
    dual_write_context: DualWriteContext,
    cache: SymbolCache,
    feed_id_cache: Arc<FeedIdCache>,
    vault_registry_cqrs: Arc<SqliteCqrs<VaultRegistryAggregate>>,
}
```

### Worker registration

```rust
let monitor = Monitor::new().register(
    WorkerBuilder::new("process-onchain-event")
        .data(process_ctx)
        .backend(event_storage.clone())
        .build_fn(ProcessOnchainEventJob::run),
);
```

### Migration ordering

Both apalis and our application use sqlx migrations, and both write to the same
`_sqlx_migrations` table. Each migrator validates that every previously applied
migration exists in its own migration set - so running either migrator after the
other fails with `VersionMissing` unless both use `set_ignore_missing(true)`.

We cannot use `SqliteStorage::setup()` because it runs apalis migrations without
`ignore_missing`. Instead, get the apalis migrator directly:

```rust
SqliteStorage::<()>::migrations()
    .set_ignore_missing(true)
    .run(&pool)
    .await?;
sqlx::migrate!("./migrations")
    .set_ignore_missing(true)
    .run(&pool)
    .await?;
```

**CLI gotcha:** `sqlx migrate run` against a database that already has apalis
migrations will fail with `VersionMissing` because the CLI sees unknown entries
in `_sqlx_migrations`. Use `sqlx migrate run --ignore-missing`.

## Startup sequence

1. **Run migrations** - apalis migrations first, then app migrations (both with
   `set_ignore_missing(true)`)
2. **Connect WebSocket** - subscribe to ClearV3 and TakeOrderV3 event streams
3. **Backfill events** - determine cutoff block, backfill missed events
4. **Build CQRS frameworks** - one `SqliteCqrs<A>` per aggregate
5. **Build ProcessOnchainEventCtx** - bundles all processing dependencies
6. **Drain legacy event_queue** - migrate unprocessed events from the legacy
   `event_queue` table to apalis `SqliteStorage`
7. **Spawn Monitor and receiver** - Monitor processes jobs from storage;
   receiver pushes new blockchain events to storage
8. **Build supervised tasks** - wrap pollers, maintenance, and spawned handles
   as `SupervisedTask` impls
9. **Start supervisor** -
   `SupervisorBuilder::new().with_task(...).build().run()` returns a
   `SupervisorHandle`
10. **Wait** - `handle.wait()` blocks until shutdown

The `start()` function in `src/conductor/mod.rs` replaces the old `Conductor`
struct. It returns a `SupervisorHandle` that the caller waits on.
