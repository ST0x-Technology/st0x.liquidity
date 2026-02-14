# Conductor

Implementation reference for the orchestration layer. Read this before working
on the conductor (startup, supervision, job registration, worker wiring).

For the architectural overview and job processing flows, see
[SPEC.md](../SPEC.md) under "Orchestration Architecture".

## Two-layer architecture

```
task-supervisor (outermost - runs indefinitely, restarts crashed tasks)
  |-- DEX WebSocket listener
  +-- apalis Monitor (the entire worker pool)

apalis (work layer - finite jobs with persistent queue)
  |-- ProcessOnchainEvent        (persistent, event-driven)
  |-- PlaceOffchainOrder         (persistent, event-driven)
  |-- EnrichOnchainTrade         (persistent, event-driven)
  |-- ExecuteRebalancing         (persistent, event-driven)
  |-- ExecuteMint                (persistent, event-driven)
  |-- ExecuteRedemption          (persistent, event-driven)
  |-- PollOrderStatus            (cron, 5s)
  |-- CheckAccumulatedPositions  (cron, 10s)
  |-- PollInventory              (cron, 60s)
  |-- ExecutorMaintenance        (cron, 30min)
  +-- ComputeAnalytics           (cron, future)
```

**task-supervisor** handles anything that must run indefinitely. Each task
implements `SupervisedTask` and is registered with a `SupervisorBuilder`. The
supervisor restarts crashed tasks with exponential backoff. The DEX WebSocket
listener has its own internal reconnect logic for transient failures; the
supervisor handles catastrophic failures (panics, unexpected exits).

**apalis** handles finite jobs. It gives us retry with backoff, persistence
across restarts, ordered execution within a worker, and visibility into job
state - all backed by the same SQLite database the rest of the system uses.

The bridge: supervised tasks detect events and push typed apalis jobs to the
persistent queue.

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
SupervisorBuilder::default()
    .with_task("dex-websocket", ws_listener)
    .with_task("apalis-monitor", monitor_task)
    .with_unlimited_restarts()
    .with_base_restart_delay(Duration::from_secs(5))
    .with_max_backoff_exponent(5)        // caps at 5s * 2^5 = 160s
    .with_health_check_interval(Duration::from_millis(200))
    .with_task_being_stable_after(Duration::from_secs(80))
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

[apalis](https://github.com/apalis-dev/apalis) (v1.0.0-rc.4) is a tower-based
job processing framework.

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
dependencies (CQRS instances, backend handles, view repos) as a single struct:

```rust
#[derive(Clone)]
struct PlaceOffchainOrderCtx {
    position_cqrs: Arc<SqliteCqrs<Position>>,
    offchain_order_cqrs: Arc<SqliteCqrs<OffchainOrder>>,
    broker: Arc<dyn Executor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PlaceOffchainOrder { symbol: Symbol, shares: Shares }

impl Job for PlaceOffchainOrder {
    type Ctx = PlaceOffchainOrderCtx;
    type Error = PlaceOffchainOrderError;

    fn label(&self) -> Label { Label::new("place-offchain-order") }

    async fn run(self, ctx: Data<Self::Ctx>) -> Result<(), Self::Error> {
        // ...
    }
}
```

### Worker registration

```rust
let monitor = Monitor::new();

monitor.register(
    WorkerBuilder::new("place-offchain-order")
        .data(place_order_ctx)
        .backend(storage.clone())
        .build_fn(PlaceOffchainOrder::run),
);

monitor.register(
    WorkerBuilder::new("poll-order-status")
        .data(poll_ctx)
        .backend(cron_storage)
        .build_fn(PollOrderStatus::run),
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
2. **Build CQRS frameworks** - one `SqliteCqrs<A>` per aggregate, with query
   processors and Services wired in
3. **Build apalis workers** - one `WorkerBuilder` per job type, each with its
   `Ctx` and backend
4. **Build apalis Monitor** - register all workers
5. **Build supervised tasks** - wrap the Monitor and the DEX WebSocket listener
   as `SupervisedTask` impls
6. **Start supervisor** -
   `SupervisorBuilder::default().with_task(...).build()
   .run()` returns a
   `SupervisorHandle`
7. **Wait** - `handle.wait()` blocks until shutdown

The supervisor runs both tasks concurrently. The WebSocket listener pushes jobs
to the persistent queue as blockchain events arrive. The apalis Monitor
processes them through the registered workers.
