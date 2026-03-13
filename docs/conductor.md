# Conductor, the orchestration layer

The conductor module (`src/conductor/`) owns the bot's runtime lifecycle. It
composes two categories of work -- **long-running supervised tasks** and
**one-shot persistent jobs** -- into a unified orchestration layer.

## Long-running tasks (task-supervisor)

Continuous async tasks that run indefinitely and restart automatically on
failure with exponential backoff. Use these for streaming connections, polling
loops, and anything that should "always be running."

### SupervisedTask trait

```rust
pub trait SupervisedTask: Clone + Send {
    async fn run(&mut self) -> TaskResult;
    // TaskResult = Result<(), Box<dyn Error>>
}
```

The `Clone` bound enables restart semantics: the supervisor stores the original
instance and clones it for each attempt. Owned fields reset on restart; fields
behind `Arc` survive across restarts.

**Key design pattern**: create ephemeral resources (WebSocket connections, HTTP
clients) INSIDE `run()`, not as struct fields. Each restart establishes fresh
connections automatically.

### Supervisor lifecycle

`SupervisorBuilder` registers tasks by name:

```
SupervisorBuilder::default()
    .with_task("task-name", task)
    .build()    // -> Supervisor
    .run()      // -> SupervisorHandle
```

`SupervisorHandle` provides runtime control: `wait()`, `shutdown()`,
`get_task_status()`, `add_task()`, `restart()`, `kill_task()`.

### OrderFillMonitor

Defined in `src/conductor/order_fill_monitor.rs`. Subscribes to
ClearV3/TakeOrderV3 WebSocket streams and pushes each event into apalis storage
as an `OrderFillJob`.

```rust
struct OrderFillMonitor {
    ws_url: Url,                  // config -- cloned on restart
    orderbook: Address,           // config -- cloned on restart
    storage: OrderFillJobQueue,   // Arc-backed -- survives restarts
}
```

The WebSocket connection is created inside `run()`. On disconnect or error,
`run()` returns `Err(...)`, the supervisor restarts the task, and a fresh
connection is established.

### Candidates for migration

These tasks currently use `tokio::spawn` with no restart logic and are
candidates for supervisor migration (#421-#424):

| Task                 | Description                                  |
| -------------------- | -------------------------------------------- |
| Order poller         | Polls broker for offchain order status       |
| Position checker     | Periodic accumulated position reconciliation |
| Inventory poller     | Polls Raindex vault balances                 |
| Executor maintenance | Broker-specific maintenance (token refresh)  |
| Rebalancer           | USDC rebalancing between onchain/offchain    |

## One-shot jobs (apalis + Job trait)

Discrete units of work that are serialized to SQLite before processing and have
a defined point of completion. If the worker crashes or the process restarts,
unprocessed jobs are still in the database.

### Job trait

Defined in `src/conductor/job.rs`. Wraps apalis's function-based handler API
with a trait-based one:

```rust
pub(crate) trait Job<Ctx>: Serialize + DeserializeOwned + Send + 'static
where
    Ctx: Send + Sync + 'static,
{
    type Error: std::error::Error + Send + Sync + 'static;

    fn label(&self) -> Label;

    async fn execute(&self, ctx: &Ctx) -> Result<(), Self::Error>;
}
```

The `Ctx` type parameter bundles all runtime dependencies into one struct,
injected via apalis `Data<Arc<Ctx>>`. This keeps job structs serializable (data
only) while the context provides access to executor, CQRS frameworks, config,
etc. `label()` returns a human-readable `Label` used by `handle_job` for
structured logging.

### handle_job

Generic apalis handler that bridges `Job` implementations with apalis's
function-based worker API:

```rust
pub(crate) async fn handle_job<J, Ctx>(job: J, ctx: Data<Arc<Ctx>>)
where
    J: Job<Ctx>,
    Ctx: Send + Sync + 'static,
{
    // Errors are logged but not propagated -- apalis sees Ok(()) and
    // will not retry the job. This is intentional: jobs are responsible
    // for their own retry/recovery logic inside execute().
    if let Err(error) = job.execute(&ctx).await {
        error!(%error, "Job failed");
    }
}
```

### Wiring a job into apalis

```
WorkerBuilder::new(name)
    .backend(storage)         // SqliteStorage<MyJob>
    .data(ctx)                // Arc<MyCtx>
    .build(handle_job::<MyJob, MyCtx>)
```

The apalis `Monitor` wraps workers and restarts them on failure:

```
Monitor::new()
    .should_restart(|_ctx, _error, _attempt| true)
    .register(|index| { /* WorkerBuilder as above */ })
    .run().await
```

### OrderFillJob

Defined in `src/conductor/order_fill_monitor.rs`. Serializable wrapper around
`QueuedEvent`, pushed into `OrderFillJobQueue` by `OrderFillMonitor`.

Implements `Job<OrderFillCtx<P, E>>` in `src/conductor/order_fill_processor.rs`.
The `execute()` method runs the hedging pipeline:

1. `convert_event_to_trade` -- resolve symbol, price, direction
2. `discover_vaults_for_trade` -- register vaults in VaultRegistry
3. `process_queued_trade` -- record OnChainTrade, update Position, place
   offsetting broker order

### OrderFillCtx

Defined in `src/conductor/order_fill_processor.rs`. Bundles all dependencies the
job needs: config, symbol cache, EVM provider, CQRS frameworks, vault registry,
executor. Wrapped in `Arc` and injected via apalis `Data`.

## Conductor assembly

`ConductorBuilder` (`src/conductor/builder.rs`) uses a typestate pattern to
enforce assembly order: `Initial` -> `WithExecutorMaintenance` -> `spawn()`.
`ConductorCtx` bundles the shared dependencies (config, symbol cache, provider,
executor, CQRS frameworks, execution threshold) and `spawn()` wires everything
up, returning a `Conductor` with handles to every task.

`Conductor` lifecycle:

- `start()` -- sets up apalis storage, CQRS frameworks, backfill, then delegates
  to `ConductorBuilder::spawn()`
- `wait_for_completion()` -- `tokio::select!` across supervisor, apalis monitor,
  order poller, and position checker; returns when any exits
- `abort_trading_tasks()` -- shuts down supervisor + monitor + order poller +
  position checker; keeps rebalancer, inventory poller, and executor maintenance
- `abort_all()` -- aborts everything

## SQLite migration coexistence

apalis uses its own sqlx migrations for internal tables. Both migration sets
share the `_sqlx_migrations` table in the same SQLite database. We use
`setup_apalis_tables()` instead of `SqliteStorage::setup()`, which runs apalis
migrations with `ignore_missing(true)` so they tolerate our pre-existing
migration versions.
