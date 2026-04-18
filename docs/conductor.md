# Conductor, the orchestration layer

The conductor module (`src/conductor/`) owns the bot's runtime lifecycle. It
composes two categories of work -- **long-running supervised tasks** and
**one-shot persistent jobs** -- into a unified orchestration layer built on
apalis (job queues) and task-supervisor (streaming services).

See SPEC.md "Orchestration" section for the full architecture vision including
the Baton/Conductor split and future lifecycle workflows.

## Long-running tasks (task-supervisor)

Continuous async tasks that run indefinitely and restart automatically on
failure with exponential backoff. Use these for streaming connections and
anything that must maintain a persistent connection.

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
ClearV3/TakeOrderV3 WebSocket streams and pushes each event into the
`DexTradeAccountingJobQueue` as an `AccountForDexTrade` job.

```rust
struct OrderFillMonitor {
    ws_url: Url,
    orderbook: Address,
    job_queue: DexTradeAccountingJobQueue,
}
```

The WebSocket connection is created inside `run()`. On disconnect or error,
`run()` returns `Err(...)`, the supervisor restarts the task, and a fresh
connection is established.

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

    async fn perform(&self, ctx: &Ctx) -> Result<(), Self::Error>;
}
```

The `Ctx` type parameter bundles all runtime dependencies into one struct,
injected via apalis `Data<Arc<Ctx>>`. This keeps job structs serializable (data
only) while the context provides access to executor, CQRS frameworks, config,
etc. `label()` returns a human-readable `Label` used by `work` for structured
logging.

### work

Generic apalis handler that bridges `Job` implementations with apalis's
function-based worker API:

```rust
pub(crate) async fn work<Ctx, J>(job: J, ctx: Data<Arc<Ctx>>)
where
    Ctx: Send + Sync + 'static,
    J: Job<Ctx>,
{
    // Retries with exponential backoff, then logs on final failure.
}
```

### Wiring a job into apalis

```
WorkerBuilder::new(name)
    .backend(job_queue)       // SqliteStorage<MyJob, ...>
    .data(ctx)                // Arc<MyCtx>
    .build(work::<MyCtx, MyJob>)
```

The apalis `Monitor` wraps workers and restarts them on failure:

```
Monitor::new()
    .should_restart(|_ctx, _error, _attempt| true)
    .register(|index| { /* WorkerBuilder as above */ })
    .run().await
```

### AccountForDexTrade

Defined in `src/trading/onchain/trade_accountant.rs`. Serializable wrapper
around `EmittedOnChain<RaindexTradeEvent>`, pushed into
`DexTradeAccountingJobQueue` by `OrderFillMonitor`.

Implements `Job<AccountantCtx<Node, Exec>>`. The `perform()` method runs the
hedging pipeline:

1. Convert event to trade -- resolve symbol, price, direction
2. `discover_vaults_for_trade` -- register vaults in VaultRegistry
3. `process_queued_trade` -- record OnChainTrade, update Position, place
   offsetting broker order

### AccountantCtx

Defined in `src/trading/onchain/trade_accountant.rs`. Bundles all dependencies
the job needs: config, symbol cache, EVM provider, CQRS frameworks, vault
registry, executor. Wrapped in `Arc` and injected via apalis `Data`.

## Conductor assembly

`builder::spawn()` (`src/conductor/builder.rs`) uses `#[bon::builder]` to
construct a running `Conductor`. Required parameters: `ConductorCtx` (shared
dependencies), `DexTradeAccountingJobQueue`, `DexEventStreams`. Optional:
`executor_maintenance`, `rebalancer`.

`ConductorCtx` bundles the shared dependencies (config, symbol cache, provider,
executor, CQRS frameworks, execution threshold, wallet polling config).

`Conductor` lifecycle:

- `run()` -- the single entry point. Sets up apalis tables, CQRS frameworks,
  determines cutoff block, backfills historical events, then calls
  `builder::spawn()` to start the runtime
- `wait_for_completion()` -- `tokio::select!` across supervisor, apalis monitor,
  order poller, and position checker; returns when any exits
- `abort_all()` -- shuts down supervisor, aborts all task handles

## Startup sequencing

```
Phase 1 (parallel):  connect_ws | setup_cqrs | setup_apalis_tables
Phase 2 (parallel):  get_cutoff_block | seed_vaults | setup_rebalancing
Phase 3 (sequential): backfill historical events to job queue
Phase 4:              builder::spawn() starts the runtime
```

The trade accounting worker starts only after backfill completes. The WS
subscription is established in phase 1, so no events are missed -- they buffer
until the monitor starts.

## Error handling in jobs

> **Known issue**: the current design uses `Ok(())` for permanent business
> rejections to avoid retries. This conflates success with rejection. Tracked in
> [RAI-210](https://linear.app/makeitrain/issue/RAI-210/job-error-handling-dont-represent-business-rejections-as-ok).

Jobs return `Result<(), Self::Error>`. The `work` handler retries on `Err` with
exponential backoff (3 attempts by default). This means the error semantics of
`Job::perform` directly control retry behavior:

- **Return `Err`** for transient/infrastructure failures (DB errors, aggregate
  conflicts, network issues). The job will be retried.
- **Return `Ok(())`** for permanent business rejections where retrying would
  produce the same result (e.g., position already has a pending order, threshold
  no longer met).

### Matching CQRS errors

`Store::send()` returns `SendError<Entity>`, which is
`AggregateError<LifecycleError<Entity>>` from cqrs-es. The variants:

| Variant                                         | Meaning                     | Retry?               |
| ----------------------------------------------- | --------------------------- | -------------------- |
| `UserError(LifecycleError::Apply(DomainError))` | Domain rejected the command | Depends on variant   |
| `UserError(LifecycleError::EventCantOriginate)` | Lifecycle state machine bug | Yes (or investigate) |
| `UserError(LifecycleError::UnexpectedEvent)`    | Lifecycle state machine bug | Yes (or investigate) |
| `UserError(LifecycleError::AlreadyFailed)`      | Entity in failed state      | Yes (or investigate) |
| `AggregateConflict`                             | Optimistic locking conflict | Yes                  |
| `DatabaseConnectionError`                       | DB unavailable              | Yes                  |
| `DeserializationError`                          | Corrupt event data          | No (investigate)     |
| `UnexpectedError`                               | Unknown technical error     | Yes                  |

**Never blanket-match `UserError`.** Always match on the inner
`LifecycleError::Apply(specific_domain_error)` variants to distinguish expected
business rejections from lifecycle bugs. Only the specific domain error variants
that represent permanent, expected conditions should return `Ok(())`.

## SQLite migration coexistence

apalis uses its own sqlx migrations for internal tables. Both migration sets
share the `_sqlx_migrations` table in the same SQLite database. We use
`setup_apalis_tables()` instead of `SqliteStorage::setup()`, which runs apalis
migrations with `ignore_missing(true)` so they tolerate our pre-existing
migration versions.
