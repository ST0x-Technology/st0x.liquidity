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

Defined in `src/conductor/monitor/order_fills.rs`. Subscribes to
ClearV3/TakeOrderV3 WebSocket streams and pushes each event into the
`DexTradeAccountingJobQueue` as an `AccountForDexTrade` job.

```rust
struct OrderFillMonitor {
    ws_url: Url,
    orderbook: Address,
    job_queue: DexTradeAccountingJobQueue,
    dex_streams: Arc<Mutex<Option<DexEventStreams>>>,
}
```

On first `run()`, the monitor consumes the pre-established `DexEventStreams`
passed in at construction (avoiding a gap between the WS subscription used for
`get_cutoff_block` and the monitor's first iteration). On subsequent restarts,
it creates a fresh WebSocket connection. On disconnect or error, `run()` returns
`Err(...)` and the supervisor restarts the task.

### PositionMonitor

Defined in `src/conductor/monitor/positions.rs`. A long-running supervised task
that periodically scans all positions, filters by orchestration policy (trading
enabled, no equity transfers in progress), and enqueues durable `PlaceHedge`
jobs for any positions ready to hedge.

The check interval is configured via `position_check_interval`. Duplicate
enqueues are safe because the Position aggregate rejects `PlaceOffChainOrder`
when a pending order already exists.

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
the job needs: orderbook address, config, symbol cache, Pyth feed ID cache, EVM
provider, CQRS frameworks, vault registry, executor. Wrapped in `Arc` and
injected via apalis `Data`.

### PlaceHedge

Defined in `src/trading/offchain/hedge.rs`. Enqueued by `PositionMonitor` into
the `HedgeJobQueue` for positions that exceed their execution threshold.
Implements `Job<HedgeCtx>`. The `perform()` method places the offsetting offchain
order and updates the Position aggregate.

## Conductor assembly

`builder::spawn()` (`src/conductor/builder.rs`) uses `#[bon::builder]` to
construct a running `Conductor`. Required parameters: `ConductorCtx` (shared
dependencies), `DexTradeAccountingJobQueue`, `HedgeJobQueue`, `DexEventStreams`,
`job_cleanup`. Optional: `executor_maintenance`, `rebalancer`.

`ConductorCtx` bundles the shared dependencies (config, symbol cache, provider,
executor, CQRS frameworks, execution threshold, database pool, poll notifier,
wallet polling config, tokenizer).

`Conductor` lifecycle:

- `run()` -- the single entry point. Connects WebSocket, sets up apalis tables,
  determines cutoff block, backfills historical events, sets up CQRS frameworks,
  seeds vaults, configures rebalancing, then calls `builder::spawn()` to start
  the runtime
- `wait_for_completion()` -- `tokio::select!` across supervisor, apalis monitor,
  and job cleanup; returns when any exits
- `abort_all()` -- shuts down supervisor, aborts all task handles

## Startup sequencing

```
Phase 1 (sequential): connect_ws | setup_apalis_tables
Phase 2 (sequential): get_cutoff_block
Phase 3 (sequential): backfill checkpoint gap to job queue
Phase 4 (sequential): setup_cqrs | seed_vaults | setup_rebalancing
Phase 5:              builder::spawn() starts the runtime
```

The trade accounting worker starts only after backfill completes. The WS
subscription is established in Phase 1, so no events are missed -- they buffer
until the monitor starts.

Backfill reads the last successful checkpoint from SQLite. The configured
`deployment_block` seeds only the first run; subsequent runs start at
`checkpoint + 1`. The checkpoint advances only after the full requested range
has been enqueued successfully.

During Phase 4, when rebalancing is configured, the conductor also queries for
interrupted `TokenizedEquityMint` and `EquityRedemption` aggregates (those whose
latest event is non-terminal) and resumes them from their persisted state. This
ensures mints and redemptions that were in progress when the process crashed are
not left stuck indefinitely.

The conductor also runs periodic cleanup for terminal apalis jobs at the
configured `apalis_finished_job_cleanup_interval_secs` cadence. Those rows are
queue bookkeeping, while trade history lives in CQRS events and projections. The
cadence is required config and must be non-zero.

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
