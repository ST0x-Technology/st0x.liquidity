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

**Key design pattern**: create ephemeral, connection-bound resources INSIDE
`run()`, not as struct fields, so each restart establishes them fresh. Cheaply
cloneable, reconnect-tolerant handles (e.g. an HTTP provider) may be held as
owned fields -- they reset to the stored clone on restart.

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

Defined in `src/conductor/monitor/order_fills.rs`. Drives continuous HTTP
`eth_getLogs` ingestion of `ClearV3`/`TakeOrderV3` fills. It is a supervised
interval task: every `order_fill_poll_interval` seconds it reads the chain's
latest block for the configured ingestion cutoff tag (set via `ingestion_cutoff`
in config; recommended value: `safe`), uses it as the cutoff, and enqueues a
`BackfillRange` job for `(checkpoint+1, cutoff)` (no block for the tag yet ->
nothing enqueued). The `backfill-worker` fetches the logs and pushes an
`AccountForDexTrade` job per fill, advancing the persisted checkpoint only on
success. The cutoff tag is unrelated to `required_confirmations`, which governs
only transaction-submission paths.

`ingestion_cutoff = "safe"` (recommended): On OP Stack chains like Base, `safe`
is the latest L2 block whose sequencer batch has been posted to L1 -- typically
only a few blocks behind the chain tip. Cuts hedging lag from ~20 min to
~seconds. Tradeoff: a sufficiently deep L1 reorg dropping the batch tx before
finalization could invalidate a safe-ingested fill; no reversal path exists
today.

`ingestion_cutoff = "finalized"` (strict): Uses
`eth_getBlockByNumber("finalized")` (Casper FFG). Full reorg protection but ~20
min hedging lag on Base.

```rust
struct OrderFillMonitor<P> {
    evm_ctx: EvmCtx,
    backfill_queue: BackfillJobQueue,
    pool: SqlitePool,
    provider: P,
    poll_interval: Duration,
}
```

There is no WebSocket and no live subscription. A previous range still in flight
is skipped (the checkpoint has not advanced, so re-enqueuing would re-scan the
same blocks). Transient per-tick errors are logged and swallowed; the loop
retries on the next tick, and the supervisor restarts only on a panic. This
replaces the former WS `.watch()` filter polling -- see the module docstring for
why `eth_subscribe`/`subscribe_logs` was also rejected.

## One-shot jobs (apalis + Job trait)

Discrete units of work that are serialized to SQLite before processing and have
a defined point of completion. If the worker crashes or the process restarts,
unprocessed jobs are still in the database.

**Gotcha -- a job that was _in flight_ (`Running`/`Queued`) when the process
died is not auto-re-driven on a quick restart.** apalis's `fetch_next` only
picks `Pending`/retryable-`Failed` rows; an orphaned in-flight row is reset to
`Pending` only by apalis's `reenqueue_orphaned` sweep, which fires once the
owning worker's heartbeat ages past `reenqueue_orphaned_after` (5 min default).
Worker names are deterministic across restarts (`{WORKER_NAME}-{index}`), so a
fresh process re-registers the same worker id and keeps its heartbeat current --
the orphan never ages out, and any per-job enqueue dedup keyed off the in-flight
row then suppresses new work indefinitely. Jobs that must survive a crash
mid-execution reset their own orphaned rows at startup, before the monitor
spawns (where every `Running` row is by definition orphaned): see
`JobQueue::requeue_orphaned`, wired for the Base->Alpaca USDC transfer.

apalis also defaults to fetching multiple rows per poll and marking the whole
batch `Queued` before the single-concurrency worker can run them. A process kill
then loses the in-memory fetch buffer while the durable rows are no longer
`Pending`. Use `Config::set_buffer_size(1)` for these workers so SQLite
reservation matches actual handler execution.

### apalis Jobs table: status semantics and payload encoding

**Status lifecycle** (apalis-sqlite v1.0.0-rc.8, source-verified via
`fetch_next.sql` and `ack.sql`):

- `Pending`/`Queued`/`Running` -> in-flight; will be processed.
- `Done`/`Killed` -> terminal; will not be processed again.
- `Failed` -> **terminal only when `attempts >= max_attempts`**. A `Failed` row
  with `attempts < max_attempts` is STILL LIVE: `fetch_next.sql` re-selects it
  (`status='Failed' AND attempts < max_attempts`, ignoring `done_at`) and a
  polling worker will re-run it. `ack.sql` writes `Failed` in place without
  rescheduling; `done_at` being set does NOT make a `Failed` row terminal.

Dedupe/guard queries that want to detect all live rows must therefore use:
`status IN ('Pending', 'Queued', 'Running') OR (status = 'Failed' AND attempts < max_attempts)`.

**Payload encoding**: the `job` column is the JSON-serialized payload stored as
a SQL BLOB (apalis `JsonCodec`). Read it as `Vec<u8>` and parse with
`serde_json::from_slice`, or use `json_extract(job, '$.field')` in SQL. Decoding
directly as a Rust `String` fails at runtime: "Rust type String (as TEXT) is not
compatible with SQL type BLOB".

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
function-based worker API. Returns `Result<(), JobError>` so apalis can
distinguish success from failure.

```rust
pub(crate) async fn work<Ctx, J>(
    job: J, ctx: Data<Arc<Ctx>>,
) -> Result<(), JobError> {
    let label = job.label();
    info!(%label, "Processing job");
    job.perform(&ctx).await.map_err(|source| JobError::Failed {
        label,
        source: Box::new(source),
    })
}
```

### Worker middleware stack

Apalis workers use a Tower middleware stack configured on `WorkerBuilder`. The
layers are applied in order — outermost first:

```
WorkerBuilder::new(name)
    .backend(job_queue)
    .data(ctx)
    .concurrency(1)                                          // sequential processing
    .retry(RetryPolicy::retries(3).with_backoff(backoff))    // 1 + 3 = 4 attempts, with backoff
    .on_event(recovering_circuit_event)                      // pause, alert, and recover
    .build(work::<MyCtx, MyJob>)
```

**Layer roles:**

- **`.concurrency(1)`** — serializes job processing. Without it,
  `CallAllUnordered` processes jobs in parallel and a failing job can't prevent
  the next job from starting.
- **`.retry(RetryPolicy::retries(3).with_backoff(RETRY_BACKOFF))`** — retries
  failed jobs (replaces backon in the handler). `retries(3)` = 4 total attempts.
  Every queued task carries the same four-attempt durable SQL limit, so one
  exhausted job produces exactly one terminal worker event. `RETRY_BACKOFF` is a
  deterministic exponential backoff (1s base, doubles each attempt, capped at
  30s) so transient failures (RPC blips, broker rate limits) don't fast-fail
  into the recovering circuit. No jitter -- single-worker queues don't thunder.
- **`.on_event(recovering_circuit_event)`** — observes terminal `Event::Error`
  and successful `Event::Success` outcomes. The first consecutive terminal
  failure opens the local worker circuit, pauses that worker, emits a structured
  transition, and sends an operator alert. After a five-minute production
  cooldown, a scheduled task closes the circuit and resumes the same worker
  without restarting the process. A success resets the consecutive-failure
  count. Tests use a short cooldown.

Best-effort workers do not install the recovering circuit. Their terminal
failures are logged and retained in the durable queue, but do not pause the
worker.

### Error propagation: handler failure -> isolated recovery

1. `work()` returns `Err` -> retry layer retries
2. Retries exhaust -> apalis persists the terminal job and emits `Event::Error`
3. `on_event` opens the local circuit and pauses only that worker
4. A structured `opened` transition and operator alert identify the worker and
   failure
5. The conductor and sibling workers remain running
6. After the cooldown, the scheduled recovery resumes the worker and emits a
   structured `recovered` transition

### Monitor configuration

```
Monitor::new()
    .should_restart(|_ctx, _error, _attempt| false)
    .register(|index| { /* WorkerBuilder as above */ })
    .run().await
```

`should_restart(false)` prevents apalis from spawning a replacement worker after
an unexpected worker exit. Terminal job failures do not exit the worker: the
recovering event handler owns its pause and resume lifecycle.

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
the job needs: config, symbol cache, configured Pyth feed IDs (`PythFeedIds`),
EVM provider, orderbook address, CQRS frameworks, vault registry, executor,
database pool, and job queue. Wrapped in `Arc` and injected via apalis `Data`.

### CheckPositions

Defined in `src/position_check.rs`. A durable, self-rescheduling apalis job that
replaced the former supervised position-polling task. A single instance is
enqueued at startup; each run scans all positions from the `Position`
projection, skips symbols with active equity transfers or an already-claimed
pending order, and enqueues an independent `PlaceHedge` job for every symbol
whose net exposure has crossed the execution threshold. Per-symbol scan errors
are logged and swallowed so one symbol's failure cannot block the others; only
failures of the loop itself propagate. After each scan the job re-enqueues
itself with a delay of the configured `position_check_interval`.

Each tick also re-drives orders stuck `Pending` between broker acceptance and
the outcome commit (ADR 0014), serialized against live placements via the shared
counter-trade submission lock.

### PlaceHedge

Defined in `src/trading/offchain/hedge.rs`. Enqueued by `CheckPositions` (one
job per ready symbol) into the `HedgeJobQueue`. Implements `Job<HedgeCtx>`. The
`perform()` method places the offsetting broker order via the `OrderPlacer`
service and rolls the position back if the broker rejects. The
`offchain_order_id` is generated at enqueue time, not inside `perform()`, so
retries reuse the same ID -- a crash between claiming the position and placing
the order cannot strand the position with a pending ID no retry can claim.

During an Extended market session, only symbols with
`extended_hours_counter_trading = enabled` place (limit) orders, priced with the
configured `counter_trade_slippage_bps` buffer; disabled symbols skip.

## Conductor assembly

`builder::spawn()` (`src/conductor/builder.rs`) uses `#[bon::builder]` to
construct a running `Conductor`. Takes a `ConductorCtx` (shared dependencies)
plus per-subsystem job queues, schedulers, and optional handles for rebalancing
and executor maintenance.

`ConductorCtx` bundles the shared dependencies (config, symbol cache, provider,
executor, CQRS frameworks, pool, execution threshold, wallet polling config,
optional `tokenizer: Option<Arc<dyn Tokenizer>>`, shutdown token).

`Conductor` lifecycle:

- `run()` -- the single entry point. Connects the HTTP provider, sets up apalis
  tables and CQRS frameworks, seeds the vault registry, requeues orphaned jobs,
  then calls `builder::spawn()` to start the runtime
- `wait_for_completion()` -- `tokio::select!` across supervisor, apalis monitor,
  and periodic job cleanup (see periodic cleanup below); returns when any exits
- `abort_all()` -- shuts down supervisor, aborts all task handles

## Startup sequencing

```
Phase 1: connect_http (with RPC probe) | setup_apalis_tables | build CQRS stores
Phase 2: seed_vault_registry (inline, must complete before downstream wiring)
Phase 3: setup_rebalancing (optional) | requeue_orphaned jobs | hydrate inventory |
         recover pending orders | bootstrap configured inventory source jobs
Phase 4: builder::spawn() starts supervisor + apalis workers
```

There is no WebSocket and no pre-runtime backfill pass. `Conductor::run()`
creates a single HTTP provider before spawn; `OrderFillMonitor` clones it and
uses it for each poll tick.

Vault registry seeding (`SeedVaultRegistry`) runs inline during Phase 2 so that
`RaindexService`, trade accounting, and inventory polling start with a populated
registry. The same `SeedVaultRegistry` job is also registered as an apalis
worker so the queue can retry on failure if seeding is re-triggered later (e.g.
from a recovery flow).

Inventory polling is eight independent durable job types rather than one
conductor sleep loop: inflight equity, onchain equity, onchain USDC, Ethereum
wallet USDC, Base wallet USDC, Base unwrapped equity, Base wrapped equity, and
offchain inventory. Startup removes abandoned active rows for all eight types,
then enqueues one immediate job for each configured source. Tokenizer and wallet
jobs are omitted when the corresponding capability or token set is absent. Each
job schedules exactly one source-local successor at the configured interval even
after a read or snapshot failure, so one unavailable integration does not stall
the other inventory observations.

Seeding is additive for vault discovery history but authoritative for the
configured primary vault. Each startup registers every configured vault ID and
then marks the first entry in the configured vault list (config file order) for
each asset as primary. A config change from an old vault ID to a new one
therefore moves deposit/withdraw/rebalancing paths to the new vault after
restart, while the old vault remains registered so inventory polling can surface
any stranded balance.

Ingestion is checkpoint-driven `eth_getLogs` polling, not a live subscription,
so no events are missed across downtime. Reading the ingestion cutoff block (tag
configured via `ingestion_cutoff`; `safe` is the recommended value) is not a
startup phase -- the `OrderFillMonitor` poll loop reads the latest cutoff block
every tick and enqueues a `BackfillRange` job for the gap since the persisted
checkpoint. The backfill and trade-accounting workers start together in Phase 4;
catch-up backfill runs continuously after spawn while the monitor always resumes
from the persisted checkpoint and re-scans any gap.

Backfill reads the last successful checkpoint from SQLite. The configured
`deployment_block` seeds only the first run; subsequent runs start at
`checkpoint + 1`. The checkpoint advances only after the full requested range
has been enqueued successfully.

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
