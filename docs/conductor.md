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
`serde_json::from_slice`, or use `json_extract(CAST(job AS TEXT), '$.field')` in
SQL. Decoding directly as a Rust `String` fails at runtime: "Rust type String
(as TEXT) is not compatible with SQL type BLOB". Prefer the `CAST` form even
where the bare form happens to work on the SQLite build in hand: it states the
JSON-text intent explicitly rather than relying on SQLite's own BLOB-vs-JSONB
auto-detection, and it is pinned by
`json_extract_reads_the_offchain_order_id_from_a_real_pushed_job`
(`src/offchain/order/poll_status.rs`), which asserts the exact extracted value
against a real apalis-pushed job rather than merely checking it is non-NULL.

**Per-order dedup guard, keyed on payload contents (RAI-1493)**: a periodic
recovery sweep that unconditionally re-pushes a job for every open item, atop a
job that self-reschedules on every non-terminal poll, is a documented
combination to watch for: each tick that finds an item still open forks a
brand-new, independent, self-perpetuating chain in addition to whatever chain(s)
already exist for it, so the live population grows without bound the longer the
item stays open. This bit `PollOrderStatus`: `recover_submitted_offchain_orders`
(`src/offchain/order/poll_status.rs`) polled every `position_check_interval`
(~60s) for each non-terminal offchain order and pushed a new poll job every
time, while `PollOrderStatus::perform` independently self-rescheduled via
`reschedule_self` on every non-terminal broker response -- production
accumulated tens of thousands of `Pending` rows for orders that stayed open for
hours. The fix (`reconcile_live_poll_jobs`, wrapped by
`reconcile_and_check_live_poll_job` and consolidated for every push site by
`push_poll_job_if_absent`) is an application-layer check before the push: query
whether a live row already exists for that specific order id via
`json_extract(CAST(job AS TEXT),
'$.offchain_order_id')`, and skip the push if
so. Despite reading like a pure predicate, this can also write: when more than
one `Pending` row already exists for the order (a pre-existing duplicate from
before this fix), it atomically collapses every non-survivor row to `Done` as a
side effect before reporting `true` -- every call site treats this as a
query-with-a-possible-write, not a read-only check.

That periodic sweep was not the only unconditional push site: an accounted
onchain fill for a symbol with a live `pending_offchain_order_id`
(`conductor.rs`'s `dispatch_post_place_state`, reached via
`reconcile_existing_pending_order` on every such fill, not only on placement),
the `PendingExecution` retry path (`recover_claimed_offchain_order`), the hedge
job's own `PendingExecution` recovery (`recover_pending_poll_status`), and that
recovery's own `Pending` re-drive follow-up (`route_placement_outcome`, shared
with the hedge job's primary placement path) each pushed unconditionally too --
an order that stayed open while its symbol kept trading could fork one new chain
per fill even with the periodic sweep guarded, and a concurrent recovery attempt
racing the primary placement path could fork one via `route_placement_outcome`
alone. All five sites now share one guard through `push_poll_job_if_absent`, so
at most one live `PollOrderStatus` row exists per order at any time, regardless
of which of the five pushed it. `dispatch_post_place_state` and
`route_placement_outcome` in particular are each reached both by a genuine new
placement (whose fresh `offchain_order_id` can never already have a poll job, so
the guard is a no-op there) and by a reconciliation/recovery path against a
possibly-already-`Submitted` order (where the guard is what actually matters) --
neither distinguishes its two callers, gating on the guard's answer instead.

A DB-enforced partial `UNIQUE` index over the same predicate was considered and
rejected on two grounds. First, `JobQueue::requeue_orphaned` (called from
`setup_trading_job_queues` on every boot, `src/conductor/trading_queues.rs`)
resets every orphaned `Running`/`Queued` row of a job type back to `Pending`
unconditionally on the boot path. Second, apalis's own orphan sweep
(`reenqueue_orphaned.sql`, apalis-sqlite 1.0.0-rc.8) -- the same sweep this
doc's earlier Gotcha describes -- resets a `Running`/`Queued` row only once its
owning worker's heartbeat ages past `reenqueue_orphaned_after` (300s default,
apalis-sql 1.0.0-rc.9 `src/config.rs`), swept every `keep_alive` tick (30s, same
file); as that Gotcha notes, deterministic worker names keep the heartbeat
current in practice, so this sweep almost never fires here. The normal steady
state is exactly one `Running` row plus its one `Pending` successor for the same
order, so either bulk update, were it to run, would promote the `Running` row
into the indexed set where its successor already sits, hitting a `UNIQUE`
violation that fails `Conductor::run()` on the boot path, or -- in the rare case
apalis's own sweep does fire -- silently stops that worker's beat stream,
reproducing the exact "worker silently stops" signature the fix was meant to
close. A plain `SELECT`-before-`push` has no such failure mode: it never writes
into apalis's own bulk-update paths.

The guard's live-row predicate bounds a retryable-`Failed` row
(`attempts < max_attempts`) by `done_at` freshness rather than treating it as
either unconditionally live or unconditionally excluded. Per the Status
lifecycle above, such a row is a live, immediately re-dispatchable chain head --
`ack.sql` never reschedules `run_at` on a `Failed` ack, so `fetch_next.sql`
picks it straight back up the moment a worker is free -- so counting it as live
is what actually prevents recovery from forking a second chain alongside the one
apalis is about to re-run. But a row stuck behind a latched worker (e.g. a
circuit-breaker fault, RAI-1495) would otherwise sit `Failed` forever without a
fresh `ack.sql` write, and `done_at` IS refreshed on every ack -- so bounding by
`done_at > now - stale_after` (the same staleness bound as the
`Queued`/`Running` arm below) gives both properties: a just-failed row counts as
live, but one that has sat `Failed` past `stale_after` without a fresh ack stops
suppressing recovery.

The predicate also bounds the `Queued`/`Running` arm by staleness
(`lock_at > now - stale_after`, `stale_after` a small multiple of the poll
interval): an unbounded `status IN ('Queued', 'Running')` check treats a
stranded row (the "Gotcha" above -- a dropped in-memory fetch buffer, a
cancelled task, a latched worker) as proof polling is armed forever, since
nothing else ever ages it out. Bounding it means a stranded row eventually stops
blocking recovery's re-push, at the cost of not counting a `Queued`/ `Running`
row that is still genuinely in flight but slow (rare; broker polls are a single
HTTP round-trip).

Beyond gating the push, `reconcile_live_poll_jobs` also atomically collapses
pre-existing duplicate chains: when more than one `Pending` row exists for the
same order (the population an unguarded recovery tick could already have forked
before this fix), it keeps the row apalis's own dispatch order
(`queries/backend/fetch_next.sql`, apalis-sqlite 1.0.0-rc.8:
`ORDER BY priority DESC, run_at ASC, id ASC`) would run first, and marks the
rest `Done` (with `done_at` set), converging back to one live row per order over
a small number of ticks. This is one `UPDATE` whose `WHERE` clause selects the
survivor via a subquery, not a `SELECT` to pick a survivor followed by a
separately-predicated `UPDATE`: SQLite evaluates the whole statement, subquery
included, as one atomic unit under its own write lock, so there is no window
between "decide the survivor" and "collapse the rest" for a concurrent writer
(another guard call racing this one, or `reschedule_self` pushing a legitimate
successor) to land in -- it either commits before the statement starts, and is
included in the survivor decision, or after the statement ends, and is left
untouched, never observed half-written mid-decision. An earlier two-statement
version of this guard captured a survivor id from a separate `SELECT`, then
re-ran `id != <that id>` in a later `UPDATE`; a successor landing in the gap
between them matched that stale predicate and was collapsed too, leaving the
order with zero live rows despite the guard reporting `true`. This
single-statement collapse is always safe to run, even while a `Queued`/`Running`
row for this order is still fresh: it only ever inspects/touches `Pending` rows,
so a currently-executing row is never a candidate, and if its `reschedule_self`
successor has not landed yet there is nothing else to collapse (a lone `Pending`
row is trivially its own survivor).

Apalis's own `ORDER BY` only ever ranks rows its `WHERE` clause has already made
eligible (`run_at IS NULL OR run_at <= strftime('%s', 'now')`), so it never
trades a due row for a not-yet-due one regardless of priority; this guard's
candidate set is deliberately wider than that (every `Pending` row, not just due
ones, since the normal steady-state successor `reschedule_self` pushes is one
poll interval in the future and excluding it would make the guard return `false`
and push a duplicate), so textually replaying apalis's `ORDER BY` alone over
that wider set would diverge whenever a due row and a higher-priority
not-yet-due row coexist. The survivor query therefore ranks due-ness first
(`(run_at IS NULL OR run_at <= strftime('%s', 'now')) DESC`), then
`priority DESC, run_at ASC, id ASC` -- the same tie-break apalis applies among
rows it would actually consider dispatching, and still needed since
`Jobs.run_at` is epoch-_seconds_, so rows pushed within the same second need
apalis's own `id ASC` tie-break -- SQLite's row order among ties is otherwise
unspecified. `Done`, not `Killed`, matches `JobQueue::cancel_all_pending`'s
precedent for discarding superseded queue rows -- this is routine dedupe, not
the non-retryable abort that `load_job_queue_health`'s operator-facing `killed`
counter exists to surface.

Every predicate above also requires `json_valid(CAST(job AS TEXT))` alongside
the `json_extract` equality check: the order-id comparison does not guarantee
`json_extract` is skipped for a row it does not match, and `json_extract` raises
a hard SQL error (not NULL) on a `job` blob that is not valid JSON at all (a
genuine corruption or foreign codec, not the known `X'6E756C6C'` poison rows --
the text `null` is valid JSON). Without the `json_valid` guard, one such row
would fail the guard query -- and, on the boot path, propagate through
`recover_submitted_offchain_orders` to fail `Conductor::run()` -- for every
order sharing this job type, not just the corrupt row's own.

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
    .break_circuit_with(fail_stop_config)                    // halt on terminal failure
    .on_event(|ctx, event| { ... })                          // observability + lifecycle
    .build(work::<MyCtx, MyJob>)
```

**Layer roles:**

- **`.concurrency(1)`** — serializes job processing. Without it,
  `CallAllUnordered` processes jobs in parallel and a failing job can't prevent
  the next job from starting.
- **`.retry(RetryPolicy::retries(3).with_backoff(RETRY_BACKOFF))`** — retries
  failed jobs (replaces backon in the handler). `retries(3)` = 4 total attempts.
  `RETRY_BACKOFF` is a deterministic exponential backoff (1s base, doubles each
  attempt, capped at 30s) so transient failures (RPC blips, broker rate limits)
  don't fast-fail into the circuit breaker. No jitter -- single-worker queues
  don't thunder.
- **`.break_circuit_with(config)`** — opens the circuit after
  `failure_threshold` errors, returning `Poll::Pending` from `poll_ready` to
  block new job pickup. Use `failure_threshold(1)` + very long
  `recovery_timeout` for fail-stop. Do not install this layer on best-effort
  workers: in apalis-core 1.0.0-rc.9, an open circuit can return `Poll::Pending`
  without scheduling a wakeup, so a short `recovery_timeout` does not guarantee
  the worker will resume.
- **`.on_event()`** — fires on `Event::Error` (after retries exhaust),
  `Event::Success`, `Event::Start`, `Event::Stop`. Use for logging AND for
  calling `ctx.stop()` on terminal failure. The circuit breaker alone only
  pauses the worker (`Poll::Pending`); `ctx.stop()` is needed to actually make
  the worker exit.

### Error propagation: handler failure -> bot shutdown

1. `work()` returns `Err` -> retry layer retries
2. Retries exhaust -> error reaches circuit breaker -> circuit opens
3. Error becomes `Ok(Event::Error)` in apalis `poll_tasks`
4. `on_event` catches `Event::Error`, fires the shared `failure_notify`
   (`tokio::sync::Notify`), and calls `ctx.stop()`
5. Worker exits cleanly (`Ok(())`)
6. The spawned monitor task's biased `tokio::select!` observes the Notify and
   returns `Err(MonitorTaskError::TerminalJobFailure)` immediately -- it does
   not wait for `apalis_monitor.run()` to wind down. The conductor's
   `wait_for_completion` sees the monitor task exit with that error and shuts
   the bot down.

**Critical:** the spawned monitor task must select on the shared Notify
alongside `apalis_monitor.run()` and return the terminal error. Without the
Notify branch, the conductor would only learn of the failure once apalis
finished tearing down all workers; without returning the error, the conductor
would never see the failure at all.

### Monitor configuration

```
Monitor::new()
    .should_restart(|_ctx, _error, _attempt| false)
    .register(|index| { /* WorkerBuilder as above */ })
    .run().await
```

`should_restart(false)` — terminal job failure is fail-stop. The worker must not
restart and process the next job with stale state.

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
         recover pending orders
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
