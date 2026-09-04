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

Apalis' in-memory retry policy can emit `Event::Error` before SQLite terminality
(for example, after attempt 4 while `max_attempts` is 5). Best-effort terminal
alerting must therefore carry the row's attempt metadata and page only when
`attempts >= max_attempts`; retry-policy exhaustion alone is not a dead letter.

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
at most one independent `PollOrderStatus` chain is created per order regardless
of which of the five pushed it. The guard holds one process-local async mutex
across both the reconciliation/check and the apalis push; serializing only the
SQL statements is insufficient because concurrent callers can all observe no
live row before any caller's later push commits. This lock deliberately covers
all order ids: the critical section is one short SQLite check plus, only when
absent, one push. This guarantee is process-scoped: production runs exactly one
Conductor process as the exclusive owner of the SQLite database and its apalis
queue. Multiple Conductor processes sharing that queue are unsupported because
the process-local mutex cannot coordinate their check-and-push operations.
Startup recovery propagates the first guard or queue failure and prevents the
Conductor from starting with submitted orders known to be unpolled. The periodic
recovery sweep instead logs and counts each failed order, then continues arming
the remaining candidates so one order cannot block the rest.
`dispatch_post_place_state` and `route_placement_outcome` in particular are each
reached both by a genuine new placement (whose fresh `offchain_order_id` can
never already have a poll job, so the guard is a no-op there) and by a
reconciliation/recovery path against a possibly-already-`Submitted` order (where
the guard is what actually matters) -- neither distinguishes its two callers,
gating on the guard's answer instead. One `Running` row and its delayed
`Pending` successor may legitimately coexist for the same chain; the invariant
is one chain, not literally one live database row at every instant.

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
apalis is about to re-run. But a row stuck behind a stalled worker process (e.g.
the crashed-mid-restart window between a supervised fail-stop exit and systemd
bringing the process back up -- the circuit-breaker latch that used to strand a
worker indefinitely here, RAI-1495, no longer exists: supervised workers install
no circuit breaker at all, so `on_terminal_failure` reliably fires and the
process exits instead of latching) would otherwise sit `Failed` forever without
a fresh `ack.sql` write, and `done_at` IS refreshed on every ack -- so bounding
by `done_at > now - stale_after` (the same staleness bound as the
`Queued`/`Running` arm below) gives both properties: a just-failed row counts as
live, but one that has sat `Failed` past `stale_after` without a fresh ack stops
suppressing recovery.

The two predicates are separate and must not be collapsed. Apalis's own retry
eligibility is `attempts < max_attempts` alone -- `fetch_next.sql` ignores
`done_at` entirely, so a stale `Failed` row stays re-dispatchable as far as
apalis is concerned. The `done_at` freshness bound is an app-level recovery
guard layered on top, applied only to `Failed` rows, and it decides one thing
only: whether this recovery pass treats the row as already-armed polling. A row
excluded by staleness is not thereby dead to apalis; recovery just stops
assuming apalis will get to it.

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

### Broker rate-limit (429) backpressure: reschedule, don't retry in place (RAI-1494)

**Problem this fixes**: before RAI-1494, a classified or unclassified error from
an Alpaca call inside `Job::perform()` -- including an HTTP 429 -- went through
the unmodified `RetryPolicy::retries(3)` path. Three quick in-place retries
exhaust in seconds against a sustained rate-limit condition, tripping the
then-installed supervised circuit breaker and reproducing RAI-1492's "hedging
silently stops" incident (the breaker itself was removed by RAI-1495; see below
-- this section describes RAI-1494's own reasoning at the time, which remains
valid independent of that later change). Retrying _in place_ for longer (a naive
fix) is worse: it would hold the job's `concurrency(1)` worker slot for the
whole backoff window, blocking every other pending item behind it -- the same
"one item monopolizes the worker" failure shape, just invisible instead of a
loud fail-stop.

**Mechanism**: a 429 is intercepted _before_ `perform()` returns `Err` at all.
The job classifies the error, computes a delay, pushes a fresh copy of itself
onto its own queue after that delay via `push_with_delay`, and returns `Ok(())`.
The row acks `Done` immediately and the worker is free on the very next poll
tick to pick up any other pending item. The pushed successor is a brand-new
`Pending` row apalis dispatches when its `run_at` arrives. Because the error
never reaches `RetryPolicy`/`calculate_status`, a pure-429 sequence can never
produce apalis's `Event::Error` and can never fail-stop the worker -- RAI-1494
is fully decoupled from RAI-1495 (the circuit-breaker latch-with-no-wakeup bug,
fixed by removing the breaker entirely -- see below) by construction, not by
scoping discipline.

**Shared building blocks** (`src/conductor/job.rs`, `crates/execution/src/`):

- `st0x_execution::Backpressure { retry_after: Option<Duration> }` -- crate root
  of `st0x-execution`, re-exported since `st0x-tokenization` also needs it.
  `None` inside `retry_after` already means "429, no usable `Retry-After`
  header"; there is no separate enum case for that.
- An inherent `pub fn backpressure(&self) -> Option<Backpressure>` on each of
  the four Alpaca error types (`AlpacaBrokerApiError`, `AlpacaMarketDataError`,
  `AlpacaWalletError`, `st0x_tokenization::AlpacaTokenizationError`) --
  exhaustive `match`, `Some` only for the `ApiError`-shaped variant with
  `status == 429`. An inherent method per type, not a free classifier function:
  these are already `pub` domain types call sites match on directly, so a free
  function would only relocate the "know about these four types" fact, not
  remove it.
- `crate::rate_limit::parse_retry_after(header_value: &str, now: SystemTime)
  -> Option<Duration>`
  (`crates/execution/src/rate_limit.rs`, re-exported from that crate's root) --
  the one parser every Alpaca client's response-handling path calls to capture
  `Retry-After` **before** consuming the response body. Tries delay-seconds
  first, falls back to the HTTP-date form. `"0"` parses to
  `Some(Duration::ZERO)` -- a legitimate broker value; flooring a near-zero
  delay is `decide_backpressure`'s job, not the parser's.
- `crate::conductor::job::find_backpressure(error: &(dyn std::error::Error +
  'static)) -> Option<Backpressure>`
  -- walks the `.source()` chain, trying a `downcast_ref` against each of the
  four error types in turn at every link, short-circuiting on the first `Some`.
  This is the one place that "knows about" all four concrete types.
- `crate::conductor::job::decide_backpressure(backpressure: &Backpressure,
  streak: u32) -> BackpressureDecision { delay: Duration, exhausted: bool }`
  -- pure, no apalis/tokio types. Honours `Retry-After` when present (clamped to
  `[MIN_BACKPRESSURE_DELAY (1s), MAX_RETRY_AFTER (5min)]`); otherwise escalates
  a fallback backoff (`BACKPRESSURE_FALLBACK_BASE` 1s up to
  `BACKPRESSURE_FALLBACK_CAP` 60s) keyed off `streak`. `exhausted` is `true`
  once `streak >= BACKPRESSURE_RESCHEDULE_LIMIT` (500) -- a persistently-429ing
  item is eventually treated as a structurally-dead integration (suspended
  account, revoked key), not endless transient rate-limiting. This function does
  not log; each job logs its own every-10th-streak visibility line and its own
  exhaustion line, mirroring
  `PollOrderStatus::
  handle_get_order_status_error`.

**Per-job streak counter, not a stateful `Policy`**: because each reschedule is
a brand-new apalis dispatch (a fresh `Pending` row), there is no live in-memory
object to carry a retry count between attempts. Every participating job's
payload gains a `backpressure_streak: u32` field, `#[serde(default)]` so an
already-enqueued row under the pre-this-change shape still deserializes (to `0`,
the correct "no streak yet" value) instead of crashing the poll stream's
`sqlx::Decode` -- a decode failure there surfaces as `WorkerError::StreamError`,
a worker-crashing fault, not a clean per-row dead-letter, so `#[serde(default)]`
is mandatory on every one of these fields, not optional polish.

**Reschedule reuses `reschedule_self`'s shape, not `push_poll_job_if_absent`'s
guard**: the 429 reschedule is a single in-perform self-replacement point --
exactly one `Running` row acking `Done` while pushing exactly one `Pending`
successor -- the same invariant `reschedule_self` (above) already relies on for
its own ordinary Pending/Submitted reschedule. It is not one of the five
external push sites `push_poll_job_if_absent` exists to guard, so it does not
need that guard. **A reschedule push's own failure must propagate as `Err`,
never be swallowed into `Ok(())`**: `push_with_delay` returns
`Result<(), QueuePushError>`; if that push itself fails (e.g. a transient SQLite
write error), silently returning `Ok(())` anyway would ack the current row
`Done` with no live successor, dropping the item. Every participating job's
error enum has a `#[from] QueuePushError` arm precisely so `?` handles this
correctly.

**Crash-window duplicate risk: shared with, not introduced by, this mechanism.**
If the process crashes between the reschedule push committing and the current
job's `Ok(())` ack landing, both the old `Running` row (reset to `Pending` by
`requeue_orphaned` at next boot) and the new `Pending` successor exist. This is
`reschedule_self`'s existing pre-RAI-1494 characteristic, not a new one --
RAI-1493's dedup guard covers the _external push site_ layer, not the in-perform
self-replacement point, before or after this change. Also note: **orphan-reaping
cannot reap or shorten a reschedule delay.** A rescheduled successor is inserted
as a `Pending` row with a future `run_at`; it is never `Running` and holds no
worker lock, and both `requeue_orphaned` (this repo) and apalis-core's own
`reenqueue_orphaned_after` operate only on locked `Running` rows past a
heartbeat timeout. Even the widest delay this mechanism produces
(`MAX_RETRY_AFTER`, 5 minutes) cannot be shortened by orphan-reaping.

**Terminal behavior at `BACKPRESSURE_RESCHEDULE_LIMIT` exhaustion depends on
worker supervision.** For a job registered on a _supervised_ worker
(`build_supervised_worker!`), propagating a bare `Err` at exhaustion would
fail-stop the whole conductor for a cause that is not a genuine bug -- sustained
rate-limiting is an expected, self-clearing condition, not the kind of fault the
supervised fail-stop exists to catch. So a supervised job instead dead-letters
at exhaustion: log a loud, distinct `error!` naming the item and the exhausted
streak, then return `Ok(())`, so the row acks `Done` and
`Event::Error`/`on_terminal_failure` never observe this cause at all. A genuine
_non-backpressure_ `Err` on these jobs is completely untouched by this decision
and still fail-stops exactly as before. A best-effort-worker job
(`build_best_effort_worker!`) alerts once and continues on any terminal failure,
so exhaustion there needs no special case: the existing best-effort terminal
handling already covers it.

**True retry vs. reschedule-then-backstop -- not every job's reschedule
re-drives the Alpaca call.** Whether a reschedule's successor attempt actually
re-hits Alpaca, or instead short-circuits past it, depends on where the 429
lands relative to that job's own committed idempotency guard:

- **True retry**: no guard commits ahead of the Alpaca call, so every reschedule
  genuinely re-attempts it. `PollOrderStatus`'s `get_order_status` is a pure
  read with no committed state ahead of it -- the one job the parent RAI-1492
  incident actually exercised. `PlaceHedge` also genuinely retries a
  rate-limited broker placement: its position claim has committed, but the
  durable offchain order remains `Pending`, so `recover_pending_poll_status`
  re-drives the placement with the same deterministic `client_order_id`.
- **Reschedule-then-backstop**: a guard commits _before_ the Alpaca touch, so
  once that guard commits, a 429 later in the same attempt reschedules, but the
  pushed successor short-circuits past the Alpaca call entirely.
  `AccountForDexTrade` commits `account_for_onchain_fill` keyed on
  `(tx_hash, log_index)` before any Alpaca call, so a rescheduled successor hits
  `FillAccountingOutcome::AlreadyAcknowledged` and never re-attempts the hedge.
  Its reschedule's value is narrower than for a true-retry job: it still
  prevents burning the terminal retry budget and still frees the worker, but
  actual completion is delegated to the existing `CheckPositions` backstop, not
  the reschedule itself, and the streak structurally caps at 1 (a guard that
  already committed cannot un-commit on a later attempt).

This distinction matters for what a job's own tests can honestly claim: a test
asserting "the reschedule completes the work" for a reschedule-then-backstop job
would pass by coincidence of the short-circuit while proving nothing about the
actual hedge/fill outcome.

An exhausted `PollOrderStatus` row is acknowledged as `Done`, but it is not
ordinary cleanup fodder: the finished-job cleanup retains rows whose serialized
`backpressure_streak` reached the limit. `push_poll_job_if_absent` checks that
durable marker before both periodic recovery and every other poll push site, so
recovery cannot replace a finite dead letter with a fresh zero-streak chain.
Once the broker order is reconciled to a terminal aggregate state, it naturally
falls out of submitted-order recovery and the marker becomes inert.

**Quarantined: USDC conversion order placement is not rescheduled at all.**
`TransferUsdcToHedging`/`TransferUsdcToMarketMaking` are "true retry" jobs for
their deposit-poll and withdrawal-poll sub-steps, but the conversion placement
sub-step (`execute_usd_to_usdc_conversion`/`execute_usdc_to_usd_conversion`,
called from inside `resume_alpaca_to_base`/`resume_base_to_alpaca`) is an
explicit exception: a 429 (or any other placement error) still fails fast,
exactly as before this mechanism existed. `InitiateConversion`/
`InitiatePostDepositConversion` commits the aggregate to `Converting` before the
broker call, but placement failure unconditionally sends `FailConversion` and
returns `UsdcTransferError::ConversionPlacementFailed` -- a variant with no
`#[source]`, so `find_backpressure` can never classify it and the job's generic
terminal path (alert + propagate `Err`) runs instead of a reschedule. An earlier
pass tried making a placement 429 reschedule-safe by leaving the aggregate in
`Converting` and routing the redrive through the order-lookup resume path
(`resume_converting`); that broke down because a 429 rejects the HTTP request,
so the order was never created, the lookup 404s, and the redrive still ended in
a terminal failure -- just one hop further away and harder to diagnose. Retrying
a placement in place carries a real double-order risk against actual money, so
this was reverted rather than iterated on further. Making it safe needs a
place-then-commit reorder of the conversion aggregate (defer
`InitiateConversion` until after a successful placement, or have the resume path
re-place idempotently by `client_order_id` instead of only looking the order up)
-- tracked as a follow-up, not part of this mechanism.

**Known gap: several equity mint/redemption/recovery jobs cannot classify a 429
at all today.** `WrappedEquityRecoveryJob`, `UnwrappedEquityRecoveryJob`,
`TransferEquityToMarketMaking`, `TransferEquityToHedging`, and
`ResumeTokenizationAggregate` all gained a `backpressure_streak` field (so their
payload schema is ready), but none has a working reschedule wired to its actual
Alpaca-touching failure path:

- `WrappedEquityRecoveryJob`/`UnwrappedEquityRecoveryJob`: the aggregate's own
  command handler (`resume_mint_or_fail`/`resume_redemption_or_fail` in each
  `aggregate.rs`) catches a `resume_mint`/`resume_redemption` failure and
  records it as a terminal `RecoveryFailed` **event** with only a
  Display-formatted `String` reason -- `ctx.store.send()` returns `Ok(())`
  regardless, so `perform()` never observes an `Err` to classify at all. A 429
  here has always immediately terminalized the recovery with zero retries,
  before and after RAI-1494.
- `TransferEquityToMarketMaking`/`TransferEquityToHedging`/
  `ResumeTokenizationAggregate`: these DO propagate a genuine `Err` (today's
  `retries(3)` already applies), but
  `TokenizedEquityMintError::RequestFailed
  { error_message: String }`
  (`error.to_string()` in `tokenized_equity_mint.rs`) and the mirrored
  `EquityRedemption` error variants discard the original error type before it
  ever reaches the job -- `find_backpressure`'s downcast-based classification
  has nothing to walk to.

Closing this needs the mint/redemption/recovery aggregates' error types to
preserve the classified error (not just its `Display` string), and for the two
recovery jobs, `transition()` to return `Err` instead of `Ok(RecoveryFailed)` on
a classified 429 plus threading a streak into the `Command` (the aggregate has
no visibility into the job's own payload today). This is a redesign of those
aggregates' error-handling contracts, out of scope for RAI-1494's per-job wiring
-- tracked as a follow-up, not silently left unaddressed.

**Synchronous (CLI) call sites get bounded in-call retry, not the reschedule
mechanism.** A CLI command is not a durable job: there is no queue row to
reschedule and no sibling work waiting behind a shared `concurrency(1)` worker
-- the "worker" is the one-shot process the operator is already waiting on, so
retrying in place cannot reproduce the incident.
`src/cli/backpressure_retry.rs`'s `retry_on_backpressure` reuses the same
`find_backpressure`/`decide_backpressure` building blocks: on a classified 429
within a small bounded attempt budget (`BACKPRESSURE_RETRY_MAX_ATTEMPTS`), it
sleeps for the classified delay and retries in place; otherwise
(non-backpressure error, or budget exhausted) it propagates to the CLI's
existing `anyhow` error path unchanged. Wired into `cli/trading.rs`
(market/limit order placement, order-status), `cli/alpaca_wallet.rs` (deposit
address lookup, whitelist reads), and `cli/rebalancing.rs` (`transfer-usdc`).
Not wired into `cli/repair.rs` (its commands never call the broker --
`RepairOrderPlacer` always errors by design) or `transfer-equity` (same
stringified-error gap as
`TransferEquityToMarketMaking`/`TransferEquityToHedging` above).

**`ExecutorMaintenance` (`src/conductor/monitor/executor_maintenance.rs`) needs
no change.** It is a `SupervisedTask`, not a job or CLI command; its tick errors
are already logged and swallowed unconditionally by the supervisor lifecycle --
there is no retry budget to burn and no circuit breaker layer to interact with.
A 429 there behaves today exactly like the reschedule mechanism this section
documents: wait, log, try again next tick. Pinned by a regression test rather
than left as an unverified assumption.

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
    job: J,
    ctx: Data<Arc<Ctx>>,
    attempt: Attempt,
    sql_context: SqliteContext,
    task_id: TaskId<impl Display + Clone + Send + Sync + 'static>,
) -> Result<(), JobError> {
    let label = job.label();
    info!(%label, "Processing job");
    job.perform(&ctx).await.map_err(|source| JobError::Failed {
        task_identity: TaskIdentity::from(&task_id),
        durably_terminal: is_durably_terminal(&attempt, &sql_context),
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
  attempt, capped at 30s) so transient failures (RPC blips, broker 5xx) get a
  few spaced-out attempts before the job is treated as terminal. Classified
  broker 429s never reach this layer at all -- they are intercepted before
  `perform()` returns `Err` and rescheduled onto their own queue (see the
  backpressure section above), so they neither consume the retry budget nor
  reach `calculate_status`.
- **`Job::PERFORM_TIMEOUT` (RAI-2218).** `work()` runs `perform()` inside
  `tokio::time::timeout` bounded by the job's `PERFORM_TIMEOUT` (30 min default;
  `BackfillRange` overrides to 2h for long catch-up ranges). A `perform()`
  future that never resolves -- e.g. an external await against an endpoint that
  accepts the connection and never responds -- otherwise parks the
  single-concurrency worker forever with no error, no retry, and no
  `on_terminal_failure` stop: neither the retry layer nor the supervisor ever
  sees it. The bound converts the hang into a `JobError` carrying a
  `PerformTimeout` source, which retries like any other failure and, on
  exhaustion, reaches the worker's terminal path. This is a backstop: the
  primary defense is that every external HTTP client (trading RPC transport,
  broker, tokenization, bridge, Turnkey) carries its own request timeout.
  `PERFORM_TIMEOUT` is an `Option`, and several money-moving jobs set `None` to
  opt out entirely: dropping their future mid-phase (e.g. after an on-chain send
  broadcasts but before the aggregate persists it) would double-drive real money
  on retry, so they rely on their own phase-aware deadlines instead. A new `Job`
  impl must decide this deliberately -- grep the impls that set `None` for the
  current opt-outs and their reasons.
- **No circuit breaker (RAI-1495).** Supervised workers do NOT install Apalis'
  `CircuitBreakerService` layer -- neither does `build_best_effort_worker!`,
  which never had one. Previously `build_supervised_worker!` did
  (`.break_circuit_with(fail_stop_config)` between `.retry()` and
  `.on_event()`), configured `failure_threshold(1)` + a ~1yr `recovery_timeout`
  for fail-stop. This was removed: in apalis-core 1.0.0-rc.9,
  `tower::retry::Retry` sits OUTER of the circuit breaker (the first
  `.layer()`-style call added ends up outermost), so its `ResponseFuture` calls
  the circuit's `poll_ready` between retry attempts. Once open, `poll_ready`
  returns a bare `Poll::Pending` with **no waker registered at all** -- not
  merely a long wait, but a future that is never polled again, ever. Because
  that `Poll::Pending` sits inside the retry future, `work()`'s call never
  resolves to `Err`, `Event::Error` is never emitted, and `on_terminal_failure`
  (wired via `.on_event()`, listening for exactly that event) can never fire. A
  single-concurrency worker latches idle forever with no log, no stop, no crash
  -- `systemctl` still reports `active`. This is what happened to
  `PollOrderStatus` on 2026-07-22 (RAI-1492): 3h25m of fully silent hedging
  outage. Removing the breaker closes its own indefinite-Pending gate, the one
  reachable at any point during ordinary retry exhaustion. `RetryPolicy`'s own
  give-up decision depends only on attempt count, never on error content, so
  once it exhausts there is no remaining layer that can defer or swallow the
  resulting `Err` -- it reaches `on_terminal_failure` for every retry-
  exhaustion cause, every time.

  A second, narrower bare-`Poll::Pending`-with-no-waker gate remains outside
  this fix's scope: `ReadinessService::poll_ready` (apalis-core, same shape of
  bug) returns `Poll::Pending` -- again without registering a waker -- when
  `ctx.is_shutting_down()` or `ctx.is_paused()` is true. This is
  retry-reachable, not outside the retry loop -- `RetryPolicy` sits OUTER of
  `ReadinessService` in this stack, so `tower::retry`'s
  `ResponseFuture::Retrying` polls `ReadinessService::poll_ready` directly
  between attempts, the identical delegation path described above for the
  removed breaker. What differs from the breaker bug is _when_ it can trip: this
  codebase never calls `.pause()` (grep confirms), so the only live trigger is
  `is_shutting_down()`, true only during the conductor's own graceful-shutdown
  sequence. A supervised job that is between retry attempts right as shutdown
  fires can therefore still hang past that window with no waker -- a
  shutdown-time hang, not a silent steady-state latch. Not closed by this
  change; tracked as a follow-up in RAI-1524. The existing
  `best_effort_worker_does_not_latch_on_single_terminal_failure` regression test
  does not prove this window is safe -- it only proves a sibling job survives a
  different job's terminal failure, never exercising shutdown or an in-flight
  retry backoff.
- **`.on_event()`** — fires on `Event::Error` (after the in-memory retry policy
  exhausts), `Event::Success`, `Event::Start`, `Event::Stop`. Supervised workers
  record failure info and call `ctx.stop()`. Best-effort workers inspect the
  carried SQLite attempt metadata, alert only when the row is durably terminal,
  and never stop the worker or conductor.

### Error propagation: handler failure -> bot shutdown

1. `work()` returns `Err` -> retry layer retries (1s/2s/4s backoff)
2. Retries exhaust -> `RetryPolicy` resolves to `Err` unconditionally -> this
   becomes `Event::Error` in apalis `poll_tasks`
3. `on_event` (`on_terminal_failure`) catches `Event::Error`, logs `error!()`,
   records a `TerminalFailureInfo { worker, context, source }` -- `source` is
   the same `Arc<BoxDynError>` apalis handed to the callback, not a flattened
   string, so the original error's chain survives -- into the shared
   `TerminalFailureSignal` (an `Arc`-wrapped `Notify` + `OnceLock<_>` threaded
   through every `build_supervised_worker!` call site exactly like the plain
   `Notify` it replaces), then calls `ctx.stop()`. The info is always recorded
   before the `Notify` fires, so a waiter that wakes is guaranteed to see it.
   `OnceLock::set` makes concurrent terminal failures from different workers
   first-writer-wins: whichever lands first is the only info any reader ever
   observes, atomically -- never a torn or mixed pair. Every worker still
   notifies and stops regardless of which one wins.
4. Worker exits cleanly (`Ok(())`)
5. The spawned monitor task's biased `tokio::select!` observes the signal and
   returns
   `Err(MonitorTaskError::TerminalJobFailure { worker, context,
   source })`
   immediately -- it does not wait for `apalis_monitor.run()` to wind down.
   `source` is `#[source]` on the error variant, so it is not rendered by
   `Display`/`{}` but IS walked by anyhow's `{:?}` "Caused by:" chain.
6. `Conductor::wait_for_completion` (the async exit path) sees this variant and
   awaits sending an operator alert through `worker_failure_notifier`
   (`Arc<dyn Notifier>`, a `LogNotifier` emitting a structured ERROR log),
   bounded by a short timeout so a slow notifier implementation cannot delay
   process exit -- then propagates the error, so the bot process exits non-zero.

**Critical:** the spawned monitor task must select on the shared signal
alongside `apalis_monitor.run()` and return the terminal error. Without that
branch, the conductor would only learn of the failure once apalis finished
tearing down all workers; without returning the error, the conductor would never
see the failure at all.

**Why the alert is sent from the exit path, not from `on_event`.** `on_event` is
a synchronous `Fn(&WorkerContext, &Event)` -- it cannot `.await` the async
notifier. Firing the alert as a detached `tokio::spawn` from inside that
callback would race process teardown: `ctx.stop()` and the resulting process
exit can complete before the spawned send does, silently dropping it in the
common case. `wait_for_completion` is async and runs strictly before the process
returns, so awaiting the alert there (with a bounded timeout) is what actually
guarantees delivery-or-timeout instead of delivery-or-silently-lost.

**Blast radius: this restores the existing fail-stop design; it does not invent
universal self-recovery.** `on_terminal_failure`'s `ctx.stop()` + non-zero
exit + systemd `Restart = "always"` / `RestartSec = 30` is not new behavior --
it is the pre-existing fail-stop path the circuit breaker bug was silently
suppressing. What changes is that it now actually fires. Two distinct outcomes
follow, and only one of them is "self-heals":

- **Transient cause** (an RPC blip, a momentary broker 5xx): the process exits,
  systemd restarts it within 30s, and the worker resumes cleanly -- no operator
  action needed. This is the "resumes by itself" case.
- **Persistent cause** (poison job, sustained downstream outage): the single
  poison row itself does not re-drive (`ack.sql` leaves an exhausted `Failed`
  row alone; `requeue_orphaned` only touches `Running`/`Queued`), so one crash
  is usually a clean restart. But several supervised jobs are re-seeded on every
  boot (e.g. `PollOrderStatus` via `recover_submitted_offchain_orders`,
  `CheckPositions`'s self-reschedule) -- against a genuinely persistent, non-429
  failure (RAI-1494's reschedule only intercepts classified 429s), each boot
  re-pushes a fresh row that fails again, producing a real crash-loop. systemd's
  `StartLimitBurst`/`StartLimitIntervalSec` (see `nix/upgradeable-services.nix`)
  eventually trips and leaves the unit fully dead -- loud and visible in
  `systemctl status`/telemetry, but operator- required, and it takes every
  supervised subsystem down together (hedging, fill detection, rebalancing), not
  just the one worker that first failed. This is strictly better than the silent
  single-subsystem latch it replaces, but it is not "always self-recovers" -- a
  persistent fault ends in a visible dead unit, not a quiet resume.

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
the job needs: config, symbol cache, EVM provider, orderbook address, CQRS
frameworks, vault registry, executor, database pool, and job queue. Wrapped in
`Arc` and injected via apalis `Data`.

### DeliverDashboardTrade

Defined in `src/dashboard/event.rs`. The dashboard CQRS reactor enqueues this
job for each terminal trade outcome instead of publishing the update inline. The
serialized `Trade` payload is the replay record. A separate
`dashboard_trade_delivery` ledger, keyed by trade ID, records whether the live
update completed. The reactor registers the ledger row before its idempotent
queue insertion. If either insertion fails after the terminal event commits, a
supervised handoff monitor retains the outcome in a bounded in-memory queue and
retries it with exponential backoff a fixed number of times. An outcome that
exhausts its immediate retries is not lost: the monitor schedules a periodic
authoritative reconciliation that rebuilds every undelivered terminal trade from
the event log, so one poison handoff cannot block later outcomes. A handoff
whose trade can never be represented (a deterministic conversion failure) stops
the monitor through the supervisor instead of retrying forever. Startup
reconstructs every terminal trade from the event log, creates any ledger rows or
jobs missing after a crash, and resets `Running`/`Queued`/`Failed`/`Killed` jobs
to `Pending` before workers start. This closes both the event-to-job handoff
window and the dequeue-to-publish restart window.

The worker checks the ledger before publishing and records `delivered_at`
afterward. A database failure in either step is retryable; a crash after publish
but before the update commits can replay the message safely. Retry exhaustion
notifies the conductor monitor and is logged as an undelivered terminal update.

The Tokio broadcast channel reports an error when it has no receivers. That is
not a delivery failure here: a later WebSocket connection loads terminal trades
from the authoritative snapshot, so zero connected clients completes the job.
Replaying a publish is safe because the dashboard replaces an existing trade
with the same trade ID; a completed ledger row prevents unnecessary replays.

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
`extended_hours_counter_trading = enabled` place limit orders; disabled symbols
skip. Ordinary extended-hours orders use the shared reference-price resolver
plus the configured `counter_trade_slippage_bps` buffer. With no primary
provider wired today, the broker position mark is the first available production
source.

The executor's `MarketSessionStatus` also classifies the gap after the current
session as `OrdinaryOvernight`, `MultiDayClosure`, or `Unknown`. Both
`CheckPositions` and `PlaceHedge` use the same `CloseFlattenPolicy`: during the
configured final window before a multi-day or unknown gap, the scanner cancels
any order resting since before the window and placement switches to aggressive
limits. This close-flatten cancellation is independent of
`extended_hours_reprice_timeout_secs`: it targets live extended-hours orders
placed before `CloseFlattenWindow.started_at`, even when they are younger than
the timeout. That timeout gates only the separate reprice-timeout cancellation
sweep. The shared resolver first asks an optional current bid/ask quote source,
then falls back to the broker's position `current_price`, then to the emergency
delayed quote. No primary provider is wired today, so effective production
behaviour remains mark first. Buys add the cross and sells subtract it. The
cross ramps linearly with elapsed time inside the window, from
`counter_trade_slippage_bps` at the window's start to
`close_flatten_cross_max_bps` at the close. `CloseFlattenCrossRamp` anchors it
to the window rather than to a per-order attempt count, so it is a pure function
of the window and the current time: restart-safe, identical across apalis
retries, and independent of how many reprice cycles land. A hedge that first
becomes ready mid-window opens partway up the ramp. See
[ADR 0019](../adrs/0019-mark-priced-close-flatten-with-widening-cross.md) for
the source-order and fallback decision.

If the optional primary provider is absent or fails, and the symbol has no
broker position or its mark lookup also fails, placement falls back to a quote
on the `delayed_sip` feed. That emergency feed is hardcoded rather than
configured: `sip` draws an entitlement 403, `iex` publishes single-venue stub
quotes once it stops trading around 16:00 ET, and `delayed_sip` is the only
value currently available that returns a real consolidated book. Its quote is a
genuine NBBO fifteen minutes stale. After validating the book, the order price
crosses its ask for a buy or its bid for a sell, regardless of spread width,
since refusing that reference would leave the position unflattened.

Only after the resolver has exhausted the optional primary quote, broker mark,
and delayed SIP fallback does the selected missing, non-positive, bid-above-ask,
or failed quote outcome classify as `ErrorScope::SymbolScoped` in
`TradeAccountingError::scope()`. That says only that the failure was raised
before this attempt ran `PositionCommand::PlaceOffChainOrder`.
`handle_place_hedge_error` needs a second answer before it abandons the job,
because the same variant boxes an opaque source: `find_permanence` walks the
error chain for a broker `AlpacaBrokerApiError::permanence()` classification. A
non-429 4xx (the entitlement 403 a `sip` quote draws on a Basic plan) and a
syntactically malformed response are permanent. A valid but incomplete,
non-positive, bid-above-ask, or wrong-symbol quote is a dynamic snapshot that
may clear on the next request; those outcomes, 5xx, timeouts, and transport
errors are transient.

A permanent cause dead-letters at once, since re-asking cannot change the
answer. A transient one is re-driven on the job's own durable budget first:
`redrive_transient_failure` pushes a successor carrying an incremented
`TransientFailureStreak` after 1s, 2s, then 4s, mirroring the supervised
worker's `RetryPolicy::retries(3)` and its backoff, so the retry cadence is
unchanged and only the terminal action differs. The streak is a `#[serde]`
payload field, so it survives a restart rather than resetting the budget. Once
`TRANSIENT_RESCHEDULE_LIMIT` re-drives are spent the symbol dead-letters like a
permanent cause, because propagating instead would exhaust the shared retry
budget and stop hedging for every other symbol -- the outage this isolation
exists to prevent (RAI-1690).

On a terminal symbol-scoped path the dead-letter is the same: the job increments
`hedge_dead_lettered_total{symbol,reason}`, logs the cause, pages the operator
through the shared `Notifier` (once per `(symbol, reason)`, so the ~60s
`CheckPositions` re-enqueue cannot spam the channel), and returns `Ok(())`
rather than consuming the shared retry budget or propagating an error that
affects other symbols. A process-scoped recovery failure instead propagates
through `dead_letter_or_propagate` so apalis can retry it. Notification delivery
is bounded so an unreachable alert channel cannot serialize the single hedge
worker; a failed or timed-out delivery releases the dedup reservation for the
next scan. `CheckPositions` re-enqueues the hedge on its next scan. The dedup
set is not a process-lifetime latch: `route_placement_outcome` drops every entry
for a symbol as soon as one of its hedges reaches the broker, so a regression
that recurs the following session pages again instead of silently accumulating a
delta.

`CheckPositions` can also drop an extended-hours buy before a `PlaceHedge` job
exists, when the scan-time preflight cannot resolve a reference price or cannot
cross it. Nothing downstream can report that, so the scan counts it on
`hedge_scan_skipped_total{symbol,reason}` with the leg that failed. A permanent
or unclassified failed reference-price lookup pages through the same
`alert_dead_letter` and shared `(symbol, reason)` set as the hedge job. A
transient or rate-limited failure remains counted but waits for the next scan
instead of creating a dead-letter page on its first observation. Any page is
released by the same successful placement.

Dead-lettering does not assume the position is unclaimed. apalis re-runs
`perform_body` from the top, so a retry can fail this pre-claim lookup after an
earlier attempt already claimed the position and left a broker order behind;
`handle_place_hedge_error` therefore resolves the claim through
`recover_pending_poll_status` -- a no-op when nothing is claimed, a re-drive or
re-arm of polling when something is -- _before_ it records the abandonment, and
propagates a failed process-scoped recovery so apalis retries instead of
counting a hedge that is still in flight as given up on. The one place pricing
runs with a claim outstanding -- `recover_pending_poll_status`'s `Pending`
re-drive -- wraps the failure as `ClaimedHedgeOrderKind`. Its symbol-scoped
source uses the same bounded backpressure/transient budgets as the pre-claim
path, then dead-letters while deliberately leaving the claim for the periodic
recovery sweep; a genuinely process-scoped source still propagates. Placement
dead-letters only once the optional primary quote, the mark, and the hardcoded
`delayed_sip` quote feed have all failed: flattening before a multi-day gap is
mandatory, so a worse fill price always beats no fill.

Ordinary extended-hours orders retain the explicit 300-second
`extended_hours_reprice_timeout_secs` cadence. Orders placed by close-flatten
use the separate 60-second `close_flatten_reprice_timeout_secs` cadence. The
initial close-flatten cancellation remains independent of both timeouts: it
targets any live extended-hours order placed before the window. Each confirmed
cancellation releases the position after applying any partial fill, so a later
scan retries the broker-executable residual using a fresh reference and a later
point on the cross ramp. That retry also waits for `position_check_interval`.
The shipped deployments use 60 seconds, but cancellation confirmation and job
execution add variable delay, so neither that interval nor the reprice timeout
defines a fixed attempt count. This produces repeated bounded-loss attempts
until the session closes, while the existing buying-power, equity-inventory,
operational-limit, and broker-minimum checks continue to block invalid or
leveraged orders.

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

Before systemd stops the existing bot, deploy activation validates staged
config/secrets and uses Turnkey's read-only policy list to verify coverage for
the exact approval targets startup will grant. Both paths call the same
deterministic target builder, limited to trading- or rebalancing-enabled symbols
plus USDC. An allow policy covers a target only when the authenticated API user
can satisfy its consensus alone and its target condition provably applies.
Applicable or unprovable deny policies take precedence. Missing or unrecognized
coverage fails activation without replacing the running process or installed
config. If stopping the validated service fails, activation also aborts before
the staged files replace the installed config and secrets.

```
Phase 1: connect_http (with RPC probe) | setup_apalis_tables | build CQRS stores
Phase 2: seed_vault_registry (inline, must complete before downstream wiring)
Phase 3: setup_rebalancing (optional) | requeue_orphaned jobs | hydrate inventory |
         recover pending orders
Phase 4: builder::spawn() starts supervisor + apalis workers
```

After Phase 4 completes, Conductor acknowledges its startup token. The session
reports startup readiness only after Conductor, both HTTP servers, the apalis
monitor, finished-job cleanup, and every configured task-supervisor loop have
all reached pending run states. Disabled optional supervisor tasks acknowledge
their slots during assembly. The deployed server writes its PID to a
systemd-managed runtime-directory file, and activation fails if that PID does
not match the live unit before the configured startup timeout. Non-deployment
sessions use the same barrier with a no-op notifier. On the first rollout, the
system profile that supplies the readiness environment must be deployed before
the service profile. A service-only deploy checks that prerequisite before
stopping the current bot and fails immediately when the installed unit is still
from before the handshake.

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

## Native-gas admission for transfers

Fresh rebalancing transfers are admitted only after reading the native-token
balance of every signing wallet their route can use. Equity transfers check the
Base wallet; USDC transfers check both Base and Ethereum. An unreadable balance
or a value below the configured `[alerts.low_balance_thresholds]` entry fails
closed before any CQRS event or fund movement. Equality with the threshold is
ready. Rebalancing therefore requires `[alerts]` at startup and shares its
thresholds and polling interval with the gas-balance monitor.

The trigger checks readiness immediately before enqueueing, and the transfer
manager checks again immediately before creating a fresh aggregate. The second
check closes the queue-delay race. A refusal keeps or releases the existing
in-progress guard as appropriate and delayed-redrives at the alert poll cadence
without consuming normal job retry budgets. Resuming an aggregate that has
already started skips fresh admission: persisted state may represent funds
already in flight, so recovery must continue rather than strand them behind a
new balance check.

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
