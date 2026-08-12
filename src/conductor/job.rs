//! Trait abstraction for apalis-backed persistent jobs.
//!
//! [`Job`] wraps apalis's function-based handler API with a
//! trait-based one. Each job is a serializable struct pushed into
//! `SqliteStorage`; the generic [`work`] handler deserializes
//! it and calls [`Job::perform`] with the shared context.

use apalis::layers::retry::backoff::Backoff;
use apalis::prelude::{Attempt, Data, TaskBuilder, TaskSink};
use apalis_core::backend::TaskSinkError;
use apalis_core::backend::poll_strategy::{BackoffConfig, IntervalStrategy, StrategyBuilder};
use apalis_core::error::BoxDynError;
use apalis_core::worker::context::WorkerContext;
use apalis_core::worker::event::Event;
use apalis_sqlite::{Config, SqliteContext, SqlitePool, SqliteStorage, SqlxError};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
#[cfg(any(test, feature = "test-support"))]
use std::sync::Mutex;
use std::time::Duration;
use tracing::{debug, error, warn};

use st0x_execution::{AlpacaBrokerApiError, AlpacaWalletError, Backpressure, Permanence};
use st0x_tokenization::{AlpacaTokenizationError, TokenizerError};

/// Deterministic exponential backoff for the apalis retry layer.
/// Doubles the delay each attempt up to `max`, with no jitter (unnecessary
/// for single-worker queues). Infallible to construct so the production
/// wiring needs no fallback path for invalid config.
#[derive(Clone, Debug)]
pub(crate) struct ExponentialBackoff {
    base: Duration,
    max: Duration,
    iteration: u32,
}

impl ExponentialBackoff {
    pub(crate) const fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            iteration: 0,
        }
    }
}

impl Backoff for ExponentialBackoff {
    type Future = tokio::time::Sleep;

    fn next_backoff(&mut self) -> Self::Future {
        let factor = 2u32.saturating_pow(self.iteration);
        let delay = self.base.saturating_mul(factor).min(self.max);
        self.iteration = self.iteration.saturating_add(1);
        tokio::time::sleep(delay)
    }
}

/// Production retry backoff: 1s base, doubles each attempt, capped at 30s.
/// Sequence for `RetryPolicy::retries(3)`: 1s, 2s, 4s.
pub(crate) const RETRY_BACKOFF: ExponentialBackoff =
    ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(30));

/// Minimum delay before a backpressure reschedule fires (RAI-1494). Floors a
/// `Retry-After: 0` (or a malformed near-zero value) so a broker signal
/// meant to be honoured cannot produce a hot reschedule loop against an
/// already-rate-limited endpoint. Not a financial clamp -- rate-limit
/// hygiene, distinct from this codebase's fail-fast rule for financial
/// values.
pub(crate) const MIN_BACKPRESSURE_DELAY: Duration = Duration::from_secs(1);

/// Ceiling on an exact `Retry-After` value, guarding only against a
/// malformed or bogus huge header. A legitimate broker-specified wait within
/// this bound is always honoured exactly.
pub(crate) const MAX_RETRY_AFTER: Duration = Duration::from_secs(5 * 60);

/// Escalating fallback when the broker signals backpressure without a usable
/// `Retry-After`. Same base as [`RETRY_BACKOFF`] so a single 429 is no more
/// aggressive than today's non-backpressure backoff; capped higher (60s vs
/// 30s) since sustained backpressure, now that rescheduling frees the worker
/// between tries, can afford a longer per-attempt ceiling.
pub(crate) const BACKPRESSURE_FALLBACK_BASE: Duration = Duration::from_secs(1);
pub(crate) const BACKPRESSURE_FALLBACK_CAP: Duration = Duration::from_secs(60);

/// Reschedule budget before a persistently-429ing item is treated as a
/// structurally-dead integration (suspended account, revoked key) rather
/// than transient rate-limiting. ~8.3h worst case when the fallback
/// escalation is in play (60s cap); up to ~41.6h if every reschedule honours
/// a broker-specified `Retry-After` at the [`MAX_RETRY_AFTER`] ceiling. A
/// judgment call, revisited once real sustained-429 durations are observed
/// in production.
pub(crate) const BACKPRESSURE_RESCHEDULE_LIMIT: u32 = 500;

/// Streak count at which sustained backpressure should page an operator
/// (RAI-1494 pass 3), rather than staying silent until the full
/// [`BACKPRESSURE_RESCHEDULE_LIMIT`] dead-letter. Before this PR, a stuck
/// Alpaca withdrawal poll paged at `WITHDRAWAL_POLL_ALERT_DEADLINE` (4h);
/// routing a classified 429 through this reschedule machinery instead must
/// not silently drop that SLA. Halfway to the limit (mirrors the
/// `warn_threshold` pattern used for redrive budgets elsewhere) lands close
/// to the pre-existing 4h bar under the escalating fallback delay (capped at
/// [`BACKPRESSURE_FALLBACK_CAP`] = 60s): 250 reschedules * 60s = ~4.17h. With
/// a broker-specified `Retry-After` at the [`MAX_RETRY_AFTER`] ceiling this
/// extends to ~20.9h -- still far tighter than the ~41.6h a dead-letter-only
/// page would allow.
pub(crate) const BACKPRESSURE_ALERT_STREAK: u32 = BACKPRESSURE_RESCHEDULE_LIMIT / 2 + 1;

/// Durable count of consecutive broker rate-limit (429) reschedules leading
/// up to a job's current attempt (RAI-1494). Every participating job payload
/// carries this in the same field name (`backpressure_streak`), distinct at
/// the type level from unrelated durable counters like
/// `revert_redrive_attempts` -- a copy-paste swap between the two at a
/// construction site is now a compile error instead of a silent behaviour
/// bug. `#[serde(transparent)]` keeps the wire format an unadorned integer,
/// so no schema migration is implied and pre-existing rows still decode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct BackpressureStreak(pub(crate) u32);

/// Outcome of [`decide_backpressure`]: the delay before a rescheduled
/// successor should run, and whether the streak has exhausted
/// [`BACKPRESSURE_RESCHEDULE_LIMIT`].
pub(crate) struct BackpressureDecision {
    pub(crate) delay: Duration,
    /// `true` once `streak` has reached [`BACKPRESSURE_RESCHEDULE_LIMIT`] --
    /// the caller must not reschedule again. Per the RAI-1494 plan's binding
    /// M2 decision, a supervised job's `perform()` treats this as its own
    /// self-contained terminal event (a loud `error!` log, then `Ok(())`)
    /// rather than propagating the original `Err` into the shared
    /// supervised on-event path, so a persistently-429ing item cannot
    /// reach `on_terminal_failure`'s `ctx.stop()` and halt the worker.
    pub(crate) exhausted: bool,
}

/// Pure: computes the reschedule delay and whether the budget is exhausted
/// for a given consecutive-backpressure streak. No apalis/tower types, no
/// I/O, no `Job` bound -- trivially unit-testable and shared by every
/// participating job. Logging (the every-10th-streak visibility line, and
/// the loud exhaustion log) is the caller's responsibility, keeping this
/// function a plain computation.
pub(crate) fn decide_backpressure(
    backpressure: &Backpressure,
    streak: BackpressureStreak,
) -> BackpressureDecision {
    let BackpressureStreak(streak) = streak;

    let delay = backpressure.retry_after.map_or_else(
        || {
            let factor = 2u32.saturating_pow(streak);
            BACKPRESSURE_FALLBACK_BASE
                .saturating_mul(factor)
                .clamp(MIN_BACKPRESSURE_DELAY, BACKPRESSURE_FALLBACK_CAP)
        },
        |retry_after| retry_after.clamp(MIN_BACKPRESSURE_DELAY, MAX_RETRY_AFTER),
    );

    BackpressureDecision {
        delay,
        exhausted: streak >= BACKPRESSURE_RESCHEDULE_LIMIT,
    }
}

/// Outcome of [`advance_backpressure`]: either the reschedule budget is
/// exhausted (the caller should dead-letter) or a successor should be
/// pushed after `delay` with `next_streak`. Centralizes the
/// exhaustion/streak-increment/every-10th-visibility decision that was
/// previously copy-pasted verbatim across every participating job's error
/// handler; each job still owns its own log message fields and successor
/// struct construction, since both are job-specific.
pub(crate) enum BackpressureStep {
    /// `streak` has reached [`BACKPRESSURE_RESCHEDULE_LIMIT`] -- the caller
    /// must log its own dead-letter message and return `Ok(())` rather than
    /// propagate the original `Err` (see [`BackpressureDecision::exhausted`]).
    DeadLetter,
    /// Reschedule a successor with `next_streak` after `delay`. `visible`
    /// is `true` on every 10th consecutive reschedule (1-indexed), telling
    /// the caller to also emit its own "still rescheduling" visibility log.
    Reschedule {
        next_streak: BackpressureStreak,
        delay: Duration,
        visible: bool,
    },
}

/// Combines [`decide_backpressure`]'s pure delay/exhaustion computation with
/// the streak-increment and every-10th visibility decision every
/// participating job repeats verbatim. Still pure and unit-testable; logging
/// text and successor construction stay with each job's own handler because
/// both carry job-specific fields.
pub(crate) fn advance_backpressure(
    backpressure: &Backpressure,
    streak: BackpressureStreak,
) -> BackpressureStep {
    let decision = decide_backpressure(backpressure, streak);

    if decision.exhausted {
        return BackpressureStep::DeadLetter;
    }

    let BackpressureStreak(streak) = streak;
    let next_streak = BackpressureStreak(streak.saturating_add(1));
    let BackpressureStreak(next_streak_value) = next_streak;
    BackpressureStep::Reschedule {
        next_streak,
        delay: decision.delay,
        visible: next_streak_value % 10 == 0,
    }
}

/// Outcome of [`apply_backpressure_step`]: which branch of a
/// [`BackpressureStep`] was taken. `next_streak` and `visible` are threaded
/// back through the `Rescheduled` variant so the caller can still log its own
/// "still rescheduling" message with the actual scheduled streak; both
/// branches' message text stay job-specific and are not centralized here.
pub(crate) enum BackpressureOutcome {
    /// The reschedule budget was exhausted; no successor was pushed. The
    /// caller logs its own dead-letter message and returns `Ok(())`.
    DeadLettered,
    /// A successor was pushed after the computed delay.
    Rescheduled {
        next_streak: BackpressureStreak,
        visible: bool,
    },
}

/// Executes a [`BackpressureStep`] against `queue`: a no-op for
/// [`BackpressureStep::DeadLetter`], or builds the successor via
/// `build_successor` and pushes it after the computed delay for
/// [`BackpressureStep::Reschedule`]. Centralizes the
/// push-then-return-the-outcome boilerplate that was copy-pasted verbatim
/// across every participating job's error handler; each job still owns its
/// own tracing text and successor struct construction, since both are
/// job-specific.
pub(crate) async fn apply_backpressure_step<
    Task: Serialize + DeserializeOwned + Send + Sync + Unpin + 'static,
>(
    step: BackpressureStep,
    queue: &mut JobQueue<Task>,
    build_successor: impl FnOnce(BackpressureStreak) -> Task,
) -> Result<BackpressureOutcome, QueuePushError> {
    match step {
        BackpressureStep::DeadLetter => Ok(BackpressureOutcome::DeadLettered),
        BackpressureStep::Reschedule {
            next_streak,
            delay,
            visible,
        } => {
            queue
                .push_with_delay(build_successor(next_streak), delay)
                .await?;
            Ok(BackpressureOutcome::Rescheduled {
                next_streak,
                visible,
            })
        }
    }
}

/// Walks the `.source()` chain of a job error looking for a classified
/// broker rate-limit (429) response, checking the error itself before
/// descending into its sources. Tries each of four known error types in
/// turn, short-circuiting on the first `Some` -- this is the one place that
/// needs to "know about" all of them (RAI-1494).
///
/// `AlpacaMarketDataError` (market-data 429s, e.g. from
/// `fetch_latest_trade_price`) is NOT one of the four: it only ever reaches
/// the wider app wrapped in `AlpacaBrokerApiError::LatestTrade`, and that
/// variant's `backpressure()` delegates straight to the wrapped error, so
/// classification happens at the `AlpacaBrokerApiError` hop already --
/// naming `AlpacaMarketDataError` here would be a redundant, leaky second
/// downcast (`st0x-execution`'s `AGENTS.md` keeps it test-only).
///
/// `TokenizerError` needs its own downcast (not just `AlpacaTokenizationError`):
/// `TokenizerError::Alpaca` is `#[error(transparent)]`, which makes `.source()`
/// forward straight through to the WRAPPED error's own source rather than
/// returning the wrapped error itself -- so a chain-walk alone would skip
/// right past an `AlpacaTokenizationError` arriving wrapped in a
/// `TokenizerError` (as every `Tokenizer` trait method returns) without ever
/// downcasting it. `TokenizerError::backpressure()` delegates internally
/// instead of relying on `.source()`.
pub(crate) fn find_backpressure(error: &(dyn std::error::Error + 'static)) -> Option<Backpressure> {
    std::iter::successors(Some(error), |error| error.source()).find_map(|error| {
        error
            .downcast_ref::<AlpacaBrokerApiError>()
            .and_then(AlpacaBrokerApiError::backpressure)
            .or_else(|| {
                error
                    .downcast_ref::<AlpacaWalletError>()
                    .and_then(AlpacaWalletError::backpressure)
            })
            .or_else(|| {
                error
                    .downcast_ref::<AlpacaTokenizationError>()
                    .and_then(AlpacaTokenizationError::backpressure)
            })
            .or_else(|| {
                error
                    .downcast_ref::<TokenizerError>()
                    .and_then(TokenizerError::backpressure)
            })
    })
}

/// Walks an error chain for a broker failure's [`Permanence`], the same way
/// [`find_backpressure`] walks it for a 429: the hedge path boxes its broker
/// failures behind `dyn Error`, so the classification sits however many hops
/// down the source chain the wrapping put it.
///
/// Only `AlpacaBrokerApiError` is downcast, for the same reason
/// `find_backpressure` names only four types: a market-data failure reaches
/// the app wrapped in `AlpacaBrokerApiError::LatestTrade`/`LatestQuote`,
/// whose `permanence()` delegates to the wrapped error at that hop.
///
/// `None` means no broker failure appears in the chain at all -- the caller,
/// not the broker, decided the outcome.
pub(crate) fn find_permanence(error: &(dyn std::error::Error + 'static)) -> Option<Permanence> {
    std::iter::successors(Some(error), |error| error.source()).find_map(|error| {
        error
            .downcast_ref::<AlpacaBrokerApiError>()
            .map(AlpacaBrokerApiError::permanence)
    })
}

type Storage<Task> = SqliteStorage<
    Task,
    apalis_codec::json::JsonCodec<apalis_sqlite::CompactType>,
    apalis_sqlite::fetcher::SqliteFetcher,
>;

/// Persistent job queue backed by apalis `SqliteStorage`.
pub(crate) struct JobQueue<Task>(Storage<Task>);

/// Concrete error returned by [`JobQueue::push`] / [`JobQueue::push_with_delay`].
/// Wrapping [`TaskSinkError`] keeps the failure chain typed so callers can
/// `#[from]` it into their own error enums instead of boxing.
#[derive(Debug, thiserror::Error)]
#[error("Failed to enqueue apalis job: {0}")]
pub(crate) struct QueuePushError(#[from] pub(crate) TaskSinkError<SqlxError>);

impl<Task> Clone for JobQueue<Task> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// Pickup latency and reservation SLO for queued jobs.
///
/// Hedge placement (and the upstream trade-processing pipeline) needs
/// to react to events on the order of a single second: a missed window
/// here translates directly into directional exposure that the hedger
/// is supposed to be neutralising. Apalis defaults to exponential
/// poll backoff capped at 60s, which is sensible for long-running
/// systems that idle for hours but violates the SLO this service
/// operates under -- after even a brief idle period a worker can be
/// sleeping for tens of seconds when a new job lands.
///
/// We cap the polling interval at 1s end-to-end so the worst-case
/// pickup latency matches the SLO regardless of prior queue state.
///
/// Apalis also defaults to fetching 10 rows per worker poll. Our workers are
/// single-concurrency, so a larger fetch buffer reserves extra rows as
/// `Queued` in SQLite before the handler can run them. If the process dies or
/// the in-memory fetch buffer is dropped, those rows are no longer `Pending`
/// and the deterministic worker heartbeat keeps them from aging out. Fetch one
/// row at a time so durable queue state mirrors actual handler execution.
fn build_poll_config<T: 'static>() -> Config {
    let strategy = StrategyBuilder::new()
        .apply(
            IntervalStrategy::new(Duration::from_millis(100))
                .with_backoff(BackoffConfig::new(Duration::from_secs(1))),
        )
        .build();

    Config::new(std::any::type_name::<T>())
        .set_buffer_size(1)
        .with_poll_interval(strategy)
}

impl<Task: Serialize + DeserializeOwned + Send + Sync + Unpin + 'static> JobQueue<Task> {
    pub(crate) fn new(pool: &SqlitePool) -> Self {
        Self(SqliteStorage::new_with_config(
            pool,
            &build_poll_config::<Task>(),
        ))
    }

    pub(crate) async fn push(&mut self, task: Task) -> Result<(), QueuePushError> {
        Ok(TaskSink::push(&mut self.0, task).await?)
    }

    /// Enqueues at most one row for a stable domain identity.
    pub(crate) async fn push_idempotent(
        &mut self,
        idempotency_key: &str,
        task: Task,
    ) -> Result<(), QueuePushError> {
        let task = TaskBuilder::<Task, SqliteContext, _>::new(task)
            .with_idempotency_key(idempotency_key)
            .build();
        Ok(TaskSink::push_task(&mut self.0, task).await?)
    }

    /// Schedules a task to run after `delay` from now. Used by self-rescheduling
    /// jobs (e.g. status pollers waiting for a broker to fill an order) to
    /// avoid burning the retry budget on a successful poll that simply hasn't
    /// observed the terminal state yet. Apalis honours the timestamp via the
    /// `Pending` row's `run_at` column.
    pub(crate) async fn push_with_delay(
        &mut self,
        task: Task,
        delay: Duration,
    ) -> Result<(), QueuePushError> {
        let scheduled = TaskBuilder::<Task, SqliteContext, _>::new(task)
            .run_after(delay)
            .build();
        Ok(TaskSink::push_task(&mut self.0, scheduled).await?)
    }

    pub(crate) fn into_storage(self) -> Storage<Task> {
        self.0
    }

    /// Returns the underlying `SqlitePool`. Used by callers that need to
    /// query or mutate the apalis Jobs table directly.
    pub(crate) fn pool(&self) -> &SqlitePool {
        self.0.pool()
    }

    /// Mark every pending row of this queue's task type as `Done`. Used by
    /// callers that need to discard stale work after a terminal domain event
    /// invalidates everything queued before it.
    pub(crate) async fn cancel_all_pending(&self) {
        let job_type = std::any::type_name::<Task>();
        if let Err(error) = sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Done' \
             WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(job_type)
        .execute(self.pool())
        .await
        {
            warn!(
                target: "rebalance",
                %error,
                job_type,
                "Failed to cancel pending rows for job type",
            );
        }
    }

    /// Resets this queue's in-flight rows (`Running`/`Queued`) back to
    /// `Pending` so the apalis monitor re-drives them, and returns the number
    /// of rows reset.
    ///
    /// Must only be called at startup, BEFORE the apalis monitor spawns: at
    /// that point no worker is alive to legitimately own a `Running` row, so
    /// every such row is necessarily orphaned by a previous process that died
    /// mid-job. apalis's own orphan recovery cannot rescue these on a quick
    /// restart -- it re-enqueues a locked row only once the owning worker's
    /// heartbeat ages past `reenqueue_orphaned_after` (5 min default), but the
    /// worker name is deterministic across restarts, so a fresh process
    /// re-registers the same worker id and keeps refreshing its heartbeat,
    /// and the orphan is never aged out. Resetting the row here closes that
    /// gap. `Failed` rows (retries exhausted) are deliberately left untouched
    /// so a latched job awaiting operator reconciliation is not re-driven on
    /// every restart. `attempts` is preserved because a crash is not a failed
    /// attempt against the retry budget.
    pub(crate) async fn requeue_orphaned(&self) -> Result<u64, SqlxError> {
        let job_type = std::any::type_name::<Task>();
        let result = sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Pending', lock_by = NULL, lock_at = NULL \
             WHERE job_type = ? AND status IN ('Running', 'Queued')",
        )
        .bind(job_type)
        .execute(self.pool())
        .await?;

        Ok(result.rows_affected())
    }

    /// Returns whether any job of this queue's task type is still in flight --
    /// i.e. present in the queue and not in a terminal state. Apalis terminal
    /// states are `Done`, `Failed`, and `Killed` (mirroring the finished-job
    /// cleanup); anything else (`Pending`, `Queued`, `Running`) counts as in
    /// flight.
    ///
    /// Used by the order-fill poller to avoid stacking overlapping backfill
    /// ranges: while a previous range is still being processed the checkpoint
    /// has not advanced yet, so re-enqueuing would re-scan the same blocks.
    pub(crate) async fn has_in_flight(&self) -> Result<bool, SqlxError> {
        let job_type = std::any::type_name::<Task>();
        let in_flight = sqlx_apalis::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM Jobs \
             WHERE job_type = ? AND status NOT IN ('Done', 'Failed', 'Killed')",
        )
        .bind(job_type)
        .fetch_one(self.pool())
        .await?;

        Ok(in_flight > 0)
    }
}

/// A persistent, retryable unit of work backed by apalis storage.
///
/// Implementations are serializable structs that carry the data
/// needed to process a single job. The `Ctx` type parameter
/// bundles all runtime dependencies (executor, CQRS frameworks,
/// config, etc.) into one struct injected via apalis `Data`.
///
/// The `Output` associated type is what downstream apalis-workflow
/// stages receive when this job is composed into a DAG. Leaf jobs
/// that don't feed anything use `type Output = ();`.
///
/// `WORKER_NAME`, `TERMINAL_FAILURE_MSG`, and `JOB_KIND` are read
/// by the shared [`build_supervised_worker!`] macro so each Job
/// impl carries everything `Monitor::register` needs.
pub(crate) trait Job<Ctx>: Serialize + DeserializeOwned + Send + 'static
where
    Ctx: Send + Sync + 'static,
{
    /// Value produced on successful completion. Becomes the input
    /// of the next stage in apalis-workflow DAGs.
    type Output: Send + 'static;

    /// Error type returned by [`perform`](Job::perform).
    type Error: std::error::Error + Send + Sync + 'static;

    /// Worker name prefix; the registered worker name is
    /// `format!("{WORKER_NAME}-{index}")`.
    const WORKER_NAME: &'static str;

    /// Logged when retries are exhausted and the supervisor receives
    /// a terminal failure for this job.
    const TERMINAL_FAILURE_MSG: &'static str = "Job failed after retries";

    /// Identifier for this job type in the e2e [`FailureInjector`].
    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: JobKind;

    /// Human-readable label for structured logging.
    fn label(&self) -> Label;

    /// Process this job using the provided context.
    async fn perform(&self, ctx: &Ctx) -> Result<Self::Output, Self::Error>;
}

/// Shared worker-construction body for [`build_supervised_worker!`]. Not part
/// of the public crate API; always called via the public macro. Must be
/// exported so the outer macro can call it from expansion sites in sibling
/// modules.
///
/// `$on_event:expr` must be a value of type
/// `impl Fn(&WorkerContext, &Event) + Send + Sync + 'static`, produced by
/// [`on_terminal_failure`].
macro_rules! build_worker_inner {
    (
        ::<$ctx_type:ty, $job:ty>,
        $index:expr,
        $queue:expr,
        $ctx:expr,
        $on_event:expr
        $(, $failure_injector:expr)? $(,)?
    ) => {{
        use ::apalis::layers::WorkerBuilderExt;
        use ::apalis::layers::retry::RetryPolicy;
        use ::apalis::prelude::WorkerBuilder;
        use ::apalis_core::worker::ext::event_listener::EventListenerExt;

        let builder = WorkerBuilder::new(format!(
            "{}-{}",
            <$job as $crate::conductor::job::Job<$ctx_type>>::WORKER_NAME,
            $index,
        ))
        .backend($queue.into_storage())
        .data($ctx);

        $(
            #[cfg(any(test, feature = "test-support"))]
            let builder = builder.data($failure_injector).data(
                <$job as $crate::conductor::job::Job<$ctx_type>>::JOB_KIND,
            );
        )?

        builder
            .concurrency(1)
            .retry(
                RetryPolicy::retries(3)
                    .with_backoff($crate::conductor::job::RETRY_BACKOFF.clone()),
            )
            .on_event($on_event)
            .build($crate::conductor::job::work::<$ctx_type, $job>)
    }};
}

pub(crate) use build_worker_inner;

/// Builds a `Worker` for a `Job<Ctx>` impl.
///
/// Mirrors the `work::<Ctx, Job>` turbofish style: pass the same two
/// types and the macro expands to a fully-wired worker (queue backend,
/// retry policy, terminal-failure notifier, `.build(work::<Ctx, Job>)`).
///
/// Deliberately installs no Apalis circuit-breaker layer -- see
/// [`build_best_effort_worker!`]'s doc comment for why: an open circuit can
/// return `Poll::Pending` from `poll_ready` without scheduling a wakeup,
/// permanently latching a single-concurrency worker idle with no log and no
/// stop (RAI-1495). `on_terminal_failure`'s `ctx.stop()`, fired the instant
/// `RetryPolicy` exhausts, is the sole halt mechanism.
///
/// A macro because `.build()` returns a deeply-nested
/// `Worker<Args, Ctx, Backend, Svc, Middleware>` whose `Svc` and
/// `Middleware` types accumulate from the layer stack and have no
/// public alias or `impl Trait` shorthand. Macro expansion lets the
/// compiler infer the type at the call site.
macro_rules! build_supervised_worker {
    (
        ::<$ctx_type:ty, $job:ty>,
        $index:expr,
        $queue:expr,
        $ctx:expr,
        $failure_notify:expr
        $(, $failure_injector:expr)? $(,)?
    ) => {{
        build_worker_inner!(
            ::<$ctx_type, $job>,
            $index,
            $queue,
            $ctx,
            $crate::conductor::job::on_terminal_failure(
                $failure_notify,
                <$job as $crate::conductor::job::Job<$ctx_type>>::TERMINAL_FAILURE_MSG,
            )
            $(, $failure_injector)?
        )
    }};
}

pub(crate) use build_supervised_worker;

/// Builds a best-effort `Worker` for a `Job<Ctx>` impl.
///
/// A terminal job failure is logged at `error!` level but does NOT trip the
/// conductor-wide fail-stop, does NOT stop the worker, and does NOT install an
/// Apalis circuit breaker. The circuit breaker can latch a single-concurrency
/// worker idle after retries are exhausted because `poll_ready` returns
/// `Pending` without scheduling a wakeup when the circuit is open.
///
/// Use for jobs whose exhausted item is a visible dead letter rather than a
/// process-level fault, including equity transfers and background tokenization
/// recovery. A persistently failing individual job must not block sibling jobs
/// or bring down hedging and fill detection.
macro_rules! build_best_effort_worker {
    (
        ::<$ctx_type:ty, $job:ty>,
        $index:expr,
        $queue:expr,
        $ctx:expr
        $(, $failure_injector:expr)? $(,)?
    ) => {{
        use ::apalis::layers::WorkerBuilderExt;
        use ::apalis::layers::retry::RetryPolicy;
        use ::apalis::prelude::WorkerBuilder;
        use ::apalis_core::worker::ext::event_listener::EventListenerExt;

        let builder = WorkerBuilder::new(format!(
            "{}-{}",
            <$job as $crate::conductor::job::Job<$ctx_type>>::WORKER_NAME,
            $index,
        ))
        .backend($queue.into_storage())
        .data($ctx);

        $(
            #[cfg(any(test, feature = "test-support"))]
            let builder = builder.data($failure_injector).data(
                <$job as $crate::conductor::job::Job<$ctx_type>>::JOB_KIND,
            );
        )?

        builder
            .concurrency(1)
            .retry(
                RetryPolicy::retries(3)
                    .with_backoff($crate::conductor::job::RETRY_BACKOFF.clone()),
            )
            .on_event($crate::conductor::job::on_terminal_failure_log_only(
                <$job as $crate::conductor::job::Job<$ctx_type>>::TERMINAL_FAILURE_MSG,
            ))
            .build($crate::conductor::job::work::<$ctx_type, $job>)
    }};
}

pub(crate) use build_best_effort_worker;

/// Human-readable identifier for an enqueued job, used in structured logging.
#[derive(Debug)]
pub(crate) struct Label(String);

impl Label {
    pub(crate) fn new(label: impl Into<String>) -> Self {
        Self(label.into())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Label {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Identifies which job queue a [`FailureInjector`] targets.
#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum JobKind {
    OrderFill,
    Hedge,
    Backfill,
    PollOrderStatus,
    ReconcileOrderFill,
    HandleOrderRejection,
    EquityRebalancingCheck,
    UsdcRebalancingCheck,
    SeedVaultRegistry,
    WrappedEquityRecovery,
    UnwrappedEquityRecovery,
    CheckPositions,
    TransferUsdcToHedging,
    TransferUsdcToMarketMaking,
    TransferEquityToMarketMaking,
    TransferEquityToHedging,
    ResumeTokenizationAggregate,
    DashboardTradeDelivery,
    PortfolioSnapshot,
    RecordBotGasReceiptCost,
}

/// Job execution error. Wraps the concrete `Job::Error` type at
/// the `work()` boundary where the handler is generic over job types.
#[derive(Debug, thiserror::Error)]
pub(crate) enum JobError {
    #[error("{label}: {source}")]
    Failed {
        label: Label,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[cfg(any(test, feature = "test-support"))]
    #[error("injected terminal job failure")]
    Injected,
}

/// Allows e2e tests to force the next job of a specific kind to
/// fail terminally. Each [`JobKind`] has an independent injection
/// state so arming one queue cannot be consumed by the other.
#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug)]
pub struct FailureInjector {
    order_fill: Arc<Mutex<InjectionState>>,
    hedge: Arc<Mutex<InjectionState>>,
    backfill: Arc<Mutex<InjectionState>>,
    poll_order_status: Arc<Mutex<InjectionState>>,
    reconcile_order_fill: Arc<Mutex<InjectionState>>,
    handle_order_rejection: Arc<Mutex<InjectionState>>,
    equity_rebalancing_check: Arc<Mutex<InjectionState>>,
    usdc_rebalancing_check: Arc<Mutex<InjectionState>>,
    seed_vault_registry: Arc<Mutex<InjectionState>>,
    wrapped_equity_recovery: Arc<Mutex<InjectionState>>,
    unwrapped_equity_recovery: Arc<Mutex<InjectionState>>,
    check_positions: Arc<Mutex<InjectionState>>,
    transfer_usdc_to_hedging: Arc<Mutex<InjectionState>>,
    transfer_usdc_to_market_making: Arc<Mutex<InjectionState>>,
    transfer_equity_to_market_making: Arc<Mutex<InjectionState>>,
    transfer_equity_to_hedging: Arc<Mutex<InjectionState>>,
    resume_tokenization_aggregate: Arc<Mutex<InjectionState>>,
    dashboard_trade_delivery: Arc<Mutex<InjectionState>>,
    portfolio_snapshot: Arc<Mutex<InjectionState>>,
    record_bot_gas_receipt_cost: Arc<Mutex<InjectionState>>,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Debug, Default)]
enum InjectionState {
    #[default]
    Idle,
    Armed,
    Targeted(String),
}

#[cfg(any(test, feature = "test-support"))]
impl Default for FailureInjector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "test-support"))]
impl FailureInjector {
    pub fn new() -> Self {
        Self {
            order_fill: Arc::new(Mutex::new(InjectionState::Idle)),
            hedge: Arc::new(Mutex::new(InjectionState::Idle)),
            backfill: Arc::new(Mutex::new(InjectionState::Idle)),
            poll_order_status: Arc::new(Mutex::new(InjectionState::Idle)),
            reconcile_order_fill: Arc::new(Mutex::new(InjectionState::Idle)),
            handle_order_rejection: Arc::new(Mutex::new(InjectionState::Idle)),
            equity_rebalancing_check: Arc::new(Mutex::new(InjectionState::Idle)),
            usdc_rebalancing_check: Arc::new(Mutex::new(InjectionState::Idle)),
            seed_vault_registry: Arc::new(Mutex::new(InjectionState::Idle)),
            wrapped_equity_recovery: Arc::new(Mutex::new(InjectionState::Idle)),
            unwrapped_equity_recovery: Arc::new(Mutex::new(InjectionState::Idle)),
            check_positions: Arc::new(Mutex::new(InjectionState::Idle)),
            transfer_usdc_to_hedging: Arc::new(Mutex::new(InjectionState::Idle)),
            transfer_usdc_to_market_making: Arc::new(Mutex::new(InjectionState::Idle)),
            transfer_equity_to_market_making: Arc::new(Mutex::new(InjectionState::Idle)),
            transfer_equity_to_hedging: Arc::new(Mutex::new(InjectionState::Idle)),
            resume_tokenization_aggregate: Arc::new(Mutex::new(InjectionState::Idle)),
            dashboard_trade_delivery: Arc::new(Mutex::new(InjectionState::Idle)),
            portfolio_snapshot: Arc::new(Mutex::new(InjectionState::Idle)),
            record_bot_gas_receipt_cost: Arc::new(Mutex::new(InjectionState::Idle)),
        }
    }

    pub fn arm(&self, kind: JobKind) {
        *self.lock_state(kind) = InjectionState::Armed;
    }

    #[cfg(test)]
    fn is_armed(&self, kind: JobKind) -> bool {
        let state = &mut *self.lock_state(kind);
        let was_armed = matches!(state, InjectionState::Armed);

        if was_armed {
            *state = InjectionState::Idle;
        }

        was_armed
    }

    fn should_inject(&self, kind: JobKind, label: &Label) -> bool {
        let state = &mut *self.lock_state(kind);

        match state {
            InjectionState::Idle => false,
            InjectionState::Armed => {
                *state = InjectionState::Targeted(label.as_str().to_owned());
                true
            }
            InjectionState::Targeted(target_label) => target_label == label.as_str(),
        }
    }

    fn lock_state(&self, kind: JobKind) -> std::sync::MutexGuard<'_, InjectionState> {
        let mutex = match kind {
            JobKind::OrderFill => &self.order_fill,
            JobKind::Hedge => &self.hedge,
            JobKind::Backfill => &self.backfill,
            JobKind::PollOrderStatus => &self.poll_order_status,
            JobKind::ReconcileOrderFill => &self.reconcile_order_fill,
            JobKind::HandleOrderRejection => &self.handle_order_rejection,
            JobKind::EquityRebalancingCheck => &self.equity_rebalancing_check,
            JobKind::UsdcRebalancingCheck => &self.usdc_rebalancing_check,
            JobKind::SeedVaultRegistry => &self.seed_vault_registry,
            JobKind::WrappedEquityRecovery => &self.wrapped_equity_recovery,
            JobKind::UnwrappedEquityRecovery => &self.unwrapped_equity_recovery,
            JobKind::CheckPositions => &self.check_positions,
            JobKind::TransferUsdcToHedging => &self.transfer_usdc_to_hedging,
            JobKind::TransferUsdcToMarketMaking => &self.transfer_usdc_to_market_making,
            JobKind::TransferEquityToMarketMaking => &self.transfer_equity_to_market_making,
            JobKind::TransferEquityToHedging => &self.transfer_equity_to_hedging,
            JobKind::ResumeTokenizationAggregate => &self.resume_tokenization_aggregate,
            JobKind::DashboardTradeDelivery => &self.dashboard_trade_delivery,
            JobKind::PortfolioSnapshot => &self.portfolio_snapshot,
            JobKind::RecordBotGasReceiptCost => &self.record_bot_gas_receipt_cost,
        };

        match mutex.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    async fn perform<Ctx, J: Job<Ctx> + Sync>(
        &self,
        kind: JobKind,
        job: &J,
        ctx: &Ctx,
        attempt: usize,
    ) -> Result<J::Output, JobError>
    where
        Ctx: Send + Sync + 'static,
    {
        let label = job.label();

        if self.should_inject(kind, &label) {
            return Err(JobError::Injected);
        }

        log_processing(&label, attempt);
        job.perform(ctx).await.map_err(|source| JobError::Failed {
            label,
            source: Box::new(source),
        })
    }
}

fn log_processing(label: &Label, attempt: usize) {
    if attempt <= 1 {
        debug!(%label, "Processing job");
    } else {
        warn!(%label, attempt, "Retrying job after transient failure");
    }
}

/// Generic apalis handler -- test-support build.
#[cfg(any(test, feature = "test-support"))]
pub(crate) async fn work<Ctx, J>(
    job: J,
    ctx: Data<Arc<Ctx>>,
    injector: Data<FailureInjector>,
    kind: Data<JobKind>,
    attempt: Attempt,
) -> Result<J::Output, JobError>
where
    Ctx: Send + Sync + 'static,
    J: Job<Ctx> + Sync,
{
    injector.perform(*kind, &job, &ctx, attempt.current()).await
}

/// Generic apalis handler -- production build.
#[cfg(not(feature = "test-support"))]
pub(crate) async fn work<Ctx, J>(
    job: J,
    ctx: Data<Arc<Ctx>>,
    attempt: Attempt,
) -> Result<J::Output, JobError>
where
    Ctx: Send + Sync + 'static,
    J: Job<Ctx> + Sync,
{
    let label = job.label();
    log_processing(&label, attempt.current());
    job.perform(&ctx).await.map_err(|source| JobError::Failed {
        label,
        source: Box::new(source),
    })
}

/// Worker name, static failure context, and the original apalis error for a
/// supervised worker's terminal failure (retries exhausted), captured for the
/// async exit path (`Conductor::wait_for_completion`) to alert an operator on
/// before the process returns a non-zero exit code. `source` is the same
/// `Arc` apalis handed to `on_terminal_failure` (cheap to clone), not a
/// flattened string, so `MonitorTaskError::TerminalJobFailure`'s `#[source]`
/// chain -- and therefore anyhow's "Caused by:" rendering -- still reaches the
/// underlying error. The error is also logged in full via `error!()` at the
/// point of failure; this is the same error carried forward, not a
/// replacement for the structured log.
#[derive(Debug, Clone)]
pub(crate) struct TerminalFailureInfo {
    pub(crate) worker: String,
    pub(crate) context: &'static str,
    pub(crate) source: Arc<BoxDynError>,
}

/// Wakes the apalis monitor's `tokio::select!` on a supervised worker's
/// terminal failure and carries the [`TerminalFailureInfo`] the async exit
/// path needs to alert on. Threaded through every `build_supervised_worker!`
/// call site as a single `Arc` clone -- the same shape as the plain
/// `tokio::sync::Notify` it replaces, so no call site needs an extra clone.
///
/// All supervised workers share one signal, so two different workers can
/// fail terminally close together. `info` resolves that race first-writer-
/// wins via `OnceLock::set`: whichever `record_and_notify` call lands first
/// is the info every reader ever observes, atomically -- never a torn or
/// mixed combination of two failures. The later failure's own `ctx.stop()`
/// and `error!()` log still happen regardless; only its entry in the alert
/// is superseded.
///
/// `info` is always recorded before `notify_waiters()` fires, so a waiter that
/// wakes always has the info available. [`notified`](Self::notified) returns it
/// directly rather than leaving the caller to re-read an `Option` it cannot
/// meaningfully handle as `None`.
#[derive(Default)]
pub(crate) struct TerminalFailureSignal {
    notify: tokio::sync::Notify,
    info: std::sync::OnceLock<TerminalFailureInfo>,
}

impl TerminalFailureSignal {
    /// Resolves with the recorded failure once any supervised worker sharing
    /// this signal fails terminally, whether that failure happened before or
    /// after this call.
    ///
    /// `Notify::notify_waiters` wakes only the waiters registered at that
    /// instant and stores no permit, so a failure recorded before the monitor
    /// reaches its `select!` would otherwise be lost -- the same missed-wakeup
    /// shape RAI-1495 is about. Registering the `Notified` future via
    /// `enable()` *before* reading `info` closes that window in both
    /// directions: a failure landing before `enable()` is caught by the
    /// `Some` check below, and one landing after is caught by the registered
    /// waiter.
    ///
    /// The loop re-registers rather than unwrapping after the await:
    /// `record_and_notify` writes `info` before waking, so the first wakeup
    /// carries it, but expressing that as a loop keeps the "signalled with no
    /// info" state unrepresentable without a panicking `expect`.
    pub(crate) async fn notified(&self) -> TerminalFailureInfo {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if let Some(info) = self.info.get() {
                return info.clone();
            }

            notified.await;
        }
    }

    fn record_and_notify(&self, info: TerminalFailureInfo) {
        // `OnceLock::set` is a first-writer-wins compare-and-set: on a race
        // between two workers, the losing `Err(_)` is intentionally dropped
        // (see the doc comment above), but every worker still notifies so no
        // failure's fail-stop is ever lost.
        let _ = self.info.set(info);
        self.notify.notify_waiters();
    }
}

/// On-event handler shared by every supervised worker: when apalis
/// reports a terminal job failure (retries exhausted), record the failure
/// info, notify the monitor task, and stop the worker.
pub(crate) fn on_terminal_failure(
    failure_signal: Arc<TerminalFailureSignal>,
    error_msg: &'static str,
) -> impl Fn(&WorkerContext, &Event) + Send + Sync + 'static {
    move |ctx, event| {
        if let Event::Error(err) = event {
            let worker = ctx.name().clone();
            error!(%err, worker = %worker, "{error_msg}");
            failure_signal.record_and_notify(TerminalFailureInfo {
                worker,
                context: error_msg,
                source: Arc::clone(err),
            });
            let _ = ctx.stop();
        }
    }
}

/// On-event handler for workers where terminal failure must not crash the
/// conductor. Logs the error at `error!` level but does NOT call
/// `failure_notify.notify_waiters()` and does NOT call `ctx.stop()`.
/// Used for dead-lettering per-item workers (equity transfers and background
/// tokenization recovery) where a persistently failing individual job should
/// not bring down hedging and fill detection. See [`build_best_effort_worker!`]
/// for why these workers do not install Apalis' circuit-breaker layer.
///
/// Structural invariant: unlike [`on_terminal_failure`], this function takes
/// no `Notify` argument and therefore can never call `notify_waiters()` or
/// `ctx.stop()`, regardless of how it is called.
pub(crate) fn on_terminal_failure_log_only(
    error_msg: &'static str,
) -> impl Fn(&WorkerContext, &Event) + Send + Sync + 'static {
    move |ctx, event| {
        if let Event::Error(err) = event {
            error!(%err, worker = %ctx.name(), "{error_msg}");
        }
    }
}

#[cfg(test)]
mod tests {
    use apalis::prelude::{Monitor, Status};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::test_utils::setup_test_apalis_pool;

    #[derive(Debug, thiserror::Error)]
    #[error("intermediate wrapper: {0}")]
    struct IntermediateTestError(#[source] AlpacaBrokerApiError);

    fn api_error_429(retry_after: Option<Duration>) -> AlpacaBrokerApiError {
        AlpacaBrokerApiError::ApiError {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            alpaca_code: None,
            message: "rate limited".to_string(),
            retry_after,
        }
    }

    #[test]
    fn find_backpressure_walks_multiple_hops_to_a_wrapped_429() {
        let error = IntermediateTestError(api_error_429(Some(Duration::from_secs(7))));

        let backpressure = find_backpressure(&error).expect("expected a classified 429");
        assert_eq!(backpressure.retry_after, Some(Duration::from_secs(7)));
    }

    #[test]
    fn find_backpressure_none_for_an_unrelated_error_chain() {
        let io_error = std::io::Error::other("boom");

        assert_eq!(find_backpressure(&io_error), None);
    }

    #[derive(Debug, thiserror::Error)]
    #[error("intermediate wrapper: {0}")]
    struct IntermediateWalletTestError(#[source] AlpacaWalletError);

    #[derive(Debug, thiserror::Error)]
    #[error("intermediate wrapper: {0}")]
    struct IntermediateTokenizationTestError(#[source] AlpacaTokenizationError);

    /// Pins the REAL production wrapping shape (not a synthetic intermediate
    /// wrapper): `fetch_latest_trade_price` failures surface as
    /// `AlpacaBrokerApiError::LatestTrade(AlpacaMarketDataError)`, and
    /// `AlpacaBrokerApiError::backpressure()` delegates straight to the
    /// wrapped market-data error (RAI-1494) instead of `find_backpressure`
    /// needing its own separate `AlpacaMarketDataError` downcast.
    #[test]
    fn find_backpressure_classifies_a_market_data_429_wrapped_in_latest_trade() {
        let error = AlpacaBrokerApiError::LatestTrade(Box::new(
            st0x_execution::AlpacaMarketDataError::ApiError {
                status: reqwest::StatusCode::TOO_MANY_REQUESTS,
                body: "rate limited".to_string(),
                retry_after: Some(Duration::from_secs(9)),
            },
        ));

        let backpressure = find_backpressure(&error).expect("expected a classified 429");
        assert_eq!(backpressure.retry_after, Some(Duration::from_secs(9)));
    }

    #[test]
    fn find_permanence_walks_multiple_hops_to_a_wrapped_403() {
        let error = IntermediateTestError(AlpacaBrokerApiError::LatestQuote(Box::new(
            st0x_execution::AlpacaMarketDataError::ApiError {
                status: reqwest::StatusCode::FORBIDDEN,
                body: "subscription does not permit querying recent SIP data".to_string(),
                retry_after: None,
            },
        )));

        assert_eq!(find_permanence(&error), Some(Permanence::Permanent));
    }

    #[test]
    fn find_permanence_walks_multiple_hops_to_a_wrapped_500() {
        let error = IntermediateTestError(AlpacaBrokerApiError::ApiError {
            status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            alpaca_code: None,
            message: "boom".to_string(),
            retry_after: None,
        });

        assert_eq!(find_permanence(&error), Some(Permanence::Transient));
    }

    #[test]
    fn find_permanence_none_for_an_unrelated_error_chain() {
        let io_error = std::io::Error::other("boom");

        assert_eq!(find_permanence(&io_error), None);
    }

    #[test]
    fn find_backpressure_walks_multiple_hops_to_a_wrapped_wallet_429() {
        let error = IntermediateWalletTestError(AlpacaWalletError::ApiError {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            message: "rate limited".to_string(),
            retry_after: Some(Duration::from_secs(11)),
        });

        let backpressure = find_backpressure(&error).expect("expected a classified 429");
        assert_eq!(backpressure.retry_after, Some(Duration::from_secs(11)));
    }

    #[test]
    fn find_backpressure_walks_multiple_hops_to_a_wrapped_tokenization_429() {
        let error = IntermediateTokenizationTestError(AlpacaTokenizationError::ApiError {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            message: st0x_tokenization::AlpacaApiErrorMessage::for_test("rate limited"),
            retry_after: Some(Duration::from_secs(13)),
        });

        let backpressure = find_backpressure(&error).expect("expected a classified 429");
        assert_eq!(backpressure.retry_after, Some(Duration::from_secs(13)));
    }

    /// Pins the `#[error(transparent)]` gotcha `TokenizerError::backpressure()`
    /// exists to work around: every `Tokenizer` trait method returns
    /// `TokenizerError`, not `AlpacaTokenizationError` directly, and
    /// `#[error(transparent)]` makes `.source()` skip straight past the
    /// wrapped `AlpacaTokenizationError` to ITS OWN source (`None` for
    /// `ApiError`) instead of returning the wrapped error itself. Without the
    /// dedicated `TokenizerError` downcast in `find_backpressure`, a 429 from
    /// any `Tokenizer` call site (e.g. `retry_on_backpressure`-wrapped CLI
    /// tokenization calls) would never classify -- this regressed silently
    /// once during this change (a `TokenizerError`-typed 429 produced zero
    /// retries against a real mocked response) and must not regress again.
    #[test]
    fn find_backpressure_classifies_a_tokenizer_error_wrapped_429_despite_transparent_forwarding() {
        let error: st0x_tokenization::TokenizerError = AlpacaTokenizationError::ApiError {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            message: st0x_tokenization::AlpacaApiErrorMessage::for_test("rate limited"),
            retry_after: Some(Duration::from_secs(21)),
        }
        .into();

        let backpressure = find_backpressure(&error).expect("expected a classified 429");
        assert_eq!(backpressure.retry_after, Some(Duration::from_secs(21)));
    }

    #[test]
    fn decide_backpressure_floors_a_zero_retry_after() {
        let backpressure = Backpressure {
            retry_after: Some(Duration::ZERO),
        };

        let decision = decide_backpressure(&backpressure, BackpressureStreak(0));

        assert_eq!(decision.delay, MIN_BACKPRESSURE_DELAY);
        assert!(!decision.exhausted);
    }

    #[test]
    fn decide_backpressure_honours_a_large_retry_after_within_the_ceiling() {
        let backpressure = Backpressure {
            retry_after: Some(Duration::from_secs(120)),
        };

        let decision = decide_backpressure(&backpressure, BackpressureStreak(0));

        assert_eq!(decision.delay, Duration::from_secs(120));
    }

    #[test]
    fn decide_backpressure_caps_a_retry_after_above_the_ceiling() {
        let backpressure = Backpressure {
            retry_after: Some(Duration::from_secs(10 * 60)),
        };

        let decision = decide_backpressure(&backpressure, BackpressureStreak(0));

        assert_eq!(decision.delay, MAX_RETRY_AFTER);
    }

    #[test]
    fn decide_backpressure_escalates_the_fallback_when_retry_after_is_absent() {
        let backpressure = Backpressure { retry_after: None };

        let delays: Vec<Duration> = (0..8)
            .map(|streak| decide_backpressure(&backpressure, BackpressureStreak(streak)).delay)
            .collect();

        for window in delays.windows(2) {
            assert!(
                window[1] >= window[0],
                "the fallback delay must never decrease as the streak grows: {delays:?}"
            );
        }
        assert_eq!(
            delays.last().copied().unwrap(),
            BACKPRESSURE_FALLBACK_CAP,
            "the fallback must plateau at its cap for a long streak: {delays:?}"
        );
        assert!(
            delays[0] < delays[3],
            "the fallback must actually escalate, not start already at the cap: {delays:?}"
        );
    }

    #[test]
    fn decide_backpressure_is_not_exhausted_below_the_limit() {
        let backpressure = Backpressure {
            retry_after: Some(Duration::from_secs(1)),
        };

        let decision = decide_backpressure(
            &backpressure,
            BackpressureStreak(BACKPRESSURE_RESCHEDULE_LIMIT - 1),
        );

        assert!(!decision.exhausted);
    }

    #[test]
    fn decide_backpressure_is_exhausted_at_the_limit() {
        let backpressure = Backpressure {
            retry_after: Some(Duration::from_secs(1)),
        };

        let decision = decide_backpressure(
            &backpressure,
            BackpressureStreak(BACKPRESSURE_RESCHEDULE_LIMIT),
        );

        assert!(decision.exhausted);
    }

    #[test]
    fn advance_backpressure_reschedules_with_incremented_streak_below_the_limit() {
        let backpressure = Backpressure {
            retry_after: Some(Duration::from_secs(5)),
        };

        let step = advance_backpressure(&backpressure, BackpressureStreak(3));

        match step {
            BackpressureStep::Reschedule {
                next_streak,
                delay,
                visible,
            } => {
                assert_eq!(next_streak, BackpressureStreak(4));
                assert_eq!(delay, Duration::from_secs(5));
                assert!(!visible);
            }
            BackpressureStep::DeadLetter => panic!("expected a reschedule step, got DeadLetter"),
        }
    }

    #[test]
    fn advance_backpressure_marks_every_tenth_streak_visible() {
        let backpressure = Backpressure {
            retry_after: Some(Duration::from_secs(1)),
        };

        let step = advance_backpressure(&backpressure, BackpressureStreak(9));

        match step {
            BackpressureStep::Reschedule {
                next_streak,
                visible,
                ..
            } => {
                assert_eq!(next_streak, BackpressureStreak(10));
                assert!(visible);
            }
            BackpressureStep::DeadLetter => panic!("expected a reschedule step, got DeadLetter"),
        }
    }

    #[test]
    fn advance_backpressure_dead_letters_at_the_limit() {
        let backpressure = Backpressure {
            retry_after: Some(Duration::from_secs(1)),
        };

        let step = advance_backpressure(
            &backpressure,
            BackpressureStreak(BACKPRESSURE_RESCHEDULE_LIMIT),
        );

        match step {
            BackpressureStep::DeadLetter => {}
            BackpressureStep::Reschedule { .. } => {
                panic!("expected DeadLetter at the reschedule limit")
            }
        }
    }

    #[tokio::test]
    async fn apply_backpressure_step_dead_letter_pushes_nothing() {
        let apalis_pool = setup_test_apalis_pool().await;
        let mut queue = JobQueue::<u32>::new(&apalis_pool);

        let outcome = apply_backpressure_step(BackpressureStep::DeadLetter, &mut queue, |_| {
            panic!("build_successor must not be called on the DeadLetter branch")
        })
        .await
        .unwrap();

        match outcome {
            BackpressureOutcome::DeadLettered => {}
            BackpressureOutcome::Rescheduled { .. } => {
                panic!("expected DeadLettered, got Rescheduled")
            }
        }
        assert!(
            !queue.has_in_flight().await.unwrap(),
            "DeadLetter must not push a successor"
        );
    }

    #[tokio::test]
    async fn apply_backpressure_step_reschedule_pushes_the_built_successor() {
        let apalis_pool = setup_test_apalis_pool().await;
        let mut queue = JobQueue::<u32>::new(&apalis_pool);

        let step = BackpressureStep::Reschedule {
            next_streak: BackpressureStreak(7),
            delay: Duration::from_millis(1),
            visible: true,
        };

        let outcome =
            apply_backpressure_step(step, &mut queue, |BackpressureStreak(streak)| streak)
                .await
                .unwrap();

        match outcome {
            BackpressureOutcome::Rescheduled {
                next_streak,
                visible,
            } => {
                assert_eq!(next_streak, BackpressureStreak(7));
                assert!(visible);
            }
            BackpressureOutcome::DeadLettered => panic!("expected Rescheduled, got DeadLettered"),
        }

        let pushed_payload = sqlx_apalis::query_scalar::<_, String>(
            "SELECT CAST(job AS TEXT) FROM Jobs WHERE job_type = ?",
        )
        .bind(std::any::type_name::<u32>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        assert_eq!(
            pushed_payload, "7",
            "the pushed successor must be built from the step's next_streak"
        );
    }

    #[tokio::test]
    async fn has_in_flight_detects_pending_and_ignores_terminal() {
        let apalis_pool = setup_test_apalis_pool().await;
        let mut queue = JobQueue::<u32>::new(&apalis_pool);

        assert!(
            !queue.has_in_flight().await.unwrap(),
            "empty queue has nothing in flight"
        );

        queue.push(42u32).await.unwrap();
        assert!(
            queue.has_in_flight().await.unwrap(),
            "a pending job counts as in flight"
        );

        // Drive the row to a terminal state; it must no longer count.
        sqlx_apalis::query("UPDATE Jobs SET status = 'Done' WHERE job_type = ?")
            .bind(std::any::type_name::<u32>())
            .execute(&apalis_pool)
            .await
            .unwrap();
        assert!(
            !queue.has_in_flight().await.unwrap(),
            "a Done job is terminal and not in flight"
        );
    }

    #[test]
    fn poll_config_fetches_one_row_per_single_concurrency_worker() {
        let config = build_poll_config::<u32>();

        assert_eq!(
            config.buffer_size(),
            1,
            "workers run with concurrency(1), so the SQLite fetch buffer must \
             not reserve extra rows as Queued before a handler can execute them",
        );
    }

    #[test]
    fn failure_injector_not_armed_by_default() {
        let injector = FailureInjector::new();
        assert!(!injector.is_armed(JobKind::OrderFill));
        assert!(!injector.is_armed(JobKind::Hedge));
    }

    #[test]
    fn failure_injector_arm_then_check_auto_disarms() {
        let injector = FailureInjector::new();

        injector.arm(JobKind::OrderFill);
        assert!(injector.is_armed(JobKind::OrderFill));
        assert!(
            !injector.is_armed(JobKind::OrderFill),
            "second check should be false (auto-disarmed)"
        );
    }

    #[test]
    fn failure_injector_kinds_are_independent() {
        let injector = FailureInjector::new();

        injector.arm(JobKind::OrderFill);
        assert!(
            !injector.is_armed(JobKind::Hedge),
            "arming OrderFill should not affect Hedge"
        );
        assert!(injector.is_armed(JobKind::OrderFill));
    }

    #[test]
    fn failure_injector_wrapped_equity_recovery_isolated() {
        let injector = FailureInjector::new();

        injector.arm(JobKind::WrappedEquityRecovery);
        assert!(
            injector.is_armed(JobKind::WrappedEquityRecovery),
            "WrappedEquityRecovery should report armed after arm()",
        );
        assert!(
            !injector.is_armed(JobKind::WrappedEquityRecovery),
            "Second check should auto-disarm WrappedEquityRecovery",
        );

        injector.arm(JobKind::WrappedEquityRecovery);
        assert!(
            !injector.is_armed(JobKind::OrderFill),
            "Arming WrappedEquityRecovery must not arm OrderFill",
        );
        assert!(
            !injector.is_armed(JobKind::Hedge),
            "Arming WrappedEquityRecovery must not arm Hedge",
        );
        assert!(
            matches!(
                &*injector.lock_state(JobKind::WrappedEquityRecovery),
                InjectionState::Armed
            ),
            "WrappedEquityRecovery state should remain Armed when an unrelated kind is queried",
        );
    }

    #[test]
    fn failure_injector_unwrapped_equity_recovery_isolated() {
        let injector = FailureInjector::new();

        injector.arm(JobKind::UnwrappedEquityRecovery);
        assert!(
            injector.is_armed(JobKind::UnwrappedEquityRecovery),
            "UnwrappedEquityRecovery should report armed after arm()",
        );
        assert!(
            !injector.is_armed(JobKind::UnwrappedEquityRecovery),
            "Second check should auto-disarm UnwrappedEquityRecovery",
        );

        injector.arm(JobKind::UnwrappedEquityRecovery);
        assert!(
            !injector.is_armed(JobKind::WrappedEquityRecovery),
            "Arming UnwrappedEquityRecovery must not arm WrappedEquityRecovery",
        );
        assert!(
            !injector.is_armed(JobKind::OrderFill),
            "Arming UnwrappedEquityRecovery must not arm OrderFill",
        );
        assert!(
            matches!(
                &*injector.lock_state(JobKind::UnwrappedEquityRecovery),
                InjectionState::Armed
            ),
            "UnwrappedEquityRecovery state should remain Armed when an unrelated kind is queried",
        );
    }

    #[test]
    fn failure_injector_targeted_label_latches_across_retries() {
        let injector = FailureInjector::new();
        let first = Label::new("job-a");
        let same = Label::new("job-a");
        let different = Label::new("job-b");

        injector.arm(JobKind::OrderFill);

        assert!(
            injector.should_inject(JobKind::OrderFill, &first),
            "Armed state should inject for the first label"
        );
        assert!(
            matches!(
                &*injector.lock_state(JobKind::OrderFill),
                InjectionState::Targeted(target) if target == "job-a"
            ),
            "state should latch to Targeted with the first label"
        );
        assert!(
            injector.should_inject(JobKind::OrderFill, &same),
            "Targeted state should keep injecting for the same label across retries"
        );
        assert!(
            !injector.should_inject(JobKind::OrderFill, &different),
            "Targeted state should not inject for a different label"
        );
    }

    #[test]
    fn failure_injector_shared_across_clones() {
        let injector = FailureInjector::new();
        let clone = injector.clone();

        injector.arm(JobKind::Hedge);
        assert!(
            clone.is_armed(JobKind::Hedge),
            "arming original should be visible from clone"
        );
        assert!(
            !injector.is_armed(JobKind::Hedge),
            "should be disarmed after clone consumed it"
        );
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct TestJob {
        should_fail: bool,
    }

    struct TestCtx {
        success_count: AtomicUsize,
        success_notify: Arc<tokio::sync::Notify>,
        failing_job_started: Arc<tokio::sync::Notify>,
    }

    impl Job<TestCtx> for TestJob {
        type Output = ();
        type Error = TestJobError;

        const WORKER_NAME: &'static str = "test-worker";
        const JOB_KIND: JobKind = JobKind::OrderFill;

        fn label(&self) -> Label {
            Label::new(format!("test-job(should_fail={})", self.should_fail))
        }

        async fn perform(&self, ctx: &TestCtx) -> Result<Self::Output, Self::Error> {
            if self.should_fail {
                ctx.failing_job_started.notify_waiters();
                return Err(TestJobError);
            }

            ctx.success_count.fetch_add(1, Ordering::SeqCst);
            ctx.success_notify.notify_waiters();
            Ok(())
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("test job deliberately failed")]
    struct TestJobError;

    /// A job that fails after all retries must halt further processing --
    /// the worker must not pick up the next job with stale state.
    ///
    /// Note: this test's single failing job halts via `on_terminal_failure`
    /// at `RetryPolicy`'s own exhaustion (attempt 4 of `retries(3)`), well
    /// below the vendored circuit breaker's hardcoded `failure_count >= 5`
    /// open threshold that used to sit in this path -- so this test passed
    /// even before RAI-1495's fix and never exercised the removed layer. See
    /// [`supervised_worker_fail_stops_past_the_vendored_circuit_breakers_hardcoded_threshold`]
    /// for the regression test that does.
    #[tokio::test]
    async fn job_failure_after_retries_halts_processing() {
        let apalis_pool = setup_test_apalis_pool().await;

        let mut queue: JobQueue<TestJob> = JobQueue::new(&apalis_pool);
        queue.push(TestJob { should_fail: true }).await.unwrap();
        let mut push_queue = queue.clone();

        let failure_notify = Arc::new(TerminalFailureSignal::default());
        let failing_job_started = Arc::new(tokio::sync::Notify::new());
        let ctx = Arc::new(TestCtx {
            success_count: AtomicUsize::new(0),
            success_notify: Arc::new(tokio::sync::Notify::new()),
            failing_job_started: failing_job_started.clone(),
        });
        let ctx_for_assert = ctx.clone();

        let failing_job_started_wait = failing_job_started.notified();

        let monitor_handle = tokio::spawn({
            let failure_notify = failure_notify.clone();
            let monitor = Monitor::new()
                .should_restart(|_ctx, _error, _attempt| false)
                .register(move |index| {
                    build_supervised_worker!(
                        ::<TestCtx, TestJob>,
                        index,
                        queue.clone(),
                        ctx.clone(),
                        failure_notify.clone(),
                        FailureInjector::new(),
                    )
                });

            async move {
                let _ = monitor.run().await;
            }
        });

        if tokio::time::timeout(Duration::from_secs(10), failing_job_started_wait)
            .await
            .is_err()
        {
            panic!(
                "failing job should start before sibling is enqueued; job rows: {:?}",
                job_rows_for_assertion(&apalis_pool).await
            );
        }

        push_queue
            .push(TestJob { should_fail: false })
            .await
            .unwrap();

        wait_for_fail_stop_without_processing_sibling(
            &apalis_pool,
            &failure_notify,
            monitor_handle,
        )
        .await;

        assert_eq!(
            ctx_for_assert.success_count.load(Ordering::SeqCst),
            0,
            "The second job should NOT have been processed after a prior \
             job failed all retries; job rows: {:?}",
            job_rows_for_assertion(&apalis_pool).await
        );
    }

    async fn wait_for_fail_stop_without_processing_sibling(
        apalis_pool: &apalis_sqlite::SqlitePool,
        failure_notify: &Arc<TerminalFailureSignal>,
        monitor_handle: tokio::task::JoinHandle<()>,
    ) {
        let terminal = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let rows = job_rows_for_assertion(apalis_pool).await;
                if rows
                    .iter()
                    .any(|(_, status, _, _, _)| status == &Status::Done.to_string())
                {
                    panic!(
                        "pre-queued sibling job must not reach Done while the failing job \
                         is still retrying or before the worker halts; job rows: {rows:?}"
                    );
                }

                if monitor_handle.is_finished() {
                    return;
                }

                tokio::select! {
                    _ = failure_notify.notified() => {
                        // Terminal failure fired stop; give the monitor a moment to exit.
                        for _ in 0..100 {
                            if monitor_handle.is_finished() {
                                return;
                            }
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        return;
                    }
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
            }
        })
        .await;

        assert!(
            terminal.is_ok(),
            "failing job should reach terminal state and halt the worker; job rows at timeout: {:?}",
            job_rows_for_assertion(apalis_pool).await
        );

        let join_result = tokio::time::timeout(Duration::from_secs(5), monitor_handle)
            .await
            .expect("Monitor should exit within 5s after terminal job failure");
        join_result.expect("Monitor task should not panic");
    }

    /// Regression test for RAI-1495: a supervised worker driven through many
    /// more consecutive failures than the vendored circuit breaker's
    /// hardcoded `failure_count >= 5` open threshold
    /// (`CircuitBreakerService::call`, apalis-core 1.0.0-rc.9) must still
    /// reach a terminal, observable fail-stop -- not latch silently idle.
    /// `retries(12)` (13 attempts total) with a fast fixed backoff (not
    /// `RETRY_BACKOFF`, to keep the test fast) comfortably crosses that
    /// threshold. Uses a raw `WorkerBuilder` (not `build_supervised_worker!`,
    /// which hardcodes `retries(3)`) so the retry budget can be pushed past
    /// the vendored threshold; `job_failure_after_retries_halts_processing`
    /// above remains the macro-path guard for the real production wiring.
    ///
    /// What this proves: after RAI-1495's fix (no circuit-breaker layer at
    /// all on supervised workers), no in-process circuit-breaker-shaped
    /// mechanism can latch a worker regardless of accumulated failure count
    /// -- the terminal failure signal fires deterministically, asserted via
    /// `failure_notify.notified()` resolving and the captured
    /// [`TerminalFailureInfo`], not merely "no latch observed within a
    /// timeout" (a weaker, non-deterministic check that would also pass if
    /// the worker were simply slow).
    ///
    /// What this does NOT prove: it does not exercise the full
    /// systemd-restart loop (out of process, untestable at this layer), and
    /// it does not cover a `Job::perform()` future that itself never
    /// resolves -- a distinct, pre-existing failure class this issue does
    /// not claim to fix.
    #[tokio::test]
    async fn supervised_worker_fail_stops_past_the_vendored_circuit_breakers_hardcoded_threshold() {
        use apalis::layers::WorkerBuilderExt;
        use apalis::layers::retry::RetryPolicy;
        use apalis::prelude::WorkerBuilder;
        use apalis_core::worker::ext::event_listener::EventListenerExt;
        use apalis_sqlite::TaskBuilderExt;

        const FAST_BACKOFF: ExponentialBackoff =
            ExponentialBackoff::new(Duration::from_millis(1), Duration::from_millis(5));
        const RETRIES_PAST_HARDCODED_CIRCUIT_THRESHOLD: usize = 12;

        let apalis_pool = setup_test_apalis_pool().await;
        let queue: JobQueue<TestJob> = JobQueue::new(&apalis_pool);

        // A plain `queue.push(...)` defaults to apalis's own `SqlContext`
        // max_attempts of 5 -- coincidentally the same number as the
        // vendored circuit breaker's hardcoded open threshold, and well
        // below the 13 attempts this test needs to exercise. Push with an
        // explicit higher `max_attempts` so the storage layer's own cap
        // cannot truncate the retry budget before `RetryPolicy` does.
        let scheduled = TaskBuilder::<TestJob, apalis_sqlite::SqliteContext, _>::new(TestJob {
            should_fail: true,
        })
        .max_attempts(
            u32::try_from(RETRIES_PAST_HARDCODED_CIRCUIT_THRESHOLD).expect("fits u32") + 1,
        )
        .build();
        TaskSink::push_task(&mut queue.clone().into_storage(), scheduled)
            .await
            .unwrap();

        let failure_notify = Arc::new(TerminalFailureSignal::default());
        let ctx = Arc::new(TestCtx {
            success_count: AtomicUsize::new(0),
            success_notify: Arc::new(tokio::sync::Notify::new()),
            failing_job_started: Arc::new(tokio::sync::Notify::new()),
        });

        let monitor_handle = tokio::spawn({
            let failure_notify = failure_notify.clone();
            let monitor = Monitor::new()
                .should_restart(|_ctx, _error, _attempt| false)
                .register(move |index| {
                    WorkerBuilder::new(format!("stress-test-worker-{index}"))
                        .backend(queue.clone().into_storage())
                        .data(ctx.clone())
                        .data(FailureInjector::new())
                        .data(JobKind::OrderFill)
                        .concurrency(1)
                        .retry(
                            RetryPolicy::retries(RETRIES_PAST_HARDCODED_CIRCUIT_THRESHOLD)
                                .with_backoff(FAST_BACKOFF),
                        )
                        .on_event(on_terminal_failure(
                            failure_notify.clone(),
                            "stress test terminal failure",
                        ))
                        .build(work::<TestCtx, TestJob>)
                });

            async move {
                let _ = monitor.run().await;
            }
        });

        let info = tokio::time::timeout(Duration::from_secs(10), failure_notify.notified())
            .await
            .expect(
                "terminal failure must fire deterministically past 13 attempts, \
                 not latch silently",
            );

        let attempts =
            sqlx_apalis::query_scalar::<_, i64>("SELECT attempts FROM Jobs WHERE job_type = ?")
                .bind(std::any::type_name::<TestJob>())
                .fetch_one(&apalis_pool)
                .await
                .unwrap();
        assert!(
            attempts > i64::try_from(RETRIES_PAST_HARDCODED_CIRCUIT_THRESHOLD).expect("fits i64"),
            "job should have exhausted all {} attempts before fail-stop fired; attempts={attempts}",
            RETRIES_PAST_HARDCODED_CIRCUIT_THRESHOLD + 1,
        );

        assert_eq!(
            info.context, "stress test terminal failure",
            "the recorded failure info must carry the exact static context",
        );
        assert_eq!(
            info.source.to_string(),
            "test-job(should_fail=true): test job deliberately failed",
            "the recorded failure info must carry the original apalis error, not a flattened \
             string",
        );

        tokio::time::timeout(Duration::from_secs(5), monitor_handle)
            .await
            .expect("Monitor should exit within 5s after terminal job failure")
            .expect("Monitor task should not panic");
    }

    async fn insert_job(
        apalis_pool: &apalis_sqlite::SqlitePool,
        id: &str,
        status: Status,
        attempts: i64,
        max_attempts: i64,
    ) {
        sqlx_apalis::query(
            "INSERT INTO Jobs \
             (job, id, job_type, status, attempts, max_attempts, run_at, priority) \
             VALUES (?, ?, 'test', ?, ?, ?, 0, 0)",
        )
        .bind(vec![0_u8])
        .bind(id)
        .bind(status.to_string())
        .bind(attempts)
        .bind(max_attempts)
        .execute(apalis_pool)
        .await
        .unwrap();
    }

    async fn job_ids(apalis_pool: &apalis_sqlite::SqlitePool) -> Vec<String> {
        sqlx_apalis::query_scalar::<_, String>("SELECT id FROM Jobs ORDER BY id")
            .fetch_all(apalis_pool)
            .await
            .unwrap()
    }

    async fn job_rows_for_assertion(
        apalis_pool: &apalis_sqlite::SqlitePool,
    ) -> Vec<(String, String, i64, i64, Option<String>)> {
        sqlx_apalis::query_as(
            "SELECT id, status, attempts, max_attempts, lock_by \
             FROM Jobs ORDER BY id",
        )
        .fetch_all(apalis_pool)
        .await
        .unwrap()
    }

    async fn wait_for_terminal_test_job(apalis_pool: &apalis_sqlite::SqlitePool) {
        let terminal = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let rows = job_rows_for_assertion(apalis_pool).await;
                if rows.iter().any(|(_, status, attempts, max_attempts, _)| {
                    (status == &Status::Killed.to_string() || status == &Status::Failed.to_string())
                        && attempts >= max_attempts
                }) {
                    return;
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;

        assert!(
            terminal.is_ok(),
            "failing job should reach terminal state before sibling is enqueued; \
             job rows at timeout: {:?}",
            job_rows_for_assertion(apalis_pool).await
        );
    }

    #[tokio::test]
    async fn cleanup_finished_jobs_deletes_terminal_rows() {
        let apalis_pool = setup_test_apalis_pool().await;

        insert_job(&apalis_pool, "done", Status::Done, 1, 25).await;
        insert_job(&apalis_pool, "killed", Status::Killed, 1, 25).await;
        insert_job(&apalis_pool, "failed-terminal", Status::Failed, 25, 25).await;
        insert_job(&apalis_pool, "failed-retryable", Status::Failed, 3, 25).await;
        insert_job(&apalis_pool, "pending", Status::Pending, 0, 25).await;
        insert_job(&apalis_pool, "running", Status::Running, 1, 25).await;

        let deleted = sqlx_apalis::query(
            "DELETE FROM Jobs \
             WHERE status = ? \
             OR status = ? \
             OR (status = ? AND max_attempts <= attempts)",
        )
        .bind(Status::Done.to_string())
        .bind(Status::Killed.to_string())
        .bind(Status::Failed.to_string())
        .execute(&apalis_pool)
        .await
        .unwrap()
        .rows_affected();

        assert_eq!(deleted, 3);
        assert_eq!(
            job_ids(&apalis_pool).await,
            vec![
                "failed-retryable".to_string(),
                "pending".to_string(),
                "running".to_string()
            ]
        );
    }

    /// A terminally-failing best-effort job does NOT latch the worker idle:
    /// subsequent jobs must still run. This is the key behavioral contract of
    /// the best-effort worker design.
    ///
    /// Regression test: installing Apalis' circuit-breaker layer on a
    /// best-effort worker can permanently block sibling jobs after retries are
    /// exhausted because an open circuit returns `Poll::Pending` without
    /// scheduling a wakeup.
    ///
    /// Goes through `build_best_effort_worker!`, so the test validates the real
    /// production path used by `register_resume_tokenization_worker` in
    /// `conductor/builder.rs`.
    #[tokio::test]
    async fn best_effort_worker_does_not_latch_on_single_terminal_failure() {
        let apalis_pool = setup_test_apalis_pool().await;

        let mut queue: JobQueue<TestJob> = JobQueue::new(&apalis_pool);
        queue.push(TestJob { should_fail: true }).await.unwrap();
        let mut push_queue = queue.clone();

        // success_notify is wired into TestCtx; TestJob::perform calls
        // notify_waiters() after each successful run.
        let success_notify = Arc::new(tokio::sync::Notify::new());
        let ctx = Arc::new(TestCtx {
            success_count: AtomicUsize::new(0),
            success_notify: success_notify.clone(),
            failing_job_started: Arc::new(tokio::sync::Notify::new()),
        });
        let ctx_for_assert = ctx.clone();

        let monitor_handle = tokio::spawn({
            let monitor = Monitor::new()
                .should_restart(|_ctx, _error, _attempt| false)
                .register(move |index| {
                    build_best_effort_worker!(
                        ::<TestCtx, TestJob>,
                        index,
                        queue.clone(),
                        ctx.clone(),
                        FailureInjector::new(),
                    )
                });
            async move { monitor.run().await }
        });

        wait_for_terminal_test_job(&apalis_pool).await;
        push_queue
            .push(TestJob { should_fail: false })
            .await
            .unwrap();

        // Wait deterministically for the successful job to complete.
        // TestJob::perform fires success_notify after incrementing success_count,
        // so this unblocks as soon as job 2 succeeds -- no fixed sleep needed.
        if tokio::time::timeout(Duration::from_secs(10), success_notify.notified())
            .await
            .is_err()
        {
            panic!(
                "second job must complete after the first job reaches terminal state; \
                 job rows at timeout: {:?}",
                job_rows_for_assertion(&apalis_pool).await
            );
        }

        monitor_handle.abort();

        assert_eq!(
            ctx_for_assert.success_count.load(Ordering::SeqCst),
            1,
            "second job must complete even after the first job terminally fails;              a permissive circuit breaker must not latch idle on a single failure"
        );
    }

    async fn insert_locked_job(
        apalis_pool: &apalis_sqlite::SqlitePool,
        id: &str,
        job_type: &str,
        status: &str,
        lock_by: Option<&str>,
    ) {
        sqlx_apalis::query(
            "INSERT INTO Jobs \
             (job, id, job_type, status, attempts, max_attempts, run_at, priority, lock_by, lock_at) \
             VALUES (?, ?, ?, ?, 0, 25, 0, 0, ?, ?)",
        )
        .bind(vec![0_u8])
        .bind(id)
        .bind(job_type)
        .bind(status)
        .bind(lock_by)
        .bind(lock_by.map(|_| 0_i64))
        .execute(apalis_pool)
        .await
        .unwrap();
    }

    async fn status_of(apalis_pool: &apalis_sqlite::SqlitePool, id: &str) -> String {
        sqlx_apalis::query_scalar::<_, String>("SELECT status FROM Jobs WHERE id = ?")
            .bind(id)
            .fetch_one(apalis_pool)
            .await
            .unwrap()
    }

    async fn lock_by_of(apalis_pool: &apalis_sqlite::SqlitePool, id: &str) -> Option<String> {
        sqlx_apalis::query_scalar::<_, Option<String>>("SELECT lock_by FROM Jobs WHERE id = ?")
            .bind(id)
            .fetch_one(apalis_pool)
            .await
            .unwrap()
    }

    async fn insert_worker(apalis_pool: &apalis_sqlite::SqlitePool, id: &str) {
        sqlx_apalis::query(
            "INSERT INTO Workers (id, worker_type, storage_name) VALUES (?, 'test', 'test')",
        )
        .bind(id)
        .execute(apalis_pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn requeue_orphaned_resets_only_in_flight_rows_of_this_queue() {
        let apalis_pool = setup_test_apalis_pool().await;
        let job_type = std::any::type_name::<TestJob>();

        // A previous process died holding the lock on these in-flight rows.
        insert_worker(&apalis_pool, "dead-worker").await;
        insert_locked_job(
            &apalis_pool,
            "running",
            job_type,
            "Running",
            Some("dead-worker"),
        )
        .await;
        insert_locked_job(
            &apalis_pool,
            "queued",
            job_type,
            "Queued",
            Some("dead-worker"),
        )
        .await;
        insert_locked_job(&apalis_pool, "pending", job_type, "Pending", None).await;
        insert_locked_job(
            &apalis_pool,
            "failed",
            job_type,
            "Failed",
            Some("dead-worker"),
        )
        .await;
        insert_locked_job(&apalis_pool, "done", job_type, "Done", Some("dead-worker")).await;
        // A Running row of a different queue's job type must be left alone.
        insert_locked_job(
            &apalis_pool,
            "other",
            "other::Job",
            "Running",
            Some("dead-worker"),
        )
        .await;

        let queue = JobQueue::<TestJob>::new(&apalis_pool);
        let reset = queue.requeue_orphaned().await.unwrap();

        assert_eq!(reset, 2, "only this queue's Running + Queued rows reset");
        assert_eq!(status_of(&apalis_pool, "running").await, "Pending");
        assert_eq!(status_of(&apalis_pool, "queued").await, "Pending");
        assert_eq!(
            lock_by_of(&apalis_pool, "running").await,
            None,
            "lock cleared so the apalis monitor re-picks the row",
        );
        assert_eq!(lock_by_of(&apalis_pool, "queued").await, None);

        assert_eq!(status_of(&apalis_pool, "pending").await, "Pending");
        assert_eq!(
            status_of(&apalis_pool, "failed").await,
            "Failed",
            "a latched failure awaiting operator reconciliation is not re-driven",
        );
        assert_eq!(status_of(&apalis_pool, "done").await, "Done");
        assert_eq!(
            status_of(&apalis_pool, "other").await,
            "Running",
            "another queue's in-flight row is untouched",
        );
    }

    /// Regression test for the concurrent-terminal-failure race: two
    /// different supervised workers sharing one `TerminalFailureSignal` both
    /// fail terminally at the same time. The recorded info must be a
    /// consistent `(worker, context, source)` triple from exactly ONE of the
    /// two failures -- never a torn/mixed combination -- and the fail-stop
    /// signal must still fire regardless of which one wins.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_terminal_failures_record_a_consistent_pair_from_one_of_them() {
        let signal = Arc::new(TerminalFailureSignal::default());
        let barrier = Arc::new(tokio::sync::Barrier::new(2));

        let source_a: Arc<BoxDynError> = Arc::new("failure-a".into());
        let source_b: Arc<BoxDynError> = Arc::new("failure-b".into());

        let task_a = tokio::spawn({
            let signal = signal.clone();
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                signal.record_and_notify(TerminalFailureInfo {
                    worker: "worker-a".to_string(),
                    context: "context-a",
                    source: source_a,
                });
            }
        });
        let task_b = tokio::spawn({
            let signal = signal.clone();
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                signal.record_and_notify(TerminalFailureInfo {
                    worker: "worker-b".to_string(),
                    context: "context-b",
                    source: source_b,
                });
            }
        });

        let info = tokio::time::timeout(Duration::from_secs(5), signal.notified())
            .await
            .expect("fail-stop must fire even when two workers fail terminally at once");
        task_a.await.unwrap();
        task_b.await.unwrap();

        // Either winner is an acceptable, genuinely equivalent outcome here
        // (first-writer-wins is explicitly non-deterministic by design) --
        // what must never happen is a mix of the two, e.g. worker-a paired
        // with context-b.
        let consistent_with_a = info.worker == "worker-a"
            && info.context == "context-a"
            && info.source.to_string() == "failure-a";
        let consistent_with_b = info.worker == "worker-b"
            && info.context == "context-b"
            && info.source.to_string() == "failure-b";
        assert!(
            consistent_with_a || consistent_with_b,
            "recorded info must be a consistent (worker, context, source) triple from exactly \
             one failure, got worker={} context={} source={}",
            info.worker,
            info.context,
            info.source,
        );
    }

    /// Regression guard for RAI-1495: no test can drive a macro-path worker
    /// past the vendored circuit breaker's hardcoded `failure_count >= 5`
    /// threshold without an explicit `max_attempts` override the production
    /// macro never applies (see
    /// `supervised_worker_fail_stops_past_the_vendored_circuit_breakers_hardcoded_threshold`'s
    /// doc comment), so no runtime test would go red if a future change
    /// reinstalled the breaker inside `build_worker_inner!`. That behavioral
    /// test remains the real guarantee that the breaker cannot latch a
    /// worker idle; this test is a cheap, deliberately non-behavioral
    /// structural backstop layered on top of it, catching a reinstalled
    /// breaker even before a behavioral test would need to exercise it.
    ///
    /// Scoped to only the `build_worker_inner!` definition (extracted from
    /// this file's own source between its `macro_rules!` header and its
    /// `pub(crate) use` re-export) -- the actual worker-builder path this
    /// guard claims to cover -- rather than the whole `src/` tree, so an
    /// unrelated file mentioning either identifier (e.g. in a string
    /// literal or doc comment about this very regression) cannot fail the
    /// build, and so this guard cannot miss a reinstalled breaker that
    /// lands anywhere inside the macro body.
    ///
    /// The forbidden identifiers below are built via `.concat()` rather than
    /// written as string literals so this test's own source does not contain
    /// a literal match for what it searches for.
    ///
    /// Matches the macro's raw source text, comments included. The macro body
    /// is a 40-line declarative expansion that carries no comments of its own,
    /// so there is nothing for a comment-aware pass to disambiguate. If a
    /// future comment inside the body needs to name either identifier while
    /// explaining why not to reinstall it, refer to it indirectly (or split it
    /// the way this test's own patterns are split) rather than reintroducing a
    /// comment stripper here.
    #[test]
    fn build_worker_inner_never_reinstalls_the_vendored_circuit_breaker() {
        let forbidden_identifiers = [
            ["break_circuit", "_with("].concat(),
            ["CircuitBreaker", "Config"].concat(),
        ];

        let file_source = include_str!("job.rs");
        let macro_start = file_source
            .find("macro_rules! build_worker_inner {")
            .expect("build_worker_inner! macro definition must exist in this file");
        let macro_and_after = &file_source[macro_start..];
        let macro_end = macro_and_after
            .find("pub(crate) use build_worker_inner;")
            .expect("build_worker_inner! definition must be followed by its pub(crate) use");
        let macro_text = &macro_and_after[..macro_end];

        let offending_identifiers: Vec<&str> = forbidden_identifiers
            .iter()
            .filter(|identifier| macro_text.contains(identifier.as_str()))
            .map(String::as_str)
            .collect();

        assert!(
            offending_identifiers.is_empty(),
            "production code must never reinstall apalis's circuit-breaker layer inside \
             `build_worker_inner!` (RAI-1495 regression): found forbidden identifiers \
             {offending_identifiers:?}",
        );
    }
}
