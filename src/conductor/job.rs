//! Trait abstraction for apalis-backed persistent jobs.
//!
//! [`Job`] wraps apalis's function-based handler API with a
//! trait-based one. Each job is a serializable struct pushed into
//! `SqliteStorage`; the generic [`work`] handler deserializes
//! it and calls [`Job::perform`] with the shared context.

use apalis::layers::retry::backoff::Backoff;
use apalis::prelude::{Attempt, Data, Status, TaskSink};
use apalis_cron::Schedule;
use apalis_sqlite::SqliteStorage;
use chrono::{DateTime, TimeZone, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::SqlitePool;
use std::fmt;
use std::sync::Arc;
#[cfg(any(test, feature = "test-support"))]
use std::sync::Mutex;
use std::time::Duration;
use tracing::{debug, error, warn};

use st0x_execution::Executor;

use crate::offchain::order_poller::{OrderPollingError, OrderStatusPoller};

/// Recovery timeout for the fail-stop circuit breaker. Effectively
/// infinite for any plausible bot uptime; chosen to be finite so
/// `last_failure + recovery_timeout` cannot overflow if apalis
/// changes its check from `elapsed() >=` to addition.
pub(crate) const FAIL_STOP_RECOVERY_TIMEOUT: Duration = Duration::from_secs(60 * 60 * 24 * 365);

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

/// `apalis_cron::Schedule` impl that fires at a fixed interval after each tick.
/// Construction is typed and infallible -- no cron-string parsing.
#[derive(Clone, Debug)]
pub(crate) struct FixedInterval {
    interval: Duration,
}

impl FixedInterval {
    pub(crate) const fn new(interval: Duration) -> Self {
        Self { interval }
    }
}

impl<Tz: TimeZone> Schedule<Tz> for FixedInterval {
    fn next_tick(&mut self, timezone: &Tz) -> Option<DateTime<Tz>> {
        let interval = chrono::Duration::from_std(self.interval).ok()?;
        Some(Utc::now().with_timezone(timezone) + interval)
    }
}

type Storage<Task> = SqliteStorage<
    Task,
    apalis_codec::json::JsonCodec<apalis_sqlite::CompactType>,
    apalis_sqlite::fetcher::SqliteFetcher,
>;

/// Persistent job queue backed by apalis `SqliteStorage`.
pub(crate) struct JobQueue<Task>(Storage<Task>);

impl<Task> Clone for JobQueue<Task> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<Task: Serialize + DeserializeOwned + Send + Sync + Unpin + 'static> JobQueue<Task> {
    pub(crate) fn new(pool: &SqlitePool) -> Self {
        Self(SqliteStorage::new(pool))
    }

    pub(crate) async fn push(
        &mut self,
        task: Task,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        TaskSink::push(&mut self.0, task)
            .await
            .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
    }

    pub(crate) fn into_storage(self) -> Storage<Task> {
        self.0
    }
}

/// A persistent, retryable unit of work backed by apalis storage.
///
/// Implementations are serializable structs that carry the data
/// needed to process a single job. The `Ctx` type parameter
/// bundles all runtime dependencies (executor, CQRS frameworks,
/// config, etc.) into one struct injected via apalis `Data`.
pub(crate) trait Job<Ctx>: Serialize + DeserializeOwned + Send + 'static
where
    Ctx: Send + Sync + 'static,
{
    /// Error type returned by [`perform`](Job::perform).
    type Error: std::error::Error + Send + Sync + 'static;

    /// Human-readable label for structured logging.
    fn label(&self) -> Label;

    /// Process this job using the provided context.
    async fn perform(&self, ctx: &Ctx) -> Result<(), Self::Error>;
}

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
    ) -> Result<(), JobError>
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
) -> Result<(), JobError>
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
) -> Result<(), JobError>
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

/// apalis-cron handler for the order status poller. One invocation per tick.
/// Per-order errors are absorbed inside [`OrderStatusPoller::poll_pending_orders`];
/// only cycle-level failures (storage, projection) propagate here. Returning
/// the error feeds apalis' retry/circuit-breaker stack so the next tick is not
/// scheduled on top of a broken cycle.
pub(crate) async fn poll_orders<E>(
    _tick: apalis_cron::Tick<Utc>,
    poller: Data<Arc<OrderStatusPoller<E>>>,
) -> Result<(), OrderPollingError>
where
    E: Executor + Clone + Send + Sync + 'static,
{
    poller.poll_pending_orders().await
}

pub(crate) async fn cleanup_finished_jobs(pool: &SqlitePool) -> Result<u64, sqlx::Error> {
    let deleted = sqlx::query(
        "DELETE FROM Jobs \
         WHERE status = ? \
         OR status = ? \
         OR (status = ? AND max_attempts <= attempts)",
    )
    .bind(Status::Done.to_string())
    .bind(Status::Killed.to_string())
    .bind(Status::Failed.to_string())
    .execute(pool)
    .await?
    .rows_affected();

    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use apalis::layers::WorkerBuilderExt;
    use apalis::layers::retry::RetryPolicy;
    use apalis::prelude::{Monitor, WorkerBuilder};
    use apalis_core::worker::event::Event;
    use apalis_core::worker::ext::circuit_breaker::{CircuitBreaker, config::CircuitBreakerConfig};
    use apalis_core::worker::ext::event_listener::EventListenerExt;
    use sqlx::SqlitePool;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::conductor::setup_apalis_tables;
    use crate::test_utils::setup_test_db;

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
    }

    impl Job<TestCtx> for TestJob {
        type Error = TestJobError;

        fn label(&self) -> Label {
            Label::new(format!("test-job(should_fail={})", self.should_fail))
        }

        async fn perform(&self, ctx: &TestCtx) -> Result<(), Self::Error> {
            if self.should_fail {
                return Err(TestJobError);
            }

            ctx.success_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("test job deliberately failed")]
    struct TestJobError;

    /// A job that fails after all retries must halt further processing --
    /// the worker must not pick up the next job with stale state.
    #[tokio::test]
    async fn job_failure_after_retries_halts_processing() {
        let pool = setup_test_db().await;
        setup_apalis_tables(&pool).await.unwrap();

        let mut queue: JobQueue<TestJob> = JobQueue::new(&pool);
        queue.push(TestJob { should_fail: true }).await.unwrap();
        queue.push(TestJob { should_fail: false }).await.unwrap();

        let ctx = Arc::new(TestCtx {
            success_count: AtomicUsize::new(0),
        });
        let ctx_for_assert = ctx.clone();

        let monitor_handle = tokio::spawn({
            let monitor = Monitor::new()
                .should_restart(|_ctx, _error, _attempt| false)
                .register(move |index| {
                    let fail_stop = CircuitBreakerConfig::default()
                        .with_failure_threshold(1)
                        .with_recovery_timeout(FAIL_STOP_RECOVERY_TIMEOUT);

                    WorkerBuilder::new(format!("test-worker-{index}"))
                        .backend(queue.clone().into_storage())
                        .data(ctx.clone())
                        .data(FailureInjector::new())
                        .data(JobKind::OrderFill)
                        .concurrency(1)
                        .retry(RetryPolicy::retries(3))
                        .break_circuit_with(fail_stop)
                        .on_event(|ctx, event| {
                            if let Event::Error(_) = event {
                                let _ = ctx.stop();
                            }
                        })
                        .build(work::<TestCtx, TestJob>)
                });

            async move { monitor.run().await }
        });

        // RetryPolicy retries instantly, so the monitor should exit
        // quickly once the failing job exhausts retries.
        let join_result = tokio::time::timeout(Duration::from_secs(5), monitor_handle)
            .await
            .expect("Monitor should exit within 5s after terminal job failure");
        let _ = join_result.expect("Monitor task should not panic");

        assert_eq!(
            ctx_for_assert.success_count.load(Ordering::SeqCst),
            0,
            "The second job should NOT have been processed after a prior \
             job failed all retries."
        );
    }

    async fn insert_job(
        pool: &SqlitePool,
        id: &str,
        status: Status,
        attempts: i64,
        max_attempts: i64,
    ) {
        sqlx::query(
            "INSERT INTO Jobs \
             (job, id, job_type, status, attempts, max_attempts, run_at, priority) \
             VALUES (?, ?, 'test', ?, ?, ?, 0, 0)",
        )
        .bind(vec![0_u8])
        .bind(id)
        .bind(status.to_string())
        .bind(attempts)
        .bind(max_attempts)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn job_ids(pool: &SqlitePool) -> Vec<String> {
        sqlx::query_scalar::<_, String>("SELECT id FROM Jobs ORDER BY id")
            .fetch_all(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn cleanup_finished_jobs_deletes_terminal_rows() {
        let pool = setup_test_db().await;
        setup_apalis_tables(&pool).await.unwrap();

        insert_job(&pool, "done", Status::Done, 1, 25).await;
        insert_job(&pool, "killed", Status::Killed, 1, 25).await;
        insert_job(&pool, "failed-terminal", Status::Failed, 25, 25).await;
        insert_job(&pool, "failed-retryable", Status::Failed, 3, 25).await;
        insert_job(&pool, "pending", Status::Pending, 0, 25).await;
        insert_job(&pool, "running", Status::Running, 1, 25).await;

        let deleted = cleanup_finished_jobs(&pool).await.unwrap();

        assert_eq!(deleted, 3);
        assert_eq!(
            job_ids(&pool).await,
            vec![
                "failed-retryable".to_string(),
                "pending".to_string(),
                "running".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn cleanup_finished_jobs_keeps_non_terminal_rows() {
        let pool = setup_test_db().await;
        setup_apalis_tables(&pool).await.unwrap();

        insert_job(&pool, "failed-retryable", Status::Failed, 3, 25).await;
        insert_job(&pool, "pending", Status::Pending, 0, 25).await;
        insert_job(&pool, "running", Status::Running, 1, 25).await;

        let deleted = cleanup_finished_jobs(&pool).await.unwrap();

        assert_eq!(deleted, 0);
        assert_eq!(
            job_ids(&pool).await,
            vec![
                "failed-retryable".to_string(),
                "pending".to_string(),
                "running".to_string()
            ]
        );
    }
}
