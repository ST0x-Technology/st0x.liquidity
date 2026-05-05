//! Trait abstraction for apalis-backed persistent jobs.
//!
//! [`Job`] wraps apalis's function-based handler API with a
//! trait-based one. Each job is a serializable struct pushed into
//! `SqliteStorage`; the generic [`work`] handler deserializes
//! it and calls [`Job::perform`] with the shared context.

use apalis::prelude::{Data, Status, TaskSink};
use apalis_sqlite::SqliteStorage;
use backon::{ExponentialBuilder, Retryable};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::SqlitePool;
use std::fmt;
use std::sync::Arc;
#[cfg(feature = "test-support")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(feature = "test-support"))]
use tracing::debug;
#[cfg(feature = "test-support")]
use tracing::info;
use tracing::{error, warn};

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
pub(crate) struct Label(String);

impl Label {
    pub(crate) fn new(label: impl Into<String>) -> Self {
        Self(label.into())
    }
}

impl fmt::Display for Label {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Identifies which job queue a [`FailureInjector`] targets.
#[cfg(feature = "test-support")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum JobKind {
    OrderFill,
    Hedge,
}

/// Allows e2e tests to force the next job of a specific kind to
/// fail terminally. Each [`JobKind`] has an independent flag so
/// arming one queue cannot be consumed by the other.
#[cfg(feature = "test-support")]
#[derive(Clone, Debug)]
pub struct FailureInjector {
    order_fill: Arc<AtomicBool>,
    hedge: Arc<AtomicBool>,
}

#[cfg(feature = "test-support")]
impl Default for FailureInjector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "test-support")]
impl FailureInjector {
    pub fn new() -> Self {
        Self {
            order_fill: Arc::new(AtomicBool::new(false)),
            hedge: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn arm(&self, kind: JobKind) {
        self.flag(kind).store(true, Ordering::SeqCst);
    }

    fn is_armed(&self, kind: JobKind) -> bool {
        self.flag(kind).swap(false, Ordering::SeqCst)
    }

    fn flag(&self, kind: JobKind) -> &AtomicBool {
        match kind {
            JobKind::OrderFill => &self.order_fill,
            JobKind::Hedge => &self.hedge,
        }
    }
}

/// Generic apalis handler - test-support build.
///
/// Accepts a [`FailureInjector`] and [`JobKind`] via apalis `Data`.
/// When the injector is armed for this kind, the job fails without
/// calling `perform`.
#[cfg(feature = "test-support")]
pub(crate) async fn work<Ctx, J>(
    job: J,
    ctx: Data<Arc<Ctx>>,
    injector: Data<FailureInjector>,
    kind: Data<JobKind>,
) where
    Ctx: Send + Sync + 'static,
    J: Job<Ctx> + Sync,
{
    if injector.is_armed(*kind) {
        error!(label = %job.label(), ?kind, "Injected terminal failure (test)");
        return;
    }

    const MAX_RETRIES: usize = 3;
    let label = job.label();
    info!(%label, max_retries = MAX_RETRIES, "Starting job");

    let result = (|| job.perform(&ctx))
        .retry(ExponentialBuilder::default().with_max_times(MAX_RETRIES))
        .notify(|error, duration| {
            warn!(%label, %error, ?duration, "Retrying job after transient failure");
        })
        .await;

    if let Err(error) = result {
        error!(%label, %error, "Job failed after retries");
    }
}

/// Generic apalis handler - production build.
#[cfg(not(feature = "test-support"))]
pub(crate) async fn work<Ctx, J>(job: J, ctx: Data<Arc<Ctx>>)
where
    Ctx: Send + Sync + 'static,
    J: Job<Ctx> + Sync,
{
    const MAX_RETRIES: usize = 3;
    let label = job.label();
    debug!(%label, max_retries = MAX_RETRIES, "Starting job");

    let result = (|| job.perform(&ctx))
        .retry(ExponentialBuilder::default().with_max_times(MAX_RETRIES))
        .notify(|error, duration| {
            warn!(%label, %error, ?duration, "Retrying job after transient failure");
        })
        .await;

    if let Err(error) = result {
        error!(%label, %error, "Job failed after retries");
    }
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
    use sqlx::SqlitePool;

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
