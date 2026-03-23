//! Trait abstraction for apalis-backed persistent jobs.
//!
//! [`Job`] wraps apalis's function-based handler API with a
//! trait-based one. Each job is a serializable struct pushed into
//! `SqliteStorage`; the generic [`work`] handler deserializes
//! it and calls [`Job::perform`] with the shared context.

use apalis::prelude::Data;
use backon::{ExponentialBuilder, Retryable};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt;
use std::sync::Arc;
use tracing::{debug, error, warn};

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

/// Generic apalis handler that bridges [`Job`] implementations
/// with apalis's function-based worker API.
///
/// Register with apalis via:
/// ```text
/// WorkerBuilder::new(name)
///     .backend(storage)
///     .data(ctx)
///     .build(work::<MyCtx, MyJob>)
/// ```
pub(crate) async fn work<Ctx, J>(job: J, ctx: Data<Arc<Ctx>>)
where
    Ctx: Send + Sync + 'static,
    J: Job<Ctx> + Sync,
{
    let label = job.label();

    debug!(%label, "Processing job");

    let result = (|| job.perform(&ctx))
        .retry(ExponentialBuilder::default().with_max_times(3))
        .notify(|error, duration| {
            warn!(%label, %error, ?duration, "Retrying job after transient failure");
        })
        .await;

    if let Err(error) = result {
        error!(%label, %error, "Job failed after retries");
    }
}

/// Human-readable identifier for a job, used in structured logging.
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
