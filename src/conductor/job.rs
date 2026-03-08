//! Trait abstraction for apalis-backed persistent jobs.
//!
//! [`Job`] wraps apalis's function-based handler API with a
//! trait-based one. Each job is a serializable struct pushed into
//! `SqliteStorage`; the generic [`handle_job`] handler deserializes
//! it and calls [`Job::execute`] with the shared context.

use std::fmt;
use std::sync::Arc;

use apalis::prelude::Data;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::{debug, error};

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
    /// Error type returned by [`execute`](Job::execute).
    type Error: std::error::Error + Send + Sync + 'static;

    /// Human-readable label for structured logging.
    fn label(&self) -> Label;

    /// Process this job using the provided context.
    async fn execute(&self, ctx: &Ctx) -> Result<(), Self::Error>;
}

/// Generic apalis handler that bridges [`Job`] implementations
/// with apalis's function-based worker API.
///
/// Register with apalis via:
/// ```text
/// WorkerBuilder::new(name)
///     .backend(storage)
///     .data(ctx)
///     .build(handle_job::<MyJob, MyCtx>)
/// ```
pub(crate) async fn handle_job<J, Ctx>(job: J, ctx: Data<Arc<Ctx>>)
where
    J: Job<Ctx>,
    Ctx: Send + Sync + 'static,
{
    let label = job.label();

    debug!(%label, "Processing job");

    if let Err(error) = job.execute(&ctx).await {
        error!(%label, %error, "Job failed");
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
