//! Trait abstraction for apalis-backed persistent jobs.
//!
//! [`Job`] wraps apalis's function-based handler API with a
//! trait-based one. Each job is a serializable struct pushed into
//! `SqliteStorage`; the generic [`Job::work`] default method logs
//! the job label, runs [`Job::execute`], and logs failures.

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

    /// Run this job with structured logging around [`execute`](Job::execute).
    ///
    /// This is the apalis handler entry point. Register with:
    /// ```text
    /// WorkerBuilder::new(name)
    ///     .backend(storage)
    ///     .data(ctx)
    ///     .build(Job::work)
    /// ```
    async fn work(self, ctx: Data<Arc<Ctx>>) {
        let label = self.label();

        debug!(%label, "Processing job");

        if let Err(error) = self.execute(&ctx).await {
            error!(%label, %error, "Job failed");
        }
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
