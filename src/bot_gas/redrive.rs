//! Single shared mechanism for redriving a job after a bot-gas receipt-cost
//! bookkeeping failure, instead of failing the money-moving operation that
//! triggered it.
//!
//! ADR 0017 SS4: recording a bot-paid gas receipt cost is a best-effort local
//! SQLite write, never a reason to fail the transfer/recovery/mint job that
//! happened to trigger it. Every job whose `perform()` can observe that
//! failure (directly, or wrapped inside a domain error returned by an
//! aggregate's command handler) must route it through
//! [`redrive_on_bot_gas_failure`] rather than hand-rolling its own "is this a
//! bot-gas failure" predicate plus a `warn!` + `push_with_delay` body. Five
//! independent review passes each found another call site that hand-rolled
//! this classification and got it wrong (missed entirely, or folded into a
//! terminal failure) -- centralizing the mechanics here means a new call site
//! inherits correct behaviour by construction instead of by remembering to
//! copy it correctly.
//!
//! What this module does NOT centralize: classification itself. Each job's
//! own error type implements [`BotGasFailureClassifier`] with an exhaustive
//! `match` over its own variants (see `docs::exhaustive match over matches!`
//! in AGENTS.md), so a new variant on that error type fails to compile until
//! someone explicitly decides whether it represents a bot-gas bookkeeping
//! failure. That is deliberate: only the job's own error type knows its
//! shape, but every job shares the same redrive mechanics.

use metrics::counter;
use std::time::Duration;
use tracing::{error, warn};

use crate::conductor::job::{Job, JobQueue};

/// Implemented by a job's error type so [`redrive_on_bot_gas_failure`] can
/// classify any error value it produces without that job hand-rolling its
/// own predicate. Implementations MUST use an exhaustive `match` over the
/// error type's own top-level variants (no `_` arm) so a new variant fails
/// to compile until it is explicitly classified true or false here.
pub(crate) trait BotGasFailureClassifier {
    /// True when `self` represents a failed bot-gas receipt-cost bookkeeping
    /// write -- never a genuine domain/RPC failure of the operation the job
    /// is actually performing.
    fn is_bot_gas_enqueue_failure(&self) -> bool;
}

/// Classifies `error` via [`BotGasFailureClassifier`]. If it is a bot-gas
/// bookkeeping failure, logs and reschedules `job` on `queue` after `delay`,
/// returning `Ok(())` so the caller's `perform()` neither fails terminally
/// nor consumes the apalis retry budget. Otherwise returns `Err(error)`
/// unchanged for the caller to propagate as normal.
///
/// If the redrive push itself fails, the ORIGINAL error propagates (not the
/// push error): the caller's normal apalis retry budget becomes the
/// fallback, so the failure is still bounded rather than silently dropped.
pub(crate) async fn redrive_on_bot_gas_failure<Ctx, TaskJob>(
    job: &TaskJob,
    queue: &JobQueue<TaskJob>,
    delay: Duration,
    error: TaskJob::Error,
) -> Result<(), TaskJob::Error>
where
    Ctx: Send + Sync + 'static,
    TaskJob: Job<Ctx> + Clone + Sync + Unpin,
    TaskJob::Error: BotGasFailureClassifier + std::fmt::Display,
{
    if !error.is_bot_gas_enqueue_failure() {
        return Err(error);
    }

    warn!(
        target: "rebalance",
        label = %job.label(),
        %error,
        ?delay,
        "Bot-gas receipt cost enqueue failed; rescheduling without consuming \
         apalis retry budget (see ADR 0017 SS4 / BotGasReceiptCostEnqueuer doc)",
    );
    counter!("bot_gas_redrive_total", "job" => TaskJob::WORKER_NAME).increment(1);

    let mut queue = queue.clone();
    if let Err(push_error) = queue.push_with_delay(job.clone(), delay).await {
        error!(
            target: "rebalance",
            label = %job.label(),
            %push_error,
            "Bot-gas redrive push itself failed; propagating the original bot-gas \
             enqueue error so the job's own apalis retry budget is the fallback",
        );
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::conductor::job::Label;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestJob {
        marker: u32,
    }

    #[derive(Debug, thiserror::Error)]
    enum TestJobError {
        #[error("bot-gas bookkeeping failure")]
        BotGas,
        #[error("genuine domain failure")]
        Domain,
    }

    impl BotGasFailureClassifier for TestJobError {
        fn is_bot_gas_enqueue_failure(&self) -> bool {
            match self {
                Self::BotGas => true,
                Self::Domain => false,
            }
        }
    }

    struct TestCtx;

    impl Job<TestCtx> for TestJob {
        type Output = ();
        type Error = TestJobError;

        const WORKER_NAME: &'static str = "redrive-test-worker";

        const PERFORM_TIMEOUT: std::time::Duration = crate::conductor::job::DEFAULT_PERFORM_TIMEOUT;

        #[cfg(any(test, feature = "test-support"))]
        const JOB_KIND: crate::conductor::job::JobKind = crate::conductor::job::JobKind::Backfill;

        fn label(&self) -> Label {
            Label::new(format!("TestJob:{}", self.marker))
        }

        async fn perform(&self, _ctx: &TestCtx) -> Result<Self::Output, Self::Error> {
            unreachable!("perform is never invoked by these unit tests")
        }
    }

    /// A classified bot-gas failure must redrive: return `Ok(())` and push a
    /// delayed replacement job, never propagate as `Err`.
    #[tokio::test]
    async fn classified_bot_gas_failure_redrives_instead_of_propagating() {
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        let queue = JobQueue::<TestJob>::new(&apalis_pool);
        let job = TestJob { marker: 1 };

        let result = redrive_on_bot_gas_failure::<TestCtx, _>(
            &job,
            &queue,
            Duration::from_secs(5),
            TestJobError::BotGas,
        )
        .await;

        assert!(
            result.is_ok(),
            "a classified bot-gas failure must redrive, not propagate: {result:?}"
        );

        let pending: i64 = sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(std::any::type_name::<TestJob>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        assert_eq!(pending, 1, "a delayed replacement job must be enqueued");
    }

    /// A non-bot-gas error must propagate unchanged -- no redrive, no push.
    #[tokio::test]
    async fn unclassified_error_propagates_without_redriving() {
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        let queue = JobQueue::<TestJob>::new(&apalis_pool);
        let job = TestJob { marker: 2 };

        let error = redrive_on_bot_gas_failure::<TestCtx, _>(
            &job,
            &queue,
            Duration::from_secs(5),
            TestJobError::Domain,
        )
        .await
        .expect_err("a non-bot-gas error must propagate as Err");

        assert!(matches!(error, TestJobError::Domain));

        let pending: i64 = sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(std::any::type_name::<TestJob>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        assert_eq!(
            pending, 0,
            "a genuine domain error must not push a redrive job"
        );
    }

    /// If the redrive push itself fails (e.g. a closed pool), the ORIGINAL
    /// bot-gas error must still propagate so the job's normal apalis retry
    /// budget is the fallback, rather than the push error masking it.
    #[tokio::test]
    async fn redrive_push_failure_propagates_original_error() {
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        apalis_pool.close().await;
        let queue = JobQueue::<TestJob>::new(&apalis_pool);
        let job = TestJob { marker: 3 };

        let error = redrive_on_bot_gas_failure::<TestCtx, _>(
            &job,
            &queue,
            Duration::from_secs(5),
            TestJobError::BotGas,
        )
        .await
        .expect_err("a failed redrive push must propagate the original error");

        assert!(matches!(error, TestJobError::BotGas));
    }
}
