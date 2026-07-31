//! Bounded in-call retry for synchronous CLI Alpaca calls (RAI-1494).
//!
//! A CLI command is not a durable job: there is no apalis queue row to
//! reschedule and no sibling work waiting behind a shared
//! `concurrency(1)` worker. Retrying a classified broker rate-limit (429)
//! in place with a small bounded budget cannot reproduce the parent
//! RAI-1492 incident's "one job monopolizes the single worker" shape --
//! the "worker" here is the one-shot CLI process the operator is already
//! waiting on. This mirrors the job-path reschedule mechanism's
//! classification (`crate::conductor::job::find_backpressure`/
//! `decide_backpressure`) but retries synchronously instead of pushing a
//! delayed successor job.

use std::future::Future;
use tracing::warn;

use crate::conductor::job::{BackpressureStreak, decide_backpressure, find_backpressure};

/// Default bounded retry budget for CLI Alpaca calls: a handful of attempts
/// is enough to ride out a brief rate-limit window without turning a CLI
/// invocation into a long-running process an operator did not expect.
pub(crate) const BACKPRESSURE_RETRY_MAX_ATTEMPTS: u32 = 3;

/// Retries `call` up to `max_attempts` times when its error is classified as
/// broker rate-limiting (429), sleeping for the same delay the job-path
/// reschedule mechanism would honour (`Retry-After` when present, else an
/// escalating fallback). Any non-backpressure error, or a backpressure error
/// once `max_attempts` is exhausted, propagates unchanged to the caller's
/// existing `anyhow` error path.
///
/// `call` is a closure returning a fresh future per attempt (an `FnMut`, not
/// a bare `Future`) since a future can only be polled to completion once.
pub(crate) async fn retry_on_backpressure<Value, ErrorType, Fut>(
    mut call: impl FnMut() -> Fut,
    max_attempts: u32,
) -> Result<Value, ErrorType>
where
    ErrorType: std::error::Error + 'static,
    Fut: Future<Output = Result<Value, ErrorType>>,
{
    let mut attempt: u32 = 0;

    loop {
        match call().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                let Some(backpressure) = find_backpressure(&error) else {
                    return Err(error);
                };

                attempt = attempt.saturating_add(1);
                if attempt >= max_attempts {
                    return Err(error);
                }

                // `decide_backpressure`'s `streak` parameter is a consecutive
                // reschedule count; `attempt - 1` (0-indexed) plays the same
                // role here, escalating the fallback delay identically to
                // the job path when `Retry-After` is absent.
                let decision = decide_backpressure(&backpressure, BackpressureStreak(attempt - 1));
                warn!(
                    target: "broker",
                    attempts_remaining = max_attempts - attempt,
                    delay = ?decision.delay,
                    "Broker rate-limited the CLI request; retrying after delay"
                );
                tokio::time::sleep(decision.delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use super::*;

    #[derive(Debug, thiserror::Error)]
    #[error("test error")]
    struct TestError {
        #[source]
        source: Option<st0x_execution::AlpacaBrokerApiError>,
    }

    fn backpressure_429(retry_after: Duration) -> TestError {
        TestError {
            source: Some(st0x_execution::AlpacaBrokerApiError::ApiError {
                status: reqwest::StatusCode::TOO_MANY_REQUESTS,
                alpaca_code: None,
                message: "rate limited".to_string(),
                retry_after: Some(retry_after),
            }),
        }
    }

    fn non_backpressure_error() -> TestError {
        TestError { source: None }
    }

    #[tokio::test]
    async fn retry_on_backpressure_retries_within_budget_then_succeeds() {
        let call_count = AtomicU32::new(0);

        let result = retry_on_backpressure(
            || {
                let attempt = call_count.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt < 2 {
                        Err(backpressure_429(Duration::from_millis(1)))
                    } else {
                        Ok::<_, TestError>("success")
                    }
                }
            },
            5,
        )
        .await
        .unwrap();

        assert_eq!(result, "success");
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn retry_on_backpressure_logs_the_delay_and_remaining_budget() {
        let call_count = AtomicU32::new(0);

        retry_on_backpressure(
            || {
                let attempt = call_count.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt == 0 {
                        Err(backpressure_429(Duration::from_millis(1)))
                    } else {
                        Ok::<_, TestError>(())
                    }
                }
            },
            3,
        )
        .await
        .unwrap();

        assert!(logs_contain("Broker rate-limited the CLI request"));
        assert!(logs_contain("attempts_remaining=2"));
        assert!(logs_contain("delay=1s"));
    }

    #[tokio::test]
    async fn retry_on_backpressure_gives_up_after_max_attempts() {
        let call_count = AtomicU32::new(0);

        let error = retry_on_backpressure(
            || {
                call_count.fetch_add(1, Ordering::SeqCst);
                async move { Err::<(), _>(backpressure_429(Duration::from_millis(1))) }
            },
            3,
        )
        .await
        .unwrap_err();

        assert!(error.source.is_some());
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            3,
            "must stop after exactly max_attempts calls"
        );
    }

    #[tokio::test]
    async fn retry_on_backpressure_does_not_retry_a_non_backpressure_error() {
        let call_count = AtomicU32::new(0);

        let error = retry_on_backpressure(
            || {
                call_count.fetch_add(1, Ordering::SeqCst);
                async move { Err::<(), _>(non_backpressure_error()) }
            },
            5,
        )
        .await
        .unwrap_err();

        assert!(error.source.is_none());
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "a non-backpressure error must fail on the first attempt, not retry"
        );
    }
}
