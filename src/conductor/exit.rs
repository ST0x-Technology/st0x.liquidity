//! Routes conductor task exits (supervisor, apalis monitor, job cleanup) to a
//! single [`ConductorExitError`]. The conductor's main `select!` produces a
//! [`ConductorExit`] describing which task exited and how, and
//! [`ConductorExit::handle`] turns that into either `Ok(())` for clean shutdown
//! or an error explaining what went wrong.

use task_supervisor::SupervisorError;
use tokio::task::JoinError;
use tracing::{error, info};

#[derive(Debug, thiserror::Error)]
pub(crate) enum MonitorTaskError {
    #[error("Apalis monitor exited unexpectedly")]
    UnexpectedExit {
        #[source]
        source: Option<apalis::prelude::MonitorError>,
    },
}

#[derive(Debug, thiserror::Error)]
pub(super) enum ConductorExitError {
    #[error(transparent)]
    Supervisor(#[from] SupervisorError),
    #[error(transparent)]
    Monitor(#[from] MonitorTaskError),
    #[error("{task} exited unexpectedly")]
    UnexpectedTaskExit { task: &'static str },
    #[error("{task} failed")]
    TaskFailed {
        task: &'static str,
        #[source]
        source: JoinError,
    },
}

pub(super) enum ConductorExit {
    Supervisor(Result<(), SupervisorError>),
    Monitor(Result<Result<(), MonitorTaskError>, JoinError>),
    JobCleanup(Result<(), JoinError>),
    GracefulShutdown,
}

impl ConductorExit {
    pub(super) fn handle(self) -> Result<(), ConductorExitError> {
        use ConductorExit::{GracefulShutdown, JobCleanup, Monitor, Supervisor};
        match self {
            Supervisor(Ok(())) => {
                info!("Supervisor finished");
                Ok(())
            }

            Supervisor(Err(err)) => {
                error!("Supervisor failed with error {err:?}");
                Err(ConductorExitError::Supervisor(err))
            }

            Monitor(Ok(Ok(()))) => {
                info!("Apalis monitor exited");
                Ok(())
            }

            Monitor(Ok(Err(err))) => Err(ConductorExitError::Monitor(err)),

            Monitor(Err(source)) => Err(ConductorExitError::TaskFailed {
                task: "Apalis monitor",
                source,
            }),

            JobCleanup(Ok(())) => Err(ConductorExitError::UnexpectedTaskExit {
                task: "Job cleanup",
            }),

            JobCleanup(Err(join_error)) if join_error.is_cancelled() => {
                info!("Job cleanup exited");
                Ok(())
            }

            JobCleanup(Err(source)) => Err(ConductorExitError::TaskFailed {
                task: "Job cleanup",
                source,
            }),

            // Handled directly in wait_for_completion before calling handle().
            GracefulShutdown => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn cancelled_join_error() -> JoinError {
        let handle = tokio::spawn(std::future::pending::<()>());
        handle.abort();
        handle
            .await
            .expect_err("aborted task must return a JoinError")
    }

    async fn panicked_join_error() -> JoinError {
        let handle = tokio::spawn(async { panic!("intentional panic") });
        handle
            .await
            .expect_err("panicking task must return a JoinError")
    }

    #[tokio::test]
    async fn supervisor_ok_returns_ok() {
        ConductorExit::Supervisor(Ok(())).handle().unwrap();
    }

    #[tokio::test]
    async fn supervisor_err_propagates_as_supervisor_variant() {
        let error = SupervisorError::TooManyDeadTasks {
            current_percentage: 50.0,
            threshold: 25.0,
        };
        assert!(matches!(
            ConductorExit::Supervisor(Err(error)).handle().unwrap_err(),
            ConductorExitError::Supervisor(SupervisorError::TooManyDeadTasks { .. })
        ));
    }

    #[tokio::test]
    async fn monitor_inner_ok_returns_ok() {
        ConductorExit::Monitor(Ok(Ok(()))).handle().unwrap();
    }

    #[tokio::test]
    async fn monitor_inner_unexpected_exit_returns_monitor_variant() {
        assert!(matches!(
            ConductorExit::Monitor(Ok(Err(MonitorTaskError::UnexpectedExit { source: None })))
                .handle()
                .unwrap_err(),
            ConductorExitError::Monitor(MonitorTaskError::UnexpectedExit { source: None })
        ));
    }

    #[tokio::test]
    async fn monitor_join_error_returns_task_failed_with_apalis_monitor_label() {
        let join_error = panicked_join_error().await;
        match ConductorExit::Monitor(Err(join_error))
            .handle()
            .unwrap_err()
        {
            ConductorExitError::TaskFailed { task, source } => {
                assert_eq!(task, "Apalis monitor");
                assert!(
                    source.is_panic(),
                    "Panicked task must surface a panic JoinError"
                );
            }
            other => panic!("expected TaskFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn job_cleanup_ok_returns_unexpected_task_exit() {
        match ConductorExit::JobCleanup(Ok(())).handle().unwrap_err() {
            ConductorExitError::UnexpectedTaskExit { task } => {
                assert_eq!(task, "Job cleanup");
            }
            other => panic!("expected UnexpectedTaskExit, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn job_cleanup_cancellation_returns_ok() {
        let join_error = cancelled_join_error().await;
        assert!(
            join_error.is_cancelled(),
            "Aborted task must produce a cancellation JoinError"
        );
        ConductorExit::JobCleanup(Err(join_error)).handle().unwrap();
    }

    #[tokio::test]
    async fn job_cleanup_panic_returns_task_failed_with_job_cleanup_label() {
        let join_error = panicked_join_error().await;
        match ConductorExit::JobCleanup(Err(join_error))
            .handle()
            .unwrap_err()
        {
            ConductorExitError::TaskFailed { task, source } => {
                assert_eq!(task, "Job cleanup");
                assert!(
                    source.is_panic(),
                    "Panicked task must surface a panic JoinError"
                );
            }
            other => panic!("expected TaskFailed, got {other:?}"),
        }
    }
}
