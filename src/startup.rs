//! Coordinates startup acknowledgements and deployment readiness reporting.
//!
//! [`StartupBarrier`] waits for one acknowledgement from each essential run
//! loop, while [`StartupToken`] acknowledges only after its wrapped future has
//! reached a pending state. Once the barrier completes, the deployment notifier
//! writes the ready process ID that activation validates against systemd.

use anyhow::Context;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context as TaskContext, Poll};
use task_supervisor::{SupervisedTask, TaskResult};
use tokio::sync::Notify;

const READY_FILE_ENV: &str = "ST0X_STARTUP_READY_FILE";

pub(crate) trait StartupNotifier: Send + Sync {
    fn notify_ready(&self) -> io::Result<()>;
}

#[derive(Debug, Default)]
pub(crate) struct NoopStartupNotifier;

impl StartupNotifier for NoopStartupNotifier {
    fn notify_ready(&self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct DeploymentStartupNotifier {
    ready_file: PathBuf,
}

impl DeploymentStartupNotifier {
    pub(crate) fn from_env() -> Option<Self> {
        std::env::var_os(READY_FILE_ENV).map(|ready_file| Self {
            ready_file: PathBuf::from(ready_file),
        })
    }
}

impl StartupNotifier for DeploymentStartupNotifier {
    fn notify_ready(&self) -> io::Result<()> {
        std::fs::write(&self.ready_file, format!("{}\n", std::process::id()))
    }
}

pub(crate) fn report_ready(notifier: &dyn StartupNotifier) -> anyhow::Result<()> {
    notifier
        .notify_ready()
        .context("failed to report that startup completed")
}

#[derive(Debug)]
pub(crate) struct StartupBarrier {
    required: usize,
    acknowledged: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl StartupBarrier {
    pub(crate) fn new(required: usize) -> Self {
        Self {
            required,
            acknowledged: Arc::new(AtomicUsize::new(0)),
            notify: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn token(&self) -> StartupToken {
        StartupToken {
            acknowledged: Arc::new(AtomicBool::new(false)),
            acknowledged_count: Arc::clone(&self.acknowledged),
            notify: Arc::clone(&self.notify),
        }
    }

    pub(crate) async fn wait(&self) {
        loop {
            let notified = self.notify.notified();

            if self.acknowledged.load(Ordering::Acquire) >= self.required {
                return;
            }

            notified.await;
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StartupToken {
    acknowledged: Arc<AtomicBool>,
    acknowledged_count: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl StartupToken {
    pub(crate) fn acknowledge(&self) {
        if !self.acknowledged.swap(true, Ordering::AcqRel) {
            self.acknowledged_count.fetch_add(1, Ordering::AcqRel);
            self.notify.notify_waiters();
        }
    }

    pub(crate) fn wrap<F>(self, future: F) -> StartupFuture<F>
    where
        F: Future,
    {
        StartupFuture {
            future: Box::pin(future),
            token: self,
        }
    }
}

pub(crate) struct StartupFuture<F> {
    future: Pin<Box<F>>,
    token: StartupToken,
}

impl<F> Future for StartupFuture<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match this.future.as_mut().poll(cx) {
            Poll::Pending => {
                this.token.acknowledge();
                Poll::Pending
            }
            Poll::Ready(output) => Poll::Ready(output),
        }
    }
}

#[derive(Clone)]
pub(crate) struct StartupTask<T> {
    pub(crate) task: T,
    pub(crate) token: StartupToken,
}

impl<T> SupervisedTask for StartupTask<T>
where
    T: SupervisedTask,
{
    async fn run(&mut self) -> TaskResult {
        self.token.clone().wrap(self.task.run()).await
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::future::{pending, ready};
    use std::io;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use task_supervisor::{SupervisedTask, TaskResult};
    use tempfile::tempdir;

    use super::{
        DeploymentStartupNotifier, StartupBarrier, StartupNotifier, StartupTask, report_ready,
    };

    #[derive(Clone)]
    struct PendingTask;

    impl SupervisedTask for PendingTask {
        async fn run(&mut self) -> TaskResult {
            pending().await
        }
    }

    struct RecordingNotifier<'a> {
        notifications: &'a AtomicUsize,
        error_kind: Option<io::ErrorKind>,
    }

    impl StartupNotifier for RecordingNotifier<'_> {
        fn notify_ready(&self) -> io::Result<()> {
            self.notifications.fetch_add(1, Ordering::SeqCst);

            self.error_kind
                .map_or(Ok(()), |kind| Err(io::Error::from(kind)))
        }
    }

    #[test]
    fn deployment_notification_writes_the_current_process_id() {
        let directory = tempdir().expect("temporary directory should be created");
        let ready_file = directory.path().join("ready");
        let notifier = DeploymentStartupNotifier {
            ready_file: ready_file.clone(),
        };

        report_ready(&notifier).expect("readiness notification should succeed");

        let contents = std::fs::read_to_string(ready_file).expect("ready file should be readable");
        assert_eq!(contents, format!("{}\n", std::process::id()));
    }

    #[test]
    fn readiness_notification_failure_preserves_the_source() {
        let notifications = AtomicUsize::new(0);
        let notifier = RecordingNotifier {
            notifications: &notifications,
            error_kind: Some(io::ErrorKind::PermissionDenied),
        };

        let error = report_ready(&notifier).expect_err("notification should fail");
        let source = error
            .downcast_ref::<io::Error>()
            .expect("the notification source should be preserved");

        assert_eq!(source.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(notifications.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn immediately_ready_future_does_not_acknowledge_startup() {
        let barrier = StartupBarrier::new(1);
        let token = barrier.token();

        token.wrap(ready(Ok::<(), Infallible>(()))).await.unwrap();

        tokio::time::timeout(std::time::Duration::from_millis(10), barrier.wait())
            .await
            .expect_err("an immediately ready future must not acknowledge startup");
    }

    #[tokio::test]
    async fn pending_future_acknowledges_startup_once_across_clones() {
        let barrier = StartupBarrier::new(1);
        let token = barrier.token();
        let cloned = token.clone();
        let first = tokio::spawn(token.wrap(pending::<()>()));
        let second = tokio::spawn(cloned.wrap(pending::<()>()));

        tokio::time::timeout(std::time::Duration::from_secs(1), barrier.wait())
            .await
            .expect("pending future should acknowledge startup");

        first.abort();
        second.abort();
    }

    #[tokio::test]
    async fn supervised_task_acknowledges_after_reaching_its_run_loop() {
        let barrier = StartupBarrier::new(1);
        let mut task = StartupTask {
            task: PendingTask,
            token: barrier.token(),
        };
        let task_handle = tokio::spawn(async move { task.run().await });

        tokio::time::timeout(std::time::Duration::from_secs(1), barrier.wait())
            .await
            .expect("a pending supervised task should acknowledge startup");

        task_handle.abort();
    }
}
