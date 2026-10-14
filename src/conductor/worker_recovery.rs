//! Keeps job processing alive through transient queue failures and surfaces fatal worker exits.

use apalis::prelude::Monitor;
use apalis_core::backend::Backend;
use apalis_core::error::WorkerError;
use apalis_core::task::Task;
use apalis_core::worker::context::WorkerContext;
use apalis_sqlite::SqlxError;
use futures_util::stream::{self, BoxStream};
use futures_util::{Stream, StreamExt};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{error, info};

use super::job::WORKER_RETRIES;

/// Restarts only queue streams, retaining the worker's handlers, middleware,
/// and reservations. Rebuilding the entire worker would drop active financial
/// operations and require replay while external side effects may still finish.
pub(crate) struct RecoveringBackend<Queue> {
    queue: Queue,
    restarts: Arc<AtomicUsize>,
}

impl<Queue> RecoveringBackend<Queue> {
    pub(crate) fn new(queue: Queue) -> Self {
        Self {
            queue,
            restarts: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl<Queue> Backend for RecoveringBackend<Queue>
where
    Queue: Backend<Error = SqlxError> + Clone + Send + 'static,
    Queue::Args: Send + 'static,
    Queue::Context: Send + 'static,
    Queue::IdType: Send + 'static,
    Queue::Stream: Send + 'static,
    Queue::Beat: Send + 'static,
{
    type Args = Queue::Args;
    type IdType = Queue::IdType;
    type Context = Queue::Context;
    type Error = SqlxError;
    type Stream = BoxStream<
        'static,
        Result<Option<Task<Self::Args, Self::Context, Self::IdType>>, SqlxError>,
    >;
    type Beat = BoxStream<'static, Result<(), SqlxError>>;
    type Layer = Queue::Layer;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let queue = self.queue.clone();
        let worker_for_stream = worker.clone();
        recover_stream(
            move || queue.heartbeat(&worker_for_stream),
            worker.clone(),
            self.restarts.clone(),
            "heartbeat",
        )
    }

    fn middleware(&self) -> Self::Layer {
        self.queue.middleware()
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let worker_for_stream = worker.clone();
        recover_stream(
            move || self.queue.clone().poll(&worker_for_stream),
            worker.clone(),
            self.restarts,
            "fetch",
        )
    }
}

fn recover_stream<Item, QueueStream, Factory>(
    factory: Factory,
    worker: WorkerContext,
    restarts: Arc<AtomicUsize>,
    stream_name: &'static str,
) -> BoxStream<'static, Result<Item, SqlxError>>
where
    Item: Send + 'static,
    QueueStream: Stream<Item = Result<Item, SqlxError>> + Send + 'static,
    Factory: Fn() -> QueueStream + Send + 'static,
{
    let initial = Some(factory().boxed());
    stream::unfold((initial, factory, worker, restarts), move |(current, factory, worker, restarts)| async move {
        let mut current = current?;
        loop {
            let source = match current.next().await {
                Some(Ok(item)) => return Some((Ok(item), (Some(current), factory, worker, restarts))),
                Some(Err(source)) => source,
                None if worker.is_shutting_down() || !worker.is_running() => return None,
                None => SqlxError::Io(io::Error::new(io::ErrorKind::UnexpectedEof, "Queue stream ended unexpectedly")),
            };
            error!(worker = %worker.name(), stream = stream_name, error = ?source, "Job queue stream failed");
            // A decode error can follow reservation of a malformed payload.
            // Retrying that stream would strand the reserved row; fail-stop.
            let retryable = match &source {
                SqlxError::Database(_) | SqlxError::Io(_) | SqlxError::PoolTimedOut | SqlxError::WorkerCrashed => true,
                _ => false, // SqlxError is non-exhaustive; unknown errors fail closed.
            };
            if !retryable {
                return Some((Err(source), (None, factory, worker, restarts)));
            }
            let attempt = restarts.fetch_update(Ordering::AcqRel, Ordering::Acquire, |attempt| {
                (attempt < WORKER_RETRIES).then_some(attempt + 1)
            });
            let Ok(attempt) = attempt else {
                error!(worker = %worker.name(), stream = stream_name, "Job queue stream restart budget exhausted");
                return Some((Err(source), (None, factory, worker, restarts)));
            };
            let delay = Duration::from_secs(1 << attempt);
            info!(worker = %worker.name(), stream = stream_name, restart = attempt + 1, ?delay, "Restarting job queue stream");
            tokio::time::sleep(delay).await;
            current = factory().boxed();
        }
    }).boxed()
}

/// Apalis joins all workers before returning errors. Notify the conductor at
/// the first fatal exit instead; notify_one retains a permit before select polls.
pub(crate) fn observe_worker_exits(monitor: Monitor, failure: Arc<Notify>) -> Monitor {
    monitor.should_restart(move |worker, source, _attempt| {
        match source {
            WorkerError::GracefulExit => {}
            WorkerError::StreamError(_)
            | WorkerError::HeartbeatError(_)
            | WorkerError::StateError(_)
            | WorkerError::PanicError(_)
            | WorkerError::IoError(_) => {
                error!(worker = %worker.name(), error = ?source, "Job worker exited unexpectedly");
                failure.notify_one();
            }
        }
        false
    })
}

#[cfg(test)]
mod tests {
    use apalis::layers::WorkerBuilderExt;
    use apalis::prelude::{TaskSink, WorkerBuilder};
    use apalis_core::backend::Backend;
    use apalis_core::worker::context::WorkerContext;
    use apalis_sqlite::SqlxError;
    use futures_util::StreamExt;
    use futures_util::stream::{self, BoxStream};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;
    use tracing_test::traced_test;

    use super::{RecoveringBackend, observe_worker_exits, recover_stream};
    use crate::conductor::exit::MonitorTaskError;
    use crate::conductor::job::JobQueue;
    use crate::test_utils::setup_test_apalis_pool;

    #[derive(Clone, Copy)]
    enum FailureSite {
        Fetch,
        Heartbeat,
    }

    #[derive(Clone)]
    struct FailingQueue<Queue> {
        queue: Queue,
        site: FailureSite,
        remaining: Arc<AtomicUsize>,
        observed: Arc<Notify>,
        trigger: Option<Arc<Notify>>,
        recovered: Arc<Notify>,
    }

    impl<Queue> FailingQueue<Queue> {
        fn fail(&self) -> bool {
            self.remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                    count.checked_sub(1)
                })
                .is_ok()
        }
    }

    impl<Queue> Backend for FailingQueue<Queue>
    where
        Queue: Backend<Error = SqlxError> + Clone,
        Queue::Args: Send + 'static,
        Queue::Context: Send + 'static,
        Queue::IdType: Send + 'static,
        Queue::Stream: Send + 'static,
        Queue::Beat: Send + 'static,
    {
        type Args = Queue::Args;
        type IdType = Queue::IdType;
        type Context = Queue::Context;
        type Error = SqlxError;
        type Stream = BoxStream<
            'static,
            Result<
                Option<apalis_core::task::Task<Self::Args, Self::Context, Self::IdType>>,
                SqlxError,
            >,
        >;
        type Beat = BoxStream<'static, Result<(), SqlxError>>;
        type Layer = Queue::Layer;

        fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
            match self.site {
                FailureSite::Heartbeat if self.fail() => {
                    let observed = self.observed.clone();
                    let trigger = self.trigger.clone();
                    stream::once(async move {
                        if let Some(trigger) = trigger {
                            trigger.notified().await;
                        }
                        observed.notify_one();
                        Err(SqlxError::PoolTimedOut)
                    })
                    .boxed()
                }
                FailureSite::Fetch | FailureSite::Heartbeat => {
                    let recovered = self.recovered.clone();
                    self.queue
                        .heartbeat(worker)
                        .map(move |result| {
                            if matches!(&result, Ok(())) {
                                recovered.notify_one();
                            }
                            result
                        })
                        .boxed()
                }
            }
        }

        fn middleware(&self) -> Self::Layer {
            self.queue.middleware()
        }

        fn poll(self, worker: &WorkerContext) -> Self::Stream {
            match self.site {
                FailureSite::Fetch if self.fail() => {
                    let observed = self.observed.clone();
                    self.queue
                        .poll(worker)
                        .take(1)
                        .chain(stream::once(async move {
                            observed.notify_one();
                            Err(SqlxError::PoolTimedOut)
                        }))
                        .boxed()
                }
                FailureSite::Fetch | FailureSite::Heartbeat => self.queue.poll(worker).boxed(),
            }
        }
    }

    async fn exercise_recovery(site: FailureSite, failures: usize) {
        let pool = setup_test_apalis_pool().await;
        let original_queue = JobQueue::<u32>::new(&pool).into_storage();
        let config = original_queue
            .config()
            .clone()
            .set_keep_alive(Duration::from_millis(20));
        let mut queue: apalis_sqlite::SqliteStorage<
            u32,
            apalis_codec::json::JsonCodec<apalis_sqlite::CompactType>,
            apalis_sqlite::fetcher::SqliteFetcher,
        > = apalis_sqlite::SqliteStorage::new_with_config(&pool, &config);
        let observed = Arc::new(Notify::new());
        let remaining = Arc::new(AtomicUsize::new(failures));
        let recovered = Arc::new(Notify::new());
        let failing_queue = FailingQueue {
            queue: queue.clone(),
            site,
            remaining: remaining.clone(),
            observed: observed.clone(),
            trigger: None,
            recovered: recovered.clone(),
        };
        let processed = Arc::new(Notify::new());
        let processed_for_worker = processed.clone();
        let fatal = Arc::new(Notify::new());
        let shutdown = CancellationToken::new();
        let shutdown_for_task = shutdown.clone();
        let monitor = observe_worker_exits(apalis::prelude::Monitor::new(), fatal.clone())
            .register(move |_| {
                let processed = processed_for_worker.clone();
                WorkerBuilder::new("recovery-test")
                    .backend(RecoveringBackend::new(failing_queue.clone()))
                    .concurrency(1)
                    .build(move |value: u32| {
                        let processed = processed.clone();
                        async move {
                            assert_eq!(value, 42);
                            processed.notify_one();
                        }
                    })
            });
        let task = tokio::spawn(async move {
            tokio::select! {
                () = fatal.notified() => Err(MonitorTaskError::UnexpectedExit { source: None }),
                result = monitor.run_with_signal(async move {
                    shutdown_for_task.cancelled().await;
                    Ok(())
                }) => result.map_err(|source| MonitorTaskError::UnexpectedExit { source: Some(source) }),
            }
        });
        tokio::time::timeout(Duration::from_secs(5), observed.notified())
            .await
            .unwrap();
        if failures == 1 {
            tokio::time::timeout(Duration::from_secs(5), recovered.notified())
                .await
                .expect("heartbeat must resume successful SQLite operations after recovery");
            queue.push(42).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), processed.notified())
                .await
                .expect("a job pushed after a storage error must process after recovery");
            assert_eq!(remaining.load(Ordering::SeqCst), 0);
            shutdown.cancel();
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        } else {
            let error = tokio::time::timeout(Duration::from_secs(12), task)
                .await
                .expect("persistent storage errors must fail-stop without an external shutdown")
                .unwrap()
                .unwrap_err();
            assert!(matches!(error, MonitorTaskError::UnexpectedExit { .. }));
            assert_eq!(remaining.load(Ordering::SeqCst), failures - 4);
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn fetch_failure_recovers_and_processes_subsequent_work() {
        exercise_recovery(FailureSite::Fetch, 1).await;
        assert!(logs_contain("Job queue stream failed"));
        assert!(logs_contain("recovery-test"));
        assert!(logs_contain("PoolTimedOut"));
        assert!(logs_contain("Restarting job queue stream"));
    }

    #[tokio::test]
    #[traced_test]
    async fn heartbeat_failure_recovers_and_processes_subsequent_work() {
        exercise_recovery(FailureSite::Heartbeat, 1).await;
        assert!(logs_contain("Job queue stream failed"));
        assert!(logs_contain("heartbeat"));
        assert!(logs_contain("Restarting job queue stream"));
    }

    #[tokio::test]
    async fn persistent_fetch_failure_fails_stop_promptly() {
        exercise_recovery(FailureSite::Fetch, 10).await;
    }

    #[tokio::test]
    async fn persistent_heartbeat_failure_fails_stop_promptly() {
        exercise_recovery(FailureSite::Heartbeat, 10).await;
    }

    #[tokio::test]
    async fn heartbeat_recovery_preserves_inflight_work_and_single_concurrency() {
        let pool = setup_test_apalis_pool().await;
        let mut queue = JobQueue::<u32>::new(&pool).into_storage();
        queue.push(1).await.unwrap();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let completed = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let trigger = Arc::new(Notify::new());
        let observed = Arc::new(Notify::new());
        let failing_queue = FailingQueue {
            queue: queue.clone(),
            site: FailureSite::Heartbeat,
            remaining: Arc::new(AtomicUsize::new(1)),
            observed: observed.clone(),
            trigger: Some(trigger.clone()),
            recovered: Arc::new(Notify::new()),
        };
        let completed_for_worker = completed.clone();
        let started_for_worker = started.clone();
        let release_for_worker = release.clone();
        let calls_for_worker = calls.clone();
        let shutdown = CancellationToken::new();
        let shutdown_for_task = shutdown.clone();
        let worker = WorkerBuilder::new("inflight-recovery-test")
            .backend(RecoveringBackend::new(failing_queue))
            .concurrency(1)
            .build(move |value: u32| {
                let started = started_for_worker.clone();
                let release = release_for_worker.clone();
                let completed = completed_for_worker.clone();
                let calls = calls_for_worker.clone();
                async move {
                    let previous_calls = calls.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(
                        value as usize,
                        previous_calls + 1,
                        "no replay or concurrency change"
                    );
                    if value == 1 {
                        started.notify_one();
                        release.notified().await;
                    } else {
                        completed.notify_one();
                    }
                }
            });
        let task = tokio::spawn(worker.run_until(async move {
            shutdown_for_task.cancelled().await;
            Ok::<(), std::io::Error>(())
        }));
        tokio::time::timeout(Duration::from_secs(5), started.notified())
            .await
            .unwrap();
        queue.push(2).await.unwrap();
        trigger.notify_one();
        tokio::time::timeout(Duration::from_secs(5), observed.notified())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), completed.notified())
            .await
            .expect(
                "the original active handler and its sibling must finish after heartbeat recovery",
            );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn fetch_and_heartbeat_share_one_restart_budget() {
        let mut worker = WorkerContext::new::<()>("shared-budget-test");
        worker.start().unwrap();
        let restarts = Arc::new(AtomicUsize::new(0));
        let fetch = recover_stream(
            || stream::once(async { Err::<(), _>(SqlxError::PoolTimedOut) }),
            worker.clone(),
            restarts.clone(),
            "fetch",
        );
        let heartbeat = recover_stream(
            || stream::once(async { Err::<(), _>(SqlxError::PoolTimedOut) }),
            worker,
            restarts.clone(),
            "heartbeat",
        );
        let (fetch_errors, beat_errors) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(fetch.collect::<Vec<_>>(), heartbeat.collect::<Vec<_>>())
        })
        .await
        .unwrap();
        assert_eq!(restarts.load(Ordering::Acquire), 3);
        assert_eq!(fetch_errors.len(), 1);
        assert_eq!(beat_errors.len(), 1);
        assert!(matches!(&fetch_errors[0], Err(SqlxError::PoolTimedOut)));
        assert!(matches!(&beat_errors[0], Err(SqlxError::PoolTimedOut)));
    }

    #[tokio::test]
    async fn reserved_payload_decode_failure_is_not_restarted() {
        let pool = setup_test_apalis_pool().await;
        let mut queue = JobQueue::<u32>::new(&pool);
        queue.push(1).await.unwrap();
        sqlx_apalis::query("UPDATE Jobs SET job = CAST('invalid-json' AS BLOB)")
            .execute(&pool)
            .await
            .unwrap();
        let backend = RecoveringBackend::new(queue.into_storage());
        let restarts = backend.restarts.clone();
        let worker = WorkerBuilder::new("decode-failure-test")
            .backend(backend)
            .build(|_: u32| async {});
        let error = tokio::time::timeout(Duration::from_secs(5), worker.run())
            .await
            .unwrap()
            .unwrap_err();
        let apalis_core::error::WorkerError::StreamError(source) = error else {
            panic!("decode failure must exit the fetch stream");
        };
        assert!(matches!(
            source.downcast_ref::<SqlxError>(),
            Some(SqlxError::Decode(_))
        ));
        assert_eq!(restarts.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn worker_panic_signals_fatal_exit_before_external_shutdown() {
        let pool = setup_test_apalis_pool().await;
        let mut queue = JobQueue::<u32>::new(&pool);
        queue.push(1).await.unwrap();
        let fatal = Arc::new(Notify::new());
        let monitor = observe_worker_exits(apalis::prelude::Monitor::new(), fatal.clone())
            .register(move |_| {
                WorkerBuilder::new("panic-test")
                    .backend(RecoveringBackend::new(queue.clone().into_storage()))
                    .build(|value: u32| async move {
                        assert_ne!(value, 1, "injected worker panic");
                    })
            });
        let task = tokio::spawn(async move {
            tokio::select! {
                () = fatal.notified() => Err(MonitorTaskError::UnexpectedExit { source: None }),
                result = monitor.run_with_signal(std::future::pending::<std::io::Result<()>>()) =>
                    result.map_err(|source| MonitorTaskError::UnexpectedExit { source: Some(source) }),
            }
        });
        let error = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(matches!(error, MonitorTaskError::UnexpectedExit { .. }));
    }
}
