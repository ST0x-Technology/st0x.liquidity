//! One trade-accounting worker per hedged chain, run outside the apalis
//! monitor so that a terminal failure halts only its own chain.
//!
//! A halted chain is observe-only for hedging: its worker is stopped, so the
//! fills queued behind the failed one wait. Every `redrive_interval` the chain
//! re-drives the failed row with a fresh budget and starts a new worker, which
//! takes that row first. The chain resumes when the row is accounted, or at
//! the next interval once an operator has marked it `Killed`. The monitor
//! cannot do this itself: it restarts a stopped worker at once or never. Each
//! generation leaves its row in `Workers`, one per interval while halted.

use alloy::providers::Provider;
use apalis::prelude::Status;
use apalis_core::worker::context::WorkerContext;
use apalis_core::worker::event::Event;
use futures_util::future::join_all;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use st0x_evm::Chain;
use st0x_execution::Executor;

#[cfg(any(test, feature = "test-support"))]
use super::job::FailureInjector;
use super::job::{
    Job, StatusLookupError, TaskIdentity, assert_terminal_failure_msg_names_a_kind,
    build_worker_inner, find_job_error, spawn_terminal_failure_alert,
};
use crate::alerts::Notifier;
use crate::trading::onchain::trade_accountant::{
    AccountForDexTrade, AccountantCtx, DexTradeAccountingJobQueue, DexTradeAccountingQueues,
    TradeAccountingError,
};

/// `1` while a chain's trade accounting is halted by a terminal failure.
pub(crate) const CHAIN_OBSERVE_ONLY_GAUGE: &str = "trade_accounting_observe_only";

/// Runs every hedged chain's accounting worker until `shutdown`. Each chain
/// halts and resumes on its own; the re-drive interval is the configured
/// `trade_accounting_redrive_interval_secs`.
pub(super) async fn run_trade_accounting_workers<Prov, Exec>(
    accounting_queues: &DexTradeAccountingQueues,
    accountant_ctx: &Arc<AccountantCtx<Prov, Exec>>,
    shutdown: CancellationToken,
    #[cfg(any(test, feature = "test-support"))] failure_injector: &FailureInjector,
) where
    Prov: Provider + Clone + Send + Sync + 'static,
    Exec: Executor + Clone + Send + Sync + 'static,
    TradeAccountingError: From<Exec::Error>,
{
    let redrive_interval =
        Duration::from_secs(accountant_ctx.ctx.trade_accounting_redrive_interval_secs);

    join_all(accounting_queues.iter().map(|(chain, queue)| {
        ChainAccountingWorker {
            chain: *chain,
            queue: queue.clone(),
            accountant_ctx: Arc::clone(accountant_ctx),
            redrive_interval,
            shutdown: shutdown.clone(),
            #[cfg(any(test, feature = "test-support"))]
            failure_injector: failure_injector.clone(),
        }
        .run()
    }))
    .await;
}

struct ChainAccountingWorker<Prov, Exec> {
    chain: Chain,
    queue: DexTradeAccountingJobQueue,
    accountant_ctx: Arc<AccountantCtx<Prov, Exec>>,
    redrive_interval: Duration,
    shutdown: CancellationToken,
    #[cfg(any(test, feature = "test-support"))]
    failure_injector: FailureInjector,
}

impl<Prov, Exec> ChainAccountingWorker<Prov, Exec>
where
    Prov: Provider + Clone + Send + Sync + 'static,
    Exec: Executor + Clone + Send + Sync + 'static,
    TradeAccountingError: From<Exec::Error>,
{
    async fn run(self) {
        let halt = Arc::new(ChainHalt {
            chain: self.chain,
            notifier: Arc::clone(&self.accountant_ctx.notifier),
            redrive_interval: self.redrive_interval,
            error_msg: <AccountForDexTrade as Job<AccountantCtx<Prov, Exec>>>::TERMINAL_FAILURE_MSG,
            halted: Mutex::new(None),
        });
        metrics::gauge!(CHAIN_OBSERVE_ONLY_GAUGE, "chain" => self.chain.as_str()).set(0.0);

        // A new name per generation: apalis refuses to register a worker
        // name whose previous holder heartbeated within its keep-alive.
        for generation in 0_u64.. {
            if self.shutdown.is_cancelled() {
                return;
            }

            let attempt = Arc::new(WorkerAttempt::default());
            assert_terminal_failure_msg_names_a_kind!(::<AccountantCtx<Prov, Exec>, AccountForDexTrade>);
            let worker = build_worker_inner!(
                ::<AccountantCtx<Prov, Exec>, AccountForDexTrade>,
                format_args!("{}-{generation}", self.chain),
                self.queue.clone(),
                Arc::clone(&self.accountant_ctx),
                on_event(Arc::clone(&halt), Arc::clone(&attempt)),
                #[cfg(any(test, feature = "test-support"))]
                self.failure_injector.clone(),
            );

            let run = worker.run();
            tokio::pin!(run);
            // `run` first: its first poll emits `Event::Start`, which hands
            // over the context the shutdown branch stops.
            let result = tokio::select! {
                biased;
                result = &mut run => result,
                () = self.shutdown.cancelled() => {
                    attempt.stop_worker();
                    if let Err(error) = run.await {
                        error!(chain = %self.chain, %error, "Trade accounting worker failed while stopping");
                    }
                    return;
                }
            };

            match (attempt.take_failure(), result) {
                (Some(failure), _) => halt.begin(failure),
                (None, Err(error)) => halt.begin(WorkerFailure {
                    halt: Halt::WorkerExit,
                    worker: format!(
                        "{}-{}-{generation}",
                        <AccountForDexTrade as Job<AccountantCtx<Prov, Exec>>>::WORKER_NAME,
                        self.chain,
                    ),
                    error: error.to_string(),
                }),
                (None, Ok(())) => {
                    warn!(chain = %self.chain, "Trade accounting worker stopped without a failure; restarting it");
                }
            }

            if !self.wait_and_redrive(&halt).await {
                return;
            }
        }
    }

    /// Waits one interval at a time until the halted row is re-queued or
    /// retired, then returns `true` so a new worker starts; `false` on
    /// shutdown. A failed lookup keeps the chain halted and tries again.
    async fn wait_and_redrive(&self, halt: &ChainHalt) -> bool {
        loop {
            tokio::select! {
                () = tokio::time::sleep(self.redrive_interval) => {}
                () = self.shutdown.cancelled() => return false,
            }

            match self.redrive(halt).await {
                Ok(()) => return true,
                Err(error) => {
                    error!(
                        target: "hedge",
                        chain = %self.chain,
                        %error,
                        "Could not re-drive halted trade accounting; the chain stays observe-only",
                    );
                }
            }
        }
    }

    async fn redrive(&self, halt: &ChainHalt) -> Result<(), RedriveError> {
        // The stopped worker may hold a prefetched row; free it so the next
        // worker takes the rows in order.
        self.queue.requeue_orphaned().await?;

        // A worker exit has no row to re-drive; the next worker that polls
        // cleanly lifts it.
        let Some(row) = halt.halted_row() else {
            return Ok(());
        };

        match self.queue.status_of(&row).await? {
            Some(Status::Failed) => {
                let count = self.queue.requeue_failed().await?;
                info!(target: "hedge", chain = %self.chain, %row, count, "Re-driving the trade accounting job that halted this chain");
            }
            None => halt.resume("its job row no longer exists"),
            Some(Status::Killed) => halt.resume("its job row was marked Killed"),
            Some(Status::Done) => halt.resume("its job row is done"),
            Some(status) => {
                info!(target: "hedge", chain = %self.chain, %row, %status, "Restarting trade accounting behind the job that halted this chain");
            }
        }

        Ok(())
    }
}

/// The failure that stopped one worker generation.
struct WorkerFailure {
    halt: Halt,
    worker: String,
    error: String,
}

/// What a chain is halted on.
#[derive(Clone, PartialEq, Eq)]
enum Halt {
    /// A job failed terminally; its row is re-driven.
    Job(TaskIdentity),
    /// The worker exited without a job error; it is restarted.
    WorkerExit,
}

impl fmt::Display for Halt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Job(row) => write!(formatter, "job row {row}"),
            Self::WorkerExit => formatter.write_str("no job row (the worker exited)"),
        }
    }
}

/// What one worker generation reports back to its chain's loop. A poisoned
/// lock here and in [`ChainHalt`] still holds whole values, so it is read
/// through.
#[derive(Default)]
struct WorkerAttempt {
    ctx: Mutex<Option<WorkerContext>>,
    failure: Mutex<Option<WorkerFailure>>,
}

impl WorkerAttempt {
    fn stop_worker(&self) {
        if let Some(ctx) = self
            .ctx
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
        {
            let _ = ctx.stop();
        }
    }

    fn take_failure(&self) -> Option<WorkerFailure> {
        self.failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

/// The current halt of one chain, shared by its loop and its workers.
struct ChainHalt {
    chain: Chain,
    notifier: Arc<dyn Notifier>,
    redrive_interval: Duration,
    error_msg: &'static str,
    /// `Some` while halted.
    halted: Mutex<Option<Halt>>,
}

impl ChainHalt {
    /// Halts the chain and pages, unless it is already halted on this row:
    /// a failed re-drive is the same halt, not a new page.
    fn begin(&self, failure: WorkerFailure) {
        let chain = self.chain;
        let WorkerFailure {
            halt,
            worker,
            error,
        } = failure;
        let mut halted = self.halted.lock().unwrap_or_else(PoisonError::into_inner);

        if halted.as_ref() == Some(&halt) {
            warn!(target: "hedge", %chain, %worker, %error, "Re-driven trade accounting job failed again; the chain stays observe-only");
            return;
        }

        *halted = Some(halt.clone());
        drop(halted);
        metrics::gauge!(CHAIN_OBSERVE_ONLY_GAUGE, "chain" => chain.as_str()).set(1.0);

        let interval = self.redrive_interval.as_secs();
        let message = format!(
            "st0x-hedge: {worker}: {error_msg}: {error}; {halt}; {chain} is now observe-only: \
             its later fills are not accounted or hedged until this job succeeds, re-driven every \
             {interval}s, or its row is marked Killed; other chains continue; this fill may \
             already be in Position, so check Position for it before hedging it by hand, and \
             set the row to Killed if you do",
            error_msg = self.error_msg,
        );
        spawn_terminal_failure_alert(Arc::clone(&self.notifier), worker, message);
    }

    fn halted_row(&self) -> Option<TaskIdentity> {
        match &*self.halted.lock().unwrap_or_else(PoisonError::into_inner) {
            Some(Halt::Job(row)) => Some(row.clone()),
            Some(Halt::WorkerExit) | None => None,
        }
    }

    /// Lifts a halt on a worker exit once a new worker polls its queue
    /// cleanly; a halt on a job waits for that job.
    fn resume_after_worker_exit(&self) {
        let worker_exited = matches!(
            *self.halted.lock().unwrap_or_else(PoisonError::into_inner),
            Some(Halt::WorkerExit)
        );
        if worker_exited {
            self.resume("its restarted worker polls cleanly");
        }
    }

    fn resume(&self, reason: &str) {
        let Some(halt) = self
            .halted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        else {
            return;
        };

        let chain = self.chain;
        metrics::gauge!(CHAIN_OBSERVE_ONLY_GAUGE, "chain" => chain.as_str()).set(0.0);
        info!(target: "hedge", %chain, %halt, "Trade accounting on {chain} resumed: {reason}; hedging continues");
    }
}

/// Records each generation's context and failure, stops it on a terminal
/// failure, and lifts the halt on the first job it completes.
fn on_event(
    halt: Arc<ChainHalt>,
    attempt: Arc<WorkerAttempt>,
) -> impl Fn(&WorkerContext, &Event) + Send + Sync + 'static {
    move |ctx, event| match event {
        Event::Start => {
            *attempt.ctx.lock().unwrap_or_else(PoisonError::into_inner) = Some(ctx.clone());
        }
        Event::Success => halt.resume("its halted job was accounted"),
        Event::Idle => halt.resume_after_worker_exit(),
        Event::Error(error) => {
            let worker = ctx.name().clone();
            error!(%error, %worker, chain = %halt.chain, "Trade accounting job failed after retries; the chain is now observe-only");
            let _ = ctx.stop();

            let mut failure = attempt
                .failure
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if failure.is_none() {
                *failure = Some(WorkerFailure {
                    halt: find_job_error(error.as_ref().as_ref())
                        .map_or(Halt::WorkerExit, |job_error| {
                            Halt::Job(job_error.task_identity().clone())
                        }),
                    worker,
                    error: error.to_string(),
                });
            }
        }
        Event::HeartBeat | Event::Custom(_) | Event::Stop => {}
    }
}

#[derive(Debug, thiserror::Error)]
enum RedriveError {
    #[error("failed to re-queue the halted chain's trade accounting jobs")]
    Requeue(#[from] apalis_sqlite::SqlxError),
    #[error("failed to read the status of the job that halted the chain")]
    Status(#[from] StatusLookupError),
}
