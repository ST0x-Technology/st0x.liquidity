//! Reactor that broadcasts aggregate events to WebSocket dashboard
//! clients as [`Trade`] fills and [`TransferOperation`] updates.

use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::collections::{HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use task_supervisor::{SupervisedTask, TaskResult};
#[cfg(test)]
use tokio::sync::Notify;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::time::{Instant, interval, sleep_until};
use tracing::{debug, info, warn};

use st0x_dto::{Statement, Trade, TradeOutcome};
use st0x_event_sorcery::{
    AggregateError, EntityList, Reactor, SendError, deps, is_retryable_sqlite_busy, load_entity,
};
use st0x_finance::{FractionalShares, NotPositive, Positive};

use crate::conductor::job::{Job, JobQueue, Label, QueuePushError};
use crate::equity_redemption::EquityRedemption;
use crate::offchain::order::{
    OffchainOrder, OffchainOrderEvent, OffchainOrderId, TradeConversionError,
};
use crate::onchain_trade::{
    OnChainTrade, OnChainTradeEvent, OnChainTradeId, ParseOnChainTradeIdError,
};
use crate::position::{Position, PositionEvent};
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::UsdcRebalance;

deps!(
    Broadcaster,
    [
        OnChainTrade,
        Position,
        OffchainOrder,
        TokenizedEquityMint,
        EquityRedemption,
        UsdcRebalance,
    ]
);

pub(crate) type DashboardTradeDeliveryJobQueue = JobQueue<DeliverDashboardTrade>;

const HANDOFF_RETRY_INITIAL_DELAY: Duration = Duration::from_secs(1);
const HANDOFF_RETRY_MAX_DELAY: Duration = Duration::from_secs(30);
const HANDOFF_RETRY_MAX_ATTEMPTS: usize = 3;
const HANDOFF_RETRY_QUEUE_CAPACITY: usize = 64;
const HANDOFF_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Default)]
struct OnchainRevisionReloadFault {
    #[cfg(test)]
    failures_remaining: Arc<AtomicUsize>,
    #[cfg(not(test))]
    never_fail: bool,
}

impl OnchainRevisionReloadFault {
    #[cfg(test)]
    fn should_fail(&self) -> bool {
        consume_failure(&self.failures_remaining)
    }

    #[cfg(not(test))]
    const fn should_fail(&self) -> bool {
        self.never_fail
    }

    #[cfg(test)]
    fn fail_next(&self, count: usize) {
        self.failures_remaining.store(count, Ordering::SeqCst);
    }
}

#[derive(Clone, Default)]
struct OnchainRevisionTracker {
    pending: Arc<Mutex<HashSet<OnChainTradeId>>>,
}

impl OnchainRevisionTracker {
    async fn track(&self, id: OnChainTradeId) {
        self.pending.lock().await.insert(id);
    }

    async fn resolve(&self, id: &OnChainTradeId) {
        self.pending.lock().await.remove(id);
    }

    async fn pending(&self) -> Vec<OnChainTradeId> {
        self.pending.lock().await.iter().cloned().collect()
    }
}

/// Runtime dependencies shared by terminal-trade reactors and their worker.
pub(crate) struct DashboardTradeDelivery {
    pub(crate) queue: DashboardTradeDeliveryJobQueue,
    pub(crate) ctx: Arc<DashboardTradeDeliveryCtx>,
    pub(crate) broadcaster: Arc<Broadcaster>,
    pub(crate) handoff_monitor: DashboardTradeHandoffMonitor,
    #[cfg(test)]
    store: Arc<DashboardTradeDeliveryStore>,
    enqueuer: Arc<DashboardTradeEnqueuer>,
}

impl DashboardTradeDelivery {
    pub(crate) fn new(
        apalis_pool: &apalis_sqlite::SqlitePool,
        pool: &SqlitePool,
        sender: broadcast::Sender<Statement>,
    ) -> Self {
        let queue = DashboardTradeDeliveryJobQueue::new(apalis_pool);
        let store = Arc::new(DashboardTradeDeliveryStore::new(pool.clone()));
        let enqueuer = Arc::new(DashboardTradeEnqueuer::new(queue.clone(), store.clone()));
        let revision_reload_fault = OnchainRevisionReloadFault::default();
        let revision_tracker = OnchainRevisionTracker::default();
        let publish_lock = Arc::new(Mutex::new(()));
        #[cfg(test)]
        let test_store = store.clone();
        let (handoff_retry_sender, handoff_retry_receiver) =
            mpsc::channel(HANDOFF_RETRY_QUEUE_CAPACITY);
        let ctx = Arc::new(DashboardTradeDeliveryCtx::with_store(
            sender.clone(),
            store,
            pool.clone(),
            publish_lock.clone(),
        ));
        let broadcaster = Arc::new(Broadcaster::new(
            sender.clone(),
            pool.clone(),
            enqueuer.clone(),
            handoff_retry_sender,
            revision_reload_fault.clone(),
            revision_tracker.clone(),
            publish_lock.clone(),
        ));
        let handoff_monitor = DashboardTradeHandoffMonitor::new(
            handoff_retry_receiver,
            enqueuer.clone(),
            pool.clone(),
            sender,
            revision_reload_fault,
            revision_tracker,
            publish_lock,
        );

        Self {
            queue,
            ctx,
            broadcaster,
            handoff_monitor,
            #[cfg(test)]
            store: test_store,
            enqueuer,
        }
    }

    /// Reconstructs missing delivery records from authoritative trade history
    /// and makes every undelivered row runnable before workers start.
    pub(crate) async fn reconcile(&self) -> anyhow::Result<usize> {
        let exhausted: i64 = sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status IN ('Failed', 'Killed')",
        )
        .bind(std::any::type_name::<DeliverDashboardTrade>())
        .fetch_one(self.queue.pool())
        .await
        .context("failed to count exhausted dashboard trade delivery jobs")?;

        if exhausted > 0 {
            warn!(
                target: "dashboard",
                exhausted,
                "Re-driving dashboard trade deliveries that previously exhausted their retry \
                 budget; a repeat of this warning across restarts indicates a poison delivery",
            );
        }

        let reset = sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Pending', attempts = 0, run_at = strftime('%s', 'now'), \
             last_result = NULL, \
             lock_at = NULL, lock_by = NULL, done_at = NULL \
             WHERE job_type = ? AND status IN ('Running', 'Queued', 'Failed', 'Killed')",
        )
        .bind(std::any::type_name::<DeliverDashboardTrade>())
        .execute(self.queue.pool())
        .await
        .context("failed to reset unfinished dashboard trade delivery jobs")?
        .rows_affected();

        let undelivered = self.enqueuer.reconcile_undelivered().await?;

        if reset > 0 || undelivered > 0 {
            info!(
                target: "dashboard",
                reset,
                undelivered,
                "Reconciled durable dashboard trade deliveries",
            );
        }

        self.handoff_monitor
            .skip_first_terminal_reconciliation
            .store(true, Ordering::SeqCst);

        Ok(undelivered)
    }
}

impl DashboardTradeEnqueuer {
    async fn reconcile_undelivered(&self) -> anyhow::Result<usize> {
        // Reconciliation must see every terminal trade, not the protocol
        // narrowing a client asked for, so it queries with the widest protocol.
        let trades = crate::dashboard::query_trades(
            &self.store.pool,
            &crate::dashboard::TradeQuery::all(crate::dashboard::TradeProtocol::TerminalOutcomesV3),
        )
        .await
        .context("failed to load terminal trades for delivery reconciliation")?
        .trades;
        let mut undelivered = 0;

        for trade in trades {
            self.store.register(&trade.id).await?;
            if self.store.is_delivered(&trade.id).await? {
                continue;
            }

            self.enqueue_reconciled(trade).await?;
            undelivered += 1;
        }

        Ok(undelivered)
    }
}

struct DashboardTradeEnqueuer {
    queue: DashboardTradeDeliveryJobQueue,
    store: Arc<DashboardTradeDeliveryStore>,
}

impl DashboardTradeEnqueuer {
    fn new(queue: DashboardTradeDeliveryJobQueue, store: Arc<DashboardTradeDeliveryStore>) -> Self {
        Self { queue, store }
    }

    async fn enqueue(&self, trade: Trade) -> Result<(), DashboardTradePersistenceError> {
        self.store.register(&trade.id).await?;
        self.enqueue_registered(trade).await
    }

    async fn enqueue_registered(&self, trade: Trade) -> Result<(), DashboardTradePersistenceError> {
        let idempotency_key = trade.id.clone();
        let mut queue = self.queue.clone();
        queue
            .push_idempotent(&idempotency_key, DeliverDashboardTrade::new(trade))
            .await?;

        Ok(())
    }

    async fn enqueue_reconciled(&self, trade: Trade) -> Result<(), DashboardTradePersistenceError> {
        let idempotency_key = trade.id.clone();
        let payload = serde_json::to_vec(&DeliverDashboardTrade::new(trade.clone()))?;
        self.enqueue_registered(trade).await?;

        // The delivery ledger is authoritative and this refresh only runs for
        // undelivered trades, so it must also reclaim a `Done` job row: a
        // crash between publishing and `mark_delivered` leaves the job `Done`
        // while the ledger still shows undelivered, and the idempotency-key
        // unique index blocks inserting a replacement row.
        sqlx_apalis::query(
            "UPDATE Jobs SET job = ?, status = 'Pending', attempts = 0, \
             run_at = strftime('%s', 'now'), last_result = NULL, \
             lock_at = NULL, lock_by = NULL, done_at = NULL \
             WHERE job_type = ? AND idempotency_key = ?",
        )
        .bind(payload)
        .bind(std::any::type_name::<DeliverDashboardTrade>())
        .bind(idempotency_key)
        .execute(self.queue.pool())
        .await?;

        Ok(())
    }
}

/// Retries terminal event-to-job handoffs that fail after the CQRS event has
/// committed, without waiting for a process restart.
#[derive(Clone, Debug)]
pub(crate) enum DashboardTradeHandoff {
    Trade(Box<Trade>),
    ReloadOffchainOrder(OffchainOrderId),
    ReloadOnchainTradeRevision(OnChainTradeId),
}

struct ScheduledDashboardTradeHandoff {
    handoff: DashboardTradeHandoff,
    next_attempt: Instant,
    retry_delay: Duration,
    attempt: usize,
}

impl ScheduledDashboardTradeHandoff {
    fn new(handoff: DashboardTradeHandoff) -> Self {
        Self {
            handoff,
            next_attempt: Instant::now(),
            retry_delay: HANDOFF_RETRY_INITIAL_DELAY,
            attempt: 1,
        }
    }

    fn reschedule(mut self) -> Self {
        self.next_attempt = Instant::now() + self.retry_delay;
        self.retry_delay = self
            .retry_delay
            .saturating_mul(2)
            .min(HANDOFF_RETRY_MAX_DELAY);
        self.attempt = self.attempt.saturating_add(1);
        self
    }
}

struct LoadedOnchainTrade {
    trade: Trade,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OnchainTradeReplayError {
    #[error("failed to replay onchain trade {id}: {source}")]
    Replay {
        id: OnChainTradeId,
        #[source]
        source: Box<SendError<OnChainTrade>>,
    },
    #[error("onchain trade {id} cannot be represented: {source}")]
    Conversion {
        id: OnChainTradeId,
        #[source]
        source: NotPositive<FractionalShares>,
    },
}

async fn load_authoritative_onchain_trade(
    pool: &SqlitePool,
    id: &OnChainTradeId,
) -> Result<Option<LoadedOnchainTrade>, OnchainTradeReplayError> {
    let Some(entity) = load_entity::<OnChainTrade>(pool, id)
        .await
        .map_err(|source| OnchainTradeReplayError::Replay {
            id: id.clone(),
            source: Box::new(source),
        })?
    else {
        return Ok(None);
    };
    let trade =
        entity
            .try_into_trade(id)
            .map_err(|source| OnchainTradeReplayError::Conversion {
                id: id.clone(),
                source,
            })?;

    Ok(Some(LoadedOnchainTrade { trade }))
}

#[derive(Clone)]
pub(crate) struct DashboardTradeHandoffMonitor {
    receiver: Arc<Mutex<mpsc::Receiver<DashboardTradeHandoff>>>,
    enqueuer: Arc<DashboardTradeEnqueuer>,
    pool: SqlitePool,
    sender: broadcast::Sender<Statement>,
    revision_reload_fault: OnchainRevisionReloadFault,
    revision_tracker: OnchainRevisionTracker,
    publish_lock: Arc<Mutex<()>>,
    skip_first_terminal_reconciliation: Arc<AtomicBool>,
    reconciliation_interval: Duration,
    #[cfg(test)]
    exhaustion_notify: Arc<Notify>,
    #[cfg(test)]
    startup_notify: Arc<Notify>,
}

impl DashboardTradeHandoffMonitor {
    fn new(
        receiver: mpsc::Receiver<DashboardTradeHandoff>,
        enqueuer: Arc<DashboardTradeEnqueuer>,
        pool: SqlitePool,
        sender: broadcast::Sender<Statement>,
        revision_reload_fault: OnchainRevisionReloadFault,
        revision_tracker: OnchainRevisionTracker,
        publish_lock: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            receiver: Arc::new(Mutex::new(receiver)),
            enqueuer,
            pool,
            sender,
            revision_reload_fault,
            revision_tracker,
            publish_lock,
            skip_first_terminal_reconciliation: Arc::new(AtomicBool::new(false)),
            reconciliation_interval: HANDOFF_RECONCILIATION_INTERVAL,
            #[cfg(test)]
            exhaustion_notify: Arc::new(Notify::new()),
            #[cfg(test)]
            startup_notify: Arc::new(Notify::new()),
        }
    }

    #[cfg(test)]
    fn with_reconciliation_interval(mut self, reconciliation_interval: Duration) -> Self {
        self.reconciliation_interval = reconciliation_interval;
        self
    }

    #[cfg(test)]
    fn exhaustion_notification(&self) -> Arc<Notify> {
        self.exhaustion_notify.clone()
    }

    #[cfg(test)]
    fn startup_notification(&self) -> Arc<Notify> {
        self.startup_notify.clone()
    }

    async fn receive(&self) -> Option<DashboardTradeHandoff> {
        self.receiver.lock().await.recv().await
    }

    async fn persist_once(
        &self,
        handoff: &DashboardTradeHandoff,
    ) -> Result<(), DashboardTradeHandoffAttemptError> {
        match handoff {
            DashboardTradeHandoff::Trade(trade) => {
                self.enqueuer.enqueue(trade.as_ref().clone()).await?;
            }
            DashboardTradeHandoff::ReloadOffchainOrder(id) => {
                let order = load_entity::<OffchainOrder>(&self.pool, id)
                    .await
                    .map_err(|source| DashboardTradeHandoffAttemptError::Replay {
                        id: *id,
                        source,
                    })?
                    .ok_or(DashboardTradeHandoffAttemptError::Missing { id: *id })?;

                let trade = order.try_into_trade(id).map_err(|source| {
                    DashboardTradeHandoffAttemptError::Conversion { id: *id, source }
                })?;

                self.enqueuer.enqueue(trade).await?;
            }
            DashboardTradeHandoff::ReloadOnchainTradeRevision(id) => {
                self.revision_tracker.track(id.clone()).await;
                self.broadcast_onchain_trade_revision(id).await?;
                self.revision_tracker.resolve(id).await;
            }
        }

        Ok(())
    }

    async fn broadcast_onchain_trade_revision(
        &self,
        id: &OnChainTradeId,
    ) -> Result<(), DashboardTradeHandoffAttemptError> {
        if self.revision_reload_fault.should_fail() {
            #[cfg(test)]
            return Err(DashboardTradeHandoffAttemptError::InjectedOnchainReplay {
                id: id.clone(),
            });
        }

        let _publish_guard = self.publish_lock.lock().await;
        let Some(loaded) = load_authoritative_onchain_trade(&self.pool, id)
            .await
            .map_err(|source| DashboardTradeHandoffAttemptError::Onchain(Box::new(source)))?
        else {
            warn!(
                target: "dashboard",
                %id,
                "Source-attributed onchain trade replayed to empty state"
            );
            return Ok(());
        };
        if let Err(error) = self.sender.send(Statement::TradeUpdate(loaded.trade)) {
            debug!(
                target: "dashboard",
                %id,
                %error,
                "No dashboard receivers for corrected onchain trade"
            );
        }

        Ok(())
    }

    async fn attempt(
        &self,
        scheduled: ScheduledDashboardTradeHandoff,
        pending: &mut VecDeque<ScheduledDashboardTradeHandoff>,
    ) -> Result<bool, DashboardTradeHandoffMonitorError> {
        let mut exhausted = false;
        if let Err(error) = self.persist_once(&scheduled.handoff).await {
            warn!(
                target: "dashboard",
                handoff = ?scheduled.handoff,
                ?error,
                attempt = scheduled.attempt,
                retry_delay_ms = scheduled.retry_delay.as_millis(),
                "Dashboard trade handoff attempt failed",
            );
            if error.is_retryable() {
                if scheduled.attempt < HANDOFF_RETRY_MAX_ATTEMPTS {
                    pending.push_back(scheduled.reschedule());
                } else if let DashboardTradeHandoff::ReloadOnchainTradeRevision(id) =
                    &scheduled.handoff
                {
                    return Err(DashboardTradeHandoffMonitorError::RevisionRetryExhausted {
                        id: id.clone(),
                    });
                } else {
                    exhausted = true;
                    #[cfg(test)]
                    self.exhaustion_notify.notify_one();
                    warn!(
                        target: "dashboard",
                        handoff = ?scheduled.handoff,
                        attempts = scheduled.attempt,
                        "Dashboard trade handoff exhausted immediate retries; periodic authoritative reconciliation will recover it",
                    );
                }
            } else {
                return Err(DashboardTradeHandoffMonitorError::DeterministicConversion(
                    Box::new(error),
                ));
            }
        }

        Ok(exhausted)
    }

    fn take_ready(
        pending: &mut VecDeque<ScheduledDashboardTradeHandoff>,
    ) -> Option<ScheduledDashboardTradeHandoff> {
        let now = Instant::now();
        let index = pending
            .iter()
            .position(|scheduled| scheduled.next_attempt <= now)?;
        pending.remove(index)
    }

    fn next_attempt(pending: &VecDeque<ScheduledDashboardTradeHandoff>) -> Option<Instant> {
        pending.iter().map(|scheduled| scheduled.next_attempt).min()
    }

    /// A reconciliation pass runs many SQLite statements, so write contention
    /// alone can fail it. Absorb that failure and leave `needed` set so the
    /// next tick retries: terminating the monitor would discard the in-memory
    /// pending queue and leave those handoffs waiting on the very pass that
    /// just failed.
    async fn reconcile_after_exhaustion(&self, needed: &mut bool) {
        match self.enqueuer.reconcile_undelivered().await {
            Ok(_) => *needed = false,
            Err(error) => warn!(
                target: "dashboard",
                ?error,
                "Dashboard trade handoff reconciliation failed; retrying on the next tick",
            ),
        }
    }

    async fn reconcile_pending_onchain_revisions(
        &self,
    ) -> Result<(), DashboardTradeHandoffMonitorError> {
        for id in self.revision_tracker.pending().await {
            self.broadcast_onchain_trade_revision(&id)
                .await
                .map_err(|source| {
                    DashboardTradeHandoffMonitorError::RevisionReconciliation(Box::new(source))
                })?;
            self.revision_tracker.resolve(&id).await;
        }

        Ok(())
    }
}

impl SupervisedTask for DashboardTradeHandoffMonitor {
    async fn run(&mut self) -> TaskResult {
        info!(target: "dashboard", "Dashboard trade handoff monitor started");
        if !self
            .skip_first_terminal_reconciliation
            .swap(false, Ordering::SeqCst)
        {
            self.enqueuer
                .reconcile_undelivered()
                .await
                .map_err(DashboardTradeHandoffMonitorError::TerminalReconciliation)?;
        }
        self.reconcile_pending_onchain_revisions().await?;
        #[cfg(test)]
        self.startup_notify.notify_one();
        let mut pending = VecDeque::with_capacity(HANDOFF_RETRY_QUEUE_CAPACITY);
        let mut reconciliation = interval(self.reconciliation_interval);
        reconciliation.tick().await;
        let mut reconciliation_needed = false;

        loop {
            if let Some(scheduled) = Self::take_ready(&mut pending) {
                if self.attempt(scheduled, &mut pending).await? {
                    if !reconciliation_needed {
                        reconciliation.reset();
                    }
                    reconciliation_needed = true;
                }
                continue;
            }

            // At least one arm is always live: an empty queue enables the
            // receive arm, and a full queue implies a scheduled retry, which
            // enables the sleep arm.
            let next_attempt = Self::next_attempt(&pending);
            tokio::select! {
                handoff = self.receive(), if pending.len() < HANDOFF_RETRY_QUEUE_CAPACITY => {
                    let handoff = handoff.ok_or(DashboardTradeHandoffMonitorError::QueueClosed)?;
                    pending.push_back(ScheduledDashboardTradeHandoff::new(handoff));
                }
                () = sleep_until(next_attempt.unwrap_or_else(Instant::now)), if next_attempt.is_some() => {}
                _ = reconciliation.tick(), if reconciliation_needed => {
                    self.reconcile_after_exhaustion(&mut reconciliation_needed).await;
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum DashboardTradeHandoffMonitorError {
    #[error("dashboard trade handoff retry queue closed unexpectedly")]
    QueueClosed,
    #[error("dashboard trade cannot be represented for durable delivery: {0}")]
    DeterministicConversion(#[source] Box<DashboardTradeHandoffAttemptError>),
    #[error("source-attribution broadcast retries exhausted for onchain trade {id}")]
    RevisionRetryExhausted { id: OnChainTradeId },
    #[error("failed to reconcile terminal dashboard trade deliveries: {0}")]
    TerminalReconciliation(#[source] anyhow::Error),
    #[error("failed to reconcile a source-attributed onchain trade broadcast: {0}")]
    RevisionReconciliation(#[source] Box<DashboardTradeHandoffAttemptError>),
}

#[derive(Debug, thiserror::Error)]
enum DashboardTradeHandoffAttemptError {
    #[error(transparent)]
    Persistence(#[from] DashboardTradePersistenceError),
    #[error("failed to replay terminal offchain order {id}: {source}")]
    Replay {
        id: OffchainOrderId,
        #[source]
        source: SendError<OffchainOrder>,
    },
    #[error("failed to load source-attributed onchain trade: {0}")]
    Onchain(#[source] Box<OnchainTradeReplayError>),
    #[cfg(test)]
    #[error("injected source-attributed onchain trade replay failure for {id}")]
    InjectedOnchainReplay { id: OnChainTradeId },
    #[error("terminal offchain order {id} replayed to empty state")]
    Missing { id: OffchainOrderId },
    #[error("terminal offchain order {id} cannot be represented for delivery: {source}")]
    Conversion {
        id: OffchainOrderId,
        #[source]
        source: TradeConversionError,
    },
}

impl DashboardTradeHandoffAttemptError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::Persistence(_) | Self::Replay { .. } | Self::Missing { .. } => true,
            Self::Onchain(source) => match source.as_ref() {
                OnchainTradeReplayError::Replay { source, .. } => match source.as_ref() {
                    AggregateError::DatabaseConnectionError(inner) => {
                        is_retryable_onchain_replay_database_error(inner.as_ref())
                    }
                    AggregateError::UserError(_)
                    | AggregateError::AggregateConflict
                    | AggregateError::DeserializationError(_)
                    | AggregateError::UnexpectedError(_) => false,
                },
                OnchainTradeReplayError::Conversion { .. } => false,
            },
            #[cfg(test)]
            Self::InjectedOnchainReplay { .. } => true,
            // Exhaustive: a new conversion failure must force a deliberate
            // retry-or-fail-stop decision here. A catch-all would silently
            // classify it as fail-stop, which terminates the monitor.
            Self::Conversion { source, .. } => match source {
                // Non-terminal states convert as soon as the order reaches a
                // terminal outcome.
                TradeConversionError::Pending
                | TradeConversionError::Submitted
                | TradeConversionError::PartiallyFilled
                | TradeConversionError::Cancelling => true,
                // Corrupt persisted quantities never become convertible.
                TradeConversionError::Arithmetic(_) | TradeConversionError::NegativeShares(_) => {
                    false
                }
            },
        }
    }
}

fn is_retryable_onchain_replay_database_error(error: &(dyn std::error::Error + 'static)) -> bool {
    if is_retryable_sqlite_busy(error) {
        return true;
    }

    let mut current = Some(error);
    while let Some(source) = current {
        if let Some(sqlx_error) = source.downcast_ref::<sqlx::Error>()
            && matches!(
                sqlx_error,
                sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut | sqlx::Error::WorkerCrashed
            )
        {
            return true;
        }
        current = source.source();
    }

    false
}

/// Persistent delivery job for one terminal dashboard trade outcome.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct DeliverDashboardTrade {
    trade: Trade,
}

impl DeliverDashboardTrade {
    fn new(trade: Trade) -> Self {
        Self { trade }
    }
}

struct DashboardTradeDeliveryStore {
    pool: SqlitePool,
    #[cfg(test)]
    completion_failures_remaining: AtomicUsize,
    #[cfg(test)]
    registration_failures_remaining: AtomicUsize,
}

impl DashboardTradeDeliveryStore {
    fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            #[cfg(test)]
            completion_failures_remaining: AtomicUsize::new(0),
            #[cfg(test)]
            registration_failures_remaining: AtomicUsize::new(0),
        }
    }

    async fn register(&self, trade_id: &str) -> Result<(), DashboardTradeDeliveryError> {
        #[cfg(test)]
        if consume_failure(&self.registration_failures_remaining) {
            return Err(DashboardTradeDeliveryError::Injected);
        }

        sqlx::query(
            "INSERT INTO dashboard_trade_delivery (trade_id) VALUES (?) \
             ON CONFLICT(trade_id) DO NOTHING",
        )
        .bind(trade_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn is_delivered(&self, trade_id: &str) -> Result<bool, DashboardTradeDeliveryError> {
        let delivered: Option<bool> = sqlx::query_scalar(
            "SELECT delivered_at IS NOT NULL FROM dashboard_trade_delivery WHERE trade_id = ?",
        )
        .bind(trade_id)
        .fetch_optional(&self.pool)
        .await?;

        delivered.ok_or_else(|| DashboardTradeDeliveryError::MissingRecord {
            trade_id: trade_id.to_owned(),
        })
    }

    async fn mark_delivered(&self, trade_id: &str) -> Result<(), DashboardTradeDeliveryError> {
        #[cfg(test)]
        if consume_failure(&self.completion_failures_remaining) {
            return Err(DashboardTradeDeliveryError::Injected);
        }

        sqlx::query("UPDATE dashboard_trade_delivery SET delivered_at = ? WHERE trade_id = ?")
            .bind(chrono::Utc::now())
            .bind(trade_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    #[cfg(test)]
    fn fail_next_completion(&self, count: usize) {
        self.completion_failures_remaining
            .store(count, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn fail_next_registration(&self, count: usize) {
        self.registration_failures_remaining
            .store(count, Ordering::SeqCst);
    }
}

#[cfg(test)]
fn consume_failure(failures_remaining: &AtomicUsize) -> bool {
    failures_remaining
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
            remaining.checked_sub(1)
        })
        .is_ok()
}

/// Shared live-stream publisher used by the dashboard delivery worker.
pub(crate) struct DashboardTradeDeliveryCtx {
    sender: broadcast::Sender<Statement>,
    store: Arc<DashboardTradeDeliveryStore>,
    pool: SqlitePool,
    publish_lock: Arc<Mutex<()>>,
}

impl DashboardTradeDeliveryCtx {
    #[cfg(test)]
    pub(crate) fn new(sender: broadcast::Sender<Statement>, pool: SqlitePool) -> Self {
        Self::with_store(
            sender,
            Arc::new(DashboardTradeDeliveryStore::new(pool.clone())),
            pool,
            Arc::new(Mutex::new(())),
        )
    }

    fn with_store(
        sender: broadcast::Sender<Statement>,
        store: Arc<DashboardTradeDeliveryStore>,
        pool: SqlitePool,
        publish_lock: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            sender,
            store,
            pool,
            publish_lock,
        }
    }

    #[cfg(test)]
    fn fail_next(&self, count: usize) {
        self.store.fail_next_completion(count);
    }

    fn publish(&self, trade: Trade) {
        let legacy_fill = trade.legacy_fill();
        if let Err(error) = self.sender.send(Statement::TradeUpdate(trade)) {
            debug!(target: "dashboard", %error, "No dashboard receivers for trade update");
        }

        if let Some(legacy_fill) = legacy_fill
            && let Err(error) = self.sender.send(Statement::TradeFill(legacy_fill))
        {
            debug!(target: "dashboard", %error, "No dashboard receivers for legacy trade fill");
        }
    }

    async fn authoritative_trade(
        &self,
        trade: &Trade,
    ) -> Result<Trade, DashboardTradeDeliveryError> {
        if !trade.venue.is_onchain() {
            return Ok(trade.clone());
        }

        let id = OnChainTradeId::from_str(&trade.id)?;
        let loaded = load_authoritative_onchain_trade(&self.pool, &id)
            .await
            .map_err(|source| DashboardTradeDeliveryError::Onchain(Box::new(source)))?
            .ok_or_else(|| DashboardTradeDeliveryError::OnchainMissing { id: id.clone() })?;

        Ok(loaded.trade)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DashboardTradeDeliveryError {
    #[error("dashboard trade delivery database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("dashboard trade delivery record is missing for {trade_id}")]
    MissingRecord { trade_id: String },
    #[error("invalid onchain dashboard trade ID: {0}")]
    OnchainId(#[from] ParseOnChainTradeIdError),
    #[error("failed to load authoritative onchain dashboard trade: {0}")]
    Onchain(#[source] Box<OnchainTradeReplayError>),
    #[error("onchain dashboard trade {id} replayed to empty state")]
    OnchainMissing { id: OnChainTradeId },
    #[cfg(test)]
    #[error("injected dashboard trade delivery failure")]
    Injected,
}

impl Job<DashboardTradeDeliveryCtx> for DeliverDashboardTrade {
    type Output = ();
    type Error = DashboardTradeDeliveryError;

    const WORKER_NAME: &'static str = "dashboard-trade-delivery-worker";
    const PERFORM_TIMEOUT: Option<std::time::Duration> =
        Some(crate::conductor::job::DEFAULT_PERFORM_TIMEOUT);
    const TERMINAL_FAILURE_MSG: &'static str =
        "Dashboard trade delivery failed after retries; terminal update remains undelivered";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind =
        crate::conductor::job::JobKind::DashboardTradeDelivery;

    fn label(&self) -> Label {
        Label::new(format!("DeliverDashboardTrade:{}", self.trade.id))
    }

    async fn perform(&self, ctx: &DashboardTradeDeliveryCtx) -> Result<Self::Output, Self::Error> {
        let _publish_guard = ctx.publish_lock.lock().await;
        if ctx.store.is_delivered(&self.trade.id).await? {
            debug!(
                target: "dashboard",
                trade_id = %self.trade.id,
                "Skipping dashboard trade delivery; ledger already records completion",
            );
            return Ok(());
        }

        ctx.publish(ctx.authoritative_trade(&self.trade).await?);
        ctx.store.mark_delivered(&self.trade.id).await
    }
}

/// Reactor that broadcasts notifications and trade fills to connected
/// WebSocket clients.
pub(crate) struct Broadcaster {
    sender: broadcast::Sender<Statement>,
    pool: SqlitePool,
    trade_enqueuer: Arc<DashboardTradeEnqueuer>,
    handoff_retry_sender: mpsc::Sender<DashboardTradeHandoff>,
    revision_reload_fault: OnchainRevisionReloadFault,
    revision_tracker: OnchainRevisionTracker,
    publish_lock: Arc<Mutex<()>>,
}

impl Broadcaster {
    fn new(
        sender: broadcast::Sender<Statement>,
        pool: SqlitePool,
        trade_enqueuer: Arc<DashboardTradeEnqueuer>,
        handoff_retry_sender: mpsc::Sender<DashboardTradeHandoff>,
        revision_reload_fault: OnchainRevisionReloadFault,
        revision_tracker: OnchainRevisionTracker,
        publish_lock: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            sender,
            pool,
            trade_enqueuer,
            handoff_retry_sender,
            revision_reload_fault,
            revision_tracker,
            publish_lock,
        }
    }

    #[cfg(test)]
    fn fail_next_onchain_revision_load(&self, count: usize) {
        self.revision_reload_fault.fail_next(count);
    }

    async fn enqueue_trade(&self, trade: Trade) -> Result<(), DashboardTradeEnqueueError> {
        if let Err(error) = self.trade_enqueuer.enqueue(trade.clone()).await {
            warn!(
                target: "dashboard",
                trade_id = %trade.id,
                ?error,
                "Durable dashboard trade handoff failed; queued for in-process retry",
            );
            self.handoff_retry_sender
                .send(DashboardTradeHandoff::Trade(Box::new(trade)))
                .await?;
        }

        Ok(())
    }

    async fn enqueue_offchain_trade(
        &self,
        id: OffchainOrderId,
    ) -> Result<(), DashboardTradeEnqueueError> {
        match load_entity::<OffchainOrder>(&self.pool, &id).await {
            Ok(Some(order)) => match order.try_into_trade(&id) {
                Ok(trade) => return self.enqueue_trade(trade).await,
                Err(error) => warn!(
                    target: "dashboard",
                    %id, %error,
                    "Failed to convert terminal OffchainOrder; queued for reload retry"
                ),
            },
            Ok(None) => warn!(
                target: "dashboard",
                %id,
                "Terminal OffchainOrder replayed to empty state; queued for reload retry"
            ),
            Err(error) => warn!(
                target: "dashboard",
                %id, ?error,
                "Failed to load terminal OffchainOrder; queued for reload retry"
            ),
        }

        self.handoff_retry_sender
            .send(DashboardTradeHandoff::ReloadOffchainOrder(id))
            .await?;
        Ok(())
    }

    async fn broadcast_onchain_trade_revision(
        &self,
        id: OnChainTradeId,
    ) -> Result<(), DashboardTradeEnqueueError> {
        let replay = {
            let _publish_guard = self.publish_lock.lock().await;
            if self.revision_reload_fault.should_fail() {
                None
            } else {
                Some(
                    match load_authoritative_onchain_trade(&self.pool, &id).await {
                        Ok(Some(loaded)) => {
                            if let Err(error) =
                                self.sender.send(Statement::TradeUpdate(loaded.trade))
                            {
                                debug!(
                                    target: "dashboard",
                                    %id,
                                    %error,
                                    "No dashboard receivers for corrected onchain trade"
                                );
                            }
                            Ok(())
                        }
                        Ok(None) => {
                            warn!(
                                target: "dashboard",
                                %id,
                                "Source-attributed onchain trade replayed to empty state"
                            );
                            Ok(())
                        }
                        Err(error) => Err(error),
                    },
                )
            }
        };

        match replay {
            Some(Ok(())) => self.revision_tracker.resolve(&id).await,
            Some(Err(OnchainTradeReplayError::Replay { source, .. })) => {
                warn!(
                    target: "dashboard",
                    %id,
                    ?source,
                    "Failed to replay source-attributed onchain trade; queued for broadcast retry"
                );
                self.queue_onchain_trade_revision(id).await?;
            }
            Some(Err(OnchainTradeReplayError::Conversion { source, .. })) => {
                return Err(DashboardTradeEnqueueError::Quantity(source));
            }
            None => {
                self.queue_onchain_trade_revision(id).await?;
            }
        }

        Ok(())
    }

    async fn queue_onchain_trade_revision(
        &self,
        id: OnChainTradeId,
    ) -> Result<(), DashboardTradeEnqueueError> {
        self.revision_tracker.track(id.clone()).await;
        self.handoff_retry_sender
            .send(DashboardTradeHandoff::ReloadOnchainTradeRevision(id))
            .await?;
        Ok(())
    }

    fn broadcast_position(&self, position: st0x_dto::Position) {
        if let Err(error) = self.sender.send(Statement::PositionUpdate(position)) {
            debug!(target: "dashboard", %error, "Failed to broadcast position update (no receivers)");
        }
    }

    fn broadcast_transfer(&self, transfer: st0x_dto::TransferOperation) {
        if let Err(error) = self.sender.send(Statement::TransferUpdate(transfer)) {
            debug!(target: "dashboard", %error, "Failed to broadcast transfer update (no receivers)");
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum DashboardTradePersistenceError {
    #[error(transparent)]
    Delivery(#[from] DashboardTradeDeliveryError),
    #[error(transparent)]
    Queue(#[from] QueuePushError),
    #[error("failed to serialize dashboard trade delivery job: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("failed to refresh dashboard trade delivery job: {0}")]
    Refresh(#[from] apalis_sqlite::SqlxError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DashboardTradeEnqueueError {
    #[error("dashboard trade handoff retry queue closed before retaining the outcome: {0}")]
    RetryQueue(#[from] mpsc::error::SendError<DashboardTradeHandoff>),
    #[error("terminal trade quantity is not positive: {0}")]
    Quantity(#[from] NotPositive<FractionalShares>),
}

#[async_trait]
impl Reactor for Broadcaster {
    type Error = DashboardTradeEnqueueError;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|id, event| async move {
                match event {
                    OnChainTradeEvent::Filled {
                        source,
                        symbol,
                        amount,
                        direction,
                        block_timestamp,
                        ..
                    } => {
                        self.enqueue_trade(Trade {
                            id: id.to_string(),
                            occurred_at: block_timestamp,
                            venue: source.trading_venue(),
                            direction,
                            symbol,
                            shares: Positive::new(FractionalShares::new(amount))?,
                            outcome: TradeOutcome::Filled,
                        })
                        .await?;
                    }
                    OnChainTradeEvent::SourceAttributed { .. } => {
                        self.broadcast_onchain_trade_revision(id).await?;
                    }
                    OnChainTradeEvent::Enriched { .. }
                    | OnChainTradeEvent::Acknowledged { .. }
                    | OnChainTradeEvent::Reorged { .. } => {}
                }

                Ok(())
            })

            .on(|id, event| async move {
                if !matches!(
                    event,
                    PositionEvent::OnChainOrderFilled { .. }
                        | PositionEvent::OffChainOrderFilled { .. }
                        | PositionEvent::ManualPositionAdjusted { .. }
                        | PositionEvent::Reorged { .. }
                ) {
                    return Ok(());
                }

                match load_entity::<Position>(&self.pool, &id).await {
                    Ok(Some(position)) => {
                        self.broadcast_position(st0x_dto::Position {
                            symbol: position.symbol,
                            net: position.net.inner(),
                        });
                    }
                    Ok(None) => warn!(target: "dashboard", %id, "Position not found after event"),
                    Err(error) => warn!(target: "dashboard", %id, ?error, "Failed to load position for broadcast"),
                }

                Ok(())
            })

            .on(|id, event| async move {
                use OffchainOrderEvent::*;
                match event {
                    Filled { .. } | Failed { .. } | Cancelled { .. } => {
                        self.enqueue_offchain_trade(id).await?;
                    }
                    Placed { .. }
                    | Submitted { .. }
                    | Accepted { .. }
                    | PartiallyFilled { .. }
                    | CancelRequested { .. } => {}
                }

                Ok(())
            })

            .on(|id, _event| async move {
                match load_entity::<TokenizedEquityMint>(&self.pool, &id).await {
                    Ok(Some(entity)) => self.broadcast_transfer(entity.to_dto(&id)),
                    Ok(None) => warn!(target: "dashboard", %id, "Mint entity not found for transfer broadcast"),
                    Err(error) => warn!(target: "dashboard", %id, ?error, "Failed to load mint for broadcast"),
                }
                Ok(())
            })

            .on(|id, _event| async move {
                match load_entity::<EquityRedemption>(&self.pool, &id).await {
                    Ok(Some(entity)) => self.broadcast_transfer(entity.to_dto(&id)),
                    Ok(None) => warn!(target: "dashboard", %id, "Redemption entity not found for broadcast"),
                    Err(error) => warn!(target: "dashboard", %id, ?error, "Failed to load redemption for broadcast"),
                }
                Ok(())
            })

            .on(|id, _event| async move {
                match load_entity::<UsdcRebalance>(&self.pool, &id).await {
                    Ok(Some(entity)) => self.broadcast_transfer(entity.to_dto(&id)),
                    Ok(None) => warn!(target: "dashboard", %id, "USDC rebalance entity not found for broadcast"),
                    Err(error) => warn!(target: "dashboard", %id, ?error, "Failed to load rebalance for broadcast"),
                }
                Ok(())
            })
            .exhaustive()
            .await
    }
}

#[cfg(test)]
mod tests {
    use apalis::prelude::Monitor;
    use std::borrow::Cow;
    use std::fmt::{Display, Formatter};
    use std::sync::Arc;
    use std::time::Duration;

    use st0x_dto::TradingVenue;
    use st0x_event_sorcery::{LifecycleError, ReactorHarness, StoreBuilder};
    use st0x_evm::Chain;
    use st0x_execution::Symbol;

    use super::*;
    use crate::conductor::job::{
        FailureInjector, TerminalFailureSignal, build_supervised_worker, build_worker_inner,
    };
    use crate::dashboard::{TradeQuery, query_trades};
    use crate::offchain::order::{OffchainOrderCommand, OffchainOrderEvent};
    use crate::onchain_trade::{
        InventoryVenue, OnChainTradeCommand, OnChainTradeError, OnChainTradeSource,
    };
    use crate::position::{PositionCommand, PositionEvent, TradeId};
    use crate::test_utils::setup_test_pools;

    #[derive(Debug)]
    struct TestDatabaseError {
        code: &'static str,
    }

    impl Display for TestDatabaseError {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("test database error")
        }
    }

    impl std::error::Error for TestDatabaseError {}

    impl sqlx::error::DatabaseError for TestDatabaseError {
        fn message(&self) -> &'static str {
            "test database error"
        }

        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.code))
        }

        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    fn onchain_replay_error(source: SendError<OnChainTrade>) -> DashboardTradeHandoffAttemptError {
        DashboardTradeHandoffAttemptError::Onchain(Box::new(OnchainTradeReplayError::Replay {
            id: OnChainTradeId {
                chain: Chain::Base,
                tx_hash: alloy::primitives::TxHash::ZERO,
                log_index: 0,
            },
            source: Box::new(source),
        }))
    }

    fn test_broadcaster(
        pool: &SqlitePool,
        apalis_pool: &apalis_sqlite::SqlitePool,
    ) -> (
        Arc<Broadcaster>,
        broadcast::Receiver<Statement>,
        DashboardTradeDeliveryJobQueue,
        Arc<DashboardTradeDeliveryCtx>,
    ) {
        let (sender, receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(apalis_pool, pool, sender);
        (delivery.broadcaster, receiver, delivery.queue, delivery.ctx)
    }

    async fn perform_pending_delivery(
        queue: &DashboardTradeDeliveryJobQueue,
        ctx: &DashboardTradeDeliveryCtx,
    ) {
        let payload: Vec<u8> = sqlx_apalis::query_scalar(
            "SELECT job FROM Jobs WHERE status = 'Pending' AND job_type = ? LIMIT 1",
        )
        .bind(std::any::type_name::<DeliverDashboardTrade>())
        .fetch_one(queue.pool())
        .await
        .unwrap();
        let job: DeliverDashboardTrade = serde_json::from_slice(&payload).unwrap();
        job.perform(ctx).await.unwrap();
    }

    fn test_trade() -> Trade {
        Trade {
            id: "terminal-trade-1".to_string(),
            occurred_at: chrono::Utc::now(),
            venue: TradingVenue::Alpaca,
            direction: st0x_execution::Direction::Sell,
            symbol: Symbol::new("AAPL").unwrap(),
            shares: Positive::new(FractionalShares::new(st0x_float_macro::float!(1))).unwrap(),
            outcome: TradeOutcome::Filled,
        }
    }

    async fn persist_failed_offchain_order(
        pool: SqlitePool,
        id: OffchainOrderId,
        filled_shares: Option<st0x_execution::FractionalShares>,
    ) {
        let (store, _projection) = StoreBuilder::<OffchainOrder>::new(pool)
            .build(crate::offchain::order::noop_order_placer())
            .await
            .unwrap();
        let shares = st0x_execution::Positive::new(st0x_execution::FractionalShares::new(
            st0x_float_macro::float!(1),
        ))
        .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::Place {
                    symbol: Symbol::new("AAPL").unwrap(),
                    shares,
                    direction: st0x_execution::Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: st0x_execution::ClientOrderId::from_uuid(id.as_uuid()),
                    kind: crate::offchain::order::CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("broker-order"),
                    placed_shares: shares,
                    submitted_at: chrono::Utc::now(),
                    market_session: st0x_execution::MarketSession::Regular,
                    limit_price: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "broker unavailable".to_string(),
                    filled_shares,
                    failed_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
    }

    /// Rewrites the persisted terminal fill to a negative quantity, modelling
    /// history written before the aggregate rejected corrupt broker fills.
    /// `MarkFailed` refuses that value now, so replayed legacy rows are the
    /// only remaining source of a non-retryable conversion failure.
    async fn corrupt_persisted_terminal_fill(pool: &SqlitePool, id: OffchainOrderId) {
        let rows: Vec<(i64, String)> = sqlx::query_as(
            "SELECT sequence, payload FROM events \
             WHERE aggregate_id = ? AND payload LIKE '%filled_shares%'",
        )
        .bind(id.to_string())
        .fetch_all(pool)
        .await
        .unwrap();
        let [(sequence, payload)] = rows.as_slice() else {
            panic!("expected exactly one persisted event carrying a fill, got: {rows:?}");
        };

        let mut event: serde_json::Value = serde_json::from_str(payload).unwrap();
        assert!(
            replace_filled_shares(&mut event, "-0.5"),
            "the persisted terminal event must carry a filled_shares field: {payload}"
        );

        sqlx::query("UPDATE events SET payload = ? WHERE aggregate_id = ? AND sequence = ?")
            .bind(serde_json::to_string(&event).unwrap())
            .bind(id.to_string())
            .bind(sequence)
            .execute(pool)
            .await
            .unwrap();
    }

    fn replace_filled_shares(event: &mut serde_json::Value, shares: &str) -> bool {
        let serde_json::Value::Object(fields) = event else {
            return false;
        };

        if fields.contains_key("filled_shares") {
            fields.insert(
                "filled_shares".to_string(),
                serde_json::Value::String(shares.to_string()),
            );
            return true;
        }

        fields
            .values_mut()
            .any(|nested| replace_filled_shares(nested, shares))
    }

    async fn enqueue_test_delivery(
        queue: &mut DashboardTradeDeliveryJobQueue,
        ctx: &DashboardTradeDeliveryCtx,
        trade: Trade,
    ) {
        ctx.store.register(&trade.id).await.unwrap();
        let idempotency_key = trade.id.clone();
        queue
            .push_idempotent(&idempotency_key, DeliverDashboardTrade::new(trade))
            .await
            .unwrap();
    }

    fn spawn_delivery_worker(
        queue: DashboardTradeDeliveryJobQueue,
        ctx: Arc<DashboardTradeDeliveryCtx>,
        failure_notify: Arc<TerminalFailureSignal>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let monitor = Monitor::new()
                .should_restart(|_ctx, _error, _attempt| false)
                .register(move |index| {
                    build_supervised_worker!(
                        ::<DashboardTradeDeliveryCtx, DeliverDashboardTrade>,
                        index,
                        queue.clone(),
                        ctx.clone(),
                        failure_notify.clone(),
                        FailureInjector::new(),
                    )
                });
            let _ = monitor.run().await;
        })
    }

    #[tokio::test]
    async fn no_broadcast_without_events() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (sender, mut receiver) = broadcast::channel(16);
        let _delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);

        let result =
            tokio::time::timeout(std::time::Duration::from_millis(10), receiver.recv()).await;

        assert!(result.is_err(), "should timeout with no messages");
    }

    #[tokio::test]
    async fn terminal_trade_reactor_persists_delivery_job() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (sender, _receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);
        let harness = ReactorHarness::new(delivery.broadcaster);
        let now = chrono::Utc::now();

        harness
            .receive::<OnChainTrade>(
                crate::onchain_trade::OnChainTradeId {
                    chain: Chain::Base,
                    tx_hash: alloy::primitives::TxHash::ZERO,
                    log_index: 0,
                },
                OnChainTradeEvent::Filled {
                    source: OnChainTradeSource::Raindex,
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: st0x_float_macro::float!(10),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_number: 12345,
                    block_hash: None,
                    block_timestamp: now,
                    filled_at: now,
                },
            )
            .await
            .unwrap();

        let pending: i64 = sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(std::any::type_name::<DeliverDashboardTrade>())
        .fetch_one(delivery.queue.pool())
        .await
        .unwrap();

        assert_eq!(pending, 1, "terminal outcome must be durably enqueued");
    }

    #[tokio::test]
    async fn failed_reactor_handoff_retries_without_restart() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (sender, _receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);
        delivery.store.fail_next_registration(1);
        let harness = ReactorHarness::new(delivery.broadcaster.clone());
        let mut handoff_monitor = delivery.handoff_monitor.clone();
        let monitor = tokio::spawn(async move { handoff_monitor.run().await });
        let now = chrono::Utc::now();

        harness
            .receive::<OnChainTrade>(
                crate::onchain_trade::OnChainTradeId {
                    chain: Chain::Base,
                    tx_hash: alloy::primitives::TxHash::ZERO,
                    log_index: 9,
                },
                OnChainTradeEvent::Filled {
                    source: OnChainTradeSource::Raindex,
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: st0x_float_macro::float!(1),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_number: 12345,
                    block_hash: None,
                    block_timestamp: now,
                    filled_at: now,
                },
            )
            .await
            .expect("the failed durable handoff should be staged for retry");

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let pending: i64 = sqlx_apalis::query_scalar(
                    "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?",
                )
                .bind(std::any::type_name::<DeliverDashboardTrade>())
                .fetch_one(delivery.queue.pool())
                .await
                .unwrap();
                if pending == 1 {
                    break;
                }

                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the in-process handoff monitor should persist the delivery job");
        monitor.abort();
    }

    #[tokio::test]
    async fn failed_offchain_reload_retries_without_restart() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (sender, _receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);
        let harness = ReactorHarness::new(delivery.broadcaster.clone());
        let mut handoff_monitor = delivery.handoff_monitor.clone();
        let monitor = tokio::spawn(async move { handoff_monitor.run().await });
        let id = crate::offchain::order::OffchainOrderId::new();
        let now = chrono::Utc::now();

        harness
            .receive::<OffchainOrder>(
                id,
                OffchainOrderEvent::Failed {
                    error: "broker unavailable".to_string(),
                    filled_shares: None,
                    failed_at: now,
                },
            )
            .await
            .expect("a failed reload should be staged for retry");

        let (store, _projection) = StoreBuilder::<OffchainOrder>::new(pool)
            .build(crate::offchain::order::noop_order_placer())
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::Place {
                    symbol: Symbol::new("AAPL").unwrap(),
                    shares: st0x_execution::Positive::new(st0x_execution::FractionalShares::new(
                        st0x_float_macro::float!(1),
                    ))
                    .unwrap(),
                    direction: st0x_execution::Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: st0x_execution::ClientOrderId::from_uuid(id.as_uuid()),
                    kind: crate::offchain::order::CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkPlacementFailed {
                    error: "broker unavailable".to_string(),
                },
            )
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let pending: i64 = sqlx_apalis::query_scalar(
                    "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?",
                )
                .bind(std::any::type_name::<DeliverDashboardTrade>())
                .fetch_one(delivery.queue.pool())
                .await
                .unwrap();
                if pending == 1 {
                    break;
                }

                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the handoff monitor should reload and enqueue the terminal order");
        monitor.abort();
    }

    #[tokio::test]
    async fn exhausted_handoff_is_recovered_by_periodic_authoritative_reconciliation() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (sender, _receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);
        let mut handoff_monitor = delivery
            .handoff_monitor
            .clone()
            .with_reconciliation_interval(Duration::from_secs(1));
        let exhausted = handoff_monitor.exhaustion_notification();
        let monitor = tokio::spawn(async move { handoff_monitor.run().await });
        let id = OffchainOrderId::new();

        delivery
            .broadcaster
            .handoff_retry_sender
            .send(DashboardTradeHandoff::ReloadOffchainOrder(id))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(4), exhausted.notified())
            .await
            .expect("the missing handoff must exhaust its immediate retry budget");

        persist_failed_offchain_order(pool, id, None).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let pending: i64 = sqlx_apalis::query_scalar(
                    "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?",
                )
                .bind(std::any::type_name::<DeliverDashboardTrade>())
                .fetch_one(delivery.queue.pool())
                .await
                .unwrap();
                if pending == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the running monitor must recover exhausted work from authoritative history");
        monitor.abort();
    }

    #[tokio::test]
    async fn failed_reconciliation_pass_keeps_the_monitor_running() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (sender, _receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);
        let mut handoff_monitor = delivery
            .handoff_monitor
            .clone()
            .with_reconciliation_interval(Duration::from_millis(100));
        let exhausted = handoff_monitor.exhaustion_notification();
        let monitor = tokio::spawn(async move { handoff_monitor.run().await });

        delivery
            .broadcaster
            .handoff_retry_sender
            .send(DashboardTradeHandoff::ReloadOffchainOrder(
                OffchainOrderId::new(),
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(4), exhausted.notified())
            .await
            .expect("the missing handoff must exhaust its immediate retry budget");

        // Closing the pool makes every reconciliation pass fail. The monitor
        // must absorb those failures rather than terminate and hand its whole
        // pending queue back to the pass that is failing.
        pool.close().await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        assert!(
            !monitor.is_finished(),
            "a failing reconciliation pass must not terminate the handoff monitor"
        );
        monitor.abort();
    }

    #[tokio::test]
    async fn deterministic_trade_conversion_failure_stops_the_handoff_monitor() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let id = OffchainOrderId::new();
        persist_failed_offchain_order(
            pool.clone(),
            id,
            Some(st0x_execution::FractionalShares::new(
                st0x_float_macro::float!(0.5),
            )),
        )
        .await;
        corrupt_persisted_terminal_fill(&pool, id).await;
        let (sender, _receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);
        let mut handoff_monitor = delivery.handoff_monitor.clone();
        let monitor = tokio::spawn(async move { handoff_monitor.run().await });

        delivery
            .broadcaster
            .handoff_retry_sender
            .send(DashboardTradeHandoff::ReloadOffchainOrder(id))
            .await
            .unwrap();

        let error = tokio::time::timeout(Duration::from_secs(1), monitor)
            .await
            .expect("deterministic conversion must stop the monitor")
            .unwrap()
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<DashboardTradeHandoffMonitorError>(),
            Some(DashboardTradeHandoffMonitorError::DeterministicConversion(
                _
            ))
        ));
    }

    #[tokio::test]
    async fn poison_handoff_does_not_block_later_terminal_trade() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (sender, _receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);
        let mut handoff_monitor = delivery.handoff_monitor.clone();
        let monitor = tokio::spawn(async move { handoff_monitor.run().await });

        for _ in 0..HANDOFF_RETRY_QUEUE_CAPACITY {
            delivery
                .broadcaster
                .handoff_retry_sender
                .send(DashboardTradeHandoff::ReloadOffchainOrder(
                    OffchainOrderId::new(),
                ))
                .await
                .unwrap();
        }
        delivery
            .broadcaster
            .handoff_retry_sender
            .send(DashboardTradeHandoff::Trade(Box::new(test_trade())))
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let pending: i64 = sqlx_apalis::query_scalar(
                    "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?",
                )
                .bind(std::any::type_name::<DeliverDashboardTrade>())
                .fetch_one(delivery.queue.pool())
                .await
                .unwrap();
                if pending == 1 {
                    break;
                }

                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("poison handoffs must be bounded and must not block a later trade");
        monitor.abort();
    }

    #[tokio::test]
    async fn saturated_revision_retry_queue_does_not_hold_the_publish_lock() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (sender, _receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);
        for _ in 0..HANDOFF_RETRY_QUEUE_CAPACITY {
            delivery
                .broadcaster
                .handoff_retry_sender
                .send(DashboardTradeHandoff::ReloadOffchainOrder(
                    OffchainOrderId::new(),
                ))
                .await
                .unwrap();
        }

        let revision_id = OnChainTradeId {
            chain: Chain::Base,
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 198,
        };
        delivery.broadcaster.fail_next_onchain_revision_load(1);
        let broadcaster = delivery.broadcaster.clone();
        let revision_id_for_retry = revision_id.clone();
        let blocked_retry = tokio::spawn(async move {
            broadcaster
                .broadcast_onchain_trade_revision(revision_id_for_retry)
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if delivery
                    .broadcaster
                    .revision_tracker
                    .pending()
                    .await
                    .contains(&revision_id)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the source revision retry must reach the saturated queue");
        assert!(
            !blocked_retry.is_finished(),
            "the revision retry must still be blocked on queue capacity"
        );

        let trade = test_trade();
        delivery.store.register(&trade.id).await.unwrap();
        tokio::time::timeout(
            Duration::from_secs(1),
            DeliverDashboardTrade::new(trade).perform(&delivery.ctx),
        )
        .await
        .expect("a saturated revision retry must not block durable publication")
        .unwrap();

        blocked_retry.abort();
    }

    #[tokio::test]
    async fn startup_reconciliation_recovers_terminal_trade_without_job() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (store, _) = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let id = crate::onchain_trade::OnChainTradeId {
            chain: Chain::Base,
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 7,
        };
        let now = chrono::Utc::now();
        store
            .send(
                &id,
                crate::onchain_trade::OnChainTradeCommand::WitnessAt {
                    source: OnChainTradeSource::Raindex,
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: st0x_float_macro::float!(1),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_number: 12345,
                    block_hash: None,
                    block_timestamp: now,
                    filled_at: now,
                },
            )
            .await
            .unwrap();

        let (sender, _receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);

        assert_eq!(delivery.reconcile().await.unwrap(), 1);
        let pending: i64 = sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(std::any::type_name::<DeliverDashboardTrade>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        assert_eq!(pending, 1, "reconciliation must recreate the missing job");
    }

    #[tokio::test]
    async fn startup_reconciliation_reclaims_done_job_with_undelivered_ledger() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (store, _) = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let id = crate::onchain_trade::OnChainTradeId {
            chain: Chain::Base,
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 9,
        };
        let now = chrono::Utc::now();
        store
            .send(
                &id,
                crate::onchain_trade::OnChainTradeCommand::WitnessAt {
                    source: OnChainTradeSource::Raindex,
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: st0x_float_macro::float!(1),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_number: 12345,
                    block_hash: None,
                    block_timestamp: now,
                    filled_at: now,
                },
            )
            .await
            .unwrap();

        let (sender, _receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);
        assert_eq!(delivery.reconcile().await.unwrap(), 1);

        // Simulates the crash window between publishing and `mark_delivered`:
        // apalis records the job `Done` while the ledger row stays
        // undelivered. The idempotency-key unique index spans terminal rows,
        // so only the refresh UPDATE can make this trade runnable again.
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Done', done_at = strftime('%s', 'now') WHERE job_type = ?",
        )
        .bind(std::any::type_name::<DeliverDashboardTrade>())
        .execute(&apalis_pool)
        .await
        .unwrap();

        assert_eq!(delivery.reconcile().await.unwrap(), 1);
        let pending: i64 = sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(std::any::type_name::<DeliverDashboardTrade>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        assert_eq!(
            pending, 1,
            "a Done job whose ledger row is undelivered must be made runnable again"
        );
    }

    #[tokio::test]
    async fn startup_reconciliation_skips_delivered_trade() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (store, _) = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let id = crate::onchain_trade::OnChainTradeId {
            chain: Chain::Base,
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 8,
        };
        let now = chrono::Utc::now();
        store
            .send(
                &id,
                crate::onchain_trade::OnChainTradeCommand::WitnessAt {
                    source: OnChainTradeSource::Raindex,
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: st0x_float_macro::float!(1),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_number: 12345,
                    block_hash: None,
                    block_timestamp: now,
                    filled_at: now,
                },
            )
            .await
            .unwrap();

        let (sender, _receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);
        delivery.store.register(&id.to_string()).await.unwrap();
        delivery
            .store
            .mark_delivered(&id.to_string())
            .await
            .unwrap();

        assert_eq!(delivery.reconcile().await.unwrap(), 0);
        let pending: i64 = sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(std::any::type_name::<DeliverDashboardTrade>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        assert_eq!(pending, 0, "delivered history must not be enqueued again");
    }

    #[tokio::test]
    async fn startup_reconciliation_refreshes_stale_offchain_job_payload() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (store, _projection) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(crate::offchain::order::noop_order_placer())
            .await
            .unwrap();
        let id = crate::offchain::order::OffchainOrderId::new();
        store
            .send(
                &id,
                OffchainOrderCommand::Place {
                    symbol: Symbol::new("AAPL").unwrap(),
                    shares: st0x_execution::Positive::new(st0x_execution::FractionalShares::new(
                        st0x_float_macro::float!(1),
                    ))
                    .unwrap(),
                    direction: st0x_execution::Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: st0x_execution::ClientOrderId::from_uuid(id.as_uuid()),
                    kind: crate::offchain::order::CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkPlacementFailed {
                    error: "broker unavailable".to_string(),
                },
            )
            .await
            .unwrap();

        let (sender, _receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);
        let mut queue = delivery.queue.clone();
        let mut stale_trade = test_trade();
        stale_trade.id = id.to_string();
        enqueue_test_delivery(&mut queue, &delivery.ctx, stale_trade).await;
        assert_eq!(delivery.reconcile().await.unwrap(), 1);

        let payload: Vec<u8> = sqlx_apalis::query_scalar(
            "SELECT job FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(std::any::type_name::<DeliverDashboardTrade>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        let job: DeliverDashboardTrade = serde_json::from_slice(&payload).unwrap();
        assert!(matches!(
            job.trade.outcome,
            TradeOutcome::Failed { error, .. } if error == "broker unavailable"
        ));
    }

    #[tokio::test]
    async fn transient_delivery_failure_is_retried() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let mut queue = DashboardTradeDeliveryJobQueue::new(&apalis_pool);
        let (sender, mut receiver) = broadcast::channel(16);
        let ctx = Arc::new(DashboardTradeDeliveryCtx::new(sender, pool));
        enqueue_test_delivery(&mut queue, &ctx, test_trade()).await;
        ctx.fail_next(1);
        let monitor = spawn_delivery_worker(
            queue,
            ctx.clone(),
            Arc::new(TerminalFailureSignal::default()),
        );

        tokio::time::timeout(Duration::from_secs(10), async {
            while !ctx.store.is_delivered("terminal-trade-1").await.unwrap() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("delivery should retry and persist completion");
        monitor.abort();

        let statements: Vec<_> = std::iter::from_fn(|| receiver.try_recv().ok()).collect();
        assert_eq!(
            statements
                .iter()
                .filter(|statement| matches!(statement, Statement::TradeUpdate(_)))
                .count(),
            2,
            "the first publish must be replayed when completion persistence fails"
        );
    }

    #[tokio::test]
    async fn orphaned_delivery_replays_after_restart() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (sender, mut receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);
        let mut queue = delivery.queue.clone();
        enqueue_test_delivery(&mut queue, &delivery.ctx, test_trade()).await;
        sqlx_apalis::query("UPDATE Jobs SET status = 'Running' WHERE job_type = ?")
            .bind(std::any::type_name::<DeliverDashboardTrade>())
            .execute(&apalis_pool)
            .await
            .unwrap();

        delivery.reconcile().await.unwrap();

        let monitor = spawn_delivery_worker(
            delivery.queue,
            delivery.ctx,
            Arc::new(TerminalFailureSignal::default()),
        );
        let statement = tokio::time::timeout(Duration::from_secs(10), receiver.recv())
            .await
            .expect("restarted worker should replay the orphaned delivery")
            .expect("dashboard receiver should stay connected");
        monitor.abort();

        assert!(matches!(statement, Statement::TradeUpdate(_)));
    }

    #[tokio::test]
    async fn exhausted_delivery_is_redriven_once_by_startup_reconciliation() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (sender, _receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);
        let mut queue = delivery.queue.clone();
        enqueue_test_delivery(&mut queue, &delivery.ctx, test_trade()).await;
        sqlx_apalis::query("UPDATE Jobs SET status = 'Killed', attempts = 5 WHERE job_type = ?")
            .bind(std::any::type_name::<DeliverDashboardTrade>())
            .execute(&apalis_pool)
            .await
            .unwrap();

        delivery.reconcile().await.unwrap();
        delivery.reconcile().await.unwrap();

        let (jobs, pending, attempts): (i64, i64, i64) = sqlx_apalis::query_as(
            "SELECT COUNT(*), SUM(status = 'Pending'), SUM(attempts) FROM Jobs \
             WHERE job_type = ?",
        )
        .bind(std::any::type_name::<DeliverDashboardTrade>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();

        assert_eq!(
            jobs, 1,
            "reconciliation must not duplicate the delivery job"
        );
        assert_eq!(
            pending, 1,
            "the exhausted delivery must become runnable again"
        );
        assert_eq!(
            attempts, 0,
            "a restart must restore the delivery retry budget"
        );
    }

    #[tokio::test]
    async fn delivery_without_connected_receivers_succeeds() {
        let (pool, _apalis_pool) = setup_test_pools().await;
        let (sender, receiver) = broadcast::channel(16);
        drop(receiver);
        let ctx = DashboardTradeDeliveryCtx::new(sender, pool);
        let trade = test_trade();
        ctx.store.register(&trade.id).await.unwrap();

        DeliverDashboardTrade::new(trade)
            .perform(&ctx)
            .await
            .expect("snapshot recovery makes zero live receivers a successful delivery");
        assert!(ctx.store.is_delivered("terminal-trade-1").await.unwrap());
    }

    #[tokio::test]
    async fn exhausted_delivery_retries_notify_fail_stop_monitor() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let mut queue = DashboardTradeDeliveryJobQueue::new(&apalis_pool);
        let (sender, _receiver) = broadcast::channel(16);
        let ctx = Arc::new(DashboardTradeDeliveryCtx::new(sender, pool));
        enqueue_test_delivery(&mut queue, &ctx, test_trade()).await;
        ctx.fail_next(usize::MAX);
        let failure_notify = Arc::new(TerminalFailureSignal::default());
        let terminal_failure = failure_notify.notified();
        let monitor = spawn_delivery_worker(queue, ctx, failure_notify.clone());

        tokio::time::timeout(Duration::from_secs(15), terminal_failure)
            .await
            .expect("retry exhaustion should notify the conductor fail-stop path");
        monitor.abort();
    }

    #[tokio::test]
    async fn onchain_trade_filled_broadcasts_fill() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (broadcaster, mut receiver, queue, delivery_ctx) =
            test_broadcaster(&pool, &apalis_pool);
        let (store, _) = StoreBuilder::<OnChainTrade>::new(pool)
            .with(broadcaster)
            .build(())
            .await
            .unwrap();

        let now = chrono::Utc::now();
        let ingested_at = now + chrono::Duration::seconds(1);
        let id = crate::onchain_trade::OnChainTradeId {
            chain: Chain::Base,
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 0,
        };

        store
            .send(
                &id,
                OnChainTradeCommand::WitnessAt {
                    source: OnChainTradeSource::Raindex,
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: st0x_float_macro::float!(10),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_number: 12345,
                    block_hash: None,
                    block_timestamp: now,
                    filled_at: ingested_at,
                },
            )
            .await
            .unwrap();

        perform_pending_delivery(&queue, &delivery_ctx).await;

        let msg = receiver.recv().await.expect("should receive fill");

        match msg {
            Statement::TradeUpdate(trade) => {
                assert!(matches!(trade.venue, TradingVenue::Raindex));
                assert!(matches!(trade.direction, st0x_dto::Direction::Buy));
                assert_eq!(trade.symbol, Symbol::new("AAPL").unwrap());
                assert_eq!(trade.occurred_at, now);
            }
            other => panic!("expected TradeUpdate message, got {other:?}"),
        }

        let legacy = receiver.recv().await.expect("should receive legacy fill");
        let legacy = serde_json::to_value(legacy).expect("legacy fill should serialize");
        assert_eq!(legacy["type"], "trade_fill");
        assert_eq!(
            legacy["data"]["filledAt"],
            serde_json::to_value(now).expect("timestamp should serialize")
        );
        assert!(legacy["data"].get("outcome").is_none());
    }

    #[tokio::test]
    async fn bebop_inventory_fill_broadcasts_bebop_venue() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (broadcaster, mut receiver, queue, delivery_ctx) =
            test_broadcaster(&pool, &apalis_pool);
        let (store, _) = StoreBuilder::<OnChainTrade>::new(pool)
            .with(broadcaster)
            .build(())
            .await
            .unwrap();
        let now = chrono::Utc::now();
        let id = OnChainTradeId {
            chain: Chain::Base,
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 194,
        };

        store
            .send(
                &id,
                OnChainTradeCommand::WitnessAt {
                    source: OnChainTradeSource::Inventory {
                        operator: alloy::primitives::address!(
                            "0x8b8b6e0507c125934c6129563f48e48c66f86475"
                        ),
                        venue: InventoryVenue::Bebop,
                    },
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: st0x_float_macro::float!(10),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_number: 12345,
                    block_hash: None,
                    block_timestamp: now,
                    filled_at: now,
                },
            )
            .await
            .unwrap();

        perform_pending_delivery(&queue, &delivery_ctx).await;

        let msg = receiver.recv().await.expect("should receive fill");
        let Statement::TradeUpdate(trade) = msg else {
            panic!("expected TradeUpdate message");
        };
        assert_eq!(trade.venue, TradingVenue::Bebop);
    }

    #[tokio::test]
    async fn pending_onchain_delivery_cannot_overwrite_a_source_correction() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (broadcaster, mut receiver, queue, delivery_ctx) =
            test_broadcaster(&pool, &apalis_pool);
        let (store, _) = StoreBuilder::<OnChainTrade>::new(pool)
            .with(broadcaster)
            .build(())
            .await
            .unwrap();
        let id = OnChainTradeId {
            chain: Chain::Base,
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 195,
        };
        let now = chrono::Utc::now();
        store
            .send(
                &id,
                OnChainTradeCommand::WitnessAt {
                    source: OnChainTradeSource::Legacy,
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: st0x_float_macro::float!(10),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_number: 12345,
                    block_hash: None,
                    block_timestamp: now,
                    filled_at: now,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OnChainTradeCommand::AttributeSource {
                    source: OnChainTradeSource::Inventory {
                        operator: alloy::primitives::Address::repeat_byte(0x8b),
                        venue: InventoryVenue::Bebop,
                    },
                },
            )
            .await
            .unwrap();

        perform_pending_delivery(&queue, &delivery_ctx).await;

        let updates = std::iter::from_fn(|| receiver.try_recv().ok())
            .filter_map(|statement| match statement {
                Statement::TradeUpdate(trade) => Some(trade),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(updates.len(), 2);
        assert!(
            updates
                .iter()
                .all(|trade| trade.venue == TradingVenue::Bebop),
            "the direct correction and delayed durable delivery must both use the authoritative venue"
        );
    }

    #[tokio::test]
    async fn source_attribution_broadcasts_corrected_venue() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (broadcaster, mut receiver, queue, delivery_ctx) =
            test_broadcaster(&pool, &apalis_pool);
        let (store, _) = StoreBuilder::<OnChainTrade>::new(pool)
            .with(broadcaster)
            .build(())
            .await
            .unwrap();
        let id = OnChainTradeId {
            chain: Chain::Base,
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 194,
        };
        let now = chrono::Utc::now();

        store
            .send(
                &id,
                OnChainTradeCommand::WitnessAt {
                    source: OnChainTradeSource::Legacy,
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: st0x_float_macro::float!(10),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_number: 12345,
                    block_hash: None,
                    block_timestamp: now,
                    filled_at: now,
                },
            )
            .await
            .unwrap();
        perform_pending_delivery(&queue, &delivery_ctx).await;
        let _legacy_update = receiver.recv().await.unwrap();
        let _legacy_fill = receiver.recv().await.unwrap();

        store
            .send(
                &id,
                OnChainTradeCommand::AttributeSource {
                    source: OnChainTradeSource::Inventory {
                        operator: alloy::primitives::Address::repeat_byte(0x8b),
                        venue: InventoryVenue::Bebop,
                    },
                },
            )
            .await
            .unwrap();

        let corrected = receiver.recv().await.expect("corrected trade update");
        let Statement::TradeUpdate(trade) = corrected else {
            panic!("expected corrected TradeUpdate");
        };
        assert_eq!(trade.id, id.to_string());
        assert_eq!(trade.venue, TradingVenue::Bebop);
    }

    #[tokio::test]
    async fn source_attribution_reload_failure_retries_corrected_broadcast() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (store, _) = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let id = OnChainTradeId {
            chain: Chain::Base,
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 195,
        };
        let now = chrono::Utc::now();
        let source = OnChainTradeSource::Inventory {
            operator: alloy::primitives::Address::repeat_byte(0x8b),
            venue: InventoryVenue::Bebop,
        };
        store
            .send(
                &id,
                OnChainTradeCommand::WitnessAt {
                    source: OnChainTradeSource::Legacy,
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: st0x_float_macro::float!(10),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_number: 12345,
                    block_hash: None,
                    block_timestamp: now,
                    filled_at: now,
                },
            )
            .await
            .unwrap();
        store
            .send(&id, OnChainTradeCommand::AttributeSource { source })
            .await
            .unwrap();

        let (sender, mut receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);
        let broadcaster = delivery.broadcaster;
        let harness = ReactorHarness::new(broadcaster.clone());
        let mut handoff_monitor = delivery.handoff_monitor;
        let started = handoff_monitor.startup_notification();
        let monitor = tokio::spawn(async move { handoff_monitor.run().await });
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("the handoff monitor must finish startup reconciliation");
        broadcaster.fail_next_onchain_revision_load(1);
        harness
            .receive::<OnChainTrade>(
                id.clone(),
                OnChainTradeEvent::SourceAttributed {
                    source,
                    attributed_at: now,
                },
            )
            .await
            .expect("the failed reload should be queued for retry");

        let corrected = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("the corrected venue should be retried")
            .unwrap();
        let Statement::TradeUpdate(trade) = corrected else {
            panic!("expected corrected TradeUpdate");
        };
        assert_eq!(trade.id, id.to_string());
        assert_eq!(trade.venue, TradingVenue::Bebop);
        monitor.abort();
    }

    #[tokio::test]
    async fn source_attribution_reconciliation_runs_after_monitor_restart() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (store, _) = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let id = OnChainTradeId {
            chain: Chain::Base,
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 196,
        };
        let now = chrono::Utc::now();
        store
            .send(
                &id,
                OnChainTradeCommand::WitnessAt {
                    source: OnChainTradeSource::Legacy,
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: st0x_float_macro::float!(10),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_number: 12345,
                    block_hash: None,
                    block_timestamp: now,
                    filled_at: now,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OnChainTradeCommand::AttributeSource {
                    source: OnChainTradeSource::Inventory {
                        operator: alloy::primitives::Address::repeat_byte(0x8b),
                        venue: InventoryVenue::Bebop,
                    },
                },
            )
            .await
            .unwrap();

        let (sender, mut receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);
        let broadcaster = delivery.broadcaster.clone();
        let harness = ReactorHarness::new(broadcaster.clone());
        let mut first_monitor = delivery.handoff_monitor.clone();
        let started = first_monitor.startup_notification();
        let first = tokio::spawn(async move { first_monitor.run().await });
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("the first monitor must finish startup reconciliation");
        broadcaster.fail_next_onchain_revision_load(usize::MAX);
        harness
            .receive::<OnChainTrade>(
                id.clone(),
                OnChainTradeEvent::SourceAttributed {
                    source: OnChainTradeSource::Inventory {
                        operator: alloy::primitives::Address::repeat_byte(0x8b),
                        venue: InventoryVenue::Bebop,
                    },
                    attributed_at: now,
                },
            )
            .await
            .expect("the failed revision must be retained for monitor restart");
        let error = tokio::time::timeout(Duration::from_secs(5), first)
            .await
            .expect("the revision must exhaust its bounded retry budget")
            .unwrap()
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<DashboardTradeHandoffMonitorError>(),
            Some(DashboardTradeHandoffMonitorError::RevisionRetryExhausted { .. })
        ));

        broadcaster.fail_next_onchain_revision_load(0);
        let mut restarted_monitor = delivery.handoff_monitor.clone();
        let restarted = tokio::spawn(async move { restarted_monitor.run().await });
        let restarted_update = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("monitor restart must reconcile the retained source attribution")
            .unwrap();
        assert!(matches!(
            restarted_update,
            Statement::TradeUpdate(Trade {
                venue: TradingVenue::Bebop,
                ..
            })
        ));
        restarted.abort();
    }

    #[tokio::test]
    async fn revision_retry_exhaustion_recovers_a_dropped_terminal_handoff_after_restart() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (sender, _receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);
        let mut first_monitor = delivery.handoff_monitor.clone();
        let started = first_monitor.startup_notification();
        let first = tokio::spawn(async move { first_monitor.run().await });
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("the first monitor must finish startup reconciliation");

        let (store, _) = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let id = OnChainTradeId {
            chain: Chain::Base,
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 197,
        };
        let now = chrono::Utc::now();
        store
            .send(
                &id,
                OnChainTradeCommand::WitnessAt {
                    source: OnChainTradeSource::Raindex,
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: st0x_float_macro::float!(10),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_number: 12345,
                    block_hash: None,
                    block_timestamp: now,
                    filled_at: now,
                },
            )
            .await
            .unwrap();
        let trade = load_entity::<OnChainTrade>(&pool, &id)
            .await
            .unwrap()
            .unwrap()
            .try_into_trade(&id)
            .unwrap();

        delivery
            .broadcaster
            .fail_next_onchain_revision_load(usize::MAX);
        delivery.store.fail_next_registration(usize::MAX);
        delivery
            .broadcaster
            .handoff_retry_sender
            .send(DashboardTradeHandoff::ReloadOnchainTradeRevision(
                id.clone(),
            ))
            .await
            .unwrap();
        delivery
            .broadcaster
            .handoff_retry_sender
            .send(DashboardTradeHandoff::Trade(Box::new(trade)))
            .await
            .unwrap();

        let error = tokio::time::timeout(Duration::from_secs(5), first)
            .await
            .expect("a poison revision must exhaust instead of filling the intake forever")
            .unwrap()
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<DashboardTradeHandoffMonitorError>(),
            Some(DashboardTradeHandoffMonitorError::RevisionRetryExhausted { .. })
        ));

        delivery.broadcaster.fail_next_onchain_revision_load(0);
        delivery.store.fail_next_registration(0);
        let mut restarted_monitor = delivery.handoff_monitor.clone();
        let restarted = tokio::spawn(async move { restarted_monitor.run().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let pending: i64 = sqlx_apalis::query_scalar(
                    "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?",
                )
                .bind(std::any::type_name::<DeliverDashboardTrade>())
                .fetch_one(delivery.queue.pool())
                .await
                .unwrap();
                if pending == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("startup reconciliation must recover the dropped terminal handoff");
        restarted.abort();
    }

    #[test]
    fn onchain_replay_retry_classification_matches_aggregate_error_variants() {
        let busy = sqlx::Error::Database(Box::new(TestDatabaseError { code: "5" }));
        assert!(
            onchain_replay_error(AggregateError::DatabaseConnectionError(Box::new(busy)))
                .is_retryable()
        );
        assert!(
            onchain_replay_error(AggregateError::DatabaseConnectionError(Box::new(
                sqlx::Error::PoolTimedOut,
            )))
            .is_retryable()
        );

        let deterministic = [
            onchain_replay_error(AggregateError::UserError(LifecycleError::Apply(
                OnChainTradeError::NotFilled,
            ))),
            onchain_replay_error(AggregateError::AggregateConflict),
            onchain_replay_error(AggregateError::DeserializationError(Box::new(
                std::io::Error::other("invalid persisted event"),
            ))),
            onchain_replay_error(AggregateError::UnexpectedError(Box::new(
                std::io::Error::other("unexpected replay failure"),
            ))),
        ];

        assert!(deterministic.iter().all(|error| !error.is_retryable()));
    }

    #[tokio::test]
    async fn offchain_order_filled_broadcasts_fill() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (store, _projection) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(crate::offchain::order::noop_order_placer())
            .await
            .unwrap();
        let (broadcaster, mut receiver, queue, delivery_ctx) =
            test_broadcaster(&pool, &apalis_pool);
        let harness = ReactorHarness::new(broadcaster);

        let now = chrono::Utc::now();
        let id = crate::offchain::order::OffchainOrderId::new();

        store
            .send(
                &id,
                OffchainOrderCommand::Place {
                    symbol: Symbol::new("TSLA").unwrap(),
                    shares: st0x_execution::Positive::new(st0x_execution::FractionalShares::new(
                        st0x_float_macro::float!(5),
                    ))
                    .unwrap(),
                    direction: st0x_execution::Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: st0x_execution::ClientOrderId::from_uuid(id.as_uuid()),
                    kind: crate::offchain::order::CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("test"),
                    placed_shares: st0x_execution::Positive::new(
                        st0x_execution::FractionalShares::new(st0x_float_macro::float!(5)),
                    )
                    .unwrap(),
                    submitted_at: now,
                    market_session: st0x_execution::MarketSession::Regular,
                    limit_price: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::CompleteFill {
                    price: st0x_finance::Usd::new(st0x_float_macro::float!(245)),
                    filled_at: now,
                },
            )
            .await
            .unwrap();

        let filled = OffchainOrderEvent::Filled {
            price: st0x_finance::Usd::new(st0x_float_macro::float!(245)),
            filled_at: now,
        };

        harness.receive::<OffchainOrder>(id, filled).await.unwrap();

        perform_pending_delivery(&queue, &delivery_ctx).await;

        let msg = receiver.recv().await.expect("should receive fill");

        match msg {
            Statement::TradeUpdate(trade) => {
                assert!(matches!(trade.venue, TradingVenue::Alpaca));
                assert!(matches!(trade.direction, st0x_dto::Direction::Sell));
                assert_eq!(trade.symbol, Symbol::new("TSLA").unwrap());
            }
            other => panic!("expected TradeUpdate message, got {other:?}"),
        }

        let legacy = receiver.recv().await.expect("should receive legacy fill");
        let legacy = serde_json::to_value(legacy).expect("legacy fill should serialize");
        assert_eq!(legacy["type"], "trade_fill");
        assert_eq!(
            legacy["data"]["filledAt"],
            serde_json::to_value(now).expect("timestamp should serialize")
        );
        assert!(legacy["data"].get("outcome").is_none());
    }

    #[tokio::test]
    async fn offchain_order_failed_broadcasts_failure() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (store, _projection) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(crate::offchain::order::noop_order_placer())
            .await
            .unwrap();
        let (broadcaster, mut receiver, queue, delivery_ctx) =
            test_broadcaster(&pool, &apalis_pool);
        let harness = ReactorHarness::new(broadcaster);

        let id = crate::offchain::order::OffchainOrderId::new();
        let now = chrono::Utc::now();
        store
            .send(
                &id,
                OffchainOrderCommand::Place {
                    symbol: Symbol::new("SPCX").unwrap(),
                    shares: st0x_execution::Positive::new(st0x_execution::FractionalShares::new(
                        st0x_float_macro::float!(1),
                    ))
                    .unwrap(),
                    direction: st0x_execution::Direction::Buy,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: st0x_execution::ClientOrderId::from_uuid(id.as_uuid()),
                    kind: crate::offchain::order::CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("partial-failure"),
                    placed_shares: st0x_execution::Positive::new(
                        st0x_execution::FractionalShares::new(st0x_float_macro::float!(1)),
                    )
                    .unwrap(),
                    submitted_at: now,
                    market_session: st0x_execution::MarketSession::Regular,
                    limit_price: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: st0x_execution::FractionalShares::new(st0x_float_macro::float!(
                        0.25
                    )),
                    avg_price: st0x_finance::Usd::new(st0x_float_macro::float!(25)),
                    partially_filled_at: now,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkFailed {
                    error: "asset is not tradable".to_string(),
                    filled_shares: None,
                    failed_at: now,
                },
            )
            .await
            .unwrap();

        let failed = OffchainOrderEvent::Failed {
            error: "asset is not tradable".to_string(),
            filled_shares: None,
            failed_at: now,
        };
        harness.receive::<OffchainOrder>(id, failed).await.unwrap();

        perform_pending_delivery(&queue, &delivery_ctx).await;

        let message = receiver.recv().await.expect("should receive failure");
        match message {
            Statement::TradeUpdate(trade) => {
                let history = query_trades(
                    &pool,
                    &TradeQuery::newest(crate::dashboard::TradeProtocol::TerminalOutcomesV2),
                )
                .await
                .unwrap()
                .trades;
                assert_eq!(
                    &trade.outcome, &history[0].outcome,
                    "live and historical failure provenance must be identical"
                );
                match trade.outcome {
                    st0x_dto::TradeOutcome::Failed {
                        error,
                        accepted_shares,
                        filled_shares,
                        remaining_shares,
                        excess_shares,
                    } => {
                        assert_eq!(error, "asset is not tradable");
                        assert!(
                            accepted_shares
                                .unwrap()
                                .inner()
                                .inner()
                                .eq(st0x_float_macro::float!(1))
                                .unwrap()
                        );
                        assert!(
                            filled_shares
                                .unwrap()
                                .inner()
                                .inner()
                                .eq(st0x_float_macro::float!(0.25))
                                .unwrap()
                        );
                        assert!(
                            remaining_shares
                                .unwrap()
                                .inner()
                                .inner()
                                .eq(st0x_float_macro::float!(0.75))
                                .unwrap()
                        );
                        assert!(excess_shares.unwrap().inner().inner().is_zero().unwrap());
                    }
                    st0x_dto::TradeOutcome::Filled | st0x_dto::TradeOutcome::Cancelled { .. } => {
                        panic!("failed order must broadcast a failure outcome")
                    }
                }
            }
            other => panic!("expected TradeUpdate message, got {other:?}"),
        }

        let unexpected =
            tokio::time::timeout(std::time::Duration::from_millis(10), receiver.recv()).await;
        assert!(
            unexpected.is_err(),
            "failed outcomes must not be broadcast as legacy fills"
        );
    }

    #[tokio::test]
    async fn offchain_order_cancelled_broadcasts_same_provenance_as_history() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (store, _projection) = StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(crate::offchain::order::noop_order_placer())
            .await
            .unwrap();
        let (broadcaster, mut receiver, queue, delivery_ctx) =
            test_broadcaster(&pool, &apalis_pool);
        let harness = ReactorHarness::new(broadcaster);
        let id = crate::offchain::order::OffchainOrderId::new();
        let now = chrono::Utc::now();
        let shares = Positive::new(FractionalShares::new(st0x_float_macro::float!(1))).unwrap();
        let filled = FractionalShares::new(st0x_float_macro::float!(0.25));

        store
            .send(
                &id,
                OffchainOrderCommand::Place {
                    symbol: Symbol::new("AAPL").unwrap(),
                    shares,
                    direction: st0x_execution::Direction::Sell,
                    executor: st0x_execution::SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: st0x_execution::ClientOrderId::from_uuid(id.as_uuid()),
                    kind: crate::offchain::order::CounterTradeOrderKind::Market,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("partial-cancel"),
                    placed_shares: shares,
                    submitted_at: now,
                    market_session: st0x_execution::MarketSession::Regular,
                    limit_price: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled: filled,
                    avg_price: st0x_finance::Usd::new(st0x_float_macro::float!(25)),
                    partially_filled_at: now,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::CancelOrder {
                    reason: crate::offchain::order::CancellationReason::MarketOpenReplacement,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                OffchainOrderCommand::ConfirmCancellation {
                    filled_shares: filled,
                    cancelled_at: now,
                },
            )
            .await
            .unwrap();

        let cancelled = OffchainOrderEvent::Cancelled {
            reason: crate::offchain::order::CancellationReason::MarketOpenReplacement,
            filled_shares: Some(filled),
            cancelled_at: now,
        };
        harness
            .receive::<OffchainOrder>(id, cancelled)
            .await
            .unwrap();
        perform_pending_delivery(&queue, &delivery_ctx).await;

        let Statement::TradeUpdate(trade) = receiver.recv().await.unwrap() else {
            panic!("cancelled order must broadcast a trade update");
        };
        let history = query_trades(
            &pool,
            &TradeQuery::newest(crate::dashboard::TradeProtocol::TerminalOutcomesV2),
        )
        .await
        .unwrap()
        .trades;
        assert_eq!(trade.outcome, history[0].outcome);
        assert!(matches!(
            trade.outcome,
            TradeOutcome::Cancelled {
                filled_shares: Some(observed),
                remaining_shares: Some(remaining),
                ..
            } if observed.inner().inner().eq(st0x_float_macro::float!(0.25)).unwrap()
                && remaining.inner().inner().eq(st0x_float_macro::float!(0.75)).unwrap()
        ));
    }

    #[tokio::test]
    async fn position_update_broadcasts_net_position() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (store, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (broadcaster, mut receiver, _queue, _delivery_ctx) =
            test_broadcaster(&pool, &apalis_pool);
        let harness = ReactorHarness::new(broadcaster);

        let symbol = Symbol::new("AAPL").unwrap();
        let now = chrono::Utc::now();
        let threshold = st0x_config::ExecutionThreshold::Shares(
            st0x_execution::Positive::new(st0x_execution::FractionalShares::new(
                st0x_float_macro::float!(1),
            ))
            .unwrap(),
        );

        store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold,
                    trade_id: TradeId {
                        chain: Chain::Base,
                        tx_hash: alloy::primitives::TxHash::ZERO,
                        log_index: 0,
                    },
                    amount: st0x_execution::FractionalShares::new(st0x_float_macro::float!(1)),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_timestamp: now,
                    block_number: None,
                },
            )
            .await
            .unwrap();

        harness
            .receive::<Position>(
                symbol.clone(),
                PositionEvent::OnChainOrderFilled {
                    trade_id: TradeId {
                        chain: Chain::Base,
                        tx_hash: alloy::primitives::TxHash::ZERO,
                        log_index: 0,
                    },
                    amount: st0x_execution::FractionalShares::new(st0x_float_macro::float!(1)),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_timestamp: now,
                    block_number: None,
                    seen_at: now,
                },
            )
            .await
            .unwrap();

        let msg = receiver
            .recv()
            .await
            .expect("should receive position update");

        match msg {
            Statement::PositionUpdate(position) => {
                assert_eq!(position.symbol, symbol);
                assert!(
                    position.net.eq(st0x_float_macro::float!(1)).unwrap(),
                    "expected net 1, got {:?}",
                    position.net
                );
            }
            other => panic!("expected PositionUpdate message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn manual_position_adjustment_broadcasts_adjusted_net() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let (store, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (broadcaster, mut receiver, _queue, _delivery_ctx) =
            test_broadcaster(&pool, &apalis_pool);
        let harness = ReactorHarness::new(broadcaster);

        let symbol = Symbol::new("AAPL").unwrap();
        let now = chrono::Utc::now();
        let threshold = crate::ExecutionThreshold::Shares(
            st0x_execution::Positive::new(st0x_execution::FractionalShares::new(
                st0x_float_macro::float!(1),
            ))
            .unwrap(),
        );

        store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold,
                    trade_id: TradeId {
                        chain: Chain::Base,
                        tx_hash: alloy::primitives::TxHash::ZERO,
                        log_index: 0,
                    },
                    amount: st0x_execution::FractionalShares::new(st0x_float_macro::float!(1)),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_timestamp: now,
                    block_number: None,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &symbol,
                PositionCommand::ManuallyAdjustPosition {
                    symbol: symbol.clone(),
                    target_net: st0x_execution::FractionalShares::new(st0x_float_macro::float!(-3)),
                    reason: "operator repair".to_string(),
                    threshold,
                    expected_net: Some(st0x_execution::FractionalShares::new(
                        st0x_float_macro::float!(1),
                    )),
                    price_usdc: None,
                },
            )
            .await
            .unwrap();

        harness
            .receive::<Position>(
                symbol.clone(),
                PositionEvent::ManualPositionAdjusted {
                    previous_net: st0x_execution::FractionalShares::new(st0x_float_macro::float!(
                        1
                    )),
                    target_net: st0x_execution::FractionalShares::new(st0x_float_macro::float!(-3)),
                    reason: "operator repair".to_string(),
                    price_usdc: None,
                    adjusted_at: now,
                },
            )
            .await
            .unwrap();

        let msg = receiver
            .recv()
            .await
            .expect("should receive position update");

        match msg {
            Statement::PositionUpdate(position) => {
                assert_eq!(position.symbol, symbol);
                assert!(
                    position.net.eq(st0x_float_macro::float!(-3)).unwrap(),
                    "expected adjusted net -3, got {:?}",
                    position.net
                );
            }
            other => panic!("expected PositionUpdate message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn reorged_broadcasts_reversed_net() {
        let pool = setup_test_db().await;
        let (store, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (broadcaster, mut receiver) = test_broadcaster(pool.clone());
        let harness = ReactorHarness::new(broadcaster);

        let symbol = Symbol::new("AAPL").unwrap();
        let now = chrono::Utc::now();
        let threshold = st0x_config::ExecutionThreshold::Shares(
            st0x_execution::Positive::new(st0x_execution::FractionalShares::new(
                st0x_float_macro::float!(1),
            ))
            .unwrap(),
        );
        let reorged_trade = TradeId {
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 0,
        };
        let surviving_trade = TradeId {
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 1,
        };

        // Two buys (net 8), then a reorg reverses the first (net 3). The
        // broadcaster loads the persisted post-reorg position, so the broadcast
        // net must reflect the reversal, not the pre-reorg total.
        store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold,
                    trade_id: reorged_trade.clone(),
                    amount: st0x_execution::FractionalShares::new(st0x_float_macro::float!(5)),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_timestamp: now,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold,
                    trade_id: surviving_trade.clone(),
                    amount: st0x_execution::FractionalShares::new(st0x_float_macro::float!(3)),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_timestamp: now,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &symbol,
                PositionCommand::RecordReorg {
                    trade_id: reorged_trade.clone(),
                    amount: st0x_execution::FractionalShares::new(st0x_float_macro::float!(5)),
                    direction: st0x_execution::Direction::Buy,
                    reorg_depth: 1,
                },
            )
            .await
            .unwrap();

        harness
            .receive::<Position>(
                symbol.clone(),
                PositionEvent::Reorged {
                    trade_id: reorged_trade,
                    amount: st0x_execution::FractionalShares::new(st0x_float_macro::float!(5)),
                    direction: st0x_execution::Direction::Buy,
                    reorg_depth: 1,
                    reorged_at: now,
                },
            )
            .await
            .unwrap();

        let msg = receiver
            .recv()
            .await
            .expect("should receive position update");

        match msg {
            Statement::PositionUpdate(position) => {
                assert_eq!(position.symbol, symbol);
                assert!(
                    position.net.eq(st0x_float_macro::float!(3)).unwrap(),
                    "expected reversed net 3, got {:?}",
                    position.net
                );
            }
            other => panic!("expected PositionUpdate message, got {other:?}"),
        }
    }
}
