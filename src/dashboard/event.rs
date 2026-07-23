//! Reactor that broadcasts aggregate events to WebSocket dashboard
//! clients as [`Trade`] fills and [`TransferOperation`] updates.

use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::collections::VecDeque;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use task_supervisor::{SupervisedTask, TaskResult};
#[cfg(test)]
use tokio::sync::Notify;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::time::{Instant, interval, sleep_until};
use tracing::{debug, info, warn};

use st0x_dto::{Statement, Trade, TradeOutcome, TradingVenue};
use st0x_event_sorcery::{EntityList, Reactor, SendError, deps, load_entity};

use crate::conductor::job::{Job, JobQueue, Label, QueuePushError};
use crate::equity_redemption::EquityRedemption;
use crate::offchain::order::{
    OffchainOrder, OffchainOrderEvent, OffchainOrderId, TradeConversionError,
};
use crate::onchain_trade::{OnChainTrade, OnChainTradeEvent};
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
        #[cfg(test)]
        let test_store = store.clone();
        let (handoff_retry_sender, handoff_retry_receiver) =
            mpsc::channel(HANDOFF_RETRY_QUEUE_CAPACITY);
        let ctx = Arc::new(DashboardTradeDeliveryCtx::with_store(sender.clone(), store));
        let broadcaster = Arc::new(Broadcaster::new(
            sender,
            pool.clone(),
            enqueuer.clone(),
            handoff_retry_sender,
        ));
        let handoff_monitor = DashboardTradeHandoffMonitor::new(
            handoff_retry_receiver,
            enqueuer.clone(),
            pool.clone(),
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
    pub(crate) async fn reconcile(&self, pool: &SqlitePool) -> anyhow::Result<usize> {
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

        let undelivered = self.enqueuer.reconcile_undelivered(pool).await?;

        if reset > 0 || undelivered > 0 {
            info!(
                target: "dashboard",
                reset,
                undelivered,
                "Reconciled durable dashboard trade deliveries",
            );
        }

        Ok(undelivered)
    }
}

impl DashboardTradeEnqueuer {
    async fn reconcile_undelivered(&self, pool: &SqlitePool) -> anyhow::Result<usize> {
        let trades = crate::dashboard::trade_loader::load_all_trades(pool)
            .await
            .context("failed to reconstruct terminal trades for delivery reconciliation")?;
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

        sqlx_apalis::query(
            "UPDATE Jobs SET job = ? \
             WHERE job_type = ? AND idempotency_key = ? AND status != 'Done'",
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

#[derive(Clone)]
pub(crate) struct DashboardTradeHandoffMonitor {
    receiver: Arc<Mutex<mpsc::Receiver<DashboardTradeHandoff>>>,
    enqueuer: Arc<DashboardTradeEnqueuer>,
    pool: SqlitePool,
    reconciliation_interval: Duration,
    #[cfg(test)]
    exhaustion_notify: Arc<Notify>,
}

impl DashboardTradeHandoffMonitor {
    fn new(
        receiver: mpsc::Receiver<DashboardTradeHandoff>,
        enqueuer: Arc<DashboardTradeEnqueuer>,
        pool: SqlitePool,
    ) -> Self {
        Self {
            receiver: Arc::new(Mutex::new(receiver)),
            enqueuer,
            pool,
            reconciliation_interval: HANDOFF_RECONCILIATION_INTERVAL,
            #[cfg(test)]
            exhaustion_notify: Arc::new(Notify::new()),
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

    async fn receive(&self) -> Option<DashboardTradeHandoff> {
        self.receiver.lock().await.recv().await
    }

    async fn persist_once(
        &self,
        handoff: &DashboardTradeHandoff,
    ) -> Result<(), DashboardTradeHandoffAttemptError> {
        let trade = match handoff {
            DashboardTradeHandoff::Trade(trade) => trade.as_ref().clone(),
            DashboardTradeHandoff::ReloadOffchainOrder(id) => {
                let order = load_entity::<OffchainOrder>(&self.pool, id)
                    .await
                    .map_err(|source| DashboardTradeHandoffAttemptError::Replay {
                        id: *id,
                        source,
                    })?
                    .ok_or(DashboardTradeHandoffAttemptError::Missing { id: *id })?;

                order.try_into_trade(id).map_err(|source| {
                    DashboardTradeHandoffAttemptError::Conversion { id: *id, source }
                })?
            }
        };

        self.enqueuer.enqueue(trade).await?;
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
                    error,
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

    async fn reconcile_after_exhaustion(&self, needed: &mut bool) -> anyhow::Result<()> {
        self.enqueuer.reconcile_undelivered(&self.pool).await?;
        *needed = false;
        Ok(())
    }
}

impl SupervisedTask for DashboardTradeHandoffMonitor {
    async fn run(&mut self) -> TaskResult {
        info!(target: "dashboard", "Dashboard trade handoff monitor started");
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

            let next_attempt = Self::next_attempt(&pending);
            if pending.len() == HANDOFF_RETRY_QUEUE_CAPACITY {
                tokio::select! {
                    () = sleep_until(next_attempt.ok_or(DashboardTradeHandoffMonitorError::QueueClosed)?) => {}
                    _ = reconciliation.tick(), if reconciliation_needed => {
                        self.reconcile_after_exhaustion(&mut reconciliation_needed).await?;
                    }
                }
                continue;
            }

            if let Some(next_attempt) = next_attempt {
                tokio::select! {
                    handoff = self.receive() => {
                        let handoff = handoff.ok_or(DashboardTradeHandoffMonitorError::QueueClosed)?;
                        pending.push_back(ScheduledDashboardTradeHandoff::new(handoff));
                    }
                    () = sleep_until(next_attempt) => {}
                    _ = reconciliation.tick(), if reconciliation_needed => {
                        self.reconcile_after_exhaustion(&mut reconciliation_needed).await?;
                    }
                }
            } else {
                tokio::select! {
                    handoff = self.receive() => {
                        let handoff = handoff.ok_or(DashboardTradeHandoffMonitorError::QueueClosed)?;
                        pending.push_back(ScheduledDashboardTradeHandoff::new(handoff));
                    }
                    _ = reconciliation.tick(), if reconciliation_needed => {
                        self.reconcile_after_exhaustion(&mut reconciliation_needed).await?;
                    }
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
    DeterministicConversion(#[source] DashboardTradeHandoffAttemptError),
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
            Self::Conversion { source, .. } => matches!(
                source,
                TradeConversionError::Pending
                    | TradeConversionError::Submitted
                    | TradeConversionError::PartiallyFilled
                    | TradeConversionError::Cancelling
            ),
        }
    }
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
}

impl DashboardTradeDeliveryCtx {
    #[cfg(test)]
    pub(crate) fn new(sender: broadcast::Sender<Statement>, pool: SqlitePool) -> Self {
        Self::with_store(sender, Arc::new(DashboardTradeDeliveryStore::new(pool)))
    }

    fn with_store(
        sender: broadcast::Sender<Statement>,
        store: Arc<DashboardTradeDeliveryStore>,
    ) -> Self {
        Self { sender, store }
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
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DashboardTradeDeliveryError {
    #[error("dashboard trade delivery database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("dashboard trade delivery record is missing for {trade_id}")]
    MissingRecord { trade_id: String },
    #[cfg(test)]
    #[error("injected dashboard trade delivery failure")]
    Injected,
}

impl Job<DashboardTradeDeliveryCtx> for DeliverDashboardTrade {
    type Output = ();
    type Error = DashboardTradeDeliveryError;

    const WORKER_NAME: &'static str = "dashboard-trade-delivery-worker";
    const TERMINAL_FAILURE_MSG: &'static str =
        "Dashboard trade delivery failed after retries; terminal update remains undelivered";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind =
        crate::conductor::job::JobKind::DashboardTradeDelivery;

    fn label(&self) -> Label {
        Label::new(format!("DeliverDashboardTrade:{}", self.trade.id))
    }

    async fn perform(&self, ctx: &DashboardTradeDeliveryCtx) -> Result<Self::Output, Self::Error> {
        if ctx.store.is_delivered(&self.trade.id).await? {
            return Ok(());
        }

        ctx.publish(self.trade.clone());
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
}

impl Broadcaster {
    fn new(
        sender: broadcast::Sender<Statement>,
        pool: SqlitePool,
        trade_enqueuer: Arc<DashboardTradeEnqueuer>,
        handoff_retry_sender: mpsc::Sender<DashboardTradeHandoff>,
    ) -> Self {
        Self {
            sender,
            pool,
            trade_enqueuer,
            handoff_retry_sender,
        }
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
                if let OnChainTradeEvent::Filled {
                    symbol,
                    amount,
                    direction,
                    block_timestamp,
                    ..
                } = event
                {
                    self.enqueue_trade(Trade {
                        id: id.to_string(),
                        occurred_at: block_timestamp,
                        venue: TradingVenue::Raindex,
                        direction,
                        symbol,
                        shares: st0x_finance::FractionalShares::new(amount),
                        outcome: TradeOutcome::Filled,
                    })
                    .await?;
                }

                Ok(())
            })

            .on(|id, event| async move {
                if !matches!(
                    event,
                    PositionEvent::OnChainOrderFilled { .. }
                        | PositionEvent::OffChainOrderFilled { .. }
                        | PositionEvent::ManualPositionAdjusted { .. }
                ) {
                    return Ok(());
                }

                match load_entity::<Position>(&self.pool, &id).await {
                    Ok(Some(position)) => {
                        self.broadcast_position(st0x_dto::Position {
                            symbol: position.symbol,
                            net: position.net.inner(),
                            last_price_usdc: position.last_price_usdc,
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
                    Filled { .. } | Failed { .. } => {
                        self.enqueue_offchain_trade(id).await?;
                    }
                    Placed { .. }
                    | Submitted { .. }
                    | Accepted { .. }
                    | PartiallyFilled { .. }
                    | CancelRequested { .. }
                    | Cancelled { .. } => {}
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
    use apalis_core::worker::ext::circuit_breaker::config::CircuitBreakerConfig;
    use std::sync::Arc;
    use std::time::Duration;

    use st0x_event_sorcery::{ReactorHarness, StoreBuilder};
    use st0x_execution::Symbol;

    use super::*;
    use crate::conductor::job::{
        FAIL_STOP_RECOVERY_TIMEOUT, FailureInjector, build_supervised_worker, build_worker_inner,
    };
    use crate::offchain::order::{OffchainOrderCommand, OffchainOrderEvent};
    use crate::position::{PositionCommand, PositionEvent, TradeId};
    use crate::test_utils::setup_test_pools;

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
            shares: st0x_finance::FractionalShares::new(st0x_float_macro::float!(1)),
            outcome: TradeOutcome::Filled,
        }
    }

    async fn persist_failed_offchain_order(pool: SqlitePool, id: OffchainOrderId) {
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
                    failed_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
    }

    async fn persist_cancelled_offchain_order(pool: SqlitePool, id: OffchainOrderId) {
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
                    cancelled_at: chrono::Utc::now(),
                },
            )
            .await
            .unwrap();
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
        failure_notify: Arc<tokio::sync::Notify>,
    ) -> tokio::task::JoinHandle<()> {
        let fail_stop = CircuitBreakerConfig::default()
            .with_failure_threshold(1)
            .with_recovery_timeout(FAIL_STOP_RECOVERY_TIMEOUT);

        tokio::spawn(async move {
            let monitor = Monitor::new()
                .should_restart(|_ctx, _error, _attempt| false)
                .register(move |index| {
                    build_supervised_worker!(
                        ::<DashboardTradeDeliveryCtx, DeliverDashboardTrade>,
                        index,
                        queue.clone(),
                        ctx.clone(),
                        fail_stop.clone(),
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
                    tx_hash: alloy::primitives::TxHash::ZERO,
                    log_index: 0,
                },
                OnChainTradeEvent::Filled {
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: st0x_float_macro::float!(10),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_number: 12345,
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
                    tx_hash: alloy::primitives::TxHash::ZERO,
                    log_index: 9,
                },
                OnChainTradeEvent::Filled {
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: st0x_float_macro::float!(1),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_number: 12345,
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

        persist_failed_offchain_order(pool, id).await;
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
    async fn deterministic_trade_conversion_failure_stops_the_handoff_monitor() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let id = OffchainOrderId::new();
        persist_cancelled_offchain_order(pool.clone(), id).await;
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
    async fn startup_reconciliation_recovers_terminal_trade_without_job() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let store = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let id = crate::onchain_trade::OnChainTradeId {
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 7,
        };
        let now = chrono::Utc::now();
        store
            .send(
                &id,
                crate::onchain_trade::OnChainTradeCommand::WitnessAt {
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: st0x_float_macro::float!(1),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_number: 12345,
                    block_timestamp: now,
                    filled_at: now,
                },
            )
            .await
            .unwrap();

        let (sender, _receiver) = broadcast::channel(16);
        let delivery = DashboardTradeDelivery::new(&apalis_pool, &pool, sender);

        assert_eq!(delivery.reconcile(&pool).await.unwrap(), 1);
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
    async fn startup_reconciliation_skips_delivered_trade() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let store = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let id = crate::onchain_trade::OnChainTradeId {
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 8,
        };
        let now = chrono::Utc::now();
        store
            .send(
                &id,
                crate::onchain_trade::OnChainTradeCommand::WitnessAt {
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: st0x_float_macro::float!(1),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_number: 12345,
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

        assert_eq!(delivery.reconcile(&pool).await.unwrap(), 0);
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
        assert_eq!(delivery.reconcile(&pool).await.unwrap(), 1);

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
        let monitor =
            spawn_delivery_worker(queue, ctx.clone(), Arc::new(tokio::sync::Notify::new()));

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

        delivery.reconcile(&pool).await.unwrap();

        let monitor = spawn_delivery_worker(
            delivery.queue,
            delivery.ctx,
            Arc::new(tokio::sync::Notify::new()),
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

        delivery.reconcile(&pool).await.unwrap();
        delivery.reconcile(&pool).await.unwrap();

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
        let failure_notify = Arc::new(tokio::sync::Notify::new());
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
        let harness = ReactorHarness::new(broadcaster);

        let now = chrono::Utc::now();
        let ingested_at = now + chrono::Duration::seconds(1);
        let id = crate::onchain_trade::OnChainTradeId {
            tx_hash: alloy::primitives::TxHash::ZERO,
            log_index: 0,
        };

        harness
            .receive::<OnChainTrade>(
                id,
                OnChainTradeEvent::Filled {
                    symbol: Symbol::new("AAPL").unwrap(),
                    amount: st0x_float_macro::float!(10),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_number: 12345,
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
                    failed_at: now,
                },
            )
            .await
            .unwrap();

        let failed = OffchainOrderEvent::Failed {
            error: "asset is not tradable".to_string(),
            failed_at: now,
        };
        harness.receive::<OffchainOrder>(id, failed).await.unwrap();

        perform_pending_delivery(&queue, &delivery_ctx).await;

        let message = receiver.recv().await.expect("should receive failure");
        match message {
            Statement::TradeUpdate(trade) => match trade.outcome {
                st0x_dto::TradeOutcome::Failed {
                    error,
                    filled_shares,
                    remaining_shares,
                    excess_shares,
                } => {
                    assert_eq!(error, "asset is not tradable");
                    assert!(
                        filled_shares
                            .inner()
                            .inner()
                            .eq(st0x_float_macro::float!(0.25))
                            .unwrap()
                    );
                    assert!(
                        remaining_shares
                            .inner()
                            .inner()
                            .eq(st0x_float_macro::float!(0.75))
                            .unwrap()
                    );
                    assert!(excess_shares.inner().inner().is_zero().unwrap());
                }
                st0x_dto::TradeOutcome::Filled => {
                    panic!("failed order must broadcast a failure outcome")
                }
            },
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
                        tx_hash: alloy::primitives::TxHash::ZERO,
                        log_index: 0,
                    },
                    amount: st0x_execution::FractionalShares::new(st0x_float_macro::float!(1)),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_timestamp: now,
                },
            )
            .await
            .unwrap();

        harness
            .receive::<Position>(
                symbol.clone(),
                PositionEvent::OnChainOrderFilled {
                    trade_id: TradeId {
                        tx_hash: alloy::primitives::TxHash::ZERO,
                        log_index: 0,
                    },
                    amount: st0x_execution::FractionalShares::new(st0x_float_macro::float!(1)),
                    direction: st0x_execution::Direction::Buy,
                    price_usdc: st0x_float_macro::float!(150),
                    block_timestamp: now,
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
                        tx_hash: alloy::primitives::TxHash::ZERO,
                        log_index: 0,
                    },
                    amount: st0x_execution::FractionalShares::new(st0x_float_macro::float!(1)),
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
}
