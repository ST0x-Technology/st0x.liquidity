//! Supervised task types and supervisor wiring for the conductor.

use std::sync::Arc;
use std::time::Duration;

use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types;
use apalis::prelude::{Monitor, WorkerBuilder, WorkerFactoryFn};
use apalis_sql::sqlite::SqliteStorage;
use async_trait::async_trait;
use futures_util::Stream;
use sqlx::SqlitePool;
use task_supervisor::{SupervisedTask, SupervisorBuilder, SupervisorHandle, TaskResult};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use st0x_execution::Executor;

use super::EventProcessingError;
use super::event_pipeline::receive_blockchain_events_to_storage;
use super::job::Job;
use crate::bindings::IOrderBookV5::{ClearV3, TakeOrderV3};
use crate::config::Ctx;
use crate::dual_write::DualWriteContext;
use crate::inventory::{InventoryPollingService, InventorySnapshotAggregate};
use crate::onchain::event_processor::{ProcessOnchainEventCtx, ProcessOnchainEventJob};
use crate::onchain::vault::VaultService;

/// Pre-spawned background task handles to register with the supervisor.
pub(super) struct BackgroundHandles {
    pub(super) executor_maintenance: Option<JoinHandle<()>>,
    pub(super) rebalancer: Option<JoinHandle<()>>,
    pub(super) monitor: JoinHandle<()>,
    pub(super) receiver: JoinHandle<()>,
}

/// Spawns the apalis Monitor and live event receiver as background tasks.
/// Returns JoinHandles that the caller should register with the
/// supervisor to keep them alive.
pub(super) fn spawn_monitor_and_receiver<S1, S2, P, E>(
    process_ctx: ProcessOnchainEventCtx<P, E>,
    event_storage: SqliteStorage<ProcessOnchainEventJob<P, E>>,
    clear_stream: S1,
    take_stream: S2,
) -> (JoinHandle<()>, JoinHandle<()>)
where
    S1: Stream<Item = Result<(ClearV3, Log), sol_types::Error>> + Unpin + Send + 'static,
    S2: Stream<Item = Result<(TakeOrderV3, Log), sol_types::Error>> + Unpin + Send + 'static,
    P: Provider + Clone + Send + Sync + Unpin + 'static,
    E: Executor + Clone + Send + Sync + Unpin + 'static,
    EventProcessingError: From<E::Error>,
{
    let monitor = Monitor::new().register(
        WorkerBuilder::new("process-onchain-event")
            .data(process_ctx)
            .backend(event_storage.clone())
            .build_fn(ProcessOnchainEventJob::run),
    );

    let monitor_handle = tokio::spawn(async move {
        if let Err(err) = monitor.run().await {
            error!(error = %err, "Apalis monitor failed");
        }
    });

    let receiver_handle = tokio::spawn(receive_blockchain_events_to_storage(
        clear_stream,
        take_stream,
        event_storage,
    ));

    (monitor_handle, receiver_handle)
}

pub(super) fn build_and_run_supervisor<P, E>(
    ctx: &Ctx,
    pool: &SqlitePool,
    executor: E,
    dual_write_context: DualWriteContext,
    provider: &P,
    snapshot_cqrs: sqlite_es::SqliteCqrs<InventorySnapshotAggregate>,
    handles: BackgroundHandles,
) -> SupervisorHandle
where
    P: Provider + Clone + Send + Sync + 'static,
    E: Executor + Clone + Send + Sync + 'static,
    EventProcessingError: From<E::Error>,
{
    let mut builder = SupervisorBuilder::new()
        .with_unlimited_restarts()
        .with_base_restart_delay(Duration::from_secs(5));

    builder = builder.with_task(
        "order-poller",
        OrderPollerTask {
            ctx: crate::offchain::order_poller::PollOrderStatusCtx {
                pool: pool.clone(),
                executor: executor.clone(),
                dual_write_context: dual_write_context.clone(),
                max_jitter: ctx.get_order_poller_config().max_jitter,
            },
            interval: ctx.get_order_poller_config().polling_interval,
        },
    );

    builder = builder.with_task(
        "position-checker",
        PositionCheckerTask {
            ctx: crate::onchain::accumulator::CheckAccumulatedPositionsCtx {
                pool: pool.clone(),
                executor: executor.clone(),
                dual_write_context,
            },
            interval: Duration::from_secs(60),
        },
    );

    builder = register_inventory_poller(builder, ctx, pool, provider, snapshot_cqrs, executor);

    if let Some(handle) = handles.executor_maintenance {
        builder = builder.with_task(
            "executor-maintenance",
            ExecutorMaintenanceTask {
                handle: Some(handle),
            },
        );
    }

    if let Some(handle) = handles.rebalancer {
        builder = builder.with_task(
            "rebalancer",
            JoinHandleTask {
                handle: Some(handle),
            },
        );
    }

    builder = builder.with_task(
        "apalis-monitor",
        JoinHandleTask {
            handle: Some(handles.monitor),
        },
    );

    builder = builder.with_task(
        "event-receiver",
        JoinHandleTask {
            handle: Some(handles.receiver),
        },
    );

    builder.build().run()
}

fn register_inventory_poller<P, E>(
    mut builder: SupervisorBuilder,
    ctx: &Ctx,
    pool: &SqlitePool,
    provider: &P,
    snapshot_cqrs: sqlite_es::SqliteCqrs<InventorySnapshotAggregate>,
    executor: E,
) -> SupervisorBuilder
where
    P: Provider + Clone + Send + Sync + 'static,
    E: Executor + Clone + Send + Sync + 'static,
{
    let Ok(order_owner) = ctx.order_owner() else {
        warn!(
            "Inventory poller disabled: \
             could not resolve order owner"
        );
        return builder;
    };

    let vault_service = Arc::new(VaultService::new(provider.clone(), ctx.evm.orderbook));

    builder = builder.with_task(
        "inventory-poller",
        InventoryPollerTask {
            service: Arc::new(InventoryPollingService {
                vault_service,
                executor,
                pool: pool.clone(),
                orderbook: ctx.evm.orderbook,
                order_owner,
                snapshot_cqrs,
            }),
            interval: Duration::from_secs(60),
        },
    );

    builder
}

/// Supervised task wrapping the order status poller.
#[derive(Clone)]
struct OrderPollerTask<E> {
    ctx: crate::offchain::order_poller::PollOrderStatusCtx<E>,
    interval: Duration,
}

#[async_trait]
impl<E> SupervisedTask for OrderPollerTask<E>
where
    E: Executor + Clone + Send + Sync + 'static,
{
    async fn run(&mut self) -> TaskResult {
        let mut interval = tokio::time::interval(self.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            debug!("Running order status poll");
            if let Err(e) = self.ctx.poll_once().await {
                error!("Order poller failed: {e}");
            }
        }
    }
}

/// Supervised task wrapping the accumulated position checker.
#[derive(Clone)]
struct PositionCheckerTask<E> {
    ctx: crate::onchain::accumulator::CheckAccumulatedPositionsCtx<E>,
    interval: Duration,
}

#[async_trait]
impl<E> SupervisedTask for PositionCheckerTask<E>
where
    E: Executor + Clone + Send + Sync + 'static,
    EventProcessingError: From<E::Error>,
{
    async fn run(&mut self) -> TaskResult {
        let mut interval = tokio::time::interval(self.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            debug!("Running accumulated position check");
            if let Err(e) = self.ctx.check_once().await {
                error!("Position checker failed: {e}");
            }
        }
    }
}

/// Supervised task wrapping the inventory poller.
#[derive(Clone)]
struct InventoryPollerTask<P, E>
where
    P: Provider + Clone,
{
    service: Arc<InventoryPollingService<P, E>>,
    interval: Duration,
}

#[async_trait]
impl<P, E> SupervisedTask for InventoryPollerTask<P, E>
where
    P: Provider + Clone + Send + Sync + 'static,
    E: Executor + Clone + Send + Sync + 'static,
{
    async fn run(&mut self) -> TaskResult {
        let mut interval = tokio::time::interval(self.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            debug!("Running inventory poll");
            if let Err(error) = self.service.poll_and_record().await {
                error!(%error, "Inventory polling failed");
            }
        }
    }
}

/// Supervised task wrapping a pre-spawned executor maintenance JoinHandle.
///
/// This task waits for the existing JoinHandle to complete. On restart,
/// it has no handle to wait on and returns immediately (executor
/// maintenance is a one-shot long-running task, not restartable).
struct ExecutorMaintenanceTask {
    handle: Option<JoinHandle<()>>,
}

impl Clone for ExecutorMaintenanceTask {
    fn clone(&self) -> Self {
        Self { handle: None }
    }
}

#[async_trait]
impl SupervisedTask for ExecutorMaintenanceTask {
    async fn run(&mut self) -> TaskResult {
        let Some(handle) = self.handle.take() else {
            warn!(
                "Executor maintenance has no handle \
                 (already consumed or restarted)"
            );
            return Ok(());
        };

        match handle.await {
            Ok(()) => {
                info!("Executor maintenance completed");
                Ok(())
            }
            Err(e) if e.is_cancelled() => {
                info!("Executor maintenance cancelled");
                Ok(())
            }
            Err(e) => Err(anyhow::anyhow!("Executor maintenance panicked: {e}")),
        }
    }
}

/// Supervised task wrapping a pre-spawned JoinHandle (rebalancer, etc.).
///
/// Like `ExecutorMaintenanceTask`, this waits for the handle to complete.
/// After the first run or restart, returns immediately.
struct JoinHandleTask {
    handle: Option<JoinHandle<()>>,
}

impl Clone for JoinHandleTask {
    fn clone(&self) -> Self {
        Self { handle: None }
    }
}

#[async_trait]
impl SupervisedTask for JoinHandleTask {
    async fn run(&mut self) -> TaskResult {
        let Some(handle) = self.handle.take() else {
            warn!(
                "JoinHandle task has no handle \
                 (already consumed or restarted)"
            );
            return Ok(());
        };

        match handle.await {
            Ok(()) => Ok(()),
            Err(e) if e.is_cancelled() => Ok(()),
            Err(e) => Err(anyhow::anyhow!("Task panicked: {e}")),
        }
    }
}
