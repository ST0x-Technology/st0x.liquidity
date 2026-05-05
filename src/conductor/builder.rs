//! Constructs a fully-wired [`Conductor`] instance from its dependencies.

use alloy::providers::Provider;
use apalis::layers::WorkerBuilderExt;
use apalis::layers::retry::RetryPolicy;
use apalis::prelude::{Monitor, WorkerBuilder};
use apalis_core::worker::event::Event;
use apalis_core::worker::ext::circuit_breaker::{CircuitBreaker, config::CircuitBreakerConfig};
use apalis_core::worker::ext::event_listener::EventListenerExt;
use apalis_cron::CronStream;
use sqlx::SqlitePool;
use std::sync::Arc;
use task_supervisor::SupervisorBuilder;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

use st0x_event_sorcery::{Projection, Store};
use st0x_evm::ReadOnlyEvm;
use st0x_execution::Executor;

use super::job::{FAIL_STOP_RECOVERY_TIMEOUT, FixedInterval, RETRY_BACKOFF, poll_orders, work};
#[cfg(any(test, feature = "test-support"))]
use super::job::{FailureInjector, JobKind};
use super::monitor::order_fills::OrderFillMonitor;
use super::monitor::positions::PositionMonitor;
use super::{Conductor, MonitorTaskError, build_order_poller, spawn_inventory_poller};
use crate::config::Ctx;
use crate::inventory::{InventoryPollingService, InventorySnapshot, WalletPollingCtx};
use crate::offchain_order::OffchainOrder;
use crate::onchain::backfill::BackfillJobQueue;
use crate::onchain::pyth::FeedIdCache;
use crate::onchain::raindex::RaindexService;
use crate::onchain_trade::OnChainTrade;
use crate::position::Position;
use crate::symbol::cache::SymbolCache;
use crate::threshold::ExecutionThreshold;
use crate::tokenization::Tokenizer;
use crate::trading::offchain::hedge::{HedgeCtx, HedgeJobQueue};
use crate::trading::onchain::trade_accountant::{
    AccountantCtx, DexTradeAccountingJobQueue, TradeAccountingError,
};
use crate::vault_registry::VaultRegistry;

pub(crate) struct CqrsFrameworks {
    pub(crate) onchain_trade: Arc<Store<OnChainTrade>>,
    pub(crate) position: Arc<Store<Position>>,
    pub(crate) position_projection: Arc<Projection<Position>>,
    pub(crate) offchain_order: Arc<Store<OffchainOrder>>,
    pub(crate) offchain_order_projection: Arc<Projection<OffchainOrder>>,
    pub(crate) vault_registry: Arc<Store<VaultRegistry>>,
    pub(crate) vault_registry_projection: Arc<Projection<VaultRegistry>>,
    pub(crate) snapshot: Arc<Store<InventorySnapshot>>,
}

/// Everything needed to construct a running [`Conductor`].
pub(crate) struct ConductorCtx<Prov, Exec> {
    pub(crate) ctx: Ctx,
    pub(crate) cache: SymbolCache,
    pub(crate) provider: Prov,
    pub(crate) executor: Exec,
    pub(crate) execution_threshold: ExecutionThreshold,
    pub(crate) frameworks: CqrsFrameworks,
    pub(crate) pool: SqlitePool,
    pub(crate) poll_notify: Arc<tokio::sync::Notify>,
    pub(crate) wallet_polling: Option<WalletPollingCtx>,
    pub(crate) tokenizer: Option<Arc<dyn Tokenizer>>,
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) failure_injector: FailureInjector,
}

/// Wires all runtime components and returns a running [`Conductor`].
#[bon::builder]
// Main spawn function, long to keep together
#[allow(clippy::too_many_lines)]
pub(crate) fn spawn<Prov, Exec>(
    context: ConductorCtx<Prov, Exec>,
    job_queue: DexTradeAccountingJobQueue,
    backfill_queue: BackfillJobQueue,
    hedge_queue: HedgeJobQueue,
    job_cleanup: JoinHandle<()>,
    executor_maintenance: Option<JoinHandle<()>>,
    rebalancer: Option<JoinHandle<()>>,
) -> Conductor
where
    Prov: Provider + Clone + Send + Sync + 'static,
    Exec: Executor + Clone + Send + Sync + 'static,
    TradeAccountingError: From<Exec::Error>,
{
    info!("Starting conductor orchestration");

    log_optional_task_status("executor maintenance", executor_maintenance.is_some());
    log_optional_task_status("rebalancer", rebalancer.is_some());

    let order_owner = context.ctx.order_owner();
    let evm = ReadOnlyEvm::new(context.provider.clone());
    let raindex_service = Arc::new(RaindexService::new(
        evm,
        context.ctx.evm.orderbook,
        context.frameworks.vault_registry_projection.clone(),
        order_owner,
    ));

    let polling_service = InventoryPollingService::new(
        raindex_service,
        context.executor.clone(),
        context.frameworks.vault_registry.clone(),
        context.ctx.evm.orderbook,
        order_owner,
        context.frameworks.snapshot,
        context.wallet_polling,
        context.tokenizer,
    );

    let inventory_poller = Some(spawn_inventory_poller(
        polling_service,
        std::time::Duration::from_secs(context.ctx.inventory_poll_interval),
        context.poll_notify.clone(),
    ));
    log_optional_task_status("inventory poller", inventory_poller.is_some());

    let order_poller = Arc::new(build_order_poller(
        &context.ctx,
        context.executor.clone(),
        (*context.frameworks.offchain_order_projection).clone(),
        context.frameworks.offchain_order.clone(),
        context.frameworks.position.clone(),
    ));
    let order_poller_schedule = FixedInterval::new(order_poller.polling_interval());

    let counter_trade_submission_lock = Arc::new(tokio::sync::Mutex::new(()));

    let hedge_ctx = Arc::new(HedgeCtx {
        position: context.frameworks.position.clone(),
        offchain_order: context.frameworks.offchain_order.clone(),
    });

    let position_monitor = PositionMonitor::new(
        context.executor.clone(),
        context.frameworks.position_projection.clone(),
        hedge_queue.clone(),
        std::time::Duration::from_secs(context.ctx.position_check_interval),
        context.ctx.clone(),
        context.pool.clone(),
    );

    let trade_cqrs = super::TradeProcessingCqrs {
        onchain_trade: context.frameworks.onchain_trade,
        position: context.frameworks.position,
        position_projection: context.frameworks.position_projection,
        offchain_order: context.frameworks.offchain_order,
        execution_threshold: context.execution_threshold,
        assets: context.ctx.assets.clone(),
        counter_trade_submission_lock,
    };

    let accountant_ctx = Arc::new(AccountantCtx {
        orderbook: context.ctx.evm.orderbook,
        ctx: context.ctx.clone(),
        cache: context.cache,
        feed_id_cache: FeedIdCache::default(),
        evm: ReadOnlyEvm::new(context.provider),
        cqrs: trade_cqrs,
        vault_registry: context.frameworks.vault_registry,
        executor: context.executor,
        pool: context.pool.clone(),
        job_queue: job_queue.clone(),
    });

    let order_fill_monitor = OrderFillMonitor::new(
        context.ctx.evm.clone(),
        job_queue.clone(),
        backfill_queue.clone(),
        context.pool,
    );

    let supervisor = SupervisorBuilder::default()
        .with_task("order-fill-monitor", order_fill_monitor)
        .with_task("position-monitor", position_monitor)
        .build()
        .run();

    #[cfg(any(test, feature = "test-support"))]
    let failure_injector = context.failure_injector;
    #[cfg(any(test, feature = "test-support"))]
    let failure_injector_for_hedge = failure_injector.clone();
    #[cfg(any(test, feature = "test-support"))]
    let failure_injector_for_backfill = failure_injector.clone();
    let failure_notify = Arc::new(tokio::sync::Notify::new());
    let failure_notify_for_hedge = failure_notify.clone();
    let failure_notify_for_poller = failure_notify.clone();

    let fail_stop = CircuitBreakerConfig::default()
        .with_failure_threshold(1)
        .with_recovery_timeout(FAIL_STOP_RECOVERY_TIMEOUT);
    let fail_stop_for_hedge = fail_stop.clone();
    let fail_stop_for_poller = fail_stop.clone();
    let fail_stop_for_backfill = fail_stop.clone();

    let accountant_ctx_for_backfill = accountant_ctx.clone();
    let failure_notify_for_backfill = failure_notify.clone();

    let monitor: JoinHandle<Result<(), MonitorTaskError>> = tokio::spawn(async move {
        let failure_notify_for_accountant = failure_notify.clone();
        let failure_notify_for_select = failure_notify.clone();
        let apalis_monitor = Monitor::new()
            .should_restart(|_ctx, _error, _attempt| false)
            .register(move |index| {
                let builder = WorkerBuilder::new(format!("order-fill-worker-{index}"))
                    .backend(job_queue.clone().into_storage())
                    .data(accountant_ctx.clone());

                #[cfg(any(test, feature = "test-support"))]
                let builder = builder
                    .data(failure_injector.clone())
                    .data(JobKind::OrderFill);

                builder
                    .concurrency(1)
                    .retry(RetryPolicy::retries(3).with_backoff(RETRY_BACKOFF.clone()))
                    .break_circuit_with(fail_stop.clone())
                    .on_event({
                        let failure_notify = failure_notify_for_accountant.clone();
                        move |ctx, event| {
                            if let Event::Error(err) = event {
                                error!(%err, worker = %ctx.name(), "Job failed after retries");
                                failure_notify.notify_waiters();
                                let _ = ctx.stop();
                            }
                        }
                    })
                    .build(work::<AccountantCtx<Prov, Exec>, _>)
            })
            .register(move |index| {
                let builder = WorkerBuilder::new(format!("hedge-worker-{index}"))
                    .backend(hedge_queue.clone().into_storage())
                    .data(hedge_ctx.clone());

                #[cfg(any(test, feature = "test-support"))]
                let builder = builder
                    .data(failure_injector_for_hedge.clone())
                    .data(JobKind::Hedge);

                builder
                    .concurrency(1)
                    .retry(RetryPolicy::retries(3).with_backoff(RETRY_BACKOFF.clone()))
                    .break_circuit_with(fail_stop_for_hedge.clone())
                    .on_event({
                        let failure_notify = failure_notify_for_hedge.clone();
                        move |ctx, event| {
                            if let Event::Error(err) = event {
                                error!(%err, worker = %ctx.name(), "Job failed after retries");
                                failure_notify.notify_waiters();
                                let _ = ctx.stop();
                            }
                        }
                    })
                    .build(work::<HedgeCtx, _>)
            })
            .register(move |index| {
                let builder = WorkerBuilder::new(format!("backfill-worker-{index}"))
                    .backend(backfill_queue.clone().into_storage())
                    .data(accountant_ctx_for_backfill.clone());

                #[cfg(any(test, feature = "test-support"))]
                let builder = builder
                    .data(failure_injector_for_backfill.clone())
                    .data(JobKind::Backfill);

                builder
                    .concurrency(1)
                    .retry(RetryPolicy::retries(3).with_backoff(RETRY_BACKOFF.clone()))
                    .break_circuit_with(fail_stop_for_backfill.clone())
                    .on_event({
                        let failure_notify = failure_notify_for_backfill.clone();
                        move |ctx, event| {
                            if let Event::Error(err) = event {
                                error!(%err, worker = %ctx.name(), "Job failed after retries");
                                failure_notify.notify_waiters();
                                let _ = ctx.stop();
                            }
                        }
                    })
                    .build(work::<AccountantCtx<Prov, Exec>, _>)
            })
            .register(move |index| {
                WorkerBuilder::new(format!("order-poller-{index}"))
                    .backend(CronStream::new(order_poller_schedule.clone()))
                    .data(order_poller.clone())
                    .concurrency(1)
                    .retry(RetryPolicy::retries(3).with_backoff(RETRY_BACKOFF.clone()))
                    .break_circuit_with(fail_stop_for_poller.clone())
                    .on_event({
                        let failure_notify = failure_notify_for_poller.clone();
                        move |ctx, event| {
                            if let Event::Error(err) = event {
                                error!(%err, worker = %ctx.name(), "Order poller failed after retries");
                                failure_notify.notify_waiters();
                                let _ = ctx.stop();
                            }
                        }
                    })
                    .build(poll_orders::<Exec>)
            });

        tokio::select! {
            biased;
            () = failure_notify_for_select.notified() => Err(MonitorTaskError::TerminalJobFailure),
            result = apalis_monitor.run() => match result {
                Ok(()) => Err(MonitorTaskError::UnexpectedExit { source: None }),
                Err(source) => Err(MonitorTaskError::UnexpectedExit { source: Some(source) }),
            },
        }
    });

    Conductor {
        supervisor,
        monitor,
        executor_maintenance,
        rebalancer,
        inventory_poller,
        job_cleanup,
    }
}

fn log_optional_task_status(task_name: &str, is_configured: bool) {
    if is_configured {
        info!("Started {task_name} task");
    } else {
        debug!("{task_name} not configured", task_name = task_name);
    }
}
