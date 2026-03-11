//! Typestate builder for constructing a fully-wired Conductor instance.

use std::sync::Arc;
use std::time::Duration;

use alloy::providers::Provider;
use apalis::prelude::{Monitor, WorkerBuilder};
use sqlx::SqlitePool;
use task_supervisor::SupervisorBuilder;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{error, info};

use st0x_event_sorcery::{Projection, Store};
use st0x_evm::ReadOnlyEvm;
use st0x_execution::Executor;

use super::job::Job;
use super::order_fill_monitor::{OrderFillJobQueue, OrderFillMonitor};
use super::order_fill_processor::OrderFillCtx;
use super::{
    Conductor, OrderFillError, spawn_inventory_poller, spawn_periodic_accumulated_position_check,
};
use crate::config::Ctx;
use crate::inventory::{InventoryPollingService, InventorySnapshot};
use crate::offchain::order_status_ctx::{OrderStatusCtx, PollOrderStatusQueue};
use crate::offchain_order::OffchainOrder;
use crate::onchain::pyth::FeedIdCache;
use crate::onchain::raindex::RaindexService;
use crate::onchain_trade::OnChainTrade;
use crate::position::Position;
use crate::symbol::cache::SymbolCache;
use crate::threshold::ExecutionThreshold;
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

/// Everything the [`ConductorBuilder`] needs to construct a running
/// [`Conductor`].
pub(crate) struct ConductorCtx<P, E> {
    pub(crate) ctx: Ctx,
    pub(crate) cache: SymbolCache,
    pub(crate) provider: P,
    pub(crate) executor: E,
    pub(crate) execution_threshold: ExecutionThreshold,
    pub(crate) frameworks: CqrsFrameworks,
    pub(crate) poll_notify: Arc<tokio::sync::Notify>,
    pub(crate) pool: SqlitePool,
}

pub(crate) struct Initial;

pub(crate) struct WithExecutorMaintenance {
    executor_maintenance: Option<JoinHandle<()>>,
    rebalancer: Option<JoinHandle<()>>,
}

pub(crate) struct ConductorBuilder<P, E, State> {
    common: ConductorCtx<P, E>,
    job_queue: OrderFillJobQueue,
    state: State,
}

impl<P: Provider + Clone + Send + 'static, E: Executor + Clone + Send + 'static>
    ConductorBuilder<P, E, Initial>
{
    pub(crate) fn new(common: ConductorCtx<P, E>, job_queue: OrderFillJobQueue) -> Self {
        Self {
            common,
            job_queue,
            state: Initial,
        }
    }

    pub(crate) fn with_executor_maintenance(
        self,
        executor_maintenance: Option<JoinHandle<()>>,
    ) -> ConductorBuilder<P, E, WithExecutorMaintenance> {
        ConductorBuilder {
            common: self.common,
            job_queue: self.job_queue,
            state: WithExecutorMaintenance {
                executor_maintenance,
                rebalancer: None,
            },
        }
    }
}

impl<P, E> ConductorBuilder<P, E, WithExecutorMaintenance>
where
    P: Provider + Clone + Send + Sync + 'static,
    E: Executor + Clone + Send + Sync + 'static,
    OrderFillError: From<E::Error>,
{
    pub(crate) fn with_rebalancer(mut self, rebalancer: JoinHandle<()>) -> Self {
        self.state.rebalancer = Some(rebalancer);
        self
    }

    pub(crate) fn spawn(self) -> Conductor {
        info!("Starting conductor orchestration");

        let executor_maintenance = self.state.executor_maintenance;
        let rebalancer = self.state.rebalancer;

        log_optional_task_status("executor maintenance", executor_maintenance.is_some());
        log_optional_task_status("rebalancer", rebalancer.is_some());

        let order_owner = self.common.ctx.order_owner();
        let evm = ReadOnlyEvm::new(self.common.provider.clone());
        let raindex_service = Arc::new(RaindexService::new(
            evm,
            self.common.ctx.evm.orderbook,
            self.common.frameworks.vault_registry_projection.clone(),
            order_owner,
        ));

        let polling_service = InventoryPollingService::new(
            raindex_service,
            self.common.executor.clone(),
            self.common.frameworks.vault_registry.clone(),
            self.common.ctx.evm.orderbook,
            order_owner,
            self.common.frameworks.snapshot,
        );

        let inventory_poller = Some(spawn_inventory_poller(
            polling_service,
            std::time::Duration::from_secs(self.common.ctx.inventory_poll_interval),
            self.common.poll_notify.clone(),
        ));
        log_optional_task_status("inventory poller", inventory_poller.is_some());

        let poll_queue: PollOrderStatusQueue = apalis_sqlite::SqliteStorage::new(&self.common.pool);

        let order_status_ctx = Arc::new(OrderStatusCtx {
            executor: self.common.executor.clone(),
            offchain_order: self.common.frameworks.offchain_order.clone(),
            position: self.common.frameworks.position.clone(),
            poll_queue: Mutex::new(poll_queue.clone()),
            poll_interval: Duration::from_secs(self.common.ctx.order_polling_interval),
        });

        let position_checker = spawn_periodic_accumulated_position_check()
            .executor(self.common.executor.clone())
            .position(self.common.frameworks.position.clone())
            .position_projection(self.common.frameworks.position_projection.clone())
            .offchain_order(self.common.frameworks.offchain_order.clone())
            .poll_queue(Arc::new(Mutex::new(poll_queue.clone())))
            .execution_threshold(self.common.execution_threshold)
            .check_interval(std::time::Duration::from_secs(
                self.common.ctx.position_check_interval,
            ))
            .ctx(self.common.ctx.clone())
            .call();

        let trade_cqrs = super::TradeProcessingCqrs {
            onchain_trade: self.common.frameworks.onchain_trade,
            position: self.common.frameworks.position,
            position_projection: self.common.frameworks.position_projection,
            offchain_order: self.common.frameworks.offchain_order,
            execution_threshold: self.common.execution_threshold,
            assets: self.common.ctx.assets.clone(),
        };

        let order_fill_ctx = Arc::new(OrderFillCtx {
            ctx: self.common.ctx.clone(),
            cache: self.common.cache,
            feed_id_cache: FeedIdCache::default(),
            evm: ReadOnlyEvm::new(self.common.provider),
            cqrs: trade_cqrs,
            vault_registry: self.common.frameworks.vault_registry,
            executor: self.common.executor,
            poll_queue: Mutex::new(poll_queue.clone()),
        });

        let order_fill_monitor = OrderFillMonitor::new(
            self.common.ctx.evm.ws_rpc_url.clone(),
            self.common.ctx.evm.orderbook,
            self.job_queue.clone(),
            self.common.pool.clone(),
        );

        let supervisor = SupervisorBuilder::default()
            .with_task("order-fill-monitor", order_fill_monitor)
            .build()
            .run();

        let offchain_order_projection = self.common.frameworks.offchain_order_projection;
        let startup_poll_queue = Arc::new(Mutex::new(poll_queue.clone()));
        tokio::spawn(async move {
            super::reenqueue_submitted_orders(&offchain_order_projection, &startup_poll_queue)
                .await;
        });

        let job_queue = self.job_queue;
        let monitor = tokio::spawn(async move {
            let monitor = Monitor::new()
                .should_restart(|_ctx, _error, attempt| {
                    info!(attempt, "Restarting apalis worker");
                    true
                })
                .register(move |index| {
                    WorkerBuilder::new(format!("order-fill-worker-{index}"))
                        .backend(job_queue.clone())
                        .data(order_fill_ctx.clone())
                        .build(Job::<OrderFillCtx<P, E>>::work)
                })
                .register(move |index| {
                    WorkerBuilder::new(format!("poll-order-status-worker-{index}"))
                        .backend(poll_queue.clone())
                        .data(order_status_ctx.clone())
                        .build(Job::<OrderStatusCtx<E>>::work)
                });

            if let Err(monitor_error) = monitor.run().await {
                error!(%monitor_error, "Apalis monitor exited with error");
            }
        });

        Conductor {
            supervisor,
            monitor,
            position_checker,
            executor_maintenance,
            rebalancer,
            inventory_poller,
        }
    }
}

fn log_optional_task_status(task_name: &str, is_configured: bool) {
    if is_configured {
        info!("Started {task_name} task");
    } else {
        info!("{task_name} not configured", task_name = task_name);
    }
}
