//! Typestate builder for constructing a fully-wired Conductor instance.

use std::sync::Arc;

use alloy::providers::Provider;
use apalis::prelude::{Monitor, WorkerBuilder};
use task_supervisor::SupervisorBuilder;
use tokio::task::JoinHandle;
use tracing::{error, info};

use st0x_event_sorcery::{Projection, Store};
use st0x_evm::ReadOnlyEvm;
use st0x_execution::Executor;
use st0x_finance::{HasZero, Positive, Usd};

use super::job::work;
use super::order_fill_monitor::{DexEventStreams, OrderFillMonitor};
use super::{
    Conductor, spawn_inventory_poller, spawn_order_poller,
    spawn_periodic_accumulated_position_check,
};
use crate::config::Ctx;
use crate::inventory::{
    InventoryPollingService, InventorySnapshot, InventorySnapshotId, WalletPollingConfig,
};
use crate::offchain_order::OffchainOrder;
use crate::onchain::pyth::FeedIdCache;
use crate::onchain::raindex::RaindexService;
use crate::onchain_trade::OnChainTrade;
use crate::position::Position;
use crate::symbol::cache::SymbolCache;
use crate::threshold::ExecutionThreshold;
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

/// Everything the [`ConductorBuilder`] needs to construct a running
/// [`Conductor`].
pub(crate) struct ConductorCtx<Prov, Exec> {
    pub(crate) ctx: Ctx,
    pub(crate) cache: SymbolCache,
    pub(crate) provider: Prov,
    pub(crate) executor: Exec,
    pub(crate) execution_threshold: ExecutionThreshold,
    pub(crate) frameworks: CqrsFrameworks,
    pub(crate) poll_notify: Arc<tokio::sync::Notify>,
    pub(crate) wallet_polling: WalletPollingConfig,
}

pub(crate) struct Initial;

pub(crate) struct WithExecutorMaintenance {
    executor_maintenance: Option<JoinHandle<()>>,
    rebalancer: Option<JoinHandle<()>>,
}

pub(crate) struct ConductorBuilder<Prov, Exec, State> {
    common: ConductorCtx<Prov, Exec>,
    job_queue: DexTradeAccountingJobQueue,
    dex_streams: DexEventStreams,
    state: State,
}

impl<Prov: Provider + Clone + Send + 'static, Exec: Executor + Clone + Send + 'static>
    ConductorBuilder<Prov, Exec, Initial>
{
    pub(crate) fn new(
        common: ConductorCtx<Prov, Exec>,
        job_queue: DexTradeAccountingJobQueue,
        dex_streams: DexEventStreams,
    ) -> Self {
        Self {
            common,
            job_queue,
            dex_streams,
            state: Initial,
        }
    }

    pub(crate) fn with_executor_maintenance(
        self,
        executor_maintenance: Option<JoinHandle<()>>,
    ) -> ConductorBuilder<Prov, Exec, WithExecutorMaintenance> {
        ConductorBuilder {
            common: self.common,
            job_queue: self.job_queue,
            dex_streams: self.dex_streams,
            state: WithExecutorMaintenance {
                executor_maintenance,
                rebalancer: None,
            },
        }
    }
}

impl<Prov, Exec> ConductorBuilder<Prov, Exec, WithExecutorMaintenance>
where
    Prov: Provider + Clone + Send + Sync + 'static,
    Exec: Executor + Clone + Send + Sync + 'static,
    TradeAccountingError: From<Exec::Error>,
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

        let reserved_cash = self
            .common
            .ctx
            .assets
            .cash
            .as_ref()
            .and_then(|cash| cash.reserved)
            .map_or(Usd::ZERO, Positive::inner);

        let snapshot_id = InventorySnapshotId {
            orderbook: self.common.ctx.evm.orderbook,
            owner: order_owner,
        };

        let polling_service = InventoryPollingService::new(
            raindex_service,
            self.common.executor.clone(),
            self.common.frameworks.vault_registry.clone(),
            snapshot_id,
            self.common.frameworks.snapshot,
            self.common.wallet_polling,
            reserved_cash,
        );

        let inventory_poller = Some(spawn_inventory_poller(
            polling_service,
            std::time::Duration::from_secs(self.common.ctx.inventory_poll_interval),
            self.common.poll_notify.clone(),
        ));
        log_optional_task_status("inventory poller", inventory_poller.is_some());

        let order_poller = spawn_order_poller(
            &self.common.ctx,
            self.common.executor.clone(),
            (*self.common.frameworks.offchain_order_projection).clone(),
            self.common.frameworks.offchain_order.clone(),
            self.common.frameworks.position.clone(),
        );

        let position_checker = spawn_periodic_accumulated_position_check()
            .executor(self.common.executor.clone())
            .position(self.common.frameworks.position.clone())
            .position_projection(self.common.frameworks.position_projection.clone())
            .offchain_order(self.common.frameworks.offchain_order.clone())
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

        let accountant_ctx = Arc::new(AccountantCtx {
            orderbook: self.common.ctx.evm.orderbook,
            ctx: self.common.ctx.clone(),
            cache: self.common.cache,
            feed_id_cache: FeedIdCache::default(),
            evm: ReadOnlyEvm::new(self.common.provider),
            cqrs: trade_cqrs,
            vault_registry: self.common.frameworks.vault_registry,
            executor: self.common.executor,
        });

        let order_fill_monitor = OrderFillMonitor::new(
            self.common.ctx.evm.ws_rpc_url.clone(),
            self.common.ctx.evm.orderbook,
            self.job_queue.clone(),
            self.dex_streams,
        );

        let supervisor = SupervisorBuilder::default()
            .with_task("order-fill-monitor", order_fill_monitor)
            .build()
            .run();

        let job_queue = self.job_queue;
        let monitor = tokio::spawn(async move {
            let monitor = Monitor::new()
                .should_restart(|_ctx, _error, attempt| {
                    info!(attempt, "Restarting order fill worker");
                    true
                })
                .register(move |index| {
                    WorkerBuilder::new(format!("order-fill-worker-{index}"))
                        .backend(job_queue.clone())
                        .data(accountant_ctx.clone())
                        .build(work::<AccountantCtx<Prov, Exec>, _>)
                });

            if let Err(monitor_error) = monitor.run().await {
                error!(%monitor_error, "Apalis monitor exited with error");
            }
        });

        Conductor {
            supervisor,
            monitor,
            order_poller,
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
