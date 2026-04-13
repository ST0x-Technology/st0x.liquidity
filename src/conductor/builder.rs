//! Constructs a fully-wired [`Conductor`] instance from its dependencies.

use alloy::providers::Provider;
use apalis::prelude::{Monitor, WorkerBuilder};
use std::sync::Arc;
use task_supervisor::SupervisorBuilder;
use tokio::task::JoinHandle;
use tracing::{error, info};

use st0x_event_sorcery::{Projection, Store};
use st0x_evm::ReadOnlyEvm;
use st0x_execution::Executor;

use super::job::work;
use super::order_fill_monitor::{DexEventStreams, OrderFillMonitor};
use super::{
    Conductor, spawn_inventory_poller, spawn_order_poller,
    spawn_periodic_accumulated_position_check,
};
use crate::config::Ctx;
use crate::inventory::{InventoryPollingService, InventorySnapshot, WalletPollingCtx};
use crate::offchain_order::OffchainOrder;
use crate::onchain::pyth::FeedIdCache;
use crate::onchain::raindex::RaindexService;
use crate::onchain_trade::OnChainTrade;
use crate::position::Position;
use crate::symbol::cache::SymbolCache;
use crate::threshold::ExecutionThreshold;
use crate::tokenization::Tokenizer;
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
    pub(crate) poll_notify: Arc<tokio::sync::Notify>,
    pub(crate) wallet_polling: Option<WalletPollingCtx>,
    pub(crate) tokenizer: Option<Arc<dyn Tokenizer>>,
}

/// Wires all runtime components and returns a running [`Conductor`].
#[bon::builder]
pub(crate) fn spawn<Prov, Exec>(
    context: ConductorCtx<Prov, Exec>,
    job_queue: DexTradeAccountingJobQueue,
    dex_streams: DexEventStreams,
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

    let order_poller_handle = spawn_order_poller(
        &context.ctx,
        context.executor.clone(),
        (*context.frameworks.offchain_order_projection).clone(),
        context.frameworks.offchain_order.clone(),
        context.frameworks.position.clone(),
    );

    let counter_trade_submission_lock = Arc::new(tokio::sync::Mutex::new(()));

    let position_checker_handle = spawn_periodic_accumulated_position_check()
        .executor(context.executor.clone())
        .position(context.frameworks.position.clone())
        .position_projection(context.frameworks.position_projection.clone())
        .offchain_order(context.frameworks.offchain_order.clone())
        .counter_trade_submission_lock(counter_trade_submission_lock.clone())
        .execution_threshold(context.execution_threshold)
        .check_interval(std::time::Duration::from_secs(
            context.ctx.position_check_interval,
        ))
        .ctx(context.ctx.clone())
        .call();

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
    });

    let order_fill_monitor = OrderFillMonitor::new(
        context.ctx.evm.ws_rpc_url.clone(),
        context.ctx.evm.orderbook,
        job_queue.clone(),
        dex_streams,
    );

    // NOTE: latest versions of apalias are adding tooling for long-running tasks too,
    // so we will be able to get rid of this task-supervisor inconsistency
    let supervisor = SupervisorBuilder::default()
        .with_task("order-fill-monitor", order_fill_monitor)
        .build()
        .run();

    let monitor = tokio::spawn(async move {
        let apalis_monitor = Monitor::new()
            .should_restart(|_ctx, _error, attempt| {
                info!(attempt, "Restarting order fill worker");
                true
            })
            .register(move |index| {
                WorkerBuilder::new(format!("order-fill-worker-{index}"))
                    .backend(job_queue.clone().into_storage())
                    .data(accountant_ctx.clone())
                    .build(work::<AccountantCtx<Prov, Exec>, _>)
            });

        tokio::select! {
            result = apalis_monitor.run() => {
                if let Err(monitor_error) = result {
                    error!(%monitor_error, "Apalis monitor exited with error");
                }
            }
            _ = order_poller_handle => {
                error!("Order poller exited unexpectedly");
            }
            _ = position_checker_handle => {
                error!("Position checker exited unexpectedly");
            }
        }
    });

    Conductor {
        supervisor,
        monitor,
        executor_maintenance,
        rebalancer,
        inventory_poller,
    }
}

fn log_optional_task_status(task_name: &str, is_configured: bool) {
    if is_configured {
        info!("Started {task_name} task");
    } else {
        info!("{task_name} not configured", task_name = task_name);
    }
}
