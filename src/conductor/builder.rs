//! Constructs a fully-wired [`Conductor`] instance from its dependencies.

use alloy::providers::Provider;
use apalis::prelude::Monitor;
use apalis_core::worker::ext::circuit_breaker::config::CircuitBreakerConfig;
use sqlx::SqlitePool;
use std::sync::Arc;
use task_supervisor::SupervisorBuilder;
use tokio::task::JoinHandle;
use tracing::{debug, info};

use st0x_event_sorcery::{Projection, Store};
use st0x_evm::ReadOnlyEvm;
use st0x_execution::Executor;
use st0x_finance::{HasZero, Positive, Usd};

use super::exit::MonitorTaskError;
#[cfg(any(test, feature = "test-support"))]
use super::job::FailureInjector;
use super::job::{FAIL_STOP_RECOVERY_TIMEOUT, build_supervised_worker};
use super::monitor::order_fills::OrderFillMonitor;
use super::monitor::positions::PositionMonitor;
use super::{Conductor, spawn_inventory_poller};
use crate::config::Ctx;
use crate::inventory::{
    InventoryPollingService, InventorySnapshot, InventorySnapshotId, WalletPollingCtx,
};
use crate::offchain::order::handle_rejection::HandleOrderRejectionCtx;
use crate::offchain::order::poll_status::PollOrderStatusCtx;
use crate::offchain::order::reconcile_fill::ReconcileOrderFillCtx;
use crate::offchain::order::{
    HandleOrderRejection, HandleOrderRejectionJobQueue, OffchainOrder, PollOrderStatus,
    PollOrderStatusJobQueue, ReconcileOrderFill, ReconcileOrderFillJobQueue,
};
use crate::onchain::backfill::{BackfillJobQueue, BackfillRange};
use crate::onchain::pyth::FeedIdCache;
use crate::onchain::raindex::RaindexService;
use crate::onchain_trade::OnChainTrade;
use crate::position::Position;
use crate::symbol::cache::SymbolCache;
use crate::threshold::ExecutionThreshold;
use crate::tokenization::Tokenizer;
use crate::trading::offchain::hedge::{HedgeCtx, HedgeJobQueue, PlaceHedge};
use crate::trading::onchain::trade_accountant::{
    AccountForDexTrade, AccountantCtx, DexTradeAccountingJobQueue, TradeAccountingError,
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
pub(crate) fn spawn<Prov, Exec>(
    context: ConductorCtx<Prov, Exec>,
    job_queue: DexTradeAccountingJobQueue,
    backfill_queue: BackfillJobQueue,
    hedge_queue: HedgeJobQueue,
    poll_status_queue: PollOrderStatusJobQueue,
    reconcile_queue: ReconcileOrderFillJobQueue,
    rejection_queue: HandleOrderRejectionJobQueue,
    job_cleanup: JoinHandle<()>,
    executor_maintenance: Option<JoinHandle<()>>,
    rebalancer: Option<JoinHandle<()>>,
) -> Conductor
where
    Prov: Provider + Clone + Send + Sync + 'static,
    Exec: Executor + Clone + Send + Sync + 'static,
    TradeAccountingError: From<Exec::Error>,
    crate::offchain::order::JobError: From<Exec::Error>,
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

    let reserved_cash = context
        .ctx
        .assets
        .cash
        .as_ref()
        .and_then(|cash| cash.reserved)
        .map_or(Usd::ZERO, Positive::inner);

    let snapshot_id = InventorySnapshotId {
        orderbook: context.ctx.evm.orderbook,
        owner: order_owner,
    };

    let polling_service = InventoryPollingService::new(
        raindex_service,
        context.executor.clone(),
        context.frameworks.vault_registry.clone(),
        snapshot_id,
        context.frameworks.snapshot,
        context.wallet_polling,
        context.tokenizer,
        reserved_cash,
    );

    let inventory_poller = Some(spawn_inventory_poller(
        polling_service,
        std::time::Duration::from_secs(context.ctx.inventory_poll_interval),
        context.poll_notify.clone(),
    ));
    log_optional_task_status("inventory poller", inventory_poller.is_some());

    let poll_interval = context.ctx.order_polling_interval();
    info!("Constructing order-job context with poll interval: {poll_interval:?}");

    let poll_status_ctx = Arc::new(PollOrderStatusCtx {
        executor: context.executor.clone(),
        offchain_order_projection: context.frameworks.offchain_order_projection.clone(),
        poll_status_queue: poll_status_queue.clone(),
        reconcile_queue: reconcile_queue.clone(),
        rejection_queue: rejection_queue.clone(),
        poll_interval,
    });

    let reconcile_ctx = Arc::new(ReconcileOrderFillCtx {
        offchain_order: context.frameworks.offchain_order.clone(),
        position: context.frameworks.position.clone(),
    });

    let rejection_ctx = Arc::new(HandleOrderRejectionCtx {
        offchain_order: context.frameworks.offchain_order.clone(),
        position: context.frameworks.position.clone(),
    });

    let counter_trade_submission_lock = Arc::new(tokio::sync::Mutex::new(()));

    let hedge_ctx = Arc::new(HedgeCtx {
        position: context.frameworks.position.clone(),
        offchain_order: context.frameworks.offchain_order.clone(),
        poll_status_queue: poll_status_queue.clone(),
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
        poll_status_queue: poll_status_queue.clone(),
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

    let monitor = spawn_apalis_monitor(MonitorWiring {
        accountant_ctx,
        hedge_ctx,
        poll_status_ctx,
        reconcile_ctx,
        rejection_ctx,
        job_queue,
        hedge_queue,
        backfill_queue,
        poll_status_queue,
        reconcile_queue,
        rejection_queue,
        #[cfg(any(test, feature = "test-support"))]
        failure_injector: context.failure_injector,
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

/// Owns every queue, ctx and toggle the apalis monitor needs. The wiring is
/// only meaningful for one call to [`spawn_apalis_monitor`], which moves the
/// whole struct into a `tokio::spawn` and registers each worker.
struct MonitorWiring<Prov, Exec>
where
    Prov: Provider + Clone + Send + Sync + 'static,
    Exec: Executor + Clone + Send + Sync + 'static,
    TradeAccountingError: From<Exec::Error>,
    crate::offchain::order::JobError: From<Exec::Error>,
{
    accountant_ctx: Arc<AccountantCtx<Prov, Exec>>,
    hedge_ctx: Arc<HedgeCtx>,
    poll_status_ctx: Arc<PollOrderStatusCtx<Exec>>,
    reconcile_ctx: Arc<ReconcileOrderFillCtx>,
    rejection_ctx: Arc<HandleOrderRejectionCtx>,
    job_queue: DexTradeAccountingJobQueue,
    hedge_queue: HedgeJobQueue,
    backfill_queue: BackfillJobQueue,
    poll_status_queue: PollOrderStatusJobQueue,
    reconcile_queue: ReconcileOrderFillJobQueue,
    rejection_queue: HandleOrderRejectionJobQueue,
    #[cfg(any(test, feature = "test-support"))]
    failure_injector: FailureInjector,
}

fn spawn_apalis_monitor<Prov, Exec>(
    wiring: MonitorWiring<Prov, Exec>,
) -> JoinHandle<Result<(), MonitorTaskError>>
where
    Prov: Provider + Clone + Send + Sync + 'static,
    Exec: Executor + Clone + Send + Sync + 'static,
    TradeAccountingError: From<Exec::Error>,
    crate::offchain::order::JobError: From<Exec::Error>,
{
    let MonitorWiring {
        accountant_ctx,
        hedge_ctx,
        poll_status_ctx,
        reconcile_ctx,
        rejection_ctx,
        job_queue,
        hedge_queue,
        backfill_queue,
        poll_status_queue,
        reconcile_queue,
        rejection_queue,
        #[cfg(any(test, feature = "test-support"))]
        failure_injector,
    } = wiring;

    #[cfg(any(test, feature = "test-support"))]
    let failure_injector_for_hedge = failure_injector.clone();
    #[cfg(any(test, feature = "test-support"))]
    let failure_injector_for_backfill = failure_injector.clone();
    #[cfg(any(test, feature = "test-support"))]
    let failure_injector_for_poll = failure_injector.clone();
    #[cfg(any(test, feature = "test-support"))]
    let failure_injector_for_reconcile = failure_injector.clone();
    #[cfg(any(test, feature = "test-support"))]
    let failure_injector_for_rejection = failure_injector.clone();
    let failure_notify = Arc::new(tokio::sync::Notify::new());
    let failure_notify_for_hedge = failure_notify.clone();
    let failure_notify_for_backfill = failure_notify.clone();
    let failure_notify_for_poll = failure_notify.clone();
    let failure_notify_for_reconcile = failure_notify.clone();
    let failure_notify_for_rejection = failure_notify.clone();
    let failure_notify_for_select = failure_notify.clone();

    let fail_stop = CircuitBreakerConfig::default()
        .with_failure_threshold(1)
        .with_recovery_timeout(FAIL_STOP_RECOVERY_TIMEOUT);
    let fail_stop_for_hedge = fail_stop.clone();
    let fail_stop_for_backfill = fail_stop.clone();
    let fail_stop_for_poll = fail_stop.clone();
    let fail_stop_for_reconcile = fail_stop.clone();
    let fail_stop_for_rejection = fail_stop.clone();

    let accountant_ctx_for_backfill = accountant_ctx.clone();

    tokio::spawn(async move {
        let apalis_monitor = Monitor::new()
            .should_restart(|_ctx, _error, _attempt| false)
            .register(move |index| {
                build_supervised_worker!(
                    ::<AccountantCtx<Prov, Exec>, AccountForDexTrade>,
                    index,
                    job_queue.clone(),
                    accountant_ctx.clone(),
                    fail_stop.clone(),
                    failure_notify.clone(),
                    #[cfg(any(test, feature = "test-support"))]
                    failure_injector.clone(),
                )
            })
            .register(move |index| {
                build_supervised_worker!(
                    ::<HedgeCtx, PlaceHedge>,
                    index,
                    hedge_queue.clone(),
                    hedge_ctx.clone(),
                    fail_stop_for_hedge.clone(),
                    failure_notify_for_hedge.clone(),
                    #[cfg(any(test, feature = "test-support"))]
                    failure_injector_for_hedge.clone(),
                )
            })
            .register(move |index| {
                build_supervised_worker!(
                    ::<AccountantCtx<Prov, Exec>, BackfillRange>,
                    index,
                    backfill_queue.clone(),
                    accountant_ctx_for_backfill.clone(),
                    fail_stop_for_backfill.clone(),
                    failure_notify_for_backfill.clone(),
                    #[cfg(any(test, feature = "test-support"))]
                    failure_injector_for_backfill.clone(),
                )
            })
            .register(move |index| {
                build_supervised_worker!(
                    ::<PollOrderStatusCtx<Exec>, PollOrderStatus>,
                    index,
                    poll_status_queue.clone(),
                    poll_status_ctx.clone(),
                    fail_stop_for_poll.clone(),
                    failure_notify_for_poll.clone(),
                    #[cfg(any(test, feature = "test-support"))]
                    failure_injector_for_poll.clone(),
                )
            })
            .register(move |index| {
                build_supervised_worker!(
                    ::<ReconcileOrderFillCtx, ReconcileOrderFill>,
                    index,
                    reconcile_queue.clone(),
                    reconcile_ctx.clone(),
                    fail_stop_for_reconcile.clone(),
                    failure_notify_for_reconcile.clone(),
                    #[cfg(any(test, feature = "test-support"))]
                    failure_injector_for_reconcile.clone(),
                )
            })
            .register(move |index| {
                build_supervised_worker!(
                    ::<HandleOrderRejectionCtx, HandleOrderRejection>,
                    index,
                    rejection_queue.clone(),
                    rejection_ctx.clone(),
                    fail_stop_for_rejection.clone(),
                    failure_notify_for_rejection.clone(),
                    #[cfg(any(test, feature = "test-support"))]
                    failure_injector_for_rejection.clone(),
                )
            });

        tokio::select! {
            biased;
            () = failure_notify_for_select.notified() => Err(MonitorTaskError::TerminalJobFailure),
            result = apalis_monitor.run() => match result {
                Ok(()) => Err(MonitorTaskError::UnexpectedExit { source: None }),
                Err(source) => Err(MonitorTaskError::UnexpectedExit { source: Some(source) }),
            },
        }
    })
}

fn log_optional_task_status(task_name: &str, is_configured: bool) {
    if is_configured {
        info!("Started {task_name} task");
    } else {
        debug!("{task_name} not configured", task_name = task_name);
    }
}
