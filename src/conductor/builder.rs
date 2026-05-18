//! Constructs a fully-wired [`Conductor`] instance from its dependencies.

use alloy::providers::Provider;
use apalis::prelude::Monitor;
use apalis_core::worker::ext::circuit_breaker::config::CircuitBreakerConfig;
use sqlx::SqlitePool;
use std::collections::HashSet;
use std::sync::Arc;
use task_supervisor::SupervisorBuilder;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use st0x_config::{Ctx, ExecutionThreshold};
use st0x_event_sorcery::{Projection, Store};
use st0x_evm::ReadOnlyEvm;
use st0x_execution::Executor;
use st0x_finance::{HasZero, Positive, Usd};

use super::Conductor;
use super::exit::MonitorTaskError;
#[cfg(any(test, feature = "test-support"))]
use super::job::FailureInjector;
use super::job::{FAIL_STOP_RECOVERY_TIMEOUT, build_supervised_worker};
use super::monitor::executor_maintenance::ExecutorMaintenance;
use super::monitor::inventory::InventoryMonitor;
use super::monitor::order_fills::OrderFillMonitor;
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
use crate::position_check::{CheckPositions, CheckPositionsCtx, CheckPositionsJobQueue};
use crate::rebalancing::{
    EquityRebalancingCheck, EquityRebalancingCheckScheduler, RebalancingService,
    UsdcRebalancingCheck, UsdcRebalancingCheckScheduler,
};
use crate::symbol::cache::SymbolCache;
use crate::tokenization::Tokenizer;
use crate::trading::offchain::hedge::{HedgeCtx, HedgeJobQueue, PlaceHedge};
use crate::trading::onchain::trade_accountant::{
    AccountForDexTrade, AccountantCtx, DexTradeAccountingJobQueue, TradeAccountingError,
};
use crate::vault_registry::{
    SeedVaultRegistry, SeedVaultRegistryCtx, SeedVaultRegistryJobQueue, VaultRegistry,
};
use crate::wrapped_equity_recovery::{
    WrappedEquityRecoveryCtx, WrappedEquityRecoveryJob, WrappedEquityRecoveryJobQueue,
};

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
    pub(crate) wallet_polling: Option<WalletPollingCtx>,
    pub(crate) tokenizer: Option<Arc<dyn Tokenizer>>,
    pub(crate) shutdown_token: CancellationToken,
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
    check_positions_queue: CheckPositionsJobQueue,
    wrapped_equity_recovery_queue: WrappedEquityRecoveryJobQueue,
    wrapped_equity_recovery_ctx: Option<Arc<WrappedEquityRecoveryCtx>>,
    equity_check_scheduler: EquityRebalancingCheckScheduler,
    usdc_check_scheduler: UsdcRebalancingCheckScheduler,
    rebalancing_service: Option<Arc<RebalancingService>>,
    seed_vault_registry_queue: SeedVaultRegistryJobQueue,
    seed_vault_registry_ctx: Arc<SeedVaultRegistryCtx>,
    job_cleanup: JoinHandle<()>,
    rebalancer: Option<JoinHandle<()>>,
    rebalancer_shutdown_token: CancellationToken,
) -> Conductor
where
    Prov: Provider + Clone + Send + Sync + 'static,
    Exec: Executor + Clone + Send + Sync + 'static,
    TradeAccountingError: From<Exec::Error>,
    crate::offchain::order::JobError: From<Exec::Error>,
{
    info!("Starting conductor orchestration");

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

    let configured_equity_symbols: HashSet<_> = context
        .ctx
        .assets
        .equities
        .symbols
        .keys()
        .filter(|symbol| {
            context.ctx.is_trading_enabled(symbol) || context.ctx.is_rebalancing_enabled(symbol)
        })
        .cloned()
        .collect();

    let wallet_polling = context.wallet_polling;
    let tokenizer = context.tokenizer;

    let mut polling_service = InventoryPollingService::new(
        raindex_service,
        context.executor.clone(),
        context.frameworks.vault_registry.clone(),
        snapshot_id,
        context.frameworks.snapshot,
        wallet_polling,
        tokenizer,
        reserved_cash,
    )
    .with_configured_equity_symbols(configured_equity_symbols);

    if let Some(rebalancing_service) = &rebalancing_service {
        polling_service =
            polling_service.with_pending_request_ownership(rebalancing_service.clone());
    }

    let polling_service = Arc::new(polling_service);

    let inventory_monitor = InventoryMonitor {
        poller: polling_service,
        interval: std::time::Duration::from_secs(context.ctx.inventory_poll_interval),
    };

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

    let check_positions_ctx = Arc::new(CheckPositionsCtx {
        executor: context.executor.clone(),
        position_projection: context.frameworks.position_projection.clone(),
        hedge_queue: hedge_queue.clone(),
        check_positions_queue: check_positions_queue.clone(),
        ctx: context.ctx.clone(),
        pool: context.pool.clone(),
        check_interval: std::time::Duration::from_secs(context.ctx.position_check_interval),
    });

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

    let maintenance_interval = context.executor.maintenance_interval();

    let accountant_ctx = Arc::new(AccountantCtx {
        orderbook: context.ctx.evm.orderbook,
        ctx: context.ctx.clone(),
        cache: context.cache,
        feed_id_cache: FeedIdCache::default(),
        evm: ReadOnlyEvm::new(context.provider),
        cqrs: trade_cqrs,
        vault_registry: context.frameworks.vault_registry,
        executor: context.executor.clone(),
        pool: context.pool.clone(),
        job_queue: job_queue.clone(),
    });

    let apalis_shutdown_token = CancellationToken::new();
    let apalis_shutdown_token_for_struct = apalis_shutdown_token.clone();

    let order_fill_monitor = OrderFillMonitor::new(
        context.ctx.evm.clone(),
        job_queue.clone(),
        backfill_queue.clone(),
        context.pool,
    );

    // Fail-fast: exit if any supervised task dies, relying on systemd restart for recovery.
    // In test builds, use aggressive timeouts so a flaky WebSocket doesn't
    // stall e2e tests for ~13 minutes of exponential backoff.
    let is_test = cfg!(any(test, feature = "test-support"));
    let mut supervisor_builder = SupervisorBuilder::default()
        .with_max_restart_attempts(if is_test { 2 } else { 10 })
        .with_max_backoff_exponent(if is_test { 2 } else { 8 })
        .with_base_restart_delay(std::time::Duration::from_secs(1))
        .with_dead_tasks_threshold(Some(0.0))
        .with_task("order-fill-monitor", order_fill_monitor)
        .with_task("inventory-monitor", inventory_monitor);

    log_optional_task_status("executor maintenance", maintenance_interval.is_some());

    if let Some(interval) = maintenance_interval {
        supervisor_builder = supervisor_builder.with_task(
            "executor-maintenance",
            ExecutorMaintenance::new(context.executor, interval),
        );
    }

    let supervisor = supervisor_builder.build().run();

    let monitor = MonitorWiring {
        accountant_ctx,
        hedge_ctx,
        poll_status_ctx,
        reconcile_ctx,
        rejection_ctx,
        check_positions_ctx,
        rebalancing_check_ctx: rebalancing_service,
        seed_vault_registry_ctx,
        job_queue,
        hedge_queue,
        backfill_queue,
        poll_status_queue,
        reconcile_queue,
        rejection_queue,
        check_positions_queue,
        wrapped_equity_recovery_queue,
        wrapped_equity_recovery_ctx,
        equity_check_scheduler,
        usdc_check_scheduler,
        apalis_shutdown_token,
        seed_vault_registry_queue,
        #[cfg(any(test, feature = "test-support"))]
        failure_injector: context.failure_injector,
    }
    .spawn_apalis_monitor();

    Conductor {
        supervisor,
        monitor,
        rebalancer,
        rebalancer_shutdown_token,
        job_cleanup,
        shutdown_token: context.shutdown_token,
        apalis_shutdown_token: apalis_shutdown_token_for_struct,
    }
}

/// Owns every queue, ctx and toggle the apalis monitor needs. The wiring is
/// only meaningful for one call to [`MonitorWiring::spawn_apalis_monitor`],
/// which moves the whole struct into a `tokio::spawn` and registers each
/// worker.
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
    check_positions_ctx: Arc<CheckPositionsCtx<Exec>>,
    rebalancing_check_ctx: Option<Arc<RebalancingService>>,
    seed_vault_registry_ctx: Arc<SeedVaultRegistryCtx>,
    job_queue: DexTradeAccountingJobQueue,
    hedge_queue: HedgeJobQueue,
    backfill_queue: BackfillJobQueue,
    poll_status_queue: PollOrderStatusJobQueue,
    reconcile_queue: ReconcileOrderFillJobQueue,
    rejection_queue: HandleOrderRejectionJobQueue,
    check_positions_queue: CheckPositionsJobQueue,
    wrapped_equity_recovery_queue: WrappedEquityRecoveryJobQueue,
    wrapped_equity_recovery_ctx: Option<Arc<WrappedEquityRecoveryCtx>>,
    equity_check_scheduler: EquityRebalancingCheckScheduler,
    usdc_check_scheduler: UsdcRebalancingCheckScheduler,
    apalis_shutdown_token: CancellationToken,
    seed_vault_registry_queue: SeedVaultRegistryJobQueue,
    #[cfg(any(test, feature = "test-support"))]
    failure_injector: FailureInjector,
}

impl<Prov, Exec> MonitorWiring<Prov, Exec>
where
    Prov: Provider + Clone + Send + Sync + 'static,
    Exec: Executor + Clone + Send + Sync + 'static,
    TradeAccountingError: From<Exec::Error>,
    crate::offchain::order::JobError: From<Exec::Error>,
{
    #[allow(clippy::too_many_lines)]
    fn spawn_apalis_monitor(self) -> JoinHandle<Result<(), MonitorTaskError>> {
        let Self {
            accountant_ctx,
            hedge_ctx,
            poll_status_ctx,
            reconcile_ctx,
            rejection_ctx,
            check_positions_ctx,
            rebalancing_check_ctx,
            seed_vault_registry_ctx,
            job_queue,
            hedge_queue,
            backfill_queue,
            poll_status_queue,
            reconcile_queue,
            rejection_queue,
            check_positions_queue,
            wrapped_equity_recovery_queue,
            wrapped_equity_recovery_ctx,
            equity_check_scheduler,
            usdc_check_scheduler,
            apalis_shutdown_token,
            seed_vault_registry_queue,
            #[cfg(any(test, feature = "test-support"))]
            failure_injector,
        } = self;

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
        #[cfg(any(test, feature = "test-support"))]
        let failure_injector_for_equity_rebalancing_check = failure_injector.clone();
        #[cfg(any(test, feature = "test-support"))]
        let failure_injector_for_usdc_rebalancing_check = failure_injector.clone();
        #[cfg(any(test, feature = "test-support"))]
        let failure_injector_for_seed_vault_registry = failure_injector.clone();
        #[cfg(any(test, feature = "test-support"))]
        let failure_injector_for_wrapped_equity_recovery = failure_injector.clone();
        #[cfg(any(test, feature = "test-support"))]
        let failure_injector_for_check_positions = failure_injector.clone();
        let failure_notify = Arc::new(tokio::sync::Notify::new());
        let failure_notify_for_hedge = failure_notify.clone();
        let failure_notify_for_backfill = failure_notify.clone();
        let failure_notify_for_poll = failure_notify.clone();
        let failure_notify_for_reconcile = failure_notify.clone();
        let failure_notify_for_rejection = failure_notify.clone();
        let failure_notify_for_equity_rebalancing_check = failure_notify.clone();
        let failure_notify_for_usdc_rebalancing_check = failure_notify.clone();
        let failure_notify_for_seed_vault_registry = failure_notify.clone();
        let failure_notify_for_wrapped_equity_recovery = failure_notify.clone();
        let failure_notify_for_check_positions = failure_notify.clone();
        let failure_notify_for_select = failure_notify.clone();

        let fail_stop = CircuitBreakerConfig::default()
            .with_failure_threshold(1)
            .with_recovery_timeout(FAIL_STOP_RECOVERY_TIMEOUT);
        let fail_stop_for_hedge = fail_stop.clone();
        let fail_stop_for_backfill = fail_stop.clone();
        let fail_stop_for_poll = fail_stop.clone();
        let fail_stop_for_reconcile = fail_stop.clone();
        let fail_stop_for_rejection = fail_stop.clone();
        let fail_stop_for_equity_rebalancing_check = fail_stop.clone();
        let fail_stop_for_usdc_rebalancing_check = fail_stop.clone();
        let fail_stop_for_seed_vault_registry = fail_stop.clone();
        let fail_stop_for_wrapped_equity_recovery = fail_stop.clone();
        let fail_stop_for_check_positions = fail_stop.clone();

        let accountant_ctx_for_backfill = accountant_ctx.clone();

        tokio::spawn(async move {
            let monitor = Monitor::new()
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
                })
                .register(move |index| {
                    build_supervised_worker!(
                        ::<SeedVaultRegistryCtx, SeedVaultRegistry>,
                        index,
                        seed_vault_registry_queue.clone(),
                        seed_vault_registry_ctx.clone(),
                        fail_stop_for_seed_vault_registry.clone(),
                        failure_notify_for_seed_vault_registry.clone(),
                        #[cfg(any(test, feature = "test-support"))]
                        failure_injector_for_seed_vault_registry.clone(),
                    )
                })
                .register(move |index| {
                    build_supervised_worker!(
                        ::<CheckPositionsCtx<Exec>, CheckPositions>,
                        index,
                        check_positions_queue.clone(),
                        check_positions_ctx.clone(),
                        fail_stop_for_check_positions.clone(),
                        failure_notify_for_check_positions.clone(),
                        #[cfg(any(test, feature = "test-support"))]
                        failure_injector_for_check_positions.clone(),
                    )
                });

            let apalis_monitor = if let Some(rebalancing_service) = rebalancing_check_ctx {
                let equity_service = Arc::clone(&rebalancing_service);
                let usdc_service = rebalancing_service;
                let equity_queue = equity_check_scheduler.queue().clone();
                let usdc_queue = usdc_check_scheduler.queue().clone();
                monitor
                    .register(move |index| {
                        build_supervised_worker!(
                            ::<RebalancingService, EquityRebalancingCheck>,
                            index,
                            equity_queue.clone(),
                            equity_service.clone(),
                            fail_stop_for_equity_rebalancing_check.clone(),
                            failure_notify_for_equity_rebalancing_check.clone(),
                            #[cfg(any(test, feature = "test-support"))]
                            failure_injector_for_equity_rebalancing_check.clone(),
                        )
                    })
                    .register(move |index| {
                        build_supervised_worker!(
                            ::<RebalancingService, UsdcRebalancingCheck>,
                            index,
                            usdc_queue.clone(),
                            usdc_service.clone(),
                            fail_stop_for_usdc_rebalancing_check.clone(),
                            failure_notify_for_usdc_rebalancing_check.clone(),
                            #[cfg(any(test, feature = "test-support"))]
                            failure_injector_for_usdc_rebalancing_check.clone(),
                        )
                    })
            } else {
                monitor
            };

            let apalis_monitor = register_wrapped_equity_recovery_worker(
                apalis_monitor,
                wrapped_equity_recovery_ctx,
                wrapped_equity_recovery_queue,
                fail_stop_for_wrapped_equity_recovery,
                failure_notify_for_wrapped_equity_recovery,
                #[cfg(any(test, feature = "test-support"))]
                failure_injector_for_wrapped_equity_recovery,
            );

            let is_draining = apalis_shutdown_token.clone();

            let shutdown_signal = async {
                apalis_shutdown_token.cancelled().await;
                Ok(())
            };

            // No inner shutdown_timeout — the outer 30s timeout in
            // drain_bot_with_timeout owns force-abort. This avoids a semantic
            // gap where apalis reports Ok(()) for both clean drain and timeout,
            // making the two cases indistinguishable.

            tokio::select! {
                biased;
                // During drain, suppress terminal job failures — let remaining
                // workers finish instead of short-circuiting the drain.
                () = failure_notify_for_select.notified(),
                    if !is_draining.is_cancelled() =>
                {
                    Err(MonitorTaskError::TerminalJobFailure)
                }
                result = apalis_monitor.run_with_signal(shutdown_signal) => match result {
                    Ok(()) => Ok(()),
                    Err(source) => Err(MonitorTaskError::UnexpectedExit { source: Some(source) }),
                },
            }
        })
    }
}

fn log_optional_task_status(task_name: &str, is_configured: bool) {
    if is_configured {
        info!("Started {task_name} task");
    } else {
        debug!("{task_name} not configured", task_name = task_name);
    }
}

/// Conditionally registers the wrapped-equity recovery worker against the
/// apalis monitor. Extracted because this is the only `Option`-gated worker
/// registration and inlining the let-else + debug log keeps
/// `spawn_apalis_monitor` over the cognitive-complexity limit.
fn register_wrapped_equity_recovery_worker(
    monitor: Monitor,
    recovery_ctx: Option<Arc<WrappedEquityRecoveryCtx>>,
    recovery_queue: WrappedEquityRecoveryJobQueue,
    fail_stop: CircuitBreakerConfig,
    failure_notify: Arc<tokio::sync::Notify>,
    #[cfg(any(test, feature = "test-support"))] failure_injector: FailureInjector,
) -> Monitor {
    let Some(recovery_ctx) = recovery_ctx else {
        debug!(
            "Wrapped-equity recovery worker not registered: rebalancing disabled \
             (no recovery ctx). Detected wtSTOCK on the bot wallet will not be recovered."
        );
        return monitor;
    };

    monitor.register(move |index| {
        build_supervised_worker!(
            ::<WrappedEquityRecoveryCtx, WrappedEquityRecoveryJob>,
            index,
            recovery_queue.clone(),
            recovery_ctx.clone(),
            fail_stop.clone(),
            failure_notify.clone(),
            #[cfg(any(test, feature = "test-support"))]
            failure_injector.clone(),
        )
    })
}
