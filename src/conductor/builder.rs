//! Constructs a fully-wired [`Conductor`] instance from its dependencies.

use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use apalis::prelude::Monitor;
use sqlx::SqlitePool;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use task_supervisor::SupervisorBuilder;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use st0x_config::{BrokerCtx, Ctx, ExecutionThreshold};
use st0x_event_sorcery::{Projection, Store};
use st0x_evm::ReadOnlyEvm;
use st0x_execution::{Executor, Symbol};
use st0x_finance::{HasZero, Positive, Usd};
use st0x_raindex::RaindexService;
use st0x_registry::SymbolCache;
use st0x_tokenization::Tokenizer;

use super::Conductor;
use super::exit::MonitorTaskError;
#[cfg(any(test, feature = "test-support"))]
use super::job::FailureInjector;
use super::job::{
    WORKER_CIRCUIT_POLICY, build_best_effort_worker, build_supervised_worker, build_worker_inner,
};
use super::monitor::executor_maintenance::ExecutorMaintenance;
use super::monitor::gas::{GasMonitor, ProviderBalanceReader};
use super::monitor::order_fills::OrderFillMonitor;
use crate::alerts::{Notifier, TelegramNotifier};
use crate::inventory::job::{
    InventoryPollingJobCtx, InventoryPollingJobQueues, PollBaseWalletUnwrappedEquity,
    PollBaseWalletUsdc, PollBaseWalletWrappedEquity, PollEthereumWalletUsdc, PollInflightEquity,
    PollOffchainInventory, PollOnchainEquity, PollOnchainUsdc,
};
use crate::inventory::{
    InventoryPollingService, InventorySnapshot, InventorySnapshotId, WalletPollingCtx,
};
use crate::offchain::order::handle_rejection::HandleOrderRejectionCtx;
use crate::offchain::order::poll_status::PollOrderStatusCtx;
use crate::offchain::order::reconcile_fill::ReconcileOrderFillCtx;
use crate::offchain::order::{ExecutorOrderPlacer, OrderPlacer};
use crate::offchain::order::{
    HandleOrderRejection, HandleOrderRejectionJobQueue, OffchainOrder, PollOrderStatus,
    PollOrderStatusJobQueue, ReconcileOrderFill, ReconcileOrderFillJobQueue,
};
use crate::onchain::backfill::{BackfillJobQueue, BackfillRange};
use crate::onchain::pyth::PythFeedIds;
use crate::onchain_trade::OnChainTrade;
use crate::position::Position;
use crate::position_check::{CheckPositions, CheckPositionsCtx, CheckPositionsJobQueue};
use crate::rebalancing::equity::{
    ResumeTokenizationAggregate, ResumeTokenizationCtx, ResumeTokenizationJobQueue,
    TransferEquityToHedging, TransferEquityToHedgingCtx, TransferEquityToHedgingJobQueue,
    TransferEquityToMarketMaking, TransferEquityToMarketMakingCtx,
    TransferEquityToMarketMakingJobQueue,
};
use crate::rebalancing::usdc::{
    TransferUsdcToHedging, TransferUsdcToHedgingCtx, TransferUsdcToHedgingJobQueue,
    TransferUsdcToMarketMaking, TransferUsdcToMarketMakingCtx, TransferUsdcToMarketMakingJobQueue,
};
use crate::rebalancing::{
    EquityRebalancingCheck, EquityRebalancingCheckScheduler, RebalancingService,
    UsdcRebalancingCheck, UsdcRebalancingCheckScheduler,
};
use crate::trading::offchain::hedge::{HedgeCtx, HedgeJobQueue, PlaceHedge};
use crate::trading::onchain::trade_accountant::{
    AccountForDexTrade, AccountantCtx, DexTradeAccountingJobQueue, TradeAccountingError,
};
use crate::unwrapped_equity_recovery::{
    UnwrappedEquityRecoveryCtx, UnwrappedEquityRecoveryJob, UnwrappedEquityRecoveryJobQueue,
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
    pub(crate) notifier: Arc<dyn Notifier>,
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) failure_injector: FailureInjector,
}

/// The configured equity symbols plus the equity/USDC vaults the inventory
/// poller watches, derived from the assets config.
struct ConfiguredInventoryVaults {
    equity_symbols: HashSet<Symbol>,
    equity_vaults: BTreeMap<Address, BTreeSet<B256>>,
    usdc_vaults: Option<BTreeSet<B256>>,
}

fn configured_inventory_vaults(ctx: &Ctx) -> ConfiguredInventoryVaults {
    let equity_symbols: HashSet<_> = ctx
        .assets
        .equities
        .symbols
        .keys()
        .filter(|symbol| {
            ctx.assets.is_trading_enabled(symbol) || ctx.assets.is_rebalancing_enabled(symbol)
        })
        .cloned()
        .collect();

    let mut equity_vaults: BTreeMap<Address, BTreeSet<B256>> = BTreeMap::new();
    for equity_config in ctx.assets.equities.symbols.values() {
        equity_vaults
            .entry(equity_config.tokenized_equity_derivative)
            .or_default()
            .extend(equity_config.vault_ids.iter().copied());
    }

    let usdc_vaults = ctx
        .assets
        .cash
        .as_ref()
        .map(|cash| cash.vault_ids.iter().copied().collect());

    ConfiguredInventoryVaults {
        equity_symbols,
        equity_vaults,
        usdc_vaults,
    }
}

/// Wires all runtime components and returns a running [`Conductor`].
// Straight-line builder that wires every runtime context; splitting it would
// scatter the wiring across helpers without reducing complexity.
#[allow(clippy::too_many_lines)]
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
    inventory_polling_queues: InventoryPollingJobQueues,
    wrapped_equity_recovery_queue: WrappedEquityRecoveryJobQueue,
    wrapped_equity_recovery_ctx: Option<Arc<WrappedEquityRecoveryCtx>>,
    unwrapped_equity_recovery_queue: UnwrappedEquityRecoveryJobQueue,
    unwrapped_equity_recovery_ctx: Option<Arc<UnwrappedEquityRecoveryCtx>>,
    equity_check_scheduler: EquityRebalancingCheckScheduler,
    usdc_check_scheduler: UsdcRebalancingCheckScheduler,
    transfer_usdc_to_hedging_queue: TransferUsdcToHedgingJobQueue,
    transfer_usdc_to_hedging_ctx: Option<Arc<TransferUsdcToHedgingCtx>>,
    transfer_usdc_to_market_making_queue: TransferUsdcToMarketMakingJobQueue,
    transfer_usdc_to_market_making_ctx: Option<Arc<TransferUsdcToMarketMakingCtx>>,
    transfer_equity_to_market_making_queue: TransferEquityToMarketMakingJobQueue,
    transfer_equity_to_market_making_ctx: Option<Arc<TransferEquityToMarketMakingCtx>>,
    transfer_equity_to_hedging_queue: TransferEquityToHedgingJobQueue,
    transfer_equity_to_hedging_ctx: Option<Arc<TransferEquityToHedgingCtx>>,
    rebalancing_service: Option<Arc<RebalancingService>>,
    seed_vault_registry_queue: SeedVaultRegistryJobQueue,
    seed_vault_registry_ctx: Arc<SeedVaultRegistryCtx>,
    resume_tokenization_queue: ResumeTokenizationJobQueue,
    resume_tokenization_ctx: Option<Arc<ResumeTokenizationCtx>>,
    job_cleanup: JoinHandle<()>,
    telemetry_writer: JoinHandle<()>,
) -> Conductor
where
    Prov: Provider + Clone + Send + Sync + 'static,
    Exec: Executor + Clone + Send + Sync + 'static,
    TradeAccountingError: From<Exec::Error>,
    crate::offchain::order::JobError: From<Exec::Error>,
{
    info!("Starting conductor orchestration");

    let order_owner = context.ctx.order_owner();
    let evm = ReadOnlyEvm::new(context.provider.clone());
    let raindex_service = Arc::new(RaindexService::new(
        evm,
        crate::onchain::raindex_contracts(&context.ctx.evm),
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

    let ConfiguredInventoryVaults {
        equity_symbols: configured_equity_symbols,
        equity_vaults: configured_equity_vaults,
        usdc_vaults: configured_usdc_vaults,
    } = configured_inventory_vaults(&context.ctx);

    let wallet_polling = context.wallet_polling;
    let tokenizer = context.tokenizer;

    let mut polling_service = InventoryPollingService::new(
        raindex_service,
        context.executor.clone(),
        context.frameworks.vault_registry.clone(),
        snapshot_id,
        context.ctx.vault_owner(),
        context.frameworks.snapshot,
        wallet_polling,
        tokenizer,
        reserved_cash,
    )
    .with_configured_equity_symbols(configured_equity_symbols)
    .with_configured_vaults(configured_equity_vaults, configured_usdc_vaults);

    if let Some(rebalancing_service) = &rebalancing_service {
        polling_service = polling_service
            .with_pending_request_ownership(rebalancing_service.clone())
            .with_fresh_offchain_usd_observer(rebalancing_service.clone());
    }

    let inventory_polling_ctx = Arc::new(InventoryPollingJobCtx {
        poller: Arc::new(polling_service),
        queues: inventory_polling_queues.clone(),
        interval: std::time::Duration::from_secs(context.ctx.inventory_poll_interval),
    });

    // Build the gas monitor before `context.provider` is consumed by the
    // order-fill monitor / accountant below. `None` when `[alerts]` is
    // unconfigured -- the monitor is then simply not spawned.
    let gas_monitor = build_gas_monitor(&context.ctx, context.provider.clone());

    let poll_interval = context.ctx.order_polling_interval();
    info!("Constructing order-job context with poll interval: {poll_interval:?}");

    let poll_status_ctx = Arc::new(PollOrderStatusCtx {
        executor: context.executor.clone(),
        offchain_order_projection: context.frameworks.offchain_order_projection.clone(),
        offchain_order_store: context.frameworks.offchain_order.clone(),
        position_store: context.frameworks.position.clone(),
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

    // The broker placement capability, lifted out of the (now pure)
    // `OffchainOrder::Place` handler: both the rebalancing hedge job and the
    // trade-processing path place through it instead of the aggregate.
    let order_placer: Arc<dyn OrderPlacer> =
        Arc::new(ExecutorOrderPlacer(context.executor.clone()));

    let hedge_ctx = Arc::new(HedgeCtx {
        position: context.frameworks.position.clone(),
        offchain_order: context.frameworks.offchain_order.clone(),
        order_placer: order_placer.clone(),
        poll_status_queue: poll_status_queue.clone(),
        assets: context.ctx.assets.clone(),
        counter_trade_submission_lock: counter_trade_submission_lock.clone(),
        counter_trade_slippage_bps: match &context.ctx.broker {
            BrokerCtx::AlpacaBrokerApi(alpaca_ctx) => alpaca_ctx.counter_trade_slippage_bps,
            BrokerCtx::DryRun => st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        },
    });

    let check_positions_ctx = Arc::new(CheckPositionsCtx {
        executor: context.executor.clone(),
        position: context.frameworks.position.clone(),
        position_projection: context.frameworks.position_projection.clone(),
        offchain_order: context.frameworks.offchain_order.clone(),
        offchain_order_projection: context.frameworks.offchain_order_projection.clone(),
        order_placer: order_placer.clone(),
        counter_trade_submission_lock: counter_trade_submission_lock.clone(),
        hedge_queue: hedge_queue.clone(),
        check_positions_queue: check_positions_queue.clone(),
        poll_status_queue: poll_status_queue.clone(),
        ctx: context.ctx.clone(),
        pool: context.pool.clone(),
        check_interval: std::time::Duration::from_secs(context.ctx.position_check_interval),
    });

    let trade_cqrs = super::TradeProcessingCqrs {
        pool: context.pool.clone(),
        onchain_trade: context.frameworks.onchain_trade,
        position: context.frameworks.position,
        position_projection: context.frameworks.position_projection,
        offchain_order: context.frameworks.offchain_order,
        order_placer,
        execution_threshold: context.execution_threshold,
        assets: context.ctx.assets.clone(),
        counter_trade_submission_lock,
        poll_status_queue: poll_status_queue.clone(),
        hedge_queue: hedge_queue.clone(),
    };

    let maintenance_interval = context.executor.maintenance_interval();

    let accountant_ctx = Arc::new(AccountantCtx {
        contracts: crate::onchain::raindex_contracts(&context.ctx.evm),
        ctx: context.ctx.clone(),
        cache: context.cache,
        pyth_feed_ids: PythFeedIds::new(context.ctx.pyth_feed_ids()),
        evm: ReadOnlyEvm::new(context.provider.clone()),
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
        backfill_queue.clone(),
        context.pool,
        context.provider,
        std::time::Duration::from_secs(context.ctx.order_fill_poll_interval),
    );

    // Fail-fast: exit if any supervised task dies, relying on systemd restart for recovery.
    // In test builds, use aggressive timeouts so a transient RPC failure doesn't
    // stall e2e tests for ~13 minutes of exponential backoff.
    let is_test = cfg!(any(test, feature = "test-support"));
    let mut supervisor_builder = SupervisorBuilder::default()
        .with_max_restart_attempts(if is_test { 2 } else { 10 })
        .with_max_backoff_exponent(if is_test { 2 } else { 8 })
        .with_base_restart_delay(std::time::Duration::from_secs(1))
        .with_dead_tasks_threshold(Some(0.0))
        .with_task("order-fill-monitor", order_fill_monitor);

    log_optional_task_status("executor maintenance", maintenance_interval.is_some());

    if let Some(interval) = maintenance_interval {
        supervisor_builder = supervisor_builder.with_task(
            "executor-maintenance",
            ExecutorMaintenance::new(context.executor, interval),
        );
    }

    log_optional_task_status("gas monitor", gas_monitor.is_some());

    if let Some(gas_monitor) = gas_monitor {
        supervisor_builder = supervisor_builder.with_task("gas-monitor", gas_monitor);
    }

    let supervisor = supervisor_builder.build().run();

    let monitor = MonitorWiring {
        accountant_ctx,
        hedge_ctx,
        poll_status_ctx,
        reconcile_ctx,
        rejection_ctx,
        check_positions_ctx,
        inventory_polling_ctx,
        rebalancing_check_ctx: rebalancing_service,
        seed_vault_registry_ctx,
        job_queue,
        hedge_queue,
        backfill_queue,
        poll_status_queue,
        reconcile_queue,
        rejection_queue,
        check_positions_queue,
        inventory_polling_queues,
        wrapped_equity_recovery_queue,
        wrapped_equity_recovery_ctx,
        unwrapped_equity_recovery_queue,
        unwrapped_equity_recovery_ctx,
        equity_check_scheduler,
        usdc_check_scheduler,
        transfer_usdc_to_hedging_queue,
        transfer_usdc_to_hedging_ctx,
        transfer_usdc_to_market_making_queue,
        transfer_usdc_to_market_making_ctx,
        transfer_equity_to_market_making_queue,
        transfer_equity_to_market_making_ctx,
        transfer_equity_to_hedging_queue,
        transfer_equity_to_hedging_ctx,
        resume_tokenization_queue,
        resume_tokenization_ctx,
        apalis_shutdown_token,
        seed_vault_registry_queue,
        notifier: context.notifier.clone(),
        #[cfg(any(test, feature = "test-support"))]
        failure_injector: context.failure_injector,
    }
    .spawn_apalis_monitor();

    Conductor {
        supervisor,
        monitor,
        job_cleanup,
        telemetry_writer,
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
    inventory_polling_ctx: Arc<InventoryPollingJobCtx>,
    rebalancing_check_ctx: Option<Arc<RebalancingService>>,
    seed_vault_registry_ctx: Arc<SeedVaultRegistryCtx>,
    job_queue: DexTradeAccountingJobQueue,
    hedge_queue: HedgeJobQueue,
    backfill_queue: BackfillJobQueue,
    poll_status_queue: PollOrderStatusJobQueue,
    reconcile_queue: ReconcileOrderFillJobQueue,
    rejection_queue: HandleOrderRejectionJobQueue,
    check_positions_queue: CheckPositionsJobQueue,
    inventory_polling_queues: InventoryPollingJobQueues,
    wrapped_equity_recovery_queue: WrappedEquityRecoveryJobQueue,
    wrapped_equity_recovery_ctx: Option<Arc<WrappedEquityRecoveryCtx>>,
    unwrapped_equity_recovery_queue: UnwrappedEquityRecoveryJobQueue,
    unwrapped_equity_recovery_ctx: Option<Arc<UnwrappedEquityRecoveryCtx>>,
    equity_check_scheduler: EquityRebalancingCheckScheduler,
    usdc_check_scheduler: UsdcRebalancingCheckScheduler,
    transfer_usdc_to_hedging_queue: TransferUsdcToHedgingJobQueue,
    transfer_usdc_to_hedging_ctx: Option<Arc<TransferUsdcToHedgingCtx>>,
    transfer_usdc_to_market_making_queue: TransferUsdcToMarketMakingJobQueue,
    transfer_usdc_to_market_making_ctx: Option<Arc<TransferUsdcToMarketMakingCtx>>,
    transfer_equity_to_market_making_queue: TransferEquityToMarketMakingJobQueue,
    transfer_equity_to_market_making_ctx: Option<Arc<TransferEquityToMarketMakingCtx>>,
    transfer_equity_to_hedging_queue: TransferEquityToHedgingJobQueue,
    transfer_equity_to_hedging_ctx: Option<Arc<TransferEquityToHedgingCtx>>,
    resume_tokenization_queue: ResumeTokenizationJobQueue,
    resume_tokenization_ctx: Option<Arc<ResumeTokenizationCtx>>,
    apalis_shutdown_token: CancellationToken,
    seed_vault_registry_queue: SeedVaultRegistryJobQueue,
    notifier: Arc<dyn Notifier>,
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
            inventory_polling_ctx,
            rebalancing_check_ctx,
            seed_vault_registry_ctx,
            job_queue,
            hedge_queue,
            backfill_queue,
            poll_status_queue,
            reconcile_queue,
            rejection_queue,
            check_positions_queue,
            inventory_polling_queues,
            wrapped_equity_recovery_queue,
            wrapped_equity_recovery_ctx,
            unwrapped_equity_recovery_queue,
            unwrapped_equity_recovery_ctx,
            equity_check_scheduler,
            usdc_check_scheduler,
            transfer_usdc_to_hedging_queue,
            transfer_usdc_to_hedging_ctx,
            transfer_usdc_to_market_making_queue,
            transfer_usdc_to_market_making_ctx,
            transfer_equity_to_market_making_queue,
            transfer_equity_to_market_making_ctx,
            transfer_equity_to_hedging_queue,
            transfer_equity_to_hedging_ctx,
            resume_tokenization_queue,
            resume_tokenization_ctx,
            apalis_shutdown_token,
            seed_vault_registry_queue,
            notifier,
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
        let failure_injector_for_unwrapped_equity_recovery = failure_injector.clone();
        #[cfg(any(test, feature = "test-support"))]
        let failure_injector_for_check_positions = failure_injector.clone();
        #[cfg(any(test, feature = "test-support"))]
        let failure_injector_for_inventory_polling = failure_injector.clone();
        #[cfg(any(test, feature = "test-support"))]
        let failure_injector_for_transfer_usdc_to_hedging = failure_injector.clone();
        #[cfg(any(test, feature = "test-support"))]
        let failure_injector_for_transfer_usdc_to_market_making = failure_injector.clone();
        #[cfg(any(test, feature = "test-support"))]
        let failure_injector_for_transfer_equity_to_market_making = failure_injector.clone();
        #[cfg(any(test, feature = "test-support"))]
        let failure_injector_for_transfer_equity_to_hedging = failure_injector.clone();
        #[cfg(any(test, feature = "test-support"))]
        let failure_injector_for_resume_tokenization = failure_injector.clone();
        let notifier_for_hedge = notifier.clone();
        let notifier_for_backfill = notifier.clone();
        let notifier_for_poll = notifier.clone();
        let notifier_for_reconcile = notifier.clone();
        let notifier_for_rejection = notifier.clone();
        let notifier_for_equity_rebalancing_check = notifier.clone();
        let notifier_for_usdc_rebalancing_check = notifier.clone();
        let notifier_for_seed_vault_registry = notifier.clone();
        let notifier_for_wrapped_equity_recovery = notifier.clone();
        let notifier_for_unwrapped_equity_recovery = notifier.clone();
        let notifier_for_check_positions = notifier.clone();
        let notifier_for_inventory_polling = notifier.clone();
        let notifier_for_transfer_usdc_to_hedging = notifier.clone();
        let notifier_for_transfer_usdc_to_market_making = notifier.clone();
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
                        WORKER_CIRCUIT_POLICY,
                        notifier.clone(),
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
                        WORKER_CIRCUIT_POLICY,
                        notifier_for_hedge.clone(),
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
                        WORKER_CIRCUIT_POLICY,
                        notifier_for_backfill.clone(),
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
                        WORKER_CIRCUIT_POLICY,
                        notifier_for_poll.clone(),
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
                        WORKER_CIRCUIT_POLICY,
                        notifier_for_reconcile.clone(),
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
                        WORKER_CIRCUIT_POLICY,
                        notifier_for_rejection.clone(),
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
                        WORKER_CIRCUIT_POLICY,
                        notifier_for_seed_vault_registry.clone(),
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
                        WORKER_CIRCUIT_POLICY,
                        notifier_for_check_positions.clone(),
                        #[cfg(any(test, feature = "test-support"))]
                        failure_injector_for_check_positions.clone(),
                    )
                });

            let monitor = register_inventory_polling_workers(
                monitor,
                &inventory_polling_ctx,
                inventory_polling_queues,
                &notifier_for_inventory_polling,
                #[cfg(any(test, feature = "test-support"))]
                &failure_injector_for_inventory_polling,
            );

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
                            WORKER_CIRCUIT_POLICY,
                            notifier_for_equity_rebalancing_check.clone(),
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
                            WORKER_CIRCUIT_POLICY,
                            notifier_for_usdc_rebalancing_check.clone(),
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
                notifier_for_wrapped_equity_recovery,
                #[cfg(any(test, feature = "test-support"))]
                failure_injector_for_wrapped_equity_recovery,
            );

            let apalis_monitor = register_unwrapped_equity_recovery_worker(
                apalis_monitor,
                unwrapped_equity_recovery_ctx,
                unwrapped_equity_recovery_queue,
                notifier_for_unwrapped_equity_recovery,
                #[cfg(any(test, feature = "test-support"))]
                failure_injector_for_unwrapped_equity_recovery,
            );

            let apalis_monitor = register_transfer_usdc_to_hedging_worker(
                apalis_monitor,
                transfer_usdc_to_hedging_ctx,
                transfer_usdc_to_hedging_queue,
                notifier_for_transfer_usdc_to_hedging,
                #[cfg(any(test, feature = "test-support"))]
                failure_injector_for_transfer_usdc_to_hedging,
            );

            let apalis_monitor = register_transfer_usdc_to_market_making_worker(
                apalis_monitor,
                transfer_usdc_to_market_making_ctx,
                transfer_usdc_to_market_making_queue,
                notifier_for_transfer_usdc_to_market_making,
                #[cfg(any(test, feature = "test-support"))]
                failure_injector_for_transfer_usdc_to_market_making,
            );

            let apalis_monitor = register_transfer_equity_to_market_making_worker(
                apalis_monitor,
                transfer_equity_to_market_making_ctx,
                transfer_equity_to_market_making_queue,
                #[cfg(any(test, feature = "test-support"))]
                failure_injector_for_transfer_equity_to_market_making,
            );

            let apalis_monitor = register_transfer_equity_to_hedging_worker(
                apalis_monitor,
                transfer_equity_to_hedging_ctx,
                transfer_equity_to_hedging_queue,
                #[cfg(any(test, feature = "test-support"))]
                failure_injector_for_transfer_equity_to_hedging,
            );

            let apalis_monitor = register_resume_tokenization_worker(
                apalis_monitor,
                resume_tokenization_ctx,
                resume_tokenization_queue,
                #[cfg(any(test, feature = "test-support"))]
                failure_injector_for_resume_tokenization,
            );

            let shutdown_signal = async {
                apalis_shutdown_token.cancelled().await;
                Ok(())
            };

            // No inner shutdown_timeout — the outer 30s timeout in
            // drain_bot_with_timeout owns force-abort. This avoids a semantic
            // gap where apalis reports Ok(()) for both clean drain and timeout,
            // making the two cases indistinguishable.

            match apalis_monitor.run_with_signal(shutdown_signal).await {
                Ok(()) => Ok(()),
                Err(source) => Err(MonitorTaskError::UnexpectedExit {
                    source: Some(source),
                }),
            }
        })
    }
}

/// Builds the low-gas balance monitor from the `[alerts]` config, or `None`
/// when alerting is unconfigured. The monitor watches the order-owner wallet
/// on the orderbook chain (Base in production), which is the same provider the
/// conductor already uses for fill polling and contract reads.
fn build_gas_monitor<Prov>(ctx: &Ctx, provider: Prov) -> Option<GasMonitor>
where
    Prov: Provider + Send + Sync + 'static,
{
    let alerts = ctx.alerts.as_ref()?;

    let notifier =
        match TelegramNotifier::new(&alerts.bot_token, alerts.chat_id, alerts.message_thread_id) {
            Ok(notifier) => notifier,
            Err(error) => {
                error!(
                    target: "gas",
                    ?error,
                    "Failed to build Telegram notifier; gas monitor will not be started"
                );
                return None;
            }
        };

    Some(GasMonitor {
        balance_reader: Arc::new(ProviderBalanceReader::new(provider)),
        notifier: Arc::new(notifier),
        wallet: ctx.order_owner(),
        chain: "base",
        threshold_wei: alerts.low_balance_threshold_wei,
        poll_interval: alerts.poll_interval,
        realert_interval: alerts.realert_interval,
    })
}

fn log_optional_task_status(task_name: &str, is_configured: bool) {
    if is_configured {
        info!("Started {task_name} task");
    } else {
        debug!("{task_name} not configured", task_name = task_name);
    }
}

fn register_inventory_polling_workers(
    monitor: Monitor,
    ctx: &Arc<InventoryPollingJobCtx>,
    queues: InventoryPollingJobQueues,
    notifier: &Arc<dyn Notifier>,
    #[cfg(any(test, feature = "test-support"))] failure_injector: &FailureInjector,
) -> Monitor {
    let InventoryPollingJobQueues {
        inflight_equity,
        onchain_equity,
        onchain_usdc,
        ethereum_wallet_usdc,
        base_wallet_usdc,
        base_wallet_unwrapped_equity,
        base_wallet_wrapped_equity,
        offchain_inventory,
    } = queues;

    macro_rules! register {
        ($monitor:expr, $job:ty, $queue:expr) => {
            $monitor.register({
                let ctx = ctx.clone();
                let notifier = notifier.clone();
                #[cfg(any(test, feature = "test-support"))]
                let failure_injector = failure_injector.clone();

                move |index| {
                    build_supervised_worker!(
                        ::<InventoryPollingJobCtx, $job>,
                        index,
                        $queue.clone(),
                        ctx.clone(),
                        WORKER_CIRCUIT_POLICY,
                        notifier.clone(),
                        #[cfg(any(test, feature = "test-support"))]
                        failure_injector.clone(),
                    )
                }
            })
        };
    }

    let monitor = register!(monitor, PollInflightEquity, inflight_equity);
    let monitor = register!(monitor, PollOnchainEquity, onchain_equity);
    let monitor = register!(monitor, PollOnchainUsdc, onchain_usdc);
    let monitor = register!(monitor, PollEthereumWalletUsdc, ethereum_wallet_usdc);
    let monitor = register!(monitor, PollBaseWalletUsdc, base_wallet_usdc);
    let monitor = register!(
        monitor,
        PollBaseWalletUnwrappedEquity,
        base_wallet_unwrapped_equity
    );
    let monitor = register!(
        monitor,
        PollBaseWalletWrappedEquity,
        base_wallet_wrapped_equity
    );
    register!(monitor, PollOffchainInventory, offchain_inventory)
}

/// Conditionally registers the wrapped-equity recovery worker against the
/// apalis monitor. Extracted because this is the only `Option`-gated worker
/// registration and inlining the let-else + debug log keeps
/// `spawn_apalis_monitor` over the cognitive-complexity limit.
fn register_wrapped_equity_recovery_worker(
    monitor: Monitor,
    recovery_ctx: Option<Arc<WrappedEquityRecoveryCtx>>,
    recovery_queue: WrappedEquityRecoveryJobQueue,
    notifier: Arc<dyn Notifier>,
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
            WORKER_CIRCUIT_POLICY,
            notifier.clone(),
            #[cfg(any(test, feature = "test-support"))]
            failure_injector.clone(),
        )
    })
}

/// Conditionally registers the unwrapped-equity recovery worker. Same
/// pattern as `register_wrapped_equity_recovery_worker`.
fn register_unwrapped_equity_recovery_worker(
    monitor: Monitor,
    recovery_ctx: Option<Arc<UnwrappedEquityRecoveryCtx>>,
    recovery_queue: UnwrappedEquityRecoveryJobQueue,
    notifier: Arc<dyn Notifier>,
    #[cfg(any(test, feature = "test-support"))] failure_injector: FailureInjector,
) -> Monitor {
    let Some(recovery_ctx) = recovery_ctx else {
        debug!(
            "Unwrapped-equity recovery worker not registered: rebalancing disabled \
             (no recovery ctx). Detected tSTOCK on the bot wallet will not be recovered."
        );
        return monitor;
    };

    monitor.register(move |index| {
        build_supervised_worker!(
            ::<UnwrappedEquityRecoveryCtx, UnwrappedEquityRecoveryJob>,
            index,
            recovery_queue.clone(),
            recovery_ctx.clone(),
            WORKER_CIRCUIT_POLICY,
            notifier.clone(),
            #[cfg(any(test, feature = "test-support"))]
            failure_injector.clone(),
        )
    })
}

/// Conditionally registers the `TransferUsdcToHedging` worker. The ctx is
/// `None` when rebalancing is disabled in config; in that case the queue is
/// still constructed (so tests that build it directly compile) but no worker
/// consumes it.
fn register_transfer_usdc_to_hedging_worker(
    monitor: Monitor,
    transfer_ctx: Option<Arc<TransferUsdcToHedgingCtx>>,
    transfer_queue: TransferUsdcToHedgingJobQueue,
    notifier: Arc<dyn Notifier>,
    #[cfg(any(test, feature = "test-support"))] failure_injector: FailureInjector,
) -> Monitor {
    let Some(transfer_ctx) = transfer_ctx else {
        warn!(
            "TransferUsdcToHedging worker not registered: rebalancing disabled \
             (no transfer ctx). Base->Alpaca USDC transfers will not be processed."
        );
        return monitor;
    };

    monitor.register(move |index| {
        build_supervised_worker!(
            ::<TransferUsdcToHedgingCtx, TransferUsdcToHedging>,
            index,
            transfer_queue.clone(),
            transfer_ctx.clone(),
            WORKER_CIRCUIT_POLICY,
            notifier.clone(),
            #[cfg(any(test, feature = "test-support"))]
            failure_injector.clone(),
        )
    })
}

/// Sibling of [`register_transfer_usdc_to_hedging_worker`] for the
/// Alpaca->Base direction.
fn register_transfer_usdc_to_market_making_worker(
    monitor: Monitor,
    transfer_ctx: Option<Arc<TransferUsdcToMarketMakingCtx>>,
    transfer_queue: TransferUsdcToMarketMakingJobQueue,
    notifier: Arc<dyn Notifier>,
    #[cfg(any(test, feature = "test-support"))] failure_injector: FailureInjector,
) -> Monitor {
    let Some(transfer_ctx) = transfer_ctx else {
        warn!(
            "TransferUsdcToMarketMaking worker not registered: rebalancing disabled \
             (no transfer ctx). Alpaca->Base USDC transfers will not be processed."
        );
        return monitor;
    };

    monitor.register(move |index| {
        build_supervised_worker!(
            ::<TransferUsdcToMarketMakingCtx, TransferUsdcToMarketMaking>,
            index,
            transfer_queue.clone(),
            transfer_ctx.clone(),
            WORKER_CIRCUIT_POLICY,
            notifier.clone(),
            #[cfg(any(test, feature = "test-support"))]
            failure_injector.clone(),
        )
    })
}

/// Conditionally registers the `TransferEquityToMarketMaking` worker. Same
/// pattern as the USDC transfer workers: the ctx is `None` when rebalancing
/// is disabled, in which case the queue exists but no worker consumes it.
/// Terminal per-transfer failures remain dead-lettered in apalis without
/// stopping this worker or the conductor.
fn register_transfer_equity_to_market_making_worker(
    monitor: Monitor,
    transfer_ctx: Option<Arc<TransferEquityToMarketMakingCtx>>,
    transfer_queue: TransferEquityToMarketMakingJobQueue,
    #[cfg(any(test, feature = "test-support"))] failure_injector: FailureInjector,
) -> Monitor {
    let Some(transfer_ctx) = transfer_ctx else {
        warn!(
            "TransferEquityToMarketMaking worker not registered: rebalancing disabled \
             (no transfer ctx). Equity mints will not be processed."
        );
        return monitor;
    };

    monitor.register(move |index| {
        build_best_effort_worker!(
            ::<TransferEquityToMarketMakingCtx, TransferEquityToMarketMaking>,
            index,
            transfer_queue.clone(),
            transfer_ctx.clone(),
            #[cfg(any(test, feature = "test-support"))]
            failure_injector.clone(),
        )
    })
}

/// Sibling of [`register_transfer_equity_to_market_making_worker`] for the
/// redemption (market-making -> hedging) direction.
fn register_transfer_equity_to_hedging_worker(
    monitor: Monitor,
    transfer_ctx: Option<Arc<TransferEquityToHedgingCtx>>,
    transfer_queue: TransferEquityToHedgingJobQueue,
    #[cfg(any(test, feature = "test-support"))] failure_injector: FailureInjector,
) -> Monitor {
    let Some(transfer_ctx) = transfer_ctx else {
        warn!(
            "TransferEquityToHedging worker not registered: rebalancing disabled \
             (no transfer ctx). Equity redemptions will not be processed."
        );
        return monitor;
    };

    monitor.register(move |index| {
        build_best_effort_worker!(
            ::<TransferEquityToHedgingCtx, TransferEquityToHedging>,
            index,
            transfer_queue.clone(),
            transfer_ctx.clone(),
            #[cfg(any(test, feature = "test-support"))]
            failure_injector.clone(),
        )
    })
}

/// Conditionally registers the `ResumeTokenizationAggregate` worker. The ctx is
/// `None` when rebalancing is disabled; in that case the queue exists but no
/// worker consumes it. When enabled, runs interrupted mint/redemption aggregates
/// off the startup path so a slow or down issuer cannot block monitoring.
fn register_resume_tokenization_worker(
    monitor: Monitor,
    resume_ctx: Option<Arc<ResumeTokenizationCtx>>,
    resume_queue: ResumeTokenizationJobQueue,
    #[cfg(any(test, feature = "test-support"))] failure_injector: FailureInjector,
) -> Monitor {
    let Some(resume_ctx) = resume_ctx else {
        debug!(
            "ResumeTokenizationAggregate worker not registered: rebalancing disabled \
             (no resume ctx). Interrupted tokenization aggregates will not be resumed."
        );
        return monitor;
    };

    monitor.register(move |index| {
        build_best_effort_worker!(
            ::<ResumeTokenizationCtx, ResumeTokenizationAggregate>,
            index,
            resume_queue.clone(),
            resume_ctx.clone(),
            #[cfg(any(test, feature = "test-support"))]
            failure_injector.clone(),
        )
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::RwLock;
    use std::time::Duration;

    use async_trait::async_trait;
    use st0x_config::EquitiesConfig;
    use st0x_event_sorcery::test_store;
    use st0x_execution::{FractionalShares, Symbol};
    use st0x_float_macro::float;
    use st0x_raindex::Raindex;
    use st0x_tokenization::IssuerRequestId;
    use st0x_tokenization::mock::MockTokenizer;
    use st0x_wrapper::{MockWrapper, Wrapper};

    use super::*;
    use crate::equity_redemption::RedemptionAggregateId;
    use crate::onchain::mock::MockRaindex;
    use crate::rebalancing::equity::{
        EquityTransferServices, MintError, MintTransferError, RedemptionError,
        ResumeEquityToHedging, ResumeEquityToMarketMaking,
    };
    use crate::test_utils::{setup_test_apalis_pool, setup_test_pools};
    use crate::vault_lookup::MockVaultLookup;

    struct PoisonThenHealthyRedemptionResume {
        poison_id: RedemptionAggregateId,
        healthy_completed: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl ResumeEquityToHedging for PoisonThenHealthyRedemptionResume {
        async fn resume_equity_to_hedging(
            &self,
            aggregate_id: &RedemptionAggregateId,
            _symbol: &Symbol,
            _quantity: FractionalShares,
        ) -> Result<(), RedemptionError> {
            if aggregate_id == &self.poison_id {
                return Err(RedemptionError::EntityNotFound {
                    aggregate_id: aggregate_id.clone(),
                });
            }

            self.healthy_completed.notify_waiters();
            Ok(())
        }
    }

    struct PoisonThenHealthyMintResume {
        poison_id: IssuerRequestId,
        healthy_completed: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl ResumeEquityToMarketMaking for PoisonThenHealthyMintResume {
        async fn resume_equity_to_market_making(
            &self,
            issuer_request_id: &IssuerRequestId,
            _symbol: &Symbol,
            _quantity: FractionalShares,
        ) -> Result<(), MintTransferError> {
            if issuer_request_id == &self.poison_id {
                return Err(MintTransferError::PreReceipt(MintError::EntityNotFound {
                    issuer_request_id: issuer_request_id.clone(),
                    expected_state: "healthy test mint",
                }));
            }

            self.healthy_completed.notify_waiters();
            Ok(())
        }
    }

    async fn wait_for_terminal_job<Task: 'static>(apalis_pool: &apalis_sqlite::SqlitePool) {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let terminal_count: i64 = sqlx_apalis::query_scalar(
                    "SELECT COUNT(*) FROM Jobs \
                     WHERE job_type = ? AND status IN ('Failed', 'Killed') \
                     AND attempts >= max_attempts",
                )
                .bind(std::any::type_name::<Task>())
                .fetch_one(apalis_pool)
                .await
                .unwrap();
                if terminal_count == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("the poison job must reach a visible terminal state");
    }

    fn mint_transfer_ctx(
        transfer: Arc<dyn ResumeEquityToMarketMaking>,
        cqrs_pool: sqlx::SqlitePool,
    ) -> Arc<TransferEquityToMarketMakingCtx> {
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());
        let wrapper: Arc<dyn Wrapper> = Arc::new(MockWrapper::new());
        let services = EquityTransferServices {
            raindex,
            vault_lookup: Arc::new(MockVaultLookup::new()),
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper,
        };

        Arc::new(TransferEquityToMarketMakingCtx {
            transfer,
            equity_in_progress: Arc::new(RwLock::new(HashMap::new())),
            mint_store: Arc::new(test_store(cqrs_pool, services)),
            equities_config: EquitiesConfig::default(),
        })
    }

    #[tokio::test]
    async fn terminal_equity_transfer_is_dead_lettered_without_stopping_worker() {
        let apalis_pool = setup_test_apalis_pool().await;
        let mut queue = TransferEquityToHedgingJobQueue::new(&apalis_pool);
        let poison_id = RedemptionAggregateId::generate();
        let healthy_id = RedemptionAggregateId::generate();
        let symbol = Symbol::new("AAPL").unwrap();

        queue
            .push(TransferEquityToHedging {
                aggregate_id: poison_id.clone(),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(float!(1)),
            })
            .await
            .unwrap();
        let mut push_queue = queue.clone();

        let healthy_completed = Arc::new(tokio::sync::Notify::new());
        let transfer_ctx = Arc::new(TransferEquityToHedgingCtx {
            transfer: Arc::new(PoisonThenHealthyRedemptionResume {
                poison_id,
                healthy_completed: healthy_completed.clone(),
            }),
        });
        let monitor = register_transfer_equity_to_hedging_worker(
            Monitor::new().should_restart(|_ctx, _error, _attempt| false),
            Some(transfer_ctx),
            queue,
            FailureInjector::new(),
        );
        let monitor_handle = tokio::spawn(async move { monitor.run().await });

        wait_for_terminal_job::<TransferEquityToHedging>(&apalis_pool).await;

        push_queue
            .push(TransferEquityToHedging {
                aggregate_id: healthy_id,
                symbol,
                quantity: FractionalShares::new(float!(1)),
            })
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(15), healthy_completed.notified())
            .await
            .expect("the worker must process a healthy sibling after dead-lettering a poison job");

        let terminal_count: i64 = sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs \
             WHERE job_type = ? AND status IN ('Failed', 'Killed') AND attempts >= max_attempts",
        )
        .bind(std::any::type_name::<TransferEquityToHedging>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        assert_eq!(terminal_count, 1, "the poison job must remain visible");

        assert!(
            !monitor_handle.is_finished(),
            "a terminal equity-transfer job must not stop the conductor monitor",
        );
        monitor_handle.abort();
    }

    #[tokio::test]
    async fn terminal_equity_mint_is_dead_lettered_without_stopping_worker() {
        let (cqrs_pool, apalis_pool) = setup_test_pools().await;
        let mut queue = TransferEquityToMarketMakingJobQueue::new(&apalis_pool);
        let poison_id = IssuerRequestId::generate();
        let healthy_id = IssuerRequestId::generate();
        let symbol = Symbol::new("AAPL").unwrap();

        queue
            .push(TransferEquityToMarketMaking {
                issuer_request_id: poison_id.clone(),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(float!(1)),
                generation: 0,
            })
            .await
            .unwrap();
        let mut push_queue = queue.clone();

        let healthy_completed = Arc::new(tokio::sync::Notify::new());
        let transfer_ctx = mint_transfer_ctx(
            Arc::new(PoisonThenHealthyMintResume {
                poison_id,
                healthy_completed: healthy_completed.clone(),
            }),
            cqrs_pool,
        );
        let monitor = register_transfer_equity_to_market_making_worker(
            Monitor::new().should_restart(|_ctx, _error, _attempt| false),
            Some(transfer_ctx),
            queue,
            FailureInjector::new(),
        );
        let monitor_handle = tokio::spawn(async move { monitor.run().await });

        wait_for_terminal_job::<TransferEquityToMarketMaking>(&apalis_pool).await;
        push_queue
            .push(TransferEquityToMarketMaking {
                issuer_request_id: healthy_id,
                symbol,
                quantity: FractionalShares::new(float!(1)),
                generation: 0,
            })
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(15), healthy_completed.notified())
            .await
            .expect("the mint worker must process a healthy sibling after a poison job");
        assert!(
            !monitor_handle.is_finished(),
            "a terminal equity-mint job must not stop the conductor monitor",
        );
        monitor_handle.abort();
    }
}
