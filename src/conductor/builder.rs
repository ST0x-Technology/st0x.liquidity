//! Constructs a fully-wired [`Conductor`] instance from its dependencies.

use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use apalis::prelude::Monitor;
use sqlx::SqlitePool;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use task_supervisor::SupervisorBuilder;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use st0x_config::{AlertsCtx, Ctx, ExecutionThreshold, OnchainWalletCtx};
use st0x_event_sorcery::{Projection, Store};
use st0x_evm::{Chain, Evm, ReadOnlyEvm, Wallet};
use st0x_execution::{Executor, Symbol};
use st0x_finance::{HasZero, Positive, Usd};
use st0x_raindex::RaindexService;
use st0x_registry::SymbolCache;
use st0x_tokenization::Tokenizer;
use st0x_wrapper::Wrapper;

use super::exit::MonitorTaskError;
#[cfg(any(test, feature = "test-support"))]
use super::job::FailureInjector;
use super::job::{
    TerminalFailureInfo, TerminalFailureSignal, build_best_effort_worker, build_supervised_worker,
    build_worker_inner,
};
use super::monitor::executor_maintenance::ExecutorMaintenance;
use super::monitor::gas::GasMonitor;
use super::monitor::inventory::InventoryMonitor;
use super::monitor::order_fills::OrderFillMonitor;
use super::{Conductor, SupervisorStartupTokens};
use crate::alerts::Notifier;
#[cfg(test)]
use crate::bot_gas::BotGasReceiptCostEnqueuer;
use crate::bot_gas::{
    RecordBotGasReceiptCost, RecordBotGasReceiptCostCtx, RecordBotGasReceiptCostJobQueue,
};
use crate::dashboard::{
    DashboardTradeDeliveryCtx, DashboardTradeDeliveryJobQueue, DashboardTradeHandoffMonitor,
    DeliverDashboardTrade,
};
use crate::inventory::{
    BroadcastingInventory, ChainVaultPolling, HedgeOrderGateReconciliationCtx,
    InventoryDivergenceRecoveryCtx, InventoryPollingService, InventorySnapshot,
    InventorySnapshotId, PollFreshness, WalletPollingCtx,
};
use crate::native_gas::ProviderBalanceReader;
use crate::offchain::order::handle_rejection::HandleOrderRejectionCtx;
use crate::offchain::order::poll_status::PollOrderStatusCtx;
use crate::offchain::order::reconcile_fill::ReconcileOrderFillCtx;
use crate::offchain::order::{ExecutorOrderPlacer, OrderPlacer};
use crate::offchain::order::{
    HandleOrderRejection, HandleOrderRejectionJobQueue, OffchainOrder, PollOrderStatus,
    PollOrderStatusJobQueue, ReconcileOrderFill, ReconcileOrderFillJobQueue,
};
use crate::onchain::backfill::{BackfillQueues, BackfillRange};
use crate::onchain_trade::OnChainTrade;
use crate::portfolio_snapshot::{
    MarketMakingSlots, PortfolioSnapshot, PortfolioSnapshotCtx, PortfolioSnapshotJob,
    PortfolioSnapshotJobQueue,
};
use crate::position::Position;
use crate::position_check::{CheckPositions, CheckPositionsCtx, CheckPositionsJobQueue};
use crate::rebalancing::equity::{
    DeliverMintAuthorization, DeliverMintAuthorizationCtx, DeliverMintAuthorizationJobQueue,
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
use crate::startup::{StartupTask, StartupToken};
use crate::trading::offchain::close_flatten::{
    CloseFlattenCrossRamp, CloseFlattenCrossRampError, CloseFlattenPolicy,
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
    pub(crate) portfolio_snapshot: Arc<Store<PortfolioSnapshot>>,
}

#[derive(Debug, Error)]
pub(crate) enum ConductorSpawnError {
    #[error(
        "watched chain {chain} has no {what} wired; the provider, queue, and \
         token maps must all be derived from the same chain registry"
    )]
    MissingWatchWiring {
        chain: st0x_evm::Chain,
        what: &'static str,
    },
    #[error(transparent)]
    CloseFlattenWindow(#[from] chrono::OutOfRangeError),
    #[error(transparent)]
    CloseFlattenCrossRamp(#[from] CloseFlattenCrossRampError),
    #[error(
        "[alerts.low_balance_thresholds] has no entry for {chain}, but a gas monitor \
         runs on it"
    )]
    MissingGasThreshold { chain: Chain },
    #[error("HyperEVM gas monitor selection and wallet availability disagree")]
    HyperEvmGasWalletMismatch,
    #[error("[alerts] requires a configured [wallet] section")]
    AlertsRequireWallet,
}

/// Everything needed to construct a running [`Conductor`].
pub(crate) struct ConductorCtx<Prov, Exec> {
    pub(crate) ctx: Ctx,
    /// Providers for watched, non-primary chains (the primary's fill watcher
    /// reuses `provider`). Keyed by chain; derived from the same registry as
    /// the queue and token maps.
    pub(crate) watch_providers: std::collections::BTreeMap<st0x_evm::Chain, Prov>,
    /// Shared freshness tracker (see the creation site in `Conductor::start`):
    /// the poller built here stamps it; the rebalancing guard reads it.
    pub(crate) poll_freshness: PollFreshness,
    pub(crate) cache: SymbolCache,
    pub(crate) provider: Prov,
    pub(crate) executor: Exec,
    pub(crate) execution_threshold: ExecutionThreshold,
    pub(crate) frameworks: CqrsFrameworks,
    pub(crate) pool: SqlitePool,
    /// The live cross-venue inventory view. Not carried by [`CqrsFrameworks`]
    /// (that struct holds only CQRS stores/projections) -- the
    /// [`PortfolioSnapshotCtx`] job context reads it directly each tick, and
    /// [`InventoryDivergenceRecoveryCtx`] verifies against it on divergence.
    pub(crate) inventory: Arc<BroadcastingInventory>,
    pub(crate) wallet_polling: WalletPollingCtx,
    pub(crate) tokenizer: Arc<dyn Tokenizer>,
    /// Ratio source for the portfolio-snapshot capture gate, one per watched
    /// chain: market making holds wrapped vault shares onchain, and the daily
    /// capture job resolves each wrapped balance through the service of the
    /// chain that balance sits on.
    pub(crate) wrappers: BTreeMap<Chain, Arc<dyn Wrapper>>,
    pub(crate) shutdown_token: CancellationToken,
    pub(crate) startup_token: StartupToken,
    pub(crate) supervisor_startup: SupervisorStartupTokens,
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) failure_injector: FailureInjector,
}

/// The equity and USDC vaults one watched chain's assets table configures.
/// The inventory poller checks its retired-vault warnings against these: a
/// registered vault outside the set is one the config no longer names.
struct ConfiguredChainVaults {
    equity_vaults: BTreeMap<Address, BTreeSet<B256>>,
    usdc_vaults: Option<BTreeSet<B256>>,
}

/// Equity symbols the portfolio treats as configured. Shared with the CLI's
/// snapshot-mark repair so the two cannot drift on what "configured" means.
pub fn configured_equity_symbols(ctx: &Ctx) -> HashSet<Symbol> {
    chain_equity_symbols(&ctx.chains.primary().assets)
}

/// The equities one chain operates: either switch on counts, both off does
/// not. A chain lists its own, so a symbol traded on one chain alone is
/// required there and nowhere else.
fn chain_equity_symbols(assets: &st0x_config::ChainAssets) -> HashSet<Symbol> {
    assets
        .equities
        .symbols
        .keys()
        .filter(|symbol| assets.is_trading_enabled(symbol) || assets.is_rebalancing_enabled(symbol))
        .cloned()
        .collect()
}

/// The market-making slots the daily portfolio-snapshot completeness gates
/// require, keyed by chain. Each chain contributes exactly what its own
/// assets table declares, so the gate demands what the inventory poller can
/// actually stamp for that chain and nothing more.
fn market_making_slots(ctx: &Ctx) -> BTreeMap<Chain, MarketMakingSlots> {
    ctx.chains
        .watched()
        .map(|watched| {
            (
                watched.chain,
                MarketMakingSlots {
                    equity_symbols: chain_equity_symbols(&watched.assets),
                    usdc_tracking_enabled: watched.assets.cash.is_some(),
                },
            )
        })
        .collect()
}

fn configured_chain_vaults(watched: &st0x_config::HedgedChain) -> ConfiguredChainVaults {
    let mut equity_vaults: BTreeMap<Address, BTreeSet<B256>> = BTreeMap::new();
    for equity_config in watched.assets.equities.symbols.values() {
        equity_vaults
            .entry(equity_config.tokenized_equity_derivative)
            .or_default()
            .extend(equity_config.vault_ids.iter().copied());
    }

    let usdc_vaults = watched
        .assets
        .cash
        .as_ref()
        .map(|cash| cash.vault_ids.iter().copied().collect());

    ConfiguredChainVaults {
        equity_vaults,
        usdc_vaults,
    }
}

/// The vault-reading leg of the inventory poller, one entry per watched
/// chain. Each entry carries that chain's own Raindex service on that chain's
/// own provider, its own orderbook and vault owner (which key its
/// chain-qualified vault registry), and the vaults its own assets table
/// configures -- the set its retired-vault warnings check against.
///
/// Without an entry a chain's inventory slots stay empty for the process
/// lifetime: nothing corrects drift there and no fill-absorption watermark
/// ever advances.
fn vault_polling_entries<Prov>(
    ctx: &Ctx,
    primary_provider: &Prov,
    watch_providers: &std::collections::BTreeMap<Chain, Prov>,
) -> Result<Vec<ChainVaultPolling<ReadOnlyEvm<Prov>>>, ConductorSpawnError>
where
    Prov: Provider + Clone + Send + Sync + 'static,
{
    let primary_chain = ctx.chains.primary().chain;

    ctx.chains
        .watched()
        .map(|watched| {
            let chain = watched.chain;
            let provider = if chain == primary_chain {
                primary_provider.clone()
            } else {
                watch_providers.get(&chain).cloned().ok_or(
                    ConductorSpawnError::MissingWatchWiring {
                        chain,
                        what: "vault polling provider",
                    },
                )?
            };
            let ConfiguredChainVaults {
                equity_vaults,
                usdc_vaults,
            } = configured_chain_vaults(watched);

            Ok(ChainVaultPolling::new(
                chain,
                Arc::new(RaindexService::new(
                    ReadOnlyEvm::new(provider),
                    crate::onchain::raindex_contracts(watched),
                    ctx.order_owner(),
                )),
                watched.orderbook,
                watched.vault_owner,
            )
            .with_configured_vaults(equity_vaults, usdc_vaults))
        })
        .collect()
}

/// Wires all runtime components and returns a running [`Conductor`].
// Straight-line builder that wires every runtime context; splitting it would
// scatter the wiring across helpers without reducing complexity.
#[allow(clippy::too_many_lines)]
#[bon::builder]
pub(crate) fn spawn<Prov, Exec>(
    context: ConductorCtx<Prov, Exec>,
    job_queue: DexTradeAccountingJobQueue,
    backfill_queues: BackfillQueues,
    dashboard_trade_delivery_queue: DashboardTradeDeliveryJobQueue,
    dashboard_trade_delivery_ctx: Arc<DashboardTradeDeliveryCtx>,
    dashboard_trade_handoff_monitor: DashboardTradeHandoffMonitor,
    hedge_queue: HedgeJobQueue,
    poll_status_queue: PollOrderStatusJobQueue,
    reconcile_queue: ReconcileOrderFillJobQueue,
    rejection_queue: HandleOrderRejectionJobQueue,
    check_positions_queue: CheckPositionsJobQueue,
    portfolio_snapshot_queue: PortfolioSnapshotJobQueue,
    notifier: Arc<dyn Notifier>,
    wrapped_equity_recovery_queue: WrappedEquityRecoveryJobQueue,
    wrapped_equity_recovery_ctx: Arc<WrappedEquityRecoveryCtx>,
    unwrapped_equity_recovery_queue: UnwrappedEquityRecoveryJobQueue,
    unwrapped_equity_recovery_ctx: Arc<UnwrappedEquityRecoveryCtx>,
    equity_check_scheduler: EquityRebalancingCheckScheduler,
    usdc_check_scheduler: UsdcRebalancingCheckScheduler,
    transfer_usdc_to_hedging_queue: TransferUsdcToHedgingJobQueue,
    transfer_usdc_to_hedging_ctx: Arc<TransferUsdcToHedgingCtx>,
    transfer_usdc_to_market_making_queue: TransferUsdcToMarketMakingJobQueue,
    transfer_usdc_to_market_making_ctx: Arc<TransferUsdcToMarketMakingCtx>,
    transfer_equity_to_market_making_queue: TransferEquityToMarketMakingJobQueue,
    transfer_equity_to_market_making_ctx: Arc<TransferEquityToMarketMakingCtx>,
    transfer_equity_to_hedging_queue: TransferEquityToHedgingJobQueue,
    transfer_equity_to_hedging_ctx: Arc<TransferEquityToHedgingCtx>,
    rebalancing_service: Arc<RebalancingService>,
    seed_vault_registry_queue: SeedVaultRegistryJobQueue,
    seed_vault_registry_ctx: Arc<SeedVaultRegistryCtx>,
    resume_tokenization_queue: ResumeTokenizationJobQueue,
    resume_tokenization_ctx: Arc<ResumeTokenizationCtx>,
    deliver_mint_authorization_queue: DeliverMintAuthorizationJobQueue,
    deliver_mint_authorization_ctx: Arc<DeliverMintAuthorizationCtx>,
    record_bot_gas_receipt_cost_queue: RecordBotGasReceiptCostJobQueue,
    record_bot_gas_receipt_cost_ctx: Arc<RecordBotGasReceiptCostCtx>,
    job_cleanup: JoinHandle<()>,
    telemetry_writer: JoinHandle<()>,
    worker_failure_notifier: Arc<dyn Notifier>,
) -> Result<Conductor, ConductorSpawnError>
where
    Prov: Provider + Clone + Send + Sync + 'static,
    Exec: Executor + Clone + Send + Sync + 'static,
    TradeAccountingError: From<Exec::Error>,
    crate::offchain::order::JobError: From<Exec::Error>,
{
    info!("Starting conductor orchestration");

    let order_owner = context.ctx.order_owner();
    // Taken before the per-chain wiring below so the inventory poller can be
    // built from the same provider map the fill watchers and accountants use.
    let watch_providers = context.watch_providers;

    let reserved_cash = context
        .ctx
        .assets
        .cash
        .as_ref()
        .map(|cash| cash.reserved)
        .map_or(Usd::ZERO, Positive::inner);

    let snapshot_id = InventorySnapshotId {
        orderbook: context.ctx.chains.primary().orderbook,
        owner: order_owner,
    };

    let configured_equity_symbols = configured_equity_symbols(&context.ctx);

    // The snapshot capture gate below must require exactly the wallet slots
    // this poller populates, so both derive from this single Option: the
    // poller consumes it, the gate reads its presence.
    let wallet_polling = Some(context.wallet_polling);
    let wallet_polling_enabled = wallet_polling.is_some();
    let tokenizer = context.tokenizer;

    // Shared instance from `Conductor::start`: cloned into the live poller
    // and the daily capture job's ctx below, so `freshness_gap` (write.rs)
    // and the rebalancing staleness guard see exactly what this poller has
    // stamped this run.
    let poll_freshness = context.poll_freshness.clone();

    let polling_service = Arc::new(
        InventoryPollingService::new(
            poll_freshness.clone(),
            vault_polling_entries(&context.ctx, &context.provider, &watch_providers)?,
            context.executor.clone(),
            context.frameworks.vault_registry.clone(),
            snapshot_id,
            context.frameworks.snapshot,
            wallet_polling,
            Some(tokenizer),
            reserved_cash,
        )
        .with_configured_equity_symbols(configured_equity_symbols.clone())
        .with_divergence_recovery(InventoryDivergenceRecoveryCtx {
            inventory: context.inventory.clone(),
            threshold: context.ctx.inventory_divergence_threshold,
            // Share the trigger's gate so transfer suppression and
            // detection agree.
            gate: rebalancing_service.divergence_gate(),
        })
        .with_hedge_order_gate_reconciliation(HedgeOrderGateReconciliationCtx {
            inventory: context.inventory.clone(),
            positions: context.frameworks.position.clone(),
            notifier: worker_failure_notifier.clone(),
            deadline: std::time::Duration::from_secs(
                context
                    .ctx
                    .hedge_order_gate_reconciliation_timeout_secs
                    .get(),
            ),
        })
        .with_pending_request_ownership(rebalancing_service.clone()),
    );

    let inventory_monitor = InventoryMonitor {
        poller: polling_service,
        interval: std::time::Duration::from_secs(context.ctx.inventory_poll_interval_secs),
    };

    // Build the gas monitors before `context.provider` is consumed by the
    // order-fill monitor / accountant below. `None` when `[alerts]` is
    // unconfigured -- no gas monitor is then spawned.
    let gas_monitors = if let Some((alerts, wallet_ctx)) = alerts_with_wallet(&context.ctx)? {
        let base_wallet = wallet_ctx.base_wallet();
        let ethereum_wallet = wallet_ctx.ethereum_wallet();

        Some(build_gas_monitors(
            alerts,
            (base_wallet.provider().clone(), base_wallet.address()),
            (
                ethereum_wallet.provider().clone(),
                ethereum_wallet.address(),
            ),
            alerts
                .low_balance_threshold_wei(Chain::HyperEvm)
                .is_some()
                .then(|| {
                    let wallet = wallet_ctx.hyperevm_wallet();
                    (wallet.provider().clone(), wallet.address())
                }),
            &notifier,
        )?)
    } else {
        None
    };

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

    // Validated once here rather than re-parsed from the raw config
    // `u64` on every hedge job / position-check tick (RAI-1404 follow-up):
    // the window is fixed for the process lifetime, so a construction-time
    // failure should fail startup, not thread a per-call `Result` through
    // the hot placement and scan paths.
    let close_flatten_policy =
        CloseFlattenPolicy::from_secs(context.ctx.extended_hours_close_flatten_window_secs)?;

    let close_flatten_ramp = CloseFlattenCrossRamp::new(
        context.ctx.broker.counter_trade_slippage_bps(),
        context.ctx.close_flatten_cross_max_bps,
    )?;

    // One set, shared by both paths: a symbol dropped by the scan and the
    // dead-letter it would have become are the same standing delta, so they
    // must page once, and the release the hedge path performs once a
    // placement reaches the broker must clear the scan's entries too.
    let alerted_dead_letters = Arc::new(tokio::sync::Mutex::new(HashSet::new()));

    let hedge_ctx = Arc::new(HedgeCtx {
        position: context.frameworks.position.clone(),
        offchain_order: context.frameworks.offchain_order.clone(),
        order_placer: order_placer.clone(),
        poll_status_queue: poll_status_queue.clone(),
        hedge_queue: hedge_queue.clone(),
        assets: context.ctx.assets.clone(),
        counter_trade_submission_lock: counter_trade_submission_lock.clone(),
        close_flatten_policy,
        close_flatten_ramp,
        poll_interval,
        notifier: notifier.clone(),
        alerted_dead_letters: alerted_dead_letters.clone(),
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
        check_interval: std::time::Duration::from_secs(context.ctx.position_check_interval_secs),
        close_flatten_policy,
        close_flatten_ramp,
        poll_interval,
        notifier: notifier.clone(),
        alerted_dead_letters,
    });

    let portfolio_snapshot_ctx = Arc::new(PortfolioSnapshotCtx {
        market_making: market_making_slots(&context.ctx),
        inventory: context.inventory.clone(),
        position_projection: context.frameworks.position_projection.clone(),
        portfolio_snapshot: context.frameworks.portfolio_snapshot.clone(),
        wrappers: context.wrappers.clone(),
        configured_equity_symbols,
        usdc_tracking_enabled: context.ctx.chains.primary().assets.cash.is_some(),
        // Derived from the same Option the poller consumed, so the gate can
        // never require wallet slots the poller does not populate.
        wallet_polling_enabled,
        poll_freshness,
        notifier: notifier.clone(),
        queue: portfolio_snapshot_queue.clone(),
    });

    let trade_cqrs = super::TradeProcessingCqrs {
        pool: context.pool.clone(),
        onchain_trade: context.frameworks.onchain_trade,
        position: context.frameworks.position,
        position_projection: context.frameworks.position_projection,
        offchain_order: context.frameworks.offchain_order,
        order_placer,
        execution_threshold: context.execution_threshold,
        hedging: context.ctx.assets.clone(),
        counter_trade_submission_lock,
        close_flatten_policy,
        close_flatten_ramp,
        poll_status_queue: poll_status_queue.clone(),
        hedge_queue: hedge_queue.clone(),
        poll_interval,
    };

    let maintenance_interval = context.executor.maintenance_interval();

    // One accounting entry per watched chain: the primary reuses the main
    // provider; secondaries take theirs from `watch_providers` (shared with
    // the per-chain monitors below).
    let mut chain_accounting = std::collections::BTreeMap::new();
    for watched in context.ctx.chains.watched() {
        let provider = if watched.chain == context.ctx.chains.primary().chain {
            context.provider.clone()
        } else {
            watch_providers.get(&watched.chain).cloned().ok_or(
                ConductorSpawnError::MissingWatchWiring {
                    chain: watched.chain,
                    what: "accounting provider",
                },
            )?
        };
        chain_accounting.insert(
            watched.chain,
            crate::trading::onchain::trade_accountant::ChainAccounting {
                trading: watched.clone(),
                contracts: crate::onchain::raindex_contracts(watched),
                evm: ReadOnlyEvm::new(provider),
            },
        );
    }

    let accountant_ctx = Arc::new(AccountantCtx {
        chains: chain_accounting,
        notifier,
        disabled_asset_alerts: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        ctx: context.ctx.clone(),
        cache: context.cache,
        cqrs: trade_cqrs,
        vault_registry: context.frameworks.vault_registry,
        executor: context.executor.clone(),
        pool: context.pool.clone(),
        job_queue: job_queue.clone(),
    });

    let apalis_shutdown_token = CancellationToken::new();
    let apalis_shutdown_token_for_struct = apalis_shutdown_token.clone();

    let SupervisorStartupTokens {
        order_fill_monitors: mut order_fill_startup_tokens,
        inventory_monitor: inventory_startup,
        dashboard_trade_handoff_monitor: dashboard_trade_handoff_startup,
        executor_maintenance: executor_maintenance_startup,
        base_gas_monitor: base_gas_monitor_startup,
        ethereum_gas_monitor: ethereum_gas_monitor_startup,
        hyperevm_gas_monitor: hyperevm_gas_monitor_startup,
    } = context.supervisor_startup;

    // Fail-fast: exit if any supervised task dies, relying on systemd restart for recovery.
    // In test builds, use aggressive timeouts so a transient RPC failure doesn't
    // stall e2e tests for ~13 minutes of exponential backoff.
    let is_test = cfg!(any(test, feature = "test-support"));
    let mut supervisor_builder = SupervisorBuilder::default()
        .with_max_restart_attempts(if is_test { 2 } else { 10 })
        .with_max_backoff_exponent(if is_test { 2 } else { 8 })
        .with_base_restart_delay(std::time::Duration::from_secs(1))
        .with_dead_tasks_threshold(Some(0.0));

    // One fill watcher per watched chain, each with its own provider, poll
    // interval, namespaced scan queue, and readiness token. The primary
    // reuses the conductor's main provider; secondaries take theirs from
    // `watch_providers` (absent entries are a startup bug: the token map,
    // queue map, and provider map are all derived from the same registry).
    let primary_chain = context.ctx.chains.primary().chain;
    let watched_chains: Vec<st0x_config::HedgedChain> =
        context.ctx.chains.watched().cloned().collect();
    for watched in watched_chains {
        let chain = watched.chain;
        let provider = if chain == primary_chain {
            context.provider.clone()
        } else {
            watch_providers
                .get(&chain)
                .cloned()
                .ok_or(ConductorSpawnError::MissingWatchWiring {
                    chain,
                    what: "provider",
                })?
        };
        let queue = backfill_queues
            .for_chain(chain)
            .ok_or(ConductorSpawnError::MissingWatchWiring {
                chain,
                what: "backfill queue",
            })?
            .clone();
        let token = order_fill_startup_tokens.remove(&chain).ok_or(
            ConductorSpawnError::MissingWatchWiring {
                chain,
                what: "startup token",
            },
        )?;

        let poll_interval = watched.order_fill_poll_interval;
        let monitor = OrderFillMonitor::new(
            watched,
            queue,
            context.pool.clone(),
            provider,
            poll_interval,
        );

        supervisor_builder = supervisor_builder.with_task(
            &format!("order-fill-monitor-{chain}"),
            StartupTask {
                task: monitor,
                token,
            },
        );
    }

    let mut supervisor_builder = supervisor_builder
        .with_task(
            "inventory-monitor",
            StartupTask {
                task: inventory_monitor,
                token: inventory_startup,
            },
        )
        .with_task(
            "dashboard-trade-handoff-monitor",
            StartupTask {
                task: dashboard_trade_handoff_monitor,
                token: dashboard_trade_handoff_startup,
            },
        );

    log_optional_task_status("executor maintenance", maintenance_interval.is_some());

    if let Some(interval) = maintenance_interval {
        supervisor_builder = supervisor_builder.with_task(
            "executor-maintenance",
            StartupTask {
                task: ExecutorMaintenance::new(context.executor, interval),
                token: executor_maintenance_startup,
            },
        );
    } else {
        executor_maintenance_startup.acknowledge();
    }

    log_optional_task_status("gas monitors", gas_monitors.is_some());

    for (name, task) in gas_monitor_tasks(
        gas_monitors,
        [
            base_gas_monitor_startup,
            ethereum_gas_monitor_startup,
            hyperevm_gas_monitor_startup,
        ],
    ) {
        supervisor_builder = supervisor_builder.with_task(name, task);
    }

    let supervisor = supervisor_builder.build().run();

    let monitor = MonitorWiring {
        accountant_ctx,
        hedge_ctx,
        poll_status_ctx,
        reconcile_ctx,
        rejection_ctx,
        check_positions_ctx,
        portfolio_snapshot_ctx,
        rebalancing_check_ctx: rebalancing_service,
        seed_vault_registry_ctx,
        job_queue,
        hedge_queue,
        backfill_queues,
        dashboard_trade_delivery_queue,
        dashboard_trade_delivery_ctx,
        poll_status_queue,
        reconcile_queue,
        rejection_queue,
        check_positions_queue,
        portfolio_snapshot_queue,
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
        deliver_mint_authorization_queue,
        deliver_mint_authorization_ctx,
        record_bot_gas_receipt_cost_queue,
        record_bot_gas_receipt_cost_ctx,
        best_effort_failure_notifier: worker_failure_notifier.clone(),
        apalis_shutdown_token,
        seed_vault_registry_queue,
        startup_token: context.startup_token,
        #[cfg(any(test, feature = "test-support"))]
        failure_injector: context.failure_injector,
    }
    .spawn_apalis_monitor();

    Ok(Conductor {
        supervisor,
        monitor,
        job_cleanup,
        telemetry_writer,
        shutdown_token: context.shutdown_token,
        apalis_shutdown_token: apalis_shutdown_token_for_struct,
        worker_failure_notifier,
        cleanup_armed: true,
    })
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
    portfolio_snapshot_ctx: Arc<PortfolioSnapshotCtx>,
    rebalancing_check_ctx: Arc<RebalancingService>,
    seed_vault_registry_ctx: Arc<SeedVaultRegistryCtx>,
    job_queue: DexTradeAccountingJobQueue,
    hedge_queue: HedgeJobQueue,
    backfill_queues: BackfillQueues,
    dashboard_trade_delivery_queue: DashboardTradeDeliveryJobQueue,
    dashboard_trade_delivery_ctx: Arc<DashboardTradeDeliveryCtx>,
    poll_status_queue: PollOrderStatusJobQueue,
    reconcile_queue: ReconcileOrderFillJobQueue,
    rejection_queue: HandleOrderRejectionJobQueue,
    check_positions_queue: CheckPositionsJobQueue,
    portfolio_snapshot_queue: PortfolioSnapshotJobQueue,
    wrapped_equity_recovery_queue: WrappedEquityRecoveryJobQueue,
    wrapped_equity_recovery_ctx: Arc<WrappedEquityRecoveryCtx>,
    unwrapped_equity_recovery_queue: UnwrappedEquityRecoveryJobQueue,
    unwrapped_equity_recovery_ctx: Arc<UnwrappedEquityRecoveryCtx>,
    equity_check_scheduler: EquityRebalancingCheckScheduler,
    usdc_check_scheduler: UsdcRebalancingCheckScheduler,
    transfer_usdc_to_hedging_queue: TransferUsdcToHedgingJobQueue,
    transfer_usdc_to_hedging_ctx: Arc<TransferUsdcToHedgingCtx>,
    transfer_usdc_to_market_making_queue: TransferUsdcToMarketMakingJobQueue,
    transfer_usdc_to_market_making_ctx: Arc<TransferUsdcToMarketMakingCtx>,
    transfer_equity_to_market_making_queue: TransferEquityToMarketMakingJobQueue,
    transfer_equity_to_market_making_ctx: Arc<TransferEquityToMarketMakingCtx>,
    transfer_equity_to_hedging_queue: TransferEquityToHedgingJobQueue,
    transfer_equity_to_hedging_ctx: Arc<TransferEquityToHedgingCtx>,
    resume_tokenization_queue: ResumeTokenizationJobQueue,
    resume_tokenization_ctx: Arc<ResumeTokenizationCtx>,
    deliver_mint_authorization_queue: DeliverMintAuthorizationJobQueue,
    deliver_mint_authorization_ctx: Arc<DeliverMintAuthorizationCtx>,
    record_bot_gas_receipt_cost_queue: RecordBotGasReceiptCostJobQueue,
    record_bot_gas_receipt_cost_ctx: Arc<RecordBotGasReceiptCostCtx>,
    best_effort_failure_notifier: Arc<dyn Notifier>,
    apalis_shutdown_token: CancellationToken,
    seed_vault_registry_queue: SeedVaultRegistryJobQueue,
    startup_token: StartupToken,
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
            portfolio_snapshot_ctx,
            rebalancing_check_ctx,
            seed_vault_registry_ctx,
            job_queue,
            hedge_queue,
            backfill_queues,
            dashboard_trade_delivery_queue,
            dashboard_trade_delivery_ctx,
            poll_status_queue,
            reconcile_queue,
            rejection_queue,
            check_positions_queue,
            portfolio_snapshot_queue,
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
            deliver_mint_authorization_queue,
            deliver_mint_authorization_ctx,
            record_bot_gas_receipt_cost_queue,
            record_bot_gas_receipt_cost_ctx,
            best_effort_failure_notifier,
            apalis_shutdown_token,
            seed_vault_registry_queue,
            startup_token,
            #[cfg(any(test, feature = "test-support"))]
            failure_injector,
        } = self;

        #[cfg(any(test, feature = "test-support"))]
        let failure_injector_for_hedge = failure_injector.clone();
        #[cfg(any(test, feature = "test-support"))]
        let failure_injector_for_backfill = failure_injector.clone();
        #[cfg(any(test, feature = "test-support"))]
        let failure_injector_for_dashboard_trade_delivery = failure_injector.clone();
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
        let failure_injector_for_portfolio_snapshot = failure_injector.clone();
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
        #[cfg(any(test, feature = "test-support"))]
        let failure_injector_for_deliver_mint_authorization = failure_injector.clone();
        #[cfg(any(test, feature = "test-support"))]
        let failure_injector_for_record_bot_gas_receipt_cost = failure_injector.clone();
        let failure_notify = Arc::new(TerminalFailureSignal::default());
        let failure_notify_for_hedge = failure_notify.clone();
        let failure_notify_for_backfill = failure_notify.clone();
        let failure_notify_for_dashboard_trade_delivery = failure_notify.clone();
        let failure_notify_for_poll = failure_notify.clone();
        let failure_notify_for_reconcile = failure_notify.clone();
        let failure_notify_for_rejection = failure_notify.clone();
        let failure_notify_for_equity_rebalancing_check = failure_notify.clone();
        let failure_notify_for_usdc_rebalancing_check = failure_notify.clone();
        let failure_notify_for_seed_vault_registry = failure_notify.clone();
        let failure_notify_for_check_positions = failure_notify.clone();
        let failure_notify_for_select = failure_notify.clone();
        let best_effort_notifier_for_portfolio_snapshot = Arc::clone(&best_effort_failure_notifier);

        let accountant_ctx_for_backfill = accountant_ctx.clone();

        tokio::spawn(startup_token.wrap(async move {
            // Supervised: an incomplete backfill can omit onchain trades. One
            // worker per watched chain, each on its own namespaced queue, so
            // one chain's scan backlog never occupies another chain's worker.
            let mut monitor = Monitor::new();
            for (worker_chain, chain_queue) in backfill_queues.iter() {
                let chain_queue = chain_queue.clone();
                let worker_chain = *worker_chain;
                let accountant_ctx_for_backfill = accountant_ctx_for_backfill.clone();
                let failure_notify_for_backfill = failure_notify_for_backfill.clone();
                #[cfg(any(test, feature = "test-support"))]
                let failure_injector_for_backfill = failure_injector_for_backfill.clone();
                monitor = monitor.register(move |index| {
                    build_supervised_worker!(
                        ::<AccountantCtx<Prov, Exec>, BackfillRange>,
                        format_args!("{worker_chain}-{index}"),
                        chain_queue.clone(),
                        accountant_ctx_for_backfill.clone(),
                        failure_notify_for_backfill.clone(),
                        #[cfg(any(test, feature = "test-support"))]
                        failure_injector_for_backfill.clone(),
                    )
                });
            }

            let monitor = monitor
                .should_restart(|_ctx, _error, _attempt| false)
                .register(move |index| {
                    // Supervised: losing trade accounting compromises hedging state.
                    build_supervised_worker!(
                        ::<AccountantCtx<Prov, Exec>, AccountForDexTrade>,
                        index,
                        job_queue.clone(),
                        accountant_ctx.clone(),
                        failure_notify.clone(),
                        #[cfg(any(test, feature = "test-support"))]
                        failure_injector.clone(),
                    )
                })
                .register(move |index| {
                    // Supervised: the durable dashboard trade handoff must not diverge.
                    build_supervised_worker!(
                        ::<DashboardTradeDeliveryCtx, DeliverDashboardTrade>,
                        index,
                        dashboard_trade_delivery_queue.clone(),
                        dashboard_trade_delivery_ctx.clone(),
                        failure_notify_for_dashboard_trade_delivery.clone(),
                        #[cfg(any(test, feature = "test-support"))]
                        failure_injector_for_dashboard_trade_delivery.clone(),
                    )
                })
                .register(move |index| {
                    // Supervised: hedge placement is part of the core trading loop.
                    build_supervised_worker!(
                        ::<HedgeCtx, PlaceHedge>,
                        index,
                        hedge_queue.clone(),
                        hedge_ctx.clone(),
                        failure_notify_for_hedge.clone(),
                        #[cfg(any(test, feature = "test-support"))]
                        failure_injector_for_hedge.clone(),
                    )
                })
                .register(move |index| {
                    // Supervised: order polling protects offchain execution state.
                    build_supervised_worker!(
                        ::<PollOrderStatusCtx<Exec>, PollOrderStatus>,
                        index,
                        poll_status_queue.clone(),
                        poll_status_ctx.clone(),
                        failure_notify_for_poll.clone(),
                        #[cfg(any(test, feature = "test-support"))]
                        failure_injector_for_poll.clone(),
                    )
                })
                .register(move |index| {
                    // Supervised: fill reconciliation protects position correctness.
                    build_supervised_worker!(
                        ::<ReconcileOrderFillCtx, ReconcileOrderFill>,
                        index,
                        reconcile_queue.clone(),
                        reconcile_ctx.clone(),
                        failure_notify_for_reconcile.clone(),
                        #[cfg(any(test, feature = "test-support"))]
                        failure_injector_for_reconcile.clone(),
                    )
                })
                .register(move |index| {
                    // Supervised: rejection handling releases core order state.
                    build_supervised_worker!(
                        ::<HandleOrderRejectionCtx, HandleOrderRejection>,
                        index,
                        rejection_queue.clone(),
                        rejection_ctx.clone(),
                        failure_notify_for_rejection.clone(),
                        #[cfg(any(test, feature = "test-support"))]
                        failure_injector_for_rejection.clone(),
                    )
                })
                .register(move |index| {
                    // Supervised: trading cannot proceed with an invalid vault registry.
                    build_supervised_worker!(
                        ::<SeedVaultRegistryCtx, SeedVaultRegistry>,
                        index,
                        seed_vault_registry_queue.clone(),
                        seed_vault_registry_ctx.clone(),
                        failure_notify_for_seed_vault_registry.clone(),
                        #[cfg(any(test, feature = "test-support"))]
                        failure_injector_for_seed_vault_registry.clone(),
                    )
                })
                .register(move |index| {
                    // Supervised: position checks decide when hedges are required.
                    build_supervised_worker!(
                        ::<CheckPositionsCtx<Exec>, CheckPositions>,
                        index,
                        check_positions_queue.clone(),
                        check_positions_ctx.clone(),
                        failure_notify_for_check_positions.clone(),
                        #[cfg(any(test, feature = "test-support"))]
                        failure_injector_for_check_positions.clone(),
                    )
                })
                .register(move |index| {
                    // Best-effort, not supervised (RAI-1457 review fix): a
                    // daily reporting job must never trip the conductor-wide
                    // fail-stop and halt trading over a transient capture
                    // failure. Mirrors `ResumeTokenizationAggregate`'s
                    // registration below.
                    build_best_effort_worker!(
                        ::<PortfolioSnapshotCtx, PortfolioSnapshotJob>,
                        index,
                        portfolio_snapshot_queue.clone(),
                        portfolio_snapshot_ctx.clone(),
                        best_effort_notifier_for_portfolio_snapshot.clone(),
                        #[cfg(any(test, feature = "test-support"))]
                        failure_injector_for_portfolio_snapshot.clone(),
                    )
                });

            let equity_service = Arc::clone(&rebalancing_check_ctx);
            let usdc_service = rebalancing_check_ctx;
            let equity_queue = equity_check_scheduler.queue().clone();
            let usdc_queue = usdc_check_scheduler.queue().clone();
            let apalis_monitor = monitor
                .register(move |index| {
                    // Supervised: missed equity checks can strand inventory imbalance.
                    build_supervised_worker!(
                        ::<RebalancingService, EquityRebalancingCheck>,
                        index,
                        equity_queue.clone(),
                        equity_service.clone(),
                        failure_notify_for_equity_rebalancing_check.clone(),
                        #[cfg(any(test, feature = "test-support"))]
                        failure_injector_for_equity_rebalancing_check.clone(),
                    )
                })
                .register(move |index| {
                    // Supervised: missed USDC checks can strand inventory imbalance.
                    build_supervised_worker!(
                        ::<RebalancingService, UsdcRebalancingCheck>,
                        index,
                        usdc_queue.clone(),
                        usdc_service.clone(),
                        failure_notify_for_usdc_rebalancing_check.clone(),
                        #[cfg(any(test, feature = "test-support"))]
                        failure_injector_for_usdc_rebalancing_check.clone(),
                    )
                });

            let apalis_monitor = register_wrapped_equity_recovery_worker(
                apalis_monitor,
                wrapped_equity_recovery_ctx,
                wrapped_equity_recovery_queue,
                Arc::clone(&best_effort_failure_notifier),
                #[cfg(any(test, feature = "test-support"))]
                failure_injector_for_wrapped_equity_recovery,
            );

            let apalis_monitor = register_unwrapped_equity_recovery_worker(
                apalis_monitor,
                unwrapped_equity_recovery_ctx,
                unwrapped_equity_recovery_queue,
                Arc::clone(&best_effort_failure_notifier),
                #[cfg(any(test, feature = "test-support"))]
                failure_injector_for_unwrapped_equity_recovery,
            );

            let apalis_monitor = register_transfer_usdc_to_hedging_worker(
                apalis_monitor,
                transfer_usdc_to_hedging_ctx,
                transfer_usdc_to_hedging_queue,
                Arc::clone(&best_effort_failure_notifier),
                #[cfg(any(test, feature = "test-support"))]
                failure_injector_for_transfer_usdc_to_hedging,
            );

            let apalis_monitor = register_transfer_usdc_to_market_making_worker(
                apalis_monitor,
                transfer_usdc_to_market_making_ctx,
                transfer_usdc_to_market_making_queue,
                Arc::clone(&best_effort_failure_notifier),
                #[cfg(any(test, feature = "test-support"))]
                failure_injector_for_transfer_usdc_to_market_making,
            );

            let apalis_monitor = register_transfer_equity_to_market_making_worker(
                apalis_monitor,
                transfer_equity_to_market_making_ctx,
                transfer_equity_to_market_making_queue,
                Arc::clone(&best_effort_failure_notifier),
                #[cfg(any(test, feature = "test-support"))]
                failure_injector_for_transfer_equity_to_market_making,
            );

            let apalis_monitor = register_transfer_equity_to_hedging_worker(
                apalis_monitor,
                transfer_equity_to_hedging_ctx,
                transfer_equity_to_hedging_queue,
                Arc::clone(&best_effort_failure_notifier),
                #[cfg(any(test, feature = "test-support"))]
                failure_injector_for_transfer_equity_to_hedging,
            );

            let apalis_monitor = register_resume_tokenization_worker(
                apalis_monitor,
                resume_tokenization_ctx,
                resume_tokenization_queue,
                Arc::clone(&best_effort_failure_notifier),
                #[cfg(any(test, feature = "test-support"))]
                failure_injector_for_resume_tokenization,
            );

            let apalis_monitor = register_deliver_mint_authorization_worker(
                apalis_monitor,
                deliver_mint_authorization_ctx,
                deliver_mint_authorization_queue,
                Arc::clone(&best_effort_failure_notifier),
                #[cfg(any(test, feature = "test-support"))]
                failure_injector_for_deliver_mint_authorization,
            );

            let apalis_monitor = register_record_bot_gas_receipt_cost_worker(
                apalis_monitor,
                record_bot_gas_receipt_cost_ctx,
                record_bot_gas_receipt_cost_queue,
                best_effort_failure_notifier,
                #[cfg(any(test, feature = "test-support"))]
                failure_injector_for_record_bot_gas_receipt_cost,
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
                // `select!` evaluates this precondition ONCE, when the select
                // is entered at monitor-task startup -- it is not re-checked
                // on each wakeup. It therefore only disarms this branch in
                // the edge case where shutdown was already requested before
                // the monitor task's first poll. A terminal failure landing
                // during a later drain window still fires this branch,
                // aborting the drain; `wait_for_completion`'s drain branch
                // alerts on exactly that path, so the failure surfaces
                // rather than being suppressed.
                TerminalFailureInfo {
                    worker,
                    context,
                    source,
                } = failure_notify_for_select.notified(),
                    if !is_draining.is_cancelled() =>
                {
                    Err(MonitorTaskError::TerminalJobFailure {
                        worker,
                        context,
                        source,
                    })
                }
                result = apalis_monitor.run_with_signal(shutdown_signal) => match result {
                    Ok(()) => Ok(()),
                    Err(source) => Err(MonitorTaskError::UnexpectedExit { source: Some(source) }),
                },
            }
        }))
    }
}

fn alerts_with_wallet(
    ctx: &Ctx,
) -> Result<Option<(&AlertsCtx, &OnchainWalletCtx)>, ConductorSpawnError> {
    ctx.alerts.as_ref().map_or(Ok(None), |alerts| {
        ctx.wallet()
            .map(|wallet| Some((alerts, wallet)))
            .map_err(|_| ConductorSpawnError::AlertsRequireWallet)
    })
}

struct GasMonitors {
    base: GasMonitor,
    ethereum: GasMonitor,
    hyperevm: Option<GasMonitor>,
}

fn gas_monitor_tasks(
    monitors: Option<GasMonitors>,
    tokens: [StartupToken; 3],
) -> impl Iterator<Item = (&'static str, StartupTask<GasMonitor>)> {
    let monitors = match monitors {
        Some(GasMonitors {
            base,
            ethereum,
            hyperevm,
        }) => [Some(base), Some(ethereum), hyperevm],
        None => [None, None, None],
    };
    [
        "gas-monitor-base",
        "gas-monitor-ethereum",
        "gas-monitor-hyperevm",
    ]
    .into_iter()
    .zip(monitors)
    .zip(tokens)
    .filter_map(|((name, monitor), token)| {
        if let Some(task) = monitor {
            Some((name, StartupTask { task, token }))
        } else {
            token.acknowledge();
            None
        }
    })
}

/// Builds monitors with each chain's own provider, wallet and validated threshold.
fn build_gas_monitors<BaseProv, EthereumProv, HyperProv>(
    alerts: &AlertsCtx,
    base: (BaseProv, Address),
    ethereum: (EthereumProv, Address),
    hyperevm: Option<(HyperProv, Address)>,
    notifier: &Arc<dyn Notifier>,
) -> Result<GasMonitors, ConductorSpawnError>
where
    BaseProv: Provider + Send + Sync + 'static,
    EthereumProv: Provider + Send + Sync + 'static,
    HyperProv: Provider + Send + Sync + 'static,
{
    if alerts.low_balance_threshold_wei(Chain::HyperEvm).is_some() != hyperevm.is_some() {
        return Err(ConductorSpawnError::HyperEvmGasWalletMismatch);
    }

    let monitor = |chain, balance_reader, wallet| -> Result<GasMonitor, ConductorSpawnError> {
        let threshold_wei = alerts
            .low_balance_threshold_wei(chain)
            .ok_or(ConductorSpawnError::MissingGasThreshold { chain })?;
        Ok(GasMonitor {
            balance_reader,
            notifier: notifier.clone(),
            wallet,
            chain,
            threshold_wei,
            poll_interval: alerts.poll_interval,
            realert_interval: alerts.realert_interval,
        })
    };

    Ok(GasMonitors {
        base: monitor(
            Chain::Base,
            Arc::new(ProviderBalanceReader::new(base.0)),
            base.1,
        )?,
        ethereum: monitor(
            Chain::Ethereum,
            Arc::new(ProviderBalanceReader::new(ethereum.0)),
            ethereum.1,
        )?,
        hyperevm: hyperevm
            .map(|(provider, wallet)| {
                monitor(
                    Chain::HyperEvm,
                    Arc::new(ProviderBalanceReader::new(provider)),
                    wallet,
                )
            })
            .transpose()?,
    })
}

fn log_optional_task_status(task_name: &str, is_configured: bool) {
    if is_configured {
        info!("Started {task_name} task");
    } else {
        debug!("{task_name} not configured", task_name = task_name);
    }
}

/// Registers the wrapped-equity recovery worker against the apalis monitor.
/// Extracted (like every `register_*` sibling below) to keep
/// `spawn_apalis_monitor` under the cognitive-complexity limit.
fn register_wrapped_equity_recovery_worker(
    monitor: Monitor,
    recovery_ctx: Arc<WrappedEquityRecoveryCtx>,
    recovery_queue: WrappedEquityRecoveryJobQueue,
    notifier: Arc<dyn Notifier>,
    #[cfg(any(test, feature = "test-support"))] failure_injector: FailureInjector,
) -> Monitor {
    // Best effort: one recoverable wallet item must not halt the trading loop.
    monitor.register(move |index| {
        build_best_effort_worker!(
            ::<WrappedEquityRecoveryCtx, WrappedEquityRecoveryJob>,
            index,
            recovery_queue.clone(),
            recovery_ctx.clone(),
            notifier.clone(),
            #[cfg(any(test, feature = "test-support"))]
            failure_injector.clone(),
        )
    })
}

/// Registers the unwrapped-equity recovery worker. Same pattern as
/// `register_wrapped_equity_recovery_worker`.
fn register_unwrapped_equity_recovery_worker(
    monitor: Monitor,
    recovery_ctx: Arc<UnwrappedEquityRecoveryCtx>,
    recovery_queue: UnwrappedEquityRecoveryJobQueue,
    notifier: Arc<dyn Notifier>,
    #[cfg(any(test, feature = "test-support"))] failure_injector: FailureInjector,
) -> Monitor {
    // Best effort: one recoverable wallet item must not halt the trading loop.
    monitor.register(move |index| {
        build_best_effort_worker!(
            ::<UnwrappedEquityRecoveryCtx, UnwrappedEquityRecoveryJob>,
            index,
            recovery_queue.clone(),
            recovery_ctx.clone(),
            notifier.clone(),
            #[cfg(any(test, feature = "test-support"))]
            failure_injector.clone(),
        )
    })
}

/// Registers the `TransferUsdcToHedging` worker, which processes
/// Base->Alpaca USDC transfers.
fn register_transfer_usdc_to_hedging_worker(
    monitor: Monitor,
    transfer_ctx: Arc<TransferUsdcToHedgingCtx>,
    transfer_queue: TransferUsdcToHedgingJobQueue,
    notifier: Arc<dyn Notifier>,
    #[cfg(any(test, feature = "test-support"))] failure_injector: FailureInjector,
) -> Monitor {
    // Best effort: a poison transfer is an operator-owned dead letter.
    monitor.register(move |index| {
        build_best_effort_worker!(
            ::<TransferUsdcToHedgingCtx, TransferUsdcToHedging>,
            index,
            transfer_queue.clone(),
            transfer_ctx.clone(),
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
    transfer_ctx: Arc<TransferUsdcToMarketMakingCtx>,
    transfer_queue: TransferUsdcToMarketMakingJobQueue,
    notifier: Arc<dyn Notifier>,
    #[cfg(any(test, feature = "test-support"))] failure_injector: FailureInjector,
) -> Monitor {
    // Best effort: a poison transfer is an operator-owned dead letter.
    monitor.register(move |index| {
        build_best_effort_worker!(
            ::<TransferUsdcToMarketMakingCtx, TransferUsdcToMarketMaking>,
            index,
            transfer_queue.clone(),
            transfer_ctx.clone(),
            notifier.clone(),
            #[cfg(any(test, feature = "test-support"))]
            failure_injector.clone(),
        )
    })
}

/// Registers the `TransferEquityToMarketMaking` worker, which processes
/// equity mints. Terminal per-transfer failures remain dead-lettered in
/// apalis without stopping this worker or the conductor.
fn register_transfer_equity_to_market_making_worker(
    monitor: Monitor,
    transfer_ctx: Arc<TransferEquityToMarketMakingCtx>,
    transfer_queue: TransferEquityToMarketMakingJobQueue,
    notifier: Arc<dyn Notifier>,
    #[cfg(any(test, feature = "test-support"))] failure_injector: FailureInjector,
) -> Monitor {
    // Best effort: a poison transfer is an operator-owned dead letter.
    monitor.register(move |index| {
        build_best_effort_worker!(
            ::<TransferEquityToMarketMakingCtx, TransferEquityToMarketMaking>,
            index,
            transfer_queue.clone(),
            transfer_ctx.clone(),
            notifier.clone(),
            #[cfg(any(test, feature = "test-support"))]
            failure_injector.clone(),
        )
    })
}

/// Sibling of [`register_transfer_equity_to_market_making_worker`] for the
/// redemption (market-making -> hedging) direction.
fn register_transfer_equity_to_hedging_worker(
    monitor: Monitor,
    transfer_ctx: Arc<TransferEquityToHedgingCtx>,
    transfer_queue: TransferEquityToHedgingJobQueue,
    notifier: Arc<dyn Notifier>,
    #[cfg(any(test, feature = "test-support"))] failure_injector: FailureInjector,
) -> Monitor {
    // Best effort: a poison transfer is an operator-owned dead letter.
    monitor.register(move |index| {
        build_best_effort_worker!(
            ::<TransferEquityToHedgingCtx, TransferEquityToHedging>,
            index,
            transfer_queue.clone(),
            transfer_ctx.clone(),
            notifier.clone(),
            #[cfg(any(test, feature = "test-support"))]
            failure_injector.clone(),
        )
    })
}

/// Registers the `ResumeTokenizationAggregate` worker, which runs
/// interrupted mint/redemption aggregates off the startup path so a slow or
/// down issuer cannot block monitoring.
fn register_resume_tokenization_worker(
    monitor: Monitor,
    resume_ctx: Arc<ResumeTokenizationCtx>,
    resume_queue: ResumeTokenizationJobQueue,
    notifier: Arc<dyn Notifier>,
    #[cfg(any(test, feature = "test-support"))] failure_injector: FailureInjector,
) -> Monitor {
    // Best effort: one recovery dead letter must not block live trading.
    monitor.register(move |index| {
        build_best_effort_worker!(
            ::<ResumeTokenizationCtx, ResumeTokenizationAggregate>,
            index,
            resume_queue.clone(),
            resume_ctx.clone(),
            notifier.clone(),
            #[cfg(any(test, feature = "test-support"))]
            failure_injector.clone(),
        )
    })
}

/// Registers the `DeliverMintAuthorization` worker. Best-effort -- a stuck
/// authorization must never halt hedging or fill detection.
fn register_deliver_mint_authorization_worker(
    monitor: Monitor,
    delivery_ctx: Arc<DeliverMintAuthorizationCtx>,
    delivery_queue: DeliverMintAuthorizationJobQueue,
    notifier: Arc<dyn Notifier>,
    #[cfg(any(test, feature = "test-support"))] failure_injector: FailureInjector,
) -> Monitor {
    // Best effort: one failed authorization must not halt unrelated work.
    monitor.register(move |index| {
        build_best_effort_worker!(
            ::<DeliverMintAuthorizationCtx, DeliverMintAuthorization>,
            index,
            delivery_queue.clone(),
            delivery_ctx.clone(),
            notifier.clone(),
            #[cfg(any(test, feature = "test-support"))]
            failure_injector.clone(),
        )
    })
}

/// Registers the `RecordBotGasReceiptCost` worker. Best-effort, matching
/// every other per-item dead-lettering worker: a terminal failure recording
/// one receipt's cost must never block or slow hedging (see ADR 0017).
fn register_record_bot_gas_receipt_cost_worker(
    monitor: Monitor,
    record_ctx: Arc<RecordBotGasReceiptCostCtx>,
    record_queue: RecordBotGasReceiptCostJobQueue,
    notifier: Arc<dyn Notifier>,
    #[cfg(any(test, feature = "test-support"))] failure_injector: FailureInjector,
) -> Monitor {
    // Best effort: receipt-cost bookkeeping cannot interrupt hedging.
    monitor.register(move |index| {
        build_best_effort_worker!(
            ::<RecordBotGasReceiptCostCtx, RecordBotGasReceiptCost>,
            index,
            record_queue.clone(),
            record_ctx.clone(),
            notifier.clone(),
            #[cfg(any(test, feature = "test-support"))]
            failure_injector.clone(),
        )
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::sync::RwLock;
    use std::time::Duration;

    use alloy::primitives::{U256, address};
    use alloy::providers::mock::Asserter;
    use alloy::providers::{ProviderBuilder, RootProvider};
    use async_trait::async_trait;
    use st0x_config::{
        ChainAssets, ChainCashAsset, ChainEquities, ChainEquityAsset, OperationMode,
        create_test_ctx_with_order_owner,
    };
    use st0x_event_sorcery::test_store;
    use st0x_execution::{FractionalShares, Symbol};
    use st0x_finance::Usdc;
    use st0x_float_macro::float;
    use st0x_raindex::Raindex;
    use st0x_tokenization::IssuerRequestId;
    use st0x_tokenization::mock::MockTokenizer;
    use st0x_wrapper::{MockWrapper, Wrapper};
    use task_supervisor::SupervisedTask;

    use super::*;
    use crate::alerts::CapturingNotifier;
    use crate::conductor::job::{BackpressureStreak, Job};
    use crate::equity_redemption::RedemptionAggregateId;
    use crate::mint_authorization::ConfiguredMintAuthorizer;
    use crate::native_gas::ConfiguredGasReadiness;
    use crate::onchain::mock::MockRaindex;
    use crate::rebalancing::equity::ChainEquityServices;
    use crate::rebalancing::equity::{
        EquityTransferServices, MintError, MintTransferError, RedemptionError,
        ResumeEquityToHedging, ResumeEquityToMarketMaking,
    };
    use crate::rebalancing::trigger::{GuardGeneration, GuardState, InProgressGuard};
    use crate::rebalancing::usdc::{
        PreflightAlertGate, ResumeAlpacaToBase, ResumeBaseToAlpaca, UsdcGuardRelease,
        UsdcTransferError,
    };
    use crate::startup::StartupBarrier;
    use crate::test_utils::{setup_test_apalis_pool, setup_test_pools};
    use crate::usdc_rebalance::UsdcRebalanceId;
    use crate::vault_lookup::MockVaultLookup;

    fn equity_asset(trading: OperationMode, rebalancing: OperationMode) -> ChainEquityAsset {
        ChainEquityAsset {
            tokenized_equity: Address::ZERO,
            tokenized_equity_derivative: Address::ZERO,
            vault_ids: vec![],
            trading,
            rebalancing,
            wrapped_equity_recovery: OperationMode::Disabled,
            operational_limit: None,
        }
    }

    #[test]
    fn configured_equity_symbols_include_trading_or_rebalancing_enabled_only() {
        // The portfolio's notion of "configured" gates snapshot marks and
        // inventory vault discovery: a symbol with both switches off must not
        // count, and either switch alone must.
        let mut ctx = create_test_ctx_with_order_owner(Address::ZERO);
        ctx.chains.primary_mut().assets = ChainAssets {
            equities: ChainEquities {
                operational_limit: None,
                symbols: HashMap::from([
                    (
                        Symbol::new("TRADE").unwrap(),
                        equity_asset(OperationMode::Enabled, OperationMode::Disabled),
                    ),
                    (
                        Symbol::new("REBAL").unwrap(),
                        equity_asset(OperationMode::Disabled, OperationMode::Enabled),
                    ),
                    (
                        Symbol::new("OFF").unwrap(),
                        equity_asset(OperationMode::Disabled, OperationMode::Disabled),
                    ),
                ]),
            },
            cash: None,
        };

        let symbols = configured_equity_symbols(&ctx);

        assert_eq!(
            symbols,
            HashSet::from([Symbol::new("TRADE").unwrap(), Symbol::new("REBAL").unwrap()]),
            "trading-enabled and rebalancing-enabled symbols count; fully disabled ones do not"
        );
    }

    /// Vault polling is what seeds a chain's inventory slots, so every
    /// watched chain needs an entry of its own -- keyed on that chain's
    /// orderbook and vault owner, not the primary's.
    #[test]
    fn vault_polling_entries_cover_every_watched_chain() {
        let ethereum_orderbook = Address::repeat_byte(0xe0);
        let ethereum_vault_owner = Address::repeat_byte(0xe1);
        let mut ctx = create_test_ctx_with_order_owner(Address::ZERO);
        ctx.chains.insert_secondary(
            st0x_config::HedgedChain::test()
                .chain(Chain::Ethereum)
                .orderbook(ethereum_orderbook)
                .vault_owner(ethereum_vault_owner)
                .call(),
        );

        let entries = vault_polling_entries(
            &ctx,
            &ProviderBuilder::new().connect_mocked_client(Asserter::new()),
            &BTreeMap::from([(
                Chain::Ethereum,
                ProviderBuilder::new().connect_mocked_client(Asserter::new()),
            )]),
        )
        .unwrap();

        assert_eq!(
            entries
                .iter()
                .map(|entry| (entry.chain, entry.orderbook, entry.vault_owner))
                .collect::<Vec<_>>(),
            vec![
                (
                    Chain::Base,
                    ctx.chains.primary().orderbook,
                    ctx.chains.primary().vault_owner
                ),
                (Chain::Ethereum, ethereum_orderbook, ethereum_vault_owner),
            ],
            "each watched chain must get its own vault-polling entry"
        );
    }

    /// A watched chain's balances are captured in the daily snapshot, so its
    /// market-making slots must gate the capture too -- with only the assets
    /// that chain's own table declares.
    #[test]
    fn market_making_slots_cover_every_watched_chain() {
        let secondary_symbol = Symbol::new("NVDA").unwrap();
        let mut ctx = create_test_ctx_with_order_owner(Address::ZERO);
        ctx.chains.primary_mut().assets = ChainAssets {
            equities: ChainEquities {
                operational_limit: None,
                symbols: HashMap::from([(
                    Symbol::new("AAPL").unwrap(),
                    equity_asset(OperationMode::Enabled, OperationMode::Disabled),
                )]),
            },
            cash: Some(ChainCashAsset {
                vault_ids: vec![B256::repeat_byte(0xc0)],
                rebalancing: OperationMode::Disabled,
                operational_limit: None,
            }),
        };
        ctx.chains.insert_secondary(
            st0x_config::HedgedChain::test()
                .chain(Chain::Ethereum)
                .assets(ChainAssets {
                    equities: ChainEquities {
                        operational_limit: None,
                        symbols: HashMap::from([(
                            secondary_symbol.clone(),
                            equity_asset(OperationMode::Enabled, OperationMode::Disabled),
                        )]),
                    },
                    cash: None,
                })
                .call(),
        );

        let slots = market_making_slots(&ctx);

        assert_eq!(
            slots
                .iter()
                .map(|(chain, slots)| (
                    *chain,
                    slots.equity_symbols.clone(),
                    slots.usdc_tracking_enabled
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    Chain::Base,
                    HashSet::from([Symbol::new("AAPL").unwrap()]),
                    true
                ),
                (Chain::Ethereum, HashSet::from([secondary_symbol]), false),
            ],
            "each watched chain contributes the slots its own assets table declares"
        );
    }

    #[test]
    fn alerts_require_a_configured_wallet() {
        let mut ctx = create_test_ctx_with_order_owner(Address::ZERO);
        ctx.alerts = Some(AlertsCtx::for_test(
            BTreeMap::from([
                (Chain::Base, U256::from(100_u64)),
                (Chain::Ethereum, U256::from(200_u64)),
            ]),
            Duration::from_secs(300),
            Duration::from_secs(3600),
        ));

        assert!(matches!(
            alerts_with_wallet(&ctx),
            Err(ConductorSpawnError::AlertsRequireWallet)
        ));
    }

    /// `AlertsCtx::new` refuses a config missing a monitored chain's
    /// threshold, so reaching this error means the monitored-chain list and
    /// the configured map have drifted apart. It must fail rather than
    /// substitute a value: zero never alerts, and anything else alerts at the
    /// wrong balance.
    #[tokio::test]
    async fn a_monitored_chain_without_a_threshold_fails_to_build_its_monitor() {
        let alerts = AlertsCtx::for_test(
            BTreeMap::from([(Chain::Base, U256::from(100_u64))]),
            Duration::from_secs(300),
            Duration::from_secs(3600),
        );

        let error = build_gas_monitors(
            &alerts,
            (
                ProviderBuilder::new().connect_mocked_client(Asserter::new()),
                address!("0x0000000000000000000000000000000000000ba5"),
            ),
            (
                ProviderBuilder::new().connect_mocked_client(Asserter::new()),
                address!("0x0000000000000000000000000000000000000e78"),
            ),
            None::<(RootProvider, Address)>,
            &(Arc::new(crate::alerts::LogNotifier) as Arc<dyn Notifier>),
        );

        let Err(error) = error else {
            panic!("a monitored chain with no threshold must not build a monitor")
        };

        assert!(
            matches!(
                error,
                ConductorSpawnError::MissingGasThreshold {
                    chain: Chain::Ethereum
                }
            ),
            "expected MissingGasThreshold for Ethereum, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn absent_alerts_acknowledge_all_gas_startup_slots_without_tasks() {
        let barrier = StartupBarrier::new();
        let names: Vec<_> =
            gas_monitor_tasks(None, [barrier.token(), barrier.token(), barrier.token()])
                .map(|(name, _)| name)
                .collect();
        assert_eq!(names, Vec::<&str>::new());
        tokio::time::timeout(Duration::from_secs(1), barrier.wait())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn unselected_hyperevm_acknowledges_its_slot_without_a_task() {
        let alerts = AlertsCtx::for_test(
            BTreeMap::from([
                (Chain::Base, U256::from(100_u64)),
                (Chain::Ethereum, U256::from(200_u64)),
            ]),
            Duration::from_secs(300),
            Duration::from_secs(3600),
        );
        let monitors = build_gas_monitors(
            &alerts,
            (
                ProviderBuilder::new().connect_mocked_client(Asserter::new()),
                Address::random(),
            ),
            (
                ProviderBuilder::new().connect_mocked_client(Asserter::new()),
                Address::random(),
            ),
            None::<(RootProvider, Address)>,
            &(Arc::new(crate::alerts::LogNotifier) as Arc<dyn Notifier>),
        )
        .unwrap();
        let barrier = StartupBarrier::new();
        let base = barrier.token();
        let ethereum = barrier.token();
        let tasks: Vec<_> = gas_monitor_tasks(
            Some(monitors),
            [base.clone(), ethereum.clone(), barrier.token()],
        )
        .collect();
        assert_eq!(
            tasks.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
            vec!["gas-monitor-base", "gas-monitor-ethereum"]
        );
        tokio::time::timeout(Duration::from_millis(10), barrier.wait())
            .await
            .unwrap_err();
        base.acknowledge();
        ethereum.acknowledge();
        tokio::time::timeout(Duration::from_secs(1), barrier.wait())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn selected_hyperevm_task_acknowledges_only_its_own_startup_slot_when_polled() {
        let alerts = AlertsCtx::for_test(
            BTreeMap::from([
                (Chain::Base, U256::from(100_u64)),
                (Chain::Ethereum, U256::from(200_u64)),
                (Chain::HyperEvm, U256::from(300_u64)),
            ]),
            Duration::from_secs(300),
            Duration::from_secs(3600),
        );
        let wallets = [Chain::Base, Chain::Ethereum, Chain::HyperEvm].map(|_| {
            let asserter = Asserter::new();
            asserter.push_success(&U256::from(1000_u64));
            (
                ProviderBuilder::new().connect_mocked_client(asserter),
                Address::random(),
            )
        });
        let [base_wallet, ethereum_wallet, hyperevm_wallet] = wallets;
        let monitors = build_gas_monitors(
            &alerts,
            base_wallet,
            ethereum_wallet,
            Some(hyperevm_wallet),
            &(Arc::new(crate::alerts::LogNotifier) as Arc<dyn Notifier>),
        )
        .unwrap();
        let barriers = [
            StartupBarrier::new(),
            StartupBarrier::new(),
            StartupBarrier::new(),
        ];
        let mut tasks: Vec<_> = gas_monitor_tasks(
            Some(monitors),
            barriers.each_ref().map(StartupBarrier::token),
        )
        .collect();
        assert_eq!(
            tasks
                .iter()
                .map(|(name, task)| (*name, task.task.chain))
                .collect::<Vec<_>>(),
            vec![
                ("gas-monitor-base", Chain::Base),
                ("gas-monitor-ethereum", Chain::Ethereum),
                ("gas-monitor-hyperevm", Chain::HyperEvm),
            ]
        );
        for (index, (_, task)) in tasks.iter_mut().enumerate() {
            for barrier in &barriers[index..] {
                tokio::time::timeout(Duration::from_millis(10), barrier.wait())
                    .await
                    .unwrap_err();
            }
            tokio::time::timeout(Duration::from_millis(10), task.run())
                .await
                .unwrap_err();
            tokio::time::timeout(Duration::from_secs(1), barriers[index].wait())
                .await
                .unwrap();
        }
    }

    #[test]
    fn hyperevm_monitor_requires_matching_wallet_selection() {
        for selected in [false, true] {
            for wallet_present in [false, true] {
                let mut thresholds = BTreeMap::from([
                    (Chain::Base, U256::from(100_u64)),
                    (Chain::Ethereum, U256::from(200_u64)),
                ]);
                if selected {
                    thresholds.insert(Chain::HyperEvm, U256::from(300_u64));
                }
                let alerts = AlertsCtx::for_test(
                    thresholds,
                    Duration::from_secs(300),
                    Duration::from_secs(3600),
                );
                let result = build_gas_monitors(
                    &alerts,
                    (
                        ProviderBuilder::new().connect_mocked_client(Asserter::new()),
                        Address::random(),
                    ),
                    (
                        ProviderBuilder::new().connect_mocked_client(Asserter::new()),
                        Address::random(),
                    ),
                    wallet_present.then(|| {
                        (
                            ProviderBuilder::new().connect_mocked_client(Asserter::new()),
                            Address::random(),
                        )
                    }),
                    &(Arc::new(crate::alerts::LogNotifier) as Arc<dyn Notifier>),
                );
                if selected == wallet_present {
                    assert_eq!(
                        result.unwrap().hyperevm.map(|monitor| monitor.chain),
                        selected.then_some(Chain::HyperEvm)
                    );
                } else {
                    assert!(matches!(
                        result,
                        Err(ConductorSpawnError::HyperEvmGasWalletMismatch)
                    ));
                }
            }
        }
    }

    #[tokio::test]
    async fn gas_monitors_use_their_chain_provider_wallet_and_threshold() {
        let base_asserter = Asserter::new();
        base_asserter.push_success(&U256::from(11_u64));
        let ethereum_asserter = Asserter::new();
        ethereum_asserter.push_success(&U256::from(22_u64));

        let hyperevm_asserter = Asserter::new();
        hyperevm_asserter.push_success(&U256::from(33_u64));
        let hyperevm_wallet = address!("0x0000000000000000000000000000000000000999");
        let alerts = AlertsCtx::for_test(
            BTreeMap::from([
                (Chain::Base, U256::from(100_u64)),
                (Chain::Ethereum, U256::from(200_u64)),
                (Chain::HyperEvm, U256::from(300_u64)),
            ]),
            Duration::from_secs(300),
            Duration::from_secs(3600),
        );
        let base_wallet = address!("0x0000000000000000000000000000000000000ba5");
        let ethereum_wallet = address!("0x0000000000000000000000000000000000000e78");
        let GasMonitors {
            base,
            ethereum,
            hyperevm,
        } = build_gas_monitors(
            &alerts,
            (
                ProviderBuilder::new().connect_mocked_client(base_asserter),
                base_wallet,
            ),
            (
                ProviderBuilder::new().connect_mocked_client(ethereum_asserter),
                ethereum_wallet,
            ),
            Some((
                ProviderBuilder::new().connect_mocked_client(hyperevm_asserter),
                hyperevm_wallet,
            )),
            &(Arc::new(crate::alerts::LogNotifier) as Arc<dyn Notifier>),
        )
        .unwrap();

        let hyperevm = hyperevm.unwrap();
        assert_eq!(hyperevm.wallet, hyperevm_wallet);
        assert_eq!(hyperevm.chain, Chain::HyperEvm);
        assert_eq!(hyperevm.threshold_wei, U256::from(300_u64));
        assert_eq!(
            hyperevm
                .balance_reader
                .native_balance(hyperevm.wallet)
                .await
                .unwrap(),
            U256::from(33_u64)
        );
        assert_eq!(base.wallet, base_wallet);
        assert_eq!(base.chain, Chain::Base);
        assert_eq!(base.threshold_wei, U256::from(100_u64));
        assert_eq!(
            base.balance_reader
                .native_balance(base.wallet)
                .await
                .unwrap(),
            U256::from(11_u64),
            "Base monitor must read through the Base provider"
        );

        assert_eq!(ethereum.wallet, ethereum_wallet);
        assert_eq!(ethereum.chain, Chain::Ethereum);
        assert_eq!(ethereum.threshold_wei, U256::from(200_u64));
        assert_eq!(
            ethereum
                .balance_reader
                .native_balance(ethereum.wallet)
                .await
                .unwrap(),
            U256::from(22_u64),
            "Ethereum monitor must read through the Ethereum provider"
        );
    }

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
            _chain: Chain,
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

    struct NoopGuardRelease;

    #[async_trait]
    impl UsdcGuardRelease for NoopGuardRelease {
        async fn release_unless_durably_held(&self) {}
    }

    struct PoisonThenHealthyUsdcResume {
        poison_id: UsdcRebalanceId,
        healthy_completed: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl ResumeBaseToAlpaca for PoisonThenHealthyUsdcResume {
        async fn resume_base_to_alpaca(
            &self,
            id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            if id == &self.poison_id {
                return Err(UsdcTransferError::WithdrawalFailed {
                    status: "injected terminal withdrawal failure".to_owned(),
                });
            }

            self.healthy_completed.notify_one();
            Ok(())
        }
    }

    #[async_trait]
    impl ResumeAlpacaToBase for PoisonThenHealthyUsdcResume {
        async fn resume_alpaca_to_base(
            &self,
            id: &UsdcRebalanceId,
            _amount: Usdc,
        ) -> Result<(), UsdcTransferError> {
            if id == &self.poison_id {
                return Err(UsdcTransferError::DepositFailed {
                    status: "injected terminal deposit failure".to_owned(),
                });
            }

            self.healthy_completed.notify_one();
            Ok(())
        }
    }

    #[async_trait]
    impl ResumeEquityToMarketMaking for PoisonThenHealthyMintResume {
        async fn resume_equity_to_market_making(
            &self,
            issuer_request_id: &IssuerRequestId,
            _symbol: &Symbol,
            _chain: Chain,
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
        job_queue: TransferEquityToMarketMakingJobQueue,
    ) -> Arc<TransferEquityToMarketMakingCtx> {
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());
        let wrapper: Arc<dyn Wrapper> = Arc::new(MockWrapper::new());
        let services = EquityTransferServices {
            chains: BTreeMap::from([(
                Chain::Base,
                ChainEquityServices {
                    wallet: Address::ZERO,
                    raindex,
                    vault_lookup: Arc::new(MockVaultLookup::new()),
                    tokenizer: Arc::new(MockTokenizer::new()),
                    wrapper,
                    mint_authorizer: ConfiguredMintAuthorizer::Disabled,
                    gas_readiness: ConfiguredGasReadiness::Unwired,
                    equities: ChainEquities::default(),
                },
            )]),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
        };

        Arc::new(TransferEquityToMarketMakingCtx {
            transfer,
            equity_in_progress: Arc::new(RwLock::new(HashMap::new())),
            mint_store: Arc::new(test_store(cqrs_pool, services)),
            transfer_services: EquityTransferServices::panicking(),
            job_queue,
        })
    }

    #[tokio::test]
    async fn terminal_equity_transfer_is_dead_lettered_without_stopping_worker() {
        let (cqrs_pool, apalis_pool) = setup_test_pools().await;
        let mut queue = TransferEquityToHedgingJobQueue::new(&apalis_pool);
        let poison_id = RedemptionAggregateId::generate();
        let healthy_id = RedemptionAggregateId::generate();
        let symbol = Symbol::new("AAPL").unwrap();
        let generation = GuardGeneration::from_parts(NonZeroU32::new(1).unwrap(), 1);
        let equity_in_progress = Arc::new(RwLock::new(HashMap::from([(
            symbol.clone(),
            GuardState::ActiveTransfer { generation },
        )])));
        let services = EquityTransferServices {
            chains: BTreeMap::from([(
                Chain::Base,
                ChainEquityServices {
                    wallet: Address::ZERO,
                    raindex: Arc::new(MockRaindex::new()),
                    vault_lookup: Arc::new(MockVaultLookup::new()),
                    tokenizer: Arc::new(MockTokenizer::new()),
                    wrapper: Arc::new(MockWrapper::new()),
                    mint_authorizer: ConfiguredMintAuthorizer::Disabled,
                    gas_readiness: ConfiguredGasReadiness::Unwired,
                    equities: ChainEquities::default(),
                },
            )]),
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
        };
        let redemption_store = Arc::new(test_store(cqrs_pool, services));

        queue
            .push(TransferEquityToHedging {
                chain: Chain::Base,
                aggregate_id: poison_id.clone(),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(float!(1)),
                generation,
                backpressure_streak: BackpressureStreak::default(),
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
            equity_in_progress: equity_in_progress.clone(),
            redemption_store,
            job_queue: queue.clone(),
        });
        let monitor = register_transfer_equity_to_hedging_worker(
            Monitor::new().should_restart(|_ctx, _error, _attempt| false),
            transfer_ctx,
            queue,
            Arc::new(crate::alerts::LogNotifier),
            FailureInjector::new(),
        );
        let monitor_handle = tokio::spawn(async move { monitor.run().await });

        wait_for_terminal_job::<TransferEquityToHedging>(&apalis_pool).await;

        push_queue
            .push(TransferEquityToHedging {
                chain: Chain::Base,
                aggregate_id: healthy_id,
                symbol,
                quantity: FractionalShares::new(float!(1)),
                generation: GuardGeneration::default(),
                backpressure_streak: BackpressureStreak::default(),
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
        assert_eq!(
            equity_in_progress
                .read()
                .unwrap()
                .get(&Symbol::new("AAPL").unwrap()),
            None,
            "a terminal redemption with no aggregate must release its exact guard"
        );

        assert!(
            !monitor_handle.is_finished(),
            "a terminal equity-transfer job must not stop the conductor monitor",
        );
        monitor_handle.abort();
    }

    #[tokio::test]
    async fn terminal_usdc_transfer_does_not_stop_worker_or_sibling_work() {
        let apalis_pool = setup_test_apalis_pool().await;
        let mut queue = TransferUsdcToHedgingJobQueue::new(&apalis_pool);
        let amount = Usdc::new(float!(100));
        let poison_id = UsdcRebalanceId(uuid::Uuid::new_v4());

        queue
            .push(TransferUsdcToHedging {
                id: poison_id.clone(),
                amount,
                revert_redrive_attempts: 0,
                backpressure_streak: BackpressureStreak::default(),
            })
            .await
            .unwrap();
        let mut push_queue = queue.clone();

        let healthy_completed = Arc::new(tokio::sync::Notify::new());
        let notifier = Arc::new(CapturingNotifier::default());
        let transfer_ctx = Arc::new(TransferUsdcToHedgingCtx {
            transfer: Arc::new(PoisonThenHealthyUsdcResume {
                poison_id,
                healthy_completed: healthy_completed.clone(),
            }),
            timeout: Duration::from_secs(1),
            job_queue: queue.clone(),
            max_burn_revert_redrives: 1,
            notifier: notifier.clone(),
        });
        let failure_injector = FailureInjector::new();
        let monitor = register_transfer_usdc_to_hedging_worker(
            Monitor::new().should_restart(|_ctx, _error, _attempt| false),
            transfer_ctx,
            queue,
            notifier.clone(),
            failure_injector,
        );
        let monitor_handle = tokio::spawn(async move { monitor.run().await });

        wait_for_terminal_job::<TransferUsdcToHedging>(&apalis_pool).await;
        push_queue
            .push(TransferUsdcToHedging {
                id: UsdcRebalanceId(uuid::Uuid::new_v4()),
                amount,
                revert_redrive_attempts: 0,
                backpressure_streak: BackpressureStreak::default(),
            })
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(15), healthy_completed.notified())
            .await
            .expect("the USDC worker must process sibling work after a terminal failure");
        assert!(
            !monitor_handle.is_finished(),
            "a terminal USDC transfer must not stop the conductor monitor",
        );

        tokio::time::timeout(Duration::from_secs(3), async {
            while notifier.messages().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the terminal failure must alert the operator");
        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "the terminal failure must alert once: {messages:?}"
        );
        assert!(
            messages[0].contains(TransferUsdcToHedging::WORKER_NAME),
            "the alert must name the worker: {}",
            messages[0],
        );
        assert!(
            messages[0].contains("injected terminal withdrawal failure"),
            "the alert must include the terminal error: {}",
            messages[0],
        );
        monitor_handle.abort();
    }

    #[tokio::test]
    async fn terminal_market_making_usdc_transfer_alerts_once_and_continues() {
        let apalis_pool = setup_test_apalis_pool().await;
        let mut queue = TransferUsdcToMarketMakingJobQueue::new(&apalis_pool);
        let amount = Usdc::new(float!(100));
        let poison_id = UsdcRebalanceId(uuid::Uuid::new_v4());

        queue
            .push(TransferUsdcToMarketMaking {
                id: poison_id.clone(),
                amount,
                revert_redrive_attempts: 0,
                backpressure_streak: BackpressureStreak::default(),
            })
            .await
            .unwrap();
        let mut push_queue = queue.clone();

        let healthy_completed = Arc::new(tokio::sync::Notify::new());
        let notifier = Arc::new(CapturingNotifier::default());
        let transfer_ctx = Arc::new(TransferUsdcToMarketMakingCtx {
            transfer: Arc::new(PoisonThenHealthyUsdcResume {
                poison_id,
                healthy_completed: healthy_completed.clone(),
            }),
            job_queue: queue.clone(),
            max_burn_revert_redrives: 1,
            notifier: notifier.clone(),
            usdc_guard: Arc::new(NoopGuardRelease),
            preflight_alerts: Arc::new(PreflightAlertGate::default()),
        });
        let monitor = register_transfer_usdc_to_market_making_worker(
            Monitor::new().should_restart(|_ctx, _error, _attempt| false),
            transfer_ctx,
            queue,
            notifier.clone(),
            FailureInjector::new(),
        );
        let monitor_handle = tokio::spawn(async move { monitor.run().await });

        wait_for_terminal_job::<TransferUsdcToMarketMaking>(&apalis_pool).await;
        push_queue
            .push(TransferUsdcToMarketMaking {
                id: UsdcRebalanceId(uuid::Uuid::new_v4()),
                amount,
                revert_redrive_attempts: 0,
                backpressure_streak: BackpressureStreak::default(),
            })
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(15), healthy_completed.notified())
            .await
            .expect("the market-making USDC worker must process sibling work after dead-lettering");
        assert!(
            !monitor_handle.is_finished(),
            "a terminal market-making USDC transfer must not stop the conductor monitor",
        );

        tokio::time::timeout(Duration::from_secs(3), async {
            while notifier.messages().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the terminal failure must alert the operator");
        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "the terminal failure must alert once: {messages:?}",
        );
        assert!(
            messages[0].contains(TransferUsdcToMarketMaking::WORKER_NAME),
            "the alert must name the worker: {}",
            messages[0],
        );
        assert!(
            messages[0].contains("injected terminal deposit failure"),
            "the alert must include the terminal error: {}",
            messages[0],
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
        let generation = GuardGeneration::from_parts(NonZeroU32::new(1).unwrap(), 2);

        queue
            .push(TransferEquityToMarketMaking {
                chain: Chain::Base,
                issuer_request_id: poison_id.clone(),
                symbol: symbol.clone(),
                quantity: FractionalShares::new(float!(1)),
                generation,

                backpressure_streak: BackpressureStreak::default(),
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
            queue.clone(),
        );
        transfer_ctx
            .equity_in_progress
            .write()
            .unwrap()
            .insert(symbol.clone(), GuardState::ActiveTransfer { generation });
        let equity_in_progress = transfer_ctx.equity_in_progress.clone();
        let monitor = register_transfer_equity_to_market_making_worker(
            Monitor::new().should_restart(|_ctx, _error, _attempt| false),
            transfer_ctx,
            queue,
            Arc::new(crate::alerts::LogNotifier),
            FailureInjector::new(),
        );
        let monitor_handle = tokio::spawn(async move { monitor.run().await });

        wait_for_terminal_job::<TransferEquityToMarketMaking>(&apalis_pool).await;
        assert_eq!(
            equity_in_progress.read().unwrap().get(&symbol),
            None,
            "a terminal mint with no aggregate must release its exact guard"
        );
        let replacement_claim =
            InProgressGuard::try_claim_for_transfer(symbol.clone(), equity_in_progress.clone())
                .expect("the next equity check must be able to claim the released symbol");
        drop(replacement_claim);

        push_queue
            .push(TransferEquityToMarketMaking {
                chain: Chain::Base,
                issuer_request_id: healthy_id,
                symbol,
                quantity: FractionalShares::new(float!(1)),
                generation: GuardGeneration::default(),

                backpressure_streak: BackpressureStreak::default(),
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
