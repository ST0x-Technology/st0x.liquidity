//! Typestate builder for constructing a fully-wired Conductor instance.

use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types;
use futures_util::Stream;
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tracing::info;

use st0x_event_sorcery::{Projection, Store};
use st0x_evm::ReadOnlyEvm;
use st0x_execution::Executor;

use super::{
    Conductor, EventProcessingError, TradingTasks, spawn_event_processor, spawn_inventory_poller,
    spawn_onchain_event_receiver, spawn_order_poller, spawn_periodic_accumulated_position_check,
    spawn_queue_processor,
};
use crate::bindings::IOrderBookV6::{ClearV3, TakeOrderV3};
use crate::config::Ctx;
use crate::inventory::InventorySnapshot;
use crate::offchain_order::OffchainOrder;
use crate::onchain::raindex::RaindexService;
use crate::onchain::trade::TradeEvent;
use crate::onchain_trade::OnChainTrade;
use crate::position::Position;
use crate::symbol::cache::SymbolCache;
use crate::threshold::ExecutionThreshold;
use crate::vault_registry::VaultRegistry;

type ClearStream = Box<dyn Stream<Item = Result<(ClearV3, Log), sol_types::Error>> + Unpin + Send>;
type TakeStream =
    Box<dyn Stream<Item = Result<(TakeOrderV3, Log), sol_types::Error>> + Unpin + Send>;

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

struct CommonFields<P, E> {
    ctx: Ctx,
    pool: SqlitePool,
    cache: SymbolCache,
    provider: P,
    executor: E,
    execution_threshold: ExecutionThreshold,
    frameworks: CqrsFrameworks,
}

pub(crate) struct Initial;

pub(crate) struct WithExecutorMaintenance {
    executor_maintenance: Option<JoinHandle<()>>,
}

pub(crate) struct WithDexStreams {
    executor_maintenance: Option<JoinHandle<()>>,
    clear_stream: ClearStream,
    take_stream: TakeStream,
    event_sender: UnboundedSender<(TradeEvent, Log)>,
    event_receiver: UnboundedReceiver<(TradeEvent, Log)>,
    rebalancer: Option<JoinHandle<()>>,
}

pub(crate) struct ConductorBuilder<P, E, State> {
    common: CommonFields<P, E>,
    state: State,
}

impl<P: Provider + Clone + Send + 'static, E: Executor + Clone + Send + 'static>
    ConductorBuilder<P, E, Initial>
{
    pub(crate) fn new(
        ctx: Ctx,
        pool: SqlitePool,
        cache: SymbolCache,
        provider: P,
        executor: E,
        execution_threshold: ExecutionThreshold,
        frameworks: CqrsFrameworks,
    ) -> Self {
        Self {
            common: CommonFields {
                ctx,
                pool,
                cache,
                provider,
                executor,
                execution_threshold,
                frameworks,
            },
            state: Initial,
        }
    }

    pub(crate) fn with_executor_maintenance(
        self,
        executor_maintenance: Option<JoinHandle<()>>,
    ) -> ConductorBuilder<P, E, WithExecutorMaintenance> {
        ConductorBuilder {
            common: self.common,
            state: WithExecutorMaintenance {
                executor_maintenance,
            },
        }
    }
}

impl<P: Provider + Clone + Send + 'static, E: Executor + Clone + Send + 'static>
    ConductorBuilder<P, E, WithExecutorMaintenance>
{
    pub(crate) fn with_dex_event_streams(
        self,
        clear_stream: impl Stream<Item = Result<(ClearV3, Log), sol_types::Error>>
        + Unpin
        + Send
        + 'static,
        take_stream: impl Stream<Item = Result<(TakeOrderV3, Log), sol_types::Error>>
        + Unpin
        + Send
        + 'static,
    ) -> ConductorBuilder<P, E, WithDexStreams> {
        let (event_sender, event_receiver) =
            tokio::sync::mpsc::unbounded_channel::<(TradeEvent, Log)>();

        ConductorBuilder {
            common: self.common,
            state: WithDexStreams {
                executor_maintenance: self.state.executor_maintenance,
                clear_stream: Box::new(clear_stream),
                take_stream: Box::new(take_stream),
                event_sender,
                event_receiver,
                rebalancer: None,
            },
        }
    }
}

impl<P, E> ConductorBuilder<P, E, WithDexStreams>
where
    P: Provider + Clone + Send + Sync + 'static,
    E: Executor + Clone + Send + 'static,
    EventProcessingError: From<E::Error>,
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

        let inventory_poller = Some(spawn_inventory_poller(
            raindex_service,
            self.common.executor.clone(),
            self.common.frameworks.vault_registry.clone(),
            self.common.ctx.evm.orderbook,
            order_owner,
            self.common.frameworks.snapshot,
        ));
        log_optional_task_status("inventory poller", inventory_poller.is_some());

        let order_poller = spawn_order_poller(
            &self.common.ctx,
            self.common.executor.clone(),
            (*self.common.frameworks.offchain_order_projection).clone(),
            self.common.frameworks.offchain_order.clone(),
            self.common.frameworks.position.clone(),
        );
        let dex_event_receiver = spawn_onchain_event_receiver(
            self.state.event_sender,
            self.state.clear_stream,
            self.state.take_stream,
        );
        let event_processor =
            spawn_event_processor(self.common.pool.clone(), self.state.event_receiver);
        let position_checker = spawn_periodic_accumulated_position_check(
            self.common.executor.clone(),
            self.common.frameworks.position.clone(),
            self.common.frameworks.position_projection.clone(),
            self.common.frameworks.offchain_order.clone(),
            self.common.execution_threshold,
            self.common.ctx.operational_limits.clone(),
            self.common.ctx.clone(),
        );
        let trade_cqrs = super::TradeProcessingCqrs {
            onchain_trade: self.common.frameworks.onchain_trade,
            position: self.common.frameworks.position,
            position_projection: self.common.frameworks.position_projection,
            offchain_order: self.common.frameworks.offchain_order,
            execution_threshold: self.common.execution_threshold,
            operational_limits: self.common.ctx.operational_limits.clone(),
        };
        let queue_processor = spawn_queue_processor(
            self.common.executor,
            &self.common.ctx,
            &self.common.pool,
            &self.common.cache,
            self.common.provider,
            trade_cqrs,
            self.common.frameworks.vault_registry,
        );

        Conductor {
            executor_maintenance,
            rebalancer,
            inventory_poller,
            trading_tasks: Some(TradingTasks {
                order_poller,
                dex_event_receiver,
                event_processor,
                position_checker,
                queue_processor,
            }),
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
