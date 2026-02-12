//! Typestate builder for constructing a fully-wired Conductor instance.

use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types;
use futures_util::Stream;
use sqlite_es::SqliteCqrs;
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use st0x_execution::Executor;

use super::{
    Conductor, EventProcessingError, spawn_event_processor, spawn_inventory_poller,
    spawn_onchain_event_receiver, spawn_order_poller, spawn_periodic_accumulated_position_check,
    spawn_queue_processor,
};
use crate::bindings::IOrderBookV5::{ClearV3, TakeOrderV3};
use crate::config::Ctx;
use crate::inventory::InventorySnapshotAggregate;
use crate::offchain_order::OffchainOrderCqrs;
use crate::onchain::trade::TradeEvent;
use crate::onchain::vault::VaultService;
use crate::onchain_trade::OnChainTradeCqrs;
use crate::position::{PositionCqrs, PositionQuery};
use crate::symbol::cache::SymbolCache;
use crate::threshold::ExecutionThreshold;
use crate::vault_registry::VaultRegistryAggregate;

type ClearStream = Box<dyn Stream<Item = Result<(ClearV3, Log), sol_types::Error>> + Unpin + Send>;
type TakeStream =
    Box<dyn Stream<Item = Result<(TakeOrderV3, Log), sol_types::Error>> + Unpin + Send>;

pub(crate) struct CqrsFrameworks {
    pub(crate) onchain_trade_cqrs: Arc<OnChainTradeCqrs>,
    pub(crate) position_cqrs: Arc<PositionCqrs>,
    pub(crate) position_query: Arc<PositionQuery>,
    pub(crate) offchain_order_cqrs: Arc<OffchainOrderCqrs>,
    pub(crate) vault_registry_cqrs: SqliteCqrs<VaultRegistryAggregate>,
    pub(crate) snapshot_cqrs: SqliteCqrs<InventorySnapshotAggregate>,
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
    P: Provider + Clone + Send + 'static,
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

        let inventory_poller = match self.common.ctx.order_owner() {
            Ok(order_owner) => {
                let vault_service = Arc::new(VaultService::new(
                    self.common.provider.clone(),
                    self.common.ctx.evm.orderbook,
                ));
                Some(spawn_inventory_poller(
                    self.common.pool.clone(),
                    vault_service,
                    self.common.executor.clone(),
                    self.common.ctx.evm.orderbook,
                    order_owner,
                    self.common.frameworks.snapshot_cqrs,
                ))
            }
            Err(error) => {
                warn!(%error, "Inventory poller disabled: could not resolve order owner");
                None
            }
        };
        log_optional_task_status("inventory poller", inventory_poller.is_some());

        let order_poller = spawn_order_poller(
            &self.common.ctx,
            &self.common.pool,
            self.common.executor.clone(),
            self.common.frameworks.offchain_order_cqrs.clone(),
            self.common.frameworks.position_cqrs.clone(),
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
            self.common.pool.clone(),
            self.common.frameworks.position_cqrs.clone(),
            self.common.frameworks.position_query.clone(),
            self.common.frameworks.offchain_order_cqrs.clone(),
            self.common.execution_threshold,
        );
        let trade_cqrs = super::TradeProcessingCqrs {
            onchain_trade_cqrs: self.common.frameworks.onchain_trade_cqrs,
            position_cqrs: self.common.frameworks.position_cqrs,
            position_query: self.common.frameworks.position_query,
            offchain_order_cqrs: self.common.frameworks.offchain_order_cqrs,
            execution_threshold: self.common.execution_threshold,
        };
        let queue_processor = spawn_queue_processor(
            self.common.executor,
            &self.common.ctx,
            &self.common.pool,
            &self.common.cache,
            self.common.provider,
            trade_cqrs,
            self.common.frameworks.vault_registry_cqrs,
        );

        Conductor {
            executor_maintenance,
            order_poller,
            dex_event_receiver,
            event_processor,
            position_checker,
            queue_processor,
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
