use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types;
use futures_util::Stream;
use sqlx::SqlitePool;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tracing::info;

use st0x_execution::Executor;

use crate::bindings::IOrderBookV5::{ClearV3, TakeOrderV3};
use crate::dual_write::DualWriteContext;
use crate::env::Config;
use crate::error::EventProcessingError;
use crate::onchain::trade::TradeEvent;
use crate::symbol::cache::SymbolCache;

use super::{
    Conductor, spawn_event_processor, spawn_onchain_event_receiver, spawn_order_poller,
    spawn_periodic_accumulated_position_check, spawn_queue_processor,
};

type ClearStream = Box<dyn Stream<Item = Result<(ClearV3, Log), sol_types::Error>> + Unpin + Send>;
type TakeStream =
    Box<dyn Stream<Item = Result<(TakeOrderV3, Log), sol_types::Error>> + Unpin + Send>;

struct CommonFields<P, E> {
    config: Config,
    pool: SqlitePool,
    cache: SymbolCache,
    provider: P,
    executor: E,
    dual_write_context: DualWriteContext,
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
        config: Config,
        pool: SqlitePool,
        cache: SymbolCache,
        provider: P,
        executor: E,
        dual_write_context: DualWriteContext,
    ) -> Self {
        Self {
            common: CommonFields {
                config,
                pool,
                cache,
                provider,
                executor,
                dual_write_context,
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

        let order_poller = spawn_order_poller(
            &self.common.config,
            &self.common.pool,
            self.common.executor.clone(),
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
            self.common.dual_write_context.clone(),
        );
        let queue_processor = spawn_queue_processor(
            self.common.executor,
            &self.common.config,
            &self.common.pool,
            &self.common.cache,
            self.common.provider,
        );

        Conductor {
            executor_maintenance,
            order_poller,
            dex_event_receiver,
            event_processor,
            position_checker,
            queue_processor,
            rebalancer,
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
