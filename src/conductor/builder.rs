use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types;
use futures_util::Stream;
use sqlx::SqlitePool;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tracing::info;

use st0x_broker::Broker;

use crate::bindings::IOrderBookV5::{ClearV3, TakeOrderV3};
use crate::dual_write::DualWriteContext;
use crate::env::Config;
use crate::onchain::trade::TradeEvent;
use crate::symbol::cache::SymbolCache;

use super::{
    Conductor, spawn_event_processor, spawn_onchain_event_receiver, spawn_order_poller,
    spawn_periodic_accumulated_position_check, spawn_queue_processor,
};

type ClearStream = Box<dyn Stream<Item = Result<(ClearV3, Log), sol_types::Error>> + Unpin + Send>;
type TakeStream =
    Box<dyn Stream<Item = Result<(TakeOrderV3, Log), sol_types::Error>> + Unpin + Send>;

struct CommonFields<P, B> {
    config: Config,
    pool: SqlitePool,
    cache: SymbolCache,
    provider: P,
    broker: B,
    dual_write_context: DualWriteContext,
}

pub(crate) struct Initial;

pub(crate) struct WithBrokerMaintenance {
    broker_maintenance: Option<JoinHandle<()>>,
}

pub(crate) struct WithDexStreams {
    broker_maintenance: Option<JoinHandle<()>>,
    clear_stream: ClearStream,
    take_stream: TakeStream,
    event_sender: UnboundedSender<(TradeEvent, Log)>,
    event_receiver: UnboundedReceiver<(TradeEvent, Log)>,
}

pub(crate) struct ConductorBuilder<P, B, State> {
    common: CommonFields<P, B>,
    state: State,
}

impl<P: Provider + Clone + Send + 'static, B: Broker + Clone + Send + 'static>
    ConductorBuilder<P, B, Initial>
{
    pub(crate) fn new(
        config: Config,
        pool: SqlitePool,
        cache: SymbolCache,
        provider: P,
        broker: B,
        dual_write_context: DualWriteContext,
    ) -> Self {
        Self {
            common: CommonFields {
                config,
                pool,
                cache,
                provider,
                broker,
                dual_write_context,
            },
            state: Initial,
        }
    }

    pub(crate) fn with_broker_maintenance(
        self,
        broker_maintenance: Option<JoinHandle<()>>,
    ) -> ConductorBuilder<P, B, WithBrokerMaintenance> {
        ConductorBuilder {
            common: self.common,
            state: WithBrokerMaintenance { broker_maintenance },
        }
    }
}

impl<P: Provider + Clone + Send + 'static, B: Broker + Clone + Send + 'static>
    ConductorBuilder<P, B, WithBrokerMaintenance>
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
    ) -> ConductorBuilder<P, B, WithDexStreams> {
        let (event_sender, event_receiver) =
            tokio::sync::mpsc::unbounded_channel::<(TradeEvent, Log)>();

        ConductorBuilder {
            common: self.common,
            state: WithDexStreams {
                broker_maintenance: self.state.broker_maintenance,
                clear_stream: Box::new(clear_stream),
                take_stream: Box::new(take_stream),
                event_sender,
                event_receiver,
            },
        }
    }
}

impl<P: Provider + Clone + Send + 'static, B: Broker + Clone + Send + 'static>
    ConductorBuilder<P, B, WithDexStreams>
{
    pub(crate) fn spawn(self) -> Conductor {
        info!("Starting conductor orchestration");

        let broker_maintenance = self.state.broker_maintenance;

        if broker_maintenance.is_some() {
            info!("Started broker maintenance tasks");
        } else {
            info!("No broker maintenance tasks needed");
        }

        let order_poller = spawn_order_poller(
            &self.common.config,
            &self.common.pool,
            self.common.broker.clone(),
        );
        let dex_event_receiver = spawn_onchain_event_receiver(
            self.state.event_sender,
            self.state.clear_stream,
            self.state.take_stream,
        );
        let event_processor =
            spawn_event_processor(self.common.pool.clone(), self.state.event_receiver);
        let position_checker = spawn_periodic_accumulated_position_check(
            self.common.broker.clone(),
            self.common.pool.clone(),
            self.common.dual_write_context.clone(),
        );
        let queue_processor = spawn_queue_processor(
            self.common.broker,
            &self.common.config,
            &self.common.pool,
            &self.common.cache,
            self.common.provider,
        );

        Conductor {
            broker_maintenance,
            order_poller,
            dex_event_receiver,
            event_processor,
            position_checker,
            queue_processor,
        }
    }
}
