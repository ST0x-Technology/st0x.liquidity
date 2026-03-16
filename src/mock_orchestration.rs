//! Apalis-driven mock orchestration for e2e tests.
//!
//! Defines job types and a worker that controls mock broker behavior
//! at runtime via the same Apalis job queue the production system uses.
//! This gives test orchestration visibility through apalis-board and
//! enables deterministic, job-driven test scenarios.
//!
//! # Usage
//!
//! ```ignore
//! // In e2e test setup, after creating TestInfra:
//! let broker = Arc::new(infra.broker_service);
//! let mock_monitor = spawn_mock_monitor(&pool, broker).await;
//!
//! // Submit commands via the queue:
//! let mut queue: MockCommandQueue = SqliteStorage::new(&pool);
//! queue.push_task(TaskBuilder::new(MockBrokerCommand::CloseMarket).build()).await?;
//! ```

use std::sync::Arc;

use apalis::prelude::{Data, Monitor, WorkerBuilder};
use apalis_codec::json::JsonCodec;
use apalis_sqlite::fetcher::SqliteFetcher;
use apalis_sqlite::{CompactType, SqliteStorage};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use st0x_execution::Symbol;
use st0x_execution::alpaca_broker_api::{AlpacaBrokerMock, MockMode};
use st0x_finance::{Usd, Usdc};

/// Persistent job queue for mock broker commands.
pub type MockCommandQueue = SqliteStorage<MockBrokerCommand, JsonCodec<CompactType>, SqliteFetcher>;

/// A job that controls mock broker behavior at runtime.
///
/// Submitted to the Apalis job queue by e2e tests and processed by a
/// worker running alongside the production workers. Each command maps
/// to a method on [`AlpacaBrokerMock`].
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum MockBrokerCommand {
    /// Switch the broker mock's response mode.
    SetMode(MockMode),

    /// Open the simulated market.
    OpenMarket,

    /// Close the simulated market.
    CloseMarket,

    /// Set the fill price for a specific symbol.
    SetFillPrice { symbol: Symbol, price: Usd },

    /// Set how many polls before a symbol's order fills.
    SetFillDelay {
        symbol: Symbol,
        polls_before_fill: usize,
    },

    /// Set the mock wallet's USDC balance.
    SetWalletBalance { balance: Usdc },
}

/// Shared context passed to the mock orchestration worker via Apalis `data()`.
pub struct MockOrchestrationCtx {
    pub broker: Arc<AlpacaBrokerMock>,
}

/// Spawns a background Apalis monitor that processes [`MockBrokerCommand`]s.
///
/// Run this alongside the production bot in e2e tests. The monitor
/// connects to the same SQLite database, so commands pushed by the
/// test are visible in apalis-board alongside production jobs.
pub async fn spawn_mock_monitor(
    pool: &SqlitePool,
    broker: Arc<AlpacaBrokerMock>,
) -> JoinHandle<()> {
    let mock_queue: MockCommandQueue = SqliteStorage::new(pool);
    let ctx = Arc::new(MockOrchestrationCtx { broker });

    tokio::spawn(async move {
        let monitor = Monitor::new().register(move |index| {
            WorkerBuilder::new(format!("mock-broker-worker-{index}"))
                .backend(mock_queue.clone())
                .data(ctx.clone())
                .build(handle_mock_broker_command)
        });

        if let Err(error) = monitor.run().await {
            warn!(%error, "Mock orchestration monitor exited");
        }
    })
}

/// Apalis worker function that applies [`MockBrokerCommand`]s to the
/// broker mock.
async fn handle_mock_broker_command(
    command: MockBrokerCommand,
    ctx: Data<Arc<MockOrchestrationCtx>>,
) {
    match command {
        MockBrokerCommand::SetMode(mode) => {
            debug!(?mode, "Setting broker mode");
            ctx.broker.set_mode(mode);
        }

        MockBrokerCommand::OpenMarket => {
            debug!("Opening market");
            ctx.broker.set_market_open();
        }

        MockBrokerCommand::CloseMarket => {
            debug!("Closing market");
            ctx.broker.set_market_closed();
        }

        MockBrokerCommand::SetFillPrice { symbol, price } => {
            debug!(%symbol, %price, "Setting fill price");
            ctx.broker.set_symbol_fill_price(symbol, price.inner());
        }

        MockBrokerCommand::SetFillDelay {
            symbol,
            polls_before_fill,
        } => {
            debug!(%symbol, polls_before_fill, "Setting fill delay");
            ctx.broker.set_symbol_fill_delay(symbol, polls_before_fill);
        }

        MockBrokerCommand::SetWalletBalance { balance } => {
            debug!(%balance, "Setting wallet USDC balance");
            ctx.broker.set_wallet_usdc_balance(balance.inner());
        }
    }
}
