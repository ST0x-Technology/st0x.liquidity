//! Market making system for tokenized equities.
//!
//! Provides onchain liquidity via Raindex orders and hedges
//! directional exposure by executing offsetting trades on
//! traditional brokerages.

use apalis_board::axum::framework::{ApiBuilder, RegisterRoute};
use apalis_board::axum::sse::TracingBroadcaster;
use apalis_board::axum::ui::ServeUI;
use axum::{Extension, Router};
use sqlx::SqlitePool;
use std::sync::{Arc, Mutex, OnceLock};
use task_supervisor::{
    SupervisedTask, SupervisorBuilder, SupervisorError, SupervisorHandle, TaskResult,
};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::{AbortHandle, JoinError, JoinHandle};
use tracing::{error, info};

use st0x_dto::Statement;
use st0x_execution::MockExecutorCtx;

use st0x_config::{BrokerCtx, Ctx};

use crate::trading::offchain::hedge::HedgeJobQueue;
use crate::trading::onchain::trade_accountant::DexTradeAccountingJobQueue;

mod alpaca_wallet;
pub mod api;
#[cfg(any(test, feature = "test-support"))]
pub mod bindings;
#[cfg(not(any(test, feature = "test-support")))]
pub(crate) mod bindings;
pub mod cli;
mod conductor;
pub(crate) mod dashboard;
mod equity_redemption;
mod inventory;
#[cfg(feature = "mock")]
pub mod mock_orchestration;
mod offchain;
mod onchain;
mod onchain_trade;
mod position;
mod position_check;
mod rebalancing;
mod shares;
mod symbol;
mod tokenization;
mod trading;
#[cfg(feature = "mock")]
pub use tokenization::mock_api;
mod tokenized_equity_mint;
mod usdc_rebalance;
mod vault_registry;
mod wrapper;

pub use st0x_config::{FileLogGuard, TelemetryError, TelemetryGuard, mk_env_filter, setup_tracing};

#[cfg(any(test, feature = "test-support"))]
pub use offchain::order::{OffchainOrder, OffchainOrderId};
#[cfg(any(test, feature = "test-support"))]
pub use position::Position;
/// Returns the apalis job type identifier for the `CheckPositions` job,
/// for use in tests when querying or asserting against the persistent job
/// queue (the `Jobs.job_type` column).
#[cfg(any(test, feature = "test-support"))]
pub fn check_positions_job_type() -> &'static str {
    std::any::type_name::<position_check::CheckPositions>()
}
#[cfg(any(test, feature = "test-support"))]
pub use st0x_config::ExecutionThreshold;
#[cfg(any(test, feature = "test-support"))]
pub use st0x_config::TradingMode;
#[cfg(any(test, feature = "test-support"))]
pub use st0x_config::{
    AssetsConfig, CashAssetConfig, EquitiesConfig, EquityAssetConfig, OperationMode,
};
#[cfg(feature = "test-support")]
pub use st0x_config::{FailureInjector, JobKind};
#[cfg(any(test, feature = "test-support"))]
pub use st0x_config::{ImbalanceThreshold, RebalancingCtx, RebalancingCtxError, UsdcRebalancing};

#[cfg(test)]
mod integration_tests;
#[cfg(test)]
pub mod test_utils;

/// Returns the global apalis-board log broadcaster, creating it on first access.
///
/// The same instance must be wired into the tracing subscriber (so log
/// events are pushed into it) and into the board router as an `Extension`
/// (so the `/api/v1/events` SSE handler can read them out).
pub fn apalis_broadcaster() -> &'static Arc<Mutex<TracingBroadcaster>> {
    static BROADCASTER: OnceLock<Arc<Mutex<TracingBroadcaster>>> = OnceLock::new();
    BROADCASTER.get_or_init(TracingBroadcaster::create)
}

/// Shared application state passed to all axum handlers.
#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) ctx: Ctx,
    pub(crate) pool: SqlitePool,
    pub(crate) event_sender: broadcast::Sender<Statement>,
    pub(crate) inventory: Arc<inventory::BroadcastingInventory>,
    pub(crate) settings: st0x_dto::Settings,
}

#[tracing::instrument(skip_all, target = "startup", level = tracing::Level::INFO)]
pub async fn run_bot_session(ctx: Ctx) -> anyhow::Result<()> {
    let (event_sender, _) = broadcast::channel::<Statement>(256);
    run_bot_session_with_event_channel(ctx, event_sender).await
}

#[tracing::instrument(skip_all, target = "startup", level = tracing::Level::INFO)]
pub async fn run_bot_session_with_event_channel(
    ctx: Ctx,
    event_sender: broadcast::Sender<Statement>,
) -> anyhow::Result<()> {
    let pool = ctx.get_sqlite_pool().await?;
    sqlx::migrate!().set_ignore_missing(true).run(&pool).await?;
    conductor::setup_apalis_tables(&pool).await?;

    let inventory = Arc::new(inventory::BroadcastingInventory::new(
        inventory::InventoryView::default(),
        event_sender.clone(),
    ));

    let server_supervisor =
        spawn_server_supervisor(&ctx, &pool, event_sender.clone(), inventory.clone());
    let bot_task = tokio::spawn(Box::pin(run_conductor_session(
        ctx,
        pool,
        event_sender,
        inventory,
    )));

    await_shutdown(server_supervisor, bot_task).await?;

    info!(target: "startup", "Shutdown complete");
    Ok(())
}

/// Long-running axum HTTP server, supervised so a bind/serve failure
/// doesn't silently take the API and dashboard down for the rest of
/// the bot's lifetime.
#[derive(Clone)]
struct ServerTask {
    router: Router,
    port: u16,
}

impl SupervisedTask for ServerTask {
    async fn run(&mut self) -> TaskResult {
        let listener = TcpListener::bind(("0.0.0.0", self.port)).await?;
        info!(target: "startup", port = self.port, "Server listening");
        axum::serve(listener, self.router.clone()).await?;
        Err("axum server exited unexpectedly".into())
    }
}

fn spawn_server_supervisor(
    ctx: &Ctx,
    pool: &SqlitePool,
    event_sender: broadcast::Sender<Statement>,
    inventory: Arc<inventory::BroadcastingInventory>,
) -> SupervisorHandle {
    let state = AppState {
        ctx: ctx.clone(),
        pool: pool.clone(),
        event_sender,
        inventory,
        settings: dashboard::settings_from_ctx(ctx),
    };

    let main_router = Router::new()
        .merge(api::routes())
        .nest("/api", dashboard::routes())
        .with_state(state);

    // apalis-board's embedded UI is built with hardcoded absolute paths
    // (`/apalis-board-web-*`) and the wasm calls `/api/v1/*` directly,
    // so it must own its origin. Run it on its own port instead of
    // nesting under the main router.
    let dex_storage = DexTradeAccountingJobQueue::new(pool).into_storage();
    let hedge_storage = HedgeJobQueue::new(pool).into_storage();
    let board_api = ApiBuilder::new(Router::new())
        .register(dex_storage)
        .register(hedge_storage)
        .build();
    let board_router = Router::new()
        .nest("/api/v1", board_api)
        .fallback_service(ServeUI::new())
        .layer(Extension(apalis_broadcaster().clone()));

    SupervisorBuilder::default()
        .with_task(
            "axum-server",
            ServerTask {
                router: main_router,
                port: ctx.server_port,
            },
        )
        .with_task(
            "apalis-board",
            ServerTask {
                router: board_router,
                port: ctx.board_port,
            },
        )
        .build()
        .run()
}

async fn await_shutdown(
    server_supervisor: SupervisorHandle,
    bot_task: JoinHandle<anyhow::Result<()>>,
) -> anyhow::Result<()> {
    let bot_abort = bot_task.abort_handle();

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!(
                target: "startup",
                "Received shutdown signal, shutting down gracefully..."
            );

            shutdown_supervisor(&server_supervisor);
            abort_task("bot", &bot_abort);

            Ok(())
        }
        result = server_supervisor.wait() => {
            abort_task("bot", &bot_abort);
            check_server_result(result)
        }
        result = bot_task => {
            shutdown_supervisor(&server_supervisor);
            check_bot_result(result)
        }
    }
}

fn shutdown_supervisor(handle: &SupervisorHandle) {
    info!(target: "startup", name = "server", "Shutting down supervisor");
    if let Err(error) = handle.shutdown() {
        error!(target: "startup", %error, "Failed to shutdown server supervisor");
    }
}

fn abort_task(name: &str, handle: &AbortHandle) {
    info!(target: "startup", name, "Aborting task");
    handle.abort();
}

fn check_server_result(result: Result<(), SupervisorError>) -> anyhow::Result<()> {
    match result {
        Ok(()) => {
            info!(target: "startup", "Server supervisor completed successfully");
            Ok(())
        }
        Err(error) => {
            error!(target: "startup", %error, "Server supervisor failed");
            Err(anyhow::anyhow!("Server supervisor failed: {error}"))
        }
    }
}

fn check_bot_result(result: Result<anyhow::Result<()>, JoinError>) -> anyhow::Result<()> {
    match result {
        Ok(Ok(())) => {
            info!(target: "startup", "Bot task completed");
            Ok(())
        }
        Ok(Err(error)) => {
            error!(target: "startup", %error, "Bot failed");
            Err(error)
        }
        Err(error) => {
            error!(target: "startup", %error, "Bot task panicked");
            Err(anyhow::anyhow!("Bot task panicked: {error}"))
        }
    }
}

/// Runs a single conductor session with the configured broker executor.
#[tracing::instrument(skip_all, target = "startup", level = tracing::Level::INFO)]
async fn run_conductor_session(
    ctx: Ctx,
    pool: SqlitePool,
    event_sender: broadcast::Sender<Statement>,
    inventory: Arc<inventory::BroadcastingInventory>,
) -> anyhow::Result<()> {
    let result = match ctx.broker.clone() {
        BrokerCtx::DryRun => {
            info!(target: "startup", "Initializing test executor for dry-run mode");
            Box::pin(conductor::Conductor::run(
                MockExecutorCtx,
                ctx,
                pool,
                event_sender,
                inventory,
            ))
            .await
        }
        BrokerCtx::AlpacaBrokerApi(alpaca_auth) => {
            info!(target: "startup", "Initializing Alpaca Broker API executor");
            Box::pin(conductor::Conductor::run(
                alpaca_auth,
                ctx,
                pool,
                event_sender,
                inventory,
            ))
            .await
        }
    };

    match result {
        Ok(()) => {
            info!(target: "startup", "Bot session completed successfully");
            Ok(())
        }
        Err(error) => {
            error!(target: "startup", %error, "Bot session failed");
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;

    use super::*;
    use st0x_config::create_test_ctx_with_order_owner;

    // `Ctx::load_files` parses `orderbook` into `Address`, so runtime tests
    // only exercise already-validated addresses. Invalid orderbook input is
    // covered in `config::tests::load_files_rejects_invalid_orderbook_address`.

    async fn create_test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        pool
    }

    fn create_test_event_sender() -> broadcast::Sender<Statement> {
        let (sender, _) = broadcast::channel(16);
        sender
    }

    fn create_test_inventory() -> Arc<inventory::BroadcastingInventory> {
        let sender = create_test_event_sender();
        Arc::new(inventory::BroadcastingInventory::new(
            inventory::InventoryView::default(),
            sender,
        ))
    }

    #[tokio::test]
    async fn test_run_function_websocket_connection_error() {
        let mut ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let pool = create_test_pool().await;
        ctx.evm.ws_rpc_url = "ws://invalid.nonexistent.url:8545".parse().unwrap();
        let error = Box::pin(run_conductor_session(
            ctx,
            pool,
            create_test_event_sender(),
            create_test_inventory(),
        ))
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to connect websocket provider"),
            "expected websocket provider connection failure, got: {error:#}"
        );
    }

    #[tokio::test]
    async fn test_run_function_error_propagation() {
        let mut ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        ctx.evm.ws_rpc_url = "ws://invalid.nonexistent.localhost:9999".parse().unwrap();
        let pool = create_test_pool().await;
        let error = Box::pin(run_conductor_session(
            ctx,
            pool,
            create_test_event_sender(),
            create_test_inventory(),
        ))
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to connect websocket provider"),
            "expected websocket provider connection failure, got: {error:#}"
        );
    }
}
