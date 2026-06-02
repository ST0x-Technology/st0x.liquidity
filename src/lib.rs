//! Market making system for tokenized equities.
//!
//! Provides onchain liquidity via Raindex orders and hedges
//! directional exposure by executing offsetting trades on
//! traditional brokerages.

use anyhow::Context;
use rocket::{Ignite, Rocket};
use sqlx::SqlitePool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::task::{AbortHandle, JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use st0x_dto::Statement;
use st0x_execution::MockExecutorCtx;

use crate::config::{BrokerCtx, Ctx};

/// How long to wait for in-flight work to drain before force-aborting.
/// Outer timeout for the entire graceful shutdown sequence. Must exceed
/// the rebalancer drain timeout (60s) plus margin for supervisor
/// shutdown and apalis drain.
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(90);

mod alpaca_wallet;
pub mod api;
#[cfg(any(test, feature = "test-support"))]
pub mod bindings;
#[cfg(not(any(test, feature = "test-support")))]
pub(crate) mod bindings;
pub mod cli;
mod conductor;
pub mod config;
pub(crate) mod dashboard;
mod equity_redemption;
mod inventory;
mod offchain;
mod onchain;
mod onchain_trade;
mod position;
mod position_check;
mod rebalancing;
mod shares;
mod symbol;
pub mod telemetry;
mod threshold;
mod tokenization;
mod trading;
#[cfg(feature = "mock")]
pub use tokenization::mock_api;
mod tokenized_equity_mint;
mod usdc_rebalance;
mod vault_registry;
#[cfg(any(test, feature = "test-support"))]
pub mod wallet;
#[cfg(not(any(test, feature = "test-support")))]
pub(crate) mod wallet;
mod wrapped_equity_recovery;
mod wrapper;

pub use telemetry::{FileLogGuard, TelemetryError, TelemetryGuard, mk_env_filter, setup_tracing};

#[cfg(feature = "test-support")]
pub use conductor::job::{FailureInjector, JobKind};
#[cfg(any(test, feature = "test-support"))]
pub use config::TradingMode;
#[cfg(any(test, feature = "test-support"))]
pub use config::{AssetsConfig, CashAssetConfig, EquitiesConfig, EquityAssetConfig, OperationMode};
#[cfg(any(test, feature = "test-support"))]
pub use inventory::ImbalanceThreshold;
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
pub use rebalancing::{RebalancingCtx, RebalancingCtxError, UsdcRebalancing};
#[cfg(any(test, feature = "test-support"))]
pub use threshold::ExecutionThreshold;

#[cfg(test)]
mod integration_tests;
#[cfg(test)]
pub mod test_utils;

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

    let inventory = Arc::new(inventory::BroadcastingInventory::new(
        inventory::InventoryView::default(),
        event_sender.clone(),
    ));

    let shutdown_token = CancellationToken::new();
    let recovery_cell = Arc::new(tokio::sync::OnceCell::new());

    let server_task = spawn_server_task(
        &ctx,
        &pool,
        event_sender.clone(),
        inventory.clone(),
        recovery_cell.clone(),
    );
    let bot_task = tokio::spawn(Box::pin(run_conductor_session(
        ctx,
        pool,
        event_sender,
        inventory,
        shutdown_token.clone(),
        recovery_cell,
    )));

    await_shutdown(server_task, bot_task, shutdown_token).await?;

    info!(target: "startup", "Shutdown complete");
    Ok(())
}

fn spawn_server_task(
    ctx: &Ctx,
    pool: &SqlitePool,
    event_sender: broadcast::Sender<Statement>,
    inventory: Arc<inventory::BroadcastingInventory>,
    recovery_cell: Arc<tokio::sync::OnceCell<api::RecoveryHandle>>,
) -> JoinHandle<Result<Rocket<Ignite>, rocket::Error>> {
    let rocket_config = rocket::Config::figment()
        .merge(("port", ctx.server_port))
        .merge(("address", "0.0.0.0"));

    let rocket = rocket::custom(rocket_config)
        .mount("/", api::routes())
        .mount("/api", dashboard::routes())
        .manage(pool.clone())
        .manage(ctx.clone())
        .manage(dashboard::Broadcast {
            sender: event_sender,
        })
        .manage(dashboard::DashboardState {
            inventory,
            pool: pool.clone(),
            settings: dashboard::settings_from_ctx(ctx),
        })
        .manage(recovery_cell)
        .manage(api::ResumeLock(tokio::sync::Mutex::new(())));

    tokio::spawn(rocket.launch())
}

async fn await_shutdown(
    mut server_task: JoinHandle<Result<Rocket<Ignite>, rocket::Error>>,
    mut bot_task: JoinHandle<anyhow::Result<()>>,
    shutdown_token: CancellationToken,
) -> anyhow::Result<()> {
    let server_abort = server_task.abort_handle();
    let bot_abort = bot_task.abort_handle();

    let trigger = await_shutdown_trigger(&mut server_task, &mut bot_task).await?;

    match trigger {
        ShutdownTrigger::Signal(early_exit) => {
            info!(target: "shutdown", "Initiating graceful shutdown");
            shutdown_token.cancel();
            abort_task("server", &server_abort);
            drain_bot_with_timeout(bot_task, bot_abort, early_exit, GRACEFUL_SHUTDOWN_TIMEOUT).await
        }
        ShutdownTrigger::BotExited(result) => {
            abort_task("server", &server_abort);
            check_bot_result(result)
        }
    }
}

enum ShutdownTrigger {
    /// An OS signal (or server exit) triggered shutdown; includes any
    /// server error to propagate after the bot drains.
    Signal(Option<anyhow::Result<()>>),
    /// The bot task exited on its own before any signal.
    BotExited(Result<anyhow::Result<()>, JoinError>),
}

/// Waits for the first shutdown trigger: SIGINT, SIGTERM, or task exit.
async fn await_shutdown_trigger(
    server_task: &mut JoinHandle<Result<Rocket<Ignite>, rocket::Error>>,
    bot_task: &mut JoinHandle<anyhow::Result<()>>,
) -> anyhow::Result<ShutdownTrigger> {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to register SIGTERM handler")?;

    let trigger = tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!(target: "shutdown", "Received SIGINT");
            ShutdownTrigger::Signal(None)
        }
        _ = sigterm.recv() => {
            info!(target: "shutdown", "Received SIGTERM");
            ShutdownTrigger::Signal(None)
        }
        result = server_task => {
            ShutdownTrigger::Signal(Some(check_server_result(result)))
        }
        result = bot_task => {
            ShutdownTrigger::BotExited(result)
        }
    };

    Ok(trigger)
}

async fn drain_bot_with_timeout(
    bot_task: JoinHandle<anyhow::Result<()>>,
    bot_abort: AbortHandle,
    early_exit: Option<anyhow::Result<()>>,
    timeout: Duration,
) -> anyhow::Result<()> {
    info!(
        target: "shutdown",
        "Waiting up to {}s for in-flight work to drain (send signal again to force quit)",
        timeout.as_secs()
    );

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to register SIGTERM handler for drain")?;

    let drain_result = tokio::select! {
        result = tokio::time::timeout(timeout, bot_task) => result,
        _ = tokio::signal::ctrl_c() => {
            warn!(target: "shutdown", "Received second signal, force-aborting immediately");
            abort_task("bot", &bot_abort);
            if let Some(server_result) = early_exit {
                return server_result;
            }
            return Ok(());
        }
        _ = sigterm.recv() => {
            warn!(target: "shutdown", "Received second SIGTERM, force-aborting immediately");
            abort_task("bot", &bot_abort);
            if let Some(server_result) = early_exit {
                return server_result;
            }
            return Ok(());
        }
    };

    match drain_result {
        Ok(result) => {
            info!(target: "shutdown", "Bot drained gracefully");
            let bot_outcome = check_bot_result(result);
            if let Some(server_result) = early_exit {
                server_result?;
            }
            bot_outcome
        }
        Err(_elapsed) => {
            warn!(
                target: "shutdown",
                "Drain timeout expired after {}s, force-aborting bot",
                timeout.as_secs()
            );
            abort_task("bot", &bot_abort);
            if let Some(server_result) = early_exit {
                server_result?;
            }
            Ok(())
        }
    }
}

fn abort_task(name: &str, handle: &AbortHandle) {
    info!(target: "shutdown", name, "Aborting task");
    handle.abort();
}

fn check_server_result(
    result: Result<Result<Rocket<Ignite>, rocket::Error>, JoinError>,
) -> anyhow::Result<()> {
    match result {
        Ok(Ok(_)) => {
            info!(target: "startup", "Server completed successfully");
            Ok(())
        }
        Ok(Err(error)) => {
            error!(target: "startup", %error, "Server failed");
            Err(anyhow::anyhow!("Server failed: {error}"))
        }
        Err(error) => {
            error!(target: "startup", %error, "Server task panicked");
            Err(anyhow::anyhow!("Server task panicked: {error}"))
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
    shutdown_token: CancellationToken,
    recovery_cell: Arc<tokio::sync::OnceCell<api::RecoveryHandle>>,
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
                shutdown_token,
                recovery_cell,
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
                shutdown_token,
                recovery_cell,
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
    use crate::config::tests::create_test_ctx_with_order_owner;

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
            CancellationToken::new(),
            Arc::new(tokio::sync::OnceCell::new()),
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
    async fn drain_bot_with_timeout_succeeds_when_bot_drains() {
        let bot_task = tokio::spawn(async { Ok(()) });
        let bot_abort = bot_task.abort_handle();
        drain_bot_with_timeout(bot_task, bot_abort, None, GRACEFUL_SHUTDOWN_TIMEOUT)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn drain_bot_with_timeout_propagates_bot_error() {
        let bot_task = tokio::spawn(async { Err(anyhow::anyhow!("bot failed")) });
        let bot_abort = bot_task.abort_handle();
        let result =
            drain_bot_with_timeout(bot_task, bot_abort, None, GRACEFUL_SHUTDOWN_TIMEOUT).await;
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("bot failed"),
            "should propagate bot error, got: {error}"
        );
    }

    #[tokio::test]
    async fn drain_bot_with_timeout_propagates_server_error_on_success() {
        let bot_task = tokio::spawn(async { Ok(()) });
        let bot_abort = bot_task.abort_handle();
        let early_exit = Some(Err(anyhow::anyhow!("server crashed")));
        let result =
            drain_bot_with_timeout(bot_task, bot_abort, early_exit, GRACEFUL_SHUTDOWN_TIMEOUT)
                .await;
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("server crashed"),
            "should propagate server error, got: {error}"
        );
    }

    #[tokio::test]
    async fn drain_bot_with_timeout_force_aborts_on_timeout() {
        // Bot that never completes, forcing the timeout path
        let bot_task = tokio::spawn(async { std::future::pending::<anyhow::Result<()>>().await });
        let bot_abort = bot_task.abort_handle();

        let result =
            drain_bot_with_timeout(bot_task, bot_abort, None, Duration::from_millis(10)).await;

        result.unwrap();
    }

    #[tokio::test]
    async fn drain_bot_with_timeout_force_abort_propagates_server_error() {
        let bot_task = tokio::spawn(async { std::future::pending::<anyhow::Result<()>>().await });
        let bot_abort = bot_task.abort_handle();
        let early_exit = Some(Err(anyhow::anyhow!("server crashed")));

        let result =
            drain_bot_with_timeout(bot_task, bot_abort, early_exit, Duration::from_millis(10))
                .await;

        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("server crashed"),
            "should propagate server error on force-abort, got: {error}"
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
            CancellationToken::new(),
            Arc::new(tokio::sync::OnceCell::new()),
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
