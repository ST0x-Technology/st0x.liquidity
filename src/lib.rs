//! Market making system for tokenized equities.
//!
//! Provides onchain liquidity via Raindex orders and hedges
//! directional exposure by executing offsetting trades on
//! traditional brokerages.

use anyhow::Context;
use apalis_board::axum::framework::{ApiBuilder, RegisterRoute};
use apalis_board::axum::sse::TracingBroadcaster;
use apalis_board::axum::ui::ServeUI;
use axum::{Extension, Router};
use sqlx::SqlitePool;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use task_supervisor::{
    SupervisedTask, SupervisorBuilder, SupervisorError, SupervisorHandle, TaskResult,
};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::{AbortHandle, JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::Layer;

use st0x_config::{BrokerCtx, Ctx};
use st0x_dto::Statement;
use st0x_execution::MockExecutorCtx;

use crate::trading::offchain::hedge::HedgeJobQueue;
use crate::trading::onchain::trade_accountant::DexTradeAccountingJobQueue;
use conductor::DatabasePools;

/// Outer timeout for the entire graceful shutdown sequence. Must exceed
/// the rebalancer drain timeout (60s) plus margin for supervisor shutdown
/// and apalis drain.
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(90);

mod alerts;
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
mod offchain;
mod onchain;
mod onchain_trade;
mod performance;
mod position;
mod position_check;
mod rebalancing;
mod symbol;
mod tokenization;
mod trading;
#[cfg(feature = "mock")]
pub use tokenization::mock_api;
mod tokenized_equity_mint;
mod unwrapped_equity_recovery;
mod usdc_rebalance;
mod vault_lookup;
mod vault_registry;
mod wrapped_equity_recovery;

pub use st0x_config::{
    ExtraLayer, FileLogGuard, TelemetryError, TelemetryGuard, mk_env_filter, setup_tracing,
};

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
pub use conductor::job::{FailureInjector, JobKind};
#[cfg(any(test, feature = "test-support"))]
pub use st0x_config::ExecutionThreshold;
#[cfg(any(test, feature = "test-support"))]
pub use st0x_config::TradingMode;
#[cfg(any(test, feature = "test-support"))]
pub use st0x_config::{
    AssetsConfig, CashAssetConfig, EquitiesConfig, EquityAssetConfig, OperationMode,
};
#[cfg(any(test, feature = "test-support"))]
pub use st0x_config::{ImbalanceThreshold, RebalancingCtx, RebalancingCtxError, UsdcRebalancing};
#[cfg(feature = "test-support")]
pub use trading::onchain::trade_accountant::AccountForDexTrade;

#[cfg(test)]
mod integration_tests;
#[cfg(test)]
pub mod test_utils;

/// Returns the global apalis-board log broadcaster, creating it on first access.
///
/// The same instance must be wired into the tracing subscriber (so log
/// events are pushed into it) and into the board router as an `Extension`
/// (so the `/api/v1/events` SSE handler can read them out). The wiring is
/// done via [`apalis_board_tracing_layer`] passed to `setup_tracing` /
/// `TelemetryConfig::setup`.
pub fn apalis_broadcaster() -> &'static Arc<Mutex<TracingBroadcaster>> {
    static BROADCASTER: OnceLock<Arc<Mutex<TracingBroadcaster>>> = OnceLock::new();
    BROADCASTER.get_or_init(TracingBroadcaster::create)
}

/// Builds the tracing layer that forwards events into the apalis-board
/// broadcaster's SSE feed. Filter level is the same as the global env filter.
///
/// Returned as an `ExtraLayer` so it can be plugged into
/// `setup_tracing` / `TelemetryConfig::setup` without coupling
/// `st0x-config` to `apalis-board`.
pub fn apalis_board_tracing_layer(level: tracing::Level) -> ExtraLayer {
    Box::new(
        apalis_board::axum::sse::TracingSubscriber::new(apalis_broadcaster())
            .layer()
            .with_filter(mk_env_filter(level)),
    )
}

/// Shared application state passed to all axum handlers.
#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) ctx: Ctx,
    pub(crate) pool: SqlitePool,
    pub(crate) event_sender: broadcast::Sender<Statement>,
    pub(crate) inventory: Arc<inventory::BroadcastingInventory>,
    pub(crate) settings: st0x_dto::Settings,
    pub(crate) recovery: Arc<tokio::sync::OnceCell<api::RecoveryHandle>>,
    pub(crate) resume_lock: Arc<api::ResumeLock>,
    pub(crate) performance: Arc<performance::cache::PerformanceCache>,
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
    run_bot_session_inner(
        ctx,
        event_sender,
        #[cfg(any(test, feature = "test-support"))]
        FailureInjector::new(),
    )
    .await
}

/// Like [`run_bot_session_with_event_channel`] but accepts a caller-supplied
/// [`FailureInjector`] so e2e tests can arm specific job types.
#[cfg(any(test, feature = "test-support"))]
#[tracing::instrument(skip_all, target = "startup", level = tracing::Level::INFO)]
pub async fn run_bot_session_with_injector(
    ctx: Ctx,
    event_sender: broadcast::Sender<Statement>,
    failure_injector: FailureInjector,
) -> anyhow::Result<()> {
    run_bot_session_inner(ctx, event_sender, failure_injector).await
}

async fn run_bot_session_inner(
    ctx: Ctx,
    event_sender: broadcast::Sender<Statement>,
    #[cfg(any(test, feature = "test-support"))] failure_injector: FailureInjector,
) -> anyhow::Result<()> {
    let pool = ctx.get_sqlite_pool().await?;
    let apalis_pool = conductor::connect_apalis_pool(&ctx.database_url).await?;
    sqlx::migrate!().set_ignore_missing(true).run(&pool).await?;
    conductor::setup_apalis_tables(&apalis_pool).await?;

    let inventory = Arc::new(inventory::BroadcastingInventory::new(
        inventory::InventoryView::default(),
        event_sender.clone(),
    ));

    let shutdown_token = CancellationToken::new();
    let recovery_cell = Arc::new(tokio::sync::OnceCell::new());
    let resume_lock = Arc::new(api::ResumeLock(tokio::sync::Mutex::new(())));

    // Pre-bind both server ports synchronously so a port-in-use failure
    // surfaces at startup instead of looping in the supervisor.
    let main_listener = TcpListener::bind(("0.0.0.0", ctx.server_port))
        .await
        .with_context(|| format!("failed to bind axum server on port {}", ctx.server_port))?;
    let board_listener = TcpListener::bind(("0.0.0.0", ctx.board_port))
        .await
        .with_context(|| format!("failed to bind apalis-board on port {}", ctx.board_port))?;

    let pools = DatabasePools {
        cqrs: pool,
        apalis: apalis_pool,
    };

    let server_supervisor = spawn_server_supervisor(
        &ctx,
        &pools,
        event_sender.clone(),
        inventory.clone(),
        recovery_cell.clone(),
        resume_lock,
        main_listener,
        board_listener,
    );
    let bot_task = tokio::spawn(Box::pin(run_conductor_session(
        ctx,
        pools,
        event_sender,
        inventory,
        shutdown_token.clone(),
        recovery_cell,
        #[cfg(any(test, feature = "test-support"))]
        failure_injector,
    )));

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to register SIGTERM handler")?;
    let shutdown_signal = async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => info!(target: "shutdown", "Received SIGINT"),
            _ = sigterm.recv() => info!(target: "shutdown", "Received SIGTERM"),
        }
    };

    await_shutdown(
        server_supervisor,
        bot_task,
        shutdown_token,
        shutdown_signal,
        GRACEFUL_SHUTDOWN_TIMEOUT,
    )
    .await?;

    info!(target: "startup", "Shutdown complete");
    Ok(())
}

/// Long-running axum HTTP server, supervised so a bind/serve failure
/// doesn't silently take the API and dashboard down for the rest of
/// the bot's lifetime.
///
/// The listener is pre-bound by the caller and handed in via
/// `Arc<Mutex<Option<TcpListener>>>` so the supervisor can't loop-restart
/// on bind failures: if `axum::serve` exits and the supervisor restarts
/// `run()`, the slot is empty and the task fails permanently, triggering
/// the supervisor's dead-task threshold instead of hot-spinning on bind.
#[derive(Clone)]
struct ServerTask {
    router: Router,
    listener: Arc<Mutex<Option<TcpListener>>>,
}

impl SupervisedTask for ServerTask {
    async fn run(&mut self) -> TaskResult {
        let listener = self
            .listener
            .lock()
            .map_err(|_| "listener mutex poisoned".to_string())?
            .take()
            .ok_or(
                "listener already consumed; supervisor cannot restart axum \
                 server without a fresh bind",
            )?;
        let port = listener.local_addr().map(|addr| addr.port()).ok();
        info!(target: "startup", ?port, "Server listening");
        axum::serve(listener, self.router.clone()).await?;
        Err("axum server exited unexpectedly".into())
    }
}

fn spawn_server_supervisor(
    ctx: &Ctx,
    pools: &DatabasePools,
    event_sender: broadcast::Sender<Statement>,
    inventory: Arc<inventory::BroadcastingInventory>,
    recovery: Arc<tokio::sync::OnceCell<api::RecoveryHandle>>,
    resume_lock: Arc<api::ResumeLock>,
    main_listener: TcpListener,
    board_listener: TcpListener,
) -> SupervisorHandle {
    let state = AppState {
        ctx: ctx.clone(),
        pool: pools.cqrs.clone(),
        event_sender,
        inventory,
        settings: dashboard::settings_from_ctx(ctx),
        recovery,
        resume_lock,
        performance: Arc::new(performance::cache::PerformanceCache::default()),
    };

    let main_router = Router::new()
        .merge(api::routes())
        .nest("/api", dashboard::routes())
        .with_state(state);

    // apalis-board's embedded UI is built with hardcoded absolute paths
    // (`/apalis-board-web-*`) and the wasm calls `/api/v1/*` directly,
    // so it must own its origin. Run it on its own port instead of
    // nesting under the main router.
    let dex_storage = DexTradeAccountingJobQueue::new(&pools.apalis).into_storage();
    let hedge_storage = HedgeJobQueue::new(&pools.apalis).into_storage();
    let board_api = ApiBuilder::new(Router::new())
        .register(dex_storage)
        .register(hedge_storage)
        .build();
    let board_router = Router::new()
        .nest("/api/v1", board_api)
        .fallback_service(ServeUI::new())
        .layer(Extension(apalis_broadcaster().clone()));

    SupervisorBuilder::default()
        // Any dead task means a server is permanently down (max restarts
        // exhausted). Both servers are essential -- escalate immediately so
        // the bot exits gracefully instead of trading without an API.
        .with_dead_tasks_threshold(Some(0.0))
        .with_task(
            "axum-server",
            ServerTask {
                router: main_router,
                listener: Arc::new(Mutex::new(Some(main_listener))),
            },
        )
        .with_task(
            "apalis-board",
            ServerTask {
                router: board_router,
                listener: Arc::new(Mutex::new(Some(board_listener))),
            },
        )
        .build()
        .run()
}

async fn await_shutdown<S>(
    server_supervisor: SupervisorHandle,
    mut bot_task: JoinHandle<anyhow::Result<()>>,
    shutdown_token: CancellationToken,
    shutdown_signal: S,
    drain_timeout: Duration,
) -> anyhow::Result<()>
where
    S: Future<Output = ()>,
{
    let bot_abort = bot_task.abort_handle();
    tokio::pin!(shutdown_signal);

    let trigger: ShutdownTrigger = tokio::select! {
        () = shutdown_signal.as_mut() => ShutdownTrigger::Signal,
        result = server_supervisor.wait() => ShutdownTrigger::ServerExit(result),
        result = &mut bot_task => ShutdownTrigger::BotExit(result),
    };

    match trigger {
        ShutdownTrigger::Signal => {
            info!(target: "shutdown", "Shutdown signal received, draining gracefully");
            shutdown_token.cancel();
            shutdown_supervisor(&server_supervisor);
            drain_bot_with_timeout(bot_task, bot_abort, drain_timeout).await
        }
        ShutdownTrigger::ServerExit(result) => {
            info!(target: "shutdown", "Server supervisor exited, draining bot");
            shutdown_token.cancel();
            drain_bot_with_timeout(bot_task, bot_abort, drain_timeout).await?;
            check_server_result(result)
        }
        ShutdownTrigger::BotExit(result) => {
            shutdown_supervisor(&server_supervisor);
            check_bot_result(result)
        }
    }
}

enum ShutdownTrigger {
    Signal,
    ServerExit(Result<(), SupervisorError>),
    BotExit(Result<anyhow::Result<()>, JoinError>),
}

/// Cancels the bot via timeout if it doesn't drain in time.
///
/// Caller must have already triggered the drain by either cancelling
/// `shutdown_token` (signal/server-exit branches) or by waiting for the
/// bot to exit naturally; this function only enforces the timeout.
async fn drain_bot_with_timeout(
    bot_task: JoinHandle<anyhow::Result<()>>,
    bot_abort: AbortHandle,
    timeout: Duration,
) -> anyhow::Result<()> {
    info!(
        target: "shutdown",
        "Waiting up to {}s for bot to drain",
        timeout.as_secs()
    );

    tokio::time::timeout(timeout, bot_task).await.map_or_else(
        |_elapsed| {
            warn!(target: "shutdown", "Bot drain timed out, force-aborting");
            bot_abort.abort();
            Ok(())
        },
        check_bot_result,
    )
}

fn shutdown_supervisor(handle: &SupervisorHandle) {
    info!(target: "startup", name = "server", "Shutting down supervisor");
    if let Err(error) = handle.shutdown() {
        error!(target: "startup", %error, "Failed to shutdown server supervisor");
    }
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
    pools: DatabasePools,
    event_sender: broadcast::Sender<Statement>,
    inventory: Arc<inventory::BroadcastingInventory>,
    shutdown_token: tokio_util::sync::CancellationToken,
    recovery_cell: Arc<tokio::sync::OnceCell<api::RecoveryHandle>>,
    #[cfg(any(test, feature = "test-support"))] failure_injector: FailureInjector,
) -> anyhow::Result<()> {
    let result = match ctx.broker.clone() {
        BrokerCtx::DryRun => {
            info!(target: "startup", "Initializing test executor for dry-run mode");
            let pools = pools.clone();
            Box::pin(conductor::Conductor::run(
                MockExecutorCtx,
                ctx,
                pools,
                event_sender,
                inventory,
                shutdown_token,
                recovery_cell,
                #[cfg(any(test, feature = "test-support"))]
                failure_injector,
            ))
            .await
        }
        BrokerCtx::AlpacaBrokerApi(alpaca_auth) => {
            info!(target: "startup", "Initializing Alpaca Broker API executor");
            Box::pin(conductor::Conductor::run(
                alpaca_auth,
                ctx,
                pools,
                event_sender,
                inventory,
                shutdown_token,
                recovery_cell,
                #[cfg(any(test, feature = "test-support"))]
                failure_injector,
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
    use std::sync::atomic::{AtomicBool, Ordering};

    use st0x_config::create_test_ctx_with_order_owner;

    use super::*;

    // `Ctx::load_files` parses `orderbook` into `Address`, so runtime tests
    // only exercise already-validated addresses. Invalid orderbook input is
    // covered in `config::tests::load_files_rejects_invalid_orderbook_address`.

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
    async fn signal_cancels_token_and_drains_bot() {
        let shutdown_token = CancellationToken::new();
        let token_clone = shutdown_token.clone();
        let drained = Arc::new(AtomicBool::new(false));
        let drained_clone = Arc::clone(&drained);

        let bot_task = tokio::spawn(async move {
            token_clone.cancelled().await;
            drained_clone.store(true, Ordering::SeqCst);
            Ok(())
        });

        let supervisor = SupervisorBuilder::default().build().run();
        let supervisor_handle_for_assert = supervisor.clone();

        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel::<()>();
        let signal_fut = async move {
            let _ = signal_rx.await;
        };

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let _ = signal_tx.send(());
        });

        await_shutdown(
            supervisor,
            bot_task,
            shutdown_token,
            signal_fut,
            Duration::from_secs(5),
        )
        .await
        .unwrap();

        assert!(
            drained.load(Ordering::SeqCst),
            "bot drain did not run after signal-triggered shutdown"
        );

        let post_wait =
            tokio::time::timeout(Duration::from_secs(1), supervisor_handle_for_assert.wait()).await;
        assert!(
            post_wait.is_ok(),
            "supervisor.wait() should return after signal-triggered shutdown"
        );
    }

    #[tokio::test]
    async fn bot_exit_shuts_down_supervisor() {
        let shutdown_token = CancellationToken::new();
        let bot_task = tokio::spawn(async { Ok(()) });
        let supervisor = SupervisorBuilder::default().build().run();
        let supervisor_handle_for_assert = supervisor.clone();

        let signal_fut = std::future::pending::<()>();

        await_shutdown(
            supervisor,
            bot_task,
            shutdown_token,
            signal_fut,
            Duration::from_secs(5),
        )
        .await
        .unwrap();

        let post_wait =
            tokio::time::timeout(Duration::from_secs(1), supervisor_handle_for_assert.wait()).await;
        assert!(
            post_wait.is_ok(),
            "supervisor.wait() should return after await_shutdown shut it down"
        );
    }

    #[tokio::test]
    async fn server_exit_cancels_token_and_drains_bot() {
        let shutdown_token = CancellationToken::new();
        let token_clone = shutdown_token.clone();
        let drained = Arc::new(AtomicBool::new(false));
        let drained_clone = Arc::clone(&drained);

        let bot_task = tokio::spawn(async move {
            token_clone.cancelled().await;
            drained_clone.store(true, Ordering::SeqCst);
            Ok(())
        });

        let supervisor = SupervisorBuilder::default().build().run();
        let supervisor_for_external_shutdown = supervisor.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let _ = supervisor_for_external_shutdown.shutdown();
        });

        let signal_fut = std::future::pending::<()>();

        await_shutdown(
            supervisor,
            bot_task,
            shutdown_token,
            signal_fut,
            Duration::from_secs(5),
        )
        .await
        .unwrap();

        assert!(
            drained.load(Ordering::SeqCst),
            "bot drain did not run after server-exit-triggered shutdown"
        );
    }

    #[tokio::test]
    async fn drain_timeout_force_aborts_unresponsive_bot() {
        let shutdown_token = CancellationToken::new();

        let bot_task = tokio::spawn(async { std::future::pending::<anyhow::Result<()>>().await });

        let supervisor = SupervisorBuilder::default().build().run();

        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel::<()>();
        let signal_fut = async move {
            let _ = signal_rx.await;
        };

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let _ = signal_tx.send(());
        });

        await_shutdown(
            supervisor,
            bot_task,
            shutdown_token,
            signal_fut,
            Duration::from_millis(50),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_run_function_unreachable_rpc_fails_startup() {
        let mut ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        // Port 1 refuses connections immediately, so the startup RPC probe
        // fails fast rather than hanging on a retry loop.
        ctx.evm.rpc_url = "http://127.0.0.1:1".parse().unwrap();
        let error = Box::pin(run_conductor_session(
            ctx,
            DatabasePools {
                cqrs: pool,
                apalis: apalis_pool,
            },
            create_test_event_sender(),
            create_test_inventory(),
            tokio_util::sync::CancellationToken::new(),
            Arc::new(tokio::sync::OnceCell::new()),
            FailureInjector::new(),
        ))
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to reach RPC endpoint at startup"),
            "expected startup RPC probe failure, got: {error:#}"
        );
    }

    #[tokio::test]
    async fn test_run_function_error_propagation() {
        let mut ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        ctx.evm.rpc_url = "http://127.0.0.1:1".parse().unwrap();
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let error = Box::pin(run_conductor_session(
            ctx,
            DatabasePools {
                cqrs: pool,
                apalis: apalis_pool,
            },
            create_test_event_sender(),
            create_test_inventory(),
            tokio_util::sync::CancellationToken::new(),
            Arc::new(tokio::sync::OnceCell::new()),
            FailureInjector::new(),
        ))
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to reach RPC endpoint at startup"),
            "expected startup RPC probe failure, got: {error:#}"
        );
    }

    #[test]
    fn check_server_result_returns_ok_for_clean_exit() {
        check_server_result(Ok(())).unwrap();
    }

    #[test]
    fn check_server_result_wraps_supervisor_error() {
        let supervisor_error = SupervisorError::TooManyDeadTasks {
            current_percentage: 100.0,
            threshold: 0.0,
        };
        let error = check_server_result(Err(supervisor_error)).unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains("Server supervisor failed"),
            "expected wrapped supervisor message, got: {message}"
        );
        assert!(
            message.contains("Too many tasks are dead"),
            "expected underlying supervisor error preserved, got: {message}"
        );
    }

    #[tokio::test]
    async fn check_bot_result_returns_ok_for_clean_exit() {
        let join_result = tokio::spawn(async { Ok::<(), anyhow::Error>(()) }).await;
        check_bot_result(join_result).unwrap();
    }

    #[tokio::test]
    async fn check_bot_result_propagates_bot_error() {
        let join_result =
            tokio::spawn(async { Err::<(), anyhow::Error>(anyhow::anyhow!("bot blew up")) }).await;
        let error = check_bot_result(join_result).unwrap_err();
        assert_eq!(error.to_string(), "bot blew up");
    }

    #[tokio::test]
    async fn check_bot_result_reports_panicked_task() {
        let handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async { panic!("explosion") });
        let error = check_bot_result(handle.await).unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains("Bot task panicked"),
            "expected panic to be reported, got: {message}"
        );
    }
}
