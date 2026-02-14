//! Market making system for tokenized equities.
//!
//! Provides onchain liquidity via Raindex orders and hedges
//! directional exposure by executing offsetting trades on
//! traditional brokerages.

use apalis_sql::sqlite::SqliteStorage;
use rocket::{Ignite, Rocket};
use sqlx::SqlitePool;
use tokio::sync::broadcast;
use tokio::task::{AbortHandle, JoinError, JoinHandle};
use tracing::{error, info, info_span};

use st0x_dto::ServerMessage;
mod alpaca_tokenization;
mod alpaca_wallet;
pub mod api;
mod bindings;
pub mod cli;
mod conductor;
pub mod config;
pub(crate) mod dashboard;
mod dual_write;
mod equity_redemption;
mod error_decoding;
mod inventory;
mod lifecycle;
mod lock;
mod offchain;
mod offchain_order;
mod onchain;
mod onchain_trade;
mod position;
mod queue;
mod rebalancing;
pub mod reporter;
mod shares;
mod symbol;
mod telemetry;
mod threshold;
mod tokenized_equity_mint;
mod trade_execution_link;
mod usdc_rebalance;
mod vault_registry;

pub use telemetry::{TelemetryError, TelemetryGuard, setup_tracing};

#[cfg(test)]
pub mod test_utils;

use st0x_execution::{Executor, MockExecutorCtx, TryIntoExecutor};

use crate::config::{BrokerCtx, Ctx};

pub async fn launch(ctx: Ctx) -> anyhow::Result<()> {
    let launch_span = info_span!("launch");
    let _enter = launch_span.enter();

    let pool = ctx.get_sqlite_pool().await?;

    // Both apalis and our code use sqlx migrations, which share a single
    // `_sqlx_migrations` table. Each migrator validates that all previously
    // applied migrations exist in its own migration set -- so running either
    // migrator after the other will fail with `VersionMissing` unless both
    // use `set_ignore_missing(true)`.
    //
    // We cannot call `SqliteStorage::setup()` because it runs apalis
    // migrations without `ignore_missing`. Instead, we get the apalis
    // migrator directly and run both with `ignore_missing`.
    sqlx::query("PRAGMA journal_mode = 'WAL'")
        .execute(&pool)
        .await?;

    SqliteStorage::<()>::migrations()
        .set_ignore_missing(true)
        .run(&pool)
        .await?;

    sqlx::migrate!().set_ignore_missing(true).run(&pool).await?;

    let (event_sender, _) = broadcast::channel::<ServerMessage>(256);

    let server_task = spawn_server_task(&ctx, &pool, event_sender.clone());
    let bot_task = spawn_bot_task(ctx, pool, event_sender);

    await_shutdown(server_task, bot_task).await;

    info!("Shutdown complete");
    Ok(())
}

fn spawn_server_task(
    ctx: &Ctx,
    pool: &SqlitePool,
    event_sender: broadcast::Sender<ServerMessage>,
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
        });

    tokio::spawn(rocket.launch())
}

fn spawn_bot_task(
    ctx: Ctx,
    pool: SqlitePool,
    event_sender: broadcast::Sender<ServerMessage>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let bot_span = info_span!("bot_task");
        let _enter = bot_span.enter();

        if let Err(e) = Box::pin(run(ctx, pool, event_sender)).await {
            error!("Bot failed: {e}");
        }
    })
}

async fn await_shutdown(
    server_task: JoinHandle<Result<Rocket<Ignite>, rocket::Error>>,
    bot_task: JoinHandle<()>,
) {
    let server_abort = server_task.abort_handle();
    let bot_abort = bot_task.abort_handle();

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            handle_ctrl_c(&server_abort, &bot_abort);
        }
        result = server_task => {
            log_server_result(result);
            abort_task("bot", &bot_abort);
        }
        result = bot_task => {
            log_bot_result(result);
            abort_task("server", &server_abort);
        }
    }
}

fn handle_ctrl_c(server_abort: &AbortHandle, bot_abort: &AbortHandle) {
    info!("Received shutdown signal, shutting down gracefully...");
    abort_task("server", server_abort);
    abort_task("bot", bot_abort);
}

fn abort_task(name: &str, handle: &AbortHandle) {
    info!("Aborting {name} task");
    handle.abort();
}

fn log_server_result(result: Result<Result<Rocket<Ignite>, rocket::Error>, JoinError>) {
    match result {
        Ok(Ok(_)) => info!("Server completed successfully"),
        Ok(Err(e)) => error!("Server failed: {e}"),
        Err(e) => error!("Server task panicked: {e}"),
    }
}

fn log_bot_result(result: Result<(), JoinError>) {
    match result {
        Ok(()) => info!("Bot task completed"),
        Err(e) => error!("Bot task panicked: {e}"),
    }
}

#[tracing::instrument(skip_all, level = tracing::Level::INFO)]
async fn run(
    ctx: Ctx,
    pool: SqlitePool,
    event_sender: broadcast::Sender<ServerMessage>,
) -> anyhow::Result<()> {
    match &ctx.broker {
        BrokerCtx::DryRun => {
            info!("Initializing test executor for dry-run mode");
            let executor = MockExecutorCtx.try_into_executor().await?;

            Box::pin(run_with_executor(ctx, pool, executor, event_sender)).await
        }
        BrokerCtx::Schwab(schwab_auth) => {
            info!("Initializing Schwab executor");
            let schwab_ctx = schwab_auth.to_schwab_ctx(pool.clone());
            let executor = schwab_ctx.try_into_executor().await?;

            Box::pin(run_with_executor(ctx, pool, executor, event_sender)).await
        }
        BrokerCtx::AlpacaTradingApi(alpaca_auth) => {
            info!("Initializing Alpaca Trading API executor");
            let executor = alpaca_auth.clone().try_into_executor().await?;

            Box::pin(run_with_executor(ctx, pool, executor, event_sender)).await
        }
        BrokerCtx::AlpacaBrokerApi(alpaca_auth) => {
            info!("Initializing Alpaca Broker API executor");
            let executor = alpaca_auth.clone().try_into_executor().await?;

            Box::pin(run_with_executor(ctx, pool, executor, event_sender)).await
        }
    }
}

async fn run_with_executor<E>(
    ctx: Ctx,
    pool: SqlitePool,
    executor: E,
    event_sender: broadcast::Sender<ServerMessage>,
) -> anyhow::Result<()>
where
    E: Executor + Clone + Send + Sync + Unpin + 'static,
    conductor::EventProcessingError: From<E::Error>,
{
    let executor_maintenance = executor.run_executor_maintenance().await;

    let handle =
        conductor::start(&ctx, &pool, executor, executor_maintenance, event_sender).await?;

    handle.wait().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address};

    use super::*;
    use crate::config::tests::create_test_ctx_with_order_owner;

    async fn create_test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        SqliteStorage::<()>::migrations()
            .set_ignore_missing(true)
            .run(&pool)
            .await
            .unwrap();
        sqlx::migrate!()
            .set_ignore_missing(true)
            .run(&pool)
            .await
            .unwrap();
        pool
    }

    fn create_test_event_sender() -> broadcast::Sender<ServerMessage> {
        let (sender, _) = broadcast::channel(16);
        sender
    }

    #[tokio::test]
    async fn test_run_function_websocket_connection_error() {
        let mut ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let pool = create_test_pool().await;
        ctx.evm.ws_rpc_url = "ws://invalid.nonexistent.url:8545".parse().unwrap();
        Box::pin(run(ctx, pool, create_test_event_sender()))
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_run_function_invalid_orderbook_address() {
        let mut ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        let pool = create_test_pool().await;
        ctx.evm.orderbook = Address::ZERO;
        ctx.evm.ws_rpc_url = "ws://localhost:8545".parse().unwrap();
        Box::pin(run(ctx, pool, create_test_event_sender()))
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_run_function_error_propagation() {
        let mut ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        ctx.evm.ws_rpc_url = "ws://invalid.nonexistent.localhost:9999".parse().unwrap();
        let pool = create_test_pool().await;
        Box::pin(run(ctx, pool, create_test_event_sender()))
            .await
            .unwrap_err();
    }
}
