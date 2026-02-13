//! Market making system for tokenized equities.
//!
//! Provides onchain liquidity via Raindex orders and hedges
//! directional exposure by executing offsetting trades on
//! traditional brokerages.

use rocket::{Ignite, Rocket};
use sqlx::SqlitePool;
use tokio::sync::broadcast;
use tokio::task::{AbortHandle, JoinError, JoinHandle};
use tracing::{error, info, info_span, warn};

use st0x_dto::ServerMessage;
use st0x_execution::{ExecutionError, Executor, MockExecutorCtx, SchwabError, TryIntoExecutor};

use crate::config::{BrokerCtx, Ctx};

mod alpaca_tokenization;
mod alpaca_wallet;
pub mod api;
mod bindings;
mod cctp;
pub mod cli;
mod conductor;
pub mod config;
pub(crate) mod dashboard;
mod equity_redemption;
mod error_decoding;
mod event_sourced;
mod inventory;
mod lifecycle;
mod offchain;
mod offchain_order;
mod onchain;
mod onchain_trade;
mod position;
mod queue;
mod rebalancing;
mod symbol;
mod telemetry;
mod threshold;
mod tokenized_equity_mint;
mod usdc_rebalance;
mod vault_registry;

pub use telemetry::{TelemetryError, TelemetryGuard, setup_tracing};

#[cfg(test)]
mod integration_tests;
#[cfg(test)]
pub mod test_utils;

pub async fn launch(ctx: Ctx) -> anyhow::Result<()> {
    let launch_span = info_span!("launch");
    let _enter = launch_span.enter();

    let pool = ctx.get_sqlite_pool().await?;
    sqlx::migrate!().run(&pool).await?;

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

        if let Err(error) = Box::pin(run(ctx, pool, event_sender)).await {
            error!("Bot failed: {error}");
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
        Ok(Err(error)) => error!("Server failed: {error}"),
        Err(error) => error!("Server task panicked: {error}"),
    }
}

fn log_bot_result(result: Result<(), JoinError>) {
    match result {
        Ok(()) => info!("Bot task completed"),
        Err(error) => error!("Bot task panicked: {error}"),
    }
}

#[tracing::instrument(skip_all, level = tracing::Level::INFO)]
async fn run(
    ctx: Ctx,
    pool: SqlitePool,
    event_sender: broadcast::Sender<ServerMessage>,
) -> anyhow::Result<()> {
    const RERUN_DELAY_SECS: u64 = 10;

    loop {
        let result = Box::pin(run_bot_session(&ctx, &pool, event_sender.clone())).await;

        match result {
            Ok(()) => {
                info!("Bot session completed successfully");
                break Ok(());
            }
            Err(error) => {
                if let Some(execution_error) = error.downcast_ref::<ExecutionError>()
                    && matches!(
                        execution_error,
                        ExecutionError::Schwab(SchwabError::RefreshTokenExpired)
                    )
                {
                    warn!("Refresh token expired, retrying in {RERUN_DELAY_SECS} seconds");
                    tokio::time::sleep(std::time::Duration::from_secs(RERUN_DELAY_SECS)).await;
                    continue;
                }

                error!("Bot session failed: {error}");
                return Err(error);
            }
        }
    }
}

#[tracing::instrument(skip_all, level = tracing::Level::INFO)]
async fn run_bot_session(
    ctx: &Ctx,
    pool: &SqlitePool,
    event_sender: broadcast::Sender<ServerMessage>,
) -> anyhow::Result<()> {
    match &ctx.broker {
        BrokerCtx::DryRun => {
            info!("Initializing test executor for dry-run mode");
            let executor = MockExecutorCtx.try_into_executor().await?;

            Box::pin(run_with_executor(
                ctx.clone(),
                pool.clone(),
                executor,
                event_sender,
            ))
            .await
        }
        BrokerCtx::Schwab(schwab_auth) => {
            info!("Initializing Schwab executor");
            let schwab_ctx = schwab_auth.to_schwab_ctx(pool.clone());
            let executor = schwab_ctx.try_into_executor().await?;

            Box::pin(run_with_executor(
                ctx.clone(),
                pool.clone(),
                executor,
                event_sender,
            ))
            .await
        }
        BrokerCtx::AlpacaTradingApi(alpaca_auth) => {
            info!("Initializing Alpaca Trading API executor");
            let executor = alpaca_auth.clone().try_into_executor().await?;

            Box::pin(run_with_executor(
                ctx.clone(),
                pool.clone(),
                executor,
                event_sender,
            ))
            .await
        }
        BrokerCtx::AlpacaBrokerApi(alpaca_auth) => {
            info!("Initializing Alpaca Broker API executor");
            let executor = alpaca_auth.clone().try_into_executor().await?;

            Box::pin(run_with_executor(
                ctx.clone(),
                pool.clone(),
                executor,
                event_sender,
            ))
            .await
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
    E: Executor + Clone + Send + 'static,
    conductor::EventProcessingError: From<E::Error>,
{
    let executor_maintenance = executor.run_executor_maintenance().await;

    conductor::run_market_hours_loop(executor, ctx, pool, executor_maintenance, event_sender).await
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address};

    use super::*;
    use crate::config::tests::create_test_ctx_with_order_owner;

    async fn create_test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
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
