use sqlx::SqlitePool;
use tokio::task::JoinHandle;
use tracing::{error, info, info_span, warn};

mod alpaca_tokenization;
mod alpaca_wallet;
pub mod api;
mod bindings;
mod cctp;
pub mod cli;
mod conductor;
pub mod env;
mod equity_redemption;
mod error;
mod error_decoding;
mod inventory;
mod lifecycle;
mod lock;
pub mod migration;
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

pub use telemetry::{TelemetryError, TelemetryGuard};

#[cfg(test)]
pub mod test_utils;

use crate::env::{BrokerConfig, Config};
use st0x_execution::schwab::{SchwabConfig, SchwabError};
use st0x_execution::{ExecutionError, Executor, MockExecutorConfig, TryIntoExecutor};

pub async fn launch(config: Config) -> anyhow::Result<()> {
    let launch_span = info_span!("launch");
    let _enter = launch_span.enter();

    let pool = config.get_sqlite_pool().await?;

    sqlx::migrate!().run(&pool).await?;

    let rocket_config = rocket::Config::figment()
        .merge(("port", config.server_port))
        .merge(("address", "0.0.0.0"));

    let rocket = rocket::custom(rocket_config)
        .mount("/", api::routes())
        .manage(pool.clone())
        .manage(config.clone());

    let server_task = tokio::spawn(rocket.launch());

    let bot_pool = pool.clone();
    let bot_task = tokio::spawn(async move {
        let bot_span = info_span!("bot_task");
        let _enter = bot_span.enter();

        if let Err(e) = Box::pin(run(config, bot_pool)).await {
            error!("Bot failed: {e}");
        }
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal, shutting down gracefully...");
        }

        result = server_task => {
            match result {
                Ok(Ok(_)) => info!("Server completed successfully"),
                Ok(Err(e)) => error!("Server failed: {e}"),
                Err(e) => error!("Server task panicked: {e}"),
            }
        }

        result = bot_task => {
            match result {
                Ok(()) => info!("Bot task completed"),
                Err(e) => error!("Bot task panicked: {e}"),
            }
        }
    }

    info!("Shutdown complete");
    Ok(())
}

#[tracing::instrument(skip_all, level = tracing::Level::INFO)]
async fn run(config: Config, pool: SqlitePool) -> anyhow::Result<()> {
    const RERUN_DELAY_SECS: u64 = 10;

    loop {
        let result = Box::pin(run_bot_session(&config, &pool)).await;

        match result {
            Ok(()) => {
                info!("Bot session completed successfully");
                break Ok(());
            }
            Err(e) => {
                if let Some(execution_error) = e.downcast_ref::<ExecutionError>() {
                    if matches!(
                        execution_error,
                        ExecutionError::Schwab(SchwabError::RefreshTokenExpired)
                    ) {
                        warn!(
                            "Refresh token expired, retrying in {} seconds",
                            RERUN_DELAY_SECS
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(RERUN_DELAY_SECS)).await;
                        continue;
                    }
                }

                error!("Bot session failed: {e}");
                return Err(e);
            }
        }
    }
}

#[tracing::instrument(skip_all, level = tracing::Level::INFO)]
async fn run_bot_session(config: &Config, pool: &SqlitePool) -> anyhow::Result<()> {
    match &config.broker {
        BrokerConfig::DryRun => {
            info!("Initializing test executor for dry-run mode");
            let executor = MockExecutorConfig.try_into_executor().await?;
            Box::pin(run_with_executor(
                config.clone(),
                pool.clone(),
                executor,
                None,
            ))
            .await
        }
        BrokerConfig::Schwab(schwab_auth) => {
            info!("Initializing Schwab executor");
            let schwab_config = SchwabConfig {
                auth: schwab_auth.clone(),
                pool: pool.clone(),
            };
            let executor = schwab_config.try_into_executor().await?;
            Box::pin(run_with_executor(
                config.clone(),
                pool.clone(),
                executor,
                None,
            ))
            .await
        }
        BrokerConfig::AlpacaTradingApi(alpaca_auth) => {
            info!("Initializing Alpaca Trading API executor");
            let executor = alpaca_auth.clone().try_into_executor().await?;

            Box::pin(run_with_executor(
                config.clone(),
                pool.clone(),
                executor,
                None,
            ))
            .await
        }
        BrokerConfig::AlpacaBrokerApi(alpaca_auth) => {
            info!("Initializing Alpaca Broker API executor");
            let executor = alpaca_auth.clone().try_into_executor().await?;

            Box::pin(run_with_executor(
                config.clone(),
                pool.clone(),
                executor,
                None,
            ))
            .await
        }
    }
}

async fn run_with_executor<E: Executor + Clone + Send + 'static>(
    config: Config,
    pool: SqlitePool,
    executor: E,
    rebalancer: Option<JoinHandle<()>>,
) -> anyhow::Result<()> {
    let executor_maintenance = executor.run_executor_maintenance().await;

    conductor::run_market_hours_loop(executor, config, pool, executor_maintenance, rebalancer).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::Address;

    use crate::env::tests::create_test_config;

    async fn create_test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn test_run_function_websocket_connection_error() {
        let mut config = create_test_config();
        let pool = create_test_pool().await;
        config.evm.ws_rpc_url = "ws://invalid.nonexistent.url:8545".parse().unwrap();
        Box::pin(run(config, pool)).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_run_function_invalid_orderbook_address() {
        let mut config = create_test_config();
        let pool = create_test_pool().await;
        config.evm.orderbook = Address::ZERO;
        config.evm.ws_rpc_url = "ws://localhost:8545".parse().unwrap();
        Box::pin(run(config, pool)).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_run_function_error_propagation() {
        let mut config = create_test_config();
        config.evm.ws_rpc_url = "ws://invalid.nonexistent.localhost:9999".parse().unwrap();
        let pool = create_test_pool().await;
        Box::pin(run(config, pool)).await.unwrap_err();
    }
}
