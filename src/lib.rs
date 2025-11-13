use sqlx::SqlitePool;
use tracing::{error, info, info_span, warn};

pub mod api;
mod bindings;
pub mod cli;
mod conductor;
pub mod env;
mod error;
mod lock;
mod offchain;
mod onchain;
mod queue;
pub mod reporter;
mod symbol;
mod telemetry;
mod trade_execution_link;

pub use telemetry::{TelemetryError, TelemetryGuard};

#[cfg(test)]
pub mod test_utils;

use crate::env::{BrokerConfig, Config};
use st0x_broker::schwab::{SchwabConfig, SchwabError};
use st0x_broker::{Broker, BrokerError, MockBrokerConfig, TryIntoBroker};

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
                if let Some(broker_error) = e.downcast_ref::<BrokerError>() {
                    if matches!(
                        broker_error,
                        BrokerError::Schwab(SchwabError::RefreshTokenExpired)
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
            info!("Initializing test broker for dry-run mode");
            let broker = MockBrokerConfig.try_into_broker().await?;
            Box::pin(run_with_broker(config.clone(), pool.clone(), broker)).await
        }
        BrokerConfig::Schwab(schwab_auth) => {
            info!("Initializing Schwab broker");
            let schwab_config = SchwabConfig {
                auth: schwab_auth.clone(),
                pool: pool.clone(),
            };
            let broker = schwab_config.try_into_broker().await?;
            Box::pin(run_with_broker(config.clone(), pool.clone(), broker)).await
        }
        BrokerConfig::Alpaca(alpaca_auth) => {
            info!("Initializing Alpaca broker");
            let broker = alpaca_auth.clone().try_into_broker().await?;
            Box::pin(run_with_broker(config.clone(), pool.clone(), broker)).await
        }
    }
}

async fn run_with_broker<B: Broker + Clone + Send + 'static>(
    config: Config,
    pool: SqlitePool,
    broker: B,
) -> anyhow::Result<()> {
    let broker_maintenance = broker.run_broker_maintenance().await;

    conductor::run_market_hours_loop(broker, config, pool, broker_maintenance).await
}

#[cfg(test)]
mod tests {
    use super::*;
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
        config.evm.orderbook = alloy::primitives::Address::ZERO;
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
