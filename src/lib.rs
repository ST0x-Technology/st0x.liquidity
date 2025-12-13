use rocket::{Ignite, Rocket};
use sqlx::SqlitePool;
use tokio::sync::broadcast;
use tokio::task::{JoinError, JoinHandle};
use tracing::{error, info, info_span, warn};

use crate::dashboard::ServerMessage;

mod alpaca_tokenization;
mod alpaca_wallet;
pub mod api;
mod bindings;
mod cctp;
pub mod cli;
mod conductor;
pub mod dashboard;
pub mod env;
mod equity_redemption;
mod error;
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

    let (event_sender, _) = broadcast::channel::<ServerMessage>(256);

    let server_task = spawn_server_task(&config, &pool, event_sender.clone());
    let bot_task = spawn_bot_task(config, pool, event_sender);

    await_shutdown(server_task, bot_task).await;

    info!("Shutdown complete");
    Ok(())
}

fn spawn_server_task(
    config: &Config,
    pool: &SqlitePool,
    event_sender: broadcast::Sender<ServerMessage>,
) -> JoinHandle<Result<Rocket<Ignite>, rocket::Error>> {
    let rocket_config = rocket::Config::figment()
        .merge(("port", config.server_port))
        .merge(("address", "0.0.0.0"));

    let rocket = rocket::custom(rocket_config)
        .mount("/", api::routes())
        .mount("/api", dashboard::routes())
        .manage(pool.clone())
        .manage(config.clone())
        .manage(dashboard::Broadcast {
            sender: event_sender,
        });

    tokio::spawn(rocket.launch())
}

fn spawn_bot_task(
    config: Config,
    pool: SqlitePool,
    event_sender: broadcast::Sender<ServerMessage>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let bot_span = info_span!("bot_task");
        let _enter = bot_span.enter();

        if let Err(e) = Box::pin(run(config, pool, event_sender)).await {
            error!("Bot failed: {e}");
        }
    })
}

async fn await_shutdown(
    server_task: JoinHandle<Result<Rocket<Ignite>, rocket::Error>>,
    bot_task: JoinHandle<()>,
) {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal, shutting down gracefully...");
        }
        result = server_task => {
            log_server_result(result);
        }
        result = bot_task => {
            log_bot_result(result);
        }
    }
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
    config: Config,
    pool: SqlitePool,
    event_sender: broadcast::Sender<ServerMessage>,
) -> anyhow::Result<()> {
    const RERUN_DELAY_SECS: u64 = 10;

    loop {
        let result = Box::pin(run_bot_session(&config, &pool, event_sender.clone())).await;

        match result {
            Ok(()) => {
                info!("Bot session completed successfully");
                break Ok(());
            }
            Err(e) => {
                if let Some(broker_error) = e.downcast_ref::<BrokerError>()
                    && matches!(
                        broker_error,
                        BrokerError::Schwab(SchwabError::RefreshTokenExpired)
                    )
                {
                    warn!("Refresh token expired, retrying in {RERUN_DELAY_SECS} seconds");
                    tokio::time::sleep(std::time::Duration::from_secs(RERUN_DELAY_SECS)).await;
                    continue;
                }

                error!("Bot session failed: {e}");
                return Err(e);
            }
        }
    }
}

#[tracing::instrument(skip_all, level = tracing::Level::INFO)]
async fn run_bot_session(
    config: &Config,
    pool: &SqlitePool,
    event_sender: broadcast::Sender<ServerMessage>,
) -> anyhow::Result<()> {
    match &config.broker {
        BrokerConfig::DryRun => {
            info!("Initializing test broker for dry-run mode");
            let broker = MockBrokerConfig.try_into_broker().await?;

            Box::pin(run_with_broker(
                config.clone(),
                pool.clone(),
                broker,
                None,
                event_sender,
            ))
            .await
        }
        BrokerConfig::Schwab(schwab_auth) => {
            info!("Initializing Schwab broker");
            let schwab_config = SchwabConfig {
                auth: schwab_auth.clone(),
                pool: pool.clone(),
            };
            let broker = schwab_config.try_into_broker().await?;

            Box::pin(run_with_broker(
                config.clone(),
                pool.clone(),
                broker,
                None,
                event_sender,
            ))
            .await
        }
        BrokerConfig::Alpaca(alpaca_auth) => {
            info!("Initializing Alpaca broker");
            let broker = alpaca_auth.clone().try_into_broker().await?;

            Box::pin(run_with_broker(
                config.clone(),
                pool.clone(),
                broker,
                None,
                event_sender,
            ))
            .await
        }
    }
}

async fn run_with_broker<B: Broker + Clone + Send + 'static>(
    config: Config,
    pool: SqlitePool,
    broker: B,
    rebalancer: Option<JoinHandle<()>>,
    event_sender: broadcast::Sender<ServerMessage>,
) -> anyhow::Result<()> {
    let broker_maintenance = broker.run_broker_maintenance().await;

    conductor::run_market_hours_loop(
        broker,
        config,
        pool,
        broker_maintenance,
        rebalancer,
        event_sender,
    )
    .await
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

    fn create_test_event_sender() -> broadcast::Sender<ServerMessage> {
        let (sender, _) = broadcast::channel(16);
        sender
    }

    #[tokio::test]
    async fn test_run_function_websocket_connection_error() {
        let mut config = create_test_config();
        let pool = create_test_pool().await;
        config.evm.ws_rpc_url = "ws://invalid.nonexistent.url:8545".parse().unwrap();
        Box::pin(run(config, pool, create_test_event_sender()))
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_run_function_invalid_orderbook_address() {
        let mut config = create_test_config();
        let pool = create_test_pool().await;
        config.evm.orderbook = alloy::primitives::Address::ZERO;
        config.evm.ws_rpc_url = "ws://localhost:8545".parse().unwrap();
        Box::pin(run(config, pool, create_test_event_sender()))
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_run_function_error_propagation() {
        let mut config = create_test_config();
        config.evm.ws_rpc_url = "ws://invalid.nonexistent.localhost:9999".parse().unwrap();
        let pool = create_test_pool().await;
        Box::pin(run(config, pool, create_test_event_sender()))
            .await
            .unwrap_err();
    }
}
