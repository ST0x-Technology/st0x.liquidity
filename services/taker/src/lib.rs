//! Order taker bot for tokenized equities.
//!
//! Monitors Raindex orders placed by other users and takes profitable
//! opportunities by acquiring tokens via Alpaca's tokenization API
//! and executing `takeOrders4` on the orderbook contract.

use tracing::info;
use tracing_subscriber::EnvFilter;

#[allow(dead_code)]
mod approval;
pub mod config;

pub use config::{Ctx, Env, LogLevel};

/// Initializes tracing with the given log level and sensible defaults.
pub fn setup_tracing(log_level: &LogLevel) {
    let level: tracing::Level = log_level.into();

    let filter = EnvFilter::new(format!(
        "st0x_taker={level},st0x_shared={level},st0x_execution={level}"
    ));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init()
        .ok();
}

/// Main entry point for the taker bot.
///
/// Sets up the database, runs migrations, and starts the event loop.
pub async fn launch(ctx: Ctx) -> anyhow::Result<()> {
    let pool = ctx.get_sqlite_pool().await?;

    sqlx::migrate!().run(&pool).await?;

    info!("taker bot started, database migrated");

    // Phase 1 scaffold: log startup and return.
    // The event loop, order collector, and profitability strategy
    // will be added in subsequent issues.
    info!("no event loop yet -- scaffold only, shutting down");

    Ok(())
}
