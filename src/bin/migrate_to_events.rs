use clap::Parser;
use sqlx::SqlitePool;
use st0x_hedge::migration::{self, MigrationEnv};
use tracing::info;

fn setup_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,migrate_to_events=info".into()),
        )
        .init();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv_override().ok();
    setup_logging();

    let env = MigrationEnv::parse();
    let database_url = &env.database_url;

    info!("Connecting to database: {database_url}");
    let pool = SqlitePool::connect(database_url).await?;

    info!("Running database migrations...");
    sqlx::migrate!("./migrations").run(&pool).await?;

    let summary = migration::run_migration(&pool, &env).await?;

    let schwab_status = if summary.schwab_auth {
        "migrated"
    } else {
        "not present"
    };
    let onchain_trades = summary.onchain_trades;
    let positions = summary.positions;
    let offchain_orders = summary.offchain_orders;

    info!("Migration complete:");
    info!("  OnChainTrade: {onchain_trades}");
    info!("  Position: {positions}");
    info!("  OffchainOrder: {offchain_orders}");
    info!("  SchwabAuth: {schwab_status}");

    Ok(())
}
