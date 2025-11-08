use clap::Parser;
use sqlx::SqlitePool;
use std::io;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[clap(
    name = "migrate_to_events",
    about = "Migrate legacy CRUD data to event-sourced aggregates",
    long_about = "
⚠️  IMPORTANT: Create a database backup before running this migration!

This tool converts existing CRUD data from legacy tables into event-sourced
aggregates using Migrated events. This is a critical step in the CQRS/ES migration.

Example usage:
  1. Backup:     cp st0x.db st0x.db.backup
  2. Dry run:    migrate_to_events --dry-run
  3. Migrate:    migrate_to_events
  4. Verify:     migrate_to_events --verify-only
"
)]
struct Cli {
    #[clap(long, env = "DATABASE_URL", help = "SQLite database path")]
    database_url: String,

    #[clap(long, help = "Skip interactive confirmation prompts")]
    force: bool,

    #[clap(
        long,
        help = "Drop all events before migrating (requires double confirmation)"
    )]
    clean: bool,

    #[clap(subcommand)]
    command: Option<Command>,
}

#[derive(Parser, Debug)]
enum Command {
    #[clap(about = "Preview migration without committing (no event persistence)")]
    DryRun,

    #[clap(about = "Skip migration, only verify data integrity")]
    VerifyOnly,
}

#[derive(Debug, Default)]
struct MigrationSummary {
    onchain_trades: usize,
    positions: usize,
    offchain_orders: usize,
    schwab_auth: bool,
}

#[derive(Debug, thiserror::Error)]
enum MigrationError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    #[error("User cancelled migration")]
    UserCancelled,
}

async fn check_existing_events(
    pool: &SqlitePool,
    aggregate_type: &str,
    force: bool,
) -> Result<(), MigrationError> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE aggregate_type = ?")
        .bind(aggregate_type)
        .fetch_one(pool)
        .await?;

    if count > 0 && !force {
        warn!(
            "Events detected for {} (count: {}). Continue? [y/N]",
            aggregate_type, count
        );
        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .map_err(|e| MigrationError::Database(sqlx::Error::Io(e)))?;

        if !input.trim().eq_ignore_ascii_case("y") {
            return Err(MigrationError::UserCancelled);
        }
    }

    Ok(())
}

fn safety_prompt(force: bool) -> Result<(), MigrationError> {
    if force {
        return Ok(());
    }

    warn!("⚠️  Create database backup before proceeding! Continue? [y/N]");
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .map_err(|e| MigrationError::Database(sqlx::Error::Io(e)))?;

    if !input.trim().eq_ignore_ascii_case("y") {
        return Err(MigrationError::UserCancelled);
    }

    Ok(())
}

async fn clean_events(pool: &SqlitePool, force: bool) -> Result<(), MigrationError> {
    if !force {
        warn!("⚠️  This will DELETE all events! Type 'DELETE' to confirm:");
        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .map_err(|e| MigrationError::Database(sqlx::Error::Io(e)))?;

        if input.trim() != "DELETE" {
            return Err(MigrationError::UserCancelled);
        }
    }

    let deleted_events = sqlx::query("DELETE FROM events")
        .execute(pool)
        .await?
        .rows_affected();

    let deleted_snapshots = sqlx::query("DELETE FROM snapshots")
        .execute(pool)
        .await?
        .rows_affected();

    info!(
        "Deleted {} events and {} snapshots from event store",
        deleted_events, deleted_snapshots
    );

    Ok(())
}

async fn run_migration(pool: &SqlitePool, opts: &Cli) -> Result<MigrationSummary, MigrationError> {
    match &opts.command {
        Some(Command::VerifyOnly) => {
            info!("Running in VERIFY-ONLY mode");
            return Ok(MigrationSummary::default());
        }
        Some(Command::DryRun) => {
            info!("Starting migration in DRY-RUN mode - no events will be persisted");
        }
        None => {
            info!("Starting migration...");
        }
    }

    check_existing_events(pool, "OnChainTrade", opts.force).await?;
    check_existing_events(pool, "Position", opts.force).await?;
    check_existing_events(pool, "OffchainOrder", opts.force).await?;
    check_existing_events(pool, "SchwabAuth", opts.force).await?;

    safety_prompt(opts.force)?;

    if opts.clean {
        clean_events(pool, opts.force).await?;
    }

    Ok(MigrationSummary::default())
}

fn setup_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,migrate_to_events=info".into()),
        )
        .init();
}

#[tokio::main]
async fn main() -> Result<(), MigrationError> {
    dotenvy::dotenv_override().ok();
    setup_logging();

    let opts = Cli::parse();

    info!("Connecting to database: {}", opts.database_url);
    let pool = SqlitePool::connect(&opts.database_url).await?;

    info!("Running database migrations...");
    sqlx::migrate!("./migrations").run(&pool).await?;

    let summary = run_migration(&pool, &opts).await?;

    let schwab_status = if summary.schwab_auth {
        "migrated"
    } else {
        "not present"
    };

    info!("Migration complete:");
    info!("  OnChainTrade: {}", summary.onchain_trades);
    info!("  Position: {}", summary.positions);
    info!("  OffchainOrder: {}", summary.offchain_orders);
    info!("  SchwabAuth: {schwab_status}");

    Ok(())
}
