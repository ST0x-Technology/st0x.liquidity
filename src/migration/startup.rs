//! Automatic migration and consistency check on startup.
//!
//! Runs as part of application startup to:
//! 1. Migrate legacy data to events if no events exist
//! 2. Verify consistency between legacy state and ES-derived state

use alloy::primitives::FixedBytes;
use cqrs_es::persist::PersistedEventStore;
use cqrs_es::{AggregateContext, EventStore};
use rust_decimal::Decimal;
use sqlite_es::{SqliteEventRepository, sqlite_cqrs};
use sqlx::SqlitePool;
use st0x_execution::Symbol;
use tracing::{error, info, warn};

use super::{ExecutionMode, MigrationError};
use crate::env::BrokerConfig;
use crate::lifecycle::Lifecycle;
use crate::position::Position;
use crate::shares::{ArithmeticError, FractionalShares};

#[derive(Debug)]
struct AggregateStats {
    legacy_count: i64,
    event_count: i64,
}

impl AggregateStats {
    fn needs_migration(&self) -> bool {
        self.event_count == 0 && self.legacy_count > 0
    }

    fn needs_consistency_check(&self) -> bool {
        self.event_count > 0 && self.legacy_count > 0
    }
}

pub(crate) async fn run_startup_check(
    pool: &SqlitePool,
    broker_config: &BrokerConfig,
) -> Result<(), StartupCheckError> {
    info!("Running startup migration/consistency check");

    let encryption_key = match broker_config {
        BrokerConfig::Schwab(schwab_auth) => Some(schwab_auth.encryption_key),
        _ => None,
    };

    check_onchain_trades(pool).await?;
    check_positions(pool).await?;
    check_offchain_orders(pool).await?;

    if let Some(key) = encryption_key {
        check_schwab_auth(pool, key).await?;
    }

    info!("Startup check complete");
    Ok(())
}

async fn get_aggregate_stats(
    pool: &SqlitePool,
    aggregate_type: &str,
    legacy_table: &str,
) -> Result<AggregateStats, sqlx::Error> {
    let legacy_count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {legacy_table}"))
        .fetch_one(pool)
        .await?;

    let event_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE aggregate_type = ?")
            .bind(aggregate_type)
            .fetch_one(pool)
            .await?;

    Ok(AggregateStats {
        legacy_count,
        event_count,
    })
}

async fn check_onchain_trades(pool: &SqlitePool) -> Result<(), StartupCheckError> {
    let stats = get_aggregate_stats(pool, "OnChainTrade", "onchain_trades").await?;

    if stats.needs_migration() {
        run_onchain_trades_migration(pool, &stats).await?;
    } else if stats.needs_consistency_check() {
        info!(
            "OnChainTrade: {} legacy records, {} events - checking consistency",
            stats.legacy_count, stats.event_count
        );
        verify_onchain_trades_consistency(pool).await;
    } else {
        info!(
            "OnChainTrade: {} legacy records, {} events - no action needed",
            stats.legacy_count, stats.event_count
        );
    }

    Ok(())
}

async fn run_onchain_trades_migration(
    pool: &SqlitePool,
    stats: &AggregateStats,
) -> Result<(), StartupCheckError> {
    info!(
        "OnChainTrade: {} legacy records, 0 events - running migration",
        stats.legacy_count
    );

    let cqrs = sqlite_cqrs(pool.clone(), vec![], ());
    super::onchain_trade::migrate_onchain_trades(pool, &cqrs, ExecutionMode::Commit).await?;

    info!("OnChainTrade migration complete");
    Ok(())
}

async fn verify_onchain_trades_consistency(pool: &SqlitePool) {
    let legacy_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM onchain_trades")
        .fetch_one(pool)
        .await
        .unwrap_or(0);

    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT aggregate_id) FROM events WHERE aggregate_type = 'OnChainTrade'",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    if legacy_count != event_count {
        error!(
            "CONSISTENCY MISMATCH: OnChainTrade has {} legacy records but {} event aggregates",
            legacy_count, event_count
        );
    }
}

async fn check_positions(pool: &SqlitePool) -> Result<(), StartupCheckError> {
    let stats = get_aggregate_stats(pool, "Position", "trade_accumulators").await?;

    if stats.needs_migration() {
        run_positions_migration(pool, &stats).await?;
    } else if stats.needs_consistency_check() {
        info!(
            "Position: {} legacy records, {} events - checking consistency",
            stats.legacy_count, stats.event_count
        );
        verify_positions_consistency(pool).await;
    } else {
        info!(
            "Position: {} legacy records, {} events - no action needed",
            stats.legacy_count, stats.event_count
        );
    }

    Ok(())
}

async fn run_positions_migration(
    pool: &SqlitePool,
    stats: &AggregateStats,
) -> Result<(), StartupCheckError> {
    info!(
        "Position: {} legacy records, 0 events - running migration",
        stats.legacy_count
    );

    let cqrs = sqlite_cqrs(pool.clone(), vec![], ());
    super::position::migrate_positions(pool, &cqrs, ExecutionMode::Commit).await?;

    info!("Position migration complete");
    Ok(())
}

async fn verify_positions_consistency(pool: &SqlitePool) {
    #[derive(sqlx::FromRow)]
    struct LegacyPosition {
        symbol: String,
        accumulated_long: f64,
        accumulated_short: f64,
    }

    let legacy_positions = match sqlx::query_as::<_, LegacyPosition>(
        "SELECT symbol, accumulated_long, accumulated_short FROM trade_accumulators",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            error!("Failed to fetch legacy positions for consistency check: {e}");
            return;
        }
    };

    let repo = SqliteEventRepository::new(pool.clone());
    let store = PersistedEventStore::<
        SqliteEventRepository,
        Lifecycle<Position, ArithmeticError<FractionalShares>>,
    >::new_event_store(repo);

    for legacy in legacy_positions {
        verify_single_position(
            &store,
            &legacy.symbol,
            legacy.accumulated_long,
            legacy.accumulated_short,
        )
        .await;
    }
}

async fn verify_single_position(
    store: &PersistedEventStore<
        SqliteEventRepository,
        Lifecycle<Position, ArithmeticError<FractionalShares>>,
    >,
    symbol_str: &str,
    accumulated_long: f64,
    accumulated_short: f64,
) {
    let Some((position, legacy_long, legacy_short)) =
        load_position_for_verification(store, symbol_str, accumulated_long, accumulated_short)
            .await
    else {
        return;
    };

    let legacy_net = legacy_long - legacy_short;

    check_field_match(symbol_str, "net_position", position.net.inner(), legacy_net);
    check_field_match(
        symbol_str,
        "accumulated_long",
        position.accumulated_long.inner(),
        legacy_long,
    );
    check_field_match(
        symbol_str,
        "accumulated_short",
        position.accumulated_short.inner(),
        legacy_short,
    );
}

async fn load_position_for_verification(
    store: &PersistedEventStore<
        SqliteEventRepository,
        Lifecycle<Position, ArithmeticError<FractionalShares>>,
    >,
    symbol_str: &str,
    accumulated_long: f64,
    accumulated_short: f64,
) -> Option<(Position, Decimal, Decimal)> {
    let position = load_es_position(store, symbol_str).await?;
    let legacy_long = parse_legacy_decimal(symbol_str, "accumulated_long", accumulated_long)?;
    let legacy_short = parse_legacy_decimal(symbol_str, "accumulated_short", accumulated_short)?;
    Some((position, legacy_long, legacy_short))
}

async fn load_es_position(
    store: &PersistedEventStore<
        SqliteEventRepository,
        Lifecycle<Position, ArithmeticError<FractionalShares>>,
    >,
    symbol_str: &str,
) -> Option<Position> {
    let Ok(symbol) = Symbol::new(symbol_str) else {
        warn!("Position consistency check: invalid symbol in legacy data: {symbol_str}");
        return None;
    };

    let aggregate_id = Position::aggregate_id(&symbol);
    let aggregate_context = match store.load_aggregate(&aggregate_id).await {
        Ok(ctx) => ctx,
        Err(e) => {
            error!("Position consistency check: failed to load ES state for {symbol_str}: {e}");
            return None;
        }
    };

    let Lifecycle::Live(position) = aggregate_context.aggregate() else {
        error!(
            "CONSISTENCY MISMATCH: Position {symbol_str} exists in legacy but ES state is not Live"
        );
        return None;
    };

    Some(position.clone())
}

fn parse_legacy_decimal(symbol_str: &str, field: &str, value: f64) -> Option<Decimal> {
    Decimal::try_from(value).map_or_else(
        |_| {
            warn!(symbol = %symbol_str, raw_value = value, "Position consistency check: invalid {field} value, skipping");
            None
        },
        Some,
    )
}

fn check_field_match(symbol: &str, field: &str, es_value: Decimal, legacy_value: Decimal) {
    if es_value != legacy_value {
        error!(
            "CONSISTENCY MISMATCH: Position {symbol} {field}: ES={es_value} legacy={legacy_value}"
        );
    }
}

async fn check_offchain_orders(pool: &SqlitePool) -> Result<(), StartupCheckError> {
    let stats = get_aggregate_stats(pool, "OffchainOrder", "offchain_trades").await?;

    if stats.needs_migration() {
        run_offchain_orders_migration(pool, &stats).await?;
    } else if stats.needs_consistency_check() {
        info!(
            "OffchainOrder: {} legacy records, {} events - checking consistency",
            stats.legacy_count, stats.event_count
        );
        verify_offchain_orders_consistency(pool).await;
    } else {
        info!(
            "OffchainOrder: {} legacy records, {} events - no action needed",
            stats.legacy_count, stats.event_count
        );
    }

    Ok(())
}

async fn run_offchain_orders_migration(
    pool: &SqlitePool,
    stats: &AggregateStats,
) -> Result<(), StartupCheckError> {
    info!(
        "OffchainOrder: {} legacy records, 0 events - running migration",
        stats.legacy_count
    );

    let cqrs = sqlite_cqrs(pool.clone(), vec![], ());
    super::offchain_order::migrate_offchain_orders(pool, &cqrs, ExecutionMode::Commit).await?;

    info!("OffchainOrder migration complete");
    Ok(())
}

async fn verify_offchain_orders_consistency(pool: &SqlitePool) {
    let legacy_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM offchain_trades")
        .fetch_one(pool)
        .await
        .unwrap_or(0);

    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT aggregate_id) FROM events WHERE aggregate_type = 'OffchainOrder'",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    if legacy_count != event_count {
        error!(
            "CONSISTENCY MISMATCH: OffchainOrder has {} legacy records but {} event aggregates",
            legacy_count, event_count
        );
    }
}

async fn check_schwab_auth(
    pool: &SqlitePool,
    encryption_key: FixedBytes<32>,
) -> Result<(), StartupCheckError> {
    let legacy_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM schwab_auth")
        .fetch_one(pool)
        .await?;

    let event_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE aggregate_type = 'SchwabAuth'")
            .bind("SchwabAuth")
            .fetch_one(pool)
            .await?;

    if event_count == 0 && legacy_count > 0 {
        run_schwab_auth_migration(pool, encryption_key).await?;
    } else {
        info!(
            "SchwabAuth: {} legacy records, {} events - no migration needed",
            legacy_count, event_count
        );
    }

    Ok(())
}

async fn run_schwab_auth_migration(
    pool: &SqlitePool,
    encryption_key: FixedBytes<32>,
) -> Result<(), StartupCheckError> {
    info!("SchwabAuth: legacy record exists, 0 events - running migration");

    let cqrs = sqlite_cqrs(pool.clone(), vec![], encryption_key);
    super::schwab_auth::migrate_schwab_auth(pool, &cqrs, ExecutionMode::Commit).await?;

    info!("SchwabAuth migration complete");
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StartupCheckError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Migration error: {0}")]
    Migration(#[from] MigrationError),
}

#[cfg(test)]
mod tests {
    use alloy::primitives::b256;
    use chrono::Utc;
    use sqlx::SqlitePool;

    use super::*;
    use crate::env::BrokerConfig;
    use crate::onchain_trade::OnChainTrade;

    fn dry_run_config() -> BrokerConfig {
        BrokerConfig::DryRun
    }

    async fn create_test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn fresh_install_no_action() {
        let pool = create_test_pool().await;

        run_startup_check(&pool, &dry_run_config()).await.unwrap();

        let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(event_count, 0);
    }

    #[tokio::test]
    async fn legacy_data_triggers_migration() {
        let pool = create_test_pool().await;

        let tx_hash = b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        let tx_hash_str = tx_hash.to_string();

        sqlx::query!(
            "INSERT INTO onchain_trades (tx_hash, log_index, symbol, amount, direction, price_usdc)
             VALUES (?, 0, 'AAPL', 10.0, 'BUY', 150.50)",
            tx_hash_str
        )
        .execute(&pool)
        .await
        .unwrap();

        run_startup_check(&pool, &dry_run_config()).await.unwrap();

        let event_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE aggregate_type = 'OnChainTrade'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(event_count, 1);
    }

    #[tokio::test]
    async fn existing_events_skips_migration() {
        let pool = create_test_pool().await;

        let tx_hash = b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        let tx_hash_str = tx_hash.to_string();

        sqlx::query!(
            "INSERT INTO onchain_trades (tx_hash, log_index, symbol, amount, direction, price_usdc)
             VALUES (?, 0, 'AAPL', 10.0, 'BUY', 150.50)",
            tx_hash_str
        )
        .execute(&pool)
        .await
        .unwrap();

        let aggregate_id = OnChainTrade::aggregate_id(tx_hash, 0);
        sqlx::query!(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
             VALUES ('OnChainTrade', ?, 1, 'Migrated', '1.0', '{}', '{}')",
            aggregate_id
        )
        .execute(&pool)
        .await
        .unwrap();

        let initial_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        run_startup_check(&pool, &dry_run_config()).await.unwrap();

        let final_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(final_count, initial_count);
    }

    #[tokio::test]
    async fn position_migration_works() {
        let pool = create_test_pool().await;

        sqlx::query!(
            "INSERT INTO trade_accumulators (symbol, accumulated_long, accumulated_short)
             VALUES ('AAPL', 15.0, 5.0)"
        )
        .execute(&pool)
        .await
        .unwrap();

        run_startup_check(&pool, &dry_run_config()).await.unwrap();

        let event_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE aggregate_type = 'Position'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(event_count, 1);
    }

    #[tokio::test]
    async fn offchain_order_migration_works() {
        let pool = create_test_pool().await;

        let executed_at = Utc::now();
        sqlx::query!(
            "INSERT INTO offchain_trades (symbol, shares, direction, broker_order_id, price_cents, status, executed_at)
             VALUES ('AAPL', 10, 'BUY', 'ORD123', 15050, 'FILLED', ?)",
            executed_at
        )
        .execute(&pool)
        .await
        .unwrap();

        run_startup_check(&pool, &dry_run_config()).await.unwrap();

        let event_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events WHERE aggregate_type = 'OffchainOrder'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(event_count, 1);
    }

    #[tokio::test]
    async fn multiple_aggregates_migrate_together() {
        let pool = create_test_pool().await;

        let tx_hash = b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        let tx_hash_str = tx_hash.to_string();

        sqlx::query!(
            "INSERT INTO onchain_trades (tx_hash, log_index, symbol, amount, direction, price_usdc)
             VALUES (?, 0, 'AAPL', 10.0, 'BUY', 150.50)",
            tx_hash_str
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query!(
            "INSERT INTO trade_accumulators (symbol, accumulated_long, accumulated_short)
             VALUES ('AAPL', 10.0, 0.0)"
        )
        .execute(&pool)
        .await
        .unwrap();

        let executed_at = Utc::now();
        sqlx::query!(
            "INSERT INTO offchain_trades (symbol, shares, direction, broker_order_id, price_cents, status, executed_at)
             VALUES ('AAPL', 10, 'SELL', 'ORD456', 15100, 'FILLED', ?)",
            executed_at
        )
        .execute(&pool)
        .await
        .unwrap();

        run_startup_check(&pool, &dry_run_config()).await.unwrap();

        let onchain_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE aggregate_type = 'OnChainTrade'")
                .fetch_one(&pool)
                .await
                .unwrap();

        let position_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE aggregate_type = 'Position'")
                .fetch_one(&pool)
                .await
                .unwrap();

        let offchain_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events WHERE aggregate_type = 'OffchainOrder'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(onchain_count, 1);
        assert_eq!(position_count, 1);
        assert_eq!(offchain_count, 1);
    }

    #[test]
    fn aggregate_stats_needs_migration_when_legacy_exists_and_no_events() {
        let stats = AggregateStats {
            legacy_count: 5,
            event_count: 0,
        };
        assert!(stats.needs_migration());
        assert!(!stats.needs_consistency_check());
    }

    #[test]
    fn aggregate_stats_needs_consistency_check_when_both_exist() {
        let stats = AggregateStats {
            legacy_count: 5,
            event_count: 5,
        };
        assert!(!stats.needs_migration());
        assert!(stats.needs_consistency_check());
    }

    #[test]
    fn aggregate_stats_no_action_when_no_legacy_data() {
        let stats = AggregateStats {
            legacy_count: 0,
            event_count: 0,
        };
        assert!(!stats.needs_migration());
        assert!(!stats.needs_consistency_check());
    }

    #[test]
    fn aggregate_stats_no_action_when_only_events() {
        let stats = AggregateStats {
            legacy_count: 0,
            event_count: 10,
        };
        assert!(!stats.needs_migration());
        assert!(!stats.needs_consistency_check());
    }

    #[test]
    fn parse_legacy_decimal_valid_value() {
        let result = parse_legacy_decimal("AAPL", "accumulated_long", 10.5);
        assert_eq!(result, Some(Decimal::try_from(10.5).unwrap()));
    }

    #[test]
    fn parse_legacy_decimal_handles_nan() {
        let result = parse_legacy_decimal("AAPL", "accumulated_long", f64::NAN);
        assert!(result.is_none());
    }

    #[test]
    fn parse_legacy_decimal_handles_infinity() {
        let result = parse_legacy_decimal("AAPL", "accumulated_long", f64::INFINITY);
        assert!(result.is_none());
    }

    #[test]
    fn check_field_match_equal_values_no_panic() {
        let value = Decimal::try_from(10.5).unwrap();
        check_field_match("AAPL", "net_position", value, value);
    }

    #[test]
    fn check_field_match_different_values_logs_error() {
        let es_value = Decimal::try_from(10.5).unwrap();
        let legacy_value = Decimal::try_from(11.0).unwrap();
        check_field_match("AAPL", "net_position", es_value, legacy_value);
    }
}
