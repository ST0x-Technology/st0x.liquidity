use chrono::{DateTime, Utc};
use clap::Parser;
use pnl::{FifoInventory, PnlError, PnlResult, TradeType};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{error, info};

use crate::symbol::Symbol;
use st0x_broker::Direction;

mod pnl;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct ReporterEnv {
    #[clap(long, env, default_value = "sqlite:./data/schwab.db")]
    database_url: String,
    #[clap(long, env, default_value = "30")]
    reporter_processing_interval_secs: u64,
    #[clap(long, env, default_value = "info")]
    log_level: crate::env::LogLevel,
}

impl crate::env::HasSqlite for ReporterEnv {
    async fn get_sqlite_pool(&self) -> Result<SqlitePool, sqlx::Error> {
        crate::env::configure_sqlite_pool(&self.database_url).await
    }
}

impl ReporterEnv {
    pub fn log_level(&self) -> &crate::env::LogLevel {
        &self.log_level
    }

    fn processing_interval(&self) -> Duration {
        Duration::from_secs(self.reporter_processing_interval_secs)
    }
}

#[derive(Debug, Clone)]
struct Trade {
    id: i64,
    r#type: TradeType,
    symbol: Symbol,
    quantity: Decimal,
    price_per_share: Decimal,
    direction: Direction,
    timestamp: DateTime<Utc>,
}

impl Trade {
    fn checkpoint_key(&self) -> Checkpoint {
        Checkpoint {
            timestamp: self.timestamp,
            trade_type: self.r#type,
            trade_id: self.id,
        }
    }

    fn from_onchain_row(
        id: i64,
        symbol: String,
        amount: f64,
        direction: &str,
        price_usdc: f64,
        created_at: Option<chrono::NaiveDateTime>,
    ) -> anyhow::Result<Self> {
        let quantity = Decimal::from_f64_retain(amount)
            .ok_or_else(|| anyhow::anyhow!("Failed to convert amount f64 to Decimal: {amount}"))?;

        let price_per_share = Decimal::from_f64_retain(price_usdc).ok_or_else(|| {
            anyhow::anyhow!("Failed to convert price_usdc f64 to Decimal: {price_usdc}")
        })?;

        let direction = direction
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid direction: {e}"))?;

        let timestamp = created_at
            .ok_or_else(|| anyhow::anyhow!("created_at is NULL"))?
            .and_utc();

        Ok(Self {
            r#type: TradeType::Onchain,
            id,
            symbol: symbol.try_into()?,
            quantity,
            price_per_share,
            direction,
            timestamp,
        })
    }

    fn from_offchain_row(
        id: i64,
        symbol: String,
        shares: i64,
        direction: &str,
        price_cents: Option<i64>,
        executed_at: Option<chrono::NaiveDateTime>,
    ) -> anyhow::Result<Self> {
        let executed_at =
            executed_at.ok_or_else(|| anyhow::anyhow!("FILLED execution missing executed_at"))?;

        let price_cents =
            price_cents.ok_or_else(|| anyhow::anyhow!("FILLED execution missing price_cents"))?;

        let quantity = Decimal::from(shares);

        let price_per_share = Decimal::from(price_cents)
            .checked_div(Decimal::from(100))
            .ok_or_else(|| anyhow::anyhow!("Division by 100 failed"))?;

        let direction = direction
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid direction: {e}"))?;

        Ok(Self {
            r#type: TradeType::Offchain,
            id,
            symbol: symbol.try_into()?,
            quantity,
            price_per_share,
            direction,
            timestamp: executed_at.and_utc(),
        })
    }

    fn to_db_values(&self, result: &PnlResult) -> anyhow::Result<DbMetricsRow> {
        let trade_type_str = match self.r#type {
            TradeType::Onchain => "ONCHAIN",
            TradeType::Offchain => "OFFCHAIN",
        };

        let direction_str = self.direction.as_str();

        let quantity_f64 = self
            .quantity
            .to_f64()
            .ok_or_else(|| anyhow::anyhow!("Failed to convert quantity to f64"))?;

        let price_per_share_f64 = self
            .price_per_share
            .to_f64()
            .ok_or_else(|| anyhow::anyhow!("Failed to convert price_per_share to f64"))?;

        let realized_pnl_f64 = result
            .realized_pnl
            .map(|p| {
                p.to_f64()
                    .ok_or_else(|| anyhow::anyhow!("Failed to convert realized_pnl to f64"))
            })
            .transpose()?;

        let cumulative_pnl_f64 = result
            .cumulative_pnl
            .to_f64()
            .ok_or_else(|| anyhow::anyhow!("Failed to convert cumulative_pnl to f64"))?;

        let net_position_after_f64 = result
            .net_position_after
            .to_f64()
            .ok_or_else(|| anyhow::anyhow!("Failed to convert net_position_after to f64"))?;

        Ok(DbMetricsRow {
            symbol: self.symbol.as_str().to_string(),
            timestamp: self.timestamp,
            trade_type: trade_type_str.to_string(),
            trade_id: self.id,
            trade_direction: direction_str.to_string(),
            quantity: quantity_f64,
            price_per_share: price_per_share_f64,
            realized_pnl: realized_pnl_f64,
            cumulative_pnl: cumulative_pnl_f64,
            net_position_after: net_position_after_f64,
        })
    }
}

struct DbMetricsRow {
    symbol: String,
    timestamp: DateTime<Utc>,
    trade_type: String,
    trade_id: i64,
    trade_direction: String,
    quantity: f64,
    price_per_share: f64,
    realized_pnl: Option<f64>,
    cumulative_pnl: f64,
    net_position_after: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Checkpoint {
    timestamp: DateTime<Utc>,
    trade_type: TradeType,
    trade_id: i64,
}

async fn load_checkpoint(pool: &SqlitePool) -> anyhow::Result<Option<Checkpoint>> {
    let result = sqlx::query!(
        "SELECT timestamp, trade_type, trade_id
         FROM metrics_pnl
         ORDER BY timestamp DESC, trade_type DESC, trade_id DESC
         LIMIT 1"
    )
    .fetch_optional(pool)
    .await?;

    result
        .map(|row| {
            let trade_type: TradeType = row
                .trade_type
                .parse()
                .map_err(|e: String| anyhow::anyhow!("Failed to parse trade_type: {e}"))?;

            Ok(Checkpoint {
                timestamp: row.timestamp.and_utc(),
                trade_type,
                trade_id: row.trade_id,
            })
        })
        .transpose()
}

async fn load_all_trades(pool: &SqlitePool) -> anyhow::Result<Vec<Trade>> {
    let onchain = sqlx::query!(
        "SELECT
            id,
            symbol,
            amount,
            direction,
            price_usdc,
            created_at
         FROM onchain_trades
         ORDER BY created_at, id"
    )
    .fetch_all(pool)
    .await?;

    let offchain = sqlx::query!(
        "SELECT
            id,
            symbol,
            shares,
            direction,
            price_cents,
            executed_at
         FROM offchain_trades
         WHERE status = 'FILLED'
         ORDER BY executed_at, id"
    )
    .fetch_all(pool)
    .await?;

    let onchain_trades = onchain
        .into_iter()
        .map(|row| {
            Trade::from_onchain_row(
                row.id,
                row.symbol,
                row.amount,
                &row.direction,
                row.price_usdc,
                row.created_at,
            )
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let offchain_trades = offchain
        .into_iter()
        .map(|row| {
            let id = row
                .id
                .ok_or_else(|| anyhow::anyhow!("offchain trade missing id"))?;

            Trade::from_offchain_row(
                id,
                row.symbol,
                row.shares,
                &row.direction,
                row.price_cents,
                row.executed_at,
            )
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let mut trades = onchain_trades;
    trades.extend(offchain_trades);

    trades.sort_by_key(|t| (t.timestamp, t.r#type as u8, t.id));
    Ok(trades)
}

fn rebuild_fifo_state(
    trades: &[Trade],
    checkpoint: Option<Checkpoint>,
) -> anyhow::Result<HashMap<Symbol, FifoInventory>> {
    trades
        .iter()
        .take_while(|t| checkpoint.is_some_and(|cp| t.checkpoint_key() <= cp))
        .try_fold(HashMap::new(), |mut inventories, trade| {
            let inventory = inventories
                .entry(trade.symbol.clone())
                .or_insert_with(FifoInventory::new);

            inventory
                .process_trade(trade.quantity, trade.price_per_share, trade.direction)
                .map_err(|e| anyhow::anyhow!("FIFO processing error: {e}"))?;

            Ok(inventories)
        })
}

async fn persist_metrics_row(pool: &SqlitePool, row: &DbMetricsRow) -> anyhow::Result<()> {
    sqlx::query!(
        "INSERT INTO metrics_pnl (
            symbol,
            timestamp,
            trade_type,
            trade_id,
            trade_direction,
            quantity,
            price_per_share,
            realized_pnl,
            cumulative_pnl,
            net_position_after
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        row.symbol,
        row.timestamp,
        row.trade_type,
        row.trade_id,
        row.trade_direction,
        row.quantity,
        row.price_per_share,
        row.realized_pnl,
        row.cumulative_pnl,
        row.net_position_after,
    )
    .execute(pool)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to insert into metrics_pnl: {e}"))?;

    Ok(())
}

async fn process_and_persist_trade(
    pool: &SqlitePool,
    inventories: &mut HashMap<Symbol, FifoInventory>,
    trade: &Trade,
) -> anyhow::Result<()> {
    let inventory = inventories
        .entry(trade.symbol.clone())
        .or_insert_with(FifoInventory::new);

    let result = inventory
        .process_trade(trade.quantity, trade.price_per_share, trade.direction)
        .map_err(|e: PnlError| anyhow::anyhow!("FIFO processing error: {e}"))?;

    let row = trade.to_db_values(&result)?;
    persist_metrics_row(pool, &row).await
}

pub(crate) async fn process_iteration(pool: &SqlitePool) -> anyhow::Result<usize> {
    let checkpoint = load_checkpoint(pool).await?;

    if checkpoint.is_none() {
        info!("No checkpoint found, processing all historical trades");
    }

    let all_trades = load_all_trades(pool).await?;
    let mut inventories = rebuild_fifo_state(&all_trades, checkpoint)?;

    let new_trades: Vec<_> = all_trades
        .into_iter()
        .filter(|t| checkpoint.is_none_or(|cp| t.checkpoint_key() > cp))
        .collect();

    for trade in &new_trades {
        process_and_persist_trade(pool, &mut inventories, trade).await?;
    }

    Ok(new_trades.len())
}

pub async fn run(env: ReporterEnv) -> anyhow::Result<()> {
    use crate::env::HasSqlite;

    let pool = env.get_sqlite_pool().await?;
    let interval = env.processing_interval();

    info!("Starting P&L reporter");
    sqlx::migrate!().run(&pool).await?;

    info!(
        "Reporter initialized with processing interval: {}s",
        interval.as_secs()
    );

    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                match result {
                    Ok(()) => info!("Shutdown signal received"),
                    Err(e) => error!("Error receiving shutdown signal: {e}"),
                }
                break;
            }
            () = tokio::time::sleep(interval) => {
                match process_iteration(&pool).await {
                    Ok(count) => info!("Processed {count} new trades"),
                    Err(e) => error!("Processing error: {e}"),
                }
            }
        }
    }

    info!("Reporter shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    async fn create_test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn test_load_checkpoint_empty_database() {
        let pool = create_test_pool().await;
        let result = load_checkpoint(&pool).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_rebuild_fifo_state_empty() {
        let trades: Vec<Trade> = vec![];
        let checkpoint = None;
        let inventories = rebuild_fifo_state(&trades, checkpoint).unwrap();
        assert!(inventories.is_empty());
    }

    #[tokio::test]
    async fn test_trade_from_onchain_row() {
        let naive_dt = DateTime::from_timestamp(0, 0).unwrap().naive_utc();

        let trade =
            Trade::from_onchain_row(1, "AAPL".to_string(), 10.0, "BUY", 100.0, Some(naive_dt))
                .unwrap();

        assert_eq!(trade.id, 1);
        assert_eq!(trade.symbol.as_str(), "AAPL");
        assert_eq!(trade.quantity, dec!(10.0));
        assert_eq!(trade.price_per_share, dec!(100.0));
        assert_eq!(trade.direction, Direction::Buy);
    }

    #[tokio::test]
    async fn test_trade_from_offchain_row() {
        let naive_dt = DateTime::from_timestamp(0, 0).unwrap().naive_utc();

        let trade = Trade::from_offchain_row(
            2,
            "AAPL".to_string(),
            5,
            "SELL",
            Some(10500),
            Some(naive_dt),
        )
        .unwrap();

        assert_eq!(trade.id, 2);
        assert_eq!(trade.symbol.as_str(), "AAPL");
        assert_eq!(trade.quantity, dec!(5));
        assert_eq!(trade.price_per_share, dec!(105.00));
        assert_eq!(trade.direction, Direction::Sell);
    }

    #[tokio::test]
    async fn test_process_iteration_no_trades() {
        let pool = create_test_pool().await;
        let count = process_iteration(&pool).await.unwrap();
        assert_eq!(count, 0);
    }

    fn assert_f64_eq(actual: f64, expected: f64) {
        const EPSILON: f64 = 1e-10;
        assert!(
            (actual - expected).abs() < EPSILON,
            "assertion failed: `(left == right)` (left: `{actual}`, right: `{expected}`)"
        );
    }

    fn assert_option_f64_eq(actual: Option<f64>, expected: Option<f64>) {
        match (actual, expected) {
            (Some(a), Some(e)) => assert_f64_eq(a, e),
            (None, None) => (),
            _ => panic!(
                "assertion failed: `(left == right)` (left: `{actual:?}`, right: `{expected:?}`)"
            ),
        }
    }

    async fn insert_onchain_trade(
        pool: &SqlitePool,
        symbol: &str,
        amount: f64,
        price_usdc: f64,
        direction: &str,
        timestamp: DateTime<Utc>,
    ) {
        let tx_hash = format!("0x{:064x}", rand::random::<u64>());
        let log_index = i64::try_from(rand::random::<u64>() % 1000).expect("log_index overflow");
        let naive_timestamp = timestamp.naive_utc();

        sqlx::query!(
            "INSERT INTO onchain_trades (
                tx_hash,
                log_index,
                symbol,
                amount,
                direction,
                price_usdc,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            tx_hash,
            log_index,
            symbol,
            amount,
            direction,
            price_usdc,
            naive_timestamp,
        )
        .execute(pool)
        .await
        .expect("Failed to insert onchain trade");
    }

    async fn insert_offchain_trade(
        pool: &SqlitePool,
        symbol: &str,
        shares: i64,
        direction: &str,
        price_cents: i64,
        timestamp: DateTime<Utc>,
    ) {
        let order_id = format!("ORDER{}", rand::random::<u32>());
        let status = "FILLED";
        let naive_timestamp = timestamp.naive_utc();

        sqlx::query!(
            "INSERT INTO offchain_trades (
                broker_order_id,
                symbol,
                shares,
                direction,
                price_cents,
                status,
                executed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            order_id,
            symbol,
            shares,
            direction,
            price_cents,
            status,
            naive_timestamp,
        )
        .execute(pool)
        .await
        .expect("Failed to insert offchain trade");
    }

    struct PnlMetric {
        realized_pnl: Option<f64>,
        cumulative_pnl: f64,
        net_position_after: f64,
        trade_type: String,
    }

    async fn query_all_pnl_metrics(pool: &SqlitePool, symbol: &str) -> Vec<PnlMetric> {
        let rows = sqlx::query!(
            "SELECT
                trade_type,
                realized_pnl,
                cumulative_pnl,
                net_position_after
            FROM metrics_pnl
            WHERE symbol = ?
            ORDER BY timestamp ASC",
            symbol
        )
        .fetch_all(pool)
        .await
        .expect("Failed to query pnl metrics");

        rows.into_iter()
            .map(|r| PnlMetric {
                trade_type: r.trade_type,
                realized_pnl: r.realized_pnl,
                cumulative_pnl: r.cumulative_pnl,
                net_position_after: r.net_position_after,
            })
            .collect()
    }

    #[tokio::test]
    async fn test_simple_buy_sell_end_to_end() {
        let pool = create_test_pool().await;

        let t1 = DateTime::from_timestamp(1000, 0).expect("Invalid timestamp");
        let t2 = DateTime::from_timestamp(2000, 0).expect("Invalid timestamp");

        insert_onchain_trade(&pool, "AAPL", 100.0, 10.0, "BUY", t1).await;
        insert_onchain_trade(&pool, "AAPL", 100.0, 11.0, "SELL", t2).await;

        let count = process_iteration(&pool)
            .await
            .expect("Failed to process iteration");
        assert_eq!(count, 2);

        let metrics = query_all_pnl_metrics(&pool, "AAPL").await;
        assert_eq!(metrics.len(), 2);

        assert_option_f64_eq(metrics[0].realized_pnl, None);
        assert_f64_eq(metrics[0].net_position_after, 100.0);

        assert_option_f64_eq(metrics[1].realized_pnl, Some(100.0));
        assert_f64_eq(metrics[1].cumulative_pnl, 100.0);
        assert_f64_eq(metrics[1].net_position_after, 0.0);
    }

    #[tokio::test]
    async fn test_multiple_trades_fifo_ordering() {
        let pool = create_test_pool().await;

        let t1 = DateTime::from_timestamp(1000, 0).expect("Invalid timestamp");
        let t2 = DateTime::from_timestamp(2000, 0).expect("Invalid timestamp");
        let t3 = DateTime::from_timestamp(3000, 0).expect("Invalid timestamp");

        insert_onchain_trade(&pool, "AAPL", 100.0, 10.0, "BUY", t1).await;
        insert_onchain_trade(&pool, "AAPL", 50.0, 12.0, "BUY", t2).await;
        insert_onchain_trade(&pool, "AAPL", 80.0, 11.0, "SELL", t3).await;

        process_iteration(&pool)
            .await
            .expect("Failed to process iteration");

        let metrics = query_all_pnl_metrics(&pool, "AAPL").await;
        assert_eq!(metrics.len(), 3);

        assert_option_f64_eq(metrics[2].realized_pnl, Some(80.0));
        assert_f64_eq(metrics[2].cumulative_pnl, 80.0);
        assert_f64_eq(metrics[2].net_position_after, 70.0);
    }

    #[tokio::test]
    async fn test_position_reversal() {
        let pool = create_test_pool().await;

        let t1 = DateTime::from_timestamp(1000, 0).expect("Invalid timestamp");
        let t2 = DateTime::from_timestamp(2000, 0).expect("Invalid timestamp");

        insert_onchain_trade(&pool, "AAPL", 100.0, 10.0, "BUY", t1).await;
        insert_onchain_trade(&pool, "AAPL", 150.0, 11.0, "SELL", t2).await;

        process_iteration(&pool)
            .await
            .expect("Failed to process iteration");

        let metrics = query_all_pnl_metrics(&pool, "AAPL").await;
        assert_eq!(metrics.len(), 2);

        assert_option_f64_eq(metrics[1].realized_pnl, Some(100.0));
        assert_f64_eq(metrics[1].net_position_after, -50.0);
    }

    #[tokio::test]
    async fn test_checkpoint_resume() {
        let pool = create_test_pool().await;

        let t1 = DateTime::from_timestamp(1000, 0).expect("Invalid timestamp");
        let t2 = DateTime::from_timestamp(2000, 0).expect("Invalid timestamp");
        let t3 = DateTime::from_timestamp(3000, 0).expect("Invalid timestamp");

        insert_onchain_trade(&pool, "AAPL", 100.0, 10.0, "BUY", t1).await;
        insert_onchain_trade(&pool, "AAPL", 100.0, 11.0, "SELL", t2).await;

        let count = process_iteration(&pool)
            .await
            .expect("Failed to process iteration");
        assert_eq!(count, 2);

        insert_onchain_trade(&pool, "AAPL", 50.0, 12.0, "BUY", t3).await;

        let count = process_iteration(&pool)
            .await
            .expect("Failed to process iteration");
        assert_eq!(count, 1);

        let metrics = query_all_pnl_metrics(&pool, "AAPL").await;
        assert_eq!(metrics.len(), 3);

        assert_f64_eq(metrics[2].net_position_after, 50.0);
    }

    #[tokio::test]
    async fn test_mixed_onchain_offchain_trades() {
        let pool = create_test_pool().await;

        let t1 = DateTime::from_timestamp(1000, 0).expect("Invalid timestamp");
        let t2 = DateTime::from_timestamp(2000, 0).expect("Invalid timestamp");
        let t3 = DateTime::from_timestamp(3000, 0).expect("Invalid timestamp");

        insert_onchain_trade(&pool, "AAPL", 100.0, 10.0, "BUY", t1).await;
        insert_offchain_trade(&pool, "AAPL", 50, "SELL", 1100, t2).await;
        insert_onchain_trade(&pool, "AAPL", 30.0, 12.0, "SELL", t3).await;

        process_iteration(&pool)
            .await
            .expect("Failed to process iteration");

        let metrics = query_all_pnl_metrics(&pool, "AAPL").await;
        assert_eq!(metrics.len(), 3);

        assert_eq!(metrics[1].trade_type, "OFFCHAIN");
        assert_option_f64_eq(metrics[1].realized_pnl, Some(50.0));

        assert_option_f64_eq(metrics[2].realized_pnl, Some(60.0));
        assert_f64_eq(metrics[2].net_position_after, 20.0);
    }

    #[tokio::test]
    async fn test_multiple_symbols_independent() {
        let pool = create_test_pool().await;

        let t1 = DateTime::from_timestamp(1000, 0).expect("Invalid timestamp");
        let t2 = DateTime::from_timestamp(2000, 0).expect("Invalid timestamp");

        insert_onchain_trade(&pool, "AAPL", 100.0, 10.0, "BUY", t1).await;
        insert_onchain_trade(&pool, "MSFT", 50.0, 200.0, "BUY", t1).await;

        insert_onchain_trade(&pool, "AAPL", 100.0, 12.0, "SELL", t2).await;
        insert_onchain_trade(&pool, "MSFT", 50.0, 210.0, "SELL", t2).await;

        process_iteration(&pool)
            .await
            .expect("Failed to process iteration");

        let aapl_metrics = query_all_pnl_metrics(&pool, "AAPL").await;
        assert_eq!(aapl_metrics.len(), 2);
        assert_f64_eq(aapl_metrics[1].cumulative_pnl, 200.0);

        let msft_metrics = query_all_pnl_metrics(&pool, "MSFT").await;
        assert_eq!(msft_metrics.len(), 2);
        assert_f64_eq(msft_metrics[1].cumulative_pnl, 500.0);
    }

    #[tokio::test]
    async fn test_duplicate_prevention() {
        let pool = create_test_pool().await;

        let t1 = DateTime::from_timestamp(1000, 0).expect("Invalid timestamp");

        insert_onchain_trade(&pool, "AAPL", 100.0, 10.0, "BUY", t1).await;

        process_iteration(&pool)
            .await
            .expect("Failed to process iteration");
        let count = process_iteration(&pool)
            .await
            .expect("Failed to process iteration");

        assert_eq!(count, 0);

        let metrics = query_all_pnl_metrics(&pool, "AAPL").await;
        assert_eq!(metrics.len(), 1);
    }

    #[tokio::test]
    async fn test_requirements_doc_seven_step_example() {
        let pool = create_test_pool().await;

        let timestamps: Vec<_> = (1..=7)
            .map(|i| DateTime::from_timestamp(i * 1000, 0).expect("Invalid timestamp"))
            .collect();

        insert_onchain_trade(&pool, "AAPL", 100.0, 10.0, "BUY", timestamps[0]).await;
        insert_onchain_trade(&pool, "AAPL", 50.0, 12.0, "BUY", timestamps[1]).await;
        insert_onchain_trade(&pool, "AAPL", 80.0, 11.0, "SELL", timestamps[2]).await;
        insert_onchain_trade(&pool, "AAPL", 60.0, 9.5, "SELL", timestamps[3]).await;
        insert_onchain_trade(&pool, "AAPL", 30.0, 12.2, "BUY", timestamps[4]).await;
        insert_onchain_trade(&pool, "AAPL", 70.0, 12.0, "SELL", timestamps[5]).await;
        insert_onchain_trade(&pool, "AAPL", 20.0, 11.5, "BUY", timestamps[6]).await;

        process_iteration(&pool)
            .await
            .expect("Failed to process iteration");

        let metrics = query_all_pnl_metrics(&pool, "AAPL").await;
        assert_eq!(metrics.len(), 7);

        assert_option_f64_eq(metrics[0].realized_pnl, None);
        assert_f64_eq(metrics[0].cumulative_pnl, 0.0);
        assert_f64_eq(metrics[0].net_position_after, 100.0);

        assert_option_f64_eq(metrics[1].realized_pnl, None);
        assert_f64_eq(metrics[1].cumulative_pnl, 0.0);
        assert_f64_eq(metrics[1].net_position_after, 150.0);

        assert_option_f64_eq(metrics[2].realized_pnl, Some(80.0));
        assert_f64_eq(metrics[2].cumulative_pnl, 80.0);
        assert_f64_eq(metrics[2].net_position_after, 70.0);

        assert_option_f64_eq(metrics[3].realized_pnl, Some(-110.0));
        assert_f64_eq(metrics[3].cumulative_pnl, -30.0);
        assert_f64_eq(metrics[3].net_position_after, 10.0);

        assert_option_f64_eq(metrics[4].realized_pnl, None);
        assert_f64_eq(metrics[4].cumulative_pnl, -30.0);
        assert_f64_eq(metrics[4].net_position_after, 40.0);

        assert_option_f64_eq(metrics[5].realized_pnl, Some(-6.0));
        assert_f64_eq(metrics[5].cumulative_pnl, -36.0);
        assert_f64_eq(metrics[5].net_position_after, -30.0);

        assert_option_f64_eq(metrics[6].realized_pnl, Some(10.0));
        assert_f64_eq(metrics[6].cumulative_pnl, -26.0);
        assert_f64_eq(metrics[6].net_position_after, -10.0);
    }

    #[tokio::test]
    async fn test_fractional_onchain_pnl_without_offchain_hedge() {
        let pool = create_test_pool().await;

        let timestamps: Vec<_> = (1..=4)
            .map(|i| DateTime::from_timestamp(i * 1000, 0).expect("Invalid timestamp"))
            .collect();

        insert_onchain_trade(&pool, "AAPL", 0.3, 150.0, "SELL", timestamps[0]).await;
        insert_onchain_trade(&pool, "AAPL", 0.4, 151.0, "SELL", timestamps[1]).await;
        insert_onchain_trade(&pool, "AAPL", 0.5, 149.0, "SELL", timestamps[2]).await;
        insert_onchain_trade(&pool, "AAPL", 0.6, 148.0, "BUY", timestamps[3]).await;

        process_iteration(&pool)
            .await
            .expect("Failed to process iteration");

        let metrics = query_all_pnl_metrics(&pool, "AAPL").await;
        assert_eq!(metrics.len(), 4);

        assert_eq!(metrics[0].trade_type, "ONCHAIN");
        assert_option_f64_eq(metrics[0].realized_pnl, None);
        assert_f64_eq(metrics[0].net_position_after, -0.3);

        assert_eq!(metrics[1].trade_type, "ONCHAIN");
        assert_option_f64_eq(metrics[1].realized_pnl, None);
        assert_f64_eq(metrics[1].net_position_after, -0.7);

        assert_eq!(metrics[2].trade_type, "ONCHAIN");
        assert_option_f64_eq(metrics[2].realized_pnl, None);
        assert_f64_eq(metrics[2].net_position_after, -1.2);

        assert_eq!(metrics[3].trade_type, "ONCHAIN");
        let expected_pnl = 2.0_f64.mul_add(0.3, 3.0 * 0.3);
        assert_option_f64_eq(metrics[3].realized_pnl, Some(expected_pnl));
        assert_f64_eq(metrics[3].cumulative_pnl, expected_pnl);
        assert_f64_eq(metrics[3].net_position_after, -0.6);
    }

    #[tokio::test]
    async fn test_same_timestamp_trades_not_skipped() {
        let pool = create_test_pool().await;

        let same_timestamp = DateTime::from_timestamp(1000, 0).expect("Invalid timestamp");

        insert_onchain_trade(&pool, "AAPL", 100.0, 10.0, "BUY", same_timestamp).await;
        insert_onchain_trade(&pool, "AAPL", 50.0, 11.0, "BUY", same_timestamp).await;

        let count = process_iteration(&pool)
            .await
            .expect("Failed to process first iteration");
        assert_eq!(count, 2, "First iteration should process both trades");

        let metrics = query_all_pnl_metrics(&pool, "AAPL").await;
        assert_eq!(
            metrics.len(),
            2,
            "Should have 2 metrics after first iteration"
        );

        insert_onchain_trade(&pool, "AAPL", 30.0, 12.0, "BUY", same_timestamp).await;

        let count = process_iteration(&pool)
            .await
            .expect("Failed to process second iteration");
        assert_eq!(
            count, 1,
            "Second iteration should process only the new trade with same timestamp"
        );

        let metrics = query_all_pnl_metrics(&pool, "AAPL").await;
        assert_eq!(
            metrics.len(),
            3,
            "Should have 3 metrics total after second iteration"
        );

        assert_f64_eq(metrics[2].net_position_after, 180.0);
    }
}
