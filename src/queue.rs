use alloy::primitives::TxHash;
use alloy::rpc::types::Log;
use chrono::{DateTime, Utc};
use futures_util::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::num::TryFromIntError;
use tracing::{error, info, warn};

use crate::bindings::IOrderBookV6::{ClearV3, TakeOrderV3};
use crate::onchain::trade::TradeEvent;

/// Trait for events that can be enqueued
pub(crate) trait Enqueueable {
    fn to_trade_event(&self) -> TradeEvent;
}

impl Enqueueable for ClearV3 {
    fn to_trade_event(&self) -> TradeEvent {
        TradeEvent::ClearV3(Box::new(self.clone()))
    }
}

impl Enqueueable for TakeOrderV3 {
    fn to_trade_event(&self) -> TradeEvent {
        TradeEvent::TakeOrderV3(Box::new(self.clone()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueuedEvent {
    pub(crate) id: Option<i64>,
    pub(crate) tx_hash: TxHash,
    pub(crate) log_index: u64,
    pub(crate) block_number: u64,
    pub(crate) event: TradeEvent,
    pub(crate) processed: bool,
    pub(crate) created_at: Option<DateTime<Utc>>,
    pub(crate) processed_at: Option<DateTime<Utc>>,
    pub(crate) block_timestamp: Option<DateTime<Utc>>,
}

impl QueuedEvent {
    /// Constructs a [`QueuedEvent`] from a decoded trade event and its
    /// log metadata, without writing to the database. Used by the apalis
    /// event monitor to build job payloads directly.
    pub(crate) fn from_log(event: TradeEvent, log: &Log) -> Result<Self, EventQueueError> {
        let tx_hash = log
            .transaction_hash
            .ok_or(EventQueueError::MissingLogField("transaction_hash"))?;

        let log_index = log
            .log_index
            .ok_or(EventQueueError::MissingLogField("log_index"))?;

        let block_number = log
            .block_number
            .ok_or(EventQueueError::MissingLogField("block_number"))?;

        let block_timestamp = log.block_timestamp.and_then(|ts| {
            let ts_i64 = i64::try_from(ts).ok()?;
            DateTime::from_timestamp(ts_i64, 0)
        });

        Ok(Self {
            id: None,
            tx_hash,
            log_index,
            block_number,
            event,
            processed: false,
            created_at: None,
            processed_at: None,
            block_timestamp,
        })
    }
}

/// Event queue persistence and processing errors.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EventQueueError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Log missing required field: {0}")]
    MissingLogField(&'static str),
    #[cfg(test)]
    #[error("Event ID {0} not found in queue")]
    MissingEventId(i64),
    #[error("Integer conversion error: {0}")]
    IntConversion(#[from] TryFromIntError),
    #[error("Event serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Invalid tx_hash format: {0}")]
    InvalidTxHash(#[from] alloy::hex::FromHexError),
}

async fn enqueue_event(
    pool: &SqlitePool,
    log: &Log,
    event: TradeEvent,
) -> Result<(), EventQueueError> {
    let tx_hash = log
        .transaction_hash
        .ok_or(EventQueueError::MissingLogField("transaction_hash"))?;

    let log_index = log
        .log_index
        .ok_or(EventQueueError::MissingLogField("log_index"))?;

    let log_index_i64 = i64::try_from(log_index)?;

    let block_number = log
        .block_number
        .ok_or(EventQueueError::MissingLogField("block_number"))?;

    let block_number_i64 = i64::try_from(block_number)?;

    let tx_hash_str = format!("{tx_hash:#x}");
    let event_json = serde_json::to_string(&event)?;

    let block_timestamp_naive = log.block_timestamp.and_then(|ts| {
        let Ok(ts_i64) = i64::try_from(ts) else {
            warn!(
                "Block timestamp {ts} exceeds i64::MAX, storing NULL for tx {tx_hash:#x} log_index {log_index}"
            );
            return None;
        };

        DateTime::from_timestamp(ts_i64, 0).map_or_else(
            || {
                warn!(
                    "Invalid block timestamp {ts_i64}, storing NULL for tx {tx_hash:#x} log_index {log_index}"
                );
                None
            },
            |dt| Some(dt.naive_utc()),
        )
    });

    sqlx::query!(
        r#"
        INSERT OR IGNORE INTO event_queue
        (tx_hash, log_index, block_number, event_data, processed, block_timestamp)
        VALUES (?, ?, ?, ?, 0, ?)
        "#,
        tx_hash_str,
        log_index_i64,
        block_number_i64,
        event_json,
        block_timestamp_naive
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Fetches all unprocessed events from `event_queue`, marks them as
/// processed, and returns them ordered by block number and log index.
///
/// Used during startup to drain legacy backfilled/buffered events into
/// the apalis job queue.
pub(crate) async fn drain_unprocessed_events(
    pool: &SqlitePool,
) -> Result<Vec<QueuedEvent>, EventQueueError> {
    let rows = sqlx::query!(
        r#"
        SELECT
            id,
            tx_hash,
            log_index,
            block_number,
            event_data,
            processed,
            created_at,
            processed_at,
            block_timestamp
        FROM event_queue
        WHERE processed = 0
        ORDER BY block_number ASC, log_index ASC
        "#
    )
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(Vec::new());
    }

    sqlx::query!(
        "UPDATE event_queue SET processed = 1, processed_at = CURRENT_TIMESTAMP WHERE processed = 0"
    )
    .execute(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let tx_hash: TxHash = row.tx_hash.parse()?;
            let event: TradeEvent = serde_json::from_str(&row.event_data)?;

            Ok(QueuedEvent {
                id: Some(row.id),
                tx_hash,
                log_index: row.log_index.try_into()?,
                block_number: row.block_number.try_into()?,
                event,
                processed: true,
                created_at: Some(row.created_at.and_utc()),
                processed_at: row.processed_at.map(|dt| dt.and_utc()),
                block_timestamp: row.block_timestamp.map(|dt| dt.and_utc()),
            })
        })
        .collect()
}

#[cfg(test)]
pub(crate) async fn get_next_unprocessed_event(
    pool: &SqlitePool,
) -> Result<Option<QueuedEvent>, EventQueueError> {
    let row = sqlx::query!(
        r#"
        SELECT
            id,
            tx_hash,
            log_index,
            block_number,
            event_data,
            processed,
            created_at,
            processed_at,
            block_timestamp
        FROM event_queue
        WHERE processed = 0
        ORDER BY block_number ASC, log_index ASC
        LIMIT 1
        "#
    )
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let tx_hash: TxHash = row.tx_hash.parse()?;

    let event: TradeEvent = serde_json::from_str(&row.event_data)?;

    Ok(Some(QueuedEvent {
        id: Some(row.id),
        tx_hash,
        log_index: row.log_index.try_into()?,
        block_number: row.block_number.try_into()?,
        event,
        processed: row.processed,
        created_at: Some(row.created_at.and_utc()),
        processed_at: row.processed_at.map(|dt| dt.and_utc()),
        block_timestamp: row.block_timestamp.map(|dt| dt.and_utc()),
    }))
}

#[cfg(test)]
pub(crate) async fn mark_event_processed(
    pool: &SqlitePool,
    event_id: i64,
) -> Result<(), EventQueueError> {
    let result = sqlx::query!(
        r#"
        UPDATE event_queue
        SET processed = 1, processed_at = CURRENT_TIMESTAMP
        WHERE id = ?
        "#,
        event_id
    )
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        return Err(EventQueueError::MissingEventId(event_id));
    }

    Ok(())
}

/// Generic function to enqueue any event that implements Enqueueable
#[tracing::instrument(skip_all, level = tracing::Level::DEBUG)]
pub(crate) async fn enqueue<E: Enqueueable + Sync>(
    pool: &SqlitePool,
    event: &E,
    log: &Log,
) -> Result<(), EventQueueError> {
    let serializable_event = event.to_trade_event();
    enqueue_event(pool, log, serializable_event).await
}

/// Enqueues buffered events that were collected during coordination phase
#[tracing::instrument(skip(pool, event_buffer), fields(buffer_size = event_buffer.len()), level = tracing::Level::INFO)]
pub(crate) async fn enqueue_buffer(
    pool: &sqlx::SqlitePool,
    event_buffer: Vec<(TradeEvent, alloy::rpc::types::Log)>,
) {
    info!(
        "Coordination Phase: Processing {} buffered events from subscription",
        event_buffer.len()
    );

    const CONCURRENT_ENQUEUE_LIMIT: usize = 10;

    stream::iter(event_buffer)
        .map(|(event, log)| async move {
            let result = match &event {
                TradeEvent::ClearV3(clear_event) => enqueue(pool, clear_event.as_ref(), &log).await,
                TradeEvent::TakeOrderV3(take_event) => {
                    enqueue(pool, take_event.as_ref(), &log).await
                }
            };

            if let Err(error) = result {
                let event_type = match event {
                    TradeEvent::ClearV3(_) => "ClearV3",
                    TradeEvent::TakeOrderV3(_) => "TakeOrderV3",
                };
                error!("Failed to enqueue buffered {event_type} event: {error}");
            }
        })
        .buffer_unordered(CONCURRENT_ENQUEUE_LIMIT)
        .collect::<Vec<_>>()
        .await;
}

#[cfg(test)]
pub(crate) async fn count_unprocessed(pool: &SqlitePool) -> Result<i64, EventQueueError> {
    let row = sqlx::query!("SELECT COUNT(*) as count FROM event_queue WHERE processed = 0")
        .fetch_one(pool)
        .await?;

    Ok(row.count)
}

/// Gets the highest processed block number from the event queue
pub(crate) async fn get_max_processed_block(
    pool: &SqlitePool,
) -> Result<Option<u64>, EventQueueError> {
    let row =
        sqlx::query!("SELECT MAX(block_number) as max_block FROM event_queue WHERE processed = 1")
            .fetch_one(pool)
            .await?;

    let Some(block) = row.max_block else {
        return Ok(None);
    };

    let block_u64 = u64::try_from(block)?;

    Ok(Some(block_u64))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, LogData, address, b256};

    use super::*;
    use crate::bindings::IOrderBookV6::{
        ClearConfigV2, ClearV3, OrderV4, TakeOrderConfigV4, TakeOrderV3,
    };
    use crate::test_utils::setup_test_db;

    #[tokio::test]
    async fn test_enqueue_and_process_event() {
        let pool = setup_test_db().await;

        let log = Log {
            inner: alloy::primitives::Log {
                address: address!("1234567890123456789012345678901234567890"),
                data: LogData::default(),
            },
            block_hash: Some(b256!(
                "1111111111111111111111111111111111111111111111111111111111111111"
            )),
            block_number: Some(100),
            block_timestamp: None,
            transaction_hash: Some(b256!(
                "2222222222222222222222222222222222222222222222222222222222222222"
            )),
            transaction_index: Some(1),
            log_index: Some(5),
            removed: false,
        };

        let test_event = TradeEvent::ClearV3(Box::new(ClearV3 {
            sender: log.inner.address,
            alice: OrderV4::default(),
            bob: OrderV4::default(),
            clearConfig: ClearConfigV2::default(),
        }));

        enqueue_event(&pool, &log, test_event.clone())
            .await
            .unwrap();

        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 1);

        let queued_event = get_next_unprocessed_event(&pool).await.unwrap().unwrap();
        assert_eq!(queued_event.tx_hash, log.transaction_hash.unwrap());
        assert_eq!(queued_event.log_index, 5);
        assert_eq!(queued_event.block_number, 100);
        assert!(matches!(queued_event.event, TradeEvent::ClearV3(_)));
        assert!(!queued_event.processed);

        mark_event_processed(&pool, queued_event.id.unwrap())
            .await
            .unwrap();

        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 0);

        let next_event = get_next_unprocessed_event(&pool).await.unwrap();
        assert!(next_event.is_none());
    }

    #[tokio::test]
    async fn test_duplicate_event_handling() {
        let pool = setup_test_db().await;

        let log = Log {
            inner: alloy::primitives::Log {
                address: address!("1234567890123456789012345678901234567890"),
                data: LogData::default(),
            },
            block_hash: Some(b256!(
                "1111111111111111111111111111111111111111111111111111111111111111"
            )),
            block_number: Some(100),
            block_timestamp: None,
            transaction_hash: Some(b256!(
                "2222222222222222222222222222222222222222222222222222222222222222"
            )),
            transaction_index: Some(1),
            log_index: Some(5),
            removed: false,
        };

        let test_event = TradeEvent::TakeOrderV3(Box::new(TakeOrderV3 {
            sender: log.inner.address,
            config: TakeOrderConfigV4::default(),
            input: B256::ZERO,
            output: B256::ZERO,
        }));

        enqueue_event(&pool, &log, test_event.clone())
            .await
            .unwrap();
        enqueue_event(&pool, &log, test_event.clone())
            .await
            .unwrap();

        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_event_ordering() {
        let pool = setup_test_db().await;

        for i in 0..3 {
            let log = Log {
                inner: alloy::primitives::Log {
                    address: address!("1234567890123456789012345678901234567890"),
                    data: LogData::default(),
                },
                block_hash: Some(b256!(
                    "1111111111111111111111111111111111111111111111111111111111111111"
                )),
                block_number: Some(100 + i),
                block_timestamp: None,
                transaction_hash: Some(B256::from([u8::try_from(i).unwrap_or(0); 32])),
                transaction_index: Some(1),
                log_index: Some(i),
                removed: false,
            };

            let test_event = TradeEvent::ClearV3(Box::new(ClearV3 {
                sender: log.inner.address,
                alice: OrderV4::default(),
                bob: OrderV4::default(),
                clearConfig: ClearConfigV2::default(),
            }));
            enqueue_event(&pool, &log, test_event).await.unwrap();
        }

        for i in 0..3 {
            let event = get_next_unprocessed_event(&pool).await.unwrap().unwrap();
            assert_eq!(event.log_index, i);
            mark_event_processed(&pool, event.id.unwrap())
                .await
                .unwrap();
        }

        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_enqueue_buffer_mixed_events() {
        let pool = setup_test_db().await;

        let log1 = Log {
            inner: alloy::primitives::Log {
                address: address!("1234567890123456789012345678901234567890"),
                data: LogData::default(),
            },
            block_hash: Some(b256!(
                "1111111111111111111111111111111111111111111111111111111111111111"
            )),
            block_number: Some(100),
            block_timestamp: None,
            transaction_hash: Some(b256!(
                "2222222222222222222222222222222222222222222222222222222222222222"
            )),
            transaction_index: Some(1),
            log_index: Some(1),
            removed: false,
        };

        let log2 = Log {
            inner: alloy::primitives::Log {
                address: address!("1234567890123456789012345678901234567890"),
                data: LogData::default(),
            },
            block_hash: Some(b256!(
                "3333333333333333333333333333333333333333333333333333333333333333"
            )),
            block_number: Some(101),
            block_timestamp: None,
            transaction_hash: Some(b256!(
                "4444444444444444444444444444444444444444444444444444444444444444"
            )),
            transaction_index: Some(2),
            log_index: Some(2),
            removed: false,
        };

        let clear_event = TradeEvent::ClearV3(Box::new(ClearV3 {
            sender: log1.inner.address,
            alice: OrderV4::default(),
            bob: OrderV4::default(),
            clearConfig: ClearConfigV2::default(),
        }));

        let take_event = TradeEvent::TakeOrderV3(Box::new(TakeOrderV3 {
            sender: log2.inner.address,
            config: TakeOrderConfigV4::default(),
            input: B256::ZERO,
            output: B256::ZERO,
        }));

        let event_buffer = vec![(clear_event, log1), (take_event, log2)];

        enqueue_buffer(&pool, event_buffer).await;

        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 2);

        let first_event = get_next_unprocessed_event(&pool).await.unwrap().unwrap();
        assert!(matches!(first_event.event, TradeEvent::ClearV3(_)));

        mark_event_processed(&pool, first_event.id.unwrap())
            .await
            .unwrap();

        let second_event = get_next_unprocessed_event(&pool).await.unwrap().unwrap();
        assert!(matches!(second_event.event, TradeEvent::TakeOrderV3(_)));
    }

    #[tokio::test]
    async fn test_enqueue_buffer_empty() {
        let pool = setup_test_db().await;
        let empty_buffer = vec![];

        enqueue_buffer(&pool, empty_buffer).await;

        let count = count_unprocessed(&pool).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_get_max_processed_block_empty_queue() {
        let pool = setup_test_db().await;

        let result = get_max_processed_block(&pool).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_get_max_processed_block_only_unprocessed() {
        let pool = setup_test_db().await;

        sqlx::query!(
            r#"
            INSERT INTO event_queue (tx_hash, log_index, block_number, event_data, processed)
            VALUES
                ('0x1111111111111111111111111111111111111111111111111111111111111111', 0, 100, '{}', 0),
                ('0x2222222222222222222222222222222222222222222222222222222222222222', 0, 150, '{}', 0)
            "#
        )
        .execute(&pool)
        .await
        .unwrap();

        let result = get_max_processed_block(&pool).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_get_max_processed_block_only_processed() {
        let pool = setup_test_db().await;

        sqlx::query!(
            r#"
            INSERT INTO event_queue (tx_hash, log_index, block_number, event_data, processed)
            VALUES
                ('0x1111111111111111111111111111111111111111111111111111111111111111', 0, 100, '{}', 1),
                ('0x2222222222222222222222222222222222222222222222222222222222222222', 0, 150, '{}', 1),
                ('0x3333333333333333333333333333333333333333333333333333333333333333', 0, 75, '{}', 1)
            "#
        )
        .execute(&pool)
        .await
        .unwrap();

        let result = get_max_processed_block(&pool).await.unwrap();
        assert_eq!(result, Some(150));
    }

    #[tokio::test]
    async fn test_get_max_processed_block_mixed_states() {
        let pool = setup_test_db().await;

        sqlx::query!(
            r#"
            INSERT INTO event_queue (tx_hash, log_index, block_number, event_data, processed)
            VALUES
                ('0x1111111111111111111111111111111111111111111111111111111111111111', 0, 100, '{}', 1),
                ('0x2222222222222222222222222222222222222222222222222222222222222222', 0, 150, '{}', 1),
                ('0x3333333333333333333333333333333333333333333333333333333333333333', 0, 200, '{}', 0),
                ('0x4444444444444444444444444444444444444444444444444444444444444444', 0, 175, '{}', 0)
            "#
        )
        .execute(&pool)
        .await
        .unwrap();

        let result = get_max_processed_block(&pool).await.unwrap();
        assert_eq!(result, Some(150));
    }

    #[tokio::test]
    async fn test_get_max_processed_block_zero_block() {
        let pool = setup_test_db().await;

        sqlx::query!(
            r#"
            INSERT INTO event_queue (tx_hash, log_index, block_number, event_data, processed)
            VALUES
                ('0x1111111111111111111111111111111111111111111111111111111111111111', 0, 0, '{}', 1),
                ('0x2222222222222222222222222222222222222222222222222222222222222222', 0, 50, '{}', 0)
            "#
        )
        .execute(&pool)
        .await
        .unwrap();

        let result = get_max_processed_block(&pool).await.unwrap();
        assert_eq!(result, Some(0));
    }

    #[tokio::test]
    async fn test_get_max_processed_block_large_numbers() {
        let pool = setup_test_db().await;

        let large_block: i64 = 999_999_999;

        sqlx::query!(
            r#"
            INSERT INTO event_queue (tx_hash, log_index, block_number, event_data, processed)
            VALUES
                ('0x1111111111111111111111111111111111111111111111111111111111111111', 0, ?1, '{}', 1)
            "#,
            large_block
        )
        .execute(&pool)
        .await
        .unwrap();

        let result = get_max_processed_block(&pool).await.unwrap();
        assert_eq!(result, Some(999_999_999));
    }

    fn test_log(tx_hash: B256, block_number: u64, log_index: u64) -> Log {
        Log {
            inner: alloy::primitives::Log {
                address: address!("0x1234567890123456789012345678901234567890"),
                data: LogData::default(),
            },
            block_hash: Some(B256::ZERO),
            block_number: Some(block_number),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: Some(0),
            log_index: Some(log_index),
            removed: false,
        }
    }

    fn test_clear(sender: alloy::primitives::Address) -> TradeEvent {
        TradeEvent::ClearV3(Box::new(ClearV3 {
            sender,
            alice: OrderV4::default(),
            bob: OrderV4::default(),
            clearConfig: ClearConfigV2::default(),
        }))
    }

    #[tokio::test]
    async fn drain_unprocessed_returns_empty_when_no_events() {
        let pool = setup_test_db().await;

        let drained = drain_unprocessed_events(&pool).await.unwrap();

        assert!(drained.is_empty());
    }

    #[tokio::test]
    async fn drain_unprocessed_returns_all_unprocessed_events() {
        let pool = setup_test_db().await;
        let sender = address!("0x1234567890123456789012345678901234567890");

        let log1 = test_log(
            b256!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            100,
            0,
        );
        let log2 = test_log(
            b256!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            101,
            1,
        );

        enqueue_event(&pool, &log1, test_clear(sender))
            .await
            .unwrap();
        enqueue_event(&pool, &log2, test_clear(sender))
            .await
            .unwrap();

        let drained = drain_unprocessed_events(&pool).await.unwrap();

        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].block_number, 100);
        assert_eq!(drained[1].block_number, 101);
    }

    #[tokio::test]
    async fn drain_unprocessed_marks_events_as_processed() {
        let pool = setup_test_db().await;
        let sender = address!("0x1234567890123456789012345678901234567890");

        let log = test_log(
            b256!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            100,
            0,
        );
        enqueue_event(&pool, &log, test_clear(sender))
            .await
            .unwrap();

        assert_eq!(count_unprocessed(&pool).await.unwrap(), 1);

        drain_unprocessed_events(&pool).await.unwrap();

        assert_eq!(count_unprocessed(&pool).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn drain_unprocessed_skips_already_processed_events() {
        let pool = setup_test_db().await;

        sqlx::query!(
            r#"
            INSERT INTO event_queue (tx_hash, log_index, block_number, event_data, processed)
            VALUES
                ('0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 0, 100, '{}', 1),
                ('0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 0, 101, '{}', 0)
            "#
        )
        .execute(&pool)
        .await
        .unwrap();

        let drained = drain_unprocessed_events(&pool).await.unwrap();

        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].block_number, 101);
    }

    #[tokio::test]
    async fn drain_unprocessed_is_idempotent() {
        let pool = setup_test_db().await;
        let sender = address!("0x1234567890123456789012345678901234567890");

        let log = test_log(
            b256!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            100,
            0,
        );
        enqueue_event(&pool, &log, test_clear(sender))
            .await
            .unwrap();

        let first_drain = drain_unprocessed_events(&pool).await.unwrap();
        assert_eq!(first_drain.len(), 1);

        let second_drain = drain_unprocessed_events(&pool).await.unwrap();
        assert!(second_drain.is_empty());
    }

    #[tokio::test]
    async fn drain_unprocessed_preserves_event_data() {
        let pool = setup_test_db().await;
        let sender = address!("0x1234567890123456789012345678901234567890");
        let tx_hash = b256!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        let log = test_log(tx_hash, 42, 7);
        enqueue_event(&pool, &log, test_clear(sender))
            .await
            .unwrap();

        let drained = drain_unprocessed_events(&pool).await.unwrap();

        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].tx_hash, tx_hash);
        assert_eq!(drained[0].block_number, 42);
        assert_eq!(drained[0].log_index, 7);
        assert!(matches!(drained[0].event, TradeEvent::ClearV3(_)));
    }
}
