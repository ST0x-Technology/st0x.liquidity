use async_trait::async_trait;
use cqrs_es::Aggregate;
use cqrs_es::persist::{
    PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent, SerializedSnapshot,
};
use serde_json::Value;
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqlitePool};

#[derive(Debug, thiserror::Error)]
enum SqliteEventRepositoryError {
    #[error("optimistic lock error: aggregate has been modified concurrently")]
    OptimisticLock,
    #[error("snapshot update was requested without persisted events")]
    EmptySnapshotUpdate,
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Integer(#[from] std::num::TryFromIntError),
}

impl From<SqliteEventRepositoryError> for PersistenceError {
    fn from(error: SqliteEventRepositoryError) -> Self {
        use SqliteEventRepositoryError::*;
        match error {
            OptimisticLock => Self::OptimisticLockError,
            EmptySnapshotUpdate => Self::UnknownError(Box::new(error)),
            Sql(source) => Self::ConnectionError(Box::new(source)),
            Json(source) => Self::DeserializationError(Box::new(source)),
            Integer(source) => Self::UnknownError(Box::new(source)),
        }
    }
}

pub(crate) struct SqliteEventRepository {
    pool: SqlitePool,
    stream_channel_size: usize,
}

impl SqliteEventRepository {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            stream_channel_size: 1000,
        }
    }

    async fn load_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, SqliteEventRepositoryError> {
        let rows = sqlx::query(
            "SELECT aggregate_type, aggregate_id, sequence, event_type, \
             event_version, payload, metadata \
             FROM events \
             WHERE aggregate_type = ?1 AND aggregate_id = ?2 \
             ORDER BY sequence",
        )
        .bind(A::aggregate_type())
        .bind(aggregate_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter().map(row_to_serialized_event).collect()
    }

    async fn load_events_since<A: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, SqliteEventRepositoryError> {
        let last_sequence = i64::try_from(last_sequence)?;
        let rows = sqlx::query(
            "SELECT aggregate_type, aggregate_id, sequence, event_type, \
             event_version, payload, metadata \
             FROM events \
             WHERE aggregate_type = ?1 AND aggregate_id = ?2 AND sequence > ?3 \
             ORDER BY sequence",
        )
        .bind(A::aggregate_type())
        .bind(aggregate_id)
        .bind(last_sequence)
        .fetch_all(&self.pool)
        .await?;

        rows.iter().map(row_to_serialized_event).collect()
    }

    async fn load_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, SqliteEventRepositoryError> {
        let row = sqlx::query(
            "SELECT aggregate_type, aggregate_id, last_sequence, snapshot_version, payload, timestamp \
             FROM snapshots \
             WHERE aggregate_type = ?1 AND aggregate_id = ?2",
        )
        .bind(A::aggregate_type())
        .bind(aggregate_id)
        .fetch_optional(&self.pool)
        .await?;

        row.as_ref().map(row_to_serialized_snapshot).transpose()
    }

    async fn persist_events<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), SqliteEventRepositoryError> {
        let mut tx = self.pool.begin().await?;

        for event in events {
            let sequence = i64::try_from(event.sequence)?;
            let result = sqlx::query(
                "INSERT INTO events \
                 (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )
            .bind(&event.aggregate_type)
            .bind(&event.aggregate_id)
            .bind(sequence)
            .bind(&event.event_type)
            .bind(&event.event_version)
            .bind(&event.payload)
            .bind(&event.metadata)
            .execute(&mut *tx)
            .await;

            if let Err(error) = result {
                if is_optimistic_lock_error(&error) {
                    return Err(SqliteEventRepositoryError::OptimisticLock);
                }
                return Err(SqliteEventRepositoryError::Sql(error));
            }
        }

        if let Some((aggregate_id, aggregate, snapshot_version)) = snapshot_update {
            let last_sequence = events
                .last()
                .map(|event| event.sequence)
                .ok_or(SqliteEventRepositoryError::EmptySnapshotUpdate)?;
            let last_sequence = i64::try_from(last_sequence)?;
            let snapshot_version = i64::try_from(snapshot_version)?;

            sqlx::query(
                "INSERT OR REPLACE INTO snapshots \
                 (aggregate_type, aggregate_id, last_sequence, snapshot_version, payload, timestamp) \
                 VALUES (?1, ?2, ?3, ?4, ?5, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
            )
            .bind(A::aggregate_type())
            .bind(aggregate_id)
            .bind(last_sequence)
            .bind(snapshot_version)
            .bind(aggregate)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Stream events from the `events` table for replay.
    ///
    /// **Compaction caveat:** This only queries the `events` table.
    /// Fully-compacted aggregates (all events deleted, only a
    /// snapshot remains) will not appear in the stream.
    /// [`load_aggregate`](cqrs_es::persist::PersistedEventStore::load_aggregate)
    /// handles this correctly via snapshot loading, but
    /// stream-based replay will miss snapshot-only entities.
    fn stream_events_impl<A: Aggregate>(&self, aggregate_id: Option<&str>) -> ReplayStream {
        let (mut feed, stream) = ReplayStream::new(self.stream_channel_size);
        let pool = self.pool.clone();
        let aggregate_type = A::aggregate_type();
        let aggregate_id = aggregate_id.map(String::from);

        tokio::spawn(async move {
            let rows = match &aggregate_id {
                Some(aggregate_id) => {
                    sqlx::query(
                        "SELECT aggregate_type, aggregate_id, sequence, event_type, \
                         event_version, payload, metadata \
                         FROM events \
                         WHERE aggregate_type = ?1 AND aggregate_id = ?2 \
                         ORDER BY sequence",
                    )
                    .bind(&aggregate_type)
                    .bind(aggregate_id)
                    .fetch_all(&pool)
                    .await
                }
                None => {
                    sqlx::query(
                        "SELECT aggregate_type, aggregate_id, sequence, event_type, \
                         event_version, payload, metadata \
                         FROM events \
                         WHERE aggregate_type = ?1 \
                         ORDER BY sequence",
                    )
                    .bind(&aggregate_type)
                    .fetch_all(&pool)
                    .await
                }
            };

            let rows = match rows {
                Ok(rows) => rows,
                Err(error) => {
                    let _ = feed
                        .push(Err(PersistenceError::ConnectionError(Box::new(error))))
                        .await;
                    return;
                }
            };

            for row in &rows {
                let event = match row_to_serialized_event(row) {
                    Ok(event) => event,
                    Err(error) => {
                        let _ = feed.push(Err(PersistenceError::from(error))).await;
                        return;
                    }
                };

                if feed.push(Ok(event)).await.is_err() {
                    return;
                }
            }
        });

        stream
    }
}

#[async_trait]
impl PersistedEventRepository for SqliteEventRepository {
    async fn get_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        Ok(self.load_events::<A>(aggregate_id).await?)
    }

    async fn get_last_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        Ok(self
            .load_events_since::<A>(aggregate_id, last_sequence)
            .await?)
    }

    async fn get_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        Ok(self.load_snapshot::<A>(aggregate_id).await?)
    }

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        Ok(self.persist_events::<A>(events, snapshot_update).await?)
    }

    async fn stream_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<ReplayStream, PersistenceError> {
        Ok(self.stream_events_impl::<A>(Some(aggregate_id)))
    }

    async fn stream_all_events<A: Aggregate>(&self) -> Result<ReplayStream, PersistenceError> {
        Ok(self.stream_events_impl::<A>(None))
    }
}

fn row_to_serialized_event(row: &SqliteRow) -> Result<SerializedEvent, SqliteEventRepositoryError> {
    let sequence: i64 = row.try_get("sequence")?;
    let payload: String = row.try_get("payload")?;
    let metadata: String = row.try_get("metadata")?;

    Ok(SerializedEvent {
        aggregate_type: row.try_get("aggregate_type")?,
        aggregate_id: row.try_get("aggregate_id")?,
        sequence: usize::try_from(sequence)?,
        event_type: row.try_get("event_type")?,
        event_version: row.try_get("event_version")?,
        payload: serde_json::from_str(&payload)?,
        metadata: serde_json::from_str(&metadata)?,
    })
}

fn row_to_serialized_snapshot(
    row: &SqliteRow,
) -> Result<SerializedSnapshot, SqliteEventRepositoryError> {
    let current_sequence: i64 = row.try_get("last_sequence")?;
    let current_snapshot: i64 = row.try_get("snapshot_version")?;
    let payload: String = row.try_get("payload")?;

    Ok(SerializedSnapshot {
        aggregate_id: row.try_get("aggregate_id")?,
        aggregate: serde_json::from_str(&payload)?,
        current_sequence: usize::try_from(current_sequence)?,
        current_snapshot: usize::try_from(current_snapshot)?,
    })
}

fn is_optimistic_lock_error(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Database(database_error) => {
            database_error.is_unique_violation()
                || database_error
                    .message()
                    .contains("UNIQUE constraint failed")
        }
        _ => false,
    }
}
