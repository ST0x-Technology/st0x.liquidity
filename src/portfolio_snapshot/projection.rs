//! Write side of the daily portfolio snapshot read model: the reactor that
//! maintains the plain `portfolio_snapshot` table from retained
//! `PortfolioSnapshot::Captured` events.
//!
//! **Durability model: forward-only, best-effort -- matching
//! [`crate::performance::HedgeLatencyProjection`] exactly, NOT
//! `QueryReplay::replay_all()`.** `QueryReplay` targets cqrs-es `Query`/`View`
//! types; the bridge that would let a `Reactor` participate in that machinery
//! is crate-private inside `event-sorcery`, and startup auto-catch-up is
//! wired only for `Materialized = Table` entities. A crash between the
//! `Captured` event committing and this reactor's write can therefore drop
//! that day's read-model row. Because the `PortfolioSnapshot` aggregate
//! permanently rejects any further `Capture` for the same `et_day`, there is
//! currently no automated repair path for a dropped row -- accepted as the
//! same best-effort risk `HedgeLatencyProjection` already carries in this
//! codebase, not a regression introduced here.

use sqlx::SqlitePool;
use st0x_event_sorcery::{EntityList, Reactor, deps};
use st0x_float_serde::format_float;
use thiserror::Error;

use super::{PortfolioSnapshot, PortfolioSnapshotEvent, PortfolioSnapshotId};

/// Reactor maintaining the `portfolio_snapshot` read model from live
/// `PortfolioSnapshot::Captured` events.
pub(crate) struct PortfolioSnapshotProjection {
    pool: SqlitePool,
}

deps!(PortfolioSnapshotProjection, [PortfolioSnapshot]);

impl PortfolioSnapshotProjection {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Writes every row of a `Captured` event for `id`'s ET day. Idempotent
    /// under redelivery: the day's existing rows are deleted and reinserted
    /// inside a single transaction, defense-in-depth beyond the aggregate's
    /// own command-level idempotency (a second `Capture` for the same day is
    /// rejected before it ever reaches this reactor).
    async fn on_captured(
        &self,
        id: PortfolioSnapshotId,
        event: PortfolioSnapshotEvent,
    ) -> Result<(), ProjectionError> {
        let PortfolioSnapshotEvent::Captured { captured_at, rows } = event;
        let et_day = id.to_string();
        let captured_at = captured_at.to_rfc3339();

        let mut transaction = self.pool.begin().await?;

        sqlx::query("DELETE FROM portfolio_snapshot WHERE et_day = ?")
            .bind(&et_day)
            .execute(&mut *transaction)
            .await?;

        for row in &rows {
            let available_balance = format_float(&row.row.available)?;
            let inflight_balance = format_float(&row.row.inflight)?;
            let usd_mark = row.usd_mark.as_ref().map(format_float).transpose()?;
            let mark_captured_at = row.mark_captured_at.map(|timestamp| timestamp.to_rfc3339());

            sqlx::query(
                "INSERT INTO portfolio_snapshot \
                 (et_day, captured_at, location, asset, available_balance, \
                  inflight_balance, usd_mark, mark_captured_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&et_day)
            .bind(&captured_at)
            .bind(row.row.location.to_string())
            .bind(row.row.asset.to_string())
            .bind(available_balance)
            .bind(inflight_balance)
            .bind(usd_mark)
            .bind(mark_captured_at)
            .execute(&mut *transaction)
            .await?;
        }

        transaction.commit().await?;

        Ok(())
    }
}

#[derive(Debug, Error)]
pub(crate) enum ProjectionError {
    #[error("portfolio-snapshot read-model write failed")]
    Database(#[from] sqlx::Error),
    #[error("failed to format a portfolio snapshot balance for persistence")]
    Float(#[from] rain_math_float::FloatError),
}

#[async_trait::async_trait]
impl Reactor for PortfolioSnapshotProjection {
    type Error = ProjectionError;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|id, event| async move { self.on_captured(id, event).await })
            .exhaustive()
            .await
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use sqlx::Row;
    use st0x_event_sorcery::ReactorHarness;
    use st0x_float_macro::float;

    use crate::inventory::{PortfolioAsset, PortfolioBalanceRow, PortfolioLocation};
    use crate::portfolio_snapshot::{PortfolioBalanceRowWithMark, PortfolioSnapshotId};
    use crate::test_utils::setup_test_db;

    use super::*;

    fn captured_at() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 20, 4, 5, 0).unwrap()
    }

    fn row(
        location: PortfolioLocation,
        available: i64,
        usd_mark: Option<i64>,
    ) -> PortfolioBalanceRowWithMark {
        PortfolioBalanceRowWithMark {
            row: PortfolioBalanceRow {
                location,
                asset: PortfolioAsset::Usdc,
                available: float!(&available.to_string()),
                inflight: float!(0),
            },
            usd_mark: usd_mark.map(|mark| float!(&mark.to_string())),
            mark_captured_at: usd_mark.map(|_| captured_at()),
        }
    }

    async fn row_count(pool: &SqlitePool, et_day: &str) -> i64 {
        sqlx::query("SELECT COUNT(*) AS count FROM portfolio_snapshot WHERE et_day = ?")
            .bind(et_day)
            .fetch_one(pool)
            .await
            .unwrap()
            .get("count")
    }

    #[tokio::test]
    async fn captured_event_with_n_rows_produces_n_rows_for_the_aggregate_id_day() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(PortfolioSnapshotProjection::new(pool.clone()));
        let id = PortfolioSnapshotId(chrono::NaiveDate::from_ymd_opt(2026, 7, 20).unwrap());

        harness
            .receive::<PortfolioSnapshot>(
                id,
                PortfolioSnapshotEvent::Captured {
                    captured_at: captured_at(),
                    rows: vec![
                        row(PortfolioLocation::MarketMaking, 100, Some(1)),
                        row(PortfolioLocation::Hedging, 200, Some(1)),
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(row_count(&pool, "2026-07-20").await, 2);
    }

    #[tokio::test]
    async fn null_mark_row_persists_sql_null_not_a_placeholder_string() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(PortfolioSnapshotProjection::new(pool.clone()));
        let id = PortfolioSnapshotId(chrono::NaiveDate::from_ymd_opt(2026, 7, 20).unwrap());

        harness
            .receive::<PortfolioSnapshot>(
                id,
                PortfolioSnapshotEvent::Captured {
                    captured_at: captured_at(),
                    rows: vec![row(PortfolioLocation::MarketMaking, 100, None)],
                },
            )
            .await
            .unwrap();

        let stored: (Option<String>, Option<String>) = sqlx::query_as(
            "SELECT usd_mark, mark_captured_at FROM portfolio_snapshot WHERE et_day = ?",
        )
        .bind("2026-07-20")
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(stored, (None, None));
    }

    #[tokio::test]
    async fn redelivered_captured_event_leaves_exactly_one_row_set() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(PortfolioSnapshotProjection::new(pool.clone()));
        let id = PortfolioSnapshotId(chrono::NaiveDate::from_ymd_opt(2026, 7, 20).unwrap());
        let event = PortfolioSnapshotEvent::Captured {
            captured_at: captured_at(),
            rows: vec![
                row(PortfolioLocation::MarketMaking, 100, Some(1)),
                row(PortfolioLocation::Hedging, 200, Some(1)),
            ],
        };

        harness
            .receive::<PortfolioSnapshot>(id, event.clone())
            .await
            .unwrap();
        harness
            .receive::<PortfolioSnapshot>(id, event)
            .await
            .unwrap();

        assert_eq!(row_count(&pool, "2026-07-20").await, 2);
    }
}
