//! Write side of the daily portfolio snapshot read model: the reactor that
//! maintains the plain `portfolio_snapshot` table from retained
//! `PortfolioSnapshot` capture and historical-mark correction events.
//!
//! The live reactor is forward-only, but the retained event stream remains the
//! source of truth: [`PortfolioSnapshotProjection::rebuild_all`] truncates and
//! replays the read model in one transaction. This gives operators a durable
//! recovery path for a reactor failure and guarantees manual mark corrections
//! survive projection rebuilds.

use rain_math_float::Float;
use sqlx::{Sqlite, SqlitePool, Transaction};
use st0x_event_sorcery::{EntityList, EventSourced, IdempotentReactor, Reactor, deps};
use st0x_finance::{Positive, Symbol};
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
    async fn on_captured_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        id: PortfolioSnapshotId,
        event: PortfolioSnapshotEvent,
    ) -> Result<(), ProjectionError> {
        let PortfolioSnapshotEvent::Captured { captured_at, rows } = event else {
            return Ok(());
        };
        let et_day = id.to_string();
        let captured_at = captured_at.to_rfc3339();

        sqlx::query("DELETE FROM portfolio_snapshot WHERE et_day = ?")
            .bind(&et_day)
            .execute(&mut **transaction)
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
            .execute(&mut **transaction)
            .await?;
        }

        Ok(())
    }

    async fn on_equity_mark_set_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        id: PortfolioSnapshotId,
        symbol: Symbol,
        usd_mark: Positive<Float>,
        observed_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), ProjectionError> {
        let et_day = id.to_string();
        let mark = format_float(&usd_mark.inner())?;
        let result = sqlx::query(
            "UPDATE portfolio_snapshot SET usd_mark = ?, mark_captured_at = ? \
             WHERE et_day = ? AND asset = ?",
        )
        .bind(mark)
        .bind(observed_at.to_rfc3339())
        .bind(&et_day)
        .bind(symbol.to_string())
        .execute(&mut **transaction)
        .await?;

        if result.rows_affected() == 0 {
            return Err(ProjectionError::EquityRowsMissing { et_day, symbol });
        }

        Ok(())
    }

    async fn on_event(
        &self,
        id: PortfolioSnapshotId,
        event: PortfolioSnapshotEvent,
    ) -> Result<(), ProjectionError> {
        let mut transaction = self.pool.begin().await?;
        Self::apply_event_tx(&mut transaction, id, event).await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn apply_event_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        id: PortfolioSnapshotId,
        event: PortfolioSnapshotEvent,
    ) -> Result<(), ProjectionError> {
        match event {
            captured @ PortfolioSnapshotEvent::Captured { .. } => {
                Self::on_captured_tx(transaction, id, captured).await
            }
            PortfolioSnapshotEvent::EquityMarkSet {
                symbol,
                usd_mark,
                observed_at,
                ..
            } => Self::on_equity_mark_set_tx(transaction, id, symbol, usd_mark, observed_at).await,
            PortfolioSnapshotEvent::UnusableMarkAlerted { .. }
            | PortfolioSnapshotEvent::UnusableMarkDetected { .. } => Ok(()),
        }
    }

    /// Rebuilds the complete read model from retained aggregate events in one
    /// transaction, so a parse or write failure leaves the prior table intact.
    pub(crate) async fn rebuild_all(&self) -> Result<u64, ProjectionError> {
        let mut transaction = self.pool.begin().await?;
        let events: Vec<(String, String)> = sqlx::query_as(
            "SELECT aggregate_id, payload FROM events WHERE aggregate_type = ? \
             ORDER BY aggregate_id, sequence ASC",
        )
        .bind(PortfolioSnapshot::AGGREGATE_TYPE)
        .fetch_all(&mut *transaction)
        .await?;

        sqlx::query("DELETE FROM portfolio_snapshot")
            .execute(&mut *transaction)
            .await?;

        for (aggregate_id, payload) in &events {
            let id = aggregate_id.parse()?;
            let event = serde_json::from_str(payload)?;
            Self::apply_event_tx(&mut transaction, id, event).await?;
        }

        transaction.commit().await?;
        u64::try_from(events.len()).map_err(|source| ProjectionError::EventCount { source })
    }
}

#[derive(Debug, Error)]
pub(crate) enum ProjectionError {
    #[error("portfolio-snapshot read-model write failed")]
    Database(#[from] sqlx::Error),
    #[error("failed to format a portfolio snapshot balance for persistence")]
    Float(#[from] rain_math_float::FloatError),
    #[error("failed to deserialize a portfolio-snapshot event")]
    Event(#[from] serde_json::Error),
    #[error("failed to parse portfolio-snapshot aggregate id")]
    AggregateId(#[from] super::ParsePortfolioSnapshotIdError),
    #[error("portfolio-snapshot event count exceeded u64")]
    EventCount {
        #[source]
        source: std::num::TryFromIntError,
    },
    #[error("portfolio-snapshot read model has no rows for {symbol} on {et_day}")]
    EquityRowsMissing { et_day: String, symbol: Symbol },
}

#[async_trait::async_trait]
impl Reactor for PortfolioSnapshotProjection {
    type Error = ProjectionError;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|id, event| async move { self.on_event(id, event).await })
            .exhaustive()
            .await
    }
}

impl IdempotentReactor for PortfolioSnapshotProjection {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};
    use sqlx::Row;
    use st0x_event_sorcery::{ReactorHarness, StoreBuilder};
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

    fn equity_row(location: PortfolioLocation) -> PortfolioBalanceRowWithMark {
        PortfolioBalanceRowWithMark {
            row: PortfolioBalanceRow {
                location,
                asset: PortfolioAsset::Equity(Symbol::new("AAPL").unwrap()),
                available: float!(10),
                inflight: float!(0),
            },
            usd_mark: None,
            mark_captured_at: None,
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

    #[tokio::test]
    async fn equity_mark_set_updates_every_location_and_latest_event_wins() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(PortfolioSnapshotProjection::new(pool.clone()));
        let id = PortfolioSnapshotId(chrono::NaiveDate::from_ymd_opt(2026, 7, 20).unwrap());
        harness
            .receive::<PortfolioSnapshot>(
                id,
                PortfolioSnapshotEvent::Captured {
                    captured_at: captured_at(),
                    rows: vec![
                        equity_row(PortfolioLocation::MarketMaking),
                        equity_row(PortfolioLocation::Hedging),
                    ],
                },
            )
            .await
            .unwrap();

        for mark in [float!(150), float!(151)] {
            harness
                .receive::<PortfolioSnapshot>(
                    id,
                    PortfolioSnapshotEvent::EquityMarkSet {
                        symbol: Symbol::new("AAPL").unwrap(),
                        usd_mark: Positive::new(mark).unwrap(),
                        observed_at: captured_at() - chrono::Duration::days(3),
                        source: "Nasdaq historical close".to_owned(),
                        reason: "operator correction".to_owned(),
                        corrected_at: captured_at() + chrono::Duration::days(2),
                    },
                )
                .await
                .unwrap();
        }

        let marks: Vec<String> = sqlx::query_scalar(
            "SELECT usd_mark FROM portfolio_snapshot \
             WHERE et_day = '2026-07-20' AND asset = 'AAPL' ORDER BY location",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(marks, vec!["151", "151"]);
    }

    #[tokio::test]
    async fn rebuild_replays_capture_and_latest_mark_correction() {
        let pool = setup_test_db().await;
        let projection = Arc::new(PortfolioSnapshotProjection::new(pool.clone()));
        let store = StoreBuilder::<PortfolioSnapshot>::new(pool.clone())
            .with(projection.clone())
            .build(())
            .await
            .unwrap();
        let id = PortfolioSnapshotId(chrono::NaiveDate::from_ymd_opt(2026, 7, 20).unwrap());

        store
            .send(
                &id,
                super::super::PortfolioSnapshotCommand::Capture {
                    captured_at: captured_at(),
                    rows: vec![
                        equity_row(PortfolioLocation::MarketMaking),
                        equity_row(PortfolioLocation::Hedging),
                    ],
                },
            )
            .await
            .unwrap();
        for mark in [float!(150), float!(151)] {
            store
                .send(
                    &id,
                    super::super::PortfolioSnapshotCommand::SetEquityMark {
                        symbol: Symbol::new("AAPL").unwrap(),
                        usd_mark: Positive::new(mark).unwrap(),
                        observed_at: captured_at() - chrono::Duration::days(3),
                        source: "Nasdaq historical close".to_owned(),
                        reason: "operator correction".to_owned(),
                        corrected_at: captured_at() + chrono::Duration::days(2),
                    },
                )
                .await
                .unwrap();
        }

        sqlx::query("DELETE FROM portfolio_snapshot")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(projection.rebuild_all().await.unwrap(), 3);

        let marks: Vec<String> = sqlx::query_scalar(
            "SELECT usd_mark FROM portfolio_snapshot \
             WHERE et_day = '2026-07-20' AND asset = 'AAPL' ORDER BY location",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(marks, vec!["151", "151"]);
    }
}
