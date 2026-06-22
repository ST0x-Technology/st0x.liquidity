//! Write side of the hedge-latency read model: the reactor that maintains the
//! append-only tables from live `Position` and `OffchainOrder` events.
//!
//! [`HedgeLatencyProjection`] subscribes to both event streams and writes the
//! `hedge_fill`, `hedge_cycle`, `hedge_submission`, and `hedge_attribution_reset`
//! tables. It holds NO persisted mutable attribution state -- each placement
//! snapshots the current uncovered pool (recomputed by [`super::uncovered_fills`])
//! as its covered batch. See the parent module doc for the forward-only and
//! attribution semantics.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use st0x_event_sorcery::{EntityList, Reactor, deps};
use thiserror::Error;
use tracing::{debug, warn};

use st0x_execution::Symbol;

use crate::offchain::order::{OffchainOrder, OffchainOrderEvent, OffchainOrderId};
use crate::position::{Position, PositionEvent, TradeId};

use super::{PerformanceError, covered_fills, uncovered_fills};

/// Reactor maintaining the hedge-latency read model from live events.
pub(crate) struct HedgeLatencyProjection {
    pool: SqlitePool,
}

deps!(HedgeLatencyProjection, [Position, OffchainOrder]);

impl HedgeLatencyProjection {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Dispatches Position events to per-variant handlers.
    async fn on_position(
        &self,
        symbol: Symbol,
        event: PositionEvent,
    ) -> Result<(), ProjectionError> {
        match event {
            // Dedup bookkeeping only (ADR 0010): no performance signal.
            PositionEvent::Initialized { .. }
            | PositionEvent::ThresholdUpdated { .. }
            | PositionEvent::OnChainFillApplied { .. }
            | PositionEvent::OnChainFillSettled { .. } => Ok(()),
            PositionEvent::OnChainOrderFilled {
                trade_id,
                block_timestamp,
                seen_at,
                ..
            } => {
                self.on_chain_fill(&symbol, &trade_id, block_timestamp, seen_at)
                    .await
            }
            PositionEvent::OffChainOrderPlaced {
                offchain_order_id,
                placed_at,
                ..
            } => {
                self.offchain_order_placed(&symbol, offchain_order_id, placed_at)
                    .await
            }
            PositionEvent::OffChainOrderFilled {
                offchain_order_id,
                broker_timestamp,
                ..
            } => {
                self.offchain_order_filled(offchain_order_id, broker_timestamp)
                    .await
            }
            PositionEvent::OffChainOrderFailed {
                offchain_order_id,
                failed_at,
                ..
            } => {
                self.offchain_order_failed(offchain_order_id, failed_at)
                    .await
            }
            // A manual adjustment means accumulated fills no longer drive
            // hedging decisions; attributing them to a later hedge would
            // overstate its exposure window. Record the reset boundary so the
            // read path drops every fill and cycle at or before adjusted_at when
            // recomputing the uncovered pool.
            PositionEvent::ManualPositionAdjusted { adjusted_at, .. } => {
                self.manual_position_adjusted(&symbol, adjusted_at).await
            }
        }
    }

    /// Records a manual-adjustment reset boundary in `hedge_attribution_reset`.
    /// The read path drops every fill (`seen_at`) and cycle (`placed_at`) at or
    /// before `adjusted_at`, so accumulated fills can no longer be attributed to
    /// a later hedge and in-flight batches cannot resurface.
    async fn manual_position_adjusted(
        &self,
        symbol: &Symbol,
        adjusted_at: DateTime<Utc>,
    ) -> Result<(), ProjectionError> {
        sqlx::query("INSERT INTO hedge_attribution_reset (symbol, adjusted_at) VALUES (?, ?)")
            .bind(symbol.to_string())
            .bind(adjusted_at.to_rfc3339())
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Records a new onchain fill in `hedge_fill`, keyed by the originating
    /// fill's `(tx_hash, log_index)`. `ON CONFLICT(tx_hash, log_index) DO
    /// NOTHING` makes redelivery idempotent: a replayed Position event cannot
    /// insert a duplicate row, so fill counts and the uncovered pool (recomputed
    /// from this table on read) stay accurate.
    async fn on_chain_fill(
        &self,
        symbol: &Symbol,
        trade_id: &TradeId,
        block_timestamp: DateTime<Utc>,
        seen_at: DateTime<Utc>,
    ) -> Result<(), ProjectionError> {
        let log_index = i64::try_from(trade_id.log_index)?;
        let updated = sqlx::query(
            "INSERT INTO hedge_fill (symbol, tx_hash, log_index, block_timestamp, seen_at) \
             VALUES (?, ?, ?, ?, ?) \
             ON CONFLICT(tx_hash, log_index) DO NOTHING",
        )
        .bind(symbol.to_string())
        .bind(trade_id.tx_hash.to_string())
        .bind(log_index)
        .bind(block_timestamp.to_rfc3339())
        .bind(seen_at.to_rfc3339())
        .execute(&self.pool)
        .await?;

        if updated.rows_affected() == 0 {
            debug!(%symbol, %trade_id, "Duplicate onchain fill event, keeping first observation");
        }

        Ok(())
    }

    /// Records a new hedge cycle in `hedge_cycle`, snapshotting the symbol's
    /// current uncovered fill set ([`super::uncovered_fills`]) as the covered
    /// batch. `ON CONFLICT(offchain_order_id) DO NOTHING` makes redelivery
    /// idempotent: the covered batch is derived from durable tables, not mutated,
    /// so a duplicate placement leaves attribution untouched.
    async fn offchain_order_placed(
        &self,
        symbol: &Symbol,
        offchain_order_id: OffchainOrderId,
        placed_at: DateTime<Utc>,
    ) -> Result<(), ProjectionError> {
        // Snapshot the current uncovered set as this placement's covered batch.
        // The per-aggregate serial-delivery invariant means no concurrent writer
        // mutates hedge_fill or hedge_attribution_reset between this read and the
        // INSERT, and ON CONFLICT DO NOTHING keeps redelivery idempotent.
        let batch = uncovered_fills(&self.pool, symbol).await?;
        let covered = covered_fills(&batch);

        let (earliest_block_ts, latest_seen_at) = covered.as_ref().map_or((None, None), |cov| {
            (
                Some(cov.earliest_block_timestamp.to_rfc3339()),
                Some(cov.latest_seen_at.to_rfc3339()),
            )
        });

        let count = i64::try_from(batch.len())?;
        sqlx::query(
            "INSERT INTO hedge_cycle \
             (offchain_order_id, symbol, placed_at, covered_count, \
              covered_earliest_block_timestamp, covered_latest_seen_at) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT(offchain_order_id) DO NOTHING",
        )
        .bind(offchain_order_id.to_string())
        .bind(symbol.to_string())
        .bind(placed_at.to_rfc3339())
        .bind(count)
        .bind(earliest_block_ts)
        .bind(latest_seen_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Stamps `filled_at` on the cycle. The broker's own fill time is
    /// authoritative over the OffchainOrder aggregate's reconciliation time,
    /// which would overstate execution latency by polling and queue delay.
    async fn offchain_order_filled(
        &self,
        offchain_order_id: OffchainOrderId,
        broker_timestamp: DateTime<Utc>,
    ) -> Result<(), ProjectionError> {
        let id_str = offchain_order_id.to_string();
        let updated = sqlx::query(
            "UPDATE hedge_cycle SET filled_at = ? \
             WHERE offchain_order_id = ? AND filled_at IS NULL",
        )
        .bind(broker_timestamp.to_rfc3339())
        .bind(&id_str)
        .execute(&self.pool)
        .await?;

        if updated.rows_affected() == 0 {
            // Distinguish a missing row from a duplicate fill event.
            let exists: Option<(String,)> = sqlx::query_as(
                "SELECT offchain_order_id FROM hedge_cycle WHERE offchain_order_id = ?",
            )
            .bind(&id_str)
            .fetch_optional(&self.pool)
            .await?;

            if exists.is_none() {
                warn!(
                    %offchain_order_id,
                    "Fill reported for a hedge with no placement in the read model"
                );
            } else {
                debug!(
                    %offchain_order_id,
                    "Duplicate fill event, keeping first filled_at timestamp"
                );
            }
        }

        Ok(())
    }

    /// Stamps `failed_at` on the cycle. A Failed cycle consumes no fills when
    /// the uncovered pool is recomputed ([`super::uncovered_fills`]), so its
    /// covered batch returns to the pool and a retry inherits attribution back to
    /// the original fill -- no mutable state to rewrite.
    async fn offchain_order_failed(
        &self,
        offchain_order_id: OffchainOrderId,
        failed_at: DateTime<Utc>,
    ) -> Result<(), ProjectionError> {
        let id_str = offchain_order_id.to_string();

        // Only set failed_at when the cycle has no terminal outcome yet:
        // a recorded fill must win.
        let updated = sqlx::query(
            "UPDATE hedge_cycle SET failed_at = ? \
             WHERE offchain_order_id = ? AND filled_at IS NULL AND failed_at IS NULL",
        )
        .bind(failed_at.to_rfc3339())
        .bind(&id_str)
        .execute(&self.pool)
        .await?;

        if updated.rows_affected() == 0 {
            let exists: Option<(String,)> = sqlx::query_as(
                "SELECT offchain_order_id FROM hedge_cycle WHERE offchain_order_id = ?",
            )
            .bind(&id_str)
            .fetch_optional(&self.pool)
            .await?;

            if exists.is_none() {
                warn!(
                    %offchain_order_id,
                    "Failure reported for a hedge with no placement in the read model"
                );
            } else {
                warn!(
                    %offchain_order_id,
                    "Failure reported for a hedge that already has a terminal outcome \
                     -- failed_at dropped"
                );
            }
        }

        Ok(())
    }

    async fn on_offchain_order(
        &self,
        id: OffchainOrderId,
        event: OffchainOrderEvent,
    ) -> Result<(), ProjectionError> {
        match event {
            // `Accepted` is the post-extraction broker-acceptance event; it
            // carries the same `submitted_at` as the legacy `Submitted`, so both
            // stamp the hedge submission time for latency.
            OffchainOrderEvent::Submitted { submitted_at, .. }
            | OffchainOrderEvent::Accepted { submitted_at, .. } => {
                // Record submission in its own table, keyed by order id, so it
                // survives regardless of whether the placement row exists yet:
                // the Position and OffchainOrder streams are independent, and a
                // Submitted event can arrive before OffChainOrderPlaced. The
                // read path LEFT JOINs this back in. ON CONFLICT DO NOTHING keeps
                // the first timestamp (idempotent redelivery).
                let updated = sqlx::query(
                    "INSERT INTO hedge_submission (offchain_order_id, submitted_at) \
                     VALUES (?, ?) \
                     ON CONFLICT(offchain_order_id) DO NOTHING",
                )
                .bind(id.to_string())
                .bind(submitted_at.to_rfc3339())
                .execute(&self.pool)
                .await?;

                if updated.rows_affected() == 0 {
                    debug!(
                        %id,
                        "Duplicate hedge-submission event (Submitted/Accepted), keeping first \
                         submitted_at timestamp"
                    );
                }

                Ok(())
            }
            OffchainOrderEvent::Placed { .. }
            | OffchainOrderEvent::PartiallyFilled { .. }
            | OffchainOrderEvent::Filled { .. }
            | OffchainOrderEvent::Failed { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum ProjectionError {
    #[error("hedge-latency read-model write failed")]
    Database(#[from] sqlx::Error),
    #[error("covered fill count does not fit in i64")]
    CoveredCount(#[from] std::num::TryFromIntError),
    #[error("recomputing the uncovered pool failed")]
    Uncovered(#[from] PerformanceError),
}

#[async_trait]
impl Reactor for HedgeLatencyProjection {
    type Error = ProjectionError;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|symbol, event| async move { self.on_position(symbol, event).await })
            .on(|id, event| async move { self.on_offchain_order(id, event).await })
            .exhaustive()
            .await
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::TxHash;
    use chrono::Duration;

    use st0x_event_sorcery::ReactorHarness;
    use st0x_execution::{Direction, FractionalShares};
    use st0x_float_macro::float;

    use crate::performance::report::{HedgeOutcome, ReportRange, load_hedge_performance};
    use crate::performance::test_helpers::{
        fill_event, placed_event, position_failed_event, position_filled_event,
        run_position_stream, symbol, timestamp,
    };
    use crate::position::{PositionEvent, TradeId};
    use crate::test_utils::setup_test_db;

    use super::*;

    #[tokio::test]
    async fn failed_hedge_returns_fills_to_open_exposure() {
        let order_id = OffchainOrderId::new();

        let (_pool, performance) = run_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 1),
                placed_event(order_id, 2),
                position_failed_event(order_id, 5),
            ],
        )
        .await;

        let open = performance.open_exposure.unwrap();
        assert_eq!(open.fill_count, 1);
        assert_eq!(open.oldest_block_timestamp, timestamp(0));
        assert_eq!(
            performance.cycles[0].outcome,
            HedgeOutcome::Failed {
                failed_at: timestamp(5)
            }
        );
    }

    #[tokio::test]
    async fn retry_after_failed_hedge_inherits_attribution() {
        let failed_order = OffchainOrderId::new();
        let retry_order = OffchainOrderId::new();

        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        for event in [
            fill_event(1, 0, 1),
            placed_event(failed_order, 2),
            position_failed_event(failed_order, 5),
            placed_event(retry_order, 10),
        ] {
            harness.receive::<Position>(symbol(), event).await.unwrap();
        }
        // The retry's broker pipeline: submitted then filled at the broker.
        harness
            .receive::<OffchainOrder>(
                retry_order,
                OffchainOrderEvent::Submitted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("broker-1"),
                    submitted_at: timestamp(11),
                },
            )
            .await
            .unwrap();
        harness
            .receive::<Position>(symbol(), position_filled_event(retry_order, 12))
            .await
            .unwrap();

        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let performance = &report[0];

        assert!(performance.open_exposure.is_none());
        let retry = &performance.cycles[1];
        let covered = retry.covered.as_ref().unwrap();
        assert_eq!(covered.count, 1);
        assert_eq!(covered.earliest_block_timestamp, timestamp(0));
        assert_eq!(retry.exposure_window(), Some(Duration::seconds(12)));
        assert_eq!(retry.submitted_at, Some(timestamp(11)));
        assert_eq!(
            retry.outcome,
            HedgeOutcome::Filled {
                filled_at: timestamp(12)
            }
        );
    }

    #[tokio::test]
    async fn manual_adjustment_clears_in_flight_attribution() {
        // The same order is placed before the reset and fails after it: the
        // reset must prevent its parked fills from resurfacing.
        let adjusted_order = OffchainOrderId::new();
        let later_order = OffchainOrderId::new();

        let (_pool, performance) = run_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 1),
                placed_event(adjusted_order, 2),
                PositionEvent::ManualPositionAdjusted {
                    previous_net: FractionalShares::new(float!(1)),
                    target_net: FractionalShares::new(float!(0)),
                    reason: "manual reset".to_string(),
                    price_usdc: None,
                    adjusted_at: timestamp(5),
                },
                position_failed_event(adjusted_order, 6),
                placed_event(later_order, 10),
            ],
        )
        .await;

        assert!(performance.open_exposure.is_none());
        assert!(performance.cycles[1].covered.is_none());
        // The failure still marks the first cycle's outcome; only the fill
        // attribution is reset.
        assert_eq!(
            performance.cycles[0].outcome,
            HedgeOutcome::Failed {
                failed_at: timestamp(6)
            }
        );
    }

    #[tokio::test]
    async fn duplicate_submitted_keeps_the_first_timestamp() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        harness
            .receive::<Position>(symbol(), placed_event(order_id, 1))
            .await
            .unwrap();
        harness
            .receive::<OffchainOrder>(
                order_id,
                OffchainOrderEvent::Submitted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("broker-1"),
                    submitted_at: timestamp(2),
                },
            )
            .await
            .unwrap();
        harness
            .receive::<OffchainOrder>(
                order_id,
                OffchainOrderEvent::Submitted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("broker-2"),
                    submitted_at: timestamp(4),
                },
            )
            .await
            .unwrap();

        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let cycle = &report[0].cycles[0];
        assert_eq!(cycle.submitted_at, Some(timestamp(2)));
    }

    /// A restart re-instantiates the reactor over the SAME database. Because
    /// the uncovered pool is recomputed from the durable, append-only tables
    /// (no persisted mutable state), a fresh instance attributes a later
    /// placement back to fills observed before the restart.
    #[tokio::test]
    async fn restart_resumes_from_durable_tables() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();

        // First reactor instance observes a fill, then is dropped.
        {
            let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));
            harness
                .receive::<Position>(symbol(), fill_event(1, 0, 1))
                .await
                .unwrap();
        }

        // Fresh reactor instance (simulated restart) places a hedge: it must
        // cover the fill recorded by the first instance, recomputed from the
        // durable hedge_fill table.
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));
        harness
            .receive::<Position>(symbol(), placed_event(order_id, 5))
            .await
            .unwrap();

        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let cycle = &report[0].cycles[0];
        let covered = cycle.covered.as_ref().unwrap();
        assert_eq!(covered.count, 1);
        assert_eq!(covered.earliest_block_timestamp, timestamp(0));
        assert_eq!(cycle.decision_latency(), Some(Duration::seconds(4)));
        assert!(report[0].open_exposure.is_none());
    }

    /// Open exposure is correct after a fresh `load_hedge_performance` with no
    /// in-memory reactor state surviving: a fill is recorded, then the only
    /// reader is a brand-new `load` call against the durable tables. This proves
    /// the read model survives a "restart" because it is derived purely from
    /// hedge_fill + hedge_cycle + hedge_attribution_reset.
    #[tokio::test]
    async fn open_exposure_recomputed_from_durable_tables_after_restart() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();

        // Reactor records a fill and a placement that covers it, then a later
        // unhedged fill, then is dropped.
        {
            let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));
            for event in [
                fill_event(1, 0, 1),
                placed_event(order_id, 2),
                fill_event(2, 40, 43),
            ] {
                harness.receive::<Position>(symbol(), event).await.unwrap();
            }
        }

        // No reactor instance is alive. The read path alone must report the
        // single post-placement fill as open exposure.
        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let open = report[0].open_exposure.as_ref().unwrap();
        assert_eq!(open.fill_count, 1);
        assert_eq!(open.oldest_block_timestamp, timestamp(40));
    }

    /// A broker fill event arrives for an order_id that was never placed.
    /// The report has no cycles (the warn path fires; rows_affected == 0).
    #[tokio::test]
    async fn filled_event_with_no_prior_placement_produces_no_cycles() {
        let order_id = OffchainOrderId::new();

        let (_pool, performance) =
            run_position_stream(symbol(), vec![position_filled_event(order_id, 5)]).await;

        assert!(performance.cycles.is_empty());
    }

    /// A broker failure event arrives for an order_id that was never placed.
    /// The report has no cycles (the warn path fires; existence check finds no row).
    #[tokio::test]
    async fn failed_event_with_no_prior_placement_produces_no_cycles() {
        let order_id = OffchainOrderId::new();

        let (_pool, performance) =
            run_position_stream(symbol(), vec![position_failed_event(order_id, 5)]).await;

        assert!(performance.cycles.is_empty());
    }

    /// A Submitted event arrives for an order_id that was never placed. The
    /// submission IS recorded durably in `hedge_submission` (keyed by order id,
    /// so it survives until the placement arrives), but with no `hedge_cycle`
    /// row the report's LEFT JOIN has nothing to attach it to and no cycle
    /// appears. Asserting the row was written guards the durable-storage INSERT:
    /// dropping it would silently lose early-arriving submissions.
    #[tokio::test]
    async fn submitted_event_with_no_prior_placement_stores_submission_but_no_cycle() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        harness
            .receive::<OffchainOrder>(
                order_id,
                OffchainOrderEvent::Submitted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("broker-x"),
                    submitted_at: timestamp(3),
                },
            )
            .await
            .unwrap();

        // The submission was stored durably even though no placement exists yet.
        let stored: Option<(String,)> =
            sqlx::query_as("SELECT submitted_at FROM hedge_submission WHERE offchain_order_id = ?")
                .bind(order_id.to_string())
                .fetch_optional(&pool)
                .await
                .unwrap();
        assert_eq!(stored, Some((timestamp(3).to_rfc3339(),)));

        // But with no cycle row, the report has nothing to attach it to.
        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        assert!(report.is_empty());
    }

    /// A duplicate `OffChainOrderFilled` event keeps the first `filled_at`
    /// timestamp, mirroring the `duplicate_submitted_keeps_the_first_timestamp`
    /// behaviour for `submitted_at`.
    #[tokio::test]
    async fn duplicate_filled_keeps_the_first_timestamp() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        harness
            .receive::<Position>(symbol(), placed_event(order_id, 1))
            .await
            .unwrap();
        harness
            .receive::<Position>(symbol(), position_filled_event(order_id, 5))
            .await
            .unwrap();
        // Second fill at a different timestamp — should be ignored.
        harness
            .receive::<Position>(symbol(), position_filled_event(order_id, 10))
            .await
            .unwrap();

        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let cycle = &report[0].cycles[0];
        assert_eq!(
            cycle.outcome,
            HedgeOutcome::Filled {
                filled_at: timestamp(5)
            }
        );
    }

    /// A redelivered `OnChainOrderFilled` for the SAME `(tx_hash, log_index)` is
    /// a no-op: the `UNIQUE(tx_hash, log_index)` constraint plus `ON CONFLICT DO
    /// NOTHING` keeps exactly one `hedge_fill` row, so the fill is not
    /// double-counted and open exposure is unchanged.
    #[tokio::test]
    async fn duplicate_onchain_fill_is_idempotent() {
        let pool = setup_test_db().await;
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        // A single fill event with a fixed identity, delivered twice.
        let fill = PositionEvent::OnChainOrderFilled {
            trade_id: TradeId {
                tx_hash: TxHash::repeat_byte(0xAB),
                log_index: 7,
            },
            amount: FractionalShares::new(float!(1)),
            direction: Direction::Buy,
            price_usdc: float!(150),
            block_timestamp: timestamp(0),
            seen_at: timestamp(1),
        };

        harness
            .receive::<Position>(symbol(), fill.clone())
            .await
            .unwrap();
        harness.receive::<Position>(symbol(), fill).await.unwrap();

        // Exactly one row survives the redelivery.
        let (fill_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM hedge_fill")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(fill_count, 1);

        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        // One fill observation, and the open exposure counts a single fill.
        assert_eq!(report[0].fills.len(), 1);
        let open = report[0].open_exposure.as_ref().unwrap();
        assert_eq!(open.fill_count, 1);
        assert_eq!(open.oldest_block_timestamp, timestamp(0));
    }

    /// A redelivered `OffChainOrderPlaced` for an already-placed cycle is a
    /// no-op: the `ON CONFLICT(offchain_order_id) DO NOTHING` guard keeps the
    /// first covered batch and does not duplicate the cycle, so open exposure
    /// is unchanged.
    #[tokio::test]
    async fn duplicate_placement_is_idempotent() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        // A fill, then a placement that covers it.
        harness
            .receive::<Position>(symbol(), fill_event(1, 0, 1))
            .await
            .unwrap();
        harness
            .receive::<Position>(symbol(), placed_event(order_id, 2))
            .await
            .unwrap();

        let before = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        assert_eq!(before[0].cycles.len(), 1);
        assert!(before[0].open_exposure.is_none());

        // Redeliver the SAME placement event.
        harness
            .receive::<Position>(symbol(), placed_event(order_id, 2))
            .await
            .unwrap();

        let after = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        // Still exactly one cycle, still covering the original fill, still no
        // open exposure: the redelivery changed nothing.
        assert_eq!(after[0].cycles.len(), 1);
        let covered = after[0].cycles[0].covered.as_ref().unwrap();
        assert_eq!(covered.count, 1);
        assert_eq!(covered.earliest_block_timestamp, timestamp(0));
        assert!(after[0].open_exposure.is_none());
    }

    /// A manual adjustment writes a durable reset boundary; fills observed after
    /// it are open exposure while fills before it are dropped. The read path
    /// derives this from `hedge_attribution_reset` alone (no in-memory state),
    /// proving the reset survives a restart.
    #[tokio::test]
    async fn manual_reset_is_read_from_durable_table() {
        let pool = setup_test_db().await;

        // Pre-reset fill, manual adjustment, post-reset fill -- all recorded by
        // one reactor instance which is then dropped.
        {
            let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));
            harness
                .receive::<Position>(symbol(), fill_event(1, 0, 1))
                .await
                .unwrap();
            harness
                .receive::<Position>(
                    symbol(),
                    PositionEvent::ManualPositionAdjusted {
                        previous_net: FractionalShares::new(float!(1)),
                        target_net: FractionalShares::new(float!(0)),
                        reason: "durable reset".to_string(),
                        price_usdc: None,
                        adjusted_at: timestamp(5),
                    },
                )
                .await
                .unwrap();
            harness
                .receive::<Position>(symbol(), fill_event(2, 40, 43))
                .await
                .unwrap();
        }

        // The read path alone must drop the pre-reset fill and report only the
        // post-reset fill as open exposure.
        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let open = report[0].open_exposure.as_ref().unwrap();
        assert_eq!(open.fill_count, 1);
        assert_eq!(open.oldest_block_timestamp, timestamp(40));
    }

    /// A `Submitted` event that arrives BEFORE its `OffChainOrderPlaced` (the
    /// Position and OffchainOrder streams are independent) must not lose its
    /// timestamp. The submission is recorded in `hedge_submission` keyed by
    /// order id, so the later placement's cycle still reports it via the LEFT
    /// JOIN -- regardless of arrival order.
    #[tokio::test]
    async fn submitted_before_placed_preserves_submitted_at() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();
        let harness = ReactorHarness::new(HedgeLatencyProjection::new(pool.clone()));

        // Submission lands first, before any placement row exists.
        harness
            .receive::<OffchainOrder>(
                order_id,
                OffchainOrderEvent::Submitted {
                    executor_order_id: st0x_execution::ExecutorOrderId::new("broker-1"),
                    submitted_at: timestamp(2),
                },
            )
            .await
            .unwrap();

        // The placement arrives afterwards.
        harness
            .receive::<Position>(symbol(), placed_event(order_id, 5))
            .await
            .unwrap();

        let report = load_hedge_performance(&pool, &ReportRange::all_time())
            .await
            .unwrap();
        let cycle = &report[0].cycles[0];
        // The fix: submitted_at survives even though Submitted preceded the
        // placement. Before the durable hedge_submission table, the UPDATE-only
        // handler found no cycle row and dropped this timestamp.
        assert_eq!(cycle.submitted_at, Some(timestamp(2)));
        // Submission preceded placement, so the submission latency
        // (placed_at - submitted_at = -3s) clamps to zero.
        assert_eq!(cycle.submission_latency(), Some(Duration::zero()));
    }

    /// The FILL path after a manual reset: a placement covers a fill, a manual
    /// adjustment resets attribution, then the broker fills the order. The cycle
    /// keeps the covered batch it snapshotted at placement time and records the
    /// broker fill as its terminal outcome. The reset drops the pre-reset fill
    /// from the uncovered-pool recompute, so no exposure is carried open.
    #[tokio::test]
    async fn fill_after_manual_adjustment_records_outcome_and_clears_exposure() {
        let order_id = OffchainOrderId::new();

        let (_pool, performance) = run_position_stream(
            symbol(),
            vec![
                fill_event(1, 0, 1),
                placed_event(order_id, 2),
                PositionEvent::ManualPositionAdjusted {
                    previous_net: FractionalShares::new(float!(1)),
                    target_net: FractionalShares::new(float!(0)),
                    reason: "manual reset".to_string(),
                    price_usdc: None,
                    adjusted_at: timestamp(5),
                },
                position_filled_event(order_id, 8),
            ],
        )
        .await;

        // The broker fill is the cycle's terminal outcome despite the reset.
        let cycle = &performance.cycles[0];
        assert_eq!(
            cycle.outcome,
            HedgeOutcome::Filled {
                filled_at: timestamp(8)
            }
        );
        // The placement (offset 2, before the reset) snapshotted the fill it
        // covered; that batch stays recorded on the cycle.
        let covered = cycle.covered.as_ref().unwrap();
        assert_eq!(covered.count, 1);
        assert_eq!(covered.earliest_block_timestamp, timestamp(0));
        // The reset (offset 5) drops the pre-reset fill (seen at 1) from the
        // recompute, so no exposure is carried open.
        assert!(performance.open_exposure.is_none());
    }
}
