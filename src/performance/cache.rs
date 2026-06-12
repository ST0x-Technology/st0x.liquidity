//! Version-checked cache for the folded performance read models.
//!
//! The hedge and rebalance reports need full event streams for correct
//! fill-to-hedge attribution, so a fold costs O(total event history).
//! Folding on every request made each 30-second dashboard poll pay that
//! cost forever. The cache stamps each folded value with the version of the
//! event streams its loader observed (see `super::events_version`, scoped
//! to the report's aggregate types) and refolds only when a fresh probe
//! disagrees; an idle poll costs one cheap probe.
//!
//! Each report sits behind its own mutex held across the refold, so
//! concurrent requests for the same report wait for one refold instead of
//! each folding the full history, while the dashboard's simultaneous
//! latency and rebalance requests still refresh in parallel.

use std::sync::Arc;

use sqlx::SqlitePool;
use tokio::sync::Mutex;

use st0x_dto::RebalanceOperationTiming;

use super::rebalance::{REBALANCE_AGGREGATE_TYPES, load_rebalance_operations};
use super::{
    EventsVersion, HEDGE_AGGREGATE_TYPES, PerformanceError, SymbolPerformance,
    load_hedge_performance,
};

/// Cached folds of the performance read models, shared via `AppState`.
#[derive(Debug, Default)]
pub(crate) struct PerformanceCache {
    hedge: Mutex<Option<Cached<Vec<SymbolPerformance>>>>,
    rebalance: Mutex<Option<Cached<Vec<RebalanceOperationTiming>>>>,
}

#[derive(Debug)]
struct Cached<Value> {
    events_version: EventsVersion,
    value: Arc<Value>,
}

impl PerformanceCache {
    /// Folded per-symbol hedge performance, refolded only when the hedge
    /// event streams changed since the cached fold.
    // The mutex guard is deliberately held across the refold await: that is
    // what collapses concurrent requests for this report into a single fold
    // (the documented thundering-herd guard), so the tightening clippy wants
    // would defeat the design.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) async fn hedge_performances(
        &self,
        pool: &SqlitePool,
    ) -> Result<Arc<Vec<SymbolPerformance>>, PerformanceError> {
        let mut slot = self.hedge.lock().await;
        if let Some(value) = fresh(slot.as_ref(), pool, HEDGE_AGGREGATE_TYPES).await? {
            return Ok(value);
        }

        let (performances, events_version) = load_hedge_performance(pool).await?;
        let value = Arc::new(performances);
        *slot = Some(Cached {
            events_version,
            value: Arc::clone(&value),
        });
        Ok(value)
    }

    /// Folded rebalance operation timings, refolded only when the rebalance
    /// event stream changed since the cached fold.
    // See `hedge_performances`: the guard is held across the refold on
    // purpose to serialize concurrent refolds of this report.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) async fn rebalance_operations(
        &self,
        pool: &SqlitePool,
    ) -> Result<Arc<Vec<RebalanceOperationTiming>>, PerformanceError> {
        let mut slot = self.rebalance.lock().await;
        if let Some(value) = fresh(slot.as_ref(), pool, REBALANCE_AGGREGATE_TYPES).await? {
            return Ok(value);
        }

        let (operations, events_version) = load_rebalance_operations(pool).await?;
        let value = Arc::new(operations);
        *slot = Some(Cached {
            events_version,
            value: Arc::clone(&value),
        });
        Ok(value)
    }
}

/// The cached value, when the report's event streams still match its
/// version stamp.
///
/// The probe deliberately runs outside the fold transaction: under
/// concurrent writes it may under-report freshness (one extra refold whose
/// entry is stamped from inside the fold's own transaction), but it can
/// never over-report freshness and serve stale data. A probe error
/// propagates to the caller without touching the slot -- the next request
/// simply probes again.
async fn fresh<Value: Send + Sync>(
    cached: Option<&Cached<Value>>,
    pool: &SqlitePool,
    aggregate_types: &[&str],
) -> Result<Option<Arc<Value>>, PerformanceError> {
    let Some(cached) = cached else {
        return Ok(None);
    };
    let current = super::events_version(pool, aggregate_types).await?;
    Ok((current == cached.events_version).then(|| Arc::clone(&cached.value)))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::TxHash;
    use chrono::{TimeZone, Utc};

    use st0x_execution::{Direction, ExecutorOrderId, FractionalShares};
    use st0x_finance::Usdc;
    use st0x_float_macro::float;

    use super::*;
    use crate::offchain::order::OffchainOrderEvent;
    use crate::position::{PositionEvent, TradeId};
    use crate::test_utils::setup_test_db;
    use crate::usdc_rebalance::{RebalanceDirection, UsdcRebalanceEvent};

    async fn insert_event(
        pool: &SqlitePool,
        aggregate_type: &str,
        aggregate_id: &str,
        sequence: i64,
        event_type: &str,
        payload: String,
    ) {
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, \
              payload, metadata) \
             VALUES ($1, $2, $3, $4, '1.0', $5, '{}')",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(sequence)
        .bind(event_type)
        .bind(payload)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_fill_event(pool: &SqlitePool, sequence: i64) {
        let event = PositionEvent::OnChainOrderFilled {
            trade_id: TradeId {
                tx_hash: TxHash::random(),
                log_index: 1,
            },
            amount: FractionalShares::new(float!(1)),
            direction: Direction::Buy,
            price_usdc: float!(150),
            block_timestamp: Utc.timestamp_opt(1_750_000_000, 0).unwrap(),
            seen_at: Utc.timestamp_opt(1_750_000_005, 0).unwrap(),
        };
        insert_event(
            pool,
            "Position",
            "AAPL",
            sequence,
            "PositionEvent::OnChainOrderFilled",
            serde_json::to_string(&event).unwrap(),
        )
        .await;
    }

    async fn insert_rebalance_event(pool: &SqlitePool, operation_id: &str) {
        let event = UsdcRebalanceEvent::WithdrawalSubmitting {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(1000)),
            from_block: 1,
            submitting_at: Utc.timestamp_opt(1_750_000_000, 0).unwrap(),
        };
        insert_event(
            pool,
            "UsdcRebalance",
            operation_id,
            1,
            "UsdcRebalanceEvent::WithdrawalSubmitting",
            serde_json::to_string(&event).unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn serves_cached_fold_while_events_unchanged() {
        let pool = setup_test_db().await;
        insert_fill_event(&pool, 1).await;
        let cache = PerformanceCache::default();

        let first = cache.hedge_performances(&pool).await.unwrap();
        let second = cache.hedge_performances(&pool).await.unwrap();

        assert!(
            Arc::ptr_eq(&first, &second),
            "unchanged events table must serve the cached Arc"
        );
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].fills.len(), 1);
    }

    #[tokio::test]
    async fn refolds_when_events_table_grows() {
        let pool = setup_test_db().await;
        insert_fill_event(&pool, 1).await;
        let cache = PerformanceCache::default();

        let first = cache.hedge_performances(&pool).await.unwrap();
        insert_fill_event(&pool, 2).await;
        let second = cache.hedge_performances(&pool).await.unwrap();

        assert!(
            !Arc::ptr_eq(&first, &second),
            "a grown events table must trigger a refold"
        );
        assert_eq!(first[0].fills.len(), 1);
        assert_eq!(second[0].fills.len(), 2);
    }

    #[tokio::test]
    async fn rebalance_cache_refolds_when_its_stream_grows() {
        let pool = setup_test_db().await;
        insert_rebalance_event(&pool, "op-1").await;
        let cache = PerformanceCache::default();

        let first = cache.rebalance_operations(&pool).await.unwrap();
        insert_rebalance_event(&pool, "op-2").await;
        let second = cache.rebalance_operations(&pool).await.unwrap();

        assert!(
            !Arc::ptr_eq(&first, &second),
            "a grown UsdcRebalance stream must trigger a refold"
        );
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 2);
    }

    #[tokio::test]
    async fn caches_empty_fold_on_fresh_database() {
        let pool = setup_test_db().await;
        let cache = PerformanceCache::default();

        let first = cache.hedge_performances(&pool).await.unwrap();
        let second = cache.hedge_performances(&pool).await.unwrap();

        assert!(first.is_empty());
        assert!(
            Arc::ptr_eq(&first, &second),
            "an empty events table must still be cached, not refolded per poll"
        );
    }

    #[tokio::test]
    async fn unrelated_aggregate_writes_do_not_evict_the_cache() {
        let pool = setup_test_db().await;
        insert_fill_event(&pool, 1).await;
        let cache = PerformanceCache::default();

        let first = cache.hedge_performances(&pool).await.unwrap();
        // High-frequency snapshot events (which are also compacted at
        // runtime) must not invalidate the hedge report's scoped stamp.
        insert_event(
            &pool,
            "InventorySnapshot",
            "inventory",
            1,
            "InventorySnapshotEvent::Recorded",
            "{}".to_string(),
        )
        .await;
        let second = cache.hedge_performances(&pool).await.unwrap();

        assert!(
            Arc::ptr_eq(&first, &second),
            "writes to unrelated aggregates must not evict the cache"
        );
    }

    #[tokio::test]
    async fn hedge_cache_refolds_when_order_stream_grows() {
        let pool = setup_test_db().await;
        insert_fill_event(&pool, 1).await;
        let cache = PerformanceCache::default();

        let first = cache.hedge_performances(&pool).await.unwrap();
        // The hedge fold reads the OffchainOrder stream too; an append
        // there must evict the cache (pins the stamp's scope to every
        // stream the fold consumes, per HEDGE_AGGREGATE_TYPES).
        let order = OffchainOrderEvent::Submitted {
            executor_order_id: ExecutorOrderId::new("broker-1"),
            submitted_at: Utc.timestamp_opt(1_750_000_010, 0).unwrap(),
        };
        insert_event(
            &pool,
            "OffchainOrder",
            "11111111-2222-3333-4444-555555555555",
            1,
            "OffchainOrderEvent::Submitted",
            serde_json::to_string(&order).unwrap(),
        )
        .await;
        let second = cache.hedge_performances(&pool).await.unwrap();

        assert!(
            !Arc::ptr_eq(&first, &second),
            "a grown OffchainOrder stream must trigger a hedge refold"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_requests_share_one_refold() {
        let pool = setup_test_db().await;
        insert_fill_event(&pool, 1).await;
        let cache = Arc::new(PerformanceCache::default());

        let (first, second) = tokio::join!(
            {
                let cache = Arc::clone(&cache);
                let pool = pool.clone();
                async move { cache.hedge_performances(&pool).await.unwrap() }
            },
            {
                let cache = Arc::clone(&cache);
                let pool = pool.clone();
                async move { cache.hedge_performances(&pool).await.unwrap() }
            }
        );

        // The mutex held across the refold serializes the racing requests:
        // whichever enters second must receive the first one's cached Arc
        // rather than folding independently.
        assert!(
            Arc::ptr_eq(&first, &second),
            "racing requests must share one fold"
        );
    }

    #[tokio::test]
    async fn rebalance_cache_is_independent_of_hedge_cache() {
        let pool = setup_test_db().await;
        insert_rebalance_event(&pool, "op-1").await;
        let cache = PerformanceCache::default();

        let operations = cache.rebalance_operations(&pool).await.unwrap();
        // A hedge-stream append must not evict the populated rebalance
        // slot: each slot's stamp is scoped to its own aggregate types.
        insert_fill_event(&pool, 1).await;
        let operations_again = cache.rebalance_operations(&pool).await.unwrap();

        assert_eq!(operations.len(), 1);
        assert!(
            Arc::ptr_eq(&operations, &operations_again),
            "hedge writes must not evict the rebalance cache"
        );
    }
}
