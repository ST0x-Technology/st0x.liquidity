//! Detects essential job queues whose runnable work is overdue.

use apalis_sqlite::{SqlitePool, SqlxError};
use sqlx_apalis::{QueryBuilder, Sqlite};
use std::any::type_name;
use std::num::TryFromIntError;
use std::time::Duration;
use tracing::error;

use super::job::DEFAULT_PERFORM_TIMEOUT;
use crate::dashboard::DeliverDashboardTrade;
use crate::offchain::order::poll_status::STRANDED_POLL_JOB_INTERVAL_MULTIPLIER;
use crate::offchain::order::{HandleOrderRejection, PollOrderStatus, ReconcileOrderFill};
use crate::onchain::backfill::{BACKFILL_PERFORM_TIMEOUT, BackfillRange};
use crate::position_check::CheckPositions;
use crate::rebalancing::{EquityRebalancingCheck, UsdcRebalancingCheck};
use crate::trading::offchain::hedge::PlaceHedge;
use crate::trading::onchain::trade_accountant::AccountForDexTrade;
use crate::vault_registry::SeedVaultRegistry;

pub(crate) async fn stalled_queues(
    pool: &SqlitePool,
    poll_interval: Duration,
) -> Result<Vec<String>, QueueHealthError> {
    let poll_bound = poll_interval
        .checked_mul(STRANDED_POLL_JOB_INTERVAL_MULTIPLIER)
        .ok_or(QueueHealthError::PollBoundOverflow)?;
    let minimum_bound = i64::try_from(poll_bound.min(DEFAULT_PERFORM_TIMEOUT).as_secs())?;
    let supervised = [
        type_name::<AccountForDexTrade>(),
        type_name::<DeliverDashboardTrade>(),
        type_name::<PlaceHedge>(),
        type_name::<ReconcileOrderFill>(),
        type_name::<HandleOrderRejection>(),
        type_name::<SeedVaultRegistry>(),
        type_name::<CheckPositions>(),
        type_name::<EquityRebalancingCheck>(),
        type_name::<UsdcRebalancingCheck>(),
    ];
    let rows = overdue_jobs(pool, minimum_bound, &supervised).await?;
    let mut stalled = Vec::new();
    for (job_type, age_secs) in rows {
        let bound = if job_type == type_name::<PollOrderStatus>() {
            poll_bound
        } else if job_type == type_name::<BackfillRange>()
            || job_type
                .strip_prefix(type_name::<BackfillRange>())
                .is_some_and(|suffix| suffix.starts_with('@'))
        {
            BACKFILL_PERFORM_TIMEOUT
        } else if supervised.contains(&job_type.as_str()) {
            DEFAULT_PERFORM_TIMEOUT
        } else {
            continue;
        };
        let bound_secs = i64::try_from(bound.as_secs())?;
        if age_secs > bound_secs {
            error!(%job_type, age_secs, bound_secs, "Essential job queue stalled: runnable work is overdue");
            stalled.push(job_type);
        }
    }
    Ok(stalled)
}

async fn overdue_jobs(
    pool: &SqlitePool,
    minimum_bound: i64,
    supervised: &[&str],
) -> Result<Vec<(String, i64)>, QueueHealthError> {
    let mut query = QueryBuilder::<Sqlite>::new(
        "SELECT job_type, MAX(CAST(strftime('%s', 'now') AS INTEGER) - run_at) \
         FROM Jobs WHERE run_at < CAST(strftime('%s', 'now') AS INTEGER) - ",
    );
    query.push_bind(minimum_bound).push(
        " AND (status = 'Pending' OR (status = 'Failed' AND attempts < max_attempts)) \
         AND (job_type IN (",
    );
    let mut types = query.separated(", ");
    for job_type in supervised
        .iter()
        .copied()
        .chain([type_name::<PollOrderStatus>(), type_name::<BackfillRange>()])
    {
        types.push_bind(job_type);
    }
    query
        .push(") OR job_type GLOB ")
        .push_bind(format!("{}@*", type_name::<BackfillRange>()))
        .push(") GROUP BY job_type ORDER BY job_type");
    Ok(tokio::time::timeout(
        Duration::from_secs(5),
        query.build_query_as().fetch_all(pool),
    )
    .await??)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum QueueHealthError {
    #[error("Failed to read queue health")]
    Database(#[from] SqlxError),
    #[error("Queue health read timed out")]
    ReadTimeout(#[from] tokio::time::error::Elapsed),
    #[error("Order polling stall bound overflowed")]
    PollBoundOverflow,
    #[error("Queue health duration is out of range")]
    DurationOutOfRange(#[from] TryFromIntError),
}

#[cfg(test)]
mod tests {
    use std::any::type_name;
    use std::time::Duration;

    use super::stalled_queues;
    use crate::conductor::job::JobQueue;
    use crate::offchain::order::{OffchainOrderId, PollOrderStatus};
    use crate::test_utils::setup_test_apalis_pool;

    #[tokio::test]
    async fn overdue_runnable_poll_is_unhealthy_but_delayed_and_terminal_jobs_are_not() {
        let pool = setup_test_apalis_pool().await;
        let mut queue = JobQueue::<PollOrderStatus>::new(&pool);
        queue
            .push_with_delay(
                PollOrderStatus {
                    offchain_order_id: OffchainOrderId::new(),
                    backpressure_streak: crate::conductor::job::BackpressureStreak::default(),
                },
                Duration::from_secs(3600),
            )
            .await
            .unwrap();
        let interval = Duration::from_secs(10);
        assert_eq!(
            stalled_queues(&pool, interval).await.unwrap(),
            Vec::<String>::new()
        );

        sqlx_apalis::query("UPDATE Jobs SET run_at = strftime('%s', 'now') - 51")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            stalled_queues(&pool, interval).await.unwrap(),
            vec![type_name::<PollOrderStatus>().to_string()]
        );

        sqlx_apalis::query("UPDATE Jobs SET status = 'Failed', attempts = 1, max_attempts = 5")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            stalled_queues(&pool, interval).await.unwrap(),
            vec![type_name::<PollOrderStatus>().to_string()]
        );

        sqlx_apalis::query("UPDATE Jobs SET attempts = max_attempts")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            stalled_queues(&pool, interval).await.unwrap(),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn queue_health_reports_database_read_failure() {
        let pool = setup_test_apalis_pool().await;
        pool.close().await;
        let error = stalled_queues(&pool, Duration::from_secs(10))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            super::QueueHealthError::Database(apalis_sqlite::SqlxError::PoolClosed)
        ));
    }

    #[tokio::test]
    async fn health_respects_backfill_namespaces_and_excludes_best_effort_queues() {
        let pool = setup_test_apalis_pool().await;
        let mut queue = JobQueue::<u32>::new(&pool);
        queue.push(1).await.unwrap();
        let backfill_type = format!(
            "{}@robinhood",
            type_name::<crate::onchain::backfill::BackfillRange>()
        );
        sqlx_apalis::query("UPDATE Jobs SET job_type = ?, run_at = strftime('%s', 'now') - 7201")
            .bind(&backfill_type)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            stalled_queues(&pool, Duration::from_secs(10))
                .await
                .unwrap(),
            vec![backfill_type]
        );
        sqlx_apalis::query("UPDATE Jobs SET job_type = ?, run_at = strftime('%s', 'now') - 100000")
            .bind(type_name::<crate::portfolio_snapshot::PortfolioSnapshotJob>())
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            stalled_queues(&pool, Duration::from_secs(10))
                .await
                .unwrap(),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn health_rejects_poll_interval_overflow() {
        let pool = setup_test_apalis_pool().await;
        let error = stalled_queues(&pool, Duration::MAX).await.unwrap_err();
        assert!(matches!(error, super::QueueHealthError::PollBoundOverflow));
    }

    #[tokio::test]
    async fn overdue_query_excludes_best_effort_rows_before_grouping() {
        let pool = setup_test_apalis_pool().await;
        let mut queue = JobQueue::<u32>::new(&pool);
        queue.push(1).await.unwrap();
        sqlx_apalis::query("UPDATE Jobs SET job_type = ?, run_at = strftime('%s', 'now') - 100000")
            .bind(type_name::<crate::portfolio_snapshot::PortfolioSnapshotJob>())
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            super::overdue_jobs(&pool, 50, &[type_name::<PollOrderStatus>()])
                .await
                .unwrap(),
            Vec::<(String, i64)>::new()
        );
    }
}
