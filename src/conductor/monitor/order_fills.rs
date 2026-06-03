//! Supervised order fill monitor that drives continuous `eth_getLogs`
//! polling over a persisted checkpoint.
//!
//! Every `order_fill_poll_interval` seconds the monitor:
//! 1. Skips the tick if a `BackfillRange` job is still in flight -- the
//!    checkpoint has not advanced yet, so re-enqueuing would re-scan the
//!    same blocks (and during a long catch-up would stack unbounded
//!    overlapping ranges).
//! 2. Reads the chain tip and derives a cutoff at `tip -
//!    required_confirmations`. The cutoff never exceeds the confirmation
//!    boundary, so ingestion never persists logs from blocks that could
//!    still reorg (under the assumed reorg depth).
//! 3. Enqueues a `BackfillRange` job covering `(checkpoint+1, cutoff)`.
//!    The `backfill-worker` fetches the logs via HTTP `eth_getLogs`,
//!    pushes an accounting job per fill, and advances the checkpoint only
//!    on success -- so a failed range is retried and never silently
//!    skipped. Downstream dedupes by `(tx_hash, log_index)`.
//!
//! This replaces the previous WebSocket `.watch()` filter-polling path.
//! On a load-balanced RPC, `.watch()` issued `eth_newFilter` once and
//! `eth_getFilterChanges` every few seconds against a filter that lived
//! on a single backend node, so most polls were round-robined to nodes
//! that returned `-32601 method not available` -- thousands of error-log
//! lines a day plus a second (WS) transport whose silent closures were a
//! recurring failure mode. `eth_subscribe`/`subscribe_logs` was rejected
//! for the same reason: push subscriptions are sticky to one WS node and
//! silently stall. A single HTTP transport with explicit, durable,
//! visible retries (the apalis job) is the deliberate choice here,
//! aligning liquidity with the issuance bot's ingestion architecture.
//!
//! Note: `required_confirmations` is a naive reorg-protection heuristic,
//! not real chain finality. A deep reorg exceeding the configured depth
//! will still corrupt state; `backfill_range` surfaces a `removed: true`
//! log past the confirmation boundary loudly rather than masking it.

use std::time::Duration;

use alloy::providers::Provider;
use sqlx::SqlitePool;
use task_supervisor::{SupervisedTask, TaskResult};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use st0x_config::EvmCtx;

use crate::conductor::job::QueuePushError;
use crate::onchain::OnChainError;
use crate::onchain::backfill::{BackfillJobQueue, BackfillRange, backfill_start_block};

/// Polls the orderbook chain for `ClearV3` / `TakeOrderV3` fills on a
/// fixed interval and enqueues durable [`BackfillRange`] jobs that the
/// `backfill-worker` processes.
///
/// Implements [`SupervisedTask`] so the supervisor restarts it on a
/// panic. Transient errors (RPC blips, a momentarily unreachable node)
/// are logged and swallowed -- the next tick retries from the same
/// checkpoint -- so a hiccup never halts ingestion.
#[derive(Clone)]
pub(crate) struct OrderFillMonitor<P> {
    evm_ctx: EvmCtx,
    backfill_queue: BackfillJobQueue,
    pool: SqlitePool,
    provider: P,
    poll_interval: Duration,
}

impl<P> OrderFillMonitor<P> {
    pub(crate) fn new(
        evm_ctx: EvmCtx,
        backfill_queue: BackfillJobQueue,
        pool: SqlitePool,
        provider: P,
        poll_interval: Duration,
    ) -> Self {
        Self {
            evm_ctx,
            backfill_queue,
            pool,
            provider,
            poll_interval,
        }
    }
}

impl<P: Provider + Clone + Send + Sync + 'static> SupervisedTask for OrderFillMonitor<P> {
    async fn run(&mut self) -> TaskResult {
        info!(
            target: "orderbook",
            interval_secs = self.poll_interval.as_secs(),
            required_confirmations = self.evm_ctx.required_confirmations,
            "Order fill monitor started (continuous eth_getLogs polling)"
        );

        let mut interval = tokio::time::interval(self.poll_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            if let Err(error) = self.poll_once().await {
                warn!(
                    target: "orderbook",
                    ?error,
                    "Order fill poll failed; retrying on next tick"
                );
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum OrderFillMonitorError {
    #[error("RPC error reading chain tip: {0}")]
    Rpc(#[from] alloy::transports::RpcError<alloy::transports::TransportErrorKind>),
    #[error(transparent)]
    OnChain(#[from] OnChainError),
    #[error(transparent)]
    Enqueue(#[from] QueuePushError),
    #[error("failed to query backfill job status: {0}")]
    JobStatus(#[from] sqlx::Error),
}

impl<P: Provider + Clone> OrderFillMonitor<P> {
    /// One poll iteration: enqueue a backfill range for the unprocessed
    /// blocks behind the confirmation boundary, unless a previous range
    /// is still in flight or there is nothing new to fetch.
    async fn poll_once(&mut self) -> Result<(), OrderFillMonitorError> {
        // Overlap guard: while a previous range is still being processed
        // the checkpoint has not advanced, so a fresh enqueue would
        // re-scan the same blocks. During a long catch-up this would
        // otherwise stack one growing range per tick.
        if self.backfill_queue.has_in_flight().await? {
            debug!(
                target: "orderbook",
                "Skipping poll: a backfill range is still in flight"
            );
            return Ok(());
        }

        // Cutoff is the latest block past the confirmation depth. Backfill
        // is guaranteed not to ingest logs above this boundary, preserving
        // reorg protection.
        let cutoff_block =
            latest_confirmed_block(&self.provider, self.evm_ctx.required_confirmations)
                .await?
                .unwrap_or(0);
        let from_block = backfill_start_block(&self.pool, &self.evm_ctx).await?;

        if from_block <= cutoff_block {
            info!(
                target: "orderbook",
                from_block,
                cutoff_block,
                required_confirmations = self.evm_ctx.required_confirmations,
                "Enqueuing order-fill backfill range"
            );

            self.backfill_queue
                .push(BackfillRange {
                    from_block,
                    to_block: cutoff_block,
                })
                .await?;
        } else {
            debug!(
                target: "orderbook",
                from_block,
                cutoff_block,
                "Caught up; nothing to enqueue this tick"
            );
        }

        Ok(())
    }
}

/// Latest block past the configured confirmation depth:
/// `latest_tip - required_confirmations`. Returns `None` when the chain has
/// fewer blocks than the requested confirmation depth (no block is
/// safe to ingest yet). This is a naive reorg-protection heuristic,
/// not real chain finality.
pub(crate) async fn latest_confirmed_block<P: Provider>(
    provider: &P,
    required_confirmations: u64,
) -> Result<Option<u64>, alloy::transports::RpcError<alloy::transports::TransportErrorKind>> {
    let tip = provider.get_block_number().await?;
    Ok(tip.checked_sub(required_confirmations))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use sqlx::SqlitePool;

    use super::*;
    use crate::conductor::setup_apalis_tables;
    use crate::test_utils::setup_test_db;

    /// Builds a mock provider whose `eth_blockNumber` resolves the tip to
    /// `block`, so `latest_confirmed_block` returns `block - confs`.
    fn provider_at_tip(block: u64) -> impl Provider + Clone {
        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::from(block));
        ProviderBuilder::new().connect_mocked_client(asserter)
    }

    async fn backfill_job_count(pool: &SqlitePool) -> i64 {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM Jobs WHERE job_type LIKE '%BackfillRange%'",
        )
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn setup<P>(
        provider: P,
        required_confirmations: u64,
    ) -> (OrderFillMonitor<P>, SqlitePool, EvmCtx) {
        let pool = setup_test_db().await;
        setup_apalis_tables(&pool).await.unwrap();
        let backfill_queue = BackfillJobQueue::new(&pool);

        let evm_ctx = EvmCtx {
            rpc_url: url::Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations,
        };

        let monitor = OrderFillMonitor::new(
            evm_ctx.clone(),
            backfill_queue,
            pool.clone(),
            provider,
            Duration::from_secs(5),
        );

        (monitor, pool, evm_ctx)
    }

    async fn loaded_backfill(pool: &SqlitePool) -> BackfillRange {
        let job_payload = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT job FROM Jobs WHERE job_type LIKE '%BackfillRange%'",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        serde_json::from_slice(&job_payload).unwrap()
    }

    #[tokio::test]
    async fn poll_once_enqueues_range_capped_at_confirmation_boundary() {
        // checkpoint=99, tip=105, confs=3 -> cutoff=102, from=100.
        let (mut monitor, pool, evm_ctx) = setup(provider_at_tip(105), 3).await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 99)
            .await
            .unwrap();

        monitor.poll_once().await.unwrap();

        assert_eq!(backfill_job_count(&pool).await, 1);
        let job = loaded_backfill(&pool).await;
        assert_eq!(job.from_block, 100, "backfill must resume after checkpoint");
        assert_eq!(
            job.to_block, 102,
            "cutoff must equal tip minus required_confirmations"
        );
    }

    #[tokio::test]
    async fn poll_once_uses_deployment_block_without_checkpoint() {
        // No checkpoint: from_block falls back to deployment_block (1).
        let (mut monitor, pool, _evm_ctx) = setup(provider_at_tip(50), 0).await;

        monitor.poll_once().await.unwrap();

        let job = loaded_backfill(&pool).await;
        assert_eq!(job.from_block, 1, "first run resumes from deployment_block");
        assert_eq!(job.to_block, 50);
    }

    #[tokio::test]
    async fn poll_once_skips_when_caught_up() {
        // checkpoint=105, tip=105, confs=3 -> cutoff=102, from=106 > cutoff.
        let (mut monitor, pool, evm_ctx) = setup(provider_at_tip(105), 3).await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 105)
            .await
            .unwrap();

        monitor.poll_once().await.unwrap();

        assert_eq!(
            backfill_job_count(&pool).await,
            0,
            "nothing to enqueue when checkpoint is past the confirmation boundary"
        );
    }

    #[tokio::test]
    async fn poll_once_does_not_enqueue_when_no_block_is_safe_yet() {
        // Chain shallower than confs: latest_confirmed_block -> None -> 0.
        // checkpoint absent so from_block = deployment_block = 1 > 0: skip.
        let (mut monitor, pool, _evm_ctx) = setup(provider_at_tip(2), 5).await;

        monitor.poll_once().await.unwrap();

        assert_eq!(
            backfill_job_count(&pool).await,
            0,
            "no safe block to ingest yet -> no enqueue"
        );
    }

    #[tokio::test]
    async fn poll_once_skips_while_backfill_in_flight() {
        // A pending BackfillRange already exists: the overlap guard must
        // short-circuit before enqueuing (and before touching the provider,
        // so the empty asserter is never polled).
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let (mut monitor, pool, evm_ctx) = setup(provider, 0).await;

        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 10)
            .await
            .unwrap();
        let mut other_handle = BackfillJobQueue::new(&pool);
        other_handle
            .push(BackfillRange {
                from_block: 11,
                to_block: 20,
            })
            .await
            .unwrap();
        assert_eq!(backfill_job_count(&pool).await, 1);

        monitor.poll_once().await.unwrap();

        assert_eq!(
            backfill_job_count(&pool).await,
            1,
            "overlap guard must prevent a second enqueue while one is in flight"
        );
    }

    #[tokio::test]
    async fn orphaned_running_backfill_wedges_poller_until_requeued() {
        // A `BackfillRange` left `Running` by a dead process keeps
        // `has_in_flight` true, so the overlap guard suppresses ingestion
        // indefinitely -- the deterministic worker name means apalis never
        // ages the orphan out. `requeue_orphaned`, wired at conductor startup,
        // is what unwedges it.
        let (mut monitor, pool, evm_ctx) = setup(provider_at_tip(105), 3).await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 10)
            .await
            .unwrap();

        let queue = BackfillJobQueue::new(&pool);
        BackfillJobQueue::new(&pool)
            .push(BackfillRange {
                from_block: 11,
                to_block: 20,
            })
            .await
            .unwrap();
        // Simulate a crash mid-process: the row is stuck `Running` with no
        // live worker owning it.
        sqlx::query("UPDATE Jobs SET status = 'Running' WHERE job_type LIKE '%BackfillRange%'")
            .execute(&pool)
            .await
            .unwrap();

        // The wedge: the poller skips while the orphan counts as in flight.
        monitor.poll_once().await.unwrap();
        assert_eq!(
            backfill_job_count(&pool).await,
            1,
            "orphaned Running range still blocks the overlap guard"
        );

        // Startup recovery resets the orphan so a fresh worker can re-drive it.
        let reset = queue.requeue_orphaned().await.unwrap();
        assert_eq!(reset, 1, "the orphaned backfill range is requeued");

        let status = sqlx::query_scalar::<_, String>(
            "SELECT status FROM Jobs WHERE job_type LIKE '%BackfillRange%'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(status, "Pending", "requeue makes the orphan re-drivable");
    }

    #[tokio::test]
    async fn latest_confirmed_block_subtracts_confirmations() {
        let confirmed = latest_confirmed_block(&provider_at_tip(105), 3)
            .await
            .unwrap();
        assert_eq!(confirmed, Some(102));
    }

    #[tokio::test]
    async fn latest_confirmed_block_returns_none_when_chain_too_shallow() {
        let confirmed = latest_confirmed_block(&provider_at_tip(2), 3)
            .await
            .unwrap();
        assert_eq!(confirmed, None);
    }
}
