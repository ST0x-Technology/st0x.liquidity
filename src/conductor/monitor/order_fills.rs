//! Supervised order fill monitor that drives continuous `eth_getLogs`
//! polling over a persisted checkpoint.
//!
//! Every `order_fill_poll_interval` seconds the monitor:
//! 1. Skips the tick if a `BackfillRange` job is still in flight -- the
//!    checkpoint has not advanced yet, so re-enqueuing would re-scan the
//!    same blocks (and during a long catch-up would stack unbounded
//!    overlapping ranges).
//! 2. Reads the chain's latest finalized block and uses it as the cutoff, so
//!    ingestion never persists logs from blocks that could still reorg. A
//!    finalized block cannot reorg, so this is real single-chain reorg
//!    protection rather than a confirmation-depth heuristic.
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
//! Capping at the finalized block is the simplified single-chain reorg
//! protection: a finalized Base block will not reorg, so an
//! ingested fill cannot be invalidated. Serious cross-chain reorg handling
//! (first-class reversal events) is tracked separately in the Reorg
//! protection project. `backfill_range` still surfaces a `removed: true` log
//! loudly rather than masking it, as defense in depth.

use std::time::Duration;

use alloy::eips::BlockNumberOrTag;
use alloy::providers::Provider;
use alloy::transports::{RpcError, TransportErrorKind};
use sqlx::SqlitePool;
use task_supervisor::{SupervisedTask, TaskResult};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use st0x_config::EvmCtx;

use crate::conductor::job::QueuePushError;
use crate::onchain::OnChainError;
use crate::onchain::backfill::{
    BackfillJobQueue, BackfillRange, backfill_start_from_checkpoint, load_backfill_checkpoint,
};

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
    #[error("RPC error reading finalized block: {0}")]
    Rpc(#[from] RpcError<TransportErrorKind>),
    #[error(transparent)]
    OnChain(#[from] OnChainError),
    #[error(transparent)]
    Enqueue(#[from] QueuePushError),
    #[error("failed to query backfill job status: {0}")]
    JobStatus(#[from] apalis_sqlite::SqlxError),
}

/// The branch a single [`OrderFillMonitor::poll_once`] tick took. The supervised
/// run loop discards it (it reacts only to `Err`); it exists so tests can observe
/// the cutoff decision -- in particular to tell an expected cold-start skip apart
/// from a checkpoint-backed skip that signals an RPC finality anomaly. Those two
/// share identical side effects (no enqueue, checkpoint frozen), so without a
/// typed outcome an inverted checkpoint check could not be caught from the
/// outside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PollOutcome {
    /// A previous backfill range is still in flight; the tick was skipped.
    RangeInFlight,
    /// No finalized block reported and no checkpoint exists yet -- expected
    /// cold-start lag on a chain shallower than finality.
    NoFinalityColdStart,
    /// No finalized block reported while a checkpoint exists -- a finality
    /// regression (likely a stale or load-balanced RPC); ingestion is paused.
    NoFinalityWithCheckpoint,
    /// A backfill range up to the finalized block was enqueued.
    Enqueued { from_block: u64, to_block: u64 },
    /// Ingestion has reached the finalized block; nothing to enqueue.
    CaughtUp,
    /// The finalized block is behind committed ingestion progress (a checkpoint
    /// exists) -- a finality regression that pauses ingestion until it advances.
    FinalityBehindCheckpoint,
    /// Finality has not yet reached the `deployment_block`: either cold start (no
    /// checkpoint) or a checkpoint at/below finality with `deployment_block`
    /// configured ahead of it. Nothing to ingest yet; no committed progress is at
    /// risk.
    FinalityBehindDeployment,
}

impl<P: Provider + Clone> OrderFillMonitor<P> {
    /// One poll iteration: enqueue a backfill range for the unprocessed
    /// blocks up to the latest finalized block, unless a previous range
    /// is still in flight or there is nothing new to fetch.
    async fn poll_once(&mut self) -> Result<PollOutcome, OrderFillMonitorError> {
        // Overlap guard: while a previous range is still being processed
        // the checkpoint has not advanced, so a fresh enqueue would
        // re-scan the same blocks. During a long catch-up this would
        // otherwise stack one growing range per tick.
        if self.backfill_queue.has_in_flight().await? {
            debug!(
                target: "orderbook",
                "Skipping poll: a backfill range is still in flight"
            );
            return Ok(PollOutcome::RangeInFlight);
        }

        // Load the checkpoint once per tick: `from_block` is derived from it, and
        // both the null-finality guard and the finality-regression branch use
        // whether it exists to tell an expected cold-start skip from a
        // checkpoint-backed skip that signals an RPC finality anomaly. One read
        // keeps those decisions consistent (no intra-tick TOCTOU) and avoids a
        // redundant round-trip.
        let checkpoint = load_backfill_checkpoint(&self.pool, &self.evm_ctx).await?;

        // Cutoff is the chain's latest finalized block. Backfill is guaranteed
        // not to ingest logs above this boundary, and a finalized block cannot
        // reorg, so an ingested fill can never be invalidated. `None` means the
        // node reports no finalized block yet (a chain shallower than finality);
        // there is nothing safe to ingest, so skip the tick.
        let Some(cutoff_block) = latest_finalized_block(&self.provider).await? else {
            // No finalized block reported. Once a checkpoint exists the chain
            // has demonstrably finalized blocks before, so a null response is an
            // RPC problem (a load-balanced node returning no finality) worth a
            // warning -- ingestion stalls until it clears. At cold start on a
            // chain shallower than finality it is expected, so stay quiet.
            if checkpoint.is_some() {
                warn!(
                    target: "orderbook",
                    "No finalized block reported while a checkpoint exists; the RPC \
                     node may be returning no finality. Ingestion paused this tick"
                );
                return Ok(PollOutcome::NoFinalityWithCheckpoint);
            }

            debug!(
                target: "orderbook",
                "No finalized block reported yet (chain shallower than \
                 finality); nothing to ingest this tick"
            );
            return Ok(PollOutcome::NoFinalityColdStart);
        };

        let from_block = backfill_start_from_checkpoint(checkpoint, self.evm_ctx.deployment_block);

        if from_block <= cutoff_block {
            info!(
                target: "orderbook",
                from_block,
                cutoff_block,
                "Enqueuing order-fill backfill range up to the finalized block"
            );

            self.backfill_queue
                .push(BackfillRange {
                    from_block,
                    to_block: cutoff_block,
                })
                .await?;

            return Ok(PollOutcome::Enqueued {
                from_block,
                to_block: cutoff_block,
            });
        }

        // Nothing to ingest this tick: the next block is past the finalized tip.
        // Classify why from committed progress (the checkpoint), not from
        // `from_block` -- the `deployment_block` floor can push `from_block` past
        // the cutoff even when the checkpoint is well behind it, so branching on
        // `from_block` would misreport that config case as a finality regression.
        match checkpoint {
            // Committed progress is past the finalized block: either finality
            // regressed (a stale or load-balanced RPC) or, right after upgrading
            // from the old confirmation-depth cutoff, finality has not yet caught
            // up to a checkpoint that ran ahead of it. Ingestion pauses either way.
            Some(checkpoint) if checkpoint > cutoff_block => {
                warn!(
                    target: "orderbook",
                    checkpoint,
                    cutoff_block,
                    "Finalized block is behind committed ingestion progress; ingestion \
                     paused until finality advances. Expected briefly after upgrading \
                     from the confirmation-depth cutoff; if it persists the RPC node \
                     may be returning stale finality data"
                );
                Ok(PollOutcome::FinalityBehindCheckpoint)
            }
            // The checkpoint has reached the finalized block; nothing new yet.
            Some(_) if from_block == cutoff_block.saturating_add(1) => {
                debug!(
                    target: "orderbook",
                    from_block,
                    cutoff_block,
                    "Caught up; nothing to enqueue this tick"
                );
                Ok(PollOutcome::CaughtUp)
            }
            // Either cold start (no checkpoint) or a checkpoint at/below finality
            // with `deployment_block` configured ahead of it: both are simply
            // waiting for finality to reach the deployment block, with no committed
            // progress at risk. Expected, so stay quiet.
            _ => {
                debug!(
                    target: "orderbook",
                    from_block,
                    cutoff_block,
                    "Finality has not yet reached the deployment block; nothing to \
                     ingest this tick"
                );
                Ok(PollOutcome::FinalityBehindDeployment)
            }
        }
    }
}

/// The chain's latest finalized block number, or `None` when the node reports
/// no finalized block yet (e.g. a chain shallower than finality). A finalized
/// block cannot reorg, so capping ingestion at it is real reorg protection for
/// single-chain operation -- unlike the previous `tip - required_confirmations`
/// heuristic, which reused the transaction-submission confirmation depth and was
/// not true finality.
///
/// The `None` arm relies on the Ethereum execution-api JSON-RPC convention that
/// `eth_getBlockByNumber` returns a `null` result (not an error) for a block tag
/// it cannot resolve yet -- which alloy maps to `Ok(None)`. A node that instead
/// errors on the `finalized` tag (unsupported method, wrong network behind a
/// broken proxy) surfaces as the `Err` arm, which
/// [`probe_finalized_block_support`] rejects at startup and `poll_once` treats
/// as a transient RPC failure (checkpoint frozen, retried next tick).
pub(crate) async fn latest_finalized_block<P: Provider>(
    provider: &P,
) -> Result<Option<u64>, RpcError<TransportErrorKind>> {
    Ok(provider
        .get_block_by_number(BlockNumberOrTag::Finalized)
        .await?
        .map(|block| block.header.number))
}

/// The outcome of [`probe_finalized_block_support`]. Logged inside the probe and
/// returned so [`crate::conductor`]'s startup decision and tests can observe
/// which finality state the endpoint is in. [`FinalityProbe::AliasesChainTip`]
/// and an `Err` are both fatal at the conductor; [`FinalityProbe::NotYetAvailable`]
/// is the only non-fatal soft signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FinalityProbe {
    /// The endpoint reports a finalized block strictly behind the chain tip --
    /// genuine finality, as expected.
    Supported,
    /// The endpoint reports no finalized block yet (a chain shallower than
    /// finality); ingestion is deferred until finality becomes available.
    NotYetAvailable,
    /// The endpoint reports a finalized block at or ahead of the chain tip,
    /// suggesting it aliases the `finalized` tag to `latest` -- which would
    /// silently disable the reorg protection this monitor depends on.
    AliasesChainTip,
}

/// Verifies at startup that the RPC endpoint can serve a usable `finalized` block
/// tag, so a misconfigured endpoint surfaces at boot instead of letting the bot
/// run while silently undermining reorg protection. Outcomes:
///
/// - An error (unsupported method, wrong-network proxy) is the `Err` arm, which
///   the caller propagates to fail startup -- mirroring the basic reachability
///   probe in [`crate::conductor`].
/// - `None` is the only non-fatal soft signal: a chain shallower than finality (a
///   fresh test chain) legitimately has no finalized block yet, and failing would
///   be racy against finality catching up. Logged at `warn` so the deferral is
///   visible.
/// - `finalized >= chain_tip` means the endpoint reports a finalized block at or
///   beyond the tip, which only an endpoint aliasing `finalized` to `latest`
///   does -- it would restore near-tip ingestion with no reorg protection. The
///   conductor treats this as fatal. The comparison assumes finality lag dwarfs
///   any cross-backend skew on a load-balanced RPC, which holds on the deployment
///   target (Base finalizes ~hundreds of blocks behind the tip, far more than a
///   few-block skew); detection fundamentally needs a tip read taken no later
///   than `finalized`, so the caller passes the tip it read just before this
///   probe rather than this probe re-reading a fresher (and thus always-ahead)
///   tip.
pub(crate) async fn probe_finalized_block_support<P: Provider>(
    provider: &P,
    chain_tip: u64,
) -> Result<FinalityProbe, RpcError<TransportErrorKind>> {
    let Some(finalized) = latest_finalized_block(provider).await? else {
        warn!(
            target: "orderbook",
            "RPC endpoint reports no finalized block at startup; order-fill \
             ingestion will not begin until the endpoint exposes finality"
        );
        return Ok(FinalityProbe::NotYetAvailable);
    };

    if finalized >= chain_tip {
        warn!(
            target: "orderbook",
            finalized,
            chain_tip,
            "RPC endpoint reports a finalized block at or ahead of the chain tip; \
             it may be aliasing the `finalized` tag to `latest`, which would \
             silently disable reorg protection"
        );
        return Ok(FinalityProbe::AliasesChainTip);
    }

    Ok(FinalityProbe::Supported)
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::rpc::types::{Block, Transaction};
    use serde_json::Value;
    use sqlx::{ConnectOptions, SqlitePool};

    use super::*;
    use crate::test_utils::setup_test_pools;

    /// Builds a mock provider whose `eth_getBlockByNumber("finalized")`
    /// resolves to a block at `number`, so `latest_finalized_block` returns it.
    fn finalized_at(number: u64) -> Block {
        let mut block = Block::<Transaction>::default();
        block.header.inner.number = number;
        block
    }

    fn provider_with_finalized(number: u64) -> impl Provider + Clone {
        let asserter = Asserter::new();
        asserter.push_success(&finalized_at(number));
        ProviderBuilder::new().connect_mocked_client(asserter)
    }

    /// A provider whose node reports no finalized block yet (null response).
    fn provider_without_finalized() -> impl Provider + Clone {
        let asserter = Asserter::new();
        asserter.push_success(&Value::Null);
        ProviderBuilder::new().connect_mocked_client(asserter)
    }

    async fn backfill_job_count(apalis_pool: &apalis_sqlite::SqlitePool) -> i64 {
        sqlx_apalis::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM Jobs WHERE job_type LIKE '%BackfillRange%'",
        )
        .fetch_one(apalis_pool)
        .await
        .unwrap()
    }

    async fn setup<P>(
        provider: P,
    ) -> (
        OrderFillMonitor<P>,
        SqlitePool,
        apalis_sqlite::SqlitePool,
        EvmCtx,
    ) {
        setup_with_deployment_block(provider, 1).await
    }

    async fn setup_with_deployment_block<P>(
        provider: P,
        deployment_block: u64,
    ) -> (
        OrderFillMonitor<P>,
        SqlitePool,
        apalis_sqlite::SqlitePool,
        EvmCtx,
    ) {
        let (pool, apalis_pool) = setup_test_pools().await;
        let backfill_queue = BackfillJobQueue::new(&apalis_pool);

        let evm_ctx = EvmCtx {
            rpc_url: url::Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block,
            // No longer used by the monitor cutoff (it caps at the finalized
            // block); kept on the ctx for transaction-submission paths.
            required_confirmations: 0,
        };

        let monitor = OrderFillMonitor::new(
            evm_ctx.clone(),
            backfill_queue,
            pool.clone(),
            provider,
            Duration::from_secs(5),
        );

        (monitor, pool, apalis_pool, evm_ctx)
    }

    async fn loaded_backfill(apalis_pool: &apalis_sqlite::SqlitePool) -> BackfillRange {
        let job_payload = sqlx_apalis::query_scalar::<_, Vec<u8>>(
            "SELECT job FROM Jobs WHERE job_type LIKE '%BackfillRange%'",
        )
        .fetch_one(apalis_pool)
        .await
        .unwrap();
        serde_json::from_slice(&job_payload).unwrap()
    }

    #[tokio::test]
    async fn poll_once_enqueues_range_up_to_finalized_block() {
        // checkpoint=99, finalized=102 -> from=100, cutoff=102.
        let (mut monitor, pool, apalis_pool, evm_ctx) = setup(provider_with_finalized(102)).await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 99)
            .await
            .unwrap();

        let outcome = monitor.poll_once().await.unwrap();

        assert_eq!(
            outcome,
            PollOutcome::Enqueued {
                from_block: 100,
                to_block: 102
            }
        );
        assert_eq!(backfill_job_count(&apalis_pool).await, 1);
        let job = loaded_backfill(&apalis_pool).await;
        assert_eq!(job.from_block, 100, "backfill must resume after checkpoint");
        assert_eq!(job.to_block, 102, "cutoff must equal the finalized block");
    }

    #[tokio::test]
    async fn poll_once_uses_deployment_block_without_checkpoint() {
        // No checkpoint: from_block falls back to deployment_block (1).
        let (mut monitor, _pool, apalis_pool, _evm_ctx) = setup(provider_with_finalized(50)).await;

        let outcome = monitor.poll_once().await.unwrap();

        assert_eq!(
            outcome,
            PollOutcome::Enqueued {
                from_block: 1,
                to_block: 50
            }
        );
        let job = loaded_backfill(&apalis_pool).await;
        assert_eq!(job.from_block, 1, "first run resumes from deployment_block");
        assert_eq!(job.to_block, 50);
    }

    #[tokio::test]
    async fn poll_once_skips_when_caught_up() {
        // checkpoint=102, finalized=102 -> from=103 > cutoff.
        let (mut monitor, pool, apalis_pool, evm_ctx) = setup(provider_with_finalized(102)).await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 102)
            .await
            .unwrap();

        let outcome = monitor.poll_once().await.unwrap();

        assert_eq!(outcome, PollOutcome::CaughtUp);
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            0,
            "nothing to enqueue when the checkpoint has reached the finalized block"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_once_skips_when_finalized_behind_checkpoint() {
        // A load-balanced RPC returns a finalized block (150) below the already
        // persisted checkpoint (200) -> from_block=201 is >1 past the cutoff.
        // Ingestion must pause (warn path) without enqueuing or moving anything.
        let (mut monitor, pool, apalis_pool, evm_ctx) = setup(provider_with_finalized(150)).await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 200)
            .await
            .unwrap();

        let outcome = monitor.poll_once().await.unwrap();

        assert_eq!(
            outcome,
            PollOutcome::FinalityBehindCheckpoint,
            "a finalized block behind the checkpoint is a finality regression"
        );
        assert!(
            logs_contain("Finalized block is behind committed ingestion progress"),
            "the finality-regression pause must warn operators"
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            0,
            "a finalized block behind the checkpoint must not enqueue a range"
        );
        assert_eq!(
            crate::onchain::backfill::load_backfill_checkpoint(&pool, &evm_ctx)
                .await
                .unwrap(),
            Some(200),
            "the checkpoint must not move when finalized regresses behind it"
        );
    }

    #[tokio::test]
    async fn poll_once_skips_when_finality_behind_deployment_block() {
        // Cold start (no checkpoint): finality (genesis block 0) sits below the
        // deployment block (1), so there is nothing to ingest yet. `from_block`
        // (1) == `cutoff` (0) + 1, but with no committed progress this is NOT
        // "caught up" -- it is the deployment block one short of finalizing, so
        // the outcome must be FinalityBehindDeployment, not CaughtUp.
        let (mut monitor, _pool, apalis_pool, _evm_ctx) = setup(provider_with_finalized(0)).await;

        let outcome = monitor.poll_once().await.unwrap();

        assert_eq!(
            outcome,
            PollOutcome::FinalityBehindDeployment,
            "cold start with finality below deployment_block must not report CaughtUp"
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            0,
            "nothing to ingest until finality reaches the deployment block"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_once_does_not_warn_when_deployment_block_is_ahead_of_finality() {
        // Unusual config: a low stale checkpoint (5) but deployment_block (100)
        // configured ahead of the finalized block (10). `from_block` is driven by
        // the deployment floor to 100 > cutoff + 1, but committed progress (5) is
        // NOT past finality -- so this must be the quiet deployment-wait, not the
        // loud finality-regression warning that blames the RPC.
        let (mut monitor, pool, apalis_pool, evm_ctx) =
            setup_with_deployment_block(provider_with_finalized(10), 100).await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 5)
            .await
            .unwrap();

        let outcome = monitor.poll_once().await.unwrap();

        assert_eq!(
            outcome,
            PollOutcome::FinalityBehindDeployment,
            "a deployment_block ahead of finality is not a finality regression"
        );
        assert!(
            !logs_contain("behind committed ingestion progress"),
            "must not blame the RPC for a deployment_block/checkpoint config gap"
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            0,
            "nothing to ingest until finality reaches the deployment block"
        );
    }

    #[tokio::test]
    async fn poll_once_does_not_enqueue_when_no_finalized_block_yet() {
        // No finalized block and no checkpoint (cold start) -> skip the tick
        // without enqueuing anything.
        let (mut monitor, _pool, apalis_pool, _evm_ctx) = setup(provider_without_finalized()).await;

        let outcome = monitor.poll_once().await.unwrap();

        assert_eq!(
            outcome,
            PollOutcome::NoFinalityColdStart,
            "no checkpoint + null finality is an expected cold-start skip"
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            0,
            "no finalized block to ingest yet -> no enqueue"
        );
    }

    /// A null finalized response while a checkpoint already exists must skip the
    /// tick without moving the checkpoint (the financial invariant: a skip never
    /// advances ingestion past unverified blocks), and a later tick that sees a
    /// real finalized block must resume from exactly where it left off.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_once_skips_intermittent_null_without_moving_checkpoint() {
        let asserter = Asserter::new();
        asserter.push_success(&Value::Null);
        asserter.push_success(&finalized_at(105));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let (mut monitor, pool, apalis_pool, evm_ctx) = setup(provider).await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 99)
            .await
            .unwrap();

        // First tick: node returns null finality -> skip, checkpoint frozen.
        // A checkpoint exists, so this is the warn branch, distinct from the
        // cold-start debug branch despite identical side effects.
        let first = monitor.poll_once().await.unwrap();
        assert_eq!(
            first,
            PollOutcome::NoFinalityWithCheckpoint,
            "null finality while a checkpoint exists is the warn (regression) branch"
        );
        assert!(
            logs_contain("No finalized block reported while a checkpoint exists"),
            "the warn branch must emit an operator-visible warning"
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            0,
            "a null finalized response must not enqueue a range"
        );
        assert_eq!(
            crate::onchain::backfill::load_backfill_checkpoint(&pool, &evm_ctx)
                .await
                .unwrap(),
            Some(99),
            "the checkpoint must not move when finality is momentarily null"
        );

        // Second tick: finality is back -> resume from exactly checkpoint+1.
        let second = monitor.poll_once().await.unwrap();
        assert_eq!(
            second,
            PollOutcome::Enqueued {
                from_block: 100,
                to_block: 105
            },
            "a recovered finalized response resumes from checkpoint+1"
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            1,
            "a recovered finalized response must resume ingestion"
        );
        let job = loaded_backfill(&apalis_pool).await;
        assert_eq!(job.from_block, 100, "resume from exactly checkpoint+1");
        assert_eq!(
            job.to_block, 105,
            "cutoff equals the recovered finalized block"
        );
    }

    /// A severed RPC must surface as a typed error -- the run loop's
    /// warn-and-retry contract depends on it -- and must neither enqueue
    /// a bogus range nor move the checkpoint, so the post-outage tick
    /// re-scans exactly the blocks the outage hid.
    #[tokio::test]
    async fn poll_once_propagates_rpc_error_without_enqueuing() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("connection refused");
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let (mut monitor, pool, apalis_pool, evm_ctx) = setup(provider).await;

        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 99)
            .await
            .unwrap();

        let result = monitor.poll_once().await;
        assert!(
            matches!(result, Err(OrderFillMonitorError::Rpc(_))),
            "A severed RPC must surface as a typed RPC error; got: {result:?}",
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            0,
            "No backfill range may be enqueued off a failed finalized-block read"
        );
        assert_eq!(
            crate::onchain::backfill::load_backfill_checkpoint(&pool, &evm_ctx)
                .await
                .unwrap(),
            Some(99),
            "The checkpoint must not move during the outage"
        );
    }

    /// A write-locked database during the backfill enqueue must surface
    /// as a typed error (the run loop logs it and retries next tick),
    /// leave nothing enqueued, and enqueue cleanly once the lock clears.
    /// The lock is a second connection holding `BEGIN IMMEDIATE` against
    /// a file-backed database -- the documented bot-vs-reporter WAL
    /// write contention -- with the pool's busy timeout scaled down so
    /// the test waits milliseconds instead of production's 10 seconds.
    #[tokio::test]
    async fn poll_once_surfaces_enqueue_error_under_db_lock_and_resumes() {
        let (pool, apalis_pool, db_path, _dir) =
            crate::test_utils::setup_file_backed_test_db(Duration::from_millis(250)).await;
        let backfill_queue = BackfillJobQueue::new(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: url::Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
        };

        // One finalized-block response per poll_once call.
        let asserter = Asserter::new();
        asserter.push_success(&finalized_at(50));
        asserter.push_success(&finalized_at(50));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let mut monitor = OrderFillMonitor::new(
            evm_ctx,
            backfill_queue,
            pool.clone(),
            provider,
            Duration::from_secs(5),
        );

        let mut locker = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&db_path)
            .connect()
            .await
            .unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut locker)
            .await
            .unwrap();

        // The in-flight check is a read and succeeds under the WAL write
        // lock; the enqueue INSERT blocks, then errors after the busy
        // timeout.
        let result = monitor.poll_once().await;
        assert!(
            matches!(result, Err(OrderFillMonitorError::Enqueue(_))),
            "A write-locked database must surface as a typed enqueue \
             error; got: {result:?}",
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            0,
            "No backfill range may be enqueued while the lock is held"
        );

        sqlx::query("ROLLBACK").execute(&mut locker).await.unwrap();

        monitor.poll_once().await.unwrap();
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            1,
            "The next tick after the lock clears must enqueue the range"
        );

        let job = loaded_backfill(&apalis_pool).await;
        assert_eq!(job.from_block, 1, "checkpoint must not have advanced");
        assert_eq!(job.to_block, 50);
    }

    #[tokio::test]
    async fn poll_once_skips_while_backfill_in_flight() {
        // A pending BackfillRange already exists: the overlap guard must
        // short-circuit before enqueuing (and before touching the provider,
        // so the empty asserter is never polled).
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let (mut monitor, pool, apalis_pool, evm_ctx) = setup(provider).await;

        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 10)
            .await
            .unwrap();
        let mut other_handle = BackfillJobQueue::new(&apalis_pool);
        other_handle
            .push(BackfillRange {
                from_block: 11,
                to_block: 20,
            })
            .await
            .unwrap();
        assert_eq!(backfill_job_count(&apalis_pool).await, 1);

        let outcome = monitor.poll_once().await.unwrap();

        assert_eq!(
            outcome,
            PollOutcome::RangeInFlight,
            "the overlap guard must return RangeInFlight"
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
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
        let (mut monitor, pool, apalis_pool, evm_ctx) = setup(provider_with_finalized(105)).await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 10)
            .await
            .unwrap();

        let mut queue = BackfillJobQueue::new(&apalis_pool);
        queue
            .push(BackfillRange {
                from_block: 11,
                to_block: 20,
            })
            .await
            .unwrap();
        // Simulate a crash mid-process: the row is stuck `Running` with no
        // live worker owning it.
        sqlx_apalis::query(
            "UPDATE Jobs SET status = 'Running' WHERE job_type LIKE '%BackfillRange%'",
        )
        .execute(&apalis_pool)
        .await
        .unwrap();

        // The wedge: the poller skips while the orphan counts as in flight.
        monitor.poll_once().await.unwrap();
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            1,
            "orphaned Running range still blocks the overlap guard"
        );

        // Startup recovery resets the orphan so a fresh worker can re-drive it.
        let reset = queue.requeue_orphaned().await.unwrap();
        assert_eq!(reset, 1, "the orphaned backfill range is requeued");

        let status = sqlx_apalis::query_scalar::<_, String>(
            "SELECT status FROM Jobs WHERE job_type LIKE '%BackfillRange%'",
        )
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        assert_eq!(status, "Pending", "requeue makes the orphan re-drivable");

        // A requeued (Pending) job still counts as in flight, so the wedge only
        // truly lifts once a worker drains it to a terminal state. Simulate that
        // completion, then confirm the next poll resumes ingestion -- it reaches
        // the finalized-block RPC (consuming the mock) and enqueues a fresh range.
        sqlx_apalis::query("UPDATE Jobs SET status = 'Done' WHERE job_type LIKE '%BackfillRange%'")
            .execute(&apalis_pool)
            .await
            .unwrap();

        monitor.poll_once().await.unwrap();
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            2,
            "once the orphan reaches a terminal state the poller enqueues a new range"
        );
    }

    #[tokio::test]
    async fn probe_finalized_block_support_accepts_a_finalized_block() {
        // Finalized (102) strictly behind the chain tip (110): genuine finality.
        let outcome = probe_finalized_block_support(&provider_with_finalized(102), 110)
            .await
            .unwrap();

        assert_eq!(outcome, FinalityProbe::Supported);
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn probe_finalized_block_support_accepts_missing_finality() {
        // A chain shallower than finality (null `finalized`) is a legitimate
        // cold-start state, so the startup probe must not reject it -- failing
        // here would be racy against finality catching up on a fresh chain.
        let outcome = probe_finalized_block_support(&provider_without_finalized(), 110)
            .await
            .unwrap();

        assert_eq!(outcome, FinalityProbe::NotYetAvailable);
        assert!(
            logs_contain("RPC endpoint reports no finalized block at startup"),
            "deferred ingestion at startup must be operator-visible"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn probe_finalized_block_support_flags_finalized_aliasing_the_tip() {
        // A provider that reports `finalized` at the chain tip (110 == 110) is
        // likely aliasing `finalized` to `latest`, which would silently disable
        // reorg protection -- the probe must flag it rather than accept it.
        let outcome = probe_finalized_block_support(&provider_with_finalized(110), 110)
            .await
            .unwrap();

        assert_eq!(outcome, FinalityProbe::AliasesChainTip);
        assert!(
            logs_contain("may be aliasing the `finalized` tag to `latest`"),
            "a tip-aliased finalized tag must be operator-visible"
        );
    }

    #[tokio::test]
    async fn probe_finalized_block_support_flags_finalized_strictly_ahead_of_tip() {
        // finalized (111) strictly ahead of the chain tip (110): a finalized read
        // beyond the tip can only mean the endpoint is not reporting real finality,
        // so the probe must flag it like the equal case (the `>` half of `>=`).
        let outcome = probe_finalized_block_support(&provider_with_finalized(111), 110)
            .await
            .unwrap();

        assert_eq!(outcome, FinalityProbe::AliasesChainTip);
    }

    #[tokio::test]
    async fn probe_finalized_block_support_rejects_an_rpc_error() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("connection refused");
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        // An endpoint that errors on the finalized tag must fail the startup
        // probe rather than letting the bot start and silently never ingest.
        let error = probe_finalized_block_support(&provider, 110)
            .await
            .unwrap_err();

        assert!(
            matches!(error, RpcError::ErrorResp(_)),
            "a finalized-tag RPC failure must surface as an error response; got: {error:?}"
        );
    }
}
