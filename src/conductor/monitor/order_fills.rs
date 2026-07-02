//! Supervised order fill monitor that drives continuous `eth_getLogs`
//! polling over a persisted checkpoint.
//!
//! Every `order_fill_poll_interval` seconds the monitor:
//! 1. Records a block-lag telemetry sample (chain tip, cutoff block,
//!    checkpoint) before any skip, so ingestion lag stays observable even
//!    during a long catch-up while a `BackfillRange` job is still in flight.
//!    Lag is measured against the cutoff block -- the actual ingestion
//!    boundary -- so a caught-up system reads zero.
//! 2. Skips the tick if a `BackfillRange` job is still in flight -- the
//!    checkpoint has not advanced yet, so re-enqueuing would re-scan the
//!    same blocks (and during a long catch-up would stack unbounded
//!    overlapping ranges).
//! 3. Reads the chain's latest cutoff block (tag configured via the required
//!    `ingestion_cutoff` setting; production configs use `safe`) and uses it
//!    as the ingestion boundary. `None` means the node has not yet surfaced
//!    this block tag; there is nothing safe to ingest, so the tick is skipped.
//! 4. Enqueues a `BackfillRange` job covering `(checkpoint+1, cutoff)`.
//!    The `backfill-worker` fetches the logs via HTTP `eth_getLogs`,
//!    pushes an accounting job per fill, and advances the checkpoint only
//!    on success -- so a failed range is retried and never silently
//!    skipped. Downstream dedupes by `(tx_hash, log_index)`.
//!
//! Each tick also records a poll-cycle telemetry sample (poll duration,
//! ticks skipped by `MissedTickBehavior::Skip`, and success/failure outcome)
//! and periodically prunes expired telemetry rows.
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
//! **Default cutoff: `safe` (configurable via `ingestion_cutoff`)**
//!
//! On OP Stack chains (e.g. Base), `safe` is the latest L2 block whose
//! sequencer batch has been posted to L1. It is typically only a few blocks
//! behind the chain tip rather than hundreds, cutting hedging lag from ~20 min
//! (with `finalized`) to ~seconds.
//!
//! **Reorg tradeoff (`safe` cutoff):** The `safe` block is not yet
//! L1-finalized (Casper FFG). A sufficiently deep L1 reorg that drops the
//! batch transaction before it finalizes could, in principle, cause the L2
//! block to be reorganized out, invalidating a fill that was ingested against
//! it. This trades fill-ingestion correctness for latency until full reorg
//! handling is implemented. In practice, L1 reorgs deep enough to drop a
//! submitted batch are extremely rare (single-slot reorgs, which are the most
//! common, do not affect posted batches in the same block). The bot currently
//! has no reversal path if a fill is invalidated; full cross-chain reorg
//! handling is tracked separately in the Reorg protection project.
//!
//! **Quiet-skew band tradeoff (`safe` cutoff):** When `safe` regresses below
//! the checkpoint within `SAFE_CUTOFF_QUIET_SKEW` blocks (normal cross-backend
//! RPC skew on a load-balanced fleet), the checkpoint is left frozen and the
//! reorged-and-replaced blocks below it are not re-scanned, so any
//! newly-canonical fills in that range are not ingested. This is consistent
//! with the no-reversal-path tradeoff above; full reorg handling is tracked
//! separately in the Reorg protection project.
//!
//! Operators who need strict reorg protection at the cost of hedging latency
//! can set `ingestion_cutoff = "finalized"` in config. `backfill_range` still
//! surfaces a `removed: true` log as a warning rather than masking it, as
//! defense in depth.

use std::time::{Duration, Instant};

use alloy::eips::BlockNumberOrTag;
use alloy::providers::Provider;
use alloy::transports::{RpcError, TransportErrorKind};
use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use task_supervisor::{SupervisedTask, TaskResult};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use st0x_config::{EvmCtx, IngestionCutoff};

use crate::conductor::job::QueuePushError;
use crate::onchain::OnChainError;
use crate::onchain::backfill::{
    BackfillJobQueue, BackfillRange, backfill_start_from_checkpoint, load_backfill_checkpoint,
};
use crate::telemetry::{
    BlockLagSample, Monitor, prune_expired, record_block_lag, record_poll_cycle,
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
        let mut previous_tick: Option<Instant> = None;
        let mut last_prune = Instant::now();

        loop {
            interval.tick().await;
            let tick = Instant::now();
            let elapsed = previous_tick.map(|previous| tick.duration_since(previous));
            let skipped = skipped_ticks(elapsed, self.poll_interval);
            previous_tick = Some(tick);

            let sampled_at = Utc::now();
            let result = self.poll_once(sampled_at).await;
            // Capture elapsed right after poll_once returns: this measures the
            // poll cycle's work -- including the in-cycle block-lag write
            // poll_once performs -- while excluding the poll-cycle telemetry
            // write below, which would otherwise measure itself.
            let poll_duration = tick.elapsed();

            // The poll-cycle outcome is a success/failure discriminator, so the
            // structured PollOutcome on the success path is collapsed to `()`.
            if let Err(error) = record_poll_cycle(
                &self.pool,
                Monitor::OrderFill,
                self.evm_ctx.orderbook,
                sampled_at,
                poll_duration,
                skipped,
                result.as_ref().map(|_| ()),
            )
            .await
            {
                warn!(target: "orderbook", ?error, "Failed to record poll-cycle telemetry");
            }

            if last_prune.elapsed() >= PRUNE_INTERVAL {
                last_prune = Instant::now();
                if let Err(error) = prune_expired(&self.pool, Utc::now()).await {
                    warn!(target: "orderbook", ?error, "Failed to prune expired telemetry");
                }
            }

            if let Err(error) = result {
                warn!(
                    target: "orderbook",
                    ?error,
                    "Order fill poll failed; retrying on next tick"
                );
            }
        }
    }
}

/// How often expired telemetry rows are pruned.
const PRUNE_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Ticks silently dropped by [`MissedTickBehavior::Skip`] since the previous
/// tick: under Skip a slow poll fires nothing for the missed slots, so the
/// only evidence is the wall-clock gap between consecutive ticks.
///
/// The accounting deliberately resets on a supervised restart
/// (`previous_tick` starts `None`): downtime across restarts shows up as a
/// gap in the `sampled_at` series rather than a skipped-tick count.
fn skipped_ticks(elapsed_since_previous_tick: Option<Duration>, poll_interval: Duration) -> u64 {
    let Some(elapsed) = elapsed_since_previous_tick else {
        return 0;
    };
    let interval_ms = poll_interval.as_millis().max(1);
    // Round to the nearest interval count to absorb timer jitter; intervals
    // beyond the first are skips.
    let elapsed_intervals = (elapsed.as_millis() + interval_ms / 2) / interval_ms;
    // Clamp at i64::MAX (not u64::MAX) so the value always survives the
    // storage layer's i64 conversion instead of failing the sample write.
    // The clamp guarantees capped <= i64::MAX, so i64::try_from succeeds;
    // unsigned_abs() then yields the equivalent u64 without sign extension.
    let capped = elapsed_intervals
        .saturating_sub(1)
        .min(u128::from(i64::MAX.unsigned_abs()));
    i64::try_from(capped)
        .map(i64::unsigned_abs)
        .unwrap_or(i64::MAX.unsigned_abs())
}

#[derive(Debug, thiserror::Error)]
enum OrderFillMonitorError {
    #[error("RPC error reading cutoff block: {0}")]
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
/// from a checkpoint-backed skip that signals an RPC cutoff anomaly. Those two
/// share identical side effects (no enqueue, checkpoint frozen), so without a
/// typed outcome an inverted checkpoint check could not be caught from the
/// outside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PollOutcome {
    /// A previous backfill range is still in flight; the tick was skipped.
    RangeInFlight,
    /// No cutoff block reported and no checkpoint exists yet -- expected
    /// cold-start lag on a chain where the block tag is not yet available.
    NoCutoffColdStart,
    /// No cutoff block reported while a checkpoint exists -- likely a stale
    /// or load-balanced RPC node; ingestion is paused.
    NoCutoffWithCheckpoint,
    /// A backfill range up to the cutoff block was enqueued.
    Enqueued { from_block: u64, to_block: u64 },
    /// Ingestion has reached the cutoff block; nothing to enqueue.
    CaughtUp,
    /// A `safe` cutoff block is slightly behind the checkpoint within
    /// `SAFE_CUTOFF_QUIET_SKEW` (normal cross-backend RPC skew). Ingestion
    /// skips this tick without the full-pause warning, but a `warn` still
    /// records the regression so possible reorgs remain observable.
    CutoffWithinQuietSkew,
    /// The cutoff block is behind committed ingestion progress under `finalized`,
    /// or significantly behind under `safe` -- a regression that pauses ingestion.
    CutoffBehindCheckpoint,
    /// The cutoff has not yet reached the `deployment_block`: either cold start
    /// (no checkpoint) or a checkpoint at/below the cutoff with `deployment_block`
    /// configured ahead of it. Nothing to ingest yet; no committed progress is at
    /// risk.
    CutoffBehindDeployment,
}

/// Maximum backwards deviation from the checkpoint that is tolerated quietly
/// under the `safe` cutoff. Cross-backend RPC skew on a load-balanced fleet
/// (e.g. dRPC on Base) can cause `safe` to momentarily report below the
/// checkpoint by a handful of blocks when consecutive reads land on backends
/// at different heads.
///
/// 32 is a conservative band chosen to absorb normal cross-backend skew
/// (typically a few blocks) while staying far below a finality-scale
/// regression that would indicate a real reorg or persistent RPC anomaly.
/// The exact value is a deliberate estimate pending measured Base `safe`-tag
/// skew data from the deployment fleet; it will be revised once measurements
/// are available. Under `finalized`, finality lag is hundreds of blocks, so
/// any cutoff < checkpoint is a meaningful regression worth a warning.
const SAFE_CUTOFF_QUIET_SKEW: u64 = 32;

/// Converts the configured ingestion cutoff to the alloy `BlockNumberOrTag`
/// used in RPC calls. Factored out so adding a new `IngestionCutoff` variant
/// produces a single compile error rather than two.
fn cutoff_block_tag(cutoff: IngestionCutoff) -> BlockNumberOrTag {
    match cutoff {
        IngestionCutoff::Safe => BlockNumberOrTag::Safe,
        IngestionCutoff::Finalized => BlockNumberOrTag::Finalized,
    }
}

impl<P: Provider + Clone> OrderFillMonitor<P> {
    /// One poll iteration: enqueue a backfill range for the unprocessed
    /// blocks up to the latest cutoff block, unless a previous range
    /// is still in flight or there is nothing new to fetch.
    async fn poll_once(
        &mut self,
        sampled_at: DateTime<Utc>,
    ) -> Result<PollOutcome, OrderFillMonitorError> {
        let cutoff_tag = cutoff_block_tag(self.evm_ctx.ingestion_cutoff);

        // Read the chain tip and latest cutoff block up front, before the
        // overlap guard, so the block-lag sample is recorded even while a
        // backfill is still in flight -- a long catch-up (or a stuck backfill)
        // is exactly when growing lag must stay observable. The cutoff block
        // read here is also the ingestion boundary used below.
        let chain_tip = self.provider.get_block_number().await?;
        let cutoff_opt = latest_cutoff_block(&self.provider, cutoff_tag).await?;

        // Block-lag telemetry is best-effort: a failed sample must never fail
        // the poll. Lag is measured against the cutoff block -- the real
        // ingestion boundary -- so a caught-up system reads zero rather than a
        // permanent floor. A null cutoff block records as 0, which saturates
        // lag to 0.
        let sampled_checkpoint = load_backfill_checkpoint(&self.pool, &self.evm_ctx).await?;
        let sample = BlockLagSample {
            sampled_at,
            orderbook: self.evm_ctx.orderbook,
            chain_tip,
            cutoff_block: cutoff_opt.unwrap_or(0),
            last_processed_block: sampled_checkpoint,
        };
        if let Err(error) = record_block_lag(&self.pool, &sample).await {
            warn!(target: "orderbook", ?error, "Failed to record block-lag telemetry");
        }

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

        // Re-read the checkpoint now that the guard has passed: an in-flight job
        // may have completed (advancing the checkpoint) between the telemetry
        // read above and the guard, and deriving the range from the stale value
        // would re-scan blocks that job just processed. This single post-guard
        // read is the source for both the null-cutoff branch and the range
        // derivation, keeping those decisions consistent (no intra-tick TOCTOU).
        let checkpoint = load_backfill_checkpoint(&self.pool, &self.evm_ctx).await?;

        // Cutoff is the chain's latest block for the configured tag. `None`
        // means the node has not yet surfaced this block tag (e.g. cold start on
        // a chain where the tag is not yet available); there is nothing safe to
        // ingest, so skip the tick.
        let Some(cutoff_block) = cutoff_opt else {
            // No cutoff block reported. Once a checkpoint exists the tag was
            // available before, so a null response is likely an RPC problem worth
            // a warning -- ingestion stalls until it clears. At cold start it is
            // expected, so stay quiet.
            if checkpoint.is_some() {
                warn!(
                    target: "orderbook",
                    "No cutoff block reported while a checkpoint exists; the RPC \
                     node may be returning a stale response for the configured tag. \
                     Ingestion paused this tick"
                );
                return Ok(PollOutcome::NoCutoffWithCheckpoint);
            }

            // Cold start: no checkpoint means ingestion has never run, so
            // the tag being absent is expected and transient. The startup
            // probe (`probe_cutoff_block_support`) already emits a one-time
            // warn if the endpoint persistently returns null, so a persistent
            // unsupported endpoint is not silent. Stay quiet here.
            let cutoff = self.evm_ctx.ingestion_cutoff;
            debug!(
                target: "orderbook",
                %cutoff,
                "No cutoff block at cold start; expected briefly -- startup \
                 probe already surfaced a persistent null"
            );
            return Ok(PollOutcome::NoCutoffColdStart);
        };

        let from_block = backfill_start_from_checkpoint(checkpoint, self.evm_ctx.deployment_block);

        if from_block <= cutoff_block {
            info!(
                target: "orderbook",
                from_block,
                cutoff_block,
                "Enqueuing order-fill backfill range up to the cutoff block"
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

        // Nothing to ingest this tick: the next block is past the cutoff.
        // Classify why from committed progress (the checkpoint), not from
        // `from_block` -- the `deployment_block` floor can push `from_block`
        // past the cutoff even when the checkpoint is well behind it, so
        // branching on `from_block` would misreport that config case as a
        // cutoff regression.
        match checkpoint {
            // Committed progress is past the cutoff block. Under `finalized`,
            // any such regression is an RPC anomaly worth a warning (finality
            // lag is hundreds of blocks, dwarfing any cross-backend skew).
            // Under `safe`, small backwards steps (within SAFE_CUTOFF_QUIET_SKEW
            // blocks) are the on-chain signature of either benign cross-backend
            // RPC skew on a load-balanced fleet OR a small reorg. Either way,
            // ingestion skips this tick without pausing the monitor. "Quiet"
            // means "do not pause ingestion", NOT "do not log" -- the regression
            // is surfaced at warn with magnitude so an in-band reorg is
            // observable. The regressed range below the frozen checkpoint is
            // not re-scanned (no reversal path). Larger regressions still warn
            // via the shared warn! below and pause ingestion.
            Some(checkpoint) if checkpoint > cutoff_block => {
                let regression = checkpoint - cutoff_block;
                let is_safe = match self.evm_ctx.ingestion_cutoff {
                    IngestionCutoff::Safe => true,
                    IngestionCutoff::Finalized => false,
                };

                if is_safe && regression <= SAFE_CUTOFF_QUIET_SKEW {
                    // Safe cutoff stepped backwards within the quiet-skew
                    // window. May be benign cross-backend RPC skew or a small
                    // reorg. Ingestion skips this tick; the regressed range is
                    // not re-scanned (no reversal path).
                    warn!(
                        target: "orderbook",
                        checkpoint,
                        cutoff_block,
                        regression,
                        "Safe cutoff block stepped backwards below checkpoint \
                         (may be RPC skew or small reorg); skipping tick without \
                         re-scanning the regressed range"
                    );
                    return Ok(PollOutcome::CutoffWithinQuietSkew);
                }

                warn!(
                    target: "orderbook",
                    checkpoint,
                    cutoff_block,
                    "Cutoff block is behind committed ingestion progress; ingestion \
                     paused until the cutoff advances. Expected briefly after \
                     upgrading from the finalized to safe cutoff; if it persists \
                     the RPC node may be returning stale data"
                );
                Ok(PollOutcome::CutoffBehindCheckpoint)
            }
            // The checkpoint has reached the cutoff block; nothing new yet.
            Some(_) if from_block == cutoff_block.saturating_add(1) => {
                debug!(
                    target: "orderbook",
                    from_block,
                    cutoff_block,
                    "Caught up; nothing to enqueue this tick"
                );
                Ok(PollOutcome::CaughtUp)
            }
            // Either cold start (no checkpoint) or a checkpoint at/below the
            // cutoff with `deployment_block` configured ahead of it: both are
            // simply waiting for the cutoff to reach the deployment block, with
            // no committed progress at risk. Expected, so stay quiet.
            _ => {
                debug!(
                    target: "orderbook",
                    from_block,
                    cutoff_block,
                    "Cutoff has not yet reached the deployment block; nothing to \
                     ingest this tick"
                );
                Ok(PollOutcome::CutoffBehindDeployment)
            }
        }
    }
}

/// The block number for the given block tag, or `None` when the node has not
/// yet surfaced that tag (e.g. a fresh chain or a cold-start race).
///
/// Relies on the Ethereum execution-API JSON-RPC convention that
/// `eth_getBlockByNumber` returns a `null` result (not an error) for a tag it
/// cannot resolve yet -- which alloy maps to `Ok(None)`. A node that instead
/// errors (unsupported method, wrong-network proxy) surfaces as `Err`, which
/// [`probe_cutoff_block_support`] rejects at startup and `poll_once` treats as
/// a transient RPC failure (checkpoint frozen, retried next tick).
pub(crate) async fn latest_cutoff_block<P: Provider>(
    provider: &P,
    tag: BlockNumberOrTag,
) -> Result<Option<u64>, RpcError<TransportErrorKind>> {
    Ok(provider
        .get_block_by_number(tag)
        .await?
        .map(|block| block.header.number))
}

/// The outcome of [`probe_cutoff_block_support`]. Returned so
/// [`crate::conductor`]'s startup decision and tests can observe which state the
/// endpoint is in. An `Err` from the probe is always fatal at the conductor.
/// [`CutoffProbe::NotYetAvailable`] is the only non-fatal soft signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CutoffProbe {
    /// The endpoint returned a usable block for the configured cutoff tag.
    Supported,
    /// The endpoint returned no block for this tag yet (null response) --
    /// valid at cold start; ingestion is deferred until the tag is available.
    NotYetAvailable,
}

/// Error returned by [`probe_cutoff_block_support`].
#[derive(Debug, thiserror::Error)]
pub(crate) enum CutoffProbeError {
    /// RPC transport failure probing the cutoff block tag.
    #[error("RPC error probing cutoff block tag: {0}")]
    Rpc(#[from] RpcError<TransportErrorKind>),
    /// The `finalized` block tag is at or beyond the chain tip, indicating the
    /// endpoint aliases `finalized` to `latest` and silently disables reorg
    /// protection.
    #[error(
        "finalized block ({cutoff_block}) at or beyond chain tip ({chain_tip}); \
         endpoint likely aliases `finalized` to `latest`"
    )]
    FinalizedAliasesChainTip { cutoff_block: u64, chain_tip: u64 },
}

/// Verifies at startup that the RPC endpoint can serve the configured cutoff
/// block tag, so a misconfigured endpoint surfaces at boot instead of letting
/// the bot run silently degraded.
///
/// For `Safe`: confirms the endpoint returns a non-null, non-error response for
/// the `safe` tag. No aliasing check -- `safe` legitimately reaches or exceeds
/// the chain tip when the sequencer batch cadence is fast, so a near-tip value
/// is expected, not an error. The probe only verifies tag support.
///
/// For `Finalized`: additionally checks that `finalized < chain_tip`. If
/// `finalized >= chain_tip` the endpoint is likely aliasing `finalized` to
/// `latest`, which would silently disable reorg protection. This case is fatal
/// at the conductor (returned as `Err`). The comparison assumes finality lag
/// (~hundreds of blocks on Base) dwarfs any cross-backend skew; the caller
/// passes the tip it read just before this probe so the comparison uses a
/// consistent read pair.
///
/// Outcomes:
/// - `Err`: RPC error (unsupported method, wrong-network proxy) or detected
///   `finalized` aliasing -- both are fatal at the conductor.
/// - `Ok(NotYetAvailable)`: tag not yet available (null response); logged and
///   allowed through so a fresh chain does not fail startup.
/// - `Ok(Supported)`: endpoint supports the tag.
pub(crate) async fn probe_cutoff_block_support<P: Provider>(
    provider: &P,
    chain_tip: u64,
    cutoff: IngestionCutoff,
) -> Result<CutoffProbe, CutoffProbeError> {
    let tag = cutoff_block_tag(cutoff);

    let Some(cutoff_block) = latest_cutoff_block(provider, tag).await? else {
        warn!(
            target: "orderbook",
            %cutoff,
            "RPC endpoint reports no {cutoff} block at startup; order-fill \
             ingestion will not begin until the endpoint exposes it",
        );
        return Ok(CutoffProbe::NotYetAvailable);
    };

    // Only check aliasing for `finalized`: on `finalized`, a value at or
    // beyond the tip is only possible if the endpoint aliases the tag to
    // `latest`, disabling reorg protection. On `safe`, the cutoff can
    // legitimately reach or slightly exceed the tip (fast sequencer batching),
    // so this check would false-positive on startup.
    let check_aliasing = match cutoff {
        IngestionCutoff::Safe => false,
        IngestionCutoff::Finalized => true,
    };
    if check_aliasing && cutoff_block >= chain_tip {
        return Err(CutoffProbeError::FinalizedAliasesChainTip {
            cutoff_block,
            chain_tip,
        });
    }

    Ok(CutoffProbe::Supported)
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

    /// Builds a mock block at `number` for use in mock provider responses.
    fn finalized_at(number: u64) -> Block {
        let mut block = Block::<Transaction>::default();
        block.header.inner.number = number;
        block
    }

    /// Single cutoff-block read, for [`probe_cutoff_block_support`] tests
    /// (the probe reads only the cutoff block; the tip is passed in).
    fn provider_with_cutoff(number: u64) -> impl Provider + Clone {
        let asserter = Asserter::new();
        asserter.push_success(&finalized_at(number));
        ProviderBuilder::new().connect_mocked_client(asserter)
    }

    /// A provider whose node reports no cutoff block yet (null response),
    /// for [`probe_cutoff_block_support`] tests (single cutoff read).
    fn provider_without_cutoff() -> impl Provider + Clone {
        let asserter = Asserter::new();
        asserter.push_success(&Value::Null);
        ProviderBuilder::new().connect_mocked_client(asserter)
    }

    /// Queues the two RPC responses a single `poll_once` tick reads, in order:
    /// `eth_blockNumber` (the chain tip) then `eth_getBlockByNumber(<tag>)`.
    /// `cutoff: None` queues a JSON null -- a node reporting the tag is not
    /// yet available.
    fn push_tick(asserter: &Asserter, chain_tip: u64, cutoff: Option<u64>) {
        asserter.push_success(&Value::from(chain_tip));
        match cutoff {
            Some(number) => asserter.push_success(&finalized_at(number)),
            None => asserter.push_success(&Value::Null),
        }
    }

    /// Mock provider answering a single `poll_once` tick: chain tip `chain_tip`
    /// then cutoff block `cutoff`.
    fn provider_at(chain_tip: u64, cutoff: u64) -> impl Provider + Clone {
        let asserter = Asserter::new();
        push_tick(&asserter, chain_tip, Some(cutoff));
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
        setup_with_deployment_block_and_cutoff(provider, deployment_block, IngestionCutoff::Safe)
            .await
    }

    async fn setup_with_deployment_block_and_cutoff<P>(
        provider: P,
        deployment_block: u64,
        ingestion_cutoff: IngestionCutoff,
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
            inventory: address!("0x1111111111111111111111111111111111111111"),
            vault_owner: None,
            deployment_block,
            required_confirmations: 0,
            ingestion_cutoff,
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
    async fn poll_once_enqueues_range_up_to_cutoff_block() {
        // checkpoint=99, cutoff=102 -> from=100, cutoff=102.
        let (mut monitor, pool, apalis_pool, evm_ctx) = setup(provider_at(110, 102)).await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 99)
            .await
            .unwrap();

        let outcome = monitor.poll_once(Utc::now()).await.unwrap();

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
        assert_eq!(job.to_block, 102, "cutoff must equal the cutoff block");
    }

    #[tokio::test]
    async fn poll_once_uses_deployment_block_without_checkpoint() {
        // No checkpoint: from_block falls back to deployment_block (1).
        let (mut monitor, _pool, apalis_pool, _evm_ctx) = setup(provider_at(55, 50)).await;

        let outcome = monitor.poll_once(Utc::now()).await.unwrap();

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
        // checkpoint=102, cutoff=102 -> from=103 > cutoff.
        let (mut monitor, pool, apalis_pool, evm_ctx) = setup(provider_at(110, 102)).await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 102)
            .await
            .unwrap();

        let outcome = monitor.poll_once(Utc::now()).await.unwrap();

        assert_eq!(outcome, PollOutcome::CaughtUp);
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            0,
            "nothing to enqueue when the checkpoint has reached the cutoff block"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_once_pauses_when_finalized_cutoff_far_behind_checkpoint() {
        // A load-balanced RPC returns a finalized block (150) far below the
        // already-persisted checkpoint (200). Under `finalized`, any regression
        // is significant and must pause with a warning.
        let (mut monitor, pool, apalis_pool, evm_ctx) = setup_with_deployment_block_and_cutoff(
            provider_at(205, 150),
            1,
            IngestionCutoff::Finalized,
        )
        .await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 200)
            .await
            .unwrap();

        let outcome = monitor.poll_once(Utc::now()).await.unwrap();

        assert_eq!(
            outcome,
            PollOutcome::CutoffBehindCheckpoint,
            "a finalized cutoff block behind the checkpoint is a cutoff regression"
        );
        assert!(
            logs_contain("Cutoff block is behind committed ingestion progress"),
            "the cutoff-regression pause must warn operators"
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            0,
            "a finalized cutoff behind the checkpoint must not enqueue a range"
        );
        assert_eq!(
            crate::onchain::backfill::load_backfill_checkpoint(&pool, &evm_ctx)
                .await
                .unwrap(),
            Some(200),
            "the checkpoint must not move when the finalized cutoff regresses"
        );
    }

    /// Under `finalized`, even a 1-block regression (checkpoint=200,
    /// finalized=199) is significant and must pause ingestion with a warning.
    /// This proves the mode boundary: the same 1-block regression under `safe`
    /// would quiet-skip (within SAFE_CUTOFF_QUIET_SKEW), but under `finalized`
    /// any cutoff < checkpoint is a meaningful anomaly.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_once_pauses_when_finalized_cutoff_one_behind_checkpoint() {
        // checkpoint=200, finalized=199 (1-block regression). Under `finalized`
        // any regression must pause with a warning, even a single block.
        let (mut monitor, pool, apalis_pool, evm_ctx) = setup_with_deployment_block_and_cutoff(
            provider_at(205, 199),
            1,
            IngestionCutoff::Finalized,
        )
        .await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 200)
            .await
            .unwrap();

        let outcome = monitor.poll_once(Utc::now()).await.unwrap();

        assert_eq!(
            outcome,
            PollOutcome::CutoffBehindCheckpoint,
            "a 1-block finalized regression must pause ingestion (no quiet-skew band under finalized)"
        );
        assert!(
            logs_contain("Cutoff block is behind committed ingestion progress"),
            "a 1-block finalized regression must warn operators"
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            0,
            "a 1-block finalized regression must not enqueue a range"
        );
        assert_eq!(
            crate::onchain::backfill::load_backfill_checkpoint(&pool, &evm_ctx)
                .await
                .unwrap(),
            Some(200),
            "the checkpoint must not move on a 1-block finalized regression"
        );
    }

    /// Under `safe`, a small backwards step (within SAFE_CUTOFF_QUIET_SKEW)
    /// skips ingestion for the tick and emits a warn (observable for reorg
    /// detection) without pausing the monitor.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_once_skips_quietly_when_safe_cutoff_slightly_behind_checkpoint() {
        // checkpoint=1000, safe=995 (5-block backwards skew). Within
        // SAFE_CUTOFF_QUIET_SKEW: must return CutoffWithinQuietSkew (not
        // CutoffBehindCheckpoint) and warn at the stepped-backwards level.
        let (mut monitor, pool, apalis_pool, evm_ctx) = setup_with_deployment_block_and_cutoff(
            provider_at(1005, 995),
            1,
            IngestionCutoff::Safe,
        )
        .await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 1000)
            .await
            .unwrap();

        let outcome = monitor.poll_once(Utc::now()).await.unwrap();

        assert_eq!(
            outcome,
            PollOutcome::CutoffWithinQuietSkew,
            "a small safe-cutoff skew must skip quietly (CutoffWithinQuietSkew), not pause ingestion"
        );
        assert!(
            logs_contain("Safe cutoff block stepped backwards below checkpoint"),
            "a small safe skew must warn so an in-band reorg is observable"
        );
        assert!(
            !logs_contain("Cutoff block is behind committed ingestion progress"),
            "a small safe skew must not emit the full-pause warning"
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            0,
            "a small safe skew must not enqueue a range"
        );
    }

    /// Under `safe`, a large regression (beyond SAFE_CUTOFF_QUIET_SKEW) still
    /// pauses ingestion with a warning.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_once_pauses_when_safe_cutoff_far_behind_checkpoint() {
        // checkpoint=200, safe=100 (100-block backwards skew). Exceeds
        // SAFE_CUTOFF_QUIET_SKEW (32), so ingestion must pause with a warning.
        let (mut monitor, pool, apalis_pool, evm_ctx) =
            setup_with_deployment_block_and_cutoff(provider_at(205, 100), 1, IngestionCutoff::Safe)
                .await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 200)
            .await
            .unwrap();

        let outcome = monitor.poll_once(Utc::now()).await.unwrap();

        assert_eq!(
            outcome,
            PollOutcome::CutoffBehindCheckpoint,
            "a large safe-cutoff regression must pause ingestion"
        );
        assert!(
            logs_contain("Cutoff block is behind committed ingestion progress"),
            "a large safe-cutoff regression must warn operators"
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            0,
            "a large safe regression must not enqueue a range"
        );
    }

    /// regression == SAFE_CUTOFF_QUIET_SKEW (32): must quiet-skip with a warn.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_once_skips_quietly_at_exact_skew_boundary() {
        // checkpoint=1032, safe=1000 (32-block regression == SAFE_CUTOFF_QUIET_SKEW).
        // Must return CutoffWithinQuietSkew (not pause) and warn.
        let (mut monitor, pool, apalis_pool, evm_ctx) = setup_with_deployment_block_and_cutoff(
            provider_at(1040, 1000),
            1,
            IngestionCutoff::Safe,
        )
        .await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 1032)
            .await
            .unwrap();

        let outcome = monitor.poll_once(Utc::now()).await.unwrap();

        assert_eq!(
            outcome,
            PollOutcome::CutoffWithinQuietSkew,
            "a regression of exactly SAFE_CUTOFF_QUIET_SKEW must quiet-skip"
        );
        assert!(
            logs_contain("Safe cutoff block stepped backwards below checkpoint"),
            "exact boundary must warn so an in-band reorg is observable"
        );
        assert!(
            !logs_contain("Cutoff block is behind committed ingestion progress"),
            "exact boundary must not emit the full-pause warning"
        );
        assert_eq!(backfill_job_count(&apalis_pool).await, 0);
    }

    /// regression == SAFE_CUTOFF_QUIET_SKEW + 1 (33): must pause with a warning.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_once_pauses_at_one_beyond_skew_boundary() {
        // checkpoint=1033, safe=1000 (33-block regression > SAFE_CUTOFF_QUIET_SKEW).
        // Must pause with a warning.
        let (mut monitor, pool, apalis_pool, evm_ctx) = setup_with_deployment_block_and_cutoff(
            provider_at(1040, 1000),
            1,
            IngestionCutoff::Safe,
        )
        .await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 1033)
            .await
            .unwrap();

        let outcome = monitor.poll_once(Utc::now()).await.unwrap();

        assert_eq!(
            outcome,
            PollOutcome::CutoffBehindCheckpoint,
            "a regression of SAFE_CUTOFF_QUIET_SKEW+1 must pause ingestion"
        );
        assert!(
            logs_contain("Cutoff block is behind committed ingestion progress"),
            "one block beyond the boundary must warn operators"
        );
        assert_eq!(backfill_job_count(&apalis_pool).await, 0);
    }

    #[tokio::test]
    async fn poll_once_skips_when_cutoff_behind_deployment_block() {
        // Cold start (no checkpoint): cutoff (genesis block 0) sits below the
        // deployment block (1), so there is nothing to ingest yet. `from_block`
        // (1) == `cutoff` (0) + 1, but with no committed progress this is NOT
        // "caught up" -- it is the deployment block one short of the cutoff, so
        // the outcome must be CutoffBehindDeployment, not CaughtUp.
        let (mut monitor, _pool, apalis_pool, _evm_ctx) = setup(provider_at(5, 0)).await;

        let outcome = monitor.poll_once(Utc::now()).await.unwrap();

        assert_eq!(
            outcome,
            PollOutcome::CutoffBehindDeployment,
            "cold start with cutoff below deployment_block must not report CaughtUp"
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            0,
            "nothing to ingest until the cutoff reaches the deployment block"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_once_does_not_warn_when_deployment_block_is_ahead_of_cutoff() {
        // Unusual config: a low stale checkpoint (5) but deployment_block (100)
        // configured ahead of the cutoff block (10). `from_block` is driven by
        // the deployment floor to 100 > cutoff + 1, but committed progress (5) is
        // NOT past the cutoff -- so this must be the quiet deployment-wait, not
        // the loud cutoff-regression warning that blames the RPC.
        let (mut monitor, pool, apalis_pool, evm_ctx) =
            setup_with_deployment_block(provider_at(15, 10), 100).await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 5)
            .await
            .unwrap();

        let outcome = monitor.poll_once(Utc::now()).await.unwrap();

        assert_eq!(
            outcome,
            PollOutcome::CutoffBehindDeployment,
            "a deployment_block ahead of the cutoff is not a cutoff regression"
        );
        assert!(
            !logs_contain("behind committed ingestion progress"),
            "must not blame the RPC for a deployment_block/checkpoint config gap"
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            0,
            "nothing to ingest until the cutoff reaches the deployment block"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_once_does_not_enqueue_when_no_cutoff_block_yet() {
        // No cutoff block and no checkpoint (cold start) -> skip the tick
        // without enqueuing anything. Cold start is expected/transient, so
        // no warn must fire (the startup probe surfaces persistent nulls).
        let asserter = Asserter::new();
        push_tick(&asserter, 50, None);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let (mut monitor, _pool, apalis_pool, _evm_ctx) = setup(provider).await;

        let outcome = monitor.poll_once(Utc::now()).await.unwrap();

        assert_eq!(
            outcome,
            PollOutcome::NoCutoffColdStart,
            "no checkpoint + null cutoff is an expected cold-start skip"
        );
        assert!(
            !logs_contain("Ingestion cannot start"),
            "cold-start null cutoff must not emit the old warn message"
        );
        assert!(
            !logs_contain("No cutoff block reported while a checkpoint exists"),
            "cold-start null cutoff must not trigger the checkpoint-exists warn"
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            0,
            "no cutoff block available yet -> no enqueue"
        );
    }

    /// A null cutoff response while a checkpoint already exists must skip the
    /// tick without moving the checkpoint (the financial invariant: a skip never
    /// advances ingestion past unverified blocks), and a later tick that sees a
    /// real cutoff block must resume from exactly where it left off.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_once_skips_intermittent_null_without_moving_checkpoint() {
        // Two ticks: the first reports null cutoff, the second a real cutoff
        // block. Each tick reads the chain tip then the cutoff block.
        let asserter = Asserter::new();
        push_tick(&asserter, 104, None);
        push_tick(&asserter, 110, Some(105));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let (mut monitor, pool, apalis_pool, evm_ctx) = setup(provider).await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 99)
            .await
            .unwrap();

        // First tick: node returns null cutoff -> skip, checkpoint frozen.
        // A checkpoint exists, so this is the warn branch, distinct from the
        // cold-start debug branch despite identical side effects.
        let first = monitor.poll_once(Utc::now()).await.unwrap();
        assert_eq!(
            first,
            PollOutcome::NoCutoffWithCheckpoint,
            "null cutoff while a checkpoint exists is the warn (regression) branch"
        );
        assert!(
            logs_contain("No cutoff block reported while a checkpoint exists"),
            "the warn branch must emit an operator-visible warning"
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            0,
            "a null cutoff response must not enqueue a range"
        );
        assert_eq!(
            crate::onchain::backfill::load_backfill_checkpoint(&pool, &evm_ctx)
                .await
                .unwrap(),
            Some(99),
            "the checkpoint must not move when the cutoff block is momentarily null"
        );

        // Second tick: cutoff is back -> resume from exactly checkpoint+1.
        let second = monitor.poll_once(Utc::now()).await.unwrap();
        assert_eq!(
            second,
            PollOutcome::Enqueued {
                from_block: 100,
                to_block: 105
            },
            "a recovered cutoff response resumes from checkpoint+1"
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            1,
            "a recovered cutoff response must resume ingestion"
        );
        let job = loaded_backfill(&apalis_pool).await;
        assert_eq!(job.from_block, 100, "resume from exactly checkpoint+1");
        assert_eq!(
            job.to_block, 105,
            "cutoff equals the recovered cutoff block"
        );
    }

    /// Same as `poll_once_skips_intermittent_null_without_moving_checkpoint`
    /// but for the `Finalized` cutoff. The null-handling path is tag-agnostic;
    /// this test guards against accidental tag-specific divergence.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn poll_once_skips_intermittent_null_finalized_without_moving_checkpoint() {
        let asserter = Asserter::new();
        push_tick(&asserter, 104, None);
        push_tick(&asserter, 110, Some(105));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let (mut monitor, pool, apalis_pool, evm_ctx) =
            setup_with_deployment_block_and_cutoff(provider, 1, IngestionCutoff::Finalized).await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 99)
            .await
            .unwrap();

        let first = monitor.poll_once(Utc::now()).await.unwrap();
        assert_eq!(first, PollOutcome::NoCutoffWithCheckpoint);
        assert!(logs_contain(
            "No cutoff block reported while a checkpoint exists"
        ));
        assert_eq!(backfill_job_count(&apalis_pool).await, 0);
        assert_eq!(
            crate::onchain::backfill::load_backfill_checkpoint(&pool, &evm_ctx)
                .await
                .unwrap(),
            Some(99),
        );

        let second = monitor.poll_once(Utc::now()).await.unwrap();
        assert_eq!(
            second,
            PollOutcome::Enqueued {
                from_block: 100,
                to_block: 105
            },
            "a recovered cutoff response resumes from checkpoint+1"
        );
        assert_eq!(backfill_job_count(&apalis_pool).await, 1);
        let job = loaded_backfill(&apalis_pool).await;
        assert_eq!(job.from_block, 100, "resume from exactly checkpoint+1");
        assert_eq!(
            job.to_block, 105,
            "cutoff equals the recovered cutoff block"
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

        let result = monitor.poll_once(Utc::now()).await;
        assert!(
            matches!(result, Err(OrderFillMonitorError::Rpc(_))),
            "A severed RPC must surface as a typed RPC error; got: {result:?}",
        );
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            0,
            "No backfill range may be enqueued off a failed chain-state read"
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
            inventory: address!("0x1111111111111111111111111111111111111111"),
            vault_owner: None,
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        // One chain-tip + cutoff-block response pair per poll_once call.
        let asserter = Asserter::new();
        push_tick(&asserter, 55, Some(50));
        push_tick(&asserter, 55, Some(50));
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

        // The block-lag telemetry write blocks then fails under the lock, but
        // it is best-effort (swallowed). The in-flight check is a read and
        // succeeds under the WAL write lock; the enqueue INSERT then blocks and
        // errors after the busy timeout -- that is the error poll_once returns.
        let result = monitor.poll_once(Utc::now()).await;
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

        monitor.poll_once(Utc::now()).await.unwrap();
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
        // short-circuit before enqueuing. The block-lag sample IS still
        // recorded first -- a long catch-up (a backfill in flight) is exactly
        // when lag must stay observable.
        let (mut monitor, pool, apalis_pool, evm_ctx) = setup(provider_at(140, 100)).await;

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

        let outcome = monitor.poll_once(Utc::now()).await.unwrap();

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
        let lag_blocks: Option<i64> =
            sqlx::query_scalar("SELECT lag_blocks FROM block_lag_samples")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            lag_blocks,
            Some(90),
            "lag (cutoff 100 - checkpoint 10) must be sampled even while a backfill is in flight"
        );
    }

    #[tokio::test]
    async fn orphaned_running_backfill_wedges_poller_until_requeued() {
        // A `BackfillRange` left `Running` by a dead process keeps
        // `has_in_flight` true, so the overlap guard suppresses ingestion
        // indefinitely -- the deterministic worker name means apalis never
        // ages the orphan out. `requeue_orphaned`, wired at conductor startup,
        // is what unwedges it.
        //
        // Both ticks read the provider (the lag sample is recorded before the
        // overlap guard): the first while wedged, the second after the orphan
        // reaches a terminal state.
        let asserter = Asserter::new();
        push_tick(&asserter, 110, Some(105));
        push_tick(&asserter, 110, Some(105));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let (mut monitor, pool, apalis_pool, evm_ctx) = setup(provider).await;
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
        // The lag sample is still recorded (reads happen before the guard).
        monitor.poll_once(Utc::now()).await.unwrap();
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
        // the cutoff-block RPC (consuming the mock) and enqueues a fresh range.
        sqlx_apalis::query("UPDATE Jobs SET status = 'Done' WHERE job_type LIKE '%BackfillRange%'")
            .execute(&apalis_pool)
            .await
            .unwrap();

        monitor.poll_once(Utc::now()).await.unwrap();
        assert_eq!(
            backfill_job_count(&apalis_pool).await,
            2,
            "once the orphan reaches a terminal state the poller enqueues a new range"
        );
    }

    #[tokio::test]
    async fn poll_once_records_block_lag_sample() {
        // tip=105, cutoff=102, checkpoint=99 -> lag = cutoff - checkpoint = 3.
        let (mut monitor, pool, _apalis_pool, evm_ctx) = setup(provider_at(105, 102)).await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 99)
            .await
            .unwrap();

        monitor.poll_once(Utc::now()).await.unwrap();

        let (chain_tip, cutoff_block, last_processed_block, lag_blocks): (
            i64,
            i64,
            Option<i64>,
            Option<i64>,
        ) = sqlx::query_as(
            "SELECT chain_tip, cutoff_block, last_processed_block, lag_blocks \
             FROM block_lag_samples",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(chain_tip, 105);
        assert_eq!(cutoff_block, 102);
        assert_eq!(last_processed_block, Some(99));
        assert_eq!(lag_blocks, Some(3));
    }

    #[tokio::test]
    async fn poll_once_reads_zero_lag_when_caught_up() {
        // checkpoint == cutoff: a healthy caught-up system must read 0, not a
        // permanent floor.
        let (mut monitor, pool, _apalis_pool, evm_ctx) = setup(provider_at(105, 102)).await;
        crate::onchain::backfill::save_backfill_checkpoint(&pool, &evm_ctx, 102)
            .await
            .unwrap();

        monitor.poll_once(Utc::now()).await.unwrap();

        let lag_blocks: Option<i64> =
            sqlx::query_scalar("SELECT lag_blocks FROM block_lag_samples")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(lag_blocks, Some(0));
    }

    #[tokio::test]
    async fn poll_once_records_null_lag_without_checkpoint() {
        let (mut monitor, pool, _apalis_pool, _evm_ctx) = setup(provider_at(55, 50)).await;

        monitor.poll_once(Utc::now()).await.unwrap();

        let lag_blocks: Option<i64> =
            sqlx::query_scalar("SELECT lag_blocks FROM block_lag_samples")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(lag_blocks, None);
    }

    #[test]
    fn skipped_ticks_is_zero_for_first_and_on_time_ticks() {
        let interval = Duration::from_secs(5);

        assert_eq!(skipped_ticks(None, interval), 0);
        assert_eq!(skipped_ticks(Some(Duration::from_secs(5)), interval), 0);
        // Jitter well under half an interval still counts as on time.
        assert_eq!(
            skipped_ticks(Some(Duration::from_millis(5_400)), interval),
            0
        );
    }

    #[test]
    fn skipped_ticks_counts_missed_intervals() {
        let interval = Duration::from_secs(5);

        assert_eq!(skipped_ticks(Some(Duration::from_secs(10)), interval), 1);
        assert_eq!(skipped_ticks(Some(Duration::from_secs(31)), interval), 5);
        // The rounding boundary: half an interval past the first tick tips
        // the count from "on time" to one skip.
        assert_eq!(
            skipped_ticks(Some(Duration::from_millis(7_499)), interval),
            0
        );
        assert_eq!(
            skipped_ticks(Some(Duration::from_millis(7_500)), interval),
            1
        );
    }

    #[test]
    fn skipped_ticks_clamps_at_i64_max_for_storage() {
        // A pathological gap must clamp at i64::MAX (the storage layer's
        // integer width), not fail the eventual sample write.
        assert_eq!(
            skipped_ticks(
                Some(Duration::from_millis(u64::MAX)),
                Duration::from_millis(1)
            ),
            i64::MAX.unsigned_abs()
        );
    }

    #[tokio::test]
    async fn probe_cutoff_block_support_accepts_a_safe_block() {
        // Safe cutoff (108) behind the chain tip (110): genuine support.
        let outcome =
            probe_cutoff_block_support(&provider_with_cutoff(108), 110, IngestionCutoff::Safe)
                .await
                .unwrap();

        assert_eq!(outcome, CutoffProbe::Supported);
    }

    #[tokio::test]
    async fn probe_cutoff_block_support_accepts_a_finalized_block() {
        // Finalized (102) strictly behind the chain tip (110): genuine finality.
        let outcome =
            probe_cutoff_block_support(&provider_with_cutoff(102), 110, IngestionCutoff::Finalized)
                .await
                .unwrap();

        assert_eq!(outcome, CutoffProbe::Supported);
    }

    /// The `safe` tag can legitimately equal or exceed the chain tip (fast
    /// sequencer batch cadence). The probe must accept this as normal, not flag
    /// it as aliasing.
    #[tokio::test]
    async fn probe_cutoff_block_support_does_not_flag_safe_near_tip() {
        // safe == chain_tip (110 == 110): valid for `safe`, must return Supported.
        let outcome =
            probe_cutoff_block_support(&provider_with_cutoff(110), 110, IngestionCutoff::Safe)
                .await
                .unwrap();

        assert_eq!(
            outcome,
            CutoffProbe::Supported,
            "safe == chain_tip is normal for fast sequencer batching; must not flag aliasing"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn probe_cutoff_block_support_accepts_null_response() {
        // A null response (tag not yet available) must be allowed through as
        // NotYetAvailable, so a fresh chain does not fail startup.
        let outcome =
            probe_cutoff_block_support(&provider_without_cutoff(), 110, IngestionCutoff::Safe)
                .await
                .unwrap();

        assert_eq!(outcome, CutoffProbe::NotYetAvailable);
        assert!(
            logs_contain("RPC endpoint reports no safe block at startup"),
            "deferred ingestion at startup must be operator-visible"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn probe_cutoff_block_support_accepts_null_finalized_response() {
        // A null response for the finalized tag (tag not yet available) must
        // be allowed through as NotYetAvailable, so a fresh chain does not
        // fail startup.
        let outcome =
            probe_cutoff_block_support(&provider_without_cutoff(), 110, IngestionCutoff::Finalized)
                .await
                .unwrap();

        assert_eq!(outcome, CutoffProbe::NotYetAvailable);
        assert!(
            logs_contain("RPC endpoint reports no finalized block at startup"),
            "deferred ingestion at startup must be operator-visible"
        );
    }

    #[tokio::test]
    async fn probe_cutoff_block_support_flags_finalized_aliasing_tip() {
        // A provider reporting `finalized` at the chain tip (110 == 110) is
        // likely aliasing `finalized` to `latest`, disabling reorg protection.
        // The probe must return a structured FinalizedAliasesChainTip error.
        let error =
            probe_cutoff_block_support(&provider_with_cutoff(110), 110, IngestionCutoff::Finalized)
                .await
                .unwrap_err();

        assert!(
            matches!(
                error,
                CutoffProbeError::FinalizedAliasesChainTip {
                    cutoff_block: 110,
                    chain_tip: 110,
                }
            ),
            "finalized aliasing the tip must surface as FinalizedAliasesChainTip; got: {error:?}"
        );
    }

    #[tokio::test]
    async fn probe_cutoff_block_support_flags_finalized_strictly_ahead_of_tip() {
        // finalized (111) strictly ahead of the chain tip (110): only an
        // endpoint aliasing `finalized` to `latest` can produce this.
        let error =
            probe_cutoff_block_support(&provider_with_cutoff(111), 110, IngestionCutoff::Finalized)
                .await
                .unwrap_err();

        assert!(
            matches!(
                error,
                CutoffProbeError::FinalizedAliasesChainTip {
                    cutoff_block: 111,
                    chain_tip: 110,
                }
            ),
            "finalized ahead of the tip must surface as FinalizedAliasesChainTip; got: {error:?}"
        );
    }

    #[tokio::test]
    async fn probe_cutoff_block_support_rpc_error_bubbles_for_safe_tag() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("connection refused");
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        // An endpoint that errors on the safe tag must fail the startup probe.
        let error = probe_cutoff_block_support(&provider, 110, IngestionCutoff::Safe)
            .await
            .unwrap_err();

        assert!(
            matches!(error, CutoffProbeError::Rpc(RpcError::ErrorResp(_))),
            "a safe-tag RPC failure must surface as a wrapped RPC error; got: {error:?}"
        );
    }

    #[tokio::test]
    async fn probe_cutoff_block_support_rpc_error_bubbles_for_finalized_tag() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("connection refused");
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        // An endpoint that errors on the finalized tag must fail the startup probe.
        let error = probe_cutoff_block_support(&provider, 110, IngestionCutoff::Finalized)
            .await
            .unwrap_err();

        assert!(
            matches!(error, CutoffProbeError::Rpc(RpcError::ErrorResp(_))),
            "a finalized-tag RPC failure must surface as a wrapped RPC error; got: {error:?}"
        );
    }
}
