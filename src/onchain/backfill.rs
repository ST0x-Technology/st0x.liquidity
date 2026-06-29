//! Historical onchain event backfill with retry logic.
//!
//! Scans past blocks for `ClearV3` and `TakeOrderV3` events and pushes them
//! into the apalis job queue for processing, ensuring no trades are missed
//! after downtime. A persisted checkpoint records the last block that was
//! fully enqueued.

use alloy::eips::BlockNumberOrTag;
use alloy::primitives::{B256, TxHash};
use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use backon::{BackoffBuilder, ExponentialBuilder, Retryable};
use chrono::{DateTime, Utc};
use futures_util::future;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::time::Duration;
use tracing::{debug, error, info, trace, warn};

use st0x_config::{EvmCtx, ExecutionThreshold};
use st0x_event_sorcery::{AggregateError, LifecycleError, Store};
use st0x_evm::Evm;
use st0x_execution::{Executor, FractionalShares};

use super::OnChainError;
use crate::bindings::IRaindexV6::{ClearV3, TakeOrderV3};
use crate::conductor::job::{Job, Label};
use crate::onchain::trade::RaindexTradeEvent;
use crate::onchain_trade::{OnChainTrade, OnChainTradeCommand, OnChainTradeError, OnChainTradeId};
use crate::position::{Position, PositionCommand, PositionError, TradeId};
use crate::symbol::lock::get_symbol_lock;
use crate::trading::onchain::inclusion::EmittedOnChain;
use crate::trading::onchain::trade_accountant::{
    AccountForDexTrade, AccountantCtx, DexTradeAccountingJobQueue, TradeAccountingError,
};

/// A confirmed fill whose block a reorg dropped past the confirmation depth,
/// identified for reversal. The `(tx_hash, log_index)` identity is the
/// [`OnChainTradeId`] of the fill to reverse; `block_number` lets the caller
/// derive how deep the reorg ran (current tip minus this block).
///
/// `reinclusion` distinguishes the two reorg signals. A `removed: true` log is a
/// fill the canonical chain truly dropped, with no re-inclusion (`None`): the
/// reversal is all there is to do. A block-hash mismatch is the SAME fill
/// re-mined on a new canonical block (`Some`): after reversing the stale-block
/// impact, [`record_reorg`] re-witnesses and re-applies the fill on the new
/// block so the net returns to the fill's impact rather than staying flat.
#[derive(Debug, Clone)]
pub(crate) struct RemovedTrade {
    pub(crate) trade_id: OnChainTradeId,
    pub(crate) block_number: u64,
    pub(crate) reinclusion: Option<ReInclusion>,
}

/// The new canonical block a reorged `(tx_hash, log_index)` fill was re-mined
/// on, carried so [`record_reorg`] can re-witness and re-apply the fill after
/// reversing its stale-block impact. The fill content (amount/direction/price)
/// is unchanged -- only the block moved -- so it is read from the persisted
/// `OnChainTrade`, not duplicated here.
#[derive(Debug, Clone)]
pub(crate) struct ReInclusion {
    /// Hash of the new canonical block. `detect_block_hash_reorgs` only produces
    /// a re-inclusion when the observed hash is present, so this is never absent.
    pub(crate) hash: B256,
    pub(crate) number: u64,
    /// Timestamp of the new canonical block. `None` when the re-scan log carried
    /// none (some nodes omit it on `eth_getLogs`); the reapply then falls back to
    /// the persisted fill's timestamp rather than fabricating one.
    pub(crate) timestamp: Option<DateTime<Utc>>,
}

/// Identity of a present (non-removed) backfill log, carried so the caller can
/// compare its freshly-observed `block_hash` against the one persisted for an
/// already-witnessed `(tx_hash, log_index)`. A mismatch means a fork swap during
/// downtime re-served the same key on a different block -- a reorg the
/// `removed: true` path never surfaces over `eth_getLogs`.
#[derive(Debug, Clone)]
pub(crate) struct PresentLog {
    pub(crate) tx_hash: TxHash,
    pub(crate) log_index: u64,
    pub(crate) block_number: u64,
    pub(crate) block_hash: Option<B256>,
    /// Timestamp of the block this log was observed in, carried so a detected
    /// block-hash reorg can re-apply the re-mined fill on the new canonical
    /// block's time. `None` when the source log omitted it.
    pub(crate) block_timestamp: Option<DateTime<Utc>>,
}

/// What an `enqueue_batch_events` pass found: how many fresh fills it enqueued,
/// which already-accounted fills its logs reported as reorged away
/// (`removed: true`), and the identities of the present logs (for block-hash
/// fork detection by the caller).
#[derive(Debug)]
struct BatchOutcome {
    enqueued: usize,
    removed: Vec<RemovedTrade>,
    present: Vec<PresentLog>,
}

/// Aggregated result of a backfill range scan: fills the logs reported reorged
/// away plus the present-log identities for block-hash fork detection.
#[derive(Debug, Default)]
pub(crate) struct BackfillScan {
    pub(crate) removed: Vec<RemovedTrade>,
    pub(crate) present: Vec<PresentLog>,
}

pub(crate) fn get_backfill_retry_strat() -> ExponentialBuilder {
    const BACKFILL_MAX_RETRIES: usize = 15;
    const BACKFILL_INITIAL_DELAY: Duration = Duration::from_millis(100);
    const BACKFILL_MAX_DELAY: Duration = Duration::from_secs(120);

    ExponentialBuilder::default()
        .with_max_times(BACKFILL_MAX_RETRIES)
        .with_min_delay(BACKFILL_INITIAL_DELAY)
        .with_max_delay(BACKFILL_MAX_DELAY)
        .with_jitter()
}

/// Loads the checkpoint and backfills up to `end_block`. Retained
/// for tests that exercise the "resume from checkpoint" branch
/// alongside the batched fetch logic. Production code uses
/// [`BackfillRange`] (and thus [`backfill_range`]) directly via the
/// monitor.
#[cfg(test)]
#[tracing::instrument(
    target = "orderbook",
    skip(provider, evm_ctx, pool, retry_strategy, job_queue),
    fields(end_block),
    level = tracing::Level::INFO,
)]
pub(crate) async fn backfill_events<P: Provider + Clone, B: BackoffBuilder + Clone>(
    provider: &P,
    evm_ctx: &EvmCtx,
    pool: &SqlitePool,
    end_block: u64,
    retry_strategy: B,
    job_queue: DexTradeAccountingJobQueue,
) -> Result<Vec<RemovedTrade>, OnChainError> {
    let start_block = backfill_start_block(pool, evm_ctx).await?;

    Ok(backfill_range(
        provider,
        evm_ctx,
        pool,
        start_block,
        end_block,
        retry_strategy,
        job_queue,
    )
    .await?
    .removed)
}

/// Fetches `ClearV3` / `TakeOrderV3` logs in `[from_block, to_block]`,
/// pushes an `AccountForDexTrade` job for each, and advances the
/// backfill checkpoint to `to_block` on success.
///
/// Skips RPC calls when `from_block > to_block` (already caught up),
/// but still moves the checkpoint forward so a stale row does not
/// cause repeated no-op fetches.
#[tracing::instrument(
    target = "orderbook",
    skip(provider, evm_ctx, pool, retry_strategy, job_queue),
    fields(from_block, to_block),
    level = tracing::Level::INFO,
)]
pub(crate) async fn backfill_range<P: Provider + Clone, B: BackoffBuilder + Clone>(
    provider: &P,
    evm_ctx: &EvmCtx,
    pool: &SqlitePool,
    from_block: u64,
    to_block: u64,
    retry_strategy: B,
    job_queue: DexTradeAccountingJobQueue,
) -> Result<BackfillScan, OnChainError> {
    if from_block > to_block {
        info!(
            target: "orderbook",
            "Already caught up to block {}, skipping backfill",
            to_block
        );

        save_backfill_checkpoint(pool, evm_ctx, to_block).await?;
        return Ok(BackfillScan::default());
    }

    let total_blocks = to_block - from_block + 1;

    info!(
        target: "orderbook",
        "Backfilling from block {} to {} ({} blocks)",
        from_block, to_block, total_blocks
    );

    let batch_ranges = generate_batch_ranges(from_block, to_block);

    let mut total_enqueued: usize = 0;
    let mut removed_trades = Vec::new();
    let mut present_logs = Vec::new();
    for (batch_start, batch_end) in batch_ranges {
        let outcome = enqueue_batch_events(
            provider,
            evm_ctx,
            batch_start,
            batch_end,
            retry_strategy.clone(),
            job_queue.clone(),
        )
        .await?;
        total_enqueued += outcome.enqueued;
        removed_trades.extend(outcome.removed);
        present_logs.extend(outcome.present);
    }

    info!(
        target: "orderbook",
        total_enqueued,
        total_removed = removed_trades.len(),
        "Backfill completed"
    );

    save_backfill_checkpoint(pool, evm_ctx, to_block).await?;

    Ok(BackfillScan {
        removed: removed_trades,
        present: present_logs,
    })
}

/// Persistent job queue for backfill jobs.
pub(crate) type BackfillJobQueue = crate::conductor::job::JobQueue<BackfillRange>;

/// Apalis job that backfills missed `ClearV3` / `TakeOrderV3` events
/// between `from_block` and `to_block` (inclusive).
///
/// Enqueued by `OrderFillMonitor::run` on every (re)connect to cover
/// the gap between the previously checkpointed block and the cutoff
/// of the new WebSocket subscription. Job durability via apalis
/// storage means a crash mid-backfill is retried automatically.
///
/// Assumes the startup WS provider from `Conductor::run` (used here
/// via `ctx.evm.provider()`) stays usable for `eth_getLogs` — backfill
/// does not pick up the fresh provider that `OrderFillMonitor` opens
/// on reconnect. Apalis retries plus the conductor restart loop
/// provide defense in depth if that assumption breaks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BackfillRange {
    pub(crate) from_block: u64,
    pub(crate) to_block: u64,
}

impl<Node, Exec> Job<AccountantCtx<Node, Exec>> for BackfillRange
where
    Node: Provider + Clone + Send + Sync + 'static,
    Exec: Executor + Clone + Send + Sync + 'static,
    TradeAccountingError: From<Exec::Error>,
{
    type Output = ();
    type Error = OnChainError;

    const WORKER_NAME: &'static str = "backfill-worker";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind = crate::conductor::job::JobKind::Backfill;

    fn label(&self) -> Label {
        Label::new(format!(
            "BackfillRange:{}:{}",
            self.from_block, self.to_block
        ))
    }

    async fn perform(&self, ctx: &AccountantCtx<Node, Exec>) -> Result<Self::Output, Self::Error> {
        let scan = backfill_range(
            ctx.evm.provider(),
            &ctx.ctx.evm,
            &ctx.pool,
            self.from_block,
            self.to_block,
            get_backfill_retry_strat(),
            ctx.job_queue.clone(),
        )
        .await?;

        // A `removed: true` log is the legacy (now-inert over `eth_getLogs`)
        // reorg signal; the authoritative re-mined signal is a present log
        // re-served on a different block_hash than the witnessed fill persisted.
        // Both funnel through the same exactly-once `record_reorg` reversal.
        let mut reorged = scan.removed;
        reorged.extend(detect_block_hash_reorgs(&ctx.cqrs.onchain_trade, &scan.present).await?);

        // The tip anchors both the unfinalized window the re-verification scans and
        // the reorg depth recorded per reversal (the dropped block is gone, so the
        // reorg spans at least `tip - block_number`). Read once and reused for every
        // reversal in this batch.
        let tip = ctx.evm.provider().get_block_number().await?;

        // `detect_block_hash_reorgs` only fires when a witnessed `(tx_hash,
        // log_index)` is RE-observed on a different block, so a fill the chain
        // DROPPED (orphaned, never re-mined) is structurally invisible to it. This
        // pass drives from the witnessed-fills side and actively re-reads each
        // unfinalized fill's canonical block by number -- INDEPENDENT of the forward
        // scan, which never re-covers a dropped block below the checkpoint. It runs
        // every tick (not gated on the scan) so a drop is caught even when the scan
        // is empty; the reversal is reverse-ONLY through the same `record_reorg`.
        reorged.extend(
            detect_dropped_fill_reorgs(
                &ctx.cqrs.onchain_trade,
                &ctx.pool,
                ctx.evm.provider(),
                &scan.present,
                tip,
                ctx.ctx.evm.required_confirmations,
            )
            .await?,
        );

        for removed_trade in reorged {
            // A tip behind the removed block would saturate `reorg_depth` to 0 --
            // a permanently underreported depth on an audit record. The load-
            // balanced RPC routed us to a lagging node; fail so apalis retries
            // against a caught-up one rather than recording a wrong depth.
            if tip < removed_trade.block_number {
                return Err(OnChainError::NodeLaggingBehindRequest {
                    observed_tip: tip,
                    required_tip: removed_trade.block_number,
                });
            }
            let reorg_depth = tip - removed_trade.block_number;

            record_reorg(
                &ctx.cqrs.onchain_trade,
                &ctx.cqrs.position,
                &removed_trade,
                reorg_depth,
                ctx.cqrs.execution_threshold,
            )
            .await?;
        }

        Ok(())
    }
}

/// Derives the block to resume backfill from, given the persisted checkpoint
/// (or its absence). Resumes at `checkpoint + 1` floored at `deployment_block`;
/// a cold start with no checkpoint begins at `deployment_block`. Pure so a
/// caller that already holds the checkpoint (the fill monitor's poll loop) can
/// reuse it without a second read.
pub(crate) fn backfill_start_from_checkpoint(
    checkpoint: Option<u64>,
    deployment_block: u64,
) -> u64 {
    checkpoint.map_or(deployment_block, |last_processed_block| {
        (last_processed_block + 1).max(deployment_block)
    })
}

/// Detects fork swaps that a `removed: true` log never surfaces over
/// `eth_getLogs`: a present (re-served) backfill log whose `(tx_hash, log_index)`
/// matches an already-witnessed fill but whose `block_hash` differs from the one
/// persisted on the `OnChainTrade` means the fill was accounted against a block
/// the canonical chain no longer holds -- a reorg to reverse.
///
/// A key is flagged only when BOTH the persisted and freshly-observed hashes are
/// present and differ. An unwitnessed key (nothing accounted), a missing
/// persisted hash (legacy fill or a log that carried none), or a missing
/// observed hash all skip the comparison -- there is nothing to compare against,
/// not evidence of a reorg. The returned [`RemovedTrade`]s flow through the same
/// exactly-once `record_reorg` reversal as the `removed: true` path.
async fn detect_block_hash_reorgs(
    onchain_trade: &Store<OnChainTrade>,
    present: &[PresentLog],
) -> Result<Vec<RemovedTrade>, OnChainError> {
    let mut reorged = Vec::new();

    for log in present {
        let Some(observed_block_hash) = log.block_hash else {
            continue;
        };

        let trade_id = OnChainTradeId {
            tx_hash: log.tx_hash,
            log_index: log.log_index,
        };
        let Some(trade) = onchain_trade.load(&trade_id).await? else {
            continue;
        };

        // Short-circuit a reorg whose reversal is already acknowledged: its
        // re-application is driven by the re-served log's own accounting job and
        // the re-witness will clear the marker, so re-flagging (and re-comparing)
        // it here is redundant work on an in-flight reorg.
        if trade.is_reorg_acknowledged() {
            continue;
        }

        let Some(persisted_block_hash) = trade.block_hash else {
            continue;
        };

        if persisted_block_hash != observed_block_hash {
            warn!(
                target: "orderbook",
                tx_hash = ?log.tx_hash,
                log_index = log.log_index,
                ?persisted_block_hash,
                ?observed_block_hash,
                "Backfill re-observed a witnessed fill on a different block_hash -- fork swap \
                 during downtime; recording reorg reversal"
            );
            reorged.push(RemovedTrade {
                trade_id: OnChainTradeId {
                    tx_hash: log.tx_hash,
                    log_index: log.log_index,
                },
                // Anchor the reorg on the block the fill was originally accounted
                // against (the persisted block), not the re-mined block observed
                // now -- reorg depth is measured from the original block. Fall
                // back to the observed block only if the persisted one is absent.
                block_number: trade.block_number.unwrap_or(log.block_number),
                // The re-served log IS the new canonical block: carry it so the
                // reversal is followed by a re-witness + re-apply of the fill.
                reinclusion: Some(ReInclusion {
                    hash: observed_block_hash,
                    number: log.block_number,
                    timestamp: log.block_timestamp,
                }),
            });
        }
    }

    Ok(reorged)
}

/// How many blocks below the chain tip the dropped-fill re-verification still
/// re-checks each tick. A witnessed fill whose block is deeper than this is
/// treated as final and dropped from consideration, keeping the pass
/// O(unfinalized fills) rather than O(all history).
///
/// Sized to cover the `safe`-cutoff reorg window with margin: a `safe` OP-stack
/// L2 block is batch-posted but not yet L1-finalized, so it can still be reorged
/// until its batch finalizes on L1 -- on Base that is ~hundreds of L2 blocks
/// (~2s/block against ~13 min L1 finality). The work is O(fills in the window),
/// not O(blocks), so a wide window stays cheap. The exact value is a deliberate
/// estimate pending measured fleet data, like the `SAFE_CUTOFF_QUIET_SKEW` band
/// in the fill monitor.
const REORG_REVERIFICATION_WINDOW: u64 = 1024;

/// Detects witnessed fills the canonical chain DROPPED -- orphaned by a reorg and
/// never re-mined -- which [`detect_block_hash_reorgs`] structurally cannot catch.
/// That detector only fires when a witnessed `(tx_hash, log_index)` is RE-observed
/// on a different `block_hash`; a dropped fill is never re-observed, so it is never
/// compared and a naked hedge is left behind. This pass instead drives from the
/// witnessed-fills side and ACTIVELY RE-VERIFIES each unfinalized fill against the
/// chain: it re-reads the fill's block by number and asks whether the block the
/// fill was accounted against is still canonical.
///
/// For each candidate fill `F` (`block_number`, persisted `block_hash`, not yet
/// reorg-acknowledged) inside the unfinalized window:
/// - fetch the current canonical block at `F.block_number` via
///   [`Provider::get_block_by_number`];
/// - if its hash EQUALS `F`'s persisted hash, the block is still canonical -- no-op;
/// - if its hash DIFFERS, the block was orphaned. When `F`'s own
///   `(tx_hash, log_index)` is ALSO absent from the current scan (not re-mined under
///   the same key, which is [`detect_block_hash_reorgs`]'s reverse-then-reapply
///   job), the fill was dropped -> flag reverse-ONLY (`reinclusion: None`).
///
/// This is INDEPENDENT of the forward scan / checkpoint: the canonical block is read
/// directly by number, so a fill whose block sits BELOW the checkpoint -- which the
/// forward scan never re-covers -- is still re-verified. The scan's present logs are
/// consulted ONLY to partition a re-mined-same-key fill (key re-observed) away from
/// this detector, so within a pass a fill is flagged by at most one detector.
///
/// Confirmation-respecting: a fill within `required_confirmations` of the tip is too
/// close to trust the canonical view (a near-tip fork may itself reorg back), so it
/// is deferred to a later tick once buried deep enough. Combined with the
/// [`REORG_REVERIFICATION_WINDOW`] floor, only fills in
/// `(tip - WINDOW, tip - required_confirmations]` are re-verified, keeping the pass
/// O(unfinalized fills).
///
/// Exactly-once across ticks: a fill whose reorg reversal is already acknowledged
/// (`is_reorg_acknowledged`) is skipped, mirroring [`detect_block_hash_reorgs`]. A
/// reversal that began but did not finish (reorged, not yet acknowledged) is
/// re-flagged so `record_reorg`'s idempotent resume completes it. `OnChainTrade` has
/// no projection, so candidates come straight from the event log; the authoritative
/// block/hash/reorg state is read from the loaded aggregate.
async fn detect_dropped_fill_reorgs<P: Provider>(
    onchain_trade: &Store<OnChainTrade>,
    pool: &SqlitePool,
    provider: &P,
    present: &[PresentLog],
    tip: u64,
    required_confirmations: u64,
) -> Result<Vec<RemovedTrade>, OnChainError> {
    let finalized_tip = tip.saturating_sub(REORG_REVERIFICATION_WINDOW);
    let finalized_tip_bound = i64::try_from(finalized_tip)?;

    // A fill closer to the tip than the confirmation depth is not yet buried deep
    // enough to trust the canonical view -- a near-tip fork can still reorg back.
    // Re-verify only at or below this ceiling; a fresher fill is re-checked on a
    // later tick once it is confirmed-deep.
    let confirmed_ceiling = tip.saturating_sub(required_confirmations);

    // Candidate ids: witnessed fills whose latest block (its `Filled`, or its
    // `ReWitnessed` after a re-mine) is still above the finality boundary. Filtered
    // in SQL so only the unfinalized fills are loaded.
    let candidates = sqlx::query_as::<_, (String,)>(
        "SELECT aggregate_id FROM events \
         WHERE aggregate_type = 'OnChainTrade' \
           AND event_type IN ('OnChainTradeEvent::Filled', 'OnChainTradeEvent::ReWitnessed') \
         GROUP BY aggregate_id \
         HAVING MAX(COALESCE( \
             json_extract(payload, '$.Filled.block_number'), \
             json_extract(payload, '$.ReWitnessed.block_number') \
         )) > ?",
    )
    .bind(finalized_tip_bound)
    .fetch_all(pool)
    .await?;

    let mut reorged = Vec::new();

    for (aggregate_id,) in candidates {
        let trade_id = match aggregate_id.parse::<OnChainTradeId>() {
            Ok(trade_id) => trade_id,
            Err(error) => {
                warn!(
                    target: "orderbook",
                    %aggregate_id,
                    %error,
                    "Skipping unparseable OnChainTrade aggregate id during dropped-fill \
                     re-verification"
                );
                continue;
            }
        };

        let Some(trade) = onchain_trade.load(&trade_id).await? else {
            continue;
        };

        // Already fully reversed (and, for a re-mined fill, since re-witnessed):
        // nothing to do. Mirrors `detect_block_hash_reorgs`'s acknowledged guard so
        // the reversal stays exactly-once across ticks.
        if trade.is_reorg_acknowledged() {
            continue;
        }

        let (Some(block_number), Some(persisted_block_hash)) =
            (trade.block_number, trade.block_hash)
        else {
            // No persisted block hash to compare against (a legacy fill, or one
            // whose source log carried none): the orphaning cannot be proven, so do
            // not flag -- matches `detect_block_hash_reorgs`.
            continue;
        };

        // The SQL bound is on the event log's block number; re-check against the
        // authoritative loaded block so a fill that finalized between the query and
        // the load is not reversed.
        if block_number <= finalized_tip {
            continue;
        }

        // Too close to the tip to trust the canonical view yet: defer to a later
        // tick. The re-verification re-runs every tick, so deferring keeps the
        // reversal exactly-once rather than dropping it.
        if block_number > confirmed_ceiling {
            continue;
        }

        // The fill's own key is still on the chain (re-observed): either still
        // canonical, or re-mined under the SAME key -- which is
        // `detect_block_hash_reorgs`'s job (reverse-then-reapply). Not a drop, and
        // checked before the canonical read so a re-mine is never reverse-only here.
        let key_reobserved = present
            .iter()
            .any(|log| log.tx_hash == trade_id.tx_hash && log.log_index == trade_id.log_index);
        if key_reobserved {
            continue;
        }

        // Active canonical re-verification: read the block the fill was accounted
        // against by number and compare its CURRENT hash to the one persisted. This
        // is the authoritative orphan signal, independent of the forward scan -- it
        // surfaces a drop even when the fill's block sits below the checkpoint the
        // scan never re-covers.
        let Some(canonical_block) = provider
            .get_block_by_number(BlockNumberOrTag::Number(block_number))
            .await?
        else {
            // The node has not surfaced a block at this height (a lagging or
            // cold-start node): the orphaning cannot be proven, so do not reverse an
            // already-hedged fill. A later tick re-checks against a caught-up node.
            warn!(
                target: "orderbook",
                tx_hash = ?trade_id.tx_hash,
                log_index = trade_id.log_index,
                block_number,
                "Canonical block absent during dropped-fill re-verification; deferring -- \
                 cannot prove the fill's block was orphaned"
            );
            continue;
        };

        // The fill's block is still canonical (its hash is unchanged): not a drop.
        if canonical_block.header.hash == persisted_block_hash {
            continue;
        }

        warn!(
            target: "orderbook",
            tx_hash = ?trade_id.tx_hash,
            log_index = trade_id.log_index,
            block_number,
            ?persisted_block_hash,
            canonical_block_hash = ?canonical_block.header.hash,
            "Witnessed fill's block was orphaned (canonical hash differs) and the fill's \
             identity did not re-appear -- dropped by a reorg; recording reverse-only reversal"
        );
        reorged.push(RemovedTrade {
            trade_id,
            block_number,
            // Truly dropped: the canonical chain no longer holds the fill, so there
            // is nothing to re-apply -- reverse only.
            reinclusion: None,
        });
    }

    Ok(reorged)
}

/// Reverses a reorged fill across both aggregates, then -- when the reorg was a
/// re-mining of the SAME fill on a new canonical block -- re-witnesses and
/// re-applies it so the net returns to the fill's impact (ADR 0012).
///
/// # Reverse-then-reapply
///
/// A `removed: true` log (`reinclusion: None`) is a fill the canonical chain
/// truly dropped: the reversal is the whole job. A block-hash mismatch
/// (`reinclusion: Some`) is the same `(tx_hash, log_index)` re-mined on a new
/// block with identical economic content; reversing alone would leave the
/// position permanently FLAT, so after the reversal this re-witnesses the fill
/// onto the new block (clearing the reorg markers) and re-acknowledges it,
/// restoring the impact.
///
/// The reverse phase resumes at whichever step is unfinished, keyed on the
/// `OnChainTrade` reorg markers (see [`reverse_reorged_fill`]). The reapply phase
/// re-drives the normal `AcknowledgeOnChainFill` -> `Acknowledge` pair (see
/// [`reapply_reincluded_fill`]). Everything appends through the CQRS framework,
/// never direct SQL. A fill we never witnessed (e.g. a non-hedgeable pair) has
/// nothing to reverse and is skipped.
///
/// # Crash-safety
///
/// The whole sequence runs under the per-symbol lock that `AccountForDexTrade`
/// also holds, so a live fill cannot interleave its `Position` writes with the
/// reversal. Across re-deliveries the resume is keyed on durable state:
///
/// - The reverse phase's exactly-once guarantee is unchanged (ADR 0012): the
///   reorg-ack marker is written only after the position reversal, and the
///   position's bounded `pending_reorged_trade_ids` set + `last_reorged_trade_id`
///   slot reject a double-reverse.
/// - `ReWitnessed` clears the reorg markers, which would otherwise make a
///   resumed reverse re-run from scratch. The guard against that is the block
///   hash itself: once the re-witness is durable the persisted `block_hash`
///   equals the new canonical hash, so `detect_block_hash_reorgs` no longer
///   flags the fill and `record_reorg` is not re-invoked for it. Within this
///   call, `already_rewitnessed` detects the same condition and skips the
///   reverse phase (and the re-`ReWitness`) so a resume after the re-witness
///   does not append a spurious second reverse cycle.
/// - If the process crashes after `ReWitnessed` but before the re-acknowledge,
///   the re-served canonical log's own `AccountForDexTrade` job (always enqueued
///   by the same backfill pass) completes the re-apply: it now finds the trade
///   witnessed-but-not-acknowledged and resumes the acknowledge pair. The reapply
///   here and that job both serialize on the symbol lock and are idempotent
///   (`DuplicateTrade` / `AlreadyAcknowledged`), so whichever runs second is a
///   no-op.
///
/// Alternative considered: treat a re-mined fill as net-zero (update only the
/// persisted `block_hash` for audit, never reversing or re-applying). Rejected
/// because it does not fit the exactly-once `Reorged` model the rest of the
/// stack and the e2e expect -- the reversal is an auditable event the inventory
/// reactor consumes, and skipping it would diverge the `Position` from its event
/// log. Reverse-then-reapply keeps every step an explicit, replayable event.
async fn record_reorg(
    onchain_trade: &Store<OnChainTrade>,
    position: &Store<Position>,
    removed: &RemovedTrade,
    reorg_depth: u64,
    threshold: ExecutionThreshold,
) -> Result<(), OnChainError> {
    let RemovedTrade {
        trade_id,
        reinclusion,
        ..
    } = removed;

    let Some(trade) = onchain_trade.load(trade_id).await? else {
        warn!(
            target: "orderbook",
            tx_hash = ?trade_id.tx_hash,
            log_index = trade_id.log_index,
            "Reorged log has no witnessed OnChainTrade; nothing to reverse"
        );
        return Ok(());
    };

    // Serialize against concurrent fill accounting for this symbol. The
    // `AccountForDexTrade` path holds this same per-symbol lock across its
    // position writes; without it a reorg reversal and a live fill for the same
    // symbol could interleave their `Position` writes and clobber one another.
    let symbol_lock = get_symbol_lock(&trade.symbol).await;
    let _symbol_guard = symbol_lock.lock().await;

    // A prior delivery already re-witnessed the fill iff the persisted block hash
    // already equals the re-inclusion's. `ReWitness` only fires once the reversal
    // is fully acknowledged, so a match means the reverse phase is complete and
    // must be skipped -- re-running it would append a spurious second reverse
    // cycle on top of an already-re-applied fill.
    let already_rewitnessed = match (reinclusion, trade.block_hash) {
        (Some(reinclusion), Some(persisted_block_hash)) => persisted_block_hash == reinclusion.hash,
        _ => false,
    };

    if !already_rewitnessed {
        reverse_reorged_fill(onchain_trade, position, &trade, trade_id, reorg_depth).await?;
    }

    if let Some(reinclusion) = reinclusion {
        reapply_reincluded_fill(
            onchain_trade,
            position,
            &trade,
            trade_id,
            reinclusion,
            threshold,
            already_rewitnessed,
        )
        .await?;
    }

    Ok(())
}

/// Reverses an already-accounted reorged fill exactly once across both
/// aggregates (ADR 0012), resuming at whichever step is unfinished:
///
/// - reorg-acknowledged -> fully reversed already; re-settle the position's
///   bounded pending-reorg set to self-heal a prune that an earlier delivery
///   crashed before issuing, then done;
/// - not yet reorged -> record the reorg, reverse the position, acknowledge,
///   then prune the pending-reorg set;
/// - reorged but not acknowledged -> a prior delivery crashed mid-reversal;
///   resume directly at the position reversal, then acknowledge and prune.
///
/// The `OnChainTrade` reorg-ack marker is written only after the position
/// reversal succeeds, and the position rejects a `trade_id` it has already
/// reversed, so a re-delivered backfill range neither skips nor double-applies a
/// reversal. Runs under the caller's per-symbol lock.
async fn reverse_reorged_fill(
    onchain_trade: &Store<OnChainTrade>,
    position: &Store<Position>,
    trade: &OnChainTrade,
    trade_id: &OnChainTradeId,
    reorg_depth: u64,
) -> Result<(), OnChainError> {
    let position_trade_id = TradeId {
        tx_hash: trade_id.tx_hash,
        log_index: trade_id.log_index,
    };

    if trade.is_reorg_acknowledged() {
        debug!(
            target: "orderbook",
            tx_hash = ?trade_id.tx_hash,
            log_index = trade_id.log_index,
            "Reorg already fully acknowledged; skipping reversal"
        );
        // Self-heal: a prior delivery may have crashed between AcknowledgeReorg
        // and SettleReorg, leaving the reversed `trade_id` in the position's
        // bounded pending-reorg set. SettleReorg is an idempotent no-op when the
        // entry is already pruned (ADR 0012), mirroring how the fill path's
        // already-acknowledged branch re-settles.
        position
            .send(
                &trade.symbol,
                PositionCommand::SettleReorg {
                    trade_id: position_trade_id,
                },
            )
            .await?;
        return Ok(());
    }

    // Start the reversal if it has not begun. `AlreadyReorged` means a prior
    // delivery already started it (and crashed before acknowledging); fall
    // through to the resume below either way.
    if !trade.is_reorged() {
        match onchain_trade
            .send(trade_id, OnChainTradeCommand::RecordReorg { reorg_depth })
            .await
        {
            Ok(())
            | Err(AggregateError::UserError(LifecycleError::Apply(
                OnChainTradeError::AlreadyReorged,
            ))) => {}
            Err(error) => return Err(error.into()),
        }
    }

    // Reverse the position. `DuplicateReorg` means this resume re-drove a
    // reversal an earlier delivery already applied -- idempotent, so proceed to
    // the marker. On a resume, `reorg_depth` is recomputed from the current tip
    // and may exceed the depth the `OnChainTrade` recorded at first detection;
    // that divergence is acceptable because `reorg_depth` is audit-only and never
    // gates the reversal (amount/direction come from the persisted trade).
    match position
        .send(
            &trade.symbol,
            PositionCommand::RecordReorg {
                trade_id: position_trade_id.clone(),
                amount: FractionalShares::new(trade.amount),
                direction: trade.direction,
                price_usdc: trade.price_usdc,
                reorg_depth,
            },
        )
        .await
    {
        Ok(())
        | Err(AggregateError::UserError(LifecycleError::Apply(PositionError::DuplicateReorg {
            ..
        }))) => {}
        Err(error) => return Err(error.into()),
    }

    // Mark the reversal complete only now that the position write has succeeded,
    // so a crash before this point resumes the position reversal on retry.
    match onchain_trade
        .send(trade_id, OnChainTradeCommand::AcknowledgeReorg)
        .await
    {
        Ok(())
        | Err(AggregateError::UserError(LifecycleError::Apply(
            OnChainTradeError::AlreadyReorgAcknowledged,
        ))) => {}
        Err(error) => return Err(error.into()),
    }

    // Prune the reversed `trade_id` from the position's bounded pending-reorg set
    // now that the reorg-ack marker is durable (ADR 0012). A no-op when the entry
    // is already gone, so a re-delivered range re-settles harmlessly.
    position
        .send(
            &trade.symbol,
            PositionCommand::SettleReorg {
                trade_id: position_trade_id,
            },
        )
        .await?;

    Ok(())
}

/// Re-witnesses a reorged fill on its new canonical block and re-applies its
/// position impact, after [`reverse_reorged_fill`] settled the reversal. This is
/// what stops a block-hash reorg from leaving the position permanently flat: the
/// same fill was re-mined, so its impact must be restored.
///
/// `ReWitness` clears the `OnChainTrade` reorg/ack markers, returning the fill to
/// a fresh post-witness state; the normal `AcknowledgeOnChainFill` -> `Acknowledge`
/// -> `SettleOnChainFill` sequence then re-applies it. Every step is idempotent
/// on resume: `already_rewitnessed` skips the `ReWitness` send (a re-send would be
/// rejected `NotReorgAcknowledged` once the markers are cleared), `DuplicateTrade`
/// absorbs a position re-apply an earlier delivery (or the re-served log's own
/// `AccountForDexTrade` job) already did, and `AlreadyAcknowledged` absorbs a
/// re-driven marker. Runs under the caller's per-symbol lock.
async fn reapply_reincluded_fill(
    onchain_trade: &Store<OnChainTrade>,
    position: &Store<Position>,
    trade: &OnChainTrade,
    trade_id: &OnChainTradeId,
    reinclusion: &ReInclusion,
    threshold: ExecutionThreshold,
    already_rewitnessed: bool,
) -> Result<(), OnChainError> {
    // Prefer the new canonical block's timestamp; fall back to the persisted
    // fill's when the re-scan log carried none (the same economic fill, so its
    // recorded time is a faithful stand-in) rather than fabricating one. The
    // timestamp is audit metadata only -- it never affects net or price.
    let block_timestamp = reinclusion.timestamp.unwrap_or(trade.block_timestamp);

    let position_trade_id = TradeId {
        tx_hash: trade_id.tx_hash,
        log_index: trade_id.log_index,
    };

    // Re-witness onto the new canonical block. Skip when a prior delivery already
    // did: `ReWitnessed` cleared the reorg-ack marker, so a re-send would be
    // rejected `NotReorgAcknowledged`.
    if !already_rewitnessed {
        onchain_trade
            .send(
                trade_id,
                OnChainTradeCommand::ReWitness {
                    block_number: reinclusion.number,
                    block_hash: Some(reinclusion.hash),
                    block_timestamp,
                },
            )
            .await?;
    }

    // Re-apply the fill's position impact. `DuplicateTrade` means a prior
    // delivery (or the re-served log's own `AccountForDexTrade` job) already
    // re-applied it; idempotent, so proceed to the marker. The threshold is only
    // consulted when the position needs initializing, which a re-mined fill never
    // does (it was applied, then reversed, on an existing position).
    match position
        .send(
            &trade.symbol,
            PositionCommand::AcknowledgeOnChainFill {
                symbol: trade.symbol.clone(),
                threshold,
                trade_id: position_trade_id.clone(),
                amount: FractionalShares::new(trade.amount),
                direction: trade.direction,
                price_usdc: trade.price_usdc,
                block_timestamp,
            },
        )
        .await
    {
        Ok(())
        | Err(AggregateError::UserError(LifecycleError::Apply(PositionError::DuplicateTrade {
            ..
        }))) => {}
        Err(error) => return Err(error.into()),
    }

    // Mark the re-applied fill acknowledged (idempotent via `AlreadyAcknowledged`)
    // only after the position write, so a crash before this resumes the re-apply.
    // The fill was just re-witnessed, so `CannotAcknowledgeReorgedFill` cannot
    // occur under the held lock; were it to, propagating it retries the pair.
    match onchain_trade
        .send(trade_id, OnChainTradeCommand::Acknowledge)
        .await
    {
        Ok(())
        | Err(AggregateError::UserError(LifecycleError::Apply(
            OnChainTradeError::AlreadyAcknowledged,
        ))) => {}
        Err(error) => return Err(error.into()),
    }

    // Prune the pending-acknowledgement entry now the marker is durable (ADR
    // 0010). A no-op when already pruned, so a re-delivered range re-settles
    // harmlessly.
    position
        .send(
            &trade.symbol,
            PositionCommand::SettleOnChainFill {
                trade_id: position_trade_id,
            },
        )
        .await?;

    Ok(())
}

/// Loads the checkpoint and derives the backfill resume point in one call.
/// Test-only: production resolves the resume point in the fill monitor's poll
/// loop, which already holds the checkpoint, via
/// [`backfill_start_from_checkpoint`].
#[cfg(test)]
pub(crate) async fn backfill_start_block(
    pool: &SqlitePool,
    evm_ctx: &EvmCtx,
) -> Result<u64, OnChainError> {
    let checkpoint = load_backfill_checkpoint(pool, evm_ctx).await?;
    Ok(backfill_start_from_checkpoint(
        checkpoint,
        evm_ctx.deployment_block,
    ))
}

pub(crate) async fn load_backfill_checkpoint(
    pool: &SqlitePool,
    evm_ctx: &EvmCtx,
) -> Result<Option<u64>, OnChainError> {
    let row = sqlx::query_as::<_, (i64,)>(
        "SELECT last_processed_block FROM backfill_checkpoints WHERE orderbook = ?",
    )
    .bind(evm_ctx.orderbook.to_string())
    .fetch_optional(pool)
    .await?;

    row.map(|(last_processed_block,)| u64::try_from(last_processed_block))
        .transpose()
        .map_err(OnChainError::IntConversion)
}

pub(crate) async fn save_backfill_checkpoint(
    pool: &SqlitePool,
    evm_ctx: &EvmCtx,
    last_processed_block: u64,
) -> Result<(), OnChainError> {
    let last_processed_block = i64::try_from(last_processed_block)?;

    sqlx::query(
        "INSERT INTO backfill_checkpoints (orderbook, last_processed_block) \
         VALUES (?, ?) \
         ON CONFLICT(orderbook) DO UPDATE SET \
         last_processed_block = MAX( \
             excluded.last_processed_block, \
             backfill_checkpoints.last_processed_block \
         ), \
         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
    )
    .bind(evm_ctx.orderbook.to_string())
    .bind(last_processed_block)
    .execute(pool)
    .await?;

    Ok(())
}

#[tracing::instrument(
    target = "orderbook",
    skip(provider, evm_ctx, retry_strategy, job_queue),
    fields(batch_start, batch_end),
    level = tracing::Level::DEBUG,
)]
async fn enqueue_batch_events<P: Provider + Clone, B: BackoffBuilder + Clone>(
    provider: &P,
    evm_ctx: &EvmCtx,
    batch_start: u64,
    batch_end: u64,
    retry_strategy: B,
    mut job_queue: DexTradeAccountingJobQueue,
) -> Result<BatchOutcome, OnChainError> {
    let clear_filter = Filter::new()
        .address(evm_ctx.orderbook)
        .from_block(batch_start)
        .to_block(batch_end)
        .event_signature(ClearV3::SIGNATURE_HASH);

    let take_filter = Filter::new()
        .address(evm_ctx.orderbook)
        .from_block(batch_start)
        .to_block(batch_end)
        .event_signature(TakeOrderV3::SIGNATURE_HASH);

    let provider_clear = provider.clone();
    let provider_take = provider.clone();
    let clear_filter_clone = clear_filter.clone();
    let take_filter_clone = take_filter.clone();

    let get_clear_logs = move || {
        let provider = provider_clear.clone();
        let filter = clear_filter_clone.clone();
        async move { fetch_logs_with_tip_check(&provider, &filter, batch_end).await }
    };
    let get_take_logs = move || {
        let provider = provider_take.clone();
        let filter = take_filter_clone.clone();
        async move { fetch_logs_with_tip_check(&provider, &filter, batch_end).await }
    };

    let (clear_logs, take_logs) = future::try_join(
        get_clear_logs
            .retry(retry_strategy.clone().build())
            .notify(|err, dur| {
                trace!(target: "orderbook", "Retrying clear_logs for blocks between {batch_start}-{batch_end} after error: {err} (waiting {dur:?})");
            }),
        get_take_logs
            .retry(retry_strategy.build())
            .notify(|err, dur| {
                trace!(target: "orderbook", "Retrying take_logs for blocks between {batch_start}-{batch_end} after error: {err} (waiting {dur:?})");
            }),
    )
    .await?;

    debug!(
        target: "orderbook",
        total_clear_logs = %clear_logs.len(),
        total_take_logs = %take_logs.len(),
        "Processed a batch of blocks from {batch_start} to {batch_end}",
    );

    let mut removed_trades = Vec::new();
    let mut present_logs = Vec::new();
    let mut present_log_ids = Vec::new();
    for log in clear_logs
        .into_iter()
        .chain(take_logs)
        .sorted_by_key(|log| (log.block_number, log.log_index))
    {
        // The ingestion cutoff (currently the `safe` block) is not final: an L1
        // reorg of the not-yet-finalized batch can still drop a fill. A
        // `removed: true` log at or below the cutoff is such a reorg -- record it
        // for reversal rather than silently dropping a vanished, already-accounted
        // event.
        //
        // NOTE: this `removed: true` path is currently INERT in production. It
        // relied on `eth_getFilterChanges`/subscription notifications, which set
        // `removed: true` on reorged-out logs; since the monitor moved to
        // continuous `eth_getLogs` polling and backfill uses `eth_getLogs` range
        // queries, neither path ever surfaces `removed: true` (a range query
        // returns only the current canonical logs). The authoritative reorg
        // detector is the block-hash-mismatch re-scan (`detect_block_hash_reorgs`):
        // the present logs collected below carry their freshly-observed
        // `block_hash`, which the caller compares against the one persisted for the
        // witnessed `(tx_hash, log_index)`. This `removed: true` branch is kept as
        // correct scaffolding; tests exercise it by synthesizing `removed: true`
        // logs.
        if !log.removed {
            // Collect the identity so the caller can compare this freshly-observed
            // block_hash against the one persisted for the same (tx_hash,
            // log_index); a mismatch is a fork swap that re-served the key on a
            // different block. Logs missing identity fields cannot be
            // fork-checked but are still accounted via the present-log decode.
            if let (Some(tx_hash), Some(log_index), Some(block_number)) =
                (log.transaction_hash, log.log_index, log.block_number)
            {
                present_log_ids.push(PresentLog {
                    tx_hash,
                    log_index,
                    block_number,
                    block_hash: log.block_hash,
                    block_timestamp: log.block_timestamp.and_then(|seconds| {
                        i64::try_from(seconds)
                            .ok()
                            .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
                    }),
                });
            }
            present_logs.push(log);
            continue;
        }

        let (Some(tx_hash), Some(log_index), Some(block_number)) =
            (log.transaction_hash, log.log_index, log.block_number)
        else {
            // Fail the batch rather than dropping the reorg: a `continue` here lets
            // the checkpoint advance past an unreversible removed log, permanently
            // losing the reversal. Erroring makes apalis retry once the provider
            // returns complete identity fields.
            return Err(OnChainError::RemovedLogMissingIdentity {
                tx_hash: log.transaction_hash,
                log_index: log.log_index,
                block_number: log.block_number,
            });
        };

        error!(
            target: "orderbook",
            ?tx_hash,
            log_index,
            block_number,
            "Backfill returned `removed: true` for a log at or below the ingestion cutoff -- \
             reorg detected; recording reversal"
        );
        removed_trades.push(RemovedTrade {
            trade_id: OnChainTradeId { tx_hash, log_index },
            block_number,
            // A `removed: true` log is a truly dropped fill: the canonical chain
            // no longer holds it, so there is nothing to re-apply.
            reinclusion: None,
        });
    }

    let trade_events = present_logs
        .into_iter()
        .filter_map(|log| {
            if let Ok(clear_event) = log.log_decode::<ClearV3>() {
                let event = RaindexTradeEvent::ClearV3(Box::new(clear_event.data().clone()));
                Some((event, log))
            } else if let Ok(take_event) = log.log_decode::<TakeOrderV3>() {
                let event = RaindexTradeEvent::TakeOrderV3(Box::new(take_event.data().clone()));
                Some((event, log))
            } else {
                None
            }
        })
        .filter_map(|(event, log)| {
            EmittedOnChain::<RaindexTradeEvent>::from_log(event, &log)
                .inspect_err(
                    |error| warn!(target: "orderbook", %error, "Failed to extract block inclusion metadata during backfill"),
                )
                .ok()
        })
        .collect::<Vec<_>>();

    let enqueued_count = trade_events.len();

    for trade_event in trade_events {
        job_queue
            .push(AccountForDexTrade { trade: trade_event })
            .await?;
    }

    Ok(BatchOutcome {
        enqueued: enqueued_count,
        removed: removed_trades,
        present: present_log_ids,
    })
}

/// Wraps `eth_getLogs` with a read-after-write check on the node's tip.
///
/// An RPC node behind a load balancer can answer a `getLogs` request
/// for a block range it has not finished indexing -- the response comes
/// back empty even though the chain contains events in that range. The
/// bot would then trust the empty result, advance its checkpoint, and
/// silently drop those events. Verifying that the responding node's
/// `eth_blockNumber` is at least `to_block` after the `getLogs` call
/// catches that case; returning [`OnChainError::NodeLaggingBehindRequest`]
/// lets the surrounding retry loop reissue the request, which routes
/// through the load balancer to a node that has caught up.
async fn fetch_logs_with_tip_check<P: Provider>(
    provider: &P,
    filter: &Filter,
    to_block: u64,
) -> Result<Vec<alloy::rpc::types::Log>, OnChainError> {
    let logs = provider.get_logs(filter).await?;
    let observed_tip = provider.get_block_number().await?;
    if observed_tip < to_block {
        return Err(OnChainError::NodeLaggingBehindRequest {
            observed_tip,
            required_tip: to_block,
        });
    }
    Ok(logs)
}

fn generate_batch_ranges(start_block: u64, end_block: u64) -> Vec<(u64, u64)> {
    const BACKFILL_BATCH_SIZE: usize = 1_000;

    (start_block..=end_block)
        .step_by(BACKFILL_BATCH_SIZE)
        .map(|batch_start| {
            let batch_end = (batch_start + u64::try_from(BACKFILL_BATCH_SIZE).unwrap_or(u64::MAX)
                - 1)
            .min(end_block);
            (batch_start, batch_end)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use alloy::primitives::{
        Address, B256, Bytes, FixedBytes, IntoLogData, LogData, TxHash, U256, address, fixed_bytes,
        uint,
    };
    use alloy::providers::{ProviderBuilder, mock::Asserter};
    use alloy::rpc::types::{Block, Log, Transaction};
    use chrono::Utc;
    use rain_math_float::Float;
    use st0x_config::{
        EvmCtx, ExecutionThreshold, IngestionCutoff, create_test_ctx_with_order_owner,
    };
    use url::Url;

    use st0x_event_sorcery::StoreBuilder;
    use st0x_execution::{Direction, MockExecutorCtx, Symbol, TryIntoExecutor};
    use st0x_float_macro::float;

    use super::*;
    use crate::bindings::IRaindexV6;
    use crate::conductor::TradeProcessingCqrs;
    use crate::offchain::order::{OffchainOrder, noop_order_placer};
    use crate::onchain::pyth::PythFeedIds;
    use crate::symbol::cache::SymbolCache;
    use crate::test_utils::{get_test_order, setup_test_db, setup_test_pools};
    use crate::vault_registry::VaultRegistry;

    fn test_retry_strategy() -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_max_times(2) // Only 2 retries for tests (3 attempts total)
            .with_min_delay(Duration::from_millis(1))
            .with_max_delay(Duration::from_millis(10))
    }

    fn setup_job_queue(apalis_pool: &apalis_sqlite::SqlitePool) -> DexTradeAccountingJobQueue {
        DexTradeAccountingJobQueue::new(apalis_pool)
    }

    async fn job_count(apalis_pool: &apalis_sqlite::SqlitePool) -> i64 {
        sqlx_apalis::query_scalar::<_, i64>("SELECT COUNT(*) FROM Jobs")
            .fetch_one(apalis_pool)
            .await
            .unwrap()
    }

    /// Pushes an `eth_blockNumber` response far above any test block range so
    /// the read-after-write tip check in [`fetch_logs_with_tip_check`] passes.
    fn push_tip_response(asserter: &Asserter) {
        asserter.push_success(&serde_json::json!("0xffffffff"));
    }

    #[tokio::test]
    async fn test_backfill_start_block_uses_deployment_block_without_checkpoint() {
        let pool = setup_test_db().await;
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 50,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let start_block = backfill_start_block(&pool, &evm_ctx).await.unwrap();

        assert_eq!(start_block, 50);
    }

    #[tokio::test]
    async fn test_backfill_start_block_resumes_after_checkpoint() {
        let pool = setup_test_db().await;
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 50,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        save_backfill_checkpoint(&pool, &evm_ctx, 80).await.unwrap();

        let start_block = backfill_start_block(&pool, &evm_ctx).await.unwrap();

        assert_eq!(start_block, 81);
    }

    #[tokio::test]
    async fn test_backfill_start_block_respects_deployment_block_floor() {
        let pool = setup_test_db().await;
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 50,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        save_backfill_checkpoint(&pool, &evm_ctx, 20).await.unwrap();

        let start_block = backfill_start_block(&pool, &evm_ctx).await.unwrap();

        assert_eq!(start_block, 50);
    }

    #[test]
    fn backfill_start_from_checkpoint_uses_deployment_block_without_checkpoint() {
        assert_eq!(backfill_start_from_checkpoint(None, 50), 50);
    }

    #[test]
    fn backfill_start_from_checkpoint_resumes_after_checkpoint() {
        assert_eq!(backfill_start_from_checkpoint(Some(80), 50), 81);
    }

    #[test]
    fn backfill_start_from_checkpoint_floors_at_deployment_block() {
        assert_eq!(backfill_start_from_checkpoint(Some(20), 50), 50);
    }

    #[tokio::test]
    async fn test_backfill_events_empty_results() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(job_count(&apalis_pool).await, 0);
        assert_eq!(
            load_backfill_checkpoint(&pool, &evm_ctx).await.unwrap(),
            Some(100)
        );
    }

    #[tokio::test]
    async fn test_backfill_events_skips_when_checkpoint_is_caught_up() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        save_backfill_checkpoint(&pool, &evm_ctx, 100)
            .await
            .unwrap();

        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(job_count(&apalis_pool).await, 0);
        assert_eq!(
            load_backfill_checkpoint(&pool, &evm_ctx).await.unwrap(),
            Some(100)
        );
    }

    #[tokio::test]
    async fn test_backfill_events_skip_preserves_newer_checkpoint() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        save_backfill_checkpoint(&pool, &evm_ctx, 100)
            .await
            .unwrap();

        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            99,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(
            load_backfill_checkpoint(&pool, &evm_ctx).await.unwrap(),
            Some(100)
        );
    }

    #[tokio::test]
    async fn test_save_backfill_checkpoint_is_monotonic() {
        let pool = setup_test_db().await;
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        save_backfill_checkpoint(&pool, &evm_ctx, 100)
            .await
            .unwrap();
        save_backfill_checkpoint(&pool, &evm_ctx, 80).await.unwrap();

        assert_eq!(
            load_backfill_checkpoint(&pool, &evm_ctx).await.unwrap(),
            Some(100)
        );
    }

    #[test]
    fn test_generate_batch_ranges_single_batch() {
        let ranges = generate_batch_ranges(100, 500);
        assert_eq!(ranges, vec![(100, 500)]);
    }

    #[test]
    fn test_generate_batch_ranges_exact_batch_size() {
        let ranges = generate_batch_ranges(100, 1099);
        assert_eq!(ranges, vec![(100, 1099)]);
    }

    #[test]
    fn test_generate_batch_ranges_multiple_batches() {
        let ranges = generate_batch_ranges(100, 2500);
        assert_eq!(ranges, vec![(100, 1099), (1100, 2099), (2100, 2500)]);
    }

    #[test]
    fn test_generate_batch_ranges_large_range() {
        let ranges = generate_batch_ranges(5000, 25000);
        assert_eq!(
            ranges,
            vec![
                (5000, 5999),
                (6000, 6999),
                (7000, 7999),
                (8000, 8999),
                (9000, 9999),
                (10000, 10999),
                (11000, 11999),
                (12000, 12999),
                (13000, 13999),
                (14000, 14999),
                (15000, 15999),
                (16000, 16999),
                (17000, 17999),
                (18000, 18999),
                (19000, 19999),
                (20000, 20999),
                (21000, 21999),
                (22000, 22999),
                (23000, 23999),
                (24000, 24999),
                (25000, 25000)
            ]
        );
    }

    #[test]
    fn test_generate_batch_ranges_boundary() {
        let ranges = generate_batch_ranges(100, 25000);
        assert_eq!(
            ranges,
            vec![
                (100, 1099),
                (1100, 2099),
                (2100, 3099),
                (3100, 4099),
                (4100, 5099),
                (5100, 6099),
                (6100, 7099),
                (7100, 8099),
                (8100, 9099),
                (9100, 10099),
                (10100, 11099),
                (11100, 12099),
                (12100, 13099),
                (13100, 14099),
                (14100, 15099),
                (15100, 16099),
                (16100, 17099),
                (17100, 18099),
                (18100, 19099),
                (19100, 20099),
                (20100, 21099),
                (21100, 22099),
                (22100, 23099),
                (23100, 24099),
                (24100, 25000)
            ]
        );
    }

    #[test]
    fn test_generate_batch_ranges_single_block() {
        let ranges = generate_batch_ranges(42, 42);
        assert_eq!(ranges, vec![(42, 42)]);
    }

    #[test]
    fn test_generate_batch_ranges_empty() {
        let ranges = generate_batch_ranges(100, 99);
        assert_eq!(ranges.len(), 0);
    }

    #[tokio::test]
    async fn test_backfill_events_with_clear_v3_events() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let order = get_test_order();
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let clear_config = IRaindexV6::ClearConfigV2 {
            aliceInputIOIndex: U256::from(0),
            aliceOutputIOIndex: U256::from(1),
            bobInputIOIndex: U256::from(1),
            bobOutputIOIndex: U256::from(0),
            aliceBountyVaultId: B256::ZERO,
            bobBountyVaultId: B256::ZERO,
        };

        let clear_event = IRaindexV6::ClearV3 {
            sender: address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
            alice: order.clone(),
            bob: order.clone(),
            clearConfig: clear_config,
        };

        let clear_log = Log {
            inner: alloy::primitives::Log {
                address: evm_ctx.orderbook,
                data: clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(50),
            block_timestamp: None,
            transaction_hash: Some(fixed_bytes!(
                "0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            )),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([clear_log])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(job_count(&apalis_pool).await, 1);
    }

    #[tokio::test]
    async fn test_backfill_events_with_take_order_v3_events() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let order = get_test_order();
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let take_event = IRaindexV6::TakeOrderV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            config: IRaindexV6::TakeOrderConfigV4 {
                order: order.clone(),
                inputIOIndex: U256::from(0),
                outputIOIndex: U256::from(1),
                signedContext: Vec::new(),
            },
            input: Float::from_fixed_decimal_lossy(uint!(100_000_000_U256), 0)
                .unwrap()
                .0
                .get_inner(),
            output: Float::from_fixed_decimal_lossy(uint!(9_000_000_000_000_000_000_U256), 18)
                .unwrap()
                .0
                .get_inner(),
        };

        let take_log = Log {
            inner: alloy::primitives::Log {
                address: evm_ctx.orderbook,
                data: take_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(50),
            block_timestamp: None,
            transaction_hash: Some(fixed_bytes!(
                "0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            )),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events (empty)
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([take_log])); // take events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(job_count(&apalis_pool).await, 1);
    }

    #[tokio::test]
    async fn test_backfill_events_enqueues_all_events() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let different_order = get_test_order();
        let clear_event = IRaindexV6::ClearV3 {
            sender: address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
            alice: different_order.clone(),
            bob: different_order.clone(),
            clearConfig: IRaindexV6::ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let take_event = IRaindexV6::TakeOrderV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            config: IRaindexV6::TakeOrderConfigV4 {
                order: different_order.clone(),
                inputIOIndex: U256::from(0),
                outputIOIndex: U256::from(1),
                signedContext: Vec::new(),
            },
            input: Float::from_fixed_decimal_lossy(uint!(100_000_000_U256), 0)
                .unwrap()
                .0
                .get_inner(),
            output: Float::from_fixed_decimal_lossy(uint!(9_000_000_000_000_000_000_U256), 18)
                .unwrap()
                .0
                .get_inner(),
        };

        let tx_hash1 =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let tx_hash2 =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");

        let clear_log = Log {
            inner: alloy::primitives::Log {
                address: evm_ctx.orderbook,
                data: clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(50),
            block_timestamp: None,
            transaction_hash: Some(tx_hash1),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let take_log = Log {
            inner: alloy::primitives::Log {
                address: evm_ctx.orderbook,
                data: take_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(60),
            block_timestamp: None,
            transaction_hash: Some(tx_hash2),
            transaction_index: None,
            log_index: Some(2),
            removed: false,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([clear_log]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([take_log]));
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(job_count(&apalis_pool).await, 2);
    }

    #[tokio::test]
    async fn test_backfill_events_rpc_failure() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let asserter = Asserter::new();
        // All retry attempts fail - need double since clear_logs and take_logs retry in parallel
        // With test retry strategy: 2 retries = 3 total attempts per call
        for _ in 0..3 {
            asserter.push_failure_msg("RPC connection error");
        }
        for _ in 0..3 {
            asserter.push_failure_msg("RPC connection error");
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await;

        assert!(matches!(result.unwrap_err(), OnChainError::RpcTransport(_)));
        assert_eq!(
            load_backfill_checkpoint(&pool, &evm_ctx).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn test_backfill_events_block_range() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 50,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(job_count(&apalis_pool).await, 0);
    }

    fn create_test_take_event(
        order: &IRaindexV6::OrderV4,
        input: U256,
        output: U256,
    ) -> IRaindexV6::TakeOrderV3 {
        IRaindexV6::TakeOrderV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            config: IRaindexV6::TakeOrderConfigV4 {
                order: order.clone(),
                inputIOIndex: U256::from(0),
                outputIOIndex: U256::from(1),
                signedContext: Vec::new(),
            },

            input: Float::from_fixed_decimal_lossy(input, 0)
                .unwrap()
                .0
                .get_inner(),

            output: Float::from_fixed_decimal_lossy(output, 18)
                .unwrap()
                .0
                .get_inner(),
        }
    }

    fn create_test_log(
        orderbook: Address,
        event: &IRaindexV6::TakeOrderV3,
        block_number: u64,
        tx_hash: FixedBytes<32>,
    ) -> Log {
        Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(block_number),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        }
    }

    /// A burst-sized batch must enqueue exactly one job per
    /// `(tx_hash, log_index)` in chronological `(block_number, log_index)`
    /// order regardless of the order the node returned the logs in. The
    /// sort matters under load: jobs drain through a `concurrency(1)`
    /// worker, so out-of-order enqueue means out-of-order position
    /// accumulation against a live hedging threshold.
    #[tokio::test]
    async fn burst_batch_enqueues_one_job_per_log_in_chronological_order() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let order = get_test_order();
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        // 30 take logs, 3 per block across 10 blocks, served by the
        // mock node in reverse-chronological order.
        let logs: Vec<Log> = (0..30u64)
            .map(|index| {
                let take_event = create_test_take_event(
                    &order,
                    uint!(100_000_000_U256),
                    uint!(1_000_000_000_000_000_000_U256),
                );
                let mut hash_bytes = [0u8; 32];
                hash_bytes[0] = 0xab;
                hash_bytes[31] = u8::try_from(index).unwrap();

                let mut log = create_test_log(
                    evm_ctx.orderbook,
                    &take_event,
                    10 + index / 3,
                    FixedBytes::from(hash_bytes),
                );
                log.log_index = Some(index % 3);
                log
            })
            .collect();
        let reversed: Vec<&Log> = logs.iter().rev().collect();

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!(reversed));
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        let payloads: Vec<Vec<u8>> = sqlx::query_scalar("SELECT job FROM Jobs ORDER BY rowid ASC")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(payloads.len(), 30, "one job per log, none dropped");

        let jobs: Vec<AccountForDexTrade> = payloads
            .iter()
            .map(|payload| serde_json::from_slice(payload).unwrap())
            .collect();

        let enqueue_order: Vec<(u64, u64)> = jobs
            .iter()
            .map(|job| (job.trade.block_number, job.trade.log_index))
            .collect();
        let mut chronological = enqueue_order.clone();
        chronological.sort_unstable();
        assert_eq!(
            enqueue_order, chronological,
            "Jobs must be enqueued in chronological (block, log_index) \
             order despite the node serving them reversed"
        );

        // The 30 enqueued jobs map one-to-one onto the 30 distinct input logs:
        // no two logs collapsed onto a shared (tx_hash, log_index) key and --
        // together with the count check above -- none was enqueued twice. Note
        // the inputs are all distinct, so this guards the 1:1 mapping, not an
        // enqueue-time dedup; per-fill dedup proper lives downstream in
        // `process_queued_trade`.
        let distinct: HashSet<(TxHash, u64)> = jobs
            .iter()
            .map(|job| (job.trade.tx_hash, job.trade.log_index))
            .collect();
        assert_eq!(
            distinct.len(),
            30,
            "the 30 jobs must carry 30 distinct (tx_hash, log_index) keys"
        );
    }

    #[tokio::test]
    async fn test_backfill_events_preserves_chronological_order() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let order = get_test_order();
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let tx_hash1 =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let tx_hash2 =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");

        let take_event1 = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(1_000_000_000_000_000_000_U256),
        );
        let take_event2 = create_test_take_event(
            &order,
            uint!(200_000_000_U256),
            uint!(2_000_000_000_000_000_000_U256),
        );

        let take_log1 = create_test_log(evm_ctx.orderbook, &take_event1, 50, tx_hash1);
        let take_log2 = create_test_log(evm_ctx.orderbook, &take_event2, 100, tx_hash2);

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([take_log2, take_log1]));
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(job_count(&apalis_pool).await, 2);
    }

    #[tokio::test]
    async fn test_backfill_events_batch_count_verification() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1000,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let asserter = Asserter::new();

        // Batch 1: blocks 1000-1999
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);

        // Batch 2: blocks 2000-2500
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            2500,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(job_count(&apalis_pool).await, 0);
    }

    #[tokio::test]
    async fn test_backfill_events_batch_boundary_verification() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 500,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let asserter = Asserter::new();

        // Batch 1: blocks 500-1499
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);

        // Batch 2: blocks 1500-1900
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            1900,
            get_backfill_retry_strat(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(job_count(&apalis_pool).await, 0);
    }

    #[tokio::test]
    async fn test_process_batch_with_realistic_data() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let order = get_test_order();
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let take_event = create_test_take_event(
            &order,
            uint!(500_000_000_U256),
            uint!(5_000_000_000_000_000_000_U256),
        );
        let tx_hash =
            fixed_bytes!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let take_log = create_test_log(evm_ctx.orderbook, &take_event, 150, tx_hash);

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([take_log]));
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let enqueued_count = enqueue_batch_events(
            &provider,
            &evm_ctx,
            100,
            200,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap()
        .enqueued;

        assert_eq!(enqueued_count, 1);
        assert_eq!(job_count(&apalis_pool).await, 1);
    }

    #[tokio::test]
    async fn test_backfill_events_large_block_range_batching() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let asserter = Asserter::new();

        for _ in 0..3 {
            asserter.push_success(&serde_json::json!([]));
            push_tip_response(&asserter);
            asserter.push_success(&serde_json::json!([]));
            push_tip_response(&asserter);
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            3000,
            get_backfill_retry_strat(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(job_count(&apalis_pool).await, 0);
    }

    #[tokio::test]
    async fn test_backfill_events_mixed_valid_and_invalid_events() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let order = get_test_order();
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let valid_take_event = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(9_000_000_000_000_000_000_U256),
        );

        // Create different order with different hash to make it invalid
        let mut different_order = get_test_order();
        different_order.nonce =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let invalid_take_event = create_test_take_event(
            &different_order,
            uint!(50_000_000_U256),
            uint!(5_000_000_000_000_000_000_U256),
        );

        let valid_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let invalid_tx_hash =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");
        let valid_log = create_test_log(evm_ctx.orderbook, &valid_take_event, 50, valid_tx_hash);
        let invalid_log =
            create_test_log(evm_ctx.orderbook, &invalid_take_event, 51, invalid_tx_hash);

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([valid_log, invalid_log]));
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        // Both events should be enqueued (filtering happens during processing, not backfill)
        assert_eq!(job_count(&apalis_pool).await, 2);
    }

    fn create_clear_log(orderbook: Address, order: &IRaindexV6::OrderV4, tx_hash: TxHash) -> Log {
        let clear_config = IRaindexV6::ClearConfigV2 {
            aliceInputIOIndex: U256::from(0),
            aliceOutputIOIndex: U256::from(1),
            bobInputIOIndex: U256::from(1),
            bobOutputIOIndex: U256::from(0),
            aliceBountyVaultId: B256::ZERO,
            bobBountyVaultId: B256::ZERO,
        };

        let clear_event = IRaindexV6::ClearV3 {
            sender: address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
            alice: order.clone(),
            bob: order.clone(),
            clearConfig: clear_config,
        };

        Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(100),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        }
    }

    #[tokio::test]
    async fn test_backfill_events_mixed_clear_and_take_events() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let order = get_test_order();
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let tx_hash1 =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let tx_hash2 =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");

        let take_event = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(9_000_000_000_000_000_000_U256),
        );
        let take_log = create_test_log(evm_ctx.orderbook, &take_event, 50, tx_hash1);
        let clear_log = create_clear_log(evm_ctx.orderbook, &order, tx_hash2);

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([clear_log]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([take_log]));
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(job_count(&apalis_pool).await, 2);
    }

    #[tokio::test]
    async fn test_process_batch_retry_mechanism() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let asserter = Asserter::new();
        // First two calls fail, third succeeds
        asserter.push_failure_msg("RPC connection error");
        asserter.push_failure_msg("Timeout error");
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            100,
            200,
            test_retry_strategy(),
            job_queue,
        )
        .await;

        let enqueued_count = result.unwrap().enqueued;
        assert_eq!(enqueued_count, 0);
    }

    #[tokio::test]
    async fn test_process_batch_exhausted_retries() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let asserter = Asserter::new();
        // All retry attempts fail - need double since clear_logs and take_logs retry in parallel
        // With test retry strategy: 2 retries = 3 total attempts per call
        for _ in 0..3 {
            asserter.push_failure_msg("Persistent RPC error");
        }
        for _ in 0..3 {
            asserter.push_failure_msg("Persistent RPC error");
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            100,
            200,
            test_retry_strategy(),
            job_queue,
        )
        .await;

        assert!(matches!(result.unwrap_err(), OnChainError::RpcTransport(_)));
        assert_eq!(
            load_backfill_checkpoint(&pool, &evm_ctx).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn test_backfill_events_partial_batch_failure() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let asserter = Asserter::new();

        // First batch succeeds
        asserter.push_success(&serde_json::json!([])); // clear logs
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take logs
        push_tip_response(&asserter);

        // Second batch fails completely (after retries)
        // Need double the failures since clear_logs and take_logs retry in parallel
        // With test retry strategy: 2 retries = 3 total attempts per call
        for _ in 0..3 {
            asserter.push_failure_msg("Network failure");
        }
        for _ in 0..3 {
            asserter.push_failure_msg("Network failure");
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            25000,
            test_retry_strategy(),
            job_queue,
        )
        .await;

        assert!(matches!(result.unwrap_err(), OnChainError::RpcTransport(_)));
        assert_eq!(
            load_backfill_checkpoint(&pool, &evm_ctx).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn test_backfill_events_corrupted_log_data() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        // Create malformed log with invalid event signature
        let corrupted_log = Log {
            inner: alloy::primitives::Log::new(
                evm_ctx.orderbook,
                Vec::new(),
                Vec::from([0x00u8; 32]).into(),
            )
            .unwrap(),
            block_hash: None,
            block_number: Some(50),
            block_timestamp: None,
            transaction_hash: Some(fixed_bytes!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            )),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([corrupted_log]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        // Corrupted logs are silently ignored during backfill
        assert_eq!(job_count(&apalis_pool).await, 0);
    }

    #[tokio::test]
    async fn test_backfill_does_not_enqueue_removed_log_as_fill() {
        // Backfill caller caps `to_block` at the ingestion cutoff
        // block. A `removed: true` log implies a reorg; skip it
        // (logged as warn) rather than ingesting a vanished event.
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let order = get_test_order();
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let take_event = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(9_000_000_000_000_000_000_U256),
        );
        let tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let mut removed_log = create_test_log(evm_ctx.orderbook, &take_event, 50, tx_hash);
        removed_log.removed = true;

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([removed_log]));
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(
            job_count(&apalis_pool).await,
            0,
            "a removed log must not be enqueued as a fresh fill; it is surfaced for reversal"
        );
    }

    #[tokio::test]
    async fn test_backfill_events_single_block_range() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 42,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            42,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(job_count(&apalis_pool).await, 0);
    }

    #[tokio::test]
    async fn test_enqueue_batch_events_database_failure() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let order = get_test_order();
        let take_event = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(9_000_000_000_000_000_000_U256),
        );
        let take_log = create_test_log(
            evm_ctx.orderbook,
            &take_event,
            50,
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111"),
        );

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([take_log])); // take events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        // Close the apalis pool to simulate job-queue connection failure.
        apalis_pool.close().await;

        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            100,
            200,
            test_retry_strategy(),
            job_queue,
        )
        .await;

        // Should succeed at RPC level but fail at database level
        assert!(
            matches!(result, Err(OnChainError::JobQueue(_))),
            "Expected JobQueue error, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn test_enqueue_batch_events_errors_when_node_tip_behind_requested_range() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        // Both the clear and take fetches succeed at getLogs but observe a tip
        // (0x32 = 50) below the requested to_block (100), simulating a node
        // behind the load balancer that answered for a range it has not indexed.
        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear getLogs
        asserter.push_success(&serde_json::json!("0x32")); // clear tip = 50
        asserter.push_success(&serde_json::json!([])); // take getLogs
        asserter.push_success(&serde_json::json!("0x32")); // take tip = 50

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            1,
            100,
            ExponentialBuilder::default().with_max_times(0),
            job_queue,
        )
        .await;

        let error = result.unwrap_err();
        assert!(
            matches!(
                error,
                OnChainError::NodeLaggingBehindRequest {
                    observed_tip: 50,
                    required_tip: 100,
                }
            ),
            "expected NodeLaggingBehindRequest {{ observed_tip: 50, required_tip: 100 }}, got {error:?}"
        );
        assert_eq!(job_count(&apalis_pool).await, 0);
    }

    #[tokio::test]
    async fn test_enqueue_batch_events_filter_creation() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            100,
            150,
            test_retry_strategy(),
            job_queue,
        )
        .await;

        assert_eq!(result.unwrap().enqueued, 0);
    }

    #[tokio::test]
    async fn test_enqueue_batch_events_partial_enqueue_failure() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let order = get_test_order();

        let take_event1 = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(9_000_000_000_000_000_000_U256),
        );
        let take_event2 = create_test_take_event(
            &order,
            uint!(200_000_000_U256),
            uint!(18_000_000_000_000_000_000_U256),
        );

        let take_log1 = create_test_log(
            evm_ctx.orderbook,
            &take_event1,
            50,
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111"),
        );
        let take_log2 = create_test_log(
            evm_ctx.orderbook,
            &take_event2,
            51,
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222"),
        );

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([take_log1, take_log2])); // take events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            100,
            200,
            test_retry_strategy(),
            job_queue,
        )
        .await;

        let enqueued = result.unwrap().enqueued;
        assert_eq!(enqueued, 2);
    }

    #[tokio::test]
    async fn test_backfill_events_concurrent_batch_processing() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let order = get_test_order();
        let take_event = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(9_000_000_000_000_000_000_U256),
        );
        let take_log = create_test_log(
            evm_ctx.orderbook,
            &take_event,
            50,
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111"),
        );

        let asserter = Asserter::new();

        // blocks 1-3000 (3 batches), second batch has take events
        for batch_idx in 0..3 {
            asserter.push_success(&serde_json::json!([])); // clear events
            push_tip_response(&asserter);
            if batch_idx == 1 {
                asserter.push_success(&serde_json::json!([take_log])); // take events
            } else {
                asserter.push_success(&serde_json::json!([])); // take events
            }
            push_tip_response(&asserter);
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            3000,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(job_count(&apalis_pool).await, 1);
    }

    #[tokio::test]
    async fn test_enqueue_batch_events_retry_exponential_backoff() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let asserter = Asserter::new();
        // First attempt fails for both parallel calls
        asserter.push_failure_msg("Temporary network failure");
        asserter.push_failure_msg("Rate limit exceeded");
        // Second attempt succeeds for both
        asserter.push_success(&serde_json::json!([])); // clear events (retry)
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events (retry)
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let start_time = std::time::Instant::now();
        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            100,
            200,
            test_retry_strategy(),
            job_queue,
        )
        .await;
        let elapsed = start_time.elapsed();

        let enqueued = result.unwrap().enqueued;
        assert_eq!(enqueued, 0);

        // Should have taken at least the test initial delay time due to retries
        assert!(elapsed >= Duration::from_millis(1));
    }

    #[tokio::test]
    async fn test_backfill_events_zero_blocks() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 100,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        // No RPC calls should be made when deployment block > end block
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            50,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(job_count(&apalis_pool).await, 0);
    }

    #[tokio::test]
    async fn test_enqueue_batch_events_mixed_log_types() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let order = get_test_order();

        let clear_event = IRaindexV6::ClearV3 {
            sender: address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
            alice: order.clone(),
            bob: order.clone(),
            clearConfig: IRaindexV6::ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let clear_log = Log {
            inner: alloy::primitives::Log {
                address: evm_ctx.orderbook,
                data: clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(50),
            block_timestamp: None,
            transaction_hash: Some(fixed_bytes!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            )),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let take_event = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(9_000_000_000_000_000_000_U256),
        );
        let take_log = create_test_log(
            evm_ctx.orderbook,
            &take_event,
            51,
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222"),
        );

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([clear_log])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([take_log])); // take events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            100,
            200,
            test_retry_strategy(),
            job_queue,
        )
        .await;

        let enqueued = result.unwrap().enqueued;
        assert_eq!(enqueued, 2);
        assert_eq!(job_count(&apalis_pool).await, 2);
    }

    #[tokio::test]
    async fn test_backfill_starts_from_deployment_block() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 50,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        // Should start from deployment_block (50) to end_block (100)
        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events for 50-100
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events for 50-100
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(job_count(&apalis_pool).await, 0);
    }

    /// A `removed: true` log past the confirmation depth is surfaced as a
    /// `RemovedTrade` for reversal instead of being silently dropped.
    #[tokio::test]
    async fn backfill_surfaces_removed_log_as_reorg() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);

        let tx_hash = TxHash::repeat_byte(0xcd);
        let removed_log = Log {
            inner: alloy::primitives::Log {
                address: address!("0x1111111111111111111111111111111111111111"),
                data: LogData::new_unchecked(vec![], Bytes::new()),
            },
            block_hash: Some(B256::repeat_byte(0x11)),
            block_number: Some(50),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: Some(0),
            log_index: Some(3),
            removed: true,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([removed_log])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let removed = backfill_events(
            &provider,
            &evm_ctx,
            &pool,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(
            removed.len(),
            1,
            "the removed log must be surfaced for reversal"
        );
        assert_eq!(removed[0].trade_id.tx_hash, tx_hash);
        assert_eq!(removed[0].trade_id.log_index, 3);
        assert_eq!(removed[0].block_number, 50);
        // The vanished fill must NOT be enqueued as a fresh trade.
        assert_eq!(job_count(&apalis_pool).await, 0);
    }

    /// A `removed: true` log missing its `(tx_hash, log_index)` identity must
    /// fail the batch with `RemovedLogMissingIdentity` rather than silently
    /// advancing the checkpoint past a reorg it can never reverse.
    #[tokio::test]
    async fn enqueue_batch_events_fails_on_removed_log_without_identity() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        // A reorged-out log whose `transaction_hash` the provider dropped: the
        // batch must error, not skip, so apalis retries once identity is intact.
        let removed_log = Log {
            inner: alloy::primitives::Log {
                address: evm_ctx.orderbook,
                data: LogData::new_unchecked(vec![], Bytes::new()),
            },
            block_hash: Some(B256::repeat_byte(0x11)),
            block_number: Some(50),
            block_timestamp: None,
            transaction_hash: None,
            transaction_index: Some(0),
            log_index: Some(3),
            removed: true,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([removed_log])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let error = enqueue_batch_events(
            &provider,
            &evm_ctx,
            1,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                error,
                OnChainError::RemovedLogMissingIdentity {
                    tx_hash: None,
                    log_index: Some(3),
                    block_number: Some(50),
                }
            ),
            "a removed log missing its tx_hash must fail with RemovedLogMissingIdentity; got {error:?}"
        );
        assert_eq!(job_count(&apalis_pool).await, 0);
    }

    /// `BackfillRange::perform` measures reorg depth from the current tip. A tip
    /// behind the removed fill's block would underreport the depth, so the job
    /// must fail with `NodeLaggingBehindRequest` (apalis then retries against a
    /// caught-up node) rather than record a wrong depth.
    #[tokio::test]
    async fn perform_fails_when_tip_lags_behind_removed_block() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let ctx = create_test_ctx_with_order_owner(Address::ZERO);

        // The batch surfaces one removed fill at block 50, then perform reads a
        // tip of 1 -- behind the removed block, so the depth cannot be computed.
        let removed_log = Log {
            inner: alloy::primitives::Log {
                address: ctx.evm.orderbook,
                data: LogData::new_unchecked(vec![], Bytes::new()),
            },
            block_hash: Some(B256::repeat_byte(0x11)),
            block_number: Some(50),
            block_timestamp: None,
            transaction_hash: Some(TxHash::repeat_byte(0xcd)),
            transaction_index: Some(0),
            log_index: Some(3),
            removed: true,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([removed_log])); // clear getLogs
        push_tip_response(&asserter); // clear tip
        asserter.push_success(&serde_json::json!([])); // take getLogs
        push_tip_response(&asserter); // take tip
        asserter.push_success(&serde_json::json!("0x1")); // perform's tip = 1

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let executor = MockExecutorCtx.try_into_executor().await.unwrap();

        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (offchain_order, _offchain_order_projection) =
            StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(())
                .await
                .unwrap();
        let (vault_registry, _vault_registry_projection) =
            StoreBuilder::<VaultRegistry>::new(pool.clone())
                .build(())
                .await
                .unwrap();

        let cqrs = TradeProcessingCqrs {
            onchain_trade,
            position,
            position_projection,
            offchain_order,
            order_placer: noop_order_placer(),
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: ctx.assets.clone(),
            counter_trade_submission_lock: Arc::new(tokio::sync::Mutex::new(())),
            poll_status_queue: crate::offchain::order::PollOrderStatusJobQueue::new(&apalis_pool),
        };

        let accountant_ctx = AccountantCtx {
            orderbook: ctx.evm.orderbook,
            ctx,
            cache: SymbolCache::default(),
            pyth_feed_ids: PythFeedIds::default(),
            evm: st0x_evm::ReadOnlyEvm::new(provider),
            cqrs,
            vault_registry,
            executor,
            pool: pool.clone(),
            job_queue: DexTradeAccountingJobQueue::new(&apalis_pool),
        };

        let job = BackfillRange {
            from_block: 1,
            to_block: 100,
        };
        let error = job.perform(&accountant_ctx).await.unwrap_err();

        assert!(
            matches!(
                error,
                OnChainError::NodeLaggingBehindRequest {
                    observed_tip: 1,
                    required_tip: 50,
                }
            ),
            "a tip behind the removed block must fail with NodeLaggingBehindRequest; got {error:?}"
        );
        // The reorg must not be recorded against the lagging tip.
        assert_eq!(count_events(&pool, "OnChainTradeEvent::Reorged").await, 0);
        assert_eq!(count_events(&pool, "PositionEvent::Reorged").await, 0);
    }

    /// `record_reorg` appends `Reorged` to both aggregates through the CQRS
    /// framework (never direct SQL) and reverses the position to flat.
    #[tokio::test]
    async fn record_reorg_emits_reorged_events_through_cqrs() {
        let pool = setup_test_db().await;

        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let tx_hash = TxHash::repeat_byte(0xab);
        let log_index = 7;
        let now = Utc::now();

        seed_witnessed_fill(&onchain_trade, &position, &symbol, tx_hash, log_index, now).await;

        let removed = RemovedTrade {
            trade_id: OnChainTradeId { tx_hash, log_index },
            block_number: 100,
            reinclusion: None,
        };
        let before_reorg = Utc::now();
        record_reorg(
            &onchain_trade,
            &position,
            &removed,
            12,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();
        let after_reorg = Utc::now();

        assert_eq!(count_events(&pool, "OnChainTradeEvent::Reorged").await, 1);
        assert_eq!(count_events(&pool, "PositionEvent::Reorged").await, 1);
        assert_eq!(
            count_events(&pool, "OnChainTradeEvent::ReorgAcknowledged").await,
            1,
            "the reorg must be acknowledged exactly once after both reversals",
        );

        let position_state = position.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            position_state.net,
            FractionalShares::ZERO,
            "the reversal must return the position to flat",
        );
        let last_reorged_at = position_state
            .last_reorged_at
            .expect("the reversal must mark the position reorged");
        assert!(
            (before_reorg..=after_reorg).contains(&last_reorged_at),
            "last_reorged_at {last_reorged_at} must fall within the reversal window \
             [{before_reorg}, {after_reorg}]"
        );

        let trade_state = onchain_trade
            .load(&OnChainTradeId { tx_hash, log_index })
            .await
            .unwrap()
            .unwrap();
        assert!(
            trade_state.is_reorg_acknowledged(),
            "the reversal must be acknowledged once both aggregates are reversed",
        );
    }

    /// The critical ADR 0012 case: a crash after `OnChainTrade::RecordReorg` but
    /// before the `Position` reversal must NOT skip the reversal forever. A
    /// re-delivery resumes the position reversal and acknowledges it.
    #[tokio::test]
    async fn record_reorg_resumes_position_reversal_after_crash() {
        let pool = setup_test_db().await;

        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let tx_hash = TxHash::repeat_byte(0xab);
        let log_index = 7;
        let now = Utc::now();

        seed_witnessed_fill(&onchain_trade, &position, &symbol, tx_hash, log_index, now).await;

        // Simulate the crash: the reorg was recorded on the OnChainTrade, but the
        // process died before the Position reversal. The position still reflects
        // the (now-vanished) fill.
        onchain_trade
            .send(
                &OnChainTradeId { tx_hash, log_index },
                OnChainTradeCommand::RecordReorg { reorg_depth: 12 },
            )
            .await
            .unwrap();
        let pre_resume = position.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            pre_resume.net,
            FractionalShares::new(float!(5)),
            "premise: the position still reflects the fill before the resume",
        );

        // Re-delivery resumes: it must reverse the position and acknowledge.
        let removed = RemovedTrade {
            trade_id: OnChainTradeId { tx_hash, log_index },
            block_number: 100,
            reinclusion: None,
        };
        record_reorg(
            &onchain_trade,
            &position,
            &removed,
            12,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();

        let position_state = position.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            position_state.net,
            FractionalShares::ZERO,
            "the crashed reversal must be recovered, returning the position to flat",
        );
        assert_eq!(count_events(&pool, "PositionEvent::Reorged").await, 1);

        // The resumed reversal must carry the witnessed fill's price (150 USDC,
        // from `seed_witnessed_fill`) so the reactor reverses the inventory's
        // USDC leg with the real price instead of falling back to an equity
        // nudge. Assert the persisted event's serialized `price_usdc` against the
        // decimal-string literal the Float serializer emits.
        let reorged_payload: String = sqlx::query_scalar(
            "SELECT payload FROM events WHERE event_type = 'PositionEvent::Reorged'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let reorged: serde_json::Value = serde_json::from_str(&reorged_payload).unwrap();
        assert_eq!(
            reorged["Reorged"]["price_usdc"],
            serde_json::json!("150"),
            "the resumed Reorged event must carry the witnessed fill's price of 150 USDC",
        );

        let trade_state = onchain_trade
            .load(&OnChainTradeId { tx_hash, log_index })
            .await
            .unwrap()
            .unwrap();
        assert!(trade_state.is_reorg_acknowledged());
    }

    /// `record_reorg`'s position reversal and the live `AccountForDexTrade` fill
    /// path serialize on the SAME process-global per-symbol lock
    /// ([`get_symbol_lock`]), so a reorg reversal cannot interleave its
    /// `Position` writes with a concurrent live fill for the symbol and clobber
    /// the net. The fill path holds `get_symbol_lock(trade.symbol.base())` across
    /// its position writes (`trade_accountant`), and the reorg path holds
    /// `get_symbol_lock(&trade.symbol)` for the same base ticker; both resolve to
    /// one `Arc<Mutex>` in the global lock map. This test holds that exact lock
    /// and proves the reversal is gated on it: while held `record_reorg` parks on
    /// `lock().await` (it can never reach the reversal, so the timeout always
    /// elapses and the net stays at the fill), and releasing it lets the reversal
    /// run to flat.
    ///
    /// Limitation: this does not drive a real concurrent `AccountForDexTrade`
    /// against the reorg -- a fully deterministic two-writer interleaving would
    /// need production scheduling seams. It proves the contention mechanism (the
    /// shared per-symbol lock) that makes such an interleave impossible, which is
    /// the invariant under test, not a staged race.
    #[tokio::test]
    async fn record_reorg_reversal_blocks_on_the_per_symbol_lock() {
        let pool = setup_test_db().await;

        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        // A symbol unique to this test keeps the process-global per-symbol lock
        // uncontended by other tests sharing the static lock map.
        let symbol = Symbol::new("REORGLOCK").unwrap();
        let tx_hash = TxHash::repeat_byte(0xab);
        let log_index = 7;
        let now = Utc::now();

        seed_witnessed_fill(&onchain_trade, &position, &symbol, tx_hash, log_index, now).await;

        let removed = RemovedTrade {
            trade_id: OnChainTradeId { tx_hash, log_index },
            block_number: 100,
            reinclusion: None,
        };

        // Hold the same per-symbol lock the live fill path acquires. While it is
        // held `record_reorg` reaches `lock().await` and parks, so the reversal
        // can never run and the timeout always elapses.
        let guard = get_symbol_lock(&symbol).await.lock_owned().await;

        let blocked = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            record_reorg(
                &onchain_trade,
                &position,
                &removed,
                12,
                ExecutionThreshold::whole_share(),
            ),
        )
        .await;
        blocked.unwrap_err();

        let pre_release = position.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            pre_release.net,
            FractionalShares::new(float!(5)),
            "the reversal must not run while the per-symbol lock is held",
        );
        assert_eq!(
            count_events(&pool, "PositionEvent::Reorged").await,
            0,
            "no Reorged event may be emitted while the lock blocks the reversal",
        );

        // Releasing the lock lets the same reversal proceed and return to flat.
        drop(guard);
        record_reorg(
            &onchain_trade,
            &position,
            &removed,
            12,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();

        let reversed = position.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            reversed.net,
            FractionalShares::ZERO,
            "the reversal proceeds once the per-symbol lock is released",
        );
        assert_eq!(count_events(&pool, "PositionEvent::Reorged").await, 1);
    }

    /// A re-delivered backfill range must not double-reverse: once acknowledged,
    /// the reorg is a no-op, and an unacknowledged re-drive is caught by the
    /// position's `DuplicateReorg` guard (ADR 0012).
    #[tokio::test]
    async fn record_reorg_is_idempotent_across_redelivery() {
        let pool = setup_test_db().await;

        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let tx_hash = TxHash::repeat_byte(0xab);
        let log_index = 7;
        let now = Utc::now();

        seed_witnessed_fill(&onchain_trade, &position, &symbol, tx_hash, log_index, now).await;

        let removed = RemovedTrade {
            trade_id: OnChainTradeId { tx_hash, log_index },
            block_number: 100,
            reinclusion: None,
        };
        record_reorg(
            &onchain_trade,
            &position,
            &removed,
            12,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();
        record_reorg(
            &onchain_trade,
            &position,
            &removed,
            12,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();

        assert_eq!(count_events(&pool, "OnChainTradeEvent::Reorged").await, 1);
        assert_eq!(count_events(&pool, "PositionEvent::Reorged").await, 1);
        assert_eq!(
            count_events(&pool, "OnChainTradeEvent::ReorgAcknowledged").await,
            1,
            "a re-delivery must not re-emit the acknowledgement (idempotent)",
        );

        let position_state = position.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            position_state.net,
            FractionalShares::ZERO,
            "a re-delivery must not reverse the fill twice",
        );
    }

    /// ADR 0012: a crash after the `Position` reversal but before
    /// `AcknowledgeReorg` must not double-reverse. The re-delivery finds the
    /// position already reversed (`DuplicateReorg`, idempotent) and only needs to
    /// write the acknowledgement marker.
    #[tokio::test]
    async fn record_reorg_resumes_acknowledge_after_crash_post_position_reversal() {
        let pool = setup_test_db().await;

        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let tx_hash = TxHash::repeat_byte(0xab);
        let log_index = 7;
        let now = Utc::now();

        seed_witnessed_fill(&onchain_trade, &position, &symbol, tx_hash, log_index, now).await;

        // Simulate the crash AFTER both the OnChainTrade reorg marker and the
        // Position reversal succeeded, but BEFORE AcknowledgeReorg: both
        // aggregates are reversed yet the trade is not reorg-acknowledged.
        let trade_id = OnChainTradeId { tx_hash, log_index };
        onchain_trade
            .send(
                &trade_id,
                OnChainTradeCommand::RecordReorg { reorg_depth: 12 },
            )
            .await
            .unwrap();
        position
            .send(
                &symbol,
                PositionCommand::RecordReorg {
                    trade_id: TradeId { tx_hash, log_index },
                    amount: FractionalShares::new(float!(5)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    reorg_depth: 12,
                },
            )
            .await
            .unwrap();
        assert!(
            !onchain_trade
                .load(&trade_id)
                .await
                .unwrap()
                .unwrap()
                .is_reorg_acknowledged(),
            "premise: the reversal is not yet acknowledged",
        );

        // Re-delivery resumes: the position reversal is idempotent
        // (`DuplicateReorg`) and the trade is acknowledged without double-reversing.
        let removed = RemovedTrade {
            trade_id: OnChainTradeId { tx_hash, log_index },
            block_number: 100,
            reinclusion: None,
        };
        record_reorg(
            &onchain_trade,
            &position,
            &removed,
            12,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();

        let position_state = position.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            position_state.net,
            FractionalShares::ZERO,
            "the position must not be reversed a second time",
        );
        assert_eq!(count_events(&pool, "PositionEvent::Reorged").await, 1);
        assert!(
            onchain_trade
                .load(&trade_id)
                .await
                .unwrap()
                .unwrap()
                .is_reorg_acknowledged(),
            "the resume must write the acknowledgement marker",
        );
    }

    /// A reorged log for a fill we never witnessed (e.g. a non-hedgeable pair)
    /// has no aggregate to reverse and is skipped without error.
    #[tokio::test]
    async fn record_reorg_skips_unwitnessed_fill() {
        let pool = setup_test_db().await;

        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let removed = RemovedTrade {
            trade_id: OnChainTradeId {
                tx_hash: TxHash::repeat_byte(0xee),
                log_index: 1,
            },
            block_number: 100,
            reinclusion: None,
        };
        record_reorg(
            &onchain_trade,
            &position,
            &removed,
            5,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();

        assert_eq!(count_events(&pool, "OnChainTradeEvent::Reorged").await, 0);
        assert_eq!(count_events(&pool, "PositionEvent::Reorged").await, 0);
    }

    async fn seed_witnessed_fill(
        onchain_trade: &Store<OnChainTrade>,
        position: &Store<Position>,
        symbol: &Symbol,
        tx_hash: TxHash,
        log_index: u64,
        now: chrono::DateTime<Utc>,
    ) {
        seed_witnessed_fill_with_block_hash(
            onchain_trade,
            position,
            symbol,
            tx_hash,
            log_index,
            None,
            now,
        )
        .await;
    }

    async fn seed_witnessed_fill_with_block_hash(
        onchain_trade: &Store<OnChainTrade>,
        position: &Store<Position>,
        symbol: &Symbol,
        tx_hash: TxHash,
        log_index: u64,
        block_hash: Option<B256>,
        now: chrono::DateTime<Utc>,
    ) {
        onchain_trade
            .send(
                &OnChainTradeId { tx_hash, log_index },
                OnChainTradeCommand::Witness {
                    symbol: symbol.clone(),
                    amount: float!(5),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_number: 100,
                    block_hash,
                    block_timestamp: now,
                },
            )
            .await
            .unwrap();

        position
            .send(
                symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId { tx_hash, log_index },
                    amount: FractionalShares::new(float!(5)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: now,
                },
            )
            .await
            .unwrap();

        // Production fills are witnessed AND acknowledged on the OnChainTrade once
        // the position has accounted them, so seed that complete state -- a reorg
        // strikes an acknowledged fill, not a half-witnessed one.
        onchain_trade
            .send(
                &OnChainTradeId { tx_hash, log_index },
                OnChainTradeCommand::Acknowledge,
            )
            .await
            .unwrap();
    }

    async fn count_events(pool: &SqlitePool, event_type: &str) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM events WHERE event_type = ?")
            .bind(event_type)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// A fork swap during downtime re-serves the same
    /// `(tx_hash, log_index)` on a NEW canonical block. The backfill re-scan
    /// detects it by comparing the freshly-observed `block_hash` against the
    /// witnessed fill's persisted one. Because the fill was re-mined (identical
    /// economic content) rather than dropped, `record_reorg` reverses the
    /// stale-block impact AND re-witnesses + re-applies the fill on the new
    /// block, returning the net to the fill's impact instead of leaving it flat.
    #[tokio::test]
    async fn backfill_block_hash_mismatch_reverses_then_reapplies_reorg() {
        let pool = setup_test_db().await;
        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let tx_hash = TxHash::repeat_byte(0xab);
        let log_index = 7;
        let block_hash_a = B256::repeat_byte(0xaa);
        let block_hash_b = B256::repeat_byte(0xbb);
        let now = Utc::now();

        seed_witnessed_fill_with_block_hash(
            &onchain_trade,
            &position,
            &symbol,
            tx_hash,
            log_index,
            Some(block_hash_a),
            now,
        )
        .await;

        // The re-scan re-served the same key on a different block.
        let present = vec![PresentLog {
            tx_hash,
            log_index,
            block_number: 100,
            block_hash: Some(block_hash_b),
            block_timestamp: Some(now),
        }];
        let reorged = detect_block_hash_reorgs(&onchain_trade, &present)
            .await
            .unwrap();

        assert_eq!(
            reorged.len(),
            1,
            "a block_hash mismatch on a witnessed fill must be flagged as a reorg",
        );
        assert_eq!(reorged[0].trade_id.tx_hash, tx_hash);
        assert_eq!(reorged[0].trade_id.log_index, log_index);

        record_reorg(
            &onchain_trade,
            &position,
            &reorged[0],
            5,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();

        // The full reverse-then-reapply cycle is recorded on the OnChainTrade.
        assert_eq!(count_events(&pool, "OnChainTradeEvent::Reorged").await, 1);
        assert_eq!(
            count_events(&pool, "OnChainTradeEvent::ReorgAcknowledged").await,
            1,
        );
        assert_eq!(
            count_events(&pool, "OnChainTradeEvent::ReWitnessed").await,
            1
        );
        assert_eq!(
            count_events(&pool, "OnChainTradeEvent::Acknowledged").await,
            2,
            "the original acknowledge plus the re-acknowledge of the re-mined fill \
             on its new canonical block",
        );
        assert_eq!(count_events(&pool, "PositionEvent::Reorged").await, 1);

        // The persisted block hash advanced to the new canonical block, so a
        // later benign re-scan no longer flags it, and the trade is live again.
        let trade = onchain_trade
            .load(&OnChainTradeId { tx_hash, log_index })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(trade.block_hash, Some(block_hash_b));
        assert!(trade.is_acknowledged());
        assert!(!trade.is_reorged());

        // The net returns to the fill's impact (+5 from the seeded Buy), NOT
        // flat: the fill was re-mined, not dropped.
        assert_eq!(
            position.load(&symbol).await.unwrap().unwrap().net,
            FractionalShares::new(float!(5)),
            "the re-mined fill's position impact must be restored, not left flat",
        );
    }

    /// The full reverse-then-reapply cycle for a fully-acknowledged fill struck
    /// by a block-hash reorg: the reversal events fire, the fill is re-witnessed
    /// on the new canonical block and acknowledged a SECOND time, and the net
    /// returns to the fill's impact rather than staying flat.
    #[tokio::test]
    async fn record_reorg_reverses_then_reapplies_a_reincluded_fill() {
        let pool = setup_test_db().await;
        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let tx_hash = TxHash::repeat_byte(0xab);
        let log_index = 7;
        let block_hash_a = B256::repeat_byte(0xaa);
        let block_hash_b = B256::repeat_byte(0xbb);
        let now = Utc::now();

        seed_witnessed_fill_with_block_hash(
            &onchain_trade,
            &position,
            &symbol,
            tx_hash,
            log_index,
            Some(block_hash_a),
            now,
        )
        .await;

        let removed = RemovedTrade {
            trade_id: OnChainTradeId { tx_hash, log_index },
            block_number: 100,
            reinclusion: Some(ReInclusion {
                hash: block_hash_b,
                number: 100,
                timestamp: Some(now),
            }),
        };
        record_reorg(
            &onchain_trade,
            &position,
            &removed,
            5,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();

        assert_eq!(count_events(&pool, "OnChainTradeEvent::Reorged").await, 1);
        assert_eq!(
            count_events(&pool, "OnChainTradeEvent::ReorgAcknowledged").await,
            1,
        );
        assert_eq!(
            count_events(&pool, "OnChainTradeEvent::ReWitnessed").await,
            1
        );
        assert_eq!(
            count_events(&pool, "OnChainTradeEvent::Acknowledged").await,
            2,
            "the original acknowledge plus the re-acknowledge of the re-mined fill",
        );
        assert_eq!(count_events(&pool, "PositionEvent::Reorged").await, 1);

        let trade = onchain_trade
            .load(&OnChainTradeId { tx_hash, log_index })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(trade.block_hash, Some(block_hash_b));
        assert!(trade.is_acknowledged());
        assert!(!trade.is_reorged());
        assert!(!trade.is_reorg_acknowledged());

        let position_state = position.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            position_state.net,
            FractionalShares::new(float!(5)),
            "the reverse-then-reapply must restore the fill's impact, not leave it flat",
        );
        // The pending sets are empty at rest: the reversal settled its reorg
        // entry and the re-apply settled its acknowledgement entry.
        assert!(position_state.pending_reorged_trade_ids.is_empty());
        assert!(position_state.pending_acknowledged_trade_ids.is_empty());
    }

    /// A re-delivered backfill range must not double-apply the re-mined fill.
    /// The second `record_reorg` finds the trade already re-witnessed
    /// (`block_hash` advanced) and re-applied (`DuplicateTrade` /
    /// `AlreadyAcknowledged`), so it appends no new events and the net stays at
    /// the fill's impact.
    #[tokio::test]
    async fn record_reorg_reapply_is_idempotent_across_redelivery() {
        let pool = setup_test_db().await;
        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let tx_hash = TxHash::repeat_byte(0xab);
        let log_index = 7;
        let block_hash_a = B256::repeat_byte(0xaa);
        let block_hash_b = B256::repeat_byte(0xbb);
        let now = Utc::now();

        seed_witnessed_fill_with_block_hash(
            &onchain_trade,
            &position,
            &symbol,
            tx_hash,
            log_index,
            Some(block_hash_a),
            now,
        )
        .await;

        let removed = RemovedTrade {
            trade_id: OnChainTradeId { tx_hash, log_index },
            block_number: 100,
            reinclusion: Some(ReInclusion {
                hash: block_hash_b,
                number: 100,
                timestamp: Some(now),
            }),
        };
        record_reorg(
            &onchain_trade,
            &position,
            &removed,
            5,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();
        // Re-deliver the exact same reorg+re-inclusion.
        record_reorg(
            &onchain_trade,
            &position,
            &removed,
            5,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();

        // No step is duplicated across the re-delivery.
        assert_eq!(count_events(&pool, "OnChainTradeEvent::Reorged").await, 1);
        assert_eq!(
            count_events(&pool, "OnChainTradeEvent::ReorgAcknowledged").await,
            1,
        );
        assert_eq!(
            count_events(&pool, "OnChainTradeEvent::ReWitnessed").await,
            1
        );
        assert_eq!(
            count_events(&pool, "OnChainTradeEvent::Acknowledged").await,
            2,
            "the seed acknowledge plus the re-apply's re-acknowledge; the \
             re-delivery must not re-acknowledge again",
        );
        assert_eq!(count_events(&pool, "PositionEvent::Reorged").await, 1);

        let position_state = position.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            position_state.net,
            FractionalShares::new(float!(5)),
            "a re-delivery must not apply the re-mined fill twice",
        );
        assert!(position_state.pending_acknowledged_trade_ids.is_empty());
    }

    /// The critical crash window: a delivery reversed the fill fully (reorg
    /// acknowledged) but died BEFORE re-witnessing it, so the persisted block
    /// hash is still the stale one. A re-delivery must self-heal the reversal and
    /// then re-witness + re-apply, restoring the net to the fill's impact rather
    /// than leaving it flat.
    #[tokio::test]
    async fn record_reorg_resumes_reapply_after_reversal_but_before_rewitness() {
        let pool = setup_test_db().await;
        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let tx_hash = TxHash::repeat_byte(0xab);
        let log_index = 7;
        let block_hash_a = B256::repeat_byte(0xaa);
        let block_hash_b = B256::repeat_byte(0xbb);
        let now = Utc::now();
        let trade_id = OnChainTradeId { tx_hash, log_index };

        seed_witnessed_fill_with_block_hash(
            &onchain_trade,
            &position,
            &symbol,
            tx_hash,
            log_index,
            Some(block_hash_a),
            now,
        )
        .await;

        // Drive the reversal to completion by hand, stopping before the
        // re-witness: this is exactly the durable state a crash there leaves.
        onchain_trade
            .send(
                &trade_id,
                OnChainTradeCommand::RecordReorg { reorg_depth: 5 },
            )
            .await
            .unwrap();
        position
            .send(
                &symbol,
                PositionCommand::RecordReorg {
                    trade_id: TradeId { tx_hash, log_index },
                    amount: FractionalShares::new(float!(5)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    reorg_depth: 5,
                },
            )
            .await
            .unwrap();
        onchain_trade
            .send(&trade_id, OnChainTradeCommand::AcknowledgeReorg)
            .await
            .unwrap();
        position
            .send(
                &symbol,
                PositionCommand::SettleReorg {
                    trade_id: TradeId { tx_hash, log_index },
                },
            )
            .await
            .unwrap();
        // Premise: reversed (flat) but still on the stale block hash.
        let pre_resume = position.load(&symbol).await.unwrap().unwrap();
        assert_eq!(pre_resume.net, FractionalShares::ZERO);
        let pre_trade = onchain_trade.load(&trade_id).await.unwrap().unwrap();
        assert_eq!(pre_trade.block_hash, Some(block_hash_a));
        assert!(pre_trade.is_reorg_acknowledged());

        // Re-deliver the reorg carrying the re-inclusion: the reverse self-heals
        // (no second reverse) and the reapply re-witnesses + re-acknowledges.
        let removed = RemovedTrade {
            trade_id: OnChainTradeId { tx_hash, log_index },
            block_number: 100,
            reinclusion: Some(ReInclusion {
                hash: block_hash_b,
                number: 100,
                timestamp: Some(now),
            }),
        };
        record_reorg(
            &onchain_trade,
            &position,
            &removed,
            5,
            ExecutionThreshold::whole_share(),
        )
        .await
        .unwrap();

        assert_eq!(
            count_events(&pool, "PositionEvent::Reorged").await,
            1,
            "the reversal must not run a second time",
        );
        assert_eq!(
            count_events(&pool, "OnChainTradeEvent::ReWitnessed").await,
            1
        );

        let trade = onchain_trade.load(&trade_id).await.unwrap().unwrap();
        assert_eq!(trade.block_hash, Some(block_hash_b));
        assert!(trade.is_acknowledged());
        assert!(!trade.is_reorged());

        assert_eq!(
            position.load(&symbol).await.unwrap().unwrap().net,
            FractionalShares::new(float!(5)),
            "the resume must restore the fill's impact after a crash before re-witness",
        );
    }

    /// The common case: a benign re-scan re-serves witnessed fills at the SAME
    /// block_hash. That is not a reorg and must not be flagged.
    #[tokio::test]
    async fn backfill_matching_block_hash_is_not_a_reorg() {
        let pool = setup_test_db().await;
        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let tx_hash = TxHash::repeat_byte(0xab);
        let log_index = 7;
        let block_hash = B256::repeat_byte(0xaa);
        let now = Utc::now();

        seed_witnessed_fill_with_block_hash(
            &onchain_trade,
            &position,
            &symbol,
            tx_hash,
            log_index,
            Some(block_hash),
            now,
        )
        .await;

        let present = vec![PresentLog {
            tx_hash,
            log_index,
            block_number: 100,
            block_hash: Some(block_hash),
            block_timestamp: Some(now),
        }];
        let reorged = detect_block_hash_reorgs(&onchain_trade, &present)
            .await
            .unwrap();

        assert!(
            reorged.is_empty(),
            "a matching block_hash on re-scan is a benign re-observation, not a reorg",
        );
    }

    /// The comparison needs both hashes to draw a conclusion. Three cases all skip
    /// -- absence of evidence is not a reorg: an unwitnessed key (nothing
    /// accounted), a witnessed fill with no persisted block_hash (legacy fill / a
    /// log that carried none), and a witnessed fill WITH a persisted hash whose
    /// freshly-observed hash is absent (an `eth_getLogs` response without a block
    /// hash).
    #[tokio::test]
    async fn backfill_block_hash_check_skips_when_nothing_to_compare() {
        let pool = setup_test_db().await;
        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let legacy_tx = TxHash::repeat_byte(0xab);
        let unwitnessed_tx = TxHash::repeat_byte(0xcd);
        let observed_absent_tx = TxHash::repeat_byte(0xef);
        let now = Utc::now();

        // A witnessed fill with NO persisted block_hash (legacy).
        seed_witnessed_fill_with_block_hash(
            &onchain_trade,
            &position,
            &symbol,
            legacy_tx,
            1,
            None,
            now,
        )
        .await;

        // A witnessed fill that DOES carry a persisted block_hash, paired below
        // with a re-scan log whose own block_hash is absent.
        seed_witnessed_fill_with_block_hash(
            &onchain_trade,
            &position,
            &symbol,
            observed_absent_tx,
            3,
            Some(B256::repeat_byte(0xaa)),
            now,
        )
        .await;

        let present = vec![
            // Legacy witnessed fill: persisted hash absent -> cannot compare.
            PresentLog {
                tx_hash: legacy_tx,
                log_index: 1,
                block_number: 100,
                block_hash: Some(B256::repeat_byte(0xbb)),
                block_timestamp: Some(now),
            },
            // Never-witnessed key: nothing accounted -> nothing to reverse.
            PresentLog {
                tx_hash: unwitnessed_tx,
                log_index: 2,
                block_number: 100,
                block_hash: Some(B256::repeat_byte(0xbb)),
                block_timestamp: Some(now),
            },
            // Witnessed fill with a persisted hash, but the re-scan log carried no
            // block_hash (RPC response without one) -> an absent observed hash is
            // not a reorg.
            PresentLog {
                tx_hash: observed_absent_tx,
                log_index: 3,
                block_number: 100,
                block_hash: None,
                block_timestamp: Some(now),
            },
        ];
        let reorged = detect_block_hash_reorgs(&onchain_trade, &present)
            .await
            .unwrap();

        assert!(
            reorged.is_empty(),
            "an absent persisted, absent observed, or unwitnessed key must not be \
             flagged as a reorg",
        );
    }

    /// The reorg anchor (`RemovedTrade.block_number`) is the block the fill was
    /// originally accounted against -- the persisted `OnChainTrade.block_number`
    /// -- not the re-mined block the re-scan observed it on. The re-mined block is
    /// carried separately as the re-inclusion's `number`. This matters because
    /// `BackfillRange::perform` measures reorg depth from the anchor.
    #[tokio::test]
    async fn detect_block_hash_reorg_anchors_on_persisted_block_number() {
        let pool = setup_test_db().await;
        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let tx_hash = TxHash::repeat_byte(0xab);
        let log_index = 7;
        let persisted_block_hash = B256::repeat_byte(0xaa);
        let observed_block_hash = B256::repeat_byte(0xbb);
        let now = Utc::now();

        // The seed witnesses the fill on block 100 (its persisted block number).
        seed_witnessed_fill_with_block_hash(
            &onchain_trade,
            &position,
            &symbol,
            tx_hash,
            log_index,
            Some(persisted_block_hash),
            now,
        )
        .await;

        // The re-scan re-served the same key on a LATER block (150) under a new
        // hash: a fork swap re-mined the fill.
        let present = vec![PresentLog {
            tx_hash,
            log_index,
            block_number: 150,
            block_hash: Some(observed_block_hash),
            block_timestamp: Some(now),
        }];
        let reorged = detect_block_hash_reorgs(&onchain_trade, &present)
            .await
            .unwrap();

        let [removed] = reorged.as_slice() else {
            panic!("a block_hash mismatch must surface exactly one reorg, got {reorged:?}");
        };
        assert_eq!(
            removed.block_number, 100,
            "the reorg must anchor on the persisted block (100), not the observed \
             re-mined block (150)",
        );
        let reinclusion = removed
            .reinclusion
            .as_ref()
            .expect("a re-served log must carry a re-inclusion");
        assert_eq!(
            reinclusion.number, 150,
            "the re-inclusion carries the new canonical block the fill was re-mined on",
        );
        assert_eq!(reinclusion.hash, observed_block_hash);
    }

    /// Pins the `eth_getLogs` wire-shape assumption the block-hash reorg detector
    /// depends on: a Base RPC log element carries the block hash in a camelCase
    /// `blockHash` field. This deserializes a hand-built wire-shape JSON element
    /// (field names written out as literals, not a re-serialized in-code `Log`)
    /// through `enqueue_batch_events` and asserts the resulting
    /// `PresentLog.block_hash` is the exact hash from the wire. A captured real
    /// response would be ideal; a hand-built element matching the documented wire
    /// shape is the next best pin.
    #[tokio::test]
    async fn present_log_carries_block_hash_from_eth_get_logs_wire_shape() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let expected_block_hash = B256::repeat_byte(0xb1);
        let expected_tx_hash = TxHash::repeat_byte(0xbe);

        // A single `eth_getLogs` result element in the exact JSON wire shape a Base
        // RPC node returns: camelCase identity fields and hex-quantity numbers. The
        // field NAMES are hand-written literals; only the hash/address VALUES come
        // from typed primitives so the hex is well-formed. The detector reads
        // `blockHash` from here, so this pins that field's name and type. The
        // topics/data are a minimal valid log -- this test pins block_hash
        // propagation, not event decoding.
        let wire_log = serde_json::json!({
            "address": evm_ctx.orderbook,
            "topics": [B256::repeat_byte(0x5e)],
            "data": "0x",
            "blockHash": expected_block_hash,
            "blockNumber": "0x32",
            "transactionHash": expected_tx_hash,
            "transactionIndex": "0x0",
            "logIndex": "0x1",
            "removed": false,
        });

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([wire_log])); // clear getLogs
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take getLogs
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let outcome = enqueue_batch_events(
            &provider,
            &evm_ctx,
            1,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        let [present] = outcome.present.as_slice() else {
            panic!(
                "the wire-shape log must yield exactly one present log, got {:?}",
                outcome.present
            );
        };
        assert_eq!(
            present.block_hash,
            Some(expected_block_hash),
            "the camelCase `blockHash` wire field must populate PresentLog.block_hash",
        );
        assert_eq!(present.tx_hash, expected_tx_hash);
        assert_eq!(present.log_index, 1);
        assert_eq!(present.block_number, 50);
    }

    /// Tip far above the seed block (100) so the fill is comfortably inside the
    /// unfinalized window in the dropped-fill tests below.
    const UNFINALIZED_TIP: u64 = 1_000;

    /// Confirmations used by the dropped-fill unit tests: zero, matching the e2e
    /// chain config, so the confirmation ceiling is the tip and never gates the
    /// in-window seed block.
    const NO_CONFIRMATIONS: u64 = 0;

    /// Mock provider answering exactly one `eth_getBlockByNumber` with a block at
    /// `number` carrying `hash` -- the canonical block the dropped-fill
    /// re-verification reads to compare against a fill's persisted hash.
    fn provider_with_block(number: u64, hash: B256) -> impl Provider + Clone {
        let mut block = Block::<Transaction>::default();
        block.header.inner.number = number;
        block.header.hash = hash;

        let asserter = Asserter::new();
        asserter.push_success(&block);
        ProviderBuilder::new().connect_mocked_client(asserter)
    }

    /// Mock provider answering one `eth_getBlockByNumber` with a JSON null -- the
    /// node has not surfaced a block at the requested height yet.
    fn provider_without_block() -> impl Provider + Clone {
        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::Null);
        ProviderBuilder::new().connect_mocked_client(asserter)
    }

    /// Mock provider with no queued responses: any RPC call panics the test. Used
    /// where the re-verification must short-circuit BEFORE the canonical block read
    /// (re-mined-same-key, already-acknowledged, finalized window).
    fn provider_never_called() -> impl Provider + Clone {
        ProviderBuilder::new().connect_mocked_client(Asserter::new())
    }

    /// The dropped-fill re-verification reverses (reverse-ONLY) a witnessed, hedged
    /// fill whose block the canonical chain orphaned: the canonical block at the
    /// fill's height now carries a DIFFERENT hash than the one persisted, and the
    /// fill's own `(tx_hash, log_index)` never re-appeared in the scan, so it is
    /// flagged with NO re-inclusion. This is the gap `detect_block_hash_reorgs`
    /// cannot close.
    #[tokio::test]
    async fn dropped_fill_with_orphaned_block_is_flagged_reverse_only() {
        let pool = setup_test_db().await;
        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let tx_hash = TxHash::repeat_byte(0xab);
        let log_index = 7;
        let persisted_block_hash = B256::repeat_byte(0xaa);
        let now = Utc::now();

        seed_witnessed_fill_with_block_hash(
            &onchain_trade,
            &position,
            &symbol,
            tx_hash,
            log_index,
            Some(persisted_block_hash),
            now,
        )
        .await;

        // The orphan-block marker the chaos `ForkDrop` getLogs drop serves: the
        // fill's block height under a competing hash, but a DIFFERENT transaction
        // identity -- the fill's own key never re-appears, so the re-mine detector
        // cannot claim it.
        let present = vec![PresentLog {
            tx_hash: TxHash::repeat_byte(0xdd),
            log_index,
            block_number: 100,
            block_hash: Some(B256::repeat_byte(0xff)),
            block_timestamp: Some(now),
        }];

        // The canonical block at the fill's height now carries a competing hash, so
        // the active re-verification sees hash != persisted and flags the drop.
        let provider = provider_with_block(100, B256::repeat_byte(0xff));

        let reorged = detect_dropped_fill_reorgs(
            &onchain_trade,
            &pool,
            &provider,
            &present,
            UNFINALIZED_TIP,
            NO_CONFIRMATIONS,
        )
        .await
        .unwrap();

        let [removed] = reorged.as_slice() else {
            panic!(
                "a dropped fill with an orphaned block must surface exactly one reorg, got {reorged:?}"
            );
        };
        assert_eq!(removed.trade_id.tx_hash, tx_hash);
        assert_eq!(removed.trade_id.log_index, log_index);
        assert_eq!(removed.block_number, 100);
        assert!(
            removed.reinclusion.is_none(),
            "a dropped fill must be reverse-only -- no re-inclusion to re-apply",
        );
    }

    /// A witnessed fill whose key never re-appears but whose canonical block STILL
    /// carries its persisted hash was not orphaned: the active re-verification reads
    /// the block by number, sees the hash is unchanged, and leaves the fill alone.
    #[tokio::test]
    async fn still_canonical_fill_is_noop() {
        let pool = setup_test_db().await;
        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let tx_hash = TxHash::repeat_byte(0xab);
        let log_index = 7;
        let persisted_block_hash = B256::repeat_byte(0xaa);
        let now = Utc::now();

        seed_witnessed_fill_with_block_hash(
            &onchain_trade,
            &position,
            &symbol,
            tx_hash,
            log_index,
            Some(persisted_block_hash),
            now,
        )
        .await;

        // The fill's key never re-appears, so the pass falls through to the canonical
        // read -- which returns the SAME hash the fill persisted.
        let provider = provider_with_block(100, persisted_block_hash);

        let reorged = detect_dropped_fill_reorgs(
            &onchain_trade,
            &pool,
            &provider,
            &[],
            UNFINALIZED_TIP,
            NO_CONFIRMATIONS,
        )
        .await
        .unwrap();

        assert!(
            reorged.is_empty(),
            "a fill whose canonical block hash is unchanged is still canonical, \
             not dropped, got {reorged:?}",
        );
    }

    /// A fill re-mined under the SAME key on a competing block is the re-mined
    /// case `detect_block_hash_reorgs` owns (reverse-then-reapply). Because its key
    /// is re-observed, the dropped-fill pass must skip it BEFORE the canonical read
    /// -- avoiding a double revert, so the provider must never be consulted.
    #[tokio::test]
    async fn re_mined_same_key_is_left_to_block_hash_detector() {
        let pool = setup_test_db().await;
        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let tx_hash = TxHash::repeat_byte(0xab);
        let log_index = 7;
        let persisted_block_hash = B256::repeat_byte(0xaa);
        let now = Utc::now();

        seed_witnessed_fill_with_block_hash(
            &onchain_trade,
            &position,
            &symbol,
            tx_hash,
            log_index,
            Some(persisted_block_hash),
            now,
        )
        .await;

        // Same key, competing block hash: a re-mine. The block-hash detector flags
        // this; the dropped-fill pass defers via the key-reobserved guard.
        let present = vec![PresentLog {
            tx_hash,
            log_index,
            block_number: 100,
            block_hash: Some(B256::repeat_byte(0xff)),
            block_timestamp: Some(now),
        }];

        // The key-reobserved guard short-circuits before any canonical read.
        let provider = provider_never_called();

        let reorged = detect_dropped_fill_reorgs(
            &onchain_trade,
            &pool,
            &provider,
            &present,
            UNFINALIZED_TIP,
            NO_CONFIRMATIONS,
        )
        .await
        .unwrap();

        assert!(
            reorged.is_empty(),
            "a re-mined same-key fill is `detect_block_hash_reorgs`'s job and must not \
             be double-reverted here, got {reorged:?}",
        );
    }

    /// A fill whose reorg reversal is already acknowledged must never be re-flagged
    /// -- the reverse-only reversal stays exactly-once across poll ticks even while
    /// the orphan marker keeps re-appearing.
    #[tokio::test]
    async fn already_reorg_acknowledged_fill_is_idempotent_noop() {
        let pool = setup_test_db().await;
        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let tx_hash = TxHash::repeat_byte(0xab);
        let log_index = 7;
        let persisted_block_hash = B256::repeat_byte(0xaa);
        let now = Utc::now();

        seed_witnessed_fill_with_block_hash(
            &onchain_trade,
            &position,
            &symbol,
            tx_hash,
            log_index,
            Some(persisted_block_hash),
            now,
        )
        .await;

        let trade_id = OnChainTradeId { tx_hash, log_index };
        onchain_trade
            .send(
                &trade_id,
                OnChainTradeCommand::RecordReorg { reorg_depth: 5 },
            )
            .await
            .unwrap();
        onchain_trade
            .send(&trade_id, OnChainTradeCommand::AcknowledgeReorg)
            .await
            .unwrap();

        // The orphan marker is still being served, but the reversal is already
        // complete, so the pass must short-circuit on the acknowledged guard before
        // ever reading the canonical block.
        let present = vec![PresentLog {
            tx_hash: TxHash::repeat_byte(0xdd),
            log_index,
            block_number: 100,
            block_hash: Some(B256::repeat_byte(0xff)),
            block_timestamp: Some(now),
        }];

        // The acknowledged guard short-circuits before any canonical read.
        let provider = provider_never_called();

        let reorged = detect_dropped_fill_reorgs(
            &onchain_trade,
            &pool,
            &provider,
            &present,
            UNFINALIZED_TIP,
            NO_CONFIRMATIONS,
        )
        .await
        .unwrap();

        assert!(
            reorged.is_empty(),
            "an already reorg-acknowledged fill must not be re-flagged, got {reorged:?}",
        );
    }

    /// When the node has not surfaced a block at the fill's height (a lagging or
    /// cold-start node returning a null `eth_getBlockByNumber`), the orphaning
    /// cannot be proven, so an already-hedged fill must NOT be reversed -- the pass
    /// defers to a later tick against a caught-up node.
    #[tokio::test]
    async fn canonical_block_absent_is_deferred_not_reversed() {
        let pool = setup_test_db().await;
        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let tx_hash = TxHash::repeat_byte(0xab);
        let log_index = 7;
        let now = Utc::now();

        seed_witnessed_fill_with_block_hash(
            &onchain_trade,
            &position,
            &symbol,
            tx_hash,
            log_index,
            Some(B256::repeat_byte(0xaa)),
            now,
        )
        .await;

        // The fill's key is absent (no re-mine), but the node returns no block at the
        // fill's height -- the orphaning is unproven, so the fill must be left alone.
        let provider = provider_without_block();

        let reorged = detect_dropped_fill_reorgs(
            &onchain_trade,
            &pool,
            &provider,
            &[],
            UNFINALIZED_TIP,
            NO_CONFIRMATIONS,
        )
        .await
        .unwrap();

        assert!(
            reorged.is_empty(),
            "an unproven orphaning (no canonical block) must never reverse a fill, got {reorged:?}",
        );
    }

    /// Fills whose block has finalized (deeper than [`REORG_REVERIFICATION_WINDOW`]
    /// below the tip) are dropped from consideration, keeping the pass bounded to
    /// the unfinalized window -- even when an orphan marker is still present.
    #[tokio::test]
    async fn finalized_fill_is_outside_the_reverification_window() {
        let pool = setup_test_db().await;
        let onchain_trade = StoreBuilder::<OnChainTrade>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let (position, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        let tx_hash = TxHash::repeat_byte(0xab);
        let log_index = 7;
        let now = Utc::now();

        // The seed witnesses on block 100.
        seed_witnessed_fill_with_block_hash(
            &onchain_trade,
            &position,
            &symbol,
            tx_hash,
            log_index,
            Some(B256::repeat_byte(0xaa)),
            now,
        )
        .await;

        let present = vec![PresentLog {
            tx_hash: TxHash::repeat_byte(0xdd),
            log_index,
            block_number: 100,
            block_hash: Some(B256::repeat_byte(0xff)),
            block_timestamp: Some(now),
        }];

        // The finality-window floor short-circuits before any canonical read.
        let provider = provider_never_called();

        // Tip far enough ahead that block 100 is below the finality boundary
        // (100 <= tip - REORG_REVERIFICATION_WINDOW), so the fill is final.
        let tip_past_finality = 100 + REORG_REVERIFICATION_WINDOW + 1;
        let reorged = detect_dropped_fill_reorgs(
            &onchain_trade,
            &pool,
            &provider,
            &present,
            tip_past_finality,
            NO_CONFIRMATIONS,
        )
        .await
        .unwrap();

        assert!(
            reorged.is_empty(),
            "a finalized fill is outside the unfinalized window and must not be re-verified, \
             got {reorged:?}",
        );
    }
}
