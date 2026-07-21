//! Historical onchain event backfill with retry logic.
//!
//! Scans past blocks for `ClearV3` and `TakeOrderV3` events on the OrderBook,
//! plus `OperatorDeposit`/`OperatorWithdraw` events on the shared
//! `RaindexInventory` (paired into `InventoryTrade`s), and pushes them into the
//! apalis job queue for processing, ensuring no trades are missed after
//! downtime. A persisted checkpoint records the last block that was fully
//! enqueued.

use alloy::primitives::TxHash;
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use backon::{BackoffBuilder, ExponentialBuilder, Retryable};
use futures_util::future;
use itertools::Itertools;
use metrics::counter;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::time::Duration;
use tracing::{debug, error, info, trace, warn};

use st0x_config::EvmCtx;
use st0x_event_sorcery::{AggregateError, LifecycleError, Store};
use st0x_evm::Evm;
use st0x_execution::{Executor, FractionalShares};
use st0x_registry::get_symbol_lock;

use super::OnChainError;
use crate::bindings::IRaindexInventory::{OperatorDeposit, OperatorWithdraw};
use crate::bindings::IRaindexV6::{ClearV3, TakeOrderV3};
use crate::conductor::job::{Job, Label};
use crate::onchain::trade::{BotOperator, InventoryTrade, RaindexTradeEvent};
use crate::onchain_trade::{OnChainTrade, OnChainTradeCommand, OnChainTradeError, OnChainTradeId};
use crate::position::{Position, PositionCommand, TradeId};
use crate::trading::onchain::inclusion::EmittedOnChain;
use crate::trading::onchain::trade_accountant::{
    AccountForDexTrade, AccountantCtx, DexTradeAccountingJobQueue, TradeAccountingError,
};

/// A confirmed fill whose block a reorg dropped past the confirmation depth,
/// identified for reversal. The `(tx_hash, log_index)` identity is the
/// [`OnChainTradeId`] of the fill to reverse; `block_number` lets the caller
/// derive how deep the reorg ran (current tip minus this block).
#[derive(Debug, Clone)]
pub(crate) struct RemovedTrade {
    pub(crate) trade_id: OnChainTradeId,
    pub(crate) block_number: u64,
}

/// What an `enqueue_batch_events` pass found: how many fresh fills it enqueued
/// and which already-accounted fills its logs reported as reorged away.
#[derive(Debug)]
struct BatchOutcome {
    enqueued: usize,
    removed: Vec<RemovedTrade>,
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
    bot_operator: BotOperator,
    pool: &SqlitePool,
    end_block: u64,
    retry_strategy: B,
    job_queue: DexTradeAccountingJobQueue,
) -> Result<Vec<RemovedTrade>, OnChainError> {
    let start_block = backfill_start_block(pool, evm_ctx).await?;

    backfill_range(
        provider,
        evm_ctx,
        bot_operator,
        pool,
        start_block,
        end_block,
        retry_strategy,
        job_queue,
    )
    .await
}

/// Fetches `ClearV3` / `TakeOrderV3` logs from the OrderBook and
/// `OperatorDeposit` / `OperatorWithdraw` logs from the shared
/// `RaindexInventory` in `[from_block, to_block]`, pushes an
/// `AccountForDexTrade` job for each (inventory legs first paired into a single
/// `InventoryTrade` per settlement tx), and advances the backfill checkpoint to
/// `to_block` on success.
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
    bot_operator: BotOperator,
    pool: &SqlitePool,
    from_block: u64,
    to_block: u64,
    retry_strategy: B,
    job_queue: DexTradeAccountingJobQueue,
) -> Result<Vec<RemovedTrade>, OnChainError> {
    if from_block > to_block {
        info!(
            target: "orderbook",
            "Already caught up to block {}, skipping backfill",
            to_block
        );

        save_backfill_checkpoint(pool, evm_ctx, to_block).await?;
        return Ok(Vec::new());
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
    for (batch_start, batch_end) in batch_ranges {
        let outcome = enqueue_batch_events(
            provider,
            evm_ctx,
            bot_operator,
            batch_start,
            batch_end,
            retry_strategy.clone(),
            job_queue.clone(),
        )
        .await?;
        total_enqueued += outcome.enqueued;
        removed_trades.extend(outcome.removed);
    }

    info!(
        target: "orderbook",
        total_enqueued,
        total_removed = removed_trades.len(),
        "Backfill completed"
    );

    save_backfill_checkpoint(pool, evm_ctx, to_block).await?;

    Ok(removed_trades)
}

/// Persistent job queue for backfill jobs.
pub(crate) type BackfillJobQueue = crate::conductor::job::JobQueue<BackfillRange>;

/// Apalis job that backfills missed `ClearV3` / `TakeOrderV3` orderbook fills
/// and `OperatorDeposit` / `OperatorWithdraw` inventory settlements between
/// `from_block` and `to_block` (inclusive).
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
        let removed = backfill_range(
            ctx.evm.provider(),
            &ctx.ctx.evm,
            BotOperator(ctx.ctx.order_owner()),
            &ctx.pool,
            self.from_block,
            self.to_block,
            get_backfill_retry_strat(),
            ctx.job_queue.clone(),
        )
        .await?;

        if removed.is_empty() {
            return Ok(());
        }

        // Depth is measured from the current tip: the dropped block is gone, so
        // the reorg spans at least `tip - block_number` blocks. Fetched once and
        // reused for every reversal in this batch.
        let tip = ctx.evm.provider().get_block_number().await?;

        for removed_trade in removed {
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
///
/// RUNBOOK: the persisted checkpoint predates inventory (`OperatorDeposit` /
/// `OperatorWithdraw`) ingestion, so resuming from it silently skips any
/// adapter/inventory fills mined before this code's deploy block. On rollout,
/// rewind the checkpoint to the inventory deploy block (or run a one-shot
/// backfill over that range) so pre-deploy inventory fills are ingested.
pub(crate) fn backfill_start_from_checkpoint(
    checkpoint: Option<u64>,
    deployment_block: u64,
) -> u64 {
    checkpoint.map_or(deployment_block, |last_processed_block| {
        (last_processed_block + 1).max(deployment_block)
    })
}

/// Reverses an already-accounted fill that a reorg dropped at or below the
/// ingestion cutoff: appends `RecordReorg` to the `OnChainTrade` and `Position`
/// aggregates through the CQRS framework (never direct SQL). A fill we never
/// witnessed (e.g. a non-hedgeable pair) has nothing to reverse and is skipped.
///
/// The two sends are NOT atomic, so this path is only partially crash-safe. A
/// crash after the `OnChainTrade` reversal but before the `Position` reversal
/// leaves the position impact unreversed; on retry the `OnChainTrade`
/// `AlreadyReorged` guard short-circuits before the `Position` repair can run,
/// so that window stays open. Closing it is the job of the exactly-once reorg
/// accounting built later in this stack, which records reversal progress per
/// trade. A retry that did not crash mid-reversal is safe: if neither send
/// landed it re-reverses cleanly, and if both landed `AlreadyReorged` makes the
/// re-run a no-op.
///
/// The `Position` reversal runs under the same per-symbol lock that
/// `AccountForDexTrade` holds while it mutates position state, so a concurrent
/// live fill for the symbol cannot interleave with the reversal and corrupt the
/// net.
async fn record_reorg(
    onchain_trade: &Store<OnChainTrade>,
    position: &Store<Position>,
    removed: &RemovedTrade,
    reorg_depth: u64,
) -> Result<(), OnChainError> {
    let RemovedTrade { trade_id, .. } = removed;

    let Some(trade) = onchain_trade.load(trade_id).await? else {
        warn!(
            target: "orderbook",
            tx_hash = ?trade_id.tx_hash,
            log_index = trade_id.log_index,
            "Reorged log has no witnessed OnChainTrade; nothing to reverse"
        );
        return Ok(());
    };

    // Serialize against `AccountForDexTrade`, which holds this same per-symbol
    // lock while it mutates position state; without it the `Position` reversal
    // below could interleave with a concurrent live fill and corrupt the net.
    let symbol_lock = get_symbol_lock(&trade.symbol).await;
    let _guard = symbol_lock.lock().await;

    match onchain_trade
        .send(trade_id, OnChainTradeCommand::RecordReorg { reorg_depth })
        .await
    {
        Ok(()) => {}
        // Already reversed by an earlier delivery. Whether the `Position`
        // reversal followed it is not knowable here (see the crash window in
        // this fn's docs), so stop rather than reverse again; repairing a
        // half-applied reversal is the exactly-once accounting's job.
        Err(AggregateError::UserError(LifecycleError::Apply(
            OnChainTradeError::AlreadyReorged,
        ))) => return Ok(()),
        Err(error) => return Err(error.into()),
    }

    position
        .send(
            &trade.symbol,
            PositionCommand::RecordReorg {
                trade_id: TradeId {
                    tx_hash: trade_id.tx_hash,
                    log_index: trade_id.log_index,
                },
                amount: FractionalShares::new(trade.amount),
                direction: trade.direction,
                reorg_depth,
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

/// A settlement tx's `OperatorDeposit` bucket: either exactly one deposit, or
/// `Ambiguous` because a second same-tx deposit landed. `deposit4`/`withdraw4`
/// are independent `RaindexInventory` calls with no atomic "settle" entrypoint
/// pairing them, so once a tx has more than one deposit there is no reliable
/// way to know which one a same-tx withdraw actually belongs to. Rather than
/// silently overwriting (and mis-hedging) a real amount, the whole tx is
/// quarantined: no `InventoryTrade` is ever built from an `Ambiguous` bucket.
pub(crate) enum DepositBucket {
    Single(OperatorDeposit),
    Ambiguous,
}

/// A settlement tx's `OperatorWithdraw` bucket, symmetric to [`DepositBucket`].
/// A tx with two `OperatorWithdraw`s is exactly as ambiguous as one with two
/// `OperatorDeposit`s -- the single deposit could belong to either withdraw --
/// so it is quarantined the same way. Carries the triggering `Log` alongside
/// `Single` so the pairing pass can build `EmittedOnChain` metadata without a
/// second lookup.
pub(crate) enum WithdrawBucket {
    Single(OperatorWithdraw, Box<Log>),
    Ambiguous,
}

/// Splits a batch's raw inventory logs into per-tx `OperatorDeposit` and
/// `OperatorWithdraw` buckets keyed by settlement tx, so the pairing pass
/// ([`pair_inventory_settlements`]) can match each tx's deposit to its
/// withdraw without an extra RPC round-trip. Drops the bot's own rebalancing
/// legs (treasury moves, not venue fills), `removed: true` logs (reorg), and
/// undecodable logs; quarantines any tx that emits multiple deposits or
/// multiple withdraws.
///
/// `pub(crate)`: also reused by `OnchainTrade::try_from_tx_hash` (the manual
/// `process-tx` recovery path) so both ingestion paths pair
/// `OperatorDeposit`/`OperatorWithdraw` under the exact same rules.
pub(crate) fn bucket_inventory_logs(
    inv_logs: Vec<Log>,
    bot_operator: BotOperator,
) -> (
    HashMap<TxHash, DepositBucket>,
    HashMap<TxHash, WithdrawBucket>,
) {
    let BotOperator(bot_operator) = bot_operator;
    let mut deposits_by_tx: HashMap<TxHash, DepositBucket> = HashMap::new();
    let mut withdraws_by_tx: HashMap<TxHash, WithdrawBucket> = HashMap::new();
    for log in inv_logs {
        let Some(topic0) = log.topic0().copied() else {
            continue;
        };
        if topic0 == OperatorDeposit::SIGNATURE_HASH {
            // Callers cap `to_block` at the ingestion cutoff block, so a
            // `removed: true` OperatorDeposit here implies a reorg -- mirror
            // the same guard applied to ClearV3/TakeOrderV3/OperatorWithdraw
            // logs below rather than bucketing a vanished event.
            if log.removed {
                warn!(
                    target: "inventory",
                    tx_hash = ?log.transaction_hash,
                    log_index = ?log.log_index,
                    block_number = ?log.block_number,
                    "Backfill returned `removed: true` for an OperatorDeposit log; \
                     reorg detected, skipping",
                );
                continue;
            }

            let (Ok(decoded), Some(tx_hash)) =
                (log.log_decode::<OperatorDeposit>(), log.transaction_hash)
            else {
                warn!(
                    target: "inventory",
                    "Skipping OperatorDeposit that failed to decode / had no tx hash",
                );
                continue;
            };
            let deposit = decoded.data().clone();

            // The bot's own rebalancing also calls deposit4/withdraw4 on the
            // inventory, emitting OperatorDeposit/OperatorWithdraw whose
            // operator is the bot's signing wallet. Those are treasury moves, not
            // venue fills to hedge -- drop them before bucketing/pairing.
            if deposit.operator == bot_operator {
                debug!(
                    target: "inventory",
                    ?tx_hash,
                    "Skipping inventory event emitted by the bot's own rebalancing",
                );
                continue;
            }

            match deposits_by_tx.entry(tx_hash) {
                Entry::Vacant(entry) => {
                    entry.insert(DepositBucket::Single(deposit));
                }
                Entry::Occupied(mut entry) => {
                    // deposit4/withdraw4 are independent contract calls with no
                    // atomic settle entrypoint pairing them, so a second same-tx
                    // deposit means this settlement cannot be safely paired 1:1.
                    // Quarantine the whole tx (fail closed) instead of silently
                    // overwriting a real amount and mis-hedging.
                    error!(
                        target: "inventory",
                        ?tx_hash,
                        "Multiple OperatorDeposits in one tx; settlement is ambiguous \
                         and cannot be safely paired, quarantining the whole tx",
                    );
                    counter!("inventory_ambiguous_settlement_total").increment(1);
                    entry.insert(DepositBucket::Ambiguous);
                }
            }
        } else if topic0 == OperatorWithdraw::SIGNATURE_HASH {
            // Mirror the deposit branch's reorg guard: callers cap `to_block`
            // at the ingestion cutoff block, so `removed: true` here implies a
            // reorg rather than a routine result.
            if log.removed {
                warn!(
                    target: "inventory",
                    tx_hash = ?log.transaction_hash,
                    log_index = ?log.log_index,
                    block_number = ?log.block_number,
                    "Backfill returned `removed: true` for an OperatorWithdraw log; \
                     reorg detected, skipping",
                );
                continue;
            }

            let (Ok(decoded), Some(tx_hash)) =
                (log.log_decode::<OperatorWithdraw>(), log.transaction_hash)
            else {
                warn!(
                    target: "inventory",
                    "Skipping OperatorWithdraw that failed to decode / had no tx hash",
                );
                continue;
            };
            let withdraw = decoded.data().clone();

            // Skip the bot's own rebalancing withdraws (see the deposit branch);
            // a lone rebalance withdraw would otherwise fire the "no paired
            // deposit" warn on every routine rebalance.
            if withdraw.operator == bot_operator {
                debug!(
                    target: "inventory",
                    ?tx_hash,
                    "Skipping inventory event emitted by the bot's own rebalancing",
                );
                continue;
            }

            match withdraws_by_tx.entry(tx_hash) {
                Entry::Vacant(entry) => {
                    entry.insert(WithdrawBucket::Single(withdraw, Box::new(log)));
                }
                Entry::Occupied(mut entry) => {
                    // Symmetric to the deposit branch: a second same-tx
                    // withdraw is exactly as ambiguous as a second deposit --
                    // the single deposit could fund either withdraw -- so
                    // quarantine the whole tx (fail closed) rather than
                    // arbitrarily pairing the first one encountered.
                    error!(
                        target: "inventory",
                        ?tx_hash,
                        "Multiple OperatorWithdraws in one tx; settlement is ambiguous \
                         and cannot be safely paired, quarantining the whole tx",
                    );
                    counter!("inventory_ambiguous_settlement_total").increment(1);
                    entry.insert(WithdrawBucket::Ambiguous);
                }
            }
        }
    }

    (deposits_by_tx, withdraws_by_tx)
}

/// Pairs bucketed deposits and withdraws into `InventoryTrade` events, one
/// per tx that has EXACTLY one deposit and exactly one withdraw. Any other
/// combination (an ambiguous bucket on either side, or a leg with no
/// counterpart at all) is quarantined -- fail closed, zero `InventoryTrade`s
/// built for that tx -- since `deposit4`/`withdraw4` are independent calls
/// with no atomic settlement id linking them.
///
/// `pub(crate)`: shared by both the batch backfill path
/// ([`enqueue_batch_events`]) and `OnchainTrade::try_from_tx_hash` (the
/// manual `process-tx` recovery path) so a multi-leg settlement is
/// quarantined identically regardless of which path discovers it.
pub(crate) fn pair_inventory_settlements(
    deposits_by_tx: HashMap<TxHash, DepositBucket>,
    withdraws_by_tx: HashMap<TxHash, WithdrawBucket>,
) -> Vec<(RaindexTradeEvent, Log)> {
    let mut buckets_by_tx: HashMap<TxHash, (Option<DepositBucket>, Option<WithdrawBucket>)> =
        HashMap::new();
    for (tx_hash, deposit_bucket) in deposits_by_tx {
        buckets_by_tx.entry(tx_hash).or_default().0 = Some(deposit_bucket);
    }
    for (tx_hash, withdraw_bucket) in withdraws_by_tx {
        buckets_by_tx.entry(tx_hash).or_default().1 = Some(withdraw_bucket);
    }

    // A plain loop rather than a `filter_map`: every non-pairing arm below has
    // to log and increment a counter, and burying those side effects inside a
    // transform hides the quarantine logic that is the whole point of this
    // function.
    let mut paired = Vec::new();
    for (tx_hash, (deposit_bucket, withdraw_bucket)) in buckets_by_tx {
        match (deposit_bucket, withdraw_bucket) {
            (Some(DepositBucket::Single(deposit)), Some(WithdrawBucket::Single(withdraw, log)))
                if deposit.operator == withdraw.operator =>
            {
                let inv = InventoryTrade { deposit, withdraw };
                paired.push((RaindexTradeEvent::InventoryTrade(Box::new(inv)), *log));
            }
            (Some(DepositBucket::Single(deposit)), Some(WithdrawBucket::Single(withdraw, _))) => {
                // The single deposit and single withdraw in this tx were emitted
                // by different OPERATOR_ROLE holders, so they are not the two
                // legs of the same settlement -- pairing them would fabricate a
                // vault delta between two unrelated actions and produce a wrong
                // hedge. Quarantine, same fail-closed treatment as a multi-leg
                // ambiguous tx.
                error!(
                    target: "inventory",
                    ?tx_hash,
                    deposit_operator = ?deposit.operator,
                    withdraw_operator = ?withdraw.operator,
                    "Settlement quarantined: OperatorDeposit and OperatorWithdraw in this \
                     tx were emitted by different operators and cannot be safely paired; \
                     no hedge emitted",
                );
                counter!("inventory_ambiguous_settlement_total").increment(1);
            }
            (Some(DepositBucket::Ambiguous), Some(WithdrawBucket::Ambiguous)) => {
                error!(
                    target: "inventory",
                    ?tx_hash,
                    "Settlement quarantined: both OperatorDeposit and OperatorWithdraw \
                     are ambiguous (multiple of each) in this tx; no hedge emitted",
                );
            }
            (Some(DepositBucket::Ambiguous), Some(WithdrawBucket::Single(..))) => {
                error!(
                    target: "inventory",
                    ?tx_hash,
                    "Settlement quarantined: ambiguous multi-deposit tx; no hedge emitted",
                );
            }
            (Some(DepositBucket::Ambiguous), None) => {
                error!(
                    target: "inventory",
                    ?tx_hash,
                    "Settlement quarantined: ambiguous multi-deposit tx never claimed by \
                     an OperatorWithdraw; no hedge emitted",
                );
            }
            (Some(DepositBucket::Single(_)), Some(WithdrawBucket::Ambiguous)) => {
                error!(
                    target: "inventory",
                    ?tx_hash,
                    "Settlement quarantined: ambiguous multi-withdraw tx; no hedge emitted",
                );
            }
            (None, Some(WithdrawBucket::Ambiguous)) => {
                error!(
                    target: "inventory",
                    ?tx_hash,
                    "Settlement quarantined: ambiguous multi-withdraw tx with no \
                     OperatorDeposit; no hedge emitted",
                );
            }
            (Some(DepositBucket::Single(deposit)), None) => {
                warn!(
                    target: "inventory",
                    ?tx_hash,
                    vault_id = ?deposit.vaultId,
                    token = ?deposit.token,
                    "OperatorDeposit without a paired OperatorWithdraw in the same batch; \
                     skipping",
                );
                counter!("inventory_unpaired_settlement_total", "leg" => "deposit").increment(1);
            }
            (None, Some(WithdrawBucket::Single(withdraw, _))) => {
                warn!(
                    target: "inventory",
                    ?tx_hash,
                    vault_id = ?withdraw.vaultId,
                    token = ?withdraw.token,
                    "OperatorWithdraw without a paired OperatorDeposit in the same batch; \
                     skipping",
                );
                counter!("inventory_unpaired_settlement_total", "leg" => "withdraw").increment(1);
            }
            (None, None) => {}
        }
    }

    paired
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
    bot_operator: BotOperator,
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

    // Inventory events: OperatorDeposit + OperatorWithdraw are emitted as a
    // pair per settlement tx that routes through the shared inventory (Bebop
    // adapter, univ4 hook, or any future venue). The pair -- one deposit +
    // one withdraw, one side equity + one side USDC -- is the trade to hedge.
    let inventory_filter = Filter::new()
        .address(evm_ctx.inventory_address())
        .from_block(batch_start)
        .to_block(batch_end)
        .event_signature(vec![
            OperatorDeposit::SIGNATURE_HASH,
            OperatorWithdraw::SIGNATURE_HASH,
        ]);

    let provider_clear = provider.clone();
    let provider_take = provider.clone();
    let provider_inv = provider.clone();
    let clear_filter_clone = clear_filter.clone();
    let take_filter_clone = take_filter.clone();
    let inv_filter_clone = inventory_filter.clone();

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
    let get_inv_logs = move || {
        let provider = provider_inv.clone();
        let filter = inv_filter_clone.clone();
        async move { fetch_logs_with_tip_check(&provider, &filter, batch_end).await }
    };

    let (clear_logs, take_logs, inv_logs) = future::try_join3(
        get_clear_logs
            .retry(retry_strategy.clone().build())
            .notify(|err, dur| {
                trace!(target: "orderbook", "Retrying clear_logs for blocks between {batch_start}-{batch_end} after error: {err} (waiting {dur:?})");
            }),
        get_take_logs
            .retry(retry_strategy.clone().build())
            .notify(|err, dur| {
                trace!(target: "orderbook", "Retrying take_logs for blocks between {batch_start}-{batch_end} after error: {err} (waiting {dur:?})");
            }),
        get_inv_logs
            .retry(retry_strategy.build())
            .notify(|err, dur| {
                trace!(target: "inventory", "Retrying inv_logs for blocks between {batch_start}-{batch_end} after error: {err} (waiting {dur:?})");
            }),
    )
    .await?;

    debug!(
        target: "orderbook",
        total_clear_logs = %clear_logs.len(),
        total_take_logs = %take_logs.len(),
        total_inventory_logs = %inv_logs.len(),
        "Processed a batch of blocks from {batch_start} to {batch_end}",
    );

    // Bucket inventory logs by tx so deposits and withdraws can be paired
    // (or quarantined) per settlement without an extra RPC round-trip.
    let (deposits_by_tx, withdraws_by_tx) = bucket_inventory_logs(inv_logs, bot_operator);
    let inventory_trade_events = pair_inventory_settlements(deposits_by_tx, withdraws_by_tx);

    let mut removed_trades = Vec::new();
    let mut present_logs = Vec::new();
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
        // NOTE: this `removed: true` path is inert in production today. Both the
        // monitor and backfill read fills via `eth_getLogs` range queries, which
        // only ever return the current canonical chain and never flag a log
        // `removed: true` -- only subscription / `eth_getFilterChanges`
        // notifications do that, and neither path uses them. The authoritative
        // reorg detector is a separate block-hash-mismatch re-scan -- comparing
        // the persisted `block_hash` for a known `(tx_hash, log_index)` against a
        // freshly observed one -- which is not yet built. This branch is kept as
        // correct scaffolding for that detector; tests exercise it by
        // synthesizing `removed: true` logs.
        if !log.removed {
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
        .chain(inventory_trade_events)
        .sorted_by_key(|(_, log)| (log.block_number, log.log_index))
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
        // Count each decoded fill enqueued into the accounting pipeline, keyed by
        // event type, so ingestion volume is observable even before any hedge is
        // placed downstream. This counts enqueue attempts, not unique fills: a
        // backfill replay re-enqueues already-processed events (the pipeline
        // dedupes on (tx_hash, log_index)). `kind()` yields
        // "ClearV3"/"TakeOrderV3"/"InventoryTrade".
        counter!("onchain_events_total", "event_type" => trade_event.event.kind()).increment(1);
        job_queue
            .push(AccountForDexTrade { trade: trade_event })
            .await?;
    }

    Ok(BatchOutcome {
        enqueued: enqueued_count,
        removed: removed_trades,
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
    use tokio::sync::Mutex;

    use alloy::primitives::{
        Address, B256, Bytes, FixedBytes, IntoLogData, LogData, TxHash, U256, address, fixed_bytes,
        uint,
    };
    use alloy::providers::{ProviderBuilder, mock::Asserter};
    use alloy::rpc::types::Log;
    use chrono::Utc;
    use rain_math_float::Float;
    use st0x_config::{
        EvmCtx, ExecutionThreshold, IngestionCutoff, InventoryMode,
        create_test_ctx_with_order_owner,
    };
    use url::Url;

    use st0x_event_sorcery::StoreBuilder;
    use st0x_evm::ReadOnlyEvm;
    use st0x_execution::{Direction, MockExecutorCtx, Symbol, TryIntoExecutor};
    use st0x_float_macro::float;
    use st0x_raindex::RaindexContracts;
    use st0x_registry::SymbolCache;

    use super::*;
    use crate::bindings::IRaindexV6;
    use crate::conductor::TradeProcessingCqrs;
    use crate::offchain::order::{OffchainOrder, PollOrderStatusJobQueue, noop_order_placer};
    use crate::onchain::pyth::PythFeedIds;
    use crate::test_utils::{get_test_order, setup_test_db, setup_test_pools};
    use crate::trading::offchain::hedge::HedgeJobQueue;
    use crate::vault_registry::VaultRegistry;

    /// A bot-operator address distinct from every event operator seeded in
    /// these tests, so the T13 "skip the bot's own rebalancing events" filter
    /// never matches unless a test deliberately uses this address.
    const TEST_BOT_OPERATOR: Address = address!("0x00000000000000000000000000000000000000b0");

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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        backfill_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 50,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);

        // Batch 2: blocks 2000-2500
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);

        // Batch 2: blocks 1500-1900
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let enqueued_count = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
            asserter.push_success(&serde_json::json!([])); // inventory events
            push_tip_response(&asserter);
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
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
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 42,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([]));
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        // Close the apalis pool to simulate job-queue connection failure.
        apalis_pool.close().await;

        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
            asserter.push_success(&serde_json::json!([])); // inventory events
            push_tip_response(&asserter);
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let start_time = std::time::Instant::now();
        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
            BotOperator(TEST_BOT_OPERATOR),
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
        // Install the process-global Prometheus recorder so the per-event
        // onchain_events_total increments are observable. nextest isolates each
        // test in its own process, so only this test's two events are counted.
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
            100,
            200,
            test_retry_strategy(),
            job_queue,
        )
        .await;

        let enqueued = result.unwrap().enqueued;
        assert_eq!(enqueued, 2);
        assert_eq!(job_count(&apalis_pool).await, 2);

        let rendered = metrics_handle.render();
        assert!(
            rendered.contains("onchain_events_total{event_type=\"ClearV3\"} 1"),
            "ClearV3 ingestion must increment onchain_events_total, got:\n{rendered}"
        );
        assert!(
            rendered.contains("onchain_events_total{event_type=\"TakeOrderV3\"} 1"),
            "TakeOrderV3 ingestion must increment onchain_events_total, got:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn test_backfill_starts_from_deployment_block() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        backfill_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
            &pool,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(job_count(&apalis_pool).await, 0);
    }

    fn operator_deposit_log(
        inventory: Address,
        operator: Address,
        token: Address,
        amount: U256,
        block_number: u64,
        tx_hash: TxHash,
        log_index: u64,
    ) -> Log {
        let event = OperatorDeposit {
            operator,
            token,
            vaultId: B256::ZERO,
            amount,
        };

        Log {
            inner: alloy::primitives::Log {
                address: inventory,
                data: event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(block_number),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(log_index),
            removed: false,
        }
    }

    fn operator_withdraw_log(
        inventory: Address,
        operator: Address,
        token: Address,
        amount: U256,
        block_number: u64,
        tx_hash: TxHash,
        log_index: u64,
    ) -> Log {
        let event = OperatorWithdraw {
            operator,
            token,
            vaultId: B256::ZERO,
            amount,
        };

        Log {
            inner: alloy::primitives::Log {
                address: inventory,
                data: event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(block_number),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(log_index),
            removed: false,
        }
    }

    fn inventory_test_evm_ctx() -> EvmCtx {
        EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            inventory: InventoryMode::Managed {
                inventory: address!("0x2222222222222222222222222222222222222222"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        }
    }

    /// A venue-driven settlement (operator != the bot) surfaces as one
    /// `OperatorDeposit` + `OperatorWithdraw` pair on the same tx, which must
    /// collapse into exactly one `InventoryTrade` job.
    #[tokio::test]
    async fn inventory_pair_enqueues_one_job() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = inventory_test_evm_ctx();
        let inventory = evm_ctx.inventory_address();

        let venue_operator = address!("0x00000000000000000000000000000000000000a1");
        let usdc = address!("0x00000000000000000000000000000000000000dc");
        let equity = address!("0x00000000000000000000000000000000000000e9");
        let tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let deposit_log = operator_deposit_log(
            inventory,
            venue_operator,
            usdc,
            uint!(1_000_000_U256),
            50,
            tx_hash,
            0,
        );
        let withdraw_log = operator_withdraw_log(
            inventory,
            venue_operator,
            equity,
            uint!(9_000_000_000_000_000_000_U256),
            50,
            tx_hash,
            1,
        );

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([deposit_log, withdraw_log])); // inventory
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let BatchOutcome { enqueued, .. } = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
            1,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(
            enqueued, 1,
            "one deposit+withdraw pair is one InventoryTrade"
        );
        assert_eq!(job_count(&apalis_pool).await, 1);
    }

    /// An `OperatorWithdraw` with no same-tx `OperatorDeposit` cannot form a
    /// trade and must be dropped rather than enqueued.
    #[tokio::test]
    async fn unpaired_inventory_withdraw_is_skipped() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = inventory_test_evm_ctx();
        let inventory = evm_ctx.inventory_address();

        let venue_operator = address!("0x00000000000000000000000000000000000000a1");
        let equity = address!("0x00000000000000000000000000000000000000e9");
        let tx_hash =
            fixed_bytes!("0x3333333333333333333333333333333333333333333333333333333333333333");

        let withdraw_log = operator_withdraw_log(
            inventory,
            venue_operator,
            equity,
            uint!(9_000_000_000_000_000_000_U256),
            50,
            tx_hash,
            1,
        );

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([withdraw_log])); // inventory
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let BatchOutcome { enqueued, .. } = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
            1,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(enqueued, 0, "an unpaired OperatorWithdraw enqueues nothing");
        assert_eq!(job_count(&apalis_pool).await, 0);
    }

    /// A deposit+withdraw pair whose operator IS the bot's signing wallet is the
    /// bot's own rebalancing (it also calls deposit4/withdraw4 on the
    /// inventory), not a venue fill; it must be skipped entirely before pairing.
    #[tokio::test]
    async fn bot_operator_inventory_pair_is_skipped() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = inventory_test_evm_ctx();
        let inventory = evm_ctx.inventory_address();

        let usdc = address!("0x00000000000000000000000000000000000000dc");
        let equity = address!("0x00000000000000000000000000000000000000e9");
        let tx_hash =
            fixed_bytes!("0x4444444444444444444444444444444444444444444444444444444444444444");

        let deposit_log = operator_deposit_log(
            inventory,
            TEST_BOT_OPERATOR,
            usdc,
            uint!(1_000_000_U256),
            50,
            tx_hash,
            0,
        );
        let withdraw_log = operator_withdraw_log(
            inventory,
            TEST_BOT_OPERATOR,
            equity,
            uint!(9_000_000_000_000_000_000_U256),
            50,
            tx_hash,
            1,
        );

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([deposit_log, withdraw_log])); // inventory
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let BatchOutcome { enqueued, .. } = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
            1,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(
            enqueued, 0,
            "the bot's own rebalancing deposit+withdraw must be skipped"
        );
        assert_eq!(job_count(&apalis_pool).await, 0);
    }

    /// A `removed: true` `OperatorDeposit` (a reorg) must be dropped during
    /// bucketing rather than paired with its same-tx `OperatorWithdraw` --
    /// mirroring `test_backfill_skips_removed_logs`'s ClearV3/TakeOrderV3
    /// reorg-guard coverage, but for the inventory ingestion path.
    #[tokio::test]
    async fn removed_operator_deposit_is_dropped_and_leaves_withdraw_unpaired() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = inventory_test_evm_ctx();
        let inventory = evm_ctx.inventory_address();

        let venue_operator = address!("0x00000000000000000000000000000000000000a1");
        let usdc = address!("0x00000000000000000000000000000000000000dc");
        let equity = address!("0x00000000000000000000000000000000000000e9");
        let tx_hash =
            fixed_bytes!("0x5555555555555555555555555555555555555555555555555555555555555555");

        let mut deposit_log = operator_deposit_log(
            inventory,
            venue_operator,
            usdc,
            uint!(1_000_000_U256),
            50,
            tx_hash,
            0,
        );
        deposit_log.removed = true;
        let withdraw_log = operator_withdraw_log(
            inventory,
            venue_operator,
            equity,
            uint!(9_000_000_000_000_000_000_U256),
            50,
            tx_hash,
            1,
        );

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([deposit_log, withdraw_log])); // inventory
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let BatchOutcome { enqueued, .. } = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
            1,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(
            enqueued, 0,
            "a removed OperatorDeposit must not be bucketed, leaving the withdraw unpaired"
        );
        assert_eq!(job_count(&apalis_pool).await, 0);
    }

    /// An inventory pair and a `TakeOrderV3` in the same batch must enqueue in
    /// chronological `(block, log_index)` order: the inventory settlement at the
    /// earlier block before the later take.
    #[tokio::test]
    async fn inventory_pair_and_take_order_enqueue_in_chronological_order() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let order = get_test_order();
        let evm_ctx = inventory_test_evm_ctx();
        let inventory = evm_ctx.inventory_address();

        let venue_operator = address!("0x00000000000000000000000000000000000000a1");
        let usdc = address!("0x00000000000000000000000000000000000000dc");
        let equity = address!("0x00000000000000000000000000000000000000e9");
        let inv_tx_hash =
            fixed_bytes!("0x5555555555555555555555555555555555555555555555555555555555555555");
        let take_tx_hash =
            fixed_bytes!("0x6666666666666666666666666666666666666666666666666666666666666666");

        // Inventory pair at block 50 (withdraw at log_index 3).
        let deposit_log = operator_deposit_log(
            inventory,
            venue_operator,
            usdc,
            uint!(1_000_000_U256),
            50,
            inv_tx_hash,
            2,
        );
        let withdraw_log = operator_withdraw_log(
            inventory,
            venue_operator,
            equity,
            uint!(9_000_000_000_000_000_000_U256),
            50,
            inv_tx_hash,
            3,
        );

        // Take order at the later block 60 (log_index 1 via create_test_log).
        let take_event = create_test_take_event(
            &order,
            uint!(100_000_000_U256),
            uint!(9_000_000_000_000_000_000_U256),
        );
        let take_log = create_test_log(evm_ctx.orderbook, &take_event, 60, take_tx_hash);

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([take_log])); // take events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([deposit_log, withdraw_log])); // inventory
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let BatchOutcome { enqueued, .. } = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
            1,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(enqueued, 2, "one InventoryTrade plus one TakeOrderV3");

        let payloads: Vec<Vec<u8>> = sqlx::query_scalar("SELECT job FROM Jobs ORDER BY rowid ASC")
            .fetch_all(&pool)
            .await
            .unwrap();
        let enqueue_order: Vec<(u64, u64)> = payloads
            .iter()
            .map(|payload| {
                let job: AccountForDexTrade = serde_json::from_slice(payload).unwrap();
                (job.trade.block_number, job.trade.log_index)
            })
            .collect();

        assert_eq!(
            enqueue_order,
            vec![(50, 3), (60, 1)],
            "inventory settlement (block 50) must precede the take (block 60)"
        );
    }

    /// Two `OperatorDeposit`s in the same tx make the settlement ambiguous:
    /// `deposit4`/`withdraw4` are independent contract calls with no atomic
    /// settle entrypoint pairing them, so there is no reliable way to know
    /// which deposit the withdraw actually belongs to. The whole tx must be
    /// quarantined -- no `InventoryTrade` job, and no silent mis-hedge from
    /// guessing which amount is real.
    #[tokio::test]
    async fn duplicate_operator_deposit_in_one_tx_is_ambiguous_and_skipped() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = inventory_test_evm_ctx();
        let inventory = evm_ctx.inventory_address();

        let venue_operator = address!("0x00000000000000000000000000000000000000a1");
        let usdc = address!("0x00000000000000000000000000000000000000dc");
        let equity = address!("0x00000000000000000000000000000000000000e9");
        let tx_hash =
            fixed_bytes!("0x7777777777777777777777777777777777777777777777777777777777777777");

        let first_deposit_log = operator_deposit_log(
            inventory,
            venue_operator,
            usdc,
            uint!(1_000_000_U256),
            50,
            tx_hash,
            0,
        );
        let second_deposit_log = operator_deposit_log(
            inventory,
            venue_operator,
            usdc,
            uint!(2_000_000_U256),
            50,
            tx_hash,
            1,
        );
        let withdraw_log = operator_withdraw_log(
            inventory,
            venue_operator,
            equity,
            uint!(9_000_000_000_000_000_000_U256),
            50,
            tx_hash,
            2,
        );

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([
            first_deposit_log,
            second_deposit_log,
            withdraw_log
        ])); // inventory
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let BatchOutcome { enqueued, .. } = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
            1,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(
            enqueued, 0,
            "an ambiguous multi-deposit settlement must not enqueue a hedge"
        );
        assert_eq!(job_count(&apalis_pool).await, 0);

        let job_rows: Vec<Vec<u8>> = sqlx::query_scalar("SELECT job FROM Jobs")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert!(
            job_rows.is_empty(),
            "no InventoryTrade job may be built from an ambiguous settlement, \
             even one carrying the second deposit's amount"
        );
    }

    /// Two `OperatorWithdraw`s in the same tx are exactly as ambiguous as two
    /// `OperatorDeposit`s (mirrors `duplicate_operator_deposit_in_one_tx_is_ambiguous_and_skipped`):
    /// the single deposit could belong to either withdraw, so the whole tx
    /// must be quarantined -- zero `InventoryTrade` jobs, not one built by
    /// arbitrarily pairing the first withdraw encountered.
    #[tokio::test]
    async fn second_withdraw_in_one_tx_is_ambiguous_and_quarantined() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = inventory_test_evm_ctx();
        let inventory = evm_ctx.inventory_address();
        let metrics_handle = crate::metrics::setup().expect("install test metrics recorder");

        let venue_operator = address!("0x00000000000000000000000000000000000000a1");
        let usdc = address!("0x00000000000000000000000000000000000000dc");
        let equity = address!("0x00000000000000000000000000000000000000e9");
        let tx_hash =
            fixed_bytes!("0x8888888888888888888888888888888888888888888888888888888888888888");

        let deposit_log = operator_deposit_log(
            inventory,
            venue_operator,
            usdc,
            uint!(1_000_000_U256),
            50,
            tx_hash,
            0,
        );
        let first_withdraw_log = operator_withdraw_log(
            inventory,
            venue_operator,
            equity,
            uint!(9_000_000_000_000_000_000_U256),
            50,
            tx_hash,
            1,
        );
        let second_withdraw_log = operator_withdraw_log(
            inventory,
            venue_operator,
            equity,
            uint!(3_000_000_000_000_000_000_U256),
            50,
            tx_hash,
            2,
        );

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([
            deposit_log,
            first_withdraw_log,
            second_withdraw_log
        ])); // inventory
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let BatchOutcome { enqueued, .. } = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
            1,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(
            enqueued, 0,
            "an ambiguous multi-withdraw settlement must not enqueue a hedge"
        );
        assert_eq!(job_count(&apalis_pool).await, 0);

        let job_rows: Vec<Vec<u8>> = sqlx::query_scalar("SELECT job FROM Jobs")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert!(
            job_rows.is_empty(),
            "no InventoryTrade job may be built from an ambiguous multi-withdraw settlement, \
             even one carrying the first withdraw's amount"
        );

        let rendered = metrics_handle.render();
        assert!(
            rendered.contains("inventory_ambiguous_settlement_total"),
            "the ambiguous-settlement counter must fire for a quarantined multi-withdraw tx, \
             got:\n{rendered}"
        );
    }

    /// A tx with exactly one `OperatorDeposit` and one `OperatorWithdraw` is
    /// only unambiguous if both legs share the same `operator` -- a deposit
    /// from operator A paired with an unrelated withdraw from operator B is
    /// just as fabricated a vault delta as the multi-leg ambiguous cases
    /// above, and must be quarantined the same way rather than hedged.
    #[tokio::test]
    async fn mismatched_operator_deposit_and_withdraw_is_ambiguous_and_quarantined() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = inventory_test_evm_ctx();
        let inventory = evm_ctx.inventory_address();
        let metrics_handle = crate::metrics::setup().expect("install test metrics recorder");

        let deposit_operator = address!("0x00000000000000000000000000000000000000a1");
        let withdraw_operator = address!("0x00000000000000000000000000000000000000b2");
        let usdc = address!("0x00000000000000000000000000000000000000dc");
        let equity = address!("0x00000000000000000000000000000000000000e9");
        let tx_hash =
            fixed_bytes!("0x7777777777777777777777777777777777777777777777777777777777777777");

        let deposit_log = operator_deposit_log(
            inventory,
            deposit_operator,
            usdc,
            uint!(1_000_000_U256),
            50,
            tx_hash,
            0,
        );
        let withdraw_log = operator_withdraw_log(
            inventory,
            withdraw_operator,
            equity,
            uint!(9_000_000_000_000_000_000_U256),
            50,
            tx_hash,
            1,
        );

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([deposit_log, withdraw_log])); // inventory
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let BatchOutcome { enqueued, .. } = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
            1,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(
            enqueued, 0,
            "a deposit and withdraw from different operators must not be paired into a hedge"
        );
        assert_eq!(job_count(&apalis_pool).await, 0);

        let job_rows: Vec<Vec<u8>> = sqlx::query_scalar("SELECT job FROM Jobs")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert!(
            job_rows.is_empty(),
            "no InventoryTrade job may be built from a deposit/withdraw pair with mismatched \
             operators"
        );

        let rendered = metrics_handle.render();
        assert!(
            rendered.contains("inventory_ambiguous_settlement_total"),
            "the ambiguous-settlement counter must fire for a mismatched-operator tx, \
             got:\n{rendered}"
        );
    }

    /// An `OperatorDeposit` whose tx never surfaces a matching
    /// `OperatorWithdraw` in the batch must not enqueue a trade; the
    /// leftover deposit is dropped after the batch (with a `warn!`, see
    /// `enqueue_batch_events`) rather than pairing against nothing.
    #[tokio::test]
    async fn unpaired_operator_deposit_enqueues_nothing() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = inventory_test_evm_ctx();
        let inventory = evm_ctx.inventory_address();

        let venue_operator = address!("0x00000000000000000000000000000000000000a1");
        let usdc = address!("0x00000000000000000000000000000000000000dc");
        let tx_hash =
            fixed_bytes!("0x9999999999999999999999999999999999999999999999999999999999999999");

        let deposit_log = operator_deposit_log(
            inventory,
            venue_operator,
            usdc,
            uint!(1_000_000_U256),
            50,
            tx_hash,
            0,
        );

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([deposit_log])); // inventory
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let BatchOutcome { enqueued, .. } = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
            1,
            100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(enqueued, 0, "an unpaired OperatorDeposit enqueues nothing");
        assert_eq!(job_count(&apalis_pool).await, 0);
    }

    /// Real prod fill on Base mainnet (tx
    /// 0xe13a11de734768f08a9c1ef66e8de3bcb9072f8cdabce9f1d819e1ae9909d4b9,
    /// captured via `cast receipt <tx> --rpc-url https://mainnet.base.org`):
    /// a Bebop-routed settlement against the shared RaindexInventory at
    /// 0x6b7b523fadd1677413ad92c9404c8f0796bacf6f. Real vaultIds, real
    /// USDC/wtCOIN token addresses, and real amounts drive the same
    /// pairing/ingestion path the synthetic tests above exercise, pinning
    /// the backfill logic against the contract's actual event shape.
    #[tokio::test]
    async fn real_bebop_settlement_pairs_and_enqueues_one_job() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = inventory_test_evm_ctx();
        let inventory = evm_ctx.inventory_address();

        let operator = address!("0x8b8b6e0507c125934c6129563f48e48c66f86475");
        let usdc = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
        let wtcoin = address!("0x5CdA0E1cA4ce2Af96315F7F8963c85399c172204");
        let tx_hash =
            fixed_bytes!("0xe13a11de734768f08a9c1ef66e8de3bcb9072f8cdabce9f1d819e1ae9909d4b9");

        let deposit_event = OperatorDeposit {
            operator,
            token: usdc,
            vaultId: fixed_bytes!(
                "0x0000000000000000000000000000000000000000000000000000000000000004"
            ),
            amount: uint!(5_000_000_U256), // 5 USDC, 6 decimals
        };
        let withdraw_event = OperatorWithdraw {
            operator,
            token: wtcoin,
            vaultId: fixed_bytes!(
                "0x0000000000000000000000000000000000000000000000000000000000000003"
            ),
            amount: uint!(34_172_366_621_067_031_U256), // 0.034172366621067031 wtCOIN, 18 decimals
        };

        let deposit_log = Log {
            inner: alloy::primitives::Log {
                address: inventory,
                data: deposit_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(48_030_415),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(0xa7),
            removed: false,
        };
        let withdraw_log = Log {
            inner: alloy::primitives::Log {
                address: inventory,
                data: withdraw_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(48_030_415),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(0x9b),
            removed: false,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([deposit_log, withdraw_log])); // inventory
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let BatchOutcome { enqueued, .. } = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
            48_030_415,
            48_030_415,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(
            enqueued, 1,
            "the real deposit+withdraw pair is one InventoryTrade"
        );
        assert_eq!(job_count(&apalis_pool).await, 1);

        let payload: Vec<u8> = sqlx::query_scalar("SELECT job FROM Jobs ORDER BY rowid ASC")
            .fetch_one(&pool)
            .await
            .unwrap();
        let job: AccountForDexTrade = serde_json::from_slice(&payload).unwrap();
        match &job.trade.event {
            RaindexTradeEvent::InventoryTrade(inventory_trade) => {
                assert_eq!(inventory_trade.deposit.token, usdc);
                assert_eq!(inventory_trade.deposit.amount, uint!(5_000_000_U256));
                assert_eq!(inventory_trade.withdraw.token, wtcoin);
                assert_eq!(
                    inventory_trade.withdraw.amount,
                    uint!(34_172_366_621_067_031_U256)
                );
            }
            other => panic!("expected InventoryTrade event, got {other:?}"),
        }
    }

    /// Real prod fill on Base mainnet (tx
    /// 0x9ee8e401a6f12227df1a30a236b60ac83c72b2b1eb610d83cf292ae789eb0805,
    /// captured via `cast receipt <tx> --rpc-url https://mainnet.base.org`):
    /// a univ4-routed settlement against the shared RaindexInventory,
    /// mirroring the Bebop coverage above (`real_bebop_settlement_pairs_and_enqueues_one_job`)
    /// but with equity on the deposit side (pool bought equity, hedges Buy).
    #[tokio::test]
    async fn real_univ4_settlement_pairs_and_enqueues_one_job() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = inventory_test_evm_ctx();
        let inventory = evm_ctx.inventory_address();

        let operator = address!("0x36ebb1e5149c60111dd035f0417a4b00d39caa88");
        let usdc = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
        let wtcoin = address!("0x5CdA0E1cA4ce2Af96315F7F8963c85399c172204");
        let tx_hash =
            fixed_bytes!("0x9ee8e401a6f12227df1a30a236b60ac83c72b2b1eb610d83cf292ae789eb0805");

        let deposit_event = OperatorDeposit {
            operator,
            token: wtcoin,
            vaultId: fixed_bytes!(
                "0x0000000000000000000000000000000000000000000000000000000000000003"
            ),
            amount: uint!(10_000_000_000_000_000_U256), // 0.01 wtCOIN, 18 decimals
        };
        let withdraw_event = OperatorWithdraw {
            operator,
            token: usdc,
            vaultId: fixed_bytes!(
                "0x0000000000000000000000000000000000000000000000000000000000000004"
            ),
            amount: uint!(2_000_000_U256), // 2 USDC, 6 decimals
        };

        let deposit_log = Log {
            inner: alloy::primitives::Log {
                address: inventory,
                data: deposit_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(48_051_940),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(0xe1),
            removed: false,
        };
        let withdraw_log = Log {
            inner: alloy::primitives::Log {
                address: inventory,
                data: withdraw_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(48_051_940),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(0xf4),
            removed: false,
        };

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([deposit_log, withdraw_log])); // inventory
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let BatchOutcome { enqueued, .. } = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
            48_051_940,
            48_051_940,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(
            enqueued, 1,
            "the real deposit+withdraw pair is one InventoryTrade"
        );
        assert_eq!(job_count(&apalis_pool).await, 1);

        let payload: Vec<u8> = sqlx::query_scalar("SELECT job FROM Jobs ORDER BY rowid ASC")
            .fetch_one(&pool)
            .await
            .unwrap();
        let job: AccountForDexTrade = serde_json::from_slice(&payload).unwrap();
        match &job.trade.event {
            RaindexTradeEvent::InventoryTrade(inventory_trade) => {
                assert_eq!(inventory_trade.deposit.token, wtcoin);
                assert_eq!(
                    inventory_trade.deposit.amount,
                    uint!(10_000_000_000_000_000_U256)
                );
                assert_eq!(inventory_trade.withdraw.token, usdc);
                assert_eq!(inventory_trade.withdraw.amount, uint!(2_000_000_U256));
            }
            other => panic!("expected InventoryTrade event, got {other:?}"),
        }
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let evm_ctx = EvmCtx {
            rpc_url: Url::parse("http://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
            deployment_block: 1,
            required_confirmations: 0,
            ingestion_cutoff: IngestionCutoff::Safe,
        };

        let removed = backfill_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
            inventory: InventoryMode::Managed {
                inventory: address!("0x1111111111111111111111111111111111111111"),
            },
            vault_owner: address!("0x1111111111111111111111111111111111111111"),
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
        asserter.push_success(&serde_json::json!([])); // inventory events
        push_tip_response(&asserter);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let error = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
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
        asserter.push_success(&serde_json::json!([])); // inventory getLogs
        push_tip_response(&asserter); // inventory tip
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
                .build(noop_order_placer())
                .await
                .unwrap();
        let (vault_registry, _vault_registry_projection) =
            StoreBuilder::<VaultRegistry>::new(pool.clone())
                .build(())
                .await
                .unwrap();

        let cqrs = TradeProcessingCqrs {
            pool: pool.clone(),
            onchain_trade,
            position,
            position_projection,
            offchain_order,
            order_placer: noop_order_placer(),
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: ctx.assets.clone(),
            counter_trade_submission_lock: Arc::new(Mutex::new(())),
            poll_status_queue: PollOrderStatusJobQueue::new(&apalis_pool),
            hedge_queue: HedgeJobQueue::new(&apalis_pool),
        };

        let accountant_ctx = AccountantCtx {
            contracts: RaindexContracts {
                orderbook: ctx.evm.orderbook,
                inventory: ctx.evm.inventory_address(),
            },
            ctx,
            cache: SymbolCache::default(),
            pyth_feed_ids: PythFeedIds::default(),
            evm: ReadOnlyEvm::new(provider),
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
        };
        let before_reorg = Utc::now();
        record_reorg(&onchain_trade, &position, &removed, 12)
            .await
            .unwrap();
        let after_reorg = Utc::now();

        assert_eq!(count_events(&pool, "OnChainTradeEvent::Reorged").await, 1);
        assert_eq!(count_events(&pool, "PositionEvent::Reorged").await, 1);

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
    }

    /// A re-delivered backfill range must not double-reverse: the OnChainTrade
    /// `AlreadyReorged` guard short-circuits the second emission.
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
        };
        record_reorg(&onchain_trade, &position, &removed, 12)
            .await
            .unwrap();
        record_reorg(&onchain_trade, &position, &removed, 12)
            .await
            .unwrap();

        assert_eq!(count_events(&pool, "OnChainTradeEvent::Reorged").await, 1);
        assert_eq!(count_events(&pool, "PositionEvent::Reorged").await, 1);

        let position_state = position.load(&symbol).await.unwrap().unwrap();
        assert_eq!(
            position_state.net,
            FractionalShares::ZERO,
            "a re-delivery must not reverse the fill twice",
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
        };
        record_reorg(&onchain_trade, &position, &removed, 5)
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
        onchain_trade
            .send(
                &OnChainTradeId { tx_hash, log_index },
                OnChainTradeCommand::Witness {
                    symbol: symbol.clone(),
                    amount: float!(5),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_number: 100,
                    block_hash: None,
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
    }

    async fn count_events(pool: &SqlitePool, event_type: &str) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM events WHERE event_type = ?")
            .bind(event_type)
            .fetch_one(pool)
            .await
            .unwrap()
    }
}
