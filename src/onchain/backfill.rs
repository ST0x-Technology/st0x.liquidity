//! Historical onchain event backfill with retry logic.
//!
//! Scans past blocks for `ClearV3` and `TakeOrderV3` events on the OrderBook,
//! plus `OperatorDeposit`/`OperatorWithdraw` events on the shared
//! `RaindexInventory` (paired into `InventoryTrade`s), and pushes them into the
//! apalis job queue for processing, ensuring no trades are missed after
//! downtime. A persisted checkpoint records the last block that was fully
//! enqueued.

use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use backon::{BackoffBuilder, ExponentialBuilder, Retryable};
use futures_util::future;
use itertools::Itertools;
use metrics::counter;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::iter::from_fn;
use std::time::Duration;
use tracing::{debug, error, info, trace, warn};

use st0x_config::HedgedChain;
use st0x_evm::{Chain, Evm};
use st0x_execution::Executor;

use super::OnChainError;
use crate::alerts::AlertKind;
use crate::bindings::IRaindexInventory::{OperatorDeposit, OperatorWithdraw};
use crate::bindings::IRaindexV6::{ClearV3, TakeOrderV3};
use crate::conductor::job::{BackpressureStreak, Job, Label};
use crate::onchain::trade::{BotOperator, InventoryTrade, RaindexTradeEvent};
use crate::trading::onchain::inclusion::EmittedOnChain;
use crate::trading::onchain::trade_accountant::{
    AccountForDexTrade, AccountantCtx, DexTradeAccountingJobQueue, TradeAccountingError,
};

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
    evm_ctx: &HedgedChain,
    bot_operator: BotOperator,
    pool: &SqlitePool,
    end_block: u64,
    retry_strategy: B,
    job_queue: DexTradeAccountingJobQueue,
) -> Result<(), OnChainError> {
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
/// `InventoryTrade` per settlement tx), advancing the backfill checkpoint after
/// each batch is fully enqueued so a failed range keeps the progress it made.
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
    evm_ctx: &HedgedChain,
    bot_operator: BotOperator,
    pool: &SqlitePool,
    from_block: u64,
    to_block: u64,
    retry_strategy: B,
    job_queue: DexTradeAccountingJobQueue,
) -> Result<(), OnChainError> {
    if from_block > to_block {
        info!(
            target: "orderbook",
            "Already caught up to block {}, skipping backfill",
            to_block
        );

        save_backfill_checkpoint(pool, evm_ctx, to_block).await?;
        return Ok(());
    }

    let total_blocks = u128::from(to_block) - u128::from(from_block) + 1;

    info!(
        target: "orderbook",
        "Backfilling from block {} to {} ({} blocks)",
        from_block, to_block, total_blocks
    );

    let batch_ranges = generate_batch_ranges(evm_ctx.chain, from_block, to_block);

    let mut total_enqueued: usize = 0;
    for (batch_start, batch_end) in batch_ranges {
        let enqueued = enqueue_batch_events(
            provider,
            evm_ctx,
            bot_operator,
            batch_start,
            batch_end,
            retry_strategy.clone(),
            job_queue.clone(),
        )
        .await?;
        total_enqueued += enqueued;

        // Commit each batch as it lands rather than the whole range at the end.
        // The batches are contiguous and ascending from `from_block`
        // (= checkpoint + 1), and are processed strictly in order, so
        // `batch_end` is genuinely the last fully enqueued block -- committing
        // it leaves no gap. This is what lets a range too large for one job
        // attempt make progress: a timed-out or failed attempt resumes at the
        // first unscanned block instead of restarting from `from_block` and
        // never converging. The queue push is durable before the checkpoint
        // moves, so a crash in between only costs a re-scan, which the pipeline
        // dedupes on `(chain, tx_hash, log_index)`.
        save_backfill_checkpoint(pool, evm_ctx, batch_end).await?;
    }

    info!(target: "orderbook", total_enqueued, "Backfill completed");

    Ok(())
}

/// Persistent job queue for backfill jobs.
pub(crate) type BackfillJobQueue = crate::conductor::job::JobQueue<BackfillRange>;

/// One namespaced backfill queue per hedged chain. A chain's scan backlog,
/// in-flight rows, and orphan recovery are invisible to every other chain's
/// queue and workers, so one chain's long catch-up cannot freeze another's
/// ingestion through the monitor's overlap guard.
#[derive(Clone)]
pub(crate) struct BackfillQueues(std::collections::BTreeMap<Chain, BackfillJobQueue>);

impl BackfillQueues {
    /// Builds one namespaced queue per hedged chain in `chains`.
    pub(crate) fn new(
        apalis_pool: &apalis_sqlite::SqlitePool,
        chains: &st0x_config::ChainRegistry,
    ) -> Self {
        Self(
            chains
                .hedged()
                .map(|hedged| {
                    (
                        hedged.chain,
                        BackfillJobQueue::new_namespaced(apalis_pool, hedged.chain.as_str()),
                    )
                })
                .collect(),
        )
    }

    /// The queue for `chain`; `None` when the chain is not hedged.
    pub(crate) fn for_chain(&self, chain: Chain) -> Option<&BackfillJobQueue> {
        self.0.get(&chain)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&Chain, &BackfillJobQueue)> {
        self.0.iter()
    }

    /// Re-labels durable rows written before per-chain queues existed: their
    /// `job_type` is the bare task type name, which no namespaced queue reads,
    /// so without this they would sit stranded until the retention sweep
    /// DELETED them -- each one an ingested block range never scanned. All
    /// legacy rows belong to Base by definition. Runs at startup, after the
    /// apalis tables exist and before any worker spawns; idempotent.
    pub(crate) async fn relabel_legacy_rows(
        apalis_pool: &apalis_sqlite::SqlitePool,
    ) -> Result<u64, apalis_sqlite::SqlxError> {
        let legacy = std::any::type_name::<BackfillRange>();
        let namespaced = format!("{legacy}@{}", Chain::Base);
        let result = sqlx_apalis::query(
            "UPDATE Jobs SET job_type = ? \
             WHERE job_type = ? AND status NOT IN ('Done', 'Failed', 'Killed')",
        )
        .bind(namespaced)
        .bind(legacy)
        .execute(apalis_pool)
        .await?;

        Ok(result.rows_affected())
    }
}

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
    /// The chain this range scans. Defaulted only for in-flight apalis
    /// payloads enqueued before per-chain watchers.
    #[serde(default = "crate::onchain::legacy_chain")]
    pub(crate) chain: Chain,
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

    /// A range covering a long outage legitimately spends far longer than the
    /// default bound working through batched log scans, so the generous bound
    /// keeps the timeout a hang detector, not a cap on legitimate catch-up
    /// work. `backfill_range` checkpoints every batch it finishes, so an
    /// attempt that still times out resumes at the first unscanned block
    /// rather than restarting the range -- which on HyperEVM's 50-block
    /// batches is what keeps a day-long backlog converging.
    const PERFORM_TIMEOUT: Option<std::time::Duration> =
        Some(std::time::Duration::from_secs(2 * 60 * 60));

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind = crate::conductor::job::JobKind::Backfill;

    fn label(&self) -> Label {
        Label::new(format!(
            "BackfillRange:{}:{}:{}",
            self.chain, self.from_block, self.to_block
        ))
    }

    async fn perform(&self, ctx: &AccountantCtx<Node, Exec>) -> Result<Self::Output, Self::Error> {
        // Both the chain's config AND its provider come from the per-chain
        // accounting entry: scanning one chain's blocks through another
        // chain's RPC would silently read the wrong ledger.
        let chain_ctx = ctx
            .chains
            .get(&self.chain)
            .ok_or(OnChainError::UnhedgedChain { chain: self.chain })?;
        let checkpoint = load_backfill_checkpoint(&ctx.pool, &chain_ctx.trading).await?;
        let from_block = backfill_job_start(
            checkpoint,
            chain_ctx.trading.deployment_block,
            self.from_block,
        );

        backfill_range(
            chain_ctx.evm.provider(),
            &chain_ctx.trading,
            BotOperator(ctx.ctx.order_owner()),
            &ctx.pool,
            from_block,
            self.to_block,
            get_backfill_retry_strat(),
            chain_ctx.job_queue.clone(),
        )
        .await
    }
}

fn backfill_job_start(checkpoint: Option<u64>, deployment_block: u64, job_from_block: u64) -> u64 {
    backfill_start_from_checkpoint(checkpoint, deployment_block).max(job_from_block)
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

/// Loads the checkpoint and derives the backfill resume point in one call.
/// Test-only: production resolves the resume point in the fill monitor's poll
/// loop, which already holds the checkpoint, via
/// [`backfill_start_from_checkpoint`].
#[cfg(test)]
pub(crate) async fn backfill_start_block(
    pool: &SqlitePool,
    evm_ctx: &HedgedChain,
) -> Result<u64, OnChainError> {
    let checkpoint = load_backfill_checkpoint(pool, evm_ctx).await?;
    Ok(backfill_start_from_checkpoint(
        checkpoint,
        evm_ctx.deployment_block,
    ))
}

pub(crate) async fn load_backfill_checkpoint(
    pool: &SqlitePool,
    evm_ctx: &HedgedChain,
) -> Result<Option<u64>, OnChainError> {
    let row = sqlx::query_as::<_, (i64,)>(
        "SELECT last_processed_block FROM backfill_checkpoints \
         WHERE chain = ? AND orderbook = ?",
    )
    .bind(evm_ctx.chain.to_string())
    .bind(evm_ctx.orderbook.to_string())
    .fetch_optional(pool)
    .await?;

    row.map(|(last_processed_block,)| u64::try_from(last_processed_block))
        .transpose()
        .map_err(OnChainError::IntConversion)
}

pub(crate) async fn save_backfill_checkpoint(
    pool: &SqlitePool,
    evm_ctx: &HedgedChain,
    last_processed_block: u64,
) -> Result<(), OnChainError> {
    let last_processed_block = i64::try_from(last_processed_block)?;

    sqlx::query(
        "INSERT INTO backfill_checkpoints (chain, orderbook, last_processed_block) \
         VALUES (?, ?, ?) \
         ON CONFLICT(chain, orderbook) DO UPDATE SET \
         last_processed_block = MAX( \
             excluded.last_processed_block, \
             backfill_checkpoints.last_processed_block \
         ), \
         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
    )
    .bind(evm_ctx.chain.to_string())
    .bind(evm_ctx.orderbook.to_string())
    .bind(last_processed_block)
    .execute(pool)
    .await?;

    Ok(())
}

/// One `OperatorDeposit` or `OperatorWithdraw` a venue emitted on the shared
/// inventory, with the log that places it in its tx.
#[derive(Debug, Clone)]
pub(crate) enum InventoryLeg {
    Deposit(OperatorDeposit, Box<Log>),
    Withdraw(OperatorWithdraw, Box<Log>),
}

impl InventoryLeg {
    fn operator(&self) -> Address {
        match self {
            Self::Deposit(deposit, _) => deposit.operator,
            Self::Withdraw(withdraw, _) => withdraw.operator,
        }
    }

    const fn is_deposit(&self) -> bool {
        match self {
            Self::Deposit(..) => true,
            Self::Withdraw(..) => false,
        }
    }

    fn log(&self) -> &Log {
        match self {
            Self::Deposit(_, log) | Self::Withdraw(_, log) => log,
        }
    }

    fn token(&self) -> Address {
        match self {
            Self::Deposit(deposit, _) => deposit.token,
            Self::Withdraw(withdraw, _) => withdraw.token,
        }
    }

    fn amount(&self) -> U256 {
        match self {
            Self::Deposit(deposit, _) => deposit.amount,
            Self::Withdraw(withdraw, _) => withdraw.amount,
        }
    }
}

impl fmt::Display for InventoryLeg {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, vault_id) = match self {
            Self::Deposit(deposit, _) => ("deposit", deposit.vaultId),
            Self::Withdraw(withdraw, _) => ("withdraw", withdraw.vaultId),
        };
        write!(
            formatter,
            "{kind} operator={} token={} vault_id={vault_id} amount={} log_index={:?}",
            self.operator(),
            self.token(),
            self.amount(),
            self.log().log_index,
        )
    }
}

/// Why a tx's inventory legs could not be paired into settlements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuarantineReason {
    /// A leg has no log index, or two legs share one, so log order is unproven.
    UnorderedLegs,
    /// One operator emitted more deposits than withdraws, or the reverse.
    UnequalLegCounts { operator: Address },
    /// One operator emitted two legs of the same kind in a row.
    ConsecutiveSameKind { operator: Address },
    /// In a tx with several settlements, a pair does not have exactly one leg
    /// in the chain's settlement stable.
    InvalidPairTokens { operator: Address },
}

impl QuarantineReason {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::UnorderedLegs => "unordered_legs",
            Self::UnequalLegCounts { .. } => "unequal_leg_counts",
            Self::ConsecutiveSameKind { .. } => "consecutive_same_kind",
            Self::InvalidPairTokens { .. } => "invalid_pair_tokens",
        }
    }
}

impl fmt::Display for QuarantineReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnorderedLegs => {
                write!(formatter, "a leg has no log index, or two legs share one")
            }
            Self::UnequalLegCounts { operator } => write!(
                formatter,
                "operator {operator} emitted unequal numbers of deposits and withdraws"
            ),
            Self::ConsecutiveSameKind { operator } => write!(
                formatter,
                "operator {operator} emitted two legs of the same kind in a row"
            ),
            Self::InvalidPairTokens { operator } => write!(
                formatter,
                "a pair from operator {operator} does not have exactly one settlement-stable leg"
            ),
        }
    }
}

/// The trades paired from a batch of inventory legs, and the reason each
/// quarantined tx was held back.
pub(crate) struct PairedSettlements {
    pub(crate) trades: Vec<(InventoryTrade, Log)>,
    pub(crate) quarantined: Vec<QuarantineReason>,
}

/// A token's net raw change in the vault across a quarantined tx's legs.
#[derive(Debug, PartialEq, Eq)]
enum NetVaultChange {
    /// More was deposited than withdrawn, by this amount.
    In(U256),
    /// More was withdrawn than deposited, by this amount.
    Out(U256),
}

impl std::fmt::Display for NetVaultChange {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::In(amount) => write!(formatter, "+{amount}"),
            Self::Out(amount) => write!(formatter, "-{amount}"),
        }
    }
}

enum TxPairing<'legs> {
    Paired(Vec<(InventoryTrade, Log)>),
    Unpaired(&'legs InventoryLeg),
    Quarantined(QuarantineReason),
}

/// Splits a batch's raw inventory logs into per-tx legs, so the pairing pass
/// ([`pair_inventory_settlements`]) can pair each tx's settlements without an
/// extra RPC round-trip. Drops the bot's own rebalancing legs (treasury
/// moves, not venue fills), `removed: true` logs (reorg), and undecodable
/// logs.
///
/// `pub(crate)`: also reused by `OnchainTrade::try_from_tx_hash` (the manual
/// `process-tx` recovery path) so both ingestion paths pair
/// `OperatorDeposit`/`OperatorWithdraw` under the exact same rules.
pub(crate) fn bucket_inventory_logs(
    inv_logs: Vec<Log>,
    bot_operator: BotOperator,
) -> HashMap<TxHash, Vec<InventoryLeg>> {
    let BotOperator(bot_operator) = bot_operator;
    let mut legs_by_tx: HashMap<TxHash, Vec<InventoryLeg>> = HashMap::new();

    for log in inv_logs {
        let Some(topic0) = log.topic0().copied() else {
            continue;
        };
        let is_deposit = topic0 == OperatorDeposit::SIGNATURE_HASH;
        if !is_deposit && topic0 != OperatorWithdraw::SIGNATURE_HASH {
            continue;
        }

        // Callers cap `to_block` at the ingestion cutoff block, so a
        // `removed: true` leg here implies a reorg -- mirror the guard applied
        // to ClearV3/TakeOrderV3 logs rather than pairing a vanished event.
        if log.removed {
            warn!(
                target: "inventory",
                tx_hash = ?log.transaction_hash,
                log_index = ?log.log_index,
                block_number = ?log.block_number,
                is_deposit,
                "Backfill returned `removed: true` for an inventory log; reorg detected, skipping",
            );
            continue;
        }

        let Some(tx_hash) = log.transaction_hash else {
            warn!(target: "inventory", is_deposit, "Skipping inventory log with no tx hash");
            continue;
        };

        let leg = if is_deposit {
            let Ok(decoded) = log.log_decode::<OperatorDeposit>() else {
                warn!(target: "inventory", ?tx_hash, "Skipping OperatorDeposit that failed to decode");
                continue;
            };
            InventoryLeg::Deposit(decoded.data().clone(), Box::new(log))
        } else {
            let Ok(decoded) = log.log_decode::<OperatorWithdraw>() else {
                warn!(target: "inventory", ?tx_hash, "Skipping OperatorWithdraw that failed to decode");
                continue;
            };
            InventoryLeg::Withdraw(decoded.data().clone(), Box::new(log))
        };

        // The bot's own rebalancing also calls deposit4/withdraw4 on the
        // inventory, emitting legs whose operator is the bot's signing wallet.
        // Those are treasury moves, not venue fills to hedge.
        if leg.operator() == bot_operator {
            debug!(
                target: "inventory",
                ?tx_hash,
                "Skipping inventory event emitted by the bot's own rebalancing",
            );
            continue;
        }

        legs_by_tx.entry(tx_hash).or_default().push(leg);
    }

    legs_by_tx
}

/// Pairs each tx's inventory legs into `InventoryTrade` events under the
/// rules in SPEC.md ("InventoryTrade: a third fill source"). A tx whose legs
/// cannot be paired is quarantined whole -- zero `InventoryTrade`s from any of
/// its legs -- and raises an operational alert listing every leg.
///
/// `pub(crate)`: shared by both the batch backfill path
/// ([`enqueue_batch_events`]) and `OnchainTrade::try_from_tx_hash` (the
/// manual `process-tx` recovery path) so a settlement is paired or
/// quarantined identically regardless of which path discovers it.
pub(crate) fn pair_inventory_settlements(
    legs_by_tx: HashMap<TxHash, Vec<InventoryLeg>>,
    chain: Chain,
) -> PairedSettlements {
    let settlement_stable = chain.settlement_stable().address;
    let mut paired = PairedSettlements {
        trades: Vec::new(),
        quarantined: Vec::new(),
    };

    for (tx_hash, legs) in legs_by_tx {
        match pair_tx_legs(&legs, settlement_stable) {
            TxPairing::Paired(trades) => paired.trades.extend(trades),
            TxPairing::Unpaired(InventoryLeg::Deposit(deposit, _)) => {
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
            TxPairing::Unpaired(InventoryLeg::Withdraw(withdraw, _)) => {
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
            TxPairing::Quarantined(reason) => {
                let legs_summary = legs.iter().join("; ");
                let net_vault_change = net_vault_change_by_token(&legs).map_or_else(
                    || "a per-token sum overflows U256".to_owned(),
                    |changes| {
                        changes
                            .iter()
                            .map(|(token, change)| format!("{token}: {change}"))
                            .join(", ")
                    },
                );
                error!(
                    target: "operational_alert",
                    alert = true,
                    kind = AlertKind::InventorySettlementQuarantined.as_str(),
                    %chain,
                    ?tx_hash,
                    %reason,
                    legs = %legs_summary,
                    %net_vault_change,
                    "Inventory settlement quarantined: the inventory legs of this tx cannot be \
                     paired into settlements, so none of them is hedged or recorded in the \
                     position; cover the net vault change by hand",
                );
                counter!("inventory_ambiguous_settlement_total", "reason" => reason.label())
                    .increment(1);
                paired.quarantined.push(reason);
            }
        }
    }

    paired
}

/// Pairs one tx's legs. Each consecutive deposit and withdraw of one operator
/// is a settlement; see [`pair_inventory_settlements`] for the rules.
fn pair_tx_legs(legs: &[InventoryLeg], settlement_stable: Address) -> TxPairing<'_> {
    if let [only_leg] = legs {
        return TxPairing::Unpaired(only_leg);
    }

    let mut seen_log_indices = HashSet::new();
    let ordered = legs.iter().all(|leg| {
        leg.log()
            .log_index
            .is_some_and(|log_index| seen_log_indices.insert(log_index))
    });
    if !ordered {
        return TxPairing::Quarantined(QuarantineReason::UnorderedLegs);
    }

    let mut trades = Vec::new();
    for (operator, operator_legs) in legs
        .iter()
        .into_group_map_by(|leg| leg.operator())
        .into_iter()
        .sorted_by_key(|(operator, _)| *operator)
    {
        match pair_operator_legs(operator, operator_legs) {
            Ok(operator_trades) => trades.extend(operator_trades),
            Err(reason) => return TxPairing::Quarantined(reason),
        }
    }

    // Every pair of a multi-pair tx must hold one settlement-stable leg: then
    // each other token's legs are emitted unchanged, so the recorded fills
    // sum to the tx's net vault change whichever legs were paired together.
    if trades.len() > 1
        && let Some((trade, _)) = trades.iter().find(|(trade, _)| {
            (trade.deposit.token == settlement_stable)
                == (trade.withdraw.token == settlement_stable)
        })
    {
        return TxPairing::Quarantined(QuarantineReason::InvalidPairTokens {
            operator: trade.deposit.operator,
        });
    }

    trades.sort_by_key(|(_, log)| log.log_index);
    TxPairing::Paired(trades)
}

/// Pairs one operator's legs, which must strictly alternate between deposit
/// and withdraw and come out even. Settlements by one operator in one tx are
/// assumed not to nest, so each settlement's two legs are adjacent.
fn pair_operator_legs(
    operator: Address,
    mut legs: Vec<&InventoryLeg>,
) -> Result<Vec<(InventoryTrade, Log)>, QuarantineReason> {
    legs.sort_by_key(|leg| leg.log().log_index);

    if legs
        .iter()
        .tuple_windows()
        .any(|(earlier, later)| earlier.is_deposit() == later.is_deposit())
    {
        return Err(QuarantineReason::ConsecutiveSameKind { operator });
    }

    if !legs.len().is_multiple_of(2) {
        return Err(QuarantineReason::UnequalLegCounts { operator });
    }

    legs.into_iter()
        .tuples()
        .map(|pair| match pair {
            (InventoryLeg::Deposit(deposit, _), InventoryLeg::Withdraw(withdraw, log))
            | (InventoryLeg::Withdraw(withdraw, log), InventoryLeg::Deposit(deposit, _)) => Ok((
                InventoryTrade {
                    deposit: deposit.clone(),
                    withdraw: withdraw.clone(),
                },
                log.as_ref().clone(),
            )),
            (InventoryLeg::Deposit(..), InventoryLeg::Deposit(..))
            | (InventoryLeg::Withdraw(..), InventoryLeg::Withdraw(..)) => {
                Err(QuarantineReason::ConsecutiveSameKind { operator })
            }
        })
        .collect()
}

/// Nets a quarantined tx's legs per token, omitting a token whose deposits
/// and withdraws cancel out, or `None` if a sum overflows `U256` (the legs
/// themselves are logged alongside).
fn net_vault_change_by_token(legs: &[InventoryLeg]) -> Option<BTreeMap<Address, NetVaultChange>> {
    let totals = legs.iter().try_fold(
        BTreeMap::<Address, (U256, U256)>::new(),
        |mut totals, leg| {
            let (deposited, withdrawn) = totals.entry(leg.token()).or_default();
            match leg {
                InventoryLeg::Deposit(..) => *deposited = deposited.checked_add(leg.amount())?,
                InventoryLeg::Withdraw(..) => *withdrawn = withdrawn.checked_add(leg.amount())?,
            }
            Some(totals)
        },
    )?;
    Some(
        totals
            .into_iter()
            .filter_map(|(token, (deposited, withdrawn))| {
                let change = match deposited.cmp(&withdrawn) {
                    std::cmp::Ordering::Greater => NetVaultChange::In(deposited - withdrawn),
                    std::cmp::Ordering::Less => NetVaultChange::Out(withdrawn - deposited),
                    std::cmp::Ordering::Equal => return None,
                };
                Some((token, change))
            })
            .collect(),
    )
}

#[tracing::instrument(
    target = "orderbook",
    skip(provider, evm_ctx, retry_strategy, job_queue),
    fields(batch_start, batch_end),
    level = tracing::Level::DEBUG,
)]
async fn enqueue_batch_events<P: Provider + Clone, B: BackoffBuilder + Clone>(
    provider: &P,
    evm_ctx: &HedgedChain,
    bot_operator: BotOperator,
    batch_start: u64,
    batch_end: u64,
    retry_strategy: B,
    mut job_queue: DexTradeAccountingJobQueue,
) -> Result<usize, OnChainError> {
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
    // pair per settlement that routes through the shared inventory (Bebop
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
    let inventory_trade_events =
        pair_inventory_settlements(bucket_inventory_logs(inv_logs, bot_operator), evm_ctx.chain)
            .trades
            .into_iter()
            .map(|(trade, log)| (RaindexTradeEvent::InventoryTrade(Box::new(trade)), log));

    let trade_events = clear_logs
        .into_iter()
        .chain(take_logs)
        .filter(|log| {
            // Callers cap `to_block` at the ingestion cutoff block.
            // Under `finalized`, this block cannot reorg, so `removed: true`
            // implies a deep reorg crossing L1 finality. Under `safe`, a reorg
            // is plausible (the batch may not yet be L1-finalized). In either
            // case the fill is skipped to avoid persisting a vanished event. If a
            // hedge was already placed against this fill, no reversal occurs; full
            // reorg handling is tracked in the Reorg protection project.
            if log.removed {
                warn!(
                    target: "orderbook",
                    tx_hash = ?log.transaction_hash,
                    log_index = ?log.log_index,
                    block_number = ?log.block_number,
                    "Backfill returned `removed: true` for a log at or below the \
                     ingestion cutoff -- reorg detected; skipping. No reversal path \
                     exists if a hedge was placed against this fill"
                );
                return false;
            }
            true
        })
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
            EmittedOnChain::<RaindexTradeEvent>::from_log(evm_ctx.chain, event, &log)
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
        // dedupes on (chain, tx_hash, log_index)). `kind()` yields
        // "ClearV3"/"TakeOrderV3"/"InventoryTrade".
        counter!("onchain_events_total", "event_type" => trade_event.event.kind()).increment(1);
        job_queue
            .push(AccountForDexTrade {
                trade: trade_event,
                backpressure_streak: BackpressureStreak::default(),
            })
            .await?;
    }

    Ok(enqueued_count)
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

fn generate_batch_ranges(
    chain: Chain,
    start_block: u64,
    end_block: u64,
) -> impl Iterator<Item = (u64, u64)> {
    // Robinhood's 1000 mirrors Base's request size: the public endpoint and
    // Tenderly's gateway both served a 50,000-block getLogs range for the
    // orderbook filter (checked 2026-09-17), so this sits well inside both.
    let batch_size = match chain {
        Chain::Base | Chain::Ethereum | Chain::Robinhood => 1_000,
        Chain::HyperEvm => 50,
    };
    let mut next_start = (start_block <= end_block).then_some(start_block);

    from_fn(move || {
        let start = next_start?;
        let end = start + (end_block - start).min(batch_size - 1);
        next_start = if end == end_block {
            None
        } else {
            Some(end + 1)
        };
        Some((start, end))
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use alloy::primitives::{
        Address, B256, FixedBytes, IntoLogData, TxHash, U256, address, fixed_bytes, uint,
    };
    use alloy::providers::{ProviderBuilder, mock::Asserter};
    use alloy::rpc::types::Log;
    use httpmock::{Mock, MockServer};
    use proptest::prelude::*;
    use rain_math_float::Float;

    use st0x_config::{HedgedChain, InventoryMode};
    use st0x_evm::Chain;

    use super::*;
    use crate::bindings::IRaindexV6;

    /// Each hedged chain advances its own checkpoint row: writing one
    /// chain's checkpoint neither clobbers nor reads through to another's.
    #[tokio::test]
    async fn checkpoints_are_scoped_per_chain() {
        let pool = setup_test_db().await;
        let base = HedgedChain::test().deployment_block(1).call();
        let ethereum = HedgedChain::test()
            .chain(Chain::Ethereum)
            .orderbook(alloy::primitives::Address::repeat_byte(0x22))
            .deployment_block(1)
            .call();

        save_backfill_checkpoint(&pool, &base, 100).await.unwrap();
        save_backfill_checkpoint(&pool, &ethereum, 7).await.unwrap();

        assert_eq!(
            load_backfill_checkpoint(&pool, &base).await.unwrap(),
            Some(100)
        );
        assert_eq!(
            load_backfill_checkpoint(&pool, &ethereum).await.unwrap(),
            Some(7)
        );

        // The monotonic MAX guard holds per (chain, orderbook).
        save_backfill_checkpoint(&pool, &base, 50).await.unwrap();
        assert_eq!(
            load_backfill_checkpoint(&pool, &base).await.unwrap(),
            Some(100),
            "a backwards write must not rewind the checkpoint"
        );
    }

    /// Deterministic deployments put the same orderbook address on several
    /// chains; each still gets its own checkpoint row.
    #[tokio::test]
    async fn chains_sharing_an_orderbook_address_checkpoint_independently() {
        let pool = setup_test_db().await;
        let base = HedgedChain::test().deployment_block(1).call();
        let ethereum = HedgedChain::test()
            .chain(Chain::Ethereum)
            .orderbook(base.orderbook)
            .deployment_block(1)
            .call();

        save_backfill_checkpoint(&pool, &base, 100).await.unwrap();
        save_backfill_checkpoint(&pool, &ethereum, 7).await.unwrap();

        assert_eq!(
            load_backfill_checkpoint(&pool, &base).await.unwrap(),
            Some(100)
        );
        assert_eq!(
            load_backfill_checkpoint(&pool, &ethereum).await.unwrap(),
            Some(7)
        );
    }

    /// Admitting a second hedged chain closes the rollback window: a
    /// checkpoint write that names no chain is refused rather than silently
    /// filed under Base.
    #[tokio::test]
    async fn checkpoint_without_a_chain_is_refused_once_a_second_chain_is_admitted() {
        let pool = setup_test_db().await;
        let base = HedgedChain::test().deployment_block(1).call();

        let error = sqlx::query(
            "INSERT INTO backfill_checkpoints (orderbook, last_processed_block) \
             VALUES (?, ?)",
        )
        .bind(base.orderbook.to_string())
        .bind(250_i64)
        .execute(&pool)
        .await
        .unwrap_err();

        assert_eq!(
            error.as_database_error().unwrap().message(),
            "NOT NULL constraint failed: backfill_checkpoints.chain"
        );
        assert_eq!(load_backfill_checkpoint(&pool, &base).await.unwrap(), None);
    }

    /// Rows enqueued by a pre-per-chain binary carry the bare type name as
    /// their job_type; startup adopts them into Base's namespaced queue so
    /// they are neither stranded nor retention-deleted.
    #[tokio::test]
    async fn legacy_backfill_rows_are_adopted_into_the_base_queue() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let mut legacy_queue = BackfillJobQueue::new(&apalis_pool);
        legacy_queue
            .push(BackfillRange {
                chain: Chain::Base,
                from_block: 1,
                to_block: 2,
            })
            .await
            .unwrap();

        let adopted = BackfillQueues::relabel_legacy_rows(&apalis_pool)
            .await
            .unwrap();
        assert_eq!(adopted, 1);

        let namespaced = BackfillJobQueue::new_namespaced(&apalis_pool, Chain::Base.as_str());
        assert!(
            namespaced.has_in_flight().await.unwrap(),
            "the adopted row must be visible to the namespaced queue"
        );

        let legacy_view = BackfillJobQueue::new(&apalis_pool);
        assert!(
            !legacy_view.has_in_flight().await.unwrap(),
            "no row may remain under the bare type name"
        );
    }

    /// Namespaced queues of the same task type are mutually invisible: one
    /// chain's in-flight rows and orphan recovery never touch another's.
    #[tokio::test]
    async fn namespaced_queues_are_mutually_invisible() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let mut base_queue = BackfillJobQueue::new_namespaced(&apalis_pool, Chain::Base.as_str());
        let ethereum_queue =
            BackfillJobQueue::new_namespaced(&apalis_pool, Chain::Ethereum.as_str());

        base_queue
            .push(BackfillRange {
                chain: Chain::Base,
                from_block: 1,
                to_block: 2,
            })
            .await
            .unwrap();

        assert!(base_queue.has_in_flight().await.unwrap());
        assert!(
            !ethereum_queue.has_in_flight().await.unwrap(),
            "another chain's rows must not read as in flight"
        );
        assert_eq!(
            ethereum_queue.requeue_orphaned().await.unwrap(),
            0,
            "orphan recovery must not adopt another chain's rows"
        );
    }
    use crate::test_utils::{get_test_order, setup_test_db, setup_test_pools};

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

    fn mock_scan_batch<'a>(
        server: &'a MockServer,
        trading: &HedgedChain,
        start: u64,
        end: u64,
        fail_clear: bool,
    ) -> Vec<Mock<'a>> {
        [
            (trading.orderbook, vec![ClearV3::SIGNATURE_HASH]),
            (trading.orderbook, vec![TakeOrderV3::SIGNATURE_HASH]),
            (
                trading.inventory_address(),
                vec![
                    OperatorDeposit::SIGNATURE_HASH,
                    OperatorWithdraw::SIGNATURE_HASH,
                ],
            ),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (address, topics))| {
            let filter = Filter::new()
                .address(address)
                .from_block(start)
                .to_block(end)
                .event_signature(topics);
            server.mock(|when, then| {
                when.json_body_includes(r#"{"method":"eth_getLogs"}"#)
                    .is_true(move |request| {
                        serde_json::from_slice::<serde_json::Value>(request.body_ref())
                            .ok()
                            .and_then(|body| {
                                serde_json::from_value::<Filter>(body["params"][0].clone()).ok()
                            })
                            .is_some_and(|actual| actual == filter)
                    });
                let response = if fail_clear && index == 0 {
                    serde_json::json!({"jsonrpc": "2.0", "id": 0,
                        "error": {"code": -32603, "message": "temporarily unavailable"}})
                } else {
                    serde_json::json!({"jsonrpc": "2.0", "id": 0, "result": []})
                };
                then.json_body(response);
            })
        })
        .collect()
    }

    #[tokio::test]
    async fn backfill_uses_each_chains_inclusive_rpc_limit_for_every_filter() {
        for (chain, ranges) in [
            (Chain::HyperEvm, vec![(100, 149), (150, 150)]),
            (Chain::Base, vec![(100, 150)]),
            (Chain::Ethereum, vec![(100, 150)]),
            (Chain::Robinhood, vec![(100, 150)]),
        ] {
            let (pool, apalis_pool) = setup_test_pools().await;
            let trading = HedgedChain::test().chain(chain).call();
            let server = MockServer::start();
            let tip = server.mock(|when, then| {
                when.json_body_includes(r#"{"method":"eth_blockNumber"}"#);
                then.json_body(serde_json::json!({
                    "jsonrpc": "2.0", "id": 0, "result": "0x96",
                }));
            });
            let batches: Vec<_> = ranges
                .iter()
                .flat_map(|&(start, end)| mock_scan_batch(&server, &trading, start, end, false))
                .collect();
            let provider = ProviderBuilder::new().connect_http(server.base_url().parse().unwrap());

            backfill_range(
                &provider,
                &trading,
                BotOperator(TEST_BOT_OPERATOR),
                &pool,
                100,
                150,
                test_retry_strategy(),
                setup_job_queue(&apalis_pool),
            )
            .await
            .unwrap();

            for batch in batches {
                batch.assert_calls(1);
            }
            tip.assert_calls(ranges.len() * 3);
            assert_eq!(
                load_backfill_checkpoint(&pool, &trading).await.unwrap(),
                Some(150)
            );
        }
    }

    #[tokio::test]
    async fn hyperevm_batch_failure_checkpoints_the_last_completed_batch() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let trading = HedgedChain::test().chain(Chain::HyperEvm).call();
        save_backfill_checkpoint(&pool, &trading, 99).await.unwrap();
        let server = MockServer::start();
        server.mock(|when, then| {
            when.json_body_includes(r#"{"method":"eth_blockNumber"}"#);
            then.json_body(serde_json::json!({"jsonrpc": "2.0", "id": 0, "result": "0x96"}));
        });
        let first = mock_scan_batch(&server, &trading, 100, 149, false);
        let mut second = mock_scan_batch(&server, &trading, 150, 150, true);
        let provider = ProviderBuilder::new().connect_http(server.base_url().parse().unwrap());

        let result = backfill_range(
            &provider,
            &trading,
            BotOperator(TEST_BOT_OPERATOR),
            &pool,
            100,
            150,
            test_retry_strategy(),
            setup_job_queue(&apalis_pool),
        )
        .await;

        let error = result.unwrap_err();
        assert!(
            matches!(&error, OnChainError::RpcTransport(rpc)
                if rpc.as_error_resp().is_some_and(|payload|
                    payload.code == -32603 && payload.message == "temporarily unavailable")),
            "expected the second batch's RPC error, got: {error:?}"
        );
        for batch in &first {
            batch.assert_calls(1);
        }
        second[0].assert_calls(3);
        for batch in &second[1..] {
            batch.assert_calls(1);
        }
        // The first batch was fully enqueued, so its blocks are committed even
        // though the range as a whole failed: a retry resumes at 150 instead of
        // re-scanning from 100. Without this a long HyperEVM catch-up that
        // exceeds PERFORM_TIMEOUT never converges.
        assert_eq!(
            load_backfill_checkpoint(&pool, &trading).await.unwrap(),
            Some(149)
        );

        for batch in &mut second {
            batch.delete();
        }
        let recovered = mock_scan_batch(&server, &trading, 150, 150, false);
        backfill_range(
            &provider,
            &trading,
            BotOperator(TEST_BOT_OPERATOR),
            &pool,
            100,
            150,
            test_retry_strategy(),
            setup_job_queue(&apalis_pool),
        )
        .await
        .unwrap();
        for batch in first {
            batch.assert_calls(2);
        }
        for batch in recovered {
            batch.assert_calls(1);
        }
        assert_eq!(
            load_backfill_checkpoint(&pool, &trading).await.unwrap(),
            Some(150)
        );
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
        let evm_ctx = HedgedChain::test().deployment_block(50).call();

        let start_block = backfill_start_block(&pool, &evm_ctx).await.unwrap();

        assert_eq!(start_block, 50);
    }

    #[tokio::test]
    async fn test_backfill_start_block_resumes_after_checkpoint() {
        let pool = setup_test_db().await;
        let evm_ctx = HedgedChain::test().deployment_block(50).call();

        save_backfill_checkpoint(&pool, &evm_ctx, 80).await.unwrap();

        let start_block = backfill_start_block(&pool, &evm_ctx).await.unwrap();

        assert_eq!(start_block, 81);
    }

    #[tokio::test]
    async fn test_backfill_start_block_respects_deployment_block_floor() {
        let pool = setup_test_db().await;
        let evm_ctx = HedgedChain::test().deployment_block(50).call();

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
    fn durable_job_retry_resumes_after_persisted_checkpoint() {
        assert_eq!(backfill_job_start(Some(1_000), 1, 1), 1_001);
        assert_eq!(
            backfill_job_start(Some(500), 1, 1_000),
            1_000,
            "a checkpoint from an older range must not move this job backwards"
        );
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
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
    fn chain_batch_ranges_cover_boundaries_and_residuals() {
        for (chain, cap) in [
            (Chain::Base, 1000),
            (Chain::Ethereum, 1000),
            (Chain::HyperEvm, 50),
            (Chain::Robinhood, 1000),
        ] {
            for start in [0, 100, u64::MAX - 3 * cap] {
                assert_eq!(
                    generate_batch_ranges(chain, start, start + cap - 1).collect::<Vec<_>>(),
                    vec![(start, start + cap - 1)]
                );
                assert_eq!(
                    generate_batch_ranges(chain, start, start + cap).collect::<Vec<_>>(),
                    vec![(start, start + cap - 1), (start + cap, start + cap)]
                );
                assert_eq!(
                    generate_batch_ranges(chain, start, start + 2 * cap + 4).collect::<Vec<_>>(),
                    vec![
                        (start, start + cap - 1),
                        (start + cap, start + 2 * cap - 1),
                        (start + 2 * cap, start + 2 * cap + 4)
                    ]
                );
            }
            assert_eq!(
                generate_batch_ranges(chain, u64::MAX, u64::MAX).collect::<Vec<_>>(),
                vec![(u64::MAX, u64::MAX)]
            );
            assert_eq!(
                generate_batch_ranges(chain, u64::MAX - cap, u64::MAX).collect::<Vec<_>>(),
                vec![(u64::MAX - cap, u64::MAX - 1), (u64::MAX, u64::MAX)]
            );
            assert_eq!(generate_batch_ranges(chain, u64::MAX, 0).next(), None);
            assert_eq!(
                generate_batch_ranges(chain, 0, u64::MAX)
                    .take(2)
                    .collect::<Vec<_>>(),
                vec![(0, cap - 1), (cap, cap * 2 - 1)]
            );
        }
    }

    proptest! {
        #[test]
        fn chain_batch_ranges_are_complete_contiguous_and_capped(
            start in any::<u64>(),
            length in 0_u64..5000,
            policy in prop::sample::select(vec![(Chain::Base, 1000_u64),
                (Chain::Ethereum, 1000), (Chain::HyperEvm, 50), (Chain::Robinhood, 1000)]),
        ) {
            let (chain, cap) = policy;
            let end = start + length.min(u64::MAX - start);
            let ranges: Vec<_> = generate_batch_ranges(chain, start, end).collect();
            prop_assert_eq!(ranges.first().unwrap().0, start);
            prop_assert_eq!(ranges.last().unwrap().1, end);
            for &(from, to) in &ranges {
                prop_assert!(from <= to);
                prop_assert!(to - from < cap);
            }
            for adjacent in ranges.windows(2) {
                prop_assert_eq!(adjacent[0].1 + 1, adjacent[1].0);
            }
        }
    }

    #[test]
    fn batch_ranges_near_max_preserve_inclusive_coverage() {
        assert_eq!(
            generate_batch_ranges(Chain::Base, u64::MAX - 50, u64::MAX).collect::<Vec<_>>(),
            vec![(u64::MAX - 50, u64::MAX)]
        );
    }

    #[test]
    fn test_generate_batch_ranges_single_batch() {
        let ranges: Vec<_> = generate_batch_ranges(Chain::Base, 100, 500).collect();
        assert_eq!(ranges, vec![(100, 500)]);
    }

    #[test]
    fn test_generate_batch_ranges_exact_batch_size() {
        let ranges: Vec<_> = generate_batch_ranges(Chain::Base, 100, 1099).collect();
        assert_eq!(ranges, vec![(100, 1099)]);
    }

    #[test]
    fn test_generate_batch_ranges_multiple_batches() {
        let ranges: Vec<_> = generate_batch_ranges(Chain::Base, 100, 2500).collect();
        assert_eq!(ranges, vec![(100, 1099), (1100, 2099), (2100, 2500)]);
    }

    #[test]
    fn test_generate_batch_ranges_large_range() {
        let ranges: Vec<_> = generate_batch_ranges(Chain::Base, 5000, 25000).collect();
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
        let ranges: Vec<_> = generate_batch_ranges(Chain::Base, 100, 25000).collect();
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
        let ranges: Vec<_> = generate_batch_ranges(Chain::Base, 42, 42).collect();
        assert_eq!(ranges, vec![(42, 42)]);
    }

    #[test]
    fn test_generate_batch_ranges_empty() {
        assert_eq!(generate_batch_ranges(Chain::Base, 100, 99).count(), 0);
    }

    #[tokio::test]
    async fn test_backfill_events_with_clear_v3_events() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let order = get_test_order();
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(50).call();

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
        // A non-Base chain, so the chain-propagation assertion below fails if
        // enqueueing hard-codes `Chain::Base` instead of the configured chain.
        let evm_ctx = HedgedChain::test()
            .chain(Chain::Ethereum)
            .deployment_block(1)
            .call();

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

        for job in &jobs {
            assert_eq!(
                job.trade.chain,
                Chain::Ethereum,
                "every enqueued fill must carry the configured chain"
            );
        }

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
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(1000).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(500).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
        .unwrap();

        assert_eq!(enqueued_count, 1);
        assert_eq!(job_count(&apalis_pool).await, 1);
    }

    #[tokio::test]
    async fn test_backfill_events_large_block_range_batching() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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

        let enqueued_count = result.unwrap();
        assert_eq!(enqueued_count, 0);
    }

    #[tokio::test]
    async fn test_process_batch_exhausted_retries() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
        // The first 1000-block batch completed before the second one failed, so
        // its blocks stay committed and the retry resumes at 1001.
        assert_eq!(
            load_backfill_checkpoint(&pool, &evm_ctx).await.unwrap(),
            Some(1000)
        );
    }

    #[tokio::test]
    async fn test_backfill_events_corrupted_log_data() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
    async fn test_backfill_skips_removed_logs() {
        // Backfill caller caps `to_block` at the ingestion cutoff
        // block. A `removed: true` log implies a reorg; skip it
        // (logged as warn) rather than ingesting a vanished event.
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let order = get_test_order();
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
            "logs with removed=true must be skipped, not ingested"
        );
    }

    #[tokio::test]
    async fn test_backfill_events_single_block_range() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = HedgedChain::test().deployment_block(42).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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

        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_enqueue_batch_events_partial_enqueue_failure() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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

        let enqueued = result.unwrap();
        assert_eq!(enqueued, 2);
    }

    #[tokio::test]
    async fn test_backfill_events_concurrent_batch_processing() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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

        let enqueued = result.unwrap();
        assert_eq!(enqueued, 0);

        // Should have taken at least the test initial delay time due to retries
        assert!(elapsed >= Duration::from_millis(1));
    }

    #[tokio::test]
    async fn test_backfill_events_zero_blocks() {
        let (pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = HedgedChain::test().deployment_block(100).call();

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
        let evm_ctx = HedgedChain::test().deployment_block(1).call();

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

        let enqueued = result.unwrap();
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
        let evm_ctx = HedgedChain::test().deployment_block(50).call();

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

    fn inventory_test_evm_ctx() -> HedgedChain {
        HedgedChain::test()
            .inventory(InventoryMode::Managed {
                inventory: address!("0x2222222222222222222222222222222222222222"),
            })
            .deployment_block(1)
            .call()
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

        let enqueued = enqueue_batch_events(
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

        let enqueued = enqueue_batch_events(
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

        let enqueued = enqueue_batch_events(
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

        let enqueued = enqueue_batch_events(
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

        let enqueued = enqueue_batch_events(
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

        let enqueued = enqueue_batch_events(
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

        let enqueued = enqueue_batch_events(
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

        let enqueued = enqueue_batch_events(
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

        let enqueued = enqueue_batch_events(
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

        let enqueued = enqueue_batch_events(
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

        let enqueued = enqueue_batch_events(
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

    const BEBOP_OPERATOR: Address = address!("0x000000000000000000000000000000000000b0b0");
    const EQUITY: Address = address!("0x000000000000000000000000000000000000e001");
    const DOUBLE_SETTLEMENT_TX: TxHash =
        fixed_bytes!("0xd5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5");

    /// One leg on the test harness's inventory, in `DOUBLE_SETTLEMENT_TX`.
    fn settlement_leg(
        is_deposit: bool,
        operator: Address,
        token: Address,
        amount: U256,
        log_index: u64,
    ) -> Log {
        let inventory = inventory_test_evm_ctx().inventory_address();
        if is_deposit {
            operator_deposit_log(
                inventory,
                operator,
                token,
                amount,
                1_000,
                DOUBLE_SETTLEMENT_TX,
                log_index,
            )
        } else {
            operator_withdraw_log(
                inventory,
                operator,
                token,
                amount,
                1_000,
                DOUBLE_SETTLEMENT_TX,
                log_index,
            )
        }
    }

    /// Two Bebop settlements in one tx, each withdrawing USDC from the vault
    /// and depositing the equity.
    fn double_settlement_legs() -> Vec<Log> {
        let usdc = inventory_test_evm_ctx().chain.settlement_stable().address;
        vec![
            settlement_leg(false, BEBOP_OPERATOR, usdc, uint!(200_000_000_U256), 10),
            settlement_leg(
                true,
                BEBOP_OPERATOR,
                EQUITY,
                U256::from(2_000_000_000_000_000_000_u64),
                12,
            ),
            settlement_leg(false, BEBOP_OPERATOR, usdc, uint!(300_000_000_U256), 20),
            settlement_leg(
                true,
                BEBOP_OPERATOR,
                EQUITY,
                U256::from(3_000_000_000_000_000_000_u64),
                22,
            ),
        ]
    }

    fn pair_logs(logs: Vec<Log>) -> PairedSettlements {
        pair_inventory_settlements(
            bucket_inventory_logs(logs, BotOperator(TEST_BOT_OPERATOR)),
            inventory_test_evm_ctx().chain,
        )
    }

    /// `(withdraw token, withdraw amount, deposit token, deposit amount, log index)`
    /// of each paired trade, in log order.
    fn trade_legs(paired: &PairedSettlements) -> Vec<(Address, U256, Address, U256, Option<u64>)> {
        paired
            .trades
            .iter()
            .map(|(trade, log)| {
                (
                    trade.withdraw.token,
                    trade.withdraw.amount,
                    trade.deposit.token,
                    trade.deposit.amount,
                    log.log_index,
                )
            })
            .sorted_by_key(|legs| legs.4)
            .collect()
    }

    #[test]
    fn a_double_bebop_settlement_pairs_into_one_trade_per_leg() {
        let usdc = inventory_test_evm_ctx().chain.settlement_stable().address;

        let paired = pair_logs(double_settlement_legs());

        assert_eq!(
            trade_legs(&paired),
            vec![
                (
                    usdc,
                    uint!(200_000_000_U256),
                    EQUITY,
                    U256::from(2_000_000_000_000_000_000_u64),
                    Some(10)
                ),
                (
                    usdc,
                    uint!(300_000_000_U256),
                    EQUITY,
                    U256::from(3_000_000_000_000_000_000_u64),
                    Some(20)
                ),
            ]
        );
        assert!(paired.quarantined.is_empty());
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn a_double_bebop_settlement_enqueues_two_jobs_without_an_alert() {
        let (_pool, apalis_pool) = setup_test_pools().await;
        let job_queue = setup_job_queue(&apalis_pool);
        let evm_ctx = inventory_test_evm_ctx();

        let asserter = Asserter::new();
        asserter.push_success(&serde_json::json!([])); // clear events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!([])); // take events
        push_tip_response(&asserter);
        asserter.push_success(&serde_json::json!(double_settlement_legs())); // inventory
        push_tip_response(&asserter);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let enqueued = enqueue_batch_events(
            &provider,
            &evm_ctx,
            BotOperator(TEST_BOT_OPERATOR),
            900,
            1_100,
            test_retry_strategy(),
            job_queue,
        )
        .await
        .unwrap();

        assert_eq!(
            enqueued, 2,
            "each Bebop settlement of the tx is its own trade"
        );
        assert_eq!(job_count(&apalis_pool).await, 2);
        assert!(!logs_contain("Inventory settlement quarantined"));
    }

    #[test]
    fn deposit_first_settlements_pair_and_are_keyed_by_each_withdraw() {
        let usdc = inventory_test_evm_ctx().chain.settlement_stable().address;
        let operator = address!("0x00000000000000000000000000000000000000a4");

        let paired = pair_logs(vec![
            settlement_leg(true, operator, EQUITY, uint!(2_U256), 10),
            settlement_leg(false, operator, usdc, uint!(200_U256), 11),
            settlement_leg(true, operator, EQUITY, uint!(3_U256), 12),
            settlement_leg(false, operator, usdc, uint!(300_U256), 13),
        ]);

        assert_eq!(
            trade_legs(&paired),
            vec![
                (usdc, uint!(200_U256), EQUITY, uint!(2_U256), Some(11)),
                (usdc, uint!(300_U256), EQUITY, uint!(3_U256), Some(13)),
            ]
        );
    }

    #[test]
    fn settlements_from_two_operators_in_one_tx_pair_per_operator() {
        let usdc = inventory_test_evm_ctx().chain.settlement_stable().address;
        let univ4 = address!("0x00000000000000000000000000000000000000a5");

        let paired = pair_logs(vec![
            settlement_leg(false, BEBOP_OPERATOR, usdc, uint!(100_U256), 1),
            settlement_leg(true, univ4, EQUITY, uint!(5_U256), 2),
            settlement_leg(true, BEBOP_OPERATOR, EQUITY, uint!(1_U256), 3),
            settlement_leg(false, univ4, usdc, uint!(500_U256), 4),
        ]);

        assert_eq!(
            trade_legs(&paired),
            vec![
                (usdc, uint!(100_U256), EQUITY, uint!(1_U256), Some(1)),
                (usdc, uint!(500_U256), EQUITY, uint!(5_U256), Some(4)),
            ]
        );
        assert!(paired.quarantined.is_empty());
    }

    #[test]
    fn net_vault_change_is_signed_per_token_and_omits_tokens_that_cancel() {
        let usdc = inventory_test_evm_ctx().chain.settlement_stable().address;
        let other = address!("0x00000000000000000000000000000000000000a6");
        let legs = bucket_inventory_logs(
            vec![
                settlement_leg(false, BEBOP_OPERATOR, usdc, uint!(100_U256), 1),
                settlement_leg(true, BEBOP_OPERATOR, usdc, uint!(30_U256), 2),
                settlement_leg(true, BEBOP_OPERATOR, EQUITY, uint!(5_U256), 3),
                settlement_leg(true, BEBOP_OPERATOR, other, uint!(7_U256), 4),
                settlement_leg(false, BEBOP_OPERATOR, other, uint!(7_U256), 5),
            ],
            BotOperator(TEST_BOT_OPERATOR),
        )
        .into_values()
        .next()
        .unwrap();

        assert_eq!(
            net_vault_change_by_token(&legs),
            Some(BTreeMap::from([
                (usdc, NetVaultChange::Out(uint!(70_U256))),
                (EQUITY, NetVaultChange::In(uint!(5_U256))),
            ]))
        );
        assert_eq!(NetVaultChange::Out(uint!(70_U256)).to_string(), "-70");
        assert_eq!(NetVaultChange::In(uint!(5_U256)).to_string(), "+5");
    }

    /// Each shape below cannot be paired with certainty, so no leg of the tx
    /// is hedged, one alert names the tx, and the counter rises once.
    #[test]
    #[tracing_test::traced_test]
    fn unpairable_txs_are_quarantined_whole_with_one_alert() {
        let usdc = inventory_test_evm_ctx().chain.settlement_stable().address;
        let other = address!("0x00000000000000000000000000000000000000a6");
        let metrics_handle = crate::metrics::setup().expect("install test metrics recorder");

        let cases = vec![
            (
                "withdraw, withdraw, deposit, deposit",
                vec![
                    settlement_leg(false, BEBOP_OPERATOR, usdc, uint!(100_U256), 1),
                    settlement_leg(false, BEBOP_OPERATOR, usdc, uint!(200_U256), 2),
                    settlement_leg(true, BEBOP_OPERATOR, EQUITY, uint!(1_U256), 3),
                    settlement_leg(true, BEBOP_OPERATOR, EQUITY, uint!(2_U256), 4),
                ],
                QuarantineReason::ConsecutiveSameKind {
                    operator: BEBOP_OPERATOR,
                },
            ),
            (
                "withdraw, deposit, withdraw",
                vec![
                    settlement_leg(false, BEBOP_OPERATOR, usdc, uint!(100_U256), 1),
                    settlement_leg(true, BEBOP_OPERATOR, EQUITY, uint!(1_U256), 2),
                    settlement_leg(false, BEBOP_OPERATOR, usdc, uint!(200_U256), 3),
                ],
                QuarantineReason::UnequalLegCounts {
                    operator: BEBOP_OPERATOR,
                },
            ),
            (
                "a complete pair plus a lone leg from another operator",
                vec![
                    settlement_leg(false, BEBOP_OPERATOR, usdc, uint!(100_U256), 1),
                    settlement_leg(true, BEBOP_OPERATOR, EQUITY, uint!(1_U256), 2),
                    settlement_leg(true, other, EQUITY, uint!(9_U256), 3),
                ],
                QuarantineReason::UnequalLegCounts { operator: other },
            ),
            (
                "a USDC pair beside an equity pair",
                vec![
                    settlement_leg(false, BEBOP_OPERATOR, usdc, uint!(100_U256), 1),
                    settlement_leg(true, BEBOP_OPERATOR, usdc, uint!(100_U256), 2),
                    settlement_leg(false, BEBOP_OPERATOR, EQUITY, uint!(1_U256), 3),
                    settlement_leg(true, BEBOP_OPERATOR, EQUITY, uint!(1_U256), 4),
                ],
                QuarantineReason::InvalidPairTokens {
                    operator: BEBOP_OPERATOR,
                },
            ),
        ];

        for (shape, legs, expected) in cases {
            let paired = pair_logs(legs);

            assert!(paired.trades.is_empty(), "{shape}: no leg may be hedged");
            assert_eq!(paired.quarantined, vec![expected], "{shape}");
        }

        assert!(logs_contain("operational_alert"));
        assert!(logs_contain("Inventory settlement quarantined"));
        assert!(logs_contain(&format!("{DOUBLE_SETTLEMENT_TX:?}")));
        let rendered = metrics_handle.render();
        for line in [
            "inventory_ambiguous_settlement_total{reason=\"consecutive_same_kind\"} 1",
            "inventory_ambiguous_settlement_total{reason=\"unequal_leg_counts\"} 2",
            "inventory_ambiguous_settlement_total{reason=\"invalid_pair_tokens\"} 1",
        ] {
            assert!(rendered.contains(line), "missing {line:?} in:\n{rendered}");
        }
    }

    /// A tx with one pair keeps today's path whatever its tokens: the
    /// accountant, not pairing, skips a pair the bot cannot hedge.
    #[test]
    #[tracing_test::traced_test]
    fn a_single_pair_is_emitted_without_the_token_check() {
        let usdc = inventory_test_evm_ctx().chain.settlement_stable().address;
        let weth = address!("0x4200000000000000000000000000000000000006");

        for (deposit_token, withdraw_token) in [(usdc, usdc), (EQUITY, weth)] {
            let paired = pair_logs(vec![
                settlement_leg(false, BEBOP_OPERATOR, withdraw_token, uint!(100_U256), 1),
                settlement_leg(true, BEBOP_OPERATOR, deposit_token, uint!(1_U256), 2),
            ]);

            assert_eq!(paired.trades.len(), 1);
            assert!(paired.quarantined.is_empty());
        }
        assert!(!logs_contain("Inventory settlement quarantined"));
    }

    #[test]
    fn a_multi_leg_tx_without_a_distinct_log_index_on_every_leg_is_quarantined() {
        let usdc = inventory_test_evm_ctx().chain.settlement_stable().address;
        let mut missing_index = double_settlement_legs();
        missing_index[1].log_index = None;
        let mut repeated_index = double_settlement_legs();
        repeated_index[2].log_index = Some(10);

        for legs in [missing_index, repeated_index] {
            let paired = pair_logs(legs);

            assert!(paired.trades.is_empty());
            assert_eq!(paired.quarantined, vec![QuarantineReason::UnorderedLegs]);
        }

        let lone_leg_without_index = {
            let mut leg = settlement_leg(false, BEBOP_OPERATOR, usdc, uint!(1_U256), 0);
            leg.log_index = None;
            leg
        };
        let paired = pair_logs(vec![lone_leg_without_index]);
        assert!(paired.trades.is_empty());
        assert!(
            paired.quarantined.is_empty(),
            "a lone leg stays unpaired, not quarantined"
        );
    }

    fn arbitrary_leg() -> impl Strategy<Value = (bool, bool, bool, u64)> {
        (any::<bool>(), any::<bool>(), any::<bool>(), 1_u64..1_000)
    }

    proptest! {
        /// A paired tx uses every leg once, joins legs of one operator only,
        /// and in a multi-pair tx records equity legs that sum to the tx's equity
        /// vault change, whichever legs were joined.
        #[test]
        fn pairing_uses_every_leg_once_and_preserves_net_quantity(
            legs in prop::collection::vec(arbitrary_leg(), 0..8)
        ) {
            let usdc = inventory_test_evm_ctx().chain.settlement_stable().address;
            let operators = [BEBOP_OPERATOR, address!("0x00000000000000000000000000000000000000a7")];
            let logs: Vec<Log> = legs
                .iter()
                .enumerate()
                .map(|(position, (is_deposit, second_operator, is_usdc, amount))| {
                    settlement_leg(
                        *is_deposit,
                        operators[usize::from(*second_operator)],
                        if *is_usdc { usdc } else { EQUITY },
                        U256::from(*amount),
                        u64::try_from(position).unwrap(),
                    )
                })
                .collect();

            let paired = pair_logs(logs.clone());

            if !paired.quarantined.is_empty() || logs.len() < 2 {
                prop_assert!(paired.trades.is_empty());
                return Ok(());
            }

            prop_assert_eq!(paired.trades.len() * 2, logs.len(), "every leg is used once");
            for (trade, _) in &paired.trades {
                prop_assert_eq!(trade.deposit.operator, trade.withdraw.operator);
            }

            if paired.trades.len() > 1 {
                let net_equity = |deposits: bool| -> U256 {
                    legs.iter()
                        .filter(|(is_deposit, _, is_usdc, _)| *is_deposit == deposits && !*is_usdc)
                        .map(|(_, _, _, amount)| U256::from(*amount))
                        .sum()
                };
                let recorded_equity = |deposits: bool| -> U256 {
                    paired
                        .trades
                        .iter()
                        .filter_map(|(trade, _)| {
                            if deposits && trade.deposit.token == EQUITY {
                                Some(trade.deposit.amount)
                            } else if !deposits && trade.withdraw.token == EQUITY {
                                Some(trade.withdraw.amount)
                            } else {
                                None
                            }
                        })
                        .sum()
                };
                prop_assert_eq!(recorded_equity(true), net_equity(true));
                prop_assert_eq!(recorded_equity(false), net_equity(false));
            }
        }
    }
}
