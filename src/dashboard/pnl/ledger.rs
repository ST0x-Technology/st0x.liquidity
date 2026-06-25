//! Checkpointed ingester maintaining the PnL ledger read model (ADR 0018).
//!
//! [`PnlLedger::catch_up`] reads typed events from the shared log via
//! `st0x-event-sorcery`'s `events_since` (commit/rowid order), extracts the
//! replay's inputs into the append-only `pnl_*` tables, and advances the
//! durable checkpoint in the same transaction as each batch's rows -- so the
//! invariant "every event at or below `last_rowid` that maps to a ledger row
//! has that row committed" survives any crash. Event variants that carry no
//! replay input -- no share movement, no fee, no gas cost (the empty match
//! arms in the `ingest_*` functions) -- map to no row and are absent by
//! design. Correctness never depends on any
//! individual invocation: a missed reactor nudge or a crash mid-batch is
//! repaired by the next call, because ingestion always resumes from the
//! checkpoint. First-deploy backfill and `LEDGER_VERSION` rebuilds are the
//! same code path with the checkpoint at zero.
//!
//! The ledger stores replay INPUTS only -- no matched lots, no PnL. Decimal
//! columns hold the canonical strings the event payloads carry
//! (`format_float`); timestamp columns hold the payloads' chrono-serde
//! RFC3339 strings byte-for-byte, because those strings flow into the /pnl
//! response verbatim.

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use metrics::{counter, gauge};
use rain_math_float::Float;
use sqlx::{Sqlite, SqlitePool, Transaction};
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

use st0x_event_sorcery::{
    EntityList, EventSourced, EventsSinceError, IdempotentReactor, Reactor, Sequenced, deps,
    events_since, head_rowid,
};
use st0x_execution::Direction;
use st0x_float_serde::format_float;

use crate::bot_gas::{BotGasReceiptCost, BotGasReceiptCostEvent};
use crate::position::{Position, PositionEvent};
use crate::tokenized_equity_mint::{TokenizedEquityMint, TokenizedEquityMintEvent};
use crate::usdc_rebalance::{UsdcRebalance, UsdcRebalanceEvent};

/// Bumped when the ledger schema or the event-to-row mapping changes. A
/// mismatch against the persisted `pnl_ledger_checkpoint.ledger_version`
/// truncates every ledger table and resets the checkpoint to zero, making
/// rebuild the same code path as first-deploy backfill.
pub(crate) const LEDGER_VERSION: i64 = 2;

/// Rows fetched per entity per ingest batch. Bounds peak memory during
/// backfill; each batch's rows and checkpoint advance commit atomically, so
/// a mid-backfill crash resumes from the last batch.
const INGEST_BATCH: NonZeroU32 = NonZeroU32::new(1_000).unwrap();

/// Text stored in the ledger's `direction` columns -- the exact values the
/// migration's CHECK constraints admit and the read side parses back.
pub(crate) const DIRECTION_BUY_TEXT: &str = "Buy";
pub(crate) const DIRECTION_SELL_TEXT: &str = "Sell";

/// `pnl_cost_entry.source` discriminators -- the exact values the
/// migration's CHECK admits and the read side parses back.
pub(crate) const TOKENIZATION_FEE_SOURCE: &str = "tokenization_fee";
pub(crate) const CCTP_FEE_SOURCE: &str = "cctp_fee";

/// Event-log head returned by [`PnlLedger::catch_up`]. Holding one is proof
/// the ledger has been caught up through that rowid, which is what makes an
/// `asOfRowid` watermark resolved against it meaningful -- the read path's
/// signatures accept this type rather than a raw `i64` so a stale or
/// computed rowid cannot slip in by accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LedgerHead(pub(crate) i64);

#[derive(Debug, thiserror::Error)]
pub(crate) enum PnlLedgerError {
    #[error("ledger database access failed")]
    Database(#[from] sqlx::Error),
    #[error("typed event stream read failed")]
    Stream(#[from] EventsSinceError),
    #[error("failed to format a financial value for ledger storage")]
    Float(#[from] rain_math_float::FloatError),
    #[error("onchain fill log_index does not fit in i64")]
    LogIndex(#[from] std::num::TryFromIntError),
    #[error("reorged onchain fill {trade_id} has no active PnL ledger row")]
    MissingReorgFill { trade_id: String },
    #[error("reorged onchain fill {trade_id} has {matches} active PnL ledger rows")]
    AmbiguousReorgFill { trade_id: String, matches: usize },
    #[error("reorged onchain fill {trade_id} does not match its active PnL ledger row")]
    ReorgFillMismatch { trade_id: String },
    #[error(transparent)]
    InvalidBotGasCost(#[from] crate::bot_gas::BotGasReceiptCostError),
}

/// Checkpointed ingester over the four PnL source aggregates. One instance
/// per process; concurrent `catch_up` calls serialize on the internal mutex,
/// so overlapping reactor nudges and request-path freshness checks cannot
/// race each other.
pub(crate) struct PnlLedger {
    pool: SqlitePool,
    ingest: Mutex<()>,
    batch_size: NonZeroU32,
}

impl PnlLedger {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            ingest: Mutex::new(()),
            batch_size: INGEST_BATCH,
        }
    }

    #[cfg(test)]
    fn with_batch_size(pool: SqlitePool, batch_size: NonZeroU32) -> Self {
        Self {
            pool,
            ingest: Mutex::new(()),
            batch_size,
        }
    }

    /// Brings the ledger up to the current head of the event log and returns
    /// that head rowid (the value `/pnl` resolves its `asOfRowid` watermark
    /// against). Safe to call from anywhere, any number of times.
    ///
    /// Failures increment `pnl_ledger_catch_up_failures_total`: a
    /// persistently invalid event wedges ingestion by design (the checkpoint
    /// must not advance past it), and through the at-most-once reactor path
    /// that is otherwise visible only as repeated error logs. Paired with the
    /// `pnl_ledger_checkpoint_lag_events` gauge -- which holds the stuck
    /// backlog size when ingestion cannot advance -- an operator can alert on
    /// a wedged ledger without reading logs.
    pub(crate) async fn catch_up(&self) -> Result<LedgerHead, PnlLedgerError> {
        let _serialized = self.ingest.lock().await;
        let result = self.ingest_to_head().await;
        if result.is_err() {
            counter!("pnl_ledger_catch_up_failures_total").increment(1);
        }

        result.map(LedgerHead)
    }

    async fn ingest_to_head(&self) -> Result<i64, PnlLedgerError> {
        self.reconcile_version().await?;

        let head = head_rowid(&self.pool).await?;
        let mut checkpoint = self.checkpoint().await?;
        record_checkpoint_lag(head, checkpoint);
        while checkpoint < head {
            checkpoint = self.ingest_batch(checkpoint, head).await?;
            record_checkpoint_lag(head, checkpoint);
        }

        Ok(head)
    }

    /// Truncates the ledger and resets the checkpoint when the persisted
    /// `ledger_version` does not match [`LEDGER_VERSION`], so the next
    /// batches rebuild every table from the retained event log.
    async fn reconcile_version(&self) -> Result<(), PnlLedgerError> {
        let (_, persisted_version) = self.checkpoint_row().await?;
        if persisted_version == LEDGER_VERSION {
            return Ok(());
        }

        info!(
            persisted_version,
            current_version = LEDGER_VERSION,
            "PnL ledger version changed; truncating for full rebuild"
        );
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM pnl_onchain_reorg")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM pnl_onchain_fill")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM pnl_offchain_fill")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM pnl_offchain_placement")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM pnl_manual_adjustment")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM pnl_cost_entry")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM pnl_bot_gas_cost")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM pnl_mint_symbol")
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "UPDATE pnl_ledger_checkpoint SET last_rowid = 0, ledger_version = ?1 WHERE id = 1",
        )
        .bind(LEDGER_VERSION)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(())
    }

    async fn checkpoint(&self) -> Result<i64, PnlLedgerError> {
        Ok(self.checkpoint_row().await?.0)
    }

    async fn checkpoint_row(&self) -> Result<(i64, i64), PnlLedgerError> {
        Ok(sqlx::query_as(
            "SELECT last_rowid, ledger_version FROM pnl_ledger_checkpoint WHERE id = 1",
        )
        .fetch_one(&self.pool)
        .await?)
    }

    /// Ingests one bounded batch starting strictly after `from` and returns
    /// the new checkpoint.
    ///
    /// The checkpoint invariant requires contiguous coverage: to advance to
    /// X, every source entity's stream must be fully ingested up to X. Each
    /// entity's page is complete up to its own last rowid only if the page
    /// came back shorter than the batch size; a full page may have more rows
    /// behind it. The safe bound is therefore the minimum last-rowid across
    /// FULL pages (rows above it from other entities are deferred to the
    /// next batch); when no page is full, every stream is drained and the
    /// checkpoint jumps to `head`.
    async fn ingest_batch(&self, from: i64, head: i64) -> Result<i64, PnlLedgerError> {
        let positions = events_since::<Position>(&self.pool, from, head, self.batch_size).await?;
        let mints =
            events_since::<TokenizedEquityMint>(&self.pool, from, head, self.batch_size).await?;
        let rebalances =
            events_since::<UsdcRebalance>(&self.pool, from, head, self.batch_size).await?;
        let gas_costs =
            events_since::<BotGasReceiptCost>(&self.pool, from, head, self.batch_size).await?;

        let batch = self.batch_size.get() as usize;
        let bound = [
            full_page_bound(&positions, batch),
            full_page_bound(&mints, batch),
            full_page_bound(&rebalances, batch),
            full_page_bound(&gas_costs, batch),
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(head);

        let mut tx = self.pool.begin().await?;
        for event in positions.into_iter().filter(|event| event.rowid <= bound) {
            ingest_position(&mut tx, event).await?;
        }
        for event in mints.into_iter().filter(|event| event.rowid <= bound) {
            ingest_mint(&mut tx, event).await?;
        }
        for event in rebalances.into_iter().filter(|event| event.rowid <= bound) {
            ingest_rebalance(&mut tx, event).await?;
        }
        for event in gas_costs.into_iter().filter(|event| event.rowid <= bound) {
            ingest_bot_gas(&mut tx, event).await?;
        }
        sqlx::query("UPDATE pnl_ledger_checkpoint SET last_rowid = ?1 WHERE id = 1")
            .bind(bound)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        Ok(bound)
    }
}

/// Doorbell reactor over the four PnL source aggregates: every delivered
/// event triggers a [`PnlLedger::catch_up`], and the delivered payload is
/// deliberately ignored -- it carries no global rowid, and at-most-once
/// reactor delivery cannot be a source of record. The ingester re-reads the
/// durable log from its checkpoint, so a swallowed nudge is repaired by the
/// next one (or by the request path's own catch-up).
pub(crate) struct PnlLedgerReactor {
    ledger: Arc<PnlLedger>,
}

deps!(
    PnlLedgerReactor,
    [
        Position,
        TokenizedEquityMint,
        UsdcRebalance,
        BotGasReceiptCost
    ]
);

impl PnlLedgerReactor {
    pub(crate) fn new(ledger: Arc<PnlLedger>) -> Self {
        Self { ledger }
    }
}

#[async_trait]
impl Reactor for PnlLedgerReactor {
    type Error = PnlLedgerError;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|_symbol, _event| async move { self.ledger.catch_up().await.map(|_head| ()) })
            .on(|_id, _event| async move { self.ledger.catch_up().await.map(|_head| ()) })
            .on(|_id, _event| async move { self.ledger.catch_up().await.map(|_head| ()) })
            .on(|_id, _event| async move { self.ledger.catch_up().await.map(|_head| ()) })
            .exhaustive()
            .await
    }
}

impl IdempotentReactor for PnlLedgerReactor {}

/// Records how many committed events the ledger has not yet ingested. The
/// gauge holds its last value when ingestion errors out, so a wedged
/// checkpoint shows up as a flat nonzero lag alongside a climbing
/// `pnl_ledger_catch_up_failures_total`. A lag beyond `u32::MAX` (only
/// conceivable mid first-deploy backfill) saturates: the signal is
/// "how stuck", not an exact count.
fn record_checkpoint_lag(head: i64, checkpoint: i64) {
    let lag = u32::try_from(head - checkpoint).map_or_else(|_| f64::from(u32::MAX), f64::from);
    gauge!("pnl_ledger_checkpoint_lag_events").set(lag);
}

/// The last rowid of a FULL page (more rows may remain behind it); `None`
/// for a short page, whose stream is fully drained.
fn full_page_bound<Entity: EventSourced>(page: &[Sequenced<Entity>], batch: usize) -> Option<i64> {
    if page.len() == batch {
        page.last().map(|event| event.rowid)
    } else {
        None
    }
}

/// Serializes a timestamp exactly as chrono's serde does for `DateTime<Utc>`
/// in the persisted payloads, so ledger strings reproduce payload strings
/// byte-for-byte (the replay passes them into the response verbatim).
fn canonical_timestamp(at: &DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

fn direction_text(direction: Direction) -> &'static str {
    match direction {
        Direction::Buy => DIRECTION_BUY_TEXT,
        Direction::Sell => DIRECTION_SELL_TEXT,
    }
}

async fn ingest_position(
    tx: &mut Transaction<'_, Sqlite>,
    event: Sequenced<Position>,
) -> Result<(), PnlLedgerError> {
    let Sequenced {
        rowid,
        id: symbol,
        event,
        ..
    } = event;
    match event {
        PositionEvent::OnChainOrderFilled {
            trade_id,
            amount,
            direction,
            price_usdc,
            block_timestamp,
            block_number: _,
            seen_at: _,
        } => {
            sqlx::query(
                "INSERT INTO pnl_onchain_fill \
                 (event_rowid, symbol, chain, tx_hash, log_index, shares, direction, price_usd, \
                  executed_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) \
                 ON CONFLICT(event_rowid) DO NOTHING",
            )
            .bind(rowid)
            .bind(symbol.to_string())
            .bind(trade_id.chain.to_string())
            .bind(trade_id.tx_hash.to_string())
            .bind(i64::try_from(trade_id.log_index)?)
            .bind(format_float(&amount.inner())?)
            .bind(direction_text(direction))
            .bind(format_float(&price_usdc)?)
            .bind(canonical_timestamp(&block_timestamp))
            .execute(&mut **tx)
            .await?;
        }
        PositionEvent::Reorged {
            trade_id,
            amount,
            direction,
            reorged_at,
            ..
        } => {
            let trade_id_text = trade_id.to_string();
            let active_rows: Vec<(i64, String, String)> = sqlx::query_as(
                "SELECT fill.event_rowid, fill.shares, fill.direction \
                 FROM pnl_onchain_fill AS fill \
                 WHERE fill.symbol = ?1 AND fill.tx_hash = ?2 AND fill.log_index = ?3 \
                   AND fill.event_rowid < ?4 \
                   AND NOT EXISTS (\
                       SELECT 1 FROM pnl_onchain_reorg AS reorg \
                       WHERE reorg.original_fill_event_rowid = fill.event_rowid\
                   ) \
                 ORDER BY fill.event_rowid",
            )
            .bind(symbol.to_string())
            .bind(trade_id.tx_hash.to_string())
            .bind(i64::try_from(trade_id.log_index)?)
            .bind(rowid)
            .fetch_all(&mut **tx)
            .await?;

            let [(original_fill_event_rowid, stored_shares, stored_direction)] =
                active_rows.as_slice()
            else {
                return Err(match active_rows.len() {
                    0 => PnlLedgerError::MissingReorgFill {
                        trade_id: trade_id_text,
                    },
                    matches => PnlLedgerError::AmbiguousReorgFill {
                        trade_id: trade_id_text,
                        matches,
                    },
                });
            };

            if stored_shares != &format_float(&amount.inner())?
                || stored_direction != direction_text(direction)
            {
                return Err(PnlLedgerError::ReorgFillMismatch {
                    trade_id: trade_id_text,
                });
            }

            sqlx::query(
                "INSERT INTO pnl_onchain_reorg \
                 (event_rowid, original_fill_event_rowid, reorged_at) \
                 VALUES (?1, ?2, ?3) \
                 ON CONFLICT(event_rowid) DO NOTHING",
            )
            .bind(rowid)
            .bind(original_fill_event_rowid)
            .bind(canonical_timestamp(&reorged_at))
            .execute(&mut **tx)
            .await?;
        }
        PositionEvent::OffChainOrderPlaced {
            offchain_order_id,
            placed_at,
            ..
        } => {
            sqlx::query(
                "INSERT INTO pnl_offchain_placement \
                 (event_rowid, symbol, offchain_order_id, placed_at) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(event_rowid) DO NOTHING",
            )
            .bind(rowid)
            .bind(symbol.to_string())
            .bind(offchain_order_id.to_string())
            .bind(canonical_timestamp(&placed_at))
            .execute(&mut **tx)
            .await?;
        }
        PositionEvent::OffChainOrderFilled {
            offchain_order_id,
            shares_filled,
            direction,
            price,
            broker_timestamp,
            ..
        } => {
            sqlx::query(
                "INSERT INTO pnl_offchain_fill \
                 (event_rowid, symbol, offchain_order_id, shares, direction, price_usd, executed_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
                 ON CONFLICT(event_rowid) DO NOTHING",
            )
            .bind(rowid)
            .bind(symbol.to_string())
            .bind(offchain_order_id.to_string())
            .bind(format_float(&shares_filled.inner().inner())?)
            .bind(direction_text(direction))
            .bind(format_float(&price.inner())?)
            .bind(canonical_timestamp(&broker_timestamp))
            .execute(&mut **tx)
            .await?;
        }
        PositionEvent::ManualPositionAdjusted {
            target_net,
            price_usdc,
            adjusted_at,
            ..
        } => {
            let price = price_usdc.map(|price| format_float(&price)).transpose()?;
            sqlx::query(
                "INSERT INTO pnl_manual_adjustment \
                 (event_rowid, symbol, target_net, price_usd, adjusted_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5) \
                 ON CONFLICT(event_rowid) DO NOTHING",
            )
            .bind(rowid)
            .bind(symbol.to_string())
            .bind(format_float(&target_net.inner())?)
            .bind(price)
            .bind(canonical_timestamp(&adjusted_at))
            .execute(&mut **tx)
            .await?;
        }
        // No share movement, so no replay input (see ADR 0018's input
        // inventory).
        PositionEvent::Initialized { .. }
        | PositionEvent::ThresholdUpdated { .. }
        | PositionEvent::OnChainFillApplied { .. }
        | PositionEvent::OnChainFillSettled { .. }
        | PositionEvent::ReorgSettled { .. }
        | PositionEvent::OffChainOrderFailed { .. }
        | PositionEvent::OffChainOrderCancelled { .. }
        | PositionEvent::ReorgSettled { .. } => {}
    }

    Ok(())
}

async fn ingest_mint(
    tx: &mut Transaction<'_, Sqlite>,
    event: Sequenced<TokenizedEquityMint>,
) -> Result<(), PnlLedgerError> {
    let Sequenced {
        rowid, id, event, ..
    } = event;
    match event {
        // Symbol attribution: the terminal fee event of the same aggregate
        // arrives at a higher rowid, so ingestion order guarantees this row
        // exists before the cost row needs it.
        TokenizedEquityMintEvent::MintRequested { symbol, .. } => {
            sqlx::query(
                "INSERT OR REPLACE INTO pnl_mint_symbol (aggregate_id, symbol) VALUES (?1, ?2)",
            )
            .bind(id.to_string())
            .bind(symbol.to_string())
            .execute(&mut **tx)
            .await?;
        }
        TokenizedEquityMintEvent::TokensReceived {
            fees, received_at, ..
        } => {
            insert_mint_fee(tx, rowid, &id.to_string(), fees, &received_at).await?;
        }
        TokenizedEquityMintEvent::ProviderCompletionRecovered {
            fees, recovered_at, ..
        } => {
            insert_mint_fee(tx, rowid, &id.to_string(), fees, &recovered_at).await?;
        }
        // No fee information.
        TokenizedEquityMintEvent::MintRejected { .. }
        | TokenizedEquityMintEvent::MintAccepted { .. }
        | TokenizedEquityMintEvent::MintAcceptanceFailed { .. }
        | TokenizedEquityMintEvent::MintAuthorizationSigned { .. }
        | TokenizedEquityMintEvent::MintAuthorizationDelivered { .. }
        | TokenizedEquityMintEvent::WrapSubmitted { .. }
        | TokenizedEquityMintEvent::TokensWrapped { .. }
        | TokenizedEquityMintEvent::WrappingFailed { .. }
        | TokenizedEquityMintEvent::VaultDepositSubmitted { .. }
        | TokenizedEquityMintEvent::DepositedIntoRaindex { .. }
        | TokenizedEquityMintEvent::RaindexDepositFailed { .. }
        | TokenizedEquityMintEvent::OperatorReconciled { .. } => {}
    }

    Ok(())
}

/// Writes a tokenization-fee observation. Three outcomes, mirroring
/// `MintCostObservation` in `costs.rs`: reported nonzero fee -> row with an
/// amount; reported zero -> no row; not reported (`None`) -> row with NULL
/// `amount_usd`, which the read side counts into
/// `missing_cost_observation_count` instead of producing a cost entry.
async fn insert_mint_fee(
    tx: &mut Transaction<'_, Sqlite>,
    rowid: i64,
    aggregate_id: &str,
    fees: Option<Float>,
    occurred_at: &DateTime<Utc>,
) -> Result<(), PnlLedgerError> {
    let amount = match fees {
        None => None,
        Some(fees) if fees.is_zero()? => return Ok(()),
        Some(fees) => Some(format_float(&fees)?),
    };
    let symbol: Option<String> =
        sqlx::query_scalar("SELECT symbol FROM pnl_mint_symbol WHERE aggregate_id = ?1")
            .bind(aggregate_id)
            .fetch_optional(&mut **tx)
            .await?;
    // Ingestion order should have written the attribution row from this
    // aggregate's earlier `MintRequested`; a miss breaks that invariant, so
    // the unattributed cost entry must not be silent.
    if symbol.is_none() {
        warn!(
            aggregate_id,
            "PnL ledger mint fee has no symbol attribution row"
        );
    }

    sqlx::query(
        "INSERT INTO pnl_cost_entry \
         (event_rowid, source, aggregate_id, symbol, amount_usd, occurred_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
         ON CONFLICT(event_rowid) DO NOTHING",
    )
    .bind(rowid)
    .bind(TOKENIZATION_FEE_SOURCE)
    .bind(aggregate_id)
    .bind(symbol)
    .bind(amount)
    .bind(canonical_timestamp(occurred_at))
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn ingest_rebalance(
    tx: &mut Transaction<'_, Sqlite>,
    event: Sequenced<UsdcRebalance>,
) -> Result<(), PnlLedgerError> {
    let Sequenced {
        rowid, id, event, ..
    } = event;
    let (fee_collected, occurred_at) = match event {
        UsdcRebalanceEvent::Bridged {
            fee_collected,
            minted_at,
            ..
        } => (fee_collected, minted_at),
        UsdcRebalanceEvent::BridgingCompletionRecovered {
            fee_collected,
            recovered_at,
            ..
        } => (fee_collected, recovered_at),
        // No fee information.
        UsdcRebalanceEvent::Initiated { .. }
        | UsdcRebalanceEvent::ConversionInitiated { .. }
        | UsdcRebalanceEvent::ConversionConfirmed { .. }
        | UsdcRebalanceEvent::ConversionFailed { .. }
        | UsdcRebalanceEvent::WithdrawalSubmitting { .. }
        | UsdcRebalanceEvent::WithdrawalConfirmed { .. }
        | UsdcRebalanceEvent::WithdrawalFailed { .. }
        | UsdcRebalanceEvent::BridgingSubmitting { .. }
        | UsdcRebalanceEvent::BridgingInitiated { .. }
        | UsdcRebalanceEvent::PendingBurnRecorded { .. }
        | UsdcRebalanceEvent::PendingBurnCleared { .. }
        | UsdcRebalanceEvent::AttestationTimedOut { .. }
        | UsdcRebalanceEvent::BridgeAttestationReceived { .. }
        | UsdcRebalanceEvent::BridgingFailed { .. }
        | UsdcRebalanceEvent::DepositInitiated { .. }
        | UsdcRebalanceEvent::DepositConfirmed { .. }
        | UsdcRebalanceEvent::DepositFailed { .. }
        | UsdcRebalanceEvent::OperatorReconciled { .. } => return Ok(()),
    };

    // A zero CCTP fee produces no entry, as in today's replay.
    if fee_collected.inner().is_zero()? {
        return Ok(());
    }

    sqlx::query(
        "INSERT INTO pnl_cost_entry \
         (event_rowid, source, aggregate_id, symbol, amount_usd, occurred_at) \
         VALUES (?1, ?2, ?3, NULL, ?4, ?5) \
         ON CONFLICT(event_rowid) DO NOTHING",
    )
    .bind(rowid)
    .bind(CCTP_FEE_SOURCE)
    .bind(id.to_string())
    .bind(format_float(&fee_collected.inner())?)
    .bind(canonical_timestamp(&occurred_at))
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn ingest_bot_gas(
    tx: &mut Transaction<'_, Sqlite>,
    event: Sequenced<BotGasReceiptCost>,
) -> Result<(), PnlLedgerError> {
    let Sequenced { rowid, event, .. } = event;
    let BotGasReceiptCostEvent::Recorded { cost } = event;
    cost.validate()?;

    sqlx::query(
        "INSERT INTO pnl_bot_gas_cost \
         (event_rowid, chain, tx_hash, usd_cost, operation_category, symbol, occurred_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
         ON CONFLICT(event_rowid) DO NOTHING",
    )
    .bind(rowid)
    .bind(cost.chain.to_string())
    .bind(cost.tx_hash.to_string())
    .bind(format_float(&cost.usd_cost.inner())?)
    .bind(cost.operation_category.to_string())
    .bind(cost.symbol.as_ref().map(ToString::to_string))
    .bind(canonical_timestamp(&cost.occurred_at))
    .execute(&mut **tx)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, TxHash, U256};
    use chrono::TimeZone;
    use serde_json::Value;
    use uuid::Uuid;

    use st0x_config::ExecutionThreshold;
    use st0x_event_sorcery::StoreBuilder;
    use st0x_evm::Chain;
    use st0x_execution::{ExecutorOrderId, SupportedExecutor};
    use st0x_finance::{FractionalShares, Positive, Symbol, Usd, Usdc};
    use st0x_float_macro::float;

    use crate::bot_gas::BotGasOperationCategory;
    use crate::offchain::order::OffchainOrderId;
    use crate::position::{PositionCommand, TradeId, TriggerReason};
    use crate::test_utils::{persist_event, setup_test_db};
    use crate::usdc_rebalance::UsdcRebalanceId;

    use super::*;

    fn timestamp(minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 15, 14, minute, 0).unwrap()
    }

    // Ethereum, not Base: the pnl_onchain_fill.chain column defaults to
    // 'base', so only a non-default chain proves the ingest binds it.
    fn onchain_fill(log_index: u64, minute: u32) -> PositionEvent {
        PositionEvent::OnChainOrderFilled {
            trade_id: TradeId {
                chain: Chain::Ethereum,
                tx_hash: TxHash::repeat_byte(0xab),
                log_index,
            },
            amount: FractionalShares::new(float!(0.5)),
            direction: Direction::Buy,
            price_usdc: float!(150.25),
            block_timestamp: timestamp(minute),
            block_number: None,
            seen_at: timestamp(minute + 1),
        }
    }

    fn reorged_fill(log_index: u64, minute: u32) -> PositionEvent {
        PositionEvent::Reorged {
            trade_id: TradeId {
                tx_hash: TxHash::repeat_byte(0xab),
                log_index,
            },
            amount: FractionalShares::new(float!(0.5)),
            direction: Direction::Buy,
            reorg_depth: 1,
            reorged_at: timestamp(minute),
        }
    }

    fn gas_cost() -> crate::bot_gas::BotGasReceiptCost {
        crate::bot_gas::BotGasReceiptCost {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0xcd),
            receipt_from: Address::repeat_byte(0x11),
            gas_used: 21_000,
            effective_gas_price_wei: 1_000_000_000,
            native_cost_wei: U256::from(21_000_000_000_000_u128),
            eth_usd_price: Usd::new(float!(2000)),
            eth_usd_price_source: "eth_usd_valuation_feed".to_owned(),
            eth_usd_price_at: timestamp(0),
            eth_usd_price_block_number: Some(123),
            usd_cost: Usd::new(float!(0.042)),
            operation_category: BotGasOperationCategory::VaultDeposit,
            symbol: Some(Symbol::new("RKLB").unwrap()),
            occurred_at: timestamp(1),
        }
    }

    /// `AssertSqlSafe`: every call site passes a literal ledger table name,
    /// audited by eye; test-only.
    async fn count(pool: &SqlitePool, table: &'static str) -> i64 {
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn ingests_every_row_kind_and_advances_checkpoint() {
        let pool = setup_test_db().await;
        let order_id = OffchainOrderId::new();
        let mint_id = Uuid::new_v4().to_string();
        let rebalance_id = UsdcRebalanceId(Uuid::new_v4()).to_string();
        let gas = gas_cost();
        let gas_id = format!("base:{}", gas.tx_hash);

        let fill = onchain_fill(7, 0);
        persist_event::<Position>(&pool, "AAPL", 1, &fill).await;
        persist_event::<Position>(
            &pool,
            "AAPL",
            2,
            &PositionEvent::OffChainOrderPlaced {
                offchain_order_id: order_id,
                shares: Positive::new(FractionalShares::new(float!(0.5))).unwrap(),
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                trigger_reason: TriggerReason::SharesThreshold {
                    net_position_shares: float!(0.5),
                    threshold_shares: float!(0.25),
                },
                placed_at: timestamp(2),
            },
        )
        .await;
        persist_event::<Position>(
            &pool,
            "AAPL",
            3,
            &PositionEvent::OffChainOrderFilled {
                offchain_order_id: order_id,
                shares_filled: Positive::new(FractionalShares::new(float!(0.5))).unwrap(),
                direction: Direction::Sell,
                executor_order_id: ExecutorOrderId::new("broker-1"),
                price: Usd::new(float!(151.5)),
                broker_timestamp: timestamp(3),
            },
        )
        .await;
        persist_event::<Position>(
            &pool,
            "AAPL",
            4,
            &PositionEvent::ManualPositionAdjusted {
                previous_net: FractionalShares::new(float!(0)),
                target_net: FractionalShares::new(float!(2)),
                reason: "test".to_string(),
                price_usdc: None,
                adjusted_at: timestamp(4),
            },
        )
        .await;
        persist_event::<TokenizedEquityMint>(
            &pool,
            &mint_id,
            1,
            &TokenizedEquityMintEvent::MintRequested {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: float!(1),
                wallet: Address::repeat_byte(0x22),
                requested_at: timestamp(5),
            },
        )
        .await;
        persist_event::<TokenizedEquityMint>(
            &pool,
            &mint_id,
            2,
            &TokenizedEquityMintEvent::TokensReceived {
                tx_hash: TxHash::repeat_byte(0xee),
                shares_minted: U256::from(1_000_000_000_000_000_000_u128),
                fees: Some(float!(0.25)),
                received_at: timestamp(6),
            },
        )
        .await;
        persist_event::<UsdcRebalance>(
            &pool,
            &rebalance_id,
            1,
            &UsdcRebalanceEvent::Bridged {
                mint_tx_hash: TxHash::repeat_byte(0xff),
                amount_received: Usdc::new(float!(998.5)),
                fee_collected: Usdc::new(float!(1.5)),
                minted_at: timestamp(7),
            },
        )
        .await;
        persist_event::<BotGasReceiptCost>(
            &pool,
            &gas_id,
            1,
            &BotGasReceiptCostEvent::Recorded { cost: gas.clone() },
        )
        .await;

        let ledger = PnlLedger::new(pool.clone());
        let LedgerHead(head) = ledger.catch_up().await.unwrap();

        assert_eq!(head, head_rowid(&pool).await.unwrap());
        assert_eq!(ledger.checkpoint().await.unwrap(), head);

        let (symbol, chain, shares, direction, price, executed_at): (
            String,
            String,
            String,
            String,
            String,
            String,
        ) = sqlx::query_as(
            "SELECT symbol, chain, shares, direction, price_usd, executed_at \
             FROM pnl_onchain_fill",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(symbol, "AAPL");
        assert_eq!(chain, "ethereum");
        assert_eq!(shares, "0.5");
        assert_eq!(direction, "Buy");
        assert_eq!(price, "150.25");
        // Byte-identity with the persisted payload: the ledger's timestamp
        // must reproduce exactly what chrono's serde wrote into the event.
        let payload = serde_json::to_value(&fill).unwrap();
        assert_eq!(
            Value::String(executed_at),
            payload["OnChainOrderFilled"]["block_timestamp"]
        );

        assert_eq!(count(&pool, "pnl_offchain_fill").await, 1);
        assert_eq!(count(&pool, "pnl_offchain_placement").await, 1);
        let (target_net, adjustment_price): (String, Option<String>) =
            sqlx::query_as("SELECT target_net, price_usd FROM pnl_manual_adjustment")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(target_net, "2");
        assert_eq!(adjustment_price, None);

        let cost_rows: Vec<(String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT source, symbol, amount_usd FROM pnl_cost_entry ORDER BY event_rowid",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            cost_rows,
            vec![
                (
                    "tokenization_fee".to_string(),
                    Some("AAPL".to_string()),
                    Some("0.25".to_string())
                ),
                ("cctp_fee".to_string(), None, Some("1.5".to_string())),
            ]
        );

        let (chain, usd_cost): (String, String) =
            sqlx::query_as("SELECT chain, usd_cost FROM pnl_bot_gas_cost")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(chain, "base");
        assert_eq!(usd_cost, "0.042");
    }

    #[tokio::test]
    async fn catch_up_is_idempotent() {
        let pool = setup_test_db().await;
        persist_event::<Position>(&pool, "AAPL", 1, &onchain_fill(1, 0)).await;

        let ledger = PnlLedger::new(pool.clone());
        let first_head = ledger.catch_up().await.unwrap();
        let second_head = ledger.catch_up().await.unwrap();

        assert_eq!(first_head, second_head);
        assert_eq!(count(&pool, "pnl_onchain_fill").await, 1);
    }

    #[tokio::test]
    async fn reorg_keeps_the_original_fill_and_appends_one_idempotent_marker() {
        let pool = setup_test_db().await;
        persist_event::<Position>(&pool, "AAPL", 1, &onchain_fill(7, 0)).await;
        persist_event::<Position>(&pool, "AAPL", 2, &reorged_fill(7, 2)).await;

        let ledger = PnlLedger::new(pool.clone());
        let first_head = ledger.catch_up().await.unwrap();
        let second_head = ledger.catch_up().await.unwrap();

        assert_eq!(first_head, second_head);
        assert_eq!(count(&pool, "pnl_onchain_fill").await, 1);
        assert_eq!(count(&pool, "pnl_onchain_reorg").await, 1);

        let (fill_rowid, reorg_rowid, original_fill_rowid): (i64, i64, i64) = sqlx::query_as(
            "SELECT fill.event_rowid, reorg.event_rowid, reorg.original_fill_event_rowid \
             FROM pnl_onchain_fill AS fill \
             JOIN pnl_onchain_reorg AS reorg \
               ON reorg.original_fill_event_rowid = fill.event_rowid",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(original_fill_rowid, fill_rowid);
        assert!(reorg_rowid > fill_rowid);
        assert_eq!(ledger.checkpoint().await.unwrap(), reorg_rowid);
    }

    #[tokio::test]
    async fn reorg_with_mismatched_amount_fails_without_advancing_checkpoint() {
        let pool = setup_test_db().await;
        persist_event::<Position>(&pool, "AAPL", 1, &onchain_fill(7, 0)).await;
        let mut reorg = reorged_fill(7, 2);
        let PositionEvent::Reorged { amount, .. } = &mut reorg else {
            unreachable!();
        };
        *amount = FractionalShares::new(float!(0.75));
        persist_event::<Position>(&pool, "AAPL", 2, &reorg).await;

        let ledger = PnlLedger::new(pool.clone());
        let error = ledger.catch_up().await.unwrap_err();

        assert!(matches!(error, PnlLedgerError::ReorgFillMismatch { .. }));
        assert_eq!(ledger.checkpoint().await.unwrap(), 0);
        assert_eq!(count(&pool, "pnl_onchain_fill").await, 0);
        assert_eq!(count(&pool, "pnl_onchain_reorg").await, 0);
    }

    #[tokio::test]
    async fn ambiguous_reorg_target_fails_without_advancing_checkpoint() {
        let pool = setup_test_db().await;
        persist_event::<Position>(&pool, "AAPL", 1, &onchain_fill(7, 0)).await;
        persist_event::<Position>(&pool, "AAPL", 2, &onchain_fill(7, 1)).await;
        persist_event::<Position>(&pool, "AAPL", 3, &reorged_fill(7, 2)).await;

        let ledger = PnlLedger::new(pool.clone());
        let error = ledger.catch_up().await.unwrap_err();

        assert!(matches!(
            error,
            PnlLedgerError::AmbiguousReorgFill { matches: 2, .. }
        ));
        assert_eq!(ledger.checkpoint().await.unwrap(), 0);
        assert_eq!(count(&pool, "pnl_onchain_fill").await, 0);
        assert_eq!(count(&pool, "pnl_onchain_reorg").await, 0);
    }

    /// Interleaved multi-entity history ingested with a batch size smaller
    /// than any single stream: the min-full-page bound must defer rows past
    /// it and resume, ending with every event ingested exactly once.
    #[tokio::test]
    async fn small_batches_page_through_mixed_streams() {
        let pool = setup_test_db().await;
        let mint_id = Uuid::new_v4().to_string();
        persist_event::<TokenizedEquityMint>(
            &pool,
            &mint_id,
            1,
            &TokenizedEquityMintEvent::MintRequested {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: float!(1),
                wallet: Address::repeat_byte(0x22),
                requested_at: timestamp(0),
            },
        )
        .await;
        for sequence in 1..=5 {
            persist_event::<Position>(
                &pool,
                "AAPL",
                sequence,
                &onchain_fill(u64::try_from(sequence).unwrap(), 0),
            )
            .await;
        }
        persist_event::<TokenizedEquityMint>(
            &pool,
            &mint_id,
            2,
            &TokenizedEquityMintEvent::TokensReceived {
                tx_hash: TxHash::repeat_byte(0xee),
                shares_minted: U256::from(1_u8),
                fees: Some(float!(0.25)),
                received_at: timestamp(6),
            },
        )
        .await;

        let ledger = PnlLedger::with_batch_size(pool.clone(), NonZeroU32::new(2).unwrap());
        let LedgerHead(head) = ledger.catch_up().await.unwrap();

        assert_eq!(ledger.checkpoint().await.unwrap(), head);
        assert_eq!(count(&pool, "pnl_onchain_fill").await, 5);
        let symbol: Option<String> = sqlx::query_scalar(
            "SELECT symbol FROM pnl_cost_entry WHERE source = 'tokenization_fee'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(symbol, Some("AAPL".to_string()));
    }

    #[tokio::test]
    async fn version_mismatch_truncates_and_rebuilds() {
        let pool = setup_test_db().await;
        persist_event::<Position>(&pool, "AAPL", 1, &onchain_fill(1, 0)).await;

        let ledger = PnlLedger::new(pool.clone());
        let LedgerHead(head) = ledger.catch_up().await.unwrap();
        sqlx::query("UPDATE pnl_ledger_checkpoint SET ledger_version = 99 WHERE id = 1")
            .execute(&pool)
            .await
            .unwrap();

        let LedgerHead(rebuilt_head) = ledger.catch_up().await.unwrap();

        assert_eq!(rebuilt_head, head);
        assert_eq!(count(&pool, "pnl_onchain_fill").await, 1);
        let (last_rowid, version): (i64, i64) =
            sqlx::query_as("SELECT last_rowid, ledger_version FROM pnl_ledger_checkpoint")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(last_rowid, head);
        assert_eq!(version, LEDGER_VERSION);
    }

    #[tokio::test]
    async fn unreported_fees_persist_a_null_row_and_zero_fees_persist_nothing() {
        let pool = setup_test_db().await;
        let unreported_mint = Uuid::new_v4().to_string();
        let zero_fee_mint = Uuid::new_v4().to_string();
        persist_event::<TokenizedEquityMint>(
            &pool,
            &unreported_mint,
            1,
            &TokenizedEquityMintEvent::TokensReceived {
                tx_hash: TxHash::repeat_byte(0xee),
                shares_minted: U256::from(1_u8),
                fees: None,
                received_at: timestamp(1),
            },
        )
        .await;
        persist_event::<TokenizedEquityMint>(
            &pool,
            &zero_fee_mint,
            1,
            &TokenizedEquityMintEvent::TokensReceived {
                tx_hash: TxHash::repeat_byte(0xef),
                shares_minted: U256::from(1_u8),
                fees: Some(float!(0)),
                received_at: timestamp(2),
            },
        )
        .await;

        PnlLedger::new(pool.clone()).catch_up().await.unwrap();

        let rows: Vec<(String, Option<String>)> =
            sqlx::query_as("SELECT aggregate_id, amount_usd FROM pnl_cost_entry")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(rows, vec![(unreported_mint, None)]);
    }

    /// Two fills with the SAME trade id are distinct rows: dedup and its
    /// audit warnings are replay semantics, not storage semantics.
    #[tokio::test]
    async fn duplicate_business_events_are_distinct_rows() {
        let pool = setup_test_db().await;
        persist_event::<Position>(&pool, "AAPL", 1, &onchain_fill(7, 0)).await;
        persist_event::<Position>(&pool, "AAPL", 2, &onchain_fill(7, 1)).await;

        PnlLedger::new(pool.clone()).catch_up().await.unwrap();

        assert_eq!(count(&pool, "pnl_onchain_fill").await, 2);
    }

    /// Drives a command through a store with the reactor REGISTERED, so the
    /// doorbell wiring itself (the `deps!` arms dispatching into `catch_up`)
    /// performs the ingestion -- no direct `catch_up` call. A broken `.on`
    /// arm or a swallowed reactor error fails this test where the
    /// direct-call tests stay green.
    #[tokio::test]
    async fn registered_reactor_ingests_committed_events_into_ledger_rows() {
        let pool = setup_test_db().await;
        let ledger = Arc::new(PnlLedger::new(pool.clone()));
        let (store, _projection) = StoreBuilder::<Position>::new(pool.clone())
            .with(Arc::new(PnlLedgerReactor::new(ledger)))
            .build(())
            .await
            .unwrap();

        let symbol = Symbol::new("AAPL").unwrap();
        store
            .send(
                &symbol,
                PositionCommand::AcknowledgeOnChainFill {
                    symbol: symbol.clone(),
                    threshold: ExecutionThreshold::whole_share(),
                    trade_id: TradeId {
                        chain: Chain::Base,
                        tx_hash: TxHash::repeat_byte(0xab),
                        log_index: 7,
                    },
                    amount: FractionalShares::new(float!(2)),
                    direction: Direction::Buy,
                    price_usdc: float!(150),
                    block_timestamp: timestamp(0),
                    block_number: None,
                },
            )
            .await
            .unwrap();

        let row: (String, String, String, String, String) = sqlx::query_as(
            "SELECT symbol, tx_hash, shares, direction, price_usd FROM pnl_onchain_fill",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            row,
            (
                "AAPL".to_owned(),
                TxHash::repeat_byte(0xab).to_string(),
                "2".to_owned(),
                "Buy".to_owned(),
                "150".to_owned(),
            )
        );
    }

    /// A row the schema rejects must fail ingestion loudly and pin the
    /// checkpoint -- never be skipped with the checkpoint advancing past it,
    /// which would permanently drop a committed event from the ledger. No
    /// typed event can violate today's schema, so the test simulates future
    /// ingester/schema drift by rebuilding the fill table with a CHECK no
    /// real direction satisfies before ingesting a genuine event. This is
    /// what `ON CONFLICT(event_rowid) DO NOTHING` buys over
    /// `INSERT OR IGNORE`: only duplicate rowids no-op; constraint
    /// violations abort the batch.
    #[tokio::test]
    async fn constraint_violating_row_fails_ingestion_without_advancing_checkpoint() {
        let pool = setup_test_db().await;
        sqlx::query("DROP TABLE pnl_onchain_fill")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE pnl_onchain_fill (
                 event_rowid INTEGER PRIMARY KEY,
                 symbol TEXT NOT NULL,
                 tx_hash TEXT NOT NULL,
                 log_index INTEGER NOT NULL,
                 shares TEXT NOT NULL,
                 direction TEXT NOT NULL CHECK (direction IN ('Neither')),
                 price_usd TEXT NOT NULL,
                 executed_at TEXT NOT NULL
             ) STRICT",
        )
        .execute(&pool)
        .await
        .unwrap();
        persist_event::<Position>(&pool, "AAPL", 1, &onchain_fill(7, 0)).await;

        let ledger = PnlLedger::new(pool.clone());
        let error = ledger.catch_up().await.unwrap_err();

        assert!(matches!(error, PnlLedgerError::Database(_)));
        assert_eq!(ledger.checkpoint().await.unwrap(), 0);
        assert_eq!(count(&pool, "pnl_onchain_fill").await, 0);
    }

    /// An invalid persisted gas cost fails ingestion closed: the batch rolls
    /// back and the checkpoint does not advance, so the failure is loud and
    /// re-hit until fixed rather than silently skipped.
    #[tokio::test]
    async fn invalid_bot_gas_cost_wedges_ingestion_without_advancing_checkpoint() {
        let pool = setup_test_db().await;
        let mut cost = gas_cost();
        cost.usd_cost = Usd::new(float!(0));
        let gas_id = format!("base:{}", cost.tx_hash);
        persist_event::<BotGasReceiptCost>(
            &pool,
            &gas_id,
            1,
            &BotGasReceiptCostEvent::Recorded { cost },
        )
        .await;

        let ledger = PnlLedger::new(pool.clone());
        let error = ledger.catch_up().await.unwrap_err();

        assert!(matches!(error, PnlLedgerError::InvalidBotGasCost(_)));
        assert_eq!(ledger.checkpoint().await.unwrap(), 0);
        assert_eq!(count(&pool, "pnl_bot_gas_cost").await, 0);
    }
}
