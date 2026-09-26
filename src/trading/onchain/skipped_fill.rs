//! Durable record of on-chain fills the accountant skipped instead of hedging.
//!
//! [`AccountForDexTrade`] swallows unpriceable fills and non-hedgeable pairs so a
//! single anomalous (or crafted) fill cannot trip the conductor-wide fail-stop,
//! and keeps fills on trading disabled assets out of the hedged `Position`.
//! Persisting them here means a skipped fill survives log rotation and can be
//! reconciled by hand, rather than being visible only in an `error!` line.
//!
//! [`AccountForDexTrade`]: super::trade_accountant::AccountForDexTrade

use alloy::primitives::TxHash;
use chrono::Utc;
use sqlx::SqlitePool;

use st0x_event_sorcery::EventSourced;
use st0x_evm::Chain;

use crate::onchain_trade::{OnChainTrade, OnChainTradeEvent};
use crate::position::{Position, PositionEvent};

/// Why the accountant skipped a fill instead of hedging it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkipReason {
    /// Non-zero equity moved at a non-positive USDC/share price.
    UnpriceableFill,
    /// The fill's token pair is not one the bot hedges.
    NonHedgeablePair,
    /// An `InventoryTrade`-supplied token failed symbol/decimals
    /// introspection (non-standard ERC20, no code, or a reverting call).
    UnintrospectableToken,
    /// An `InventoryTrade`-supplied deposit/withdraw amount could not be
    /// converted to a `Float` (a malformed or extreme fixed-decimal value).
    InvalidInventoryAmount,
    /// An `InventoryTrade` leg's token address did not match the configured
    /// canonical address for the symbol its `symbol()` claims to be (a
    /// spoofed or misconfigured token supplied by an `OPERATOR_ROLE` holder).
    UnrecognizedInventoryToken,
    /// The cash leg, truncated to the settlement stable's own grid, still
    /// carried digits the six-decimal internal amount cannot hold.
    UnrepresentableCashAmount,
    /// The fill is excluded from hedging: trading is disabled for the symbol
    /// on the fill's own chain, or the fill landed while it was disabled and is
    /// accounted after it was enabled again. It is kept out of the hedged
    /// `Position` and never counter traded; its delta is exposure an operator
    /// covers by hand.
    TradingDisabled,
}

impl SkipReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::UnpriceableFill => "unpriceable_fill",
            Self::NonHedgeablePair => "non_hedgeable_pair",
            Self::UnintrospectableToken => "unintrospectable_token",
            Self::InvalidInventoryAmount => "invalid_inventory_amount",
            Self::UnrecognizedInventoryToken => "unrecognized_inventory_token",
            Self::UnrepresentableCashAmount => "unrepresentable_cash_amount",
            Self::TradingDisabled => "trading_disabled",
        }
    }
}

/// Failure persisting a skipped fill.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SkippedFillError {
    #[error("log_index {log_index} exceeds i64::MAX")]
    LogIndexOutOfRange { log_index: u64 },
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Persist a fill the accountant skipped, for manual reconciliation. Idempotent
/// on `(chain, tx_hash, log_index)` -- the fill identity -- so a backfill re-scan that
/// re-processes the same fill records a single row rather than duplicating it on
/// every pass.
///
/// The first write wins, except that a `trading_disabled` record supersedes an
/// earlier row for another reason: it is the durable decision not to hedge the
/// fill, which [`trading_disabled_detail`] must find and whose cover side the
/// operator reconciles from. The earlier reason, event type, time and detail
/// are kept in its detail.
pub(crate) async fn record_skipped_fill(
    pool: &SqlitePool,
    chain: Chain,
    tx_hash: TxHash,
    log_index: u64,
    event_type: &str,
    reason: SkipReason,
    detail: &str,
) -> Result<(), SkippedFillError> {
    let chain = chain.to_string();
    let tx_hash = tx_hash.to_string();
    let log_index =
        i64::try_from(log_index).map_err(|_| SkippedFillError::LogIndexOutOfRange { log_index })?;
    let skipped_at = Utc::now().to_rfc3339();
    let is_trading_disabled = reason == SkipReason::TradingDisabled;
    let reason = reason.as_str();

    if is_trading_disabled {
        sqlx::query!(
            "INSERT INTO skipped_fills \
             (chain, tx_hash, log_index, event_type, reason, detail, skipped_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (chain, tx_hash, log_index) DO UPDATE SET \
             event_type = excluded.event_type, \
             reason = excluded.reason, \
             detail = excluded.detail || ' (earlier skipped as ' || skipped_fills.reason \
             || ' on ' || skipped_fills.event_type || ' at ' || skipped_fills.skipped_at \
             || ': ' || skipped_fills.detail || ')', \
             skipped_at = excluded.skipped_at \
             WHERE skipped_fills.reason <> excluded.reason",
            chain,
            tx_hash,
            log_index,
            event_type,
            reason,
            detail,
            skipped_at,
        )
        .execute(pool)
        .await?;

        return Ok(());
    }

    sqlx::query!(
        "INSERT INTO skipped_fills \
         (chain, tx_hash, log_index, event_type, reason, detail, skipped_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT (chain, tx_hash, log_index) DO NOTHING",
        chain,
        tx_hash,
        log_index,
        event_type,
        reason,
        detail,
        skipped_at,
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// The recorded detail when the fill is excluded because trading was
/// disabled on its chain. That record is the durable decision not to hedge
/// the fill: an operator covers its delta by hand from it, so the hedged path
/// must not hedge the same fill when a redrive runs after trading was enabled
/// again, and every surface reporting it repeats the detail and cover side.
pub(crate) async fn trading_disabled_detail(
    pool: &SqlitePool,
    chain: Chain,
    tx_hash: TxHash,
    log_index: u64,
) -> Result<Option<String>, SkippedFillError> {
    let log_index =
        i64::try_from(log_index).map_err(|_| SkippedFillError::LogIndexOutOfRange { log_index })?;

    let chain = chain.to_string();
    let tx_hash = tx_hash.to_string();
    let reason = SkipReason::TradingDisabled.as_str();

    Ok(sqlx::query_scalar!(
        "SELECT detail FROM skipped_fills \
         WHERE chain = ? AND tx_hash = ? AND log_index = ? AND reason = ?",
        chain,
        tx_hash,
        log_index,
        reason,
    )
    .fetch_optional(pool)
    .await?)
}

/// Whether the operational alert for an excluded fill is still owed. Only a
/// `trading_disabled` row is paged, once: a redelivery after a crash between
/// the exclusion and the page finds it unpaged and pages it. A fill whose
/// cover is already recorded (its `ExclusionCovered` event, read from the
/// event log rather than a view) is owed no page: telling the operator to
/// cover it again would double the cover.
pub(crate) async fn excluded_fill_unpaged(
    pool: &SqlitePool,
    chain: Chain,
    tx_hash: TxHash,
    log_index: u64,
) -> Result<bool, SkippedFillError> {
    let log_index =
        i64::try_from(log_index).map_err(|_| SkippedFillError::LogIndexOutOfRange { log_index })?;
    let chain = chain.to_string();
    let tx_hash = tx_hash.to_string();
    let reason = SkipReason::TradingDisabled.as_str();
    let aggregate_type = OnChainTrade::AGGREGATE_TYPE;
    let cover_event_type = OnChainTradeEvent::EXCLUSION_COVERED_EVENT_TYPE;

    let unpaged = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM skipped_fills AS skipped \
         WHERE skipped.chain = ? AND skipped.tx_hash = ? AND skipped.log_index = ? \
         AND skipped.reason = ? AND skipped.paged_at IS NULL \
         AND NOT EXISTS ( \
           SELECT 1 FROM events AS cover_event \
           WHERE cover_event.aggregate_type = ? \
           AND cover_event.aggregate_id = \
             skipped.chain || ':' || skipped.tx_hash || ':' || skipped.log_index \
           AND cover_event.event_type = ?)",
        chain,
        tx_hash,
        log_index,
        reason,
        aggregate_type,
        cover_event_type,
    )
    .fetch_one(pool)
    .await?;

    Ok(unpaged > 0)
}

/// Records that the excluded fill's operational alert was emitted.
pub(crate) async fn mark_excluded_fill_paged(
    pool: &SqlitePool,
    chain: Chain,
    tx_hash: TxHash,
    log_index: u64,
) -> Result<(), SkippedFillError> {
    let log_index =
        i64::try_from(log_index).map_err(|_| SkippedFillError::LogIndexOutOfRange { log_index })?;
    let chain = chain.to_string();
    let tx_hash = tx_hash.to_string();
    let paged_at = Utc::now().to_rfc3339();

    sqlx::query!(
        "UPDATE skipped_fills SET paged_at = ? \
         WHERE chain = ? AND tx_hash = ? AND log_index = ? AND paged_at IS NULL",
        paged_at,
        chain,
        tx_hash,
        log_index,
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// One excluded fill on `symbol` and `chain` whose manual cover is not
/// recorded yet.
pub(crate) struct UncoveredExcludedFill {
    /// The onchain fill's side, as its `OnChainTrade` serializes it.
    pub(crate) direction: String,
    /// The fill's amount, in the canonical decimal form its `OnChainTrade`
    /// serializes.
    pub(crate) amount: String,
}

/// Every excluded fill on `symbol` and `chain` still waiting for its manual
/// cover: the exposure the operator has left to cover by hand. A fill also
/// found in `Position` (concurrent runs classified it both ways) is hedged by
/// the bot, so it is never listed as owed a cover. The fill's terms come from
/// its `Filled` event, never a view.
pub(crate) async fn uncovered_excluded_fills(
    pool: &SqlitePool,
    chain: Chain,
    symbol: &str,
) -> Result<Vec<UncoveredExcludedFill>, SkippedFillError> {
    let chain = chain.to_string();

    let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
        "SELECT json_extract(filled_event.payload, '$.Filled.direction') AS direction, \
         json_extract(filled_event.payload, '$.Filled.amount') AS amount \
         FROM skipped_fills AS skipped",
    );
    push_trade_event_join(
        &mut query,
        "filled_event",
        OnChainTradeEvent::FILLED_EVENT_TYPE,
    );
    push_trade_event_join(
        &mut query,
        "excluded_event",
        OnChainTradeEvent::EXCLUDED_FROM_HEDGING_EVENT_TYPE,
    );
    push_trade_event_join(
        &mut query,
        "cover_event",
        OnChainTradeEvent::EXCLUSION_COVERED_EVENT_TYPE,
    );
    query
        .push(" WHERE skipped.chain = ")
        .push_bind(chain)
        .push(" AND json_extract(filled_event.payload, '$.Filled.symbol') = ")
        .push_bind(symbol.to_owned());
    push_owed_cover(&mut query);
    let rows: Vec<(String, String)> = query.build_query_as().fetch_all(pool).await?;

    Ok(rows
        .into_iter()
        .map(|(direction, amount)| UncoveredExcludedFill { direction, amount })
        .collect())
}

/// Symbols of fills excluded before exclusions were recorded on trades and
/// not adopted yet: a `trading_disabled` record whose trade is acknowledged,
/// has no `ExcludedFromHedging` event and is not in `Position`. These are the
/// fills `adopt_legacy_exclusions` adopts at the next start, read without
/// writing so the deploy gate can see their uncovered exposure first.
pub(crate) async fn unadopted_legacy_exclusion_symbols(
    pool: &SqlitePool,
) -> Result<Vec<String>, SkippedFillError> {
    let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
        "SELECT DISTINCT json_extract(filled_event.payload, '$.Filled.symbol') \
         FROM skipped_fills AS skipped",
    );
    push_trade_event_join(
        &mut query,
        "filled_event",
        OnChainTradeEvent::FILLED_EVENT_TYPE,
    );
    push_trade_event_join(
        &mut query,
        "acknowledged_event",
        OnChainTradeEvent::ACKNOWLEDGED_EVENT_TYPE,
    );
    push_trade_event_join(
        &mut query,
        "excluded_event",
        OnChainTradeEvent::EXCLUDED_FROM_HEDGING_EVENT_TYPE,
    );
    query
        .push(" WHERE skipped.reason = ")
        .push_bind(SkipReason::TradingDisabled.as_str())
        .push(
            " AND filled_event.aggregate_id IS NOT NULL \
             AND acknowledged_event.aggregate_id IS NOT NULL \
             AND excluded_event.aggregate_id IS NULL AND NOT ",
        );
    push_fill_in_position(&mut query);
    let symbols: Vec<(String,)> = query.build_query_as().fetch_all(pool).await?;

    Ok(symbols.into_iter().map(|(symbol,)| symbol).collect())
}

/// Pushes the test that the skipped fill (alias `skipped`, joined to its
/// `filled_event`, `excluded_event` and `cover_event`) is owed a manual cover:
/// its exclusion is recorded, no cover is recorded, and it is not in
/// `Position`. This is exactly what the cover route accepts, so every fill
/// listed or netted as owed can take its cover. A `trading_disabled` row whose
/// marker was interrupted is not owed yet: the unfiltered listing shows it
/// with no `excluded_at`, and the redelivery or a rerun of `process-tx`
/// finishes it. Every part reads durable records, the `trading_disabled` row
/// and the event log, never `onchain_trade_view`: the projection drops a live
/// update it cannot write under contention, and startup `catch_up` compares
/// versions only, so an event skipped in the middle of a trade's stream can
/// stay missing from the view for good.
fn push_owed_cover(query: &mut sqlx::QueryBuilder<sqlx::Sqlite>) {
    query
        .push(" AND skipped.reason = ")
        .push_bind(SkipReason::TradingDisabled.as_str())
        .push(
            " AND excluded_event.aggregate_id IS NOT NULL \
             AND cover_event.aggregate_id IS NULL AND NOT ",
        );
    push_fill_in_position(query);
}

/// Pushes a `LEFT JOIN` of the skipped fill's (alias `skipped`) `OnChainTrade`
/// event of `event_type` as `alias`, found through the event store's primary
/// key. Each of the joined event types occurs at most once per trade.
fn push_trade_event_join(
    query: &mut sqlx::QueryBuilder<sqlx::Sqlite>,
    alias: &'static str,
    event_type: &'static str,
) {
    query
        .push(format!(
            " LEFT JOIN events AS {alias} ON {alias}.aggregate_type = "
        ))
        .push_bind(OnChainTrade::AGGREGATE_TYPE)
        .push(format!(
            " AND {alias}.aggregate_id = \
             skipped.chain || ':' || skipped.tx_hash || ':' || skipped.log_index \
             AND {alias}.event_type = "
        ))
        .push_bind(event_type);
}

/// Pushes an `EXISTS` test that `Position` holds the skipped fill (alias
/// `skipped`, joined to its `filled_event`), bound to the same aggregate and
/// event type as [`crate::conductor::position_fill_already_recorded`]: such a
/// fill is hedged by the bot, so it is never owed a manual cover. The
/// aggregate is the symbol of the fill's `Filled` event; a fill never
/// witnessed has no symbol and so never matches, and it never reached
/// `Position` either.
///
/// It runs once per listed or netted row, so it seeks the fill's hash through
/// `idx_events_position_fill_tx_hash` instead of scanning the symbol's whole
/// `Position` stream. That partial index is only usable when the event type is
/// literal in the SQL text, and the unary `+` keeps the column's TEXT affinity
/// off the comparison, which would otherwise rule the expression index out.
fn push_fill_in_position(query: &mut sqlx::QueryBuilder<sqlx::Sqlite>) {
    query
        .push(
            "EXISTS (SELECT 1 FROM events AS position_event \
             WHERE position_event.aggregate_type = ",
        )
        .push_bind(Position::AGGREGATE_TYPE)
        .push(format!(
            " AND position_event.aggregate_id = \
             json_extract(filled_event.payload, '$.Filled.symbol') \
             AND position_event.event_type = '{}'",
            PositionEvent::ON_CHAIN_ORDER_FILLED_EVENT_TYPE
        ))
        .push(
            " AND json_extract(position_event.payload, '$.OnChainOrderFilled.trade_id.chain') \
               = skipped.chain \
             AND json_extract(position_event.payload, '$.OnChainOrderFilled.trade_id.tx_hash') \
               = +skipped.tx_hash \
             AND CAST(json_extract(position_event.payload, \
               '$.OnChainOrderFilled.trade_id.log_index') AS INTEGER) = skipped.log_index)",
        );
}

/// Filters for [`list_skipped_fills`]. Every filter is optional.
#[derive(Debug, Default)]
pub(crate) struct SkippedFillFilter {
    pub(crate) reason: Option<String>,
    pub(crate) chain: Option<String>,
    pub(crate) symbol: Option<String>,
    /// RFC 3339; rows skipped at or after it.
    pub(crate) since: Option<String>,
    /// `Some(false)` lists only excluded fills still waiting for a cover,
    /// never one also found in `Position`.
    pub(crate) covered: Option<bool>,
    pub(crate) limit: i64,
    /// Keyset cursor: only rows recorded before the row with this id, the
    /// `next_before` of the previous page.
    pub(crate) before: Option<i64>,
}

/// One page of [`list_skipped_fills`].
pub(crate) struct SkippedFillPage {
    pub(crate) rows: Vec<SkippedFillListing>,
    /// The cursor for the next page, when more rows match past this one.
    pub(crate) next_before: Option<i64>,
}

/// A skipped fill with the terms of its `OnChainTrade`, when it was witnessed,
/// read from the trade's `Filled`, `ExcludedFromHedging` and `ExclusionCovered`
/// events rather than a view. Fills skipped before witnessing (an unpriceable
/// or non hedgeable fill) have only the record's own columns.
#[derive(Debug, sqlx::FromRow)]
pub(crate) struct SkippedFillListing {
    /// Insertion order of the record; the pagination cursor.
    pub(crate) id: i64,
    pub(crate) chain: String,
    pub(crate) tx_hash: String,
    pub(crate) log_index: i64,
    pub(crate) event_type: String,
    pub(crate) reason: String,
    pub(crate) detail: String,
    pub(crate) skipped_at: String,
    pub(crate) paged_at: Option<String>,
    pub(crate) symbol: Option<String>,
    pub(crate) direction: Option<String>,
    pub(crate) amount: Option<String>,
    pub(crate) price_usdc: Option<String>,
    pub(crate) block_timestamp: Option<String>,
    pub(crate) excluded_at: Option<String>,
    pub(crate) cover_price_usdc: Option<String>,
    pub(crate) cover_broker_order_id: Option<String>,
    pub(crate) covered_at: Option<String>,
    /// The fill is also in `Position`: concurrent runs classified it both
    /// ways, so the bot hedges it and it must not be covered by hand.
    pub(crate) in_position: bool,
}

/// Skipped fills matching `filter`, most recently recorded first, one page of
/// `limit` rows before the `before` cursor. The order is the record's
/// insertion order, which rows recorded while paging never shift.
pub(crate) async fn list_skipped_fills(
    pool: &SqlitePool,
    filter: &SkippedFillFilter,
) -> Result<SkippedFillPage, SkippedFillError> {
    let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
        "SELECT skipped.rowid AS id, skipped.chain, skipped.tx_hash, skipped.log_index, skipped.event_type, \
         skipped.reason, skipped.detail, skipped.skipped_at, skipped.paged_at, \
         json_extract(filled_event.payload, '$.Filled.symbol') AS symbol, \
         json_extract(filled_event.payload, '$.Filled.direction') AS direction, \
         json_extract(filled_event.payload, '$.Filled.amount') AS amount, \
         json_extract(filled_event.payload, '$.Filled.price_usdc') AS price_usdc, \
         json_extract(filled_event.payload, '$.Filled.block_timestamp') AS block_timestamp, \
         json_extract(excluded_event.payload, '$.ExcludedFromHedging.excluded_at') AS excluded_at, \
         json_extract(cover_event.payload, '$.ExclusionCovered.price_usdc') AS cover_price_usdc, \
         json_extract(cover_event.payload, '$.ExclusionCovered.broker_order_id') \
           AS cover_broker_order_id, \
         json_extract(cover_event.payload, '$.ExclusionCovered.covered_at') AS covered_at, ",
    );
    push_fill_in_position(&mut query);
    query.push(" AS in_position FROM skipped_fills AS skipped");
    push_trade_event_join(
        &mut query,
        "filled_event",
        OnChainTradeEvent::FILLED_EVENT_TYPE,
    );
    push_trade_event_join(
        &mut query,
        "excluded_event",
        OnChainTradeEvent::EXCLUDED_FROM_HEDGING_EVENT_TYPE,
    );
    push_trade_event_join(
        &mut query,
        "cover_event",
        OnChainTradeEvent::EXCLUSION_COVERED_EVENT_TYPE,
    );
    query.push(" WHERE 1 = 1");

    if let Some(reason) = &filter.reason {
        query.push(" AND skipped.reason = ").push_bind(reason);
    }
    if let Some(chain) = &filter.chain {
        query.push(" AND skipped.chain = ").push_bind(chain);
    }
    if let Some(symbol) = &filter.symbol {
        query
            .push(" AND json_extract(filled_event.payload, '$.Filled.symbol') = ")
            .push_bind(symbol);
    }
    if let Some(since) = &filter.since {
        query.push(" AND skipped.skipped_at >= ").push_bind(since);
    }
    match filter.covered {
        Some(true) => {
            query.push(" AND cover_event.aggregate_id IS NOT NULL");
        }
        Some(false) => push_owed_cover(&mut query),
        None => {}
    }
    if let Some(before) = filter.before {
        query.push(" AND skipped.rowid < ").push_bind(before);
    }
    // One row past the page tells whether another page follows.
    query
        .push(" ORDER BY skipped.rowid DESC LIMIT ")
        .push_bind(filter.limit.saturating_add(1));

    let mut rows: Vec<SkippedFillListing> = query.build_query_as().fetch_all(pool).await?;
    let has_more = i64::try_from(rows.len()).is_ok_and(|len| len > filter.limit);
    rows.truncate(usize::try_from(filter.limit).unwrap_or(0));
    let next_before = has_more.then(|| rows.last().map(|row| row.id)).flatten();

    Ok(SkippedFillPage { rows, next_before })
}

#[cfg(test)]
mod tests {
    use alloy::primitives::b256;
    use sqlx::Row;

    use super::*;
    use crate::test_utils::{OnchainTradeBuilder, setup_test_pools};

    struct SkippedRow {
        tx_hash: String,
        log_index: i64,
        event_type: String,
        reason: String,
        detail: String,
    }

    async fn skipped_rows(pool: &SqlitePool) -> Vec<SkippedRow> {
        sqlx::query!(
            "SELECT tx_hash, log_index, event_type, reason, detail FROM skipped_fills \
             ORDER BY log_index"
        )
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|row| SkippedRow {
            tx_hash: row.tx_hash,
            log_index: row.log_index,
            event_type: row.event_type,
            reason: row.reason,
            detail: row.detail,
        })
        .collect()
    }

    #[tokio::test]
    async fn record_persists_the_skipped_fill() {
        let (pool, _apalis) = setup_test_pools().await;
        let tx_hash = b256!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        record_skipped_fill(
            &pool,
            Chain::Base,
            tx_hash,
            7,
            "ClearV3",
            SkipReason::UnpriceableFill,
            "price=0",
        )
        .await
        .unwrap();

        let rows = skipped_rows(&pool).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tx_hash, tx_hash.to_string());
        assert_eq!(rows[0].log_index, 7);
        assert_eq!(rows[0].event_type, "ClearV3");
        assert_eq!(rows[0].reason, "unpriceable_fill");
        assert_eq!(rows[0].detail, "price=0");
    }

    #[tokio::test]
    async fn record_is_idempotent_per_fill_identity() {
        let (pool, _apalis) = setup_test_pools().await;
        let tx_hash = b256!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        record_skipped_fill(
            &pool,
            Chain::Base,
            tx_hash,
            7,
            "ClearV3",
            SkipReason::UnpriceableFill,
            "first",
        )
        .await
        .unwrap();
        // Re-scan of the same (tx_hash, log_index) must not add a second row.
        record_skipped_fill(
            &pool,
            Chain::Base,
            tx_hash,
            7,
            "ClearV3",
            SkipReason::NonHedgeablePair,
            "second",
        )
        .await
        .unwrap();

        let rows = skipped_rows(&pool).await;
        assert_eq!(rows.len(), 1);
        // First write wins under ON CONFLICT DO NOTHING.
        assert_eq!(rows[0].reason, "unpriceable_fill");
        assert_eq!(rows[0].detail, "first");
    }

    #[tokio::test]
    async fn trading_disabled_record_supersedes_an_earlier_skip_and_keeps_it() {
        let (pool, _apalis) = setup_test_pools().await;
        let tx_hash = b256!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        let record = |event_type, reason, detail| {
            record_skipped_fill(&pool, Chain::Base, tx_hash, 7, event_type, reason, detail)
        };

        record(
            "InventoryTrade",
            SkipReason::UnrecognizedInventoryToken,
            "token 0xabc",
        )
        .await
        .unwrap();
        record(
            "process-tx",
            SkipReason::TradingDisabled,
            "cover by BUY 3 COIN",
        )
        .await
        .unwrap();

        let detail = trading_disabled_detail(&pool, Chain::Base, tx_hash, 7)
            .await
            .unwrap()
            .expect("the trading_disabled decision must be retrievable");
        assert!(
            detail.starts_with(
                "cover by BUY 3 COIN (earlier skipped as unrecognized_inventory_token on \
                 InventoryTrade at "
            ) && detail.ends_with(": token 0xabc)"),
            "the detail must lead with the cover side and keep the earlier record: {detail}"
        );

        // A redrive of the exclusion and a later skip leave the decision as is.
        record(
            "process-tx",
            SkipReason::TradingDisabled,
            "cover by BUY 3 COIN",
        )
        .await
        .unwrap();
        record("ClearV3", SkipReason::NonHedgeablePair, "later")
            .await
            .unwrap();

        let rows = skipped_rows(&pool).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].reason, "trading_disabled");
        assert_eq!(rows[0].detail, detail);
    }

    /// Paging with the cursor reaches every older row exactly once, even when
    /// newer fills are recorded between pages.
    #[tokio::test]
    async fn cursor_pages_are_stable_while_fills_are_recorded() {
        let (pool, _apalis) = setup_test_pools().await;
        let tx_hash = b256!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        let record = |log_index| {
            record_skipped_fill(
                &pool,
                Chain::Base,
                tx_hash,
                log_index,
                "ClearV3",
                SkipReason::NonHedgeablePair,
                "x",
            )
        };
        for log_index in 1..=3 {
            record(log_index).await.unwrap();
        }
        let page = |before| SkippedFillFilter {
            limit: 2,
            before,
            ..SkippedFillFilter::default()
        };

        let first = list_skipped_fills(&pool, &page(None)).await.unwrap();
        let first_logs: Vec<_> = first.rows.iter().map(|row| row.log_index).collect();
        assert_eq!(first_logs, vec![3, 2]);

        record(4).await.unwrap();
        let second = list_skipped_fills(&pool, &page(first.next_before))
            .await
            .unwrap();
        let second_logs: Vec<_> = second.rows.iter().map(|row| row.log_index).collect();
        assert_eq!(second_logs, vec![1], "no row repeats and none is skipped");
        assert_eq!(second.next_before, None);
    }

    /// The page tells the operator to recheck with the fill's chain and
    /// symbol, so each filter must keep exactly its own rows: the chain by
    /// the record, the symbol by the fill's `Filled` event (its base symbol),
    /// and `since` by when the fill was recorded.
    #[tokio::test]
    async fn listing_filters_keep_only_their_own_rows() {
        let (pool, _apalis) = setup_test_pools().await;
        let (onchain_trade, _) = st0x_event_sorcery::StoreBuilder::<
            crate::onchain_trade::OnChainTrade,
        >::new(pool.clone())
        .build(())
        .await
        .unwrap();
        let aapl_on_base = OnchainTradeBuilder::new().with_log_index(1).build();
        let mut coin_on_ethereum = OnchainTradeBuilder::new()
            .with_symbol("wtCOIN")
            .with_log_index(2)
            .build();
        coin_on_ethereum.chain = Chain::Ethereum;
        for (trade, skipped_at) in [
            (&aapl_on_base, "2026-09-25T10:00:00+00:00"),
            (&coin_on_ethereum, "2026-09-25T12:00:00+00:00"),
        ] {
            crate::conductor::execute_witness_trade(
                &onchain_trade,
                trade,
                1,
                trade.block_timestamp.unwrap(),
            )
            .await
            .unwrap();
            record_skipped_fill(
                &pool,
                trade.chain,
                trade.tx_hash,
                trade.log_index,
                "ClearV3",
                SkipReason::TradingDisabled,
                "x",
            )
            .await
            .unwrap();
            sqlx::query("UPDATE skipped_fills SET skipped_at = ? WHERE log_index = ?")
                .bind(skipped_at)
                .bind(i64::try_from(trade.log_index).unwrap())
                .execute(&pool)
                .await
                .unwrap();
        }

        let listed = async |filter: SkippedFillFilter| {
            list_skipped_fills(
                &pool,
                &SkippedFillFilter {
                    limit: 10,
                    ..filter
                },
            )
            .await
            .unwrap()
            .rows
            .iter()
            .map(|row| row.log_index)
            .collect::<Vec<_>>()
        };

        assert_eq!(
            listed(SkippedFillFilter {
                chain: Some("base".to_owned()),
                ..SkippedFillFilter::default()
            })
            .await,
            [1]
        );
        assert_eq!(
            listed(SkippedFillFilter {
                symbol: Some("COIN".to_owned()),
                ..SkippedFillFilter::default()
            })
            .await,
            [2]
        );
        assert_eq!(
            listed(SkippedFillFilter {
                since: Some("2026-09-25T11:00:00+00:00".to_owned()),
                ..SkippedFillFilter::default()
            })
            .await,
            [2]
        );
    }

    #[tokio::test]
    async fn distinct_fills_are_separate_rows() {
        let (pool, _apalis) = setup_test_pools().await;
        let tx_hash = b256!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        record_skipped_fill(
            &pool,
            Chain::Base,
            tx_hash,
            7,
            "ClearV3",
            SkipReason::UnpriceableFill,
            "a",
        )
        .await
        .unwrap();
        record_skipped_fill(
            &pool,
            Chain::Base,
            tx_hash,
            8,
            "TakeOrderV3",
            SkipReason::NonHedgeablePair,
            "b",
        )
        .await
        .unwrap();

        let rows = skipped_rows(&pool).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].log_index, 7);
        assert_eq!(rows[1].log_index, 8);
        assert_eq!(rows[1].reason, "non_hedgeable_pair");
    }

    /// The in position check runs once per listed or netted row, so it must
    /// seek `idx_events_position_fill_tx_hash` rather than scan the symbol's
    /// whole `Position` stream. The statement comes from the production
    /// builders, so binding the event type or dropping the `+` that strips the
    /// column's affinity fails here instead of quietly slowing the listing and
    /// the page.
    #[tokio::test]
    async fn in_position_check_seeks_the_fill_hash_index() {
        let (pool, _apalis) = setup_test_pools().await;
        let mut query =
            sqlx::QueryBuilder::<sqlx::Sqlite>::new("SELECT 1 FROM skipped_fills AS skipped");
        push_trade_event_join(
            &mut query,
            "filled_event",
            OnChainTradeEvent::FILLED_EVENT_TYPE,
        );
        query.push(" WHERE ");
        push_fill_in_position(&mut query);

        // `EXPLAIN QUERY PLAN` answers id, parent, notused and detail; only
        // the last is the readable step. Placeholders stay unbound: the
        // planner never sees their values.
        let plan: Vec<String> = sqlx::query(sqlx::AssertSqlSafe(format!(
            "EXPLAIN QUERY PLAN {}",
            query.sql().as_str()
        )))
        .fetch_all(&pool)
        .await
        .unwrap()
        .iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect();

        // Only the table and index names are matched: SQLite's wording
        // between them changes across versions (3.53 adds `EXISTS`).
        assert!(
            plan.iter().any(|step| step.contains("position_event")
                && step.contains("idx_events_position_fill_tx_hash")),
            "{plan:?}"
        );
    }
}
