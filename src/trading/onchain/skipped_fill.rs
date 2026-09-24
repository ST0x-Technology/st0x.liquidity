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

use st0x_evm::Chain;

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
    /// Trading is disabled for the symbol on the fill's own chain, so the fill
    /// is kept out of the hedged `Position` and never counter traded. Its
    /// delta is exposure an operator covers by hand.
    TradingDisabled,
}

impl SkipReason {
    fn as_str(self) -> &'static str {
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
/// the exclusion and the page finds it unpaged and pages it.
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

    let unpaged = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM skipped_fills \
         WHERE chain = ? AND tx_hash = ? AND log_index = ? AND reason = ? \
         AND paged_at IS NULL",
        chain,
        tx_hash,
        log_index,
        reason,
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
/// cover: the exposure the operator has left to cover by hand.
pub(crate) async fn uncovered_excluded_fills(
    pool: &SqlitePool,
    chain: Chain,
    symbol: &str,
) -> Result<Vec<UncoveredExcludedFill>, SkippedFillError> {
    let chain = chain.to_string();
    let reason = SkipReason::TradingDisabled.as_str();

    let rows = sqlx::query!(
        r#"SELECT
             json_extract(trade_view.payload, '$.Live.direction') AS "direction!: String",
             json_extract(trade_view.payload, '$.Live.amount') AS "amount!: String"
           FROM skipped_fills AS skipped
           JOIN onchain_trade_view AS trade_view
             ON trade_view.view_id = skipped.chain || ':' || skipped.tx_hash || ':' || skipped.log_index
           WHERE skipped.chain = ? AND skipped.reason = ?
             AND json_extract(trade_view.payload, '$.Live.symbol') = ?
             AND json_extract(trade_view.payload, '$.Live.exclusion') IS NOT NULL
             AND json_extract(trade_view.payload, '$.Live.exclusion.cover') IS NULL"#,
        chain,
        reason,
        symbol,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| UncoveredExcludedFill {
            direction: row.direction,
            amount: row.amount,
        })
        .collect())
}

/// Filters for [`list_skipped_fills`]. Every filter is optional.
#[derive(Debug, Default)]
pub(crate) struct SkippedFillFilter {
    pub(crate) reason: Option<String>,
    pub(crate) chain: Option<String>,
    pub(crate) symbol: Option<String>,
    /// RFC 3339; rows skipped at or after it.
    pub(crate) since: Option<String>,
    /// `Some(false)` lists only excluded fills still waiting for a cover.
    pub(crate) covered: Option<bool>,
    pub(crate) limit: i64,
}

/// A skipped fill with the terms of its `OnChainTrade`, when it was witnessed.
/// Fills skipped before witnessing (an unpriceable or non hedgeable fill) have
/// only the record's own columns.
#[derive(Debug, sqlx::FromRow)]
pub(crate) struct SkippedFillListing {
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
}

/// Skipped fills matching `filter`, newest first.
pub(crate) async fn list_skipped_fills(
    pool: &SqlitePool,
    filter: &SkippedFillFilter,
) -> Result<Vec<SkippedFillListing>, SkippedFillError> {
    let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
        "SELECT skipped.chain, skipped.tx_hash, skipped.log_index, skipped.event_type, \
         skipped.reason, skipped.detail, skipped.skipped_at, skipped.paged_at, \
         json_extract(trade_view.payload, '$.Live.symbol') AS symbol, \
         json_extract(trade_view.payload, '$.Live.direction') AS direction, \
         json_extract(trade_view.payload, '$.Live.amount') AS amount, \
         json_extract(trade_view.payload, '$.Live.price_usdc') AS price_usdc, \
         json_extract(trade_view.payload, '$.Live.block_timestamp') AS block_timestamp, \
         json_extract(trade_view.payload, '$.Live.exclusion.excluded_at') AS excluded_at, \
         json_extract(trade_view.payload, '$.Live.exclusion.cover.price_usdc') AS cover_price_usdc, \
         json_extract(trade_view.payload, '$.Live.exclusion.cover.broker_order_id') \
           AS cover_broker_order_id, \
         json_extract(trade_view.payload, '$.Live.exclusion.cover.covered_at') AS covered_at \
         FROM skipped_fills AS skipped \
         LEFT JOIN onchain_trade_view AS trade_view \
           ON trade_view.view_id = skipped.chain || ':' || skipped.tx_hash || ':' || skipped.log_index \
         WHERE 1 = 1",
    );

    if let Some(reason) = &filter.reason {
        query.push(" AND skipped.reason = ").push_bind(reason);
    }
    if let Some(chain) = &filter.chain {
        query.push(" AND skipped.chain = ").push_bind(chain);
    }
    if let Some(symbol) = &filter.symbol {
        query
            .push(" AND json_extract(trade_view.payload, '$.Live.symbol') = ")
            .push_bind(symbol);
    }
    if let Some(since) = &filter.since {
        query.push(" AND skipped.skipped_at >= ").push_bind(since);
    }
    match filter.covered {
        Some(true) => {
            query.push(
                " AND json_extract(trade_view.payload, '$.Live.exclusion.cover') IS NOT NULL",
            );
        }
        Some(false) => {
            query.push(
                " AND json_extract(trade_view.payload, '$.Live.exclusion') IS NOT NULL \
                 AND json_extract(trade_view.payload, '$.Live.exclusion.cover') IS NULL",
            );
        }
        None => {}
    }
    query
        .push(" ORDER BY skipped.skipped_at DESC, skipped.rowid DESC LIMIT ")
        .push_bind(filter.limit);

    Ok(query.build_query_as().fetch_all(pool).await?)
}

#[cfg(test)]
mod tests {
    use alloy::primitives::b256;

    use super::*;
    use crate::test_utils::setup_test_pools;

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
}
