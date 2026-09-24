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

    if reason == SkipReason::TradingDisabled {
        sqlx::query(
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
        )
        .bind(chain)
        .bind(tx_hash)
        .bind(log_index)
        .bind(event_type)
        .bind(reason.as_str())
        .bind(detail)
        .bind(skipped_at)
        .execute(pool)
        .await?;

        return Ok(());
    }

    let reason = reason.as_str();
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

    let detail: Option<(String,)> = sqlx::query_as(
        "SELECT detail FROM skipped_fills \
         WHERE chain = ? AND tx_hash = ? AND log_index = ? AND reason = ?",
    )
    .bind(chain.to_string())
    .bind(tx_hash.to_string())
    .bind(log_index)
    .bind(SkipReason::TradingDisabled.as_str())
    .fetch_optional(pool)
    .await?;

    Ok(detail.map(|(detail,)| detail))
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
