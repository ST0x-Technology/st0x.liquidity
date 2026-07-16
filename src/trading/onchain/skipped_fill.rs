//! Durable record of on-chain fills the accountant skipped instead of hedging.
//!
//! [`AccountForDexTrade`] swallows unpriceable fills and non-hedgeable pairs so a
//! single anomalous (or crafted) fill cannot open the worker circuit.
//! Persisting them here means a skipped fill survives log rotation and can be
//! reconciled by hand, rather than being visible only in an `error!` line.
//!
//! [`AccountForDexTrade`]: super::trade_accountant::AccountForDexTrade

use alloy::primitives::TxHash;
use chrono::Utc;
use sqlx::SqlitePool;

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
}

impl SkipReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::UnpriceableFill => "unpriceable_fill",
            Self::NonHedgeablePair => "non_hedgeable_pair",
            Self::UnintrospectableToken => "unintrospectable_token",
            Self::InvalidInventoryAmount => "invalid_inventory_amount",
            Self::UnrecognizedInventoryToken => "unrecognized_inventory_token",
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
/// on `(tx_hash, log_index)` -- the fill identity -- so a backfill re-scan that
/// re-processes the same fill records a single row rather than duplicating it on
/// every pass.
pub(crate) async fn record_skipped_fill(
    pool: &SqlitePool,
    tx_hash: TxHash,
    log_index: u64,
    event_type: &str,
    reason: SkipReason,
    detail: &str,
) -> Result<(), SkippedFillError> {
    let tx_hash = tx_hash.to_string();
    let log_index =
        i64::try_from(log_index).map_err(|_| SkippedFillError::LogIndexOutOfRange { log_index })?;
    let reason = reason.as_str();
    let skipped_at = Utc::now().to_rfc3339();

    sqlx::query!(
        "INSERT INTO skipped_fills \
         (tx_hash, log_index, event_type, reason, detail, skipped_at) \
         VALUES (?, ?, ?, ?, ?, ?) \
         ON CONFLICT (tx_hash, log_index) DO NOTHING",
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
    async fn distinct_fills_are_separate_rows() {
        let (pool, _apalis) = setup_test_pools().await;
        let tx_hash = b256!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        record_skipped_fill(
            &pool,
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
