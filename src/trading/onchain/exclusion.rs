//! Which fills are excluded from hedging, and the trading enablement history
//! that decides it for fills accounted after an asset is enabled again.
//!
//! A fill is excluded when trading is disabled for its symbol on its own
//! chain, or when it landed before the restart that enabled trading for that
//! symbol and chain: it landed while the asset was disabled, so it stays out of
//! the hedged `Position` even if it is accounted afterwards (still queued, not
//! yet backfilled, or landing during the restart itself).

use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use tracing::info;

use st0x_config::{ChainAssets, ChainRegistry};
use st0x_evm::Chain;
use st0x_execution::Symbol;

use crate::onchain::OnchainTrade;

/// Why a fill is kept out of the hedged `Position`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExclusionCause {
    /// Trading is disabled for the symbol on the fill's chain.
    TradingDisabled,
    /// Trading is enabled, but the fill landed before the restart that
    /// enabled it, while the asset was still disabled.
    LandedBeforeEnabled { enabled_since: DateTime<Utc> },
}

impl std::fmt::Display for ExclusionCause {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TradingDisabled => write!(formatter, "trading is disabled"),
            Self::LandedBeforeEnabled { enabled_since } => write!(
                formatter,
                "the fill landed while trading was disabled, before it was enabled at {}",
                enabled_since.to_rfc3339()
            ),
        }
    }
}

/// Whether `trade` is excluded from hedging, per its own chain's `assets`.
///
/// # Errors
///
/// Fails when the trading enablement history cannot be read.
pub async fn exclusion_cause(
    pool: &SqlitePool,
    assets: &ChainAssets,
    trade: &OnchainTrade,
) -> Result<Option<ExclusionCause>, sqlx::Error> {
    if !assets.is_trading_enabled(trade.symbol()) {
        return Ok(Some(ExclusionCause::TradingDisabled));
    }

    let Some(block_timestamp) = trade.block_timestamp else {
        // Witnessing rejects a fill without a block timestamp, so it never
        // gets far enough for the cutoff to matter.
        return Ok(None);
    };

    let enabled_since = trading_enabled_since(pool, trade.chain, trade.symbol()).await?;
    Ok(enabled_since
        .filter(|enabled_since| block_timestamp < *enabled_since)
        .map(|enabled_since| ExclusionCause::LandedBeforeEnabled { enabled_since }))
}

/// The restart that observed trading for `symbol` on `chain` go from disabled
/// to enabled, if it is enabled now and such a transition was observed.
async fn trading_enabled_since(
    pool: &SqlitePool,
    chain: Chain,
    symbol: &Symbol,
) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    let chain = chain.to_string();
    let symbol = symbol.to_string();
    let enabled_since = sqlx::query_scalar!(
        "SELECT enabled_since FROM trading_enablement \
         WHERE chain = ? AND symbol = ? AND trading_enabled = 1",
        chain,
        symbol,
    )
    .fetch_optional(pool)
    .await?
    .flatten();

    Ok(enabled_since
        .as_deref()
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|enabled_since| enabled_since.with_timezone(&Utc)))
}

/// Records every hedged chain's configured trading flags as observed by this
/// restart at `now`, before any fill is accounted.
///
/// An asset seen going from disabled to enabled gets `enabled_since = now`,
/// the cutoff [`exclusion_cause`] applies to fills that landed before it. An
/// asset first seen enabled has no cutoff, since no disabled period is known.
/// Disabling clears the cutoff; the next enable sets a new one.
///
/// # Errors
///
/// Fails when the enablement table cannot be read or written.
pub(crate) async fn record_trading_enablement(
    pool: &SqlitePool,
    chains: &ChainRegistry,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let now_text = now.to_rfc3339();

    for hedged in chains.hedged() {
        let chain = hedged.chain.to_string();
        for symbol in hedged.assets.equities.symbols.keys() {
            let enabled = hedged.assets.is_trading_enabled(symbol);
            let symbol_text = symbol.to_string();

            let previous = sqlx::query_scalar!(
                "SELECT trading_enabled FROM trading_enablement WHERE chain = ? AND symbol = ?",
                chain,
                symbol_text,
            )
            .fetch_optional(pool)
            .await?;

            let enabled_flag = i64::from(enabled);
            if previous == Some(enabled_flag) {
                continue;
            }

            if previous == Some(0) && enabled {
                info!(
                    %chain,
                    %symbol,
                    enabled_since = %now_text,
                    "Trading enabled again; fills that landed before now stay excluded from hedging"
                );
                sqlx::query!(
                    "UPDATE trading_enablement \
                     SET trading_enabled = 1, enabled_since = ?, observed_at = ? \
                     WHERE chain = ? AND symbol = ?",
                    now_text,
                    now_text,
                    chain,
                    symbol_text,
                )
                .execute(pool)
                .await?;
                continue;
            }

            // First observation, or enabled to disabled: no cutoff.
            sqlx::query!(
                "INSERT INTO trading_enablement \
                 (chain, symbol, trading_enabled, enabled_since, observed_at) \
                 VALUES (?, ?, ?, NULL, ?) \
                 ON CONFLICT (chain, symbol) DO UPDATE SET \
                 trading_enabled = excluded.trading_enabled, \
                 enabled_since = NULL, \
                 observed_at = excluded.observed_at",
                chain,
                symbol_text,
                enabled_flag,
                now_text,
            )
            .execute(pool)
            .await?;
        }

        // A symbol dropped from the chain's config is trading disabled (the
        // flag fails closed), so re-adding it later is a new enable.
        let recorded_enabled = sqlx::query_scalar!(
            "SELECT symbol FROM trading_enablement WHERE chain = ? AND trading_enabled = 1",
            chain,
        )
        .fetch_all(pool)
        .await?;
        for symbol_text in recorded_enabled {
            let still_configured = hedged
                .assets
                .equities
                .symbols
                .keys()
                .any(|symbol| symbol.to_string() == symbol_text);
            if still_configured {
                continue;
            }

            sqlx::query!(
                "UPDATE trading_enablement \
                 SET trading_enabled = 0, enabled_since = NULL, observed_at = ? \
                 WHERE chain = ? AND symbol = ?",
                now_text,
                chain,
                symbol_text,
            )
            .execute(pool)
            .await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;
    use chrono::Duration;
    use std::collections::HashMap;

    use st0x_config::{ChainEquityAsset, OperationMode, create_test_ctx_with_order_owner};

    use super::*;
    use crate::test_utils::{OnchainTradeBuilder, setup_test_pools};

    fn aapl(trading: OperationMode) -> ChainEquityAsset {
        ChainEquityAsset {
            tokenized_equity: Address::ZERO,
            tokenized_equity_derivative: Address::ZERO,
            vault_ids: vec![],
            trading,
            rebalancing: OperationMode::Disabled,
            wrapped_equity_recovery: OperationMode::Disabled,
            operational_limit: None,
            target_share: None,
        }
    }

    /// A registry whose primary chain lists AAPL with `trading`, or not at all.
    fn chains_with(trading: Option<OperationMode>) -> ChainRegistry {
        let mut ctx = create_test_ctx_with_order_owner(Address::ZERO);
        ctx.chains.primary_mut().assets.equities.symbols = trading
            .map(|trading| HashMap::from([(Symbol::new("AAPL").unwrap(), aapl(trading))]))
            .unwrap_or_default();
        ctx.chains
    }

    fn fill_at(block_timestamp: DateTime<Utc>) -> OnchainTrade {
        OnchainTradeBuilder::new()
            .with_block_timestamp(Some(block_timestamp))
            .build()
    }

    async fn cause_for(
        pool: &SqlitePool,
        chains: &ChainRegistry,
        block_timestamp: DateTime<Utc>,
    ) -> Option<ExclusionCause> {
        exclusion_cause(pool, &chains.primary().assets, &fill_at(block_timestamp))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn fill_on_a_disabled_asset_is_excluded() {
        let (pool, _apalis) = setup_test_pools().await;
        let chains = chains_with(Some(OperationMode::Disabled));
        record_trading_enablement(&pool, &chains, Utc::now())
            .await
            .unwrap();

        assert_eq!(
            cause_for(&pool, &chains, Utc::now()).await,
            Some(ExclusionCause::TradingDisabled)
        );
    }

    /// An asset first seen enabled has no known disabled period, so even a fill
    /// that landed long before the restart is hedged.
    #[tokio::test]
    async fn asset_first_seen_enabled_has_no_cutoff() {
        let (pool, _apalis) = setup_test_pools().await;
        let chains = chains_with(Some(OperationMode::Enabled));
        let restart = Utc::now();
        record_trading_enablement(&pool, &chains, restart)
            .await
            .unwrap();

        assert_eq!(
            cause_for(&pool, &chains, restart - Duration::days(1)).await,
            None
        );
    }

    /// Enabling a disabled asset keeps every fill that landed before that
    /// restart excluded, even when it is accounted afterwards, and hedges the
    /// fills that land from the restart on.
    #[tokio::test]
    async fn fill_that_landed_before_the_enabling_restart_stays_excluded() {
        let (pool, _apalis) = setup_test_pools().await;
        let disabled_at = Utc::now() - Duration::hours(2);
        record_trading_enablement(
            &pool,
            &chains_with(Some(OperationMode::Disabled)),
            disabled_at,
        )
        .await
        .unwrap();

        let enabled = chains_with(Some(OperationMode::Enabled));
        let restart = Utc::now();
        record_trading_enablement(&pool, &enabled, restart)
            .await
            .unwrap();

        assert_eq!(
            cause_for(&pool, &enabled, restart - Duration::minutes(1)).await,
            Some(ExclusionCause::LandedBeforeEnabled {
                enabled_since: DateTime::parse_from_rfc3339(&restart.to_rfc3339())
                    .unwrap()
                    .with_timezone(&Utc)
            })
        );
        assert_eq!(
            cause_for(&pool, &enabled, restart + Duration::seconds(1)).await,
            None
        );

        // A later restart with the asset still enabled keeps the same cutoff.
        record_trading_enablement(&pool, &enabled, restart + Duration::hours(1))
            .await
            .unwrap();
        assert!(matches!(
            cause_for(&pool, &enabled, restart - Duration::minutes(1)).await,
            Some(ExclusionCause::LandedBeforeEnabled { .. })
        ));
    }

    /// A symbol dropped from the config trades as disabled, so adding it back
    /// enabled is a new enable with its own cutoff.
    #[tokio::test]
    async fn symbol_dropped_from_config_and_readded_gets_a_cutoff() {
        let (pool, _apalis) = setup_test_pools().await;
        let enabled = chains_with(Some(OperationMode::Enabled));
        record_trading_enablement(&pool, &enabled, Utc::now() - Duration::hours(3))
            .await
            .unwrap();
        record_trading_enablement(&pool, &chains_with(None), Utc::now() - Duration::hours(2))
            .await
            .unwrap();

        let readded_at = Utc::now();
        record_trading_enablement(&pool, &enabled, readded_at)
            .await
            .unwrap();

        assert!(matches!(
            cause_for(&pool, &enabled, readded_at - Duration::minutes(1)).await,
            Some(ExclusionCause::LandedBeforeEnabled { .. })
        ));
    }
}
