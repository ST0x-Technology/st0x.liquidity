//! Which fills are excluded from hedging, and the trading enablement history
//! that decides it for fills accounted after an asset is enabled again.
//!
//! A fill is excluded when trading is disabled for its symbol on its own
//! chain, or when it landed inside a closed disabled period of that symbol and
//! chain: it landed while the asset was disabled, so it stays out of the hedged
//! `Position` even if it is accounted afterwards (still queued, not yet
//! backfilled, or landing during the restart itself). Period boundaries are
//! block numbers on the fill's chain, so a fill is placed by its own block and
//! never against a host clock.

use std::collections::BTreeMap;

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
    /// Trading is enabled, but the fill landed in `fill_block`, inside a
    /// disabled period that ended when trading was enabled from
    /// `enabled_from_block`.
    LandedWhileDisabled {
        fill_block: u64,
        enabled_from_block: u64,
    },
    /// The config read by this process enables trading, but the last restart
    /// recorded it disabled from `disabled_from_block` and no restart has
    /// closed that period yet: the fill landed in `fill_block`, inside it.
    DisabledPeriodStillOpen {
        fill_block: u64,
        disabled_from_block: u64,
    },
}

impl std::fmt::Display for ExclusionCause {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TradingDisabled => write!(formatter, "trading is disabled"),
            Self::LandedWhileDisabled {
                fill_block,
                enabled_from_block,
            } => write!(
                formatter,
                "the fill landed in block {fill_block} while trading was disabled, before it \
                 was enabled from block {enabled_from_block}"
            ),
            Self::DisabledPeriodStillOpen {
                fill_block,
                disabled_from_block,
            } => write!(
                formatter,
                "the fill landed in block {fill_block}, inside the disabled period recorded from \
                 block {disabled_from_block}, which stays open until the bot restarts with \
                 trading enabled"
            ),
        }
    }
}

/// Failure reading or recording the trading enablement history.
#[derive(Debug, thiserror::Error)]
pub enum ExclusionError {
    #[error("block number {block} exceeds i64::MAX")]
    BlockOutOfRange { block: u64 },
    #[error("stored block number {block} for {symbol} on {chain} is negative")]
    NegativeStoredBlock {
        chain: Chain,
        symbol: Symbol,
        block: i64,
    },
    #[error("no chain head was read at startup for hedged chain {chain}")]
    MissingChainHead { chain: Chain },
    #[error("{symbol} on {chain} is recorded disabled without the block it was disabled from")]
    MissingDisabledStart { chain: String, symbol: String },
    #[error(
        "the head read for {chain} ({head_block}) is behind block {disabled_from_block}, from \
         which {symbol} was recorded disabled; not closing its disabled period on a stale head"
    )]
    HeadBehindDisabledStart {
        chain: String,
        symbol: String,
        head_block: i64,
        disabled_from_block: i64,
    },
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

fn block_to_i64(block: u64) -> Result<i64, ExclusionError> {
    i64::try_from(block).map_err(|_| ExclusionError::BlockOutOfRange { block })
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
    fill_block: u64,
) -> Result<Option<ExclusionCause>, ExclusionError> {
    if !assets.is_trading_enabled(trade.symbol()) {
        return Ok(Some(ExclusionCause::TradingDisabled));
    }

    let chain = trade.chain.to_string();
    let symbol = trade.symbol().to_string();
    let block = block_to_i64(fill_block)?;

    // The config says enabled, but the last restart recorded the asset
    // disabled: the config was edited and read by a process (CLI `process-tx`)
    // before the bot restarted to close the period. A fill from the open
    // period stays excluded, as the bot will decide once it closes it.
    let open_period_start = sqlx::query_scalar!(
        "SELECT disabled_from_block FROM trading_enablement \
         WHERE chain = ? AND symbol = ? AND trading_enabled = 0 \
         AND disabled_from_block <= ?",
        chain,
        symbol,
        block,
    )
    .fetch_optional(pool)
    .await?
    .flatten();
    if let Some(disabled_from_block) = open_period_start {
        let disabled_from_block = u64::try_from(disabled_from_block).map_err(|_| {
            ExclusionError::NegativeStoredBlock {
                chain: trade.chain,
                symbol: trade.symbol().clone(),
                block: disabled_from_block,
            }
        })?;
        return Ok(Some(ExclusionCause::DisabledPeriodStillOpen {
            fill_block,
            disabled_from_block,
        }));
    }

    let enabled_from_block = sqlx::query_scalar!(
        "SELECT enabled_from_block FROM trading_disabled_period \
         WHERE chain = ? AND symbol = ? \
         AND disabled_from_block <= ? AND ? < enabled_from_block \
         ORDER BY enabled_from_block LIMIT 1",
        chain,
        symbol,
        block,
        block,
    )
    .fetch_optional(pool)
    .await?;

    enabled_from_block
        .map(|enabled_from_block| {
            u64::try_from(enabled_from_block)
                .map(|enabled_from_block| ExclusionCause::LandedWhileDisabled {
                    fill_block,
                    enabled_from_block,
                })
                .map_err(|_| ExclusionError::NegativeStoredBlock {
                    chain: trade.chain,
                    symbol: trade.symbol().clone(),
                    block: enabled_from_block,
                })
        })
        .transpose()
}

/// Records every hedged chain's configured trading flags as observed by this
/// restart, before any fill is accounted. `heads` is each hedged chain's head
/// block read by this restart: blocks up to it landed before the restart, and
/// blocks after it land under the flags observed now.
///
/// A disable opens a disabled period from the block after the head. An enable
/// of a disabled asset closes that period at the block after the head, which
/// [`exclusion_cause`] applies to fills accounted later; at the disabling head
/// itself no block landed while disabled, so no period is kept. An asset first seen
/// disabled opens a period from that restart, since it is not known to have
/// been disabled earlier; an asset first seen enabled has no disabled period.
/// A symbol dropped from a chain's config trades as disabled, so it opens a
/// period too. That covers its Raindex fills only: an `InventoryTrade` fill
/// needs the symbol's configured token to be recognized at all, so after the
/// entry is removed it is skipped as an unrecognized token, not excluded.
/// Operators stop trading by setting `trading = "disabled"` instead.
///
/// # Errors
///
/// Fails when a hedged chain has no head in `heads`, when a head is behind
/// the head read by the restart that disabled an asset it would enable (a
/// stale node; the start fails and the next one reads the head again), or
/// when the
/// enablement tables cannot be read or written. The whole observation commits
/// in one transaction, so a failure records nothing and the next restart
/// observes every symbol again against the head it reads.
pub(crate) async fn record_trading_enablement(
    pool: &SqlitePool,
    chains: &ChainRegistry,
    heads: &BTreeMap<Chain, u64>,
    now: DateTime<Utc>,
) -> Result<(), ExclusionError> {
    let now_text = now.to_rfc3339();
    // Reads then writes in one transaction while the telemetry writer is
    // already running: reserve the writer first, so a concurrent commit waits
    // out the busy timeout instead of failing the read to write upgrade
    // (docs/cqrs.md).
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;

    for hedged in chains.hedged() {
        let head = *heads
            .get(&hedged.chain)
            .ok_or(ExclusionError::MissingChainHead {
                chain: hedged.chain,
            })?;
        let next_block = block_to_i64(head.saturating_add(1))?;
        let chain = hedged.chain.to_string();

        let mut observed: Vec<(String, bool)> = hedged
            .assets
            .equities
            .symbols
            .keys()
            .map(|symbol| (symbol.to_string(), hedged.assets.is_trading_enabled(symbol)))
            .collect();
        // A symbol dropped from the config trades as disabled.
        let recorded_enabled = sqlx::query_scalar!(
            "SELECT symbol FROM trading_enablement WHERE chain = ? AND trading_enabled = 1",
            chain,
        )
        .fetch_all(&mut *tx)
        .await?;
        for symbol in recorded_enabled {
            if !observed.iter().any(|(configured, _)| *configured == symbol) {
                observed.push((symbol, false));
            }
        }

        for (symbol, enabled) in observed {
            record_symbol_enablement(&mut tx, &chain, &symbol, enabled, next_block, &now_text)
                .await?;
        }
    }
    tx.commit().await?;

    Ok(())
}

async fn record_symbol_enablement(
    tx: &mut sqlx::SqliteConnection,
    chain: &str,
    symbol: &str,
    enabled: bool,
    next_block: i64,
    now_text: &str,
) -> Result<(), ExclusionError> {
    let previous = sqlx::query!(
        "SELECT trading_enabled, disabled_from_block FROM trading_enablement \
         WHERE chain = ? AND symbol = ?",
        chain,
        symbol,
    )
    .fetch_optional(&mut *tx)
    .await?;
    let enabled_flag = i64::from(enabled);

    match previous {
        Some(row) if row.trading_enabled == enabled_flag => {
            sqlx::query!(
                "UPDATE trading_enablement SET observed_at = ? WHERE chain = ? AND symbol = ?",
                now_text,
                chain,
                symbol,
            )
            .execute(&mut *tx)
            .await?;
        }
        Some(row) if enabled => {
            let disabled_from_block =
                row.disabled_from_block
                    .ok_or_else(|| ExclusionError::MissingDisabledStart {
                        chain: chain.to_owned(),
                        symbol: symbol.to_owned(),
                    })?;
            // `next_block` is the exact exclusive end of the period. Equal
            // to its start, no block landed while disabled and there is no
            // period to keep. Behind its start, the node answered a head
            // older than the one the disabling restart read; any boundary
            // would be a guess, so the observation is refused and the next
            // start reads the head again.
            if next_block < disabled_from_block {
                return Err(ExclusionError::HeadBehindDisabledStart {
                    chain: chain.to_owned(),
                    symbol: symbol.to_owned(),
                    head_block: next_block - 1,
                    disabled_from_block,
                });
            }
            if next_block > disabled_from_block {
                info!(
                    %chain,
                    %symbol,
                    enabled_from_block = next_block,
                    "Trading enabled again; fills that landed while it was disabled stay \
                     excluded from hedging"
                );
                // Two periods ending on one block keep the wider one.
                sqlx::query!(
                    "INSERT INTO trading_disabled_period \
                     (chain, symbol, disabled_from_block, enabled_from_block, enabled_at) \
                     VALUES (?, ?, ?, ?, ?) \
                     ON CONFLICT (chain, symbol, enabled_from_block) DO UPDATE SET \
                     disabled_from_block = MIN(disabled_from_block, excluded.disabled_from_block)",
                    chain,
                    symbol,
                    disabled_from_block,
                    next_block,
                    now_text,
                )
                .execute(&mut *tx)
                .await?;
            } else {
                info!(
                    %chain,
                    %symbol,
                    "Trading enabled again before any block landed while it was disabled"
                );
            }
            sqlx::query!(
                "UPDATE trading_enablement \
                 SET trading_enabled = 1, disabled_from_block = NULL, observed_at = ? \
                 WHERE chain = ? AND symbol = ?",
                now_text,
                chain,
                symbol,
            )
            .execute(&mut *tx)
            .await?;
        }
        Some(_) => {
            sqlx::query!(
                "UPDATE trading_enablement \
                 SET trading_enabled = 0, disabled_from_block = ?, observed_at = ? \
                 WHERE chain = ? AND symbol = ?",
                next_block,
                now_text,
                chain,
                symbol,
            )
            .execute(&mut *tx)
            .await?;
        }
        None => {
            // First seen: an enabled asset has no known disabled period. A
            // disabled one is known disabled only from this restart on, so
            // earlier fills keep the hedged path once it is enabled.
            let disabled_from_block = (!enabled).then_some(next_block);
            sqlx::query!(
                "INSERT INTO trading_enablement \
                 (chain, symbol, trading_enabled, disabled_from_block, observed_at) \
                 VALUES (?, ?, ?, ?, ?)",
                chain,
                symbol,
                enabled_flag,
                disabled_from_block,
                now_text,
            )
            .execute(&mut *tx)
            .await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;
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

    /// A restart that reads `head` on the primary chain.
    async fn restart(pool: &SqlitePool, chains: &ChainRegistry, head: u64) {
        let heads = BTreeMap::from([(chains.primary().chain, head)]);
        record_trading_enablement(pool, chains, &heads, Utc::now())
            .await
            .unwrap();
    }

    async fn cause_for(
        pool: &SqlitePool,
        chains: &ChainRegistry,
        fill_block: u64,
    ) -> Option<ExclusionCause> {
        let fill = OnchainTradeBuilder::new()
            .with_block_number(fill_block)
            .build();
        exclusion_cause(pool, &chains.primary().assets, &fill, fill_block)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn fill_on_a_disabled_asset_is_excluded() {
        let (pool, _apalis) = setup_test_pools().await;
        let chains = chains_with(Some(OperationMode::Disabled));
        restart(&pool, &chains, 100).await;

        assert_eq!(
            cause_for(&pool, &chains, 150).await,
            Some(ExclusionCause::TradingDisabled)
        );
    }

    /// An asset first seen enabled has no known disabled period, so even a fill
    /// that landed long before the restart is hedged.
    #[tokio::test]
    async fn asset_first_seen_enabled_has_no_disabled_period() {
        let (pool, _apalis) = setup_test_pools().await;
        let chains = chains_with(Some(OperationMode::Enabled));
        restart(&pool, &chains, 100).await;

        assert_eq!(cause_for(&pool, &chains, 5).await, None);
    }

    /// Enabled, then disabled at head 100, then enabled at head 200: a fill that
    /// landed in the disabled period stays excluded when accounted later, while
    /// fills from the enabled periods on either side of it are hedged. The
    /// block right after each head read belongs to the new flags.
    #[tokio::test]
    async fn only_fills_inside_the_disabled_period_stay_excluded() {
        let (pool, _apalis) = setup_test_pools().await;
        let enabled = chains_with(Some(OperationMode::Enabled));
        restart(&pool, &enabled, 10).await;
        restart(&pool, &chains_with(Some(OperationMode::Disabled)), 100).await;
        restart(&pool, &enabled, 200).await;

        assert_eq!(cause_for(&pool, &enabled, 100).await, None);
        assert_eq!(
            cause_for(&pool, &enabled, 101).await,
            Some(ExclusionCause::LandedWhileDisabled {
                fill_block: 101,
                enabled_from_block: 201,
            })
        );
        assert_eq!(
            cause_for(&pool, &enabled, 200).await,
            Some(ExclusionCause::LandedWhileDisabled {
                fill_block: 200,
                enabled_from_block: 201,
            })
        );
        assert_eq!(cause_for(&pool, &enabled, 201).await, None);

        // A later restart with the asset still enabled keeps the period.
        restart(&pool, &enabled, 300).await;
        assert_eq!(
            cause_for(&pool, &enabled, 150).await,
            Some(ExclusionCause::LandedWhileDisabled {
                fill_block: 150,
                enabled_from_block: 201,
            })
        );
    }

    /// Enabled again while the head has not moved since the disable: no
    /// block landed while disabled, so no fill is excluded, including the
    /// next block, which lands after the enable.
    #[tokio::test]
    async fn an_enable_at_the_disabling_head_keeps_no_period() {
        let (pool, _apalis) = setup_test_pools().await;
        let enabled = chains_with(Some(OperationMode::Enabled));
        restart(&pool, &enabled, 10).await;
        restart(&pool, &chains_with(Some(OperationMode::Disabled)), 100).await;
        restart(&pool, &enabled, 100).await;

        assert_eq!(cause_for(&pool, &enabled, 101).await, None);
        let (periods,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM trading_disabled_period")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(periods, 0);
    }

    /// A load balanced node can answer the enabling restart with a head
    /// behind the one the disabling restart read. The observation is refused
    /// and nothing is recorded, so the asset stays disabled from its start
    /// until a later start reads a current head.
    #[tokio::test]
    async fn a_head_that_went_backwards_is_refused() {
        let (pool, _apalis) = setup_test_pools().await;
        let enabled = chains_with(Some(OperationMode::Enabled));
        restart(&pool, &enabled, 10).await;
        restart(&pool, &chains_with(Some(OperationMode::Disabled)), 100).await;

        let heads = BTreeMap::from([(enabled.primary().chain, 90)]);
        let error = record_trading_enablement(&pool, &enabled, &heads, Utc::now())
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                ExclusionError::HeadBehindDisabledStart {
                    head_block: 90,
                    disabled_from_block: 101,
                    ..
                }
            ),
            "{error}"
        );
        assert_eq!(
            cause_for(&pool, &enabled, 150).await,
            Some(ExclusionCause::DisabledPeriodStillOpen {
                fill_block: 150,
                disabled_from_block: 101,
            }),
            "the period stays open"
        );
    }

    /// Periods and their boundaries are per chain: AAPL disabled on Ethereum
    /// only excludes Ethereum fills, bounded by Ethereum's own heads, while a
    /// Base fill at a block inside those numbers is hedged.
    #[tokio::test]
    async fn disabled_periods_are_scoped_to_their_own_chain() {
        let (pool, _apalis) = setup_test_pools().await;
        let registry = |ethereum: OperationMode| {
            let mut chains = chains_with(Some(OperationMode::Enabled));
            let mut secondary = chains.primary().clone();
            secondary.chain = Chain::Ethereum;
            secondary.assets.equities.symbols =
                HashMap::from([(Symbol::new("AAPL").unwrap(), aapl(ethereum))]);
            chains.insert_secondary(secondary);
            chains
        };
        let heads = |base: u64, ethereum: u64| {
            BTreeMap::from([(Chain::Base, base), (Chain::Ethereum, ethereum)])
        };
        let enabled = registry(OperationMode::Enabled);
        record_trading_enablement(&pool, &enabled, &heads(1000, 10), Utc::now())
            .await
            .unwrap();
        record_trading_enablement(
            &pool,
            &registry(OperationMode::Disabled),
            &heads(1100, 100),
            Utc::now(),
        )
        .await
        .unwrap();
        record_trading_enablement(&pool, &enabled, &heads(1200, 200), Utc::now())
            .await
            .unwrap();

        let cause_on = async |chain: Chain, block: u64| {
            let mut fill = OnchainTradeBuilder::new().with_block_number(block).build();
            fill.chain = chain;
            let assets = &enabled.hedged_chain(chain).unwrap().assets;
            exclusion_cause(&pool, assets, &fill, block).await.unwrap()
        };

        assert_eq!(
            cause_on(Chain::Ethereum, 150).await,
            Some(ExclusionCause::LandedWhileDisabled {
                fill_block: 150,
                enabled_from_block: 201,
            })
        );
        assert_eq!(cause_on(Chain::Ethereum, 1150).await, None);
        assert_eq!(cause_on(Chain::Base, 150).await, None);
        assert_eq!(cause_on(Chain::Base, 1150).await, None);
    }

    /// Two disable and enable cycles: each period excludes only its own fills.
    #[tokio::test]
    async fn every_disabled_period_is_kept() {
        let (pool, _apalis) = setup_test_pools().await;
        let enabled = chains_with(Some(OperationMode::Enabled));
        let disabled = chains_with(Some(OperationMode::Disabled));
        restart(&pool, &enabled, 10).await;
        restart(&pool, &disabled, 100).await;
        restart(&pool, &enabled, 200).await;
        restart(&pool, &disabled, 300).await;
        restart(&pool, &enabled, 400).await;

        assert_eq!(
            cause_for(&pool, &enabled, 150).await,
            Some(ExclusionCause::LandedWhileDisabled {
                fill_block: 150,
                enabled_from_block: 201,
            })
        );
        assert_eq!(cause_for(&pool, &enabled, 250).await, None);
        assert_eq!(
            cause_for(&pool, &enabled, 350).await,
            Some(ExclusionCause::LandedWhileDisabled {
                fill_block: 350,
                enabled_from_block: 401,
            })
        );
    }

    /// An asset first seen disabled is known disabled only from that restart:
    /// once enabled, fills from before it keep the hedged path.
    #[tokio::test]
    async fn asset_first_seen_disabled_is_disabled_from_that_restart() {
        let (pool, _apalis) = setup_test_pools().await;
        restart(&pool, &chains_with(Some(OperationMode::Disabled)), 100).await;
        let enabled = chains_with(Some(OperationMode::Enabled));
        restart(&pool, &enabled, 200).await;

        assert_eq!(cause_for(&pool, &enabled, 100).await, None);
        assert_eq!(
            cause_for(&pool, &enabled, 101).await,
            Some(ExclusionCause::LandedWhileDisabled {
                fill_block: 101,
                enabled_from_block: 201,
            })
        );
        assert_eq!(cause_for(&pool, &enabled, 201).await, None);
    }

    /// A symbol dropped from the config trades as disabled, so adding it back
    /// enabled closes a disabled period starting at the drop.
    #[tokio::test]
    async fn symbol_dropped_from_config_and_readded_opens_a_disabled_period() {
        let (pool, _apalis) = setup_test_pools().await;
        let enabled = chains_with(Some(OperationMode::Enabled));
        restart(&pool, &enabled, 10).await;
        restart(&pool, &chains_with(None), 100).await;
        restart(&pool, &enabled, 200).await;

        assert_eq!(cause_for(&pool, &enabled, 50).await, None);
        assert_eq!(
            cause_for(&pool, &enabled, 150).await,
            Some(ExclusionCause::LandedWhileDisabled {
                fill_block: 150,
                enabled_from_block: 201,
            })
        );
    }

    /// A failure partway through an observation records none of it: the
    /// chains observed before the failure must not keep boundaries from this
    /// restart while the rest get the next restart's.
    #[tokio::test]
    async fn a_failed_observation_records_nothing() {
        let (pool, _apalis) = setup_test_pools().await;
        let mut chains = chains_with(Some(OperationMode::Enabled));
        let mut secondary = chains.primary().clone();
        secondary.chain = Chain::Ethereum;
        chains.insert_secondary(secondary);
        let heads = BTreeMap::from([(chains.primary().chain, 100)]);

        let error = record_trading_enablement(&pool, &chains, &heads, Utc::now())
            .await
            .unwrap_err();
        assert!(matches!(error, ExclusionError::MissingChainHead { .. }));

        let (recorded,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM trading_enablement")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(recorded, 0);
    }

    /// A process whose config already enables the asset, before the bot has
    /// restarted to close the disabled period, still excludes a fill from the
    /// open period; a fill from before the period is hedged.
    #[tokio::test]
    async fn fill_in_an_open_disabled_period_stays_excluded_under_a_newer_config() {
        let (pool, _apalis) = setup_test_pools().await;
        restart(&pool, &chains_with(Some(OperationMode::Enabled)), 10).await;
        restart(&pool, &chains_with(Some(OperationMode::Disabled)), 100).await;
        let enabled = chains_with(Some(OperationMode::Enabled));

        assert_eq!(
            cause_for(&pool, &enabled, 150).await,
            Some(ExclusionCause::DisabledPeriodStillOpen {
                fill_block: 150,
                disabled_from_block: 101,
            })
        );
        assert_eq!(cause_for(&pool, &enabled, 100).await, None);
    }

    #[tokio::test]
    async fn a_hedged_chain_without_a_head_is_refused() {
        let (pool, _apalis) = setup_test_pools().await;
        let error =
            record_trading_enablement(&pool, &chains_with(None), &BTreeMap::new(), Utc::now())
                .await
                .unwrap_err();

        assert!(matches!(error, ExclusionError::MissingChainHead { .. }));
    }
}
