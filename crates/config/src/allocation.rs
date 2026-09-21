//! Equity allocation across chains: the per-chain target shares, the broker
//! floor and the sizing rules the allocation planner reads.

use rain_math_float::{Float, FloatError};
use serde::{Deserialize, Deserializer};
use std::collections::BTreeMap;
use std::sync::LazyLock;
use std::time::Duration;

use st0x_evm::Chain;
use st0x_execution::{Positive, Symbol};
use st0x_finance::Usdc;
use st0x_float_macro::float;
use st0x_float_serde::{deserialize_float_from_number_or_string, format_float_with_fallback};

use crate::{ChainConfig, ChainLifecycle, OperationMode, TradingConfig};

static EXACT_ONE: LazyLock<Float> = LazyLock::new(|| float!(1));

/// The fraction of a symbol's total inventory that belongs at one venue,
/// in `[0, 1]`.
#[derive(Debug, Clone, Copy)]
pub struct TargetShare(Float);

impl TargetShare {
    /// # Errors
    ///
    /// Returns [`AllocationConfigError::TargetShareOutOfRange`] outside `[0, 1]`.
    pub fn new(value: Float) -> Result<Self, AllocationConfigError> {
        let out_of_range = value.lt(Float::zero()?)? || value.gt(*EXACT_ONE)?;

        if out_of_range {
            return Err(AllocationConfigError::TargetShareOutOfRange { value });
        }

        Ok(Self(value))
    }

    pub fn inner(self) -> Float {
        self.0
    }
}

impl<'de> Deserialize<'de> for TargetShare {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = deserialize_float_from_number_or_string(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// How far a chain's actual share may drift from its target before the
/// planner acts, as a non-negative fraction of the total.
#[derive(Debug, Clone, Copy)]
pub struct DeviationBand(Float);

impl DeviationBand {
    /// # Errors
    ///
    /// Returns [`AllocationConfigError::NegativeDeviationBand`] below zero.
    pub fn new(value: Float) -> Result<Self, AllocationConfigError> {
        if value.lt(Float::zero()?)? {
            return Err(AllocationConfigError::NegativeDeviationBand { value });
        }

        Ok(Self(value))
    }

    pub fn inner(self) -> Float {
        self.0
    }
}

impl<'de> Deserialize<'de> for DeviationBand {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = deserialize_float_from_number_or_string(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// The `[rebalancing.allocation]` table.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllocationConfig {
    /// Default target share of every equity's total inventory per chain. A
    /// chain's equity can override its entry with `target_share`.
    pub targets: BTreeMap<Chain, TargetShare>,
    /// The share of every equity's total that stays at the broker: a mint
    /// never takes the broker's available shares below it.
    pub alpaca_floor: TargetShare,
    /// The band around each chain's target inside which no operation runs.
    pub deviation: DeviationBand,
    /// The smallest transfer worth its gas, valued at the last hedge price. A
    /// chain's trading table can override it with its own `min_operation_usd`.
    pub min_operation_usd: Positive<Usdc>,
    /// How long a `(symbol, chain)` pair is not re-planned after an operation.
    pub cooldown_secs: u64,
}

impl AllocationConfig {
    /// Checks the targets against the chain tables: a target may name only a
    /// hedged chain, every rebalanced listing needs an effective target, and
    /// per symbol those targets plus the floor must not exceed 1. A listing
    /// with rebalancing disabled is never planned, so it needs no target and
    /// does not count.
    ///
    /// # Errors
    ///
    /// The first violation, in chain and then symbol order.
    pub fn validate(
        &self,
        chains: &BTreeMap<Chain, ChainConfig>,
    ) -> Result<(), AllocationConfigError> {
        let hedged: BTreeMap<Chain, &TradingConfig> = chains
            .iter()
            .filter(|(_, config)| config.lifecycle != ChainLifecycle::Disabled)
            .filter_map(|(chain, config)| config.trading.as_ref().map(|trading| (*chain, trading)))
            .collect();

        if let Some(chain) = self
            .targets
            .keys()
            .find(|chain| !hedged.contains_key(chain))
        {
            return Err(AllocationConfigError::TargetOnUnhedgedChain { chain: *chain });
        }

        let mut sums: BTreeMap<&Symbol, Float> = BTreeMap::new();
        for (chain, trading) in &hedged {
            let mut listings: Vec<_> = trading
                .assets
                .equities
                .symbols
                .iter()
                .filter(|(_, equity)| equity.rebalancing == OperationMode::Enabled)
                .collect();
            listings.sort_by_key(|(symbol, _)| *symbol);

            for (symbol, equity) in listings {
                let target = equity
                    .target_share
                    .or_else(|| self.targets.get(chain).copied())
                    .ok_or_else(|| AllocationConfigError::MissingTarget {
                        chain: *chain,
                        symbol: symbol.clone(),
                    })?;
                let running = match sums.get(symbol) {
                    Some(sum) => (*sum + target.inner())?,
                    None => target.inner(),
                };
                sums.insert(symbol, running);
            }
        }

        for (symbol, sum) in sums {
            let total = (sum + self.alpaca_floor.inner())?;
            if total.gt(*EXACT_ONE)? {
                return Err(AllocationConfigError::TargetsExceedOne {
                    symbol: symbol.clone(),
                    total,
                });
            }
        }

        Ok(())
    }
}

/// [`AllocationConfig`] after validation, with durations resolved.
#[derive(Debug, Clone)]
pub struct AllocationCtx {
    pub targets: BTreeMap<Chain, TargetShare>,
    pub alpaca_floor: TargetShare,
    pub deviation: DeviationBand,
    pub min_operation_usd: Positive<Usdc>,
    pub cooldown: Duration,
}

impl AllocationCtx {
    /// # Errors
    ///
    /// Returns [`AllocationConfigError::ZeroCooldown`] for a zero cooldown.
    pub fn new(config: &AllocationConfig) -> Result<Self, AllocationConfigError> {
        if config.cooldown_secs == 0 {
            return Err(AllocationConfigError::ZeroCooldown);
        }

        Ok(Self {
            targets: config.targets.clone(),
            alpaca_floor: config.alpaca_floor,
            deviation: config.deviation,
            min_operation_usd: config.min_operation_usd,
            cooldown: Duration::from_secs(config.cooldown_secs),
        })
    }
}

/// Why an allocation config was refused.
#[derive(Debug, thiserror::Error)]
pub enum AllocationConfigError {
    #[error(
        "target share must be between 0 and 1 inclusive, got {}",
        format_float_with_fallback(value)
    )]
    TargetShareOutOfRange { value: Float },
    #[error(
        "deviation band must be >= 0, got {}",
        format_float_with_fallback(value)
    )]
    NegativeDeviationBand { value: Float },
    #[error("cooldown_secs must be non-zero")]
    ZeroCooldown,
    #[error(
        "[rebalancing.allocation] targets names {chain}, which has no enabled \
         [chains.{chain}.trading] table and so is not a hedged chain"
    )]
    TargetOnUnhedgedChain { chain: Chain },
    #[error(
        "{symbol} rebalances on {chain} but has no target share there: set \
         [rebalancing.allocation].targets.{chain} or target_share on \
         [chains.{chain}.trading.assets.equities.{symbol}]"
    )]
    MissingTarget { chain: Chain, symbol: Symbol },
    #[error(
        "{symbol}: its chain target shares plus alpaca_floor sum to {}, which exceeds 1",
        format_float_with_fallback(total)
    )]
    TargetsExceedOne { symbol: Symbol, total: Float },
    #[error(transparent)]
    Float(#[from] FloatError),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allocation(targets: &str, alpaca_floor: &str) -> AllocationConfig {
        toml::from_str(&format!(
            r"
            targets = {targets}
            alpaca_floor = {alpaca_floor}
            deviation = 0.05
            min_operation_usd = 100
            cooldown_secs = 600
            "
        ))
        .unwrap()
    }

    /// A hedged chain listing AAPL with the given flag and optional
    /// `target_share` override.
    fn hedged(lifecycle: ChainLifecycle, rebalancing: &str, target_share: &str) -> ChainConfig {
        let trading: TradingConfig = toml::from_str(&format!(
            r#"
            orderbook = "0x1111111111111111111111111111111111111111"
            inventory_mode = "legacy"
            inventory_adapters = []
            vault_owner = "0x3333333333333333333333333333333333333333"
            deployment_block = 1
            order_fill_poll_interval_secs = 1
            ingestion_cutoff = "safe"

            [assets.equities.AAPL]
            tokenized_equity = "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
            tokenized_equity_derivative = "0xf4f8c66085910d583c01f3b4e44bf731d4e2c565"
            trading = "enabled"
            rebalancing = "{rebalancing}"
            wrapped_equity_recovery = "disabled"
            {target_share}
            "#
        ))
        .unwrap();

        ChainConfig {
            lifecycle,
            required_confirmations: 1,
            trading: Some(trading),
        }
    }

    fn transport() -> ChainConfig {
        ChainConfig {
            lifecycle: ChainLifecycle::Active,
            required_confirmations: 1,
            trading: None,
        }
    }

    fn aapl() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    #[test]
    fn targets_that_leave_room_for_the_floor_pass() {
        let chains = BTreeMap::from([
            (Chain::Base, hedged(ChainLifecycle::Active, "enabled", "")),
            (Chain::Ethereum, transport()),
        ]);

        allocation(r"{ base = 0.6 }", "0.4")
            .validate(&chains)
            .unwrap();
    }

    #[test]
    fn targets_plus_floor_above_one_are_refused_per_symbol() {
        let chains = BTreeMap::from([
            (Chain::Base, hedged(ChainLifecycle::Active, "enabled", "")),
            (
                Chain::HyperEvm,
                hedged(ChainLifecycle::Active, "enabled", ""),
            ),
        ]);

        let error = allocation(r"{ base = 0.5, hyperevm = 0.4 }", "0.2")
            .validate(&chains)
            .unwrap_err();

        let AllocationConfigError::TargetsExceedOne { symbol, total } = error else {
            panic!("expected TargetsExceedOne, got {error:?}");
        };
        assert_eq!(symbol, aapl());
        assert!(total.eq(float!(1.1)).unwrap(), "got {total:?}");
    }

    /// The per-equity override replaces the chain default in the sum, in
    /// both directions.
    #[test]
    fn per_asset_target_share_replaces_the_chain_target_in_the_sum() {
        let over = BTreeMap::from([(
            Chain::Base,
            hedged(ChainLifecycle::Active, "enabled", r"target_share = 0.9"),
        )]);
        let error = allocation(r"{ base = 0.5 }", "0.2")
            .validate(&over)
            .unwrap_err();
        assert!(
            matches!(error, AllocationConfigError::TargetsExceedOne { .. }),
            "got {error:?}"
        );

        let under = BTreeMap::from([(
            Chain::Base,
            hedged(ChainLifecycle::Active, "enabled", r"target_share = 0.7"),
        )]);
        allocation(r"{ base = 0.9 }", "0.2")
            .validate(&under)
            .unwrap();
    }

    #[test]
    fn target_on_a_chain_without_a_trading_table_is_refused() {
        let chains = BTreeMap::from([
            (Chain::Base, hedged(ChainLifecycle::Active, "enabled", "")),
            (Chain::Ethereum, transport()),
        ]);

        let error = allocation(r"{ base = 0.5, ethereum = 0.1 }", "0.1")
            .validate(&chains)
            .unwrap_err();

        assert!(
            matches!(
                error,
                AllocationConfigError::TargetOnUnhedgedChain {
                    chain: Chain::Ethereum
                }
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn target_on_a_disabled_chain_is_refused() {
        let chains = BTreeMap::from([
            (Chain::Base, hedged(ChainLifecycle::Active, "enabled", "")),
            (
                Chain::HyperEvm,
                hedged(ChainLifecycle::Disabled, "disabled", ""),
            ),
        ]);

        let error = allocation(r"{ base = 0.5, hyperevm = 0.1 }", "0.1")
            .validate(&chains)
            .unwrap_err();

        assert!(
            matches!(
                error,
                AllocationConfigError::TargetOnUnhedgedChain {
                    chain: Chain::HyperEvm
                }
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn rebalanced_equity_without_a_target_on_its_chain_is_refused() {
        let chains = BTreeMap::from([
            (Chain::Base, hedged(ChainLifecycle::Active, "enabled", "")),
            (
                Chain::HyperEvm,
                hedged(ChainLifecycle::Active, "enabled", ""),
            ),
        ]);

        let error = allocation(r"{ base = 0.5 }", "0.1")
            .validate(&chains)
            .unwrap_err();

        let AllocationConfigError::MissingTarget { chain, symbol } = error else {
            panic!("expected MissingTarget, got {error:?}");
        };
        assert_eq!(chain, Chain::HyperEvm);
        assert_eq!(symbol, aapl());
    }

    /// A hedge-only listing is never planned, so it needs no target and
    /// its chain's default does not count against the symbol.
    #[test]
    fn a_listing_with_rebalancing_disabled_needs_no_target() {
        let chains = BTreeMap::from([
            (Chain::Base, hedged(ChainLifecycle::Active, "enabled", "")),
            (
                Chain::Robinhood,
                hedged(ChainLifecycle::Prefunded, "disabled", ""),
            ),
        ]);

        allocation(r"{ base = 0.6 }", "0.4")
            .validate(&chains)
            .unwrap();
        allocation(r"{ base = 0.6, robinhood = 0.4 }", "0.4")
            .validate(&chains)
            .unwrap();
    }
}
