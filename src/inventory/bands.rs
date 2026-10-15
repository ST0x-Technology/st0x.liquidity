//! Each hedged chain's rebalancing band as the dashboard inventory shows it,
//! read from the config once at startup.

use std::collections::{BTreeMap, BTreeSet};

use tracing::warn;

use st0x_config::Ctx;
use st0x_dto::RebalancingBand;
use st0x_evm::Chain;
use st0x_execution::Symbol;

/// The hedged chains and the band each one's vaults are held to. A chain
/// that is not hedged has no per-chain inventory row at all.
#[derive(Debug, Clone, Default)]
pub(crate) struct RebalancingBands {
    pub(crate) hedged: BTreeSet<Chain>,
    pub(crate) equity: BTreeMap<(Symbol, Chain), RebalancingBand>,
    pub(crate) usdc: BTreeMap<Chain, RebalancingBand>,
}

impl RebalancingBands {
    /// An equity's band on a chain is its `target_share` there, else the
    /// chain's allocation target, inside the allocation deviation. Enabled
    /// and paused listings have one; a disabled listing does not. A chain
    /// whose cash asset rebalances has its corridor's band, whatever the
    /// USDC mode.
    pub(crate) fn from_ctx(ctx: &Ctx) -> Self {
        let allocation = &ctx.rebalancing.allocation;

        let equity = ctx
            .chains
            .hedged()
            .flat_map(|hedged| {
                hedged
                    .assets
                    .equities
                    .symbols
                    .iter()
                    .filter(|(_, listing)| listing.rebalancing.keeps_services())
                    .map(move |(symbol, listing)| (hedged.chain, symbol, listing))
            })
            .filter_map(|(chain, symbol, listing)| {
                let Some(target) = listing
                    .target_share
                    .or_else(|| allocation.targets.get(&chain).copied())
                else {
                    warn!(
                        target: "dashboard",
                        %symbol,
                        %chain,
                        "A rebalancing listing has no target share, so the dashboard shows no \
                         band for it -- config validation should have refused this"
                    );
                    return None;
                };

                let band = RebalancingBand {
                    target: target.inner(),
                    deviation: allocation.deviation.inner(),
                };

                Some(((symbol.clone(), chain), band))
            })
            .collect();

        let rebalances_cash = |chain| {
            ctx.chains
                .hedged_chain(chain)
                .and_then(|hedged| hedged.assets.rebalancing_cash())
                .is_some()
        };
        let usdc = ctx
            .rebalancing
            .usdc
            .configured()
            .filter(|usdc| rebalances_cash(usdc.corridor.chain()))
            .map(|usdc| {
                let band = RebalancingBand {
                    target: usdc.threshold.target,
                    deviation: usdc.threshold.deviation,
                };

                (usdc.corridor.chain(), band)
            })
            .collect();

        Self {
            hedged: ctx.chains.hedged().map(|hedged| hedged.chain).collect(),
            equity,
            usdc,
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address};
    use std::collections::HashMap;

    use st0x_bridge::corridor::{HopKind, UsdcCorridor};
    use st0x_config::{
        ChainAssets, ChainCashAsset, ChainEquities, ChainEquityAsset, HedgedChain,
        ImbalanceThreshold, OperationMode, RebalancingMode, TargetShare, UsdcCorridorCtx,
        UsdcCorridors, create_test_ctx_with_order_owner,
    };
    use st0x_float_macro::float;

    use super::*;

    fn listing(
        rebalancing: RebalancingMode,
        target_share: Option<TargetShare>,
    ) -> ChainEquityAsset {
        ChainEquityAsset {
            tokenized_equity: Address::random(),
            tokenized_equity_derivative: Address::random(),
            vault_ids: Vec::new(),
            trading: OperationMode::Enabled,
            rebalancing,
            wrapped_equity_recovery: OperationMode::Disabled,
            operational_limit: None,
            target_share,
        }
    }

    fn symbol(name: &str) -> Symbol {
        Symbol::new(name).unwrap()
    }

    fn wire(band: Option<RebalancingBand>) -> serde_json::Value {
        serde_json::to_value(band).unwrap()
    }

    /// A listing's own `target_share` wins over its chain's default, a
    /// paused listing keeps its band, a disabled one has none, and a cash
    /// corridor shows its band while the USDC mode is disabled, but only on a
    /// chain whose cash asset rebalances.
    #[test]
    fn bands_resolve_each_listings_effective_target() {
        let mut ctx = create_test_ctx_with_order_owner(address!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ));
        assert_eq!(ctx.chains.primary().chain, Chain::Base);
        ctx.chains.primary_mut().assets.equities.symbols.extend([
            (symbol("AAPL"), listing(RebalancingMode::Enabled, None)),
            (symbol("TSLA"), listing(RebalancingMode::Disabled, None)),
        ]);
        ctx.chains.insert_secondary(
            HedgedChain::test()
                .chain(Chain::Robinhood)
                .assets(ChainAssets {
                    equities: ChainEquities {
                        operational_limit: None,
                        symbols: HashMap::from([
                            (
                                symbol("AAPL"),
                                listing(
                                    RebalancingMode::Enabled,
                                    Some(TargetShare::new(float!(0.3)).unwrap()),
                                ),
                            ),
                            (symbol("TSLA"), listing(RebalancingMode::Paused, None)),
                        ]),
                    },
                    cash: Some(ChainCashAsset {
                        vault_ids: Vec::new(),
                        rebalancing: OperationMode::Enabled,
                        operational_limit: None,
                    }),
                })
                .call(),
        );
        ctx.rebalancing
            .allocation
            .targets
            .insert(Chain::Robinhood, TargetShare::new(float!(0.2)).unwrap());
        ctx.rebalancing.usdc = UsdcCorridors::for_test(
            OperationMode::Disabled,
            [
                UsdcCorridorCtx {
                    corridor: UsdcCorridor::HubRouted {
                        chain: Chain::Robinhood,
                        hop: HopKind::Relay,
                    },
                    threshold: ImbalanceThreshold {
                        target: float!(0.25),
                        deviation: float!(0.125),
                    },
                },
                UsdcCorridorCtx {
                    corridor: UsdcCorridor::BASE_CCTP,
                    threshold: ImbalanceThreshold {
                        target: float!(0.6),
                        deviation: float!(0.2),
                    },
                },
            ],
        );

        let bands = RebalancingBands::from_ctx(&ctx);

        assert_eq!(
            bands.hedged,
            BTreeSet::from([Chain::Base, Chain::Robinhood])
        );
        let equity = |name: &str, chain| wire(bands.equity.get(&(symbol(name), chain)).copied());
        assert_eq!(
            equity("AAPL", Chain::Base),
            serde_json::json!({ "target": "0.5", "deviation": "0.1" })
        );
        assert_eq!(
            equity("AAPL", Chain::Robinhood),
            serde_json::json!({ "target": "0.3", "deviation": "0.1" })
        );
        assert_eq!(
            equity("TSLA", Chain::Robinhood),
            serde_json::json!({ "target": "0.2", "deviation": "0.1" })
        );
        assert_eq!(equity("TSLA", Chain::Base), serde_json::json!(null));
        assert_eq!(
            wire(bands.usdc.get(&Chain::Robinhood).copied()),
            serde_json::json!({ "target": "0.25", "deviation": "0.125" })
        );
        assert_eq!(
            wire(bands.usdc.get(&Chain::Base).copied()),
            serde_json::json!(null)
        );
    }
}
