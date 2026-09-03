//! Chain lifecycle states and the enablement predicate that gates them.
//!
//! Configuration says what the operator wants a chain to do. This module says
//! what the compiled binary can actually do on that chain, and refuses the
//! difference at startup rather than discovering it one runtime failure at a
//! time.

use serde::Deserialize;
use std::collections::BTreeSet;
use std::fmt;
use thiserror::Error;

use st0x_evm::Chain;
use st0x_execution::Symbol;

use crate::assets::{ChainAssets, OperationMode};

/// How far along a chain is in its bring-up.
///
/// A ladder rather than a set of booleans: each state is everything the one
/// before it does, plus one more class of action.
///
/// ```text
///   disabled -> observe-only -> prefunded -> active
///      |            |               |           |
///      |            |               |           +-- rebalancing runs, subject
///      |            |               |               to the per-asset flags
///      |            |               +-- the bot holds and moves funds here,
///      |            |                   but tops them up by hand
///      |            +-- the bot reads this chain; it places no orders and
///      |                moves no funds
///      +-- not constructed at all: no RPC, no watcher, no signer
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChainLifecycle {
    Disabled,
    ObserveOnly,
    Prefunded,
    Active,
}

impl fmt::Display for ChainLifecycle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Disabled => "disabled",
            Self::ObserveOnly => "observe-only",
            Self::Prefunded => "prefunded",
            Self::Active => "active",
        })
    }
}

/// Something the binary must have wired for a chain before that chain can be
/// asked to do the corresponding work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChainCapability {
    /// A fill watcher that decodes this chain's orderbook fills.
    FillIngestion,
    /// Ingested fills feed the `Position` aggregate and are hedged.
    Hedging,
    /// A signing wallet exists for this chain.
    WalletSigning,
    /// Wrapper, issuance mint/redeem and a redemption wallet, so equity can be
    /// moved between this chain and the broker.
    EquityRebalancing,
    /// A USDC address and a transport to the broker's deposit chain.
    CashRebalancing,
    /// A price source for this chain's native gas token, so bot-paid gas can be
    /// valued in USD.
    GasValuation,
}

impl fmt::Display for ChainCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::FillIngestion => "fill ingestion",
            Self::Hedging => "hedging",
            Self::WalletSigning => "wallet signing",
            Self::EquityRebalancing => "equity rebalancing",
            Self::CashRebalancing => "cash rebalancing",
            Self::GasValuation => "gas valuation",
        })
    }
}

/// What this build can do on `chain`.
///
/// This is a statement about wiring, not about configuration, so it is a
/// constant rather than something read from a file. Each entry is only as
/// true as the code behind it:
///
/// - Base has everything: the fill watcher, the wrapper, CCTP's domain 6, and
///   an ETH/USD Pyth feed read at a Base block.
/// - Ethereum signs, holds USDC (CCTP domain 0) and pays gas in ETH, but has
///   no orderbook wiring and no wrapper.
/// - HyperEVM has a signer and nothing else: no CCTP domain is known for it,
///   and its native token is HYPE, which the ETH/USD feed cannot value.
pub fn provided_capabilities(chain: Chain) -> BTreeSet<ChainCapability> {
    use ChainCapability::*;

    match chain {
        Chain::Base => BTreeSet::from([
            FillIngestion,
            Hedging,
            WalletSigning,
            EquityRebalancing,
            CashRebalancing,
            GasValuation,
        ]),
        // FillIngestion + Hedging granted with the per-chain watcher and
        // chain-aware accounting code (RAI-2079); rebalancing stays Base-only.
        Chain::Ethereum => BTreeSet::from([
            FillIngestion,
            Hedging,
            WalletSigning,
            CashRebalancing,
            GasValuation,
        ]),
        Chain::HyperEvm => BTreeSet::from([WalletSigning]),
    }
}

/// What a chain needs before it can run at `lifecycle`.
///
/// The rebalancing capabilities are driven by the per-asset flags rather than
/// by the lifecycle alone: an `active` chain that rebalances nothing needs no
/// rebalancing wiring, and demanding it would refuse a valid configuration.
pub fn required_capabilities(
    lifecycle: ChainLifecycle,
    is_trading: bool,
    assets: Option<&ChainAssets>,
) -> BTreeSet<ChainCapability> {
    use ChainCapability::*;

    let mut required = BTreeSet::new();

    if lifecycle == ChainLifecycle::Disabled {
        return required;
    }

    required.insert(WalletSigning);

    if is_trading {
        required.insert(FillIngestion);

        // Exhaustive rather than a `matches!`, so a lifecycle state added later
        // forces a decision here instead of silently not requiring hedging.
        // `ObserveOnly` reads without placing anything, and `Disabled` returned
        // above; neither hedges.
        let hedges = match lifecycle {
            ChainLifecycle::Disabled | ChainLifecycle::ObserveOnly => false,
            ChainLifecycle::Prefunded | ChainLifecycle::Active => true,
        };

        if hedges {
            required.insert(Hedging);
        }
    }

    if lifecycle == ChainLifecycle::Active {
        required.insert(GasValuation);

        let rebalances_equity = assets.is_some_and(|assets| {
            assets
                .equities
                .symbols
                .values()
                .any(|equity| equity.rebalancing == OperationMode::Enabled)
        });
        if rebalances_equity {
            required.insert(EquityRebalancing);
        }

        let rebalances_cash = assets.is_some_and(|assets| {
            assets
                .cash
                .as_ref()
                .is_some_and(|cash| cash.rebalancing == OperationMode::Enabled)
        });
        if rebalances_cash {
            required.insert(CashRebalancing);
        }
    }

    required
}

/// Why a configured chain cannot run as asked.
#[derive(Debug, Error)]
pub enum ChainEnablementError {
    #[error(
        "[chains.{chain}] is configured as \"{lifecycle}\", but this build provides no \
         {missing} for it"
    )]
    MissingCapabilities {
        chain: Chain,
        lifecycle: ChainLifecycle,
        missing: MissingCapabilities,
    },
    #[error(
        "[chains.{chain}] is \"observe-only\" while {symbol} has trading = \"enabled\": \
         its fills would be recorded and never hedged, which is exactly the exposure \
         observe-only exists to avoid"
    )]
    ObserveOnlyWithTrading { chain: Chain, symbol: Symbol },
}

/// The capabilities a chain asked for and did not get, rendered as a list.
#[derive(Debug)]
pub struct MissingCapabilities(Vec<ChainCapability>);

impl MissingCapabilities {
    /// The capabilities themselves, so callers and tests can inspect the gap
    /// rather than parse its rendering.
    pub fn into_inner(self) -> Vec<ChainCapability> {
        self.0
    }
}

impl fmt::Display for MissingCapabilities {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rendered: Vec<String> = self
            .0
            .iter()
            .map(std::string::ToString::to_string)
            .collect();

        formatter.write_str(&rendered.join(", "))
    }
}

/// Refuses a chain the binary cannot serve as configured.
pub fn check_enablement(
    chain: Chain,
    lifecycle: ChainLifecycle,
    is_trading: bool,
    assets: Option<&ChainAssets>,
) -> Result<(), ChainEnablementError> {
    if lifecycle == ChainLifecycle::ObserveOnly
        && let Some(assets) = assets
        && let Some((symbol, _)) = assets
            .equities
            .symbols
            .iter()
            .find(|(_, equity)| equity.trading == OperationMode::Enabled)
    {
        return Err(ChainEnablementError::ObserveOnlyWithTrading {
            chain,
            symbol: symbol.clone(),
        });
    }

    let provided = provided_capabilities(chain);
    let missing: Vec<ChainCapability> = required_capabilities(lifecycle, is_trading, assets)
        .into_iter()
        .filter(|capability| !provided.contains(capability))
        .collect();

    if missing.is_empty() {
        return Ok(());
    }

    Err(ChainEnablementError::MissingCapabilities {
        chain,
        lifecycle,
        missing: MissingCapabilities(missing),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assets::ChainEquityAsset;

    #[test]
    fn lifecycle_names_are_pinned_literals() {
        for (raw, expected) in [
            ("\"disabled\"", ChainLifecycle::Disabled),
            ("\"observe-only\"", ChainLifecycle::ObserveOnly),
            ("\"prefunded\"", ChainLifecycle::Prefunded),
            ("\"active\"", ChainLifecycle::Active),
        ] {
            assert_eq!(
                serde_json::from_str::<ChainLifecycle>(raw).unwrap(),
                expected
            );
            assert_eq!(format!("\"{expected}\""), raw);
        }
    }

    #[test]
    fn base_can_serve_every_lifecycle() {
        for lifecycle in [
            ChainLifecycle::Disabled,
            ChainLifecycle::ObserveOnly,
            ChainLifecycle::Prefunded,
            ChainLifecycle::Active,
        ] {
            check_enablement(Chain::Base, lifecycle, true, None).unwrap();
        }
    }

    /// The predicate's whole point: a config that reads as reasonable is
    /// refused with the specific gaps named, instead of starting and failing
    /// one runtime path at a time.
    #[test]
    fn hyperevm_cannot_be_active_because_it_pays_gas_in_hype() {
        let error =
            check_enablement(Chain::HyperEvm, ChainLifecycle::Active, false, None).unwrap_err();

        let ChainEnablementError::MissingCapabilities {
            chain,
            lifecycle,
            missing,
        } = error
        else {
            panic!("expected MissingCapabilities, got: {error:?}")
        };

        assert_eq!(chain, Chain::HyperEvm);
        assert_eq!(lifecycle, ChainLifecycle::Active);
        assert_eq!(
            missing.into_inner(),
            vec![ChainCapability::GasValuation],
            "HyperEVM's native token is not ETH, so gas valuation is the gap"
        );
    }

    #[test]
    fn hyperevm_cannot_trade_because_no_watcher_is_wired_for_it() {
        let error =
            check_enablement(Chain::HyperEvm, ChainLifecycle::Prefunded, true, None).unwrap_err();

        let ChainEnablementError::MissingCapabilities { missing, .. } = error else {
            panic!("expected MissingCapabilities, got: {error:?}")
        };

        assert_eq!(
            missing.into_inner(),
            vec![ChainCapability::FillIngestion, ChainCapability::Hedging],
            "a trading HyperEVM is refused for both gaps"
        );
    }

    #[test]
    fn ethereum_may_hold_funds_and_watch_fills_but_not_rebalance_equity() {
        check_enablement(Chain::Ethereum, ChainLifecycle::Active, false, None).unwrap();

        // Per-chain watchers + chain-aware accounting granted FillIngestion
        // and Hedging, so a trading Ethereum passes...
        check_enablement(Chain::Ethereum, ChainLifecycle::Prefunded, true, None).unwrap();

        // ...but equity rebalancing stays Base-only: an Ethereum asset
        // flagged for rebalancing is still refused.
        let mut assets = ChainAssets::default();
        assets.equities.symbols.insert(
            Symbol::new("AAPL").unwrap(),
            ChainEquityAsset {
                tokenized_equity: alloy::primitives::Address::repeat_byte(0x11),
                tokenized_equity_derivative: alloy::primitives::Address::ZERO,
                vault_ids: vec![],
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Enabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                operational_limit: None,
            },
        );
        let error = check_enablement(Chain::Ethereum, ChainLifecycle::Active, true, Some(&assets))
            .unwrap_err();

        let ChainEnablementError::MissingCapabilities { missing, .. } = error else {
            panic!("expected MissingCapabilities, got: {error:?}")
        };
        assert_eq!(
            missing.into_inner(),
            vec![ChainCapability::EquityRebalancing],
            "equity rebalancing wiring is still Base-only"
        );
    }

    /// The guard the state exists for: an observe-only chain records fills it
    /// will never hedge the moment one of its assets trades.
    #[test]
    fn observe_only_with_a_trading_asset_is_refused_by_name() {
        let mut assets = ChainAssets::default();
        assets.equities.symbols.insert(
            Symbol::new("AAPL").unwrap(),
            crate::ChainEquityAsset {
                tokenized_equity: alloy::primitives::Address::ZERO,
                tokenized_equity_derivative: alloy::primitives::Address::ZERO,
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                operational_limit: None,
            },
        );

        let error = check_enablement(
            Chain::Base,
            ChainLifecycle::ObserveOnly,
            true,
            Some(&assets),
        )
        .unwrap_err();

        let ChainEnablementError::ObserveOnlyWithTrading { chain, symbol } = error else {
            panic!("expected ObserveOnlyWithTrading, got: {error:?}")
        };
        assert_eq!(chain, Chain::Base);
        assert_eq!(symbol, Symbol::new("AAPL").unwrap());
    }

    /// The rebalancing capabilities are driven by the per-asset flags: an
    /// active chain that rebalances demands the wiring, and a chain without
    /// it is refused for exactly that capability.
    #[test]
    fn rebalancing_assets_demand_their_capabilities() {
        let mut assets = ChainAssets::default();
        assets.equities.symbols.insert(
            Symbol::new("AAPL").unwrap(),
            crate::ChainEquityAsset {
                tokenized_equity: alloy::primitives::Address::ZERO,
                tokenized_equity_derivative: alloy::primitives::Address::ZERO,
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Enabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                operational_limit: None,
            },
        );
        assets.cash = Some(crate::ChainCashAsset {
            vault_ids: Vec::new(),
            rebalancing: OperationMode::Enabled,
            operational_limit: None,
        });

        let required = required_capabilities(ChainLifecycle::Active, true, Some(&assets));
        assert!(required.contains(&ChainCapability::EquityRebalancing));
        assert!(required.contains(&ChainCapability::CashRebalancing));

        // Base provides both, so the same configuration passes there.
        check_enablement(Chain::Base, ChainLifecycle::Active, true, Some(&assets)).unwrap();
    }

    #[test]
    fn a_disabled_chain_needs_nothing() {
        for chain in Chain::ALL {
            assert!(required_capabilities(ChainLifecycle::Disabled, true, None).is_empty());
            check_enablement(chain, ChainLifecycle::Disabled, true, None).unwrap();
        }
    }

    #[test]
    fn active_without_rebalancing_assets_needs_no_rebalancing_wiring() {
        let required = required_capabilities(ChainLifecycle::Active, true, None);

        assert!(!required.contains(&ChainCapability::EquityRebalancing));
        assert!(!required.contains(&ChainCapability::CashRebalancing));
    }
}
