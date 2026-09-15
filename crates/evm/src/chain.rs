//! The closed set of EVM chains this bot can operate on.
//!
//! Chain identity used to be a shape rather than a value: a struct field per
//! chain, a newtype per chain, and string literals in config validators. One
//! enum replaces those, so "which chain" can be stored, matched, and keyed on.

use std::fmt;
use std::str::FromStr;

use alloy::primitives::Address;
use serde::{Deserialize, Serialize};

use crate::tokens::{USDC_BASE, USDC_ETHEREUM, USDC_HYPEREVM, USDC_ROBINHOOD};

/// An EVM chain the bot acts on.
///
/// The wire names are pinned: they appear in persisted event payloads, in
/// aggregate ids, and in config keys, so renaming a variant is a data
/// migration rather than a refactor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Chain {
    Base,
    Ethereum,
    /// `rename_all = "snake_case"` would spell this `hyper_evm`. The wire name
    /// is `hyperevm`, matching the name the issuer and the broker use for the
    /// same chain.
    #[serde(rename = "hyperevm")]
    HyperEvm,
    /// Robinhood Chain, an Arbitrum Orbit L2 that pays gas in ETH. The
    /// `rename_all = "snake_case"` spelling is already `robinhood`, the name
    /// the issuer and the broker use for the same chain.
    Robinhood,
}

impl Chain {
    /// Every variant, so callers can enumerate chains without a match that
    /// silently misses one added later.
    pub const ALL: [Self; 4] = [Self::Base, Self::Ethereum, Self::HyperEvm, Self::Robinhood];

    /// The chain id the network reports over RPC.
    ///
    /// Held on the type rather than read from config: a config-supplied chain
    /// id validates nothing, because it is the value being checked.
    pub const fn chain_id(self) -> u64 {
        match self {
            Self::Base => 8453,
            Self::Ethereum => 1,
            Self::HyperEvm => 999,
            Self::Robinhood => 4663,
        }
    }

    /// The canonical USDC contract on this chain. USDC differs per chain, so a
    /// new variant cannot compile until its own contract is pinned here.
    pub const fn usdc(self) -> Address {
        match self {
            Self::Base => USDC_BASE,
            Self::Ethereum => USDC_ETHEREUM,
            Self::HyperEvm => USDC_HYPEREVM,
            Self::Robinhood => USDC_ROBINHOOD,
        }
    }

    /// The stablecoin this chain's cash leg settles in. Pinned in code, not
    /// config: a config-supplied value would let a typo point fill validation
    /// at the wrong token. A new variant cannot compile until its stable is
    /// pinned here.
    pub const fn settlement_stable(self) -> SettlementStable {
        match self {
            Self::Base => SettlementStable {
                address: USDC_BASE,
                symbol: "USDC",
                decimals: 6,
            },
            Self::Ethereum => SettlementStable {
                address: USDC_ETHEREUM,
                symbol: "USDC",
                decimals: 6,
            },
            Self::HyperEvm => SettlementStable {
                address: USDC_HYPEREVM,
                symbol: "USDC",
                decimals: 6,
            },
        }
    }

    /// Circle's USDC on this chain, the only token CCTP burns and mints:
    /// `Some` exactly where the settlement stable is that USDC, so the bridge
    /// is never handed a stable it cannot carry.
    pub const fn cctp_usdc(self) -> Option<Address> {
        match self {
            Self::Base => Some(USDC_BASE),
            Self::Ethereum => Some(USDC_ETHEREUM),
            Self::HyperEvm => Some(USDC_HYPEREVM),
        }
    }

    /// The pinned wire name. Shared by [`fmt::Display`] and [`FromStr`] so the
    /// two cannot drift from each other or from serde.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::Ethereum => "ethereum",
            Self::HyperEvm => "hyperevm",
            Self::Robinhood => "robinhood",
        }
    }
}

/// The token a chain's cash leg settles in: the quote token fill validation
/// accepts, the token the cash vault holds, and the label the inventory
/// surfaces show for that balance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettlementStable {
    pub address: Address,
    /// The ticker the token reports on chain and the dashboard displays.
    pub symbol: &'static str,
    /// The ERC-20 decimals: the grid an on-chain transfer truncates to.
    pub decimals: u8,
}

impl fmt::Display for Chain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("expected chain 'base', 'ethereum', 'hyperevm' or 'robinhood'")]
pub struct ParseChainError;

impl FromStr for Chain {
    type Err = ParseChainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|chain| chain.as_str() == value)
            .ok_or(ParseChainError)
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;

    use super::*;

    /// The three encoders of a chain name -- serde, `Display` and `FromStr` --
    /// are independent, and `snake_case` disagrees with the pinned spelling of
    /// `HyperEvm`. Pin all three against each other for every variant so a new
    /// variant cannot land with only some of them right.
    #[test]
    fn every_chain_serializes_displays_and_parses_to_the_same_name() {
        for chain in Chain::ALL {
            let serialized = serde_json::to_string(&chain).unwrap();
            let expected = format!("\"{}\"", chain.as_str());

            assert_eq!(
                serialized, expected,
                "serde name for {chain:?} disagrees with as_str"
            );
            assert_eq!(
                chain.to_string(),
                chain.as_str(),
                "Display for {chain:?} disagrees with as_str"
            );
            assert_eq!(
                chain.as_str().parse::<Chain>().unwrap(),
                chain,
                "FromStr for {chain:?} does not round-trip"
            );
            assert_eq!(
                serde_json::from_str::<Chain>(&serialized).unwrap(),
                chain,
                "serde does not round-trip {chain:?}"
            );
        }
    }

    /// The wire names are persisted, so they are asserted as literals rather
    /// than re-derived from the type under test.
    #[test]
    fn wire_names_are_pinned_literals() {
        assert_eq!(serde_json::to_string(&Chain::Base).unwrap(), "\"base\"");
        assert_eq!(
            serde_json::to_string(&Chain::Ethereum).unwrap(),
            "\"ethereum\""
        );
        assert_eq!(
            serde_json::to_string(&Chain::HyperEvm).unwrap(),
            "\"hyperevm\""
        );
        assert_eq!(
            serde_json::to_string(&Chain::Robinhood).unwrap(),
            "\"robinhood\""
        );
    }

    /// The settlement stable is what fill validation, vault polling and the
    /// inventory surfaces read, so each chain's is asserted as literals
    /// rather than re-derived from the constants the accessor reads.
    #[test]
    fn settlement_stables_are_pinned_literals() {
        assert_eq!(
            Chain::Base.settlement_stable(),
            SettlementStable {
                address: address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
                symbol: "USDC",
                decimals: 6,
            }
        );
        assert_eq!(
            Chain::Ethereum.settlement_stable(),
            SettlementStable {
                address: address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
                symbol: "USDC",
                decimals: 6,
            }
        );
        assert_eq!(
            Chain::HyperEvm.settlement_stable(),
            SettlementStable {
                address: address!("0xb88339CB7199b77E23DB6E890353E22632Ba630f"),
                symbol: "USDC",
                decimals: 6,
            }
        );
    }

    /// CCTP carries Circle's USDC alone, so the bridge accessor is `Some`
    /// exactly on the chains whose settlement stable is that USDC, and it
    /// names the same contract.
    #[test]
    fn cctp_usdc_is_some_exactly_where_the_stable_is_circles_usdc() {
        for chain in Chain::ALL {
            let stable = chain.settlement_stable();
            let expected = (stable.symbol == "USDC").then_some(stable.address);

            assert_eq!(chain.cctp_usdc(), expected, "cctp_usdc for {chain:?}");
        }

        assert_eq!(
            Chain::ALL.map(Chain::cctp_usdc),
            [
                Some(address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")),
                Some(address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")),
                Some(address!("0xb88339CB7199b77E23DB6E890353E22632Ba630f")),
            ]
        );
    }

    #[test]
    fn robinhood_usdc_is_the_canonical_contract() {
        assert_eq!(
            Chain::Robinhood.usdc(),
            alloy::primitives::address!("0x80e0e24718dbFcad49ECAA6F1e6C89A190586cA8")
        );
    }

    #[test]
    fn chain_ids_are_pinned_literals() {
        assert_eq!(Chain::Base.chain_id(), 8453);
        assert_eq!(Chain::Ethereum.chain_id(), 1);
        assert_eq!(Chain::HyperEvm.chain_id(), 999);
        assert_eq!(Chain::Robinhood.chain_id(), 4663);
    }

    #[test]
    fn snake_case_spelling_of_hyperevm_is_rejected() {
        let error = "hyper_evm".parse::<Chain>().unwrap_err();

        assert_eq!(error, ParseChainError);

        let serde_error = serde_json::from_str::<Chain>("\"hyper_evm\"").unwrap_err();
        assert!(
            serde_error.to_string().contains("unknown variant"),
            "expected an unknown-variant error, got: {serde_error}"
        );
    }

    #[test]
    fn unknown_chain_name_is_rejected() {
        assert_eq!("solana".parse::<Chain>().unwrap_err(), ParseChainError);
    }
}
