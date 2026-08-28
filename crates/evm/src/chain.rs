//! The closed set of EVM chains this bot can operate on.
//!
//! Chain identity used to be a shape rather than a value: a struct field per
//! chain, a newtype per chain, and string literals in config validators. One
//! enum replaces those, so "which chain" can be stored, matched, and keyed on.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

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
}

impl Chain {
    /// Every variant, so callers can enumerate chains without a match that
    /// silently misses one added later.
    pub const ALL: [Self; 3] = [Self::Base, Self::Ethereum, Self::HyperEvm];

    /// The chain id the network reports over RPC.
    ///
    /// Held on the type rather than read from config: a config-supplied chain
    /// id validates nothing, because it is the value being checked.
    pub const fn chain_id(self) -> u64 {
        match self {
            Self::Base => 8453,
            Self::Ethereum => 1,
            Self::HyperEvm => 999,
        }
    }

    /// The pinned wire name. Shared by [`fmt::Display`] and [`FromStr`] so the
    /// two cannot drift from each other or from serde.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::Ethereum => "ethereum",
            Self::HyperEvm => "hyperevm",
        }
    }
}

impl fmt::Display for Chain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("expected chain 'base', 'ethereum' or 'hyperevm'")]
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
    }

    #[test]
    fn chain_ids_are_pinned_literals() {
        assert_eq!(Chain::Base.chain_id(), 8453);
        assert_eq!(Chain::Ethereum.chain_id(), 1);
        assert_eq!(Chain::HyperEvm.chain_id(), 999);
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
