//! The route a cash transfer takes between Alpaca and one chain's cash vault.
//!
//! Persisted on every `UsdcRebalance`, so the wire names are pinned: renaming a
//! variant or a hop kind is a data migration, not a refactor.

use serde::{Deserialize, Serialize};
use st0x_evm::Chain;

/// How USDC crosses between a corridor chain and the Ethereum hub.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HopKind {
    /// Circle CCTP burn and mint.
    Cctp,
    /// Reserved for Robinhood: no build wires it yet.
    Relay,
}

impl std::fmt::Display for HopKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cctp => formatter.write_str("cctp"),
            Self::Relay => formatter.write_str("relay"),
        }
    }
}

/// The route a cash transfer takes between Alpaca and one chain's vault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UsdcCorridor {
    /// Chain vault <-> hop <-> Ethereum wallet <-> Alpaca.
    HubRouted { chain: Chain, hop: HopKind },
}

impl UsdcCorridor {
    /// Base via CCTP, the corridor this build wires.
    pub const BASE_CCTP: Self = Self::HubRouted {
        chain: Chain::Base,
        hop: HopKind::Cctp,
    };

    /// The chain whose cash vault the corridor serves.
    pub const fn chain(self) -> Chain {
        match self {
            Self::HubRouted { chain, .. } => chain,
        }
    }
}

impl std::fmt::Display for UsdcCorridor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HubRouted { chain, hop } => write!(formatter, "{chain} via {hop}"),
        }
    }
}

/// The corridor of a transfer recorded before corridors were: Base via CCTP,
/// the only route there was. For `#[serde(default = ...)]` on legacy payloads.
pub const fn legacy_base_cctp() -> UsdcCorridor {
    UsdcCorridor::BASE_CCTP
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn corridor_wire_shape_is_pinned() {
        assert_eq!(
            serde_json::to_value(UsdcCorridor::BASE_CCTP).unwrap(),
            json!({"HubRouted": {"chain": "base", "hop": "cctp"}})
        );
        assert_eq!(
            serde_json::to_value(UsdcCorridor::HubRouted {
                chain: Chain::Robinhood,
                hop: HopKind::Relay,
            })
            .unwrap(),
            json!({"HubRouted": {"chain": "robinhood", "hop": "relay"}})
        );
    }

    #[test]
    fn corridor_deserializes_from_its_wire_shape() {
        let corridor: UsdcCorridor =
            serde_json::from_value(json!({"HubRouted": {"chain": "robinhood", "hop": "relay"}}))
                .unwrap();

        assert_eq!(
            corridor,
            UsdcCorridor::HubRouted {
                chain: Chain::Robinhood,
                hop: HopKind::Relay,
            }
        );
    }

    #[test]
    fn legacy_corridor_is_base_via_cctp() {
        assert_eq!(
            legacy_base_cctp(),
            UsdcCorridor::HubRouted {
                chain: Chain::Base,
                hop: HopKind::Cctp,
            }
        );
    }

    #[test]
    fn corridor_displays_chain_and_hop() {
        assert_eq!(UsdcCorridor::BASE_CCTP.to_string(), "base via cctp");
    }
}
