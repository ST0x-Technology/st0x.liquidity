//! The route a cash transfer takes between Alpaca and one chain's cash vault.
//!
//! Persisted on every `UsdcRebalance`, so the wire names are pinned: renaming a
//! variant or a hop kind is a data migration, not a refactor.

use alloy::primitives::{Address, Selector};
#[cfg(all(feature = "cctp", feature = "relay"))]
use alloy::sol_types::SolCall;
use serde::{Deserialize, Serialize};
use st0x_evm::Chain;
#[cfg(all(feature = "cctp", feature = "relay"))]
use st0x_evm::IERC20;

#[cfg(all(feature = "cctp", feature = "relay"))]
use crate::cctp::{
    MESSAGE_TRANSMITTER_V2, MessageTransmitterV2, TOKEN_MESSENGER_V2, TokenMessengerV2,
};
#[cfg(all(feature = "cctp", feature = "relay"))]
use crate::relay::depositErc20Call;

/// How USDC crosses between a corridor chain and the Ethereum hub.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HopKind {
    /// Circle CCTP burn and mint.
    Cctp,
    /// Relay's USDG -> USDC swap (Robinhood).
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

#[cfg(all(feature = "cctp", feature = "relay"))]
impl UsdcCorridor {
    /// Every transaction shape the hop signs on either end: the corridor
    /// chain's, then the Ethereum hub's. Both directions run through one
    /// corridor, so each end sends what either direction's origin sends, and
    /// on CCTP what its destination sends too.
    pub fn hop_calls(self) -> Result<Vec<CorridorCall>, NoRelayDepository> {
        let Self::HubRouted { chain, hop } = self;
        let mut calls = Vec::new();

        for end in [chain, Chain::Ethereum] {
            calls.extend(end_calls(end, hop)?);
        }

        Ok(calls)
    }
}

/// A Relay hop named on a chain where Relay has no pinned depository.
#[cfg(all(feature = "cctp", feature = "relay"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Relay has no pinned depository on {chain}")]
pub struct NoRelayDepository {
    pub chain: Chain,
}

#[cfg(all(feature = "cctp", feature = "relay"))]
fn end_calls(end: Chain, hop: HopKind) -> Result<Vec<CorridorCall>, NoRelayDepository> {
    let stable = end.settlement_stable().address;
    let approve = |spender| CorridorCall {
        chain: end,
        to: stable,
        selector: IERC20::approveCall::SELECTOR.into(),
        first_argument: FirstArgument::Address(spender),
        function: "approve",
    };

    match hop {
        HopKind::Cctp => Ok(vec![
            approve(TOKEN_MESSENGER_V2),
            CorridorCall {
                chain: end,
                to: TOKEN_MESSENGER_V2,
                selector: TokenMessengerV2::depositForBurnCall::SELECTOR.into(),
                first_argument: FirstArgument::NotAnAddress,
                function: "depositForBurn",
            },
            CorridorCall {
                chain: end,
                to: MESSAGE_TRANSMITTER_V2,
                selector: MessageTransmitterV2::receiveMessageCall::SELECTOR.into(),
                first_argument: FirstArgument::NotAnAddress,
                function: "receiveMessage",
            },
        ]),
        HopKind::Relay => {
            let depository = end
                .relay_depository()
                .ok_or(NoRelayDepository { chain: end })?;

            // Relay's solver pays the fill or refund: the destination end
            // signs nothing to receive it.
            Ok(vec![
                approve(depository),
                CorridorCall {
                    chain: end,
                    to: depository,
                    selector: depositErc20Call::SELECTOR.into(),
                    first_argument: FirstArgument::Signer,
                    function: "depositErc20",
                },
            ])
        }
    }
}

/// One transaction a corridor signs, a hop call or a cash-leg call: its chain,
/// target contract, selector, and what its first ABI word holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorridorCall {
    pub chain: Chain,
    pub to: Address,
    pub selector: Selector,
    pub first_argument: FirstArgument,
    pub function: &'static str,
}

/// What a call's first ABI word holds, for policies that constrain it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirstArgument {
    /// A fixed address, such as an approve's spender.
    Address(Address),
    /// The signing wallet's own address.
    Signer,
    /// An address only a run knows, such as a deposit address it fetches.
    ChosenAtRunTime,
    /// An amount, an offset, or anything else that is not an address.
    NotAnAddress,
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

    /// Every call each end signs, pinned to literal addresses and selectors
    /// so a drift in the bridge's contracts shows up here and at the gate.
    #[cfg(all(feature = "cctp", feature = "relay"))]
    #[test]
    fn hop_calls_list_every_call_on_both_ends() {
        use alloy::primitives::{address, fixed_bytes};

        let messenger = address!("0x28b5a0e9C621a5BadaA536219b3a228C8168cf5d");
        let transmitter = address!("0x81D40F21F12A8F0E3252Bccb954D722d4c464B64");
        let depository = address!("0x4cd00e387622c35bddb9b4c962c136462338bc31");
        let usdc_base = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
        let usdc_ethereum = address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let usdg = address!("0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168");
        let approve = fixed_bytes!("0x095ea7b3");
        let cctp_end = |chain, usdc| {
            [
                CorridorCall {
                    chain,
                    to: usdc,
                    selector: approve,
                    first_argument: FirstArgument::Address(messenger),
                    function: "approve",
                },
                CorridorCall {
                    chain,
                    to: messenger,
                    selector: fixed_bytes!("0x8e0250ee"),
                    first_argument: FirstArgument::NotAnAddress,
                    function: "depositForBurn",
                },
                CorridorCall {
                    chain,
                    to: transmitter,
                    selector: fixed_bytes!("0x57ecfd28"),
                    first_argument: FirstArgument::NotAnAddress,
                    function: "receiveMessage",
                },
            ]
        };
        let relay_end = |chain, stable| {
            [
                CorridorCall {
                    chain,
                    to: stable,
                    selector: approve,
                    first_argument: FirstArgument::Address(depository),
                    function: "approve",
                },
                CorridorCall {
                    chain,
                    to: depository,
                    selector: fixed_bytes!("0xe8017952"),
                    first_argument: FirstArgument::Signer,
                    function: "depositErc20",
                },
            ]
        };

        assert_eq!(
            UsdcCorridor::BASE_CCTP.hop_calls().unwrap(),
            [
                cctp_end(Chain::Base, usdc_base),
                cctp_end(Chain::Ethereum, usdc_ethereum),
            ]
            .concat()
        );
        assert_eq!(
            UsdcCorridor::HubRouted {
                chain: Chain::Robinhood,
                hop: HopKind::Relay,
            }
            .hop_calls()
            .unwrap(),
            [
                relay_end(Chain::Robinhood, usdg),
                relay_end(Chain::Ethereum, usdc_ethereum),
            ]
            .concat()
        );
    }

    #[cfg(all(feature = "cctp", feature = "relay"))]
    #[test]
    fn relay_hop_on_a_chain_without_a_depository_is_refused() {
        assert_eq!(
            UsdcCorridor::HubRouted {
                chain: Chain::Base,
                hop: HopKind::Relay,
            }
            .hop_calls()
            .unwrap_err(),
            NoRelayDepository { chain: Chain::Base }
        );
    }
}
