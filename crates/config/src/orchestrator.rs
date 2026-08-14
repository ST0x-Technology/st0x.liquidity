//! Configuration for the ST0xOrchestrator contracts.
//!
//! Orchestrator-mode mints require a signed `MintAuthV1` recipient
//! authorization, and producing one needs the orchestrator's address (the
//! EIP-712 verifying contract, read via its `mintAuthDigest` view). Which
//! assets are orchestrator-mode is owned by the issuance bot (the
//! `vault_mode` field on its per-asset status endpoint) -- this section only
//! supplies contract addresses, never an asset list.
//!
//! Each chain carries its own orchestrator deployment, so addresses are
//! keyed per network under `[orchestrator.addresses]`, mirroring the
//! issuance bot's config shape. Mint authorization signs against the chain
//! the mint's tokenized equity lives on -- Base for every equity today, so
//! the authorizer resolves the `base` entry.
//!
//! The section is optional as a whole: while every asset is vault-direct the
//! bot runs unchanged without it ("deploys dark"). A mint that discovers an
//! orchestrator-mode asset with no configured address must fail loudly at
//! that point, never guess. When the section IS present, an empty map, an
//! unknown network key, or a malformed or zero address fails at parse time
//! -- even while dark -- so `validate-config` catches a typo'd deploy before
//! the cutover depends on it.

use alloy::primitives::Address;
use serde::Deserialize;

/// `[orchestrator]` section of the plaintext config TOML.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "OrchestratorConfigToml")]
pub struct OrchestratorConfig {
    pub addresses: OrchestratorAddresses,
}

/// Per-network orchestrator contract addresses from
/// `[orchestrator.addresses]`. A network without a deployment simply has no
/// entry; validation guarantees at least one entry and no zero addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrchestratorAddresses {
    pub base: Option<Address>,
    pub ethereum: Option<Address>,
}

#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    /// The zero address can never be a deployed orchestrator; a config carrying
    /// it is a placeholder that slipped through, so parsing fails closed.
    #[error("[orchestrator.addresses] {network} must not be the zero address")]
    ZeroOrchestratorAddress { network: &'static str },
    /// A present section with no entries supplies nothing an orchestrator-mode
    /// mint could sign against; require at least one so the section is never
    /// dead weight that reads as configured.
    #[error("[orchestrator.addresses] must carry at least one network entry")]
    NoAddresses,
}

/// Raw TOML shape for `[orchestrator]`; validation lives in the
/// `TryFrom<OrchestratorConfigToml>` conversion so an invalid value can
/// never become an [`OrchestratorConfig`] through parsing.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OrchestratorConfigToml {
    addresses: OrchestratorAddressesToml,
}

/// Known network keys only: an unknown network (or the retired top-level
/// `address` key) is rejected by `deny_unknown_fields` instead of silently
/// configuring nothing.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OrchestratorAddressesToml {
    base: Option<Address>,
    ethereum: Option<Address>,
}

impl TryFrom<OrchestratorConfigToml> for OrchestratorConfig {
    type Error = OrchestratorError;

    fn try_from(raw: OrchestratorConfigToml) -> Result<Self, Self::Error> {
        let OrchestratorAddressesToml { base, ethereum } = raw.addresses;

        for (network, address) in [("base", base), ("ethereum", ethereum)] {
            if address == Some(Address::ZERO) {
                return Err(OrchestratorError::ZeroOrchestratorAddress { network });
            }
        }

        if base.is_none() && ethereum.is_none() {
            return Err(OrchestratorError::NoAddresses);
        }

        Ok(Self {
            addresses: OrchestratorAddresses { base, ethereum },
        })
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;

    use super::*;

    #[test]
    fn parses_valid_config() {
        let config: OrchestratorConfig = toml::from_str(
            r#"[addresses]
            base = "0x4444444444444444444444444444444444444444""#,
        )
        .unwrap();

        assert_eq!(
            config.addresses.base,
            Some(address!("0x4444444444444444444444444444444444444444"))
        );
        assert_eq!(config.addresses.ethereum, None);
    }

    #[test]
    fn parses_an_address_per_network() {
        let config: OrchestratorConfig = toml::from_str(
            r#"[addresses]
            base = "0x4444444444444444444444444444444444444444"
            ethereum = "0x5555555555555555555555555555555555555555""#,
        )
        .unwrap();

        assert_eq!(
            config.addresses.base,
            Some(address!("0x4444444444444444444444444444444444444444"))
        );
        assert_eq!(
            config.addresses.ethereum,
            Some(address!("0x5555555555555555555555555555555555555555"))
        );
    }

    #[test]
    fn rejects_zero_base_address() {
        let result: Result<OrchestratorConfig, _> = toml::from_str(
            r#"[addresses]
            base = "0x0000000000000000000000000000000000000000""#,
        );

        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("base must not be the zero address"),
            "expected zero-address rejection, got: {error}"
        );
    }

    /// Each network's zero check is an independent branch; a valid base
    /// entry must not mask a zero ethereum entry.
    #[test]
    fn rejects_zero_ethereum_address() {
        let result: Result<OrchestratorConfig, _> = toml::from_str(
            r#"[addresses]
            base = "0x4444444444444444444444444444444444444444"
            ethereum = "0x0000000000000000000000000000000000000000""#,
        );

        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("ethereum must not be the zero address"),
            "expected zero-address rejection, got: {error}"
        );
    }

    #[test]
    fn rejects_empty_addresses() {
        let result: Result<OrchestratorConfig, _> = toml::from_str("[addresses]");

        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("must carry at least one network entry"),
            "expected empty-map rejection, got: {error}"
        );
    }

    #[test]
    fn malformed_address_fails_to_parse() {
        let result: Result<OrchestratorConfig, _> = toml::from_str(
            r#"[addresses]
            base = "not-an-address""#,
        );

        // The rendered toml error carries the offending source line, so
        // asserting on the exact rejected value pins THIS failure (a value
        // that does not parse as an address) apart from the zero-address
        // and missing-field rejections, which carry different messages.
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains(r#"base = "not-an-address""#),
            "expected the malformed-value parse failure, got: {error}"
        );
    }

    #[test]
    fn missing_addresses_fails_to_parse() {
        let result: Result<OrchestratorConfig, _> = toml::from_str("");

        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("missing field `addresses`"),
            "expected missing-field error for addresses, got: {error}"
        );
    }

    /// The pre-multichain `address` key must fail loudly, never parse as a
    /// section that silently dropped the address.
    #[test]
    fn rejects_the_retired_single_address_key() {
        let result: Result<OrchestratorConfig, _> =
            toml::from_str(r#"address = "0x4444444444444444444444444444444444444444""#);

        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unknown field `address`")
        );
    }

    #[test]
    fn rejects_unknown_network_keys() {
        let result: Result<OrchestratorConfig, _> = toml::from_str(
            r#"[addresses]
            base = "0x4444444444444444444444444444444444444444"
            solana = "0x5555555555555555555555555555555555555555"
            "#,
        );

        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unknown field `solana`")
        );
    }
}
