//! Configuration for the ST0xOrchestrator contract.
//!
//! Orchestrator-mode mints require a signed `MintAuthV1` recipient
//! authorization, and producing one needs the orchestrator's address (the
//! EIP-712 verifying contract, read via its `mintAuthDigest` view). Which
//! assets are orchestrator-mode is owned by the issuance bot (the
//! `vault_mode` field on its per-asset status endpoint) -- this section only
//! supplies the contract address, never an asset list.
//!
//! The section is optional as a whole: while every asset is vault-direct the
//! bot runs unchanged without it ("deploys dark"). A mint that discovers an
//! orchestrator-mode asset with no configured address must fail loudly at
//! that point, never guess. When the section IS present, a malformed or zero
//! address fails at parse time -- even while dark -- so `validate-config`
//! catches a typo'd deploy before the cutover depends on it.

use alloy::primitives::Address;
use serde::Deserialize;

/// `[orchestrator]` section of the plaintext config TOML.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "OrchestratorConfigToml")]
pub struct OrchestratorConfig {
    pub address: Address,
}

#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    /// The zero address can never be a deployed orchestrator; a config carrying
    /// it is a placeholder that slipped through, so parsing fails closed.
    #[error("[orchestrator] address must not be the zero address")]
    ZeroOrchestratorAddress,
}

/// Raw TOML shape for `[orchestrator]`; validation lives in the
/// `TryFrom<OrchestratorConfigToml>` conversion so an invalid value can
/// never become an [`OrchestratorConfig`] through parsing.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OrchestratorConfigToml {
    address: Address,
}

impl TryFrom<OrchestratorConfigToml> for OrchestratorConfig {
    type Error = OrchestratorError;

    fn try_from(raw: OrchestratorConfigToml) -> Result<Self, Self::Error> {
        if raw.address == Address::ZERO {
            return Err(OrchestratorError::ZeroOrchestratorAddress);
        }

        Ok(Self {
            address: raw.address,
        })
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;

    use super::*;

    #[test]
    fn parses_valid_config() {
        let config: OrchestratorConfig =
            toml::from_str(r#"address = "0x4444444444444444444444444444444444444444""#).unwrap();

        assert_eq!(
            config.address,
            address!("0x4444444444444444444444444444444444444444")
        );
    }

    #[test]
    fn rejects_zero_address() {
        let result: Result<OrchestratorConfig, _> =
            toml::from_str(r#"address = "0x0000000000000000000000000000000000000000""#);

        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("address must not be the zero address"),
            "expected zero-address rejection, got: {error}"
        );
    }

    #[test]
    fn malformed_address_fails_to_parse() {
        let result: Result<OrchestratorConfig, _> = toml::from_str(r#"address = "not-an-address""#);

        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("address"),
            "expected an address-field parse failure, got: {error}"
        );
    }

    #[test]
    fn missing_address_fails_to_parse() {
        let result: Result<OrchestratorConfig, _> = toml::from_str("");

        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("missing field `address`"),
            "expected missing-field error for address, got: {error}"
        );
    }

    #[test]
    fn rejects_unknown_fields() {
        let result: Result<OrchestratorConfig, _> = toml::from_str(
            r#"
            address = "0x4444444444444444444444444444444444444444"
            unknown_field = "surprise"
            "#,
        );

        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unknown field `unknown_field`")
        );
    }
}
