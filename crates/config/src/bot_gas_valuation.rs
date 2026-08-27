//! Configuration for valuing bot-paid gas costs in USD.
//!
//! See ADR 0020: ETH/USD valuation reads Chainlink's standard proxy on Base,
//! pinned to a block. The address is public (not a secret) and is
//! required whenever rebalancing is enabled -- bot-gas cost recording only
//! runs on rebalancing paths, and it cannot produce a valid valuation
//! without a configured source, so a missing section must fail startup
//! rather than silently skip cost recording.

use alloy::primitives::Address;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BotGasValuationConfig {
    /// Chainlink standard ETH/USD proxy address on Base.
    pub chainlink_feed: Address,
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;

    use super::*;

    #[test]
    fn parses_valid_config() {
        let config: BotGasValuationConfig = toml::from_str(
            r#"
            chainlink_feed = "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70"
            "#,
        )
        .unwrap();

        assert_eq!(
            config.chainlink_feed,
            address!("0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70")
        );
    }

    #[test]
    fn missing_chainlink_feed_fails_to_parse() {
        let result: Result<BotGasValuationConfig, _> = toml::from_str("");

        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("missing field `chainlink_feed`"),
            "expected missing-field error for chainlink_feed, got: {error}"
        );
    }

    #[test]
    fn rejects_unknown_fields() {
        let result: Result<BotGasValuationConfig, _> = toml::from_str(
            r#"
            chainlink_feed = "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70"
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
