//! Configuration for valuing bot-paid gas costs in USD.
//!
//! See ADR 0017: ETH/USD valuation reads Pyth's `getPriceUnsafe` on Base,
//! pinned to a block. Both fields are public values (not secrets) and are
//! required whenever rebalancing is enabled -- bot-gas cost recording only
//! runs on rebalancing paths, and it cannot produce a valid valuation
//! without a configured source, so a missing section must fail startup
//! rather than silently skip cost recording.

use alloy::primitives::{Address, B256};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BotGasValuationConfig {
    /// Pyth oracle contract address on Base.
    pub pyth_contract: Address,
    /// Pyth ETH/USD price feed id (full 32-byte hex).
    pub eth_usd_feed_id: B256,
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256};

    use super::*;

    #[test]
    fn parses_valid_config() {
        let config: BotGasValuationConfig = toml::from_str(
            r#"
            pyth_contract = "0x8250f4aF4B972684F7b336503E2D6dFeDeB1487a"
            eth_usd_feed_id = "0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace"
            "#,
        )
        .unwrap();

        assert_eq!(
            config.pyth_contract,
            address!("0x8250f4aF4B972684F7b336503E2D6dFeDeB1487a")
        );
        assert_eq!(
            config.eth_usd_feed_id,
            b256!("0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace")
        );
    }

    #[test]
    fn missing_pyth_contract_fails_to_parse() {
        let result: Result<BotGasValuationConfig, _> = toml::from_str(
            r#"eth_usd_feed_id = "0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace""#,
        );

        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("missing field `pyth_contract`"),
            "expected missing-field error for pyth_contract, got: {error}"
        );
    }

    #[test]
    fn missing_eth_usd_feed_id_fails_to_parse() {
        let result: Result<BotGasValuationConfig, _> =
            toml::from_str(r#"pyth_contract = "0x8250f4aF4B972684F7b336503E2D6dFeDeB1487a""#);

        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("missing field `eth_usd_feed_id`"),
            "expected missing-field error for eth_usd_feed_id, got: {error}"
        );
    }

    #[test]
    fn rejects_unknown_fields() {
        let result: Result<BotGasValuationConfig, _> = toml::from_str(
            r#"
            pyth_contract = "0x8250f4aF4B972684F7b336503E2D6dFeDeB1487a"
            eth_usd_feed_id = "0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace"
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
