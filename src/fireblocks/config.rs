use std::collections::HashMap;
use std::path::PathBuf;

use clap::Parser;

/// Environment configuration for Fireblocks signing.
#[derive(Parser, Debug, Clone)]
pub(crate) struct FireblocksEnv {
    /// Fireblocks API key
    #[clap(long = "fireblocks-api-key", env = "FIREBLOCKS_API_KEY")]
    pub(crate) api_key: String,

    /// Path to the RSA private key file for Fireblocks API authentication
    #[clap(long = "fireblocks-secret-path", env = "FIREBLOCKS_SECRET_PATH")]
    pub(crate) secret_path: PathBuf,

    /// Fireblocks vault account ID containing the signing key
    #[clap(
        long = "fireblocks-vault-account-id",
        env = "FIREBLOCKS_VAULT_ACCOUNT_ID"
    )]
    pub(crate) vault_account_id: String,

    /// Mapping of chain ID to Fireblocks asset ID, e.g. "1:ETH,8453:BASECHAIN_ETH".
    /// The first entry is also used as the default for address lookup.
    #[clap(
        long = "fireblocks-chain-asset-ids",
        env = "FIREBLOCKS_CHAIN_ASSET_IDS",
        default_value = "1:ETH",
        value_parser = parse_chain_asset_ids,
    )]
    pub(crate) chain_asset_ids: ChainAssetIds,

    /// Use Fireblocks sandbox environment
    #[clap(long = "fireblocks-sandbox", env = "FIREBLOCKS_SANDBOX", default_value = "false", action = clap::ArgAction::Set)]
    pub(crate) sandbox: bool,
}

/// Parsed chain ID to Fireblocks asset ID mapping.
#[derive(Debug, Clone)]
pub(crate) struct ChainAssetIds {
    map: HashMap<u64, String>,
    /// The first asset ID in the mapping, used as default for address lookup.
    default_asset_id: String,
}

impl ChainAssetIds {
    pub(crate) fn get(&self, chain_id: u64) -> Option<&str> {
        self.map.get(&chain_id).map(String::as_str)
    }

    pub(crate) fn default_asset_id(&self) -> &str {
        &self.default_asset_id
    }
}

pub(crate) fn parse_chain_asset_ids(s: &str) -> Result<ChainAssetIds, String> {
    let mut map = HashMap::new();
    let mut default_asset_id = None;

    for pair in s.split(',') {
        let pair = pair.trim();
        let (chain_str, asset_id) = pair.split_once(':').ok_or_else(|| {
            format!("invalid chain:asset pair: {pair:?}, expected format like 1:ETH")
        })?;

        let chain_id: u64 = chain_str
            .trim()
            .parse()
            .map_err(|_| format!("invalid chain ID: {chain_str:?}"))?;

        let asset_id = asset_id.trim().to_string();

        if default_asset_id.is_none() {
            default_asset_id = Some(asset_id.clone());
        }

        map.insert(chain_id, asset_id);
    }

    let default_asset_id =
        default_asset_id.ok_or_else(|| "at least one chain:asset pair is required".to_string())?;

    Ok(ChainAssetIds {
        map,
        default_asset_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_chain() {
        let ids = parse_chain_asset_ids("1:ETH").unwrap();
        assert_eq!(ids.get(1).unwrap(), "ETH");
        assert_eq!(ids.default_asset_id(), "ETH");
        assert!(ids.get(8453).is_none());
    }

    #[test]
    fn parse_multiple_chains() {
        let ids = parse_chain_asset_ids("1:ETH,8453:BASECHAIN_ETH").unwrap();
        assert_eq!(ids.get(1).unwrap(), "ETH");
        assert_eq!(ids.get(8453).unwrap(), "BASECHAIN_ETH");
        assert_eq!(ids.default_asset_id(), "ETH");
    }

    #[test]
    fn parse_with_whitespace() {
        let ids = parse_chain_asset_ids(" 1 : ETH , 8453 : BASECHAIN_ETH ").unwrap();
        assert_eq!(ids.get(1).unwrap(), "ETH");
        assert_eq!(ids.get(8453).unwrap(), "BASECHAIN_ETH");
    }

    #[test]
    fn parse_invalid_format() {
        let result = parse_chain_asset_ids("ETH");
        assert!(result.is_err());
    }

    #[test]
    fn parse_invalid_chain_id() {
        let result = parse_chain_asset_ids("abc:ETH");
        assert!(result.is_err());
    }

    #[test]
    fn parse_empty_string() {
        let result = parse_chain_asset_ids("");
        assert!(result.is_err());
    }
}
