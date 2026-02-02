use clap::Parser;
use std::collections::HashMap;
use std::num::ParseIntError;
use std::path::PathBuf;

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

#[derive(Debug, thiserror::Error)]
pub(crate) enum ChainAssetIdsParseError {
    #[error("invalid chain:asset pair: {pair:?}, expected format like 1:ETH")]
    InvalidFormat { pair: String },
    #[error("invalid chain ID: {value:?}")]
    InvalidChainId {
        value: String,
        #[source]
        source: ParseIntError,
    },
    #[error("empty asset ID for chain {chain_id}")]
    EmptyAssetId { chain_id: u64 },
    #[error("duplicate chain ID: {chain_id}")]
    DuplicateChainId { chain_id: u64 },
    #[error("at least one chain:asset pair is required")]
    EmptyInput,
}

// clap value_parser requires the error to be convertible to a boxed error,
// which thiserror's Display + Error impls satisfy. But clap also accepts
// `Fn(&str) -> Result<T, E>` where E: Into<Box<dyn Error + Send + Sync>>,
// and our thiserror enum satisfies that.
pub(crate) fn parse_chain_asset_ids(s: &str) -> Result<ChainAssetIds, ChainAssetIdsParseError> {
    let mut map = HashMap::new();
    let mut default_asset_id = None;

    for pair in s.split(',') {
        let pair = pair.trim();
        let (chain_str, asset_id) =
            pair.split_once(':')
                .ok_or_else(|| ChainAssetIdsParseError::InvalidFormat {
                    pair: pair.to_string(),
                })?;

        let chain_id: u64 =
            chain_str
                .trim()
                .parse()
                .map_err(|source| ChainAssetIdsParseError::InvalidChainId {
                    value: chain_str.trim().to_string(),
                    source,
                })?;

        let asset_id = asset_id.trim().to_string();

        if asset_id.is_empty() {
            return Err(ChainAssetIdsParseError::EmptyAssetId { chain_id });
        }

        if map.contains_key(&chain_id) {
            return Err(ChainAssetIdsParseError::DuplicateChainId { chain_id });
        }

        if default_asset_id.is_none() {
            default_asset_id = Some(asset_id.clone());
        }

        map.insert(chain_id, asset_id);
    }

    let default_asset_id = default_asset_id.ok_or(ChainAssetIdsParseError::EmptyInput)?;

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
        assert!(matches!(
            parse_chain_asset_ids("ETH").unwrap_err(),
            ChainAssetIdsParseError::InvalidFormat { .. }
        ));
    }

    #[test]
    fn parse_invalid_chain_id() {
        assert!(matches!(
            parse_chain_asset_ids("abc:ETH").unwrap_err(),
            ChainAssetIdsParseError::InvalidChainId { .. }
        ));
    }

    #[test]
    fn parse_empty_string() {
        assert!(matches!(
            parse_chain_asset_ids("").unwrap_err(),
            ChainAssetIdsParseError::InvalidFormat { .. }
        ));
    }

    #[test]
    fn parse_empty_asset_id() {
        assert!(matches!(
            parse_chain_asset_ids("1:").unwrap_err(),
            ChainAssetIdsParseError::EmptyAssetId { chain_id: 1 }
        ));
    }

    #[test]
    fn parse_duplicate_chain_id() {
        assert!(matches!(
            parse_chain_asset_ids("1:ETH,1:ETH_TEST").unwrap_err(),
            ChainAssetIdsParseError::DuplicateChainId { chain_id: 1 }
        ));
    }
}
