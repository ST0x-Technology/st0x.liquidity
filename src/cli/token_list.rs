//! Resolves wrap/unwrap token addresses from an st0x.registry token list.
//!
//! Non Base networks have no `[chains.<name>.trading.assets.equities]` config source, so operators
//! point the CLI at a per chain token list JSON from a checked out
//! st0x.registry (`token-lists/<network>.json`). Entries are the wrapped
//! tokens (`wtSYM`); each carries the underlying tStock address in
//! `extensions.unwrappedAddress` and the chain id it belongs to.

use std::collections::HashMap;
use std::path::Path;

use alloy::primitives::Address;
use serde::Deserialize;

use st0x_execution::Symbol;
use st0x_wrapper::WrappedEquity;

/// Prefix of wrapped token symbols in the registry token lists.
const WRAPPED_SYMBOL_PREFIX: &str = "wt";

/// Loads a registry token list and builds the symbol to address map the
/// wrapper service consumes.
///
/// Every entry must belong to `expected_chain_id`; a single mismatch fails
/// the whole load so a token list for the wrong network can never resolve an
/// address. Entries without the `wt` prefix are skipped (the wrap paths only
/// consume wrapped tokens); `wt` entries missing the underlying address fail
/// closed.
pub(super) fn load_wrapped_equities(
    path: &Path,
    expected_chain_id: u64,
) -> Result<HashMap<Symbol, WrappedEquity>, TokenListError> {
    let raw = std::fs::read_to_string(path).map_err(|source| TokenListError::Io {
        path: path.display().to_string(),
        source,
    })?;
    let list: TokenList = serde_json::from_str(&raw).map_err(|source| TokenListError::Parse {
        path: path.display().to_string(),
        source,
    })?;

    let mut equities = HashMap::new();
    for entry in list.tokens {
        if entry.chain_id != expected_chain_id {
            return Err(TokenListError::ChainIdMismatch {
                symbol: entry.symbol,
                found: entry.chain_id,
                expected: expected_chain_id,
            });
        }

        let has_unwrapped_address = entry
            .extensions
            .as_ref()
            .and_then(|extensions| extensions.unwrapped_address)
            .is_some();
        let Some(underlying_symbol) = entry.symbol.strip_prefix(WRAPPED_SYMBOL_PREFIX) else {
            if has_unwrapped_address {
                return Err(TokenListError::UnwrappedAddressWithoutPrefix {
                    symbol: entry.symbol,
                });
            }
            continue;
        };

        let underlying = entry
            .extensions
            .as_ref()
            .and_then(|extensions| extensions.unwrapped_address)
            .ok_or_else(|| TokenListError::MissingUnwrappedAddress {
                symbol: entry.symbol.clone(),
            })?;

        let symbol =
            Symbol::new(underlying_symbol).map_err(|source| TokenListError::InvalidSymbol {
                symbol: entry.symbol.clone(),
                source,
            })?;

        let previous = equities.insert(
            symbol,
            WrappedEquity {
                underlying,
                derivative: entry.address,
            },
        );
        if previous.is_some() {
            return Err(TokenListError::DuplicateSymbol {
                symbol: entry.symbol,
            });
        }
    }

    Ok(equities)
}

#[derive(Debug, Deserialize)]
struct TokenList {
    tokens: Vec<TokenEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TokenEntry {
    chain_id: u64,
    address: Address,
    symbol: String,
    #[serde(default)]
    extensions: Option<TokenExtensions>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TokenExtensions {
    #[serde(default)]
    unwrapped_address: Option<Address>,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum TokenListError {
    #[error("failed to read token list {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to parse token list {path}: {source}")]
    Parse {
        path: String,
        source: serde_json::Error,
    },
    #[error(
        "token list entry {symbol} has chain id {found} but the selected \
         network expects {expected}; pass the token list for the selected \
         network"
    )]
    ChainIdMismatch {
        symbol: String,
        found: u64,
        expected: u64,
    },
    #[error(
        "wrapped token entry {symbol} has no extensions.unwrappedAddress; \
         cannot resolve the underlying tStock"
    )]
    MissingUnwrappedAddress { symbol: String },
    #[error(
        "entry {symbol} has extensions.unwrappedAddress but no wt prefix; \
         it looks wrapped under a naming convention this loader does not \
         understand"
    )]
    UnwrappedAddressWithoutPrefix { symbol: String },
    #[error("invalid symbol {symbol} in token list")]
    InvalidSymbol {
        symbol: String,
        #[source]
        source: st0x_execution::EmptySymbolError,
    },
    #[error(
        "duplicate wrapped token entry {symbol} in token list; cannot pick \
         one of the conflicting address sets"
    )]
    DuplicateSymbol { symbol: String },
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use std::io::Write as _;

    use super::*;

    const ETHEREUM_CHAIN_ID: u64 = 1;

    fn write_list(json: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(json.as_bytes()).unwrap();
        file
    }

    #[test]
    fn resolves_wrapped_entry_to_underlying_and_derivative() {
        let file = write_list(
            r#"{
                "name": "ST0x Ethereum Token List",
                "tokens": [{
                    "chainId": 1,
                    "address": "0xF4f8c66085910d583c01f3b4e44Bf731D4e2c565",
                    "decimals": 18,
                    "symbol": "wtRKLB",
                    "extensions": {
                        "unwrappedAddress": "0xED0c085d92C262FB46937CB0B3C9763Af7fCCf30",
                        "receiptAddress": "0x34Bf3d8DFaa92e554FBCf48135E5d814210DA1dd"
                    }
                }]
            }"#,
        );

        let equities = load_wrapped_equities(file.path(), ETHEREUM_CHAIN_ID).unwrap();

        let rklb = &equities[&Symbol::new("RKLB").unwrap()];
        assert_eq!(
            rklb.underlying,
            address!("0xED0c085d92C262FB46937CB0B3C9763Af7fCCf30")
        );
        assert_eq!(
            rklb.derivative,
            address!("0xF4f8c66085910d583c01f3b4e44Bf731D4e2c565")
        );
    }

    #[test]
    fn chain_id_mismatch_fails_closed() {
        let file = write_list(
            r#"{
                "tokens": [{
                    "chainId": 8453,
                    "address": "0xF4f8c66085910d583c01f3b4e44Bf731D4e2c565",
                    "symbol": "wtRKLB",
                    "extensions": {
                        "unwrappedAddress": "0xED0c085d92C262FB46937CB0B3C9763Af7fCCf30"
                    }
                }]
            }"#,
        );

        let error = load_wrapped_equities(file.path(), ETHEREUM_CHAIN_ID).unwrap_err();

        assert!(
            matches!(
                error,
                TokenListError::ChainIdMismatch {
                    found: 8453,
                    expected: ETHEREUM_CHAIN_ID,
                    ..
                }
            ),
            "expected ChainIdMismatch, got {error:?}"
        );
    }

    #[test]
    fn wrapped_entry_without_unwrapped_address_fails_closed() {
        let file = write_list(
            r#"{
                "tokens": [{
                    "chainId": 1,
                    "address": "0xF4f8c66085910d583c01f3b4e44Bf731D4e2c565",
                    "symbol": "wtRKLB"
                }]
            }"#,
        );

        let error = load_wrapped_equities(file.path(), ETHEREUM_CHAIN_ID).unwrap_err();

        assert!(
            matches!(
                error,
                TokenListError::MissingUnwrappedAddress { ref symbol } if symbol == "wtRKLB"
            ),
            "expected MissingUnwrappedAddress, got {error:?}"
        );
    }

    #[test]
    fn entries_without_the_wrapped_prefix_are_skipped() {
        let file = write_list(
            r#"{
                "tokens": [{
                    "chainId": 1,
                    "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                    "symbol": "USDC"
                }]
            }"#,
        );

        let equities = load_wrapped_equities(file.path(), ETHEREUM_CHAIN_ID).unwrap();

        assert!(equities.is_empty());
    }

    #[test]
    fn unwrapped_address_without_the_prefix_fails_closed() {
        let file = write_list(
            r#"{
                "tokens": [{
                    "chainId": 1,
                    "address": "0x8FC87Be766C0cB6f254F1FDc9351D4B85B560FB3",
                    "symbol": "wrappedRKLB",
                    "extensions": {
                        "unwrappedAddress": "0xED0c085d92C262FB46937CB0B3C9763Af7fCCf30"
                    }
                }]
            }"#,
        );

        let error = load_wrapped_equities(file.path(), ETHEREUM_CHAIN_ID).unwrap_err();

        assert!(
            matches!(
                error,
                TokenListError::UnwrappedAddressWithoutPrefix { ref symbol }
                    if symbol == "wrappedRKLB"
            ),
            "expected UnwrappedAddressWithoutPrefix, got {error:?}"
        );
    }

    #[test]
    fn duplicate_wrapped_entries_fail_closed() {
        let file = write_list(
            r#"{
                "tokens": [{
                    "chainId": 1,
                    "address": "0xF4f8c66085910d583c01f3b4e44Bf731D4e2c565",
                    "symbol": "wtRKLB",
                    "extensions": {
                        "unwrappedAddress": "0xED0c085d92C262FB46937CB0B3C9763Af7fCCf30"
                    }
                }, {
                    "chainId": 1,
                    "address": "0x8FC87Be766C0cB6f254F1FDc9351D4B85B560FB3",
                    "symbol": "wtRKLB",
                    "extensions": {
                        "unwrappedAddress": "0xED0c085d92C262FB46937CB0B3C9763Af7fCCf30"
                    }
                }]
            }"#,
        );

        let error = load_wrapped_equities(file.path(), ETHEREUM_CHAIN_ID).unwrap_err();

        assert!(
            matches!(
                error,
                TokenListError::DuplicateSymbol { ref symbol } if symbol == "wtRKLB"
            ),
            "expected DuplicateSymbol, got {error:?}"
        );
    }
}
