//! Asset configuration, split by what the value is a property of.
//!
//! An equity's token addresses, vault ids and per-asset switches are facts
//! about one chain: the same underlying listed on two chains is two sets of
//! addresses under one symbol. Those live under
//! `[chains.<name>.trading.assets]`.
//!
//! Whether the bot will hedge that underlying outside regular hours is not.
//! There is one broker account and one `Position` per symbol, so a per-chain
//! extended-hours switch would let one chain's fills hedge as limits while
//! another's waited for the open -- two hedge policies for one position. That
//! lives under the global `[assets]` table, as does the broker-side cash
//! reserve, which sits at the broker rather than on any chain.

use std::collections::HashMap;

use alloy::primitives::{Address, B256};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use st0x_execution::{FractionalShares, Positive, Symbol};
use st0x_finance::{Usd, Usdc};

/// Whether a per-asset operation (trading or rebalancing) is active.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OperationMode {
    Enabled,
    Disabled,
}

/// Why a vault-id hex string does not parse into a `B256`.
#[derive(Debug, Error)]
enum PaddedB256Error {
    #[error("empty hex string for B256")]
    Empty,
    #[error("hex string too long for B256: {hex}")]
    TooLong { hex: String },
    #[error("invalid hex string for B256")]
    InvalidHex(#[from] alloy::hex::FromHexError),
}

/// Parses a hex string (possibly short, e.g. `"0xfab"`) into a
/// left-padded `B256`.
fn parse_padded_b256(hex_str: &str) -> Result<B256, PaddedB256Error> {
    let stripped = hex_str
        .strip_prefix("0x")
        .or_else(|| hex_str.strip_prefix("0X"))
        .unwrap_or(hex_str);

    if stripped.len() > 64 {
        return Err(PaddedB256Error::TooLong {
            hex: stripped.to_string(),
        });
    }

    if stripped.is_empty() {
        return Err(PaddedB256Error::Empty);
    }

    let padded = format!("{stripped:0>64}");
    Ok(padded.parse::<B256>()?)
}

/// Deserializes vault IDs from either a single hex string or an array of hex
/// strings. Each value is left-padded to a full `B256`.
///
/// Accepts both `vault_id = "0xfab"` (single) and
/// `vault_ids = ["0xfab", "0xfab2"]` (multiple) in TOML.
fn deserialize_vault_ids<'de, D>(deserializer: D) -> Result<Vec<B256>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }

    let raw = OneOrMany::deserialize(deserializer)?;

    let hex_strings = match raw {
        OneOrMany::One(single) => vec![single],
        OneOrMany::Many(many) => many,
    };

    hex_strings
        .into_iter()
        .map(|hex_str| parse_padded_b256(&hex_str).map_err(serde::de::Error::custom))
        .collect()
}

/// One equity as it exists on one chain.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainEquityAsset {
    pub tokenized_equity: Address,
    pub tokenized_equity_derivative: Address,
    #[serde(
        default,
        alias = "vault_id",
        deserialize_with = "deserialize_vault_ids"
    )]
    pub vault_ids: Vec<B256>,
    pub trading: OperationMode,
    pub rebalancing: OperationMode,
    pub wrapped_equity_recovery: OperationMode,
    /// Cap on how much of this equity the bot will hold or move on this chain.
    /// Per chain because tokens are deliverable only where they sit: a symbol
    /// that is net flat across chains can still be untradeable on each of them.
    pub operational_limit: Option<Positive<FractionalShares>>,
}

/// Cash (USDC) as it exists on one chain.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainCashAsset {
    #[serde(
        default,
        alias = "vault_id",
        deserialize_with = "deserialize_vault_ids"
    )]
    pub vault_ids: Vec<B256>,
    pub rebalancing: OperationMode,
    pub operational_limit: Option<Positive<Usdc>>,
}

/// The equities listed on one chain, with an optional chain-wide limit.
///
/// Uses `#[serde(flatten)]` so per-symbol tables live alongside
/// `operational_limit` under `[chains.<name>.trading.assets.equities]`.
/// `deny_unknown_fields` is intentionally absent because it is incompatible
/// with `flatten`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ChainEquities {
    pub operational_limit: Option<Positive<FractionalShares>>,
    #[serde(flatten)]
    pub symbols: HashMap<Symbol, ChainEquityAsset>,
}

/// Everything the bot holds on one chain.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainAssets {
    #[serde(default)]
    pub equities: ChainEquities,
    pub cash: Option<ChainCashAsset>,
}

impl ChainAssets {
    /// Returns whether trading is enabled for the given equity on this chain.
    ///
    /// Fail-closed: assets not present in the config are treated as
    /// trading-disabled.
    pub fn is_trading_enabled(&self, symbol: &Symbol) -> bool {
        self.equities
            .symbols
            .get(symbol)
            .is_some_and(|config| config.trading == OperationMode::Enabled)
    }

    /// Returns whether rebalancing is enabled for the given equity on this
    /// chain. Assets not present in the config are treated as
    /// rebalancing-disabled.
    pub fn is_rebalancing_enabled(&self, symbol: &Symbol) -> bool {
        self.equities
            .symbols
            .get(symbol)
            .is_some_and(|config| config.rebalancing == OperationMode::Enabled)
    }

    /// Returns whether wrapped/unwrapped wallet equity recovery is enabled
    /// for the given equity on this chain.
    ///
    /// Independent of `rebalancing`: a symbol may opt into recovery while
    /// keeping automatic rebalancing disabled. Assets not present in the
    /// config are treated as recovery-disabled.
    pub fn is_wrapped_equity_recovery_enabled(&self, symbol: &Symbol) -> bool {
        self.equities
            .symbols
            .get(symbol)
            .is_some_and(|config| config.wrapped_equity_recovery == OperationMode::Enabled)
    }

    /// Returns the configured tokenized-equity (minted onchain token) address
    /// for `symbol` on this chain, or `None` when the symbol is not listed
    /// here. Lets operator commands resolve the token from the ticker instead
    /// of taking an error-prone address argument.
    pub fn tokenized_equity(&self, symbol: &Symbol) -> Option<Address> {
        self.equities
            .symbols
            .get(symbol)
            .map(|config| config.tokenized_equity)
    }
}

/// How the bot hedges one underlying, independent of where it is listed.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EquityHedgePolicy {
    /// When enabled, counter-trades for this equity may be placed during
    /// extended (pre-/after-market) sessions as limit orders, instead of
    /// waiting for the regular open. Must be explicitly configured.
    ///
    /// Global rather than per chain: there is one broker account and one
    /// `Position` per symbol, so the bot cannot hedge the same exposure under
    /// two session policies at once.
    pub extended_hours_counter_trading: OperationMode,
}

/// Broker-side cash policy.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CashHedgePolicy {
    /// USD amount subtracted from offchain cash to compute available balance.
    /// Prevents the system from rebalancing funds that should remain
    /// untouched. Sits at the broker, so it is not a property of any chain.
    ///
    /// Required whenever `[assets.cash]` is present: an empty table would
    /// silently make the full broker balance available. Omit the table
    /// entirely to declare that no reserve is intended.
    pub reserved: Positive<Usd>,
}

/// The per-symbol hedging policy for each hedged equity.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct HedgedEquities {
    #[serde(flatten)]
    pub symbols: HashMap<Symbol, EquityHedgePolicy>,
}

/// The global `[assets]` table: what the bot hedges, and how.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HedgingAssets {
    #[serde(default)]
    pub equities: HedgedEquities,
    pub cash: Option<CashHedgePolicy>,
}

impl HedgingAssets {
    /// Returns whether extended-hours counter-trading is enabled for the
    /// given equity.
    ///
    /// Fail-closed: assets not present in the config are treated as disabled.
    pub fn is_extended_hours_enabled(&self, symbol: &Symbol) -> bool {
        self.equities
            .symbols
            .get(symbol)
            .is_some_and(|policy| policy.extended_hours_counter_trading == OperationMode::Enabled)
    }

    /// Returns whether any configured equity enables extended-hours
    /// counter-trading.
    pub fn any_extended_hours_enabled(&self) -> bool {
        self.equities
            .symbols
            .values()
            .any(|policy| policy.extended_hours_counter_trading == OperationMode::Enabled)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use st0x_finance::Usd;
    use st0x_float_macro::float;

    use super::*;

    #[test]
    fn assets_config_parses_equities_and_cash() {
        let toml_str = r#"
            [equities.RKLB]
            tokenized_equity = "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
            tokenized_equity_derivative = "0xf4f8c66085910d583c01f3b4e44bf731d4e2c565"
            vault_id = "0xfab"
            trading = "disabled"
            rebalancing = "enabled"
            wrapped_equity_recovery = "disabled"
            operational_limit = 5

            [equities.SPYM]
            tokenized_equity = "0x8fdf41116f755771bfe0747d5f8c3711d5debfbb"
            tokenized_equity_derivative = "0x31c2c14134e6e3b7ef9478297f199331133fc2d8"
            trading = "disabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"

            [cash]
            vault_id = "0x0000000000000000000000000000000000000000000000000000000000000fab"
            rebalancing = "disabled"
            operational_limit = 100
        "#;

        let config: ChainAssets = toml::from_str(toml_str).unwrap();

        assert_eq!(config.equities.symbols.len(), 2);

        let rklb = &config.equities.symbols[&Symbol::new("RKLB").unwrap()];
        assert_eq!(
            rklb.tokenized_equity,
            "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
                .parse::<Address>()
                .unwrap()
        );
        assert_eq!(rklb.trading, OperationMode::Disabled);
        assert_eq!(rklb.rebalancing, OperationMode::Enabled);
        assert_eq!(rklb.vault_ids.len(), 1);
        assert!(rklb.operational_limit.is_some());

        let cash = config.cash.unwrap();
        assert_eq!(cash.rebalancing, OperationMode::Disabled);
        assert_eq!(cash.vault_ids.len(), 1);
    }
    #[test]
    fn extended_hours_counter_trading_parses_enabled_and_disabled_from_toml() {
        let toml_str = r#"
            [equities.AAPL]
            extended_hours_counter_trading = "enabled"

            [equities.TSLA]
            extended_hours_counter_trading = "disabled"
        "#;

        let hedging: HedgingAssets = toml::from_str(toml_str).unwrap();

        assert!(hedging.is_extended_hours_enabled(&Symbol::new("AAPL").unwrap()));
        assert!(!hedging.is_extended_hours_enabled(&Symbol::new("TSLA").unwrap()));
    }

    #[test]
    fn short_vault_id_left_pads_to_b256() {
        let toml_str = r#"
            [equities.RKLB]
            tokenized_equity = "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
            tokenized_equity_derivative = "0xf4f8c66085910d583c01f3b4e44bf731d4e2c565"
            vault_id = "0xfab"
            trading = "disabled"
            rebalancing = "enabled"
            wrapped_equity_recovery = "disabled"
        "#;

        let config: ChainAssets = toml::from_str(toml_str).unwrap();
        let rklb = &config.equities.symbols[&Symbol::new("RKLB").unwrap()];
        let expected: B256 = "0000000000000000000000000000000000000000000000000000000000000fab"
            .parse()
            .unwrap();
        assert_eq!(rklb.vault_ids[0], expected);
    }
    #[test]
    fn vault_ids_array_parses_multiple_values() {
        let toml_str = r#"
            [equities.RKLB]
            tokenized_equity = "0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b"
            tokenized_equity_derivative = "0xf4f8c66085910d583c01f3b4e44bf731d4e2c565"
            vault_ids = ["0xfab", "0xfab2"]
            trading = "disabled"
            rebalancing = "enabled"
            wrapped_equity_recovery = "disabled"
        "#;

        let config: ChainAssets = toml::from_str(toml_str).unwrap();
        let rklb = &config.equities.symbols[&Symbol::new("RKLB").unwrap()];
        assert_eq!(rklb.vault_ids.len(), 2);

        let expected_1: B256 = "0000000000000000000000000000000000000000000000000000000000000fab"
            .parse()
            .unwrap();
        let expected_2: B256 = "000000000000000000000000000000000000000000000000000000000000fab2"
            .parse()
            .unwrap();
        assert_eq!(rklb.vault_ids[0], expected_1);
        assert_eq!(rklb.vault_ids[1], expected_2);
    }
    #[test]
    fn equity_missing_trading_field_rejects() {
        let toml_str = r#"
            [equities.AAPL]
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"
        "#;

        let error = match toml::from_str::<ChainAssets>(toml_str) {
            Ok(parsed) => panic!("missing trading must be rejected, parsed: {parsed:?}"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("trading"),
            "the error must name the missing field, got: {error}"
        );
    }
    #[test]
    fn equity_missing_rebalancing_field_rejects() {
        let toml_str = r#"
            [equities.AAPL]
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            trading = "enabled"
            wrapped_equity_recovery = "disabled"
        "#;

        let error = match toml::from_str::<ChainAssets>(toml_str) {
            Ok(parsed) => panic!("missing rebalancing must be rejected, parsed: {parsed:?}"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("rebalancing"),
            "the error must name the missing field, got: {error}"
        );
    }
    #[test]
    fn equity_missing_wrapped_equity_recovery_field_rejects() {
        let toml_str = r#"
            [equities.AAPL]
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            trading = "enabled"
            rebalancing = "disabled"
        "#;

        let error = match toml::from_str::<ChainAssets>(toml_str) {
            Ok(parsed) => {
                panic!("missing wrapped_equity_recovery must be rejected, parsed: {parsed:?}")
            }
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("wrapped_equity_recovery"),
            "the error must name the missing field, got: {error}"
        );
    }
    #[test]
    fn per_asset_operational_limits_parsed_independently() {
        let toml_str = r#"
            [equities.AAPL]
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"
            operational_limit = 10

            [equities.TSLA]
            tokenized_equity = "0xcccccccccccccccccccccccccccccccccccccccc"
            tokenized_equity_derivative = "0xdddddddddddddddddddddddddddddddddddddddd"
            trading = "enabled"
            rebalancing = "disabled"
            wrapped_equity_recovery = "disabled"
        "#;

        let config: ChainAssets = toml::from_str(toml_str).unwrap();
        let aapl = &config.equities.symbols[&Symbol::new("AAPL").unwrap()];
        let tsla = &config.equities.symbols[&Symbol::new("TSLA").unwrap()];
        assert!(
            aapl.operational_limit.is_some(),
            "AAPL should have an operational limit"
        );
        assert!(
            tsla.operational_limit.is_none(),
            "TSLA should not have an operational limit"
        );
    }
    #[test]
    fn is_trading_enabled_returns_configured_value() {
        let mut symbols = HashMap::new();
        symbols.insert(
            Symbol::new("RKLB").unwrap(),
            ChainEquityAsset {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Enabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                operational_limit: None,
            },
        );

        let assets = ChainAssets {
            equities: ChainEquities {
                operational_limit: None,
                symbols,
            },
            cash: None,
        };

        assert!(
            !assets.is_trading_enabled(&Symbol::new("RKLB").unwrap()),
            "RKLB trading should be disabled"
        );
    }
    #[test]
    fn is_trading_disabled_for_unknown_assets() {
        let assets = ChainAssets::default();

        assert!(
            !assets.is_trading_enabled(&Symbol::new("UNKNOWN").unwrap()),
            "Unknown assets should default to trading disabled (fail-closed)"
        );
    }
    #[test]
    fn is_extended_hours_enabled_returns_configured_value() {
        let mut symbols = HashMap::new();
        symbols.insert(
            Symbol::new("AAPL").unwrap(),
            EquityHedgePolicy {
                extended_hours_counter_trading: OperationMode::Enabled,
            },
        );
        symbols.insert(
            Symbol::new("RKLB").unwrap(),
            EquityHedgePolicy {
                extended_hours_counter_trading: OperationMode::Disabled,
            },
        );

        let hedging = HedgingAssets {
            equities: HedgedEquities { symbols },
            cash: None,
        };

        assert!(hedging.is_extended_hours_enabled(&Symbol::new("AAPL").unwrap()));
        assert!(!hedging.is_extended_hours_enabled(&Symbol::new("RKLB").unwrap()));
        assert!(
            !hedging.is_extended_hours_enabled(&Symbol::new("UNKNOWN").unwrap()),
            "unknown symbols are fail-closed"
        );
        assert!(hedging.any_extended_hours_enabled());
    }

    #[test]
    fn is_rebalancing_enabled_returns_configured_value() {
        let mut symbols = HashMap::new();
        symbols.insert(
            Symbol::new("RKLB").unwrap(),
            ChainEquityAsset {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Enabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                operational_limit: None,
            },
        );

        let assets = ChainAssets {
            equities: ChainEquities {
                operational_limit: None,
                symbols,
            },
            cash: None,
        };

        assert!(
            assets.is_rebalancing_enabled(&Symbol::new("RKLB").unwrap()),
            "RKLB rebalancing should be enabled"
        );
    }
    #[test]
    fn is_rebalancing_enabled_defaults_to_false_for_unknown() {
        let assets = ChainAssets::default();

        assert!(
            !assets.is_rebalancing_enabled(&Symbol::new("UNKNOWN").unwrap()),
            "Unknown assets should default to rebalancing disabled"
        );
    }
    #[test]
    fn is_wrapped_equity_recovery_enabled_is_independent_of_rebalancing() {
        let mut symbols = HashMap::new();
        symbols.insert(
            Symbol::new("AAPL").unwrap(),
            ChainEquityAsset {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Enabled,
                operational_limit: None,
            },
        );

        let assets = ChainAssets {
            equities: ChainEquities {
                operational_limit: None,
                symbols,
            },
            cash: None,
        };

        let aapl = Symbol::new("AAPL").unwrap();
        assert!(
            assets.is_wrapped_equity_recovery_enabled(&aapl),
            "Recovery should follow wrapped_equity_recovery config"
        );
        assert!(
            !assets.is_rebalancing_enabled(&aapl),
            "Recovery-enabled symbol must not imply rebalancing is enabled"
        );
        assert!(
            !assets.is_wrapped_equity_recovery_enabled(&Symbol::new("UNKNOWN").unwrap()),
            "Unknown assets should default to recovery disabled"
        );
    }
    #[test]
    fn base_symbol_config_keys_fix_lookup_bug() {
        // Config keys use base symbols (SPYM not tSPYM).
        // is_trading_enabled uses Symbol directly, which matches
        // base symbol keys. This verifies the bug fix.
        let mut symbols = HashMap::new();
        symbols.insert(
            Symbol::new("SPYM").unwrap(),
            ChainEquityAsset {
                tokenized_equity: Address::ZERO,
                tokenized_equity_derivative: Address::ZERO,
                vault_ids: Vec::new(),
                trading: OperationMode::Disabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                operational_limit: None,
            },
        );

        let assets = ChainAssets {
            equities: ChainEquities {
                operational_limit: None,
                symbols,
            },
            cash: None,
        };

        // The lookup uses base symbol "SPYM" which matches the config key
        assert!(
            !assets.is_trading_enabled(&Symbol::new("SPYM").unwrap()),
            "SPYM trading should be disabled per config"
        );
    }
    /// Generates a valid hex digit string of length 1..=64.
    fn arb_hex_digits() -> impl Strategy<Value = String> {
        prop::collection::vec(
            prop::sample::select(vec![
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
            ]),
            1..=64,
        )
        .prop_map(|chars| chars.into_iter().collect::<String>())
    }

    proptest! {
        /// Arbitrary short hex strings left-pad to correct B256.
        ///
        /// The padded result should equal the hex digits
        /// zero-filled on the left to 64 chars.
        #[test]
        fn padded_b256_roundtrip(hex_digits in arb_hex_digits()) {
            let hex_str = format!("0x{hex_digits}");
            let parsed = parse_padded_b256(&hex_str).unwrap();

            let expected_hex = format!("{hex_digits:0>64}");
            let expected: B256 = expected_hex.parse().unwrap();
            prop_assert_eq!(parsed, expected);
        }

        /// Invalid hex characters must produce a parse error.
        #[test]
        fn padded_b256_rejects_invalid_hex(
            bad_char in "[g-zG-Z!@#$%^&*]",
            prefix in arb_hex_digits(),
        ) {
            let hex_str = format!("0x{prefix}{bad_char}");
            let result = parse_padded_b256(&hex_str);
            prop_assert!(
                result.is_err(),
                "Expected error for invalid hex '{hex_str}', got {result:?}",
            );
        }

        /// OperationMode serializes and deserializes as lowercase strings.
        #[test]
        fn operation_mode_serde_roundtrip(enabled in any::<bool>()) {
            #[derive(Debug, PartialEq, Serialize, Deserialize)]
            struct Wrapper {
                mode: OperationMode,
            }

            let wrapper = Wrapper {
                mode: if enabled {
                    OperationMode::Enabled
                } else {
                    OperationMode::Disabled
                },
            };

            let serialized = toml::to_string(&wrapper).unwrap();
            let expected = if enabled { "enabled" } else { "disabled" };
            prop_assert_eq!(serialized.trim(), format!("mode = \"{}\"", expected));

            let deserialized: Wrapper = toml::from_str(&serialized).unwrap();
            prop_assert_eq!(wrapper, deserialized);
        }

        /// OperationMode rejects strings that are not "enabled" or
        /// "disabled".
        #[test]
        fn operation_mode_rejects_invalid_strings(
            invalid in "[a-z]{3,10}"
                .prop_filter("must not be a valid mode", |value| {
                    value != "enabled" && value != "disabled"
                })
        ) {
            #[derive(Debug, Deserialize)]
            struct Wrapper {
                #[allow(dead_code)]
                mode: OperationMode,
            }

            let toml_str = format!(r#"mode = "{invalid}""#);

            let result = toml::from_str::<Wrapper>(&toml_str);
            prop_assert!(
                result.is_err(),
                "Expected error for invalid mode '{invalid}', got {result:?}"
            );
        }

        /// ChainEquityAsset parses when all required fields are present.
        #[test]
        fn equity_asset_config_parses_with_addresses(
            share_byte in any::<u8>(),
            derivative_byte in any::<u8>(),
            trading_enabled in any::<bool>(),
            rebalancing_enabled in any::<bool>(),
        ) {
            let trading = if trading_enabled { "enabled" } else { "disabled" };
            let rebalancing = if rebalancing_enabled { "enabled" } else { "disabled" };
            let toml_str = format!(
                r#"
                tokenized_equity = "0x{share_byte:02x}{:0>38}"
                tokenized_equity_derivative = "0x{derivative_byte:02x}{:0>38}"
                trading = "{trading}"
                rebalancing = "{rebalancing}"
                wrapped_equity_recovery = "disabled"
                "#,
                "", "",
            );

            let result = toml::from_str::<ChainEquityAsset>(&toml_str);
            prop_assert!(
                result.is_ok(),
                "Expected successful parse, got {result:?}"
            );

            let config = result.unwrap();
            let expected_trading = if trading_enabled {
                OperationMode::Enabled
            } else {
                OperationMode::Disabled
            };
            let expected_rebalancing = if rebalancing_enabled {
                OperationMode::Enabled
            } else {
                OperationMode::Disabled
            };
            prop_assert_eq!(config.trading, expected_trading);
            prop_assert_eq!(config.rebalancing, expected_rebalancing);
        }
    }

    #[test]
    fn cash_asset_config_parses_without_token_addresses() {
        let toml_str = r#"
            vault_id = "0xfab"
            rebalancing = "disabled"
            operational_limit = 100
        "#;

        let config: ChainCashAsset = toml::from_str(toml_str).unwrap();
        assert_eq!(config.rebalancing, OperationMode::Disabled);
        assert_eq!(config.vault_ids.len(), 1);
    }

    #[test]
    fn cash_reserved_parses_positive_usd() {
        let toml_str = "reserved = 5000.00";

        let config: CashHedgePolicy = toml::from_str(toml_str).unwrap();
        let reserved = config.reserved;
        assert!(
            reserved.inner().eq(&Usd::new(float!(5000))).unwrap(),
            "Expected $5000 reserved, got {reserved}"
        );
    }

    #[test]
    fn cash_table_without_reserved_is_rejected() {
        // An empty [assets.cash] would otherwise mean "no reserve" implicitly,
        // making the full broker balance available to rebalancing. Declaring
        // no reserve is done by omitting the table, not leaving it blank.
        let toml_str = "";

        let error = toml::from_str::<CashHedgePolicy>(toml_str).unwrap_err();
        assert!(
            error.to_string().contains("reserved"),
            "expected missing-field error for reserved, got: {error}"
        );
    }

    #[test]
    fn cash_reserved_rejects_zero() {
        let toml_str = "reserved = 0";

        let result = toml::from_str::<CashHedgePolicy>(toml_str);
        assert!(
            result.is_err(),
            "Expected error for zero reserved, got {result:?}"
        );
    }

    #[test]
    fn cash_reserved_rejects_negative() {
        let toml_str = "reserved = -100";

        let result = toml::from_str::<CashHedgePolicy>(toml_str);
        assert!(
            result.is_err(),
            "Expected error for negative reserved, got {result:?}"
        );
    }

    #[test]
    fn equity_asset_config_rejects_missing_tokenized_equity() {
        let toml_str = r#"
            tokenized_equity_derivative = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        "#;

        let result = toml::from_str::<ChainEquityAsset>(toml_str);
        assert!(
            result.is_err(),
            "Expected error for missing tokenized_equity, got {result:?}"
        );
    }

    #[test]
    fn equity_asset_config_rejects_missing_tokenized_equity_derivative() {
        let toml_str = r#"
            tokenized_equity = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        "#;

        let result = toml::from_str::<ChainEquityAsset>(toml_str);
        assert!(
            result.is_err(),
            "Expected error for missing tokenized_equity_derivative, got {result:?}"
        );
    }

    #[test]
    fn padded_b256_rejects_empty_hex() {
        let error = parse_padded_b256("0x").unwrap_err();
        assert!(matches!(error, PaddedB256Error::Empty), "got {error:?}");
    }

    #[test]
    fn padded_b256_rejects_too_long_hex() {
        let long_hex = "a".repeat(65);
        let hex_str = format!("0x{long_hex}");
        let error = parse_padded_b256(&hex_str).unwrap_err();
        assert!(
            matches!(error, PaddedB256Error::TooLong { .. }),
            "got {error:?}"
        );
    }

    #[test]
    fn operation_mode_enabled_from_string() {
        #[derive(Debug, Deserialize)]
        struct Wrapper {
            mode: OperationMode,
        }

        let wrapper: Wrapper = toml::from_str(r#"mode = "enabled""#).unwrap();
        assert_eq!(wrapper.mode, OperationMode::Enabled);
    }

    #[test]
    fn operation_mode_disabled_from_string() {
        #[derive(Debug, Deserialize)]
        struct Wrapper {
            mode: OperationMode,
        }

        let wrapper: Wrapper = toml::from_str(r#"mode = "disabled""#).unwrap();
        assert_eq!(wrapper.mode, OperationMode::Disabled);
    }
}
