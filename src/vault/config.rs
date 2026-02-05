//! Configuration for wrapped token mappings.
//!
//! Maps equity symbols to their wrapped/unwrapped token addresses.

use std::collections::HashMap;

use alloy::primitives::{Address, address};
use serde::{Deserialize, Serialize};
use st0x_execution::{EmptySymbolError, Symbol};

/// Configuration for a single wrapped token pair.
///
/// Each equity symbol maps to a wrapped token (ERC-4626 vault) and
/// its underlying unwrapped token.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WrappedTokenConfig {
    /// Stock ticker symbol (e.g., "AAPL") for Alpaca/position tracking.
    pub equity_symbol: Symbol,

    /// ERC-4626 vault address - this IS the wrapped token (e.g., wAAPL).
    pub wrapped_token: Address,

    /// Underlying asset address (e.g., tAAPL from Alpaca tokenization).
    pub unwrapped_token: Address,
}

/// Registry for looking up wrapped token configurations.
///
/// Provides efficient lookups by:
/// - Equity symbol (for mint/redemption flows)
/// - Wrapped token address (for trade processing)
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(from = "Vec<WrappedTokenConfig>")]
pub(crate) struct WrappedTokenRegistry {
    /// Configs indexed by equity symbol.
    symbols: HashMap<Symbol, WrappedTokenConfig>,

    /// Configs indexed by wrapped token address.
    wrapped: HashMap<Address, WrappedTokenConfig>,
}

impl From<Vec<WrappedTokenConfig>> for WrappedTokenRegistry {
    fn from(configs: Vec<WrappedTokenConfig>) -> Self {
        Self::new(configs)
    }
}

impl WrappedTokenRegistry {
    /// Creates a new registry from a list of configs.
    pub(crate) fn new(configs: Vec<WrappedTokenConfig>) -> Self {
        let mut symbols = HashMap::with_capacity(configs.len());
        let mut wrapped = HashMap::with_capacity(configs.len());

        for config in configs {
            symbols.insert(config.equity_symbol.clone(), config.clone());
            wrapped.insert(config.wrapped_token, config);
        }

        Self { symbols, wrapped }
    }

    /// Creates an empty registry (for tests that don't involve wrapped tokens).
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    /// Returns the hardcoded wrapped token registry.
    ///
    /// Temporary until onchain contract lookups or new config format replaces this.
    pub(crate) fn hardcoded() -> Result<Self, EmptySymbolError> {
        Ok(Self::new(vec![
            // TODO: replace placeholder addresses with real ones
            WrappedTokenConfig {
                equity_symbol: Symbol::new("RKLB")?,
                wrapped_token: address!("0x0000000000000000000000000000000000000001"),
                unwrapped_token: address!("0x0000000000000000000000000000000000000002"),
            },
        ]))
    }

    /// Looks up config by equity symbol.
    pub(crate) fn get_by_symbol(&self, symbol: &Symbol) -> Option<&WrappedTokenConfig> {
        self.symbols.get(symbol)
    }

    /// Looks up config by wrapped token address.
    pub(crate) fn get_by_wrapped(&self, wrapped: &Address) -> Option<&WrappedTokenConfig> {
        self.wrapped.get(wrapped)
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;

    use super::*;

    fn create_test_config() -> WrappedTokenConfig {
        WrappedTokenConfig {
            equity_symbol: Symbol::new("AAPL").unwrap(),
            wrapped_token: address!("0x1111111111111111111111111111111111111111"),
            unwrapped_token: address!("0x2222222222222222222222222222222222222222"),
        }
    }

    #[test]
    fn registry_lookups_by_symbol() {
        let config = create_test_config();
        let registry = WrappedTokenRegistry::new(vec![config.clone()]);

        let found = registry.get_by_symbol(&Symbol::new("AAPL").unwrap());
        assert!(found.is_some());
        assert_eq!(found.unwrap(), &config);

        let not_found = registry.get_by_symbol(&Symbol::new("TSLA").unwrap());
        assert!(not_found.is_none());
    }

    #[test]
    fn registry_lookups_by_wrapped() {
        let config = create_test_config();
        let registry = WrappedTokenRegistry::new(vec![config.clone()]);

        let found = registry.get_by_wrapped(&config.wrapped_token);
        assert!(found.is_some());
        assert_eq!(found.unwrap(), &config);

        let other_address = address!("0x3333333333333333333333333333333333333333");
        let not_found = registry.get_by_wrapped(&other_address);
        assert!(not_found.is_none());
    }

    #[test]
    fn empty_registry_returns_none() {
        let registry = WrappedTokenRegistry::empty();

        assert!(
            registry
                .get_by_symbol(&Symbol::new("AAPL").unwrap())
                .is_none()
        );
    }

    #[test]
    fn multiple_configs() {
        let aapl = WrappedTokenConfig {
            equity_symbol: Symbol::new("AAPL").unwrap(),
            wrapped_token: address!("0x1111111111111111111111111111111111111111"),
            unwrapped_token: address!("0x2222222222222222222222222222222222222222"),
        };
        let tsla = WrappedTokenConfig {
            equity_symbol: Symbol::new("TSLA").unwrap(),
            wrapped_token: address!("0x3333333333333333333333333333333333333333"),
            unwrapped_token: address!("0x4444444444444444444444444444444444444444"),
        };

        let registry = WrappedTokenRegistry::new(vec![aapl.clone(), tsla.clone()]);

        assert_eq!(
            registry.get_by_symbol(&Symbol::new("AAPL").unwrap()),
            Some(&aapl)
        );
        assert_eq!(
            registry.get_by_symbol(&Symbol::new("TSLA").unwrap()),
            Some(&tsla)
        );
    }
}
