//! Stock symbol newtype with non-empty validation.

use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Validation error for empty symbol strings.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Symbol cannot be empty")]
pub struct EmptySymbolError;

/// Stock symbol newtype wrapper with validation.
///
/// Ensures symbols are non-empty and provides type safety to prevent
/// mixing symbols with other string types.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Symbol(String);

impl Symbol {
    /// # Errors
    ///
    /// Returns [`EmptySymbolError`] if the input is empty or whitespace-only.
    pub fn new(symbol: impl Into<String>) -> Result<Self, EmptySymbolError> {
        let trimmed = symbol.into().trim().to_owned();
        if trimmed.is_empty() {
            return Err(EmptySymbolError);
        }
        Ok(Self(trimmed))
    }
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let symbol = String::deserialize(deserializer)?;
        Self::new(symbol).map_err(serde::de::Error::custom)
    }
}

impl Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for Symbol {
    type Err = EmptySymbolError;

    fn from_str(symbol: &str) -> Result<Self, Self::Err> {
        Self::new(symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_string() {
        assert_eq!(Symbol::new(""), Err(EmptySymbolError));
    }

    #[test]
    fn rejects_whitespace_only() {
        assert_eq!(Symbol::new("   "), Err(EmptySymbolError));
        assert_eq!(Symbol::new(" "), Err(EmptySymbolError));
        assert_eq!(Symbol::new("\t"), Err(EmptySymbolError));
    }

    #[test]
    fn trims_leading_and_trailing_whitespace() {
        let symbol = Symbol::new(" AAPL ").unwrap();
        assert_eq!(symbol.to_string(), "AAPL");
    }

    #[test]
    fn preserves_valid_symbol() {
        let symbol = Symbol::new("TSLA").unwrap();
        assert_eq!(symbol.to_string(), "TSLA");
    }

    #[test]
    fn deserialize_rejects_empty() {
        let result: Result<Symbol, _> = serde_json::from_str(r#""""#);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_rejects_whitespace_only() {
        let result: Result<Symbol, _> = serde_json::from_str(r#""   ""#);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_trims_whitespace() {
        let symbol: Symbol = serde_json::from_str(r#"" AAPL ""#).unwrap();
        assert_eq!(symbol.to_string(), "AAPL");
    }

    #[test]
    fn from_str_trims_whitespace() {
        let symbol: Symbol = " GOOG ".parse().unwrap();
        assert_eq!(symbol.to_string(), "GOOG");
    }
}
