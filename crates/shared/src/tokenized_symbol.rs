//! Tokenization-form-aware symbol types for onchain tokenized equities.
//!
//! `TokenizedSymbol<Form>` distinguishes minted (tTICKER) from wrapped
//! (wtTICKER) symbols at the type level via the `TokenizationForm` trait.

use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use st0x_execution::{EmptySymbolError, Symbol};

/// Error parsing a tokenized symbol string.
#[derive(Debug, thiserror::Error)]
pub enum TokenizedSymbolError {
    #[error(
        "Expected symbol with prefix '{expected_prefix}' \
         but got '{symbol_provided}'"
    )]
    NotTokenizedEquity {
        expected_prefix: &'static str,
        symbol_provided: String,
    },
    #[error(transparent)]
    EmptySymbol(#[from] EmptySymbolError),
}

/// Distinguishes how an equity was tokenized onchain.
/// Each form defines the symbol prefix used in Raindex orders.
///
/// Sealed: only [`OneToOneTokenizedShares`] and [`WrappedTokenizedShares`]
/// may implement this trait.
pub trait TokenizationForm: sealed::Sealed + fmt::Debug + Clone + PartialEq + Eq {
    fn prefix() -> &'static str;
}

mod sealed {
    pub trait Sealed {}

    impl Sealed for super::OneToOneTokenizedShares {}
    impl Sealed for super::WrappedTokenizedShares {}
}

/// 1:1 minted tokenized shares (tTICKER, e.g. tAAPL, tSPYM).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneToOneTokenizedShares;

impl TokenizationForm for OneToOneTokenizedShares {
    fn prefix() -> &'static str {
        "t"
    }
}

/// ERC-4626 vault shares wrapping tokenized equity
/// (wtTICKER, e.g. wtCOIN, wtAAPL).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrappedTokenizedShares;

impl TokenizationForm for WrappedTokenizedShares {
    fn prefix() -> &'static str {
        "wt"
    }
}

/// A tokenized equity symbol consisting of a tokenization-form
/// prefix and a base ticker. Parameterized by the form to
/// distinguish minted (t) from wrapped (wt) symbols at the
/// type level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenizedSymbol<Form: TokenizationForm> {
    _form: PhantomData<Form>,
    symbol: Symbol,
}

impl<Form: TokenizationForm> TokenizedSymbol<Form> {
    pub fn parse(input: &str) -> Result<Self, TokenizedSymbolError> {
        let Some(stripped) = input.strip_prefix(Form::prefix()) else {
            return Err(TokenizedSymbolError::NotTokenizedEquity {
                expected_prefix: Form::prefix(),
                symbol_provided: input.to_string(),
            });
        };

        let symbol = Symbol::new(stripped)?;
        Ok(Self {
            _form: PhantomData,
            symbol,
        })
    }

    pub fn base(&self) -> &Symbol {
        &self.symbol
    }
}

impl<Form: TokenizationForm> fmt::Display for TokenizedSymbol<Form> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Form::prefix(), self.symbol)
    }
}

impl<Form: TokenizationForm> FromStr for TokenizedSymbol<Form> {
    type Err = TokenizedSymbolError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Test-only macro to create a `TokenizedSymbol<Form>`.
/// The form type must be specified explicitly.
#[cfg(test)]
#[macro_export]
macro_rules! tokenized_symbol {
    ($form:ty, $symbol:expr) => {
        $crate::tokenized_symbol::TokenizedSymbol::<$form>::parse($symbol).unwrap()
    };
}

/// Test-only macro to create a Symbol.
#[cfg(test)]
#[macro_export]
macro_rules! symbol {
    ($symbol:expr) => {
        st0x_execution::Symbol::new($symbol).unwrap()
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenized_equity_symbol_parse() {
        TokenizedSymbol::<OneToOneTokenizedShares>::parse("tGME").unwrap();
        TokenizedSymbol::<OneToOneTokenizedShares>::parse("tAAPL").unwrap();
        TokenizedSymbol::<OneToOneTokenizedShares>::parse("tSPYM").unwrap();

        let err = TokenizedSymbol::<OneToOneTokenizedShares>::parse("USDC").unwrap_err();
        assert!(
            matches!(
                err,
                TokenizedSymbolError::NotTokenizedEquity {
                    ref symbol_provided, ..
                } if symbol_provided == "USDC"
            ),
            "Expected NotTokenizedEquity for USDC, got: {err:?}"
        );

        let err = TokenizedSymbol::<OneToOneTokenizedShares>::parse("AAPL").unwrap_err();
        assert!(
            matches!(
                err,
                TokenizedSymbolError::NotTokenizedEquity {
                    ref symbol_provided, ..
                } if symbol_provided == "AAPL"
            ),
            "Expected NotTokenizedEquity for AAPL, got: {err:?}"
        );
    }

    #[test]
    fn test_tokenized_equity_symbol_extract_base() {
        let symbol = TokenizedSymbol::<OneToOneTokenizedShares>::parse("tGME").unwrap();
        assert_eq!(symbol.base().to_string(), "GME");

        let symbol = TokenizedSymbol::<OneToOneTokenizedShares>::parse("tAAPL").unwrap();
        assert_eq!(symbol.base().to_string(), "AAPL");

        // Prefix-only "t" -> empty base -> Symbol validation fails
        let error = TokenizedSymbol::<OneToOneTokenizedShares>::parse("t").unwrap_err();
        assert!(matches!(error, TokenizedSymbolError::EmptySymbol(_)));
    }

    #[test]
    fn test_wrapped_tokenized_equity_symbol_parse() {
        let symbol = TokenizedSymbol::<WrappedTokenizedShares>::parse("wtCOIN").unwrap();
        assert_eq!(symbol.base(), &Symbol::new("COIN").unwrap());
        assert_eq!(symbol.to_string(), "wtCOIN");

        let err = TokenizedSymbol::<WrappedTokenizedShares>::parse("tCOIN").unwrap_err();
        assert!(matches!(
            err,
            TokenizedSymbolError::NotTokenizedEquity { .. }
        ));

        let err = TokenizedSymbol::<WrappedTokenizedShares>::parse("wt").unwrap_err();
        assert!(matches!(err, TokenizedSymbolError::EmptySymbol(_)));
    }
}
