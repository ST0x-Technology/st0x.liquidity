//! Order I/O handling logic for processing symbol pairs, amounts, and trade details.
//! This module centralizes all logic related to parsing and validating onchain order data.

use std::fmt;
use std::str::FromStr;

use crate::error::{OnChainError, TradeValidationError};
use st0x_broker::{Direction, Symbol};

/// Macro to create a TokenizedEquitySymbol.
/// This macro provides a convenient way to create tokenized equity symbols.
#[macro_export]
macro_rules! tokenized_symbol {
    ($symbol:expr) => {
        $crate::onchain::io::TokenizedEquitySymbol::parse($symbol).unwrap()
    };
}

// The macro is available via the crate::tokenized_symbol path

/// Macro to create a Symbol.
/// This macro provides a convenient way to create symbols.
#[macro_export]
macro_rules! symbol {
    ($symbol:expr) => {
        st0x_broker::Symbol::new($symbol).unwrap()
    };
}

/// Represents a validated number of shares (non-negative)
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Shares(f64);

impl Shares {
    pub(crate) fn new(value: f64) -> Result<Self, TradeValidationError> {
        if value < 0.0 {
            return Err(TradeValidationError::NegativeShares(value));
        }
        Ok(Self(value))
    }

    pub(crate) fn value(self) -> f64 {
        self.0
    }
}

/// Represents a validated USDC amount (non-negative)
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Usdc(f64);

impl Usdc {
    pub(crate) fn new(value: f64) -> Result<Self, TradeValidationError> {
        if value < 0.0 {
            return Err(TradeValidationError::NegativeUsdc(value));
        }
        Ok(Self(value))
    }

    pub(crate) fn value(self) -> f64 {
        self.0
    }
}

/// The marker for tokenized equity symbols (can be prefix or suffix)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TokenizedEquityMarker {
    T,     // "t" (prefix)
    ZeroX, // "0x" (suffix)
    S1,    // "s1" (suffix)
}

impl TokenizedEquityMarker {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::T => "t",
            Self::ZeroX => "0x",
            Self::S1 => "s1",
        }
    }

    pub(crate) const fn is_prefix(self) -> bool {
        matches!(self, Self::T)
    }
}

impl fmt::Display for TokenizedEquityMarker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Represents a validated tokenized equity symbol with guaranteed format
/// Composed of a base equity symbol and a tokenized marker (prefix or suffix)
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TokenizedEquitySymbol {
    base: Symbol,
    marker: TokenizedEquityMarker,
}

impl TokenizedEquitySymbol {
    /// Creates a new TokenizedEquitySymbol from components
    pub(crate) fn new(base: Symbol, marker: TokenizedEquityMarker) -> Self {
        Self { base, marker }
    }

    /// Creates a new TokenizedEquitySymbol from a string (e.g., "AAPL0x", "tAAPL")
    pub(crate) fn parse(symbol: &str) -> Result<Self, OnChainError> {
        // Try to extract prefix first
        if let Some(stripped) = symbol.strip_prefix('t') {
            let base = Symbol::new(stripped)?;
            return Ok(Self::new(base, TokenizedEquityMarker::T));
        }

        // Try to extract suffix
        if let Some(stripped) = symbol.strip_suffix("0x") {
            let base = Symbol::new(stripped)?;
            return Ok(Self::new(base, TokenizedEquityMarker::ZeroX));
        }

        if let Some(stripped) = symbol.strip_suffix("s1") {
            let base = Symbol::new(stripped)?;
            return Ok(Self::new(base, TokenizedEquityMarker::S1));
        }

        // No valid marker found
        Err(OnChainError::Validation(
            TradeValidationError::NotTokenizedEquity(symbol.to_string()),
        ))
    }

    /// Gets the base equity symbol
    pub(crate) fn base(&self) -> &Symbol {
        &self.base
    }

    /// Extract the base symbol (equivalent to the old extract_base_from_tokenized)
    pub(crate) fn extract_base(&self) -> String {
        self.base.to_string()
    }
}

impl fmt::Display for TokenizedEquitySymbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.marker.is_prefix() {
            write!(f, "{}{}", self.marker, self.base)
        } else {
            write!(f, "{}{}", self.base, self.marker)
        }
    }
}

impl FromStr for TokenizedEquitySymbol {
    type Err = OnChainError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Trade details extracted from symbol pair processing
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TradeDetails {
    ticker: Symbol,
    equity_amount: Shares,
    usdc_amount: Usdc,
    direction: Direction,
}

impl TradeDetails {
    /// Gets the ticker symbol
    #[cfg(test)]
    pub(crate) fn ticker(&self) -> &Symbol {
        &self.ticker
    }

    /// Gets the equity amount
    pub(crate) fn equity_amount(&self) -> Shares {
        self.equity_amount
    }

    /// Gets the USDC amount
    pub(crate) fn usdc_amount(&self) -> Usdc {
        self.usdc_amount
    }

    /// Gets the trade direction
    pub(crate) fn direction(&self) -> Direction {
        self.direction
    }
    /// Extracts trade details from input/output symbol and amount pairs
    pub(crate) fn try_from_io(
        input_symbol: &str,
        input_amount: f64,
        output_symbol: &str,
        output_amount: f64,
    ) -> Result<Self, OnChainError> {
        // Determine direction and ticker using existing logic
        let (ticker, direction) = determine_schwab_trade_details(input_symbol, output_symbol)?;

        // Extract equity and USDC amounts based on which symbol is the tokenized equity
        let (equity_amount_raw, usdc_amount_raw) = if input_symbol == "USDC"
            && TokenizedEquitySymbol::parse(output_symbol).is_ok()
        {
            // USDC → tokenized equity: output is equity, input is USDC
            (output_amount, input_amount)
        } else if output_symbol == "USDC" && TokenizedEquitySymbol::parse(input_symbol).is_ok() {
            // tokenized equity → USDC: input is equity, output is USDC
            (input_amount, output_amount)
        } else {
            // This should not happen if determine_schwab_trade_details passed, but be defensive
            return Err(TradeValidationError::InvalidSymbolConfiguration(
                input_symbol.to_string(),
                output_symbol.to_string(),
            )
            .into());
        };

        // Validate amounts using newtype constructors
        let equity_amount = Shares::new(equity_amount_raw)?;
        let usdc_amount = Usdc::new(usdc_amount_raw)?;

        Ok(Self {
            ticker,
            equity_amount,
            usdc_amount,
            direction,
        })
    }
}

/// Determines onchain trade direction and ticker based on onchain symbol configuration.
///
/// If the on-chain order has USDC as input and a tokenized stock (0x or s1 suffix) as
/// output then it means the order received USDC and gave away a tokenized stock,
/// i.e. sold the tokenized stock onchain.
fn determine_schwab_trade_details(
    onchain_input_symbol: &str,
    onchain_output_symbol: &str,
) -> Result<(Symbol, Direction), OnChainError> {
    // USDC input + tokenized stock output = sold tokenized stock onchain
    if onchain_input_symbol == "USDC" {
        if let Ok(tokenized) = TokenizedEquitySymbol::parse(onchain_output_symbol) {
            return Ok((tokenized.base().clone(), Direction::Sell));
        }
    }

    // tokenized stock input + USDC output = bought tokenized stock onchain
    if onchain_output_symbol == "USDC" {
        if let Ok(tokenized) = TokenizedEquitySymbol::parse(onchain_input_symbol) {
            return Ok((tokenized.base().clone(), Direction::Buy));
        }
    }

    Err(TradeValidationError::InvalidSymbolConfiguration(
        onchain_input_symbol.to_string(),
        onchain_output_symbol.to_string(),
    )
    .into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenized_equity_symbol_parse() {
        // Test that TokenizedEquitySymbol::parse correctly identifies tokenized symbols
        assert!(TokenizedEquitySymbol::parse("AAPL0x").is_ok());
        assert!(TokenizedEquitySymbol::parse("NVDAs1").is_ok());
        assert!(TokenizedEquitySymbol::parse("GME0x").is_ok());
        assert!(TokenizedEquitySymbol::parse("tGME").is_ok());
        assert!(TokenizedEquitySymbol::parse("tAAPL").is_ok());
        assert!(TokenizedEquitySymbol::parse("USDC").is_err());
        assert!(TokenizedEquitySymbol::parse("AAPL").is_err());
        assert!(TokenizedEquitySymbol::parse("").is_err());
    }

    #[test]
    fn test_tokenized_equity_symbol_extract_base() {
        // Test the extract_base method (replaces extract_base_from_tokenized)
        let symbol = TokenizedEquitySymbol::parse("AAPL0x").unwrap();
        assert_eq!(symbol.extract_base(), "AAPL");

        let symbol = TokenizedEquitySymbol::parse("NVDAs1").unwrap();
        assert_eq!(symbol.extract_base(), "NVDA");

        let symbol = TokenizedEquitySymbol::parse("GME0x").unwrap();
        assert_eq!(symbol.extract_base(), "GME");

        let symbol = TokenizedEquitySymbol::parse("tGME").unwrap();
        assert_eq!(symbol.extract_base(), "GME");

        let symbol = TokenizedEquitySymbol::parse("tAAPL").unwrap();
        assert_eq!(symbol.extract_base(), "AAPL");

        // Test edge cases - marker-only symbols should be invalid
        // These fail because after stripping the marker, we're left with an empty string
        // which fails Symbol validation with "Symbol cannot be empty"
        let error = TokenizedEquitySymbol::parse("0x").unwrap_err();
        assert!(matches!(error, OnChainError::Broker(_)));

        let error = TokenizedEquitySymbol::parse("s1").unwrap_err();
        assert!(matches!(error, OnChainError::Broker(_)));

        let error = TokenizedEquitySymbol::parse("t").unwrap_err();
        assert!(matches!(error, OnChainError::Broker(_)));
    }

    #[test]
    fn test_tokenized_equity_symbol_valid() {
        let symbol = TokenizedEquitySymbol::parse("AAPL0x").unwrap();
        assert_eq!(symbol.to_string(), "AAPL0x");
        assert_eq!(symbol.extract_base(), "AAPL");

        let symbol = TokenizedEquitySymbol::parse("NVDAs1").unwrap();
        assert_eq!(symbol.to_string(), "NVDAs1");
        assert_eq!(symbol.extract_base(), "NVDA");

        let symbol = TokenizedEquitySymbol::parse("tGME").unwrap();
        assert_eq!(symbol.to_string(), "tGME");
        assert_eq!(symbol.extract_base(), "GME");

        let symbol = TokenizedEquitySymbol::parse("tAAPL").unwrap();
        assert_eq!(symbol.to_string(), "tAAPL");
        assert_eq!(symbol.extract_base(), "AAPL");
    }

    #[test]
    fn test_tokenized_equity_symbol_invalid() {
        // Empty symbol
        let error = TokenizedEquitySymbol::parse("").unwrap_err();
        assert!(matches!(
            error,
            OnChainError::Validation(TradeValidationError::NotTokenizedEquity(ref s)) if s.is_empty()
        ));

        // USDC symbol
        let error = TokenizedEquitySymbol::parse("USDC").unwrap_err();
        assert!(matches!(
            error,
            OnChainError::Validation(TradeValidationError::NotTokenizedEquity(ref s)) if s == "USDC"
        ));

        // Non-tokenized equity symbols
        let error = TokenizedEquitySymbol::parse("AAPL").unwrap_err();
        assert!(
            matches!(error, OnChainError::Validation(TradeValidationError::NotTokenizedEquity(ref s)) if s == "AAPL")
        );

        let error = TokenizedEquitySymbol::parse("INVALID").unwrap_err();
        assert!(
            matches!(error, OnChainError::Validation(TradeValidationError::NotTokenizedEquity(ref s)) if s == "INVALID")
        );

        let error = TokenizedEquitySymbol::parse("MSFT").unwrap_err();
        assert!(
            matches!(error, OnChainError::Validation(TradeValidationError::NotTokenizedEquity(ref s)) if s == "MSFT")
        );
    }

    #[test]
    fn test_shares_validation() {
        // Test valid shares
        let shares = Shares::new(100.5).unwrap();
        assert!((shares.value() - 100.5).abs() < f64::EPSILON);

        // Test zero shares (valid)
        let shares = Shares::new(0.0).unwrap();
        assert!((shares.value() - 0.0).abs() < f64::EPSILON);

        // Test negative shares (invalid)
        let result = Shares::new(-1.0);
        assert!(matches!(
            result.unwrap_err(),
            TradeValidationError::NegativeShares(-1.0)
        ));
    }

    #[test]
    fn test_usdc_validation() {
        // Test valid USDC amount
        let usdc = Usdc::new(1000.50).unwrap();
        assert!((usdc.value() - 1000.50).abs() < f64::EPSILON);

        // Test zero USDC (valid)
        let usdc = Usdc::new(0.0).unwrap();
        assert!((usdc.value() - 0.0).abs() < f64::EPSILON);

        // Test negative USDC (invalid)
        let result = Usdc::new(-100.0);
        assert!(matches!(
            result.unwrap_err(),
            TradeValidationError::NegativeUsdc(-100.0)
        ));
    }

    #[test]
    fn test_shares_usdc_equality() {
        let shares1 = Shares::new(100.0).unwrap();
        let shares2 = Shares::new(100.0).unwrap();
        let shares3 = Shares::new(200.0).unwrap();

        assert_eq!(shares1, shares2);
        assert_ne!(shares1, shares3);

        let usdc1 = Usdc::new(1000.0).unwrap();
        let usdc2 = Usdc::new(1000.0).unwrap();
        let usdc3 = Usdc::new(2000.0).unwrap();

        assert_eq!(usdc1, usdc2);
        assert_ne!(usdc1, usdc3);
    }

    #[test]
    fn test_determine_schwab_trade_details_usdc_to_0x() {
        let result = determine_schwab_trade_details("USDC", "AAPL0x").unwrap();
        assert_eq!(result.0, symbol!("AAPL"));
        assert_eq!(result.1, Direction::Sell); // Onchain sold AAPL0x for USDC

        let result = determine_schwab_trade_details("USDC", "TSLA0x").unwrap();
        assert_eq!(result.0, symbol!("TSLA"));
        assert_eq!(result.1, Direction::Sell); // Onchain sold TSLA0x for USDC
    }

    #[test]
    fn test_determine_schwab_trade_details_usdc_to_s1() {
        let result = determine_schwab_trade_details("USDC", "NVDAs1").unwrap();
        assert_eq!(result.0, symbol!("NVDA"));
        assert_eq!(result.1, Direction::Sell); // Onchain sold NVDAs1 for USDC
    }

    #[test]
    fn test_determine_schwab_trade_details_0x_to_usdc() {
        let result = determine_schwab_trade_details("AAPL0x", "USDC").unwrap();
        assert_eq!(result.0, symbol!("AAPL"));
        assert_eq!(result.1, Direction::Buy); // Onchain bought AAPL0x with USDC

        let result = determine_schwab_trade_details("TSLA0x", "USDC").unwrap();
        assert_eq!(result.0, symbol!("TSLA"));
        assert_eq!(result.1, Direction::Buy); // Onchain bought TSLA0x with USDC
    }

    #[test]
    fn test_determine_schwab_trade_details_s1_to_usdc() {
        let result = determine_schwab_trade_details("NVDAs1", "USDC").unwrap();
        assert_eq!(result.0, symbol!("NVDA"));
        assert_eq!(result.1, Direction::Buy); // Onchain bought NVDAs1 with USDC
    }

    #[test]
    fn test_determine_schwab_trade_details_usdc_to_t() {
        let result = determine_schwab_trade_details("USDC", "tGME").unwrap();
        assert_eq!(result.0, symbol!("GME"));
        assert_eq!(result.1, Direction::Sell);

        let result = determine_schwab_trade_details("USDC", "tAAPL").unwrap();
        assert_eq!(result.0, symbol!("AAPL"));
        assert_eq!(result.1, Direction::Sell);
    }

    #[test]
    fn test_determine_schwab_trade_details_t_to_usdc() {
        let result = determine_schwab_trade_details("tGME", "USDC").unwrap();
        assert_eq!(result.0, symbol!("GME"));
        assert_eq!(result.1, Direction::Buy);

        let result = determine_schwab_trade_details("tAAPL", "USDC").unwrap();
        assert_eq!(result.0, symbol!("AAPL"));
        assert_eq!(result.1, Direction::Buy);
    }

    #[test]
    fn test_determine_schwab_trade_details_invalid_configurations() {
        let result = determine_schwab_trade_details("BTC", "ETH");
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));

        let result = determine_schwab_trade_details("USDC", "USDC");
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));

        let result = determine_schwab_trade_details("AAPL0x", "TSLA0x");
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));

        let result = determine_schwab_trade_details("", "");
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));
    }

    #[test]
    fn test_trade_details_try_from_io_usdc_to_0x_equity() {
        let details = TradeDetails::try_from_io("USDC", 100.0, "AAPL0x", 0.5).unwrap();

        assert_eq!(details.ticker(), &symbol!("AAPL"));
        assert!((details.equity_amount().value() - 0.5).abs() < f64::EPSILON);
        assert!((details.usdc_amount().value() - 100.0).abs() < f64::EPSILON);
        assert_eq!(details.direction(), Direction::Sell);
    }

    #[test]
    fn test_trade_details_try_from_io_usdc_to_s1_equity_fixes_bug() {
        // This is the key test - s1 suffix should work correctly now
        let details = TradeDetails::try_from_io("USDC", 64.17, "NVDAs1", 0.374).unwrap();

        assert_eq!(details.ticker(), &symbol!("NVDA"));
        assert!((details.equity_amount().value() - 0.374).abs() < f64::EPSILON); // Should be 0.374, not 64.17!
        assert!((details.usdc_amount().value() - 64.17).abs() < f64::EPSILON);
        assert_eq!(details.direction(), Direction::Sell);
    }

    #[test]
    fn test_trade_details_try_from_io_0x_equity_to_usdc() {
        let details = TradeDetails::try_from_io("AAPL0x", 0.5, "USDC", 100.0).unwrap();

        assert_eq!(details.ticker(), &symbol!("AAPL"));
        assert!((details.equity_amount().value() - 0.5).abs() < f64::EPSILON);
        assert!((details.usdc_amount().value() - 100.0).abs() < f64::EPSILON);
        assert_eq!(details.direction(), Direction::Buy);
    }

    #[test]
    fn test_trade_details_try_from_io_s1_equity_to_usdc() {
        let details = TradeDetails::try_from_io("NVDAs1", 0.374, "USDC", 64.17).unwrap();

        assert_eq!(details.ticker(), &symbol!("NVDA"));
        assert!((details.equity_amount().value() - 0.374).abs() < f64::EPSILON);
        assert!((details.usdc_amount().value() - 64.17).abs() < f64::EPSILON);
        assert_eq!(details.direction(), Direction::Buy);
    }

    #[test]
    fn test_trade_details_try_from_io_usdc_to_t_equity() {
        let details = TradeDetails::try_from_io("USDC", 100.0, "tGME", 0.5).unwrap();

        assert_eq!(details.ticker(), &symbol!("GME"));
        assert!((details.equity_amount().value() - 0.5).abs() < f64::EPSILON);
        assert!((details.usdc_amount().value() - 100.0).abs() < f64::EPSILON);
        assert_eq!(details.direction(), Direction::Sell);
    }

    #[test]
    fn test_trade_details_try_from_io_t_equity_to_usdc() {
        let details = TradeDetails::try_from_io("tAAPL", 0.25, "USDC", 50.0).unwrap();

        assert_eq!(details.ticker(), &symbol!("AAPL"));
        assert!((details.equity_amount().value() - 0.25).abs() < f64::EPSILON);
        assert!((details.usdc_amount().value() - 50.0).abs() < f64::EPSILON);
        assert_eq!(details.direction(), Direction::Buy);
    }

    #[test]
    fn test_trade_details_try_from_io_invalid_configurations() {
        let result = TradeDetails::try_from_io("USDC", 100.0, "USDC", 100.0);
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));

        let result = TradeDetails::try_from_io("BTC", 1.0, "ETH", 3000.0);
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));
    }

    #[test]
    fn test_trade_details_negative_amount_validation() {
        // Test negative equity amount
        let result = TradeDetails::try_from_io("USDC", 100.0, "AAPL0x", -0.5);
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::NegativeShares(_))
        ));

        // Test negative USDC amount
        let result = TradeDetails::try_from_io("USDC", -100.0, "AAPL0x", 0.5);
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::NegativeUsdc(_))
        ));
    }

    #[test]
    fn test_tokenized_symbol_macro() {
        // Test that the macro creates valid symbols
        let aapl_symbol = tokenized_symbol!("AAPL0x");
        assert_eq!(aapl_symbol.to_string(), "AAPL0x");
        assert_eq!(aapl_symbol.base().to_string(), "AAPL");

        let nvda_symbol = tokenized_symbol!("NVDAs1");
        assert_eq!(nvda_symbol.to_string(), "NVDAs1");
        assert_eq!(nvda_symbol.base().to_string(), "NVDA");

        // Test that compile-time validation works (these should compile)
        let _valid_symbols = [
            tokenized_symbol!("MSFT0x"),
            tokenized_symbol!("GOOGs1"),
            tokenized_symbol!("TSLA0x"),
        ];
    }

    #[test]
    fn test_symbol_macro() {
        // Test that the macro creates valid symbols
        let aapl_symbol = symbol!("AAPL");
        assert_eq!(aapl_symbol.to_string(), "AAPL");

        let nvda_symbol = symbol!("NVDA");
        assert_eq!(nvda_symbol.to_string(), "NVDA");

        // Test that compile-time validation works (these should compile)
        let _valid_symbols = [symbol!("MSFT"), symbol!("GOOG"), symbol!("TSLA")];
    }

    // Integration tests with real transaction data that caused the 175x overexecution bug

    #[test]
    fn test_real_transaction_0x844_nvda_s1_bug_fix() {
        // Real transaction 0x844...a42d4: 0.374 NVDAs1 sold for 64.169234 USDC
        // The bug was using 64.169234 as share amount instead of 0.374
        let details = TradeDetails::try_from_io("USDC", 64.169_234, "NVDAs1", 0.374).unwrap();

        // Verify we extract the correct amounts
        assert_eq!(details.ticker(), &symbol!("NVDA"));
        assert!((details.equity_amount().value() - 0.374).abs() < 0.0001); // Share amount is 0.374, NOT 64.169234
        assert!((details.usdc_amount().value() - 64.169_234).abs() < 0.0001); // USDC amount is 64.169234
        assert_eq!(details.direction(), Direction::Sell); // Selling NVDAs1 onchain = Sell to Schwab

        // Verify price calculation
        let price_per_share = 64.169_234 / 0.374;
        assert!((price_per_share - 171.58_f64).abs() < 0.01); // ~$171.58 per share
    }

    #[test]
    fn test_real_transaction_0x700_nvda_s1_bug_fix() {
        // Real transaction 0x700...bfb85: 0.2 NVDAs1 sold for 34.645024 USDC
        // The bug was using 34.645024 as share amount instead of 0.2
        let details = TradeDetails::try_from_io("USDC", 34.645_024, "NVDAs1", 0.2).unwrap();

        // Verify we extract the correct amounts
        assert_eq!(details.ticker(), &symbol!("NVDA"));
        assert!((details.equity_amount().value() - 0.2).abs() < 0.0001); // Share amount is 0.2, NOT 34.645024
        assert!((details.usdc_amount().value() - 34.645_024).abs() < 0.0001); // USDC amount is 34.645024
        assert_eq!(details.direction(), Direction::Sell); // Selling NVDAs1 onchain = Sell to Schwab

        // Verify price calculation
        let price_per_share = 34.645_024 / 0.2;
        assert!((price_per_share - 173.23_f64).abs() < 0.01); // ~$173.23 per share
    }

    #[test]
    fn test_gme_trades_with_different_markers_extract_same_ticker() {
        // Test that GME0x, GMEs1, and tGME all map to base symbol "GME"
        let gme_0x_details = TradeDetails::try_from_io("USDC", 5.2, "GME0x", 0.2).unwrap();
        let gme_s1_details = TradeDetails::try_from_io("USDC", 5.1, "GMEs1", 0.2).unwrap();
        let gme_t_details = TradeDetails::try_from_io("USDC", 5.3, "tGME", 0.2).unwrap();

        // All should map to the same base ticker
        assert_eq!(gme_0x_details.ticker(), &symbol!("GME"));
        assert_eq!(gme_s1_details.ticker(), &symbol!("GME"));
        assert_eq!(gme_t_details.ticker(), &symbol!("GME"));
        assert_eq!(gme_0x_details.ticker(), gme_s1_details.ticker());
        assert_eq!(gme_0x_details.ticker(), gme_t_details.ticker());

        // Verify amounts are extracted correctly
        assert!((gme_0x_details.equity_amount().value() - 0.2).abs() < f64::EPSILON);
        assert!((gme_s1_details.equity_amount().value() - 0.2).abs() < f64::EPSILON);
        assert!((gme_t_details.equity_amount().value() - 0.2).abs() < f64::EPSILON);
        assert!((gme_0x_details.usdc_amount().value() - 5.2).abs() < f64::EPSILON);
        assert!((gme_s1_details.usdc_amount().value() - 5.1).abs() < f64::EPSILON);
        assert!((gme_t_details.usdc_amount().value() - 5.3).abs() < f64::EPSILON);
    }

    #[test]
    fn test_edge_case_validation_very_small_amounts() {
        // Test very small but valid amounts
        let details = TradeDetails::try_from_io("USDC", 0.01, "AAPLs1", 0.0001).unwrap();
        assert_eq!(details.ticker(), &symbol!("AAPL"));
        assert!((details.equity_amount().value() - 0.0001).abs() < f64::EPSILON);
        assert!((details.usdc_amount().value() - 0.01).abs() < f64::EPSILON);
    }

    #[test]
    fn test_edge_case_validation_very_large_amounts() {
        // Test large but realistic amounts
        let details = TradeDetails::try_from_io("USDC", 1_000_000.0, "BRKs1", 100.0).unwrap();
        assert_eq!(details.ticker(), &symbol!("BRK"));
        assert!((details.equity_amount().value() - 100.0).abs() < f64::EPSILON);
        assert!((details.usdc_amount().value() - 1_000_000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_tokenized_equity_marker_variants() {
        let t_marker = TokenizedEquityMarker::T;
        let zerox_marker = TokenizedEquityMarker::ZeroX;
        let s1_marker = TokenizedEquityMarker::S1;

        assert_eq!(t_marker.as_str(), "t");
        assert_eq!(zerox_marker.as_str(), "0x");
        assert_eq!(s1_marker.as_str(), "s1");

        assert!(t_marker.is_prefix());
        assert!(!zerox_marker.is_prefix());
        assert!(!s1_marker.is_prefix());
    }

    #[test]
    fn test_invalid_marker_combinations() {
        let gme_symbol = Symbol::new("GME0x").unwrap();
        let t_symbol = TokenizedEquitySymbol::new(gme_symbol, TokenizedEquityMarker::T);

        assert_eq!(t_symbol.to_string(), "tGME0x");
        assert_eq!(t_symbol.extract_base(), "GME0x");
    }
}
