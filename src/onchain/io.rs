//! Order I/O parsing: extracts symbol pairs, amounts, prices,
//! and trade direction from raw Raindex order inputs/outputs.
//!
//! Determines which side of a Raindex fill is USDC vs
//! tokenized equity (tTICKER format, e.g. tAAPL, tSPYM),
//! computes the trade direction (buy/sell), and validates
//! amounts before further processing.

use rust_decimal::Decimal;
use std::fmt;
use std::str::FromStr;

use st0x_execution::{Direction, FractionalShares, Symbol};

use super::OnChainError;
use crate::onchain::trade::TradeValidationError;

/// Test-only macro to create a TokenizedEquitySymbol.
#[cfg(test)]
#[macro_export]
macro_rules! tokenized_symbol {
    ($symbol:expr) => {
        $crate::onchain::io::TokenizedEquitySymbol::parse($symbol).unwrap()
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

/// Represents a validated USDC amount (non-negative)
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Usdc(Decimal);

impl Usdc {
    pub(crate) fn new(value: Decimal) -> Result<Self, TradeValidationError> {
        if value < Decimal::ZERO {
            return Err(TradeValidationError::NegativeUsdc(value));
        }
        Ok(Self(value))
    }

    pub(crate) fn value(self) -> Decimal {
        self.0
    }
}

/// Validated tokenized equity symbol in tTICKER format
/// (e.g. tAAPL, tSPYM).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TokenizedEquitySymbol(Symbol);

impl TokenizedEquitySymbol {
    /// Parses a tTICKER string (e.g. "tAAPL", "tSPYM").
    pub(crate) fn parse(symbol: &str) -> Result<Self, OnChainError> {
        let Some(stripped) = symbol.strip_prefix('t') else {
            return Err(OnChainError::Validation(
                TradeValidationError::NotTokenizedEquity {
                    symbol_provided: symbol.to_string(),
                },
            ));
        };

        let base = Symbol::new(stripped)?;
        Ok(Self(base))
    }

    /// Gets the base equity symbol
    pub(crate) fn base(&self) -> &Symbol {
        &self.0
    }

    /// Extract the base symbol (equivalent to the old extract_base_from_tokenized)
    pub(crate) fn extract_base(&self) -> String {
        self.0.to_string()
    }
}

impl fmt::Display for TokenizedEquitySymbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "t{}", self.0)
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
    equity_amount: FractionalShares,
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
    pub(crate) fn equity_amount(&self) -> FractionalShares {
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
        input_amount: Decimal,
        output_symbol: &str,
        output_amount: Decimal,
    ) -> Result<Self, OnChainError> {
        let (ticker, direction) = determine_schwab_trade_details(input_symbol, output_symbol)?;

        // Extract equity and USDC amounts based on which side is the tokenized equity
        let (equity_amount_raw, usdc_amount_raw) = if input_symbol == "USDC"
            && TokenizedEquitySymbol::parse(output_symbol).is_ok()
        {
            // USDC -> tTICKER: output is equity, input is USDC
            (output_amount, input_amount)
        } else if output_symbol == "USDC" && TokenizedEquitySymbol::parse(input_symbol).is_ok() {
            // tTICKER -> USDC: input is equity, output is USDC
            (input_amount, output_amount)
        } else {
            // Unreachable if determine_schwab_trade_details passed
            return Err(TradeValidationError::InvalidSymbolConfiguration(
                input_symbol.to_string(),
                output_symbol.to_string(),
            )
            .into());
        };

        if equity_amount_raw < Decimal::ZERO {
            return Err(TradeValidationError::NegativeShares(equity_amount_raw).into());
        }
        let equity_amount = FractionalShares::new(equity_amount_raw);
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
/// If the on-chain order has USDC as input and a tokenized stock (t prefix) as
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
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn test_tokenized_equity_symbol_parse() {
        TokenizedEquitySymbol::parse("tGME").unwrap();
        TokenizedEquitySymbol::parse("tAAPL").unwrap();
        TokenizedEquitySymbol::parse("tSPYM").unwrap();

        let err = TokenizedEquitySymbol::parse("USDC").unwrap_err();
        assert!(
            matches!(
                err,
                OnChainError::Validation(
                    TradeValidationError::NotTokenizedEquity { ref symbol_provided }
                ) if symbol_provided == "USDC"
            ),
            "Expected NotTokenizedEquity for USDC, got: {err:?}"
        );

        let err = TokenizedEquitySymbol::parse("AAPL").unwrap_err();
        assert!(
            matches!(
                err,
                OnChainError::Validation(
                    TradeValidationError::NotTokenizedEquity { ref symbol_provided }
                ) if symbol_provided == "AAPL"
            ),
            "Expected NotTokenizedEquity for AAPL, got: {err:?}"
        );

        let err = TokenizedEquitySymbol::parse("").unwrap_err();
        assert!(
            matches!(
                err,
                OnChainError::Validation(
                    TradeValidationError::NotTokenizedEquity { ref symbol_provided }
                ) if symbol_provided.is_empty()
            ),
            "Expected NotTokenizedEquity for empty, got: {err:?}"
        );
    }

    #[test]
    fn test_tokenized_equity_symbol_extract_base() {
        let symbol = TokenizedEquitySymbol::parse("tGME").unwrap();
        assert_eq!(symbol.extract_base(), "GME");

        let symbol = TokenizedEquitySymbol::parse("tAAPL").unwrap();
        assert_eq!(symbol.extract_base(), "AAPL");

        let symbol = TokenizedEquitySymbol::parse("tNVDA").unwrap();
        assert_eq!(symbol.extract_base(), "NVDA");

        let symbol = TokenizedEquitySymbol::parse("tSPYM").unwrap();
        assert_eq!(symbol.extract_base(), "SPYM");

        // Prefix-only "t" -> empty base -> Symbol validation fails
        let error = TokenizedEquitySymbol::parse("t").unwrap_err();
        assert!(matches!(error, OnChainError::EmptySymbol(_)));
    }

    #[test]
    fn test_tokenized_equity_symbol_valid() {
        let symbol = TokenizedEquitySymbol::parse("tGME").unwrap();
        assert_eq!(symbol.to_string(), "tGME");
        assert_eq!(symbol.extract_base(), "GME");

        let symbol = TokenizedEquitySymbol::parse("tAAPL").unwrap();
        assert_eq!(symbol.to_string(), "tAAPL");
        assert_eq!(symbol.extract_base(), "AAPL");

        let symbol = TokenizedEquitySymbol::parse("tSPYM").unwrap();
        assert_eq!(symbol.to_string(), "tSPYM");
        assert_eq!(symbol.extract_base(), "SPYM");
    }

    #[test]
    fn test_tokenized_equity_symbol_invalid() {
        let error = TokenizedEquitySymbol::parse("").unwrap_err();
        assert!(matches!(
            error,
            OnChainError::Validation(
                TradeValidationError::NotTokenizedEquity { ref symbol_provided }
            ) if symbol_provided.is_empty()
        ));

        let error = TokenizedEquitySymbol::parse("USDC").unwrap_err();
        assert!(matches!(
            error,
            OnChainError::Validation(
                TradeValidationError::NotTokenizedEquity { ref symbol_provided }
            ) if symbol_provided == "USDC"
        ));

        let error = TokenizedEquitySymbol::parse("AAPL").unwrap_err();
        assert!(matches!(
            error,
            OnChainError::Validation(
                TradeValidationError::NotTokenizedEquity { ref symbol_provided }
            ) if symbol_provided == "AAPL"
        ));

        // Legacy formats are no longer accepted
        let error = TokenizedEquitySymbol::parse("AAPL0x").unwrap_err();
        assert!(matches!(
            error,
            OnChainError::Validation(
                TradeValidationError::NotTokenizedEquity { ref symbol_provided }
            ) if symbol_provided == "AAPL0x"
        ));

        let error = TokenizedEquitySymbol::parse("NVDAs1").unwrap_err();
        assert!(matches!(
            error,
            OnChainError::Validation(
                TradeValidationError::NotTokenizedEquity { ref symbol_provided }
            ) if symbol_provided == "NVDAs1"
        ));
    }

    #[test]
    fn test_usdc_validation() {
        let usdc = Usdc::new(dec!(1000.50)).unwrap();
        assert_eq!(usdc.value(), dec!(1000.50));

        let usdc = Usdc::new(Decimal::ZERO).unwrap();
        assert_eq!(usdc.value(), Decimal::ZERO);

        let result = Usdc::new(dec!(-100));
        assert!(matches!(
            result.unwrap_err(),
            TradeValidationError::NegativeUsdc(_)
        ));
    }

    #[test]
    fn test_usdc_equality() {
        let usdc1 = Usdc::new(dec!(1000)).unwrap();
        let usdc2 = Usdc::new(dec!(1000)).unwrap();
        let usdc3 = Usdc::new(dec!(2000)).unwrap();

        assert_eq!(usdc1, usdc2);
        assert_ne!(usdc1, usdc3);
    }

    #[test]
    fn test_determine_schwab_trade_details_usdc_to_tokenized() {
        let result = determine_schwab_trade_details("USDC", "tAAPL").unwrap();
        assert_eq!(result.0, symbol!("AAPL"));
        assert_eq!(result.1, Direction::Sell);

        let result = determine_schwab_trade_details("USDC", "tTSLA").unwrap();
        assert_eq!(result.0, symbol!("TSLA"));
        assert_eq!(result.1, Direction::Sell);

        let result = determine_schwab_trade_details("USDC", "tGME").unwrap();
        assert_eq!(result.0, symbol!("GME"));
        assert_eq!(result.1, Direction::Sell);
    }

    #[test]
    fn test_determine_schwab_trade_details_tokenized_to_usdc() {
        let result = determine_schwab_trade_details("tAAPL", "USDC").unwrap();
        assert_eq!(result.0, symbol!("AAPL"));
        assert_eq!(result.1, Direction::Buy);

        let result = determine_schwab_trade_details("tTSLA", "USDC").unwrap();
        assert_eq!(result.0, symbol!("TSLA"));
        assert_eq!(result.1, Direction::Buy);

        let result = determine_schwab_trade_details("tGME", "USDC").unwrap();
        assert_eq!(result.0, symbol!("GME"));
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

        let result = determine_schwab_trade_details("tAAPL", "tTSLA");
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
    fn test_trade_details_try_from_io_usdc_to_tokenized() {
        let details = TradeDetails::try_from_io("USDC", dec!(100), "tAAPL", dec!(0.5)).unwrap();

        assert_eq!(details.ticker(), &symbol!("AAPL"));
        assert_eq!(details.equity_amount().inner(), dec!(0.5));
        assert_eq!(details.usdc_amount().value(), dec!(100));
        assert_eq!(details.direction(), Direction::Sell);
    }

    #[test]
    fn test_trade_details_try_from_io_tokenized_to_usdc() {
        let details = TradeDetails::try_from_io("tAAPL", dec!(0.5), "USDC", dec!(100)).unwrap();

        assert_eq!(details.ticker(), &symbol!("AAPL"));
        assert_eq!(details.equity_amount().inner(), dec!(0.5));
        assert_eq!(details.usdc_amount().value(), dec!(100));
        assert_eq!(details.direction(), Direction::Buy);
    }

    #[test]
    fn test_trade_details_try_from_io_nvda() {
        let details = TradeDetails::try_from_io("USDC", dec!(64.17), "tNVDA", dec!(0.374)).unwrap();

        assert_eq!(details.ticker(), &symbol!("NVDA"));
        assert_eq!(details.equity_amount().inner(), dec!(0.374));
        assert_eq!(details.usdc_amount().value(), dec!(64.17));
        assert_eq!(details.direction(), Direction::Sell);

        let details = TradeDetails::try_from_io("tNVDA", dec!(0.374), "USDC", dec!(64.17)).unwrap();

        assert_eq!(details.ticker(), &symbol!("NVDA"));
        assert_eq!(details.equity_amount().inner(), dec!(0.374));
        assert_eq!(details.usdc_amount().value(), dec!(64.17));
        assert_eq!(details.direction(), Direction::Buy);
    }

    #[test]
    fn test_trade_details_try_from_io_invalid_configurations() {
        let result = TradeDetails::try_from_io("USDC", dec!(100), "USDC", dec!(100));
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));

        let result = TradeDetails::try_from_io("BTC", dec!(1), "ETH", dec!(3000));
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));
    }

    #[test]
    fn test_trade_details_negative_amount_validation() {
        let result = TradeDetails::try_from_io("USDC", dec!(100), "tAAPL", dec!(-0.5));
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::NegativeShares(_))
        ));

        let result = TradeDetails::try_from_io("USDC", dec!(-100), "tAAPL", dec!(0.5));
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::NegativeUsdc(_))
        ));
    }

    #[test]
    fn test_tokenized_symbol_macro() {
        let aapl_symbol = tokenized_symbol!("tAAPL");
        assert_eq!(aapl_symbol.to_string(), "tAAPL");
        assert_eq!(aapl_symbol.base().to_string(), "AAPL");

        let nvda_symbol = tokenized_symbol!("tNVDA");
        assert_eq!(nvda_symbol.to_string(), "tNVDA");
        assert_eq!(nvda_symbol.base().to_string(), "NVDA");

        let _valid_symbols = [
            tokenized_symbol!("tMSFT"),
            tokenized_symbol!("tGOOG"),
            tokenized_symbol!("tTSLA"),
        ];
    }

    #[test]
    fn test_symbol_macro() {
        let aapl_symbol = symbol!("AAPL");
        assert_eq!(aapl_symbol.to_string(), "AAPL");

        let nvda_symbol = symbol!("NVDA");
        assert_eq!(nvda_symbol.to_string(), "NVDA");

        let _valid_symbols = [symbol!("MSFT"), symbol!("GOOG"), symbol!("TSLA")];
    }

    #[test]
    fn test_real_transaction_nvda_amount_extraction() {
        // Real transaction: 0.374 tNVDA sold for 64.169234 USDC
        // Verifies equity vs USDC amounts are not swapped
        let details =
            TradeDetails::try_from_io("USDC", dec!(64.169234), "tNVDA", dec!(0.374)).unwrap();

        assert_eq!(details.ticker(), &symbol!("NVDA"));
        assert_eq!(details.equity_amount().inner(), dec!(0.374));
        assert_eq!(details.usdc_amount().value(), dec!(64.169234));
        assert_eq!(details.direction(), Direction::Sell);

        let price_per_share = dec!(64.169234) / dec!(0.374);
        assert!((price_per_share - dec!(171.58)).abs() < dec!(0.01));
    }

    #[test]
    fn test_real_transaction_nvda_small_trade() {
        // Real transaction: 0.2 tNVDA sold for 34.645024 USDC
        let details =
            TradeDetails::try_from_io("USDC", dec!(34.645024), "tNVDA", dec!(0.2)).unwrap();

        assert_eq!(details.ticker(), &symbol!("NVDA"));
        assert_eq!(details.equity_amount().inner(), dec!(0.2));
        assert_eq!(details.usdc_amount().value(), dec!(34.645024));
        assert_eq!(details.direction(), Direction::Sell);

        let price_per_share = dec!(34.645024) / dec!(0.2);
        assert!((price_per_share - dec!(173.23)).abs() < dec!(0.01));
    }

    #[test]
    fn test_edge_case_validation_very_small_amounts() {
        let details = TradeDetails::try_from_io("USDC", dec!(0.01), "tAAPL", dec!(0.0001)).unwrap();
        assert_eq!(details.ticker(), &symbol!("AAPL"));
        assert_eq!(details.equity_amount().inner(), dec!(0.0001));
        assert_eq!(details.usdc_amount().value(), dec!(0.01));
    }

    #[test]
    fn test_edge_case_validation_very_large_amounts() {
        let details = TradeDetails::try_from_io("USDC", dec!(1000000), "tBRK", dec!(100)).unwrap();
        assert_eq!(details.ticker(), &symbol!("BRK"));
        assert_eq!(details.equity_amount().inner(), dec!(100));
        assert_eq!(details.usdc_amount().value(), dec!(1000000));
    }
}
