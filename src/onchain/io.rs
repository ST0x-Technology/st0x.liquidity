//! Order I/O parsing: extracts symbol pairs, amounts, prices,
//! and trade direction from raw Raindex order inputs/outputs.
//!
//! Determines which side of a Raindex fill is USDC vs
//! tokenized equity (tTICKER or wtTICKER format), computes the
//! trade direction (buy/sell), and validates amounts before
//! further processing.

use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use st0x_exact_decimal::ExactDecimal;
use st0x_execution::{Direction, FractionalShares, Symbol};

use super::OnChainError;
use crate::onchain::trade::TradeValidationError;

/// Test-only macro to create a `TokenizedSymbol<Form>`.
/// The form type must be specified explicitly.
#[cfg(test)]
#[macro_export]
macro_rules! tokenized_symbol {
    ($form:ty, $symbol:expr) => {
        $crate::onchain::io::TokenizedSymbol::<$form>::parse($symbol).unwrap()
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
pub(crate) struct Usdc(ExactDecimal);

impl Usdc {
    pub(crate) fn new(value: ExactDecimal) -> Result<Self, TradeValidationError> {
        if value.is_negative().map_err(TradeValidationError::Float)? {
            return Err(TradeValidationError::NegativeUsdc(value));
        }
        Ok(Self(value))
    }

    pub(crate) fn value(self) -> ExactDecimal {
        self.0
    }
}

/// Distinguishes how an equity was tokenized onchain.
/// Each form defines the symbol prefix used in Raindex orders.
pub(crate) trait TokenizationForm: fmt::Debug + Clone + PartialEq + Eq {
    fn prefix() -> &'static str;
}

/// 1:1 minted tokenized shares (tTICKER, e.g. tAAPL, tSPYM).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OneToOneTokenizedShares;

impl TokenizationForm for OneToOneTokenizedShares {
    fn prefix() -> &'static str {
        "t"
    }
}

/// ERC-4626 vault shares wrapping tokenized equity
/// (wtTICKER, e.g. wtCOIN, wtAAPL).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WrappedTokenizedShares;

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
pub(crate) struct TokenizedSymbol<Form: TokenizationForm> {
    _form: PhantomData<Form>,
    symbol: Symbol,
}

impl<Form: TokenizationForm> TokenizedSymbol<Form> {
    pub(crate) fn parse(input: &str) -> Result<Self, OnChainError> {
        let Some(stripped) = input.strip_prefix(Form::prefix()) else {
            return Err(OnChainError::Validation(
                TradeValidationError::NotTokenizedEquity {
                    symbol_provided: input.to_string(),
                },
            ));
        };

        let symbol = Symbol::new(stripped)?;
        Ok(Self {
            _form: PhantomData,
            symbol,
        })
    }

    pub(crate) fn base(&self) -> &Symbol {
        &self.symbol
    }
}

impl<Form: TokenizationForm> fmt::Display for TokenizedSymbol<Form> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Form::prefix(), self.symbol)
    }
}

impl<Form: TokenizationForm> FromStr for TokenizedSymbol<Form> {
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
    /// Extracts trade details from input/output symbol and amount pairs.
    /// Both symbols must be either USDC or a wrapped tokenized equity
    /// (wtTICKER), since Raindex orders always involve wrapped tokens.
    pub(crate) fn try_from_io(
        input_symbol: &str,
        input_amount: ExactDecimal,
        output_symbol: &str,
        output_amount: ExactDecimal,
    ) -> Result<Self, OnChainError> {
        let (ticker, direction) = determine_trade_details(input_symbol, output_symbol)?;

        let is_wrapped_equity =
            |symbol: &str| TokenizedSymbol::<WrappedTokenizedShares>::parse(symbol).is_ok();

        // Extract equity and USDC amounts based on which side is the tokenized equity
        let (equity_amount_raw, usdc_amount_raw) =
            if input_symbol == "USDC" && is_wrapped_equity(output_symbol) {
                (output_amount, input_amount)
            } else if output_symbol == "USDC" && is_wrapped_equity(input_symbol) {
                (input_amount, output_amount)
            } else {
                return Err(TradeValidationError::InvalidSymbolConfiguration(
                    input_symbol.to_string(),
                    output_symbol.to_string(),
                )
                .into());
            };

        if equity_amount_raw
            .is_negative()
            .map_err(TradeValidationError::Float)?
        {
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

/// Determines onchain trade direction and ticker based on onchain
/// symbol configuration.
///
/// If the on-chain order has USDC as input and a wrapped tokenized
/// equity (wt prefix) as output, the order sold tokenized equity
/// onchain. Raindex orders always involve wrapped tokens (wtTICKER).
fn determine_trade_details(
    onchain_input_symbol: &str,
    onchain_output_symbol: &str,
) -> Result<(Symbol, Direction), OnChainError> {
    if onchain_input_symbol == "USDC"
        && let Ok(equity) = TokenizedSymbol::<WrappedTokenizedShares>::parse(onchain_output_symbol)
    {
        return Ok((equity.base().clone(), Direction::Sell));
    }

    if onchain_output_symbol == "USDC"
        && let Ok(equity) = TokenizedSymbol::<WrappedTokenizedShares>::parse(onchain_input_symbol)
    {
        return Ok((equity.base().clone(), Direction::Buy));
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

    fn ed(value: &str) -> ExactDecimal {
        ExactDecimal::parse(value).unwrap()
    }

    #[test]
    fn test_tokenized_equity_symbol_parse() {
        TokenizedSymbol::<OneToOneTokenizedShares>::parse("tGME").unwrap();
        TokenizedSymbol::<OneToOneTokenizedShares>::parse("tAAPL").unwrap();
        TokenizedSymbol::<OneToOneTokenizedShares>::parse("tSPYM").unwrap();

        let err = TokenizedSymbol::<OneToOneTokenizedShares>::parse("USDC").unwrap_err();
        assert!(
            matches!(
                err,
                OnChainError::Validation(
                    TradeValidationError::NotTokenizedEquity { ref symbol_provided }
                ) if symbol_provided == "USDC"
            ),
            "Expected NotTokenizedEquity for USDC, got: {err:?}"
        );

        let err = TokenizedSymbol::<OneToOneTokenizedShares>::parse("AAPL").unwrap_err();
        assert!(
            matches!(
                err,
                OnChainError::Validation(
                    TradeValidationError::NotTokenizedEquity { ref symbol_provided }
                ) if symbol_provided == "AAPL"
            ),
            "Expected NotTokenizedEquity for AAPL, got: {err:?}"
        );

        let err = TokenizedSymbol::<OneToOneTokenizedShares>::parse("").unwrap_err();
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
        let symbol = TokenizedSymbol::<OneToOneTokenizedShares>::parse("tGME").unwrap();
        assert_eq!(symbol.base().to_string(), "GME");

        let symbol = TokenizedSymbol::<OneToOneTokenizedShares>::parse("tAAPL").unwrap();
        assert_eq!(symbol.base().to_string(), "AAPL");

        let symbol = TokenizedSymbol::<OneToOneTokenizedShares>::parse("tNVDA").unwrap();
        assert_eq!(symbol.base().to_string(), "NVDA");

        let symbol = TokenizedSymbol::<OneToOneTokenizedShares>::parse("tSPYM").unwrap();
        assert_eq!(symbol.base().to_string(), "SPYM");

        // Prefix-only "t" -> empty base -> Symbol validation fails
        let error = TokenizedSymbol::<OneToOneTokenizedShares>::parse("t").unwrap_err();
        assert!(matches!(error, OnChainError::EmptySymbol(_)));
    }

    #[test]
    fn test_tokenized_equity_symbol_valid() {
        let symbol = TokenizedSymbol::<OneToOneTokenizedShares>::parse("tGME").unwrap();
        assert_eq!(symbol.to_string(), "tGME");
        assert_eq!(symbol.base().to_string(), "GME");

        let symbol = TokenizedSymbol::<OneToOneTokenizedShares>::parse("tAAPL").unwrap();
        assert_eq!(symbol.to_string(), "tAAPL");
        assert_eq!(symbol.base().to_string(), "AAPL");

        let symbol = TokenizedSymbol::<OneToOneTokenizedShares>::parse("tSPYM").unwrap();
        assert_eq!(symbol.to_string(), "tSPYM");
        assert_eq!(symbol.base().to_string(), "SPYM");
    }

    #[test]
    fn test_tokenized_equity_symbol_invalid() {
        let error = TokenizedSymbol::<OneToOneTokenizedShares>::parse("").unwrap_err();
        assert!(matches!(
            error,
            OnChainError::Validation(
                TradeValidationError::NotTokenizedEquity { ref symbol_provided }
            ) if symbol_provided.is_empty()
        ));

        let error = TokenizedSymbol::<OneToOneTokenizedShares>::parse("USDC").unwrap_err();
        assert!(matches!(
            error,
            OnChainError::Validation(
                TradeValidationError::NotTokenizedEquity { ref symbol_provided }
            ) if symbol_provided == "USDC"
        ));

        let error = TokenizedSymbol::<OneToOneTokenizedShares>::parse("AAPL").unwrap_err();
        assert!(matches!(
            error,
            OnChainError::Validation(
                TradeValidationError::NotTokenizedEquity { ref symbol_provided }
            ) if symbol_provided == "AAPL"
        ));

        // Legacy formats are no longer accepted
        let error = TokenizedSymbol::<OneToOneTokenizedShares>::parse("AAPL0x").unwrap_err();
        assert!(matches!(
            error,
            OnChainError::Validation(
                TradeValidationError::NotTokenizedEquity { ref symbol_provided }
            ) if symbol_provided == "AAPL0x"
        ));

        let error = TokenizedSymbol::<OneToOneTokenizedShares>::parse("NVDAs1").unwrap_err();
        assert!(matches!(
            error,
            OnChainError::Validation(
                TradeValidationError::NotTokenizedEquity { ref symbol_provided }
            ) if symbol_provided == "NVDAs1"
        ));
    }

    #[test]
    fn test_usdc_validation() {
        let usdc = Usdc::new(ed("1000.50")).unwrap();
        assert_eq!(usdc.value(), ed("1000.50"));

        let usdc = Usdc::new(ExactDecimal::zero()).unwrap();
        assert_eq!(usdc.value(), ExactDecimal::zero());

        let result = Usdc::new(ed("-100"));
        assert!(matches!(
            result.unwrap_err(),
            TradeValidationError::NegativeUsdc(_)
        ));
    }

    #[test]
    fn test_usdc_equality() {
        let usdc1 = Usdc::new(ed("1000")).unwrap();
        let usdc2 = Usdc::new(ed("1000")).unwrap();
        let usdc3 = Usdc::new(ed("2000")).unwrap();

        assert_eq!(usdc1, usdc2);
        assert_ne!(usdc1, usdc3);
    }

    #[test]
    fn test_determine_trade_details_usdc_to_wrapped() {
        let result = determine_trade_details("USDC", "wtAAPL").unwrap();
        assert_eq!(result.0, symbol!("AAPL"));
        assert_eq!(result.1, Direction::Sell);

        let result = determine_trade_details("USDC", "wtTSLA").unwrap();
        assert_eq!(result.0, symbol!("TSLA"));
        assert_eq!(result.1, Direction::Sell);

        let result = determine_trade_details("USDC", "wtGME").unwrap();
        assert_eq!(result.0, symbol!("GME"));
        assert_eq!(result.1, Direction::Sell);
    }

    #[test]
    fn test_determine_trade_details_wrapped_to_usdc() {
        let result = determine_trade_details("wtAAPL", "USDC").unwrap();
        assert_eq!(result.0, symbol!("AAPL"));
        assert_eq!(result.1, Direction::Buy);

        let result = determine_trade_details("wtTSLA", "USDC").unwrap();
        assert_eq!(result.0, symbol!("TSLA"));
        assert_eq!(result.1, Direction::Buy);

        let result = determine_trade_details("wtGME", "USDC").unwrap();
        assert_eq!(result.0, symbol!("GME"));
        assert_eq!(result.1, Direction::Buy);
    }

    #[test]
    fn test_determine_trade_details_rejects_unwrapped_prefix() {
        let result = determine_trade_details("USDC", "tAAPL");
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));

        let result = determine_trade_details("tAAPL", "USDC");
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));
    }

    #[test]
    fn test_determine_trade_details_invalid_configurations() {
        let result = determine_trade_details("BTC", "ETH");
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));

        let result = determine_trade_details("USDC", "USDC");
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));

        let result = determine_trade_details("wtAAPL", "wtTSLA");
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));

        let result = determine_trade_details("", "");
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));
    }

    #[test]
    fn test_trade_details_try_from_io_usdc_to_wrapped() {
        let details = TradeDetails::try_from_io("USDC", ed("100"), "wtAAPL", ed("0.5")).unwrap();

        assert_eq!(details.ticker(), &symbol!("AAPL"));
        assert_eq!(details.equity_amount().inner(), ed("0.5"));
        assert_eq!(details.usdc_amount().value(), ed("100"));
        assert_eq!(details.direction(), Direction::Sell);
    }

    #[test]
    fn test_trade_details_try_from_io_wrapped_to_usdc() {
        let details = TradeDetails::try_from_io("wtAAPL", ed("0.5"), "USDC", ed("100")).unwrap();

        assert_eq!(details.ticker(), &symbol!("AAPL"));
        assert_eq!(details.equity_amount().inner(), ed("0.5"));
        assert_eq!(details.usdc_amount().value(), ed("100"));
        assert_eq!(details.direction(), Direction::Buy);
    }

    #[test]
    fn test_trade_details_try_from_io_nvda() {
        let details =
            TradeDetails::try_from_io("USDC", ed("64.17"), "wtNVDA", ed("0.374")).unwrap();

        assert_eq!(details.ticker(), &symbol!("NVDA"));
        assert_eq!(details.equity_amount().inner(), ed("0.374"));
        assert_eq!(details.usdc_amount().value(), ed("64.17"));
        assert_eq!(details.direction(), Direction::Sell);

        let details =
            TradeDetails::try_from_io("wtNVDA", ed("0.374"), "USDC", ed("64.17")).unwrap();

        assert_eq!(details.ticker(), &symbol!("NVDA"));
        assert_eq!(details.equity_amount().inner(), ed("0.374"));
        assert_eq!(details.usdc_amount().value(), ed("64.17"));
        assert_eq!(details.direction(), Direction::Buy);
    }

    #[test]
    fn test_trade_details_try_from_io_invalid_configurations() {
        let result = TradeDetails::try_from_io("USDC", ed("100"), "USDC", ed("100"));
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));

        let result = TradeDetails::try_from_io("BTC", ed("1"), "ETH", ed("3000"));
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));
    }

    #[test]
    fn test_trade_details_negative_amount_validation() {
        let result = TradeDetails::try_from_io("USDC", ed("100"), "wtAAPL", ed("-0.5"));
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::NegativeShares(_))
        ));

        let result = TradeDetails::try_from_io("USDC", ed("-100"), "wtAAPL", ed("0.5"));
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::NegativeUsdc(_))
        ));
    }

    #[test]
    fn test_tokenized_symbol_macro() {
        let aapl_symbol = tokenized_symbol!(WrappedTokenizedShares, "wtAAPL");
        assert_eq!(aapl_symbol.to_string(), "wtAAPL");
        assert_eq!(aapl_symbol.base().to_string(), "AAPL");

        let nvda_symbol = tokenized_symbol!(WrappedTokenizedShares, "wtNVDA");
        assert_eq!(nvda_symbol.to_string(), "wtNVDA");
        assert_eq!(nvda_symbol.base().to_string(), "NVDA");

        let _valid_symbols = [
            tokenized_symbol!(WrappedTokenizedShares, "wtMSFT"),
            tokenized_symbol!(WrappedTokenizedShares, "wtGOOG"),
            tokenized_symbol!(WrappedTokenizedShares, "wtTSLA"),
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
        // Real transaction: 0.374 wtNVDA sold for 64.169234 USDC
        // Verifies equity vs USDC amounts are not swapped
        let details =
            TradeDetails::try_from_io("USDC", ed("64.169234"), "wtNVDA", ed("0.374")).unwrap();

        assert_eq!(details.ticker(), &symbol!("NVDA"));
        assert_eq!(details.equity_amount().inner(), ed("0.374"));
        assert_eq!(details.usdc_amount().value(), ed("64.169234"));
        assert_eq!(details.direction(), Direction::Sell);

        let price_per_share = (ed("64.169234") / ed("0.374")).unwrap();
        let diff = (price_per_share - ed("171.58")).unwrap().abs().unwrap();
        assert!(diff < ed("0.01"));
    }

    #[test]
    fn test_edge_case_validation_very_small_amounts() {
        let details =
            TradeDetails::try_from_io("USDC", ed("0.01"), "wtAAPL", ed("0.0001")).unwrap();
        assert_eq!(details.ticker(), &symbol!("AAPL"));
        assert_eq!(details.equity_amount().inner(), ed("0.0001"));
        assert_eq!(details.usdc_amount().value(), ed("0.01"));
    }

    #[test]
    fn test_edge_case_validation_very_large_amounts() {
        let details = TradeDetails::try_from_io("USDC", ed("1000000"), "wtBRK", ed("100")).unwrap();
        assert_eq!(details.ticker(), &symbol!("BRK"));
        assert_eq!(details.equity_amount().inner(), ed("100"));
        assert_eq!(details.usdc_amount().value(), ed("1000000"));
    }

    #[test]
    fn test_wrapped_tokenized_equity_symbol_parse() {
        let symbol = TokenizedSymbol::<WrappedTokenizedShares>::parse("wtCOIN").unwrap();
        assert_eq!(symbol.base(), &symbol!("COIN"));
        assert_eq!(symbol.to_string(), "wtCOIN");

        let symbol = TokenizedSymbol::<WrappedTokenizedShares>::parse("wtAAPL").unwrap();
        assert_eq!(symbol.base(), &symbol!("AAPL"));

        let err = TokenizedSymbol::<WrappedTokenizedShares>::parse("tCOIN").unwrap_err();
        assert!(matches!(
            err,
            OnChainError::Validation(TradeValidationError::NotTokenizedEquity { .. })
        ));

        let err = TokenizedSymbol::<WrappedTokenizedShares>::parse("USDC").unwrap_err();
        assert!(matches!(
            err,
            OnChainError::Validation(TradeValidationError::NotTokenizedEquity { .. })
        ));

        let err = TokenizedSymbol::<WrappedTokenizedShares>::parse("wt").unwrap_err();
        assert!(matches!(err, OnChainError::EmptySymbol(_)));
    }

    #[test]
    fn test_trade_details_rejects_unwrapped_prefix() {
        let result = TradeDetails::try_from_io("USDC", ed("100"), "tAAPL", ed("0.5"));
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));

        let result = TradeDetails::try_from_io("tAAPL", ed("0.5"), "USDC", ed("100"));
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));
    }
}
