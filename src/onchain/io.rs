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

use rain_math_float::{Float, FloatError};
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
#[derive(Clone, Copy)]
pub(crate) struct Usdc(Float);

impl fmt::Debug for Usdc {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Usdc({})",
            st0x_float_serde::format_float_with_fallback(&self.0)
        )
    }
}

impl fmt::Display for Usdc {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}",
            st0x_float_serde::format_float_with_fallback(&self.0)
        )
    }
}

impl PartialEq for Usdc {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(other.0).unwrap_or(false)
    }
}

impl Usdc {
    pub(crate) fn new(value: Float) -> Result<Self, TradeValidationError> {
        if value
            .lt(Float::zero()?)
            .map_err(TradeValidationError::Float)?
        {
            return Err(TradeValidationError::NegativeUsdc(value));
        }
        Ok(Self(value))
    }

    pub(crate) fn value(self) -> Float {
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
        input_amount: Float,
        output_symbol: &str,
        output_amount: Float,
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
            .lt(Float::zero()?)
            .map_err(TradeValidationError::Float)?
        {
            return Err(TradeValidationError::NegativeShares(equity_amount_raw).into());
        }

        if usdc_amount_raw
            .lt(Float::zero()?)
            .map_err(TradeValidationError::Float)?
        {
            return Err(TradeValidationError::NegativeUsdc(usdc_amount_raw).into());
        }

        // Truncate precision dust beyond token decimal scales.
        // Rain's orderbook emits raw Float values in events, which can
        // carry more precision than the token's fixed-point representation.
        // The actual ERC-20 transfer truncates to the token's decimals
        // (6 for USDC, 18 for ERC-20 shares), so we align with that.
        let equity_amount = FractionalShares::new(
            truncate_to_dp(equity_amount_raw, 18).map_err(TradeValidationError::Float)?,
        );
        let usdc_amount =
            Usdc::new(truncate_to_dp(usdc_amount_raw, 6).map_err(TradeValidationError::Float)?)?;

        Ok(Self {
            ticker,
            equity_amount,
            usdc_amount,
            direction,
        })
    }
}

/// Truncates a Float value to the given number of decimal places.
///
/// Converts to fixed-point with the target scale, then back,
/// dropping any digits beyond the scale limit.
fn truncate_to_dp(value: Float, decimal_places: u8) -> Result<Float, FloatError> {
    let (fixed, _lossless) = value.to_fixed_decimal_lossy(decimal_places)?;
    Float::from_fixed_decimal(fixed, decimal_places)
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
    use st0x_float_macro::float;

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
        let usdc = Usdc::new(float!(1000.50)).unwrap();
        assert!(usdc.value().eq(float!(1000.50)).unwrap());

        let usdc = Usdc::new(Float::zero().unwrap()).unwrap();
        assert!(usdc.value().eq(Float::zero().unwrap()).unwrap());

        let result = Usdc::new(float!(-100));
        assert!(matches!(
            result.unwrap_err(),
            TradeValidationError::NegativeUsdc(_)
        ));
    }

    #[test]
    fn test_usdc_equality() {
        let usdc1 = Usdc::new(float!(1000)).unwrap();
        let usdc2 = Usdc::new(float!(1000)).unwrap();
        let usdc3 = Usdc::new(float!(2000)).unwrap();

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
        let details =
            TradeDetails::try_from_io("USDC", float!(100), "wtAAPL", float!(0.5)).unwrap();

        assert_eq!(details.ticker(), &symbol!("AAPL"));
        assert!(details.equity_amount().inner().eq(float!(0.5)).unwrap());
        assert!(details.usdc_amount().value().eq(float!(100)).unwrap());
        assert_eq!(details.direction(), Direction::Sell);
    }

    #[test]
    fn test_trade_details_try_from_io_wrapped_to_usdc() {
        let details =
            TradeDetails::try_from_io("wtAAPL", float!(0.5), "USDC", float!(100)).unwrap();

        assert_eq!(details.ticker(), &symbol!("AAPL"));
        assert!(details.equity_amount().inner().eq(float!(0.5)).unwrap());
        assert!(details.usdc_amount().value().eq(float!(100)).unwrap());
        assert_eq!(details.direction(), Direction::Buy);
    }

    #[test]
    fn test_trade_details_try_from_io_nvda() {
        let details =
            TradeDetails::try_from_io("USDC", float!(64.17), "wtNVDA", float!(0.374)).unwrap();

        assert_eq!(details.ticker(), &symbol!("NVDA"));
        assert!(details.equity_amount().inner().eq(float!(0.374)).unwrap());
        assert!(details.usdc_amount().value().eq(float!(64.17)).unwrap());
        assert_eq!(details.direction(), Direction::Sell);

        let details =
            TradeDetails::try_from_io("wtNVDA", float!(0.374), "USDC", float!(64.17)).unwrap();

        assert_eq!(details.ticker(), &symbol!("NVDA"));
        assert!(details.equity_amount().inner().eq(float!(0.374)).unwrap());
        assert!(details.usdc_amount().value().eq(float!(64.17)).unwrap());
        assert_eq!(details.direction(), Direction::Buy);
    }

    #[test]
    fn test_trade_details_try_from_io_invalid_configurations() {
        let result = TradeDetails::try_from_io("USDC", float!(100), "USDC", float!(100));
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));

        let result = TradeDetails::try_from_io("BTC", float!(1), "ETH", float!(3000));
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));
    }

    #[test]
    fn test_trade_details_negative_amount_validation() {
        let result = TradeDetails::try_from_io("USDC", float!(100), "wtAAPL", float!(-0.5));
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::NegativeShares(_))
        ));

        let result = TradeDetails::try_from_io("USDC", float!(-100), "wtAAPL", float!(0.5));
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
            TradeDetails::try_from_io("USDC", float!(64.169234), "wtNVDA", float!(0.374)).unwrap();

        assert_eq!(details.ticker(), &symbol!("NVDA"));
        assert!(details.equity_amount().inner().eq(float!(0.374)).unwrap());
        assert!(details.usdc_amount().value().eq(float!(64.169234)).unwrap());
        assert_eq!(details.direction(), Direction::Sell);

        let price_per_share = (float!(64.169234) / float!(0.374)).unwrap();
        let diff = (price_per_share - float!(171.58)).unwrap().abs().unwrap();
        assert!(diff.lt(float!(0.01)).unwrap());
    }

    #[test]
    fn test_trade_details_normalizes_spurious_precision_beyond_onchain_scales() {
        let usdc_with_dust = float!(&"64.169234000001".to_string());
        let shares_with_dust = float!(&"0.374000000000000000001".to_string());

        let details =
            TradeDetails::try_from_io("USDC", usdc_with_dust, "wtNVDA", shares_with_dust).unwrap();

        assert!(details.usdc_amount().value().eq(float!(64.169234)).unwrap());
        assert!(details.equity_amount().inner().eq(float!(0.374)).unwrap());
    }

    #[test]
    fn test_trade_details_regression_precision_dust_breaks_exact_equality_without_normalization() {
        let shares_with_dust = float!(&"0.200000000000000000001".to_string());
        let pre_fix_shares = FractionalShares::new(shares_with_dust);

        assert_ne!(
            pre_fix_shares,
            FractionalShares::new(float!(0.2)),
            "Pre-fix behavior preserved dust and broke exact equality checks",
        );

        let details = TradeDetails::try_from_io(
            "USDC",
            float!(&"34.645024000001".to_string()),
            "wtNVDA",
            shares_with_dust,
        )
        .unwrap();

        assert!(details.equity_amount().inner().eq(float!(0.2)).unwrap());
        assert!(details.usdc_amount().value().eq(float!(34.645024)).unwrap());
    }

    #[test]
    fn test_edge_case_validation_very_small_amounts() {
        let details =
            TradeDetails::try_from_io("USDC", float!(0.01), "wtAAPL", float!(0.0001)).unwrap();
        assert_eq!(details.ticker(), &symbol!("AAPL"));
        assert!(details.equity_amount().inner().eq(float!(0.0001)).unwrap());
        assert!(details.usdc_amount().value().eq(float!(0.01)).unwrap());
    }

    #[test]
    fn test_edge_case_validation_very_large_amounts() {
        let details =
            TradeDetails::try_from_io("USDC", float!(1000000), "wtBRK", float!(100)).unwrap();
        assert_eq!(details.ticker(), &symbol!("BRK"));
        assert!(details.equity_amount().inner().eq(float!(100)).unwrap());
        assert!(details.usdc_amount().value().eq(float!(1000000)).unwrap());
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
        let result = TradeDetails::try_from_io("USDC", float!(100), "tAAPL", float!(0.5));
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));

        let result = TradeDetails::try_from_io("tAAPL", float!(0.5), "USDC", float!(100));
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::InvalidSymbolConfiguration(_, _))
        ));
    }
}
