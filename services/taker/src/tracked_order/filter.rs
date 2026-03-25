//! Order filtering logic for the order collector.
//!
//! Determines whether an `AddOrderV3` event represents an order the
//! taker bot should track, based on:
//! - Excluded owner (orders from the liquidity bot are ignored)
//! - Supported token pairs (USDC + one configured wtToken)

use alloy::primitives::Address;
use std::collections::HashSet;

use st0x_execution::Symbol;

use super::Scenario;

/// Result of matching an order's IO tokens against configured addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SupportedDirection {
    pub(crate) symbol: Symbol,
    pub(crate) scenario: Scenario,
    pub(crate) output_token: Address,
    pub(crate) input_token: Address,
}

/// Stateless filter that checks whether an order should be tracked.
pub(crate) struct OrderFilter {
    excluded_owner: Address,
    usdc_address: Address,
    /// Set of configured wtToken addresses, keyed for fast lookup.
    known_wrapped_tokens: HashSet<Address>,
    /// Maps wrapped token address -> base equity symbol.
    wrapped_to_symbol: Vec<(Address, Symbol)>,
}

impl OrderFilter {
    pub(crate) fn new(
        excluded_owner: Address,
        usdc_address: Address,
        equity_tokens: impl IntoIterator<Item = (Symbol, Address)>,
    ) -> Self {
        let mut known_wrapped_tokens = HashSet::new();
        let mut wrapped_to_symbol = Vec::new();

        for (symbol, wrapped_address) in equity_tokens {
            known_wrapped_tokens.insert(wrapped_address);
            wrapped_to_symbol.push((wrapped_address, symbol));
        }

        Self {
            excluded_owner,
            usdc_address,
            known_wrapped_tokens,
            wrapped_to_symbol,
        }
    }

    /// Returns `true` if the order owner is the excluded address.
    pub(crate) fn is_excluded_owner(&self, owner: Address) -> bool {
        owner == self.excluded_owner
    }

    /// Attempts to match an order's valid IO tokens against
    /// supported pairs (USDC + configured wtToken).
    ///
    /// An order is supported if exactly one IO token is USDC
    /// and the other is a configured wtToken. Returns the
    /// matched direction (Scenario A or B) and the equity symbol.
    ///
    /// `valid_inputs` and `valid_outputs` come from the OrderV4
    /// struct's `validInputs` and `validOutputs` arrays. We check
    /// whether any (input, output) combination matches a supported
    /// pair.
    pub(crate) fn match_supported_pair(
        &self,
        valid_input_tokens: &[Address],
        valid_output_tokens: &[Address],
    ) -> Option<SupportedDirection> {
        // Check every (input_token, output_token) combination.
        // In practice, orders have 1-2 valid inputs/outputs.
        for &input_token in valid_input_tokens {
            for &output_token in valid_output_tokens {
                if let Some(direction) = self.classify_pair(input_token, output_token) {
                    return Some(direction);
                }
            }
        }

        None
    }

    /// Classifies a single (input_token, output_token) pair.
    ///
    /// From the order *owner's* perspective:
    /// - input_token = what the owner wants to receive
    /// - output_token = what the owner provides
    ///
    /// Scenario A: owner provides wtToken (output), wants USDC (input)
    ///   -> Bot sends USDC, receives wtToken
    /// Scenario B: owner provides USDC (output), wants wtToken (input)
    ///   -> Bot sends wtToken, receives USDC
    fn classify_pair(
        &self,
        input_token: Address,
        output_token: Address,
    ) -> Option<SupportedDirection> {
        let is_usdc = |addr: Address| addr == self.usdc_address;
        let lookup_symbol = |addr: Address| -> Option<Symbol> {
            if self.known_wrapped_tokens.contains(&addr) {
                self.wrapped_to_symbol
                    .iter()
                    .find(|(wrapped, _)| *wrapped == addr)
                    .map(|(_, symbol)| symbol.clone())
            } else {
                None
            }
        };

        // Scenario A: owner outputs wtToken, inputs USDC
        if is_usdc(input_token)
            && let Some(symbol) = lookup_symbol(output_token)
        {
            return Some(SupportedDirection {
                symbol,
                scenario: Scenario::A,
                output_token,
                input_token,
            });
        }

        // Scenario B: owner outputs USDC, inputs wtToken
        if is_usdc(output_token)
            && let Some(symbol) = lookup_symbol(input_token)
        {
            return Some(SupportedDirection {
                symbol,
                scenario: Scenario::B,
                output_token,
                input_token,
            });
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const USDC: Address = Address::repeat_byte(0x01);
    const WT_AAPL: Address = Address::repeat_byte(0xAA);
    const WT_TSLA: Address = Address::repeat_byte(0xBB);
    const EXCLUDED: Address = Address::repeat_byte(0xFF);
    const UNKNOWN_TOKEN: Address = Address::repeat_byte(0x99);

    fn symbol(ticker: &str) -> Symbol {
        Symbol::new(ticker).unwrap()
    }

    fn filter() -> OrderFilter {
        OrderFilter::new(
            EXCLUDED,
            USDC,
            vec![(symbol("AAPL"), WT_AAPL), (symbol("TSLA"), WT_TSLA)],
        )
    }

    // ── Excluded owner ─────────────────────────────────────────

    #[test]
    fn excluded_owner_is_rejected() {
        let filter = filter();
        assert!(filter.is_excluded_owner(EXCLUDED));
    }

    #[test]
    fn non_excluded_owner_is_accepted() {
        let filter = filter();
        assert!(!filter.is_excluded_owner(Address::repeat_byte(0x02)));
    }

    // ── Scenario A: owner sells wtToken for USDC ───────────────

    #[test]
    fn scenario_a_owner_outputs_wt_aapl_inputs_usdc() {
        let filter = filter();

        let result = filter.match_supported_pair(
            &[USDC],    // owner wants USDC (input)
            &[WT_AAPL], // owner provides wtAAPL (output)
        );

        let direction = result.expect("Should match Scenario A");
        assert_eq!(direction.symbol.to_string(), "AAPL");
        assert_eq!(direction.scenario, Scenario::A);
        assert_eq!(direction.output_token, WT_AAPL);
        assert_eq!(direction.input_token, USDC);
    }

    #[test]
    fn scenario_a_with_tsla() {
        let filter = filter();

        let result = filter.match_supported_pair(&[USDC], &[WT_TSLA]);

        let direction = result.expect("Should match Scenario A for TSLA");
        assert_eq!(direction.symbol.to_string(), "TSLA");
        assert_eq!(direction.scenario, Scenario::A);
    }

    // ── Scenario B: owner buys wtToken with USDC ───────────────

    #[test]
    fn scenario_b_owner_outputs_usdc_inputs_wt_aapl() {
        let filter = filter();

        let result = filter.match_supported_pair(
            &[WT_AAPL], // owner wants wtAAPL (input)
            &[USDC],    // owner provides USDC (output)
        );

        let direction = result.expect("Should match Scenario B");
        assert_eq!(direction.symbol.to_string(), "AAPL");
        assert_eq!(direction.scenario, Scenario::B);
        assert_eq!(direction.output_token, USDC);
        assert_eq!(direction.input_token, WT_AAPL);
    }

    // ── Unsupported pairs ──────────────────────────────────────

    #[test]
    fn unknown_token_pair_is_rejected() {
        let filter = filter();

        let result = filter.match_supported_pair(&[UNKNOWN_TOKEN], &[USDC]);

        assert!(result.is_none(), "Unknown token should not match");
    }

    #[test]
    fn two_unknown_tokens_rejected() {
        let filter = filter();

        let result = filter.match_supported_pair(&[UNKNOWN_TOKEN], &[Address::repeat_byte(0x88)]);

        assert!(result.is_none());
    }

    #[test]
    fn usdc_to_usdc_rejected() {
        let filter = filter();

        let result = filter.match_supported_pair(&[USDC], &[USDC]);

        assert!(result.is_none(), "USDC<->USDC should not match");
    }

    #[test]
    fn wt_token_to_wt_token_rejected() {
        let filter = filter();

        let result = filter.match_supported_pair(&[WT_AAPL], &[WT_TSLA]);

        assert!(
            result.is_none(),
            "wtToken<->wtToken without USDC should not match"
        );
    }

    #[test]
    fn usdc_with_unknown_non_wt_token_rejected() {
        let filter = filter();

        let result = filter.match_supported_pair(&[USDC], &[UNKNOWN_TOKEN]);

        assert!(
            result.is_none(),
            "USDC paired with unknown token should not match"
        );
    }

    // ── Multiple valid inputs/outputs ──────────────────────────

    #[test]
    fn matches_first_supported_pair_from_multiple_ios() {
        let filter = filter();

        // Order with multiple valid inputs and outputs — one pair matches
        let result = filter.match_supported_pair(&[UNKNOWN_TOKEN, USDC], &[UNKNOWN_TOKEN, WT_AAPL]);

        let direction = result.expect("Should find the USDC+wtAAPL pair");
        assert_eq!(direction.symbol.to_string(), "AAPL");
        assert_eq!(direction.scenario, Scenario::A);
    }

    #[test]
    fn no_match_when_all_ios_unsupported() {
        let filter = filter();

        let result = filter.match_supported_pair(
            &[UNKNOWN_TOKEN, Address::repeat_byte(0x77)],
            &[Address::repeat_byte(0x88), Address::repeat_byte(0x66)],
        );

        assert!(result.is_none());
    }

    // ── Empty IO arrays ────────────────────────────────────────

    #[test]
    fn empty_inputs_returns_none() {
        let filter = filter();
        let result = filter.match_supported_pair(&[], &[WT_AAPL]);
        assert!(result.is_none());
    }

    #[test]
    fn empty_outputs_returns_none() {
        let filter = filter();
        let result = filter.match_supported_pair(&[USDC], &[]);
        assert!(result.is_none());
    }

    #[test]
    fn empty_both_returns_none() {
        let filter = filter();
        let result = filter.match_supported_pair(&[], &[]);
        assert!(result.is_none());
    }
}
