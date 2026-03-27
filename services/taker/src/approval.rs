//! ERC20 approval management for the orderbook contract.
//!
//! On startup, the bot sets max approvals for the orderbook contract
//! to spend USDC and each configured wtToken from its wallet. This is
//! a one-time operation per token -- the bot checks current allowance
//! and only submits an `approve()` transaction if below threshold.

use alloy::primitives::{Address, U256};

/// Threshold below which a new approval is submitted.
/// Set to `type(uint256).max / 2` to avoid approving on every restart
/// while still refreshing before the allowance runs out.
///
/// Computed as `U256::MAX >> 1` (equivalent to dividing by 2).
pub(crate) const APPROVAL_THRESHOLD: U256 = U256::MAX.wrapping_shr(1);

/// Determines whether a new approval transaction is needed based on
/// the current allowance.
pub(crate) fn needs_approval(current_allowance: U256) -> bool {
    current_allowance < APPROVAL_THRESHOLD
}

/// Collects the set of token addresses that need orderbook approval.
///
/// Returns USDC address plus all configured wtToken (wrapped) addresses.
pub(crate) fn tokens_requiring_approval(
    usdc_address: Address,
    wrapped_addresses: impl Iterator<Item = Address>,
) -> Vec<Address> {
    let mut tokens = vec![usdc_address];
    tokens.extend(wrapped_addresses);
    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_allowance_does_not_need_approval() {
        assert!(!needs_approval(U256::MAX));
    }

    #[test]
    fn zero_allowance_needs_approval() {
        assert!(needs_approval(U256::ZERO));
    }

    #[test]
    fn below_threshold_needs_approval() {
        assert!(needs_approval(APPROVAL_THRESHOLD - U256::from(1)));
    }

    #[test]
    fn at_threshold_does_not_need_approval() {
        assert!(!needs_approval(APPROVAL_THRESHOLD));
    }

    #[test]
    fn above_threshold_does_not_need_approval() {
        assert!(!needs_approval(APPROVAL_THRESHOLD + U256::from(1)));
    }

    #[test]
    fn tokens_requiring_approval_includes_usdc_and_wrapped() {
        let usdc = Address::repeat_byte(0x01);
        let wrapped_a = Address::repeat_byte(0x02);
        let wrapped_b = Address::repeat_byte(0x03);

        let tokens = tokens_requiring_approval(usdc, [wrapped_a, wrapped_b].into_iter());

        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0], usdc);
        assert_eq!(tokens[1], wrapped_a);
        assert_eq!(tokens[2], wrapped_b);
    }

    #[test]
    fn tokens_requiring_approval_with_no_wrapped() {
        let usdc = Address::repeat_byte(0x01);

        let tokens = tokens_requiring_approval(usdc, std::iter::empty());

        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0], usdc);
    }
}
