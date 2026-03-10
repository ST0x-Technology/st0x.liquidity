//! Mock implementation of the Wrapper trait for testing.

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;

use st0x_execution::Symbol;

use super::{RATIO_ONE, UnderlyingPerWrapped, Wrapper, WrapperError};

/// Mock wrapper for testing that returns predictable values.
pub(crate) struct MockWrapper {
    owner: Address,
    unwrap_tx: TxHash,
    tokenized_shares: Address,
    wrapped_token: Address,
    ratio: U256,
    wrap_fails: bool,
    unwrap_fails: bool,
    lookup_fails: bool,
}

impl MockWrapper {
    pub(crate) fn new() -> Self {
        Self {
            owner: Address::random(),
            unwrap_tx: TxHash::random(),
            tokenized_shares: Address::random(),
            wrapped_token: Address::random(),
            ratio: RATIO_ONE,
            wrap_fails: false,
            unwrap_fails: false,
            lookup_fails: false,
        }
    }

    /// Creates a mock wrapper with a custom ratio.
    pub(crate) fn with_ratio(ratio: U256) -> Self {
        Self {
            owner: Address::random(),
            unwrap_tx: TxHash::random(),
            tokenized_shares: Address::random(),
            wrapped_token: Address::random(),
            ratio,
            wrap_fails: false,
            unwrap_fails: false,
            lookup_fails: false,
        }
    }

    /// Sets the tokenized shares address returned by `lookup_underlying`.
    pub(crate) fn with_tokenized_shares(mut self, token: Address) -> Self {
        self.tokenized_shares = token;
        self
    }

    /// Sets the wrapped (derivative) token address returned by
    /// `lookup_derivative`.
    pub(crate) fn with_wrapped_token(mut self, token: Address) -> Self {
        self.wrapped_token = token;
        self
    }

    /// Creates a mock wrapper that fails on wrap operations.
    pub(crate) fn failing() -> Self {
        Self {
            owner: Address::random(),
            unwrap_tx: TxHash::random(),
            tokenized_shares: Address::random(),
            wrapped_token: Address::random(),
            ratio: RATIO_ONE,
            wrap_fails: true,
            unwrap_fails: false,
            lookup_fails: false,
        }
    }

    /// Creates a mock wrapper that fails on unwrap operations.
    pub(crate) fn failing_unwrap() -> Self {
        Self {
            owner: Address::random(),
            unwrap_tx: TxHash::random(),
            tokenized_shares: Address::random(),
            wrapped_token: Address::random(),
            ratio: RATIO_ONE,
            wrap_fails: false,
            unwrap_fails: true,
            lookup_fails: false,
        }
    }

    /// Creates a mock wrapper that fails on underlying token lookup.
    pub(crate) fn failing_lookup() -> Self {
        Self {
            owner: Address::random(),
            unwrap_tx: TxHash::random(),
            tokenized_shares: Address::random(),
            wrapped_token: Address::random(),
            ratio: RATIO_ONE,
            wrap_fails: false,
            unwrap_fails: false,
            lookup_fails: true,
        }
    }
}

#[async_trait]
impl Wrapper for MockWrapper {
    async fn get_ratio_for_symbol(
        &self,
        _symbol: &Symbol,
    ) -> Result<UnderlyingPerWrapped, WrapperError> {
        Ok(UnderlyingPerWrapped::new(self.ratio).expect("ratio is non-zero"))
    }

    fn lookup_underlying(&self, symbol: &Symbol) -> Result<Address, WrapperError> {
        if self.lookup_fails {
            return Err(WrapperError::SymbolNotConfigured(symbol.clone()));
        }
        Ok(self.tokenized_shares)
    }

    fn lookup_derivative(&self, symbol: &Symbol) -> Result<Address, WrapperError> {
        if self.lookup_fails {
            return Err(WrapperError::SymbolNotConfigured(symbol.clone()));
        }
        Ok(self.wrapped_token)
    }

    async fn to_wrapped(
        &self,
        _wrapped_token: Address,
        underlying_amount: U256,
        _receiver: Address,
    ) -> Result<(TxHash, U256), WrapperError> {
        if self.wrap_fails {
            return Err(WrapperError::MissingDepositEvent);
        }
        // 1:1 ratio for mock - wrapped amount equals underlying amount
        Ok((TxHash::random(), underlying_amount))
    }

    async fn to_underlying(
        &self,
        _wrapped_token: Address,
        wrapped_amount: U256,
        _receiver: Address,
        _owner: Address,
    ) -> Result<(TxHash, U256), WrapperError> {
        if self.unwrap_fails {
            return Err(WrapperError::MissingWithdrawEvent);
        }
        // 1:1 ratio for mock - underlying amount equals wrapped amount
        Ok((self.unwrap_tx, wrapped_amount))
    }

    fn owner(&self) -> Address {
        self.owner
    }
}
