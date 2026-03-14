//! Mock implementation of the Wrapper trait for testing.

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;

use st0x_execution::Symbol;

use super::{RATIO_ONE, UnderlyingPerWrapped, Wrapper, WrapperError};

/// Which operation the mock should simulate failing.
#[derive(Default, PartialEq, Eq)]
enum MockFailure {
    #[default]
    None,
    Wrap,
    Unwrap,
    Lookup,
    DerivativeLookup,
}

/// Mock wrapper for testing that returns predictable values.
pub(crate) struct MockWrapper {
    owner: Address,
    unwrap_tx: TxHash,
    tokenized_shares: Address,
    wrapped_token: Address,
    ratio: U256,
    failure: MockFailure,
}

impl MockWrapper {
    pub(crate) fn new() -> Self {
        Self {
            owner: Address::random(),
            unwrap_tx: TxHash::random(),
            tokenized_shares: Address::random(),
            wrapped_token: Address::random(),
            ratio: RATIO_ONE,
            failure: MockFailure::None,
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
            failure: MockFailure::None,
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
            failure: MockFailure::Wrap,
            ..Self::new()
        }
    }

    /// Creates a mock wrapper that fails on unwrap operations.
    pub(crate) fn failing_unwrap() -> Self {
        Self {
            failure: MockFailure::Unwrap,
            ..Self::new()
        }
    }

    /// Creates a mock wrapper that fails on underlying token lookup.
    pub(crate) fn failing_lookup() -> Self {
        Self {
            failure: MockFailure::Lookup,
            ..Self::new()
        }
    }

    /// Creates a mock wrapper that succeeds on underlying token lookup but
    /// fails on derivative token lookup.
    pub(crate) fn failing_derivative_lookup() -> Self {
        Self {
            failure: MockFailure::DerivativeLookup,
            ..Self::new()
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
        if self.failure == MockFailure::Lookup {
            return Err(WrapperError::SymbolNotConfigured(symbol.clone()));
        }
        Ok(self.tokenized_shares)
    }

    fn lookup_derivative(&self, symbol: &Symbol) -> Result<Address, WrapperError> {
        if self.failure == MockFailure::DerivativeLookup {
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
        if self.failure == MockFailure::Wrap {
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
        if self.failure == MockFailure::Unwrap {
            return Err(WrapperError::MissingWithdrawEvent);
        }
        // 1:1 ratio for mock - underlying amount equals wrapped amount
        Ok((self.unwrap_tx, wrapped_amount))
    }

    fn owner(&self) -> Address {
        self.owner
    }
}
