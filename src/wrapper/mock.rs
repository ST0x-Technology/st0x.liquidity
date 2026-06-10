//! Mock implementation of the Wrapper trait for testing.

use alloy::primitives::{Address, TxHash, U256};
use alloy::transports::RpcError;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex;

use st0x_evm::EvmError;
use st0x_execution::Symbol;

use super::{RATIO_ONE, UnderlyingPerWrapped, Wrapper, WrapperError};

/// Which operation the mock should simulate failing.
#[derive(Default, PartialEq, Eq)]
enum MockFailure {
    #[default]
    None,
    Wrap,
    ConfirmWrap,
    RetryableConfirmWrap,
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
    /// Maps tx hashes to their submitted amounts for confirm methods.
    submitted_amounts: Mutex<HashMap<TxHash, U256>>,
}

impl MockWrapper {
    pub(crate) fn new() -> Self {
        Self {
            owner: Address::ZERO,
            unwrap_tx: TxHash::random(),
            tokenized_shares: Address::random(),
            wrapped_token: Address::ZERO,
            ratio: RATIO_ONE,
            failure: MockFailure::None,
            submitted_amounts: Mutex::new(HashMap::new()),
        }
    }

    /// Creates a mock wrapper with a custom ratio.
    pub(crate) fn with_ratio(ratio: U256) -> Self {
        Self {
            owner: Address::ZERO,
            unwrap_tx: TxHash::random(),
            tokenized_shares: Address::random(),
            wrapped_token: Address::ZERO,
            ratio,
            failure: MockFailure::None,
            submitted_amounts: Mutex::new(HashMap::new()),
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
        let mut mock = Self::new();
        mock.failure = MockFailure::Wrap;
        mock
    }

    /// Creates a mock wrapper that fails on unwrap operations.
    pub(crate) fn failing_unwrap() -> Self {
        let mut mock = Self::new();
        mock.failure = MockFailure::Unwrap;
        mock
    }

    /// Creates a mock wrapper that fails on underlying token lookup.
    pub(crate) fn failing_lookup() -> Self {
        let mut mock = Self::new();
        mock.failure = MockFailure::Lookup;
        mock
    }

    /// Creates a mock wrapper that submits wraps successfully but fails on
    /// `confirm_wrap`, simulating a receipt that never surfaces the deposit
    /// event.
    pub(crate) fn failing_confirm_wrap() -> Self {
        let mut mock = Self::new();
        mock.failure = MockFailure::ConfirmWrap;
        mock
    }

    /// Creates a mock wrapper that fails `confirm_wrap` with a retryable RPC
    /// error rather than a confirmed missing-event receipt.
    pub(crate) fn retryable_confirm_wrap() -> Self {
        let mut mock = Self::new();
        mock.failure = MockFailure::RetryableConfirmWrap;
        mock
    }

    /// Creates a mock wrapper that succeeds on underlying token lookup but
    /// fails on derivative token lookup.
    pub(crate) fn failing_derivative_lookup() -> Self {
        let mut mock = Self::new();
        mock.failure = MockFailure::DerivativeLookup;
        mock
    }

    /// Pre-seeds a tx hash into `submitted_amounts` so that `confirm_wrap`
    /// recognises it. Used by tests that manually advance the aggregate to
    /// `WrapSubmitted` without going through `submit_wrap`.
    pub(crate) fn seed_submitted_amount(&self, tx_hash: TxHash, amount: U256) {
        self.submitted_amounts
            .lock()
            .unwrap()
            .insert(tx_hash, amount);
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

    async fn submit_wrap(
        &self,
        _wrapped_token: Address,
        underlying_amount: U256,
        _receiver: Address,
    ) -> Result<TxHash, WrapperError> {
        if self.failure == MockFailure::Wrap {
            return Err(WrapperError::MissingDepositEvent);
        }
        let tx_hash = TxHash::random();
        self.submitted_amounts
            .lock()
            .unwrap()
            .insert(tx_hash, underlying_amount);
        Ok(tx_hash)
    }

    async fn confirm_wrap(
        &self,
        _wrapped_token: Address,
        tx_hash: TxHash,
    ) -> Result<U256, WrapperError> {
        if self.failure == MockFailure::ConfirmWrap {
            return Err(WrapperError::MissingDepositEvent);
        }
        if self.failure == MockFailure::RetryableConfirmWrap {
            return Err(WrapperError::Evm(EvmError::Transport(RpcError::ErrorResp(
                alloy::rpc::json_rpc::ErrorPayload {
                    code: -32000,
                    message: "temporary RPC failure".into(),
                    data: None,
                },
            ))));
        }

        // 1:1 ratio — return the amount that was submitted for this tx
        let amount = self
            .submitted_amounts
            .lock()
            .unwrap()
            .remove(&tx_hash)
            .expect("confirm called with tx hash that was never submitted");
        Ok(amount)
    }

    async fn submit_unwrap(
        &self,
        _wrapped_token: Address,
        wrapped_amount: U256,
        _receiver: Address,
        _owner: Address,
    ) -> Result<TxHash, WrapperError> {
        if self.failure == MockFailure::Unwrap {
            return Err(WrapperError::MissingWithdrawEvent);
        }
        self.submitted_amounts
            .lock()
            .unwrap()
            .insert(self.unwrap_tx, wrapped_amount);
        Ok(self.unwrap_tx)
    }

    async fn confirm_unwrap(
        &self,
        _wrapped_token: Address,
        tx_hash: TxHash,
    ) -> Result<U256, WrapperError> {
        // 1:1 ratio — return the amount that was submitted for this tx
        let amount = self
            .submitted_amounts
            .lock()
            .unwrap()
            .remove(&tx_hash)
            .expect("confirm called with tx hash that was never submitted");
        Ok(amount)
    }

    fn owner(&self) -> Address {
        self.owner
    }
}
