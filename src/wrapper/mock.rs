//! Mock implementation of the Wrapper trait for testing.

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use st0x_execution::Symbol;

use super::Wrapper;
use crate::vault::{UnderlyingPerWrapped, WrapperError};

/// Mock wrapper for testing that returns predictable values.
pub(crate) struct MockWrapper {
    owner: Address,
    unwrap_tx: TxHash,
}

impl MockWrapper {
    pub(crate) fn new() -> Self {
        Self {
            owner: Address::random(),
            unwrap_tx: TxHash::random(),
        }
    }

    pub(crate) fn with_owner(owner: Address) -> Self {
        Self {
            owner,
            unwrap_tx: TxHash::random(),
        }
    }
}

#[async_trait]
impl Wrapper for MockWrapper {
    async fn get_ratio_for_symbol(&self, _symbol: &Symbol) -> Result<UnderlyingPerWrapped, WrapperError> {
        Ok(UnderlyingPerWrapped::one_to_one())
    }

    async fn to_wrapped(
        &self,
        _wrapped_token: Address,
        underlying_amount: U256,
        _receiver: Address,
    ) -> Result<(TxHash, U256), WrapperError> {
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
        // 1:1 ratio for mock - underlying amount equals wrapped amount
        Ok((self.unwrap_tx, wrapped_amount))
    }

    fn owner(&self) -> Address {
        self.owner
    }
}
