//! Mock implementation of the Redeemer trait for testing.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;

use super::{RedeemError, Redeemer, RedemptionServices};
use crate::tokenization::mock::MockTokenizer;

pub(crate) struct MockRedeemer {
    vault_tx: TxHash,
    redemption_wallet: Address,
    redemption_tx: TxHash,
    fail_send: AtomicBool,
}

impl MockRedeemer {
    pub(crate) fn new() -> Self {
        Self {
            vault_tx: TxHash::random(),
            redemption_wallet: Address::random(),
            redemption_tx: TxHash::random(),
            fail_send: AtomicBool::new(false),
        }
    }

    pub(crate) fn with_redemption_tx(redemption_tx: TxHash) -> Self {
        Self {
            vault_tx: TxHash::random(),
            redemption_wallet: Address::random(),
            redemption_tx,
            fail_send: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl Redeemer for MockRedeemer {
    async fn withdraw_from_raindex(
        &self,
        _token: Address,
        _amount: U256,
    ) -> Result<TxHash, RedeemError> {
        Ok(self.vault_tx)
    }

    async fn send_for_redemption(
        &self,
        _token: Address,
        _amount: U256,
    ) -> Result<(Address, TxHash), RedeemError> {
        if self.fail_send.load(Ordering::Relaxed) {
            Err(RedeemError::VaultNotFound(Address::ZERO))
        } else {
            Ok((self.redemption_wallet, self.redemption_tx))
        }
    }
}

pub(crate) fn mock_redeemer_services() -> RedemptionServices {
    Arc::new(MockRedeemer::new())
}
