//! Mock implementations for onchain services.

use alloy::primitives::{Address, B256, TxHash, U256};
use async_trait::async_trait;
use st0x_execution::Symbol;

use super::raindex::{Raindex, RaindexError, RaindexVaultId};

pub(crate) struct MockRaindex {
    vault_id: RaindexVaultId,
    token: Address,
    withdraw_tx: TxHash,
    deposit_tx: TxHash,
    deposit_fails: bool,
}

impl MockRaindex {
    pub(crate) fn new() -> Self {
        Self {
            vault_id: RaindexVaultId(B256::ZERO),
            token: Address::ZERO,
            withdraw_tx: TxHash::random(),
            deposit_tx: TxHash::random(),
            deposit_fails: false,
        }
    }

    pub(crate) fn failing_deposit() -> Self {
        Self {
            vault_id: VaultId(B256::ZERO),
            withdraw_tx: TxHash::random(),
            deposit_tx: TxHash::random(),
            deposit_fails: true,
        }
    }
}

#[async_trait]
impl Raindex for MockRaindex {
    async fn lookup_vault_id(&self, _token: Address) -> Result<RaindexVaultId, RaindexError> {
        Ok(self.vault_id)
    }

    async fn lookup_vault_info(
        &self,
        _symbol: &Symbol,
    ) -> Result<(Address, RaindexVaultId), RaindexError> {
        Ok((self.token, self.vault_id))
    }

    async fn deposit(
        &self,
        _token: Address,
        _vault_id: RaindexVaultId,
        _amount: U256,
        _decimals: u8,
    ) -> Result<TxHash, RaindexError> {
        if self.deposit_fails {
            return Err(RaindexError::ZeroAmount);
        }
        Ok(self.deposit_tx)
    }

    async fn withdraw(
        &self,
        _token: Address,
        _vault_id: RaindexVaultId,
        _target_amount: U256,
        _decimals: u8,
    ) -> Result<TxHash, RaindexError> {
        Ok(self.withdraw_tx)
    }
}
