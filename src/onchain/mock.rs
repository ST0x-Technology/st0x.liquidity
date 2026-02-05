//! Mock implementations for onchain services.

use alloy::primitives::{Address, B256, TxHash, U256};
use async_trait::async_trait;

use super::vault::{Vault, VaultError, VaultId};

pub(crate) struct MockVault {
    vault_id: VaultId,
    withdraw_tx: TxHash,
    deposit_tx: TxHash,
}

impl MockVault {
    pub(crate) fn new() -> Self {
        Self {
            vault_id: VaultId(B256::ZERO),
            withdraw_tx: TxHash::random(),
            deposit_tx: TxHash::random(),
        }
    }
}

#[async_trait]
impl Vault for MockVault {
    async fn lookup_vault_id(&self, _token: Address) -> Result<VaultId, VaultError> {
        Ok(self.vault_id)
    }

    async fn deposit(
        &self,
        _token: Address,
        _vault_id: VaultId,
        _amount: U256,
        _decimals: u8,
    ) -> Result<TxHash, VaultError> {
        Ok(self.deposit_tx)
    }

    async fn withdraw(
        &self,
        _token: Address,
        _vault_id: VaultId,
        _target_amount: U256,
        _decimals: u8,
    ) -> Result<TxHash, VaultError> {
        Ok(self.withdraw_tx)
    }
}
