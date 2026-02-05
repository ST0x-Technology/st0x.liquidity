//! Mock implementations for onchain services.

use alloy::primitives::{Address, B256, TxHash, U256};
use async_trait::async_trait;
use st0x_execution::Symbol;

use super::raindex::{Raindex, RaindexError, VaultId};

pub(crate) struct MockRaindex {
    vault_id: VaultId,
    token: Address,
    withdraw_tx: TxHash,
    deposit_tx: TxHash,
}

impl MockRaindex {
    pub(crate) fn new() -> Self {
        Self {
            vault_id: VaultId(B256::ZERO),
            token: Address::ZERO,
            withdraw_tx: TxHash::random(),
            deposit_tx: TxHash::random(),
        }
    }
}

#[async_trait]
impl Raindex for MockRaindex {
    async fn lookup_vault_id(&self, _token: Address) -> Result<VaultId, RaindexError> {
        Ok(self.vault_id)
    }

    async fn lookup_vault_info(
        &self,
        _symbol: &Symbol,
    ) -> Result<(Address, VaultId), RaindexError> {
        Ok((self.token, self.vault_id))
    }

    async fn deposit(
        &self,
        _token: Address,
        _vault_id: VaultId,
        _amount: U256,
        _decimals: u8,
    ) -> Result<TxHash, RaindexError> {
        Ok(self.deposit_tx)
    }

    async fn withdraw(
        &self,
        _token: Address,
        _vault_id: VaultId,
        _target_amount: U256,
        _decimals: u8,
    ) -> Result<TxHash, RaindexError> {
        Ok(self.withdraw_tx)
    }
}
