use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::Provider;
use alloy::signers::Signer;

use crate::bindings::IOrderBookV4;
use crate::cctp::EvmAccount;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VaultId(pub(crate) U256);

#[derive(Debug, thiserror::Error)]
pub(crate) enum VaultError {
    #[error("Transaction error: {0}")]
    Transaction(#[from] alloy::providers::PendingTransactionError),
    #[error("Contract error: {0}")]
    Contract(#[from] alloy::contract::Error),
    #[error("Insufficient balance: requested {requested}, available {available}")]
    InsufficientBalance { requested: U256, available: U256 },
    #[error("Amount cannot be zero")]
    ZeroAmount,
}

pub(crate) struct VaultService<P, S>
where
    P: Provider + Clone,
    S: Signer + Clone + Sync,
{
    account: EvmAccount<P, S>,
    orderbook: Address,
}

impl<P, S> VaultService<P, S>
where
    P: Provider + Clone,
    S: Signer + Clone + Sync,
{
    pub(crate) fn new(account: EvmAccount<P, S>, orderbook: Address) -> Self {
        Self { account, orderbook }
    }

    pub(crate) async fn deposit(
        &self,
        token: Address,
        vault_id: VaultId,
        amount: U256,
    ) -> Result<TxHash, VaultError> {
        if amount.is_zero() {
            return Err(VaultError::ZeroAmount);
        }

        let contract = IOrderBookV4::new(self.orderbook, &self.account.provider);

        let tasks = Vec::new();

        let receipt = contract
            .deposit2(token, vault_id.0, amount, tasks)
            .send()
            .await?
            .get_receipt()
            .await?;

        Ok(receipt.transaction_hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::signers::local::PrivateKeySigner;

    const TEST_ORDERBOOK: Address = address!("1234567890123456789012345678901234567890");
    const TEST_TOKEN: Address = address!("0000000000000000000000000000000000000001");
    const TEST_VAULT_ID: u64 = 1;

    #[tokio::test]
    async fn deposit_rejects_zero_amount() {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let signer = PrivateKeySigner::random();

        let account = EvmAccount::new(provider, signer);
        let service = VaultService::new(account, TEST_ORDERBOOK);

        let result = service
            .deposit(TEST_TOKEN, VaultId(U256::from(TEST_VAULT_ID)), U256::ZERO)
            .await;

        assert!(matches!(result.unwrap_err(), VaultError::ZeroAmount));
    }
}
