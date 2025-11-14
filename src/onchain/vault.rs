use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::signers::Signer;

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
}
