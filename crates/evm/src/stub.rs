//! Panicking wallet stub for tests that need a `Wallet` impl without
//! real chain connectivity.

use std::sync::Arc;

use alloy::primitives::{Address, Bytes, TxHash};
use alloy::providers::RootProvider;
use alloy::rpc::client::RpcClient;
use alloy::rpc::types::TransactionReceipt;
use async_trait::async_trait;

use crate::{Evm, EvmError, Wallet};

/// Panicking wallet stub for tests.
///
/// Constructs contexts requiring a `Wallet` without needing real chain
/// connectivity. Provider type matches production (`RootProvider`) so it
/// fits `Arc<dyn Wallet<Provider = RootProvider>>`.
pub struct StubWallet {
    address: Address,
    provider: RootProvider,
}

impl StubWallet {
    pub fn stub(address: Address) -> Arc<dyn Wallet<Provider = RootProvider>> {
        let url = "http://stub.invalid"
            .parse()
            .unwrap_or_else(|_| unreachable!("hardcoded URL must parse"));
        Arc::new(Self {
            address,
            provider: RootProvider::new(RpcClient::builder().http(url)),
        })
    }
}

#[async_trait]
impl Evm for StubWallet {
    type Provider = RootProvider;

    fn provider(&self) -> &RootProvider {
        &self.provider
    }
}

#[async_trait]
impl Wallet for StubWallet {
    fn address(&self) -> Address {
        self.address
    }

    async fn send_pending(
        &self,
        _contract: Address,
        _calldata: Bytes,
        _note: &str,
    ) -> Result<TxHash, EvmError> {
        panic!(
            "StubWallet::send_pending called - use a real wallet in tests that need transactions"
        )
    }

    async fn await_receipt(&self, _tx_hash: TxHash) -> Result<TransactionReceipt, EvmError> {
        panic!(
            "StubWallet::await_receipt called - use a real wallet in tests that need transactions"
        )
    }

    async fn send(
        &self,
        _contract: Address,
        _calldata: Bytes,
        _note: &str,
    ) -> Result<TransactionReceipt, EvmError> {
        panic!("StubWallet::send called - use a real wallet in tests that need transactions")
    }
}
