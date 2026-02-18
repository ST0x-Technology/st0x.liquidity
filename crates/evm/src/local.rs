//! Local signer implementation for test environments (anvil).
//!
//! `RawPrivateKeyWallet` wraps an alloy provider with an embedded `EthereumWallet`
//! and submits transactions directly. This is only compiled when the
//! `local-signer` feature is enabled and should only be used in tests.

use alloy::primitives::{Address, Bytes};
use alloy::providers::{Provider, WalletProvider};
use alloy::rpc::types::TransactionReceipt;
use async_trait::async_trait;
use tracing::info;

use crate::{Evm, EvmError, Wallet};

/// Local wallet that signs and submits transactions directly.
///
/// Wraps a provider that includes a wallet filler (e.g., built with
/// `ProviderBuilder::new().wallet(wallet).connect_http(...)`).
///
/// The wallet address is derived from the provider's default signer —
/// no separate address parameter is needed.
pub struct RawPrivateKeyWallet<P> {
    provider: P,
    required_confirmations: u64,
}

impl<P> RawPrivateKeyWallet<P> {
    /// Creates a new `RawPrivateKeyWallet` with the given provider and confirmation count.
    pub fn new(provider: P, required_confirmations: u64) -> Self {
        Self {
            provider,
            required_confirmations,
        }
    }
}

#[async_trait]
impl<P> Evm for RawPrivateKeyWallet<P>
where
    P: Provider + WalletProvider + Clone + Send + Sync,
{
    type Provider = P;

    fn provider(&self) -> &P {
        &self.provider
    }
}

#[async_trait]
impl<P> Wallet for RawPrivateKeyWallet<P>
where
    P: Provider + WalletProvider + Clone + Send + Sync,
{
    fn address(&self) -> Address {
        self.provider.default_signer_address()
    }

    async fn send(
        &self,
        contract: Address,
        calldata: Bytes,
        note: &str,
    ) -> Result<TransactionReceipt, EvmError> {
        info!(%contract, note, "Submitting local contract call");

        let tx = alloy::rpc::types::TransactionRequest::default()
            .to(contract)
            .input(calldata.into());

        let pending = self.provider.send_transaction(tx).await?;

        info!(tx_hash = %pending.tx_hash(), note, "Transaction submitted");

        let receipt = pending
            .with_required_confirmations(self.required_confirmations)
            .get_receipt()
            .await?;

        if !receipt.status() {
            return Err(EvmError::Reverted {
                tx_hash: receipt.transaction_hash,
            });
        }

        info!(tx_hash = %receipt.transaction_hash, note, "Transaction confirmed");

        Ok(receipt)
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::node_bindings::Anvil;
    use alloy::primitives::{B256, U256};
    use alloy::providers::ProviderBuilder;
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol;
    use alloy::sol_types::SolCall;

    use super::*;

    sol!(
        #![sol(all_derives = true, rpc)]
        TestERC20,
        "../../lib/rain.orderbook/out/ArbTest.sol/Token.json"
    );

    sol!(
        #![sol(all_derives = true, rpc)]
        IERC20,
        "../../lib/forge-std/out/IERC20.sol/IERC20.json"
    );

    async fn setup_anvil_with_token() -> (
        RawPrivateKeyWallet<impl Provider + WalletProvider + Clone>,
        impl Provider + Clone,
        Address,
        Address,
    ) {
        let anvil = Box::leak(Box::new(Anvil::new().spawn()));

        let private_key_bytes = anvil.keys()[0].to_bytes();
        let signer = PrivateKeySigner::from_bytes(&B256::from_slice(&private_key_bytes)).unwrap();
        let signer_address = signer.address();
        let wallet = EthereumWallet::from(signer);

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_http(anvil.endpoint().parse().unwrap());

        let token = TestERC20::deploy(&provider).await.unwrap();
        let token_address = *token.address();

        let mint_amount = U256::from(1_000_000) * U256::from(10).pow(U256::from(18));
        token
            .mint(signer_address, mint_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let caller = RawPrivateKeyWallet::new(provider.clone(), 1);

        (caller, provider, token_address, signer_address)
    }

    #[tokio::test]
    async fn send_submits_and_returns_receipt() {
        let (caller, _provider, token_address, _signer) = setup_anvil_with_token().await;

        let recipient = Address::random();
        let amount = U256::from(1000);
        let calldata = IERC20::transferCall {
            to: recipient,
            amount,
        }
        .abi_encode();

        let receipt = caller
            .send(token_address, Bytes::from(calldata), "ERC20 transfer")
            .await
            .unwrap();

        assert!(receipt.status(), "transaction should succeed");
        assert_eq!(receipt.to.unwrap(), token_address);
    }

    #[tokio::test]
    async fn send_state_change_persists() {
        let (caller, provider, token_address, _signer) = setup_anvil_with_token().await;

        let recipient = Address::random();
        let amount = U256::from(1000);

        let token = IERC20::new(token_address, &provider);
        let before = token.balanceOf(recipient).call().await.unwrap();
        assert_eq!(before, U256::ZERO);

        let calldata = IERC20::transferCall {
            to: recipient,
            amount,
        }
        .abi_encode();

        caller
            .send(token_address, Bytes::from(calldata), "ERC20 transfer")
            .await
            .unwrap();

        let after = token.balanceOf(recipient).call().await.unwrap();
        assert_eq!(after, amount);
    }

    #[tokio::test]
    async fn send_detects_revert() {
        let (caller, _provider, token_address, _signer) = setup_anvil_with_token().await;

        // Transfer more than balance to trigger revert
        let recipient = Address::random();
        let excessive_amount = U256::from(999_999_999) * U256::from(10).pow(U256::from(18));

        let calldata = IERC20::transferCall {
            to: recipient,
            amount: excessive_amount,
        }
        .abi_encode();

        let result = caller
            .send(token_address, Bytes::from(calldata), "should revert")
            .await;

        assert!(
            result.is_err(),
            "expected error for transfer exceeding balance, got: {result:?}"
        );
    }
}
