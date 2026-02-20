//! Local signer implementation for test environments (anvil).
//!
//! `RawPrivateKeyWallet` takes a private key and a base provider, wraps the
//! provider with a [`WalletFiller`] internally, and submits transactions
//! directly. This is only compiled when the `local-signer` feature is enabled
//! and should only be used in tests.

use alloy::network::{Ethereum, EthereumWallet};
use alloy::primitives::{Address, B256, Bytes};
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
};
use alloy::providers::{Identity, Provider, ProviderBuilder, WalletProvider};
use alloy::rpc::types::{TransactionReceipt, TransactionRequest};
use alloy::signers::local::PrivateKeySigner;
use async_trait::async_trait;
use tracing::info;

use crate::{Evm, EvmError, Wallet};

/// Provider type produced by wrapping a base provider with default fillers
/// and a [`WalletFiller`].
type SignerProvider<P> = FillProvider<
    JoinFill<
        JoinFill<
            Identity,
            JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
        >,
        WalletFiller<EthereumWallet>,
    >,
    P,
    Ethereum,
>;

/// Local wallet that signs and submits transactions directly.
///
/// Takes a raw private key and a base provider (without wallet filler),
/// wraps the provider with signing capabilities internally.
///
/// `P` is the **base** provider type (e.g., from
/// `ProviderBuilder::new().connect_http(url)`). The wallet-equipped provider
/// is derived internally and exposed via [`Evm::provider()`].
#[derive(Clone)]
pub struct RawPrivateKeyWallet<P: Provider> {
    provider: SignerProvider<P>,
    required_confirmations: u64,
}

impl<P: Provider + Clone + Send + Sync + 'static> RawPrivateKeyWallet<P> {
    /// Creates a new `RawPrivateKeyWallet` from a private key and base provider.
    ///
    /// The base provider is wrapped with gas, nonce, chain ID, and wallet
    /// fillers. Use [`Evm::provider()`] to access the signing provider for
    /// contract deployments in tests.
    pub fn new(
        private_key: &B256,
        provider: P,
        required_confirmations: u64,
    ) -> Result<Self, EvmError> {
        let signer = PrivateKeySigner::from_bytes(private_key)?;
        let eth_wallet = EthereumWallet::from(signer);

        let signing_provider = ProviderBuilder::new()
            .wallet(eth_wallet)
            .connect_provider(provider);

        Ok(Self {
            provider: signing_provider,
            required_confirmations,
        })
    }
}

#[async_trait]
impl<P> Evm for RawPrivateKeyWallet<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    type Provider = SignerProvider<P>;

    fn provider(&self) -> &SignerProvider<P> {
        &self.provider
    }
}

#[async_trait]
impl<P> Wallet for RawPrivateKeyWallet<P>
where
    P: Provider + Clone + Send + Sync + 'static,
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

        let tx = TransactionRequest::default()
            .to(contract)
            .input(calldata.into());

        let pending = self.provider.send_transaction(tx).await?;

        info!(tx_hash = %pending.tx_hash(), note, "Transaction submitted");

        let receipt = pending
            .with_required_confirmations(self.required_confirmations)
            .get_receipt()
            .await?;

        info!(tx_hash = %receipt.transaction_hash, note, "Transaction confirmed");

        Ok(receipt)
    }
}

#[cfg(test)]
mod tests {
    use alloy::node_bindings::Anvil;
    use alloy::primitives::U256;
    use alloy::sol;

    use crate::NoOpErrorRegistry;

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

    async fn setup_anvil_with_token()
    -> (RawPrivateKeyWallet<impl Provider + Clone>, Address, Address) {
        let anvil = Box::leak(Box::new(Anvil::new().spawn()));
        let private_key = B256::from_slice(&anvil.keys()[0].to_bytes());

        let base_provider = ProviderBuilder::new().connect_http(anvil.endpoint().parse().unwrap());

        let wallet = RawPrivateKeyWallet::new(&private_key, base_provider, 1).unwrap();
        let signer_address = wallet.address();

        let token = TestERC20::deploy(wallet.provider()).await.unwrap();
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

        (wallet, token_address, signer_address)
    }

    #[tokio::test]
    async fn submit_returns_receipt() {
        let (wallet, token_address, _signer) = setup_anvil_with_token().await;

        let recipient = Address::random();
        let amount = U256::from(1000);

        let receipt = wallet
            .submit::<NoOpErrorRegistry, _>(
                token_address,
                IERC20::transferCall {
                    to: recipient,
                    amount,
                },
                "ERC20 transfer",
            )
            .await
            .unwrap();

        assert!(receipt.status(), "transaction should succeed");
        assert_eq!(receipt.to.unwrap(), token_address);
    }

    #[tokio::test]
    async fn submit_state_change_persists() {
        let (wallet, token_address, _signer) = setup_anvil_with_token().await;

        let recipient = Address::random();
        let amount = U256::from(1000);

        let before: U256 = wallet
            .call::<NoOpErrorRegistry, _>(
                token_address,
                IERC20::balanceOfCall { account: recipient },
            )
            .await
            .unwrap();
        assert_eq!(before, U256::ZERO);

        wallet
            .submit::<NoOpErrorRegistry, _>(
                token_address,
                IERC20::transferCall {
                    to: recipient,
                    amount,
                },
                "ERC20 transfer",
            )
            .await
            .unwrap();

        let after: U256 = wallet
            .call::<NoOpErrorRegistry, _>(
                token_address,
                IERC20::balanceOfCall { account: recipient },
            )
            .await
            .unwrap();
        assert_eq!(after, amount);
    }

    #[tokio::test]
    async fn submit_detects_revert() {
        let (wallet, token_address, _signer) = setup_anvil_with_token().await;

        let recipient = Address::random();
        let excessive_amount = U256::from(999_999_999) * U256::from(10).pow(U256::from(18));

        let error = wallet
            .submit::<NoOpErrorRegistry, _>(
                token_address,
                IERC20::transferCall {
                    to: recipient,
                    amount: excessive_amount,
                },
                "should revert",
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, EvmError::Transport(_)),
            "expected Transport error from Anvil pre-simulation revert, got: {error:?}"
        );
    }
}
