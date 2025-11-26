use alloy::network::EthereumWallet;
use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::transports::{RpcError, TransportErrorKind};

use crate::bindings::{IOrderBookV4, OrderBook, TestERC20};

type LocalEvmProvider = alloy::providers::fillers::FillProvider<
    alloy::providers::fillers::JoinFill<
        alloy::providers::fillers::JoinFill<
            alloy::providers::Identity,
            alloy::providers::fillers::JoinFill<
                alloy::providers::fillers::GasFiller,
                alloy::providers::fillers::JoinFill<
                    alloy::providers::fillers::BlobGasFiller,
                    alloy::providers::fillers::JoinFill<
                        alloy::providers::fillers::NonceFiller,
                        alloy::providers::fillers::ChainIdFiller,
                    >,
                >,
            >,
        >,
        alloy::providers::fillers::WalletFiller<EthereumWallet>,
    >,
    alloy::providers::RootProvider<alloy::network::Ethereum>,
    alloy::network::Ethereum,
>;

pub(crate) struct LocalEvm {
    _anvil: AnvilInstance,
    pub(crate) provider: LocalEvmProvider,
    pub(crate) signer: PrivateKeySigner,
    pub(crate) orderbook_address: Address,
    pub(crate) token_address: Address,
}

impl LocalEvm {
    pub(crate) async fn new() -> Result<Self, LocalEvmError> {
        let anvil = Anvil::new().spawn();
        let endpoint = anvil.endpoint();

        let private_key_bytes = anvil.keys()[0].to_bytes();
        let signer = PrivateKeySigner::from_bytes(&B256::from_slice(&private_key_bytes))
            .map_err(|e| LocalEvmError::InvalidPrivateKey(e.into()))?;

        let wallet = EthereumWallet::from(signer.clone());
        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_http(endpoint.parse()?);

        let orderbook_address = Self::deploy_orderbook(&provider).await?;

        let token_address = Self::deploy_token(&provider, signer.address()).await?;

        Ok(Self {
            _anvil: anvil,
            provider,
            signer,
            orderbook_address,
            token_address,
        })
    }

    async fn deploy_orderbook(provider: &impl Provider) -> Result<Address, LocalEvmError> {
        let orderbook = OrderBook::deploy(provider).await?;

        Ok(*orderbook.address())
    }

    async fn deploy_token(
        provider: &impl Provider,
        recipient: Address,
    ) -> Result<Address, LocalEvmError> {
        let token = TestERC20::deploy(provider).await?;

        let initial_supply = U256::from(1_000_000) * U256::from(10).pow(U256::from(18));

        token
            .mint(recipient, initial_supply)
            .send()
            .await?
            .get_receipt()
            .await?;

        Ok(*token.address())
    }

    pub(crate) async fn mint_tokens(
        &self,
        token: Address,
        to: Address,
        amount: U256,
    ) -> Result<(), LocalEvmError> {
        let token_contract = TestERC20::new(token, &self.provider);

        token_contract
            .mint(to, amount)
            .send()
            .await?
            .get_receipt()
            .await?;

        Ok(())
    }

    pub(crate) async fn approve_tokens(
        &self,
        token: Address,
        spender: Address,
        amount: U256,
    ) -> Result<(), LocalEvmError> {
        let token_contract = TestERC20::new(token, &self.provider);

        token_contract
            .approve(spender, amount)
            .send()
            .await?
            .get_receipt()
            .await?;

        Ok(())
    }

    pub(crate) async fn get_balance(
        &self,
        token: Address,
        account: Address,
    ) -> Result<U256, LocalEvmError> {
        let token_contract = TestERC20::new(token, &self.provider);

        let balance = token_contract.balanceOf(account).call().await?;

        Ok(balance)
    }

    pub(crate) async fn get_vault_balance(
        &self,
        token: Address,
        vault_id: U256,
    ) -> Result<U256, LocalEvmError> {
        let orderbook = IOrderBookV4::new(self.orderbook_address, &self.provider);

        let balance = orderbook
            .vaultBalance(self.signer.address(), token, vault_id)
            .call()
            .await?;

        Ok(balance)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LocalEvmError {
    #[error("Invalid private key")]
    InvalidPrivateKey(#[source] alloy::signers::Error),
    #[error("Contract error")]
    Contract(#[from] alloy::contract::Error),
    #[error("Provider error")]
    Provider(#[from] alloy::providers::PendingTransactionError),
    #[error("RPC error")]
    Rpc(#[from] RpcError<TransportErrorKind>),
    #[error("URL parse error")]
    UrlParse(#[from] url::ParseError),
}
