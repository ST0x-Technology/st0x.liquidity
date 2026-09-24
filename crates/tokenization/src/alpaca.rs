//! Alpaca tokenization transport is provided by `st0x-alpaca` while this
//! adapter owns the onchain redemption and mint verification steps.

use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::Provider;
use alloy::sol_types::SolEvent;
use async_trait::async_trait;
use tracing::info;

use st0x_alpaca::core::Network as AlpacaChain;
use st0x_alpaca::tokenization::AlpacaTokenizationService as SharedService;
use st0x_evm::{
    Chain, EvmError, IERC20, IntoErrorRegistry, NODE_SYNC_MAX_ATTEMPTS, NODE_SYNC_POLL_INTERVAL,
    OpenChainErrorRegistry, Wallet, wait_for_node_sync,
};
use st0x_execution::{AlpacaAccountId, AlpacaBrokerAuth, FractionalShares, PollingConfig, Symbol};
use st0x_wrapper::UnwrappedToken;

use super::{
    IssuerRequestId, MintVerificationError, TokenizationRequestId, Tokenizer, TokenizerError,
};

pub use st0x_alpaca::tokenization::{
    AlpacaApiErrorMessage, AlpacaTokenizationError, TokenizationRequest, TokenizationRequestStatus,
    TokenizationRequestType,
};

/// Alpaca HTTP client with Liquidity's onchain redemption and mint checks.
pub struct AlpacaTokenizationService<W: Wallet> {
    client: SharedService,
    wallet: W,
    redemption_wallet: Option<Address>,
}

impl<W: Wallet> AlpacaTokenizationService<W> {
    /// Binds Alpaca tokenization to the selected chain and signing wallet.
    pub fn new(
        base_url: String,
        account_id: AlpacaAccountId,
        auth: AlpacaBrokerAuth,
        wallet: W,
        chain: Chain,
        redemption_wallet: Option<Address>,
    ) -> Result<Self, AlpacaTokenizationError> {
        let network = match chain {
            Chain::Base => AlpacaChain::Base,
            Chain::Ethereum => AlpacaChain::Ethereum,
            Chain::HyperEvm => AlpacaChain::HyperEvm,
            Chain::Robinhood => AlpacaChain::Robinhood,
        };
        let client = SharedService::new(base_url, account_id, auth, network)?;
        Ok(Self {
            client,
            wallet,
            redemption_wallet,
        })
    }

    #[must_use]
    pub fn with_polling_config(mut self, polling_config: PollingConfig) -> Self {
        self.client = self.client.with_polling_config(polling_config);
        self
    }

    pub(crate) async fn send_for_redemption<Registry: IntoErrorRegistry>(
        &self,
        token: UnwrappedToken,
        amount: U256,
    ) -> Result<TxHash, TokenizerError> {
        let redemption_wallet = self
            .redemption_wallet
            .ok_or(TokenizerError::MissingRedemptionWallet)?;
        let receipt = self
            .wallet
            .submit::<Registry, _>(
                token.address(),
                IERC20::transferCall {
                    to: redemption_wallet,
                    amount,
                },
                "ERC20 transfer for redemption",
            )
            .await?;
        Ok(receipt.transaction_hash)
    }

    pub async fn list_requests(&self) -> Result<Vec<TokenizationRequest>, AlpacaTokenizationError> {
        self.client.list_requests().await
    }

    /// Verify that a mint transaction landed onchain by parsing
    /// Transfer event logs from the receipt.
    pub(crate) async fn verify_mint_tx(
        &self,
        tx_hash: TxHash,
        token_address: Address,
        wallet: Address,
        expected_amount: U256,
    ) -> Result<(), MintVerificationError> {
        let receipt = self
            .wallet
            .provider()
            .get_transaction_receipt(tx_hash)
            .await?
            .ok_or(MintVerificationError::ReceiptNotFound { tx_hash })?;

        if !receipt.status() {
            return Err(MintVerificationError::TransactionReverted { tx_hash });
        }

        info!(target: "tokenization", %tx_hash, "Mint transaction receipt verified (status: success)");

        // Sum all Transfer events from the token contract to the expected wallet.
        let total_transferred: U256 = receipt
            .inner
            .logs()
            .iter()
            .filter(|log| log.address() == token_address)
            .filter(|log| log.topics().first() == Some(&IERC20::Transfer::SIGNATURE_HASH))
            .filter_map(|log| log.log_decode::<IERC20::Transfer>().ok())
            .filter(|decoded| decoded.data().to == wallet)
            .map(|decoded| decoded.data().value)
            .try_fold(U256::ZERO, |acc, val| {
                acc.checked_add(val)
                    .ok_or(MintVerificationError::TransferOverflow { tx_hash })
            })?;

        if total_transferred.is_zero() {
            return Err(MintVerificationError::NoMatchingTransfer {
                tx_hash,
                wallet,
                token: token_address,
            });
        }

        if total_transferred < expected_amount {
            return Err(MintVerificationError::InsufficientTransferAmount {
                tx_hash,
                expected: expected_amount,
                actual: total_transferred,
            });
        }

        info!(
            target: "tokenization",
            %tx_hash,
            %total_transferred,
            %expected_amount,
            "Onchain mint verification passed (Transfer events confirmed)"
        );
        Ok(())
    }
}

#[async_trait]
impl<W: Wallet> Tokenizer for AlpacaTokenizationService<W> {
    async fn request_mint(
        &self,
        symbol: Symbol,
        quantity: FractionalShares,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        Ok(self
            .client
            .request_mint(symbol, quantity, wallet, issuer_request_id)
            .await?)
    }

    async fn poll_mint_until_complete(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        Ok(self.client.poll_mint_until_complete(id).await?)
    }

    async fn find_mint_by_issuer_request_id(
        &self,
        issuer_request_id: &IssuerRequestId,
    ) -> Result<Option<TokenizationRequest>, TokenizerError> {
        Ok(self
            .client
            .find_mint_by_issuer_request_id(issuer_request_id)
            .await?)
    }

    async fn get_request(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        Ok(self.client.get_request(id).await?)
    }

    fn redemption_wallet(&self) -> Option<Address> {
        self.redemption_wallet
    }

    async fn wait_for_block(&self, block: u64) -> Result<(), EvmError> {
        wait_for_node_sync(
            self.wallet.provider(),
            block,
            NODE_SYNC_POLL_INTERVAL,
            NODE_SYNC_MAX_ATTEMPTS,
        )
        .await
    }

    async fn send_for_redemption(
        &self,
        token: UnwrappedToken,
        amount: U256,
    ) -> Result<TxHash, TokenizerError> {
        Self::send_for_redemption::<OpenChainErrorRegistry>(self, token, amount).await
    }

    async fn poll_for_redemption(
        &self,
        tx_hash: &TxHash,
    ) -> Result<TokenizationRequest, TokenizerError> {
        Ok(self.client.poll_for_redemption(tx_hash).await?)
    }

    async fn find_redemption_by_tx(
        &self,
        tx_hash: &TxHash,
    ) -> Result<Option<TokenizationRequest>, TokenizerError> {
        Ok(self.client.find_redemption_by_tx(tx_hash).await?)
    }

    async fn poll_redemption_until_complete(
        &self,
        id: &TokenizationRequestId,
    ) -> Result<TokenizationRequest, TokenizerError> {
        Ok(self.client.poll_redemption_until_complete(id).await?)
    }

    async fn verify_mint_tx(
        &self,
        tx_hash: TxHash,
        token_address: Address,
        wallet: Address,
        expected_amount: U256,
    ) -> Result<(), MintVerificationError> {
        Self::verify_mint_tx(self, tx_hash, token_address, wallet, expected_amount).await
    }

    async fn list_pending_requests(&self) -> Result<Vec<TokenizationRequest>, TokenizerError> {
        Ok(self.client.list_pending_requests().await?)
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::TransactionBuilder;
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{Address, B256, address, fixed_bytes};
    use alloy::providers::ProviderBuilder;
    use httpmock::MockServer;
    use uuid::uuid;

    use st0x_evm::OpenChainErrorRegistry;
    use st0x_evm::local::RawPrivateKeyWallet;

    use super::*;
    use crate::bindings::TestERC20;

    const TEST_REDEMPTION_WALLET: Address = address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    fn setup_anvil() -> (AnvilInstance, String, B256) {
        let anvil = Anvil::new().spawn();
        let endpoint = anvil.endpoint();
        let private_key = B256::from_slice(&anvil.keys()[0].to_bytes());
        (anvil, endpoint, private_key)
    }

    async fn create_test_service_from_mock(
        server: &MockServer,
        anvil_endpoint: &str,
        private_key: &B256,
        redemption_wallet: Address,
    ) -> AlpacaTokenizationService<impl Wallet> {
        let provider = ProviderBuilder::new()
            .connect(anvil_endpoint)
            .await
            .unwrap();
        let wallet = RawPrivateKeyWallet::new(private_key, provider, 1).unwrap();
        AlpacaTokenizationService::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            AlpacaBrokerAuth::Basic {
                api_key: "test_api_key".to_string(),
                api_secret: "test_api_secret".to_string(),
            },
            wallet,
            Chain::Base,
            Some(redemption_wallet),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_send_tokens_for_redemption_success() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let wallet = RawPrivateKeyWallet::new(
            &key,
            ProviderBuilder::new().connect(&endpoint).await.unwrap(),
            1,
        )
        .unwrap();

        let provider = wallet.signing_provider().clone();
        let token = TestERC20::deploy(&provider).await.unwrap();
        let token_address = *token.address();

        let mint_amount = U256::from(1_000_000_000u64);
        token
            .mint(wallet.address(), mint_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let client = AlpacaTokenizationService::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            AlpacaBrokerAuth::Basic {
                api_key: "test_api_key".to_string(),
                api_secret: "test_api_secret".to_string(),
            },
            wallet,
            Chain::Base,
            Some(TEST_REDEMPTION_WALLET),
        )
        .expect("basic-auth tokenization client");

        let transfer_amount = U256::from(100_000u64);

        client
            .send_for_redemption::<OpenChainErrorRegistry>(
                UnwrappedToken::unchecked(token_address),
                transfer_amount,
            )
            .await
            .unwrap();

        let balance = token
            .balanceOf(TEST_REDEMPTION_WALLET)
            .call()
            .await
            .unwrap();
        assert_eq!(
            balance, transfer_amount,
            "redemption wallet should have received tokens"
        );
    }

    #[tokio::test]
    async fn test_wait_for_block_succeeds_when_node_already_at_block() {
        let (_anvil, endpoint, key) = setup_anvil();
        let wallet = RawPrivateKeyWallet::new(
            &key,
            ProviderBuilder::new().connect(&endpoint).await.unwrap(),
            1,
        )
        .unwrap();

        // The wait polls the wallet's own provider, so a block it already
        // reports must clear the gate on the first poll.
        let current_block = wallet.signing_provider().get_block_number().await.unwrap();

        let client = AlpacaTokenizationService::new(
            "https://unused.invalid".to_string(),
            TEST_ACCOUNT_ID,
            AlpacaBrokerAuth::Basic {
                api_key: "test_api_key".to_string(),
                api_secret: "test_api_secret".to_string(),
            },
            wallet,
            Chain::Base,
            Some(TEST_REDEMPTION_WALLET),
        )
        .expect("basic-auth tokenization client");

        client
            .wait_for_block(current_block)
            .await
            .expect("wait_for_block must succeed when the node is already at the block");
    }

    #[tokio::test]
    async fn test_send_tokens_for_redemption_insufficient_balance() {
        let (_anvil, endpoint, key) = setup_anvil();
        let wallet = RawPrivateKeyWallet::new(
            &key,
            ProviderBuilder::new().connect(&endpoint).await.unwrap(),
            1,
        )
        .unwrap();

        let provider = wallet.signing_provider().clone();
        let token = TestERC20::deploy(&provider).await.unwrap();
        let token_address = *token.address();

        let client = AlpacaTokenizationService::new(
            "https://unused.invalid".to_string(),
            TEST_ACCOUNT_ID,
            AlpacaBrokerAuth::Basic {
                api_key: "test_api_key".to_string(),
                api_secret: "test_api_secret".to_string(),
            },
            wallet,
            Chain::Base,
            Some(TEST_REDEMPTION_WALLET),
        )
        .expect("basic-auth tokenization client");

        let transfer_amount = U256::from(100_000u64);
        let err = client
            .send_for_redemption::<OpenChainErrorRegistry>(
                UnwrappedToken::unchecked(token_address),
                transfer_amount,
            )
            .await
            .unwrap_err();

        assert!(
            matches!(err, TokenizerError::Evm(_)),
            "expected Evm error variant, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn test_verify_mint_tx_no_matching_transfer_event() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service =
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let wallet = RawPrivateKeyWallet::new(
            &key,
            ProviderBuilder::new().connect(&endpoint).await.unwrap(),
            1,
        )
        .unwrap();

        let provider = wallet.signing_provider().clone();
        let token = TestERC20::deploy(&provider).await.unwrap();

        // Mint tokens to the signer (creates a Transfer event from 0x0 -> signer)
        let mint_amount = U256::from(1_000_000u64);
        let receipt = token
            .mint(wallet.address(), mint_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        // Verify with a different token address -- the Transfer event in the
        // receipt came from the real token contract, not this unrelated address,
        // so no matching Transfer should be found.
        let unrelated_token = Address::random();
        let error = service
            .verify_mint_tx(
                receipt.transaction_hash,
                unrelated_token,
                wallet.address(),
                mint_amount,
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintVerificationError::NoMatchingTransfer {
                    token,
                    ..
                } if token == unrelated_token
            ),
            "Expected NoMatchingTransfer for unrelated token address, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_verify_mint_tx_insufficient_transfer_amount() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service =
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let wallet = RawPrivateKeyWallet::new(
            &key,
            ProviderBuilder::new().connect(&endpoint).await.unwrap(),
            1,
        )
        .unwrap();

        let provider = wallet.signing_provider().clone();
        let token = TestERC20::deploy(&provider).await.unwrap();
        let token_address = *token.address();

        let mint_amount = U256::from(500u64);
        let receipt = token
            .mint(wallet.address(), mint_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        // Expect more than what was actually transferred
        let expected_amount = U256::from(1_000u64);
        let error = service
            .verify_mint_tx(
                receipt.transaction_hash,
                token_address,
                wallet.address(),
                expected_amount,
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintVerificationError::InsufficientTransferAmount {
                    expected,
                    actual,
                    ..
                } if expected == expected_amount && actual == mint_amount
            ),
            "Expected InsufficientTransferAmount, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_verify_mint_tx_success_with_transfer_events() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service =
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let wallet = RawPrivateKeyWallet::new(
            &key,
            ProviderBuilder::new().connect(&endpoint).await.unwrap(),
            1,
        )
        .unwrap();

        let provider = wallet.signing_provider().clone();
        let token = TestERC20::deploy(&provider).await.unwrap();
        let token_address = *token.address();

        let mint_amount = U256::from(1_000_000u64);
        let receipt = token
            .mint(wallet.address(), mint_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        service
            .verify_mint_tx(
                receipt.transaction_hash,
                token_address,
                wallet.address(),
                mint_amount,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_verify_mint_tx_receipt_not_found() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service =
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let nonexistent_tx =
            fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let token_address = Address::random();
        let wallet = Address::random();

        let error = service
            .verify_mint_tx(nonexistent_tx, token_address, wallet, U256::from(1000u64))
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintVerificationError::ReceiptNotFound { tx_hash } if tx_hash == nonexistent_tx
            ),
            "Expected ReceiptNotFound, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_verify_mint_tx_reverted_transaction() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service =
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let signer: alloy::signers::local::PrivateKeySigner =
            alloy::signers::local::PrivateKeySigner::from_bytes(&key).unwrap();
        let eth_wallet = alloy::network::EthereumWallet::from(signer);
        let provider = ProviderBuilder::new()
            .wallet(eth_wallet)
            .connect(&endpoint)
            .await
            .unwrap();

        // Deploy a contract whose runtime code is PUSH0 PUSH0 REVERT (always reverts).
        // Init code copies the 3-byte runtime from bytecode offset 10 into memory.
        let deploy_tx = alloy::rpc::types::TransactionRequest::default()
            .with_deploy_code(alloy::hex!("6003600a5f3960035ff35f5ffd"));
        let deploy_receipt = provider
            .send_transaction(deploy_tx)
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        let reverting_address = deploy_receipt.contract_address.unwrap();

        // Call the reverting contract with explicit gas to bypass estimation
        // (eth_estimateGas would reject the call since it reverts).
        let call_tx = alloy::rpc::types::TransactionRequest::default()
            .to(reverting_address)
            .with_gas_limit(100_000);
        let reverted_receipt = provider
            .send_transaction(call_tx)
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        assert!(
            !reverted_receipt.status(),
            "Transaction should have reverted"
        );

        let error = service
            .verify_mint_tx(
                reverted_receipt.transaction_hash,
                Address::random(),
                Address::random(),
                U256::from(1u64),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintVerificationError::TransactionReverted { tx_hash }
                    if tx_hash == reverted_receipt.transaction_hash
            ),
            "Expected TransactionReverted, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_verify_mint_tx_wrong_wallet() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service =
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let wallet = RawPrivateKeyWallet::new(
            &key,
            ProviderBuilder::new().connect(&endpoint).await.unwrap(),
            1,
        )
        .unwrap();

        let provider = wallet.signing_provider().clone();
        let token = TestERC20::deploy(&provider).await.unwrap();
        let token_address = *token.address();

        // Mint tokens to the signer wallet
        let mint_amount = U256::from(1_000_000u64);
        let receipt = token
            .mint(wallet.address(), mint_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        // Verify expecting tokens at a different wallet than where they were sent
        let wrong_wallet = Address::random();
        let error = service
            .verify_mint_tx(
                receipt.transaction_hash,
                token_address,
                wrong_wallet,
                mint_amount,
            )
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                MintVerificationError::NoMatchingTransfer {
                    wallet: error_wallet,
                    token,
                    ..
                } if error_wallet == wrong_wallet && token == token_address
            ),
            "Expected NoMatchingTransfer for wrong wallet, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_verify_mint_tx_exact_amount_succeeds() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service =
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let wallet = RawPrivateKeyWallet::new(
            &key,
            ProviderBuilder::new().connect(&endpoint).await.unwrap(),
            1,
        )
        .unwrap();

        let provider = wallet.signing_provider().clone();
        let token = TestERC20::deploy(&provider).await.unwrap();
        let token_address = *token.address();

        // Mint exactly the expected amount -- boundary case where
        // total_transferred == expected_amount should pass
        let exact_amount = U256::from(999u64);
        let receipt = token
            .mint(wallet.address(), exact_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        service
            .verify_mint_tx(
                receipt.transaction_hash,
                token_address,
                wallet.address(),
                exact_amount,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_verify_mint_tx_overmint_succeeds() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service =
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await;

        let wallet = RawPrivateKeyWallet::new(
            &key,
            ProviderBuilder::new().connect(&endpoint).await.unwrap(),
            1,
        )
        .unwrap();

        let provider = wallet.signing_provider().clone();
        let token = TestERC20::deploy(&provider).await.unwrap();
        let token_address = *token.address();

        // Mint more than the expected amount -- verification should pass
        // since total_transferred > expected_amount
        let mint_amount = U256::from(2_000u64);
        let expected_amount = U256::from(1_000u64);
        let receipt = token
            .mint(wallet.address(), mint_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        service
            .verify_mint_tx(
                receipt.transaction_hash,
                token_address,
                wallet.address(),
                expected_amount,
            )
            .await
            .unwrap();
    }
}
