//! WrapperService implementation for ERC-4626 token wrapping/unwrapping.
//!
//! This module and its private `IERC20`/`IERC4626` bindings are gated behind the
//! `erc4626` feature; the [`Wrapper`](crate::Wrapper) trait and its domain types ship
//! in the default build.

use alloy::primitives::{Address, TxHash, U256};
use alloy::sol;
use alloy::sol_types::SolEvent;
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Duration;
use tracing::info;

use st0x_evm::{
    IntoErrorRegistry, NODE_SYNC_MAX_ATTEMPTS, NODE_SYNC_POLL_INTERVAL, OpenChainErrorRegistry,
    Wallet, wait_for_node_sync,
};
use st0x_execution::Symbol;

use crate::{
    UnderlyingPerWrapped, UnwrapConfirmation, UnwrappedToken, WrapConfirmation, WrappedEquity,
    Wrapper, WrapperError,
};

sol!(
    #![sol(all_derives = true, rpc)]
    IERC20, env!("ST0X_IERC20_ABI")
);

sol!(
    #![sol(all_derives = true, rpc)]
    IERC4626, env!("ST0X_IERC4626_ABI")
);

/// One unit with 18 decimals for ratio queries.
const RATIO_QUERY_AMOUNT: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);

/// Service for managing ERC-4626 token wrapping/unwrapping operations.
///
/// Uses the wallet's embedded provider for read-only view calls (ratio
/// queries) and the wallet itself for write transactions (deposit/redeem).
pub struct WrapperService<W: Wallet> {
    wallet: W,
    config: HashMap<Symbol, WrappedEquity>,
    /// Poll interval used by node-sync waits (`wait_for_block` and the
    /// post-approval deposit gate). Production always uses
    /// [`NODE_SYNC_POLL_INTERVAL`]; tests override it to avoid sleeping
    /// through the full production budget.
    node_sync_poll_interval: Duration,
}

impl<W: Wallet> WrapperService<W> {
    pub fn new(wallet: W, config: HashMap<Symbol, WrappedEquity>) -> Self {
        Self {
            wallet,
            config,
            node_sync_poll_interval: NODE_SYNC_POLL_INTERVAL,
        }
    }

    /// Constructs a service with a custom node-sync poll interval. Test-only:
    /// lets node-sync exhaustion paths run without sleeping at the production
    /// one-second cadence.
    #[cfg(test)]
    fn with_node_sync_poll_interval(
        wallet: W,
        config: HashMap<Symbol, WrappedEquity>,
        node_sync_poll_interval: Duration,
    ) -> Self {
        Self {
            wallet,
            config,
            node_sync_poll_interval,
        }
    }

    /// Fetches the current conversion ratio for a wrapped token.
    async fn get_ratio<Registry: IntoErrorRegistry>(
        &self,
        wrapped_token: Address,
    ) -> Result<UnderlyingPerWrapped, WrapperError> {
        let assets_per_share = self
            .wallet
            .call::<Registry, _>(
                wrapped_token,
                IERC4626::convertToAssetsCall {
                    shares: RATIO_QUERY_AMOUNT,
                },
            )
            .await?;

        Ok(UnderlyingPerWrapped::new(assets_per_share)?)
    }

    /// Gets the configured token pair for a symbol.
    fn lookup_equity(&self, symbol: &Symbol) -> Option<&WrappedEquity> {
        self.config.get(symbol)
    }
}

#[async_trait]
impl<W: Wallet> Wrapper for WrapperService<W> {
    async fn get_ratio_for_symbol(
        &self,
        symbol: &Symbol,
    ) -> Result<UnderlyingPerWrapped, WrapperError> {
        let asset = self
            .lookup_equity(symbol)
            .ok_or_else(|| WrapperError::SymbolNotConfigured(symbol.clone()))?;

        self.get_ratio::<OpenChainErrorRegistry>(asset.derivative)
            .await
    }

    fn lookup_underlying(&self, symbol: &Symbol) -> Result<Address, WrapperError> {
        let asset = self
            .lookup_equity(symbol)
            .ok_or_else(|| WrapperError::SymbolNotConfigured(symbol.clone()))?;

        Ok(asset.underlying)
    }

    fn lookup_derivative(&self, symbol: &Symbol) -> Result<Address, WrapperError> {
        let asset = self
            .lookup_equity(symbol)
            .ok_or_else(|| WrapperError::SymbolNotConfigured(symbol.clone()))?;

        Ok(asset.derivative)
    }

    async fn attest_underlying(&self, symbol: &Symbol) -> Result<UnwrappedToken, WrapperError> {
        let equity = self
            .lookup_equity(symbol)
            .ok_or_else(|| WrapperError::SymbolNotConfigured(symbol.clone()))?;

        let attested: Address = self
            .wallet
            .call::<OpenChainErrorRegistry, _>(equity.derivative, IERC4626::assetCall {})
            .await?;

        if attested != equity.underlying {
            return Err(WrapperError::VaultAssetMismatch {
                symbol: symbol.clone(),
                vault: equity.derivative,
                configured: equity.underlying,
                attested,
            });
        }

        Ok(UnwrappedToken(attested))
    }

    async fn to_wrapped(
        &self,
        wrapped_token: Address,
        underlying_amount: U256,
        receiver: Address,
    ) -> Result<(TxHash, U256), WrapperError> {
        let tx_hash = self
            .submit_wrap(wrapped_token, underlying_amount, receiver)
            .await?;

        let WrapConfirmation { shares, .. } = self.confirm_wrap(wrapped_token, tx_hash).await?;

        Ok((tx_hash, shares))
    }

    async fn to_underlying(
        &self,
        wrapped_token: Address,
        wrapped_amount: U256,
        receiver: Address,
        owner: Address,
    ) -> Result<(TxHash, U256), WrapperError> {
        let tx_hash = self
            .submit_unwrap(wrapped_token, wrapped_amount, receiver, owner)
            .await?;

        let UnwrapConfirmation { assets, .. } = self.confirm_unwrap(wrapped_token, tx_hash).await?;

        Ok((tx_hash, assets))
    }

    async fn donate(
        &self,
        wrapped_token: Address,
        underlying_amount: U256,
    ) -> Result<TxHash, WrapperError> {
        let underlying_token: Address = self
            .wallet
            .call::<OpenChainErrorRegistry, _>(wrapped_token, IERC4626::assetCall {})
            .await?;

        info!(
            target: "orderbook",
            %underlying_token,
            %wrapped_token,
            %underlying_amount,
            "Donating underlying into ERC-4626 wrapper to bump NAV"
        );

        let receipt = self
            .wallet
            .submit::<OpenChainErrorRegistry, _>(
                underlying_token,
                IERC20::transferCall {
                    to: wrapped_token,
                    amount: underlying_amount,
                },
                "ERC20 transfer donating into ERC4626 wrapper",
            )
            .await?;

        let tx_hash = receipt.transaction_hash;

        info!(target: "orderbook", %tx_hash, "Wrapper NAV donation confirmed");

        Ok(tx_hash)
    }

    async fn submit_wrap(
        &self,
        wrapped_token: Address,
        underlying_amount: U256,
        receiver: Address,
    ) -> Result<TxHash, WrapperError> {
        let underlying_token: Address = self
            .wallet
            .call::<OpenChainErrorRegistry, _>(wrapped_token, IERC4626::assetCall {})
            .await?;

        let owner = self.wallet.address();

        let current_allowance: U256 = self
            .wallet
            .call::<OpenChainErrorRegistry, _>(
                underlying_token,
                IERC20::allowanceCall {
                    owner,
                    spender: wrapped_token,
                },
            )
            .await?;

        if current_allowance < underlying_amount {
            info!(
                target: "orderbook",
                %underlying_token,
                %wrapped_token,
                %underlying_amount,
                "Approving ERC-4626 vault to spend underlying tokens"
            );

            let approval_receipt = self
                .wallet
                .submit::<OpenChainErrorRegistry, _>(
                    underlying_token,
                    IERC20::approveCall {
                        spender: wrapped_token,
                        amount: underlying_amount,
                    },
                    "ERC20 approve for ERC4626 deposit",
                )
                .await?;

            // The deposit's pre-flight simulation reads the freshly granted
            // allowance. A load-balanced backend that has not yet indexed the
            // approval would see a zero allowance and revert the deposit, so
            // wait for the node to reach the approval block before submitting.
            let approval_block =
                approval_receipt
                    .block_number
                    .ok_or(WrapperError::MissingBlockNumber {
                        tx_hash: approval_receipt.transaction_hash,
                    })?;

            wait_for_node_sync(
                self.wallet.provider(),
                approval_block,
                self.node_sync_poll_interval,
                NODE_SYNC_MAX_ATTEMPTS,
            )
            .await?;
        }

        info!(target: "orderbook", %wrapped_token, %underlying_amount, "Sending ERC4626 deposit");
        let tx_hash = self
            .wallet
            .submit_pending(
                wrapped_token,
                IERC4626::depositCall {
                    assets: underlying_amount,
                    receiver,
                },
                "ERC4626 deposit",
            )
            .await?;

        info!(target: "orderbook", %tx_hash, "ERC4626 deposit submitted");

        Ok(tx_hash)
    }

    async fn confirm_wrap(
        &self,
        wrapped_token: Address,
        tx_hash: TxHash,
    ) -> Result<WrapConfirmation, WrapperError> {
        let receipt = self
            .wallet
            .confirm::<OpenChainErrorRegistry>(tx_hash)
            .await?;

        let block = receipt
            .block_number
            .ok_or(WrapperError::MissingBlockNumber { tx_hash })?;

        let shares = receipt
            .inner
            .logs()
            .iter()
            .filter(|log| log.address() == wrapped_token)
            .find_map(|log| {
                IERC4626::Deposit::decode_log(log.as_ref())
                    .ok()
                    .map(|event| event.data.shares)
            })
            .ok_or(WrapperError::MissingDepositEvent)?;

        Ok(WrapConfirmation { shares, block })
    }

    async fn submit_unwrap(
        &self,
        wrapped_token: Address,
        wrapped_amount: U256,
        receiver: Address,
        owner: Address,
    ) -> Result<TxHash, WrapperError> {
        let max_redeem: U256 = self
            .wallet
            .call::<OpenChainErrorRegistry, _>(wrapped_token, IERC4626::maxRedeemCall { owner })
            .await?;

        check_redeem_within_max(wrapped_token, wrapped_amount, max_redeem)?;

        info!(target: "orderbook", %wrapped_token, %wrapped_amount, "Sending ERC4626 redeem");
        let tx_hash = self
            .wallet
            .submit_pending(
                wrapped_token,
                IERC4626::redeemCall {
                    shares: wrapped_amount,
                    receiver,
                    owner,
                },
                "ERC4626 redeem",
            )
            .await?;

        info!(target: "orderbook", %tx_hash, "ERC4626 redeem submitted");

        Ok(tx_hash)
    }

    async fn confirm_unwrap(
        &self,
        wrapped_token: Address,
        tx_hash: TxHash,
    ) -> Result<UnwrapConfirmation, WrapperError> {
        let receipt = self
            .wallet
            .confirm::<OpenChainErrorRegistry>(tx_hash)
            .await?;

        let block = receipt
            .block_number
            .ok_or(WrapperError::MissingBlockNumber { tx_hash })?;

        let logs = receipt.inner.logs();
        let withdraw = logs
            .iter()
            .filter(|log| log.address() == wrapped_token)
            .find_map(|log| IERC4626::Withdraw::decode_log(log.as_ref()).ok())
            .ok_or(WrapperError::MissingWithdrawEvent)?;
        let assets = withdraw.data.assets;
        let receiver = withdraw.data.receiver;

        // Read the asset as of the receipt block: that is the token the redeem
        // delivered, whatever the vault reports later.
        let token: Address = self
            .wallet
            .call_at::<OpenChainErrorRegistry, _>(wrapped_token, IERC4626::assetCall {}, block)
            .await?;

        // The vault's word is not enough: the same receipt must show that token
        // leaving for the receiver in the withdrawn amount.
        let delivered = logs
            .iter()
            .filter(|log| log.address() == token)
            .filter_map(|log| IERC20::Transfer::decode_log(log.as_ref()).ok())
            .any(|transfer| transfer.data.to == receiver && transfer.data.value == assets);
        if !delivered {
            return Err(WrapperError::MissingUnderlyingTransfer {
                tx_hash,
                asset: token,
                receiver,
                assets,
            });
        }

        Ok(UnwrapConfirmation {
            token: UnwrappedToken(token),
            assets,
            block,
        })
    }

    async fn wait_for_block(&self, block: u64) -> Result<(), WrapperError> {
        wait_for_node_sync(
            self.wallet.provider(),
            block,
            self.node_sync_poll_interval,
            NODE_SYNC_MAX_ATTEMPTS,
        )
        .await
        .map_err(Into::into)
    }

    fn owner(&self) -> Address {
        self.wallet.address()
    }
}

/// Preflight check that a requested redeem does not exceed the vault's
/// `maxRedeem(owner)`.
///
/// Returns [`WrapperError::RedeemExceedsMax`] when `requested > max_redeem`, so
/// the caller can fail fast before submitting any transaction. We deliberately do
/// not clamp `requested` down to `max_redeem`: a request that exceeds `maxRedeem`
/// signals inventory/balance drift (the bot believes it holds more wrapped shares
/// than the wallet actually owns), and the financial-integrity rule requires range
/// violations to error rather than be silently capped.
fn check_redeem_within_max(
    wrapped_token: Address,
    requested: U256,
    max_redeem: U256,
) -> Result<(), WrapperError> {
    if requested > max_redeem {
        return Err(WrapperError::RedeemExceedsMax {
            wrapped_token,
            requested,
            max_redeem,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::consensus::{Receipt, ReceiptEnvelope, ReceiptWithBloom};
    use alloy::primitives::{Bloom, Bytes, Log as PrimitiveLog};
    use alloy::providers::Provider;
    use alloy::providers::ProviderBuilder;
    use alloy::providers::RootProvider;
    use alloy::providers::mock::Asserter;
    use alloy::rpc::client::RpcClient;
    use alloy::rpc::json_rpc::{RequestPacket, Response, ResponsePacket, ResponsePayload};
    use alloy::rpc::types::Log;
    use alloy::rpc::types::TransactionReceipt;
    use alloy::sol_types::SolCall;
    use alloy::transports::{TransportError, TransportFut};
    use serde_json::value::RawValue;
    use st0x_evm::{Evm, EvmError};
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use tower::Service;

    use super::*;

    /// Panicking wallet stub for tests that only exercise config lookups and
    /// error paths (no real chain connectivity). Provider type matches
    /// production (`RootProvider`).
    struct StubWallet {
        address: Address,
        provider: RootProvider,
    }

    impl StubWallet {
        fn stub(address: Address) -> Arc<dyn Wallet<Provider = RootProvider>> {
            Arc::new(Self {
                address,
                provider: RootProvider::new(
                    RpcClient::builder().http("http://stub.invalid".parse().unwrap()),
                ),
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

        async fn sign_typed_data(
            &self,
            _payload_json: String,
            _expected_digest: alloy::primitives::B256,
        ) -> Result<alloy::primitives::Signature, EvmError> {
            panic!(
                "StubWallet::sign_typed_data called - use a real wallet in tests that need signing"
            )
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

    fn test_equity() -> WrappedEquity {
        WrappedEquity {
            underlying: Address::random(),
            derivative: Address::random(),
        }
    }

    fn service_with_symbol(symbol: &str, equity: WrappedEquity) -> WrapperService<impl Wallet> {
        let wallet = StubWallet::stub(Address::random());
        let mut equities = HashMap::new();
        equities.insert(symbol.parse::<Symbol>().unwrap(), equity);
        WrapperService::new(wallet, equities)
    }

    #[test]
    fn lookup_equity_returns_configured_symbol() {
        let equity = test_equity();
        let expected_underlying = equity.underlying;
        let expected_derivative = equity.derivative;
        let service = service_with_symbol("AAPL", equity);

        let result = service.lookup_equity(&"AAPL".parse().unwrap());

        assert!(result.is_some());
        assert_eq!(result.unwrap().derivative, expected_derivative);
        assert_eq!(result.unwrap().underlying, expected_underlying);
    }

    #[test]
    fn lookup_equity_returns_none_for_unconfigured() {
        let service = service_with_symbol("AAPL", test_equity());

        let result = service.lookup_equity(&"TSLA".parse().unwrap());

        assert!(result.is_none());
    }

    #[test]
    fn lookup_derivative_returns_vault_address() {
        let equity = test_equity();
        let expected = equity.derivative;
        let service = service_with_symbol("AAPL", equity);

        let result = service.lookup_derivative(&"AAPL".parse().unwrap()).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn lookup_underlying_returns_underlying_address() {
        let equity = test_equity();
        let expected = equity.underlying;
        let service = service_with_symbol("AAPL", equity);

        let result = service.lookup_underlying(&"AAPL".parse().unwrap()).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn lookup_derivative_errors_on_unconfigured_symbol() {
        let service = service_with_symbol("AAPL", test_equity());

        let error = service
            .lookup_derivative(&"MSFT".parse().unwrap())
            .unwrap_err();

        assert!(
            matches!(
                error,
                WrapperError::SymbolNotConfigured(ref symbol)
                    if *symbol == "MSFT"
            ),
            "expected SymbolNotConfigured for MSFT, got: {error:?}"
        );
    }

    #[test]
    fn owner_returns_wallet_address() {
        let wallet_address = Address::random();
        let wallet = StubWallet::stub(wallet_address);
        let service = WrapperService::new(wallet, HashMap::new());

        assert_eq!(service.owner(), wallet_address);
    }

    #[tokio::test]
    async fn get_ratio_for_symbol_errors_on_unconfigured() {
        let service = service_with_symbol("AAPL", test_equity());

        let error = service
            .get_ratio_for_symbol(&"XYZ".parse().unwrap())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                WrapperError::SymbolNotConfigured(ref symbol)
                    if *symbol == "XYZ"
            ),
            "expected SymbolNotConfigured for XYZ, got: {error:?}"
        );
    }

    #[test]
    fn check_redeem_within_max_allows_requested_below_max() {
        let wrapped_token = Address::random();
        let requested = U256::from(50u64);
        let max_redeem = U256::from(100u64);

        check_redeem_within_max(wrapped_token, requested, max_redeem)
            .expect("redeem within max must be allowed");
    }

    #[test]
    fn check_redeem_within_max_allows_requested_equal_to_max() {
        let wrapped_token = Address::random();
        let amount = U256::from(100u64);

        check_redeem_within_max(wrapped_token, amount, amount)
            .expect("redeem equal to max must be allowed");
    }

    #[test]
    fn check_redeem_within_max_allows_zero_against_zero_max() {
        let wrapped_token = Address::random();

        check_redeem_within_max(wrapped_token, U256::ZERO, U256::ZERO)
            .expect("zero redeem against zero max must be allowed");
    }

    #[test]
    fn check_redeem_within_max_rejects_requested_above_max() {
        let wrapped_token = Address::random();
        // Mirrors the observed MSTR drift: requested 5.4336 vs maxRedeem 5.0989
        // (scaled to 18 decimals).
        let requested = U256::from(5_433_600_000_000_000_000u128);
        let max_redeem = U256::from(5_098_900_000_000_000_000u128);

        let error = check_redeem_within_max(wrapped_token, requested, max_redeem).unwrap_err();

        assert!(
            matches!(
                error,
                WrapperError::RedeemExceedsMax {
                    wrapped_token: token,
                    requested: req,
                    max_redeem: max,
                } if token == wrapped_token && req == requested && max == max_redeem
            ),
            "expected RedeemExceedsMax carrying the requested/max amounts, got: {error:?}"
        );
    }

    /// A wallet whose provider is backed by a mock transport (Asserter).
    /// `wait_for_block` tests only need the provider; `submit_wrap` tests also
    /// configure canned write results so the approve/deposit flow runs without
    /// a real chain.
    struct MockedWallet<P> {
        address: Address,
        provider: P,
        send_receipt: Option<TransactionReceipt>,
        send_pending_tx_hash: TxHash,
    }

    impl<P: Provider + Clone + Send + Sync + 'static> MockedWallet<P> {
        fn new(address: Address, provider: P) -> Self {
            Self {
                address,
                provider,
                send_receipt: None,
                send_pending_tx_hash: TxHash::ZERO,
            }
        }

        /// Configures the receipt returned by `send` (the approve tx) and the
        /// hash returned by `send_pending` (the deposit tx) so `submit_wrap`
        /// can be driven end to end.
        fn with_write_results(
            mut self,
            send_receipt: TransactionReceipt,
            send_pending_tx_hash: TxHash,
        ) -> Self {
            self.send_receipt = Some(send_receipt);
            self.send_pending_tx_hash = send_pending_tx_hash;
            self
        }
    }

    #[async_trait]
    impl<P: Provider + Clone + Send + Sync + 'static> Evm for MockedWallet<P> {
        type Provider = P;

        fn provider(&self) -> &P {
            &self.provider
        }
    }

    #[async_trait]
    impl<P: Provider + Clone + Send + Sync + 'static> Wallet for MockedWallet<P> {
        fn address(&self) -> Address {
            self.address
        }

        async fn sign_typed_data(
            &self,
            _payload_json: String,
            _expected_digest: alloy::primitives::B256,
        ) -> Result<alloy::primitives::Signature, EvmError> {
            panic!("MockedWallet::sign_typed_data should not be called in wrapper tests")
        }

        async fn send_pending(
            &self,
            _contract: Address,
            _calldata: Bytes,
            _note: &str,
        ) -> Result<TxHash, EvmError> {
            Ok(self.send_pending_tx_hash)
        }

        async fn await_receipt(&self, _tx_hash: TxHash) -> Result<TransactionReceipt, EvmError> {
            Ok(self
                .send_receipt
                .clone()
                .expect("await_receipt called without a configured send_receipt"))
        }

        async fn send(
            &self,
            _contract: Address,
            _calldata: Bytes,
            _note: &str,
        ) -> Result<TransactionReceipt, EvmError> {
            Ok(self
                .send_receipt
                .clone()
                .expect("send called without a configured send_receipt"))
        }
    }

    /// Verifies that `WrapperService::wait_for_block` delegates to
    /// `wait_for_node_sync` correctly: when the node is already at the required
    /// block, the call returns `Ok(())`.
    #[tokio::test]
    async fn wait_for_block_succeeds_when_node_is_at_required_block() {
        let required_block = 42u64;
        let asserter = Asserter::new();
        asserter.push_success(&serde_json::Value::from(required_block));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let wallet = MockedWallet::new(Address::ZERO, provider);
        let service = WrapperService::new(wallet, HashMap::new());

        service
            .wait_for_block(required_block)
            .await
            .expect("node is at required block -- wait_for_block must succeed");
    }

    /// Verifies that `WrapperService::wait_for_block` propagates
    /// `EvmError::NodeBehindRequiredBlock` as `WrapperError::Evm` when the
    /// node is consistently behind, confirming the `?` propagation inside the
    /// service is wired correctly. Exercises all NODE_SYNC_MAX_ATTEMPTS (30)
    /// poll attempts; the zero poll interval keeps the test instant instead of
    /// sleeping through the production one-second cadence.
    #[tokio::test]
    async fn wait_for_block_propagates_node_behind_error_as_wrapper_evm_error() {
        let required_block = 100u64;
        let stale_tip = 1u64;
        let asserter = Asserter::new();
        // Push NODE_SYNC_MAX_ATTEMPTS stale-block responses so all attempts see
        // a stale tip and the error is NodeBehindRequiredBlock.
        for _ in 0..NODE_SYNC_MAX_ATTEMPTS {
            asserter.push_success(&serde_json::Value::from(stale_tip));
        }
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let wallet = MockedWallet::new(Address::ZERO, provider);
        let service =
            WrapperService::with_node_sync_poll_interval(wallet, HashMap::new(), Duration::ZERO);

        let error = service
            .wait_for_block(required_block)
            .await
            .expect_err("stale node must cause wait_for_block to fail");

        assert!(
            matches!(
                error,
                WrapperError::Evm(EvmError::NodeBehindRequiredBlock {
                    required_block: 100,
                    observed_tip: 1,
                    attempts: NODE_SYNC_MAX_ATTEMPTS,
                })
            ),
            "wait_for_block must surface NodeBehindRequiredBlock via WrapperError::Evm, got: {error:?}"
        );
    }

    /// Builds a successful receipt carrying the given tx hash and block number.
    fn receipt_with_block(tx_hash: TxHash, block_number: Option<u64>) -> TransactionReceipt {
        TransactionReceipt {
            inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
                receipt: Receipt {
                    status: true.into(),
                    cumulative_gas_used: 0,
                    logs: vec![],
                },
                logs_bloom: Bloom::default(),
            }),
            transaction_hash: tx_hash,
            transaction_index: Some(0),
            block_hash: None,
            block_number,
            gas_used: 21000,
            effective_gas_price: 1,
            blob_gas_used: None,
            blob_gas_price: None,
            from: Address::ZERO,
            to: Some(Address::ZERO),
            contract_address: None,
        }
    }

    /// Queues the two `eth_call` reads `submit_wrap` performs before the
    /// approval branch: `asset()` (returns the underlying token) and
    /// `allowance()` (returned as zero so the on-demand approval path runs).
    fn push_asset_and_zero_allowance(asserter: &Asserter, underlying_token: Address) {
        asserter.push_success(&<IERC4626::assetCall as SolCall>::abi_encode_returns(
            &underlying_token,
        ));
        asserter.push_success(&<IERC20::allowanceCall as SolCall>::abi_encode_returns(
            &U256::ZERO,
        ));
    }

    /// On the on-demand approval path, `submit_wrap` must wait for the node to
    /// reach the approval block before submitting the deposit, then return the
    /// deposit tx hash once the gate clears.
    #[tokio::test]
    async fn submit_wrap_waits_for_node_sync_before_deposit_after_approval() {
        let underlying = Address::repeat_byte(0x11);
        let wrapped = Address::repeat_byte(0x22);
        let receiver = Address::repeat_byte(0x33);
        let amount = U256::from(1_000u64);
        let approval_block = 7u64;
        let approve_tx = TxHash::repeat_byte(0xaa);
        let deposit_tx = TxHash::repeat_byte(0xbb);

        let asserter = Asserter::new();
        push_asset_and_zero_allowance(&asserter, underlying);
        // Node is already at the approval block, so the gate clears on the
        // first poll and the deposit proceeds.
        asserter.push_success(&serde_json::Value::from(approval_block));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let wallet = MockedWallet::new(Address::ZERO, provider).with_write_results(
            receipt_with_block(approve_tx, Some(approval_block)),
            deposit_tx,
        );
        let service =
            WrapperService::with_node_sync_poll_interval(wallet, HashMap::new(), Duration::ZERO);

        let tx_hash = service
            .submit_wrap(wrapped, amount, receiver)
            .await
            .expect("submit_wrap must succeed once the node-sync gate clears");

        assert_eq!(
            tx_hash, deposit_tx,
            "submit_wrap must return the deposit tx hash after the node-sync gate clears"
        );
    }

    /// If the node never reaches the approval block, `submit_wrap` must surface
    /// `NodeBehindRequiredBlock` and never attempt the deposit -- proving the
    /// deposit is gated on the post-approval node-sync wait.
    #[tokio::test]
    async fn submit_wrap_gates_deposit_when_node_stays_behind_approval_block() {
        let underlying = Address::repeat_byte(0x11);
        let wrapped = Address::repeat_byte(0x22);
        let receiver = Address::repeat_byte(0x33);
        let amount = U256::from(1_000u64);
        let approval_block = 100u64;
        let stale_tip = 1u64;
        let approve_tx = TxHash::repeat_byte(0xaa);

        let asserter = Asserter::new();
        push_asset_and_zero_allowance(&asserter, underlying);
        // Every poll sees a stale tip, so the gate never clears.
        for _ in 0..NODE_SYNC_MAX_ATTEMPTS {
            asserter.push_success(&serde_json::Value::from(stale_tip));
        }

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let wallet = MockedWallet::new(Address::ZERO, provider).with_write_results(
            receipt_with_block(approve_tx, Some(approval_block)),
            TxHash::repeat_byte(0xbb),
        );
        let service =
            WrapperService::with_node_sync_poll_interval(wallet, HashMap::new(), Duration::ZERO);

        let error = service
            .submit_wrap(wrapped, amount, receiver)
            .await
            .expect_err("a node stuck behind the approval block must gate the deposit");

        assert!(
            matches!(
                error,
                WrapperError::Evm(EvmError::NodeBehindRequiredBlock {
                    required_block: 100,
                    observed_tip: 1,
                    attempts: NODE_SYNC_MAX_ATTEMPTS,
                })
            ),
            "submit_wrap must gate the deposit on node sync and surface NodeBehindRequiredBlock, got: {error:?}"
        );
    }

    /// A confirmed approval receipt without a block number must fail hard --
    /// silently skipping the node-sync wait would re-open the stale-RPC race.
    #[tokio::test]
    async fn submit_wrap_fails_when_approval_receipt_has_no_block_number() {
        let underlying = Address::repeat_byte(0x11);
        let wrapped = Address::repeat_byte(0x22);
        let receiver = Address::repeat_byte(0x33);
        let amount = U256::from(1_000u64);
        let approve_tx = TxHash::repeat_byte(0xaa);

        let asserter = Asserter::new();
        push_asset_and_zero_allowance(&asserter, underlying);

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let wallet = MockedWallet::new(Address::ZERO, provider).with_write_results(
            receipt_with_block(approve_tx, None),
            TxHash::repeat_byte(0xbb),
        );
        let service =
            WrapperService::with_node_sync_poll_interval(wallet, HashMap::new(), Duration::ZERO);

        let error = service
            .submit_wrap(wrapped, amount, receiver)
            .await
            .expect_err("a blockless approval receipt must fail submit_wrap");

        assert!(
            matches!(error, WrapperError::MissingBlockNumber { tx_hash } if tx_hash == approve_tx),
            "submit_wrap must fail with MissingBlockNumber when the approval receipt has no block, got: {error:?}"
        );
    }

    /// Builds a confirmed redeem receipt at block 7 whose only log is the
    /// vault's `Withdraw` event for `assets` underlying tokens.
    /// The wallet the redeem pays out to in the receipts below.
    const RECEIVER: Address = Address::repeat_byte(0x77);

    fn receipt_log(address: Address, data: alloy::primitives::LogData) -> Log {
        Log {
            inner: PrimitiveLog { address, data },
            transaction_hash: None,
            transaction_index: None,
            block_hash: None,
            block_number: None,
            block_timestamp: None,
            log_index: None,
            removed: false,
        }
    }

    /// A confirmed redeem receipt at block 7: the vault's `Withdraw` for
    /// `assets` to [`RECEIVER`], plus, when given, an ERC-20 `Transfer` of
    /// `(token, value)` to the same receiver, the way a real redeem pays out.
    fn unwrap_receipt(
        tx_hash: TxHash,
        wrapped_token: Address,
        assets: U256,
        transfer: Option<(Address, U256)>,
    ) -> TransactionReceipt {
        let withdraw = IERC4626::Withdraw {
            sender: Address::ZERO,
            receiver: RECEIVER,
            owner: Address::ZERO,
            assets,
            shares: assets,
        };
        let mut logs = vec![receipt_log(wrapped_token, withdraw.encode_log_data())];
        if let Some((token, value)) = transfer {
            let transfer = IERC20::Transfer {
                from: wrapped_token,
                to: RECEIVER,
                value,
            };
            logs.push(receipt_log(token, transfer.encode_log_data()));
        }
        let mut receipt = receipt_with_block(tx_hash, Some(7));
        receipt.inner = ReceiptEnvelope::Eip1559(ReceiptWithBloom {
            receipt: Receipt {
                status: true.into(),
                cumulative_gas_used: 0,
                logs,
            },
            logs_bloom: Bloom::default(),
        });
        receipt
    }

    /// `confirm_unwrap` returns the token the vault itself reports as its
    /// `asset()`, typed as the attested `UnwrappedToken`, next to the amount
    /// from the `Withdraw` log.
    #[tokio::test]
    async fn confirm_unwrap_attests_the_vaults_asset_as_the_unwrapped_token() {
        let equity = test_equity();
        let tx_hash = TxHash::random();
        let assets = U256::from(5_000_000_000_000_000_000_u128);
        let asserter = Asserter::new();
        asserter.push_success(&<IERC4626::assetCall as SolCall>::abi_encode_returns(
            &equity.underlying,
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let wallet = MockedWallet::new(Address::ZERO, provider).with_write_results(
            unwrap_receipt(
                tx_hash,
                equity.derivative,
                assets,
                Some((equity.underlying, assets)),
            ),
            tx_hash,
        );
        let service = WrapperService::new(
            wallet,
            HashMap::from([(Symbol::new("AAPL").unwrap(), equity)]),
        );

        let confirmation = service
            .confirm_unwrap(equity.derivative, tx_hash)
            .await
            .unwrap();

        assert_eq!(confirmation.token.address(), equity.underlying);
        assert_eq!(confirmation.assets, assets);
        assert_eq!(confirmation.block, 7);
    }

    /// The receipt must prove the redeem paid out: a `Transfer` of the vault's
    /// `asset()` to the receiver for the withdrawn amount. Without it, or with
    /// another token or amount, the unwrap is refused rather than inferred.
    #[tokio::test]
    async fn confirm_unwrap_refuses_a_receipt_that_does_not_show_the_underlying_transfer() {
        let equity = test_equity();
        let assets = U256::from(5_000_000_000_000_000_000_u128);
        let other_token = Address::random();

        for transfer in [
            None,
            Some((other_token, assets)),
            Some((equity.underlying, assets - U256::from(1))),
        ] {
            let tx_hash = TxHash::random();
            let asserter = Asserter::new();
            asserter.push_success(&<IERC4626::assetCall as SolCall>::abi_encode_returns(
                &equity.underlying,
            ));
            let provider = ProviderBuilder::new().connect_mocked_client(asserter);
            let wallet = MockedWallet::new(Address::ZERO, provider).with_write_results(
                unwrap_receipt(tx_hash, equity.derivative, assets, transfer),
                tx_hash,
            );
            let service = WrapperService::new(
                wallet,
                HashMap::from([(Symbol::new("AAPL").unwrap(), equity)]),
            );

            let error = service
                .confirm_unwrap(equity.derivative, tx_hash)
                .await
                .unwrap_err();

            assert!(
                matches!(
                    error,
                    WrapperError::MissingUnderlyingTransfer { asset, receiver, assets: amount, .. }
                        if asset == equity.underlying && receiver == RECEIVER && amount == assets
                ),
                "expected MissingUnderlyingTransfer for {transfer:?}, got: {error:?}"
            );
        }
    }

    /// A transport serving the vault's `asset()` by block tag: `pinned` for an
    /// `eth_call` at `pinned_block`, `latest` for any other tag.
    #[derive(Clone)]
    struct AssetByBlockTransport {
        pinned_block: u64,
        pinned: Address,
        latest: Address,
    }

    impl Service<RequestPacket> for AssetByBlockTransport {
        type Response = ResponsePacket;
        type Error = TransportError;
        type Future = TransportFut<'static>;

        fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), TransportError>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: RequestPacket) -> Self::Future {
            let RequestPacket::Single(request) = request else {
                panic!("AssetByBlockTransport serves single requests only");
            };
            assert_eq!(request.method(), "eth_call");
            let params: Vec<serde_json::Value> =
                serde_json::from_str(request.params().unwrap().get()).unwrap();
            let pinned_tag = serde_json::Value::from(format!("{:#x}", self.pinned_block));
            let asset = if params.get(1) == Some(&pinned_tag) {
                self.pinned
            } else {
                self.latest
            };
            let payload = serde_json::to_string(&Bytes::from(
                <IERC4626::assetCall as SolCall>::abi_encode_returns(&asset),
            ))
            .unwrap();
            let response = Response {
                id: request.id().clone(),
                payload: ResponsePayload::Success(RawValue::from_string(payload).unwrap()),
            };

            Box::pin(async move { Ok(ResponsePacket::Single(response)) })
        }
    }

    /// The redeem delivered the vault's `asset()` as of the receipt block, so
    /// the attestation reads that block rather than whatever the vault reports
    /// later.
    #[tokio::test]
    async fn confirm_unwrap_reads_the_vaults_asset_at_the_redeem_block() {
        let equity = test_equity();
        let tx_hash = TxHash::random();
        let assets = U256::from(5_000_000_000_000_000_000_u128);
        let transport = AssetByBlockTransport {
            pinned_block: 7,
            pinned: equity.underlying,
            latest: Address::random(),
        };
        let provider = ProviderBuilder::new().connect_client(RpcClient::new(transport, true));
        let wallet = MockedWallet::new(Address::ZERO, provider).with_write_results(
            unwrap_receipt(
                tx_hash,
                equity.derivative,
                assets,
                Some((equity.underlying, assets)),
            ),
            tx_hash,
        );
        let service = WrapperService::new(
            wallet,
            HashMap::from([(Symbol::new("AAPL").unwrap(), equity)]),
        );

        let confirmation = service
            .confirm_unwrap(equity.derivative, tx_hash)
            .await
            .unwrap();

        assert_eq!(confirmation.token.address(), equity.underlying);
    }

    #[tokio::test]
    async fn attest_underlying_returns_the_configured_underlying_when_the_vault_agrees() {
        let symbol = Symbol::new("AAPL").unwrap();
        let equity = test_equity();
        let asserter = Asserter::new();
        asserter.push_success(&<IERC4626::assetCall as SolCall>::abi_encode_returns(
            &equity.underlying,
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let service = WrapperService::new(
            MockedWallet::new(Address::ZERO, provider),
            HashMap::from([(symbol.clone(), equity)]),
        );

        let token = service.attest_underlying(&symbol).await.unwrap();

        assert_eq!(token.address(), equity.underlying);
    }

    /// A vault whose `asset()` is not the configured underlying is config
    /// drift: the attestation refuses rather than returning either address.
    #[tokio::test]
    async fn attest_underlying_refuses_a_vault_whose_asset_differs_from_config() {
        let symbol = Symbol::new("AAPL").unwrap();
        let equity = test_equity();
        let other = Address::random();
        let asserter = Asserter::new();
        asserter.push_success(&<IERC4626::assetCall as SolCall>::abi_encode_returns(
            &other,
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let service = WrapperService::new(
            MockedWallet::new(Address::ZERO, provider),
            HashMap::from([(symbol.clone(), equity)]),
        );

        let error = service.attest_underlying(&symbol).await.unwrap_err();

        assert!(
            matches!(
                error,
                WrapperError::VaultAssetMismatch { vault, configured, attested, .. }
                    if vault == equity.derivative
                        && configured == equity.underlying
                        && attested == other
            ),
            "expected VaultAssetMismatch, got: {error:?}"
        );
    }
}
