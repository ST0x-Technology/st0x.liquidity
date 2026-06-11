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
use tracing::info;

use st0x_evm::{IntoErrorRegistry, OpenChainErrorRegistry, Wallet};
use st0x_execution::Symbol;

use crate::{UnderlyingPerWrapped, WrappedEquity, Wrapper, WrapperError};

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
}

impl<W: Wallet> WrapperService<W> {
    pub fn new(wallet: W, config: HashMap<Symbol, WrappedEquity>) -> Self {
        Self { wallet, config }
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

    async fn to_wrapped(
        &self,
        wrapped_token: Address,
        underlying_amount: U256,
        receiver: Address,
    ) -> Result<(TxHash, U256), WrapperError> {
        let tx_hash = self
            .submit_wrap(wrapped_token, underlying_amount, receiver)
            .await?;

        let actual_shares = self.confirm_wrap(wrapped_token, tx_hash).await?;

        Ok((tx_hash, actual_shares))
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

        let actual_assets = self.confirm_unwrap(wrapped_token, tx_hash).await?;

        Ok((tx_hash, actual_assets))
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

            self.wallet
                .submit::<OpenChainErrorRegistry, _>(
                    underlying_token,
                    IERC20::approveCall {
                        spender: wrapped_token,
                        amount: underlying_amount,
                    },
                    "ERC20 approve for ERC4626 deposit",
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
    ) -> Result<U256, WrapperError> {
        let receipt = self
            .wallet
            .confirm::<OpenChainErrorRegistry>(tx_hash)
            .await?;

        let actual_shares = receipt
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

        Ok(actual_shares)
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
    ) -> Result<U256, WrapperError> {
        let receipt = self
            .wallet
            .confirm::<OpenChainErrorRegistry>(tx_hash)
            .await?;

        let actual_assets = receipt
            .inner
            .logs()
            .iter()
            .filter(|log| log.address() == wrapped_token)
            .find_map(|log| {
                IERC4626::Withdraw::decode_log(log.as_ref())
                    .ok()
                    .map(|event| event.data.assets)
            })
            .ok_or(WrapperError::MissingWithdrawEvent)?;

        Ok(actual_assets)
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
    use alloy::primitives::Bytes;
    use alloy::providers::RootProvider;
    use alloy::rpc::client::RpcClient;
    use alloy::rpc::types::TransactionReceipt;
    use std::sync::Arc;

    use st0x_evm::{Evm, EvmError};

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
                    if symbol.to_string() == "MSFT"
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
                    if symbol.to_string() == "XYZ"
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
}
