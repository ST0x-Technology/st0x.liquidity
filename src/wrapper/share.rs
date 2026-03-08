//! WrapperService implementation for ERC-4626 token wrapping/unwrapping.

use alloy::primitives::{Address, TxHash, U256};
use alloy::sol_types::SolEvent;
use async_trait::async_trait;
use std::collections::HashMap;
use tracing::{info, warn};

use st0x_evm::{IntoErrorRegistry, OpenChainErrorRegistry, Wallet};
use st0x_execution::Symbol;

use super::{UnderlyingPerWrapped, Wrapper, WrapperError};
use crate::bindings::{IERC20, IERC4626};
use crate::config::EquityAssetConfig;

/// One unit with 18 decimals for ratio queries.
const RATIO_QUERY_AMOUNT: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);

/// Service for managing ERC-4626 token wrapping/unwrapping operations.
///
/// Uses the wallet's embedded provider for read-only view calls (ratio
/// queries) and the wallet itself for write transactions (deposit/redeem).
pub(crate) struct WrapperService<W: Wallet> {
    wallet: W,
    config: HashMap<Symbol, EquityAssetConfig>,
}

impl<W: Wallet> WrapperService<W> {
    pub(crate) fn new(wallet: W, config: HashMap<Symbol, EquityAssetConfig>) -> Self {
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

    /// Gets the asset config for a symbol.
    pub(crate) fn lookup_equity(&self, symbol: &Symbol) -> Option<&EquityAssetConfig> {
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

        self.get_ratio::<OpenChainErrorRegistry>(asset.tokenized_equity_derivative)
            .await
    }

    fn lookup_underlying(&self, symbol: &Symbol) -> Result<Address, WrapperError> {
        let asset = self
            .lookup_equity(symbol)
            .ok_or_else(|| WrapperError::SymbolNotConfigured(symbol.clone()))?;

        Ok(asset.tokenized_equity)
    }

    fn lookup_derivative(&self, symbol: &Symbol) -> Result<Address, WrapperError> {
        let asset = self
            .lookup_equity(symbol)
            .ok_or_else(|| WrapperError::SymbolNotConfigured(symbol.clone()))?;

        Ok(asset.tokenized_equity_derivative)
    }

    async fn to_wrapped(
        &self,
        wrapped_token: Address,
        underlying_amount: U256,
        receiver: Address,
    ) -> Result<(TxHash, U256), WrapperError> {
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

        let balance_before: U256 = self
            .wallet
            .call::<OpenChainErrorRegistry, _>(
                wrapped_token,
                IERC20::balanceOfCall { account: receiver },
            )
            .await?;

        info!("Sending ERC4626 deposit to {wrapped_token}");
        match self
            .wallet
            .submit::<OpenChainErrorRegistry, _>(
                wrapped_token,
                IERC4626::depositCall {
                    assets: underlying_amount,
                    receiver,
                },
                "ERC4626 deposit",
            )
            .await
        {
            Ok(receipt) => {
                let tx_hash = receipt.transaction_hash;

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

                Ok((tx_hash, actual_shares))
            }

            Err(submit_error) => {
                warn!(
                    %submit_error,
                    "ERC4626 deposit receipt failed, \
                     falling back to balance diff"
                );

                let balance_after: U256 = self
                    .wallet
                    .call::<OpenChainErrorRegistry, _>(
                        wrapped_token,
                        IERC20::balanceOfCall { account: receiver },
                    )
                    .await?;

                let actual_shares = balance_after.saturating_sub(balance_before);

                if actual_shares.is_zero() {
                    warn!("Balance unchanged after deposit timeout");
                    return Err(submit_error.into());
                }

                info!(
                    %actual_shares,
                    %balance_before,
                    %balance_after,
                    "Recovered deposit amount from balance diff"
                );

                // No tx_hash available since the receipt failed,
                // use a zero hash as sentinel
                Ok((TxHash::ZERO, actual_shares))
            }
        }
    }

    async fn to_underlying(
        &self,
        wrapped_token: Address,
        wrapped_amount: U256,
        receiver: Address,
        owner: Address,
    ) -> Result<(TxHash, U256), WrapperError> {
        let underlying_token: Address = self
            .wallet
            .call::<OpenChainErrorRegistry, _>(wrapped_token, IERC4626::assetCall {})
            .await?;

        let balance_before: U256 = self
            .wallet
            .call::<OpenChainErrorRegistry, _>(
                underlying_token,
                IERC20::balanceOfCall { account: receiver },
            )
            .await?;

        info!("Sending ERC4626 redeem to {wrapped_token}");
        match self
            .wallet
            .submit::<OpenChainErrorRegistry, _>(
                wrapped_token,
                IERC4626::redeemCall {
                    shares: wrapped_amount,
                    receiver,
                    owner,
                },
                "ERC4626 redeem",
            )
            .await
        {
            Ok(receipt) => {
                let tx_hash = receipt.transaction_hash;

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

                Ok((tx_hash, actual_assets))
            }

            Err(submit_error) => {
                warn!(
                    %submit_error,
                    "ERC4626 redeem receipt failed, \
                     falling back to balance diff"
                );

                let balance_after: U256 = self
                    .wallet
                    .call::<OpenChainErrorRegistry, _>(
                        underlying_token,
                        IERC20::balanceOfCall { account: receiver },
                    )
                    .await?;

                let actual_assets = balance_after.saturating_sub(balance_before);

                if actual_assets.is_zero() {
                    warn!("Balance unchanged after redeem timeout");
                    return Err(submit_error.into());
                }

                info!(
                    %actual_assets,
                    %balance_before,
                    %balance_after,
                    "Recovered redeem amount from balance diff"
                );

                Ok((TxHash::ZERO, actual_assets))
            }
        }
    }

    fn owner(&self) -> Address {
        self.wallet.address()
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;
    use std::collections::HashMap;

    use st0x_execution::Symbol;

    use super::*;
    use crate::config::{EquityAssetConfig, OperationMode};
    use crate::test_utils::StubWallet;

    fn test_asset_config() -> EquityAssetConfig {
        EquityAssetConfig {
            tokenized_equity: Address::random(),
            tokenized_equity_derivative: Address::random(),
            vault_id: None,
            trading: OperationMode::Enabled,
            rebalancing: OperationMode::Disabled,
            operational_limit: None,
        }
    }

    fn service_with_symbol(symbol: &str, config: EquityAssetConfig) -> WrapperService<impl Wallet> {
        let wallet = StubWallet::stub(Address::random());
        let mut equities = HashMap::new();
        equities.insert(symbol.parse::<Symbol>().unwrap(), config);
        WrapperService::new(wallet, equities)
    }

    #[test]
    fn lookup_equity_returns_configured_symbol() {
        let config = test_asset_config();
        let expected_tokenized = config.tokenized_equity;
        let expected_trd = config.tokenized_equity_derivative;
        let service = service_with_symbol("AAPL", config);

        let result = service.lookup_equity(&"AAPL".parse().unwrap());

        assert!(result.is_some());
        assert_eq!(result.unwrap().tokenized_equity_derivative, expected_trd);
        assert_eq!(result.unwrap().tokenized_equity, expected_tokenized);
    }

    #[test]
    fn lookup_equity_returns_none_for_unconfigured() {
        let service = service_with_symbol("AAPL", test_asset_config());

        let result = service.lookup_equity(&"TSLA".parse().unwrap());

        assert!(result.is_none());
    }

    #[test]
    fn lookup_derivative_returns_vault_address() {
        let config = test_asset_config();
        let expected = config.tokenized_equity_derivative;
        let service = service_with_symbol("AAPL", config);

        let result = service.lookup_derivative(&"AAPL".parse().unwrap()).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn lookup_underlying_returns_underlying_address() {
        let config = test_asset_config();
        let expected = config.tokenized_equity;
        let service = service_with_symbol("AAPL", config);

        let result = service.lookup_underlying(&"AAPL".parse().unwrap()).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn lookup_derivative_errors_on_unconfigured_symbol() {
        let service = service_with_symbol("AAPL", test_asset_config());

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
        let service = service_with_symbol("AAPL", test_asset_config());

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
}
