//! WrapperService implementation for ERC-4626 token wrapping/unwrapping.

use alloy::primitives::{Address, TxHash, U256};
use alloy::sol_types::SolEvent;
use async_trait::async_trait;
use serde::Deserialize;
use std::collections::HashMap;
use tracing::info;

use st0x_evm::{IntoErrorRegistry, OpenChainErrorRegistry, Wallet};
use st0x_execution::Symbol;

use super::{UnderlyingPerWrapped, Wrapper, WrapperError};
use crate::bindings::{IERC20, IERC4626};

/// One unit with 18 decimals for ratio queries.
const RATIO_QUERY_AMOUNT: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);

/// Wrapped and unwrapped token addresses for an equity.
#[derive(Debug, Clone, Deserialize)]
pub struct EquityTokenAddresses {
    pub wrapped: Address,
    pub unwrapped: Address,
    pub enabled: bool,
}

/// Service for managing ERC-4626 token wrapping/unwrapping operations.
///
/// Uses the wallet's embedded provider for read-only view calls (ratio
/// queries) and the wallet itself for write transactions (deposit/redeem).
pub(crate) struct WrapperService<W: Wallet> {
    wallet: W,
    config: HashMap<Symbol, EquityTokenAddresses>,
}

impl<W: Wallet> WrapperService<W> {
    pub(crate) fn new(wallet: W, config: HashMap<Symbol, EquityTokenAddresses>) -> Self {
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

    /// Gets the token addresses for a symbol.
    pub(crate) fn lookup_equity(&self, symbol: &Symbol) -> Option<&EquityTokenAddresses> {
        self.config.get(symbol)
    }
}

#[async_trait]
impl<W: Wallet> Wrapper for WrapperService<W> {
    async fn get_ratio_for_symbol(
        &self,
        symbol: &Symbol,
    ) -> Result<UnderlyingPerWrapped, WrapperError> {
        let addresses = self
            .lookup_equity(symbol)
            .ok_or_else(|| WrapperError::SymbolNotConfigured(symbol.clone()))?;

        self.get_ratio::<OpenChainErrorRegistry>(addresses.wrapped)
            .await
    }

    fn lookup_unwrapped(&self, symbol: &Symbol) -> Result<Address, WrapperError> {
        let addresses = self
            .lookup_equity(symbol)
            .ok_or_else(|| WrapperError::SymbolNotConfigured(symbol.clone()))?;

        Ok(addresses.unwrapped)
    }

    fn lookup_wrapped(&self, symbol: &Symbol) -> Result<Address, WrapperError> {
        let addresses = self
            .lookup_equity(symbol)
            .ok_or_else(|| WrapperError::SymbolNotConfigured(symbol.clone()))?;

        Ok(addresses.wrapped)
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

        info!("Sending ERC4626 deposit to {wrapped_token}");
        let receipt = self
            .wallet
            .submit::<OpenChainErrorRegistry, _>(
                wrapped_token,
                IERC4626::depositCall {
                    assets: underlying_amount,
                    receiver,
                },
                "ERC4626 deposit",
            )
            .await?;
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

    async fn to_underlying(
        &self,
        wrapped_token: Address,
        wrapped_amount: U256,
        receiver: Address,
        owner: Address,
    ) -> Result<(TxHash, U256), WrapperError> {
        info!("Sending ERC4626 redeem to {wrapped_token}");
        let receipt = self
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
            .await?;
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
    use crate::test_utils::StubWallet;

    fn test_addresses() -> EquityTokenAddresses {
        EquityTokenAddresses {
            wrapped: Address::random(),
            unwrapped: Address::random(),
            enabled: true,
        }
    }

    fn service_with_symbol(
        symbol: &str,
        addresses: EquityTokenAddresses,
    ) -> WrapperService<impl Wallet> {
        let wallet = StubWallet::stub(Address::random());
        let mut config = HashMap::new();
        config.insert(symbol.parse::<Symbol>().unwrap(), addresses);
        WrapperService::new(wallet, config)
    }

    #[test]
    fn lookup_equity_returns_configured_symbol() {
        let addresses = test_addresses();
        let service = service_with_symbol("tAAPL", addresses.clone());

        let result = service.lookup_equity(&"tAAPL".parse().unwrap());

        assert!(result.is_some());
        assert_eq!(result.unwrap().wrapped, addresses.wrapped);
        assert_eq!(result.unwrap().unwrapped, addresses.unwrapped);
    }

    #[test]
    fn lookup_equity_returns_none_for_unconfigured() {
        let service = service_with_symbol("tAAPL", test_addresses());

        let result = service.lookup_equity(&"tTSLA".parse().unwrap());

        assert!(result.is_none());
    }

    #[test]
    fn lookup_wrapped_returns_vault_address() {
        let addresses = test_addresses();
        let expected = addresses.wrapped;
        let service = service_with_symbol("tAAPL", addresses);

        let result = service.lookup_wrapped(&"tAAPL".parse().unwrap()).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn lookup_unwrapped_returns_underlying_address() {
        let addresses = test_addresses();
        let expected = addresses.unwrapped;
        let service = service_with_symbol("tAAPL", addresses);

        let result = service.lookup_unwrapped(&"tAAPL".parse().unwrap()).unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn lookup_wrapped_errors_on_unconfigured_symbol() {
        let service = service_with_symbol("tAAPL", test_addresses());

        let error = service
            .lookup_wrapped(&"tMSFT".parse().unwrap())
            .unwrap_err();

        assert!(
            matches!(
                error,
                WrapperError::SymbolNotConfigured(ref symbol)
                    if symbol.to_string() == "tMSFT"
            ),
            "expected SymbolNotConfigured for tMSFT, got: {error:?}"
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
        let service = service_with_symbol("tAAPL", test_addresses());

        let error = service
            .get_ratio_for_symbol(&"tXYZ".parse().unwrap())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                WrapperError::SymbolNotConfigured(ref symbol)
                    if symbol.to_string() == "tXYZ"
            ),
            "expected SymbolNotConfigured for tXYZ, got: {error:?}"
        );
    }

    #[test]
    fn enabled_is_required() {
        let json = r#"{"wrapped": "0x1111111111111111111111111111111111111111", "unwrapped": "0x2222222222222222222222222222222222222222"}"#;
        let result = serde_json::from_str::<EquityTokenAddresses>(json);
        assert!(
            result.is_err(),
            "omitting `enabled` should fail deserialization"
        );
    }
}
