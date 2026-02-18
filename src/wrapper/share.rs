//! WrapperService implementation for ERC-4626 token wrapping/unwrapping.

use alloy::primitives::{Address, Bytes, TxHash, U256};
use alloy::sol_types::{SolCall, SolEvent};
use async_trait::async_trait;
use serde::Deserialize;
use std::collections::HashMap;
use tracing::info;

use st0x_evm::Wallet;
use st0x_execution::Symbol;

use super::{UnderlyingPerWrapped, Wrapper, WrapperError};
use crate::bindings::IERC4626;

/// One unit with 18 decimals for ratio queries.
const RATIO_QUERY_AMOUNT: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);

/// Wrapped and unwrapped token addresses for an equity.
#[derive(Debug, Clone, Deserialize)]
pub struct EquityTokenAddresses {
    pub wrapped: Address,
    pub unwrapped: Address,
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
    async fn get_ratio(
        &self,
        wrapped_token: Address,
    ) -> Result<UnderlyingPerWrapped, WrapperError> {
        let vault = IERC4626::new(wrapped_token, self.wallet.provider().clone());
        let assets_per_share = vault.convertToAssets(RATIO_QUERY_AMOUNT).call().await?;

        UnderlyingPerWrapped::new(assets_per_share).map_err(Into::into)
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

        self.get_ratio(addresses.wrapped).await
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
        let calldata = IERC4626::depositCall {
            assets: underlying_amount,
            receiver,
        };

        info!("Sending ERC4626 deposit to {wrapped_token}");
        let receipt = self
            .wallet
            .send(
                wrapped_token,
                Bytes::from(SolCall::abi_encode(&calldata)),
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
        let calldata = IERC4626::redeemCall {
            shares: wrapped_amount,
            receiver,
            owner,
        };

        info!("Sending ERC4626 redeem to {wrapped_token}");
        let receipt = self
            .wallet
            .send(
                wrapped_token,
                Bytes::from(SolCall::abi_encode(&calldata)),
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
