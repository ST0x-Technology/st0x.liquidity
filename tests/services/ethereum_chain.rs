//! Ethereum mainnet fork infrastructure for e2e USDC rebalancing tests.
//!
//! Forks Ethereum mainnet via Anvil so CCTP contracts (`TokenMessengerV2`,
//! `MessageTransmitterV2`) and Circle's USDC are live at their production
//! addresses. Uses Anvil cheat codes to mint USDC for test accounts.

use alloy::network::EthereumWallet;
use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::{Address, B256, U256, keccak256, utils::parse_units};
use alloy::providers::ext::AnvilApi as _;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use url::Url;

/// USDC on Ethereum mainnet.
pub const USDC_ETHEREUM: Address =
    alloy::primitives::address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");

/// Circle FiatTokenV2 `balances` mapping storage slot (same as Base).
const USDC_BALANCES_SLOT: u8 = 9;

/// A forked Ethereum mainnet running locally via Anvil.
///
/// CCTP contracts (`TokenMessengerV2`, `MessageTransmitterV2`) are live
/// at their production addresses for cross-chain USDC bridging tests.
pub struct EthereumChain<P> {
    anvil: AnvilInstance,
    pub provider: P,
    pub owner: Address,
    pub owner_key: B256,
}

impl EthereumChain<()> {
    /// Forks Ethereum mainnet using the given RPC URL, mints initial USDC
    /// to the first Anvil account.
    pub async fn start(rpc_url: &str) -> anyhow::Result<EthereumChain<impl Provider + Clone>> {
        let anvil = Anvil::new().fork(rpc_url).spawn();

        let key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let signer = PrivateKeySigner::from_bytes(&key)?;
        let owner = signer.address();
        let wallet = EthereumWallet::from(signer);

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(&anvil.endpoint())
            .await?;

        // Mint 1M USDC to owner via storage slot manipulation
        let million_usdc: U256 = parse_units("1000000", 6)?.into();
        mint_usdc_ethereum(&provider, owner, million_usdc).await?;

        Ok(EthereumChain {
            anvil,
            provider,
            owner,
            owner_key: key,
        })
    }
}

impl<P: Provider + Clone> EthereumChain<P> {
    /// Returns the WebSocket endpoint URL for the Anvil node.
    pub fn ws_endpoint(&self) -> anyhow::Result<Url> {
        Ok(self.anvil.ws_endpoint().parse()?)
    }

    /// Returns the HTTP endpoint URL for the Anvil node.
    pub fn endpoint(&self) -> String {
        self.anvil.endpoint()
    }

    /// Sets `recipient`'s USDC balance on Ethereum.
    pub async fn mint_usdc(&self, recipient: Address, amount: U256) -> anyhow::Result<()> {
        mint_usdc_ethereum(&self.provider, recipient, amount).await
    }
}

/// Sets `recipient`'s USDC balance on Ethereum by writing directly to
/// Circle's FiatToken `balances` storage slot.
async fn mint_usdc_ethereum<P: Provider>(
    provider: &P,
    recipient: Address,
    amount: U256,
) -> anyhow::Result<()> {
    let mut slot_data = [0u8; 64];
    slot_data[12..32].copy_from_slice(recipient.as_slice());
    slot_data[63] = USDC_BALANCES_SLOT;
    let balance_slot = keccak256(slot_data);

    provider
        .anvil_set_storage_at(USDC_ETHEREUM, balance_slot.into(), amount.into())
        .await?;

    Ok(())
}
