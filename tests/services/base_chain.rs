//! Base chain fork infrastructure for e2e testing.
//!
//! Forks Base mainnet via Anvil so all Rain/Raindex contracts are live at
//! their production addresses, then uses Anvil cheat codes to mint tokens
//! for test accounts.

use alloy::network::EthereumWallet;
use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::{Address, B256, U256, address, keccak256, utils::parse_units};
use alloy::providers::ext::AnvilApi as _;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;

pub use st0x_hedge::USDC_BASE;
pub use st0x_hedge::bindings::{DeployableERC20, IERC20};

/// Raindex OrderBook on Base mainnet.
const ORDERBOOK_BASE: Address = address!("52CEB8eBEf648744fFDDE89F7Bc9C3aC35944775");

/// Circle FiatTokenV2 `balances` mapping storage slot.
const USDC_BALANCES_SLOT: u8 = 9;

/// A forked Base chain running locally via Anvil, with the real Raindex
/// OrderBook and all Rain infrastructure already deployed.
pub struct BaseChain<P> {
    _anvil: AnvilInstance,
    pub provider: P,
    pub owner: Address,
    pub orderbook_addr: Address,
}

impl BaseChain<()> {
    /// Forks Base mainnet and mints initial USDC to the owner account.
    ///
    /// Uses `BASE_RPC_URL` env var if set, otherwise defaults to the
    /// public Base RPC endpoint.
    pub async fn start() -> anyhow::Result<BaseChain<impl Provider + Clone>> {
        let rpc_url = std::env::var("BASE_RPC_URL")
            .unwrap_or_else(|_| "https://mainnet.base.org".to_string());

        let anvil = Anvil::new().fork(rpc_url).spawn();

        let key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let signer = PrivateKeySigner::from_bytes(&key)?;
        let owner = signer.address();
        let wallet = EthereumWallet::from(signer);

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(&anvil.endpoint())
            .await?;

        let chain = BaseChain {
            _anvil: anvil,
            provider,
            owner,
            orderbook_addr: ORDERBOOK_BASE,
        };

        // Mint 1M USDC to owner
        let million_usdc: U256 = parse_units("1000000", 6)?.into();
        chain.mint_usdc(chain.owner, million_usdc).await?;

        Ok(chain)
    }
}

impl<P: Provider + Clone> BaseChain<P> {
    /// Sets `recipient`'s USDC balance to `amount` (6-decimal raw units)
    /// by writing directly to Circle's FiatToken `balances` storage slot.
    pub async fn mint_usdc(&self, recipient: Address, amount: U256) -> anyhow::Result<()> {
        let mut slot_data = [0u8; 64];
        slot_data[12..32].copy_from_slice(recipient.as_slice());
        slot_data[63] = USDC_BALANCES_SLOT;
        let balance_slot = keccak256(slot_data);

        self.provider
            .anvil_set_storage_at(USDC_BASE, balance_slot.into(), amount.into())
            .await?;

        Ok(())
    }

    /// Deploys a test ERC20 with `t{symbol}` name, 18 decimals, and 1M
    /// tokens minted to the owner.
    pub async fn deploy_equity_token(&self, symbol: &str) -> anyhow::Result<Address> {
        let name = format!("t{symbol}");
        let supply: U256 = parse_units("1000000", 18)?.into();

        let token =
            DeployableERC20::deploy(&self.provider, name.clone(), name, 18, self.owner, supply)
                .await?;

        Ok(*token.address())
    }
}
