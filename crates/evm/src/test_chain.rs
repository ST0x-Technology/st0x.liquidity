//! Anvil-based test chain infrastructure for integration and e2e tests.
//!
//! Provides [`AnvilTestChain`] -- a local Anvil instance with USDC deployed
//! at the canonical Base address, owner and taker accounts with funded
//! balances, and helpers for manipulating EVM storage directly.

use alloy::network::EthereumWallet;
use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::{Address, B256, U256, address, keccak256, utils::parse_units};
use alloy::providers::ext::AnvilApi as _;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;

sol!(
    #![sol(all_derives = true, rpc)]
    DeployableERC20,
    "../../lib/rain.orderbook/out/ArbTest.sol/Token.json"
);

sol!(
    #![sol(all_derives = true, rpc)]
    IERC20,
    "../../lib/forge-std/out/IERC20.sol/IERC20.json"
);

/// Base chain USDC address.
pub const USDC_BASE: Address = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");

/// OpenZeppelin ERC20 `_balances` mapping storage slot.
///
/// When USDC is deployed fresh via `DeployableERC20` bytecode (not forked
/// from Circle's FiatTokenV2), the balances mapping lives at slot 0
/// (standard OpenZeppelin ERC20 layout) instead of slot 9.
const USDC_BALANCES_SLOT: u8 = 0;
const TOTAL_SUPPLY_SLOT: u8 = 2;

const EVM_WORD_SIZE: usize = 32;
const EVM_ADDRESS_SIZE: usize = 20;
const ABI_ADDRESS_OFFSET: usize = EVM_WORD_SIZE - EVM_ADDRESS_SIZE;

/// A local Anvil chain with USDC deployed at the canonical `USDC_BASE`
/// address. Provides owner and taker accounts with funded ETH and USDC
/// balances.
pub struct AnvilTestChain<P> {
    anvil: AnvilInstance,
    pub provider: P,
    pub owner: Address,
    pub owner_key: B256,
    pub taker: Address,
    pub taker_provider: P,
}

impl AnvilTestChain<()> {
    /// Spawns a fresh Anvil instance with auto-mining (1s block time),
    /// deploys USDC at `USDC_BASE`, and funds owner + taker accounts.
    pub async fn start() -> anyhow::Result<AnvilTestChain<impl Provider + Clone>> {
        let anvil = Anvil::new().block_time(1).spawn();

        let key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let signer = PrivateKeySigner::from_bytes(&key)?;
        let owner = signer.address();
        let wallet = EthereumWallet::from(signer);

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(&anvil.endpoint())
            .await?;

        deploy_usdc_at_base(&provider, owner).await?;

        let taker_key = B256::from_slice(&anvil.keys()[1].to_bytes());
        let taker_signer = PrivateKeySigner::from_bytes(&taker_key)?;
        let taker = taker_signer.address();
        let taker_wallet = EthereumWallet::from(taker_signer);

        let taker_provider = ProviderBuilder::new()
            .wallet(taker_wallet)
            .connect(&anvil.endpoint())
            .await?;

        let hundred_eth: U256 = parse_units("100", 18)?.into();
        provider.anvil_set_balance(taker, hundred_eth).await?;

        let million_usdc: U256 = parse_units("1000000", 6)?.into();
        mint_usdc(&provider, taker, million_usdc).await?;

        Ok(AnvilTestChain {
            anvil,
            provider,
            owner,
            owner_key: key,
            taker,
            taker_provider,
        })
    }
}

impl<P: Provider + Clone> AnvilTestChain<P> {
    /// Returns the HTTP endpoint URL for the Anvil node.
    pub fn endpoint(&self) -> String {
        self.anvil.endpoint()
    }

    /// Returns the WebSocket endpoint URL for the Anvil node.
    pub fn ws_endpoint(&self) -> anyhow::Result<url::Url> {
        Ok(self.anvil.ws_endpoint().parse()?)
    }

    /// Mines `count` empty blocks.
    pub async fn mine_blocks(&self, count: u64) -> anyhow::Result<()> {
        for _ in 0..count {
            self.provider.anvil_mine(Some(1), None).await?;
        }
        Ok(())
    }

    /// Sets a recipient's USDC balance by writing directly to the
    /// OpenZeppelin ERC20 `_balances` storage slot.
    pub async fn mint_usdc(&self, recipient: Address, amount: U256) -> anyhow::Result<()> {
        mint_usdc(&self.provider, recipient, amount).await
    }

    /// Deploys `anvil_set_code` to place arbitrary bytecode at an address.
    pub async fn set_code(
        &self,
        address: Address,
        bytecode: alloy::primitives::Bytes,
    ) -> anyhow::Result<()> {
        self.provider.anvil_set_code(address, bytecode).await?;
        Ok(())
    }
}

/// Computes the EVM storage slot for `mapping(address => ...)` at
/// `base_slot`. Solidity stores `mapping[key]` at
/// `keccak256(abi_encode(key, base_slot))`.
pub fn evm_mapping_slot(address: Address, base_slot: u8) -> U256 {
    let mut slot_data = [0u8; EVM_WORD_SIZE * 2];
    slot_data[ABI_ADDRESS_OFFSET..EVM_WORD_SIZE].copy_from_slice(address.as_slice());
    slot_data[EVM_WORD_SIZE * 2 - 1] = base_slot;
    keccak256(slot_data).into()
}

/// Encodes a byte slice as a Solidity short string (< 32 bytes).
///
/// Short strings are stored as `data_left_aligned | (len * 2)` in a
/// single 32-byte slot.
pub fn solidity_short_string(value: &[u8]) -> B256 {
    assert!(
        value.len() < EVM_WORD_SIZE,
        "solidity_short_string only supports strings shorter than 32 bytes, \
         got {} bytes",
        value.len()
    );
    let mut bytes = [0u8; EVM_WORD_SIZE];
    bytes[..value.len()].copy_from_slice(value);
    let Ok(double_len) = u8::try_from(value.len() * 2) else {
        unreachable!("value.len() < 32 is asserted above, so len * 2 < 64 fits u8")
    };
    bytes[EVM_WORD_SIZE - 1] = double_len;
    B256::from(bytes)
}

/// Deploys a USDC ERC20 at the canonical `USDC_BASE` address using
/// `anvil_set_code` with `DeployableERC20` bytecode, then initialises
/// OpenZeppelin storage slots (totalSupply, name, symbol, decimals) and
/// gives the owner 1B USDC.
async fn deploy_usdc_at_base<P: Provider>(provider: &P, owner: Address) -> anyhow::Result<()> {
    let total_supply = U256::from(1_000_000_000_000u64);

    provider
        .anvil_set_code(USDC_BASE, DeployableERC20::DEPLOYED_BYTECODE.clone())
        .await?;

    // Slot 2: _totalSupply
    provider
        .anvil_set_storage_at(
            USDC_BASE,
            U256::from(TOTAL_SUPPLY_SLOT),
            total_supply.into(),
        )
        .await?;

    // Slot 3: _name = "USD Coin"
    provider
        .anvil_set_storage_at(USDC_BASE, U256::from(3), solidity_short_string(b"USD Coin"))
        .await?;

    // Slot 4: _symbol = "USDC"
    provider
        .anvil_set_storage_at(USDC_BASE, U256::from(4), solidity_short_string(b"USDC"))
        .await?;

    // Slot 5: _decimals = 6
    provider
        .anvil_set_storage_at(USDC_BASE, U256::from(5), U256::from(6).into())
        .await?;

    // _balances[owner] at slot 0 (OpenZeppelin ERC20 layout)
    let balance_slot = evm_mapping_slot(owner, USDC_BALANCES_SLOT);
    provider
        .anvil_set_storage_at(USDC_BASE, balance_slot, total_supply.into())
        .await?;

    Ok(())
}

/// Mints USDC to `recipient` by adding `amount` to their existing
/// balance and incrementing `totalSupply` (slot 2). Uses
/// `anvil_get_storage_at` to read current values before writing.
async fn mint_usdc<P: Provider>(
    provider: &P,
    recipient: Address,
    amount: U256,
) -> anyhow::Result<()> {
    let balance_slot = evm_mapping_slot(recipient, USDC_BALANCES_SLOT);

    let current_balance: U256 = provider.get_storage_at(USDC_BASE, balance_slot).await?;
    let new_balance = current_balance + amount;

    provider
        .anvil_set_storage_at(USDC_BASE, balance_slot, new_balance.into())
        .await?;

    let current_supply: U256 = provider
        .get_storage_at(USDC_BASE, U256::from(TOTAL_SUPPLY_SLOT))
        .await?;
    let new_supply = current_supply + amount;

    provider
        .anvil_set_storage_at(USDC_BASE, U256::from(TOTAL_SUPPLY_SLOT), new_supply.into())
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, U256, address};
    use alloy::providers::Provider;

    use super::{AnvilTestChain, IERC20, USDC_BASE, evm_mapping_slot, solidity_short_string};

    #[test]
    fn evm_mapping_slot_produces_deterministic_result() {
        let address = address!("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
        let slot_a = evm_mapping_slot(address, 0);
        let slot_b = evm_mapping_slot(address, 0);
        assert_eq!(slot_a, slot_b);
    }

    #[test]
    fn evm_mapping_slot_differs_by_base_slot() {
        let address = Address::random();
        let slot_0 = evm_mapping_slot(address, 0);
        let slot_1 = evm_mapping_slot(address, 1);
        assert_ne!(slot_0, slot_1);
    }

    #[test]
    fn evm_mapping_slot_differs_by_address() {
        let slot_a = evm_mapping_slot(Address::random(), 0);
        let slot_b = evm_mapping_slot(Address::random(), 0);
        assert_ne!(slot_a, slot_b);
    }

    #[test]
    fn solidity_short_string_encodes_usdc() {
        let encoded = solidity_short_string(b"USDC");
        let bytes: [u8; 32] = encoded.0;
        assert_eq!(&bytes[..4], b"USDC");
        assert_eq!(bytes[31], 8); // len("USDC") * 2
        assert_eq!(bytes[4..31], [0u8; 27]);
    }

    #[test]
    fn solidity_short_string_encodes_usd_coin() {
        let encoded = solidity_short_string(b"USD Coin");
        let bytes: [u8; 32] = encoded.0;
        assert_eq!(&bytes[..8], b"USD Coin");
        assert_eq!(bytes[31], 16); // len("USD Coin") * 2
    }

    #[test]
    fn solidity_short_string_encodes_empty() {
        let encoded = solidity_short_string(b"");
        assert_eq!(encoded, B256::ZERO);
    }

    #[test]
    #[should_panic(expected = "shorter than 32 bytes")]
    fn solidity_short_string_rejects_long_input() {
        solidity_short_string(&[b'A'; 32]);
    }

    #[tokio::test]
    async fn anvil_test_chain_starts_and_has_usdc() {
        let chain = AnvilTestChain::start().await.unwrap();

        let usdc = IERC20::new(USDC_BASE, &chain.provider);
        let owner_balance = usdc.balanceOf(chain.owner).call().await.unwrap();
        assert!(owner_balance > U256::ZERO, "Owner should have USDC");

        let taker_balance = usdc.balanceOf(chain.taker).call().await.unwrap();
        assert!(taker_balance > U256::ZERO, "Taker should have USDC");

        // Verify RPC is responsive
        chain.provider.get_block_number().await.unwrap();
    }
}
