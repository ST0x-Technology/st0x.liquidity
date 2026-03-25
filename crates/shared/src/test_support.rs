//! Shared Anvil test infrastructure for Rain OrderBook integration tests.
//!
//! Provides [`RainOrderBook`] -- a local Anvil instance with the full Rain
//! expression stack (Interpreter, Store, Parser, Deployer) and OrderBook
//! deployed, ready for placing and taking orders in tests.
//!
//! Builds on top of [`AnvilTestChain`] from `st0x-evm` which handles
//! Anvil spawning, USDC deployment, and account funding.

use alloy::primitives::{Address, B256, Bytes, U256, address};
use alloy::providers::Provider;
use alloy::providers::ext::AnvilApi as _;

use st0x_evm::test_chain::AnvilTestChain;

use crate::bindings::{
    DeployableERC20, Deployer, IOrderBookV6, Interpreter, OrderBook, Parser, Store as RainStore,
    TOFUTokenDecimals,
};

/// Address where [`TOFUTokenDecimals`] bytecode is placed via `anvil_set_code`.
///
/// This is a precompile-like contract used by the Rain expression stack
/// to determine token decimals from addresses at expression evaluation time.
const TOFU_TOKEN_DECIMALS: Address = address!("0xF66761F6b5F58202998D6Cd944C81b22Dc6d4f1E");

/// Anvil chain with the Rain OrderBook and expression stack deployed.
///
/// Composes [`AnvilTestChain`] (USDC + accounts) with Rain-specific
/// contracts (OrderBook, Interpreter, Store, Parser, Deployer).
pub struct RainOrderBook<P> {
    pub chain: AnvilTestChain<P>,
    pub orderbook: Address,
    pub deployer: Address,
    pub interpreter: Address,
    pub store: Address,
}

/// Result of adding an order via [`RainOrderBook::add_order`].
pub struct AddOrderResult {
    pub block: u64,
    pub order_hash: B256,
    pub order: IOrderBookV6::OrderV4,
}

impl RainOrderBook<()> {
    /// Deploys the full Rain stack on a fresh Anvil instance.
    ///
    /// This is the primary entry point for test setup. After calling this,
    /// you have a fully functional OrderBook with expression compilation
    /// support, USDC at the canonical Base address, and funded accounts.
    pub async fn start() -> anyhow::Result<RainOrderBook<impl Provider + Clone>> {
        let chain = AnvilTestChain::start().await?;

        chain
            .set_code(
                TOFU_TOKEN_DECIMALS,
                TOFUTokenDecimals::DEPLOYED_BYTECODE.clone(),
            )
            .await?;

        let interpreter = Interpreter::deploy(&chain.provider).await?;
        let store = RainStore::deploy(&chain.provider).await?;
        let parser = Parser::deploy(&chain.provider).await?;

        let deployer = Deployer::deploy(
            &chain.provider,
            Deployer::RainterpreterExpressionDeployerConstructionConfigV2 {
                interpreter: *interpreter.address(),
                store: *store.address(),
                parser: *parser.address(),
            },
        )
        .await?;

        let orderbook = OrderBook::deploy(&chain.provider).await?;

        Ok(RainOrderBook {
            orderbook: *orderbook.address(),
            deployer: *deployer.address(),
            interpreter: *interpreter.address(),
            store: *store.address(),
            chain,
        })
    }
}

impl<P: Provider + Clone> RainOrderBook<P> {
    /// Compiles a Rainlang expression into bytecode using the on-chain Parser.
    pub async fn compile_expression(&self, expression: &str) -> anyhow::Result<Bytes> {
        let deployer_instance =
            Deployer::DeployerInstance::new(self.deployer, &self.chain.provider);

        let parsed = deployer_instance
            .parse2(Bytes::copy_from_slice(expression.as_bytes()))
            .call()
            .await?
            .0;

        Ok(Bytes::from(parsed))
    }

    /// Deploys an ERC20 token with the given parameters. Returns the token address.
    pub async fn deploy_erc20(
        &self,
        name: &str,
        symbol: &str,
        decimals: u8,
        supply: U256,
    ) -> anyhow::Result<Address> {
        let token = DeployableERC20::deploy(
            &self.chain.provider,
            name.to_string(),
            symbol.to_string(),
            decimals,
            self.chain.owner,
            supply,
        )
        .await?;

        Ok(*token.address())
    }

    /// Adds an order to the OrderBook with the given expression, metadata,
    /// and IO token configuration.
    ///
    /// Returns the block number, order hash from the `AddOrderV3` event,
    /// and the full `OrderV4` data (needed for `remove_order`).
    pub async fn add_order(
        &self,
        expression: &str,
        meta: Bytes,
        input_token: Address,
        output_token: Address,
    ) -> anyhow::Result<AddOrderResult> {
        let bytecode = self.compile_expression(expression).await?;
        let orderbook =
            IOrderBookV6::IOrderBookV6Instance::new(self.orderbook, &self.chain.provider);

        let order_config = IOrderBookV6::OrderConfigV4 {
            evaluable: IOrderBookV6::EvaluableV4 {
                interpreter: self.interpreter,
                store: self.store,
                bytecode,
            },
            validInputs: vec![IOrderBookV6::IOV2 {
                token: input_token,
                vaultId: B256::random(),
            }],
            validOutputs: vec![IOrderBookV6::IOV2 {
                token: output_token,
                vaultId: B256::random(),
            }],
            nonce: B256::random(),
            secret: B256::ZERO,
            meta,
        };

        let receipt = orderbook
            .addOrder4(order_config, vec![])
            .send()
            .await?
            .get_receipt()
            .await?;

        let add_event = receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| log.log_decode::<IOrderBookV6::AddOrderV3>().ok())
            .ok_or_else(|| anyhow::anyhow!("AddOrderV3 event not found in receipt"))?;

        let block = receipt
            .block_number
            .ok_or_else(|| anyhow::anyhow!("receipt missing block_number"))?;

        Ok(AddOrderResult {
            block,
            order_hash: add_event.data().orderHash,
            order: add_event.data().order.clone(),
        })
    }

    /// Removes a previously-added order. Returns the block number of the removal.
    pub async fn remove_order(&self, order: &IOrderBookV6::OrderV4) -> anyhow::Result<u64> {
        let orderbook =
            IOrderBookV6::IOrderBookV6Instance::new(self.orderbook, &self.chain.provider);

        let receipt = orderbook
            .removeOrder3(order.clone(), vec![])
            .send()
            .await?
            .get_receipt()
            .await?;

        receipt
            .block_number
            .ok_or_else(|| anyhow::anyhow!("receipt missing block_number"))
    }

    /// Deploys USDC at an arbitrary address (not the canonical `USDC_BASE`).
    ///
    /// Useful when tests need USDC at a specific address different from
    /// the canonical one already deployed by [`AnvilTestChain`].
    pub async fn deploy_usdc_at(&self, usdc_address: Address) -> anyhow::Result<()> {
        let provider = &self.chain.provider;

        provider
            .anvil_set_code(usdc_address, DeployableERC20::DEPLOYED_BYTECODE.clone())
            .await?;

        let total_supply = U256::from(1_000_000_000_000u64);

        // Slot 2: _totalSupply
        provider
            .anvil_set_storage_at(usdc_address, U256::from(2), total_supply.into())
            .await?;

        // Slot 3: _name = "USD Coin"
        provider
            .anvil_set_storage_at(
                usdc_address,
                U256::from(3),
                st0x_evm::test_chain::solidity_short_string(b"USD Coin"),
            )
            .await?;

        // Slot 4: _symbol = "USDC"
        provider
            .anvil_set_storage_at(
                usdc_address,
                U256::from(4),
                st0x_evm::test_chain::solidity_short_string(b"USDC"),
            )
            .await?;

        // Slot 5: _decimals = 6
        provider
            .anvil_set_storage_at(usdc_address, U256::from(5), U256::from(6).into())
            .await?;

        // _balances[owner] at slot 0 (OpenZeppelin ERC20 layout)
        let balance_slot = st0x_evm::test_chain::evm_mapping_slot(self.chain.owner, 0);
        provider
            .anvil_set_storage_at(usdc_address, balance_slot, total_supply.into())
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::utils::parse_units;
    use alloy::providers::Provider;

    use super::*;

    #[tokio::test]
    async fn rain_orderbook_starts_and_compiles_expression() {
        let rain = RainOrderBook::start().await.unwrap();

        // Verify the chain is functional
        rain.chain.provider.get_block_number().await.unwrap();

        // Verify expression compilation works
        let bytecode = rain.compile_expression("_ _: 1000 100;:;").await.unwrap();
        assert!(
            !bytecode.is_empty(),
            "Compiled bytecode should not be empty"
        );
    }

    #[tokio::test]
    async fn add_and_remove_order() {
        let rain = RainOrderBook::start().await.unwrap();

        let usdc = rain
            .deploy_erc20(
                "USD Coin",
                "USDC",
                6,
                parse_units("1000000", 6).unwrap().into(),
            )
            .await
            .unwrap();

        let equity = rain
            .deploy_erc20(
                "Wrapped AAPL",
                "wtAAPL",
                18,
                parse_units("1000000", 18).unwrap().into(),
            )
            .await
            .unwrap();

        let result = rain
            .add_order("_ _: 1000 100;:;", Bytes::new(), usdc, equity)
            .await
            .unwrap();

        assert!(result.block > 0);
        assert_ne!(result.order_hash, B256::ZERO);

        let remove_block = rain.remove_order(&result.order).await.unwrap();
        assert!(remove_block >= result.block);
    }
}
