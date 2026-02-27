//! Local Anvil chain infrastructure for e2e testing.
//!
//! Deploys all contracts fresh on a plain Anvil instance (no mainnet fork),
//! including the OrderBook, USDC (placed at the canonical `USDC_BASE`
//! address via `anvil_set_code`), and the Rain expression stack
//! (Interpreter, Store, Parser, Deployer) for compiling order expressions
//! used in `take_order()`.

use alloy::network::EthereumWallet;
use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::{Address, B256, Bytes, U256, address, keccak256, utils::parse_units};
use alloy::providers::ext::AnvilApi as _;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use alloy::sol_types::SolEvent as _;
use rain_math_float::Float;
use st0x_exact_decimal::ExactDecimal;
use std::collections::HashMap;
use url::Url;

pub use st0x_hedge::USDC_BASE;
pub use st0x_hedge::bindings::{DeployableERC20, IERC20};

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    TestVault, "src/services/TestVault.json"
);

use st0x_hedge::bindings::IOrderBookV6::{self, TakeOrderV3};
use st0x_hedge::bindings::{
    Deployer, Interpreter, OrderBook, Parser, Store as RainStore, TOFUTokenDecimals,
};

/// OpenZeppelin ERC20 `_balances` mapping storage slot.
///
/// When USDC is deployed fresh via `DeployableERC20` bytecode (not forked
/// from Circle's FiatTokenV2), the balances mapping lives at slot 0
/// (standard OpenZeppelin ERC20 layout) instead of slot 9.
const USDC_BALANCES_SLOT: u8 = 0;

/// Result of a successful `take_order` call, providing both the tx hash
/// and the vault/token details needed for on-chain assertions.
pub struct TakeOrderResult {
    pub tx_hash: B256,
    pub input_vault_id: B256,
    pub output_vault_id: B256,
    pub input_token: Address,
    pub output_token: Address,
    /// Owner input vault delta reported by `TakeOrderV3` (order input token
    /// received by the owner), encoded as a raw Rain Float.
    pub input_vault_delta_from_take_event: B256,
    /// Owner output vault delta reported by `TakeOrderV3` (order output token
    /// spent by the owner), encoded as a raw Rain Float.
    pub output_vault_delta_from_take_event: B256,
    /// Owner vault balances immediately before the take transaction.
    pub input_vault_balance_before_take: B256,
    pub output_vault_balance_before_take: B256,
    /// Owner vault balances immediately after the take transaction receipt.
    pub input_vault_balance_after_take: B256,
    pub output_vault_balance_after_take: B256,
}

/// Direction of the take-order from the order owner's perspective.
#[derive(Clone, Copy)]
pub enum TakeDirection {
    /// Owner's order sells equity for USDC. The taker buys equity.
    /// Bot hedge: BUY on broker (inverse of onchain sell).
    SellEquity,

    /// Owner's order buys equity with USDC. The taker sells equity.
    /// Bot hedge: SELL on broker (inverse of onchain buy).
    BuyEquity,

    /// Opposing trades cancelled out, resulting in net zero exposure.
    /// No offchain hedge is expected.
    NetZero,
}

/// An order that has been placed on the OrderBook and funded, ready to be
/// taken by a separate account. Created by `setup_order()`, consumed by
/// `take_prepared_order()`.
pub struct PreparedOrder {
    pub order: IOrderBookV6::OrderV4,
    pub input_vault_id: B256,
    pub output_vault_id: B256,
    pub input_token: Address,
    pub output_token: Address,
}

/// A local Anvil chain with a freshly deployed OrderBook, USDC, and Rain
/// expression stack for compiling order expressions. No mainnet fork.
pub struct BaseChain<P> {
    anvil: AnvilInstance,
    pub provider: P,
    pub owner: Address,
    /// Private key for the owner account (first Anvil key). Needed by
    /// rebalancing tests where `Ctx::order_owner()` derives the address
    /// from `RebalancingSecrets.evm_private_key` instead of `evm.order_owner`.
    pub owner_key: B256,
    /// Separate taker account (Anvil account #1) with its own provider
    /// and nonce management. Used by rebalancing tests to avoid nonce
    /// collisions with the bot's concurrent transactions from the owner.
    pub taker: Address,
    taker_provider: P,
    pub orderbook_addr: Address,
    deployer_addr: Address,
    interpreter_addr: Address,
    store_addr: Address,
    equity_tokens: HashMap<String, Address>,
}

impl BaseChain<()> {
    /// Deploys all contracts fresh on a plain Anvil instance (no mainnet
    /// fork): OrderBook, USDC at `USDC_BASE`, and the Rain expression
    /// stack (Interpreter, Store, Parser, Deployer).
    pub async fn start() -> anyhow::Result<BaseChain<impl Provider + Clone>> {
        // Auto-mine a block every second so that onchain transactions
        // requiring multiple confirmations (e.g., REQUIRED_CONFIRMATIONS=3
        // in ShareWrapper) complete on Anvil instead of hanging forever.
        let anvil = Anvil::new().block_time(1).spawn();

        let key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let signer = PrivateKeySigner::from_bytes(&key)?;
        let owner = signer.address();
        let wallet = EthereumWallet::from(signer);

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(&anvil.endpoint())
            .await?;

        let orderbook = OrderBook::deploy(&provider).await?;
        let orderbook_addr = *orderbook.address();

        // Place USDC contract at the canonical USDC_BASE address so vault
        // discovery and all downstream code recognizes it.
        deploy_usdc_at_base(&provider, owner).await?;

        provider
            .anvil_set_code(
                address!("F66761F6b5F58202998D6Cd944C81b22Dc6d4f1E"),
                TOFUTokenDecimals::DEPLOYED_BYTECODE.clone(),
            )
            .await?;

        let interpreter = Interpreter::deploy(&provider).await?;
        let store = RainStore::deploy(&provider).await?;
        let parser = Parser::deploy(&provider).await?;

        let interpreter_addr = *interpreter.address();
        let store_addr = *store.address();

        let deployer = Deployer::deploy(
            &provider,
            Deployer::RainterpreterExpressionDeployerConstructionConfigV2 {
                interpreter: interpreter_addr,
                store: store_addr,
                parser: *parser.address(),
            },
        )
        .await?;

        let deployer_addr = *deployer.address();

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

        // Fund taker with USDC (for SellEquity takes where taker pays USDC)
        let million_usdc: U256 = parse_units("1000000", 6)?.into();
        mint_usdc(&provider, taker, million_usdc).await?;

        Ok(BaseChain {
            anvil,
            provider,
            owner,
            owner_key: key,
            taker,
            taker_provider,
            orderbook_addr,
            deployer_addr,
            interpreter_addr,
            store_addr,
            equity_tokens: HashMap::new(),
        })
    }
}

#[bon::bon]
impl<P: Provider + Clone> BaseChain<P> {
    /// Returns the WebSocket endpoint URL for the Anvil node.
    pub fn ws_endpoint(&self) -> anyhow::Result<Url> {
        Ok(self.anvil.ws_endpoint().parse()?)
    }

    /// Mines `count` empty blocks.
    pub async fn mine_blocks(&self, count: u64) -> anyhow::Result<()> {
        for _ in 0..count {
            self.provider.anvil_mine(Some(1), None).await?;
        }
        Ok(())
    }

    /// Deploys a test ERC20 + ERC-4626 vault wrapper for an equity symbol.
    ///
    /// The vault has a 1:1 asset ratio (fresh, no appreciation). Half the
    /// underlying supply is deposited into the vault so the owner has vault
    /// shares available for orderbook deposits in `take_order()`. The vault
    /// address is stored in `equity_tokens` so orders trade vault shares.
    ///
    /// Returns `(vault_address, underlying_token_address)` where:
    /// - `vault_address` = the ERC-4626 wrapper (used as `wrapped` in config)
    /// - `underlying_token_address` = the plain ERC20 (used as `unwrapped`)
    pub async fn deploy_equity_vault(
        &mut self,
        symbol: &str,
    ) -> anyhow::Result<(Address, Address)> {
        let name = format!("wt{symbol}");
        let supply: U256 = parse_units("1000000", 18)?.into();

        let underlying = DeployableERC20::deploy(
            &self.provider,
            name.clone(),
            name.clone(),
            18,
            self.owner,
            supply,
        )
        .await?;
        let underlying_addr = *underlying.address();

        let vault = TestVault::deploy(&self.provider, name.clone(), name, underlying_addr).await?;
        let vault_addr = *vault.address();

        // Approve unlimited so both the initial deposit and later wrapping
        // steps (during the mint flow) can spend underlying tokens.
        underlying
            .approve(vault_addr, U256::MAX)
            .send()
            .await?
            .get_receipt()
            .await?;

        // Deposit half the supply into the vault so the owner has vault
        // shares for orderbook orders. The remaining underlying stays
        // available for the wrapping step after tokenization mints.
        let half_supply = supply / U256::from(2);
        vault
            .deposit(half_supply, self.owner)
            .send()
            .await?
            .get_receipt()
            .await?;

        self.equity_tokens.insert(symbol.to_string(), vault_addr);

        Ok((vault_addr, underlying_addr))
    }

    /// Returns the HTTP endpoint URL for the Anvil node.
    pub fn endpoint(&self) -> String {
        self.anvil.endpoint()
    }

    /// Creates a USDC vault on the Raindex OrderBook and returns the vault ID.
    ///
    /// Deposits `amount` USDC (6-decimal raw units) into a fresh vault.
    /// Used by USDC rebalancing tests so the bot has a known vault to
    /// withdraw from (BaseToAlpaca) or deposit into (AlpacaToBase).
    pub async fn create_usdc_vault(&self, amount: U256) -> anyhow::Result<B256> {
        let orderbook =
            IOrderBookV6::IOrderBookV6Instance::new(self.orderbook_addr, &self.provider);
        let vault_id = B256::random();

        // Over-approve for Rain float precision rounding
        IERC20::new(USDC_BASE, &self.provider)
            .approve(*orderbook.address(), amount * U256::from(2))
            .send()
            .await?
            .get_receipt()
            .await?;

        let deposit_float = Float::from_fixed_decimal_lossy(amount, 6)
            .map_err(|err| anyhow::anyhow!("Float conversion: {err:?}"))?
            .0
            .get_inner();

        orderbook
            .deposit4(USDC_BASE, vault_id, deposit_float, vec![])
            .send()
            .await?
            .get_receipt()
            .await?;

        Ok(vault_id)
    }

    /// Deposits tokens into an existing Raindex vault.
    ///
    /// `amount` is in raw units (e.g. 18-decimal for equity tokens).
    /// Used by rebalancing tests to pre-fund vaults with additional tokens
    /// beyond what trade orders deposit.
    pub async fn deposit_into_raindex_vault(
        &self,
        token: Address,
        vault_id: B256,
        amount: U256,
        decimals: u8,
    ) -> anyhow::Result<()> {
        let orderbook =
            IOrderBookV6::IOrderBookV6Instance::new(self.orderbook_addr, &self.provider);

        // Over-approve for Rain float precision rounding
        IERC20::new(token, &self.provider)
            .approve(*orderbook.address(), amount * U256::from(2))
            .send()
            .await?
            .get_receipt()
            .await?;

        let deposit_float = Float::from_fixed_decimal(amount, decimals)
            .map_err(|err| anyhow::anyhow!("Float conversion: {err:?}"))?
            .get_inner();

        orderbook
            .deposit4(token, vault_id, deposit_float, vec![])
            .send()
            .await?
            .get_receipt()
            .await?;

        Ok(())
    }

    /// Creates and funds an order on the OrderBook without taking it.
    ///
    /// All transactions (addOrder3, approve, deposit3) are submitted from
    /// the owner account. The returned `PreparedOrder` can later be taken
    /// via `take_prepared_order()` from the taker account, avoiding nonce
    /// collisions when the bot is also transacting from the owner.
    ///
    /// `usdc_vault_id`: Override the USDC vault ID instead of generating a
    /// random one. Use this when the test needs the order's USDC vault to
    /// match a pre-funded vault (e.g. USDC rebalancing tests where the
    /// inventory poller must discover the pre-funded vault via TakeOrderV3).
    ///
    /// `rain_expression_override`: Optional raw Rainlang expression string for
    /// the whole order expression (e.g. `"_ _: <max> <ratio>;:;"`).
    /// Used only by regression tests that need to inject a precomputed
    /// reciprocal literal instead of `inv(price)`.
    #[builder]
    pub async fn setup_order(
        &self,
        symbol: &str,
        amount: ExactDecimal,
        price: ExactDecimal,
        direction: TakeDirection,
        usdc_vault_id: Option<B256>,
        rain_expression_override: Option<String>,
    ) -> anyhow::Result<PreparedOrder> {
        let equity_vault_addr = *self
            .equity_tokens
            .get(symbol)
            .ok_or_else(|| anyhow::anyhow!("Equity token for {symbol} not deployed"))?;

        let orderbook =
            IOrderBookV6::IOrderBookV6Instance::new(self.orderbook_addr, &self.provider);
        let deployer_instance = Deployer::DeployerInstance::new(self.deployer_addr, &self.provider);

        let is_sell = matches!(direction, TakeDirection::SellEquity);
        let usdc_total = (amount * price)?;
        let amount_str = amount.round_dp(6)?.to_string();
        let usdc_total_str = usdc_total.round_dp(6)?.to_string();

        let (input_token, output_token) = if is_sell {
            (USDC_BASE, equity_vault_addr)
        } else {
            (equity_vault_addr, USDC_BASE)
        };

        let (max_amount_base, io_ratio_str) = if is_sell {
            let base: U256 = parse_units(&amount_str, 18)?.into();
            (base, price.to_string())
        } else {
            let base: U256 = parse_units(&usdc_total_str, 6)?.into();
            (base, format!("inv({price})"))
        };
        let default_expression = format!("_ _: {max_amount_base} {io_ratio_str};:;");
        let expression = rain_expression_override.unwrap_or(default_expression);
        let parsed_bytecode = deployer_instance
            .parse2(Bytes::copy_from_slice(expression.as_bytes()))
            .call()
            .await?
            .0;

        // When a USDC vault ID override is provided, use it for whichever
        // side of the order holds USDC (input for SellEquity, output for
        // BuyEquity). This ensures the VaultRegistry discovers the same
        // vault that was pre-funded by the test.
        let is_usdc_input = is_sell;
        let input_vault_id = if is_usdc_input {
            usdc_vault_id.unwrap_or_else(B256::random)
        } else {
            B256::random()
        };
        let output_vault_id = if is_usdc_input {
            B256::random()
        } else {
            usdc_vault_id.unwrap_or_else(B256::random)
        };

        let order_config = IOrderBookV6::OrderConfigV4 {
            evaluable: IOrderBookV6::EvaluableV4 {
                interpreter: self.interpreter_addr,
                store: self.store_addr,
                bytecode: Bytes::from(parsed_bytecode),
            },
            validInputs: vec![IOrderBookV6::IOV2 {
                token: input_token,
                vaultId: input_vault_id,
            }],
            validOutputs: vec![IOrderBookV6::IOV2 {
                token: output_token,
                vaultId: output_vault_id,
            }],
            nonce: B256::random(),
            secret: B256::ZERO,
            meta: Bytes::new(),
        };

        let add_receipt = orderbook
            .addOrder4(order_config, vec![])
            .send()
            .await?
            .get_receipt()
            .await?;

        let add_event = add_receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| log.log_decode::<IOrderBookV6::AddOrderV3>().ok())
            .ok_or_else(|| anyhow::anyhow!("AddOrderV3 event not found"))?;
        let order = add_event.data().order.clone();

        let deposit_amount_str = if is_sell {
            &amount_str
        } else {
            &usdc_total_str
        };
        let deposit_micro: U256 = parse_units(deposit_amount_str, 6)?.into();
        let deposit_float = Float::from_fixed_decimal_lossy(deposit_micro, 6)
            .map_err(|err| anyhow::anyhow!("Float conversion: {err:?}"))?
            .0
            .get_inner();

        // Over-approve for Rain float precision rounding
        let deposit_approve: U256 = if is_sell {
            let base: U256 = parse_units(&amount_str, 18)?.into();
            base * U256::from(2)
        } else {
            let base: U256 = parse_units(&usdc_total_str, 6)?.into();
            base * U256::from(2)
        };

        DeployableERC20::new(output_token, &self.provider)
            .approve(*orderbook.address(), deposit_approve)
            .send()
            .await?
            .get_receipt()
            .await?;

        orderbook
            .deposit4(output_token, output_vault_id, deposit_float, vec![])
            .send()
            .await?
            .get_receipt()
            .await?;

        Ok(PreparedOrder {
            order,
            input_vault_id,
            output_vault_id,
            input_token,
            output_token,
        })
    }

    /// Takes a previously prepared order using the taker account.
    ///
    /// All transactions (approve, takeOrders3) are submitted from the
    /// taker account, which has its own provider and nonce management.
    /// This avoids nonce collisions with the owner account that the bot
    /// uses for concurrent rebalancing transactions.
    pub async fn take_prepared_order(
        &self,
        prepared: &PreparedOrder,
    ) -> anyhow::Result<TakeOrderResult> {
        let orderbook =
            IOrderBookV6::IOrderBookV6Instance::new(self.orderbook_addr, &self.taker_provider);

        // Approve taker's input token payment (taker pays input_token)
        DeployableERC20::new(prepared.input_token, &self.taker_provider)
            .approve(*orderbook.address(), U256::MAX / U256::from(2))
            .send()
            .await?
            .get_receipt()
            .await?;

        let input_vault_balance_before_take = orderbook
            .vaultBalance2(self.owner, prepared.input_token, prepared.input_vault_id)
            .call()
            .await?;
        let output_vault_balance_before_take = orderbook
            .vaultBalance2(self.owner, prepared.output_token, prepared.output_vault_id)
            .call()
            .await?;

        let take_config = IOrderBookV6::TakeOrdersConfigV5 {
            minimumIO: B256::ZERO,
            maximumIO: Float::from_fixed_decimal_lossy(U256::from(1_000_000), 0)
                .map_err(|err| anyhow::anyhow!("Float conversion: {err:?}"))?
                .0
                .get_inner(),
            maximumIORatio: Float::from_fixed_decimal_lossy(U256::from(1_000_000), 0)
                .map_err(|err| anyhow::anyhow!("Float conversion: {err:?}"))?
                .0
                .get_inner(),
            IOIsInput: true,
            orders: vec![IOrderBookV6::TakeOrderConfigV4 {
                order: prepared.order.clone(),
                inputIOIndex: U256::from(0),
                outputIOIndex: U256::from(0),
                signedContext: vec![],
            }],
            data: Bytes::new(),
        };

        let take_receipt = orderbook
            .takeOrders4(take_config)
            .send()
            .await?
            .get_receipt()
            .await?;

        anyhow::ensure!(take_receipt.status(), "takeOrders4 reverted");

        let take_log = take_receipt
            .inner
            .logs()
            .iter()
            .find(|log| log.topic0() == Some(&TakeOrderV3::SIGNATURE_HASH))
            .ok_or_else(|| anyhow::anyhow!("TakeOrderV3 event not found"))?;
        let take_event = take_log
            .log_decode::<TakeOrderV3>()
            .map_err(|error| anyhow::anyhow!("Failed to decode TakeOrderV3: {error:?}"))?;

        let input_vault_balance_after_take = orderbook
            .vaultBalance2(self.owner, prepared.input_token, prepared.input_vault_id)
            .call()
            .await?;
        let output_vault_balance_after_take = orderbook
            .vaultBalance2(self.owner, prepared.output_token, prepared.output_vault_id)
            .call()
            .await?;

        Ok(TakeOrderResult {
            tx_hash: take_log
                .transaction_hash
                .ok_or_else(|| anyhow::anyhow!("TakeOrderV3 log missing transaction_hash"))?,
            input_vault_id: prepared.input_vault_id,
            output_vault_id: prepared.output_vault_id,
            input_token: prepared.input_token,
            output_token: prepared.output_token,
            input_vault_delta_from_take_event: take_event.data().output,
            output_vault_delta_from_take_event: take_event.data().input,
            input_vault_balance_before_take,
            output_vault_balance_before_take,
            input_vault_balance_after_take,
            output_vault_balance_after_take,
        })
    }

    /// Creates an order on the OrderBook, takes it, and returns the result
    /// including tx hash and vault/token details for on-chain assertions.
    ///
    /// The order is created with the owner account and immediately taken by
    /// the same account. This emits a `TakeOrderV3` event that the bot
    /// detects. Price is hardcoded to 100 (1 equity token = 100 USDC).
    ///
    /// `rain_expression_override`: Optional raw Rainlang expression string for
    /// the whole order expression. Used by regression tests that need exact
    /// control over the emitted `ioRatio`.
    #[builder]
    pub async fn take_order(
        &self,
        symbol: &str,
        amount: ExactDecimal,
        price: ExactDecimal,
        direction: TakeDirection,
        rain_expression_override: Option<String>,
    ) -> anyhow::Result<TakeOrderResult> {
        let equity_vault_addr = *self
            .equity_tokens
            .get(symbol)
            .ok_or_else(|| anyhow::anyhow!("Equity token for {symbol} not deployed"))?;

        let orderbook =
            IOrderBookV6::IOrderBookV6Instance::new(self.orderbook_addr, &self.provider);
        let deployer_instance = Deployer::DeployerInstance::new(self.deployer_addr, &self.provider);

        let is_sell = matches!(direction, TakeDirection::SellEquity);
        let usdc_total = (amount * price)?;
        let amount_str = amount.round_dp(6)?.to_string();
        let usdc_total_str = usdc_total.round_dp(6)?.to_string();

        // Order: input = what order receives, output = what order gives
        let (input_token, output_token) = if is_sell {
            (USDC_BASE, equity_vault_addr)
        } else {
            (equity_vault_addr, USDC_BASE)
        };

        // Rain expression: maxAmount (output in base units) and ioRatio.
        // Sell: output = equity, input = USDC, ioRatio = price (USDC per equity)
        // Buy:  output = USDC, input = equity, ioRatio = inv(price)
        // Keep the reciprocal inside Rainlang to avoid precomputing it in Rust.
        let (max_amount_base, io_ratio_str) = if is_sell {
            let base: U256 = parse_units(&amount_str, 18)?.into();
            (base, price.to_string())
        } else {
            let base: U256 = parse_units(&usdc_total_str, 6)?.into();
            (base, format!("inv({price})"))
        };
        let default_expression = format!("_ _: {max_amount_base} {io_ratio_str};:;");
        let expression = rain_expression_override.unwrap_or(default_expression);

        let parsed_bytecode = deployer_instance
            .parse2(Bytes::copy_from_slice(expression.as_bytes()))
            .call()
            .await?
            .0;

        let input_vault_id = B256::random();
        let output_vault_id = B256::random();

        let order_config = IOrderBookV6::OrderConfigV4 {
            evaluable: IOrderBookV6::EvaluableV4 {
                interpreter: self.interpreter_addr,
                store: self.store_addr,
                bytecode: Bytes::from(parsed_bytecode),
            },
            validInputs: vec![IOrderBookV6::IOV2 {
                token: input_token,
                vaultId: input_vault_id,
            }],
            validOutputs: vec![IOrderBookV6::IOV2 {
                token: output_token,
                vaultId: output_vault_id,
            }],
            nonce: B256::random(),
            secret: B256::ZERO,
            meta: Bytes::new(),
        };

        let add_receipt = orderbook
            .addOrder4(order_config, vec![])
            .send()
            .await?
            .get_receipt()
            .await?;

        let add_event = add_receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| log.log_decode::<IOrderBookV6::AddOrderV3>().ok())
            .ok_or_else(|| anyhow::anyhow!("AddOrderV3 event not found"))?;
        let order = add_event.data().order.clone();

        let deposit_amount_str = if is_sell {
            &amount_str
        } else {
            &usdc_total_str
        };
        let deposit_micro: U256 = parse_units(deposit_amount_str, 6)?.into();
        let deposit_float = Float::from_fixed_decimal_lossy(deposit_micro, 6)
            .map_err(|err| anyhow::anyhow!("Float conversion: {err:?}"))?
            .0
            .get_inner();

        // Over-approve for Rain float precision rounding
        let deposit_approve: U256 = if is_sell {
            let base: U256 = parse_units(&amount_str, 18)?.into();
            base * U256::from(2)
        } else {
            let base: U256 = parse_units(&usdc_total_str, 6)?.into();
            base * U256::from(2)
        };

        DeployableERC20::new(output_token, &self.provider)
            .approve(*orderbook.address(), deposit_approve)
            .send()
            .await?
            .get_receipt()
            .await?;

        orderbook
            .deposit4(output_token, output_vault_id, deposit_float, vec![])
            .send()
            .await?
            .get_receipt()
            .await?;

        // Over-approve taker's payment for the same Rain float precision reason
        let taker_approve: U256 = if is_sell {
            let base: U256 = parse_units(&usdc_total_str, 6)?.into();
            base * U256::from(2)
        } else {
            let base: U256 = parse_units(&amount_str, 18)?.into();
            base * U256::from(2)
        };

        DeployableERC20::new(input_token, &self.provider)
            .approve(*orderbook.address(), taker_approve)
            .send()
            .await?
            .get_receipt()
            .await?;

        let input_vault_balance_before_take = orderbook
            .vaultBalance2(self.owner, input_token, input_vault_id)
            .call()
            .await?;
        let output_vault_balance_before_take = orderbook
            .vaultBalance2(self.owner, output_token, output_vault_id)
            .call()
            .await?;

        let take_config = IOrderBookV6::TakeOrdersConfigV5 {
            minimumIO: B256::ZERO,
            maximumIO: Float::from_fixed_decimal_lossy(U256::from(1_000_000), 0)
                .map_err(|err| anyhow::anyhow!("Float conversion: {err:?}"))?
                .0
                .get_inner(),
            maximumIORatio: Float::from_fixed_decimal_lossy(U256::from(1_000_000), 0)
                .map_err(|err| anyhow::anyhow!("Float conversion: {err:?}"))?
                .0
                .get_inner(),
            IOIsInput: true,
            orders: vec![IOrderBookV6::TakeOrderConfigV4 {
                order: order.clone(),
                inputIOIndex: U256::from(0),
                outputIOIndex: U256::from(0),
                signedContext: vec![],
            }],
            data: Bytes::new(),
        };

        let take_receipt = orderbook
            .takeOrders4(take_config)
            .send()
            .await?
            .get_receipt()
            .await?;

        anyhow::ensure!(take_receipt.status(), "takeOrders4 reverted");

        let take_log = take_receipt
            .inner
            .logs()
            .iter()
            .find(|log| log.topic0() == Some(&TakeOrderV3::SIGNATURE_HASH))
            .ok_or_else(|| anyhow::anyhow!("TakeOrderV3 event not found"))?;
        let take_event = take_log
            .log_decode::<TakeOrderV3>()
            .map_err(|error| anyhow::anyhow!("Failed to decode TakeOrderV3: {error:?}"))?;

        let input_vault_balance_after_take = orderbook
            .vaultBalance2(self.owner, input_token, input_vault_id)
            .call()
            .await?;
        let output_vault_balance_after_take = orderbook
            .vaultBalance2(self.owner, output_token, output_vault_id)
            .call()
            .await?;

        Ok(TakeOrderResult {
            tx_hash: take_log
                .transaction_hash
                .ok_or_else(|| anyhow::anyhow!("TakeOrderV3 log missing transaction_hash"))?,
            input_vault_id,
            output_vault_id,
            input_token,
            output_token,
            input_vault_delta_from_take_event: take_event.data().output,
            output_vault_delta_from_take_event: take_event.data().input,
            input_vault_balance_before_take,
            output_vault_balance_before_take,
            input_vault_balance_after_take,
            output_vault_balance_after_take,
        })
    }
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
        .anvil_set_storage_at(USDC_BASE, U256::from(2), total_supply.into())
        .await?;

    // Slot 3: _name = "USD Coin" (Solidity short-string: data left-aligned,
    // len*2 in last byte)
    let mut name_bytes = [0u8; 32];
    name_bytes[..8].copy_from_slice(b"USD Coin");
    name_bytes[31] = 16;
    provider
        .anvil_set_storage_at(USDC_BASE, U256::from(3), B256::from(name_bytes))
        .await?;

    // Slot 4: _symbol = "USDC" (Solidity short-string encoding)
    let mut symbol_bytes = [0u8; 32];
    symbol_bytes[..4].copy_from_slice(b"USDC");
    symbol_bytes[31] = 8;
    provider
        .anvil_set_storage_at(USDC_BASE, U256::from(4), B256::from(symbol_bytes))
        .await?;

    // Slot 5: _decimals = 6
    provider
        .anvil_set_storage_at(USDC_BASE, U256::from(5), U256::from(6).into())
        .await?;

    // _balances[owner] at slot 0 (OpenZeppelin ERC20 layout)
    let mut slot_key = [0u8; 64];
    slot_key[12..32].copy_from_slice(owner.as_slice());
    let balance_slot = U256::from_be_bytes(keccak256(slot_key).0);
    provider
        .anvil_set_storage_at(USDC_BASE, balance_slot, total_supply.into())
        .await?;

    Ok(())
}

/// Sets `recipient`'s USDC balance by writing directly to the
/// OpenZeppelin ERC20 `_balances` storage slot.
async fn mint_usdc<P: Provider>(
    provider: &P,
    recipient: Address,
    amount: U256,
) -> anyhow::Result<()> {
    let mut slot_data = [0u8; 64];
    slot_data[12..32].copy_from_slice(recipient.as_slice());
    slot_data[63] = USDC_BALANCES_SLOT;
    let balance_slot = keccak256(slot_data);

    provider
        .anvil_set_storage_at(USDC_BASE, balance_slot.into(), amount.into())
        .await?;

    Ok(())
}
