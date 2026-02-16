//! Base chain fork infrastructure for e2e testing.
//!
//! Forks Base mainnet via Anvil so all Rain/Raindex contracts are live at
//! their production addresses, then uses Anvil cheat codes to mint tokens
//! for test accounts. Deploys a fresh Rain expression stack (Interpreter,
//! Store, Parser, Deployer) for compiling order expressions used in
//! `take_order()`.

use alloy::network::EthereumWallet;
use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::{Address, B256, Bytes, U256, address, keccak256, utils::parse_units};
use alloy::providers::ext::AnvilApi as _;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolEvent;
use rain_math_float::Float;
use std::collections::HashMap;
use url::Url;

pub use st0x_hedge::USDC_BASE;
pub use st0x_hedge::bindings::{DeployableERC20, IERC20};

use st0x_hedge::bindings::IOrderBookV5::{self, TakeOrderV3};
use st0x_hedge::bindings::{Deployer, Interpreter, Parser, Store as RainStore, TOFUTokenDecimals};

/// Raindex OrderBook on Base mainnet.
const ORDERBOOK_BASE: Address = address!("52CEB8eBEf648744fFDDE89F7Bc9C3aC35944775");

/// Circle FiatTokenV2 `balances` mapping storage slot.
const USDC_BALANCES_SLOT: u8 = 9;

/// Direction of the take-order from the order owner's perspective.
pub enum TakeDirection {
    /// Owner's order sells equity for USDC. The taker buys equity.
    /// Bot hedge: BUY on broker (inverse of onchain sell).
    SellEquity,

    /// Owner's order buys equity with USDC. The taker sells equity.
    /// Bot hedge: SELL on broker (inverse of onchain buy).
    BuyEquity,
}

/// A forked Base chain running locally via Anvil, with the real Raindex
/// OrderBook and a freshly deployed Rain expression stack for compiling
/// order expressions.
pub struct BaseChain<P> {
    anvil: AnvilInstance,
    pub provider: P,
    pub owner: Address,
    pub orderbook_addr: Address,
    deployer_addr: Address,
    interpreter_addr: Address,
    store_addr: Address,
    equity_tokens: HashMap<String, Address>,
}

impl BaseChain<()> {
    /// Forks Base mainnet, mints initial USDC, and deploys the Rain
    /// expression stack (Interpreter, Store, Parser, Deployer).
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

        // Mint 1M USDC to owner via storage slot manipulation
        let million_usdc: U256 = parse_units("1000000", 6)?.into();
        mint_usdc(&provider, owner, million_usdc).await?;

        // Deploy Rain expression stack for compiling order expressions.
        // The production Rain contracts exist on Base but we deploy our
        // own set because addOrder3 accepts any interpreter/store in the
        // order config.
        provider
            .anvil_set_code(
                address!("4f1C29FAAB7EDdF8D7794695d8259996734Cc665"),
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

        Ok(BaseChain {
            anvil,
            provider,
            owner,
            orderbook_addr: ORDERBOOK_BASE,
            deployer_addr,
            interpreter_addr,
            store_addr,
            equity_tokens: HashMap::new(),
        })
    }
}

impl<P: Provider + Clone> BaseChain<P> {
    /// Returns the WebSocket endpoint URL for the Anvil node.
    pub fn ws_endpoint(&self) -> anyhow::Result<Url> {
        Ok(self.anvil.ws_endpoint().parse()?)
    }

    /// Sets `recipient`'s USDC balance to `amount` (6-decimal raw units)
    /// by writing directly to Circle's FiatToken `balances` storage slot.
    pub async fn mint_usdc(&self, recipient: Address, amount: U256) -> anyhow::Result<()> {
        mint_usdc(&self.provider, recipient, amount).await
    }

    /// Deploys a test ERC20 with `t{symbol}` name, 18 decimals, and 1M
    /// tokens minted to the owner. Caches the address for `take_order()`.
    pub async fn deploy_equity_token(&mut self, symbol: &str) -> anyhow::Result<Address> {
        let name = format!("t{symbol}");
        let supply: U256 = parse_units("1000000", 18)?.into();

        let token =
            DeployableERC20::deploy(&self.provider, name.clone(), name, 18, self.owner, supply)
                .await?;

        let addr = *token.address();
        self.equity_tokens.insert(symbol.to_string(), addr);
        Ok(addr)
    }

    /// Creates an order on the OrderBook, takes it, and returns the tx hash.
    ///
    /// The order is created with the owner account and immediately taken by
    /// the same account. This emits a `TakeOrderV3` event that the bot
    /// detects. Price is hardcoded to 100 (1 equity token = 100 USDC).
    pub async fn take_order(
        &self,
        symbol: &str,
        amount: &str,
        direction: TakeDirection,
    ) -> anyhow::Result<B256> {
        let equity_addr = *self
            .equity_tokens
            .get(symbol)
            .ok_or_else(|| anyhow::anyhow!("Equity token for {symbol} not deployed"))?;

        let orderbook =
            IOrderBookV5::IOrderBookV5Instance::new(self.orderbook_addr, &self.provider);
        let deployer_instance = Deployer::DeployerInstance::new(self.deployer_addr, &self.provider);

        let is_sell = matches!(direction, TakeDirection::SellEquity);
        let price = 100u32;
        let amount_f64: f64 = amount
            .parse()
            .map_err(|err| anyhow::anyhow!("Invalid amount: {err}"))?;
        let usdc_total = amount_f64 * f64::from(price);
        let amount_str = format!("{amount_f64:.6}");
        let usdc_total_str = format!("{usdc_total:.6}");

        // Order: input = what order receives, output = what order gives
        let (input_token, output_token) = if is_sell {
            (USDC_BASE, equity_addr)
        } else {
            (equity_addr, USDC_BASE)
        };

        // Rain expression: maxAmount (output in base units) and ioRatio
        // Sell: maxAmount = shares in 18-dec, ioRatio = price
        // Buy: maxAmount = USDC in 6-dec, ioRatio = 1/price (only price=1 supported)
        let (max_amount_base, io_ratio_str) = if is_sell {
            let base: U256 = parse_units(&amount_str, 18)?.into();
            (base, price.to_string())
        } else {
            let base: U256 = parse_units(&usdc_total_str, 6)?.into();
            (base, "1".to_string())
        };
        let expression = format!("_ _: {max_amount_base} {io_ratio_str};:;");

        let parsed_bytecode = deployer_instance
            .parse2(Bytes::copy_from_slice(expression.as_bytes()))
            .call()
            .await?
            .0;

        let input_vault_id = B256::random();
        let output_vault_id = B256::random();

        let order_config = IOrderBookV5::OrderConfigV4 {
            evaluable: IOrderBookV5::EvaluableV4 {
                interpreter: self.interpreter_addr,
                store: self.store_addr,
                bytecode: Bytes::from(parsed_bytecode),
            },
            validInputs: vec![IOrderBookV5::IOV2 {
                token: input_token,
                vaultId: input_vault_id,
            }],
            validOutputs: vec![IOrderBookV5::IOV2 {
                token: output_token,
                vaultId: output_vault_id,
            }],
            nonce: B256::random(),
            secret: B256::ZERO,
            meta: Bytes::new(),
        };

        let add_receipt = orderbook
            .addOrder3(order_config, vec![])
            .send()
            .await?
            .get_receipt()
            .await?;

        let add_event = add_receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| log.log_decode::<IOrderBookV5::AddOrderV3>().ok())
            .ok_or_else(|| anyhow::anyhow!("AddOrderV3 event not found"))?;
        let order = add_event.data().order.clone();

        // Deposit output token into the order's output vault
        let deposit_amount_str = if is_sell {
            &amount_str
        } else {
            &usdc_total_str
        };
        let deposit_micro: U256 = parse_units(deposit_amount_str, 6)?.into();
        let deposit_float = Float::from_fixed_decimal_lossy(deposit_micro, 6)
            .map_err(|err| anyhow::anyhow!("Float conversion: {err:?}"))?
            .get_inner();

        let deposit_approve: U256 = if is_sell {
            parse_units(&amount_str, 18)?.into()
        } else {
            parse_units(&usdc_total_str, 6)?.into()
        };

        DeployableERC20::new(output_token, &self.provider)
            .approve(*orderbook.address(), deposit_approve)
            .send()
            .await?
            .get_receipt()
            .await?;

        orderbook
            .deposit3(output_token, output_vault_id, deposit_float, vec![])
            .send()
            .await?
            .get_receipt()
            .await?;

        // Approve taker's payment (input token from order's perspective)
        let taker_approve: U256 = if is_sell {
            parse_units(&usdc_total_str, 6)?.into()
        } else {
            parse_units(&amount_str, 18)?.into()
        };

        DeployableERC20::new(input_token, &self.provider)
            .approve(*orderbook.address(), taker_approve)
            .send()
            .await?
            .get_receipt()
            .await?;

        let take_config = IOrderBookV5::TakeOrdersConfigV4 {
            minimumInput: B256::ZERO,
            maximumInput: Float::from_fixed_decimal_lossy(U256::from(1_000_000), 0)
                .map_err(|err| anyhow::anyhow!("Float conversion: {err:?}"))?
                .get_inner(),
            maximumIORatio: Float::from_fixed_decimal_lossy(U256::from(1_000_000), 0)
                .map_err(|err| anyhow::anyhow!("Float conversion: {err:?}"))?
                .get_inner(),
            orders: vec![IOrderBookV5::TakeOrderConfigV4 {
                order: order.clone(),
                inputIOIndex: U256::from(0),
                outputIOIndex: U256::from(0),
                signedContext: vec![],
            }],
            data: Bytes::new(),
        };

        let take_receipt = orderbook
            .takeOrders3(take_config)
            .send()
            .await?
            .get_receipt()
            .await?;

        anyhow::ensure!(take_receipt.status(), "takeOrders3 reverted");

        let take_log = take_receipt
            .inner
            .logs()
            .iter()
            .find(|log| log.topic0() == Some(&TakeOrderV3::SIGNATURE_HASH))
            .ok_or_else(|| anyhow::anyhow!("TakeOrderV3 event not found"))?;

        Ok(take_log.transaction_hash.unwrap_or_default())
    }
}

/// Sets `recipient`'s USDC balance by writing directly to Circle's
/// FiatToken `balances` storage slot.
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
