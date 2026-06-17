//! Local Anvil chain infrastructure for e2e testing.
//!
//! Deploys all contracts fresh on a plain Anvil instance (no mainnet fork),
//! including the OrderBook, USDC (placed at the canonical `USDC_BASE`
//! address via `anvil_set_code`), and the Rain expression stack
//! (Interpreter, Store, Parser, Deployer) for compiling order expressions
//! used in `take_order()`.

use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::{Address, B256, Bytes, U256, address, utils::parse_units};
use alloy::providers::ext::AnvilApi as _;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use alloy::sol_types::SolEvent as _;
use rain_math_float::Float;
use std::collections::HashMap;

use st0x_evm::test_chain::{evm_mapping_slot, solidity_short_string};
pub use st0x_evm::{IERC20, USDC_BASE};
pub use st0x_hedge::bindings::DeployableERC20;
use st0x_hedge::bindings::IRaindexV6::{self, TakeOrderV3};
use st0x_hedge::bindings::{Deployer, Interpreter, Parser, RaindexV6, Store as RainStore};

/// Rounds a Float to `decimals` decimal places and formats as a string.
fn round_and_format(value: Float, decimals: u8) -> anyhow::Result<String> {
    let (fixed, _) = value
        .to_fixed_decimal_lossy(decimals)
        .map_err(|err| anyhow::anyhow!("to_fixed_decimal_lossy failed: {err:?}"))?;

    let (rounded, _) = Float::from_fixed_decimal_lossy(fixed, decimals)
        .map_err(|err| anyhow::anyhow!("from_fixed_decimal_lossy failed: {err:?}"))?;

    rounded
        .format_with_scientific(false)
        .map_err(|err| anyhow::anyhow!("format_with_scientific failed: {err:?}"))
}

/// Formats a Float as a decimal string.
fn format_float(value: Float) -> anyhow::Result<String> {
    value
        .format_with_scientific(false)
        .map_err(|err| anyhow::anyhow!("format_with_scientific failed: {err:?}"))
}

// Rainlang interpreter components deploy to deterministic "zoltu" addresses; the
// expression deployer references the parser/store/interpreter by these hardcoded
// constants (see LibInterpreterDeploy in rainlang). Tests therefore etch each
// runtime at its canonical address rather than deploying to sequential ones.
const RAINLANG_INTERPRETER: Address = address!("0xb3A710b89A5569893dA4Ca0dB7D178593b5BE8a0");
const RAINLANG_STORE: Address = address!("0x1Aa775533E28B1D843e1A589034984E3a62005DC");
const RAINLANG_PARSER: Address = address!("0x9179445a637E6Ae72Bb38273944FAB96834488dd");
const RAINLANG_EXPRESSION_DEPLOYER: Address =
    address!("0xC9e1D673eD122193b28376016AC506De2fA20beE");

/// Etches the Rainlang interpreter, store, parser, and expression deployer
/// runtimes at their deterministic deployment addresses. The expression deployer
/// hardcodes these addresses, so order evaluation only works when each runtime
/// lives at its canonical address -- the alloy/anvil equivalent of upstream's
/// `LibInterpreterDeploy.etchRainlang`.
async fn etch_rainlang<P: Provider>(provider: &P) -> anyhow::Result<()> {
    provider
        .anvil_set_code(RAINLANG_INTERPRETER, Interpreter::DEPLOYED_BYTECODE.clone())
        .await?;
    provider
        .anvil_set_code(RAINLANG_STORE, RainStore::DEPLOYED_BYTECODE.clone())
        .await?;
    provider
        .anvil_set_code(RAINLANG_PARSER, Parser::DEPLOYED_BYTECODE.clone())
        .await?;
    provider
        .anvil_set_code(
            RAINLANG_EXPRESSION_DEPLOYER,
            Deployer::DEPLOYED_BYTECODE.clone(),
        )
        .await?;
    Ok(())
}

/// Deterministic singleton address of the TOFUTokenDecimals contract, hardcoded by
/// the orderbook's `LibTOFUTokenDecimals.ensureDeployed` (which checks address + codehash).
const TOFU_TOKEN_DECIMALS: Address = address!("0x200e12D10bb0c5E4a17e7018f0F1161919bb9389");

/// Canonical TOFUTokenDecimals init bytecode, copied from rain-tofu-erc20-decimals'
/// `LibTOFUTokenDecimals.TOFU_DECIMALS_EXPECTED_CREATION_CODE`. Deploying it and etching the
/// resulting runtime at `TOFU_TOKEN_DECIMALS` yields the codehash `ensureDeployed` requires;
/// rain.orderbook's own recompile of TOFUTokenDecimals.sol does not match that hash.
const TOFU_DECIMALS_CREATION_CODE: &str = "0x6080604052348015600e575f80fd5b5061044b8061001c5f395ff3fe608060405234801561000f575f80fd5b506004361061004a575f3560e01c80630782d7e11461004e57806354636d2b14610078578063b7bad1b11461009d578063f5c36eaf146100b0575b5f80fd5b61006161005c366004610363565b6100c3565b60405161006f929190610403565b60405180910390f35b61008b610086366004610363565b6100d8565b60405160ff909116815260200161006f565b6100616100ab366004610363565b6100e9565b61008b6100be366004610363565b6100f5565b5f806100cf5f84610100565b91509150915091565b5f6100e35f836101f0565b92915050565b5f806100cf5f84610281565b5f6100e35f83610356565b73ffffffffffffffffffffffffffffffffffffffff81165f9081526020838152604080832081518083019092525460ff8082161515835261010090910416818301527f313ce56700000000000000000000000000000000000000000000000000000000808452839283908190816004818a5afa915060203d1015610182575f91505b811561019857505f5160ff811115610198575f91505b816101af57505050602001516003925090506101e9565b83516101c3575f955093506101e992505050565b836020015160ff1681146101d85760026101db565b60015b846020015195509550505050505b9250929050565b5f805f6101fd8585610281565b909250905060018260038111156102165761021661039d565b1415801561023557505f8260038111156102325761023261039d565b14155b156102795783826040517fee07877f000000000000000000000000000000000000000000000000000000008152600401610270929190610421565b60405180910390fd5b949350505050565b5f805f8061028f8686610100565b90925090505f8260038111156102a7576102a761039d565b0361034b576040805180820182526001815260ff838116602080840191825273ffffffffffffffffffffffffffffffffffffffff8a165f908152908b9052939093209151825493517fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00009094169015157fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00ff161761010093909116929092029190911790555b909590945092505050565b5f805f6101fd8585610100565b5f60208284031215610373575f80fd5b813573ffffffffffffffffffffffffffffffffffffffff81168114610396575f80fd5b9392505050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52602160045260245ffd5b600481106103ff577f4e487b71000000000000000000000000000000000000000000000000000000005f52602160045260245ffd5b9052565b6040810161041182856103ca565b60ff831660208301529392505050565b73ffffffffffffffffffffffffffffffffffffffff831681526040810161039660208301846103ca56";

/// Deploys the canonical TOFUTokenDecimals init bytecode and etches the resulting
/// runtime at [`TOFU_TOKEN_DECIMALS`]. The orderbook checks both the address and the
/// codehash, so the runtime must come from executing the canonical creation code.
async fn deploy_tofu_singleton<P: Provider>(provider: &P) -> anyhow::Result<()> {
    let creation_code = alloy::hex::decode(TOFU_DECIMALS_CREATION_CODE)?;
    let deployed = provider
        .send_transaction(TransactionRequest::default().with_deploy_code(creation_code))
        .await?
        .get_receipt()
        .await?
        .contract_address
        .ok_or_else(|| anyhow::anyhow!("TOFU deployment produced no contract address"))?;
    let runtime = provider.get_code_at(deployed).await?;
    provider
        .anvil_set_code(TOFU_TOKEN_DECIMALS, runtime)
        .await?;
    Ok(())
}

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    TestVault, "tests/e2e/TestVault.json"
);

/// OpenZeppelin ERC20 `_balances` mapping storage slot.
///
/// When USDC is deployed fresh via `DeployableERC20` bytecode (not forked
/// from Circle's FiatTokenV2), the balances mapping lives at slot 0
/// (standard OpenZeppelin ERC20 layout) instead of slot 9.
const USDC_BALANCES_SLOT: u8 = 0;

/// Result of a successful `take_order` call, providing both the tx hash
/// and the vault/token details needed for on-chain assertions.
pub struct TakeOrderResult {
    pub _tx_hash: B256,
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
    pub order: IRaindexV6::OrderV4,
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
    /// Separate minter account (Anvil account #2) used by the mock
    /// tokenization mint executor to transfer underlying tokens without
    /// nonce collisions with the bot's concurrent owner transactions.
    pub minter: Address,
    pub minter_provider: P,
    pub orderbook: Address,
    deployer: Address,
    interpreter: Address,
    store: Address,
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
        //
        // `--slots-in-an-epoch 1` (Foundry anvil flag) shrinks Anvil's finality
        // lag so the finalized-block fill monitor can ingest within a short
        // test. Anvil advances the `finalized` block tag a fixed number of
        // slots/epochs behind the tip; empirically, with the default 32-slot
        // epoch the lag is large enough that a brief e2e never mines past it --
        // ingestion is starved and the 120s poll times out. A 1-slot epoch
        // drops the observed lag to a couple of blocks, which the 1s
        // auto-mining clears within seconds. (If a future Anvil changes this
        // relationship the failure is a loud e2e timeout, not a silent pass.)
        let anvil = Anvil::new()
            .block_time(1)
            .arg("--slots-in-an-epoch")
            .arg("1")
            .spawn();

        let key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let signer = PrivateKeySigner::from_bytes(&key)?;
        let owner = signer.address();
        let wallet = EthereumWallet::from(signer);

        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(&anvil.endpoint())
            .await?;

        let orderbook = RaindexV6::deploy(&provider).await?;
        let orderbook = *orderbook.address();

        // Place USDC contract at the canonical USDC_BASE address so vault
        // discovery and all downstream code recognizes it.
        deploy_usdc_at(&provider, USDC_BASE, owner).await?;

        // Place USDC at the canonical USDC_ETHEREUM address so the
        // inventory poller can call balanceOf when an ethereum_wallet
        // shares this Anvil provider (equity e2e tests).
        deploy_usdc_at(&provider, crate::cctp::USDC_ETHEREUM, owner).await?;

        // Place USDC at the canonical USDC_ETHEREUM address so the
        // inventory poller can call balanceOf when an ethereum_wallet
        // shares this Anvil provider (equity e2e tests).
        deploy_usdc_at(&provider, crate::cctp::USDC_ETHEREUM, owner).await?;

        deploy_tofu_singleton(&provider).await?;

        etch_rainlang(&provider).await?;

        let interpreter = RAINLANG_INTERPRETER;
        let store = RAINLANG_STORE;
        let deployer = RAINLANG_EXPRESSION_DEPLOYER;

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

        let minter_key = B256::from_slice(&anvil.keys()[2].to_bytes());
        let minter_signer = PrivateKeySigner::from_bytes(&minter_key)?;
        let minter = minter_signer.address();
        let minter_wallet = EthereumWallet::from(minter_signer);

        let minter_provider = ProviderBuilder::new()
            .wallet(minter_wallet)
            .connect(&anvil.endpoint())
            .await?;

        provider.anvil_set_balance(minter, hundred_eth).await?;

        Ok(BaseChain {
            anvil,
            provider,
            owner,
            owner_key: key,
            taker,
            taker_provider,
            minter,
            minter_provider,
            orderbook,
            deployer,
            interpreter,
            store,
            equity_tokens: HashMap::new(),
        })
    }
}

#[bon::bon]
impl<P: Provider + Clone> BaseChain<P> {
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

        // Split supply: 1/2 into vault (owner gets vault shares for orders),
        // 1/4 stays with owner (wrapping after redemption), 1/4 to minter
        // (mock mint executor needs its own tokens to avoid nonce collisions).
        let half_supply = supply / U256::from(2);
        let quarter_supply = supply / U256::from(4);

        vault
            .deposit(half_supply, self.owner)
            .send()
            .await?
            .get_receipt()
            .await?;

        underlying
            .transfer(self.minter, quarter_supply)
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
        let orderbook = IRaindexV6::IRaindexV6Instance::new(self.orderbook, &self.provider);
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

    /// Sets the owner's USDC wallet balance directly (via Anvil storage
    /// override). Used by tests to model production-like flows where the
    /// bot's wallet is empty except mid-transfer.
    pub async fn set_owner_usdc_balance(&self, amount: U256) -> anyhow::Result<()> {
        let balance_slot = evm_mapping_slot(self.owner, USDC_BALANCES_SLOT);
        self.provider
            .anvil_set_storage_at(USDC_BASE, balance_slot, amount.into())
            .await?;
        Ok(())
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
        let orderbook = IRaindexV6::IRaindexV6Instance::new(self.orderbook, &self.provider);

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
        amount: Float,
        price: Float,
        direction: TakeDirection,
        usdc_vault_id: Option<B256>,
        equity_vault_id: Option<B256>,
        rain_expression_override: Option<String>,
    ) -> anyhow::Result<PreparedOrder> {
        let equity_vault_addr = *self
            .equity_tokens
            .get(symbol)
            .ok_or_else(|| anyhow::anyhow!("Equity token for {symbol} not deployed"))?;

        let orderbook = IRaindexV6::IRaindexV6Instance::new(self.orderbook, &self.provider);
        let deployer_instance = Deployer::DeployerInstance::new(self.deployer, &self.provider);

        let is_sell = matches!(direction, TakeDirection::SellEquity);
        let usdc_total =
            (amount * price).map_err(|err| anyhow::anyhow!("Float mul failed: {err:?}"))?;
        let amount_str = round_and_format(amount, 6)?;
        let usdc_total_str = round_and_format(usdc_total, 6)?;

        let (input_token, output_token) = if is_sell {
            (USDC_BASE, equity_vault_addr)
        } else {
            (equity_vault_addr, USDC_BASE)
        };

        let price_str = format_float(price)?;
        let (max_output_str, io_ratio_str) = if is_sell {
            (amount_str.clone(), price_str)
        } else {
            (usdc_total_str.clone(), format!("inv({price_str})"))
        };
        let default_expression = format!("_ _: {max_output_str} {io_ratio_str};:;");
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
            equity_vault_id.unwrap_or_else(B256::random)
        };
        let output_vault_id = if is_usdc_input {
            equity_vault_id.unwrap_or_else(B256::random)
        } else {
            usdc_vault_id.unwrap_or_else(B256::random)
        };

        let order_config = IRaindexV6::OrderConfigV4 {
            evaluable: IRaindexV6::EvaluableV4 {
                interpreter: self.interpreter,
                store: self.store,
                bytecode: Bytes::from(parsed_bytecode),
            },
            validInputs: vec![IRaindexV6::IOV2 {
                token: input_token,
                vaultId: input_vault_id,
            }],
            validOutputs: vec![IRaindexV6::IOV2 {
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
            .find_map(|log| log.log_decode::<IRaindexV6::AddOrderV3>().ok())
            .ok_or_else(|| anyhow::anyhow!("AddOrderV3 event not found"))?;
        let order = add_event.data().order.clone();

        // Only deposit into the output vault if it wasn't pre-funded.
        // SellEquity outputs equity: skip if equity_vault_id was provided.
        // BuyEquity outputs USDC: skip if usdc_vault_id was provided.
        let output_vault_pre_funded = if is_sell {
            equity_vault_id.is_some()
        } else {
            usdc_vault_id.is_some()
        };

        if !output_vault_pre_funded {
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
        }

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
        self.take_prepared_order_with_max(prepared, None).await
    }

    pub async fn take_prepared_order_with_max(
        &self,
        prepared: &PreparedOrder,
        max_amount: Option<Float>,
    ) -> anyhow::Result<TakeOrderResult> {
        let orderbook = IRaindexV6::IRaindexV6Instance::new(self.orderbook, &self.taker_provider);

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

        let default_max = Float::from_fixed_decimal_lossy(U256::from(1_000_000), 0)
            .map_err(|err| anyhow::anyhow!("Float conversion: {err:?}"))?
            .0
            .get_inner();
        let max_io = max_amount.map_or(default_max, |f| f.get_inner());

        let take_config = IRaindexV6::TakeOrdersConfigV5 {
            minimumIO: B256::ZERO,
            maximumIO: max_io,
            maximumIORatio: Float::from_fixed_decimal_lossy(U256::from(1_000_000), 0)
                .map_err(|err| anyhow::anyhow!("Float conversion: {err:?}"))?
                .0
                .get_inner(),
            IOIsInput: true,
            orders: vec![IRaindexV6::TakeOrderConfigV4 {
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
            _tx_hash: take_log
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
    /// detects.
    ///
    /// `rain_expression_override`: Optional raw Rainlang expression string for
    /// the whole order expression. Used by regression tests that need exact
    /// control over the emitted `ioRatio`.
    #[builder]
    pub async fn take_order(
        &self,
        symbol: &str,
        amount: Float,
        price: Float,
        direction: TakeDirection,
        rain_expression_override: Option<String>,
    ) -> anyhow::Result<TakeOrderResult> {
        let equity_vault_addr = *self
            .equity_tokens
            .get(symbol)
            .ok_or_else(|| anyhow::anyhow!("Equity token for {symbol} not deployed"))?;

        let orderbook = IRaindexV6::IRaindexV6Instance::new(self.orderbook, &self.provider);
        let deployer_instance = Deployer::DeployerInstance::new(self.deployer, &self.provider);

        let is_sell = matches!(direction, TakeDirection::SellEquity);
        let usdc_total =
            (amount * price).map_err(|err| anyhow::anyhow!("Float mul failed: {err:?}"))?;
        let amount_str = round_and_format(amount, 6)?;
        let usdc_total_str = round_and_format(usdc_total, 6)?;

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
        let price_str = format_float(price)?;
        let (max_output_str, io_ratio_str) = if is_sell {
            (amount_str.clone(), price_str)
        } else {
            (usdc_total_str.clone(), format!("inv({price_str})"))
        };
        let default_expression = format!("_ _: {max_output_str} {io_ratio_str};:;");
        let expression = rain_expression_override.unwrap_or(default_expression);

        let parsed_bytecode = deployer_instance
            .parse2(Bytes::copy_from_slice(expression.as_bytes()))
            .call()
            .await?
            .0;

        let input_vault_id = B256::random();
        let output_vault_id = B256::random();

        let order_config = IRaindexV6::OrderConfigV4 {
            evaluable: IRaindexV6::EvaluableV4 {
                interpreter: self.interpreter,
                store: self.store,
                bytecode: Bytes::from(parsed_bytecode),
            },
            validInputs: vec![IRaindexV6::IOV2 {
                token: input_token,
                vaultId: input_vault_id,
            }],
            validOutputs: vec![IRaindexV6::IOV2 {
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
            .find_map(|log| log.log_decode::<IRaindexV6::AddOrderV3>().ok())
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

        let take_config = IRaindexV6::TakeOrdersConfigV5 {
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
            orders: vec![IRaindexV6::TakeOrderConfigV4 {
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
            _tx_hash: take_log
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

/// Deploys a USDC ERC20 at the given address using `anvil_set_code` with
/// `DeployableERC20` bytecode, then initialises OpenZeppelin storage slots
/// (totalSupply, name, symbol, decimals) and gives the owner 1B USDC.
async fn deploy_usdc_at<P: Provider>(
    provider: &P,
    usdc_address: Address,
    owner: Address,
) -> anyhow::Result<()> {
    let total_supply = U256::from(1_000_000_000_000u64);

    provider
        .anvil_set_code(usdc_address, DeployableERC20::DEPLOYED_BYTECODE.clone())
        .await?;

    // Slot 2: _totalSupply
    provider
        .anvil_set_storage_at(usdc_address, U256::from(2), total_supply.into())
        .await?;

    // Slot 3: _name = "USD Coin"
    provider
        .anvil_set_storage_at(
            usdc_address,
            U256::from(3),
            solidity_short_string(b"USD Coin"),
        )
        .await?;

    // Slot 4: _symbol = "USDC"
    provider
        .anvil_set_storage_at(usdc_address, U256::from(4), solidity_short_string(b"USDC"))
        .await?;

    // Slot 5: _decimals = 6
    provider
        .anvil_set_storage_at(usdc_address, U256::from(5), U256::from(6).into())
        .await?;

    // _balances[owner] at slot 0 (OpenZeppelin ERC20 layout)
    let balance_slot = evm_mapping_slot(owner, USDC_BALANCES_SLOT);
    provider
        .anvil_set_storage_at(usdc_address, balance_slot, total_supply.into())
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
    let balance_slot = evm_mapping_slot(recipient, USDC_BALANCES_SLOT);

    provider
        .anvil_set_storage_at(USDC_BASE, balance_slot, amount.into())
        .await?;

    Ok(())
}
