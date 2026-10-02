//! Shared test fixtures: database setup, stub orders/logs,
//! and builders for onchain trades and offchain executions.

#[cfg(test)]
use alloy::hex;
#[cfg(test)]
use alloy::network::{EthereumWallet, TransactionBuilder};
#[cfg(test)]
use alloy::node_bindings::{Anvil, AnvilInstance};
#[cfg(test)]
use alloy::primitives::LogData;
use alloy::primitives::{Address, B256, TxHash, address, bytes, fixed_bytes};
#[cfg(test)]
use alloy::primitives::{U256, keccak256};
#[cfg(test)]
use alloy::providers::ext::AnvilApi as _;
#[cfg(test)]
use alloy::providers::{Provider, ProviderBuilder, RootProvider};
#[cfg(test)]
use alloy::rpc::client::RpcClient;
#[cfg(test)]
use alloy::rpc::types::{Log, TransactionReceipt, TransactionRequest};
#[cfg(test)]
use alloy::signers::local::PrivateKeySigner;
use chrono::{DateTime, Utc};
use rain_math_float::Float;
use sqlx::SqlitePool;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::Duration;

#[cfg(test)]
#[cfg(feature = "test-support")]
use st0x_bridge::cctp::{deploy_cctp_on_chain, link_chains, mint_usdc};
use st0x_config::{BrokerCtx, ChainEquities, ChainEquityAsset, OperationMode, RebalancingMode};
#[cfg(any(test, feature = "test-support"))]
use st0x_event_sorcery::{DomainEvent, EventSourced};
use st0x_evm::Chain;
#[cfg(test)]
use st0x_evm::local::RawPrivateKeyWallet;
#[cfg(test)]
use st0x_evm::{Evm, IERC20, NoOpErrorRegistry, Wallet};
use st0x_execution::{AlpacaBrokerApiMode, Direction, FractionalShares, Positive, Symbol};
#[cfg(test)]
use st0x_execution::{CounterTradePreflight, CounterTradeReservation, MarketOrder};

use crate::bindings::IRaindexV6::{EvaluableV4, IOV2, OrderV4};
#[cfg(test)]
use crate::bindings::{DeployableERC20, IRaindexV6, RaindexV6};
use crate::onchain::OnchainTrade;
use crate::onchain::io::{TokenizedSymbol, Usdc, WrappedTokenizedShares};
use crate::onchain_trade::OnChainTradeSource;

#[cfg(test)]
const MAX_CONCURRENT_TEST_ANVILS: usize = 4;

/// Shared `order_polling_interval_secs` equivalent for tests.
///
/// Tests use a
/// realistic-but-arbitrary poll interval (e.g. to derive a staleness bound or
/// populate a `*Ctx.poll_interval` field). A single source of truth avoids the
/// silent-drift risk of multiple modules each defining their own copy of the
/// same value under a "matches such-and-such module" comment that nothing
/// enforces.
pub const TEST_POLL_INTERVAL: Duration = Duration::from_secs(15);

#[cfg(test)]
static ANVIL_PERMITS: LazyLock<(Mutex<usize>, Condvar)> =
    LazyLock::new(|| (Mutex::new(0), Condvar::new()));

#[cfg(test)]
pub(crate) struct TestAnvilInstance {
    instance: AnvilInstance,
    _permit: AnvilPermit,
}

#[cfg(test)]
impl std::ops::Deref for TestAnvilInstance {
    type Target = AnvilInstance;

    fn deref(&self) -> &Self::Target {
        &self.instance
    }
}

#[cfg(test)]
struct AnvilPermit;

#[cfg(test)]
impl Drop for AnvilPermit {
    fn drop(&mut self) {
        let (lock, available) = &*ANVIL_PERMITS;
        let mut in_use = match lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        *in_use = in_use.saturating_sub(1);
        drop(in_use);
        available.notify_all();
    }
}

#[cfg(test)]
fn acquire_anvil_permits(count: usize) -> Vec<AnvilPermit> {
    assert!(
        (1..=MAX_CONCURRENT_TEST_ANVILS).contains(&count),
        "requested {count} Anvil permits, but the limit is {MAX_CONCURRENT_TEST_ANVILS}"
    );

    let (lock, available) = &*ANVIL_PERMITS;
    let mut in_use = match lock.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };

    while *in_use + count > MAX_CONCURRENT_TEST_ANVILS {
        in_use = match available.wait(in_use) {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
    }

    *in_use += count;
    drop(in_use);
    (0..count).map(|_| AnvilPermit).collect()
}

#[cfg(test)]
pub(crate) fn spawn_anvil(anvil: Anvil) -> TestAnvilInstance {
    let permit = acquire_anvil_permits(1).pop().unwrap();
    let instance = anvil.spawn();
    TestAnvilInstance {
        instance,
        _permit: permit,
    }
}

#[cfg(test)]
pub(crate) fn spawn_anvil_pair(
    first: Anvil,
    second: Anvil,
) -> (TestAnvilInstance, TestAnvilInstance) {
    let mut permits = acquire_anvil_permits(2);
    let second_permit = permits.pop().unwrap();
    let first_permit = permits.pop().unwrap();
    let first_instance = first.spawn();
    let second_instance = second.spawn();
    (
        TestAnvilInstance {
            instance: first_instance,
            _permit: first_permit,
        },
        TestAnvilInstance {
            instance: second_instance,
            _permit: second_permit,
        },
    )
}

/// Builds an equity assets config with whitelisted symbols.
///
/// The symbols are enabled for
/// rebalancing. The trigger only dispatches transfers for symbols configured
/// with `rebalancing = "enabled"`, so trigger tests must whitelist the
/// symbols they exercise.
pub fn try_rebalancing_enabled_equities(symbols: &[&str]) -> anyhow::Result<ChainEquities> {
    Ok(ChainEquities {
        operational_limit: None,
        symbols: symbols
            .iter()
            .map(|symbol| {
                Ok((
                    Symbol::new(*symbol)?,
                    ChainEquityAsset {
                        tokenized_equity: Address::ZERO,
                        tokenized_equity_derivative: Address::ZERO,
                        vault_ids: Vec::new(),
                        trading: OperationMode::Disabled,
                        rebalancing: RebalancingMode::Enabled,
                        wrapped_equity_recovery: OperationMode::Disabled,
                        operational_limit: None,
                        target_share: None,
                    },
                ))
            })
            .collect::<anyhow::Result<_>>()?,
    })
}

#[cfg(test)]
pub fn rebalancing_enabled_equities(symbols: &[&str]) -> ChainEquities {
    try_rebalancing_enabled_equities(symbols).expect("test symbols must be valid")
}

/// An equity asset enabled for trading only, with zero token addresses and
/// no vaults: the minimal asset a hedging test needs for its symbol.
pub fn trading_enabled_equity() -> ChainEquityAsset {
    ChainEquityAsset {
        tokenized_equity: Address::ZERO,
        tokenized_equity_derivative: Address::ZERO,
        vault_ids: Vec::new(),
        trading: OperationMode::Enabled,
        rebalancing: RebalancingMode::Disabled,
        wrapped_equity_recovery: OperationMode::Disabled,
        operational_limit: None,
        target_share: None,
    }
}

/// A decoded process-tx fill for `symbol`: log index 7, a sell of 1.5 shares
/// at 123.45.
pub fn try_process_tx_fill_fixture(
    tx_hash: TxHash,
    symbol: &str,
) -> anyhow::Result<crate::operator::process_tx::ProcessTxFill> {
    Ok(crate::operator::process_tx::ProcessTxFill {
        tx_hash,
        log_index: 7,
        symbol: Symbol::new(symbol)?,
        direction: Direction::Sell,
        quantity: FractionalShares::new(Float::parse("1.5".to_owned())?),
        price: Float::parse("123.45".to_owned())?,
    })
}

#[cfg(test)]
pub fn process_tx_fill_fixture(
    tx_hash: TxHash,
    symbol: &str,
) -> crate::operator::process_tx::ProcessTxFill {
    try_process_tx_fill_fixture(tx_hash, symbol).expect("test fill fields must be valid")
}

/// The preflight verdict a broker-backed `OrderPlacer` answers a counter trade
/// with: a sell reserves equity inventory in the symbol it sells, a buy
/// reserves cash buying power. The `OrderPlacer` default allows with no
/// reservation at all, and every config resolves to
/// `SupportedExecutor::AlpacaBrokerApi`, so a test placer that keeps the
/// default is refused by the process-tx placement preflight as an unreserved
/// Alpaca placement. Stand-in placers that are meant to reach the broker
/// answer with this instead.
///
/// The reservation approves exactly the requested size, so a fixture's hedge
/// is never clamped, and prices a buy at the 150 USDC per share the onchain
/// trade fixtures fill at.
#[cfg(test)]
pub(crate) fn reserving_counter_trade_preflight(order: &MarketOrder) -> CounterTradePreflight {
    let reservation = match order.direction {
        Direction::Sell => CounterTradeReservation::Equity {
            symbol: order.symbol.clone(),
            required: order.shares,
            available: order.shares.inner(),
        },
        Direction::Buy => {
            let (cost_cents, _) = (order.shares.inner().inner() * st0x_float_macro::float!(150))
                .and_then(|cost| cost.to_fixed_decimal_lossy(2))
                .expect("a fixture buy cost converts to cents");
            CounterTradeReservation::BuyingPower {
                required: order.shares,
                estimated_cost_cents: i64::try_from(cost_cents)
                    .expect("a fixture buy cost fits in i64 cents"),
                available_buying_power_cents: 10_000_000,
            }
        }
    };

    CounterTradePreflight::Allowed {
        reservation: Some(reservation),
    }
}

/// Broker ctx whose Alpaca mode points at an in-process mock server (e.g.
/// `AlpacaBrokerMock`), so constructing a real executor from it stays
/// network-free.
///
/// Field values come from `test_alpaca_broker_ctx` -- whose
/// account id matches the mock's `TEST_ACCOUNT_ID` -- with only the mode
/// overridden.
pub fn mock_alpaca_broker_ctx(base_url: String) -> BrokerCtx {
    let BrokerCtx::AlpacaBrokerApi(mut alpaca) = st0x_config::test_alpaca_broker_ctx();
    alpaca.mode = Some(AlpacaBrokerApiMode::Mock(base_url));
    BrokerCtx::AlpacaBrokerApi(alpaca)
}

/// Deterministic singleton address of the TOFUTokenDecimals contract. The
/// orderbook's `LibTOFUTokenDecimals.ensureDeployed` hardcodes this address and
/// checks the codehash, so any test exercising deposits, withdrawals, or order
/// takes must place the canonical runtime here.
#[cfg(test)]
pub(crate) const TOFU_TOKEN_DECIMALS: Address =
    address!("0x200e12D10bb0c5E4a17e7018f0F1161919bb9389");

/// Canonical TOFUTokenDecimals init bytecode, copied from
/// rain-tofu-erc20-decimals' `LibTOFUTokenDecimals.TOFU_DECIMALS_EXPECTED_CREATION_CODE`.
/// Deploying this and etching the resulting runtime at `TOFU_TOKEN_DECIMALS` yields the
/// codehash `ensureDeployed` requires; rain.orderbook's own recompile of TOFUTokenDecimals.sol
/// does not match that hash, so its artifact bytecode cannot be used directly.
#[cfg(test)]
const TOFU_DECIMALS_CREATION_CODE: &str = "0x6080604052348015600e575f80fd5b5061044b8061001c5f395ff3fe608060405234801561000f575f80fd5b506004361061004a575f3560e01c80630782d7e11461004e57806354636d2b14610078578063b7bad1b11461009d578063f5c36eaf146100b0575b5f80fd5b61006161005c366004610363565b6100c3565b60405161006f929190610403565b60405180910390f35b61008b610086366004610363565b6100d8565b60405160ff909116815260200161006f565b6100616100ab366004610363565b6100e9565b61008b6100be366004610363565b6100f5565b5f806100cf5f84610100565b91509150915091565b5f6100e35f836101f0565b92915050565b5f806100cf5f84610281565b5f6100e35f83610356565b73ffffffffffffffffffffffffffffffffffffffff81165f9081526020838152604080832081518083019092525460ff8082161515835261010090910416818301527f313ce56700000000000000000000000000000000000000000000000000000000808452839283908190816004818a5afa915060203d1015610182575f91505b811561019857505f5160ff811115610198575f91505b816101af57505050602001516003925090506101e9565b83516101c3575f955093506101e992505050565b836020015160ff1681146101d85760026101db565b60015b846020015195509550505050505b9250929050565b5f805f6101fd8585610281565b909250905060018260038111156102165761021661039d565b1415801561023557505f8260038111156102325761023261039d565b14155b156102795783826040517fee07877f000000000000000000000000000000000000000000000000000000008152600401610270929190610421565b60405180910390fd5b949350505050565b5f805f8061028f8686610100565b90925090505f8260038111156102a7576102a761039d565b0361034b576040805180820182526001815260ff838116602080840191825273ffffffffffffffffffffffffffffffffffffffff8a165f908152908b9052939093209151825493517fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00009094169015157fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00ff161761010093909116929092029190911790555b909590945092505050565b5f805f6101fd8585610100565b5f60208284031215610373575f80fd5b813573ffffffffffffffffffffffffffffffffffffffff81168114610396575f80fd5b9392505050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52602160045260245ffd5b600481106103ff577f4e487b71000000000000000000000000000000000000000000000000000000005f52602160045260245ffd5b9052565b6040810161041182856103ca565b60ff831660208301529392505050565b73ffffffffffffffffffffffffffffffffffffffff831681526040810161039660208301846103ca56";

/// Deploys the canonical TOFUTokenDecimals init bytecode and etches the resulting
/// runtime at [`TOFU_TOKEN_DECIMALS`]. The orderbook checks both the address and
/// the codehash, so the runtime must come from executing the canonical creation
/// code rather than from a recompiled artifact.
#[cfg(test)]
pub(crate) async fn deploy_tofu_singleton<P: Provider>(provider: &P) {
    let creation_code = hex::decode(TOFU_DECIMALS_CREATION_CODE).unwrap();
    let tx = TransactionRequest::default().with_deploy_code(creation_code);

    let deployed = provider
        .send_transaction(tx)
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap()
        .contract_address
        .unwrap();

    let runtime = provider.get_code_at(deployed).await.unwrap();
    provider
        .anvil_set_code(TOFU_TOKEN_DECIMALS, runtime)
        .await
        .unwrap();
}

/// The bot's signer over an Anvil node: the private key wallet the bot builds
/// from its `[wallet]` table, behind the `dyn Wallet` the bot's routes take.
#[cfg(test)]
pub(crate) fn anvil_wallet(
    endpoint: url::Url,
    private_key: &B256,
) -> Arc<dyn Wallet<Provider = RootProvider>> {
    let provider: RootProvider = RootProvider::new(RpcClient::builder().http(endpoint));
    Arc::new(RawPrivateKeyWallet::new(private_key, provider, 1).unwrap())
}

/// A provider signing as `private_key`, for fixture deployments that must not
/// touch the bot wallet's nonces.
#[cfg(test)]
fn fixture_signer(endpoint: url::Url, private_key: B256) -> impl Provider {
    let signer = PrivateKeySigner::from_bytes(&private_key).unwrap();
    ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer))
        .connect_http(endpoint)
}

/// `account`'s balance of the ERC-20 `token`.
#[cfg(test)]
pub(crate) async fn erc20_balance(
    wallet: &Arc<dyn Wallet<Provider = RootProvider>>,
    token: Address,
    account: Address,
) -> U256 {
    wallet
        .call::<NoOpErrorRegistry, _>(token, IERC20::balanceOfCall { account })
        .await
        .unwrap()
}

/// The ERC-20 allowance `owner` granted `spender` over `token`.
#[cfg(test)]
pub(crate) async fn erc20_allowance(
    wallet: &Arc<dyn Wallet<Provider = RootProvider>>,
    token: Address,
    owner: Address,
    spender: Address,
) -> U256 {
    wallet
        .call::<NoOpErrorRegistry, _>(token, IERC20::allowanceCall { owner, spender })
        .await
        .unwrap()
}

/// The receipt of `tx`, which must be mined and must have succeeded.
#[cfg(test)]
pub(crate) async fn mined_receipt(
    wallet: &Arc<dyn Wallet<Provider = RootProvider>>,
    tx: TxHash,
) -> TransactionReceipt {
    let receipt = wallet
        .provider()
        .get_transaction_receipt(tx)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("{tx} must be mined"));
    assert!(receipt.status(), "{tx} must succeed: {receipt:?}");
    receipt
}

/// A fresh Anvil node with a Raindex orderbook deployed and the bot's signer
/// (Anvil account 0) over it. Contracts are deployed from Anvil account 1, so
/// the bot's nonces start untouched. In the Legacy inventory mode the bot's
/// vaults live on this orderbook under the bot's own address.
#[cfg(test)]
pub(crate) struct AnvilRaindexChain {
    _anvil: TestAnvilInstance,
    pub(crate) endpoint: url::Url,
    deployer_key: B256,
    pub(crate) orderbook: Address,
    pub(crate) bot: Address,
    pub(crate) bot_wallet: Arc<dyn Wallet<Provider = RootProvider>>,
}

#[cfg(test)]
impl AnvilRaindexChain {
    pub(crate) async fn deploy() -> Self {
        let anvil = spawn_anvil(Anvil::new());
        let endpoint = anvil.endpoint_url();
        let bot_key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let deployer_key = B256::from_slice(&anvil.keys()[1].to_bytes());

        let deployer = fixture_signer(endpoint.clone(), deployer_key);
        deploy_tofu_singleton(&deployer).await;
        let orderbook = *RaindexV6::deploy(&deployer).await.unwrap().address();

        let bot_wallet = anvil_wallet(endpoint.clone(), &bot_key);

        Self {
            _anvil: anvil,
            endpoint,
            deployer_key,
            orderbook,
            bot: bot_wallet.address(),
            bot_wallet,
        }
    }

    /// Deploys an ERC-20 with `decimals` whose whole `supply` sits in the
    /// bot's wallet.
    pub(crate) async fn deploy_bot_token(&self, decimals: u8, supply: U256) -> Address {
        let deployer = fixture_signer(self.endpoint.clone(), self.deployer_key);
        let token = DeployableERC20::deploy(
            &deployer,
            "Capital Test Token".to_owned(),
            "CAPT".to_owned(),
            decimals,
            self.bot,
            supply,
        )
        .await
        .unwrap();
        *token.address()
    }

    /// Places a 6-decimal ERC-20 at `token`, a canonical address no deploy
    /// lands on, with `balance` in the bot's wallet. The storage follows the
    /// deployable ERC-20's layout: balances at slot 0, the total supply at
    /// slot 2 and the decimals at slot 5.
    pub(crate) async fn etch_bot_stable(&self, token: Address, balance: U256) {
        let provider = ProviderBuilder::new().connect_http(self.endpoint.clone());
        provider
            .anvil_set_code(token, DeployableERC20::DEPLOYED_BYTECODE.clone())
            .await
            .unwrap();
        provider
            .anvil_set_storage_at(token, U256::from(2), balance.into())
            .await
            .unwrap();
        provider
            .anvil_set_storage_at(token, U256::from(5), U256::from(6).into())
            .await
            .unwrap();

        let mut balance_key = [0_u8; 64];
        balance_key[12..32].copy_from_slice(self.bot.as_slice());
        let balance_slot = U256::from_be_bytes(keccak256(balance_key).0);
        provider
            .anvil_set_storage_at(token, balance_slot, balance.into())
            .await
            .unwrap();
    }

    /// Grants `spender` an allowance of `amount` over `token` from the bot's
    /// wallet.
    pub(crate) async fn approve_from_bot(&self, token: Address, spender: Address, amount: U256) {
        self.bot_wallet
            .submit::<NoOpErrorRegistry, _>(
                token,
                IERC20::approveCall { spender, amount },
                "test standing allowance",
            )
            .await
            .unwrap();
    }

    /// The bot's balance of `token` in its vault `vault_id`, in the token's
    /// smallest unit.
    pub(crate) async fn vault_balance(&self, token: Address, vault_id: B256, decimals: u8) -> U256 {
        let balance = self
            .bot_wallet
            .call::<NoOpErrorRegistry, _>(
                self.orderbook,
                IRaindexV6::vaultBalance2Call {
                    owner: self.bot,
                    token,
                    vaultId: vault_id,
                },
            )
            .await
            .unwrap();
        Float::from_raw(balance).to_fixed_decimal(decimals).unwrap()
    }

    /// Turns block production on each tx on or off. With it off, a broadcast
    /// tx stays pending, with no receipt, until [`Self::mine`].
    pub(crate) async fn set_automine(&self, on: bool) {
        self.bot_wallet
            .provider()
            .anvil_set_auto_mine(on)
            .await
            .unwrap();
    }

    /// Mines one block with every pending tx.
    pub(crate) async fn mine(&self) {
        self.bot_wallet
            .provider()
            .anvil_mine(Some(1), None)
            .await
            .unwrap();
    }

    /// Replaces the code at `address` with one that reverts every call with
    /// no revert data, so a pending tx calling it mines with a status 0
    /// receipt.
    pub(crate) async fn make_always_revert(&self, address: Address) {
        // PUSH0 PUSH0 REVERT
        self.bot_wallet
            .provider()
            .anvil_set_code(address, alloy::primitives::bytes!("5f5ffd"))
            .await
            .unwrap();
    }
}

#[cfg(test)]
pub(crate) use held_receipt::{HeldReceiptWallet, ReceiptGate};

/// CCTP V2 on two fresh Anvil nodes standing in for Base and Ethereum, linked
/// both ways, with the bot's signer (Anvil account 0, one address on both) on
/// each. Deploying from Anvil account 1 with the same nonces lands every
/// contract, the mint/burn USDC included, at one address on both nodes.
#[cfg(test)]
#[cfg(feature = "test-support")]
pub(crate) struct AnvilCctpPair {
    _base_anvil: TestAnvilInstance,
    _ethereum_anvil: TestAnvilInstance,
    pub(crate) usdc: Address,
    pub(crate) token_messenger: Address,
    pub(crate) message_transmitter: Address,
    pub(crate) bot: Address,
    pub(crate) base_wallet: Arc<dyn Wallet<Provider = RootProvider>>,
    pub(crate) ethereum_wallet: Arc<dyn Wallet<Provider = RootProvider>>,
}

/// Deploys an [`AnvilCctpPair`] with `base_usdc` of USDC in the bot's Base
/// wallet.
// Two attributes, not `cfg(all(test, ...))`: clippy's `allow-unwrap-in-tests`
// only recognizes a plain `cfg(test)` on the enclosing item.
#[cfg(test)]
#[cfg(feature = "test-support")]
pub(crate) async fn deploy_anvil_cctp_pair(base_usdc: U256) -> AnvilCctpPair {
    let (base_anvil, ethereum_anvil) = spawn_anvil_pair(
        Anvil::new(),
        Anvil::new().chain_id(Chain::Ethereum.chain_id()),
    );
    let base_endpoint = base_anvil.endpoint();
    let ethereum_endpoint = ethereum_anvil.endpoint();
    let bot_key = B256::from_slice(&base_anvil.keys()[0].to_bytes());
    let deployer_key = B256::from_slice(&base_anvil.keys()[1].to_bytes());
    // Only a mint checks the attestation, and no burn test mints.
    let attester = Address::repeat_byte(0xA7);

    let ethereum = deploy_cctp_on_chain(&ethereum_endpoint, &deployer_key, 0, attester)
        .await
        .unwrap();
    let base = deploy_cctp_on_chain(&base_endpoint, &deployer_key, 6, attester)
        .await
        .unwrap();
    link_chains(
        &ethereum_endpoint,
        &base_endpoint,
        &deployer_key,
        &ethereum,
        &base,
    )
    .await
    .unwrap();

    let base_wallet = anvil_wallet(base_anvil.endpoint_url(), &bot_key);
    let ethereum_wallet = anvil_wallet(ethereum_anvil.endpoint_url(), &bot_key);
    let bot = base_wallet.address();
    mint_usdc(&base_endpoint, &deployer_key, base.usdc, bot, base_usdc)
        .await
        .unwrap();

    AnvilCctpPair {
        _base_anvil: base_anvil,
        _ethereum_anvil: ethereum_anvil,
        usdc: base.usdc,
        token_messenger: base.token_messenger,
        message_transmitter: base.message_transmitter,
        bot,
        base_wallet,
        ethereum_wallet,
    }
}

/// Returns a test `OrderV4` instance that is shared across multiple
/// unit-tests. The exact values are not important -- only that the
/// structure is valid and deterministic.
pub fn get_test_order() -> OrderV4 {
    OrderV4 {
        owner: address!("0xdddddddddddddddddddddddddddddddddddddddd"),
        evaluable: EvaluableV4 {
            interpreter: address!("0x2222222222222222222222222222222222222222"),
            store: address!("0x3333333333333333333333333333333333333333"),
            bytecode: bytes!("0x00"),
        },
        nonce: fixed_bytes!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
        validInputs: vec![
            IOV2 {
                token: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                vaultId: B256::ZERO,
            },
            IOV2 {
                token: address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                vaultId: B256::ZERO,
            },
        ],
        validOutputs: vec![
            IOV2 {
                token: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                vaultId: B256::ZERO,
            },
            IOV2 {
                token: address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                vaultId: B256::ZERO,
            },
        ],
    }
}

/// Preloads ERC20 symbols for the fixed token addresses in [`get_test_order`].
///
/// Seeding by ADDRESS (rather than mocking `symbol()`/`decimals()` RPC calls in
/// call order) makes direction assertions sensitive to an inverted IO-index
/// mapping -- a positional mock would return USDC-then-wtAAPL regardless of
/// which token was actually queried, so a swapped input/output index would go
/// undetected.
#[cfg(test)]
pub(crate) fn seed_get_test_order_token_symbols(cache: &st0x_registry::SymbolCache) {
    // Seeded on both chains: fixtures run against Base and (post per-chain
    // cache keying) Ethereum-flavored tests alike.
    for chain in [st0x_evm::Chain::Base, st0x_evm::Chain::Ethereum] {
        cache.preload_symbol(
            chain,
            address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            "USDC",
        );
        cache.preload_symbol(
            chain,
            address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            "wtAAPL",
        );
    }
}

/// Builds a JSON-RPC error payload for a genuine on-chain revert
/// (`Panic(uint256)` with the arithmetic-overflow reason `0x11`).
/// `decode_panic` resolves this selector synchronously without a registry
/// lookup, so pushing it never triggers a real HTTP call to the OpenChain
/// selector registry that token introspection uses in production.
///
/// Only a payload carrying revert *data* satisfies `EvmError::is_revert`, so a
/// bare `push_failure_msg` is not a substitute: it lands as a transport error
/// and is classified retryable rather than as a genuine revert.
#[cfg(test)]
pub(crate) fn panic_revert_payload() -> alloy::rpc::json_rpc::ErrorPayload {
    let revert_data = format!("0x4e487b71{:064x}", 0x11u8);
    alloy::rpc::json_rpc::ErrorPayload {
        code: 3,
        message: "execution reverted".into(),
        data: Some(serde_json::value::to_raw_value(&revert_data).expect("valid json")),
    }
}

/// Creates a generic `Log` stub with the supplied log index. This helper is
/// useful when the concrete value of most fields is irrelevant for the
/// assertion being performed.
#[cfg(test)]
pub(crate) fn create_log(log_index: u64) -> Log {
    Log {
        inner: alloy::primitives::Log {
            address: address!("0xfefefefefefefefefefefefefefefefefefefefe"),
            data: LogData::empty(),
        },
        block_hash: None,
        block_number: Some(12345),
        block_timestamp: Some(1_700_000_000),
        transaction_hash: Some(fixed_bytes!(
            "0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        )),
        transaction_index: None,
        log_index: Some(log_index),
        removed: false,
    }
}

/// Convenience wrapper that returns the log routinely used by the
/// higher-level tests in `trade::mod` (with log index set to `293`).
#[cfg(test)]
pub(crate) fn get_test_log() -> Log {
    create_log(293)
}

static TEST_DATABASE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn test_database_url() -> String {
    let database_id = TEST_DATABASE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("file:st0x-hedge-test-{database_id}?mode=memory&cache=shared")
}

/// CQRS pool (sqlx 0.9) and apalis worker pool (sqlx 0.8) over the same DB.
pub async fn try_setup_test_pools() -> anyhow::Result<(SqlitePool, apalis_sqlite::SqlitePool)> {
    let database_url = test_database_url();
    let pool = st0x_config::configure_sqlite_pool(&database_url).await?;

    sqlx::migrate!().set_ignore_missing(true).run(&pool).await?;
    let apalis_pool = crate::conductor::connect_apalis_pool(&database_url).await?;

    crate::conductor::setup_apalis_tables(&apalis_pool).await?;

    Ok((pool, apalis_pool))
}

#[cfg(test)]
pub async fn setup_test_pools() -> (SqlitePool, apalis_sqlite::SqlitePool) {
    try_setup_test_pools()
        .await
        .expect("test database setup must succeed")
}

/// apalis worker pool (sqlx 0.8) over the same in-memory DB as [`setup_test_db`].
#[cfg(test)]
pub(crate) async fn setup_test_apalis_pool() -> apalis_sqlite::SqlitePool {
    setup_test_pools().await.1
}

/// Waits until the one job of type `Task` in `apalis_pool` is a dead letter:
/// failed with its whole retry budget spent.
#[cfg(test)]
pub(crate) async fn wait_for_terminal_job<Task: 'static>(apalis_pool: &apalis_sqlite::SqlitePool) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let terminal_count: i64 = sqlx_apalis::query_scalar(
                "SELECT COUNT(*) FROM Jobs \
                 WHERE job_type = ? AND status IN ('Failed', 'Killed') \
                 AND attempts >= max_attempts",
            )
            .bind(std::any::type_name::<Task>())
            .fetch_one(apalis_pool)
            .await
            .unwrap();
            if terminal_count == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the poison job must reach a visible terminal state");
}

/// Centralized test database setup to eliminate duplication across test files.
/// Creates an in-memory SQLite database with all migrations applied.
#[cfg(test)]
pub async fn setup_test_db() -> SqlitePool {
    setup_test_pools().await.0
}

/// Fallible database fixture constructor for external test crates.
pub async fn try_setup_test_db() -> anyhow::Result<SqlitePool> {
    Ok(try_setup_test_pools().await?.0)
}

/// Persists a typed event directly into the `events` table.
///
/// Deliberate, fixture-only deviation from the "never write the events table
/// directly" rule: fixtures sometimes need exact persisted states that command
/// choreography cannot express without invoking external services, including
/// historical event shapes, cross-stream interleavings, and interrupted
/// transfers. Production code must never use this helper.
#[cfg(any(test, feature = "test-support"))]
pub async fn try_persist_event<Entity: EventSourced>(
    pool: &SqlitePool,
    aggregate_id: &str,
    sequence: i64,
    event: &Entity::Event,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO events (aggregate_type, aggregate_id, sequence, \
         event_type, event_version, payload, metadata) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, '{}')",
    )
    .bind(Entity::AGGREGATE_TYPE)
    .bind(aggregate_id)
    .bind(sequence)
    .bind(event.event_type())
    .bind(event.event_version())
    .bind(serde_json::to_string(event)?)
    .execute(pool)
    .await?;

    Ok(())
}

#[cfg(test)]
pub(crate) async fn persist_event<Entity: EventSourced>(
    pool: &SqlitePool,
    aggregate_id: &str,
    sequence: i64,
    event: &Entity::Event,
) {
    try_persist_event::<Entity>(pool, aggregate_id, sequence, event)
        .await
        .unwrap();
}

/// File-backed variant of [`setup_test_db`] with production-shaped pool
/// options (WAL journal mode, explicit busy timeout). Needed by tests
/// that inject SQLite lock contention: an in-memory database cannot be
/// locked from a second connection, a file can. The `busy_timeout` is
/// injectable so lock tests wait milliseconds instead of production's
/// 10 seconds -- the code path under test (`SQLITE_BUSY` surfacing
/// through the store) is identical, only the wait differs. The apalis
/// worker pool (sqlx 0.8) is opened over the same file with the same
/// scaled-down timeout so enqueue contention surfaces just as fast.
///
/// Returns the CQRS pool, the apalis worker pool, the database file path
/// (for opening contending connections), and the tempdir guard keeping
/// the file alive.
#[cfg(test)]
pub(crate) async fn setup_file_backed_test_db(
    busy_timeout: std::time::Duration,
) -> (
    SqlitePool,
    apalis_sqlite::SqlitePool,
    std::path::PathBuf,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.sqlite");

    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&db_path)
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .busy_timeout(busy_timeout);
    let pool = SqlitePool::connect_with(options).await.unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    let apalis_options = sqlx_apalis::sqlite::SqliteConnectOptions::new()
        .filename(&db_path)
        .create_if_missing(true)
        .journal_mode(sqlx_apalis::sqlite::SqliteJournalMode::Wal)
        .busy_timeout(busy_timeout);
    let apalis_pool = apalis_sqlite::SqlitePool::connect_with(apalis_options)
        .await
        .unwrap();

    crate::conductor::setup_apalis_tables(&apalis_pool)
        .await
        .unwrap();

    (pool, apalis_pool, db_path, dir)
}

/// Exercises a read-model replay while another connection owns the WAL writer.
#[cfg(test)]
pub(crate) async fn replay_after_competing_writer<T: std::fmt::Debug>(
    pool: &SqlitePool,
    replay: impl std::future::Future<Output = T>,
) -> T {
    let blocker = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    tokio::pin!(replay);
    let _elapsed = tokio::time::timeout(Duration::from_millis(50), &mut replay)
        .await
        .expect_err("replay must wait for the writer before reading its snapshot");
    blocker.commit().await.unwrap();
    replay.await
}

/// Counts the live (`Pending`, `Queued` or `Running`) `PollOrderStatus` jobs
/// queued for one offchain order.
#[cfg(test)]
pub(crate) async fn live_poll_job_count(
    apalis_pool: &apalis_sqlite::SqlitePool,
    offchain_order_id: crate::offchain::order::OffchainOrderId,
) -> i64 {
    sqlx_apalis::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM Jobs \
         WHERE job_type = ? \
           AND json_extract(CAST(job AS TEXT), '$.offchain_order_id') = ? \
           AND status IN ('Pending', 'Queued', 'Running')",
    )
    .bind(std::any::type_name::<crate::offchain::order::PollOrderStatus>())
    .bind(offchain_order_id.to_string())
    .fetch_one(apalis_pool)
    .await
    .unwrap()
}

/// Shared constructor for positive share quantities in tests.
pub fn try_positive_shares(value: &str) -> anyhow::Result<Positive<FractionalShares>> {
    let value = Float::parse(value.to_string())?;
    Ok(Positive::new(FractionalShares::new(value))?)
}

#[cfg(test)]
pub fn positive_shares(value: &str) -> Positive<FractionalShares> {
    try_positive_shares(value).expect("test shares must be valid and positive")
}

/// Builder for creating OnchainTrade test instances with sensible defaults.
/// Reduces duplication in test data setup.
pub struct OnchainTradeBuilder {
    trade: OnchainTrade,
}

#[cfg(test)]
impl Default for OnchainTradeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OnchainTradeBuilder {
    #[cfg(test)]
    pub fn new() -> Self {
        Self::try_new().expect("default onchain trade fixture must be valid")
    }

    pub fn try_new() -> anyhow::Result<Self> {
        Ok(Self {
            trade: OnchainTrade {
                chain: Chain::Base,
                source: OnChainTradeSource::Raindex,
                tx_hash: fixed_bytes!(
                    "0x1111111111111111111111111111111111111111111111111111111111111111"
                ),
                log_index: 1,
                symbol: "wtAAPL".parse::<TokenizedSymbol<WrappedTokenizedShares>>()?,
                equity_token: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                amount: FractionalShares::new(st0x_float_macro::float!(1)),
                direction: Direction::Buy,
                price: Usdc::new(st0x_float_macro::float!(150))?,
                underlying_per_wrapped: Some(st0x_wrapper::RATIO_ONE),
                block_number: Some(1),
                block_timestamp: Some(Utc::now()),
            },
        })
    }

    #[must_use]
    #[cfg(test)]
    pub fn with_symbol(mut self, symbol: &str) -> Self {
        self.trade.symbol = symbol
            .parse::<TokenizedSymbol<WrappedTokenizedShares>>()
            .expect("test symbol must parse");
        self
    }

    #[must_use]
    pub fn with_equity_token(mut self, token: Address) -> Self {
        self.trade.equity_token = token;
        self
    }

    #[must_use]
    pub fn with_source(mut self, source: OnChainTradeSource) -> Self {
        self.trade.source = source;
        self
    }

    #[must_use]
    pub fn with_amount(mut self, amount: Float) -> Self {
        self.trade.amount = FractionalShares::new(amount);
        self
    }

    #[must_use]
    pub fn with_direction(mut self, direction: Direction) -> Self {
        self.trade.direction = direction;
        self
    }

    #[must_use]
    pub fn with_log_index(mut self, index: u64) -> Self {
        self.trade.log_index = index;
        self
    }

    #[must_use]
    pub fn with_block_number(mut self, block_number: impl IntoOptionalBlockNumber) -> Self {
        self.trade.block_number = block_number.into_optional_block_number();
        self
    }

    #[must_use]
    pub fn with_block_timestamp(mut self, block_timestamp: Option<DateTime<Utc>>) -> Self {
        self.trade.block_timestamp = block_timestamp;
        self
    }

    pub fn build(self) -> OnchainTrade {
        self.trade
    }
}

pub trait IntoOptionalBlockNumber {
    fn into_optional_block_number(self) -> Option<u64>;
}

impl IntoOptionalBlockNumber for u64 {
    fn into_optional_block_number(self) -> Option<u64> {
        Some(self)
    }
}

impl IntoOptionalBlockNumber for Option<u64> {
    fn into_optional_block_number(self) -> Option<u64> {
        self
    }
}

#[cfg(test)]
mod held_receipt {
    use std::sync::Arc;

    use alloy::primitives::{Address, B256, Bytes, Signature, TxHash};
    use alloy::providers::RootProvider;
    use alloy::rpc::types::TransactionReceipt;
    use async_trait::async_trait;
    use tokio::sync::watch;

    use st0x_evm::{Evm, EvmError, PreparedTransaction, Wallet};

    /// What a [`HeldReceiptWallet`] does when asked for a receipt.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum ReceiptGate {
        /// Waits until the gate moves.
        Held,
        /// Waits for the receipt through the wrapped wallet.
        Released,
        /// Panics, as a confirmation that blows up after its broadcast.
        Panic,
        /// Fails with a revert, as a confirmation that does not land.
        Fail,
        /// Fails with a receipt timeout, as a confirmation whose outcome is
        /// still unknown: the tx may yet land.
        TimeOut,
        /// Fails with a formal JSON-RPC error reply, as a receipt poll that a
        /// struggling node answers with an error: the tx's fate is unknown.
        RpcError,
        /// Fails as a drop report: the node the wallet asked has neither a
        /// receipt nor the pending tx, though another node may still hold it.
        Dropped,
        /// Waits for the real receipt, and if it has status 0, fails as the
        /// wallet's revert replay does on a node that pruned the block's
        /// state: with an error that decodes no revert.
        Unreplayable,
    }

    /// Delegates to the wrapped wallet, except that `await_receipt` waits
    /// behind a gate the test moves. A route that answers at the broadcast and
    /// confirms afterwards can then be observed while its confirmation is
    /// still pending. `send` is not gated, since it runs the wrapped wallet's
    /// own receipt wait, so a deposit's approve still confirms on its own.
    pub(crate) struct HeldReceiptWallet {
        inner: Arc<dyn Wallet<Provider = RootProvider>>,
        gate: watch::Receiver<ReceiptGate>,
    }

    impl HeldReceiptWallet {
        /// Wraps `inner` with every confirmation held; the returned sender
        /// moves the gate.
        pub(crate) fn wrap(
            inner: Arc<dyn Wallet<Provider = RootProvider>>,
        ) -> (
            Arc<dyn Wallet<Provider = RootProvider>>,
            watch::Sender<ReceiptGate>,
        ) {
            let (gate, receiver) = watch::channel(ReceiptGate::Held);
            (
                Arc::new(Self {
                    inner,
                    gate: receiver,
                }),
                gate,
            )
        }
    }

    #[async_trait]
    impl Evm for HeldReceiptWallet {
        type Provider = RootProvider;

        fn provider(&self) -> &RootProvider {
            self.inner.provider()
        }
    }

    #[async_trait]
    impl Wallet for HeldReceiptWallet {
        fn address(&self) -> Address {
            self.inner.address()
        }

        async fn sign_typed_data(
            &self,
            payload_json: String,
            expected_digest: B256,
        ) -> Result<Signature, EvmError> {
            self.inner
                .sign_typed_data(payload_json, expected_digest)
                .await
        }

        async fn prepare_pending(
            &self,
            contract: Address,
            calldata: Bytes,
            note: &str,
        ) -> Result<PreparedTransaction, EvmError> {
            self.inner.prepare_pending(contract, calldata, note).await
        }

        async fn broadcast_prepared(
            &self,
            prepared: &PreparedTransaction,
            note: &str,
        ) -> Result<TxHash, EvmError> {
            self.inner.broadcast_prepared(prepared, note).await
        }

        async fn discard_prepared(&self, tx_hash: TxHash) {
            self.inner.discard_prepared(tx_hash).await;
        }

        async fn release_superseded(&self, tx_hash: TxHash) {
            self.inner.release_superseded(tx_hash).await;
        }

        async fn restore_prepared(&self, prepared: &PreparedTransaction) {
            self.inner.restore_prepared(prepared).await;
        }

        async fn restore_transaction(&self, tx_hash: TxHash) -> Result<(), EvmError> {
            self.inner.restore_transaction(tx_hash).await
        }

        async fn send_pending(
            &self,
            contract: Address,
            calldata: Bytes,
            note: &str,
        ) -> Result<TxHash, EvmError> {
            self.inner.send_pending(contract, calldata, note).await
        }

        async fn await_receipt(&self, tx_hash: TxHash) -> Result<TransactionReceipt, EvmError> {
            let mut gate = self.gate.clone();
            let opened = *gate
                .wait_for(|gate| *gate != ReceiptGate::Held)
                .await
                .expect("the test dropped the receipt gate while a receipt was held");
            match opened {
                ReceiptGate::Released => self.inner.await_receipt(tx_hash).await,
                ReceiptGate::Panic => {
                    panic!("HeldReceiptWallet panics at the receipt of {tx_hash}")
                }
                ReceiptGate::Fail => Err(EvmError::Reverted { tx_hash }),
                ReceiptGate::TimeOut => Err(EvmError::ReceiptTimeout {
                    tx_hash,
                    timeout_secs: 0,
                }),
                ReceiptGate::RpcError => Err(EvmError::Transport(
                    alloy::transports::RpcError::ErrorResp(alloy::rpc::json_rpc::ErrorPayload {
                        code: -32603,
                        message: "internal error".into(),
                        data: None,
                    }),
                )),
                ReceiptGate::Dropped => Err(EvmError::TransactionDropped {
                    tx_hash,
                    elapsed_secs: 0,
                }),
                ReceiptGate::Unreplayable => {
                    let receipt = self.inner.await_receipt(tx_hash).await?;
                    if receipt.status() {
                        return Ok(receipt);
                    }
                    Err(EvmError::Transport(alloy::transports::RpcError::ErrorResp(
                        alloy::rpc::json_rpc::ErrorPayload {
                            code: -32000,
                            message: "missing trie node".into(),
                            data: None,
                        },
                    )))
                }
                ReceiptGate::Held => unreachable!("wait_for returned a held gate"),
            }
        }

        async fn send(
            &self,
            contract: Address,
            calldata: Bytes,
            note: &str,
        ) -> Result<TransactionReceipt, EvmError> {
            self.inner.send(contract, calldata, note).await
        }
    }
}
