//! Relay infrastructure for the Robinhood corridor e2e tests.
//!
//! [`RobinhoodChain`] is an Anvil chain reporting Robinhood's chain id, with
//! an orderbook, USDG and Relay's depository at their pinned addresses.
//! [`RelayMock`] serves Relay's quote and status endpoints over HTTP and runs
//! a solver: it watches both depositories for our deposits and pays each one
//! on the other chain, the way Relay's solver fills an order.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::{Address, B256, Bytes, TxHash, U256};
use alloy::providers::ext::AnvilApi as _;
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::rpc::types::{Filter, TransactionRequest};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use alloy::sol_types::{SolCall, SolEvent};
use httpmock::prelude::*;
use rain_math_float::Float;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use st0x_bridge::relay::{BasisPoints, QuoteRequest, deploy_relay_end, quote_body_for_test};
use st0x_evm::{Chain, IERC20};
use st0x_hedge::bindings::{IRaindexV6, RaindexV6};

use crate::base_chain::deploy_tofu_singleton;

sol! {
    event RelayErc20Deposit(address from, address token, uint256 amount, bytes32 id);

    function mint(address to, uint256 amount) external;
}

/// The fee the mock solver keeps from every fill, in basis points of the
/// deposit.
pub const RELAY_FEE_BPS: u64 = 5;

/// What a fill of a `deposit` pays after [`RELAY_FEE_BPS`].
pub fn relay_fill_amount(deposit: U256) -> U256 {
    deposit - deposit * U256::from(RELAY_FEE_BPS) / U256::from(10_000)
}

/// Anvil key of the solver, whose stable a fill pays out.
const SOLVER_KEY: usize = 5;

/// Anvil key of the relayer EOA that sends the solver's fills.
const RELAYER_KEY: usize = 6;

/// Anvil key of the account that deploys on Ethereum and funds the solver
/// before each fill.
const MINTER_KEY: usize = 7;

/// A Robinhood stand-in: an orderbook, the tokens decimals singleton the
/// orderbook needs, a `MockStable` at the pinned USDG address and a
/// `MockDepository` at Relay's pinned depository. Our wallet is Anvil key 0,
/// the same address as the Base and Ethereum harness wallets.
pub struct RobinhoodChain {
    anvil: AnvilInstance,
    pub provider: DynProvider,
    pub owner: Address,
    pub orderbook: Address,
}

impl RobinhoodChain {
    pub async fn start() -> anyhow::Result<Self> {
        // As on the Base harness chain: one block a second so a multi-block
        // confirmation wait completes, and a one-slot epoch so the fill
        // monitor's `safe` tag trails the tip by a couple of blocks only.
        let anvil = Anvil::new()
            .block_time(1)
            .chain_id(Chain::Robinhood.chain_id())
            .arg("--slots-in-an-epoch")
            .arg("1")
            .spawn();
        let provider = signing_provider(&anvil, 0);
        let owner = anvil.addresses()[0];

        let orderbook = *RaindexV6::deploy(&provider).await?.address();
        deploy_tofu_singleton(&provider).await?;
        etch_relay_end(&provider, Chain::Robinhood, true).await?;

        Ok(Self {
            anvil,
            provider,
            owner,
            orderbook,
        })
    }

    pub fn endpoint(&self) -> String {
        self.anvil.endpoint()
    }

    /// Mints `amount` USDG to our wallet and deposits it into a fresh vault.
    pub async fn create_usdg_vault(&self, amount: U256) -> anyhow::Result<B256> {
        let usdg = Chain::Robinhood.settlement_stable().address;
        let vault_id = B256::random();

        send(
            &self.provider,
            usdg,
            mintCall {
                to: self.owner,
                amount,
            }
            .abi_encode(),
        )
        .await?;
        IERC20::new(usdg, &self.provider)
            .approve(self.orderbook, amount)
            .send()
            .await?
            .get_receipt()
            .await?;

        let deposit = Float::from_fixed_decimal(amount, 6)
            .map_err(|error| anyhow::anyhow!("Float conversion: {error:?}"))?
            .get_inner();
        IRaindexV6::IRaindexV6Instance::new(self.orderbook, &self.provider)
            .deposit4(usdg, vault_id, deposit, vec![])
            .send()
            .await?
            .get_receipt()
            .await?;

        Ok(vault_id)
    }

    /// Our USDG vault balance, in USDG's smallest unit.
    pub async fn vault_usdg(&self, vault_id: B256) -> anyhow::Result<U256> {
        let usdg = Chain::Robinhood.settlement_stable().address;
        let raw = IRaindexV6::IRaindexV6Instance::new(self.orderbook, &self.provider)
            .vaultBalance2(self.owner, usdg, vault_id)
            .call()
            .await?;
        let (units, lossless) = Float::from_raw(raw)
            .to_fixed_decimal_lossy(6)
            .map_err(|error| anyhow::anyhow!("Float conversion: {error:?}"))?;
        anyhow::ensure!(lossless, "vault balance has more than 6 decimals");

        Ok(units)
    }

    /// Our wallet's USDG balance outside the vault.
    pub async fn wallet_usdg(&self) -> anyhow::Result<U256> {
        let usdg = Chain::Robinhood.settlement_stable().address;
        Ok(IERC20::new(usdg, &self.provider)
            .balanceOf(self.owner)
            .call()
            .await?)
    }
}

/// Etches Relay's depository at its pinned address on `chain`, and, with
/// `stable`, a `MockStable` at the chain's settlement stable. Ethereum keeps
/// the USDC the CCTP harness etched there.
async fn etch_relay_end<P: Provider>(
    provider: &P,
    chain: Chain,
    stable: bool,
) -> anyhow::Result<()> {
    let depository = chain
        .relay_depository()
        .ok_or_else(|| anyhow::anyhow!("{chain} has no Relay depository"))?;
    let deployed = deploy_relay_end(provider).await?;

    let code = provider.get_code_at(deployed.depository).await?;
    provider.anvil_set_code(depository, code).await?;

    if stable {
        let code = provider.get_code_at(deployed.stable).await?;
        provider
            .anvil_set_code(chain.settlement_stable().address, code)
            .await?;
    }

    Ok(())
}

/// One order the mock quoted, keyed by its order id (also its request id).
#[derive(Clone)]
struct RelayOrder {
    destination: Chain,
    recipient: Address,
    amount_in: U256,
    amount_out: U256,
    deposit_tx: Option<TxHash>,
    fill_tx: Option<TxHash>,
}

#[derive(Default)]
struct RelayState {
    orders: HashMap<B256, RelayOrder>,
}

/// One chain the solver watches and pays on.
#[derive(Clone)]
struct SolverChain {
    chain: Chain,
    endpoint: String,
}

/// The accounts behind a fill, the same Anvil dev keys on both chains.
#[derive(Clone, Copy)]
struct SolverKeys {
    solver: B256,
    relayer: B256,
    minter: B256,
}

/// A mocked Relay API: `POST /quote/v2` answers every request with a quote
/// `RelayClient::quote` accepts, and `GET /intents/status/v3` reports each
/// order as `waiting`, `pending` once its deposit is on chain, and `success`
/// with the fill tx once the solver paid it.
pub struct RelayMock {
    server: MockServer,
    state: Arc<Mutex<RelayState>>,
    solver: JoinHandle<()>,
}

impl RelayMock {
    /// Etches Relay's depository on the Ethereum chain at
    /// `ethereum_endpoint`, then starts the API and the solver over it and
    /// `robinhood`.
    pub async fn start(
        robinhood: &RobinhoodChain,
        ethereum_endpoint: String,
    ) -> anyhow::Result<Self> {
        let dev_key = |index: usize| B256::from_slice(&robinhood.anvil.keys()[index].to_bytes());
        let keys = SolverKeys {
            solver: dev_key(SOLVER_KEY),
            relayer: dev_key(RELAYER_KEY),
            minter: dev_key(MINTER_KEY),
        };
        etch_relay_end(
            &key_provider(&keys.minter, &ethereum_endpoint)?,
            Chain::Ethereum,
            false,
        )
        .await?;

        let server = MockServer::start_async().await;
        let state = Arc::new(Mutex::new(RelayState::default()));

        let quote_state = Arc::clone(&state);
        server.mock(|when, then| {
            when.method(POST).path("/quote/v2");
            then.respond_with(move |request: &HttpMockRequest| {
                match quote_response(&quote_state, request.body().as_ref()) {
                    Ok(body) => json_response(200, &body),
                    Err(error) => {
                        warn!(?error, "Relay mock could not quote");
                        json_response(400, &json!({"message": error.to_string()}))
                    }
                }
            });
        });

        let status_state = Arc::clone(&state);
        server.mock(|when, then| {
            when.method(GET).path("/intents/status/v3");
            then.respond_with(move |request: &HttpMockRequest| {
                let request_id = request
                    .uri()
                    .query()
                    .unwrap_or_default()
                    .split('&')
                    .find_map(|param| param.strip_prefix("requestId="))
                    .and_then(|id| id.parse::<B256>().ok());
                json_response(200, &status_body(&lock(&status_state), request_id))
            });
        });

        let solver = tokio::spawn(run_solver(
            Arc::clone(&state),
            keys,
            [
                SolverChain {
                    chain: Chain::Robinhood,
                    endpoint: robinhood.endpoint(),
                },
                SolverChain {
                    chain: Chain::Ethereum,
                    endpoint: ethereum_endpoint,
                },
            ],
        ));

        Ok(Self {
            server,
            state,
            solver,
        })
    }

    pub fn base_url(&self) -> String {
        self.server.base_url()
    }

    /// Every deposit the solver saw, as `(amount_in, amount_out, filled)`.
    pub fn deposits(&self) -> Vec<(U256, U256, bool)> {
        lock(&self.state)
            .orders
            .values()
            .filter(|order| order.deposit_tx.is_some())
            .map(|order| (order.amount_in, order.amount_out, order.fill_tx.is_some()))
            .collect()
    }
}

impl Drop for RelayMock {
    fn drop(&mut self) {
        self.solver.abort();
    }
}

fn lock(state: &Mutex<RelayState>) -> MutexGuard<'_, RelayState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

fn json_response(status: u16, body: &Value) -> HttpMockResponse {
    HttpMockResponse {
        status: Some(status),
        headers: Some(vec![(
            "content-type".to_owned(),
            "application/json".to_owned(),
        )]),
        body: Some(serde_json::to_vec(body).unwrap_or_default().into()),
    }
}

fn chain_by_id(chain_id: u64) -> anyhow::Result<Chain> {
    Chain::ALL
        .into_iter()
        .find(|chain| chain.chain_id() == chain_id)
        .ok_or_else(|| anyhow::anyhow!("no chain has id {chain_id}"))
}

/// Builds the quote for one `POST /quote/v2` body and records its order.
fn quote_response(state: &Mutex<RelayState>, body: &[u8]) -> anyhow::Result<Value> {
    let request: Value = serde_json::from_slice(body)?;
    let field = |name: &str| {
        request
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("quote request has no {name}"))
    };
    let chain = |name: &str| -> anyhow::Result<Chain> {
        let id = field(name)?
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("{name} is not a number"))?;
        chain_by_id(id)
    };
    let address = |name: &str| -> anyhow::Result<Address> {
        Ok(serde_json::from_value(field(name)?.clone())?)
    };
    let text = |name: &str| -> anyhow::Result<String> {
        field(name)?
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| anyhow::anyhow!("{name} is not a string"))
    };

    let quote_request = QuoteRequest {
        origin: chain("originChainId")?,
        destination: chain("destinationChainId")?,
        amount: U256::from_str_radix(&text("amount")?, 10)?,
        user: address("user")?,
        recipient: address("recipient")?,
        refund_to: address("refundTo")?,
        slippage: BasisPoints::new(text("slippageTolerance")?.parse()?)?,
        ttl: Duration::from_secs(
            field("ttl")?
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("ttl is not a number"))?,
        ),
    };
    let amount_out = relay_fill_amount(quote_request.amount);

    let mut quote = quote_body_for_test(&quote_request, amount_out, amount_out)?;
    let order_id: B256 = serde_json::from_value(quote["protocol"]["v2"]["orderId"].clone())?;
    // One request id per order, so each status read names its own order.
    quote["requestId"] = json!(order_id);

    lock(state).orders.entry(order_id).or_insert(RelayOrder {
        destination: quote_request.destination,
        recipient: quote_request.recipient,
        amount_in: quote_request.amount,
        amount_out,
        deposit_tx: None,
        fill_tx: None,
    });
    debug!(%order_id, amount = %quote_request.amount, "Relay mock quoted");

    Ok(quote)
}

fn status_body(state: &RelayState, request_id: Option<B256>) -> Value {
    let Some(order) = request_id.and_then(|id| state.orders.get(&id)) else {
        return json!({"status": "waiting"});
    };

    match (order.deposit_tx, order.fill_tx) {
        (Some(deposit), Some(fill)) => {
            json!({"status": "success", "inTxHashes": [deposit], "txHashes": [fill]})
        }
        (Some(deposit), None) => {
            json!({"status": "pending", "inTxHashes": [deposit], "txHashes": []})
        }
        (None, _) => json!({"status": "waiting"}),
    }
}

/// Scans both depositories for deposits of quoted orders and fills each
/// one on its destination chain.
async fn run_solver<const CHAINS: usize>(
    state: Arc<Mutex<RelayState>>,
    keys: SolverKeys,
    chains: [SolverChain; CHAINS],
) {
    let mut scanned: HashMap<Chain, u64> = HashMap::new();

    loop {
        tokio::time::sleep(Duration::from_millis(250)).await;

        for solver_chain in &chains {
            if let Err(error) = scan_deposits(&state, solver_chain, &mut scanned).await {
                warn!(chain = %solver_chain.chain, ?error, "Relay mock deposit scan failed");
            }
        }

        if let Err(error) = fill_deposits(&state, keys, &chains).await {
            warn!(?error, "Relay mock fill failed");
        }
    }
}

async fn scan_deposits(
    state: &Mutex<RelayState>,
    solver_chain: &SolverChain,
    scanned: &mut HashMap<Chain, u64>,
) -> anyhow::Result<()> {
    let provider = ProviderBuilder::new()
        .connect_http(solver_chain.endpoint.parse()?)
        .erased();
    let head = provider.get_block_number().await?;
    let from = scanned.get(&solver_chain.chain).map_or(0, |last| last + 1);
    if from > head {
        return Ok(());
    }

    let depository = solver_chain
        .chain
        .relay_depository()
        .ok_or_else(|| anyhow::anyhow!("{} has no Relay depository", solver_chain.chain))?;
    let logs = provider
        .get_logs(
            &Filter::new()
                .address(depository)
                .from_block(from)
                .to_block(head),
        )
        .await?;

    let deposits: Vec<(B256, Option<TxHash>)> = logs
        .iter()
        .filter_map(|log| {
            let deposit = RelayErc20Deposit::decode_log_data(log.data()).ok()?;
            Some((deposit.id, log.transaction_hash))
        })
        .collect();
    record_deposits(&mut lock(state), deposits);
    scanned.insert(solver_chain.chain, head);

    Ok(())
}

/// Marks each quoted order a deposit pays as deposited, first deposit wins.
fn record_deposits(state: &mut RelayState, deposits: Vec<(B256, Option<TxHash>)>) {
    for (order_id, deposit_tx) in deposits {
        let Some(order) = state.orders.get_mut(&order_id) else {
            warn!(%order_id, "Relay mock saw a deposit it never quoted");
            continue;
        };
        if order.deposit_tx.is_none() {
            order.deposit_tx = deposit_tx;
            debug!(%order_id, "Relay mock saw a deposit");
        }
    }
}

async fn fill_deposits(
    state: &Mutex<RelayState>,
    keys: SolverKeys,
    chains: &[SolverChain],
) -> anyhow::Result<()> {
    let unfilled: Vec<(B256, RelayOrder)> = lock(state)
        .orders
        .iter()
        .filter(|(_, order)| order.deposit_tx.is_some() && order.fill_tx.is_none())
        .map(|(id, order)| (*id, order.clone()))
        .collect();

    for (order_id, order) in unfilled {
        let destination = chains
            .iter()
            .find(|solver_chain| solver_chain.chain == order.destination)
            .ok_or_else(|| anyhow::anyhow!("the solver does not pay on {}", order.destination))?;
        let fill = pay(
            destination,
            keys,
            order.recipient,
            order.amount_out,
            order_id,
        )
        .await?;

        if let Some(order) = lock(state).orders.get_mut(&order_id) {
            order.fill_tx = Some(fill);
        }
        debug!(%order_id, %fill, "Relay mock filled a deposit");
    }

    Ok(())
}

/// Pays `recipient` the way Relay's solver does: the relayer EOA calls the
/// stable's `transferFrom(solver, recipient, amount)` with the order id
/// appended, after the solver was funded and approved the relayer.
async fn pay(
    solver_chain: &SolverChain,
    keys: SolverKeys,
    recipient: Address,
    amount: U256,
    order_id: B256,
) -> anyhow::Result<TxHash> {
    let stable = solver_chain.chain.settlement_stable().address;
    let solver = PrivateKeySigner::from_bytes(&keys.solver)?.address();
    let relayer = PrivateKeySigner::from_bytes(&keys.relayer)?.address();
    let endpoint = &solver_chain.endpoint;

    send(
        &key_provider(&keys.minter, endpoint)?,
        stable,
        mintCall { to: solver, amount }.abi_encode(),
    )
    .await?;
    send(
        &key_provider(&keys.solver, endpoint)?,
        stable,
        IERC20::approveCall {
            spender: relayer,
            amount,
        }
        .abi_encode(),
    )
    .await?;

    let mut calldata = IERC20::transferFromCall {
        from: solver,
        to: recipient,
        amount,
    }
    .abi_encode();
    calldata.extend_from_slice(order_id.as_slice());

    send(&key_provider(&keys.relayer, endpoint)?, stable, calldata).await
}

/// Sends `calldata` to `to` and waits for a successful receipt.
async fn send<P: Provider>(provider: &P, to: Address, calldata: Vec<u8>) -> anyhow::Result<TxHash> {
    let request = TransactionRequest::default()
        .with_to(to)
        .with_input(Bytes::from(calldata));
    let receipt = provider
        .send_transaction(request)
        .await?
        .get_receipt()
        .await?;
    anyhow::ensure!(receipt.status(), "tx {} reverted", receipt.transaction_hash);

    Ok(receipt.transaction_hash)
}

fn key_provider(key: &B256, endpoint: &str) -> anyhow::Result<DynProvider> {
    Ok(ProviderBuilder::new()
        .wallet(EthereumWallet::from(PrivateKeySigner::from_bytes(key)?))
        .connect_http(endpoint.parse()?)
        .erased())
}

fn signing_provider(anvil: &AnvilInstance, key: usize) -> DynProvider {
    let signer = PrivateKeySigner::from_bytes(&B256::from_slice(&anvil.keys()[key].to_bytes()))
        .unwrap_or_else(|_| unreachable!("Anvil keys are valid"));

    ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer))
        .connect_http(anvil.endpoint_url())
        .erased()
}
