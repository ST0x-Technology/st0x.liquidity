//! Integration tests for the hedging pipeline: onchain trades flow through
//! the CQRS aggregates (OnChainTrade, Position, OffchainOrder), accumulate
//! fractional shares, and trigger offchain hedge orders when the position
//! crosses the execution threshold.
//!
//! Tests use a real Rain OrderBook deployed on Anvil to produce authentic
//! `TakeOrderV3` events, ensuring the full parsing and conversion pipeline
//! is exercised end-to-end.

use alloy::network::EthereumWallet;
use alloy::node_bindings::Anvil;
use alloy::primitives::{Address, B256, Bytes, U256, address, keccak256, utils::parse_units};
use alloy::providers::ext::AnvilApi as _;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::Log;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolEvent;
use chrono::{DateTime, Utc};
use rain_math_float::Float;
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::sync::Arc;

use st0x_config::{
    AssetsConfig, EquitiesConfig, EquityAssetConfig, ExecutionThreshold, OperationMode,
};
use st0x_event_sorcery::{Projection, Store, StoreBuilder, test_store};
use st0x_evm::{ReadOnlyEvm, USDC_BASE};
use st0x_execution::{
    Direction, ExecutorOrderId, FractionalShares, MockExecutor, OrderState, Positive, Symbol,
};
use st0x_float_macro::float;
use st0x_float_serde::format_float_with_fallback;
use st0x_registry::SymbolCache;

use super::{ExpectedEvent, StoredEvent, assert_events, fetch_events};
use crate::bindings::IRaindexV6::{self, TakeOrderV3};
use crate::bindings::{
    DeployableERC20, Deployer, Interpreter, Parser, RaindexV6, Store as RainStore,
};
use crate::conductor::{
    AccumulatedPositionExecutionCtx, TradeProcessingCqrs, VaultDiscoveryCtx,
    check_and_execute_accumulated_positions, discover_vaults_for_trade, process_queued_trade,
};
use crate::offchain::order::{
    ExecutorOrderPlacer, OffchainOrder, OffchainOrderCommand, OffchainOrderId,
};
use crate::onchain::OnchainTrade;
use crate::onchain::pyth::PythFeedIds;
use crate::onchain::trade::RaindexTradeEvent;
use crate::position::{Position, PositionCommand};
use crate::test_utils::{
    TEST_POLL_INTERVAL, TestAnvilInstance, deploy_tofu_singleton, setup_test_pools, spawn_anvil,
};
use crate::trading::onchain::inclusion::EmittedOnChain;
use crate::trading::onchain::trade_accountant::TradeAccountingError;
use crate::vault_registry::VaultRegistryId;

struct RetainedPollFill {
    shares_filled: Positive<FractionalShares>,
    executor_order_id: ExecutorOrderId,
    price: st0x_finance::Usd,
    broker_timestamp: DateTime<Utc>,
}

const TEST_AAPL: &str = "AAPL";
const TEST_MSFT: &str = "MSFT";
const AAPL_PRICE: u32 = 100;
const MSFT_PRICE: u32 = 200;

fn nth_matching_event<'event>(
    events: &'event [StoredEvent],
    aggregate_type: &str,
    aggregate_id: &str,
    event_type: &str,
    ordinal: usize,
) -> &'event StoredEvent {
    events
        .iter()
        .filter(|event| {
            event.aggregate_type == aggregate_type
                && event.aggregate_id == aggregate_id
                && event.event_type == event_type
        })
        .nth(ordinal)
        .unwrap_or_else(|| {
            panic!(
                "expected {aggregate_type}/{aggregate_id}/{event_type} event \
                 at ordinal {ordinal}"
            )
        })
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

/// Loads a position and asserts it matches the expected field values.
///
/// The `last_updated` timestamp is non-deterministic and is asserted against the loaded value.
#[bon::builder]
async fn assert_position(
    query: &Projection<Position>,
    symbol: &Symbol,
    net: FractionalShares,
    accumulated_long: FractionalShares,
    accumulated_short: FractionalShares,
    #[builder(required)] pending: Option<OffchainOrderId>,
    last_price_usdc: Float,
) {
    let position = query
        .load(symbol)
        .await
        .unwrap()
        .expect("Position should exist");
    assert_eq!(position.symbol, symbol.clone());
    assert_eq!(position.threshold, ExecutionThreshold::whole_share());
    assert_eq!(position.net, net);
    assert_eq!(position.accumulated_long, accumulated_long);
    assert_eq!(position.accumulated_short, accumulated_short);
    assert_eq!(position.pending_offchain_order_id, pending);
    let price = position
        .last_price_usdc
        .expect("last_price_usdc should be Some");
    assert!(price.eq(last_price_usdc).unwrap());
}

/// A trade produced by a real OrderBook take-order on Anvil, containing the parsed
/// `OnchainTrade` and a matching `ChainIncluded` event ready for CQRS processing.
struct AnvilTrade {
    trade: OnchainTrade,
    trade_event: EmittedOnChain<RaindexTradeEvent>,
    tx_hash: B256,
    log_index: u64,
    input_vault_id: B256,
    output_vault_id: B256,
}

impl AnvilTrade {
    fn aggregate_id(&self) -> String {
        format!("{}:{}", self.tx_hash, self.log_index)
    }

    async fn submit(
        &self,
        cqrs: &TradeProcessingCqrs,
    ) -> Result<Option<OffchainOrderId>, TradeAccountingError> {
        let executor = MockExecutor::default();
        process_queued_trade(&executor, &self.trade_event, self.trade.clone(), cqrs, true).await
    }
}

/// Drives every submitted offchain order through one polling cycle by asking
/// `executor` for status and applying the same CQRS commands the
/// [`PollOrderStatus`]/[`ReconcileOrderFill`]/[`HandleOrderRejection`] job
/// chain would emit. Keeps integration tests focused on the broker→CQRS flow
/// without spinning up the full apalis worker monitor.
///
/// [`PollOrderStatus`]: crate::offchain::order::PollOrderStatus
/// [`ReconcileOrderFill`]: crate::offchain::order::ReconcileOrderFill
/// [`HandleOrderRejection`]: crate::offchain::order::HandleOrderRejection
async fn poll_submitted_orders<E: st0x_execution::Executor + Clone>(
    executor: &E,
    offchain_order_projection: &Projection<OffchainOrder>,
    offchain_order: &Arc<Store<crate::offchain::order::OffchainOrder>>,
    position: &Arc<Store<Position>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Match the production poll path which polls both Submitted and
    // PartiallyFilled orders. `filter` only takes one value, so load all
    // and reject the irrelevant states in Rust.
    let pollable: Vec<_> = offchain_order_projection
        .load_all()
        .await?
        .into_iter()
        .filter(|(_, order)| {
            matches!(
                order,
                crate::offchain::order::OffchainOrder::Submitted { .. }
                    | crate::offchain::order::OffchainOrder::PartiallyFilled { .. }
            )
        })
        .collect();

    for (order_id, order) in pollable {
        let Some(executor_order_id) = order.executor_order_id() else {
            continue;
        };
        let parsed = executor
            .parse_order_id(executor_order_id.as_ref())
            .map_err(|error| format!("parse_order_id: {error}"))?;
        let state = executor
            .get_order_status(&parsed)
            .await
            .map_err(|error| format!("get_order_status: {error}"))?;

        use OrderState::{Cancelled, Failed, Filled, PartiallyFilled, Pending, Submitted};
        match state {
            Filled {
                price,
                order_id: broker_order_id,
                executed_at,
            } => {
                offchain_order
                    .send(
                        &order_id,
                        OffchainOrderCommand::CompleteFill {
                            price,
                            filled_at: executed_at,
                        },
                    )
                    .await?;

                position
                    .send(
                        order.symbol(),
                        PositionCommand::CompleteOffChainOrder {
                            offchain_order_id: order_id,
                            shares_filled: order.shares(),
                            direction: order.direction(),
                            executor_order_id: broker_order_id.clone(),
                            price,
                            broker_timestamp: executed_at,
                        },
                    )
                    .await?;
            }

            Failed {
                error_reason,
                shares_filled,
                avg_price,
                failed_at,
            } => {
                let error =
                    error_reason.unwrap_or_else(|| "Order failed with no error reason".to_string());
                let partial_fill = match shares_filled {
                    Some(shares_filled) => {
                        record_poll_partial_fill(
                            &order_id,
                            executor_order_id,
                            &order,
                            offchain_order,
                            shares_filled,
                            avg_price,
                            failed_at,
                        )
                        .await?
                    }
                    None => None,
                };

                if let Some(fill) = &partial_fill {
                    position
                        .send(
                            order.symbol(),
                            PositionCommand::CompleteOffChainOrder {
                                offchain_order_id: order_id,
                                shares_filled: fill.shares_filled,
                                direction: order.direction(),
                                executor_order_id: fill.executor_order_id.clone(),
                                price: fill.price,
                                broker_timestamp: fill.broker_timestamp,
                            },
                        )
                        .await?;
                }

                offchain_order
                    .send(
                        &order_id,
                        OffchainOrderCommand::MarkFailed {
                            error: error.clone(),
                            failed_at,
                        },
                    )
                    .await?;

                if partial_fill.is_none() {
                    position
                        .send(
                            order.symbol(),
                            PositionCommand::FailOffChainOrder {
                                offchain_order_id: order_id,
                                error,
                            },
                        )
                        .await?;
                }
            }

            // This helper cannot faithfully replay a broker cancellation:
            // the production path (recover_unrequested_cancellation) drives
            // CancelOrder + ConfirmCancellation through the aggregate's
            // OrderPlacer services, but this store is wired with its own
            // MockExecutor whose status reads would reconcile against the
            // wrong broker state. Fail loudly so a future test that needs
            // cancellations drives the real PollOrderStatus job instead of
            // silently leaving the position stuck.
            Cancelled { .. } => {
                return Err(format!(
                    "poll_submitted_orders does not support broker cancellations \
                     (order {order_id}); drive the real PollOrderStatus job instead"
                )
                .into());
            }

            PartiallyFilled {
                shares_filled,
                avg_price,
                partially_filled_at,
                ..
            } => {
                record_poll_partial_fill(
                    &order_id,
                    executor_order_id,
                    &order,
                    offchain_order,
                    shares_filled,
                    avg_price,
                    partially_filled_at,
                )
                .await?;
            }

            Pending | Submitted { .. } => {}
        }
    }

    Ok(())
}

async fn record_poll_partial_fill(
    order_id: &OffchainOrderId,
    executor_order_id: &ExecutorOrderId,
    order: &OffchainOrder,
    offchain_order: &Arc<Store<OffchainOrder>>,
    shares_filled: FractionalShares,
    avg_price: Option<st0x_finance::Usd>,
    broker_timestamp: DateTime<Utc>,
) -> Result<Option<RetainedPollFill>, Box<dyn std::error::Error>> {
    if shares_filled == FractionalShares::ZERO {
        return Ok(None);
    }

    let Some(avg_price) = avg_price else {
        return Err(format!("order {order_id} reported shares_filled without avg_price").into());
    };

    if !matches!(
        order,
        OffchainOrder::PartiallyFilled {
            shares_filled: current_shares_filled,
            avg_price: current_avg_price,
            ..
        } if *current_shares_filled == shares_filled && *current_avg_price == avg_price
    ) {
        offchain_order
            .send(
                order_id,
                OffchainOrderCommand::UpdatePartialFill {
                    shares_filled,
                    avg_price,
                    partially_filled_at: broker_timestamp,
                },
            )
            .await?;
    }

    Ok(Some(RetainedPollFill {
        shares_filled: Positive::new(shares_filled)
            .map_err(|error| format!("order {order_id} reported invalid partial fill: {error}"))?,
        executor_order_id: executor_order_id.clone(),
        price: avg_price,
        broker_timestamp,
    }))
}

/// Holds a deployed Rain OrderBook on a local Anvil node, ready to create real take-order events.
struct AnvilOrderBook<P> {
    _anvil: TestAnvilInstance,
    provider: P,
    orderbook_addr: Address,
    deployer_addr: Address,
    interpreter_addr: Address,
    store_addr: Address,
    owner: Address,
    usdc_addr: Address,
    equity_tokens: HashMap<String, Address>,
    symbol_cache: SymbolCache,
    pyth_feed_ids: PythFeedIds,
}

/// Places USDC contract code and storage directly at the canonical `USDC_BASE` address
/// via Anvil cheat-codes. The system hardcodes this address for vault discovery, so the
/// contract must live at that exact address -- a normal deploy would land elsewhere.
///
/// Initializes the OpenZeppelin ERC20 storage layout: totalSupply, name ("USD Coin"),
/// symbol ("USDC"), decimals (6), and balance for `owner`.
async fn deploy_usdc_at_base<P: Provider>(provider: &P, owner: Address) {
    let total_supply = U256::from(1_000_000_000_000u64);

    provider
        .anvil_set_code(USDC_BASE, DeployableERC20::DEPLOYED_BYTECODE.clone())
        .await
        .unwrap();

    // Slot 2: _totalSupply
    provider
        .anvil_set_storage_at(USDC_BASE, U256::from(2), total_supply.into())
        .await
        .unwrap();

    // Slot 3: _name = "USD Coin" (Solidity short-string: data left-aligned, len*2 in last byte)
    let mut name_bytes = [0u8; 32];
    name_bytes[..8].copy_from_slice(b"USD Coin");
    name_bytes[31] = 16;
    provider
        .anvil_set_storage_at(USDC_BASE, U256::from(3), B256::from(name_bytes))
        .await
        .unwrap();

    // Slot 4: _symbol = "USDC" (Solidity short-string encoding)
    let mut symbol_bytes = [0u8; 32];
    symbol_bytes[..4].copy_from_slice(b"USDC");
    symbol_bytes[31] = 8;
    provider
        .anvil_set_storage_at(USDC_BASE, U256::from(4), B256::from(symbol_bytes))
        .await
        .unwrap();

    // Slot 5: _decimals = 6
    provider
        .anvil_set_storage_at(USDC_BASE, U256::from(5), U256::from(6).into())
        .await
        .unwrap();

    // _balances[owner]: keccak256(abi.encode(owner, 0)) where 0 is the balances mapping slot
    let mut slot_key = [0u8; 64];
    slot_key[12..32].copy_from_slice(owner.as_slice());
    let balance_slot = U256::from_be_bytes(keccak256(slot_key).0);
    provider
        .anvil_set_storage_at(USDC_BASE, balance_slot, total_supply.into())
        .await
        .unwrap();
}

/// Etches the Rainlang interpreter, store, parser, and expression deployer
/// runtimes at their deterministic deployment addresses. The expression deployer
/// hardcodes these addresses, so order evaluation only works when each runtime
/// lives at its canonical address -- the alloy/anvil equivalent of upstream's
/// `LibInterpreterDeploy.etchRainlang`.
async fn etch_rainlang<P: Provider>(provider: &P) {
    provider
        .anvil_set_code(RAINLANG_INTERPRETER, Interpreter::DEPLOYED_BYTECODE.clone())
        .await
        .unwrap();
    provider
        .anvil_set_code(RAINLANG_STORE, RainStore::DEPLOYED_BYTECODE.clone())
        .await
        .unwrap();
    provider
        .anvil_set_code(RAINLANG_PARSER, Parser::DEPLOYED_BYTECODE.clone())
        .await
        .unwrap();
    provider
        .anvil_set_code(
            RAINLANG_EXPRESSION_DEPLOYER,
            Deployer::DEPLOYED_BYTECODE.clone(),
        )
        .await
        .unwrap();
}

async fn setup_anvil_orderbook() -> AnvilOrderBook<impl alloy::providers::Provider + Clone> {
    let anvil = spawn_anvil(Anvil::new());
    let endpoint = anvil.endpoint();
    let key = B256::from_slice(&anvil.keys()[0].to_bytes());
    let signer = PrivateKeySigner::from_bytes(&key).unwrap();
    let wallet = EthereumWallet::from(signer.clone());
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect(&endpoint)
        .await
        .unwrap();
    let owner = signer.address();

    deploy_tofu_singleton(&provider).await;

    etch_rainlang(&provider).await;

    let orderbook = RaindexV6::deploy(&provider).await.unwrap();

    // Place USDC contract directly at USDC_BASE so vault discovery recognizes it.
    deploy_usdc_at_base(&provider, owner).await;

    // Extract addresses before moving provider into the struct
    let orderbook_addr = *orderbook.address();
    let deployer_addr = RAINLANG_EXPRESSION_DEPLOYER;
    let interpreter_addr = RAINLANG_INTERPRETER;
    let store_addr = RAINLANG_STORE;

    AnvilOrderBook {
        _anvil: anvil,
        provider,
        orderbook_addr,
        deployer_addr,
        interpreter_addr,
        store_addr,
        owner,
        usdc_addr: USDC_BASE,
        equity_tokens: HashMap::new(),
        symbol_cache: SymbolCache::default(),
        pyth_feed_ids: PythFeedIds::default(),
    }
}

#[bon::bon]
impl<P: Provider + Clone + Send + Sync + 'static> AnvilOrderBook<P> {
    /// Deploys an equity token for the given symbol if one doesn't already exist.
    /// Returns the token address (newly deployed or cached from a previous call).
    async fn ensure_equity_token(&mut self, symbol: &str) -> Address {
        if !self.equity_tokens.contains_key(symbol) {
            let equity = DeployableERC20::deploy(
                &self.provider,
                format!("{symbol} Tokenized"),
                format!("wt{symbol}"),
                18,
                self.owner,
                parse_units("1000000", 18).unwrap().into(),
            )
            .await
            .unwrap();

            self.equity_tokens
                .insert(symbol.to_string(), *equity.address());
        }

        self.equity_tokens[symbol]
    }

    /// Parses a `TakeOrderV3` log through the full `OnchainTrade` pipeline and
    /// wraps the result into an `AnvilTrade` ready for CQRS processing.
    #[builder]
    async fn take_log_to_anvil_trade(
        &self,
        take_log: &Log,
        order_owner: Address,
        input_vault_id: B256,
        output_vault_id: B256,
    ) -> AnvilTrade {
        let take_event = take_log.log_decode::<TakeOrderV3>().unwrap().data().clone();
        let take_event_for_queue = take_event.clone();

        let log_metadata = Log {
            inner: take_log.inner.clone(),
            block_hash: take_log.block_hash,
            block_number: take_log.block_number,
            block_timestamp: take_log.block_timestamp,
            transaction_hash: take_log.transaction_hash,
            transaction_index: take_log.transaction_index,
            log_index: take_log.log_index,
            removed: false,
        };

        let evm = ReadOnlyEvm::new(self.provider.clone());
        let trade = OnchainTrade::try_from_take_order_if_target_owner(
            &self.symbol_cache,
            &evm,
            take_event,
            log_metadata,
            order_owner,
            &self.pyth_feed_ids,
        )
        .await
        .unwrap()
        .expect("Pipeline should produce an OnchainTrade");

        let tx_hash = trade.tx_hash;
        let log_index = trade.log_index;

        let trade_event = EmittedOnChain {
            tx_hash,
            log_index,
            block_number: take_log.block_number.unwrap(),
            event: RaindexTradeEvent::TakeOrderV3(Box::new(take_event_for_queue)),
            block_timestamp: trade.block_timestamp,
        };

        AnvilTrade {
            trade,
            trade_event,
            tx_hash,
            log_index,
            input_vault_id,
            output_vault_id,
        }
    }

    /// Creates an order on the real OrderBook, takes it, and parses the resulting
    /// `TakeOrderV3` event through the full `OnchainTrade` pipeline.
    ///
    /// Equity tokens are deployed on first use per symbol and reused for subsequent calls.
    #[builder]
    async fn take_order(
        &mut self,
        symbol: &str,
        amount: Float,
        direction: Direction,
        price: u32,
    ) -> AnvilTrade {
        // Mutable borrow must happen before creating orderbook/deployer instances
        // which hold immutable references to self.provider.
        let equity_addr = self.ensure_equity_token(symbol).await;

        let orderbook = IRaindexV6::IRaindexV6Instance::new(self.orderbook_addr, &self.provider);
        let deployer_instance = Deployer::DeployerInstance::new(self.deployer_addr, &self.provider);

        let is_sell = direction == Direction::Sell;
        let usdc_addr = self.usdc_addr;
        let usdc_total = (amount * float!(&price.to_string())).unwrap();

        let amount_str = format_float_with_fallback(&amount);
        let usdc_total_str = format_float_with_fallback(&usdc_total);

        // Order: input = what order receives, output = what order gives
        let (input_token, output_token) = if is_sell {
            (usdc_addr, equity_addr)
        } else {
            (equity_addr, usdc_addr)
        };

        // Expression: maxAmount (output in base units) and ioRatio (as Float)
        // Sell: output = equity, input = USDC, ioRatio = price (USDC per equity)
        // Buy:  output = USDC, input = equity, ioRatio = 1/price (equity per USDC)
        // Rain's parser supports decimal literals (e.g. "0.01"), so we compute
        // the reciprocal price as a decimal string.
        let (max_amount_base, io_ratio_str) = if is_sell {
            let base: U256 = parse_units(&amount_str, 18).unwrap().into();
            (base, price.to_string())
        } else {
            let base: U256 = parse_units(&usdc_total_str, 6).unwrap().into();
            let reciprocal = 1.0 / f64::from(price);
            (base, format!("{reciprocal}"))
        };
        let expression = format!("_ _: {max_amount_base} {io_ratio_str};:;");

        let parsed_bytecode = deployer_instance
            .parse2(Bytes::copy_from_slice(expression.as_bytes()))
            .call()
            .await
            .unwrap()
            .0;

        // Each order gets unique vault IDs to prevent vault balance leaking between
        // orders. Input and output use distinct IDs so tests can verify correct
        // mapping in vault discovery (e.g. USDC vault_id != equity vault_id).
        let input_vault_id = B256::random();
        let output_vault_id = B256::random();

        let order_config = IRaindexV6::OrderConfigV4 {
            evaluable: IRaindexV6::EvaluableV4 {
                interpreter: self.interpreter_addr,
                store: self.store_addr,
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

        let add_order_receipt = orderbook
            .addOrder4(order_config, vec![])
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        let add_order_event = add_order_receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| log.log_decode::<IRaindexV6::AddOrderV3>().ok())
            .expect("AddOrderV3 event not found");
        let order = add_order_event.data().order.clone();

        // Deposit output token into the order's output vault
        let deposit_amount_str = if is_sell {
            &amount_str
        } else {
            &usdc_total_str
        };
        let deposit_micro: U256 = parse_units(deposit_amount_str, 6).unwrap().into();
        let deposit_float = Float::from_fixed_decimal_lossy(deposit_micro, 6)
            .unwrap()
            .0
            .get_inner();

        let deposit_approve: U256 = if is_sell {
            parse_units(&amount_str, 18).unwrap().into()
        } else {
            parse_units(&usdc_total_str, 6).unwrap().into()
        };

        DeployableERC20::new(output_token, &self.provider)
            .approve(*orderbook.address(), deposit_approve)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        orderbook
            .deposit4(output_token, output_vault_id, deposit_float, vec![])
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        // Approve taker's payment (input token from order's perspective)
        let taker_approve: U256 = if is_sell {
            parse_units(&usdc_total_str, 6).unwrap().into()
        } else {
            parse_units(&amount_str, 18).unwrap().into()
        };

        DeployableERC20::new(input_token, &self.provider)
            .approve(*orderbook.address(), taker_approve)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        // Take the order with permissive bounds (large maximumIO/maximumIORatio)
        let take_config = IRaindexV6::TakeOrdersConfigV5 {
            minimumIO: B256::ZERO,
            maximumIO: Float::from_fixed_decimal_lossy(U256::from(1_000_000), 0)
                .unwrap()
                .0
                .get_inner(),
            maximumIORatio: Float::from_fixed_decimal_lossy(U256::from(1_000_000), 0)
                .unwrap()
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
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        assert!(
            take_receipt.status(),
            "takeOrders4 reverted. Logs: {:?}",
            take_receipt.inner.logs()
        );

        let take_log = take_receipt
            .inner
            .logs()
            .iter()
            .find(|log| log.topic0() == Some(&TakeOrderV3::SIGNATURE_HASH))
            .expect("TakeOrderV3 event not found");

        self.take_log_to_anvil_trade()
            .take_log(take_log)
            .order_owner(order.owner)
            .input_vault_id(input_vault_id)
            .output_vault_id(output_vault_id)
            .call()
            .await
    }
}

/// Constructs the CQRS frameworks needed by the integration tests.
///
/// Uses `ExecutorOrderPlacer(MockExecutor::new())` so that the `PlaceOrder`
/// command atomically calls the mock executor and emits `Placed` + `Submitted`.
///
/// Creates a `Projection` wired to the `StoreBuilder` so it receives event
/// dispatches, while the same projection is returned for type-safe loading.
async fn create_test_cqrs(
    pool: &SqlitePool,
    apalis_pool: &apalis_sqlite::SqlitePool,
) -> (
    TradeProcessingCqrs,
    Arc<Store<Position>>,
    Arc<Projection<Position>>,
    Arc<Store<crate::offchain::order::OffchainOrder>>,
    Arc<Projection<OffchainOrder>>,
) {
    create_test_cqrs_with_assets(
        pool,
        apalis_pool,
        AssetsConfig {
            equities: EquitiesConfig {
                operational_limit: None,
                ..EquitiesConfig::default()
            },
            cash: None,
        },
    )
    .await
}

async fn create_test_cqrs_with_assets(
    pool: &SqlitePool,
    apalis_pool: &apalis_sqlite::SqlitePool,
    assets: AssetsConfig,
) -> (
    TradeProcessingCqrs,
    Arc<Store<Position>>,
    Arc<Projection<Position>>,
    Arc<Store<crate::offchain::order::OffchainOrder>>,
    Arc<Projection<OffchainOrder>>,
) {
    let onchain_trade = Arc::new(test_store(pool.clone(), ()));

    let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
        .build(())
        .await
        .unwrap();

    let order_placer: Arc<dyn crate::offchain::order::OrderPlacer> =
        Arc::new(ExecutorOrderPlacer(MockExecutor::new()));

    let (offchain_order, offchain_order_projection) =
        StoreBuilder::<OffchainOrder>::new(pool.clone())
            .build(order_placer.clone())
            .await
            .unwrap();

    let cqrs = TradeProcessingCqrs {
        pool: pool.clone(),
        onchain_trade,
        position: position.clone(),
        position_projection: position_projection.clone(),
        offchain_order: offchain_order.clone(),
        execution_threshold: ExecutionThreshold::whole_share(),
        assets,
        counter_trade_submission_lock: Arc::new(tokio::sync::Mutex::new(())),
        poll_status_queue: crate::offchain::order::PollOrderStatusJobQueue::new(apalis_pool),
        order_placer,
        hedge_queue: crate::trading::offchain::hedge::HedgeJobQueue::new(apalis_pool),
        poll_interval: TEST_POLL_INTERVAL,
    };

    (
        cqrs,
        position,
        position_projection,
        offchain_order,
        offchain_order_projection,
    )
}

#[tokio::test]
async fn onchain_trades_accumulate_and_trigger_offchain_fill()
-> Result<(), Box<dyn std::error::Error>> {
    let mut orderbook = setup_anvil_orderbook().await;
    let (pool, apalis_pool) = setup_test_pools().await;
    let (cqrs, position, position_query, offchain_order, offchain_order_projection) =
        create_test_cqrs(&pool, &apalis_pool).await;
    let symbol = Symbol::new(TEST_AAPL).unwrap();

    // Checkpoint 1: before any trades -- no Position aggregate exists
    assert!(position_query.load(&symbol).await?.is_none());

    // Trade 1: 0.5 shares Sell, below whole-share threshold
    let trade1 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(0.5))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade1_agg = trade1.aggregate_id();
    let result1 = trade1.submit(&cqrs).await?;
    assert!(
        result1.is_none(),
        "No execution should be created below threshold"
    );

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-0.5)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(0.5)))
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    let mut expected = vec![
        ExpectedEvent::new("OnChainTrade", &trade1_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::Initialized"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
        ExpectedEvent::new(
            "OnChainTrade",
            &trade1_agg,
            "OnChainTradeEvent::Acknowledged",
        ),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
    ];
    assert_events(&pool, &expected).await;

    // Trade 2: 0.7 shares Sell, total net = -1.2, crosses threshold
    let trade2 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(0.7))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade2_agg = trade2.aggregate_id();
    let order_id = trade2
        .submit(&cqrs)
        .await?
        .expect("Threshold crossed, should return OffchainOrderId");
    let order_id_str = order_id.to_string();

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-1.2)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(1.2)))
        .pending(Some(order_id))
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    expected.extend([
        ExpectedEvent::new("OnChainTrade", &trade2_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
        ExpectedEvent::new(
            "OnChainTrade",
            &trade2_agg,
            "OnChainTradeEvent::Acknowledged",
        ),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new(
            "OffchainOrder",
            &order_id_str,
            "OffchainOrderEvent::Accepted",
        ),
    ]);
    let events = assert_events(&pool, &expected).await;

    // Payload spot-checks: financial values in OnChainOrderFilled events
    let trade1_filled = &nth_matching_event(
        &events,
        "Position",
        TEST_AAPL,
        "PositionEvent::OnChainOrderFilled",
        0,
    )
    .payload["OnChainOrderFilled"];
    assert_eq!(trade1_filled["amount"].as_str().unwrap(), "0.5");
    assert_eq!(trade1_filled["direction"].as_str().unwrap(), "sell");
    assert_eq!(trade1_filled["price_usdc"].as_str().unwrap(), "100");

    assert_eq!(events[7].event_type, "PositionEvent::OnChainOrderFilled");
    let trade2_filled = &events[7].payload["OnChainOrderFilled"];
    assert_eq!(trade2_filled["amount"].as_str().unwrap(), "0.7");
    assert_eq!(trade2_filled["direction"].as_str().unwrap(), "sell");
    assert_eq!(trade2_filled["price_usdc"].as_str().unwrap(), "100");

    // Payload spot-checks: OffChainOrderPlaced and Placed shares/direction
    assert_eq!(events[11].event_type, "PositionEvent::OffChainOrderPlaced");
    let placed_pos = &events[11].payload["OffChainOrderPlaced"];
    assert_eq!(
        placed_pos["offchain_order_id"].as_str().unwrap(),
        order_id_str
    );
    assert_eq!(placed_pos["direction"].as_str().unwrap(), "buy");
    assert_eq!(placed_pos["shares"].as_str().unwrap(), "1.2");

    assert_eq!(events[12].event_type, "OffchainOrderEvent::Placed");
    let offchain_placed = &events[12].payload["Placed"];
    assert_eq!(offchain_placed["symbol"].as_str().unwrap(), TEST_AAPL);
    assert_eq!(offchain_placed["direction"].as_str().unwrap(), "buy");
    assert_eq!(offchain_placed["shares"].as_str().unwrap(), "1.2");

    // Fulfillment: order poller detects the filled order and completes the lifecycle
    poll_submitted_orders(
        &MockExecutor::new(),
        &offchain_order_projection,
        &offchain_order,
        &position,
    )
    .await?;

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::ZERO)
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(1.2)))
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    expected.extend([
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Filled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OffChainOrderFilled"),
    ]);
    let events = assert_events(&pool, &expected).await;
    assert_eq!(events[15].event_type, "PositionEvent::OffChainOrderFilled");
    assert_eq!(
        events[15].payload["OffChainOrderFilled"]["offchain_order_id"]
            .as_str()
            .unwrap(),
        order_id_str,
    );

    Ok(())
}

/// Tests the recovery path: accumulate -> threshold -> execute -> broker fails ->
/// poller handles failure -> position checker picks up unexecuted position -> retry -> fill.
#[tokio::test]
async fn position_checker_recovers_failed_execution() -> Result<(), Box<dyn std::error::Error>> {
    let mut orderbook = setup_anvil_orderbook().await;
    let (pool, apalis_pool) = setup_test_pools().await;
    let (cqrs, position, position_query, offchain_order, offchain_order_projection) =
        create_test_cqrs(&pool, &apalis_pool).await;
    let symbol = Symbol::new(TEST_AAPL).unwrap();

    // Two trades: 0.5 + 0.7 = 1.2 shares, crosses threshold
    let trade1 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(0.5))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade1_agg = trade1.aggregate_id();
    trade1.submit(&cqrs).await?;

    let trade2 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(0.7))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade2_agg = trade2.aggregate_id();
    let order_id = trade2.submit(&cqrs).await?.expect("Threshold crossed");
    let order_id_str = order_id.to_string();

    // Poller discovers the broker FAILED the order
    let failed_executor = MockExecutor::new().with_order_status(OrderState::Failed {
        failed_at: Utc::now(),
        error_reason: Some("Broker rejected order".to_string()),
        shares_filled: None,
        avg_price: None,
    });
    poll_submitted_orders(
        &failed_executor,
        offchain_order_projection.as_ref(),
        &offchain_order,
        &position,
    )
    .await?;

    // After failure: pending cleared, position still has net exposure
    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-1.2)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(1.2)))
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    let mut expected = vec![
        ExpectedEvent::new("OnChainTrade", &trade1_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::Initialized"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
        ExpectedEvent::new(
            "OnChainTrade",
            &trade1_agg,
            "OnChainTradeEvent::Acknowledged",
        ),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
        ExpectedEvent::new("OnChainTrade", &trade2_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
        ExpectedEvent::new(
            "OnChainTrade",
            &trade2_agg,
            "OnChainTradeEvent::Acknowledged",
        ),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new(
            "OffchainOrder",
            &order_id_str,
            "OffchainOrderEvent::Accepted",
        ),
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Failed"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OffChainOrderFailed"),
    ];
    let events = assert_events(&pool, &expected).await;
    assert_eq!(events[12].event_type, "OffchainOrderEvent::Placed");
    let placed = &events[12].payload["Placed"];
    assert_eq!(placed["symbol"].as_str().unwrap(), TEST_AAPL);
    assert_eq!(placed["direction"].as_str().unwrap(), "buy");

    // Position checker finds the unexecuted position and retries
    check_and_execute_accumulated_positions(
        &MockExecutor::new(),
        AccumulatedPositionExecutionCtx {
            position: &position,
            position_projection: &position_query,
            offchain_order: &offchain_order,
            order_placer: cqrs.order_placer.as_ref(),
            counter_trade_submission_lock: &cqrs.counter_trade_submission_lock,
            threshold: &ExecutionThreshold::whole_share(),
            assets: &AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },
        },
        |_| true,
    )
    .await?;

    poll_submitted_orders(
        &MockExecutor::new(),
        &offchain_order_projection,
        &offchain_order,
        &position,
    )
    .await?;

    // Final: position fully hedged
    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::ZERO)
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(1.2)))
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    // Extract retry order ID from the new OffchainOrderEvent::Placed event
    let all_events = fetch_events(&pool).await;
    assert_eq!(all_events[17].event_type, "OffchainOrderEvent::Placed");
    let retry_id = all_events[17].aggregate_id.clone();
    assert_ne!(
        retry_id, order_id_str,
        "Retry should create a new offchain order"
    );

    expected.extend([
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &retry_id, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new("OffchainOrder", &retry_id, "OffchainOrderEvent::Accepted"),
        ExpectedEvent::new("OffchainOrder", &retry_id, "OffchainOrderEvent::Filled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OffChainOrderFilled"),
    ]);
    assert_events(&pool, &expected).await;

    Ok(())
}

#[tokio::test]
async fn retains_failed_terminal_partial_fill() -> Result<(), Box<dyn std::error::Error>> {
    let mut orderbook = setup_anvil_orderbook().await;
    let (pool, apalis_pool) = setup_test_pools().await;
    let (cqrs, position, position_query, offchain_order, offchain_order_projection) =
        create_test_cqrs(&pool, &apalis_pool).await;
    let symbol = Symbol::new(TEST_AAPL).unwrap();

    let trade = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(1.2))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let order_id = trade.submit(&cqrs).await?.expect("Threshold crossed");

    let partially_filled_executor =
        MockExecutor::new().with_order_status(OrderState::PartiallyFilled {
            order_id: ExecutorOrderId::new("broker-order-id"),
            shares_filled: FractionalShares::new(float!(0.4)),
            avg_price: Some(st0x_finance::Usd::new(float!(100.25))),
            partially_filled_at: Utc::now(),
        });
    poll_submitted_orders(
        &partially_filled_executor,
        offchain_order_projection.as_ref(),
        &offchain_order,
        &position,
    )
    .await?;

    let order = offchain_order
        .load(&order_id)
        .await?
        .expect("offchain order should exist");
    assert!(
        matches!(order, OffchainOrder::PartiallyFilled { .. }),
        "poll helper must retain partial fill details before terminal state"
    );
    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-1.2)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(1.2)))
        .pending(Some(order_id))
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    let failed_executor = MockExecutor::new().with_order_status(OrderState::Failed {
        failed_at: Utc::now(),
        error_reason: Some("broker rejected after partial fill".to_string()),
        shares_filled: Some(FractionalShares::new(float!(0.4))),
        avg_price: Some(st0x_finance::Usd::new(float!(100.25))),
    });
    poll_submitted_orders(
        &failed_executor,
        offchain_order_projection.as_ref(),
        &offchain_order,
        &position,
    )
    .await?;

    let order = offchain_order
        .load(&order_id)
        .await?
        .expect("offchain order should exist");
    assert!(
        matches!(order, OffchainOrder::Failed { .. }),
        "terminal partial fill must still mark the offchain order failed"
    );
    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-0.8)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(1.2)))
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    Ok(())
}

/// Tests that two symbols processed through the pipeline don't contaminate each other's
/// Position state or event streams. Initial submissions are concurrent to verify
/// different-symbol trades can be processed in parallel without interference.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn multi_symbol_isolation() -> Result<(), Box<dyn std::error::Error>> {
    let mut orderbook = setup_anvil_orderbook().await;
    let (pool, apalis_pool) = setup_test_pools().await;
    let (cqrs, position, position_query, offchain_order, offchain_order_projection) =
        create_test_cqrs(&pool, &apalis_pool).await;
    let aapl = Symbol::new(TEST_AAPL).unwrap();
    let msft = Symbol::new(TEST_MSFT).unwrap();

    // Phase 1: concurrent submission of different-symbol trades, then AAPL accumulation
    let trade1 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(0.6))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade1_agg = trade1.aggregate_id();

    let trade2 = orderbook
        .take_order()
        .symbol(TEST_MSFT)
        .amount(float!(0.4))
        .direction(Direction::Sell)
        .price(MSFT_PRICE)
        .call()
        .await;
    let trade2_agg = trade2.aggregate_id();

    // Submit both below-threshold trades concurrently (different symbols)
    let (aapl_result, msft_result) = tokio::join!(trade1.submit(&cqrs), trade2.submit(&cqrs));
    assert!(aapl_result?.is_none());
    assert!(msft_result?.is_none());

    let trade3 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(0.6))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade3_agg = trade3.aggregate_id();
    let aapl_order_id = trade3
        .submit(&cqrs)
        .await?
        .expect("AAPL 1.2 crosses threshold");
    let aapl_order_str = aapl_order_id.to_string();

    assert_position()
        .query(&position_query)
        .symbol(&aapl)
        .net(FractionalShares::new(float!(-1.2)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(1.2)))
        .pending(Some(aapl_order_id))
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;
    assert_position()
        .query(&position_query)
        .symbol(&msft)
        .net(FractionalShares::new(float!(-0.4)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(0.4)))
        .pending(None)
        .last_price_usdc(float!(&MSFT_PRICE.to_string()))
        .call()
        .await;

    // Concurrent submissions produce non-deterministic global event ordering:
    // Position and OnChainTrade events for different symbols can fully interleave.
    // Verify the first 12 events contain the expected set regardless of order.
    let initial_events = fetch_events(&pool).await;
    let actual_initial: Vec<ExpectedEvent> = initial_events[..12]
        .iter()
        .map(|event| {
            ExpectedEvent::new(
                &event.aggregate_type,
                &event.aggregate_id,
                &event.event_type,
            )
        })
        .collect();

    let concurrent_set = [
        ExpectedEvent::new("OnChainTrade", &trade1_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::Initialized"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
        ExpectedEvent::new(
            "OnChainTrade",
            &trade1_agg,
            "OnChainTradeEvent::Acknowledged",
        ),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
        ExpectedEvent::new("OnChainTrade", &trade2_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", TEST_MSFT, "PositionEvent::Initialized"),
        ExpectedEvent::new("Position", TEST_MSFT, "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("Position", TEST_MSFT, "PositionEvent::OnChainFillApplied"),
        ExpectedEvent::new(
            "OnChainTrade",
            &trade2_agg,
            "OnChainTradeEvent::Acknowledged",
        ),
        ExpectedEvent::new("Position", TEST_MSFT, "PositionEvent::OnChainFillSettled"),
    ];
    for expected_event in &concurrent_set {
        assert!(
            actual_initial.contains(expected_event),
            "Missing concurrent event: {expected_event:?}\nActual: {actual_initial:?}"
        );
    }

    // Build the expected sequence using the actual ordering of the first 12 events
    let mut expected: Vec<ExpectedEvent> = actual_initial;

    expected.extend([
        ExpectedEvent::new("OnChainTrade", &trade3_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
        ExpectedEvent::new(
            "OnChainTrade",
            &trade3_agg,
            "OnChainTradeEvent::Acknowledged",
        ),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new(
            "OffchainOrder",
            &aapl_order_str,
            "OffchainOrderEvent::Placed",
        ),
        ExpectedEvent::new(
            "OffchainOrder",
            &aapl_order_str,
            "OffchainOrderEvent::Accepted",
        ),
    ]);
    let events = assert_events(&pool, &expected).await;

    assert_eq!(events[18].event_type, "OffchainOrderEvent::Placed");
    let aapl_placed = &events[18].payload["Placed"];
    assert_eq!(aapl_placed["symbol"].as_str().unwrap(), TEST_AAPL);
    assert_eq!(aapl_placed["direction"].as_str().unwrap(), "buy");
    assert_eq!(aapl_placed["shares"].as_str().unwrap(), "1.2");

    // Poll and fill AAPL, verify MSFT unchanged
    poll_submitted_orders(
        &MockExecutor::new(),
        &offchain_order_projection,
        &offchain_order,
        &position,
    )
    .await?;

    assert_position()
        .query(&position_query)
        .symbol(&aapl)
        .net(FractionalShares::ZERO)
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(1.2)))
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;
    assert_position()
        .query(&position_query)
        .symbol(&msft)
        .net(FractionalShares::new(float!(-0.4)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(0.4)))
        .pending(None)
        .last_price_usdc(float!(&MSFT_PRICE.to_string()))
        .call()
        .await;

    expected.extend([
        ExpectedEvent::new(
            "OffchainOrder",
            &aapl_order_str,
            "OffchainOrderEvent::Filled",
        ),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OffChainOrderFilled"),
    ]);
    assert_events(&pool, &expected).await;

    // MSFT crosses threshold (0.4 + 0.6 = 1.0, exactly at threshold)
    let trade4 = orderbook
        .take_order()
        .symbol(TEST_MSFT)
        .amount(float!(0.6))
        .direction(Direction::Sell)
        .price(MSFT_PRICE)
        .call()
        .await;
    let trade4_agg = trade4.aggregate_id();
    let msft_order_id = trade4
        .submit(&cqrs)
        .await?
        .expect("MSFT 1.0 hits threshold");
    let msft_order_str = msft_order_id.to_string();

    assert_position()
        .query(&position_query)
        .symbol(&msft)
        .net(FractionalShares::new(float!(-1.0)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(1.0)))
        .pending(Some(msft_order_id))
        .last_price_usdc(float!(&MSFT_PRICE.to_string()))
        .call()
        .await;
    assert_ne!(
        aapl_order_id, msft_order_id,
        "Separate offchain orders per symbol"
    );

    expected.extend([
        ExpectedEvent::new("OnChainTrade", &trade4_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", TEST_MSFT, "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("Position", TEST_MSFT, "PositionEvent::OnChainFillApplied"),
        ExpectedEvent::new(
            "OnChainTrade",
            &trade4_agg,
            "OnChainTradeEvent::Acknowledged",
        ),
        ExpectedEvent::new("Position", TEST_MSFT, "PositionEvent::OnChainFillSettled"),
        ExpectedEvent::new("Position", TEST_MSFT, "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new(
            "OffchainOrder",
            &msft_order_str,
            "OffchainOrderEvent::Placed",
        ),
        ExpectedEvent::new(
            "OffchainOrder",
            &msft_order_str,
            "OffchainOrderEvent::Accepted",
        ),
    ]);
    let events = assert_events(&pool, &expected).await;

    assert_eq!(events[28].event_type, "OffchainOrderEvent::Placed");
    let msft_placed = &events[28].payload["Placed"];
    assert_eq!(msft_placed["symbol"].as_str().unwrap(), TEST_MSFT);
    assert_eq!(msft_placed["direction"].as_str().unwrap(), "buy");
    assert_eq!(msft_placed["shares"].as_str().unwrap(), "1");

    // Poll and fill MSFT, both positions end fully hedged
    poll_submitted_orders(
        &MockExecutor::new(),
        &offchain_order_projection,
        &offchain_order,
        &position,
    )
    .await?;

    assert_position()
        .query(&position_query)
        .symbol(&aapl)
        .net(FractionalShares::ZERO)
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(1.2)))
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;
    assert_position()
        .query(&position_query)
        .symbol(&msft)
        .net(FractionalShares::ZERO)
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(1.0)))
        .pending(None)
        .last_price_usdc(float!(&MSFT_PRICE.to_string()))
        .call()
        .await;

    expected.extend([
        ExpectedEvent::new(
            "OffchainOrder",
            &msft_order_str,
            "OffchainOrderEvent::Filled",
        ),
        ExpectedEvent::new("Position", TEST_MSFT, "PositionEvent::OffChainOrderFilled"),
    ]);
    assert_events(&pool, &expected).await;

    Ok(())
}

/// Tests that Buy direction onchain trades accumulate `accumulated_long` and produce a
/// Sell hedge when the position crosses threshold.
#[tokio::test]
async fn buy_direction_accumulates_long() -> Result<(), Box<dyn std::error::Error>> {
    let mut orderbook = setup_anvil_orderbook().await;
    let (pool, apalis_pool) = setup_test_pools().await;
    let (cqrs, position, position_query, offchain_order, offchain_order_projection) =
        create_test_cqrs(&pool, &apalis_pool).await;
    let symbol = Symbol::new(TEST_AAPL).unwrap();

    // Trade 1: Buy 0.5 shares, below threshold
    let trade1 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(0.5))
        .direction(Direction::Buy)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade1_agg = trade1.aggregate_id();
    let result1 = trade1.submit(&cqrs).await?;
    assert!(result1.is_none(), "Below threshold");

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(0.5)))
        .accumulated_long(FractionalShares::new(float!(0.5)))
        .accumulated_short(FractionalShares::ZERO)
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    // Trade 2: Buy 0.7 shares, crosses threshold -> hedge is Sell
    let trade2 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(0.7))
        .direction(Direction::Buy)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade2_agg = trade2.aggregate_id();
    let order_id = trade2.submit(&cqrs).await?.expect("Threshold crossed");
    let order_id_str = order_id.to_string();

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(1.2)))
        .accumulated_long(FractionalShares::new(float!(1.2)))
        .accumulated_short(FractionalShares::ZERO)
        .pending(Some(order_id))
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    // Verify offchain order is Sell direction (hedge for long position)
    let mut expected = vec![
        ExpectedEvent::new("OnChainTrade", &trade1_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::Initialized"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
        ExpectedEvent::new(
            "OnChainTrade",
            &trade1_agg,
            "OnChainTradeEvent::Acknowledged",
        ),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
        ExpectedEvent::new("OnChainTrade", &trade2_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
        ExpectedEvent::new(
            "OnChainTrade",
            &trade2_agg,
            "OnChainTradeEvent::Acknowledged",
        ),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new(
            "OffchainOrder",
            &order_id_str,
            "OffchainOrderEvent::Accepted",
        ),
    ];
    let events = assert_events(&pool, &expected).await;

    // Verify financial values in OnChainOrderFilled events (Buy direction)
    let trade1_filled = &nth_matching_event(
        &events,
        "Position",
        TEST_AAPL,
        "PositionEvent::OnChainOrderFilled",
        0,
    )
    .payload["OnChainOrderFilled"];
    assert_eq!(trade1_filled["amount"].as_str().unwrap(), "0.5");
    assert_eq!(trade1_filled["direction"].as_str().unwrap(), "buy");
    assert_eq!(trade1_filled["price_usdc"].as_str().unwrap(), "100");

    assert_eq!(events[7].event_type, "PositionEvent::OnChainOrderFilled");
    let trade2_filled = &events[7].payload["OnChainOrderFilled"];
    assert_eq!(trade2_filled["amount"].as_str().unwrap(), "0.7");
    assert_eq!(trade2_filled["direction"].as_str().unwrap(), "buy");
    assert_eq!(trade2_filled["price_usdc"].as_str().unwrap(), "100");

    // Hedge direction should be Sell (opposite of onchain Buy), shares = abs(net)
    assert_eq!(events[12].event_type, "OffchainOrderEvent::Placed");
    assert_eq!(
        events[12].payload["Placed"]["direction"].as_str().unwrap(),
        "sell"
    );
    assert_eq!(
        events[12].payload["Placed"]["shares"].as_str().unwrap(),
        "1.2"
    );

    // Fill the hedge order
    poll_submitted_orders(
        &MockExecutor::new(),
        &offchain_order_projection,
        &offchain_order,
        &position,
    )
    .await?;

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::ZERO)
        .accumulated_long(FractionalShares::new(float!(1.2)))
        .accumulated_short(FractionalShares::ZERO)
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    expected.extend([
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Filled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OffChainOrderFilled"),
    ]);
    assert_events(&pool, &expected).await;

    Ok(())
}

/// Tests that a single trade of exactly 1.0 shares immediately triggers execution.
#[tokio::test]
async fn exact_threshold_triggers_execution() -> Result<(), Box<dyn std::error::Error>> {
    let mut orderbook = setup_anvil_orderbook().await;
    let (pool, apalis_pool) = setup_test_pools().await;
    let (cqrs, _position, position_query, _offchain_order, _) =
        create_test_cqrs(&pool, &apalis_pool).await;
    let symbol = Symbol::new(TEST_AAPL).unwrap();

    let trade1 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(1))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade1_agg = trade1.aggregate_id();
    let order_id = trade1
        .submit(&cqrs)
        .await?
        .expect("Exactly 1.0 should cross whole-share threshold");

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-1.0)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(1.0)))
        .pending(Some(order_id))
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    let order_id_str = order_id.to_string();
    let expected = vec![
        ExpectedEvent::new("OnChainTrade", &trade1_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::Initialized"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
        ExpectedEvent::new(
            "OnChainTrade",
            &trade1_agg,
            "OnChainTradeEvent::Acknowledged",
        ),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new(
            "OffchainOrder",
            &order_id_str,
            "OffchainOrderEvent::Accepted",
        ),
    ];
    let events = assert_events(&pool, &expected).await;

    // Payload spot-checks: financial values in the single-trade threshold crossing
    let filled = &nth_matching_event(
        &events,
        "Position",
        TEST_AAPL,
        "PositionEvent::OnChainOrderFilled",
        0,
    )
    .payload["OnChainOrderFilled"];
    assert_eq!(filled["amount"].as_str().unwrap(), "1");
    assert_eq!(filled["direction"].as_str().unwrap(), "sell");
    assert_eq!(filled["price_usdc"].as_str().unwrap(), "100");

    assert_eq!(events[6].event_type, "PositionEvent::OffChainOrderPlaced");
    let placed_pos = &events[6].payload["OffChainOrderPlaced"];
    assert_eq!(placed_pos["direction"].as_str().unwrap(), "buy");
    assert_eq!(placed_pos["shares"].as_str().unwrap(), "1");

    assert_eq!(events[7].event_type, "OffchainOrderEvent::Placed");
    let placed = &events[7].payload["Placed"];
    assert_eq!(placed["symbol"].as_str().unwrap(), TEST_AAPL);
    assert_eq!(placed["direction"].as_str().unwrap(), "buy");
    assert_eq!(placed["shares"].as_str().unwrap(), "1");

    Ok(())
}

/// Tests that the position checker is a no-op when all positions are already hedged.
#[tokio::test]
async fn position_checker_noop_when_hedged() -> Result<(), Box<dyn std::error::Error>> {
    let mut orderbook = setup_anvil_orderbook().await;
    let (pool, apalis_pool) = setup_test_pools().await;
    let (cqrs, position, position_query, offchain_order, offchain_order_projection) =
        create_test_cqrs(&pool, &apalis_pool).await;

    // Complete a full hedge cycle so position net=0
    let trade1 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(1))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    trade1.submit(&cqrs).await?.expect("Threshold crossed");
    poll_submitted_orders(
        &MockExecutor::new(),
        &offchain_order_projection,
        &offchain_order,
        &position,
    )
    .await?;

    let events_before = fetch_events(&pool).await;

    // Position checker should find nothing to do
    check_and_execute_accumulated_positions(
        &MockExecutor::new(),
        AccumulatedPositionExecutionCtx {
            position: &position,
            position_projection: &position_query,
            offchain_order: &offchain_order,
            order_placer: cqrs.order_placer.as_ref(),
            counter_trade_submission_lock: &cqrs.counter_trade_submission_lock,
            threshold: &ExecutionThreshold::whole_share(),
            assets: &AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },
        },
        |_| true,
    )
    .await?;

    let events_after = fetch_events(&pool).await;
    assert_eq!(
        events_before.len(),
        events_after.len(),
        "Position checker should not emit any new events when positions are hedged"
    );

    Ok(())
}

/// Tests that after completing a full hedge cycle, new trades can accumulate and trigger
/// a second hedge cycle.
#[tokio::test]
async fn second_hedge_after_full_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let mut orderbook = setup_anvil_orderbook().await;
    let (pool, apalis_pool) = setup_test_pools().await;
    let (cqrs, position, position_query, offchain_order, offchain_order_projection) =
        create_test_cqrs(&pool, &apalis_pool).await;
    let symbol = Symbol::new(TEST_AAPL).unwrap();

    // First cycle: 1.0 share sell -> hedge -> fill
    let trade1 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(1))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade1_agg = trade1.aggregate_id();
    let order1 = trade1
        .submit(&cqrs)
        .await?
        .expect("First threshold crossing");
    poll_submitted_orders(
        &MockExecutor::new(),
        &offchain_order_projection,
        &offchain_order,
        &position,
    )
    .await?;

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::ZERO)
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(1.0)))
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    // Second cycle: another 1.5 share sell -> crosses threshold again
    let trade2 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(1.5))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade2_agg = trade2.aggregate_id();
    let order2 = trade2
        .submit(&cqrs)
        .await?
        .expect("Second threshold crossing");
    assert_ne!(order1, order2, "Second cycle should create a new order");

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-1.5)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(2.5)))
        .pending(Some(order2))
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    poll_submitted_orders(
        &MockExecutor::new(),
        &offchain_order_projection,
        &offchain_order,
        &position,
    )
    .await?;

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::ZERO)
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(2.5)))
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    // Verify the full event sequence across both cycles
    let order1_str = order1.to_string();
    let order2_str = order2.to_string();
    let expected = vec![
        // First cycle
        ExpectedEvent::new("OnChainTrade", &trade1_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::Initialized"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
        ExpectedEvent::new(
            "OnChainTrade",
            &trade1_agg,
            "OnChainTradeEvent::Acknowledged",
        ),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &order1_str, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new("OffchainOrder", &order1_str, "OffchainOrderEvent::Accepted"),
        ExpectedEvent::new("OffchainOrder", &order1_str, "OffchainOrderEvent::Filled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OffChainOrderFilled"),
        // Second cycle
        ExpectedEvent::new("OnChainTrade", &trade2_agg, "OnChainTradeEvent::Filled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
        ExpectedEvent::new(
            "OnChainTrade",
            &trade2_agg,
            "OnChainTradeEvent::Acknowledged",
        ),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OffChainOrderPlaced"),
        ExpectedEvent::new("OffchainOrder", &order2_str, "OffchainOrderEvent::Placed"),
        ExpectedEvent::new("OffchainOrder", &order2_str, "OffchainOrderEvent::Accepted"),
        ExpectedEvent::new("OffchainOrder", &order2_str, "OffchainOrderEvent::Filled"),
        ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OffChainOrderFilled"),
    ];
    assert_events(&pool, &expected).await;

    Ok(())
}

#[tokio::test]
async fn take_order_discovers_equity_vault() -> Result<(), Box<dyn std::error::Error>> {
    let mut orderbook = setup_anvil_orderbook().await;
    let (pool, apalis_pool) = setup_test_pools().await;
    let (cqrs, _, _, _, _) = create_test_cqrs(&pool, &apalis_pool).await;

    let trade1 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(1))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    trade1.submit(&cqrs).await?;

    // Run vault discovery using the same trade data
    let vault_registry = test_store(pool.clone(), ());
    let context = VaultDiscoveryCtx {
        vault_registry: &vault_registry,
        orderbook: orderbook.orderbook_addr,
        order_owner: orderbook.owner,
    };

    discover_vaults_for_trade(&trade1.trade_event, &trade1.trade, &context).await?;

    let vault_agg_id = VaultRegistryId {
        orderbook: orderbook.orderbook_addr,
        owner: orderbook.owner,
    }
    .to_string();
    let events = fetch_events(&pool).await;

    // The trade produces Position + OnChainTrade + offchain order events,
    // followed by VaultRegistry discovery for both the USDC and equity vaults.
    let vault_events: Vec<_> = events
        .iter()
        .filter(|event| event.aggregate_type == "VaultRegistry")
        .collect();

    assert_eq!(vault_events.len(), 2, "Expected USDC + equity vault events");

    // Both events belong to the same VaultRegistry aggregate
    for vault_event in &vault_events {
        assert_eq!(vault_event.aggregate_id, vault_agg_id);
    }

    // Sell order: input=USDC (receives USDC), output=equity (gives equity).
    // Vault discovery processes input first, so UsdcVaultDiscovered uses input_vault_id
    // and EquityVaultDiscovered uses output_vault_id.
    let expected_usdc_vault_id = format!("{:#x}", trade1.input_vault_id);
    let expected_equity_vault_id = format!("{:#x}", trade1.output_vault_id);
    assert_ne!(
        expected_usdc_vault_id, expected_equity_vault_id,
        "Input and output vault IDs must be distinct to detect swap bugs"
    );

    assert_eq!(
        vault_events[0].event_type,
        "VaultRegistryEvent::UsdcVaultDiscovered"
    );
    assert_eq!(
        vault_events[0].payload["UsdcVaultDiscovered"]["vault_id"]
            .as_str()
            .unwrap(),
        expected_usdc_vault_id
    );

    assert_eq!(
        vault_events[1].event_type,
        "VaultRegistryEvent::EquityVaultDiscovered"
    );

    let equity_payload = &vault_events[1].payload["EquityVaultDiscovered"];
    assert_eq!(equity_payload["symbol"].as_str().unwrap(), TEST_AAPL);
    assert_eq!(
        equity_payload["vault_id"].as_str().unwrap(),
        expected_equity_vault_id
    );
    assert_eq!(
        equity_payload["token"].as_str().unwrap(),
        format!("{:#x}", orderbook.equity_tokens[TEST_AAPL])
    );

    Ok(())
}

/// Tests that very small fractional trades (0.001 shares) are tracked
/// precisely through the full onchain -> CQRS pipeline without triggering
/// execution.
#[tokio::test]
async fn tiny_fractional_trade_tracks_precisely() -> Result<(), Box<dyn std::error::Error>> {
    let mut orderbook = setup_anvil_orderbook().await;
    let (pool, apalis_pool) = setup_test_pools().await;
    let (cqrs, _position, position_query, _offchain_order, _) =
        create_test_cqrs(&pool, &apalis_pool).await;
    let symbol = Symbol::new(TEST_AAPL).unwrap();

    let trade1 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(0.001))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade1_agg = trade1.aggregate_id();
    let result = trade1.submit(&cqrs).await?;
    assert!(result.is_none(), "Tiny trade should not trigger execution");

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-0.001)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(0.001)))
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    assert_events(
        &pool,
        &[
            ExpectedEvent::new("OnChainTrade", &trade1_agg, "OnChainTradeEvent::Filled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::Initialized"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
            ExpectedEvent::new(
                "OnChainTrade",
                &trade1_agg,
                "OnChainTradeEvent::Acknowledged",
            ),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
        ],
    )
    .await;

    Ok(())
}

/// Tests that a single large trade (500 shares) immediately triggers execution
/// and is tracked correctly through the full pipeline.
#[tokio::test]
async fn large_trade_triggers_immediate_execution() -> Result<(), Box<dyn std::error::Error>> {
    let mut orderbook = setup_anvil_orderbook().await;
    let (pool, apalis_pool) = setup_test_pools().await;
    let (cqrs, _position, position_query, _offchain_order, _) =
        create_test_cqrs(&pool, &apalis_pool).await;
    let symbol = Symbol::new(TEST_AAPL).unwrap();

    let trade1 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(500))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade1_agg = trade1.aggregate_id();
    let order_id = trade1
        .submit(&cqrs)
        .await?
        .expect("500 shares should immediately cross threshold");

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-500)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(500)))
        .pending(Some(order_id))
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    let order_id_str = order_id.to_string();
    assert_events(
        &pool,
        &[
            ExpectedEvent::new("OnChainTrade", &trade1_agg, "OnChainTradeEvent::Filled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::Initialized"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
            ExpectedEvent::new(
                "OnChainTrade",
                &trade1_agg,
                "OnChainTradeEvent::Acknowledged",
            ),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OffChainOrderPlaced"),
            ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Placed"),
            ExpectedEvent::new(
                "OffchainOrder",
                &order_id_str,
                "OffchainOrderEvent::Accepted",
            ),
        ],
    )
    .await;

    Ok(())
}

/// Tests that buy + sell trades partially cancel each other. The net exposure
/// only crosses the threshold after enough sells accumulate, and the hedge
/// direction reflects the actual net position (Buy to offset a net-short).
#[tokio::test]
async fn mixed_direction_trades_partially_cancel() -> Result<(), Box<dyn std::error::Error>> {
    let mut orderbook = setup_anvil_orderbook().await;
    let (pool, apalis_pool) = setup_test_pools().await;
    let (cqrs, position, position_query, offchain_order, offchain_order_projection) =
        create_test_cqrs(&pool, &apalis_pool).await;
    let symbol = Symbol::new(TEST_AAPL).unwrap();

    // Trade 1: Buy 0.8 AAPL -> net=+0.8, below threshold
    let trade1 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(0.8))
        .direction(Direction::Buy)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade1_agg = trade1.aggregate_id();
    assert!(trade1.submit(&cqrs).await?.is_none());

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(0.8)))
        .accumulated_long(FractionalShares::new(float!(0.8)))
        .accumulated_short(FractionalShares::ZERO)
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    // Trade 2: Sell 0.5 AAPL -> net=+0.3, below threshold
    let trade2 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(0.5))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade2_agg = trade2.aggregate_id();
    assert!(trade2.submit(&cqrs).await?.is_none());

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(0.3)))
        .accumulated_long(FractionalShares::new(float!(0.8)))
        .accumulated_short(FractionalShares::new(float!(0.5)))
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    // Trade 3: Sell 0.8 AAPL -> net=-0.5, below threshold
    let trade3 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(0.8))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade3_agg = trade3.aggregate_id();
    assert!(trade3.submit(&cqrs).await?.is_none());

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-0.5)))
        .accumulated_long(FractionalShares::new(float!(0.8)))
        .accumulated_short(FractionalShares::new(float!(1.3)))
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    // Trade 4: Sell 0.6 AAPL -> net=-1.1, crosses threshold -> Buy hedge for 1.1 shares
    let trade4 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(0.6))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade4_agg = trade4.aggregate_id();
    let order_id = trade4
        .submit(&cqrs)
        .await?
        .expect("Net -1.1 crosses threshold");
    let order_id_str = order_id.to_string();

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-1.1)))
        .accumulated_long(FractionalShares::new(float!(0.8)))
        .accumulated_short(FractionalShares::new(float!(1.9)))
        .pending(Some(order_id))
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    // Verify the hedge is a Buy for 1.1 shares (offsetting net-short)
    let events = assert_events(
        &pool,
        &[
            ExpectedEvent::new("OnChainTrade", &trade1_agg, "OnChainTradeEvent::Filled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::Initialized"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
            ExpectedEvent::new(
                "OnChainTrade",
                &trade1_agg,
                "OnChainTradeEvent::Acknowledged",
            ),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
            ExpectedEvent::new("OnChainTrade", &trade2_agg, "OnChainTradeEvent::Filled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
            ExpectedEvent::new(
                "OnChainTrade",
                &trade2_agg,
                "OnChainTradeEvent::Acknowledged",
            ),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
            ExpectedEvent::new("OnChainTrade", &trade3_agg, "OnChainTradeEvent::Filled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
            ExpectedEvent::new(
                "OnChainTrade",
                &trade3_agg,
                "OnChainTradeEvent::Acknowledged",
            ),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
            ExpectedEvent::new("OnChainTrade", &trade4_agg, "OnChainTradeEvent::Filled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
            ExpectedEvent::new(
                "OnChainTrade",
                &trade4_agg,
                "OnChainTradeEvent::Acknowledged",
            ),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OffChainOrderPlaced"),
            ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Placed"),
            ExpectedEvent::new(
                "OffchainOrder",
                &order_id_str,
                "OffchainOrderEvent::Accepted",
            ),
        ],
    )
    .await;

    assert_eq!(events[22].event_type, "OffchainOrderEvent::Placed");
    let placed = &events[22].payload["Placed"];
    assert_eq!(placed["direction"].as_str().unwrap(), "buy");
    assert_eq!(placed["shares"].as_str().unwrap(), "1.1");

    // Fill the hedge and verify net returns to zero
    poll_submitted_orders(
        &MockExecutor::new(),
        &offchain_order_projection,
        &offchain_order,
        &position,
    )
    .await?;

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::ZERO)
        .accumulated_long(FractionalShares::new(float!(0.8)))
        .accumulated_short(FractionalShares::new(float!(1.9)))
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    Ok(())
}

/// Tests that new trades arriving while an offchain order is pending don't trigger
/// a second offchain order. The position updates but no new PlaceOrder command fires.
#[tokio::test]
async fn pending_order_blocks_new_execution() -> Result<(), Box<dyn std::error::Error>> {
    let mut orderbook = setup_anvil_orderbook().await;
    let (pool, apalis_pool) = setup_test_pools().await;
    let (cqrs, _position, position_query, _offchain_order, _) =
        create_test_cqrs(&pool, &apalis_pool).await;
    let symbol = Symbol::new(TEST_AAPL).unwrap();

    // Trade 1: Sell 1.5 AAPL -> crosses threshold, offchain order placed
    let trade1 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(1.5))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade1_agg = trade1.aggregate_id();
    let order_id = trade1
        .submit(&cqrs)
        .await?
        .expect("1.5 shares crosses threshold");
    let order_id_str = order_id.to_string();

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-1.5)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(1.5)))
        .pending(Some(order_id))
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    // Trade 2: Sell 0.5 more while pending -> position updates, but no new offchain order
    let trade2 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(0.5))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let trade2_agg = trade2.aggregate_id();
    let result2 = trade2.submit(&cqrs).await?;
    assert!(
        result2.is_none(),
        "No new offchain order while one is pending"
    );

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-2.0)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(2.0)))
        .pending(Some(order_id))
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    // Assert event sequence: trade 2 only produces OnChainOrderFilled, no OffChainOrderPlaced
    assert_events(
        &pool,
        &[
            // Trade 1 events + offchain order
            ExpectedEvent::new("OnChainTrade", &trade1_agg, "OnChainTradeEvent::Filled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::Initialized"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
            ExpectedEvent::new(
                "OnChainTrade",
                &trade1_agg,
                "OnChainTradeEvent::Acknowledged",
            ),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OffChainOrderPlaced"),
            ExpectedEvent::new("OffchainOrder", &order_id_str, "OffchainOrderEvent::Placed"),
            ExpectedEvent::new(
                "OffchainOrder",
                &order_id_str,
                "OffchainOrderEvent::Accepted",
            ),
            // Trade 2 events: only onchain fill, no offchain order
            ExpectedEvent::new("OnChainTrade", &trade2_agg, "OnChainTradeEvent::Filled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
            ExpectedEvent::new(
                "OnChainTrade",
                &trade2_agg,
                "OnChainTradeEvent::Acknowledged",
            ),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
        ],
    )
    .await;

    Ok(())
}

/// Tests that the event queue deduplicates identical onchain events (same tx_hash + log_index),
/// ensuring the CQRS pipeline processes each blockchain event exactly once.
#[tokio::test]
async fn duplicate_onchain_event_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    let mut orderbook = setup_anvil_orderbook().await;
    let (pool, apalis_pool) = setup_test_pools().await;
    let (cqrs, _position, position_query, _offchain_order, _) =
        create_test_cqrs(&pool, &apalis_pool).await;
    let symbol = Symbol::new(TEST_AAPL).unwrap();

    let trade1 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(0.5))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;

    // Process the trade through the CQRS pipeline
    trade1.submit(&cqrs).await?;

    // Submit the same trade again — CQRS aggregate rejects the duplicate
    trade1.submit(&cqrs).await?;

    // Verify exactly one set of CQRS events was emitted
    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-0.5)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(0.5)))
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    let trade1_agg = trade1.aggregate_id();
    assert_events(
        &pool,
        &[
            ExpectedEvent::new("OnChainTrade", &trade1_agg, "OnChainTradeEvent::Filled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::Initialized"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainOrderFilled"),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillApplied"),
            ExpectedEvent::new(
                "OnChainTrade",
                &trade1_agg,
                "OnChainTradeEvent::Acknowledged",
            ),
            ExpectedEvent::new("Position", TEST_AAPL, "PositionEvent::OnChainFillSettled"),
        ],
    )
    .await;

    Ok(())
}

/// Tests that max_amount constrains counter trade sizes when its
/// share-equivalent is tighter than max_shares. At $100/share, max_amount=$100
/// converts to a 1-share cap (tighter than max_shares=2). A 3-share onchain
/// sell requires 3 cycles of 1-share hedges to fully close.
#[tokio::test]
async fn operational_limits_dollar_cap_constrains_counter_trades_across_cycles()
-> Result<(), Box<dyn std::error::Error>> {
    let mut orderbook = setup_anvil_orderbook().await;
    let (pool, apalis_pool) = setup_test_pools().await;

    // Per-asset limit = 1 share. A 3-share onchain sell requires
    // 3 cycles of 1-share hedges to fully close.
    let assets = AssetsConfig {
        equities: EquitiesConfig {
            operational_limit: None,
            symbols: HashMap::from([(
                Symbol::new(TEST_AAPL).unwrap(),
                EquityAssetConfig {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    pyth_feed_id: None,
                    vault_ids: Vec::new(),
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    extended_hours_counter_trading: OperationMode::Disabled,
                    operational_limit: Some(
                        Positive::new(FractionalShares::new(float!(1))).unwrap(),
                    ),
                },
            )]),
        },
        cash: None,
    };
    let (cqrs, position, position_query, offchain_order, offchain_order_projection) =
        create_test_cqrs_with_assets(&pool, &apalis_pool, assets.clone()).await;
    let symbol = Symbol::new(TEST_AAPL).unwrap();

    // 3-share sell -> net = -3
    let trade1 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(3))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let order1 = trade1
        .submit(&cqrs)
        .await?
        .expect("3 shares crosses threshold");

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-3)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(3)))
        .pending(Some(order1))
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    // Dollar cap limits to 1 share despite max_shares=2
    let events = fetch_events(&pool).await;
    let placed1 = events
        .iter()
        .find(|event| event.event_type == "OffchainOrderEvent::Placed")
        .unwrap();
    assert_eq!(
        placed1.payload["Placed"]["shares"].as_str().unwrap(),
        "1",
        "First hedge capped to 1 share (max_amount $100 / $100 per share)"
    );

    // Fill first order -> net becomes -2
    poll_submitted_orders(
        &MockExecutor::new(),
        &offchain_order_projection,
        &offchain_order,
        &position,
    )
    .await?;

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-2)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(3)))
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    // Cycle 2: max_amount still the binding constraint at 1 share
    check_and_execute_accumulated_positions(
        &MockExecutor::new(),
        AccumulatedPositionExecutionCtx {
            position: &position,
            position_projection: &position_query,
            offchain_order: &offchain_order,
            order_placer: cqrs.order_placer.as_ref(),
            counter_trade_submission_lock: &cqrs.counter_trade_submission_lock,
            threshold: &ExecutionThreshold::whole_share(),
            assets: &assets,
        },
        |_| true,
    )
    .await?;

    let events = fetch_events(&pool).await;
    let placed_events: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == "OffchainOrderEvent::Placed")
        .collect();
    assert_eq!(placed_events.len(), 2, "Should have two Placed events");
    assert_eq!(
        placed_events[1].payload["Placed"]["shares"]
            .as_str()
            .unwrap(),
        "1",
        "Second hedge also capped to 1 share by max_amount"
    );

    // Fill second order -> net becomes -1
    poll_submitted_orders(
        &MockExecutor::new(),
        &offchain_order_projection,
        &offchain_order,
        &position,
    )
    .await?;

    // Cycle 3: 1 remaining share, still capped to 1 (but matches)
    check_and_execute_accumulated_positions(
        &MockExecutor::new(),
        AccumulatedPositionExecutionCtx {
            position: &position,
            position_projection: &position_query,
            offchain_order: &offchain_order,
            order_placer: cqrs.order_placer.as_ref(),
            counter_trade_submission_lock: &cqrs.counter_trade_submission_lock,
            threshold: &ExecutionThreshold::whole_share(),
            assets: &assets,
        },
        |_| true,
    )
    .await?;

    let events = fetch_events(&pool).await;
    let placed_events: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == "OffchainOrderEvent::Placed")
        .collect();
    assert_eq!(placed_events.len(), 3, "Should have three Placed events");
    assert_eq!(
        placed_events[2].payload["Placed"]["shares"]
            .as_str()
            .unwrap(),
        "1",
        "Third hedge is 1 share (remaining exposure equals max_amount share-equivalent)"
    );

    // Fill third order -> fully hedged
    poll_submitted_orders(
        &MockExecutor::new(),
        &offchain_order_projection,
        &offchain_order,
        &position,
    )
    .await?;

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::ZERO)
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(3)))
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    // No-op after fully hedged
    let events_before = fetch_events(&pool).await;
    check_and_execute_accumulated_positions(
        &MockExecutor::new(),
        AccumulatedPositionExecutionCtx {
            position: &position,
            position_projection: &position_query,
            offchain_order: &offchain_order,
            order_placer: cqrs.order_placer.as_ref(),
            counter_trade_submission_lock: &cqrs.counter_trade_submission_lock,
            threshold: &ExecutionThreshold::whole_share(),
            assets: &assets,
        },
        |_| true,
    )
    .await?;
    let events_after = fetch_events(&pool).await;
    assert_eq!(
        events_before.len(),
        events_after.len(),
        "No new events after fully hedged"
    );

    Ok(())
}

/// Tests that max_shares constrains counter trade sizes when it is tighter
/// than the max_amount share-equivalent. max_shares=2 with max_amount=$10000
/// (100-share equivalent at $100/share), so max_shares is binding. A 5-share
/// onchain sell hedges 2 shares, fails, retries (also capped to 2), and the
/// pending order blocks concurrent checker cycles.
#[tokio::test]
async fn operational_limits_shares_cap_constrains_counter_trades_with_failure_and_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let mut orderbook = setup_anvil_orderbook().await;
    let (pool, apalis_pool) = setup_test_pools().await;

    // Per-asset shares limit = 2. A 5-share onchain sell hedges 2 shares,
    // fails, retries (also capped to 2), and the pending order blocks
    // concurrent checker cycles.
    let assets = AssetsConfig {
        equities: EquitiesConfig {
            operational_limit: None,
            symbols: HashMap::from([(
                Symbol::new(TEST_AAPL).unwrap(),
                EquityAssetConfig {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    pyth_feed_id: None,
                    vault_ids: Vec::new(),
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    extended_hours_counter_trading: OperationMode::Disabled,
                    operational_limit: Some(
                        Positive::new(FractionalShares::new(float!(2))).unwrap(),
                    ),
                },
            )]),
        },
        cash: None,
    };
    let (cqrs, position, position_query, offchain_order, offchain_order_projection) =
        create_test_cqrs_with_assets(&pool, &apalis_pool, assets.clone()).await;
    let symbol = Symbol::new(TEST_AAPL).unwrap();

    // 5-share sell -> net = -5, capped to 2 shares by max_shares
    let trade1 = orderbook
        .take_order()
        .symbol(TEST_AAPL)
        .amount(float!(5))
        .direction(Direction::Sell)
        .price(AAPL_PRICE)
        .call()
        .await;
    let order1 = trade1
        .submit(&cqrs)
        .await?
        .expect("5 shares crosses threshold");

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-5)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(5)))
        .pending(Some(order1))
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    // Verify first hedge is capped to 2 shares by max_shares
    let events = fetch_events(&pool).await;
    let placed1 = events
        .iter()
        .find(|event| event.event_type == "OffchainOrderEvent::Placed")
        .unwrap();
    assert_eq!(
        placed1.payload["Placed"]["shares"].as_str().unwrap(),
        "2",
        "First hedge capped to 2 shares by max_shares"
    );

    // Broker FAILS the order
    let failed_executor = MockExecutor::new().with_order_status(OrderState::Failed {
        failed_at: Utc::now(),
        error_reason: Some("Broker rejected".to_string()),
        shares_filled: None,
        avg_price: None,
    });
    poll_submitted_orders(
        &failed_executor,
        offchain_order_projection.as_ref(),
        &offchain_order,
        &position,
    )
    .await?;

    // Pending cleared after failure, position still has -5 net
    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-5)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(5)))
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    // Position checker retries: also limited to 2 shares by max_shares
    check_and_execute_accumulated_positions(
        &MockExecutor::new(),
        AccumulatedPositionExecutionCtx {
            position: &position,
            position_projection: &position_query,
            offchain_order: &offchain_order,
            order_placer: cqrs.order_placer.as_ref(),
            counter_trade_submission_lock: &cqrs.counter_trade_submission_lock,
            threshold: &ExecutionThreshold::whole_share(),
            assets: &assets,
        },
        |_| true,
    )
    .await?;

    let pos = position_query.load(&symbol).await?.unwrap();
    let order2 = pos
        .pending_offchain_order_id
        .expect("Should have a new pending order after retry");
    assert_ne!(order1, order2, "Retry creates a new offchain order");

    let events = fetch_events(&pool).await;
    let placed_events: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == "OffchainOrderEvent::Placed")
        .collect();
    assert_eq!(placed_events.len(), 2, "Original + retry = 2 placed events");
    assert_eq!(
        placed_events[1].payload["Placed"]["shares"]
            .as_str()
            .unwrap(),
        "2",
        "Retry also limited to 2 shares by max_shares"
    );

    // While pending, position checker should NOT place another order
    let events_before = fetch_events(&pool).await;
    check_and_execute_accumulated_positions(
        &MockExecutor::new(),
        AccumulatedPositionExecutionCtx {
            position: &position,
            position_projection: &position_query,
            offchain_order: &offchain_order,
            order_placer: cqrs.order_placer.as_ref(),
            counter_trade_submission_lock: &cqrs.counter_trade_submission_lock,
            threshold: &ExecutionThreshold::whole_share(),
            assets: &assets,
        },
        |_| true,
    )
    .await?;
    let events_after = fetch_events(&pool).await;
    assert_eq!(
        events_before.len(),
        events_after.len(),
        "Pending order blocks new execution"
    );

    // Fill the retry -> net becomes -3
    poll_submitted_orders(
        &MockExecutor::new(),
        &offchain_order_projection,
        &offchain_order,
        &position,
    )
    .await?;

    assert_position()
        .query(&position_query)
        .symbol(&symbol)
        .net(FractionalShares::new(float!(-3)))
        .accumulated_long(FractionalShares::ZERO)
        .accumulated_short(FractionalShares::new(float!(5)))
        .pending(None)
        .last_price_usdc(float!(&AAPL_PRICE.to_string()))
        .call()
        .await;

    Ok(())
}
